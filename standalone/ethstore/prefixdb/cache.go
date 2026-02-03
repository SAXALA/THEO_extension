package prefixdb

import (
	"bytes"
	"container/list"
	"fmt"
	"sync"
	"time"
)

// NodeCache manages node caching with reference counting
// Uses an array of linked lists, each corresponding to a reference count value
// Nodes with refCount=1 are in refLists[1], refCount=2 in refLists[2], etc.
// Within each list, nodes are ordered by insertion time, with newest at the front
type NodeCache struct {
	capacity           int                      // Cache capacity
	cache              map[string]*list.Element // Map from keys to list nodes
	refLists           []*list.List             // Lists grouped by reference count
	maxAllowedRefCount int                      // Maximum allowed reference count
	lock               sync.RWMutex
	db                 *PrefixDB // Reference to PrefixDB for batch operations

	pathQueue       chan pathRequest
	workerWg        sync.WaitGroup
	isWorkerRunning bool
	workerLock      sync.Mutex
	batchSize       int
}

// Request structure for caching paths
type pathRequest struct {
	key        string
	db         *PrefixDB
	resultChan chan bool // Channel to signal completion
}

type ModifiedType int

const (
	None          ModifiedType = iota
	ValueModified              // Value was modified
)

// Data structure stored in list nodes
type cacheEntry struct {
	key           string
	value         []byte
	storageFileID uint32 // Storage file ID
	storageOffset int64  // Storage offset
	storageSize   uint64 // Storage size
	modifiedType  ModifiedType
	refCount      int // Reference count
}

type CacheInfo struct {
	storageFileID uint32 // Storage file ID
	storageOffset int64  // Storage offset
	storageSize   uint64 // Storage size
}

// NewNodeCache creates a new node cache
func NewNodeCache(capacity int, db *PrefixDB) *NodeCache {
	// Initially create a fixed number of lists, can be expanded later
	const initialListCount = 1025
	const defaultMaxRefCount = 1024
	const defaultQueueSize = 10000
	const defaultBatchSize = 100

	refLists := make([]*list.List, initialListCount)
	for i := range refLists {
		refLists[i] = list.New()
	}

	nc := &NodeCache{
		capacity:           capacity,
		cache:              make(map[string]*list.Element),
		refLists:           refLists,
		maxAllowedRefCount: defaultMaxRefCount,
		pathQueue:          make(chan pathRequest, defaultQueueSize),
		batchSize:          defaultBatchSize,
		db:                 db,
	}

	nc.startWorker()

	return nc
}

// MarkNodeModified marks a node as modified
func (nc *NodeCache) MarkNodeModified(key string) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	if element, exists := nc.cache[key]; exists {
		entry := element.Value.(*cacheEntry)
		entry.modifiedType = ValueModified
	}
}

// ensureRefListCapacity ensures there are enough reference count lists
func (nc *NodeCache) ensureRefListCapacity(refCount int) {
	if refCount > nc.maxAllowedRefCount {
		refCount = nc.maxAllowedRefCount
	}
	for refCount >= len(nc.refLists) && len(nc.refLists) <= nc.maxAllowedRefCount {
		nc.refLists = append(nc.refLists, list.New())
	}
}

func (nc *NodeCache) Has(key string) bool {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	_, exists := nc.cache[key]
	return exists
}

// Get retrieves a value from cache
func (nc *NodeCache) Get(key string) ([]byte, CacheInfo, bool) {
	nc.lock.RLock()
	element, exists := nc.cache[key]
	if !exists {
		nc.lock.RUnlock()
		return nil, CacheInfo{}, false
	}

	// Get entry
	entry := element.Value.(*cacheEntry)
	refCount := entry.refCount
	nc.lock.RUnlock()

	// Increase reference count and move to appropriate list
	nc.lock.Lock()
	defer nc.lock.Unlock()

	// Ensure element still exists (may have been deleted between read and write locks)
	if element, exists = nc.cache[key]; !exists {
		return entry.value, CacheInfo{
			storageFileID: entry.storageFileID,
			storageOffset: entry.storageOffset,
			storageSize:   entry.storageSize,
		}, true // Return previously read value
	}

	entry = element.Value.(*cacheEntry)

	nc.refLists[refCount].MoveToFront(element)
	return entry.value, CacheInfo{
		storageFileID: entry.storageFileID,
		storageOffset: entry.storageOffset,
		storageSize:   entry.storageSize,
	}, true
}

// Put adds or updates a value in cache
func (nc *NodeCache) Put(key string, value []byte, cacheInfo CacheInfo, modfiedType ModifiedType) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	// If key already exists, update value and reference count
	if element, exists := nc.cache[key]; exists {
		entry := element.Value.(*cacheEntry)
		oldRefCount := entry.refCount
		newRefCount := oldRefCount + 1
		if newRefCount > nc.maxAllowedRefCount {
			newRefCount = nc.maxAllowedRefCount
		}
		entry.value = value
		entry.refCount = newRefCount

		entry.storageFileID = cacheInfo.storageFileID
		entry.storageOffset = cacheInfo.storageOffset
		entry.storageSize = cacheInfo.storageSize

		// Just Update modifiedType to the highest level
		if entry.modifiedType < 1 {
			entry.modifiedType = modfiedType
		}

		// Ensure enough lists are available
		nc.ensureRefListCapacity(newRefCount)

		// Remove from old list and add to new list
		nc.refLists[oldRefCount].Remove(element)
		newElement := nc.refLists[newRefCount].PushFront(entry)
		nc.cache[key] = newElement
		return
	}

	// If cache is full, evict item with lowest reference count
	if len(nc.cache) >= nc.capacity {
		nc.evictMinRefCount()
	}

	// Add new item with initial reference count of 1
	entry := &cacheEntry{
		key:           key,
		value:         value,
		storageFileID: cacheInfo.storageFileID,
		modifiedType:  modfiedType,
		storageOffset: cacheInfo.storageOffset,
		storageSize:   cacheInfo.storageSize,
		refCount:      1,
	}

	// Add to reference count 1 list
	nc.ensureRefListCapacity(1)
	element := nc.refLists[1].PushFront(entry)
	nc.cache[key] = element
}

// evictMinRefCount evicts item with lowest reference count
func (nc *NodeCache) evictMinRefCount() {
	// Start search from lowest reference count list
	for i := 1; i <= nc.maxAllowedRefCount; i++ {
		list := nc.refLists[i]
		if list.Len() > 0 {
			// Remove from list tail (least recently used)
			element := list.Back()
			entry := element.Value.(*cacheEntry)

			if entry.modifiedType > 0 && nc.db != nil && nc.db.batch != nil {
				nc.db.batch.add([]byte(entry.key), entry.value, entry.storageFileID, entry.storageOffset, entry.storageSize, entry.modifiedType)
			}

			delete(nc.cache, entry.key)
			list.Remove(element)
			return
		}
	}
}

// IncrementRefCount increases a node's reference count
func (nc *NodeCache) IncrementRefCount(key string) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	element, exists := nc.cache[key]
	if !exists {
		return
	}

	entry := element.Value.(*cacheEntry)
	oldRefCount := entry.refCount
	newRefCount := oldRefCount + 1
	if newRefCount > nc.maxAllowedRefCount {
		newRefCount = nc.maxAllowedRefCount
	}
	entry.refCount = newRefCount

	// Ensure enough lists are available
	nc.ensureRefListCapacity(newRefCount)

	// Remove from old list and add to new list
	nc.refLists[oldRefCount].Remove(element)
	newElement := nc.refLists[newRefCount].PushFront(entry)
	nc.cache[key] = newElement
}

// CachePathToNode caches all nodes on path from root to the current node
func (nc *NodeCache) CachePathToNode(key string, db *PrefixDB) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	currentKey := key
	for len(currentKey) > 0 {
		// Move to parent node
		currentKey = currentKey[:len(currentKey)-1]

		if currentKey == "" {
			break // Don't process empty string (root)
		}

		// If already in cache, increment reference count
		if element, exists := nc.cache[currentKey]; exists {
			entry := element.Value.(*cacheEntry)
			oldRefCount := entry.refCount
			newRefCount := oldRefCount + 1
			if newRefCount > nc.maxAllowedRefCount {
				newRefCount = nc.maxAllowedRefCount
				entry.refCount = newRefCount
				continue
			}
			entry.refCount = newRefCount
			nc.ensureRefListCapacity(newRefCount)
			nc.refLists[oldRefCount].Remove(element)
			newElement := nc.refLists[newRefCount].PushFront(entry)
			nc.cache[currentKey] = newElement
		} else {

			// offset, exists := db.accountIndex.get(currentKey)
			// if !exists {
			// 	// fmt.Printf("Account key %s not found in index\n", currentKey)
			// 	continue // Skip if key not found
			// }

			node, err := db.getNode([]byte(currentKey))
			if err != nil {
				fmt.Printf("Error retrieving node %s: %v\n", currentKey, err)
				continue
			}
			if node == nil {
				// fmt.Printf("Account key %s not found in index\n", currentKey)
				continue // Skip if key not found
			}

			nodeValue, err := db.readFromFile(node.offset)
			if err != nil {
				continue // Skip on error
			}

			// If cache is full, evict item with lowest reference count
			if len(nc.cache) >= nc.capacity {
				nc.evictMinRefCount()
			}

			//build slot indices

			// Add new entry to cache
			entry := &cacheEntry{
				key:           currentKey,
				value:         nodeValue,
				storageFileID: node.storageFileID,
				storageOffset: node.storageOffset,
				storageSize:   node.storageSize,
				modifiedType:  0,
				refCount:      1,
			}
			nc.ensureRefListCapacity(1)
			element := nc.refLists[1].PushFront(entry)
			nc.cache[currentKey] = element
		}
	}
}

// ContainsKey checks if a key exists in cache
func (nc *NodeCache) ContainsKey(key string) bool {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if _, exists := nc.cache[key]; exists {
		return true
	}
	return false
}

// GetRefCount gets a key's reference count
func (nc *NodeCache) GetRefCount(key string) int {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if element, exists := nc.cache[key]; exists {
		return element.Value.(*cacheEntry).refCount
	}
	return 0
}

// Evict removes a key from the cache ,if it has been modified ,adds it to the batch for writing
func (nc *NodeCache) Evict(key string) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	element, exists := nc.cache[key]
	if !exists {
		return
	}

	entry := element.Value.(*cacheEntry)
	refCount := entry.refCount

	if entry.modifiedType > 0 && nc.db != nil && nc.db.batch != nil {
		// If the node was modified, add it to the batch for writing
		nc.db.batch.add([]byte(entry.key), entry.value, entry.storageFileID, entry.storageOffset, entry.storageSize, entry.modifiedType)
	}

	// Remove from the appropriate reference count list
	nc.refLists[refCount].Remove(element)

	// Remove from cache map
	delete(nc.cache, key)
}

// Delete removes a key from the cache,don't add to batch
func (nc *NodeCache) Delete(key string) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	element, exists := nc.cache[key]
	if !exists {
		return
	}

	entry := element.Value.(*cacheEntry)
	refCount := entry.refCount

	// if entry.modifiedType > 0 && nc.db != nil && nc.db.batch != nil {
	// 	// If the node was modified, add it to the batch for writing
	// 	nc.db.batch.add([]byte(entry.key), entry.value, entry.slotIndices, entry.modifiedType)
	// }

	// Remove from the appropriate reference count list
	nc.refLists[refCount].Remove(element)

	// Remove from cache map
	delete(nc.cache, key)
}

// UpdateStoragePointer updates storage file ID and offset for a cached node
func (nc *NodeCache) UpdateStoragePointer(key string, cacheInfo CacheInfo) {
	nc.lock.Lock()

	if el, ok := nc.cache[key]; ok {
		ent := el.Value.(*cacheEntry)
		ent.storageFileID = cacheInfo.storageFileID
		ent.storageOffset = cacheInfo.storageOffset
		ent.storageSize = cacheInfo.storageSize
	}
	nc.lock.Unlock()

	// check in batch
	if nc.db != nil && nc.db.batch != nil {
		nc.db.batch.updateStoragePointer([]byte(key), cacheInfo)
	}
}

func (nc *NodeCache) UpdateValue(key string, value []byte, modifiedType ModifiedType) {
	nc.lock.Lock()

	if el, ok := nc.cache[key]; ok {
		ent := el.Value.(*cacheEntry)
		ent.value = value
		if ent.modifiedType < modifiedType {
			ent.modifiedType = modifiedType
		}
	} else {
		entry := &cacheEntry{
			key:          key,
			value:        value,
			modifiedType: modifiedType,
			refCount:     1,
		}
		nc.ensureRefListCapacity(1)
		element := nc.refLists[1].PushFront(entry)
		nc.cache[key] = element

		if len(nc.cache) > nc.capacity {
			nc.evictMinRefCount()
		}

	}
	nc.lock.Unlock()
}

func (nc *NodeCache) FlushModifiedNodes() {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	// result := make(map[string]cacheEntry)

	for i := 1; i <= nc.maxAllowedRefCount; i++ {
		for e := nc.refLists[i].Front(); e != nil; e = e.Next() {
			entry := e.Value.(*cacheEntry)
			if entry.modifiedType > 0 {
				if nc.db != nil && nc.db.batch != nil {
					nc.db.batch.add([]byte(entry.key), entry.value, entry.storageFileID, entry.storageOffset, entry.storageSize, entry.modifiedType)
				}
				entry.modifiedType = None // Reset modified status after flushing
			}
		}
	}

}

const defaultStorageBufferChunks = 16
const defaultStorageEntryPool = 8192 * 2

type storageChunkEntry struct {
	keyStart   []byte
	keyEnd     []byte
	entries    []kvPair
	backing    []byte
	lastAccess uint64
}

type storageChunkBuffer struct {
	accountKey    string
	chunks        []*storageChunkEntry
	maxChunks     int
	accessCounter uint64
	entryPool     [][]kvPair
	maxEntryPool  int
}

func (b *storageChunkBuffer) ensureLimits() {
	if b.maxChunks <= 0 {
		b.maxChunks = defaultStorageBufferChunks
	}
}

func (b *storageChunkBuffer) ensureEntryPoolLimits() {
	if b.maxEntryPool <= 0 {
		b.maxEntryPool = defaultStorageEntryPool
	}
}

func (b *storageChunkBuffer) borrowEntries(size int) []kvPair {
	if size <= 0 {
		return nil
	}
	b.ensureEntryPoolLimits()
	for i := len(b.entryPool) - 1; i >= 0; i-- {
		buf := b.entryPool[i]
		if cap(buf) >= size {
			entries := buf[:size]
			b.entryPool = append(b.entryPool[:i], b.entryPool[i+1:]...)
			return entries
		}
	}
	return make([]kvPair, size)
}

func (b *storageChunkBuffer) returnEntries(entries []kvPair) {
	if entries == nil {
		return
	}
	if cap(entries) == 0 {
		return
	}
	for i := range entries {
		entries[i] = kvPair{}
	}
	entries = entries[:0]
	b.ensureEntryPoolLimits()
	if len(b.entryPool) >= b.maxEntryPool {
		return
	}
	b.entryPool = append(b.entryPool, entries)
}

func (b *storageChunkBuffer) releaseChunk(chunk *storageChunkEntry) {
	if chunk == nil {
		return
	}
	if len(chunk.entries) > 0 {
		b.returnEntries(chunk.entries)
		chunk.entries = nil
	}
	if chunk.backing != nil {
		putDataBuffer(chunk.backing)
		chunk.backing = nil
	}
}

func (b *storageChunkBuffer) reset() {
	for _, chunk := range b.chunks {
		b.releaseChunk(chunk)
	}
	b.chunks = nil
	b.accountKey = ""
	b.accessCounter = 0
}

func (b *storageChunkBuffer) covers(accountKey string, key []byte) bool {
	if b.accountKey != accountKey {
		return false
	}
	if len(b.chunks) == 0 {
		return true
	}
	if len(key) == 0 {
		return true
	}
	for _, chunk := range b.chunks {
		if len(chunk.keyStart) > 0 && bytes.Compare(key, chunk.keyStart) < 0 {
			continue
		}
		if len(chunk.keyEnd) > 0 && bytes.Compare(key, chunk.keyEnd) > 0 {
			continue
		}
		return true
	}
	return false
}

func (b *storageChunkBuffer) adopt(accountKey string, entries []kvPair, backing []byte) {
	b.ensureLimits()
	if accountKey == "" {
		if len(entries) > 0 {
			b.returnEntries(entries)
		}
		if backing != nil {
			putDataBuffer(backing)
		}
		b.reset()
		return
	}
	if b.accountKey != accountKey {
		b.reset()
		b.accountKey = accountKey
	}
	if len(entries) == 0 {
		if backing != nil {
			putDataBuffer(backing)
		}
		return
	}
	chunk := &storageChunkEntry{
		keyStart: entries[0].key,
		keyEnd:   entries[len(entries)-1].key,
		entries:  entries,
		backing:  backing,
	}
	b.accessCounter++
	chunk.lastAccess = b.accessCounter
	for i, existing := range b.chunks {
		if bytes.Equal(existing.keyStart, chunk.keyStart) && bytes.Equal(existing.keyEnd, chunk.keyEnd) {
			b.releaseChunk(existing)
			b.chunks[i] = chunk
			return
		}
	}
	b.chunks = append(b.chunks, chunk)
	b.evictIfNeeded()
}

func (b *storageChunkBuffer) evictIfNeeded() {
	b.ensureLimits()
	for len(b.chunks) > b.maxChunks {
		idx := 0
		oldest := b.chunks[0].lastAccess
		for i := 1; i < len(b.chunks); i++ {
			if b.chunks[i].lastAccess < oldest {
				oldest = b.chunks[i].lastAccess
				idx = i
			}
		}
		victim := b.chunks[idx]
		b.releaseChunk(victim)
		b.chunks = append(b.chunks[:idx], b.chunks[idx+1:]...)
	}
}

func (b *storageChunkBuffer) lookup(key []byte) ([]byte, bool) {
	if len(b.chunks) == 0 || len(key) == 0 {
		return nil, false
	}
	for _, chunk := range b.chunks {
		if len(chunk.keyStart) > 0 && bytes.Compare(key, chunk.keyStart) < 0 {
			continue
		}
		if len(chunk.keyEnd) > 0 && bytes.Compare(key, chunk.keyEnd) > 0 {
			continue
		}
		idx, ok := binarySearchKVPairs(chunk.entries, key)
		if !ok {
			continue
		}
		b.accessCounter++
		chunk.lastAccess = b.accessCounter
		return chunk.entries[idx].val, true
	}
	return nil, false
}

func (b *storageChunkBuffer) invalidate(accountKey string) {
	if b.accountKey == accountKey {
		b.reset()
	}
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dup := make([]byte, len(src))
	copy(dup, src)
	return dup
}

// startWorker starts the background worker for processing path caching requests
func (nc *NodeCache) startWorker() {
	nc.workerLock.Lock()
	defer nc.workerLock.Unlock()

	if !nc.isWorkerRunning {
		nc.isWorkerRunning = true
		nc.workerWg.Add(1)

		go func() {
			defer nc.workerWg.Done()

			pathBatch := make(map[string]pathRequest)
			timer := time.NewTimer(100 * time.Millisecond)

			for {
				select {
				case req, ok := <-nc.pathQueue:
					if !ok {
						// Channel closed, process remaining batch and exit
						nc.processBatch(pathBatch)
						return
					}

					// add request to batch
					pathBatch[req.key] = req

					// if batch size reached, process it
					if len(pathBatch) >= nc.batchSize {
						nc.processBatch(pathBatch)
						pathBatch = make(map[string]pathRequest)
						timer.Reset(100 * time.Millisecond)
					}

				case <-timer.C:
					// Time to process the current batch, ensure low frequency calls are handled
					if len(pathBatch) > 0 {
						nc.processBatch(pathBatch)
						pathBatch = make(map[string]pathRequest)
					}
					timer.Reset(100 * time.Millisecond)
				}
			}
		}()
	}
}

// close the worker and wait for it to finish
func (nc *NodeCache) Close() {
	nc.workerLock.Lock()

	if nc.isWorkerRunning {
		nc.isWorkerRunning = false
		close(nc.pathQueue)
	}
	nc.workerLock.Unlock()
	// Wait for worker to finish
	nc.workerWg.Wait()

	nc.FlushModifiedNodes()
}

// processBatch processes a batch of path caching requests
func (nc *NodeCache) processBatch(batch map[string]pathRequest) {
	if len(batch) == 0 {
		return
	}

	// collect all unique path nodes
	pathNodes := make(map[string]struct{})
	dbRef := (*PrefixDB)(nil)

	for key, req := range batch {
		if dbRef == nil && req.db != nil {
			dbRef = req.db
		}

		// add all nodes in the path to the set
		currentKey := key
		for len(currentKey) > 0 {
			currentKey = currentKey[:len(currentKey)-1]
			if currentKey != "" {
				pathNodes[currentKey] = struct{}{}
			}
		}
	}

	// cache all path nodes
	if dbRef != nil {
		nc.batchCachePathNodes(pathNodes, dbRef)
	}

	// notify all requesters
	for _, req := range batch {
		if req.resultChan != nil {
			req.resultChan <- true
		}
	}
}

// batchCachePathNodes caches multiple path nodes in a single operation
func (nc *NodeCache) batchCachePathNodes(nodes map[string]struct{}, db *PrefixDB) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	for currentKey := range nodes {
		// if already in cache, increment reference count
		if element, exists := nc.cache[currentKey]; exists {
			entry := element.Value.(*cacheEntry)
			oldRefCount := entry.refCount
			newRefCount := oldRefCount + 1
			if newRefCount > nc.maxAllowedRefCount {
				newRefCount = nc.maxAllowedRefCount
				entry.refCount = newRefCount
				continue
			}
			entry.refCount = newRefCount

			nc.ensureRefListCapacity(newRefCount)
			nc.refLists[oldRefCount].Remove(element)
			newElement := nc.refLists[newRefCount].PushFront(entry)
			nc.cache[currentKey] = newElement
		} else {
			// read node from file
			node, err := db.getNode([]byte(currentKey))
			if err != nil {
				fmt.Printf("Error reading node %s from file: %v\n", currentKey, err)
				continue
			}
			if node == nil {
				// fmt.Printf("Account key %s not found in index\n", currentKey)
				continue
			}
			nodeValue, err := db.readFromFile(node.offset)
			if err != nil {
				// fmt.Printf("Error reading node %s from file: %v\n", currentKey, err)
				continue
			}

			if len(nc.cache) >= nc.capacity {
				nc.evictMinRefCount()
			}
			// add new entry to cache
			entry := &cacheEntry{
				key:           currentKey,
				value:         nodeValue,
				storageFileID: node.storageFileID,
				storageOffset: node.storageOffset,
				modifiedType:  None,
				refCount:      1,
			}
			nc.ensureRefListCapacity(1)
			element := nc.refLists[1].PushFront(entry)
			nc.cache[currentKey] = element
		}
	}
}

// asynchronous caching of path nodes - does not wait for completion
func (nc *NodeCache) AsyncCachePathToNode(key string, db *PrefixDB) {
	if !nc.isWorkerRunning {
		nc.startWorker()
	}

	select {
	case nc.pathQueue <- pathRequest{key: key, db: db}:
	default:
		nc.CachePathToNode(key, db)
	}
}

// synchronous caching of path nodes - waits for completion
func (nc *NodeCache) SyncCachePathToNode(key string, db *PrefixDB) {
	if !nc.isWorkerRunning {
		nc.startWorker()
	}

	resultChan := make(chan bool, 1)

	select {
	case nc.pathQueue <- pathRequest{key: key, db: db, resultChan: resultChan}:
		<-resultChan
	default:
		nc.CachePathToNode(key, db)
	}
}
