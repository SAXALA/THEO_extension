package prefixdb

import (
	"container/list"
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

// Data structure stored in list nodes
type cacheEntry struct {
	key         string
	value       []byte
	slotIndices []int64
	refCount    int // Reference count
}

// NewNodeCache creates a new node cache
func NewNodeCache(capacity int) *NodeCache {
	// Initially create a fixed number of lists, can be expanded later
	const initialListCount = 10
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
	}

	nc.startWorker()

	return nc
}

// // SetMaxAllowedRefCount 设置最大允许的引用计数
// func (nc *NodeCache) SetMaxAllowedRefCount(maxCount int) {
// 	nc.lock.Lock()
// 	defer nc.lock.Unlock()

// 	if maxCount > 0 && maxCount < len(nc.refLists) {
// 		// 如果设置的值小于当前列表长度，可能需要合并一些列表
// 		nc.maxAllowedRefCount = maxCount
// 		nc.consolidateExcessiveLists()
// 	} else if maxCount > 0 {
// 		nc.maxAllowedRefCount = maxCount
// 	}
// }

// // 合并超过最大引用计数的列表
// func (nc *NodeCache) consolidateExcessiveLists() {
// 	if len(nc.refLists) <= nc.maxAllowedRefCount {
// 		return
// 	}

// 	// 获取最大允许引用计数的列表
// 	maxList := nc.refLists[nc.maxAllowedRefCount]

// 	// 将所有超过最大引用计数的列表中的元素合并到maxList
// 	for i := nc.maxAllowedRefCount + 1; i < len(nc.refLists); i++ {
// 		list := nc.refLists[i]
// 		for list.Len() > 0 {
// 			// 移动元素到maxList
// 			element := list.Front()
// 			entry := element.Value.(*cacheEntry)
// 			list.Remove(element)

// 			// 更新引用计数并放入maxList
// 			entry.refCount = nc.maxAllowedRefCount
// 			newElement := maxList.PushFront(entry)
// 			nc.cache[entry.key] = newElement
// 		}
// 	}

// 	// 截断refLists，丢弃超过maxAllowedRefCount的列表
// 	nc.refLists = nc.refLists[:nc.maxAllowedRefCount+1]

// 	// 更新maxRefCount
// 	if nc.maxRefCount > nc.maxAllowedRefCount {
// 		nc.maxRefCount = nc.maxAllowedRefCount
// 	}
// }

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
func (nc *NodeCache) Get(key string) ([]byte, []int64, bool) {
	nc.lock.RLock()
	element, exists := nc.cache[key]
	if !exists {
		nc.lock.RUnlock()
		return nil, nil, false
	}

	// Get entry
	entry := element.Value.(*cacheEntry)
	oldRefCount := entry.refCount
	nc.lock.RUnlock()

	// Increase reference count and move to appropriate list
	nc.lock.Lock()
	defer nc.lock.Unlock()

	// Ensure element still exists (may have been deleted between read and write locks)
	if element, exists = nc.cache[key]; !exists {
		return entry.value, entry.slotIndices, true // Return previously read value
	}

	entry = element.Value.(*cacheEntry)
	newRefCount := entry.refCount + 1
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

	return entry.value, entry.slotIndices, true
}

// Put adds or updates a value in cache
func (nc *NodeCache) Put(key string, value []byte, slotIndices []int64) {
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
		entry.slotIndices = slotIndices
		entry.refCount = newRefCount

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
		key:         key,
		value:       value,
		slotIndices: slotIndices,
		refCount:    1,
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

			offset, exists := db.accountIndex.get(currentKey)
			if !exists {
				// fmt.Printf("Account key %s not found in index\n", currentKey)
				continue // Skip if key not found
			}
			nodeValue, slotIndices, err := db.readFromFile(offset, TrieAccount)
			if err != nil {
				continue // Skip on error
			}

			// If cache is full, evict item with lowest reference count
			if len(nc.cache) >= nc.capacity {
				nc.evictMinRefCount()
			}

			// Add new entry to cache
			entry := &cacheEntry{
				key:         currentKey,
				value:       nodeValue,
				slotIndices: slotIndices,
				refCount:    1,
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

// Delete removes a key from the cache
func (nc *NodeCache) Delete(key string) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	element, exists := nc.cache[key]
	if !exists {
		return
	}

	entry := element.Value.(*cacheEntry)
	refCount := entry.refCount

	// Remove from the appropriate reference count list
	nc.refLists[refCount].Remove(element)

	// Remove from cache map
	delete(nc.cache, key)
}

// SlotCache manages slot LRU caching
type SlotCache struct {
	capacity int                     // Cache capacity
	cache    map[int64]*list.Element // Map from slot indices to list nodes
	lruList  *list.List              // List ordered by access time
	lock     sync.RWMutex
	db       *PrefixDB // Reference to PrefixDB for batch operations
}

// Data structure stored in slot cache entries
type slotCacheEntry struct {
	slotIndex int64
	data      map[string][]byte
	// timestamp int64
	modified bool // Track if slot has been modified
}

// NewSlotCache creates a new slot cache
func NewSlotCache(capacity int, db *PrefixDB) *SlotCache {
	return &SlotCache{
		capacity: capacity,
		cache:    make(map[int64]*list.Element),
		lruList:  list.New(),
		db:       db,
	}
}

// Get retrieves slot data from cache, O(1) time complexity
func (sc *SlotCache) Get(slotIndex int64) (map[string][]byte, bool) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	if element, exists := sc.cache[slotIndex]; exists {
		// Move to list head to indicate recent access
		sc.lruList.MoveToFront(element)
		entry := element.Value.(*slotCacheEntry)
		// entry.timestamp = time.Now().UnixNano()
		return entry.data, true
	}
	return nil, false
}

// Put adds or updates slot data in cache, O(1) time complexity
func (sc *SlotCache) Put(slotIndex int64, data map[string][]byte) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if element, exists := sc.cache[slotIndex]; exists {
		// If exists, update value
		sc.lruList.MoveToFront(element)
		entry := element.Value.(*slotCacheEntry)
		entry.data = data
		// entry.timestamp = time.Now().UnixNano()
		// Don't change modified status for loaded data
		return
	}

	// If cache is full, remove least recently used item
	if len(sc.cache) >= sc.capacity {
		sc.evictLRU()
	}

	// Add new item
	entry := &slotCacheEntry{
		slotIndex: slotIndex,
		data:      data,
		// timestamp: time.Now().UnixNano(),
		modified: false,
	}
	element := sc.lruList.PushFront(entry)
	sc.cache[slotIndex] = element
}

// evictLRU removes least recently used slot, O(1) time complexity
func (sc *SlotCache) evictLRU() {
	if sc.lruList.Len() == 0 {
		return
	}

	// Get element from list tail
	element := sc.lruList.Back()
	if element != nil {
		entry := element.Value.(*slotCacheEntry)

		// 如果slot被修改过且数据库和batch存在，将整个slot加入batch
		if entry.modified && sc.db != nil && sc.db.batch != nil {
			// 将整个slot的数据加入batch
			sc.db.batch.addSlot(entry.slotIndex, entry.data)
		}

		delete(sc.cache, entry.slotIndex)
		sc.lruList.Remove(element)
	}
}

// ContainsKey checks if a specific slot contains a key, O(1) time complexity
func (sc *SlotCache) ContainsKey(slotIndex int64, key string) bool {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	if element, exists := sc.cache[slotIndex]; exists {
		entry := element.Value.(*slotCacheEntry)
		_, hasKey := entry.data[key]
		return hasKey
	}
	return false
}

// UpdateKey updates a key's value in a specific slot, O(1) time complexity
func (sc *SlotCache) UpdateKey(slotIndex int64, key string, value []byte) bool {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if element, exists := sc.cache[slotIndex]; exists {
		entry := element.Value.(*slotCacheEntry)
		entry.data[key] = value
		// entry.timestamp = time.Now().UnixNano()
		entry.modified = true
		sc.lruList.MoveToFront(element)
		return true
	}
	return false
}

// Delete removes a slot from the cache
func (sc *SlotCache) Delete(slotIndex int64) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if element, exists := sc.cache[slotIndex]; exists {
		entry := element.Value.(*slotCacheEntry)

		if entry.modified && sc.db != nil && sc.db.batch != nil {
			sc.db.batch.addSlot(entry.slotIndex, entry.data)
		}

		sc.lruList.Remove(element)
		delete(sc.cache, slotIndex)
	}
}

func (sc *SlotCache) MarkSlotModified(slotIndex int64) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if element, exists := sc.cache[slotIndex]; exists {
		entry := element.Value.(*slotCacheEntry)
		entry.modified = true
	}
}

// FlushModifiedSlots flushes all modified slots to the database
func (sc *SlotCache) FlushModifiedSlots() map[int64]map[string][]byte {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	result := make(map[int64]map[string][]byte)

	for slotIndex, element := range sc.cache {
		entry := element.Value.(*slotCacheEntry)
		if entry.modified {
			slotData := make(map[string][]byte)
			for k, v := range entry.data {
				slotData[k] = v
			}
			result[slotIndex] = slotData

			if sc.db != nil && sc.db.batch != nil {
				sc.db.batch.addSlot(slotIndex, slotData)
			}

			entry.modified = false
		}
	}

	return result
}

// Get current timestamp
func getCurrentTimestamp() int64 {
	return time.Now().UnixNano()
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
			offset, exists := db.accountIndex.get(currentKey)
			if !exists {
				// fmt.Printf("Account key %s not found in index\n", currentKey)
				continue
			}
			nodeValue, slotIndices, err := db.readFromFile(offset, TrieAccount)
			if err != nil {
				// fmt.Printf("Error reading node %s from file: %v\n", currentKey, err)
				continue
			}

			if len(nc.cache) >= nc.capacity {
				nc.evictMinRefCount()
			}

			// add new entry to cache
			entry := &cacheEntry{
				key:         currentKey,
				value:       nodeValue,
				slotIndices: slotIndices,
				refCount:    1,
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
