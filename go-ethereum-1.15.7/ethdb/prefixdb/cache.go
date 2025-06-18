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
	capacity    int                      // Cache capacity
	cache       map[string]*list.Element // Map from keys to list nodes
	refLists    []*list.List             // Lists grouped by reference count
	maxRefCount int                      // Current maximum reference count
	lock        sync.RWMutex
}

// Data structure stored in list nodes
type cacheEntry struct {
	key      string
	value    []byte
	refCount int // Reference count
}

// NewNodeCache creates a new node cache
func NewNodeCache(capacity int) *NodeCache {
	// Initially create a fixed number of lists, can be expanded later
	const initialListCount = 10
	refLists := make([]*list.List, initialListCount)
	for i := range refLists {
		refLists[i] = list.New()
	}

	return &NodeCache{
		capacity:    capacity,
		cache:       make(map[string]*list.Element),
		refLists:    refLists,
		maxRefCount: 0,
	}
}

// ensureRefListCapacity ensures there are enough reference count lists
func (nc *NodeCache) ensureRefListCapacity(refCount int) {
	for refCount >= len(nc.refLists) {
		nc.refLists = append(nc.refLists, list.New())
	}
	if refCount > nc.maxRefCount {
		nc.maxRefCount = refCount
	}
}

// Get retrieves a value from cache
func (nc *NodeCache) Get(key string) ([]byte, bool) {
	nc.lock.RLock()
	element, exists := nc.cache[key]
	if !exists {
		nc.lock.RUnlock()
		return nil, false
	}

	// Get entry
	entry := element.Value.(*cacheEntry)
	value := entry.value
	oldRefCount := entry.refCount
	nc.lock.RUnlock()

	// Increase reference count and move to appropriate list
	nc.lock.Lock()
	defer nc.lock.Unlock()

	// Ensure element still exists (may have been deleted between read and write locks)
	if element, exists = nc.cache[key]; !exists {
		return value, true // Return previously read value
	}

	entry = element.Value.(*cacheEntry)
	newRefCount := entry.refCount + 1
	entry.refCount = newRefCount

	// Ensure enough lists are available
	nc.ensureRefListCapacity(newRefCount)

	// Remove from old list and add to new list
	nc.refLists[oldRefCount].Remove(element)
	newElement := nc.refLists[newRefCount].PushFront(entry)
	nc.cache[key] = newElement

	return value, true
}

// Put adds or updates a value in cache
func (nc *NodeCache) Put(key string, value []byte) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	// If key already exists, update value and reference count
	if element, exists := nc.cache[key]; exists {
		entry := element.Value.(*cacheEntry)
		oldRefCount := entry.refCount
		newRefCount := oldRefCount + 1
		entry.value = value
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
		key:      key,
		value:    value,
		refCount: 1,
	}

	// Add to reference count 1 list
	nc.ensureRefListCapacity(1)
	element := nc.refLists[1].PushFront(entry)
	nc.cache[key] = element
}

// evictMinRefCount evicts item with lowest reference count, O(1) time complexity
func (nc *NodeCache) evictMinRefCount() {
	// Start search from lowest reference count list
	for i := 1; i <= nc.maxRefCount; i++ {
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
		if _, exists := nc.cache[currentKey]; exists {
			nc.IncrementRefCount(currentKey)
			continue
		}

		// Load node from storage
		node, err := db.findNode([]byte(currentKey))
		if err != nil || node == nil {
			continue // Skip if node not found
		}

		var nodeValue []byte
		if node.isLeaf {
			nodeValue, err = db.readFromFile(node.offset, TrieAccount)
			if err != nil {
				continue // Skip on error
			}
		} else {
			// For internal nodes, use empty value
			nodeValue = []byte{}
		}

		// Create new entry and add to cache
		entry := &cacheEntry{
			key:      currentKey,
			value:    nodeValue,
			refCount: 1,
		}
		nc.ensureRefListCapacity(1)
		element := nc.refLists[1].PushFront(entry)
		nc.cache[currentKey] = element
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

// SlotCache manages slot LRU caching
type SlotCache struct {
	capacity int                   // Cache capacity
	cache    map[int]*list.Element // Map from slot indices to list nodes
	lruList  *list.List            // List ordered by access time
	lock     sync.RWMutex
}

// Data structure stored in slot cache entries
type slotCacheEntry struct {
	slotIndex int
	data      map[string][]byte
	timestamp int64
}

// NewSlotCache creates a new slot cache
func NewSlotCache(capacity int) *SlotCache {
	return &SlotCache{
		capacity: capacity,
		cache:    make(map[int]*list.Element),
		lruList:  list.New(),
	}
}

// Get retrieves slot data from cache, O(1) time complexity
func (sc *SlotCache) Get(slotIndex int) (map[string][]byte, bool) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	if element, exists := sc.cache[slotIndex]; exists {
		// Move to list head to indicate recent access
		sc.lruList.MoveToFront(element)
		entry := element.Value.(*slotCacheEntry)
		entry.timestamp = time.Now().UnixNano()
		return entry.data, true
	}
	return nil, false
}

// Put adds or updates slot data in cache, O(1) time complexity
func (sc *SlotCache) Put(slotIndex int, data map[string][]byte) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if element, exists := sc.cache[slotIndex]; exists {
		// If exists, update value
		sc.lruList.MoveToFront(element)
		entry := element.Value.(*slotCacheEntry)
		entry.data = data
		entry.timestamp = time.Now().UnixNano()
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
		timestamp: time.Now().UnixNano(),
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
		delete(sc.cache, entry.slotIndex)
		sc.lruList.Remove(element)
	}
}

// ContainsKey checks if a specific slot contains a key, O(1) time complexity
func (sc *SlotCache) ContainsKey(slotIndex int, key string) bool {
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
func (sc *SlotCache) UpdateKey(slotIndex int, key string, value []byte) bool {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if element, exists := sc.cache[slotIndex]; exists {
		entry := element.Value.(*slotCacheEntry)
		entry.data[key] = value
		entry.timestamp = time.Now().UnixNano()
		sc.lruList.MoveToFront(element)
		return true
	}
	return false
}

// Get current timestamp
func getCurrentTimestamp() int64 {
	return time.Now().UnixNano()
}
