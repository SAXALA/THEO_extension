package prefixdb

import (
	"container/list"
	"sync"
	"time"
)

// NodeCache 管理节点的LRU缓存
type NodeCache struct {
	capacity int                      // 缓存容量
	cache    map[string]*list.Element // 键到链表节点的映射
	lruList  *list.List               // 按访问顺序排列的双向链表
	refCount map[string]int           // 引用计数
	lock     sync.RWMutex
}

// 链表节点中存储的数据结构
type cacheEntry struct {
	key   string
	value []byte
}

// NewNodeCache 创建新的节点缓存
func NewNodeCache(capacity int) *NodeCache {
	return &NodeCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		lruList:  list.New(),
		refCount: make(map[string]int),
	}
}

// Get 获取缓存中的值，O(1)时间复杂度
func (nc *NodeCache) Get(key string) ([]byte, bool) {
	nc.lock.RLock()
	defer nc.lock.RUnlock()

	if element, exists := nc.cache[key]; exists {
		// 移动到链表头部表示最近访问
		nc.lruList.MoveToFront(element)
		// 增加引用计数
		nc.refCount[key]++
		return element.Value.(*cacheEntry).value, true
	}
	return nil, false
}

// Put 添加或更新缓存中的值，O(1)时间复杂度
func (nc *NodeCache) Put(key string, value []byte) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	if element, exists := nc.cache[key]; exists {
		// 已存在则更新值
		nc.lruList.MoveToFront(element)
		element.Value.(*cacheEntry).value = value
		nc.IncrementRefCount(key)
		return
	}

	// 缓存已满，移除引用计数最小的项
	if len(nc.cache) >= nc.capacity {
		nc.evictByRefCount()
	}

	// 添加新项
	entry := &cacheEntry{
		key:   key,
		value: value,
	}
	element := nc.lruList.PushFront(entry)
	nc.cache[key] = element
	nc.refCount[key] = 1
}

// evictByRefCount 移除引用计数最小的项，O(n)时间复杂度
// 当有多个引用计数相同的最小值时，优先移除LRU最不常用的
func (nc *NodeCache) evictByRefCount() {
	if nc.lruList.Len() == 0 {
		return
	}

	minRefCount := int(^uint(0) >> 1) // 最大int值
	var minElement *list.Element

	// 第一次遍历，找出最小引用计数
	for e := nc.lruList.Back(); e != nil; e = e.Prev() {
		entry := e.Value.(*cacheEntry)
		if count, exists := nc.refCount[entry.key]; exists && count < minRefCount {
			minRefCount = count
			minElement = e
		}
	}

	// 如果所有节点引用计数相同，使用LRU策略
	if minElement == nil {
		minElement = nc.lruList.Back()
	}

	if minElement != nil {
		entry := minElement.Value.(*cacheEntry)
		delete(nc.cache, entry.key)
		delete(nc.refCount, entry.key)
		nc.lruList.Remove(minElement)
	}
}

// IncrementRefCount 增加节点引用计数
func (nc *NodeCache) IncrementRefCount(key string) {
	if count, exists := nc.refCount[key]; exists {
		nc.refCount[key] = count + 1
	} else {
		nc.refCount[key] = 1
	}
}

// CachePathToNode 缓存从根到节点路径上的所有节点
func (nc *NodeCache) CachePathToNode(key string, db *PrefixDB) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	currentKey := key
	for len(currentKey) > 0 {
		// 移动到父节点
		currentKey = currentKey[:len(currentKey)-1]

		if currentKey == "" {
			break // 不处理空字符串(根节点)
		}

		// 如果已在缓存中，增加引用计数
		if _, exists := nc.cache[currentKey]; exists {
			nc.IncrementRefCount(currentKey)
			continue
		}

		// 从存储加载节点
		node, err := db.findNode([]byte(currentKey))
		if err != nil || node == nil {
			continue // 节点未找到则跳过
		}

		var nodeValue []byte
		if node.isLeaf {
			nodeValue, err = db.readFromFile(node.offset, TrieAccount)
			if err != nil {
				continue // 有错误则跳过
			}
		} else {
			// 对于内部节点，使用空值
			nodeValue = []byte{}
		}

		// 创建新条目并添加到缓存
		entry := &cacheEntry{
			key:   currentKey,
			value: nodeValue,
		}
		element := nc.lruList.PushFront(entry)
		nc.cache[currentKey] = element
		nc.refCount[currentKey] = 1
	}
}

// ContainsKey 检查键是否存在于缓存中
func (nc *NodeCache) ContainsKey(key string) bool {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if _, exists := nc.cache[key]; exists {
		return true
	}
	return false
}

// GetRefCount 获取键的引用计数
func (nc *NodeCache) GetRefCount(key string) int {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if count, exists := nc.refCount[key]; exists {
		return count
	}
	return 0
}

// SlotCache 管理slot的LRU缓存
type SlotCache struct {
	capacity int                   // 缓存容量
	cache    map[int]*list.Element // slotIndex到链表节点的映射
	lruList  *list.List            // 按访问顺序排列的双向链表
	lock     sync.RWMutex
}

// slot缓存中存储的数据结构
type slotCacheEntry struct {
	slotIndex int
	data      map[string][]byte
	timestamp int64
}

// NewSlotCache 创建新的slot缓存
func NewSlotCache(capacity int) *SlotCache {
	return &SlotCache{
		capacity: capacity,
		cache:    make(map[int]*list.Element),
		lruList:  list.New(),
	}
}

// Get 获取缓存中的slot数据，O(1)时间复杂度
func (sc *SlotCache) Get(slotIndex int) (map[string][]byte, bool) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	if element, exists := sc.cache[slotIndex]; exists {
		// 移动到链表头部表示最近访问
		sc.lruList.MoveToFront(element)
		entry := element.Value.(*slotCacheEntry)
		entry.timestamp = time.Now().UnixNano()
		return entry.data, true
	}
	return nil, false
}

// Put 添加或更新缓存中的slot数据，O(1)时间复杂度
func (sc *SlotCache) Put(slotIndex int, data map[string][]byte) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if element, exists := sc.cache[slotIndex]; exists {
		// 已存在则更新值
		sc.lruList.MoveToFront(element)
		entry := element.Value.(*slotCacheEntry)
		entry.data = data
		entry.timestamp = time.Now().UnixNano()
		return
	}

	// 缓存已满，移除最久未使用的项
	if len(sc.cache) >= sc.capacity {
		sc.evictLRU()
	}

	// 添加新项
	entry := &slotCacheEntry{
		slotIndex: slotIndex,
		data:      data,
		timestamp: time.Now().UnixNano(),
	}
	element := sc.lruList.PushFront(entry)
	sc.cache[slotIndex] = element
}

// evictLRU 移除最久未使用的slot，O(1)时间复杂度
func (sc *SlotCache) evictLRU() {
	if sc.lruList.Len() == 0 {
		return
	}

	// 获取链表尾部元素
	element := sc.lruList.Back()
	if element != nil {
		entry := element.Value.(*slotCacheEntry)
		delete(sc.cache, entry.slotIndex)
		sc.lruList.Remove(element)
	}
}

// ContainsKey 检查特定slot是否包含某key，O(1)时间复杂度
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

// UpdateKey 更新特定slot中key的值，O(1)时间复杂度
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
