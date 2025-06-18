package prefixdb

import (
	"container/list"
	"sync"
	"time"
)

// NodeCache 管理节点的缓存，按引用计数排序
// 使用数组存储多个链表，每个链表对应一个引用计数值引用计数为1的节点在refLists[1]中，引用计数为2的节点在refLists[2]中，依此类推,在每个链表内部，节点按照插入顺序排列，最新插入的在链表头部
type NodeCache struct {
	capacity    int                      // 缓存容量
	cache       map[string]*list.Element // 键到链表节点的映射
	refLists    []*list.List             // 按引用计数分组的链表数组
	maxRefCount int                      // 当前最大引用计数
	lock        sync.RWMutex
}

// 链表节点中存储的数据结构
type cacheEntry struct {
	key      string
	value    []byte
	refCount int // 引用计数
}

// NewNodeCache 创建新的节点缓存
func NewNodeCache(capacity int) *NodeCache {
	// 初始创建一定数量的链表，后续可以根据需要扩展
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

// ensureRefListCapacity 确保有足够的引用计数链表
func (nc *NodeCache) ensureRefListCapacity(refCount int) {
	for refCount >= len(nc.refLists) {
		nc.refLists = append(nc.refLists, list.New())
	}
	if refCount > nc.maxRefCount {
		nc.maxRefCount = refCount
	}
}

// Get 获取缓存中的值，O(1)时间复杂度
func (nc *NodeCache) Get(key string) ([]byte, bool) {
	nc.lock.RLock()
	element, exists := nc.cache[key]
	if !exists {
		nc.lock.RUnlock()
		return nil, false
	}

	// 获取条目
	entry := element.Value.(*cacheEntry)
	value := entry.value
	oldRefCount := entry.refCount
	nc.lock.RUnlock()

	// 增加引用计数并移动到相应的链表
	nc.lock.Lock()
	defer nc.lock.Unlock()

	// 确保元素仍存在（可能在获取读锁和写锁之间被删除）
	if element, exists = nc.cache[key]; !exists {
		return value, true // 返回之前读取的值
	}

	entry = element.Value.(*cacheEntry)
	newRefCount := entry.refCount + 1
	entry.refCount = newRefCount

	// 确保有足够的链表
	nc.ensureRefListCapacity(newRefCount)

	// 从旧链表移除并添加到新链表
	nc.refLists[oldRefCount].Remove(element)
	newElement := nc.refLists[newRefCount].PushFront(entry)
	nc.cache[key] = newElement

	return value, true
}

// Put 添加或更新缓存中的值，O(1)时间复杂度
func (nc *NodeCache) Put(key string, value []byte) {
	nc.lock.Lock()
	defer nc.lock.Unlock()

	// 如果键已存在，更新值和引用计数
	if element, exists := nc.cache[key]; exists {
		entry := element.Value.(*cacheEntry)
		oldRefCount := entry.refCount
		newRefCount := oldRefCount + 1
		entry.value = value
		entry.refCount = newRefCount

		// 确保有足够的链表
		nc.ensureRefListCapacity(newRefCount)

		// 从旧链表移除并添加到新链表
		nc.refLists[oldRefCount].Remove(element)
		newElement := nc.refLists[newRefCount].PushFront(entry)
		nc.cache[key] = newElement
		return
	}

	// 如果缓存已满，淘汰引用计数最小的项
	if len(nc.cache) >= nc.capacity {
		nc.evictMinRefCount()
	}

	// 添加新项，初始引用计数为1
	entry := &cacheEntry{
		key:      key,
		value:    value,
		refCount: 1,
	}

	// 添加到引用计数为1的链表
	nc.ensureRefListCapacity(1)
	element := nc.refLists[1].PushFront(entry)
	nc.cache[key] = element
}

// evictMinRefCount 淘汰引用计数最小的项，O(1)时间复杂度
func (nc *NodeCache) evictMinRefCount() {
	// 从引用计数最小的链表开始查找
	for i := 1; i <= nc.maxRefCount; i++ {
		list := nc.refLists[i]
		if list.Len() > 0 {
			// 从链表尾部移除（最近最少使用的）
			element := list.Back()
			entry := element.Value.(*cacheEntry)
			delete(nc.cache, entry.key)
			list.Remove(element)
			return
		}
	}
}

// IncrementRefCount 增加节点引用计数
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

	// 确保有足够的链表
	nc.ensureRefListCapacity(newRefCount)

	// 从旧链表移除并添加到新链表
	nc.refLists[oldRefCount].Remove(element)
	newElement := nc.refLists[newRefCount].PushFront(entry)
	nc.cache[key] = newElement
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
			key:      currentKey,
			value:    nodeValue,
			refCount: 1,
		}
		nc.ensureRefListCapacity(1)
		element := nc.refLists[1].PushFront(entry)
		nc.cache[currentKey] = element
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
	if element, exists := nc.cache[key]; exists {
		return element.Value.(*cacheEntry).refCount
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
