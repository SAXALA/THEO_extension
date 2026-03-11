package prefixdb

import (
	"container/list"
	"encoding/binary"
	"sync"
	"unsafe"
)

type sharedCacheNamespace uint8

const (
	sharedCacheNamespaceNode sharedCacheNamespace = iota
	sharedCacheNamespaceStorage
	sharedCacheNamespaceSegmentIndex
	sharedCacheNamespaceFileNode
)

type sharedCacheEvictor interface {
	onSharedCacheEvict()
}

type sharedCacheCompositeKey struct {
	namespace sharedCacheNamespace
	key       string
}

type sharedCacheEntry struct {
	namespace sharedCacheNamespace
	key       string
	value     interface{}
	sizeBytes uint64
}

type sharedByteCache struct {
	mu             sync.Mutex
	capacityBytes  uint64
	usedBytes      uint64
	namespaceUsage map[sharedCacheNamespace]uint64
	ll             *list.List
	items          map[sharedCacheCompositeKey]*list.Element
}

func newSharedByteCache(capacityBytes uint64) *sharedByteCache {
	if capacityBytes == 0 {
		capacityBytes = 1
	}
	return &sharedByteCache{
		capacityBytes:  capacityBytes,
		namespaceUsage: make(map[sharedCacheNamespace]uint64),
		ll:             list.New(),
		items:          make(map[sharedCacheCompositeKey]*list.Element),
	}
}

func (c *sharedByteCache) Get(namespace sharedCacheNamespace, key string) (interface{}, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lookup := sharedCacheCompositeKey{namespace: namespace, key: key}
	elem, ok := c.items[lookup]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(elem)
	return elem.Value.(*sharedCacheEntry).value, true
}

func (c *sharedByteCache) Add(namespace sharedCacheNamespace, key string, value interface{}, sizeBytes uint64) {
	if c == nil || key == "" {
		return
	}
	if sizeBytes == 0 {
		sizeBytes = 1
	}
	lookup := sharedCacheCompositeKey{namespace: namespace, key: key}

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.items[lookup]; ok {
		c.removeElementLocked(existing)
	}
	if sizeBytes > c.capacityBytes {
		return
	}
	entry := &sharedCacheEntry{
		namespace: namespace,
		key:       key,
		value:     value,
		sizeBytes: sizeBytes,
	}
	elem := c.ll.PushFront(entry)
	c.items[lookup] = elem
	c.usedBytes += sizeBytes
	c.namespaceUsage[namespace] += sizeBytes
	for c.usedBytes > c.capacityBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.removeElementLocked(back)
	}
}

func (c *sharedByteCache) Remove(namespace sharedCacheNamespace, key string) {
	if c == nil || key == "" {
		return
	}
	lookup := sharedCacheCompositeKey{namespace: namespace, key: key}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[lookup]; ok {
		c.removeElementLocked(elem)
	}
}

func (c *sharedByteCache) NamespaceStats(namespace sharedCacheNamespace) (usedBytes uint64, capacityBytes uint64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.namespaceUsage[namespace], c.capacityBytes
}

func (c *sharedByteCache) removeElementLocked(elem *list.Element) {
	if c == nil || elem == nil {
		return
	}
	entry := elem.Value.(*sharedCacheEntry)
	if evictor, ok := entry.value.(sharedCacheEvictor); ok {
		evictor.onSharedCacheEvict()
	}
	lookup := sharedCacheCompositeKey{namespace: entry.namespace, key: entry.key}
	delete(c.items, lookup)
	c.ll.Remove(elem)
	if c.usedBytes >= entry.sizeBytes {
		c.usedBytes -= entry.sizeBytes
	} else {
		c.usedBytes = 0
	}
	if used := c.namespaceUsage[entry.namespace]; used >= entry.sizeBytes {
		c.namespaceUsage[entry.namespace] = used - entry.sizeBytes
	} else {
		c.namespaceUsage[entry.namespace] = 0
	}
}

type storageValueCache struct {
	shared *sharedByteCache
}

func newSharedStorageValueCache(shared *sharedByteCache) *storageValueCache {
	if shared == nil {
		return nil
	}
	return &storageValueCache{shared: shared}
}

func (c *storageValueCache) Get(key string) (interface{}, bool) {
	if c == nil {
		return nil, false
	}
	return c.shared.Get(sharedCacheNamespaceStorage, key)
}

func (c *storageValueCache) Add(key string, value interface{}) {
	if c == nil || key == "" {
		return
	}
	storedValue := value
	if valueBytes, ok := value.([]byte); ok {
		storedValue = cloneBytes(valueBytes)
	}
	c.shared.Add(sharedCacheNamespaceStorage, key, storedValue, estimateStorageCacheValueSize(key, storedValue))
}

func (c *storageValueCache) Remove(key string) {
	if c == nil {
		return
	}
	c.shared.Remove(sharedCacheNamespaceStorage, key)
}

func estimateStorageCacheValueSize(key string, value interface{}) uint64 {
	total := uint64(len(key)) + uint64(unsafe.Sizeof(value))
	if valueBytes, ok := value.([]byte); ok {
		total += uint64(len(valueBytes))
	}
	if total == 0 {
		return 1
	}
	return total
}

type segmentIndexCacheEntry struct {
	folderID  uint32
	metas     []segmentChunkMeta
	sizeBytes uint64
}

type segmentIndexCache struct {
	shared        *sharedByteCache
	capacityBytes uint64
	usedBytes     uint64
}

func newSegmentIndexCache(capacityMiB int) *segmentIndexCache {
	if capacityMiB <= 0 {
		return nil
	}
	return newSharedSegmentIndexCache(newSharedByteCache(uint64(capacityMiB) * 1024 * 1024))
}

func newSharedSegmentIndexCache(shared *sharedByteCache) *segmentIndexCache {
	if shared == nil {
		return nil
	}
	cache := &segmentIndexCache{shared: shared}
	cache.refreshUsage()
	return cache
}

func (c *segmentIndexCache) Get(folderID uint32) ([]segmentChunkMeta, bool) {
	if c == nil {
		return nil, false
	}
	raw, ok := c.shared.Get(sharedCacheNamespaceSegmentIndex, segmentIndexCacheKey(folderID))
	if !ok {
		c.refreshUsage()
		return nil, false
	}
	entry, _ := raw.(*segmentIndexCacheEntry)
	c.refreshUsage()
	if entry == nil {
		return nil, false
	}
	return entry.metas, true
}

func (c *segmentIndexCache) Add(folderID uint32, metas []segmentChunkMeta) {
	if c == nil {
		return
	}
	sizeBytes := estimateSegmentChunkMetasMemory(metas)
	if sizeBytes == 0 {
		c.shared.Remove(sharedCacheNamespaceSegmentIndex, segmentIndexCacheKey(folderID))
		c.refreshUsage()
		return
	}
	entry := &segmentIndexCacheEntry{
		folderID:  folderID,
		metas:     cloneSegmentChunkMetas(metas),
		sizeBytes: sizeBytes,
	}
	c.shared.Add(sharedCacheNamespaceSegmentIndex, segmentIndexCacheKey(folderID), entry, sizeBytes)
	c.refreshUsage()
}

func (c *segmentIndexCache) Remove(folderID uint32) {
	if c == nil {
		return
	}
	c.shared.Remove(sharedCacheNamespaceSegmentIndex, segmentIndexCacheKey(folderID))
	c.refreshUsage()
}

func (c *segmentIndexCache) refreshUsage() {
	if c == nil || c.shared == nil {
		return
	}
	c.usedBytes, c.capacityBytes = c.shared.NamespaceStats(sharedCacheNamespaceSegmentIndex)
}

func segmentIndexCacheKey(folderID uint32) string {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], folderID)
	return string(buf[:])
}
