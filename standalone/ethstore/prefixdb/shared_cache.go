package prefixdb

import (
	"container/list"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
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
	freq      uint32
	lastTouch uint64
}

type sharedCacheLockOpStats struct {
	count     uint64
	waitNanos uint64
	holdNanos uint64
}

type sharedCacheLockOpSnapshot struct {
	Count     uint64
	WaitNanos uint64
	HoldNanos uint64
}

type sharedCacheLockStats struct {
	getTouch   sharedCacheLockOpStats
	getNoTouch sharedCacheLockOpStats
	add        sharedCacheLockOpStats
	remove     sharedCacheLockOpStats
	namespace  sharedCacheLockOpStats
}

type sharedCacheLockStatsSnapshot struct {
	GetTouch   sharedCacheLockOpSnapshot
	GetNoTouch sharedCacheLockOpSnapshot
	Add        sharedCacheLockOpSnapshot
	Remove     sharedCacheLockOpSnapshot
	Namespace  sharedCacheLockOpSnapshot
}

const sharedCacheEvictionSample = 32 // lower means more close to pure LRU, higher means more LFU influence

type sharedByteCache struct {
	mu             sync.RWMutex
	capacityBytes  uint64
	usedBytes      uint64
	namespaceUsage map[sharedCacheNamespace]uint64
	ll             *list.List
	items          map[sharedCacheCompositeKey]*list.Element
	clock          uint64
	lockStats      sharedCacheLockStats
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
	acquiredAt := c.lockWrite(&c.lockStats.getTouch)
	defer c.unlockWrite(&c.lockStats.getTouch, acquiredAt)
	lookup := sharedCacheCompositeKey{namespace: namespace, key: key}
	elem, ok := c.items[lookup]
	if !ok {
		return nil, false
	}
	entry := elem.Value.(*sharedCacheEntry)
	c.touchEntryLocked(elem, entry)
	return entry.value, true
}

func (c *sharedByteCache) GetNoTouch(namespace sharedCacheNamespace, key string) (interface{}, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	acquiredAt := c.lockRead(&c.lockStats.getNoTouch)
	defer c.unlockRead(&c.lockStats.getNoTouch, acquiredAt)
	lookup := sharedCacheCompositeKey{namespace: namespace, key: key}
	elem, ok := c.items[lookup]
	if !ok {
		return nil, false
	}
	entry, _ := elem.Value.(*sharedCacheEntry)
	if entry == nil {
		return nil, false
	}
	bumpSharedCacheEntryFreq(entry)
	return entry.value, true
}

func (c *sharedByteCache) Add(namespace sharedCacheNamespace, key string, value interface{}, sizeBytes uint64) {
	if c == nil || key == "" {
		return
	}
	if sizeBytes == 0 {
		sizeBytes = 1
	}
	lookup := sharedCacheCompositeKey{namespace: namespace, key: key}

	acquiredAt := c.lockWrite(&c.lockStats.add)
	defer c.unlockWrite(&c.lockStats.add, acquiredAt)
	freq := uint32(1)
	if existing, ok := c.items[lookup]; ok {
		freq = atomic.LoadUint32(&existing.Value.(*sharedCacheEntry).freq)
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
		freq:      freq,
	}
	elem := c.ll.PushFront(entry)
	c.touchEntryLocked(elem, entry)
	c.items[lookup] = elem
	c.usedBytes += sizeBytes
	c.namespaceUsage[namespace] += sizeBytes
	for c.usedBytes > c.capacityBytes {
		victim := c.selectVictimLocked()
		if victim == nil {
			break
		}
		c.removeElementLocked(victim)
	}
}

func (c *sharedByteCache) Remove(namespace sharedCacheNamespace, key string) {
	if c == nil || key == "" {
		return
	}
	lookup := sharedCacheCompositeKey{namespace: namespace, key: key}
	acquiredAt := c.lockWrite(&c.lockStats.remove)
	defer c.unlockWrite(&c.lockStats.remove, acquiredAt)
	if elem, ok := c.items[lookup]; ok {
		c.removeElementLocked(elem)
	}
}

func (c *sharedByteCache) NamespaceStats(namespace sharedCacheNamespace) (usedBytes uint64, capacityBytes uint64) {
	if c == nil {
		return 0, 0
	}
	acquiredAt := c.lockRead(&c.lockStats.namespace)
	defer c.unlockRead(&c.lockStats.namespace, acquiredAt)
	return c.namespaceUsage[namespace], c.capacityBytes
}

func (c *sharedByteCache) LockStatsSnapshot() sharedCacheLockStatsSnapshot {
	if !analysisStatsEnabled || c == nil {
		return sharedCacheLockStatsSnapshot{}
	}
	return sharedCacheLockStatsSnapshot{
		GetTouch:   snapshotSharedCacheLockOpStats(&c.lockStats.getTouch),
		GetNoTouch: snapshotSharedCacheLockOpStats(&c.lockStats.getNoTouch),
		Add:        snapshotSharedCacheLockOpStats(&c.lockStats.add),
		Remove:     snapshotSharedCacheLockOpStats(&c.lockStats.remove),
		Namespace:  snapshotSharedCacheLockOpStats(&c.lockStats.namespace),
	}
}

func snapshotSharedCacheLockOpStats(stats *sharedCacheLockOpStats) sharedCacheLockOpSnapshot {
	if !analysisStatsEnabled || stats == nil {
		return sharedCacheLockOpSnapshot{}
	}
	return sharedCacheLockOpSnapshot{
		Count:     atomic.LoadUint64(&stats.count),
		WaitNanos: atomic.LoadUint64(&stats.waitNanos),
		HoldNanos: atomic.LoadUint64(&stats.holdNanos),
	}
}

func (c *sharedByteCache) lockWrite(stats *sharedCacheLockOpStats) time.Time {
	if !analysisStatsEnabled {
		c.mu.Lock()
		return time.Time{}
	}
	start := time.Now()
	c.mu.Lock()
	acquiredAt := time.Now()
	if stats != nil {
		atomic.AddUint64(&stats.count, 1)
		atomic.AddUint64(&stats.waitNanos, uint64(acquiredAt.Sub(start)))
	}
	return acquiredAt
}

func (c *sharedByteCache) unlockWrite(stats *sharedCacheLockOpStats, acquiredAt time.Time) {
	if analysisStatsEnabled && stats != nil {
		atomic.AddUint64(&stats.holdNanos, uint64(time.Since(acquiredAt)))
	}
	c.mu.Unlock()
}

func (c *sharedByteCache) lockRead(stats *sharedCacheLockOpStats) time.Time {
	if !analysisStatsEnabled {
		c.mu.RLock()
		return time.Time{}
	}
	start := time.Now()
	c.mu.RLock()
	acquiredAt := time.Now()
	if stats != nil {
		atomic.AddUint64(&stats.count, 1)
		atomic.AddUint64(&stats.waitNanos, uint64(acquiredAt.Sub(start)))
	}
	return acquiredAt
}

func (c *sharedByteCache) unlockRead(stats *sharedCacheLockOpStats, acquiredAt time.Time) {
	if analysisStatsEnabled && stats != nil {
		atomic.AddUint64(&stats.holdNanos, uint64(time.Since(acquiredAt)))
	}
	c.mu.RUnlock()
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

func (c *sharedByteCache) touchEntryLocked(elem *list.Element, entry *sharedCacheEntry) {
	if c == nil || elem == nil || entry == nil {
		return
	}
	bumpSharedCacheEntryFreq(entry)
	c.clock++
	entry.lastTouch = c.clock
	c.ll.MoveToFront(elem)
}

func bumpSharedCacheEntryFreq(entry *sharedCacheEntry) {
	if entry == nil {
		return
	}
	for {
		freq := atomic.LoadUint32(&entry.freq)
		if freq == ^uint32(0) {
			return
		}
		if atomic.CompareAndSwapUint32(&entry.freq, freq, freq+1) {
			return
		}
	}
}

func (c *sharedByteCache) selectVictimLocked() *list.Element {
	if c == nil {
		return nil
	}
	front := c.ll.Front()
	var victim *list.Element
	var victimEntry *sharedCacheEntry
	sampled := 0
	for elem := c.ll.Back(); elem != nil && sampled < sharedCacheEvictionSample; elem = elem.Prev() {
		if elem == front && elem != c.ll.Back() {
			continue
		}
		entry, _ := elem.Value.(*sharedCacheEntry)
		if entry == nil {
			continue
		}
		sampled++
		entryFreq := atomic.LoadUint32(&entry.freq)
		victimFreq := uint32(0)
		if victimEntry != nil {
			victimFreq = atomic.LoadUint32(&victimEntry.freq)
		}
		if victimEntry == nil || entryFreq < victimFreq || (entryFreq == victimFreq && entry.lastTouch < victimEntry.lastTouch) {
			victim = elem
			victimEntry = entry
		}
	}
	if victim != nil {
		return victim
	}
	return c.ll.Back()
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
		if valueBytes == nil {
			storedValue = nil
		} else {
			storedValue = append(make([]byte, 0, len(valueBytes)), valueBytes...)
		}
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
	folderKey string
	metas     []segmentChunkMeta
	sizeBytes uint64
}

type segmentIndexLayoutCacheEntry struct {
	layout    segmentIndexLayout
	sizeBytes uint64
}

type segmentIndexCache struct {
	shared        *sharedByteCache
	capacityBytes uint64
	usedBytes     uint64
	layoutMu      sync.RWMutex
	layouts       map[string]segmentIndexLayout
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
	cache := &segmentIndexCache{
		shared:  shared,
		layouts: make(map[string]segmentIndexLayout),
	}
	cache.refreshUsage()
	return cache
}

func (c *segmentIndexCache) GetByPath(folderPath string) ([]segmentChunkMeta, bool) {
	if c == nil {
		return nil, false
	}
	raw, ok := c.shared.GetNoTouch(sharedCacheNamespaceSegmentIndex, segmentIndexCacheKey(folderPath))
	if !ok {
		return nil, false
	}
	entry, _ := raw.(*segmentIndexCacheEntry)
	if entry == nil {
		return nil, false
	}
	return entry.metas, true
}

func (c *segmentIndexCache) Get(folderID uint32) ([]segmentChunkMeta, bool) {
	return c.GetByPath(segmentIndexFolderIDCacheKey(folderID))
}

func (c *segmentIndexCache) GetLevel2ByPath(folderPath string, metaID uint32, generation uint64) ([]segmentChunkMeta, bool) {
	if c == nil {
		return nil, false
	}
	raw, ok := c.shared.GetNoTouch(sharedCacheNamespaceSegmentIndex, segmentIndexLevel2CacheKey(folderPath, metaID, generation))
	if !ok {
		return nil, false
	}
	entry, _ := raw.(*segmentIndexCacheEntry)
	if entry == nil {
		return nil, false
	}
	return entry.metas, true
}

func (c *segmentIndexCache) GetLevel2(folderID uint32, metaID uint32, generation uint64) ([]segmentChunkMeta, bool) {
	return c.GetLevel2ByPath(segmentIndexFolderIDCacheKey(folderID), metaID, generation)
}

func (c *segmentIndexCache) GetLayoutByPath(folderPath string) (segmentIndexLayout, bool) {
	if c == nil {
		return segmentIndexLayout{}, false
	}
	c.layoutMu.RLock()
	layout, ok := c.layouts[folderPath]
	c.layoutMu.RUnlock()
	if !ok {
		raw, sharedOK := c.shared.GetNoTouch(sharedCacheNamespaceSegmentIndex, segmentIndexLayoutCacheKey(folderPath))
		if !sharedOK {
			return segmentIndexLayout{}, false
		}
		entry, _ := raw.(*segmentIndexLayoutCacheEntry)
		if entry == nil {
			return segmentIndexLayout{}, false
		}
		return entry.layout, true
	}
	return layout, true
}

func (c *segmentIndexCache) AddByPath(folderPath string, metas []segmentChunkMeta) {
	if c == nil {
		return
	}
	sizeBytes := estimateSegmentChunkMetasMemory(metas)
	if sizeBytes == 0 {
		c.shared.Remove(sharedCacheNamespaceSegmentIndex, segmentIndexCacheKey(folderPath))
		c.refreshUsage()
		return
	}
	entry := &segmentIndexCacheEntry{
		folderKey: folderPath,
		metas:     cloneSegmentChunkMetas(metas),
		sizeBytes: sizeBytes,
	}
	c.shared.Add(sharedCacheNamespaceSegmentIndex, segmentIndexCacheKey(folderPath), entry, sizeBytes)
	c.refreshUsage()
}

func (c *segmentIndexCache) Add(folderID uint32, metas []segmentChunkMeta) {
	c.AddByPath(segmentIndexFolderIDCacheKey(folderID), metas)
}

func (c *segmentIndexCache) AddLevel2ByPath(folderPath string, metaID uint32, generation uint64, metas []segmentChunkMeta) {
	if c == nil {
		return
	}
	sizeBytes := estimateSegmentChunkMetasMemory(metas)
	if sizeBytes == 0 {
		c.shared.Remove(sharedCacheNamespaceSegmentIndex, segmentIndexLevel2CacheKey(folderPath, metaID, generation))
		c.refreshUsage()
		return
	}
	entry := &segmentIndexCacheEntry{
		folderKey: folderPath,
		metas:     cloneSegmentChunkMetas(metas),
		sizeBytes: sizeBytes,
	}
	c.shared.Add(sharedCacheNamespaceSegmentIndex, segmentIndexLevel2CacheKey(folderPath, metaID, generation), entry, sizeBytes)
	c.refreshUsage()
}

func (c *segmentIndexCache) AddLevel2(folderID uint32, metaID uint32, generation uint64, metas []segmentChunkMeta) {
	c.AddLevel2ByPath(segmentIndexFolderIDCacheKey(folderID), metaID, generation, metas)
}

func (c *segmentIndexCache) AddLayoutByPath(folderPath string, layout segmentIndexLayout) {
	if c == nil {
		return
	}
	if folderPath == "" {
		return
	}
	if layout.mode != indexLayoutMultiLevel {
		c.layoutMu.Lock()
		delete(c.layouts, folderPath)
		c.layoutMu.Unlock()
		sizeBytes := estimateSegmentIndexLayoutMemory(layout)
		if sizeBytes == 0 {
			c.shared.Remove(sharedCacheNamespaceSegmentIndex, segmentIndexLayoutCacheKey(folderPath))
			c.refreshUsage()
			return
		}
		entry := &segmentIndexLayoutCacheEntry{
			layout:    cloneSegmentIndexLayout(layout),
			sizeBytes: sizeBytes,
		}
		c.shared.Add(sharedCacheNamespaceSegmentIndex, segmentIndexLayoutCacheKey(folderPath), entry, sizeBytes)
		c.refreshUsage()
		return
	}
	c.shared.Remove(sharedCacheNamespaceSegmentIndex, segmentIndexLayoutCacheKey(folderPath))
	c.refreshUsage()
	c.layoutMu.Lock()
	c.layouts[folderPath] = cloneSegmentIndexLayout(layout)
	c.layoutMu.Unlock()
}

func (c *segmentIndexCache) RemoveByPath(folderPath string) {
	if c == nil {
		return
	}
	c.shared.Remove(sharedCacheNamespaceSegmentIndex, segmentIndexCacheKey(folderPath))
	c.refreshUsage()
}

func (c *segmentIndexCache) Remove(folderID uint32) {
	c.RemoveByPath(segmentIndexFolderIDCacheKey(folderID))
}

func (c *segmentIndexCache) RemoveLayoutByPath(folderPath string) {
	if c == nil {
		return
	}
	if folderPath == "" {
		return
	}
	c.layoutMu.Lock()
	delete(c.layouts, folderPath)
	c.layoutMu.Unlock()
	c.shared.Remove(sharedCacheNamespaceSegmentIndex, segmentIndexLayoutCacheKey(folderPath))
	c.refreshUsage()
}

func (c *segmentIndexCache) refreshUsage() {
	if c == nil || c.shared == nil {
		return
	}
	c.usedBytes, c.capacityBytes = c.shared.NamespaceStats(sharedCacheNamespaceSegmentIndex)
}

func segmentIndexFolderIDCacheKey(folderID uint32) string {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], folderID)
	return "id:" + string(buf[:])
}

func segmentIndexCacheKey(folderKey string) string {
	return "l1:" + folderKey
}

func segmentIndexLayoutCacheKey(folderKey string) string {
	return "layout:" + folderKey
}

func segmentIndexLevel2CacheKey(folderKey string, metaID uint32, generation uint64) string {
	var buf [12]byte
	binary.BigEndian.PutUint32(buf[:4], metaID)
	binary.BigEndian.PutUint64(buf[4:12], generation)
	return "l2:" + folderKey + ":" + string(buf[:])
}
