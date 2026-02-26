package prefixdb

import (
	"bytes"
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

// NodeCacheEntry mirrors the data kept in the prefix tree plus the cached value.
type NodeCacheEntry struct {
	Key           string
	Value         []byte
	AccountOffset int64
	StorageInfo   StorageInfo
}

// StorageInfo keeps track of persisted storage metadata for a trie account node.
type StorageInfo struct {
	storageFileID uint32
	storageOffset int64
	storageSize   uint64
}

type nodeCacheRecord struct {
	value         []byte
	accountOffset int64
	storageInfo   StorageInfo
}

// NodeCache is a thin wrapper around hashicorp's LRU cache implementation.
type NodeCache struct {
	lock  sync.RWMutex
	cache *lru.Cache
}

// NewNodeCache instantiates an LRU cache with the provided capacity.
func NewNodeCache(capacity int) (*NodeCache, error) {
	if capacity <= 0 {
		capacity = 1
	}
	lruCache, err := lru.New(capacity)
	if err != nil {
		return nil, err
	}
	return &NodeCache{cache: lruCache}, nil
}

func (nc *NodeCache) Close() {}

// Get returns the cached entry when present.
func (nc *NodeCache) Get(key string) (NodeCacheEntry, bool) {
	if nc == nil || key == "" {
		return NodeCacheEntry{}, false
	}
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if raw, ok := nc.cache.Get(key); ok {
		rec := raw.(*nodeCacheRecord)
		return NodeCacheEntry{
			Key:           key,
			Value:         cloneBytes(rec.value),
			AccountOffset: rec.accountOffset,
			StorageInfo:   rec.storageInfo,
		}, true
	}
	return NodeCacheEntry{}, false
}

// Put inserts or replaces an entry. Callers should only add entries after the
// prefix tree has been read so metadata stays consistent.
func (nc *NodeCache) Put(entry NodeCacheEntry) {
	if nc == nil || entry.Key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.cache.Add(entry.Key, &nodeCacheRecord{
		value:         cloneBytes(entry.Value),
		accountOffset: entry.AccountOffset,
		storageInfo:   entry.StorageInfo,
	})
}

// StoreMetadata records node metadata while preserving any cached value.
func (nc *NodeCache) StoreMetadata(key string, accountOffset int64, storageInfo StorageInfo) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if raw, ok := nc.cache.Get(key); ok {
		rec := raw.(*nodeCacheRecord)
		rec.accountOffset = accountOffset
		rec.storageInfo = storageInfo
		nc.cache.Add(key, rec)
		return
	}
	nc.cache.Add(key, &nodeCacheRecord{
		accountOffset: accountOffset,
		storageInfo:   storageInfo,
	})
}

// UpdateValue refreshes the cached payload when the entry already exists.
func (nc *NodeCache) UpdateValue(key string, value []byte) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if raw, ok := nc.cache.Get(key); ok {
		rec := raw.(*nodeCacheRecord)
		rec.value = cloneBytes(value)
		nc.cache.Add(key, rec)
	}
}

func (nc *NodeCache) UpdateAccountOffset(key string, accountOffset int64) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if raw, ok := nc.cache.Get(key); ok {
		rec := raw.(*nodeCacheRecord)
		rec.accountOffset = accountOffset
		nc.cache.Add(key, rec)
	}
}

// UpdateStoragePointer updates storage metadata when a node's storage segment changes.
func (nc *NodeCache) UpdateStoragePointer(key string, storageInfo StorageInfo) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if raw, ok := nc.cache.Get(key); ok {
		rec := raw.(*nodeCacheRecord)
		rec.storageInfo = storageInfo
		nc.cache.Add(key, rec)
	}
}

// Delete removes an entry from the cache.
func (nc *NodeCache) Delete(key string) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.cache.Remove(key)
}

const defaultStorageBufferChunks = 16
const defaultStorageEntryPool = 8192 * 2

type storageChunkEntry struct {
	keyStart   []byte
	keyEnd     []byte
	entries    []kvPair
	backing    *bufferLease
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
		chunk.backing.Release()
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

func (b *storageChunkBuffer) adopt(accountKey string, entries []kvPair, backing *bufferLease) {
	b.ensureLimits()
	if accountKey == "" {
		if len(entries) > 0 {
			b.returnEntries(entries)
		}
		if backing != nil {
			backing.Release()
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
			backing.Release()
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
