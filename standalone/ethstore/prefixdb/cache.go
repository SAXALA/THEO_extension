package prefixdb

import (
	"sync"
	"unsafe"
)

// NodeCacheEntry mirrors the data kept in the prefix tree plus the cached value.
type NodeCacheEntry struct {
	Key           string
	Value         []byte
	AccountOffset uint64
	AccountSize   uint32
	StorageInfo   StorageInfo
}

// StorageInfo keeps track of persisted storage metadata for a trie account node.
type StorageInfo struct {
	storageFileID uint32
	storageOffset uint64
	storageSize   uint64
}

type nodeCacheRecord struct {
	value         []byte
	accountOffset uint64
	accountSize   uint32
	storageInfo   StorageInfo
}

// NodeCache is a typed view over the shared byte-budgeted hybrid LRU+LFU implementation.
type NodeCache struct {
	lock   sync.RWMutex
	shared *sharedByteCache
}

// NewNodeCache instantiates a hybrid LRU+LFU cache with the provided capacity.
func NewNodeCache(capacityBytes uint64) (*NodeCache, error) {
	return newSharedNodeCache(newSharedByteCache(capacityBytes)), nil
}

func (nc *NodeCache) Close() {}

func (nc *NodeCache) Clear() {
	if nc == nil || nc.shared == nil {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.shared.RemoveNamespace(sharedCacheNamespaceNode)
}

func newSharedNodeCache(shared *sharedByteCache) *NodeCache {
	if shared == nil {
		return nil
	}
	return &NodeCache{shared: shared}
}

// Get returns the cached entry when present.
func (nc *NodeCache) Get(key string) (NodeCacheEntry, bool) {
	if nc == nil || key == "" {
		return NodeCacheEntry{}, false
	}
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	if raw, ok := nc.shared.Get(sharedCacheNamespaceNode, key); ok {
		rec := raw.(*nodeCacheRecord)
		return NodeCacheEntry{
			Key:           key,
			Value:         cloneBytes(rec.value),
			AccountOffset: rec.accountOffset,
			AccountSize:   rec.accountSize,
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
	rec := &nodeCacheRecord{
		value:         cloneBytes(entry.Value),
		accountOffset: entry.AccountOffset,
		accountSize:   entry.AccountSize,
		storageInfo:   entry.StorageInfo,
	}
	nc.shared.Add(sharedCacheNamespaceNode, entry.Key, rec, estimateNodeCacheRecordSize(entry.Key, rec))
}

// StoreMetadata records node metadata while preserving any cached value.
func (nc *NodeCache) StoreMetadata(key string, accountOffset uint64, accountSize uint32, storageInfo StorageInfo) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	rec := &nodeCacheRecord{accountOffset: accountOffset, accountSize: accountSize, storageInfo: storageInfo}
	if raw, ok := nc.shared.Get(sharedCacheNamespaceNode, key); ok {
		current := raw.(*nodeCacheRecord)
		rec.value = cloneBytes(current.value)
		if rec.accountSize == 0 && rec.accountOffset == current.accountOffset {
			rec.accountSize = current.accountSize
		}
	}
	nc.shared.Add(sharedCacheNamespaceNode, key, rec, estimateNodeCacheRecordSize(key, rec))
}

// UpdateValue refreshes the cached payload when the entry already exists.
func (nc *NodeCache) UpdateValue(key string, value []byte) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if raw, ok := nc.shared.Get(sharedCacheNamespaceNode, key); ok {
		current := raw.(*nodeCacheRecord)
		rec := &nodeCacheRecord{
			value:         cloneBytes(value),
			accountOffset: current.accountOffset,
			accountSize:   current.accountSize,
			storageInfo:   current.storageInfo,
		}
		nc.shared.Add(sharedCacheNamespaceNode, key, rec, estimateNodeCacheRecordSize(key, rec))
	}
}

func (nc *NodeCache) UpdateAccountOffset(key string, accountOffset uint64, accountSize uint32) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if raw, ok := nc.shared.Get(sharedCacheNamespaceNode, key); ok {
		current := raw.(*nodeCacheRecord)
		rec := &nodeCacheRecord{
			value:         cloneBytes(current.value),
			accountOffset: accountOffset,
			accountSize:   accountSize,
			storageInfo:   current.storageInfo,
		}
		nc.shared.Add(sharedCacheNamespaceNode, key, rec, estimateNodeCacheRecordSize(key, rec))
	}
}

// UpdateStoragePointer updates storage metadata when a node's storage segment changes.
func (nc *NodeCache) UpdateStoragePointer(key string, storageInfo StorageInfo) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	if raw, ok := nc.shared.Get(sharedCacheNamespaceNode, key); ok {
		current := raw.(*nodeCacheRecord)
		rec := &nodeCacheRecord{
			value:         cloneBytes(current.value),
			accountOffset: current.accountOffset,
			accountSize:   current.accountSize,
			storageInfo:   storageInfo,
		}
		nc.shared.Add(sharedCacheNamespaceNode, key, rec, estimateNodeCacheRecordSize(key, rec))
	}
}

// Delete removes an entry from the cache.
func (nc *NodeCache) Delete(key string) {
	if nc == nil || key == "" {
		return
	}
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.shared.Remove(sharedCacheNamespaceNode, key)
}

func estimateNodeCacheRecordSize(key string, rec *nodeCacheRecord) uint64 {
	if rec == nil {
		return 1
	}
	total := uint64(len(key)) + uint64(len(rec.value)) + uint64(unsafe.Sizeof(nodeCacheRecord{}))
	if total == 0 {
		return 1
	}
	return total
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dup := make([]byte, len(src))
	copy(dup, src)
	return dup
}
