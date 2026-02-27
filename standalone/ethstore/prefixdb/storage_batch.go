package prefixdb

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type storageBatcher struct {
	mu      sync.Mutex
	pending map[string]map[string][]byte
}

func newStorageBatcher() *storageBatcher {
	return &storageBatcher{
		pending: make(map[string]map[string][]byte),
	}
}

func (sb *storageBatcher) reset() {
	sb.mu.Lock()
	sb.pending = make(map[string]map[string][]byte)
	sb.mu.Unlock()
}

func (sb *storageBatcher) put(accountKey string, storageKey, value []byte) {
	if accountKey == "" {
		return
	}
	sb.mu.Lock()
	perAccount := sb.pending[accountKey]
	if perAccount == nil {
		perAccount = make(map[string][]byte)
		sb.pending[accountKey] = perAccount
	}
	keyStr := string(storageKey)
	if value == nil {
		perAccount[keyStr] = nil
		sb.mu.Unlock()
		return
	}
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	perAccount[keyStr] = valCopy
	sb.mu.Unlock()
}

func (sb *storageBatcher) get(accountKey string, storageKey []byte) ([]byte, bool) {
	if accountKey == "" {
		return nil, false
	}
	keyStr := string(storageKey)
	sb.mu.Lock()
	perAccount := sb.pending[accountKey]
	if perAccount == nil {
		sb.mu.Unlock()
		return nil, false
	}
	value, ok := perAccount[keyStr]
	sb.mu.Unlock()
	return value, ok
}

// drain transfers ownership of all pending storage kvs to the caller.
func (sb *storageBatcher) drain() map[string]map[string][]byte {
	sb.mu.Lock()
	if len(sb.pending) == 0 {
		sb.mu.Unlock()
		return nil
	}
	batch := sb.pending
	sb.pending = make(map[string]map[string][]byte)
	sb.mu.Unlock()
	return batch
}

func (db *PrefixDB) initStorageBatcher() {
	if db.storageBatch == nil {
		db.storageBatch = newStorageBatcher()
	}
}

func (db *PrefixDB) stopStorageBatcher() {
	if db.storageBatch == nil {
		return
	}
	db.storageBatch.reset()
	db.storageBatch = nil
}

// BatchPut stages storage kvs in memory until BatchCommit.
func (db *PrefixDB) BatchPut(key, value, accountKey []byte) error {
	if db.storageBatch == nil {
		return errors.New("storage batcher not initialized")
	}
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}
	if keyType != TrieStorage {
		return errors.New("BatchPut only accepts storage keys")
	}
	if len(accountKey) == 0 {
		return errors.New("account key is required for BatchPut")
	}
	storageKey, err := db.normalizeStorageKey(key, keyType)
	if err != nil {
		return err
	}
	if db.storageCache != nil {
		db.storageCache.Remove(db.storageCacheKey(accountKey, storageKey))
	}
	db.storageBatch.put(string(accountKey), storageKey, value)
	return nil
}

// BatchCommit persists all staged storage kvs and waits for storage GC completion.
func (db *PrefixDB) BatchCommit() error {
	if db.storageBatch == nil {
		return nil
	}
	batch := db.storageBatch.drain()
	if len(batch) == 0 {
		return nil
	}

	// Hold the write lock across the commit to serialize with regular Put/Delete.
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	accountKeys := make([]string, 0, len(batch))
	for accountKey := range batch {
		accountKeys = append(accountKeys, accountKey)
	}
	sort.Strings(accountKeys)

	for _, accountKey := range accountKeys {
		perAccount := batch[accountKey]
		if len(perAccount) == 0 {
			if err := db.commitStorageForAccount(accountKey, nil); err != nil {
				return err
			}
			continue
		}
		kvs := make([]kvPair, 0, len(perAccount))
		for k, v := range perAccount {
			keyBytes := []byte(k)
			var valCopy []byte
			if v != nil {
				valCopy = make([]byte, len(v))
				copy(valCopy, v)
			}
			kvs = append(kvs, kvPair{key: keyBytes, val: valCopy})
		}
		sortKVPairs(kvs)
		if err := db.commitStorageForAccount(accountKey, kvs); err != nil {
			return err
		}
	}

	return db.waitForStorageGCIdle()
}

func (db *PrefixDB) commitStorageForAccount(accountKey string, kvs []kvPair) error {
	var (
		accOff         int64
		existingFileID uint32
		existingOffset int64
		existingSize   uint64
	)

	node, err := db.getNode([]byte(accountKey))
	if err != nil {
		return err
	}
	if node != nil {
		accOff = node.offset
		existingFileID = node.storageFileID
		existingOffset = node.storageOffset
		existingSize = node.storageSize
	}
	if len(kvs) == 0 {
		if err := db.prefixTree.Put([]byte(accountKey), accOff, 0, 0, 0); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(accountKey, StorageInfo{})
		if db.batch != nil {
			_ = db.batch.updateStoragePointer(stringToBytes(accountKey), StorageInfo{})
		}
		db.invalidateStorageBuffer(accountKey)
		return nil
	}

	fileID, off, sz, err := db.persistStorageEntries(kvs, existingFileID, existingOffset, existingSize)
	if err != nil {
		return err
	}
	if err := db.prefixTree.Put([]byte(accountKey), accOff, fileID, off, sz); err != nil {
		return err
	}
	info := StorageInfo{
		storageFileID: fileID,
		storageOffset: off,
		storageSize:   sz,
	}
	db.nodeCache.UpdateStoragePointer(accountKey, info)
	if db.batch != nil {
		_ = db.batch.updateStoragePointer(stringToBytes(accountKey), info)
	}

	// cacheKeyHex := hex.EncodeToString([]byte(accountKey))
	// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", info.storageFileID) + ", offset:" + fmt.Sprintf("%d", info.storageOffset) + ", size:" + fmt.Sprintf("%d", info.storageSize))
	// db.invalidateStorageBuffer(accountKey)
	return nil
}

// batchGetOverlay returns staged values for read-your-writes semantics.
func (db *PrefixDB) batchGetOverlay(key, accountKey []byte) ([]byte, bool) {
	if db.storageBatch == nil || len(accountKey) == 0 {
		return nil, false
	}
	storageKey, err := db.normalizeStorageKey(key, TrieStorage)
	if err != nil {
		return nil, false
	}
	return db.storageBatch.get(string(accountKey), storageKey)
}

func (db *PrefixDB) waitForStorageGCIdle() error {
	for {
		if db.isStorageGCIdle() {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}
