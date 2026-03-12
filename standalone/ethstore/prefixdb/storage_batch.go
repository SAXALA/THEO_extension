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
	// unresolved stores original full keys (string) -> value for entries
	// where the accountKey was not provided at BatchPut time.
	unresolved map[string][]byte
}

func newStorageBatcher() *storageBatcher {
	return &storageBatcher{
		pending:    make(map[string]map[string][]byte),
		unresolved: make(map[string][]byte),
	}
}

func (sb *storageBatcher) reset() {
	sb.mu.Lock()
	sb.pending = make(map[string]map[string][]byte)
	sb.unresolved = make(map[string][]byte)
	sb.mu.Unlock()
}

func (sb *storageBatcher) put(accountKey string, storageKey, value []byte) {
	if accountKey == "" {
		// Should not drop unresolved here; caller should use putUnresolved when accountKey is unknown.
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

func (sb *storageBatcher) putUnresolved(originalKey string, value []byte) {
	if originalKey == "" {
		return
	}
	sb.mu.Lock()
	if value == nil {
		sb.unresolved[originalKey] = nil
		sb.mu.Unlock()
		return
	}
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	sb.unresolved[originalKey] = valCopy
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
// drain transfers ownership of all pending storage kvs to the caller.
// Returns both the per-account pending map and the unresolved original-key map.
func (sb *storageBatcher) drain() (map[string]map[string][]byte, map[string][]byte) {
	sb.mu.Lock()
	emptyPending := len(sb.pending) == 0
	emptyUnresolved := len(sb.unresolved) == 0
	if emptyPending && emptyUnresolved {
		sb.mu.Unlock()
		return nil, nil
	}
	batch := sb.pending
	unresolved := sb.unresolved
	sb.pending = make(map[string]map[string][]byte)
	sb.unresolved = make(map[string][]byte)
	sb.mu.Unlock()
	return batch, unresolved
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

// StorageBatchPut stages storage kvs in memory until BatchCommit.
func (db *PrefixDB) StorageBatchPut(key, value, accountKey []byte) error {
	if db.storageBatch == nil {
		return errors.New("storage batcher not initialized")
	}
	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		return err
	}
	if len(accountKey) == 0 {
		// Defer resolution of the parent account key until BatchCommit.
		db.storageBatch.putUnresolved(string(key), value)
		return nil
	}
	if db.storageCache != nil {
		db.storageCache.Remove(db.storageCacheKey(accountKey, storageKey))
	}
	db.storageBatch.put(string(accountKey), storageKey, value)
	return nil
}

// StorageBatchCommit persists all staged storage kvs and waits for storage GC completion.
func (db *PrefixDB) StorageBatchCommit() (err error) {
	if db.storageBatch == nil {
		return nil
	}
	if db.prefixTree != nil {
		db.prefixTree.beginGlobalCommit()
		defer func() {
			if endErr := db.prefixTree.endGlobalCommit(); err == nil {
				err = endErr
			}
		}()
	}
	batch, unresolved := db.storageBatch.drain()
	if len(batch) == 0 && len(unresolved) == 0 {
		return nil
	}
	if batch == nil {
		batch = make(map[string]map[string][]byte)
	}

	// Hold the write lock across the commit to serialize with regular Put/Delete.
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	if len(unresolved) > 0 {
		for origKeyStr, v := range unresolved {
			origKeyBytes := []byte(origKeyStr)
			var accountKey []byte
			if db.ParentKeyResolver != nil {
				accountKey = db.ParentKeyResolver(origKeyBytes)
			}
			if accountKey == nil {
				// fmt.Printf("Warning: failed to resolve parent account key for storage key %s\n", origKeyStr)
				continue
			}
			storageKey, err := db.normalizeStorageKey(origKeyBytes)
			if err != nil {
				return err
			}
			accStr := string(accountKey)
			perAcc := batch[accStr]
			if perAcc == nil {
				perAcc = make(map[string][]byte)
				batch[accStr] = perAcc
			}
			storageKeyStr := string(storageKey)
			// v is already owned by this commit (copied in putUnresolved),
			// so assign directly to avoid an extra allocation+copy here.
			perAcc[storageKeyStr] = v
			if db.storageCache != nil {
				db.storageCache.Add(db.storageCacheKey(accountKey, storageKey), v)
			}
		}
	}

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
			// v is already a stable copy owned by this commit (sb.put makes a copy).
			kvs = append(kvs, kvPair{key: keyBytes, val: v})
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

	accountKeyBytes := []byte(accountKey)
	node, err := db.getNode(accountKeyBytes)
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
		if err := db.prefixTree.Put(accountKeyBytes, accOff, 0, 0, 0); err != nil {
			return err
		}
		db.nodeCache.StoreMetadata(accountKey, accOff, StorageInfo{})
		if db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(accountKey, StorageInfo{})
		}
		return nil
	}

	fileID, off, sz, err := db.persistStorageEntries(kvs, existingFileID, existingOffset, existingSize)
	if err != nil {
		return err
	}
	info := StorageInfo{
		storageFileID: fileID,
		storageOffset: off,
		storageSize:   sz,
	}
	if err := db.prefixTree.Put(accountKeyBytes, accOff, fileID, off, sz); err != nil {
		return err
	}
	db.nodeCache.StoreMetadata(accountKey, accOff, info)
	if db.accountBatch != nil {
		_ = db.accountBatch.updateStoragePointer(accountKey, info)
	}

	// cacheKeyHex := hex.EncodeToString([]byte(accountKey))
	// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", info.storageFileID) + ", offset:" + fmt.Sprintf("%d", info.storageOffset) + ", size:" + fmt.Sprintf("%d", info.storageSize))
	return nil
}

// batchGetOverlay returns staged values for read-your-writes semantics.
func (db *PrefixDB) batchGetOverlay(key, accountKey []byte) ([]byte, bool) {
	if db.storageBatch == nil || len(accountKey) == 0 {
		return nil, false
	}
	storageKey, err := db.normalizeStorageKey(key)
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
