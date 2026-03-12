package prefixdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tinoryj/EthStore/standalone/ethstore/pebblestore"
)

const storageMaxFileSize int64 = 1 << 30 // 1GB

const (
	segmentedStorageFlag            uint32 = 1 << 31
	segmentedDirNamePrefix                 = "storage_seg_"
	segmentIndexFileName                   = "index.meta"
	segmentIndexCacheThresholdBytes        = 0   // cache all decoded segment indexes
	segmentIndexCacheCapacityMiB           = 64  // total segment-index cache budget in MiB
	defaultStorageGCThreshold              = 2.0 // when chunk file size > chunkSize * threshold, trigger GC for the segment
	storageGCQueueMultiplier               = 8
)

const (
	segmentIndexMultiLevelThreshold = 16 * 1024
	segmentIndexLevel2TargetSize    = 4 * 1024
	segmentIndexLevel2MaxSize       = 8 * 1024
	segmentIndexFlatMagic           = 0x464c4958 // 'FLIX'
	segmentIndexMultiLevelMagic     = 0x4d4c4958 // 'MLIX'
	segmentIndexFormatVersion       = 1
	segmentIndexFlatVersion         = 1
)

const segmentIndexLevel2Pattern = "index.meta.l2.%08d"

const ()

type KeyType int

const (
	TrieAccount KeyType = iota // TrieAccount
	TrieStorage                // TrieStorage
)

const storageKeyTrimOffset = 33 // 'O' + 32-byte account hash

type kvPair struct {
	key []byte
	val []byte
}

type segmentChunkMeta struct {
	FileName  string
	KeyStart  []byte
	KeyEnd    []byte
	KVCount   uint32
	ChunkSize uint64
}

type segmentIndexLayoutMode uint8

const (
	indexLayoutFlat segmentIndexLayoutMode = iota
	indexLayoutMultiLevel
)

type segmentIndexL1Entry struct {
	MetaID     uint32
	KeyStart   []byte
	KeyEnd     []byte
	ChunkCount uint32
}

type segmentIndexLayout struct {
	mode       segmentIndexLayoutMode
	entries    []segmentIndexL1Entry
	nextMetaID uint32
	flatData   []byte
}

type storageGCJob struct {
	folderID uint32
	fileName string
	backing  *bufferLease
}

func (job storageGCJob) key() string {
	return fmt.Sprintf("%d:%s", job.folderID, job.fileName)
}

// binarySearchKVPairs locates key in a sorted kvPair slice using bytes.Compare.
// Returns the index and true when found, or the insertion point and false otherwise.
func binarySearchKVPairs(pairs []kvPair, key []byte) (int, bool) {
	low, high := 0, len(pairs)-1
	for low <= high {
		mid := (low + high) >> 1
		cmp := bytes.Compare(pairs[mid].key, key)
		switch {
		case cmp == 0:
			return mid, true
		case cmp < 0:
			low = mid + 1
		default:
			high = mid - 1
		}
	}
	return low, false

}

type AccountType int

const (
	NormalAccount AccountType = iota
	ContractAccount
)

type StateAccount struct {
	Nonce    uint64
	Balance  *big.Int
	Root     common.Hash
	CodeHash []byte
}

type storageOpBuffer struct {
	accountKey   string
	storagekvs   []kvPair
	pendingCount int
}

type PrefixDB struct {
	prefixTree  *PrefixTree
	accountFile *os.File
	// slotFile    *os.File

	nodeCache    *NodeCache
	sharedCache  *sharedByteCache
	accountBatch *WriteBatch
	// triePath             string       // path to the prefix tree file
	accountHashKeyPebble *pebblestore.PebbleStore // pebble store for account hash key index
	// hashIndex  hashIndex to aviod hash collision
	writeMutex sync.Mutex // mutex for writeCommit

	// segmentedMu coordinates readers with segmented storage rewrites (GC / index updates).
	// Readers take RLock across index selection + chunk read; writers take Lock.
	segmentedMu sync.RWMutex

	// segmentIndexMu protects in-memory segment index caches/layouts (they are mutated on reads).
	segmentIndexMu sync.Mutex
	// segmentIndexFolderLocks serializes segment index operations per folderID.
	segmentIndexFolderLocksMu sync.Mutex
	segmentIndexFolderLocks   map[uint32]*segmentIndexFolderLock

	storageDir       string
	storageCurFile   *os.File
	storageCurFileID uint32
	storageCurSize   int64
	fileHandleCache  *fileHandleCache
	storageBuf       storageOpBuffer
	segmentDirSeq    uint32

	// a index file maybe accessed frequently
	storageIndexFolderId uint32
	storageIndexMetas    []segmentChunkMeta
	storageIndexCache    *segmentIndexCache
	storageIndexReusable bool
	storageIndexArena    []byte
	storageGetCacheCount int

	storageIndexPartialFolder   uint32
	storageIndexPartialMetaID   uint32
	storageIndexPartialMetas    []segmentChunkMeta
	storageIndexPartialReusable bool
	storageIndexPartialArena    []byte

	storageIndexLayoutPath  string
	storageIndexLayoutCache segmentIndexLayout
	storageIndexLayoutReady bool

	storageGCQueue    chan storageGCJob
	storageGCInFlight map[string]struct{}
	storageGCStop     chan struct{}
	storageGCWait     sync.WaitGroup
	storageGCMu       sync.Mutex
	gcWorkerLimiter   chan struct{}

	nodeFileGCUnsortedRatioThreshold float64
	gcWorkers                        int

	storageCache            *storageValueCache
	stroageCacheSizeLimit   uint64
	storageChunkSize        int
	segmentedChunkHardLimit int // hard cap for individual chunk files

	// storageBatcher enables BatchPut/BatchCommit for storage-only kvs.
	storageBatch *storageBatcher
	// ParentKeyResolver, when set, is used to resolve a parent account key from
	// a storage key. It is intended to be set by the owning `ethstore.Database`
	// so that PrefixDB can defer resolution to the higher-level store.
	ParentKeyResolver func([]byte) []byte
	// for debug
	totalOps     uint64
	cachedOps    uint64
	timeOnRead   time.Duration
	readCount    uint64
	sortedOps    int
	GCCount      uint64
	GCWriteBytes uint64

	commitOldKVReadCount uint64
	commitOldKVReadBytes uint64
	totalReadBytes       uint64
	getReadReqCount      uint64
	getReadBytesSum      uint64

	trieStorageCachePairs uint64
	trieStorageCacheBytes uint64
	trieStorageLogPairs   uint64
	trieStorageLogBytes   uint64

	// nodeCache access stats (read path)
	nodeCacheLookups uint64
	nodeCacheHits    uint64
	nodeCacheMisses  uint64
	// Served means we returned from nodeCache without consulting PrefixTree/NodeFile.
	nodeCacheServed uint64
	// NodeFile (PrefixTree) access after nodeCache lookup.
	nodeCacheToNodeFile            uint64
	nodeCacheMissToNodeFile        uint64
	nodeCacheHitFallbackToNodeFile uint64
	diskIOStats                    [diskIOUsageCount]diskIOCounters
}

type diskIOUsage uint8

const (
	diskIOUsageAccountData diskIOUsage = iota
	diskIOUsageNodeFileLookup
	diskIOUsageNodeFileMutation
	diskIOUsageNodeFileGC
	diskIOUsageStorageCommonLogs
	diskIOUsageStorageSeparatedLogs
	diskIOUsageStorageGC
	diskIOUsageStorageSegmentIndex
	diskIOUsageCount
)

var diskIOUsageNames = [...]string{
	"account-data",
	"nodefile-lookup",
	"nodefile-mutation",
	"nodefile-gc",
	"storage-common-logs",
	"storage-separated-logs",
	"storage-gc",
	"storage-segment-index",
}

type diskIOCounters struct {
	readOps    uint64
	readBytes  uint64
	writeOps   uint64
	writeBytes uint64
}

// SerializedTrieNode
type SerializedTrieNode struct {
	Path        string
	IsLeaf      bool
	SlotIndices []int
	Offset      int64
}

type segmentIndexFolderLock struct {
	mu   sync.Mutex
	refs int
	gen  uint64
}

/*
*
  - NewPrefixDB creates a new PrefixDB instance.
  - It initializes the necessary files, directories, caches, and workers based on the provided configuration.
    the storageChunkFileSize is in bytes, and cacheSize is in bytes.
*/
func NewPrefixDB(dirpath string, storageChunkFileSize int, totalCacheSizeMiB int, storageGetCacheCount int) (*PrefixDB, error) {
	return NewPrefixDBWithRuntimeOptions(dirpath, storageChunkFileSize, totalCacheSizeMiB, storageGetCacheCount, 0, 0, 0)
}

// NewPrefixDBWithCacheSettings creates PrefixDB with a single shared cache
// budget in MiB. All PrefixDB caches share this total budget.
// Use <=0 values to fallback to the default shared cache size.
func NewPrefixDBWithCacheSettings(dirpath string, storageChunkFileSize int, totalCacheSizeMiB int, storageGetCacheCount int) (*PrefixDB, error) {
	return NewPrefixDBWithRuntimeOptions(dirpath, storageChunkFileSize, totalCacheSizeMiB, storageGetCacheCount, 0, 0, 0)
}

func NewPrefixDBWithRuntimeOptions(dirpath string, storageChunkFileSize int, totalCacheSizeMiB int, storageGetCacheCount int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64) (*PrefixDB, error) {
	fmt.Println(dirpath + " prefixDB Initializing...")
	// Try to load config from config.json in dirpath
	configPath := filepath.Join(dirpath, "config.json")
	cfg, err := LoadConfig(configPath)
	if err != nil {
		// If config file doesn't exist or fails to load, use default config
		cfg = DefaultConfig(dirpath)
	} else {
		// If BaseDir is not set in config, use dirpath
		if cfg.BaseDir == "" {
			cfg.BaseDir = dirpath
		}
		if cfg.GCWorkers == 0 {
			cfg.GCWorkers = cfg.NodeFileGCWorkers
		}
		if cfg.StorageGCThreshold == 0 {
			cfg.StorageGCThreshold = DefaultConfig(dirpath).StorageGCThreshold
		}
	}

	resolvedStorageGCThreshold := sanitizeStorageGCThreshold(cfg.StorageGCThreshold)
	if storageGCThreshold > 0 {
		resolvedStorageGCThreshold = sanitizeStorageGCThreshold(storageGCThreshold)
	}

	// Ensure base directory exists
	if err := os.MkdirAll(cfg.BaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base dir: %v", err)
	}

	// Resolve paths
	accountFilePath := resolvePath(cfg.BaseDir, cfg.AccountDir)
	storageDir := resolvePath(cfg.BaseDir, cfg.StorageDir)

	// Ensure directories exist
	if err := os.MkdirAll(filepath.Dir(accountFilePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create account dir: %v", err)
	}
	accountFile, err := os.OpenFile(accountFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("failed to open normal account file")
	}

	db := &PrefixDB{
		accountFile:                      accountFile,
		writeMutex:                       sync.Mutex{},
		segmentIndexFolderLocks:          make(map[uint32]*segmentIndexFolderLock),
		fileHandleCache:                  getGlobalFileHandleCache(),
		storageDir:                       storageDir,
		storageGetCacheCount:             storageGetCacheCount,
		storageChunkSize:                 storageChunkFileSize,
		segmentedChunkHardLimit:          computeSegmentedChunkHardLimit(storageChunkFileSize, resolvedStorageGCThreshold),
		nodeFileGCUnsortedRatioThreshold: cfg.NodeFileGCUnsortedRatioThreshold,
		gcWorkers:                        cfg.GCWorkers,
	}
	if nodeFileGCRatioThreshold > 0 {
		db.nodeFileGCUnsortedRatioThreshold = nodeFileGCRatioThreshold
	}
	if gcWorkers > 0 {
		db.gcWorkers = gcWorkers
	}
	db.gcWorkerLimiter = make(chan struct{}, sanitizePrefixTreeGCWorkerCount(db.gcWorkers))

	db.accountBatch = NewWriteBatch(db)
	sharedCacheBudgetMiB := segmentIndexCacheCapacityMiB
	if totalCacheSizeMiB > 0 {
		sharedCacheBudgetMiB = totalCacheSizeMiB
	}
	db.stroageCacheSizeLimit = uint64(sharedCacheBudgetMiB) * 1024 * 1024
	if err := os.MkdirAll(db.storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage dir: %v", err)
	}
	if err := db.openOrCreateStorageFile(); err != nil {
		return nil, fmt.Errorf("failed to init storage shard: %v", err)
	}

	sharedCache := newSharedByteCache(db.stroageCacheSizeLimit)
	db.sharedCache = sharedCache

	db.nodeCache = newSharedNodeCache(sharedCache)

	prefixTree, err := NewPrefixTree(db, dirpath)
	if err != nil {
		return nil, fmt.Errorf("failed to create prefix tree: %v", err)
	}

	db.prefixTree = prefixTree

	db.storageIndexCache = newSharedSegmentIndexCache(sharedCache)
	db.storageCache = newSharedStorageValueCache(sharedCache)

	db.startStorageGCWorker()

	db.initStorageBatcher()

	fmt.Println(dirpath + " prefixDB Initialized.")
	return db, nil
}

func (db *PrefixDB) Get(key []byte, accountKey []byte) ([]byte, bool, error) {
	readBefore := atomic.LoadUint64(&db.totalReadBytes)
	defer func() {
		readAfter := atomic.LoadUint64(&db.totalReadBytes)
		if readAfter >= readBefore {
			atomic.AddUint64(&db.getReadBytesSum, readAfter-readBefore)
		}
		atomic.AddUint64(&db.getReadReqCount, 1)
	}()

	keyType, err := db.getKeyType(key)
	if err != nil {
		return nil, false, err
	}

	switch keyType {
	case TrieAccount:
		cacheKey := string(key)
		if entry, ok := db.nodeCache.Get(cacheKey); ok && entry.Value != nil {
			return entry.Value, true, nil
		}

		if db.accountBatch != nil {
			if value, _, ok := db.accountBatch.get(key); ok {
				return value, true, nil
			}
		}

		node, err := db.getAccountNode(key)
		if err != nil {
			return nil, false, err
		}
		if node == nil {
			keyHex := fmt.Sprintf("%x", key)
			fmt.Printf("Account key %s not found in index\n", keyHex)
			return nil, false, nil
		}
		value, err := db.readFromFile(node.offset)
		if err != nil {
			return nil, false, err
		}

		db.nodeCache.Put(NodeCacheEntry{
			Key:           cacheKey,
			Value:         value,
			AccountOffset: node.offset,
			StorageInfo: StorageInfo{
				storageFileID: node.storageFileID,
				storageOffset: node.storageOffset,
				storageSize:   node.storageSize,
			},
		})

		// cacheKeyHex := hex.EncodeToString([]byte(cacheKey))
		// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", node.storageFileID) + ", offset:" + fmt.Sprintf("%d", node.storageOffset) + ", size:" + fmt.Sprintf("%d", node.storageSize))

		return value, true, nil

	case TrieStorage:
		storageKey, err := db.normalizeStorageKey(key)
		if err != nil {
			return nil, false, err
		}

		if accountKey == nil {
			fmt.Printf("Parent account key not found for %x\n", key)
			return nil, false, nil
		}

		// Read-your-writes for BatchPut: consult staged overlay before caches/disk.
		if v, present := db.batchGetOverlay(key, accountKey); present {
			if v == nil {
				return nil, false, nil
			}
			return v, true, nil
		}

		if value, ok := db.storageCache.Get(db.storageCacheKey(accountKey, storageKey)); ok {
			if value == nil {
				return nil, false, nil
			}
			valueBytes := value.([]byte)
			db.addTrieStorageFetchStats(true, valueBytes)
			return valueBytes, true, nil
		}

		value, ok, err := db.readAccountStorageValue(accountKey, storageKey)
		if err != nil {
			fmt.Println("Error reading account storage:", err)
			return nil, false, err
		}
		if ok {
			db.addTrieStorageFetchStats(false, value)
			return value, true, nil
		}
		return nil, false, nil
	default:
		return nil, false, errors.New("unknown key type")
	}
}

func (db *PrefixDB) Put(key, value, accountKey []byte) error {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}

	switch keyType {
	case TrieAccount:
		cacheKey := string(key)
		var stroageInfo StorageInfo
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			stroageInfo = entry.StorageInfo
			db.nodeCache.UpdateValue(cacheKey, value)

			// cacheKeyHex := hex.EncodeToString([]byte(cacheKey))
			// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", entry.StorageInfo.storageFileID) + ", offset:" + fmt.Sprintf("%d", entry.StorageInfo.storageOffset) + ", size:" + fmt.Sprintf("%d", entry.StorageInfo.storageSize))

		} else {
			// node, err := db.getAccountNode(key)
			// if err != nil {
			// 	return err
			// }
			stroageInfo = StorageInfo{
				storageFileID: 0,
				storageOffset: 0,
				storageSize:   0,
			}
			db.nodeCache.Put(NodeCacheEntry{
				Key:           cacheKey,
				Value:         value,
				AccountOffset: 0,
				StorageInfo:   stroageInfo,
			})
			// cacheKeyHex := hex.EncodeToString([]byte(cacheKey))
			// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", node.storageFileID) + ", offset:" + fmt.Sprintf("%d", node.storageOffset) + ", size:" + fmt.Sprintf("%d", node.storageSize))

		}
		if db.accountBatch != nil {
			db.accountBatch.add(key, value, stroageInfo.storageFileID, stroageInfo.storageOffset, stroageInfo.storageSize, ValueModified)
		}

	case TrieStorage:
		storageKey, err := db.normalizeStorageKey(key)
		if err != nil {
			return err
		}
		// db.totalOps++
		// if db.totalOps%10000 == 0 {
		// 	fmt.Printf("Total Ops: %d, Cached Ops: %d, Sorted Ops: %d, Read Count: %d, Time on Read: %s\n",
		// 		db.totalOps, db.cachedOps, db.sortedOps, db.readCount, db.timeOnRead)
		// }
		if db.storageCache != nil {
			if accountKey != nil {
				db.storageCache.Remove(db.storageCacheKey(accountKey, storageKey))
			}
		}

		if accountKey == nil {
			fmt.Printf("Parent account key not found for %x\n", key)
			return nil
		}

		return db.bufferStorageMutation(accountKey, storageKey, value)
	}
	return nil
}

func (db *PrefixDB) BatchPut(key, value, accountKey []byte) error {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}
	switch keyType {
	case TrieAccount:
		cacheKey := string(key)
		var stroageInfo StorageInfo
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			stroageInfo = entry.StorageInfo
			db.nodeCache.UpdateValue(cacheKey, value)
		} else {
			// node, err := db.getAccountNode(key)
			// if err != nil {
			// 	return err
			// }
			stroageInfo = StorageInfo{
				storageFileID: 0,
				storageOffset: 0,
				storageSize:   0,
			}
			db.nodeCache.Put(NodeCacheEntry{
				Key:           cacheKey,
				Value:         value,
				AccountOffset: 0,
				StorageInfo:   stroageInfo,
			})
		}
		if db.accountBatch != nil {
			db.accountBatch.add(key, value, stroageInfo.storageFileID, stroageInfo.storageOffset, stroageInfo.storageSize, ValueModified)
		}
		return nil
	case TrieStorage:
		return db.StorageBatchPut(key, value, accountKey)
	}
	return nil
}

func (db *PrefixDB) BatchCommit() (err error) {
	if db.prefixTree != nil {
		db.prefixTree.beginGlobalCommit()
		defer func() {
			if endErr := db.prefixTree.endGlobalCommit(); err == nil {
				err = endErr
			}
		}()
	}
	if db.accountBatch != nil {
		if err := db.accountBatch.CommitBatch(); err != nil {
			return err
		}
	}
	if err := db.StorageBatchCommit(); err != nil {

		return err
	}
	return nil
}

func (db *PrefixDB) Has(key []byte, accountKey []byte) (bool, error) {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return false, err
	}

	switch keyType {
	case TrieAccount:
		cacheKey := string(key)
		if _, ok := db.nodeCache.Get(cacheKey); ok {
			return true, nil
		}

		if db.accountBatch != nil {
			if _, _, ok := db.accountBatch.get(key); ok {
				return true, nil
			}
		}

		node, err := db.getAccountNode(key)
		if err != nil {
			return false, err
		}
		if node == nil {
			fmt.Printf("Account key %s not found in index\n", string(key))
			return false, nil
		}
		value, err := db.readFromFile(node.offset)
		if err != nil {
			return false, err
		}

		db.nodeCache.Put(NodeCacheEntry{
			Key:           cacheKey,
			Value:         value,
			AccountOffset: node.offset,
			StorageInfo: StorageInfo{
				storageFileID: node.storageFileID,
				storageOffset: node.storageOffset,
				storageSize:   node.storageSize,
			},
		})

		// cacheKeyHex := hex.EncodeToString([]byte(cacheKey))
		// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", node.storageFileID) + ", offset:" + fmt.Sprintf("%d", node.storageOffset) + ", size:" + fmt.Sprintf("%d", node.storageSize))

		return true, nil
	case TrieStorage:
		storageKey, err := db.normalizeStorageKey(key)
		if err != nil {
			return false, err
		}

		if accountKey == nil {
			fmt.Printf("Parent account key not found for %x\n", key)
			return false, nil
		}

		// Read-your-writes for BatchPut.
		if v, present := db.batchGetOverlay(key, accountKey); present {
			return v != nil, nil
		}

		if v, ok := db.storageCache.Get(db.storageCacheKey(accountKey, storageKey)); ok {
			return v != nil, nil
		}
		_, ok, err := db.readAccountStorageValue(accountKey, storageKey)
		if err != nil {
			fmt.Println("Error reading account storage:", err)
			return false, err
		}
		if ok {
			return true, nil
		}
		return false, nil
	default:
		return false, errors.New("unknown key type")
	}
}

func (db *PrefixDB) Delete(key []byte, accountKey []byte) error {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}

	switch keyType {
	case TrieAccount:
		if db.accountBatch != nil {
			db.accountBatch.delete(key)
		}
		if db.nodeCache != nil {
			db.nodeCache.Delete(string(key))
		}
		return db.storeNode(key, &TrieNode{
			storageFileID: 0,
			storageOffset: 0,
			offset:        0,
			storageSize:   0,
		})

	case TrieStorage:
		storageKey, err := db.normalizeStorageKey(key)
		if err != nil {
			return err
		}

		if db.storageCache != nil {
			if accountKey != nil {
				db.storageCache.Remove(db.storageCacheKey(accountKey, storageKey))
			}
		}

		if accountKey == nil {
			fmt.Printf("Parent account key not found for %x\n", key)
			return nil
		}

		return db.bufferStorageMutation(accountKey, storageKey, nil)

	default:
		return errors.New("unknown key type")
	}
}

func (db *PrefixDB) bufferStorageMutation(accountKey, storageKey, value []byte) error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	accountStr := string(accountKey)
	if db.storageBuf.accountKey != accountStr {
		if db.storageBuf.accountKey != "" {
			if err := db.flushStorageBuffer(); err != nil {
				return err
			}
		}
		db.storageBuf.reset()
		db.storageBuf.accountKey = accountStr
		db.storageBuf.storagekvs = make([]kvPair, 0)
	}
	db.storageBuf.storagekvs = append(db.storageBuf.storagekvs, kvPair{key: storageKey, val: value})
	return nil
}

func (db *PrefixDB) flushStorageBuffer() error {
	buf := &db.storageBuf
	if buf.accountKey == "" {
		return nil
	}
	var (
		accOff         int64
		existingFileID uint32
		existingOffset int64
		existingSize   uint64
	)

	node, err := db.getNode([]byte(buf.accountKey))
	if err != nil {
		return err
	}
	if node != nil {
		accOff = node.offset
		existingFileID = node.storageFileID
		existingOffset = node.storageOffset
		existingSize = node.storageSize
	}
	sortKVPairs(buf.storagekvs)
	if len(buf.storagekvs) == 0 {
		if err := db.prefixTree.Put([]byte(buf.accountKey), accOff, 0, 0, 0); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(buf.accountKey, StorageInfo{})
		if db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(buf.accountKey, StorageInfo{})
		}
	} else {
		fileID, off, sz, err := db.persistStorageEntries(buf.storagekvs, existingFileID, existingOffset, existingSize)
		if err != nil {
			return err
		}
		if err := db.prefixTree.Put([]byte(buf.accountKey), accOff, fileID, off, sz); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(buf.accountKey, StorageInfo{
			storageFileID: fileID,
			storageOffset: off,
			storageSize:   sz,
		})

		// cacheKeyHex := hex.EncodeToString([]byte(buf.accountKey))
		// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", fileID) + ", offset:" + fmt.Sprintf("%d", off) + ", size:" + fmt.Sprintf("%d", sz))

		if db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(buf.accountKey, StorageInfo{
				storageFileID: fileID,
				storageOffset: off,
				storageSize:   sz,
			})
		}
	}
	buf.reset()
	return nil
}

func (sb *storageOpBuffer) reset() {
	*sb = storageOpBuffer{}
}

var (
	headerPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4)
		},
	}
	smallBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 1024)
		},
	}
	mediumBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 32*1024) // 32KB
		},
	}
	oneMBBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 1024*1024) // 1MB
		},
	}
	fourMBBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4*1024*1024) // 4MB
		},
	}

	kvPairScratchPool = sync.Pool{
		New: func() interface{} {
			return make([]kvPair, 0)
		},
	}
	kvPairEntryPool = sync.Pool{
		New: func() interface{} {
			return make([]kvPair, 0, 64)
		},
	}
)

// sortKVPairs performs a stable merge sort on the provided kvPair slice using a pooled buffer.
func sortKVPairs(entries []kvPair) {
	if len(entries) < 2 {
		return
	}

	if len(entries) <= 65536 {
		sort.SliceStable(entries, func(i, j int) bool {
			return bytes.Compare(entries[i].key, entries[j].key) < 0
		})
		return
	}
	buf := kvPairScratchPool.Get().([]kvPair)
	if cap(buf) < len(entries) {
		buf = make([]kvPair, len(entries))
	}
	buf = buf[:len(entries)]
	copy(buf, entries)

	src := buf
	dst := entries
	srcIsEntries := false
	for width := 1; width < len(entries); width <<= 1 {
		for start := 0; start < len(entries); start += 2 * width {
			mid := start + width
			if mid > len(entries) {
				mid = len(entries)
			}
			end := start + 2*width
			if end > len(entries) {
				end = len(entries)
			}
			left := start
			right := mid
			pos := start
			for left < mid && right < end {
				if bytes.Compare(src[left].key, src[right].key) <= 0 {
					dst[pos] = src[left]
					left++
				} else {
					dst[pos] = src[right]
					right++
				}
				pos++
			}
			for left < mid {
				dst[pos] = src[left]
				left++
				pos++
			}
			for right < end {
				dst[pos] = src[right]
				right++
				pos++
			}

		}
		src, dst = dst, src
		srcIsEntries = !srcIsEntries
	}

	if !srcIsEntries {
		copy(entries, src)
	}
	kvPairScratchPool.Put(buf[:0])
}

// getDataBuffer returns a byte slice of the requested size from the appropriate buffer pool.
func getDataBuffer(size int) []byte {
	var buffer []byte
	if size <= 1024 {
		buffer = smallBufferPool.Get().([]byte)
		return buffer[:size]
	} else if size <= 32*1024 {
		buffer = mediumBufferPool.Get().([]byte)
		return buffer[:size]
	} else if size <= 1024*1024 {
		buffer = oneMBBufferPool.Get().([]byte)
		return buffer[:size]
	} else if size <= 4*1024*1024 {
		buffer = fourMBBufferPool.Get().([]byte)
		return buffer[:size]
	}
	return make([]byte, size)
}

func putDataBuffer(buf []byte) {
	if buf == nil {
		return
	}

	capacity := cap(buf)
	switch {
	case capacity <= 1024:
		smallBufferPool.Put(buf[:capacity])
	case capacity <= 32*1024:
		mediumBufferPool.Put(buf[:capacity])
	case capacity <= 1024*1024:
		oneMBBufferPool.Put(buf[:capacity])
	case capacity <= 4*1024*1024:
		fourMBBufferPool.Put(buf[:capacity])
	default:
		// do nothing for large buffers
	}
}

type bufferLease struct {
	buf  []byte
	refs int
	mu   sync.Mutex
}

func newBufferLease(buf []byte) *bufferLease {
	if buf == nil {
		return nil
	}
	return &bufferLease{buf: buf, refs: 1}
}

func (l *bufferLease) Retain() *bufferLease {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	l.refs++
	l.mu.Unlock()
	return l
}

func (l *bufferLease) Release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.refs--
	refs := l.refs
	buf := l.buf
	l.mu.Unlock()
	if refs == 0 {
		putDataBuffer(buf)
	}
}

func (l *bufferLease) Bytes() []byte {
	if l == nil {
		return nil
	}
	return l.buf
}

func readUint16BE(b []byte) uint16 {
	return uint16(b[0])<<8 | uint16(b[1])
}

func readUint32BE(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func readUint64BE(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

func writeUint16BE(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

func writeUint32BE(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func writeUint64BE(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func (db *PrefixDB) readFromFile(offset int64) ([]byte, error) {
	var file *os.File
	file = db.accountFile
	header := headerPool.Get().([]byte)
	defer headerPool.Put(header)

	if cap(header) < 6 {
		header = make([]byte, 4)
	} else {
		header = header[:4]
	}

	n, err := file.ReadAt(header, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read header at offset %d: %v", offset, err)
	}
	db.addDiskRead(diskIOUsageAccountData, n)

	keySize := int(uint16(header[0])<<8 | uint16(header[1]))
	valueSize := int(uint16(header[2])<<8 | uint16(header[3]))

	totalSize := keySize + valueSize

	combinedData := getDataBuffer(totalSize)
	defer putDataBuffer(combinedData)

	n, err = file.ReadAt(combinedData, offset+4)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read combined data at offset %d: %v", offset+6, err)
	}
	db.addDiskRead(diskIOUsageAccountData, n)

	value := make([]byte, valueSize)
	copy(value, combinedData[keySize:totalSize])

	return value, nil
}

func (db *PrefixDB) addCommitOldKVReadStats(pairCount int, bytes uint64) {
	if pairCount > 0 {
		atomic.AddUint64(&db.commitOldKVReadCount, uint64(pairCount))
	}
	if bytes > 0 {
		atomic.AddUint64(&db.commitOldKVReadBytes, bytes)
	}
}

func (db *PrefixDB) addReadBytes(n int) {
	if n > 0 {
		atomic.AddUint64(&db.totalReadBytes, uint64(n))
	}
}

func (db *PrefixDB) addDiskRead(usage diskIOUsage, n int) {
	if db == nil || usage >= diskIOUsageCount {
		return
	}
	atomic.AddUint64(&db.diskIOStats[usage].readOps, 1)
	if n > 0 {
		atomic.AddUint64(&db.diskIOStats[usage].readBytes, uint64(n))
		db.addReadBytes(n)
	}
}

func (db *PrefixDB) addDiskWrite(usage diskIOUsage, n int) {
	if db == nil || usage >= diskIOUsageCount {
		return
	}
	atomic.AddUint64(&db.diskIOStats[usage].writeOps, 1)
	if n > 0 {
		atomic.AddUint64(&db.diskIOStats[usage].writeBytes, uint64(n))
	}
}

func (db *PrefixDB) readFileWithStats(path string, usage diskIOUsage) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		db.addDiskRead(usage, len(data))
	}
	return data, err
}

func (db *PrefixDB) writeFileWithStats(path string, data []byte, perm os.FileMode, usage diskIOUsage) error {
	if err := os.WriteFile(path, data, perm); err != nil {
		return err
	}
	db.addDiskWrite(usage, len(data))
	return nil
}

func (db *PrefixDB) printDiskIOStats() {
	if db == nil {
		return
	}
	var totalReadOps, totalReadBytes, totalWriteOps, totalWriteBytes uint64
	for usage := diskIOUsage(0); usage < diskIOUsageCount; usage++ {
		stats := &db.diskIOStats[usage]
		readOps := atomic.LoadUint64(&stats.readOps)
		readBytes := atomic.LoadUint64(&stats.readBytes)
		writeOps := atomic.LoadUint64(&stats.writeOps)
		writeBytes := atomic.LoadUint64(&stats.writeBytes)
		totalReadOps += readOps
		totalReadBytes += readBytes
		totalWriteOps += writeOps
		totalWriteBytes += writeBytes
		if readOps == 0 && readBytes == 0 && writeOps == 0 && writeBytes == 0 {
			continue
		}
		fmt.Printf("PrefixDB disk IO stats [%s]: readOps=%d readBytes=%d writeOps=%d writeBytes=%d\n",
			diskIOUsageNames[usage], readOps, readBytes, writeOps, writeBytes,
		)
	}
	fmt.Printf("PrefixDB disk IO stats [total]: readOps=%d readBytes=%d writeOps=%d writeBytes=%d\n",
		totalReadOps, totalReadBytes, totalWriteOps, totalWriteBytes,
	)
}

func (db *PrefixDB) addTrieStorageFetchStats(fromCache bool, value []byte) {
	if len(value) == 0 {
		return
	}
	if fromCache {
		atomic.AddUint64(&db.trieStorageCachePairs, 1)
		atomic.AddUint64(&db.trieStorageCacheBytes, uint64(len(value)))
		return
	}
	atomic.AddUint64(&db.trieStorageLogPairs, 1)
	atomic.AddUint64(&db.trieStorageLogBytes, uint64(len(value)))
}

func (db *PrefixDB) Close() error {
	errs := []error{}
	// Flush any pending storage batch writes before tearing down files.
	if db.storageBatch != nil {
		if err := db.StorageBatchCommit(); err != nil {
			// best-effort: keep closing even if batch commit fails
			errs = append(errs, fmt.Errorf("failed to commit storage batch: %v", err))
		}
		db.stopStorageBatcher()
	}

	db.stopStorageGCWorker()

	fmt.Printf("PrefixDB GC stats: count=%d writeBytes=%d\n",
		atomic.LoadUint64(&db.GCCount),
		atomic.LoadUint64(&db.GCWriteBytes),
	)
	getReqs := atomic.LoadUint64(&db.getReadReqCount)
	getReadBytes := atomic.LoadUint64(&db.getReadBytesSum)
	avgGetReadBytes := float64(0)
	if getReqs > 0 {
		avgGetReadBytes = float64(getReadBytes) / float64(getReqs)
	}
	fmt.Printf("PrefixDB commit old KV read stats: pairs=%d bytes=%d\n",
		atomic.LoadUint64(&db.commitOldKVReadCount),
		atomic.LoadUint64(&db.commitOldKVReadBytes),
	)
	fmt.Printf("PrefixDB get read stats: requests=%d totalBytes=%d avgBytes=%.2f\n",
		getReqs,
		getReadBytes,
		avgGetReadBytes,
	)
	fmt.Printf("PrefixDB TrieNodeStorage fetch stats: cachePairs=%d cacheBytes=%d logPairs=%d logBytes=%d\n",
		atomic.LoadUint64(&db.trieStorageCachePairs),
		atomic.LoadUint64(&db.trieStorageCacheBytes),
		atomic.LoadUint64(&db.trieStorageLogPairs),
		atomic.LoadUint64(&db.trieStorageLogBytes),
	)
	lookups := atomic.LoadUint64(&db.nodeCacheLookups)
	hits := atomic.LoadUint64(&db.nodeCacheHits)
	misses := atomic.LoadUint64(&db.nodeCacheMisses)
	served := atomic.LoadUint64(&db.nodeCacheServed)
	toNodeFile := atomic.LoadUint64(&db.nodeCacheToNodeFile)
	missToNodeFile := atomic.LoadUint64(&db.nodeCacheMissToNodeFile)
	hitFallbackToNodeFile := atomic.LoadUint64(&db.nodeCacheHitFallbackToNodeFile)
	fallback := uint64(0)
	if hits >= served {
		fallback = hits - served
	}
	fmt.Printf("PrefixDB nodeCache stats: lookups=%d hits=%d misses=%d served=%d fallback=%d toNodeFile=%d missToNodeFile=%d hitFallbackToNodeFile=%d\n",
		lookups, hits, misses, served, fallback, toNodeFile, missToNodeFile, hitFallbackToNodeFile,
	)

	if err := db.flushStorageBuffer(); err != nil {
		errs = append(errs, fmt.Errorf("failed to flush storage buffer: %v", err))
	}

	if db.nodeCache != nil {
		db.nodeCache.Close()
	}

	// if db.storageCache != nil {
	// 	db.storageCache.Close()
	// }

	// forbid further writes to the database
	if db.accountBatch != nil {
		db.accountBatch.DisableAutoCommit()

		// wait for any ongoing background commit to finish
		if db.accountBatch.bgCommit {
			db.accountBatch.DisableBackgroundCommit()
		}
	}

	if db.accountBatch != nil {
		if len(db.accountBatch.operations) > 0 {
			if err := db.WriteCommit(db.accountBatch); err != nil {
				fmt.Printf("Error committing batch operations: %v\n", err)
			}
		}
	}

	if err := db.prefixTree.Close(); err != nil {
		return fmt.Errorf("failed to close prefix tree: %v", err)
	}

	if err := db.accountFile.Sync(); err != nil {
		// Check if file is already closed
		if !errors.Is(err, os.ErrClosed) {
			errs = append(errs, fmt.Errorf("failed to sync account file: %v", err))
		}
	}

	if err := db.accountFile.Close(); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
	}

	db.nodeCache = nil
	db.accountBatch = nil

	if db.storageCurFile != nil {
		_ = db.storageCurFile.Sync()
		_ = db.storageCurFile.Close()
		db.storageCurFile = nil
	}
	// db.accountHashKeyPebble = nil
	db.printDiskIOStats()

	if len(errs) > 0 {
		fmt.Printf("Errors occurred during closing: %v\n", errs)
		return errs[0]
	}

	if db.accountHashKeyPebble != nil {
		if err := db.accountHashKeyPebble.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close pebble store: %v", err))
		}
		db.accountHashKeyPebble = nil
	}
	return nil
}

// ExtractAccountData extracts account data from the value of a TrieAccount node.
func (db *PrefixDB) ExtractAccountData(key, value []byte) (*StateAccount, error) {
	if key == nil || value == nil || key[0] != 'A' {
		return nil, errors.New("invalid key or value")
	}

	var rawNode []interface{}
	if err := rlp.DecodeBytes(value, &rawNode); err != nil {
		return nil, errors.New("failed to decode account data")
	}

	var accountRLP []byte

	switch len(rawNode) {
	case 2:
		// leaf node
		firstItem, ok := rawNode[0].([]byte)
		if !ok || len(firstItem) == 0 {
			return nil, errors.New("invalid node format")
		}

		// check prefix
		prefix := firstItem[0] >> 4
		if prefix == 2 || prefix == 3 { // the prefix indicates a leaf node
			if valBytes, ok := rawNode[1].([]byte); ok {
				accountRLP = valBytes
			} else {
				return nil, errors.New("invalid account data format")
			}
		} else {
			fmt.Println("extend node, not a leaf node")
			return nil, nil // not a leaf node
		}

	case 17:
		// branch node
		if valBytes, ok := rawNode[16].([]byte); ok && len(valBytes) > 0 {
			accountRLP = valBytes
		} else {
			fmt.Println("Branch node without value")
			return nil, nil
		}

	default:
		return nil, errors.New("unknow node format") // unknown node format
	}

	if len(accountRLP) == 0 {
		return nil, errors.New("no account data found in node")
	}

	// RLP decode
	var account StateAccount
	err := db.decodeAccountRLP(accountRLP, &account)
	if err != nil {
		return nil, fmt.Errorf("failed to decode account data: %v", err)
	}

	return &account, nil
}

// decodeAccountRLP decodes the RLP encoded account data into a StateAccount struct.
func (db *PrefixDB) decodeAccountRLP(accountRLP []byte, account *StateAccount) error {
	// Decode the RLP encoded account data
	kind, content, _, err := rlp.Split(accountRLP)
	if err != nil {
		return fmt.Errorf("failed to split RLP data: %v", err)
	}
	if kind != rlp.List {
		return fmt.Errorf("expected RLP list, got %v", kind)
	}

	remainingData := content
	var fields [][]byte

	for i := 0; i < 4; i++ {
		if len(remainingData) == 0 {
			return fmt.Errorf("not enough fields in RLP data, expected 4, got %d", i)
		}

		_, val, rest, err := rlp.Split(remainingData)
		if err != nil {
			return fmt.Errorf("failed to split field %d: %v", i, err)
		}

		remainingData = rest
		fields = append(fields, val)

		// fmt.Printf("字段 %d: 类型=%d, 长度=%d\n", i, kind, len(val))
	}

	// decode nonce
	if len(fields[0]) > 0 {
		nonce, err := decodeUint64(fields[0])
		if err != nil {
			return fmt.Errorf("解码nonce失败: %w", err)
		}
		account.Nonce = nonce
	}

	// decode balance
	if len(fields[1]) > 0 {
		account.Balance = new(big.Int)
		account.Balance.SetBytes(fields[1])
	} else {
		account.Balance = new(big.Int)
	}

	// decode storage root
	if len(fields[2]) != common.HashLength {
		return fmt.Errorf("invalid storage root length: %d, expected %d", len(fields[2]), common.HashLength)
	}
	copy(account.Root[:], fields[2])

	// decode code hash
	if len(fields[3]) > 0 {
		if len(fields[3]) != common.HashLength {
			return fmt.Errorf("invalid code hash length: %d, expected %d", len(fields[3]), common.HashLength)
		}
		account.CodeHash = make([]byte, len(fields[3]))
		copy(account.CodeHash, fields[3])
	}

	return nil
}

// decodeUint64 decodes a uint64 from RLP-encoded bytes.
func decodeUint64(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if len(b) == 1 && b[0] < 128 {
		return uint64(b[0]), nil
	}

	var n uint64
	for _, byte := range b {
		n = (n << 8) | uint64(byte)
	}
	return n, nil
}

// Convert key-value pair to byte array: <key size (short) + value size (short) + key + value>
func (db *PrefixDB) ConvertKV(key, value []byte) ([]byte, error) {

	if key == nil || value == nil {
		return nil, errors.New("key or value is nil")
	}
	keySize := int16(len(key))
	valueSize := int16(len(value))
	formattedData := make([]byte, 4+len(key)+len(value))

	// Use bitwise operations to set the first 4 bytes
	formattedData[0] = byte(keySize >> 8)
	formattedData[1] = byte(keySize)
	formattedData[2] = byte(valueSize >> 8)
	formattedData[3] = byte(valueSize)

	// Copy key and value directly
	copy(formattedData[4:], key)
	copy(formattedData[4+len(key):], value)

	return formattedData, nil
}

// getKeyType determines the type of key based on its prefix.
func (db *PrefixDB) getKeyType(key []byte) (KeyType, error) {
	if len(key) == 0 {
		return -1, errors.New("invalid key")
	}

	switch key[0] {
	case 'A':
		return TrieAccount, nil
	case 'O':
		return TrieStorage, nil
	default:
	}
	return -1, errors.New("unknown key type")
}

func (db *PrefixDB) normalizeStorageKey(rawKey []byte) ([]byte, error) {

	// Storage keys are expected to include the account-hash prefix: 'O' + 32-byte account hash.
	// Some workloads also emit a "root" storage key that is exactly this prefix (no slot suffix).
	if len(rawKey) < storageKeyTrimOffset {
		return nil, errors.New("invalid storage key")
	}
	if len(rawKey) == storageKeyTrimOffset {
		// Root storage key marker.
		// IMPORTANT: return a single byte 0x4f (same as 'O'), not the ASCII bytes for "4f".
		// This key is used within an account-scoped storage segment.
		return []byte{0x4f}, nil
	}
	return rawKey[storageKeyTrimOffset:], nil

}

func normalizeStoredStorageKey(key []byte) []byte {
	if len(key) > storageKeyTrimOffset && key[0] == 'O' {
		return key[storageKeyTrimOffset:]
	}
	return key
}

// GetParentAccountKey retrieves the parent account key from a given (code or storage)key.
func (db *PrefixDB) GetParentAccountKey(key []byte) []byte {
	if len(key) < 21 {
		return nil
	}
	accountHash := key[1:33]

	key, err := db.accountHashKeyPebble.Get(accountHash)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil // account not found
		}
		return nil
	}
	return key
}

func (db *PrefixDB) storeNode(key []byte, node *TrieNode) error {
	return db.prefixTree.Put(key, node.offset, node.storageFileID, node.storageOffset, node.storageSize)
}

func (db *PrefixDB) getNode(key []byte) (*TrieNode, error) {
	cacheKey := string(key)
	atomic.AddUint64(&db.nodeCacheLookups, 1)
	cacheHit := false
	if entry, ok := db.nodeCache.Get(cacheKey); ok {
		cacheHit = true
		atomic.AddUint64(&db.nodeCacheHits, 1)
		if entry.StorageInfo.storageFileID != 0 {
			atomic.AddUint64(&db.nodeCacheServed, 1)
			return &TrieNode{
				storageFileID: entry.StorageInfo.storageFileID,
				storageOffset: entry.StorageInfo.storageOffset,
				storageSize:   entry.StorageInfo.storageSize,
				offset:        entry.AccountOffset,
			}, nil
		}
	} else {
		atomic.AddUint64(&db.nodeCacheMisses, 1)
	}

	atomic.AddUint64(&db.nodeCacheToNodeFile, 1)
	if cacheHit {
		atomic.AddUint64(&db.nodeCacheHitFallbackToNodeFile, 1)
	} else {
		atomic.AddUint64(&db.nodeCacheMissToNodeFile, 1)
	}
	nodeInfo, found, err := db.prefixTree.Get(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	node := &TrieNode{
		storageFileID: nodeInfo.storageFileID,
		storageOffset: nodeInfo.storageOffset,
		storageSize:   nodeInfo.storageSize,
		offset:        nodeInfo.accountOffset,
	}
	// accountOffset==0 is a tombstone delete for account nodes.
	if node.offset == 0 && node.storageFileID == 0 {
		return nil, nil
	}
	db.nodeCache.StoreMetadata(cacheKey, node.offset, StorageInfo{
		storageFileID: node.storageFileID,
		storageOffset: node.storageOffset,
		storageSize:   node.storageSize,
	})

	// cacheKeyHex := hex.EncodeToString([]byte(cacheKey))
	// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", node.storageFileID) + ", offset:" + fmt.Sprintf("%d", node.storageOffset) + ", size:" + fmt.Sprintf("%d", node.storageSize))

	if nodeInfoGet, found := db.nodeCache.Get(cacheKey); found {
		if nodeInfoGet.StorageInfo.storageFileID != node.storageFileID {
			fmt.Printf("Metadata store mismatch for key %s: expected file ID %d, got %d\n", string(key), node.storageFileID, nodeInfoGet.StorageInfo.storageFileID)
		}
	} else {
		fmt.Printf("Failed to retrieve metadata for key %s after storing it\n", string(key))
	}
	return node, nil
}

func (db *PrefixDB) getAccountNode(key []byte) (*TrieNode, error) {
	cacheKey := string(key)
	atomic.AddUint64(&db.nodeCacheLookups, 1)
	cacheHit := false
	if entry, ok := db.nodeCache.Get(cacheKey); ok {
		cacheHit = true
		atomic.AddUint64(&db.nodeCacheHits, 1)
		if entry.AccountOffset != 0 || entry.StorageInfo.storageFileID != 0 || entry.Value != nil {
			atomic.AddUint64(&db.nodeCacheServed, 1)
			return &TrieNode{
				storageFileID: entry.StorageInfo.storageFileID,
				storageOffset: entry.StorageInfo.storageOffset,
				storageSize:   entry.StorageInfo.storageSize,
				offset:        entry.AccountOffset,
			}, nil
		}
	} else {
		atomic.AddUint64(&db.nodeCacheMisses, 1)
	}

	atomic.AddUint64(&db.nodeCacheToNodeFile, 1)
	if cacheHit {
		atomic.AddUint64(&db.nodeCacheHitFallbackToNodeFile, 1)
	} else {
		atomic.AddUint64(&db.nodeCacheMissToNodeFile, 1)
	}

	nodeInfo, found, err := db.prefixTree.Get(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	node := &TrieNode{
		storageFileID: nodeInfo.storageFileID,
		storageOffset: nodeInfo.storageOffset,
		storageSize:   nodeInfo.storageSize,
		offset:        nodeInfo.accountOffset,
	}
	// accountOffset==0 is a tombstone delete for account nodes.
	if node.offset == 0 && node.storageFileID == 0 {
		return nil, nil
	}
	return node, nil
}

func (db *PrefixDB) openOrCreateStorageFile() error {
	// find max FileID
	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		return fmt.Errorf("failed to read storage directory: %v", err)
	}

	var maxID uint32 = 0
	var maxSegmentID uint32 = 0
	for _, e := range entries {
		if e.IsDir() {
			var segID uint32
			if n, _ := fmt.Sscanf(e.Name(), segmentedDirNamePrefix+"%08d", &segID); n == 1 && segID > maxSegmentID {
				maxSegmentID = segID
			}
			continue
		}
		var id uint32
		n, _ := fmt.Sscanf(e.Name(), "storage_%08d.dat", &id)
		if n == 1 && id > maxID {
			maxID = id
		}
	}
	tryID := maxID
	if maxSegmentID > db.segmentDirSeq {
		db.segmentDirSeq = maxSegmentID
	}
	path := func(id uint32) string { return filepath.Join(db.storageDir, fmt.Sprintf("storage_%08d.dat", id)) }

	if tryID > 0 {
		p := path(tryID)
		file, err := os.OpenFile(p, os.O_RDWR, 0644)
		if err == nil {
			fi, _ := file.Stat()
			if fi.Size() < storageMaxFileSize && fi != nil {
				db.storageCurFile = file
				db.storageCurFileID = tryID
				db.storageCurSize = fi.Size()
				return nil
			}
			file.Close()
		}
	}

	newID := maxID + 1
	p := path(newID)
	file, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create storage file: %v", err)
	}
	db.storageCurFile = file
	db.storageCurFileID = newID
	db.storageCurSize = 0
	return nil
}

func (db *PrefixDB) ensureStorageCapacity(need int64) error {
	// if need > storageMaxFileSize {
	// 	return errors.New("need size lager than storageMaxFileSize")
	// }

	if db.storageCurFile == nil {
		return db.openOrCreateStorageFile()
	}
	if db.storageCurSize+need > storageMaxFileSize {
		db.storageCurFile.Close()
		db.storageCurFile = nil
		db.storageCurSize = 0
		db.storageCurFileID++
		p := filepath.Join(db.storageDir, fmt.Sprintf("storage_%08d.dat", db.storageCurFileID))
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		db.storageCurFile = f
	}
	return nil
}

// [kvCount u32] [keyLen u16][valLen u32][key][val]...
func (db *PrefixDB) serializeStorageSegment(kvs []kvPair) ([]byte, func(), int, error) {
	total := 4
	for _, v := range kvs {
		if len(v.key) > 0xFFFF {
			return nil, func() {}, 0, fmt.Errorf("key too large: %d", len(v.key))
		}
		total += 6 + len(v.key) + len(v.val)
	}

	buf := getDataBuffer(total)
	release := func() {
		putDataBuffer(buf)
	}
	offset := 0
	writeUint32BE(buf[offset:offset+4], uint32(len(kvs)))
	offset += 4
	var header [6]byte

	for _, v := range kvs {
		writeUint16BE(header[:2], uint16(len(v.key)))
		writeUint32BE(header[2:6], uint32(len(v.val)))
		copy(buf[offset:], header[:])
		offset += 6
		copy(buf[offset:], v.key)
		offset += len(v.key)
		copy(buf[offset:], v.val)
		offset += len(v.val)
	}
	return buf, release, total, nil
}

// appendStorageSegment appends a serialized storage segment to the storage file and returns its file ID, offset, and size.

func (db *PrefixDB) appendStorageSegment(kvs []kvPair) (fileID uint32, offset int64, size uint64, err error) {
	seg, release, _, err := db.serializeStorageSegment(kvs)
	if err != nil {
		return 0, 0, 0, err
	}
	defer release()
	need := int64(len(seg))
	if err := db.ensureStorageCapacity(need); err != nil {
		return 0, 0, 0, err
	}
	offset = db.storageCurSize
	if _, err := db.storageCurFile.WriteAt(seg, offset); err != nil {
		return 0, 0, 0, err
	}
	db.addDiskWrite(diskIOUsageStorageCommonLogs, len(seg))
	db.storageCurSize += need
	return db.storageCurFileID, offset, uint64(need), nil
}

func (db *PrefixDB) persistStorageEntries(kvs []kvPair, existingFileID uint32, existingOffset int64, existingSize uint64) (uint32, int64, uint64, error) {
	if len(kvs) == 0 {
		return 0, 0, 0, nil
	}

	if isSegmentedStorage(existingFileID) {
		kvs = dedupSortedKVPairs(kvs)
		return db.updateSegmentedStorage(existingFileID, kvs)
	}
	merged := kvs
	var existingBacking *bufferLease
	if existingFileID != 0 && existingSize > 0 {
		existingEntries, backing, err := db.readStorageSegmentPairs(existingFileID, existingOffset, existingSize)
		if err != nil {
			return 0, 0, 0, err
		}
		db.addCommitOldKVReadStats(len(existingEntries), existingSize)
		if backing != nil {
			existingBacking = backing
		}
		if len(existingEntries) > 0 {
			merged = mergeAndDedupPairs(existingEntries, kvs)
		}
	}
	if existingBacking != nil {
		defer existingBacking.Release()
	}
	//merged = filterDeletedPairs(merged)
	if len(merged) == 0 {
		return 0, 0, 0, nil
	}
	size := estimateSegmentSize(merged)
	if size <= db.storageChunkSize {
		return db.appendStorageSegment(merged)
	}
	return db.appendSegmentedStorage(merged)
}

func estimateSegmentSize(kvs []kvPair) int {
	total := 4
	for _, kv := range kvs {
		total += 6 + len(kv.key) + len(kv.val)
	}
	return total
}

func (db *PrefixDB) appendSegmentedStorage(kvs []kvPair) (uint32, int64, uint64, error) {
	db.segmentedMu.Lock()
	defer db.segmentedMu.Unlock()
	folderID := db.nextSegmentedDirID()
	folderPath := db.segmentedFolderPath(folderID)
	if err := os.MkdirAll(folderPath, 0755); err != nil {
		return 0, 0, 0, err
	}
	success := false
	defer func() {
		if !success {
			_ = os.RemoveAll(folderPath)
		}
	}()

	chunkMetas := make([]segmentChunkMeta, 0)
	chunk := make([]kvPair, 0)
	chunkSize := 4
	chunkIdx := 0
	flushChunk := func() error {
		if len(chunk) == 0 {
			return nil
		}
		seg, release, _, err := db.serializeStorageSegment(chunk)
		if err != nil {
			return err
		}
		defer release()
		name := fmt.Sprintf("chunk_%04d.dat", chunkIdx)
		fullPath := filepath.Join(folderPath, name)
		if err := db.writeFileWithStats(fullPath, seg, 0644, diskIOUsageStorageSeparatedLogs); err != nil {
			return err
		}
		meta := segmentChunkMeta{
			FileName:  name,
			KeyStart:  cloneBytes(chunk[0].key),
			KeyEnd:    cloneBytes(chunk[len(chunk)-1].key),
			KVCount:   uint32(len(chunk)),
			ChunkSize: uint64(len(seg)),
		}
		chunkMetas = append(chunkMetas, meta)
		chunk = make([]kvPair, 0)
		chunkSize = 4
		chunkIdx++
		return nil
	}

	for _, kv := range kvs {
		sz := 6 + len(kv.key) + len(kv.val)
		if chunkSize+sz > db.storageChunkSize && len(chunk) > 0 {
			if err := flushChunk(); err != nil {
				return 0, 0, 0, err
			}
		}
		chunk = append(chunk, kv)
		chunkSize += sz
	}
	if err := flushChunk(); err != nil {
		return 0, 0, 0, err
	}
	if len(chunkMetas) == 0 {
		return 0, 0, 0, errors.New("failed to build segmented storage chunks")
	}

	if err := db.writeSegmentIndex(folderPath, chunkMetas); err != nil {
		return 0, 0, 0, err
	}
	db.invalidateSegmentIndexCache(folderID)

	success = true
	return segmentedStorageFlag | folderID, 0, uint64(len(chunkMetas)), nil
}

func dedupSortedKVPairs(kvs []kvPair) []kvPair {
	if len(kvs) < 2 {
		return kvs
	}
	out := kvs[:0]
	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && bytes.Equal(kvs[j].key, kvs[i].key) {
			j++
		}
		out = append(out, kvs[j-1])
		i = j
	}
	return out
}

func (db *PrefixDB) updateSegmentedStorage(existingFileID uint32, kvs []kvPair) (uint32, int64, uint64, error) {
	db.segmentedMu.Lock()
	defer db.segmentedMu.Unlock()
	return db.updateSegmentedStorageWithLockHeld(existingFileID, kvs)
}

func (db *PrefixDB) updateSegmentedStorageWithLockHeld(existingFileID uint32, kvs []kvPair) (uint32, int64, uint64, error) {
	folderID := existingFileID & ^segmentedStorageFlag
	metas, err := db.readSegmentIndexNoCache(folderID)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(metas) == 0 {
		return 0, 0, 0, fmt.Errorf("segment index missing for folder %d", folderID)
	}
	allocator := newChunkFileAllocator(metas)
	buckets := partitionEntriesByChunks(metas, kvs)
	folderPath := db.segmentedFolderPath(folderID)
	updated := make([]segmentChunkMeta, 0, len(metas)+len(kvs)/64+1)
	for idx, meta := range metas {
		additions := buckets[idx]
		if len(additions) == 0 {
			updated = append(updated, meta)
			continue
		}
		chunkMetas, err := db.mutateSegmentChunk(folderID, folderPath, meta, additions, allocator)
		if err != nil {
			return 0, 0, 0, err
		}
		if len(chunkMetas) == 0 {
			continue
		}
		updated = append(updated, chunkMetas...)
	}
	if err := db.writeSegmentIndex(folderPath, updated); err != nil {
		return 0, 0, 0, err
	}
	db.invalidateSegmentIndexCache(folderID)
	return existingFileID, 0, uint64(len(updated)), nil
}

// partitionEntriesByChunks takes advantage of kvs being sorted lexicographically to
// walk the chunk metadata once, yielding O(len(metas)+len(kvs)) complexity.
func partitionEntriesByChunks(metas []segmentChunkMeta, kvs []kvPair) [][]kvPair {
	buckets := make([][]kvPair, len(metas))
	if len(metas) == 0 || len(kvs) == 0 {
		return buckets
	}
	idx := 0
	for _, kv := range kvs {
		idx = findChunkIndexForKey(metas, kv.key, idx)
		if idx < 0 {
			continue
		}
		buckets[idx] = append(buckets[idx], kv)
	}
	return buckets
}

func findChunkIndexForKey(metas []segmentChunkMeta, key []byte, start int) int {
	if len(metas) == 0 {
		return -1
	}
	if start < 0 {
		start = 0
	} else if start >= len(metas) {
		start = len(metas) - 1
	}
	for i := start; i < len(metas); i++ {
		meta := metas[i]
		startOK := len(meta.KeyStart) == 0 || bytes.Compare(key, meta.KeyStart) >= 0
		endOK := len(meta.KeyEnd) == 0 || bytes.Compare(key, meta.KeyEnd) <= 0
		if startOK && endOK {
			return i
		}
		if len(meta.KeyEnd) > 0 && bytes.Compare(key, meta.KeyEnd) < 0 {
			return i
		}
	}
	return len(metas) - 1
}

type chunkFileAllocator struct {
	next int
}

func newChunkFileAllocator(metas []segmentChunkMeta) *chunkFileAllocator {
	maxIdx := -1
	for _, meta := range metas {
		if idx := parseChunkOrdinal(meta.FileName); idx > maxIdx {
			maxIdx = idx
		}
	}
	return &chunkFileAllocator{next: maxIdx + 1}
}

func (a *chunkFileAllocator) nextName() string {
	name := chunkFileNameForOrdinal(uint32(a.next))
	a.next++
	return name
}

func chunkFileNameForOrdinal(ordinal uint32) string {
	return fmt.Sprintf("chunk_%04d.dat", ordinal)
}

func parseChunkOrdinal(name string) int {
	const prefix = "chunk_"
	const suffix = ".dat"
	if len(name) <= len(prefix)+len(suffix) {
		return -1
	}
	if name[:len(prefix)] != prefix {
		return -1
	}
	if name[len(name)-len(suffix):] != suffix {
		return -1
	}
	num := name[len(prefix) : len(name)-len(suffix)]
	if len(num) == 0 {
		return -1
	}
	idx := 0
	for i := 0; i < len(num); i++ {
		c := num[i]
		if c < '0' || c > '9' {
			return -1
		}
		idx = idx*10 + int(c-'0')
	}
	return idx
}

func (db *PrefixDB) mutateSegmentChunk(folderID uint32, folderPath string, meta segmentChunkMeta, additions []kvPair, allocator *chunkFileAllocator) ([]segmentChunkMeta, error) {
	if len(additions) == 0 {
		return []segmentChunkMeta{meta}, nil
	}
	chunkPath := filepath.Join(folderPath, meta.FileName)
	currentSize := int64(meta.ChunkSize)
	appendBytes := payloadSize(additions)
	if appendBytes == 0 {
		return []segmentChunkMeta{meta}, nil
	}
	metaCopy := meta
	if err := db.appendChunkFile(chunkPath, metaCopy.KVCount, additions, currentSize); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// The index references a chunk file that no longer exists.
			// We cannot append safely, so recreate the chunk from the current additions.
			chunkMetas, _, rewriteErr := db.rewriteChunkWithDedup(folderID, folderPath, metaCopy, additions, allocator, []kvPair{}, nil)
			if rewriteErr != nil {
				return nil, rewriteErr
			}
			fmt.Printf("prefixdb: recreated missing chunk %s in folder %d during write\n", metaCopy.FileName, folderID)
			return chunkMetas, nil
		}
		return nil, err
	}
	metaCopy.KVCount += uint32(len(additions))
	metaCopy.ChunkSize += uint64(appendBytes)
	adjustMetaRange(&metaCopy, additions)
	return []segmentChunkMeta{metaCopy}, nil
}

func (db *PrefixDB) appendChunkFile(path string, currentCount uint32, additions []kvPair, currentSize int64) error {
	if len(additions) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	var header [4]byte
	writeUint32BE(header[:], currentCount+uint32(len(additions)))
	if _, err := f.WriteAt(header[:], 0); err != nil {
		return err
	}
	db.addDiskWrite(diskIOUsageStorageSeparatedLogs, len(header))
	seg, release, _, err := db.serializeStorageSegment(additions)
	if err != nil {
		return err
	}
	defer release()
	data := seg[4:]
	if _, err := f.WriteAt(data, currentSize); err != nil {
		return err
	}
	db.addDiskWrite(diskIOUsageStorageSeparatedLogs, len(data))
	return nil
}

func adjustMetaRange(meta *segmentChunkMeta, additions []kvPair) {
	if len(additions) == 0 {
		return
	}
	first := additions[0].key
	if len(meta.KeyStart) == 0 || bytes.Compare(first, meta.KeyStart) < 0 {
		meta.KeyStart = cloneBytes(first)
	}
	last := additions[len(additions)-1].key
	if len(meta.KeyEnd) == 0 || bytes.Compare(last, meta.KeyEnd) > 0 {
		meta.KeyEnd = cloneBytes(last)
	}
}

func (db *PrefixDB) rewriteChunkWithDedup(folderID uint32, folderPath string, meta segmentChunkMeta, additions []kvPair, allocator *chunkFileAllocator, existing []kvPair, backing *bufferLease) ([]segmentChunkMeta, bool, error) {
	var err error
	if existing == nil {
		existing, backing, err = db.readSegmentChunkFile(folderID, meta.FileName)
		if err != nil {
			return nil, false, err
		}
		db.addCommitOldKVReadStats(len(existing), meta.ChunkSize)
	}
	if backing != nil {
		defer backing.Release()
	}
	// Chunk files are append-only (see appendChunkFile) so their on-disk kv order is not
	// guaranteed to be sorted. mergeAndDedupPairs assumes sorted inputs; normalize first
	// to avoid dropping keys during GC rewrites.
	if len(existing) > 1 {
		existing = db.maybeNormalizeChunkEntries(existing, &meta)
	}
	merged := mergeAndDedupPairs(existing, additions)
	if len(merged) == 0 {
		return nil, true, nil
	}
	chunks := splitEntriesBySize(merged, db.segmentedChunkTargetSize())
	result := make([]segmentChunkMeta, 0, len(chunks))
	var chunkSize int
	for idx, chunk := range chunks {
		name := meta.FileName
		if idx > 0 {
			name = allocator.nextName()
		}
		if chunkSize, err = db.writeChunkFile(folderPath, name, chunk); err != nil {
			return nil, false, err
		}
		result = append(result, segmentChunkMeta{
			FileName:  name,
			KeyStart:  cloneBytes(chunk[0].key),
			KeyEnd:    cloneBytes(chunk[len(chunk)-1].key),
			KVCount:   uint32(len(chunk)),
			ChunkSize: uint64(chunkSize),
		})
	}
	return result, false, nil
}

func (db *PrefixDB) repairMissingChunkFile(folderID uint32, fileName string) error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	db.segmentedMu.Lock()
	defer db.segmentedMu.Unlock()
	metas, err := db.readSegmentIndexNoCache(folderID)
	if err != nil {
		return err
	}
	filtered := make([]segmentChunkMeta, 0, len(metas))
	removed := false
	for _, meta := range metas {
		if meta.FileName == fileName {
			removed = true
			continue
		}
		filtered = append(filtered, meta)
	}
	if !removed {
		return fmt.Errorf("missing chunk %s not referenced in folder %d", fileName, folderID)
	}
	folderPath := db.segmentedFolderPath(folderID)
	if err := db.writeSegmentIndex(folderPath, filtered); err != nil {
		return err
	}
	db.invalidateSegmentIndexCache(folderID)
	fmt.Printf("prefixdb: repaired missing chunk %s in folder %d\n", fileName, folderID)
	return nil
}

func mergeAndDedupPairs(existing, additions []kvPair) []kvPair {
	merged := make([]kvPair, 0, len(existing)+len(additions))
	i, j := 0, 0
	for i < len(existing) && j < len(additions) {
		cmp := bytes.Compare(existing[i].key, additions[j].key)
		switch {
		case cmp < 0:
			merged = append(merged, existing[i])
			i++
		case cmp > 0:
			merged = append(merged, additions[j])
			j++
		default:
			merged = append(merged, additions[j])
			i++
			j++
		}
	}
	if i < len(existing) {
		merged = append(merged, existing[i:]...)
	}
	if j < len(additions) {
		merged = append(merged, additions[j:]...)
	}
	return merged
}

func splitEntriesBySize(entries []kvPair, limit int) [][]kvPair {
	if len(entries) == 0 {
		return nil
	}
	chunks := make([][]kvPair, 0, len(entries)/64+1)
	start := 0
	var size int = 4
	for i := 0; i < len(entries); i++ {
		entrySize := 6 + len(entries[i].key) + len(entries[i].val)
		if size+entrySize > limit && i > start {
			chunk := entries[start:i:i]
			chunks = append(chunks, chunk)
			start = i
			size = 4
		}
		size += entrySize
	}
	if start < len(entries) {
		chunk := entries[start:len(entries):len(entries)]
		chunks = append(chunks, chunk)
	}
	return chunks
}

func payloadSize(entries []kvPair) int64 {
	var total int64
	for _, kv := range entries {
		total += int64(6 + len(kv.key) + len(kv.val))
	}
	return total
}

func (db *PrefixDB) segmentedChunkTargetSize() int {
	if db != nil && db.storageChunkSize > 0 {
		return db.storageChunkSize
	}
	if db != nil && db.segmentedChunkHardLimit > 0 {
		return db.segmentedChunkHardLimit
	}
	return 16 * 1024
}

func (db *PrefixDB) segmentedChunkTriggerSize() int {
	if db != nil && db.segmentedChunkHardLimit > 0 {
		return db.segmentedChunkHardLimit
	}
	return db.segmentedChunkTargetSize()
}

func sanitizeStorageGCThreshold(threshold float64) float64 {
	if threshold <= 0 {
		return defaultStorageGCThreshold
	}
	return threshold
}

func computeSegmentedChunkHardLimit(storageChunkFileSize int, threshold float64) int {
	if storageChunkFileSize <= 0 {
		return 0
	}
	return int(math.Ceil(float64(storageChunkFileSize) * sanitizeStorageGCThreshold(threshold)))
}

func storageGCQueueCapacity(workers int) int {
	return sanitizePrefixTreeGCWorkerCount(workers) * storageGCQueueMultiplier
}

func (db *PrefixDB) acquireSharedGCWorker() func() {
	if db == nil || db.gcWorkerLimiter == nil {
		return func() {}
	}
	db.gcWorkerLimiter <- struct{}{}
	return func() {
		<-db.gcWorkerLimiter
	}
}

func (db *PrefixDB) writeChunkFile(folderPath, fileName string, entries []kvPair) (int, error) {
	return db.writeChunkFileWithUsage(folderPath, fileName, entries, diskIOUsageStorageSeparatedLogs)
}

func (db *PrefixDB) writeChunkFileWithUsage(folderPath, fileName string, entries []kvPair, usage diskIOUsage) (int, error) {
	seg, release, chunkSize, err := db.serializeStorageSegment(entries)
	if err != nil {
		return 0, err
	}
	defer release()
	fullPath := filepath.Join(folderPath, fileName)
	// Write atomically to avoid readers observing a partially rewritten chunk
	// (GC rewrites truncate and rewrite existing files).
	tmpPath := fullPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(seg); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return 0, err
	}
	db.addDiskWrite(usage, len(seg))
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return 0, err
	}
	if err := os.Rename(tmpPath, fullPath); err != nil {
		_ = os.Remove(tmpPath)
		return 0, err
	}
	return chunkSize, nil
}

func (db *PrefixDB) nextSegmentedDirID() uint32 {
	if db.segmentDirSeq == 0 {
		// ensure we never collide with existing ids by scanning once if needed
		entries, err := os.ReadDir(db.storageDir)
		if err == nil {
			var maxID uint32
			for _, entry := range entries {
				if entry.IsDir() {
					var id uint32
					if n, _ := fmt.Sscanf(entry.Name(), segmentedDirNamePrefix+"%08d", &id); n == 1 {
						if id > maxID {
							maxID = id
						}
					}
				}
			}
			db.segmentDirSeq = maxID
		}
	}
	db.segmentDirSeq++
	return db.segmentDirSeq
}

func (db *PrefixDB) segmentedFolderPath(id uint32) string {
	return filepath.Join(db.storageDir, fmt.Sprintf("%s%08d", segmentedDirNamePrefix, id))
}

func (db *PrefixDB) lockSegmentIndexFolder(folderID uint32) func() {
	_, unlock := db.lockSegmentIndexFolderEntry(folderID)
	return unlock
}

func (db *PrefixDB) lockSegmentIndexFolderEntry(folderID uint32) (*segmentIndexFolderLock, func()) {
	db.segmentIndexFolderLocksMu.Lock()
	if db.segmentIndexFolderLocks == nil {
		db.segmentIndexFolderLocks = make(map[uint32]*segmentIndexFolderLock)
	}
	entry := db.segmentIndexFolderLocks[folderID]
	if entry == nil {
		entry = &segmentIndexFolderLock{}
		db.segmentIndexFolderLocks[folderID] = entry
	}
	entry.refs++
	db.segmentIndexFolderLocksMu.Unlock()

	entry.mu.Lock()
	return entry, func() {
		entry.mu.Unlock()
		db.segmentIndexFolderLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(db.segmentIndexFolderLocks, folderID)
		}
		db.segmentIndexFolderLocksMu.Unlock()
	}
}

func (db *PrefixDB) segmentIndexGenerationLocked(folderID uint32) uint64 {
	entry, unlock := db.lockSegmentIndexFolderEntry(folderID)
	gen := atomic.LoadUint64(&entry.gen)
	unlock()
	return gen
}

func (db *PrefixDB) bumpSegmentIndexGenerationLocked(entry *segmentIndexFolderLock) {
	if entry == nil {
		return
	}
	atomic.AddUint64(&entry.gen, 1)
}

func (db *PrefixDB) readSegmentIndexNoCacheWithGen(folderID uint32) ([]segmentChunkMeta, uint64, error) {
	entry, unlock := db.lockSegmentIndexFolderEntry(folderID)
	defer unlock()
	gen := atomic.LoadUint64(&entry.gen)
	metas, err := db.readSegmentIndexLockedInternal(folderID, false)
	return metas, gen, err
}

func segmentFolderIDFromPath(folderPath string) uint32 {
	base := filepath.Base(folderPath)
	var folderID uint32
	if _, err := fmt.Sscanf(base, segmentedDirNamePrefix+"%08d", &folderID); err != nil {
		return 0
	}
	return folderID
}

func level2IndexFilePath(folderPath string, metaID uint32) string {
	return filepath.Join(folderPath, fmt.Sprintf(segmentIndexLevel2Pattern, metaID))
}

func segmentChunkMetaCanUseCompactEncoding(meta segmentChunkMeta) bool {
	return parseChunkOrdinal(meta.FileName) >= 0 && meta.ChunkSize <= math.MaxUint32
}

func canUseCompactSegmentEncoding(metas []segmentChunkMeta) bool {
	for _, meta := range metas {
		if !segmentChunkMetaCanUseCompactEncoding(meta) {
			return false
		}
	}
	return true
}

func estimateSegmentEntrySize(meta segmentChunkMeta) int {
	if segmentChunkMetaCanUseCompactEncoding(meta) {
		return 4 + 2 + len(meta.KeyStart) + 2 + len(meta.KeyEnd) + 4 + 4
	}
	return 2 + len(meta.FileName) + 2 + len(meta.KeyStart) + 2 + len(meta.KeyEnd) + 4 + 8
}

func estimateSegmentIndexSize(metas []segmentChunkMeta) int {
	total := 4
	if canUseCompactSegmentEncoding(metas) {
		total = 12
	}
	for _, meta := range metas {
		total += estimateSegmentEntrySize(meta)
	}
	return total
}

func encodeSegmentChunkMetas(metas []segmentChunkMeta) ([]byte, error) {
	buf := make([]byte, 0, estimateSegmentIndexSize(metas))
	var tmp32 [4]byte
	if !canUseCompactSegmentEncoding(metas) {
		return nil, fmt.Errorf("segment index requires compact encoding compatible metas")
	}
	writeUint32BE(tmp32[:], segmentIndexFlatMagic)
	buf = append(buf, tmp32[:]...)
	var tmp16 [2]byte
	writeUint16BE(tmp16[:], segmentIndexFlatVersion)
	buf = append(buf, tmp16[:]...)
	buf = append(buf, 0, 0)
	writeUint32BE(tmp32[:], uint32(len(metas)))
	buf = append(buf, tmp32[:]...)
	for _, meta := range metas {
		ordinal := parseChunkOrdinal(meta.FileName)
		writeUint32BE(tmp32[:], uint32(ordinal))
		buf = append(buf, tmp32[:]...)
		var err error
		if buf, err = appendVarBytes(buf, meta.KeyStart); err != nil {
			return nil, err
		}
		if buf, err = appendVarBytes(buf, meta.KeyEnd); err != nil {
			return nil, err
		}
		writeUint32BE(tmp32[:], meta.KVCount)
		buf = append(buf, tmp32[:]...)
		writeUint32BE(tmp32[:], uint32(meta.ChunkSize))
		buf = append(buf, tmp32[:]...)
	}
	return buf, nil
}

func writeFileIfChanged(db *PrefixDB, path string, data []byte) error {
	fi, err := os.Stat(path)
	if err == nil {
		// Fast path: if sizes differ, content differs.
		if fi.Size() == int64(len(data)) {
			same, cmpErr := fileContentEqualsBytes(db, path, data)
			if cmpErr == nil && same {
				return nil
			}
		}
	} else if !os.IsNotExist(err) {
		// Preserve prior behavior: on read/stat errors, fall back to writing.
	}
	return writeFileAtomic(db, path, data)
}

func fileContentEqualsBytes(db *PrefixDB, path string, data []byte) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Compare in fixed-size chunks to avoid allocating a full copy of the file.
	var buf [32 * 1024]byte
	offset := 0
	for offset < len(data) {
		need := len(data) - offset
		if need > len(buf) {
			need = len(buf)
		}
		if _, err := io.ReadFull(f, buf[:need]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return false, nil
			}
			return false, err
		}
		if db != nil {
			db.addDiskRead(diskIOUsageStorageSegmentIndex, need)
		}
		if !bytes.Equal(buf[:need], data[offset:offset+need]) {
			return false, nil
		}
		offset += need
	}

	// Ensure the file doesn't contain extra bytes (handles races between Stat/Open).
	if n, err := f.Read(buf[:1]); n > 0 {
		if db != nil {
			db.addDiskRead(diskIOUsageStorageSegmentIndex, n)
		}
		return false, nil
	} else if err == io.EOF {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func writeFileAtomic(db *PrefixDB, path string, data []byte) error {
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	if db != nil {
		db.addDiskWrite(diskIOUsageStorageSegmentIndex, len(data))
	}
	return os.Rename(tmpPath, path)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func segmentChunkMetaEqual(a, b segmentChunkMeta) bool {
	if a.FileName != b.FileName || a.KVCount != b.KVCount || a.ChunkSize != b.ChunkSize {
		return false
	}
	if !bytes.Equal(a.KeyStart, b.KeyStart) {
		return false
	}
	return bytes.Equal(a.KeyEnd, b.KeyEnd)
}

func segmentChunkMetasEqual(a, b []segmentChunkMeta) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !segmentChunkMetaEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func removeLevel2IndexFilesByIDs(folderPath string, ids []uint32) error {
	for _, id := range ids {
		full := level2IndexFilePath(folderPath, id)
		if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func removeStaleLevel2IndexFiles(folderPath string, oldEntries []segmentIndexL1Entry, keep map[uint32]struct{}) error {
	if len(oldEntries) == 0 {
		return nil
	}
	toDelete := make([]uint32, 0, len(oldEntries))
	for _, entry := range oldEntries {
		if keep != nil {
			if _, ok := keep[entry.MetaID]; ok {
				continue
			}
		}
		toDelete = append(toDelete, entry.MetaID)
	}
	return removeLevel2IndexFilesByIDs(folderPath, toDelete)
}

func removeLevel2IndexFiles(folderPath string, keep map[uint32]struct{}) error {
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var metaID uint32
		if _, err := fmt.Sscanf(entry.Name(), "index.meta.l2.%08d", &metaID); err != nil {
			continue
		}
		if keep != nil {
			if _, ok := keep[metaID]; ok {
				continue
			}
		}
		full := filepath.Join(folderPath, entry.Name())
		if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func splitSegmentMetas(metas []segmentChunkMeta) [][]segmentChunkMeta {
	if len(metas) == 0 {
		return nil
	}
	groups := make([][]segmentChunkMeta, 0, len(metas)/16+1)
	groupStart := 0
	groupSize := 4
	for i, meta := range metas {
		entrySize := estimateSegmentEntrySize(meta)
		if groupSize+entrySize > segmentIndexLevel2TargetSize && i > groupStart {
			groups = append(groups, metas[groupStart:i])
			groupStart = i
			groupSize = 4
		}
		groupSize += entrySize
		if groupSize >= segmentIndexLevel2MaxSize {
			groups = append(groups, metas[groupStart:i+1])
			groupStart = i + 1
			groupSize = 4
		}
	}
	if groupStart < len(metas) {
		groups = append(groups, metas[groupStart:])
	}
	return groups
}

func selectSegmentL1Entry(entries []segmentIndexL1Entry, key []byte) *segmentIndexL1Entry {
	if len(entries) == 0 {
		return nil
	}
	if len(key) == 0 {
		return &entries[0]
	}
	idx := sort.Search(len(entries), func(i int) bool {
		end := entries[i].KeyEnd
		if len(end) == 0 {
			return true
		}
		return bytes.Compare(key, end) <= 0
	})
	if idx == len(entries) {
		idx = len(entries) - 1
	}
	entry := &entries[idx]
	if len(entry.KeyStart) == 0 || bytes.Compare(key, entry.KeyStart) >= 0 {
		return entry
	}
	for i := idx - 1; i >= 0; i-- {
		startOK := len(entries[i].KeyStart) == 0 || bytes.Compare(key, entries[i].KeyStart) >= 0
		endOK := len(entries[i].KeyEnd) == 0 || bytes.Compare(key, entries[i].KeyEnd) <= 0
		if startOK && endOK {
			return &entries[i]
		}
	}
	for i := idx + 1; i < len(entries); i++ {
		startOK := len(entries[i].KeyStart) == 0 || bytes.Compare(key, entries[i].KeyStart) >= 0
		endOK := len(entries[i].KeyEnd) == 0 || bytes.Compare(key, entries[i].KeyEnd) <= 0
		if startOK && endOK {
			return &entries[i]
		}
		if len(entries[i].KeyStart) > 0 && bytes.Compare(key, entries[i].KeyStart) < 0 {
			break
		}
	}
	return &entries[len(entries)-1]
}

func decodeSegmentIndexBuffer(data []byte, metas *[]segmentChunkMeta, arena *[]byte, appendExisting bool, chunkDir string) error {
	if len(data) < 4 {
		return fmt.Errorf("invalid segment index payload")
	}
	cursor := 0
	if binary.BigEndian.Uint32(data[:4]) == segmentIndexFlatMagic {
		if len(data) < 12 {
			return fmt.Errorf("corrupted compact segment index header")
		}
		version := binary.BigEndian.Uint16(data[4:6])
		if version != segmentIndexFlatVersion {
			return fmt.Errorf("unsupported flat index version %d", version)
		}
		cursor = 12
	} else {
		return fmt.Errorf("unsupported legacy segment index format")
	}
	count := int(binary.BigEndian.Uint32(data[cursor-4 : cursor]))
	if count == 0 {
		if !appendExisting {
			*metas = (*metas)[:0]
			*arena = (*arena)[:0]
		}
		return nil
	}
	if !appendExisting {
		if cap(*metas) < count {
			*metas = make([]segmentChunkMeta, 0, count)
		} else {
			*metas = (*metas)[:0]
		}
		*arena = (*arena)[:0]
	}
	needed := len(*metas) + count
	if cap(*metas) < needed {
		newCap := needed
		if newCap < 2*cap(*metas) {
			newCap = 2 * cap(*metas)
		}
		buf := make([]segmentChunkMeta, len(*metas), newCap)
		copy(buf, *metas)
		*metas = buf
	}
	for i := 0; i < count; i++ {
		if cursor+4 > len(data) {
			return io.ErrUnexpectedEOF
		}
		fileName := chunkFileNameForOrdinal(readUint32BE(data[cursor : cursor+4]))
		cursor += 4
		start, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return err
		}
		cursor += n
		end, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return err
		}
		cursor += n
		if cursor+4 > len(data) {
			return io.ErrUnexpectedEOF
		}
		kvCount := readUint32BE(data[cursor : cursor+4])
		cursor += 4
		var chunkSize uint64
		if cursor+4 <= len(data) {
			chunkSize = uint64(readUint32BE(data[cursor : cursor+4]))
			cursor += 4
		} else if chunkDir != "" {
			chunkPath := filepath.Join(chunkDir, fileName)
			info, err := os.Stat(chunkPath)
			if err != nil {
				return err
			}
			chunkSize = uint64(info.Size())
		} else {
			return io.ErrUnexpectedEOF
		}
		meta := segmentChunkMeta{
			FileName:  fileName,
			KVCount:   kvCount,
			ChunkSize: chunkSize,
		}
		// Avoid copying into the arena: KeyStart/KeyEnd can safely reference the
		// index buffer. This eliminates a lot of growslice/memmove work when decoding
		// large indexes.
		meta.KeyStart = start
		meta.KeyEnd = end
		*metas = append(*metas, meta)
	}
	return nil
}

func decodeLegacySegmentIndexBuffer(data []byte, metas *[]segmentChunkMeta, arena *[]byte, appendExisting bool, chunkDir string) error {
	if len(data) < 4 {
		return fmt.Errorf("invalid legacy segment index payload")
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if count == 0 {
		if !appendExisting {
			*metas = (*metas)[:0]
			*arena = (*arena)[:0]
		}
		return nil
	}
	if !appendExisting {
		if cap(*metas) < count {
			*metas = make([]segmentChunkMeta, 0, count)
		} else {
			*metas = (*metas)[:0]
		}
		*arena = (*arena)[:0]
	}
	needed := len(*metas) + count
	if cap(*metas) < needed {
		newCap := needed
		if newCap < 2*cap(*metas) {
			newCap = 2 * cap(*metas)
		}
		buf := make([]segmentChunkMeta, len(*metas), newCap)
		copy(buf, *metas)
		*metas = buf
	}
	cursor := 4
	for i := 0; i < count; i++ {
		nameBytes, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return err
		}
		fileName := bytesToString(nameBytes)
		cursor += n
		start, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return err
		}
		cursor += n
		end, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return err
		}
		cursor += n
		if cursor+4 > len(data) {
			return io.ErrUnexpectedEOF
		}
		kvCount := readUint32BE(data[cursor : cursor+4])
		cursor += 4
		var chunkSize uint64
		if cursor+8 <= len(data) {
			chunkSize = readUint64BE(data[cursor : cursor+8])
			cursor += 8
		} else if chunkDir != "" {
			chunkPath := filepath.Join(chunkDir, fileName)
			info, err := os.Stat(chunkPath)
			if err != nil {
				return err
			}
			chunkSize = uint64(info.Size())
		} else {
			return io.ErrUnexpectedEOF
		}
		meta := segmentChunkMeta{
			FileName:  fileName,
			KVCount:   kvCount,
			ChunkSize: chunkSize,
		}
		meta.KeyStart = start
		meta.KeyEnd = end
		*metas = append(*metas, meta)
	}
	return nil
}

func isCompactSegmentIndexBuffer(data []byte) bool {
	return len(data) >= 4 && binary.BigEndian.Uint32(data[:4]) == segmentIndexFlatMagic
}

func isMultiLevelSegmentIndexBuffer(data []byte) bool {
	return len(data) >= 4 && binary.BigEndian.Uint32(data[:4]) == segmentIndexMultiLevelMagic
}

func (db *PrefixDB) decodeSegmentIndexBufferForMigration(data []byte, metas *[]segmentChunkMeta, arena *[]byte, appendExisting bool, chunkDir string) (bool, error) {
	if isCompactSegmentIndexBuffer(data) {
		return false, decodeSegmentIndexBuffer(data, metas, arena, appendExisting, chunkDir)
	}
	if isMultiLevelSegmentIndexBuffer(data) {
		return false, fmt.Errorf("unexpected multi-level segment index payload during migration")
	}
	return true, decodeLegacySegmentIndexBuffer(data, metas, arena, appendExisting, chunkDir)
}

func (db *PrefixDB) loadSegmentIndexLayout(folderPath string) (segmentIndexLayout, error) {
	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	data, err := db.readFileWithStats(indexPath, diskIOUsageStorageSegmentIndex)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return segmentIndexLayout{mode: indexLayoutFlat, nextMetaID: 1}, nil
		}
		return segmentIndexLayout{}, err
	}
	if len(data) < 4 {
		return segmentIndexLayout{}, fmt.Errorf("invalid segment index: %s", indexPath)
	}
	var layout segmentIndexLayout
	if binary.BigEndian.Uint32(data[:4]) != segmentIndexMultiLevelMagic {
		layout = segmentIndexLayout{mode: indexLayoutFlat, nextMetaID: 1, flatData: data}
	} else {
		layout, err = parseMultiLevelLayout(data)
		if err != nil {
			return segmentIndexLayout{}, err
		}
	}
	return layout, nil
}

func parseMultiLevelLayout(data []byte) (segmentIndexLayout, error) {
	if len(data) < 16 {
		return segmentIndexLayout{}, fmt.Errorf("corrupted multi-level index header")
	}
	layout := segmentIndexLayout{mode: indexLayoutMultiLevel}
	cursor := 4
	version := binary.BigEndian.Uint16(data[cursor : cursor+2])
	cursor += 2
	if version != segmentIndexFormatVersion {
		return segmentIndexLayout{}, fmt.Errorf("unsupported index meta version %d", version)
	}
	cursor += 2 // reserved
	layout.nextMetaID = readUint32BE(data[cursor : cursor+4])
	cursor += 4
	count := int(readUint32BE(data[cursor : cursor+4]))
	cursor += 4
	layout.entries = make([]segmentIndexL1Entry, 0, count)
	for i := 0; i < count; i++ {
		if cursor+8 > len(data) {
			return segmentIndexLayout{}, io.ErrUnexpectedEOF
		}
		metaID := readUint32BE(data[cursor : cursor+4])
		chunkCount := readUint32BE(data[cursor+4 : cursor+8])
		cursor += 8
		start, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return segmentIndexLayout{}, err
		}
		cursor += n
		end, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return segmentIndexLayout{}, err
		}
		cursor += n
		layout.entries = append(layout.entries, segmentIndexL1Entry{
			MetaID:     metaID,
			KeyStart:   start,
			KeyEnd:     end,
			ChunkCount: chunkCount,
		})
	}
	if layout.nextMetaID == 0 {
		layout.nextMetaID = uint32(len(layout.entries)) + 1
	}
	return layout, nil
}

func encodeTopLevelIndex(layout segmentIndexLayout) ([]byte, error) {
	if layout.mode != indexLayoutMultiLevel {
		return nil, fmt.Errorf("invalid layout mode")
	}
	buf := make([]byte, 0, 32+len(layout.entries)*48)
	var tmp32 [4]byte
	writeUint32BE(tmp32[:], segmentIndexMultiLevelMagic)
	buf = append(buf, tmp32[:]...)
	var tmp16 [2]byte
	writeUint16BE(tmp16[:], segmentIndexFormatVersion)
	buf = append(buf, tmp16[:]...)
	buf = append(buf, 0, 0)
	writeUint32BE(tmp32[:], layout.nextMetaID)
	buf = append(buf, tmp32[:]...)
	writeUint32BE(tmp32[:], uint32(len(layout.entries)))
	buf = append(buf, tmp32[:]...)
	for _, entry := range layout.entries {
		writeUint32BE(tmp32[:], entry.MetaID)
		buf = append(buf, tmp32[:]...)
		writeUint32BE(tmp32[:], entry.ChunkCount)
		buf = append(buf, tmp32[:]...)
		var err error
		if buf, err = appendVarBytes(buf, entry.KeyStart); err != nil {
			return nil, err
		}
		if buf, err = appendVarBytes(buf, entry.KeyEnd); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func layoutEntriesEqual(a, b []segmentIndexL1Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].MetaID != b[i].MetaID || a[i].ChunkCount != b[i].ChunkCount {
			return false
		}
		if !bytes.Equal(a[i].KeyStart, b[i].KeyStart) || !bytes.Equal(a[i].KeyEnd, b[i].KeyEnd) {
			return false
		}
	}
	return true
}

func (db *PrefixDB) writeSegmentIndex(folderPath string, metas []segmentChunkMeta) error {
	folderID := segmentFolderIDFromPath(folderPath)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderID)
	defer unlock()
	// Capture the previous layout so we can remove stale L2 files without scanning
	// the whole folder (which may contain many chunk_*.dat files).
	prevLayout, _ := db.loadSegmentIndexLayout(folderPath)
	var prevEntries []segmentIndexL1Entry
	if prevLayout.mode == indexLayoutMultiLevel {
		prevEntries = prevLayout.entries
	}
	if len(metas) == 0 {
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := os.Remove(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		db.bumpSegmentIndexGenerationLocked(entry)
		if len(prevEntries) > 0 {
			return removeStaleLevel2IndexFiles(folderPath, prevEntries, nil)
		}
		return removeLevel2IndexFiles(folderPath, nil)
	}
	serializedSize := estimateSegmentIndexSize(metas)
	if serializedSize <= segmentIndexMultiLevelThreshold {
		buf, err := encodeSegmentChunkMetas(metas)
		if err != nil {
			return err
		}
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := writeFileIfChanged(db, indexPath, buf); err != nil {
			return err
		}
		db.bumpSegmentIndexGenerationLocked(entry)
		if len(prevEntries) > 0 {
			return removeStaleLevel2IndexFiles(folderPath, prevEntries, nil)
		}
		return removeLevel2IndexFiles(folderPath, nil)
	}
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return err
	}
	oldEntries := layout.entries
	if layout.mode != indexLayoutMultiLevel {
		oldEntries = nil
		layout = segmentIndexLayout{mode: indexLayoutMultiLevel, nextMetaID: 1}
	}
	var reuseLayoutGrouping bool
	var groupOffsets []int
	// Try to reuse existing L2 grouping if it matches the current meta count.
	var groups [][]segmentChunkMeta
	if layout.mode == indexLayoutMultiLevel && len(layout.entries) > 0 {
		sum := 0
		for _, entry := range layout.entries {
			sum += int(entry.ChunkCount)
		}
		if sum == len(metas) {
			groups = make([][]segmentChunkMeta, 0, len(layout.entries))
			groupOffsets = make([]int, 0, len(layout.entries))
			off := 0
			for _, entry := range layout.entries {
				cnt := int(entry.ChunkCount)
				groupOffsets = append(groupOffsets, off)
				groups = append(groups, metas[off:off+cnt])
				off += cnt
			}
			reuseLayoutGrouping = true
		}
	}
	if len(groups) == 0 {
		groups = splitSegmentMetas(metas)
		if len(groups) == 0 {
			groups = [][]segmentChunkMeta{metas}
		}
		reuseLayoutGrouping = false
		groupOffsets = nil
	}
	nextID := layout.nextMetaID
	if nextID == 0 {
		nextID = 1
	}
	idAssignments := make([]uint32, len(groups))
	for i := range groups {
		if i < len(layout.entries) {
			idAssignments[i] = layout.entries[i].MetaID
		}
		if idAssignments[i] == 0 {
			idAssignments[i] = nextID
			nextID++
		}
	}
	keep := make(map[uint32]struct{}, len(idAssignments))
	for _, id := range idAssignments {
		keep[id] = struct{}{}
	}
	newEntries := make([]segmentIndexL1Entry, 0, len(groups))
	for idx, group := range groups {
		path := level2IndexFilePath(folderPath, idAssignments[idx])
		skipWrite := false
		_ = reuseLayoutGrouping
		_ = groupOffsets
		if !skipWrite {
			buf, err := encodeSegmentChunkMetas(group)
			if err != nil {
				return err
			}
			// We already decided the content changed (or file is missing), so avoid an extra
			// ReadFile+bytes.Equal pass.
			if err := writeFileAtomic(db, path, buf); err != nil {
				return err
			}
		}
		entry := segmentIndexL1Entry{
			MetaID:     idAssignments[idx],
			ChunkCount: uint32(len(group)),
		}
		entry.KeyStart = cloneBytes(group[0].KeyStart)
		entry.KeyEnd = cloneBytes(group[len(group)-1].KeyEnd)
		newEntries = append(newEntries, entry)
	}
	newLayout := segmentIndexLayout{
		mode:       indexLayoutMultiLevel,
		entries:    newEntries,
		nextMetaID: nextID,
	}
	needTopUpdate := layout.mode != indexLayoutMultiLevel || !layoutEntriesEqual(layout.entries, newEntries) || layout.nextMetaID != newLayout.nextMetaID
	if needTopUpdate {
		buf, err := encodeTopLevelIndex(newLayout)
		if err != nil {
			return err
		}
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := writeFileIfChanged(db, indexPath, buf); err != nil {
			return err
		}
	}
	// Even if the top-level layout didn't change, L2 files may have been rewritten.
	db.bumpSegmentIndexGenerationLocked(entry)
	// Remove only those L2 files that were previously referenced but are no longer
	// needed. This avoids scanning the whole folder (which can be huge).
	if len(oldEntries) > 0 {
		if err := removeStaleLevel2IndexFiles(folderPath, oldEntries, keep); err != nil {
			return err
		}
	}
	return nil
}

func (db *PrefixDB) invalidateSegmentIndexCache(folderID uint32) {
	unlock := db.lockSegmentIndexFolder(folderID)
	defer unlock()
	if folderID == 0 {
		return
	}
	db.segmentIndexMu.Lock()
	defer db.segmentIndexMu.Unlock()
	if db.storageIndexFolderId == folderID {
		db.storageIndexFolderId = 0
		db.storageIndexMetas = nil
		db.storageIndexReusable = true
		db.storageIndexArena = nil
	}
	if db.storageIndexPartialFolder == folderID {
		db.storageIndexPartialFolder = 0
		db.storageIndexPartialMetaID = 0
		db.storageIndexPartialMetas = nil
		db.storageIndexPartialReusable = true
		db.storageIndexPartialArena = nil
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.Remove(folderID)
	}
	if db.storageIndexLayoutReady {
		if db.storageIndexLayoutPath == db.segmentedFolderPath(folderID) {
			db.storageIndexLayoutReady = false
			db.storageIndexLayoutPath = ""
			db.storageIndexLayoutCache = segmentIndexLayout{}
		}
	}
}

func (db *PrefixDB) refreshSegmentIndexCache(folderID uint32, metas []segmentChunkMeta) {
	unlock := db.lockSegmentIndexFolder(folderID)
	defer unlock()
	if folderID == 0 {
		return
	}
	cloned := cloneSegmentChunkMetas(metas)
	db.segmentIndexMu.Lock()
	defer db.segmentIndexMu.Unlock()
	if db.storageIndexFolderId == folderID {
		db.storageIndexFolderId = folderID
		db.storageIndexMetas = cloneSegmentChunkMetas(cloned)
		db.storageIndexReusable = true
		db.storageIndexArena = nil
	}
	if db.storageIndexPartialFolder == folderID {
		db.storageIndexPartialFolder = 0
		db.storageIndexPartialMetaID = 0
		db.storageIndexPartialMetas = nil
		db.storageIndexPartialReusable = true
		db.storageIndexPartialArena = nil
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.Add(folderID, cloned)
	}
	if db.storageIndexLayoutReady && db.storageIndexLayoutPath == db.segmentedFolderPath(folderID) {
		db.storageIndexLayoutReady = false
		db.storageIndexLayoutPath = ""
		db.storageIndexLayoutCache = segmentIndexLayout{}
	}
}

func appendVarBytes(buf []byte, data []byte) ([]byte, error) {
	if len(data) > 0xFFFF {
		return buf, fmt.Errorf("segment meta field too large: %d", len(data))
	}
	var hdr [2]byte
	writeUint16BE(hdr[:], uint16(len(data)))
	buf = append(buf, hdr[:]...)
	buf = append(buf, data...)
	return buf, nil
}

func (db *PrefixDB) readSegmentIndex(folderID uint32) ([]segmentChunkMeta, error) {
	unlock := db.lockSegmentIndexFolder(folderID)
	defer unlock()
	return db.readSegmentIndexLockedInternal(folderID, true)
}

func (db *PrefixDB) readSegmentIndexNoCache(folderID uint32) ([]segmentChunkMeta, error) {
	unlock := db.lockSegmentIndexFolder(folderID)
	defer unlock()
	return db.readSegmentIndexLockedInternal(folderID, false)
}

func (db *PrefixDB) readSegmentIndexLockedInternal(folderID uint32, useLRU bool) ([]segmentChunkMeta, error) {
	if useLRU && db.storageIndexCache != nil {
		db.segmentIndexMu.Lock()
		if metas, ok := db.storageIndexCache.Get(folderID); ok {
			db.segmentIndexMu.Unlock()
			return metas, nil
		}
		db.segmentIndexMu.Unlock()
	}
	folderPath := db.segmentedFolderPath(folderID)
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return nil, err
	}
	var metas []segmentChunkMeta
	if layout.mode == indexLayoutMultiLevel {
		total := 0
		for _, entry := range layout.entries {
			total += int(entry.ChunkCount)
		}
		metas = make([]segmentChunkMeta, 0, total)
		var arena []byte
		for idx, entry := range layout.entries {
			data, err := db.readFileWithStats(level2IndexFilePath(folderPath, entry.MetaID), diskIOUsageStorageSegmentIndex)
			if err != nil {
				return nil, err
			}
			appendExisting := idx != 0
			if err := decodeSegmentIndexBuffer(data, &metas, &arena, appendExisting, folderPath); err != nil {
				return nil, err
			}
		}
	} else {
		data := layout.flatData
		if len(data) == 0 {
			indexPath := filepath.Join(folderPath, segmentIndexFileName)
			data, err = db.readFileWithStats(indexPath, diskIOUsageStorageSegmentIndex)
			if err != nil {
				return nil, err
			}
		}
		metas = nil
		var arena []byte
		if err := decodeSegmentIndexBuffer(data, &metas, &arena, false, folderPath); err != nil {
			return nil, err
		}
	}
	estimatedSize := estimateSegmentIndexSize(metas)
	if useLRU && estimatedSize >= segmentIndexCacheThresholdBytes && db.storageIndexCache != nil {
		db.segmentIndexMu.Lock()
		db.storageIndexCache.Add(folderID, metas)
		db.segmentIndexMu.Unlock()
	}
	return metas, nil
}

func (db *PrefixDB) readSegmentIndexForKey(folderID uint32, key []byte) ([]segmentChunkMeta, error) {
	unlock := db.lockSegmentIndexFolder(folderID)
	defer unlock()
	if len(key) == 0 {
		return db.readSegmentIndexLockedInternal(folderID, true)
	}
	if db.storageIndexCache != nil {
		db.segmentIndexMu.Lock()
		if metas, ok := db.storageIndexCache.Get(folderID); ok {
			db.segmentIndexMu.Unlock()
			return metas, nil
		}
		db.segmentIndexMu.Unlock()
	}
	folderPath := db.segmentedFolderPath(folderID)
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return nil, err
	}
	if layout.mode != indexLayoutMultiLevel {
		return db.readSegmentIndexLockedInternal(folderID, true)
	}
	entry := selectSegmentL1Entry(layout.entries, key)
	if entry == nil {
		return nil, fmt.Errorf("segment index entry not found for folder %d", folderID)
	}
	metas := make([]segmentChunkMeta, 0, entry.ChunkCount)
	var arena []byte
	data, err := db.readFileWithStats(level2IndexFilePath(folderPath, entry.MetaID), diskIOUsageStorageSegmentIndex)
	if err != nil {
		return nil, err
	}
	if err := decodeSegmentIndexBuffer(data, &metas, &arena, false, folderPath); err != nil {
		return nil, err
	}
	return metas, nil
}

func cloneIntoArena(arena *[]byte, src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	start := len(*arena)
	*arena = append(*arena, src...)
	return (*arena)[start:]
}

func cloneSegmentChunkMetas(src []segmentChunkMeta) []segmentChunkMeta {
	if len(src) == 0 {
		return nil
	}
	dst := make([]segmentChunkMeta, len(src))
	for i := range src {
		dst[i] = segmentChunkMeta{
			FileName:  strings.Clone(src[i].FileName),
			KVCount:   src[i].KVCount,
			ChunkSize: src[i].ChunkSize,
		}
		dst[i].KeyStart = cloneBytes(src[i].KeyStart)
		dst[i].KeyEnd = cloneBytes(src[i].KeyEnd)
	}
	return dst
}

func estimateSegmentChunkMetasMemory(metas []segmentChunkMeta) uint64 {
	if len(metas) == 0 {
		return 0
	}
	total := uint64(len(metas)) * uint64(unsafe.Sizeof(segmentChunkMeta{}))
	for i := range metas {
		total += uint64(len(metas[i].FileName))
		total += uint64(len(metas[i].KeyStart))
		total += uint64(len(metas[i].KeyEnd))
	}
	return total
}

func readVarBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	ln := int(buf[0])<<8 | int(buf[1])
	if len(buf) < 2+ln {
		return nil, 0, io.ErrUnexpectedEOF
	}
	return buf[2 : 2+ln], 2 + ln, nil
}

func selectSegmentChunkMeta(metas []segmentChunkMeta, key []byte) *segmentChunkMeta {
	if len(metas) == 0 {
		return nil
	}
	if len(key) == 0 {
		return &metas[0]
	}
	idx := sort.Search(len(metas), func(i int) bool {
		end := metas[i].KeyEnd
		if len(end) == 0 {
			return true
		}
		return bytes.Compare(key, end) <= 0
	})
	if idx == len(metas) {
		idx = len(metas) - 1
	}
	if meta := metas[idx]; len(meta.KeyStart) == 0 || bytes.Compare(key, meta.KeyStart) >= 0 {
		return &metas[idx]
	}
	for i := idx - 1; i >= 0; i-- {
		startOK := len(metas[i].KeyStart) == 0 || bytes.Compare(key, metas[i].KeyStart) >= 0
		endOK := len(metas[i].KeyEnd) == 0 || bytes.Compare(key, metas[i].KeyEnd) <= 0
		if startOK && endOK {
			return &metas[i]
		}
	}
	for i := idx + 1; i < len(metas); i++ {
		startOK := len(metas[i].KeyStart) == 0 || bytes.Compare(key, metas[i].KeyStart) >= 0
		endOK := len(metas[i].KeyEnd) == 0 || bytes.Compare(key, metas[i].KeyEnd) <= 0
		if startOK && endOK {
			return &metas[i]
		}
		if len(metas[i].KeyStart) > 0 && bytes.Compare(key, metas[i].KeyStart) < 0 {
			break
		}
	}
	return &metas[len(metas)-1]
}

func (db *PrefixDB) readSegmentChunkFile(folderID uint32, fileName string) ([]kvPair, *bufferLease, error) {
	return db.readSegmentChunkFileWithUsage(folderID, fileName, diskIOUsageStorageSeparatedLogs)
}

func (db *PrefixDB) readSegmentChunkFileWithUsage(folderID uint32, fileName string, usage diskIOUsage) ([]kvPair, *bufferLease, error) {
	lease, err := db.readSegmentFileBufferWithUsage(folderID, fileName, usage)
	if err != nil {
		return nil, nil, err
	}
	payload, kvCount, err := parseSegmentBuffer(lease.Bytes())
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	entries, err := buildPairsFromPayload(payload, kvCount, nil)
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	return entries, lease, nil
}

func (db *PrefixDB) readSegmentChunkPayload(folderID uint32, fileName string) ([]byte, int, *bufferLease, error) {
	return db.readSegmentChunkPayloadWithUsage(folderID, fileName, diskIOUsageStorageSeparatedLogs)
}

func (db *PrefixDB) readSegmentChunkPayloadWithUsage(folderID uint32, fileName string, usage diskIOUsage) ([]byte, int, *bufferLease, error) {
	lease, err := db.readSegmentFileBufferWithUsage(folderID, fileName, usage)
	if err != nil {
		return nil, 0, nil, err
	}
	payload, kvCount, err := parseSegmentBuffer(lease.Bytes())
	if err != nil {
		lease.Release()
		return nil, 0, nil, err
	}
	return payload, kvCount, lease, nil
}

func (db *PrefixDB) readSegmentFileBuffer(folderID uint32, fileName string) (*bufferLease, error) {
	return db.readSegmentFileBufferWithUsage(folderID, fileName, diskIOUsageStorageSeparatedLogs)
}

func (db *PrefixDB) readSegmentFileBufferWithUsage(folderID uint32, fileName string, usage diskIOUsage) (*bufferLease, error) {
	fullPath := filepath.Join(db.segmentedFolderPath(folderID), fileName)
	f, err := db.openCachedReadOnlyFile(fullPath)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return nil, fmt.Errorf("empty segment chunk: %s", fullPath)
	}
	if size > int64(^uint32(0)) {
		return nil, fmt.Errorf("segment chunk too large: %s", fullPath)
	}
	intSize := int(size)
	buf := getDataBuffer(intSize)
	// NOTE: file handles may be reused via fileHandleCache. Do not rely on the
	// shared file offset (Read/Seek). Use a ReaderAt-based reader to always read
	// from offset 0.
	sr := io.NewSectionReader(f, 0, size)
	if _, err := io.ReadFull(sr, buf[:intSize]); err != nil {
		putDataBuffer(buf)
		return nil, err
	}
	db.addDiskRead(usage, intSize)
	return newBufferLease(buf[:intSize]), nil
}

func (db *PrefixDB) maybeNormalizeChunkEntries(entries []kvPair, meta *segmentChunkMeta) []kvPair {
	if len(entries) < 2 || meta == nil {
		return entries
	}
	if meta.ChunkSize <= uint64(db.storageChunkSize) {
		return entries
	}
	return normalizeStorageEntries(entries)
}

func (db *PrefixDB) readAccountStorageValue(accountKey, storageKey []byte) ([]byte, bool, error) {
	if len(accountKey) == 0 {
		return nil, false, nil
	}

	cacheInfo, err := db.resolveAccountStoragePointer(accountKey)
	if err != nil {
		return nil, false, err
	}

	if cacheInfo.storageFileID == 0 {
		return nil, false, nil
	}

	if isSegmentedStorage(cacheInfo.storageFileID) {
		val := db.readSegmentedChunkToCache(cacheInfo.storageFileID, accountKey, storageKey)
		if val == nil {
			return nil, false, nil
		}
		return val, true, nil

	} else {
		val := db.readStorageSegmentFile(cacheInfo.storageFileID, cacheInfo.storageOffset, cacheInfo.storageSize, accountKey, storageKey)
		if val == nil {
			return nil, false, nil
		}
		return val, true, nil
	}
}

func borrowStorageEntries(count int) []kvPair {
	if count <= 0 {
		return nil
	}
	if buf := kvPairEntryPool.Get(); buf != nil {
		entries := buf.([]kvPair)
		if cap(entries) >= count {
			return entries[:count]
		}
	}
	return make([]kvPair, count)
}

func releaseStorageEntries(entries []kvPair) {
	if entries == nil {
		return
	}
	for i := range entries {
		entries[i] = kvPair{}
	}
	kvPairEntryPool.Put(entries[:0])
}

func (db *PrefixDB) buildStorageEntries(payload []byte, kvCount int) ([]kvPair, error) {
	if kvCount == 0 {
		return nil, nil
	}
	entries := borrowStorageEntries(kvCount)
	decoded, err := buildPairsFromPayload(payload, kvCount, entries)
	if err != nil {
		releaseStorageEntries(entries)
		return nil, err
	}
	return decoded, nil
	// return normalizeStorageEntries(decoded), nil
}

func normalizeStorageEntries(entries []kvPair) []kvPair {
	if len(entries) <= 1 {
		return entries
	}
	// Fast path: if the chunk is already sorted, we can avoid map allocations.
	sorted := true
	strictlyIncreasing := true
	for i := 1; i < len(entries); i++ {
		cmp := bytes.Compare(entries[i-1].key, entries[i].key)
		if cmp > 0 {
			sorted = false
			strictlyIncreasing = false
			break
		}
		if cmp == 0 {
			strictlyIncreasing = false
		}
	}
	if sorted {
		if strictlyIncreasing {
			return entries
		}
		// Sorted with duplicates: keep the last entry for each key.
		out := entries[:0]
		for i := 0; i < len(entries); {
			j := i + 1
			for j < len(entries) && bytes.Equal(entries[j].key, entries[i].key) {
				j++
			}
			out = append(out, entries[j-1])
			i = j
		}
		return out
	}

	// General path: last write wins (append order), then sort for binary search.
	// Use unsafe byte->string conversion to avoid per-key allocations.
	lastIdx := make(map[string]int, len(entries))
	for i := range entries {
		lastIdx[bytesToString(entries[i].key)] = i
	}
	out := entries[:0]
	for i := range entries {
		if lastIdx[bytesToString(entries[i].key)] != i {
			continue
		}
		out = append(out, entries[i])
	}
	sortKVPairs(out)
	return out
}

func (db *PrefixDB) resolveAccountStoragePointer(accountKey []byte) (StorageInfo, error) {

	node, err := db.getNode(accountKey)
	if err != nil {
		return StorageInfo{}, err
	}

	if node != nil && node.storageFileID != 0 {
		cacheInfo := StorageInfo{
			storageFileID: node.storageFileID,
			storageOffset: node.storageOffset,
			storageSize:   node.storageSize,
		}
		return cacheInfo, nil
	}
	return StorageInfo{}, nil
}

func (db *PrefixDB) readStorageSegmentFile(fileID uint32, offset int64, size uint64, accountKey, storageKey []byte) []byte {
	if isSegmentedStorage(fileID) {
		return nil
	}
	p, _ := db.storagePathByFileID(fileID)

	f, err := db.openCachedReadOnlyFile(p)
	if err != nil {
		return nil
	}

	if size == 0 {
		return nil

	}

	total := int(size)
	buf := getDataBuffer(total)
	read := 0
	var ret []byte
	for read < total {
		n, err := f.ReadAt(buf[read:total], offset+int64(read))
		if err != nil {
			if err == io.EOF && read+n == total {
				read += n
				db.addDiskRead(diskIOUsageStorageCommonLogs, n)
				break
			}
			putDataBuffer(buf)
			return nil
		}
		read += n
		db.addDiskRead(diskIOUsageStorageCommonLogs, n)
	}
	if read != total {
		putDataBuffer(buf)
		return nil
	}
	buf = buf[:total]

	if db.storageCache != nil && len(accountKey) > 0 && len(storageKey) > 0 {
		payload := buf
		if len(payload) >= 4 {
			kvCount := int(readUint32BE(payload[:4]))
			payload = payload[4:]
			cursor := 0
			payloadLen := len(payload)
			var klen, vlen int
			hit := false
			malformed := false
			count := 0
			for i := 0; i < kvCount; i++ {
				if cursor+6 > payloadLen {
					malformed = true
					break
				}
				header := payload[cursor : cursor+6]
				klen = int(header[0])<<8 | int(header[1])
				vlen = int(header[2])<<24 | int(header[3])<<16 | int(header[4])<<8 | int(header[5])
				cursor += 6
				totalLen := klen + vlen
				if cursor+totalLen > payloadLen {
					malformed = true
					break
				}
				keyRaw := payload[cursor : cursor+klen]
				key := normalizeStoredStorageKey(keyRaw)
				if bytes.HasPrefix(key, storageKey) {
					var value []byte
					if vlen > 0 {
						value = payload[cursor+klen : cursor+totalLen]
					}
					if bytes.Equal(key, storageKey) {
						if value == nil {
							ret = nil
							db.storageCache.Add(db.storageCacheKey(accountKey, key), nil)
						} else {
							ret = append([]byte(nil), value...)
							valueCopy := append([]byte(nil), value...)
							db.storageCache.Add(db.storageCacheKey(accountKey, key), valueCopy)
						}
						hit = true
					}
					if hit && count < 16 {
						if value == nil {
							db.storageCache.Add(db.storageCacheKey(accountKey, key), nil)
						} else {
							valueCopy := append([]byte(nil), value...)
							db.storageCache.Add(db.storageCacheKey(accountKey, key), valueCopy)
						}
						count++
					}
				}
				cursor += totalLen
			}
			if !hit && !malformed {
				db.storageCache.Add(db.storageCacheKey(accountKey, storageKey), nil)
			}
		}
	}
	putDataBuffer(buf)
	return ret
}

func (db *PrefixDB) readStorageSegmentPayload(fileID uint32, offset int64, size uint64) ([]byte, int, *bufferLease, error) {
	if isSegmentedStorage(fileID) {
		return db.readSegmentedStoragePayload(fileID)
	}
	p, _ := db.storagePathByFileID(fileID)
	f, err := db.openCachedReadOnlyFile(p)
	if err != nil {
		return nil, 0, nil, err
	}
	if size == 0 {
		return nil, 0, nil, nil
	}
	total := int(size)
	buf := getDataBuffer(total)
	read := 0
	for read < total {
		n, err := f.ReadAt(buf[read:total], offset+int64(read))
		if err != nil {
			if err == io.EOF && read+n == total {
				read += n
				db.addDiskRead(diskIOUsageStorageCommonLogs, n)
				break
			}
			putDataBuffer(buf)
			return nil, 0, nil, err
		}
		read += n
		db.addDiskRead(diskIOUsageStorageCommonLogs, n)
	}
	if read != total {
		putDataBuffer(buf)
		return nil, 0, nil, io.ErrUnexpectedEOF
	}
	buf = buf[:total]

	if len(buf) < 4 {
		putDataBuffer(buf)
		return nil, 0, nil, io.ErrUnexpectedEOF
	}
	kvCount := int(readUint32BE(buf[:4]))
	payload := buf[4:]
	return payload, kvCount, newBufferLease(buf), nil

}

func (db *PrefixDB) readSegmentedStoragePayload(fileID uint32) ([]byte, int, *bufferLease, error) {
	folderID := fileID & ^segmentedStorageFlag
	db.segmentedMu.RLock()
	metas, err := db.readSegmentIndex(folderID)
	if err != nil {
		db.segmentedMu.RUnlock()
		return nil, 0, nil, err
	}
	if len(metas) == 0 {
		db.segmentedMu.RUnlock()
		return nil, 0, nil, nil
	}
	fileNames := make([]string, len(metas))
	for i := range metas {
		fileNames[i] = metas[i].FileName
	}
	totalCount := 0
	totalSize := 0
	type chunkData struct {
		payload []byte
		count   int
		backing *bufferLease
	}
	pieces := make([]chunkData, 0, len(metas))
	for _, name := range fileNames {
		payload, count, backing, err := db.readSegmentChunkPayload(folderID, name)
		if err != nil {
			db.segmentedMu.RUnlock()
			for _, piece := range pieces {
				if piece.backing != nil {
					piece.backing.Release()
				}
			}
			return nil, 0, nil, err
		}
		pieces = append(pieces, chunkData{payload: payload, count: count, backing: backing})
		totalCount += count
		totalSize += len(payload)
	}
	db.segmentedMu.RUnlock()
	merged := getDataBuffer(totalSize)
	cursor := 0
	for _, piece := range pieces {
		copy(merged[cursor:], piece.payload)
		cursor += len(piece.payload)
		piece.backing.Release()
	}
	merged = merged[:totalSize]
	return merged, totalCount, newBufferLease(merged), nil
}

func parseSegmentBuffer(buf []byte) ([]byte, int, error) {
	if len(buf) < 4 {
		return nil, 0, fmt.Errorf("segment too small")
	}
	kvCount := int(readUint32BE(buf[:4]))
	if kvCount < 0 {
		return nil, 0, fmt.Errorf("invalid kv count: %d", kvCount)
	}
	return buf[4:], kvCount, nil
}

func buildPairsFromPayload(payload []byte, kvCount int, dst []kvPair) ([]kvPair, error) {
	if kvCount <= 0 {
		return dst[:0], nil
	}

	if cap(dst) < kvCount {
		dst = make([]kvPair, kvCount)
	}
	entries := dst[:kvCount]
	cursor := 0
	payloadLen := len(payload)

	var klen, vlen int
	for i := 0; i < kvCount; i++ {
		if cursor+6 > payloadLen {
			return nil, io.ErrUnexpectedEOF
		}
		header := payload[cursor : cursor+6]
		klen = int(header[0])<<8 | int(header[1])
		vlen = int(header[2])<<24 | int(header[3])<<16 | int(header[4])<<8 | int(header[5])
		cursor += 6
		totalLen := klen + vlen
		if cursor+totalLen > payloadLen {
			return nil, io.ErrUnexpectedEOF
		}
		var val []byte
		if vlen > 0 {
			val = payload[cursor+klen : cursor+totalLen]
		}
		entries[i] = kvPair{
			key: normalizeStoredStorageKey(payload[cursor : cursor+klen]),
			// vlen==0 is a tombstone delete; preserve it as nil
			// so cache/read paths treat it as not-found.
			val: val,
		}
		cursor += totalLen
	}

	return entries, nil
}

func (db *PrefixDB) readStorageSegmentPairs(fileID uint32, offset int64, size uint64) ([]kvPair, *bufferLease, error) {
	if isSegmentedStorage(fileID) {
		return nil, nil, fmt.Errorf("file %d references segmented storage", fileID)
	}
	if size == 0 {
		return nil, nil, nil
	}
	payload, kvCount, backing, err := db.readStorageSegmentPayload(fileID, offset, size)
	if err != nil {
		return nil, nil, err
	}
	if kvCount == 0 {
		if backing != nil {
			backing.Release()
		}
		return nil, nil, nil
	}
	entries, err := buildPairsFromPayload(payload, kvCount, nil)
	if err != nil {
		if backing != nil {
			backing.Release()
		}
		return nil, nil, err
	}
	return entries, backing, nil
}

func (db *PrefixDB) GetStorageCount(accountKey []byte) (int, uint64, error) {
	node, err := db.getNode(accountKey)
	if err != nil {
		return 0, 0, err
	}
	if node == nil || node.storageFileID == 0 {
		return 0, 0, nil
	}

	p, _ := db.storagePathByFileID(node.storageFileID)

	f, err := db.openCachedReadOnlyFile(p)
	if err != nil {
		return 0, 0, err
	}

	if node.storageSize == 0 {
		return 0, 0, nil
	}

	// just read kv count
	buf := make([]byte, 10)

	n, err := f.ReadAt(buf, node.storageOffset)
	if err != nil && err != io.EOF {
		return 0, 0, err
	}
	db.addDiskRead(diskIOUsageStorageCommonLogs, n)
	buf = buf[:n]

	var kvCount int

	if len(buf) < 4 {
		return 0, 0, fmt.Errorf("segment too small")
	}
	kvCount = int(readUint32BE(buf[:4]))

	if kvCount < 0 {
		return 0, 0, fmt.Errorf("invalid kv count: %d", kvCount)
	}

	return kvCount, node.storageSize, nil

}

// storagePathByFileID returns the storage file path, whether it's hot storage, and the real file ID.
func (db *PrefixDB) storagePathByFileID(fileID uint32) (path string, realID uint32) {
	if isSegmentedStorage(fileID) {
		realID = fileID & ^segmentedStorageFlag
		return db.segmentedFolderPath(realID), realID
	}
	realID = fileID
	return filepath.Join(db.storageDir, fmt.Sprintf("storage_%08d.dat", realID)), realID
}

func (db *PrefixDB) openCachedReadOnlyFile(path string) (*os.File, error) {
	if db != nil && db.fileHandleCache != nil {
		return db.fileHandleCache.Open(path, os.O_RDONLY)
	}
	return os.Open(path)
}

func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (db *PrefixDB) storageCacheKey(accountKey, storageKey []byte) string {
	// Unambiguous, binary-safe composite key:
	//   [u32 accountKeyLen (big-endian)] [accountKey bytes] [storageKey bytes]
	// This avoids collisions even if accountKey/storageKey contain '\x00' bytes.
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(accountKey)))

	var b strings.Builder
	b.Grow(4 + len(accountKey) + len(storageKey))
	_, _ = b.Write(lenBuf[:])
	_, _ = b.Write(accountKey)
	_, _ = b.Write(storageKey)
	return b.String()
}

func stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

func isSegmentedStorage(fileID uint32) bool {
	return fileID&segmentedStorageFlag != 0
}

func (db *PrefixDB) startStorageGCWorker() {
	if db.storageGCQueue != nil {
		return
	}
	db.storageGCQueue = make(chan storageGCJob, storageGCQueueCapacity(db.gcWorkers))
	db.storageGCInFlight = make(map[string]struct{})
	db.storageGCStop = make(chan struct{})
	db.storageGCWait.Add(1)
	go func() {
		defer db.storageGCWait.Done()
		var batchWait sync.WaitGroup
		pending := make(map[uint32][]storageGCJob)
		active := make(map[uint32]struct{})
		batchDone := make(chan uint32, storageGCQueueCapacity(db.gcWorkers))
		launchBatch := func(folderID uint32) {
			jobs := pending[folderID]
			if len(jobs) == 0 {
				return
			}
			if _, exists := active[folderID]; exists {
				return
			}
			delete(pending, folderID)
			active[folderID] = struct{}{}
			batchWait.Add(1)
			go func(id uint32, jobs []storageGCJob) {
				defer batchWait.Done()
				db.processStorageGCBatch(jobs)
				batchDone <- id
			}(folderID, jobs)
		}
		launchAllReady := func() {
			for folderID := range pending {
				launchBatch(folderID)
			}
		}
		drainQueue := func() {
			for {
				select {
				case job := <-db.storageGCQueue:
					pending[job.folderID] = append(pending[job.folderID], job)
				default:
					return
				}
			}
		}
		stopRequested := false
		for {
			if stopRequested && len(active) == 0 {
				launchAllReady()
				if len(active) == 0 && len(pending) == 0 {
					break
				}
			}
			select {
			case job := <-db.storageGCQueue:
				pending[job.folderID] = append(pending[job.folderID], job)
				drainQueue()
				launchAllReady()
			case folderID := <-batchDone:
				delete(active, folderID)
				launchBatch(folderID)
			case <-db.storageGCStop:
				stopRequested = true
				drainQueue()
				launchAllReady()
			}
		}
		batchWait.Wait()
	}()
}

func (db *PrefixDB) stopStorageGCWorker() {
	if db.storageGCStop == nil {
		return
	}
	select {
	case <-db.storageGCStop:
	default:
		close(db.storageGCStop)
	}
	db.storageGCWait.Wait()
	db.storageGCStop = nil
	db.storageGCQueue = nil
	db.storageGCInFlight = nil
}

func (db *PrefixDB) isStorageGCIdle() bool {
	if db == nil {
		return true
	}
	queued := 0
	if db.storageGCQueue != nil {
		queued = len(db.storageGCQueue)
	}
	db.storageGCMu.Lock()
	inFlight := len(db.storageGCInFlight)
	db.storageGCMu.Unlock()
	return queued == 0 && inFlight == 0
}

func (db *PrefixDB) maybeScheduleStorageGC(folderID uint32, meta *segmentChunkMeta, backing *bufferLease) {
	release := func() {
		if backing != nil {
			backing.Release()
			backing = nil
		}
	}
	if db == nil || meta == nil || meta.FileName == "" {
		release()
		return
	}
	if db.storageGCQueue == nil || meta.ChunkSize <= uint64(db.segmentedChunkTriggerSize()) {
		release()
		return
	}
	job := storageGCJob{folderID: folderID, fileName: meta.FileName, backing: backing}
	key := job.key()
	db.storageGCMu.Lock()
	if db.storageGCInFlight == nil {
		db.storageGCMu.Unlock()
		release()
		return
	}
	if _, exists := db.storageGCInFlight[key]; exists {
		db.storageGCMu.Unlock()
		release()
		return
	}
	db.storageGCInFlight[key] = struct{}{}
	db.storageGCMu.Unlock()

	select {
	case db.storageGCQueue <- job:
	default:
		go db.processStorageGCJob(job)
	}
}

func (db *PrefixDB) processStorageGCJob(job storageGCJob) {
	release := db.acquireSharedGCWorker()
	defer release()
	defer db.finishStorageGCJob(job)
	if err := db.runStorageGCJob(job); err != nil {
		fmt.Printf("storage GC failed for folder %d file %s: %v\n", job.folderID, job.fileName, err)
	}
}

func (db *PrefixDB) processStorageGCBatch(jobs []storageGCJob) {
	if len(jobs) == 0 {
		return
	}
	release := db.acquireSharedGCWorker()
	defer release()
	for i := range jobs {
		job := jobs[i]
		defer db.finishStorageGCJob(job)
	}
	if err := db.runStorageGCBatch(jobs); err != nil {
		// Print one summary line to avoid log spam.
		fmt.Printf("storage GC batch failed for folder %d jobs %d: %v\n", jobs[0].folderID, len(jobs), err)
	}
}

func (db *PrefixDB) runStorageGCBatch(jobs []storageGCJob) error {
	if len(jobs) == 0 {
		return nil
	}
	folderID := jobs[0].folderID
	// Ensure we always release any backings not consumed by chunk rewrite.
	backings := make([]*bufferLease, len(jobs))
	for i := range jobs {
		backings[i] = jobs[i].backing
	}
	defer func() {
		for i := range backings {
			if backings[i] != nil {
				backings[i].Release()
			}
		}
	}()

	// Phase 1: read index once and rewrite multiple target chunks into new files.
	metas, gen0, err := db.readSegmentIndexNoCacheWithGen(folderID)
	if err != nil {
		return err
	}
	if len(metas) == 0 {
		return nil
	}

	// Deduplicate by fileName within this batch.
	seen := make(map[string]struct{}, len(jobs))
	unique := make([]storageGCJob, 0, len(jobs))
	uniqueIdx := make([]int, 0, len(jobs))
	for i := range jobs {
		j := jobs[i]
		if j.folderID != folderID || j.fileName == "" {
			continue
		}
		if _, ok := seen[j.fileName]; ok {
			continue
		}
		seen[j.fileName] = struct{}{}
		unique = append(unique, j)
		uniqueIdx = append(uniqueIdx, i)
	}
	if len(unique) == 0 {
		return nil
	}

	folderPath := db.segmentedFolderPath(folderID)
	maxOrd := -1
	for i := range metas {
		if ord := parseChunkOrdinal(metas[i].FileName); ord > maxOrd {
			maxOrd = ord
		}
	}
	nextOrd := maxOrd + 1

	// replacements maps old fileName -> new metas (may be nil to delete from index).
	replacements := make(map[string][]segmentChunkMeta, len(unique))

	for u := range unique {
		job := unique[u]
		// Find the meta for this chunk in the snapshot.
		idx := -1
		for i := range metas {
			if metas[i].FileName == job.fileName {
				idx = i
				break
			}
		}
		if idx == -1 {
			continue
		}

		var (
			preloaded      []kvPair
			preloadBacking *bufferLease
		)
		// Try to decode from the captured backing buffer to avoid a re-read.
		backingIdx := uniqueIdx[u]
		if backings[backingIdx] != nil {
			payload, kvCount, pErr := parseSegmentBuffer(backings[backingIdx].Bytes())
			if pErr == nil {
				entries := borrowStorageEntries(kvCount)
				if decoded, decErr := buildPairsFromPayload(payload, kvCount, entries); decErr == nil {
					preloaded = decoded
					preloadBacking = backings[backingIdx]
					backings[backingIdx] = nil
				} else {
					releaseStorageEntries(entries)
				}
			}
		}

		chunkMetas, next, rErr := db.rewriteChunkWithDedupToNewFiles(folderID, folderPath, metas[idx], nil, nextOrd, preloaded, preloadBacking)
		if preloaded != nil {
			releaseStorageEntries(preloaded)
		}
		if rErr != nil {
			return rErr
		}
		nextOrd = next
		replacements[job.fileName] = chunkMetas
	}

	if len(replacements) == 0 {
		return nil
	}

	// Phase 2: commit by updating the index once.
	genNow := db.segmentIndexGenerationLocked(folderID)
	latest := metas
	if genNow != gen0 {
		latest, _, err = db.readSegmentIndexNoCacheWithGen(folderID)
		if err != nil {
			return err
		}
	}

	changed := false
	updated := make([]segmentChunkMeta, 0, len(latest)+len(replacements))
	for i := range latest {
		if repl, ok := replacements[latest[i].FileName]; ok {
			changed = true
			if len(repl) > 0 {
				updated = append(updated, repl...)
			}
			continue
		}
		updated = append(updated, latest[i])
	}
	if !changed {
		// All targeted chunks disappeared/changed concurrently; new chunks are left as garbage.
		return nil
	}
	if err := db.writeSegmentIndex(folderPath, updated); err != nil {
		return err
	}
	db.refreshSegmentIndexCache(folderID, updated)
	return nil
}

func (db *PrefixDB) finishStorageGCJob(job storageGCJob) {
	db.storageGCMu.Lock()
	if db.storageGCInFlight != nil {
		delete(db.storageGCInFlight, job.key())
	}
	db.storageGCMu.Unlock()
}

func (db *PrefixDB) runStorageGCJob(job storageGCJob) error {
	defer func() {
		if job.backing != nil {
			job.backing.Release()
		}
	}()
	// Phase 1: build rewritten chunk(s) into NEW files (do not overwrite old fileName).
	// This allows concurrent readers to keep using the old index+old chunk file safely.
	metas, gen0, err := db.readSegmentIndexNoCacheWithGen(job.folderID)
	if err != nil {
		return err
	}
	if len(metas) == 0 {
		return nil
	}
	idx := -1
	for i, meta := range metas {
		if meta.FileName == job.fileName {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil
	}
	folderPath := db.segmentedFolderPath(job.folderID)

	// Start allocating new chunk ordinals after the current max.
	maxOrd := -1
	for i := range metas {
		if ord := parseChunkOrdinal(metas[i].FileName); ord > maxOrd {
			maxOrd = ord
		}
	}
	nextOrd := maxOrd + 1

	var (
		preloaded      []kvPair
		preloadBacking *bufferLease
	)
	if job.backing != nil {
		payload, kvCount, err := parseSegmentBuffer(job.backing.Bytes())
		if err == nil {
			entries := borrowStorageEntries(kvCount)
			if decoded, decErr := buildPairsFromPayload(payload, kvCount, entries); decErr == nil {
				preloaded = decoded
				preloadBacking = job.backing
				job.backing = nil
			} else {
				releaseStorageEntries(entries)
			}
		}
		if job.backing != nil {
			job.backing.Release()
			job.backing = nil
		}
	}

	chunkMetas, nextOrd2, err := db.rewriteChunkWithDedupToNewFiles(job.folderID, folderPath, metas[idx], nil, nextOrd, preloaded, preloadBacking)
	if preloaded != nil {
		releaseStorageEntries(preloaded)
	}
	if err != nil {
		return err
	}
	_ = nextOrd2

	// Phase 2: commit by updating the index to point to the new files.
	// Re-read metas so we don't clobber concurrent index updates (e.g., another GC job).
	genNow := db.segmentIndexGenerationLocked(job.folderID)
	latest := metas
	if genNow != gen0 {
		var latestGen uint64
		latest, latestGen, err = db.readSegmentIndexNoCacheWithGen(job.folderID)
		_ = latestGen
		if err != nil {
			return err
		}
	}
	idx2 := -1
	for i := range latest {
		if latest[i].FileName == job.fileName {
			idx2 = i
			break
		}
	}
	if idx2 == -1 {
		// Someone else already removed/replaced it; leave the newly written chunks as garbage.
		return nil
	}
	updated := make([]segmentChunkMeta, 0, len(latest)-1+len(chunkMetas))
	updated = append(updated, latest[:idx2]...)
	if len(chunkMetas) > 0 {
		updated = append(updated, chunkMetas...)
	}
	if idx2+1 < len(latest) {
		updated = append(updated, latest[idx2+1:]...)
	}
	if err := db.writeSegmentIndex(folderPath, updated); err != nil {
		return err
	}
	db.refreshSegmentIndexCache(job.folderID, updated)
	// Option B: do NOT delete the original chunk file. It becomes garbage and can be cleaned later.
	return nil
}

// reserveChunkFileName tries to reserve a unique chunk_%04d.dat name by creating the destination
// path with O_EXCL. The created file is a placeholder and will be replaced atomically by writeChunkFile.
func reserveChunkFileName(folderPath string, startOrdinal int) (name string, nextOrdinal int, err error) {
	ord := startOrdinal
	for {
		candidate := fmt.Sprintf("chunk_%04d.dat", ord)
		fullPath := filepath.Join(folderPath, candidate)
		f, openErr := os.OpenFile(fullPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if openErr == nil {
			_ = f.Close()
			return candidate, ord + 1, nil
		}
		if errors.Is(openErr, os.ErrExist) {
			ord++
			continue
		}
		return "", ord, openErr
	}
}

// rewriteChunkWithDedupToNewFiles rewrites a chunk with deduplication and splits by target size,
// writing results into NEW chunk files (never overwriting meta.FileName). It returns the new metas
// and the next suggested ordinal.
func (db *PrefixDB) rewriteChunkWithDedupToNewFiles(folderID uint32, folderPath string, meta segmentChunkMeta, additions []kvPair, startOrdinal int, existing []kvPair, backing *bufferLease) ([]segmentChunkMeta, int, error) {
	var err error
	var bytesWritten uint64
	if existing == nil {
		existing, backing, err = db.readSegmentChunkFileWithUsage(folderID, meta.FileName, diskIOUsageStorageGC)
		if err != nil {
			return nil, startOrdinal, err
		}
	}
	if backing != nil {
		defer backing.Release()
	}
	if len(existing) > 1 {
		existing = db.maybeNormalizeChunkEntries(existing, &meta)
	}
	merged := mergeAndDedupPairs(existing, additions)
	if len(merged) == 0 {
		// Nothing left; caller should remove from index. Original file is left as garbage.
		atomic.AddUint64(&db.GCCount, 1)
		return nil, startOrdinal, nil
	}
	chunks := splitEntriesBySize(merged, db.segmentedChunkTargetSize())
	result := make([]segmentChunkMeta, 0, len(chunks))
	ordinal := startOrdinal
	reserved := make([]string, 0, len(chunks))
	defer func() {
		// Best-effort cleanup of placeholders on early error. Any successfully written chunk
		// files or index-less placeholders are safe to leave as garbage.
		if err == nil {
			return
		}
		for _, name := range reserved {
			_ = os.Remove(filepath.Join(folderPath, name))
		}
	}()

	for _, chunk := range chunks {
		name, next, rErr := reserveChunkFileName(folderPath, ordinal)
		if rErr != nil {
			err = rErr
			return nil, startOrdinal, rErr
		}
		reserved = append(reserved, name)
		ordinal = next
		chunkSize, wErr := db.writeChunkFileWithUsage(folderPath, name, chunk, diskIOUsageStorageGC)
		if wErr != nil {
			err = wErr
			return nil, startOrdinal, wErr
		}
		bytesWritten += uint64(chunkSize)
		result = append(result, segmentChunkMeta{
			FileName:  name,
			KeyStart:  cloneBytes(chunk[0].key),
			KeyEnd:    cloneBytes(chunk[len(chunk)-1].key),
			KVCount:   uint32(len(chunk)),
			ChunkSize: uint64(chunkSize),
		})
	}
	atomic.AddUint64(&db.GCCount, 1)
	atomic.AddUint64(&db.GCWriteBytes, bytesWritten)
	return result, ordinal, nil
}

func (db *PrefixDB) InsertAccountHashPebble(accountHash []byte, pebbleKey []byte) error {
	return db.accountHashKeyPebble.Put(accountHash, pebbleKey)
}

func (db *PrefixDB) readSegmentedChunkToCache(fileID uint32, accountKey []byte, storageKey []byte) []byte {
	folderID := fileID & ^segmentedStorageFlag
	cache := db.storageCache
	prefetchLimit := db.storageGetCacheCount
	repaired := false
	for {
		db.segmentedMu.RLock()
		metas, err := db.readSegmentIndexForKey(folderID, storageKey)
		if err != nil || len(metas) == 0 {
			db.segmentedMu.RUnlock()
			return nil
		}
		meta := selectSegmentChunkMeta(metas, storageKey)
		if meta == nil {
			db.segmentedMu.RUnlock()
			return nil
		}
		metaCopy := *meta
		fileName := metaCopy.FileName
		lease, err := db.readSegmentFileBuffer(folderID, fileName)
		db.segmentedMu.RUnlock()
		if err != nil {
			if !repaired && errors.Is(err, os.ErrNotExist) {
				if repairErr := db.repairMissingChunkFile(folderID, fileName); repairErr == nil {
					repaired = true
					continue
				}
			}
			return nil
		}
		buf := lease.Bytes()
		if len(buf) < 4 {
			lease.Release()
			return nil
		}
		kvCount := int(binary.BigEndian.Uint32(buf[:4]))
		if kvCount <= 0 {
			lease.Release()
			return nil
		}
		var res []byte
		buf = buf[4:]
		cursor := 0
		bufLen := len(buf)
		var klen, vlen int
		hit := false
		exactFound := false
		exactTombstone := false
		var exactValue []byte // points into buf; copied only once at the end
		malformed := false
		var count int
		for i := 0; i < kvCount; i++ {
			if cursor+6 > bufLen {
				malformed = true
				break
			}
			header := buf[cursor : cursor+6]
			klen = int(header[0])<<8 | int(header[1])
			vlen = int(header[2])<<24 | int(header[3])<<16 | int(header[4])<<8 | int(header[5])
			cursor += 6
			totalLen := klen + vlen
			if cursor+totalLen > bufLen {
				malformed = true
				break
			}
			keyRaw := buf[cursor : cursor+klen]
			key := normalizeStoredStorageKey(keyRaw)
			var value []byte
			if vlen > 0 {
				value = buf[cursor+klen : cursor+totalLen]
			}

			// Exact key may appear multiple times (e.g. append-order segments);
			// last occurrence should win.
			if bytes.Equal(key, storageKey) {
				exactFound = true
				hit = true
				if value == nil {
					exactTombstone = true
					exactValue = nil
				} else {
					exactTombstone = false
					exactValue = value
				}
				// Don't count the exact key towards prefetch.
				cursor += totalLen
				continue
			}

			// After the first hit, opportunistically prefetch a few related keys.
			// This is best-effort only; we still scan the whole chunk for correctness.
			if hit && prefetchLimit > 0 && count < prefetchLimit && bytes.HasPrefix(key, storageKey) {
				if cache != nil {
					if value == nil {
						cache.Add(db.storageCacheKey(accountKey, key), nil)
					} else {
						valueCopy := append([]byte(nil), value...)
						cache.Add(db.storageCacheKey(accountKey, key), valueCopy)
					}
				}
				count++
			}
			cursor += totalLen
		}
		if malformed {
			lease.Release()
			return res
		}

		// Publish the exact-key result and cache it once, based on the last occurrence.
		if exactFound {
			if exactTombstone {
				res = nil
				if cache != nil {
					cache.Add(db.storageCacheKey(accountKey, storageKey), nil)
				}
			} else {
				res = append([]byte(nil), exactValue...)
				if cache != nil {
					valueCopy := append([]byte(nil), exactValue...)
					cache.Add(db.storageCacheKey(accountKey, storageKey), valueCopy)
				}
			}
		}
		if !exactFound && !malformed && cache != nil {
			cache.Add(db.storageCacheKey(accountKey, storageKey), nil)
		}
		db.maybeScheduleStorageGC(folderID, &metaCopy, lease.Retain())
		lease.Release()
		return res
	}
}

func (db *PrefixDB) migrateLegacySegmentIndexFolder(folderID uint32, folderPath string) (bool, error) {
	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if layout.mode == indexLayoutMultiLevel {
		total := 0
		for _, entry := range layout.entries {
			total += int(entry.ChunkCount)
		}
		metas := make([]segmentChunkMeta, 0, total)
		var arena []byte
		migrated := false
		for idx, entry := range layout.entries {
			data, err := db.readFileWithStats(level2IndexFilePath(folderPath, entry.MetaID), diskIOUsageStorageSegmentIndex)
			if err != nil {
				return false, err
			}
			changed, err := db.decodeSegmentIndexBufferForMigration(data, &metas, &arena, idx != 0, folderPath)
			if err != nil {
				return false, err
			}
			if changed {
				migrated = true
			}
		}
		if !migrated {
			return false, nil
		}
		if err := db.writeSegmentIndex(folderPath, metas); err != nil {
			return false, err
		}
		return true, nil
	}

	data := layout.flatData
	if len(data) == 0 {
		data, err = db.readFileWithStats(indexPath, diskIOUsageStorageSegmentIndex)
		if err != nil {
			return false, err
		}
	}
	if isCompactSegmentIndexBuffer(data) {
		return false, nil
	}
	if isMultiLevelSegmentIndexBuffer(data) {
		return false, fmt.Errorf("unexpected multi-level index header in flat layout: %s", indexPath)
	}
	var metas []segmentChunkMeta
	var arena []byte
	if err := decodeLegacySegmentIndexBuffer(data, &metas, &arena, false, folderPath); err != nil {
		return false, err
	}
	if err := db.writeSegmentIndex(folderPath, metas); err != nil {
		return false, err
	}
	return true, nil
}

func (db *PrefixDB) MigrateLegacySegmentIndexFormats() error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		var folderID uint32
		if _, err := fmt.Sscanf(entry.Name(), segmentedDirNamePrefix+"%08d", &folderID); err != nil {
			continue
		}
		folderPath := filepath.Join(db.storageDir, entry.Name())
		db.segmentedMu.Lock()
		migrated, err := db.migrateLegacySegmentIndexFolder(folderID, folderPath)
		if err != nil {
			db.segmentedMu.Unlock()
			return err
		}
		if migrated {
			db.invalidateSegmentIndexCache(folderID)
		}
		db.segmentedMu.Unlock()
	}
	return nil
}

func (db *PrefixDB) UpgradeSegmentIndexFiles() error {
	return db.MigrateLegacySegmentIndexFormats()
}

// GCCollectGarbageChunks removes chunk files that are not referenced by the current
// segment index for the given folderID.
//
// This is an explicit, offline-style cleanup helper for the "Option B" GC strategy
// where old chunk files are intentionally left behind as garbage.
// It does not modify the index and should be called only when you can tolerate
// briefly blocking segmented readers via segmentedMu.
func (db *PrefixDB) GCCollectGarbageChunks(folderID uint32) (int, error) {
	if db == nil || folderID == 0 {
		return 0, nil
	}
	folderPath := db.segmentedFolderPath(folderID)

	// Serialize with any ongoing index/chunk mutations.
	db.segmentedMu.Lock()
	defer db.segmentedMu.Unlock()

	metas, err := db.readSegmentIndexNoCache(folderID)
	if err != nil {
		return 0, err
	}
	referenced := make(map[string]struct{}, len(metas))
	for i := range metas {
		if metas[i].FileName != "" {
			referenced[metas[i].FileName] = struct{}{}
		}
	}

	entries, err := os.ReadDir(folderPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}

	deleted := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		fullPath := filepath.Join(folderPath, name)

		// Remove leftover temp files from atomic chunk writes.
		if strings.HasPrefix(name, "chunk_") && strings.HasSuffix(name, ".dat.tmp") {
			if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return deleted, err
			}
			deleted++
			continue
		}

		// Only consider chunk_*.dat files.
		if !(strings.HasPrefix(name, "chunk_") && strings.HasSuffix(name, ".dat")) {
			continue
		}
		if _, ok := referenced[name]; ok {
			continue
		}
		if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return deleted, err
		}
		deleted++
	}

	return deleted, nil
}

// GCAllStorageChunkFiles runs a full sweep GC for all segmented storage chunk files.
// It rewrites every chunk file with deduplication and splits by target chunk size,
// then updates index metadata for each segmented folder.
func (db *PrefixDB) GCAllStorageChunkFiles() error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	fmt.Println("start GC for all segmented storage chunk files")

	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		var folderID uint32
		if _, err := fmt.Sscanf(entry.Name(), segmentedDirNamePrefix+"%08d", &folderID); err != nil {
			continue
		}

		db.segmentedMu.Lock()

		metas, err := db.readSegmentIndexNoCache(folderID)
		if err != nil {
			db.segmentedMu.Unlock()
			return err
		}
		if len(metas) == 0 {
			db.segmentedMu.Unlock()
			continue
		}

		folderPath := db.segmentedFolderPath(folderID)
		allEntries := make([]kvPair, 0)
		for _, meta := range metas {
			entries, backing, err := db.readSegmentChunkFileWithUsage(folderID, meta.FileName, diskIOUsageStorageGC)
			if err != nil {
				db.segmentedMu.Unlock()
				return err
			}
			for _, entry := range entries {
				keyCopy := append([]byte(nil), entry.key...)
				var valCopy []byte
				if entry.val != nil {
					valCopy = append([]byte(nil), entry.val...)
				}
				allEntries = append(allEntries, kvPair{key: keyCopy, val: valCopy})
			}
			if backing != nil {
				backing.Release()
			}
		}

		if len(allEntries) > 1 {
			sortKVPairs(allEntries)
			allEntries = dedupSortedKVPairs(allEntries)
		}

		updated := make([]segmentChunkMeta, 0, len(metas))
		keep := make(map[string]struct{})
		if len(allEntries) > 0 {
			chunks := splitEntriesBySize(allEntries, db.segmentedChunkTargetSize())
			for i, chunk := range chunks {
				fileName := fmt.Sprintf("chunk_%04d.dat", i)
				chunkSize, err := db.writeChunkFileWithUsage(folderPath, fileName, chunk, diskIOUsageStorageGC)
				if err != nil {
					db.segmentedMu.Unlock()
					return err
				}
				updated = append(updated, segmentChunkMeta{
					FileName:  fileName,
					KeyStart:  cloneBytes(chunk[0].key),
					KeyEnd:    cloneBytes(chunk[len(chunk)-1].key),
					KVCount:   uint32(len(chunk)),
					ChunkSize: uint64(chunkSize),
				})
				keep[fileName] = struct{}{}
			}
		}

		for _, meta := range metas {
			if _, ok := keep[meta.FileName]; ok {
				continue
			}
			fullPath := filepath.Join(folderPath, meta.FileName)
			if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				db.segmentedMu.Unlock()
				return err
			}
		}

		if err := db.writeSegmentIndex(folderPath, updated); err != nil {
			db.segmentedMu.Unlock()
			return err
		}
		db.invalidateSegmentIndexCache(folderID)
		db.segmentedMu.Unlock()
	}
	fmt.Println("Completed GC for all segmented storage chunk files")
	return nil
}

func (db *PrefixDB) GCPrefixTree() error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	if count := db.prefixTree.GC(); count >= 0 {
		return nil
	}
	return fmt.Errorf("prefix tree GC failed")
}
