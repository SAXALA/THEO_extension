package prefixdb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
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
	datatypepkg "github.com/tinoryj/EthStore/standalone/ethstore/datatype"
	"github.com/tinoryj/EthStore/standalone/ethstore/pebblestore"
)

const storageMaxFileSize int64 = 1 << 30 // 1GB

const (
	segmentedStorageFlag            uint32 = 1 << 31
	segmentIndexFileName                   = "index.meta"
	accountFolderBloomBitCount      uint64 = 1 << 20
	segmentIndexCacheThresholdBytes        = 0   // cache all decoded segment indexes
	segmentIndexCacheCapacityMiB           = 64  // total segment-index cache budget in MiB
	defaultStorageGCThreshold              = 2.0 // when chunk file size > chunkSize * threshold, trigger GC for the segment
	storageGCQueueMultiplier               = 8
)

const (
	segmentIndexMultiLevelThreshold = 16 * 1024
	segmentIndexLevel2TargetSize    = 4 * 1024
	segmentIndexLevel2MaxSize       = 8 * 1024
	segmentIndexCompressionMinSize  = 4 * 1024
	segmentIndexKeyStartMaxBytes    = 32
	segmentIndexFixedKeyFieldBytes  = 1 + segmentIndexKeyStartMaxBytes
	segmentIndexFlatEntryBytes      = 4 + segmentIndexFixedKeyFieldBytes
	segmentIndexL1EntryBytes        = 8 + segmentIndexFixedKeyFieldBytes
	segmentChunkStreamReadThreshold = 64 * 1024 * 1024
	segmentIndexFlatMagic           = 0x464c4958 // 'FLIX'
	segmentIndexMultiLevelMagic     = 0x4d4c4958 // 'MLIX'
	segmentIndexFormatVersion       = 3
	segmentIndexFlatVersion         = 3
)

const segmentIndexLevel2Pattern = "index.meta.l2.%08d"

const ()

var prefixdbLogWriter io.Writer = os.Stdout
var prefixdbDebugLogging atomic.Bool

func SetPrefixDBDebugLogging(enabled bool) {
	prefixdbDebugLogging.Store(enabled)
}

func prefixdbDebugf(format string, args ...interface{}) {
	if !prefixdbDebugLogging.Load() {
		return
	}
	fmt.Fprintf(prefixdbLogWriter, "prefixdb DEBUG: "+format+"\n", args...)
}

var errSegmentIndexEntryNotFound = errors.New("segment index entry not found")

type segmentedStorageReadFailure struct {
	folderPath string
	indexFile  string
	chunkFile  string
	reason     string
}

const storageKeyTrimOffset = 33 // 'O' + 32-byte account hash

type kvPair struct {
	key []byte
	val []byte
}

type segmentChunkMeta struct {
	FileName string
	KeyStart []byte
}

const segmentedChunkEntryHeaderSize = 4 // [keyLen u16][valLen u16] trailer stored after [key][val]

type segmentIndexLayoutMode uint8

const (
	indexLayoutFlat segmentIndexLayoutMode = iota
	indexLayoutMultiLevel
)

type segmentIndexL1Entry struct {
	MetaID     uint32
	KeyStart   []byte
	ChunkCount uint32
}

type segmentIndexLayout struct {
	mode       segmentIndexLayoutMode
	entries    []segmentIndexL1Entry
	nextMetaID uint32
	flatData   []byte
}

type accountFolderFilter struct {
	mu    sync.RWMutex
	bits  []uint64
	mask  uint64
	exact map[string]struct{}
}

func newAccountFolderFilter(bitCount uint64) *accountFolderFilter {
	if bitCount == 0 {
		bitCount = accountFolderBloomBitCount
	}
	if bitCount&(bitCount-1) != 0 {
		pow := uint64(1)
		for pow < bitCount {
			pow <<= 1
		}
		bitCount = pow
	}
	return &accountFolderFilter{
		bits:  make([]uint64, bitCount/64),
		mask:  bitCount - 1,
		exact: make(map[string]struct{}),
	}
}

func mixAccountFolderHash(key []byte, seed uint64) uint64 {
	h := seed ^ 0x9e3779b97f4a7c15
	for _, c := range key {
		h ^= uint64(c) + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

func (f *accountFolderFilter) bloomIndexes(accountKey []byte) [3]uint64 {
	h1 := mixAccountFolderHash(accountKey, 0x100000001b3)
	h2 := mixAccountFolderHash(accountKey, 0x84222325cbf29ce4)
	return [3]uint64{h1 & f.mask, (h1 + h2) & f.mask, (h1 + 2*h2) & f.mask}
}

func (f *accountFolderFilter) add(accountKey []byte) {
	if len(accountKey) == 0 {
		return
	}
	idx := f.bloomIndexes(accountKey)
	f.mu.Lock()
	for _, bitIdx := range idx {
		word := bitIdx >> 6
		bit := bitIdx & 63
		f.bits[word] |= uint64(1) << bit
	}
	f.exact[string(accountKey)] = struct{}{}
	f.mu.Unlock()
}

func (f *accountFolderFilter) remove(accountKey []byte) {
	if len(accountKey) == 0 {
		return
	}
	f.mu.Lock()
	delete(f.exact, string(accountKey))
	f.mu.Unlock()
}

func (f *accountFolderFilter) maybeContains(accountKey []byte) bool {
	if len(accountKey) == 0 {
		return false
	}
	idx := f.bloomIndexes(accountKey)
	f.mu.RLock()
	for _, bitIdx := range idx {
		word := bitIdx >> 6
		bit := bitIdx & 63
		if (f.bits[word] & (uint64(1) << bit)) == 0 {
			f.mu.RUnlock()
			return false
		}
	}
	_, ok := f.exact[string(accountKey)]
	f.mu.RUnlock()
	return ok
}

type storageGCJob struct {
	folderPath string
	fileName   string
	backing    *bufferLease
}

func (job storageGCJob) key() string {
	return job.folderPath + ":" + job.fileName
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

type trieStorageGetBreakdownStepStats struct {
	cacheCount   uint64
	cacheNanos   uint64
	noCacheCount uint64
	noCacheNanos uint64
}

type segmentIndexLookupSource uint8

const (
	segmentIndexLookupSourceNoCache segmentIndexLookupSource = iota
	segmentIndexLookupSourceL1Cache
	segmentIndexLookupSourceL2Cache
)

func (s segmentIndexLookupSource) fromCache() bool {
	return s == segmentIndexLookupSourceL1Cache || s == segmentIndexLookupSourceL2Cache
}

type trieStorageSegmentIndexLayerStats struct {
	l1CacheCount uint64
	l1CacheNanos uint64
	l2CacheCount uint64
	l2CacheNanos uint64
}

type trieStoragePrefetchStats struct {
	addCount    uint64
	addBytes    uint64
	addNilCount uint64
	hitCount    uint64
	hitBytes    uint64
	hitNilCount uint64
	clearCount  uint64
}

const storagePrefetchStatsSampleMask uint32 = 0xff

func addUint64Stat(dst *uint64, delta uint64) {
	if !analysisStatsEnabled || dst == nil || delta == 0 {
		return
	}
	atomic.AddUint64(dst, delta)
}

func loadUint64Stat(src *uint64) uint64 {
	if !analysisStatsEnabled || src == nil {
		return 0
	}
	return atomic.LoadUint64(src)
}

func shouldSampleStoragePrefetchKey(cacheKey string) bool {
	if !analysisStatsEnabled || cacheKey == "" {
		return false
	}
	last := len(cacheKey) - 1
	hash := uint32(len(cacheKey))
	hash = hash*16777619 ^ uint32(cacheKey[0])
	hash = hash*16777619 ^ uint32(cacheKey[len(cacheKey)/2])
	hash = hash*16777619 ^ uint32(cacheKey[last])
	return hash&storagePrefetchStatsSampleMask == 0
}

func storagePrefetchStatsSampleDenominator() uint64 {
	return uint64(storagePrefetchStatsSampleMask) + 1
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

	// segmentIndexMu protects in-memory segment index caches/layouts (they are mutated on reads).
	segmentIndexMu sync.Mutex
	// segmentIndexFolderLocks serializes segment index operations per folder path.
	segmentIndexFolderLocksMu sync.Mutex
	segmentIndexFolderLocks   map[string]*segmentIndexFolderLock

	storageDir       string
	storageFileMu    sync.Mutex
	storageCurFile   *os.File
	storageCurFileID uint32
	storageCurSize   int64
	fileHandleCache  *fileHandleCache
	storageBuf       storageOpBuffer
	segmentDirSeq    uint32

	// a index file maybe accessed frequently
	storageIndexFolderPath string
	storageIndexMetas      []segmentChunkMeta
	storageIndexCache      *segmentIndexCache
	storageIndexReusable   bool
	storageIndexArena      []byte
	storageGetCacheCount   int

	storageIndexPartialFolderPath string
	storageIndexPartialMetaID     uint32
	storageIndexPartialMetas      []segmentChunkMeta
	storageIndexPartialReusable   bool
	storageIndexPartialArena      []byte

	storageGCQueue    chan storageGCJob
	storageGCInFlight map[string]struct{}
	storageGCStop     chan struct{}
	storageGCWait     sync.WaitGroup
	storageGCMu       sync.Mutex
	gcWorkerLimiter   chan struct{}
	accountFolderSet  *accountFolderFilter

	nodeFileGCUnsortedRatioThreshold float64
	gcWorkers                        int
	nodeFileSortedCompression        bool
	segmentIndexCompression          bool

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

	trieStorageAccountEntryStats      trieStorageGetBreakdownStepStats
	trieStorageSegmentIndexStats      trieStorageGetBreakdownStepStats
	trieStorageSegmentIndexLayerStats trieStorageSegmentIndexLayerStats
	trieStoragePrefetchStats          trieStoragePrefetchStats
	trieStorageKVStats                trieStorageGetBreakdownStepStats
	storagePrefetchMu                 sync.Mutex
	storagePrefetchTrackedCount       uint64
	storagePrefetchPending            map[string]struct{}

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

	testSegmentedReadHook    func(folderPath string, meta segmentChunkMeta)
	testBuildStoragePlanHook func(accountKey string)
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
	mu   sync.RWMutex
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
	return NewPrefixDBWithRuntimeOptions(dirpath, storageChunkFileSize, totalCacheSizeMiB, storageGetCacheCount, 0, 0, 0, false, false, 0)
}

// NewPrefixDBWithCacheSettings creates PrefixDB with a single shared cache
// budget in MiB. All PrefixDB caches share this total budget.
// Use <=0 values to fallback to the default shared cache size.
func NewPrefixDBWithCacheSettings(dirpath string, storageChunkFileSize int, totalCacheSizeMiB int, storageGetCacheCount int) (*PrefixDB, error) {
	return NewPrefixDBWithRuntimeOptions(dirpath, storageChunkFileSize, totalCacheSizeMiB, storageGetCacheCount, 0, 0, 0, false, false, 0)
}

func NewPrefixDBWithFileHandleCacheSettings(dirpath string, storageChunkFileSize int, totalCacheSizeMiB int, storageGetCacheCount int, fileHandleCacheSize int) (*PrefixDB, error) {
	return NewPrefixDBWithRuntimeOptions(dirpath, storageChunkFileSize, totalCacheSizeMiB, storageGetCacheCount, 0, 0, 0, false, false, fileHandleCacheSize)
}

func NewPrefixDBWithRuntimeOptions(dirpath string, storageChunkFileSize int, totalCacheSizeMiB int, storageGetCacheCount int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool, fileHandleCacheSize int) (*PrefixDB, error) {
	fmt.Println(dirpath + " prefixDB Initializing...")
	SetPrefixDBDebugLogging(false)
	defaultCfg := DefaultConfig(dirpath)
	// Try to load config from config.json in dirpath
	configPath := filepath.Join(dirpath, "config.json")
	cfg, err := LoadConfig(configPath)
	if err != nil {
		// If config file doesn't exist or fails to load, use default config
		cfg = defaultCfg
	} else {
		// If BaseDir is not set in config, use dirpath
		if cfg.BaseDir == "" {
			cfg.BaseDir = dirpath
		}
		if cfg.AccountDir == "" {
			cfg.AccountDir = defaultCfg.AccountDir
		}
		if cfg.StorageDir == "" {
			cfg.StorageDir = defaultCfg.StorageDir
		}
		if cfg.NodeFileGCUnsortedRatioThreshold <= 0 {
			cfg.NodeFileGCUnsortedRatioThreshold = defaultCfg.NodeFileGCUnsortedRatioThreshold
		}
		if cfg.GCWorkers == 0 {
			cfg.GCWorkers = cfg.NodeFileGCWorkers
		}
		if cfg.StorageGCThreshold == 0 {
			cfg.StorageGCThreshold = defaultCfg.StorageGCThreshold
		}
	}
	if storageGCThreshold > 0 {
		cfg.StorageGCThreshold = storageGCThreshold
	}

	resolvedStorageGCThreshold := sanitizeStorageGCThreshold(cfg.StorageGCThreshold)

	// Ensure base directory exists
	if err := os.MkdirAll(cfg.BaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base dir: %v", err)
	}

	resolvedNodeFileSortedCompression := cfg.NodeFileSortedCompression || nodeFileSortedCompression
	resolvedSegmentIndexCompression := cfg.SegmentIndexCompression || segmentIndexCompression

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
		segmentIndexFolderLocks:          make(map[string]*segmentIndexFolderLock),
		fileHandleCache:                  getGlobalFileHandleCache(fileHandleCacheSize),
		accountFolderSet:                 newAccountFolderFilter(accountFolderBloomBitCount),
		storageDir:                       storageDir,
		storageGetCacheCount:             storageGetCacheCount,
		storageChunkSize:                 storageChunkFileSize,
		segmentedChunkHardLimit:          computeSegmentedChunkHardLimit(storageChunkFileSize, resolvedStorageGCThreshold),
		nodeFileGCUnsortedRatioThreshold: cfg.NodeFileGCUnsortedRatioThreshold,
		gcWorkers:                        cfg.GCWorkers,
		nodeFileSortedCompression:        resolvedNodeFileSortedCompression,
		segmentIndexCompression:          resolvedSegmentIndexCompression,
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
	if err := db.primeAccountFolderSetFromStorageDir(); err != nil {
		return nil, fmt.Errorf("failed to initialize account folder set: %v", err)
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

func (db *PrefixDB) getAccount(key []byte) ([]byte, bool, error) {
	readBefore := loadUint64Stat(&db.totalReadBytes)
	defer db.finishGetReadStats(readBefore)
	cacheKey := string(key)
	useNodeCache := !db.shouldBypassNodeCache(key)

	if db.accountBatch != nil {
		if value, _, ok := db.accountBatch.get(key); ok {
			return value, true, nil
		}
	}

	if useNodeCache {
		if entry, ok := db.nodeCache.Get(cacheKey); ok && entry.Value != nil {
			return entry.Value, true, nil
		}
	}

	node, err := db.getAccountNode(key)
	if err != nil {
		db.logAccountKVReadFailure(key, 0, "load-account-node", err)
		return nil, false, err
	}
	if node == nil {
		db.logAccountKVReadFailure(key, 0, "account-not-found", nil)
		return nil, false, nil
	}
	value, err := db.readFromFile(node.offset)
	if err != nil {
		db.logAccountKVReadFailure(key, node.offset, "read-account-file", err)
		return nil, false, err
	}

	if useNodeCache {
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
	}

	return value, true, nil
}

func (db *PrefixDB) Get(dataType datatypepkg.DataType, key []byte, accountKey []byte) ([]byte, bool, error) {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.getAccount(key)
	case datatypepkg.TrieNodeStorageDataType:
		return db.getStorage(key, accountKey)
	default:
		return nil, false, errors.New("unknown data type")
	}
}

func (db *PrefixDB) getStorage(key []byte, accountKey []byte) ([]byte, bool, error) {
	readBefore := loadUint64Stat(&db.totalReadBytes)
	defer db.finishGetReadStats(readBefore)

	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		db.logStorageKVReadFailure(key, accountKey, "normalize-storage-key", err)
		return nil, false, err
	}

	if accountKey == nil {
		db.logStorageKVReadFailure(storageKey, nil, "missing-parent-account", nil)
		return nil, false, nil
	}

	if v, present := db.batchGetOverlayNormalized(storageKey, accountKey); present {
		if v == nil {
			db.logStorageKVReadFailure(storageKey, accountKey, "storage-overlay-tombstone", nil)
			return nil, false, nil
		}
		return v, true, nil
	}

	storageCacheStart := time.Now()
	cacheKey := db.storageCacheKey(accountKey, storageKey)
	if value, ok := db.storageCache.Get(cacheKey); ok {
		recordTrieStorageGetBreakdownStep(&db.trieStorageKVStats, true, time.Since(storageCacheStart))
		db.noteStoragePrefetchHit(cacheKey, value)
		if value == nil {
			db.logStorageKVReadFailure(storageKey, accountKey, "storage-cache-tombstone", nil)
			return nil, false, nil
		}
		valueBytes := value.([]byte)
		db.addTrieStorageFetchStats(true, valueBytes)
		return valueBytes, true, nil
	}

	value, ok, failure, err := db.readAccountStorageValue(accountKey, storageKey)
	if err != nil {
		if failure != nil {
			db.logSegmentedStorageKVReadFailure(storageKey, accountKey, failure, err)
		} else {
			db.logStorageKVReadFailure(storageKey, accountKey, "read-account-storage", err)
		}
		return nil, false, err
	}
	if ok {
		db.addTrieStorageFetchStats(false, value)
		return value, true, nil
	}
	if failure != nil {
		db.logSegmentedStorageKVReadFailure(storageKey, accountKey, failure, nil)
	} else {
		db.logStorageKVReadFailure(storageKey, accountKey, "storage-not-found", nil)
	}
	return nil, false, nil
}

func splitLogPath(path string) (string, string) {
	if path == "" {
		return "", ""
	}
	return filepath.Dir(path), filepath.Base(path)
}

func (db *PrefixDB) accountReadLogFields(offset int64) (string, string, int64, uint64) {
	if db == nil || db.accountFile == nil {
		return "", "", offset, 0
	}
	dir, file := splitLogPath(db.accountFile.Name())
	return dir, file, offset, 0
}

func (db *PrefixDB) storageReadLogFields(accountKey []byte) (string, string, uint32, int64, uint64) {
	if db == nil || len(accountKey) == 0 {
		return "", "", 0, 0, 0
	}
	if db.isAccountStorageFolderManaged(accountKey) {
		folderPath := db.segmentedFolderPathForAccount(accountKey)
		dir, file := splitLogPath(folderPath)
		return dir, file, segmentedStorageFlag, 0, 0
	}
	node, err := db.getNode(accountKey)
	if err != nil || node == nil {
		return "", "", 0, 0, 0
	}
	if isSegmentedStorage(node.storageFileID) {
		folderPath := db.segmentedFolderPathForAccount(accountKey)
		dir, file := splitLogPath(folderPath)
		return dir, file, node.storageFileID, node.storageOffset, node.storageSize
	}
	path, _ := db.storagePathByFileID(node.storageFileID)
	dir, file := splitLogPath(path)
	return dir, file, node.storageFileID, node.storageOffset, node.storageSize
}

func (db *PrefixDB) logAccountKVReadFailure(key []byte, offset int64, reason string, err error) {
	dir, file, offset, size := db.accountReadLogFields(offset)
	if err != nil {
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: account kv read failed key=%x dir=%s file=%s offset=%d size=%d reason=%s err=%v\n", key, dir, file, offset, size, reason, err)
		return
	}
	fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: account kv read failed key=%x dir=%s file=%s offset=%d size=%d reason=%s\n", key, dir, file, offset, size, reason)
}

func (db *PrefixDB) logStorageKVReadFailure(storageKey, accountKey []byte, reason string, err error) {
	dir, file, fileID, offset, size := db.storageReadLogFields(accountKey)
	if err != nil {
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: storage kv read failed account=%x storage=%x dir=%s file=%s fileID=%d offset=%d size=%d reason=%s err=%v\n", accountKey, storageKey, dir, file, fileID, offset, size, reason, err)
		return
	}
	fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: storage kv read failed account=%x storage=%x dir=%s file=%s fileID=%d offset=%d size=%d reason=%s\n", accountKey, storageKey, dir, file, fileID, offset, size, reason)
}

func (db *PrefixDB) logSegmentedStorageKVReadFailure(storageKey, accountKey []byte, failure *segmentedStorageReadFailure, err error) {
	if failure == nil {
		db.logStorageKVReadFailure(storageKey, accountKey, "storage-not-found", err)
		return
	}
	dir, file := splitLogPath(failure.folderPath)
	indexFile := failure.indexFile
	if indexFile == "" {
		indexFile = segmentIndexFileName
	}
	if err != nil {
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: storage kv read failed account=%x storage=%x dir=%s file=%s fileID=%d offset=0 size=0 mode=folder index=%s chunk=%s reason=%s err=%v\n", accountKey, storageKey, dir, file, segmentedStorageFlag, indexFile, failure.chunkFile, failure.reason, err)
		return
	}
	fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: storage kv read failed account=%x storage=%x dir=%s file=%s fileID=%d offset=0 size=0 mode=folder index=%s chunk=%s reason=%s\n", accountKey, storageKey, dir, file, segmentedStorageFlag, indexFile, failure.chunkFile, failure.reason)
}

func (db *PrefixDB) putAccount(key, value []byte) error {
	cacheKey := string(key)
	var stroageInfo StorageInfo
	if !db.shouldBypassNodeCache(key) {
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			stroageInfo = entry.StorageInfo
			db.nodeCache.UpdateValue(cacheKey, value)
		}
	}
	if db.accountBatch != nil {
		db.accountBatch.add(key, value, stroageInfo.storageFileID, stroageInfo.storageOffset, stroageInfo.storageSize, ValueModified)
	}
	return nil
}

func (db *PrefixDB) putStorage(key, value, accountKey []byte) error {
	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		return err
	}
	if db.storageCache != nil {
		if accountKey != nil {
			db.removeStorageCacheValue(accountKey, storageKey)
		}
	}

	if accountKey == nil {
		fmt.Printf("Parent account key not found for %x\n", key)
		return nil
	}

	return db.bufferStorageMutation(accountKey, storageKey, value)
}

func (db *PrefixDB) Put(dataType datatypepkg.DataType, key, value, accountKey []byte) error {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.putAccount(key, value)
	case datatypepkg.TrieNodeStorageDataType:
		return db.putStorage(key, value, accountKey)
	default:
		return errors.New("unknown data type")
	}
}

func (db *PrefixDB) batchPutAccount(key, value []byte) error {
	cacheKey := string(key)
	var stroageInfo StorageInfo
	if !db.shouldBypassNodeCache(key) {
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			stroageInfo = entry.StorageInfo
			db.nodeCache.UpdateValue(cacheKey, value)
		}
	} else if db.nodeCache != nil {
		db.nodeCache.Delete(cacheKey)
	}
	if db.accountBatch != nil {
		db.accountBatch.add(key, value, stroageInfo.storageFileID, stroageInfo.storageOffset, stroageInfo.storageSize, ValueModified)
	}
	return nil
}

func (db *PrefixDB) batchPutStorage(key, value, accountKey []byte) error {
	return db.StorageBatchPut(key, value, accountKey)
}

func (db *PrefixDB) BatchPut(dataType datatypepkg.DataType, key, value, accountKey []byte) error {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.batchPutAccount(key, value)
	case datatypepkg.TrieNodeStorageDataType:
		return db.batchPutStorage(key, value, accountKey)
	default:
		return errors.New("unknown data type")
	}
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

	var accountOps map[string]WriteOperation
	if db.accountBatch != nil {
		accountOps = db.accountBatch.drainOperations()
	}
	var (
		storageBatch      map[string]map[string][]byte
		storageUnresolved map[string][]byte
		storagePlans      []storageCommitPlan
	)
	if db.storageBatch != nil {
		storageBatch, storageUnresolved = db.storageBatch.drain()
	}

	// Log batch commit statistics
	totalStorageKeys := 0
	for _, perAccount := range storageBatch {
		totalStorageKeys += len(perAccount)
	}
	if len(accountOps) > 0 || totalStorageKeys > 0 || len(storageUnresolved) > 0 {
		prefixdbDebugf("BatchCommit: starting commit - accountOps=%d storageAccounts=%d storageKeys=%d unresolved=%d",
			len(accountOps), len(storageBatch), totalStorageKeys, len(storageUnresolved))
	}

	if len(accountOps) == 0 && len(storageBatch) == 0 && len(storageUnresolved) == 0 {
		return nil
	}
	shouldWaitForStorageGC := false
	commitStart := time.Now()

	db.writeMutex.Lock()
	err = func() error {
		stageStart := time.Now()
		storagePlans, err = db.prepareStorageCommitPlans(storageBatch, storageUnresolved, accountOps)
		if err != nil {
			return err
		}
		prefixdbDebugf("BatchCommit: storage plans ready count=%d elapsed=%s", len(storagePlans), time.Since(stageStart))
		if len(storagePlans) > 0 && accountOps != nil {
			stageStart = time.Now()
			for _, plan := range storagePlans {
				op, ok := accountOps[plan.accountKey]
				if !ok || op.value == nil {
					continue
				}
				op.storageFileID = plan.storageInfo.storageFileID
				op.storageOffset = plan.storageInfo.storageOffset
				op.storageSize = plan.storageInfo.storageSize
				accountOps[plan.accountKey] = op
			}
			prefixdbDebugf("BatchCommit: storage pointers merged elapsed=%s", time.Since(stageStart))
		}

		stageStart = time.Now()
		prepared, err := db.prepareAccountCommit(accountOps)
		if err != nil {
			return err
		}
		prefixdbDebugf("BatchCommit: account entries prepared count=%d bytes=%d elapsed=%s",
			len(prepared.order), prepared.totalSize, time.Since(stageStart))

		trieAccountOffset, _ := db.accountFile.Seek(0, io.SeekEnd)
		if trieAccountOffset == 0 {
			trieAccountOffset = 1
		}

		naEntry := make([]byte, 0, prepared.totalSize)
		stageStart = time.Now()
		processedAccounts := 0
		for _, key := range prepared.order {
			op := accountOps[key]
			keyBytes := []byte(key)
			if op.modifiedType == None {
				continue
			}
			if op.value == nil {
				if db.nodeCache != nil {
					db.nodeCache.Delete(key)
				}
				if err := db.storeNode(keyBytes, &TrieNode{offset: 0, storageFileID: 0, storageOffset: 0, storageSize: 0}); err != nil {
					return err
				}
				continue
			}

			entry := prepared.entries[key]
			offset := trieAccountOffset + int64(len(naEntry))
			naEntry = append(naEntry, entry...)

			node := &TrieNode{
				storageFileID: op.storageFileID,
				storageOffset: op.storageOffset,
				storageSize:   op.storageSize,
				offset:        offset,
			}
			if err := db.storeNode(keyBytes, node); err != nil {
				return err
			}
			if db.nodeCache != nil {
				db.nodeCache.StoreMetadata(key, offset, StorageInfo{
					storageFileID: op.storageFileID,
					storageOffset: op.storageOffset,
					storageSize:   op.storageSize,
				})
			}
			processedAccounts++
			if processedAccounts%10000 == 0 {
				prefixdbDebugf("BatchCommit: account node writes progress=%d/%d elapsed=%s",
					processedAccounts, len(prepared.order), time.Since(stageStart))
			}
		}
		prefixdbDebugf("BatchCommit: account node writes done count=%d elapsed=%s", processedAccounts, time.Since(stageStart))

		if len(naEntry) > 0 {
			stageStart = time.Now()
			_, err := db.accountFile.WriteAt(naEntry, trieAccountOffset)
			if err != nil {
				return err
			}
			db.addDiskWrite(diskIOUsageAccountData, len(naEntry))
			prefixdbDebugf("BatchCommit: account file append bytes=%d elapsed=%s", len(naEntry), time.Since(stageStart))
		}

		if len(storagePlans) > 0 {
			shouldWaitForStorageGC = true
			stageStart = time.Now()
			appliedStoragePlans := 0
			for _, plan := range storagePlans {
				if _, ok := accountOps[plan.accountKey]; ok || plan.skipNodeWrite {
					continue
				}
				appliedStoragePlans++
				if appliedStoragePlans%25 == 0 {
					prefixdbDebugf("BatchCommit: storage pointer writes progress=%d/%d elapsed=%s",
						appliedStoragePlans, len(storagePlans), time.Since(stageStart))
				}
			}
			if err := db.applyStorageCommitPlans(storagePlans, accountOps, false); err != nil {
				return err
			}
			prefixdbDebugf("BatchCommit: storage pointer writes done applied=%d total=%d elapsed=%s",
				appliedStoragePlans, len(storagePlans), time.Since(stageStart))
		}
		prefixdbDebugf("BatchCommit: write phase finished totalElapsed=%s", time.Since(commitStart))
		return nil
	}()
	db.writeMutex.Unlock()
	if err != nil {
		return err
	}
	for _, plan := range storagePlans {
		db.syncStorageCacheEntries([]byte(plan.accountKey), plan.cacheEntries)
	}
	if shouldWaitForStorageGC {
		waitStart := time.Now()
		prefixdbDebugf("BatchCommit: waiting storage GC idle")
		if waitErr := db.waitForStorageGCIdle(); err == nil {
			err = waitErr
		}
		prefixdbDebugf("BatchCommit: storage GC idle wait done elapsed=%s", time.Since(waitStart))
	}
	prefixdbDebugf("BatchCommit: finished totalElapsed=%s", time.Since(commitStart))
	return nil
}

func (db *PrefixDB) hasAccount(key []byte) (bool, error) {
	cacheKey := string(key)
	useNodeCache := !db.shouldBypassNodeCache(key)
	if useNodeCache {
		if _, ok := db.nodeCache.Get(cacheKey); ok {
			return true, nil
		}
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

	if useNodeCache {
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
	}

	return true, nil
}

func (db *PrefixDB) Has(dataType datatypepkg.DataType, key []byte, accountKey []byte) (bool, error) {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.hasAccount(key)
	case datatypepkg.TrieNodeStorageDataType:
		return db.hasStorage(key, accountKey)
	default:
		return false, errors.New("unknown data type")
	}
}

func (db *PrefixDB) hasStorage(key []byte, accountKey []byte) (bool, error) {
	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		return false, err
	}

	if accountKey == nil {
		fmt.Printf("Parent account key not found for %x\n", key)
		return false, nil
	}

	if v, present := db.batchGetOverlayNormalized(storageKey, accountKey); present {
		return v != nil, nil
	}

	if v, ok := db.storageCache.Get(db.storageCacheKey(accountKey, storageKey)); ok {
		return v != nil, nil
	}
	_, ok, _, err := db.readAccountStorageValue(accountKey, storageKey)
	if err != nil {
		fmt.Println("Error reading account storage:", err)
		return false, err
	}
	if ok {
		return true, nil
	}
	return false, nil
}

func (db *PrefixDB) deleteAccount(key []byte) error {
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
}

func (db *PrefixDB) deleteStorage(key, accountKey []byte) error {
	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		return err
	}

	if db.storageCache != nil {
		if accountKey != nil {
			db.removeStorageCacheValue(accountKey, storageKey)
		}
	}

	if accountKey == nil {
		fmt.Printf("Parent account key not found for %x\n", key)
		return nil
	}

	return db.bufferStorageMutation(accountKey, storageKey, nil)
}

func (db *PrefixDB) Delete(dataType datatypepkg.DataType, key, accountKey []byte) error {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.deleteAccount(key)
	case datatypepkg.TrieNodeStorageDataType:
		return db.deleteStorage(key, accountKey)
	default:
		return errors.New("unknown data type")
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
	// Check for duplicate key in the buffer
	for _, existing := range db.storageBuf.storagekvs {
		if string(existing.key) == string(storageKey) {
			fmt.Printf("bufferStorageMutation: duplicate storage key detected in buffer - accountKey=%s storageKey=%x (will be overwritten)\n",
				accountStr, storageKey)
			break
		}
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
	if len(buf.storagekvs) == 0 {
		fmt.Printf("flushStorageBuffer: empty buffer for account - accountKey=%s\n",
			buf.accountKey)
		if err := db.prefixTree.Put([]byte(buf.accountKey), accOff, 0, 0, 0); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(buf.accountKey, StorageInfo{})
		if db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(buf.accountKey, StorageInfo{})
		}
		return nil
	}
	sortKVPairs(buf.storagekvs)
	fileID, off, sz, err := db.persistStorageEntries([]byte(buf.accountKey), buf.storagekvs, existingFileID, existingOffset, existingSize)
	if err != nil {
		return err
	}
	skipAccountPointerUpdate := shouldSkipAccountEntryPointerUpdate(existingFileID, fileID, off, sz)
	if !skipAccountPointerUpdate {
		if err := db.prefixTree.Put([]byte(buf.accountKey), accOff, fileID, off, sz); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(buf.accountKey, StorageInfo{
			storageFileID: fileID,
			storageOffset: off,
			storageSize:   sz,
		})
	}

	// cacheKeyHex := hex.EncodeToString([]byte(buf.accountKey))
	// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", fileID) + ", offset:" + fmt.Sprintf("%d", off) + ", size:" + fmt.Sprintf("%d", sz))

	if db.accountBatch != nil && !skipAccountPointerUpdate {
		_ = db.accountBatch.updateStoragePointer(buf.accountKey, StorageInfo{
			storageFileID: fileID,
			storageOffset: off,
			storageSize:   sz,
		})
	}
	db.syncStorageCacheEntries([]byte(buf.accountKey), buf.storagekvs)
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
	addUint64Stat(&db.commitOldKVReadCount, uint64(pairCount))
	addUint64Stat(&db.commitOldKVReadBytes, bytes)
}

func (db *PrefixDB) addReadBytes(n int) {
	addUint64Stat(&db.totalReadBytes, uint64(n))
}

func (db *PrefixDB) finishGetReadStats(readBefore uint64) {
	if !analysisStatsEnabled || db == nil {
		return
	}
	readAfter := atomic.LoadUint64(&db.totalReadBytes)
	if readAfter >= readBefore {
		atomic.AddUint64(&db.getReadBytesSum, readAfter-readBefore)
	}
	atomic.AddUint64(&db.getReadReqCount, 1)
}

func (db *PrefixDB) addDiskRead(usage diskIOUsage, n int) {
	if !analysisStatsEnabled || db == nil || usage >= diskIOUsageCount {
		return
	}
	atomic.AddUint64(&db.diskIOStats[usage].readOps, 1)
	if n > 0 {
		atomic.AddUint64(&db.diskIOStats[usage].readBytes, uint64(n))
		db.addReadBytes(n)
	}
}

func (db *PrefixDB) addDiskWrite(usage diskIOUsage, n int) {
	if !analysisStatsEnabled || db == nil || usage >= diskIOUsageCount {
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
	if !analysisStatsEnabled || db == nil {
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
	if !analysisStatsEnabled || len(value) == 0 {
		return
	}
	valueSize := uint64(len(value))
	if fromCache {
		atomic.AddUint64(&db.trieStorageCachePairs, 1)
		atomic.AddUint64(&db.trieStorageCacheBytes, valueSize)
		return
	}
	atomic.AddUint64(&db.trieStorageLogPairs, 1)
	atomic.AddUint64(&db.trieStorageLogBytes, valueSize)
}

func recordTrieStorageGetBreakdownStep(stats *trieStorageGetBreakdownStepStats, fromCache bool, duration time.Duration) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	nanos := uint64(duration)
	if fromCache {
		atomic.AddUint64(&stats.cacheCount, 1)
		atomic.AddUint64(&stats.cacheNanos, nanos)
		return
	}
	atomic.AddUint64(&stats.noCacheCount, 1)
	atomic.AddUint64(&stats.noCacheNanos, nanos)
}

func printTrieStorageGetBreakdownStep(label string, stats *trieStorageGetBreakdownStepStats) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	cacheCount := atomic.LoadUint64(&stats.cacheCount)
	cacheNanos := atomic.LoadUint64(&stats.cacheNanos)
	noCacheCount := atomic.LoadUint64(&stats.noCacheCount)
	noCacheNanos := atomic.LoadUint64(&stats.noCacheNanos)
	cacheAvgMicros := 0.0
	if cacheCount > 0 {
		cacheAvgMicros = float64(cacheNanos) / float64(cacheCount) / 1000.0
	}
	noCacheAvgMicros := 0.0
	if noCacheCount > 0 {
		noCacheAvgMicros = float64(noCacheNanos) / float64(noCacheCount) / 1000.0
	}
	fmt.Printf("PrefixDB TrieNodeStorage get breakdown [%s]: cacheCount=%d cacheTotal=%s cacheAvg=%0.2fus noCacheCount=%d noCacheTotal=%s noCacheAvg=%0.2fus\n",
		label,
		cacheCount,
		time.Duration(cacheNanos),
		cacheAvgMicros,
		noCacheCount,
		time.Duration(noCacheNanos),
		noCacheAvgMicros,
	)
}

func recordTrieStorageSegmentIndexLayer(source segmentIndexLookupSource, duration time.Duration, stats *trieStorageSegmentIndexLayerStats) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	nanos := uint64(duration)
	switch source {
	case segmentIndexLookupSourceL1Cache:
		atomic.AddUint64(&stats.l1CacheCount, 1)
		atomic.AddUint64(&stats.l1CacheNanos, nanos)
	case segmentIndexLookupSourceL2Cache:
		atomic.AddUint64(&stats.l2CacheCount, 1)
		atomic.AddUint64(&stats.l2CacheNanos, nanos)
	}
}

func printTrieStorageSegmentIndexLayerStats(stats *trieStorageSegmentIndexLayerStats) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	l1Count := atomic.LoadUint64(&stats.l1CacheCount)
	l1Nanos := atomic.LoadUint64(&stats.l1CacheNanos)
	l2Count := atomic.LoadUint64(&stats.l2CacheCount)
	l2Nanos := atomic.LoadUint64(&stats.l2CacheNanos)
	l1AvgMicros := 0.0
	if l1Count > 0 {
		l1AvgMicros = float64(l1Nanos) / float64(l1Count) / 1000.0
	}
	l2AvgMicros := 0.0
	if l2Count > 0 {
		l2AvgMicros = float64(l2Nanos) / float64(l2Count) / 1000.0
	}
	fmt.Printf("PrefixDB TrieNodeStorage segment-index cache layer stats: l1Count=%d l1Total=%s l1Avg=%0.2fus l2Count=%d l2Total=%s l2Avg=%0.2fus\n",
		l1Count,
		time.Duration(l1Nanos),
		l1AvgMicros,
		l2Count,
		time.Duration(l2Nanos),
		l2AvgMicros,
	)
}

func printSharedCacheLockOpStats(label string, stats sharedCacheLockOpSnapshot) {
	if !analysisStatsEnabled {
		return
	}
	if stats.Count == 0 {
		return
	}
	waitAvgMicros := float64(stats.WaitNanos) / float64(stats.Count) / 1000.0
	holdAvgMicros := float64(stats.HoldNanos) / float64(stats.Count) / 1000.0
	fmt.Printf("PrefixDB shared cache lock stats [%s]: count=%d waitTotal=%s waitAvg=%0.2fus holdTotal=%s holdAvg=%0.2fus\n",
		label,
		stats.Count,
		time.Duration(stats.WaitNanos),
		waitAvgMicros,
		time.Duration(stats.HoldNanos),
		holdAvgMicros,
	)
}

func printSharedCacheLockStats(shared *sharedByteCache) {
	if !analysisStatsEnabled || shared == nil {
		return
	}
	stats := shared.LockStatsSnapshot()
	printSharedCacheLockOpStats("get-touch", stats.GetTouch)
	printSharedCacheLockOpStats("get-notouch", stats.GetNoTouch)
	printSharedCacheLockOpStats("add", stats.Add)
	printSharedCacheLockOpStats("remove", stats.Remove)
	printSharedCacheLockOpStats("namespace", stats.Namespace)
}

func printTrieStoragePrefetchStats(db *PrefixDB) {
	if !analysisStatsEnabled || db == nil {
		return
	}
	addCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.addCount)
	addBytes := atomic.LoadUint64(&db.trieStoragePrefetchStats.addBytes)
	addNilCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.addNilCount)
	hitCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.hitCount)
	hitBytes := atomic.LoadUint64(&db.trieStoragePrefetchStats.hitBytes)
	hitNilCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.hitNilCount)
	clearCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.clearCount)
	pendingCount := db.storagePrefetchPendingCount()
	hitRate := 0.0
	if addCount > 0 {
		hitRate = float64(hitCount) / float64(addCount) * 100.0
	}
	fmt.Printf("PrefixDB storage prefetch stats: sampleRate=1/%d addCount=%d addBytes=%d addNilCount=%d hitCount=%d hitBytes=%d hitNilCount=%d clearCount=%d pendingCount=%d hitRate=%0.2f%%\n",
		storagePrefetchStatsSampleDenominator(),
		addCount,
		addBytes,
		addNilCount,
		hitCount,
		hitBytes,
		hitNilCount,
		clearCount,
		pendingCount,
		hitRate,
	)
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

	if analysisStatsEnabled {
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
		printTrieStorageGetBreakdownStep("account-entry", &db.trieStorageAccountEntryStats)
		printTrieStorageGetBreakdownStep("segment-index", &db.trieStorageSegmentIndexStats)
		printTrieStorageSegmentIndexLayerStats(&db.trieStorageSegmentIndexLayerStats)
		printSharedCacheLockStats(db.sharedCache)
		printTrieStoragePrefetchStats(db)
		printTrieStorageGetBreakdownStep("storage-kv-pairs", &db.trieStorageKVStats)
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
		db.printDiskIOStats()
	}

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

func (db *PrefixDB) normalizeStorageKey(rawKey []byte) ([]byte, error) {

	// Storage keys are expected to include the account-hash prefix: 'O' + 32-byte account hash.
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

// GetParentAccountKey retrieves the parent account key from a given (storage)key.
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

func (db *PrefixDB) shouldBypassNodeCache(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	return len(key) < MaxPrefixDepth
}

func (db *PrefixDB) getNode(key []byte) (*TrieNode, error) {
	node, _, err := db.getNodeWithSource(key)
	return node, err
}

func (db *PrefixDB) getNodeWithSource(key []byte) (*TrieNode, bool, error) {
	cacheKey := string(key)
	cacheHit := false
	useNodeCache := !db.shouldBypassNodeCache(key)
	if useNodeCache {
		addUint64Stat(&db.nodeCacheLookups, 1)
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			cacheHit = true
			addUint64Stat(&db.nodeCacheHits, 1)
			if entry.StorageInfo.storageFileID != 0 {
				addUint64Stat(&db.nodeCacheServed, 1)
				return &TrieNode{
					storageFileID: entry.StorageInfo.storageFileID,
					storageOffset: entry.StorageInfo.storageOffset,
					storageSize:   entry.StorageInfo.storageSize,
					offset:        entry.AccountOffset,
				}, true, nil
			}
		} else {
			addUint64Stat(&db.nodeCacheMisses, 1)
		}
	}

	if useNodeCache {
		addUint64Stat(&db.nodeCacheToNodeFile, 1)
		if cacheHit {
			addUint64Stat(&db.nodeCacheHitFallbackToNodeFile, 1)
		} else {
			addUint64Stat(&db.nodeCacheMissToNodeFile, 1)
		}
	}
	nodeInfo, found, err := db.prefixTree.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	node := &TrieNode{
		storageFileID: nodeInfo.storageFileID,
		storageOffset: nodeInfo.storageOffset,
		storageSize:   nodeInfo.storageSize,
		offset:        nodeInfo.accountOffset,
	}
	// accountOffset==0 is a tombstone delete for account nodes.
	if node.offset == 0 && node.storageFileID == 0 {
		return nil, false, nil
	}
	if useNodeCache {
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
	}
	return node, false, nil
}

func (db *PrefixDB) getAccountNode(key []byte) (*TrieNode, error) {
	cacheKey := string(key)
	cacheHit := false
	useNodeCache := !db.shouldBypassNodeCache(key)
	if useNodeCache {
		addUint64Stat(&db.nodeCacheLookups, 1)
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			cacheHit = true
			addUint64Stat(&db.nodeCacheHits, 1)
			if entry.AccountOffset != 0 || entry.StorageInfo.storageFileID != 0 || entry.Value != nil {
				addUint64Stat(&db.nodeCacheServed, 1)
				return &TrieNode{
					storageFileID: entry.StorageInfo.storageFileID,
					storageOffset: entry.StorageInfo.storageOffset,
					storageSize:   entry.StorageInfo.storageSize,
					offset:        entry.AccountOffset,
				}, nil
			}
		} else {
			addUint64Stat(&db.nodeCacheMisses, 1)
		}
	}

	if useNodeCache {
		addUint64Stat(&db.nodeCacheToNodeFile, 1)
		if cacheHit {
			addUint64Stat(&db.nodeCacheHitFallbackToNodeFile, 1)
		} else {
			addUint64Stat(&db.nodeCacheMissToNodeFile, 1)
		}
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
	if useNodeCache {
		db.nodeCache.StoreMetadata(cacheKey, node.offset, StorageInfo{
			storageFileID: node.storageFileID,
			storageOffset: node.storageOffset,
			storageSize:   node.storageSize,
		})
	}
	return node, nil
}

func (db *PrefixDB) openOrCreateStorageFile() error {
	db.storageFileMu.Lock()
	defer db.storageFileMu.Unlock()
	return db.openOrCreateStorageFileLocked()
}

func (db *PrefixDB) openOrCreateStorageFileLocked() error {
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
			if n, _ := fmt.Sscanf(e.Name(), "%08d", &segID); n == 1 && segID > maxSegmentID {
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
	db.storageFileMu.Lock()
	defer db.storageFileMu.Unlock()
	return db.ensureStorageCapacityLocked(need)
}

func (db *PrefixDB) ensureStorageCapacityLocked(need int64) error {
	// if need > storageMaxFileSize {
	// 	return errors.New("need size lager than storageMaxFileSize")
	// }

	if db.storageCurFile == nil {
		return db.openOrCreateStorageFileLocked()
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

// Common storage segment format: [keyLen u16][valLen u16][key][val]...
func (db *PrefixDB) serializeStorageSegment(kvs []kvPair) ([]byte, func(), int, error) {
	total := 0
	for _, v := range kvs {
		if len(v.key) > 0xFFFF {
			return nil, func() {}, 0, fmt.Errorf("key too large: %d", len(v.key))
		}
		if len(v.val) > 0xFFFF {
			return nil, func() {}, 0, fmt.Errorf("value too large: %d", len(v.val))
		}
		total += segmentedChunkEntryHeaderSize + len(v.key) + len(v.val)
	}

	buf := getDataBuffer(total)
	release := func() {
		putDataBuffer(buf)
	}
	offset := 0
	var header [segmentedChunkEntryHeaderSize]byte

	for _, v := range kvs {
		writeUint16BE(header[:2], uint16(len(v.key)))
		writeUint16BE(header[2:4], uint16(len(v.val)))
		copy(buf[offset:], header[:])
		offset += segmentedChunkEntryHeaderSize
		copy(buf[offset:], v.key)
		offset += len(v.key)
		copy(buf[offset:], v.val)
		offset += len(v.val)
	}
	return buf, release, total, nil
}

// Segmented chunk format: [key][val][keyLen u16][valLen u16]...
func serializeChunkPayload(kvs []kvPair) ([]byte, func(), int, error) {
	total := 0
	for _, v := range kvs {
		if len(v.key) > 0xFFFF {
			return nil, func() {}, 0, fmt.Errorf("key too large: %d", len(v.key))
		}
		if len(v.val) > 0xFFFF {
			return nil, func() {}, 0, fmt.Errorf("value too large for segmented chunk: %d", len(v.val))
		}
		total += segmentedChunkEntryHeaderSize + len(v.key) + len(v.val)
	}

	buf := getDataBuffer(total)
	release := func() {
		putDataBuffer(buf)
	}
	offset := 0
	for _, v := range kvs {
		copy(buf[offset:], v.key)
		offset += len(v.key)
		copy(buf[offset:], v.val)
		offset += len(v.val)
		writeUint16BE(buf[offset:offset+2], uint16(len(v.key)))
		writeUint16BE(buf[offset+2:offset+4], uint16(len(v.val)))
		offset += segmentedChunkEntryHeaderSize
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
	db.storageFileMu.Lock()
	defer db.storageFileMu.Unlock()
	if err := db.ensureStorageCapacityLocked(need); err != nil {
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

func (db *PrefixDB) persistStorageEntries(accountKey []byte, kvs []kvPair, existingFileID uint32, existingOffset int64, existingSize uint64) (uint32, int64, uint64, error) {
	if len(kvs) == 0 {
		return 0, 0, 0, nil
	}
	if isSegmentedStorage(existingFileID) {
		kvs = dedupSortedKVPairs(kvs)
		if isAccountNamedSegmentedStorage(existingFileID) {
			return db.updateAccountNamedSegmentedStorage(accountKey, kvs)
		}
		return 0, 0, 0, errors.New("legacy segmented storage pointers are no longer supported")
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
	return db.appendSegmentedStorage(accountKey, merged)
}

func estimateSegmentSize(kvs []kvPair) int {
	total := 0
	for _, kv := range kvs {
		total += segmentedChunkEntryHeaderSize + len(kv.key) + len(kv.val)
	}
	return total
}

func (db *PrefixDB) appendSegmentedStorage(accountKey []byte, kvs []kvPair) (uint32, int64, uint64, error) {
	if len(accountKey) == 0 {
		return 0, 0, 0, errors.New("account key required for segmented storage")
	}
	return db.rewriteAccountNamedSegmentedStorage(accountKey, kvs)
}

func (db *PrefixDB) rewriteAccountNamedSegmentedStorage(accountKey []byte, kvs []kvPair) (uint32, int64, uint64, error) {
	if len(accountKey) == 0 {
		return 0, 0, 0, errors.New("account key required for account-named segmented storage")
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry)
}

func (db *PrefixDB) rewriteAccountNamedSegmentedStorageWithLockHeld(accountKey []byte, kvs []kvPair) (uint32, int64, uint64, error) {
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry)
}

func (db *PrefixDB) rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath string, accountKey []byte, kvs []kvPair, entry *segmentIndexFolderLock) (uint32, int64, uint64, error) {
	if err := os.RemoveAll(folderPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, 0, 0, err
	}
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		return 0, 0, 0, err
	}
	chunkMetas, err := db.writeSegmentedChunksToFolder(folderPath, kvs)
	if err != nil {
		return 0, 0, 0, err
	}
	if err := db.writeSegmentIndexLocked(folderPath, chunkMetas, entry); err != nil {
		return 0, 0, 0, err
	}
	db.markAccountStorageFolder(accountKey)
	db.invalidateSegmentIndexLayoutForPath(folderPath)
	return segmentedStorageFlag, 0, 0, nil
}

func (db *PrefixDB) updateAccountNamedSegmentedStorage(accountKey []byte, kvs []kvPair) (uint32, int64, uint64, error) {
	if len(accountKey) == 0 {
		return 0, 0, 0, errors.New("account key required for account-named segmented storage")
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry)
		}
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if errors.Is(err, errSegmentIndexEntryNotFound) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, os.ErrNotExist) || !fileExists(indexPath) {
			return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry)
		}
		return 0, 0, 0, err
	}
	if len(metas) == 0 {
		return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry)
	}
	orderChanged := false
	metas = normalizeSegmentChunkMetasOrder(metas, &orderChanged)
	allocator := newChunkFileAllocator(metas)
	buckets, unmatched := partitionEntriesByChunks(metas, kvs)
	updated := make([]segmentChunkMeta, 0, len(metas)+len(kvs)/64+1)
	indexDirty := orderChanged
	for idx, meta := range metas {
		additions := buckets[idx]
		if len(additions) == 0 {
			updated = append(updated, meta)
			continue
		}
		chunkMetas, mutateErr := db.mutateSegmentChunk(folderPath, meta, additions, allocator)
		if mutateErr != nil {
			return 0, 0, 0, mutateErr
		}
		if len(chunkMetas) == 0 {
			indexDirty = true
			continue
		}
		if len(chunkMetas) != 1 || chunkMetas[0].FileName != meta.FileName || !bytes.Equal(chunkMetas[0].KeyStart, meta.KeyStart) {
			indexDirty = true
		}
		updated = append(updated, chunkMetas...)
	}
	// Handle unmatched KV pairs by creating new chunks
	if len(unmatched) > 0 {
		// Sort unmatched pairs to ensure proper ordering
		sortKVPairs(unmatched)
		// Create new chunks for unmatched pairs using the allocator to avoid filename conflicts
		newChunkMetas, err := db.writeSegmentedChunksToFolderWithAllocator(folderPath, unmatched, allocator)
		if err != nil {
			return 0, 0, 0, err
		}
		// Append new chunks to the updated list
		updated = append(updated, newChunkMetas...)
		indexDirty = true
	}
	if indexDirty {
		if err := db.writeSegmentIndexLocked(folderPath, updated, entry); err != nil {
			return 0, 0, 0, err
		}
		db.invalidateSegmentIndexLayoutForPath(folderPath)
	}
	db.markAccountStorageFolder(accountKey)
	return segmentedStorageFlag, 0, 0, nil
}

func (db *PrefixDB) writeSegmentedChunksToFolder(folderPath string, kvs []kvPair) ([]segmentChunkMeta, error) {
	return db.writeSegmentedChunksToFolderWithAllocator(folderPath, kvs, nil)
}

func (db *PrefixDB) writeSegmentedChunksToFolderWithAllocator(folderPath string, kvs []kvPair, allocator *chunkFileAllocator) ([]segmentChunkMeta, error) {
	chunkMetas := make([]segmentChunkMeta, 0)
	chunk := make([]kvPair, 0)
	chunkSize := 0
	chunkIdx := 0
	flushChunk := func() error {
		if len(chunk) == 0 {
			return nil
		}
		seg, release, _, err := serializeChunkPayload(chunk)
		if err != nil {
			return err
		}
		defer release()
		// Use allocator if provided to generate unique chunk filenames
		var name string
		if allocator != nil {
			name = allocator.nextName()
		} else {
			name = chunkFileNameForOrdinal(uint32(chunkIdx))
		}
		fullPath := filepath.Join(folderPath, name)
		if err := db.writeFileWithStats(fullPath, seg, 0o644, diskIOUsageStorageSeparatedLogs); err != nil {
			return err
		}
		chunkMetas = append(chunkMetas, segmentChunkMeta{
			FileName: name,
			KeyStart: cloneBytes(chunk[0].key),
		})
		chunk = make([]kvPair, 0)
		chunkSize = 0
		chunkIdx++
		return nil
	}
	for _, kv := range kvs {
		sz := segmentedChunkEntryHeaderSize + len(kv.key) + len(kv.val)
		if chunkSize+sz > db.storageChunkSize && len(chunk) > 0 {
			if err := flushChunk(); err != nil {
				return nil, err
			}
		}
		chunk = append(chunk, kv)
		chunkSize += sz
	}
	if err := flushChunk(); err != nil {
		return nil, err
	}
	if len(chunkMetas) == 0 {
		return nil, errors.New("failed to build segmented storage chunks")
	}
	return chunkMetas, nil
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

func (db *PrefixDB) updateSegmentedStorageWithLockHeld(existingFileID uint32, kvs []kvPair) (uint32, int64, uint64, error) {
	folderID := existingFileID & ^segmentedStorageFlag
	folderPath := db.segmentedFolderPath(folderID)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(metas) == 0 {
		return 0, 0, 0, fmt.Errorf("segment index missing for folder %d", folderID)
	}
	orderChanged := false
	metas = normalizeSegmentChunkMetasOrder(metas, &orderChanged)
	allocator := newChunkFileAllocator(metas)
	buckets, unmatched := partitionEntriesByChunks(metas, kvs)
	updated := make([]segmentChunkMeta, 0, len(metas)+len(kvs)/64+1)
	indexDirty := orderChanged
	for idx, meta := range metas {
		additions := buckets[idx]
		if len(additions) == 0 {
			updated = append(updated, meta)
			continue
		}
		chunkMetas, err := db.mutateSegmentChunk(folderPath, meta, additions, allocator)
		if err != nil {
			return 0, 0, 0, err
		}
		if len(chunkMetas) == 0 {
			indexDirty = true
			continue
		}
		if len(chunkMetas) != 1 || chunkMetas[0].FileName != meta.FileName || !bytes.Equal(chunkMetas[0].KeyStart, meta.KeyStart) {
			indexDirty = true
		}
		updated = append(updated, chunkMetas...)
	}
	// Handle unmatched KV pairs by creating new chunks
	if len(unmatched) > 0 {
		// Sort unmatched pairs to ensure proper ordering
		sortKVPairs(unmatched)
		// Create new chunks for unmatched pairs using the allocator to avoid filename conflicts
		newChunkMetas, err := db.writeSegmentedChunksToFolderWithAllocator(folderPath, unmatched, allocator)
		if err != nil {
			return 0, 0, 0, err
		}
		// Append new chunks to the updated list
		updated = append(updated, newChunkMetas...)
		indexDirty = true
	}
	if indexDirty {
		if err := db.writeSegmentIndexLocked(folderPath, updated, entry); err != nil {
			return 0, 0, 0, err
		}
		db.invalidateSegmentIndexLayoutForPath(folderPath)
		db.refreshSegmentIndexCacheByPathLocked(folderPath, updated)
	}
	return existingFileID, 0, 0, nil
}

// partitionEntriesByChunks assigns sorted kvs to chunk ranges using binary search
// on KeyStart boundaries.
func partitionEntriesByChunks(metas []segmentChunkMeta, kvs []kvPair) ([][]kvPair, []kvPair) {
	buckets := make([][]kvPair, len(metas))
	var unmatched []kvPair
	if len(metas) == 0 || len(kvs) == 0 {
		return buckets, unmatched
	}
	idx := 0
	for _, kv := range kvs {
		idx = findChunkIndexForKey(metas, kv.key, idx)
		if idx < 0 {
			unmatched = append(unmatched, kv)
			continue
		}
		buckets[idx] = append(buckets[idx], kv)
	}
	return buckets, unmatched
}

func findChunkIndexForKey(metas []segmentChunkMeta, key []byte, start int) int {
	if len(metas) == 0 {
		return -1
	}
	if len(key) == 0 {
		return 0
	}
	idx := sort.Search(len(metas), func(i int) bool {
		startKey := metas[i].KeyStart
		if len(startKey) == 0 {
			return false
		}
		return compareSegmentIndexKeyStarts(key, startKey) < 0
	})
	if idx == 0 {
		if len(metas[0].KeyStart) == 0 {
			return 0
		}
		if compareSegmentIndexKeyStarts(key, metas[0].KeyStart) < 0 {
			return -1
		}
		return 0
	}
	selected := idx - 1
	if len(metas[selected].KeyStart) == 0 {
		return selected
	}
	if compareSegmentIndexKeyStarts(key, metas[selected].KeyStart) < 0 {
		return -1
	}
	if idx < len(metas) {
		nextStart := metas[idx].KeyStart
		if len(nextStart) > 0 && compareSegmentIndexKeyStarts(key, nextStart) >= 0 {
			return -1
		}
	}
	_ = start
	return selected
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
	return fmt.Sprintf("%04d.dat", ordinal)
}

func parseChunkOrdinal(name string) int {
	const suffix = ".dat"
	if len(name) <= len(suffix) {
		return -1
	}
	if name[len(name)-len(suffix):] != suffix {
		return -1
	}
	num := name[:len(name)-len(suffix)]
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

func (db *PrefixDB) mutateSegmentChunk(folderPath string, meta segmentChunkMeta, additions []kvPair, allocator *chunkFileAllocator) ([]segmentChunkMeta, error) {
	if len(additions) == 0 {
		return []segmentChunkMeta{meta}, nil
	}
	chunkPath := filepath.Join(folderPath, meta.FileName)
	info, err := os.Stat(chunkPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			chunkMetas, _, rewriteErr := db.rewriteChunkWithDedup(folderPath, meta, additions, allocator, []kvPair{}, nil)
			if rewriteErr != nil {
				return nil, rewriteErr
			}
			fmt.Printf("prefixdb: recreated missing chunk %s in folder %s during write\n", meta.FileName, folderPath)
			return chunkMetas, nil
		}
		return nil, err
	}
	currentSize := info.Size()
	appendBytes := payloadSize(additions)
	if appendBytes == 0 {
		return []segmentChunkMeta{meta}, nil
	}
	if trigger := int64(db.segmentedChunkTriggerSize()); trigger > 0 && currentSize+int64(appendBytes) > trigger {
		chunkMetas, _, err := db.rewriteChunkWithDedup(folderPath, meta, additions, allocator, nil, nil)
		return chunkMetas, err
	}
	if err := db.appendChunkFile(chunkPath, additions, currentSize); err != nil {
		return nil, err
	}
	adjustMetaRange(&meta, additions)
	return []segmentChunkMeta{meta}, nil
}

func (db *PrefixDB) appendChunkFile(path string, additions []kvPair, currentSize int64) error {
	if len(additions) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	seg, release, _, err := serializeChunkPayload(additions)
	if err != nil {
		return err
	}
	defer release()
	if _, err := f.WriteAt(seg, currentSize); err != nil {
		return err
	}
	db.addDiskWrite(diskIOUsageStorageSeparatedLogs, len(seg))
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
	// Ranges are KeyStart-only.
}

func (db *PrefixDB) rewriteChunkWithDedup(folderPath string, meta segmentChunkMeta, additions []kvPair, allocator *chunkFileAllocator, existing []kvPair, backing *bufferLease) ([]segmentChunkMeta, bool, error) {
	var err error
	if existing == nil {
		existing, backing, err = db.readSegmentChunkFileWithUsageByPath(folderPath, meta.FileName, diskIOUsageStorageSeparatedLogs)
		if err != nil {
			return nil, false, err
		}
		// ChunkSize is no longer tracked in meta - get from filesystem if needed
		db.addCommitOldKVReadStats(len(existing), 0)
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
	for idx, chunk := range chunks {
		name := meta.FileName
		if idx > 0 {
			name = allocator.nextName()
		}
		if _, err = db.writeChunkFile(folderPath, name, chunk); err != nil {
			return nil, false, err
		}
		result = append(result, segmentChunkMeta{
			FileName: name,
			KeyStart: cloneBytes(chunk[0].key),
		})
	}
	return result, false, nil
}

func (db *PrefixDB) repairMissingChunkFile(folderID uint32, fileName string) error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	folderPath := db.segmentedFolderPath(folderID)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
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
	if err := db.writeSegmentIndexLocked(folderPath, filtered, entry); err != nil {
		return err
	}
	db.invalidateSegmentIndexLayoutForPath(folderPath)
	db.refreshSegmentIndexCacheByPathLocked(folderPath, filtered)
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
	var size int
	for i := 0; i < len(entries); i++ {
		entrySize := segmentedChunkEntryHeaderSize + len(entries[i].key) + len(entries[i].val)
		if size+entrySize > limit && i > start {
			chunk := entries[start:i:i]
			chunks = append(chunks, chunk)
			start = i
			size = 0
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
		total += int64(segmentedChunkEntryHeaderSize + len(kv.key) + len(kv.val))
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
	seg, release, chunkSize, err := serializeChunkPayload(entries)
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

func (db *PrefixDB) segmentedFolderPath(id uint32) string {
	return filepath.Join(db.storageDir, fmt.Sprintf("%08d", id))
}

func (db *PrefixDB) segmentedFolderPathForAccount(accountKey []byte) string {
	return filepath.Join(db.storageDir, hex.EncodeToString(accountKey))
}

func (db *PrefixDB) managedAccountKeyForFolderPath(folderPath string) ([]byte, bool) {
	if db == nil {
		return nil, false
	}
	name := filepath.Base(folderPath)
	accountKey, err := hex.DecodeString(name)
	if err != nil {
		return nil, false
	}
	if !db.isAccountStorageFolderManaged(accountKey) {
		return nil, false
	}
	return accountKey, true
}

func isAccountNamedSegmentedStorage(fileID uint32) bool {
	return fileID == segmentedStorageFlag
}

func (db *PrefixDB) markAccountStorageFolder(accountKey []byte) {
	if db == nil || db.accountFolderSet == nil {
		return
	}
	db.accountFolderSet.add(accountKey)
}

func (db *PrefixDB) isAccountStorageFolderManaged(accountKey []byte) bool {
	if db == nil || db.accountFolderSet == nil {
		return false
	}
	return db.accountFolderSet.maybeContains(accountKey)
}

func (db *PrefixDB) clearAccountStorageFolder(accountKey []byte) {
	if db == nil || db.accountFolderSet == nil {
		return
	}
	db.accountFolderSet.remove(accountKey)
}

func shouldFallbackMissingFolderRead(err error) bool {
	return err != nil && errors.Is(err, os.ErrNotExist)
}

func (db *PrefixDB) primeAccountFolderSetFromStorageDir() error {
	if db == nil || db.accountFolderSet == nil {
		return nil
	}
	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		accountKey, decodeErr := hex.DecodeString(name)
		if decodeErr != nil || len(accountKey) == 0 {
			continue
		}
		db.markAccountStorageFolder(accountKey)
	}
	return nil
}

func shouldSkipAccountEntryPointerUpdate(existingFileID uint32, fileID uint32, off int64, size uint64) bool {
	return isAccountNamedSegmentedStorage(existingFileID) && isAccountNamedSegmentedStorage(fileID) && off == 0 && size == 0
}

func (db *PrefixDB) lockSegmentIndexFolder(folderPath string) func() {
	_, unlock := db.lockSegmentIndexFolderReadEntry(folderPath)
	return unlock
}

func (db *PrefixDB) readSegmentIndexNoCacheByPathLocked(folderPath string) ([]segmentChunkMeta, error) {
	metas, _, err := db.readSegmentIndexLockedInternalByPath(folderPath, false)
	return metas, err
}

func (db *PrefixDB) lockSegmentIndexFolderReadEntry(folderPath string) (*segmentIndexFolderLock, func()) {
	db.segmentIndexFolderLocksMu.Lock()
	if db.segmentIndexFolderLocks == nil {
		db.segmentIndexFolderLocks = make(map[string]*segmentIndexFolderLock)
	}
	entry := db.segmentIndexFolderLocks[folderPath]
	if entry == nil {
		entry = &segmentIndexFolderLock{}
		db.segmentIndexFolderLocks[folderPath] = entry
	}
	entry.refs++
	db.segmentIndexFolderLocksMu.Unlock()

	entry.mu.RLock()
	return entry, func() {
		entry.mu.RUnlock()
		db.segmentIndexFolderLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(db.segmentIndexFolderLocks, folderPath)
		}
		db.segmentIndexFolderLocksMu.Unlock()
	}
}

func (db *PrefixDB) lockSegmentIndexFolderEntry(folderPath string) (*segmentIndexFolderLock, func()) {
	db.segmentIndexFolderLocksMu.Lock()
	if db.segmentIndexFolderLocks == nil {
		db.segmentIndexFolderLocks = make(map[string]*segmentIndexFolderLock)
	}
	entry := db.segmentIndexFolderLocks[folderPath]
	if entry == nil {
		entry = &segmentIndexFolderLock{}
		db.segmentIndexFolderLocks[folderPath] = entry
	}
	entry.refs++
	db.segmentIndexFolderLocksMu.Unlock()

	entry.mu.Lock()
	return entry, func() {
		entry.mu.Unlock()
		db.segmentIndexFolderLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(db.segmentIndexFolderLocks, folderPath)
		}
		db.segmentIndexFolderLocksMu.Unlock()
	}
}

func (db *PrefixDB) segmentIndexGenerationLocked(folderPath string) uint64 {
	entry, unlock := db.lockSegmentIndexFolderReadEntry(folderPath)
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

func (db *PrefixDB) readSegmentIndexWithGenByPath(folderPath string, useLRU bool) ([]segmentChunkMeta, uint64, error) {
	entry, unlock := db.lockSegmentIndexFolderReadEntry(folderPath)
	defer unlock()
	gen := atomic.LoadUint64(&entry.gen)
	metas, _, err := db.readSegmentIndexLockedInternalByPath(folderPath, useLRU)
	return metas, gen, err
}

func (db *PrefixDB) readSegmentIndexWithGen(folderID uint32, useLRU bool) ([]segmentChunkMeta, uint64, error) {
	return db.readSegmentIndexWithGenByPath(db.segmentedFolderPath(folderID), useLRU)
}

func (db *PrefixDB) readSegmentIndexNoCacheWithGen(folderID uint32) ([]segmentChunkMeta, uint64, error) {
	return db.readSegmentIndexWithGen(folderID, false)
}

func level2IndexFilePath(folderPath string, metaID uint32) string {
	return filepath.Join(folderPath, fmt.Sprintf(segmentIndexLevel2Pattern, metaID))
}

func segmentChunkMetaCanUseCompactEncoding(meta segmentChunkMeta) bool {
	if len(meta.KeyStart) > segmentIndexKeyStartMaxBytes {
		return false
	}
	return parseChunkOrdinal(meta.FileName) >= 0
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
		return segmentIndexFlatEntryBytes
	}
	return 2 + len(meta.FileName) + 2 + len(meta.KeyStart)
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
		if buf, err = appendFixedSegmentIndexKeyStart(buf, meta.KeyStart); err != nil {
			return nil, err
		}
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

func (db *PrefixDB) encodeSegmentIndexFileData(data []byte) ([]byte, error) {
	if db == nil || !db.segmentIndexCompression || len(data) <= segmentIndexCompressionMinSize {
		return data, nil
	}
	return encodeCompressedMetadataBlock(data)
}

func (db *PrefixDB) decodeSegmentIndexFileData(path string, data []byte) ([]byte, error) {
	raw, _, err := maybeDecodeCompressedMetadataBlock(data)
	if err != nil {
		return nil, fmt.Errorf("decode compressed segment index %s failed: %w", path, err)
	}
	return raw, nil
}

func (db *PrefixDB) readSegmentIndexFile(path string) ([]byte, error) {
	data, err := db.readFileWithStats(path, diskIOUsageStorageSegmentIndex)
	if err != nil {
		return nil, err
	}
	return db.decodeSegmentIndexFileData(path, data)
}

func (db *PrefixDB) writeSegmentIndexFileIfChanged(path string, data []byte) error {
	encoded, err := db.encodeSegmentIndexFileData(data)
	if err != nil {
		return err
	}
	return writeFileIfChanged(db, path, encoded)
}

func (db *PrefixDB) writeSegmentIndexFileAtomic(path string, data []byte) error {
	encoded, err := db.encodeSegmentIndexFileData(data)
	if err != nil {
		return err
	}
	return writeFileAtomic(db, path, encoded)
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
	if a.FileName != b.FileName {
		return false
	}
	return bytes.Equal(a.KeyStart, b.KeyStart)
}

func compareSegmentIndexKeyStarts(a, b []byte) int {
	return bytes.Compare(a, b)
}

var zeroSegmentIndexKeyPadding [segmentIndexKeyStartMaxBytes]byte

func appendFixedSegmentIndexKeyStart(buf []byte, key []byte) ([]byte, error) {
	if len(key) > segmentIndexKeyStartMaxBytes {
		return nil, fmt.Errorf("segment key start too large: %d", len(key))
	}
	buf = append(buf, byte(len(key)))
	if len(key) > 0 {
		buf = append(buf, key...)
	}
	if pad := segmentIndexKeyStartMaxBytes - len(key); pad > 0 {
		buf = append(buf, zeroSegmentIndexKeyPadding[:pad]...)
	}
	return buf, nil
}

func decodeFixedSegmentIndexKeyStart(data []byte) ([]byte, int, error) {
	if len(data) < segmentIndexFixedKeyFieldBytes {
		return nil, 0, io.ErrUnexpectedEOF
	}
	keyLen := int(data[0])
	if keyLen > segmentIndexKeyStartMaxBytes {
		return nil, 0, fmt.Errorf("invalid fixed segment key length %d", keyLen)
	}
	if keyLen == 0 {
		return nil, segmentIndexFixedKeyFieldBytes, nil
	}
	return data[1 : 1+keyLen], segmentIndexFixedKeyFieldBytes, nil
}

func compareSearchKeyToEncodedFixedSegmentIndexKey(search []byte, encoded []byte) (int, error) {
	if len(encoded) < segmentIndexFixedKeyFieldBytes {
		return 0, io.ErrUnexpectedEOF
	}
	keyLen := int(encoded[0])
	if keyLen > segmentIndexKeyStartMaxBytes {
		return 0, fmt.Errorf("invalid fixed segment key length %d", keyLen)
	}
	keyData := encoded[1 : 1+keyLen]
	return compareSegmentIndexKeyStarts(search, keyData), nil
}

func flatSegmentIndexCount(data []byte) (int, uint16, error) {
	if len(data) < 12 {
		return 0, 0, fmt.Errorf("corrupted compact segment index header")
	}
	if binary.BigEndian.Uint32(data[:4]) != segmentIndexFlatMagic {
		return 0, 0, fmt.Errorf("unsupported segment index format")
	}
	version := binary.BigEndian.Uint16(data[4:6])
	if version != segmentIndexFlatVersion {
		return 0, 0, fmt.Errorf("unsupported flat index version %d", version)
	}
	count := int(binary.BigEndian.Uint32(data[8:12]))
	return count, version, nil
}

func flatSegmentIndexEntryOffset(version uint16, idx int) (int, error) {
	if idx < 0 {
		return 0, fmt.Errorf("invalid flat segment index entry index %d", idx)
	}
	if version != segmentIndexFlatVersion {
		return 0, fmt.Errorf("unsupported flat index version %d", version)
	}
	return 12 + idx*segmentIndexFlatEntryBytes, nil
}

func flatSegmentIndexMetaAt(data []byte, version uint16, idx int) (segmentChunkMeta, error) {
	offset, err := flatSegmentIndexEntryOffset(version, idx)
	if err != nil {
		return segmentChunkMeta{}, err
	}
	if offset+segmentIndexFlatEntryBytes > len(data) {
		return segmentChunkMeta{}, io.ErrUnexpectedEOF
	}
	ordinal := readUint32BE(data[offset : offset+4])
	keyStart, _, err := decodeFixedSegmentIndexKeyStart(data[offset+4 : offset+4+segmentIndexFixedKeyFieldBytes])
	if err != nil {
		return segmentChunkMeta{}, err
	}
	return segmentChunkMeta{FileName: chunkFileNameForOrdinal(ordinal), KeyStart: keyStart}, nil
}

func selectFixedFlatSegmentIndexMeta(data []byte, key []byte) (*segmentChunkMeta, error) {
	count, version, err := flatSegmentIndexCount(data)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	idx := sort.Search(count, func(i int) bool {
		offset := 12 + i*segmentIndexFlatEntryBytes + 4
		cmp, cmpErr := compareSearchKeyToEncodedFixedSegmentIndexKey(key, data[offset:offset+segmentIndexFixedKeyFieldBytes])
		if cmpErr != nil {
			return true
		}
		return cmp < 0
	})
	if idx == 0 {
		cmp, cmpErr := compareSearchKeyToEncodedFixedSegmentIndexKey(key, data[16:16+segmentIndexFixedKeyFieldBytes])
		if cmpErr != nil {
			return nil, cmpErr
		}
		if cmp < 0 {
			return nil, nil
		}
		meta, err := flatSegmentIndexMetaAt(data, version, 0)
		if err != nil {
			return nil, err
		}
		return &meta, nil
	}
	selectedIdx := idx - 1
	selected, err := flatSegmentIndexMetaAt(data, version, selectedIdx)
	if err != nil {
		return nil, err
	}
	if compareSegmentIndexKeyStarts(key, selected.KeyStart) < 0 {
		return nil, nil
	}
	if idx < count {
		nextOffset := 12 + idx*segmentIndexFlatEntryBytes + 4
		cmp, cmpErr := compareSearchKeyToEncodedFixedSegmentIndexKey(key, data[nextOffset:nextOffset+segmentIndexFixedKeyFieldBytes])
		if cmpErr != nil {
			return nil, cmpErr
		}
		if cmp >= 0 {
			return nil, nil
		}
	}
	return &selected, nil
}

func lessSegmentChunkMeta(a, b segmentChunkMeta) bool {
	cmp := compareSegmentIndexKeyStarts(a.KeyStart, b.KeyStart)
	if cmp != 0 {
		return cmp < 0
	}
	return a.FileName < b.FileName
}

func isSegmentChunkMetasOrdered(metas []segmentChunkMeta) bool {
	for i := 1; i < len(metas); i++ {
		if lessSegmentChunkMeta(metas[i], metas[i-1]) {
			return false
		}
	}
	return true
}

func normalizeSegmentChunkMetasOrder(metas []segmentChunkMeta, changed *bool) []segmentChunkMeta {
	if len(metas) <= 1 {
		if changed != nil {
			*changed = false
		}
		return metas
	}
	if isSegmentChunkMetasOrdered(metas) {
		if changed != nil {
			*changed = false
		}
		return metas
	}
	sorted := cloneSegmentChunkMetas(metas)
	sort.Slice(sorted, func(i, j int) bool {
		return lessSegmentChunkMeta(sorted[i], sorted[j])
	})
	if changed != nil {
		*changed = true
	}
	return sorted
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
	idx := upperBoundSegmentIndexL1Entries(entries, key)
	if idx == 0 {
		return nil
	}
	return &entries[idx-1]
}

func decodeSegmentIndexBuffer(data []byte, metas *[]segmentChunkMeta, arena *[]byte, appendExisting bool, chunkDir string) error {
	count, version, err := flatSegmentIndexCount(data)
	if err != nil {
		return err
	}
	cursor := 12
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
	if version != segmentIndexFlatVersion {
		return fmt.Errorf("unsupported flat index version %d", version)
	}
	for i := 0; i < count; i++ {
		if cursor+segmentIndexFlatEntryBytes > len(data) {
			return io.ErrUnexpectedEOF
		}
		fileName := chunkFileNameForOrdinal(readUint32BE(data[cursor : cursor+4]))
		cursor += 4
		start, n, err := decodeFixedSegmentIndexKeyStart(data[cursor : cursor+segmentIndexFixedKeyFieldBytes])
		if err != nil {
			return err
		}
		cursor += n
		meta := segmentChunkMeta{FileName: fileName, KeyStart: start}
		_ = chunkDir
		*metas = append(*metas, meta)
	}
	return nil
}

func (db *PrefixDB) loadSegmentIndexLayout(folderPath string) (segmentIndexLayout, error) {
	if db.storageIndexCache != nil {
		if layout, ok := db.storageIndexCache.GetLayoutByPath(folderPath); ok {
			return layout, nil
		}
	}

	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	data, err := db.readSegmentIndexFile(indexPath)
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
	if db.storageIndexCache != nil {
		db.storageIndexCache.AddLayoutByPath(folderPath, layout)
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
		if cursor+segmentIndexFixedKeyFieldBytes > len(data) {
			return segmentIndexLayout{}, io.ErrUnexpectedEOF
		}
		start, n, err := decodeFixedSegmentIndexKeyStart(data[cursor : cursor+segmentIndexFixedKeyFieldBytes])
		if err != nil {
			return segmentIndexLayout{}, err
		}
		cursor += n
		layout.entries = append(layout.entries, segmentIndexL1Entry{
			MetaID:     metaID,
			KeyStart:   start,
			ChunkCount: chunkCount,
		})
	}
	if layout.nextMetaID == 0 {
		layout.nextMetaID = uint32(len(layout.entries)) + 1
	}
	// Read-path hardening: keep top-level entries ordered even if on-disk
	// layout was produced by an older/buggy writer. Key lookup relies on
	// binary search and requires monotonic KeyStart order.
	layout.entries = normalizeSegmentIndexL1EntriesOrder(layout.entries, nil)
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
		if buf, err = appendFixedSegmentIndexKeyStart(buf, entry.KeyStart); err != nil {
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
		if !bytes.Equal(a[i].KeyStart, b[i].KeyStart) {
			return false
		}
	}
	return true
}

func lessSegmentIndexL1Entry(a, b segmentIndexL1Entry) bool {
	cmp := compareSegmentIndexKeyStarts(a.KeyStart, b.KeyStart)
	if cmp != 0 {
		return cmp < 0
	}
	if a.MetaID != b.MetaID {
		return a.MetaID < b.MetaID
	}
	return a.ChunkCount < b.ChunkCount
}

func isSegmentIndexL1EntriesOrdered(entries []segmentIndexL1Entry) bool {
	for i := 1; i < len(entries); i++ {
		if lessSegmentIndexL1Entry(entries[i], entries[i-1]) {
			return false
		}
	}
	return true
}

func normalizeSegmentIndexL1EntriesOrder(entries []segmentIndexL1Entry, changed *bool) []segmentIndexL1Entry {
	if len(entries) <= 1 {
		if changed != nil {
			*changed = false
		}
		return entries
	}
	if isSegmentIndexL1EntriesOrdered(entries) {
		if changed != nil {
			*changed = false
		}
		return entries
	}
	sorted := make([]segmentIndexL1Entry, len(entries))
	for i := range entries {
		sorted[i] = segmentIndexL1Entry{
			MetaID:     entries[i].MetaID,
			ChunkCount: entries[i].ChunkCount,
			KeyStart:   cloneBytes(entries[i].KeyStart),
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return lessSegmentIndexL1Entry(sorted[i], sorted[j])
	})
	if changed != nil {
		*changed = true
	}
	return sorted
}

func (db *PrefixDB) writeSegmentIndex(folderPath string, metas []segmentChunkMeta) error {
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	return db.writeSegmentIndexLocked(folderPath, metas, entry)
}

func (db *PrefixDB) writeSegmentIndexLocked(folderPath string, metas []segmentChunkMeta, entry *segmentIndexFolderLock) error {
	// Writers must observe the on-disk top-level layout so external rewrites of
	// index.meta cannot leave us reusing stale cached ordering information.
	db.invalidateSegmentIndexLayoutForPath(folderPath)
	metas = normalizeSegmentChunkMetasOrder(metas, nil)
	// Capture the previous layout so we can remove stale L2 files without scanning
	// the whole folder (which may contain many *.dat files).
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
		db.invalidateSegmentIndexLayoutForPath(folderPath)
		db.bumpSegmentIndexGenerationLocked(entry)
		db.refreshSegmentIndexCacheByPathLocked(folderPath, nil)
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
		if err := db.writeSegmentIndexFileIfChanged(indexPath, buf); err != nil {
			return err
		}
		db.invalidateSegmentIndexLayoutForPath(folderPath)
		db.bumpSegmentIndexGenerationLocked(entry)
		db.refreshSegmentIndexCacheByPathLocked(folderPath, metas)
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
		_ = reuseLayoutGrouping
		_ = groupOffsets
		buf, err := encodeSegmentChunkMetas(group)
		if err != nil {
			return err
		}
		// Incremental write: only update this L2 index file when payload changed.
		if err := db.writeSegmentIndexFileIfChanged(path, buf); err != nil {
			return err
		}
		entry := segmentIndexL1Entry{
			MetaID:     idAssignments[idx],
			ChunkCount: uint32(len(group)),
		}
		entry.KeyStart = cloneBytes(group[0].KeyStart)
		newEntries = append(newEntries, entry)
	}
	newLayout := segmentIndexLayout{
		mode:       indexLayoutMultiLevel,
		entries:    newEntries,
		nextMetaID: nextID,
	}
	topOrderChanged := false
	newLayout.entries = normalizeSegmentIndexL1EntriesOrder(newLayout.entries, &topOrderChanged)
	needTopUpdate := layout.mode != indexLayoutMultiLevel || !layoutEntriesEqual(layout.entries, newLayout.entries) || layout.nextMetaID != newLayout.nextMetaID || topOrderChanged
	if needTopUpdate {
		buf, err := encodeTopLevelIndex(newLayout)
		if err != nil {
			return err
		}
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := db.writeSegmentIndexFileIfChanged(indexPath, buf); err != nil {
			return err
		}
		db.invalidateSegmentIndexLayoutForPath(folderPath)
	}
	// Even if the top-level layout didn't change, L2 files may have been rewritten.
	db.bumpSegmentIndexGenerationLocked(entry)
	db.refreshSegmentIndexCacheByPathLocked(folderPath, metas)
	// Remove only those L2 files that were previously referenced but are no longer
	// needed. This avoids scanning the whole folder (which can be huge).
	if len(oldEntries) > 0 {
		if err := removeStaleLevel2IndexFiles(folderPath, oldEntries, keep); err != nil {
			return err
		}
	}
	return nil
}

func buildLayoutGroupsFromMetas(layout segmentIndexLayout, metas []segmentChunkMeta) ([][]segmentChunkMeta, bool) {
	if layout.mode != indexLayoutMultiLevel || len(layout.entries) == 0 {
		return nil, false
	}
	sum := 0
	for _, entry := range layout.entries {
		sum += int(entry.ChunkCount)
	}
	if sum != len(metas) {
		return nil, false
	}
	groups := make([][]segmentChunkMeta, 0, len(layout.entries))
	off := 0
	for _, entry := range layout.entries {
		cnt := int(entry.ChunkCount)
		groups = append(groups, metas[off:off+cnt])
		off += cnt
	}
	return groups, true
}

func applyGroupReplacement(group []segmentChunkMeta, oldFileName string, replacement []segmentChunkMeta) ([]segmentChunkMeta, bool) {
	idx := -1
	for i := range group {
		if group[i].FileName == oldFileName {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, false
	}
	updated := make([]segmentChunkMeta, 0, len(group)-1+len(replacement))
	updated = append(updated, group[:idx]...)
	if len(replacement) > 0 {
		updated = append(updated, replacement...)
	}
	if idx+1 < len(group) {
		updated = append(updated, group[idx+1:]...)
	}
	return updated, true
}

func buildUpdatedSegmentChunkMetas(metas []segmentChunkMeta, replacements map[string][]segmentChunkMeta) ([]segmentChunkMeta, bool) {
	if len(replacements) == 0 {
		return cloneSegmentChunkMetas(metas), false
	}
	updated := make([]segmentChunkMeta, 0, len(metas))
	changed := false
	for i := range metas {
		if repl, ok := replacements[metas[i].FileName]; ok {
			changed = true
			if len(repl) > 0 {
				updated = append(updated, cloneSegmentChunkMetas(repl)...)
			}
			continue
		}
		updated = append(updated, metas[i])
	}
	return updated, changed
}

func (db *PrefixDB) writeSegmentIndexIncrementalGC(folderPath string, latest []segmentChunkMeta, replacements map[string][]segmentChunkMeta) (bool, error) {
	if len(replacements) == 0 {
		return true, nil
	}
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	return db.writeSegmentIndexIncrementalGCLocked(folderPath, latest, replacements, entry)
}

func (db *PrefixDB) writeSegmentIndexIncrementalGCLocked(folderPath string, latest []segmentChunkMeta, replacements map[string][]segmentChunkMeta, entry *segmentIndexFolderLock) (bool, error) {
	if len(replacements) == 0 {
		return true, nil
	}

	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return false, err
	}
	current, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
	if err != nil {
		return false, err
	}
	groups, ok := buildLayoutGroupsFromMetas(layout, current)
	if !ok {
		updated, changed := buildUpdatedSegmentChunkMetas(current, replacements)
		if !changed {
			return false, nil
		}
		if err := db.writeSegmentIndexLocked(folderPath, updated, entry); err != nil {
			return false, err
		}
		return true, nil
	}

	affected := make(map[int]struct{}, len(replacements))
	for oldFileName, replacement := range replacements {
		found := false
		for groupIdx := range groups {
			updated, hit := applyGroupReplacement(groups[groupIdx], oldFileName, replacement)
			if !hit {
				continue
			}
			groups[groupIdx] = updated
			affected[groupIdx] = struct{}{}
			found = true
			break
		}
		if !found {
			updated, changed := buildUpdatedSegmentChunkMetas(current, replacements)
			if !changed {
				return false, nil
			}
			if err := db.writeSegmentIndexLocked(folderPath, updated, entry); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	oldEntries := layout.entries
	newEntries := make([]segmentIndexL1Entry, 0, len(layout.entries))
	keep := make(map[uint32]struct{}, len(layout.entries))
	for idx := range groups {
		if len(groups[idx]) == 0 {
			continue
		}
		if _, touch := affected[idx]; touch {
			buf, err := encodeSegmentChunkMetas(groups[idx])
			if err != nil {
				return false, err
			}
			if err := db.writeSegmentIndexFileIfChanged(level2IndexFilePath(folderPath, layout.entries[idx].MetaID), buf); err != nil {
				return false, err
			}
		}
		e := segmentIndexL1Entry{
			MetaID:     layout.entries[idx].MetaID,
			ChunkCount: uint32(len(groups[idx])),
			KeyStart:   cloneBytes(groups[idx][0].KeyStart),
		}
		newEntries = append(newEntries, e)
		keep[e.MetaID] = struct{}{}
	}

	newLayout := segmentIndexLayout{mode: indexLayoutMultiLevel, entries: newEntries, nextMetaID: layout.nextMetaID}
	topOrderChanged := false
	newLayout.entries = normalizeSegmentIndexL1EntriesOrder(newLayout.entries, &topOrderChanged)
	needTopUpdate := !layoutEntriesEqual(layout.entries, newLayout.entries) || layout.nextMetaID != newLayout.nextMetaID || topOrderChanged
	if needTopUpdate {
		buf, err := encodeTopLevelIndex(newLayout)
		if err != nil {
			return false, err
		}
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := db.writeSegmentIndexFileIfChanged(indexPath, buf); err != nil {
			return false, err
		}
		db.invalidateSegmentIndexLayoutForPath(folderPath)
	}

	db.bumpSegmentIndexGenerationLocked(entry)
	if len(oldEntries) > 0 {
		if err := removeStaleLevel2IndexFiles(folderPath, oldEntries, keep); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (db *PrefixDB) invalidateSegmentIndexCacheByPath(folderPath string) {
	unlock := db.lockSegmentIndexFolder(folderPath)
	defer unlock()
	if folderPath == "" {
		return
	}
	db.segmentIndexMu.Lock()
	defer db.segmentIndexMu.Unlock()
	if db.storageIndexFolderPath == folderPath {
		db.storageIndexFolderPath = ""
		db.storageIndexMetas = nil
		db.storageIndexReusable = true
		db.storageIndexArena = nil
	}
	if db.storageIndexPartialFolderPath == folderPath {
		db.storageIndexPartialFolderPath = ""
		db.storageIndexPartialMetaID = 0
		db.storageIndexPartialMetas = nil
		db.storageIndexPartialReusable = true
		db.storageIndexPartialArena = nil
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.RemoveByPath(folderPath)
		db.storageIndexCache.RemoveLayoutByPath(folderPath)
	}
}

func (db *PrefixDB) invalidateSegmentIndexCache(folderID uint32) {
	db.invalidateSegmentIndexCacheByPath(db.segmentedFolderPath(folderID))
}

func (db *PrefixDB) refreshSegmentIndexCacheByPath(folderPath string, metas []segmentChunkMeta) {
	unlock := db.lockSegmentIndexFolder(folderPath)
	defer unlock()
	db.refreshSegmentIndexCacheByPathLocked(folderPath, metas)
}

func (db *PrefixDB) refreshSegmentIndexCacheByPathLocked(folderPath string, metas []segmentChunkMeta) {
	if folderPath == "" {
		return
	}
	cloned := cloneSegmentChunkMetas(metas)
	var layout segmentIndexLayout
	layoutReady := false
	if canUseCompactSegmentEncoding(cloned) && estimateSegmentIndexSize(cloned) <= segmentIndexMultiLevelThreshold {
		if flatData, err := encodeSegmentChunkMetas(cloned); err == nil {
			layout = segmentIndexLayout{mode: indexLayoutFlat, nextMetaID: 1, flatData: flatData}
			layoutReady = true
		}
	}
	db.segmentIndexMu.Lock()
	defer db.segmentIndexMu.Unlock()
	if db.storageIndexFolderPath == folderPath {
		db.storageIndexFolderPath = folderPath
		db.storageIndexMetas = cloneSegmentChunkMetas(cloned)
		db.storageIndexReusable = true
		db.storageIndexArena = nil
	}
	if db.storageIndexPartialFolderPath == folderPath {
		db.storageIndexPartialFolderPath = ""
		db.storageIndexPartialMetaID = 0
		db.storageIndexPartialMetas = nil
		db.storageIndexPartialReusable = true
		db.storageIndexPartialArena = nil
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.AddByPath(folderPath, cloned)
		if layoutReady {
			db.storageIndexCache.AddLayoutByPath(folderPath, layout)
		} else {
			db.storageIndexCache.RemoveLayoutByPath(folderPath)
		}
	}
}

func (db *PrefixDB) refreshSegmentIndexCache(folderID uint32, metas []segmentChunkMeta) {
	db.refreshSegmentIndexCacheByPath(db.segmentedFolderPath(folderID), metas)
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

func (db *PrefixDB) readSegmentIndexLockedInternalByPath(folderPath string, useLRU bool) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	if useLRU && db.storageIndexCache != nil {
		if metas, ok := db.storageIndexCache.GetByPath(folderPath); ok {
			return metas, segmentIndexLookupSourceL1Cache, nil
		}
	}
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return nil, segmentIndexLookupSourceNoCache, err
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
			data, err := db.readSegmentIndexFile(level2IndexFilePath(folderPath, entry.MetaID))
			if err != nil {
				return nil, segmentIndexLookupSourceNoCache, err
			}
			appendExisting := idx != 0
			if err := decodeSegmentIndexBuffer(data, &metas, &arena, appendExisting, folderPath); err != nil {
				return nil, segmentIndexLookupSourceNoCache, err
			}
		}
	} else {
		data := layout.flatData
		if len(data) == 0 {
			indexPath := filepath.Join(folderPath, segmentIndexFileName)
			data, err = db.readSegmentIndexFile(indexPath)
			if err != nil {
				return nil, segmentIndexLookupSourceNoCache, err
			}
		}
		metas = nil
		var arena []byte
		if err := decodeSegmentIndexBuffer(data, &metas, &arena, false, folderPath); err != nil {
			return nil, segmentIndexLookupSourceNoCache, err
		}
	}
	estimatedSize := estimateSegmentIndexSize(metas)
	if useLRU && estimatedSize >= segmentIndexCacheThresholdBytes && db.storageIndexCache != nil {
		db.storageIndexCache.AddByPath(folderPath, metas)
	}
	return metas, segmentIndexLookupSourceNoCache, nil
}

func (db *PrefixDB) readSegmentIndexNoCache(folderID uint32) ([]segmentChunkMeta, error) {
	unlock := db.lockSegmentIndexFolder(db.segmentedFolderPath(folderID))
	defer unlock()
	metas, _, err := db.readSegmentIndexLockedInternalByPath(db.segmentedFolderPath(folderID), false)
	return metas, err
}

func (db *PrefixDB) readSegmentIndexNoCacheByPath(folderPath string) ([]segmentChunkMeta, error) {
	unlock := db.lockSegmentIndexFolder(folderPath)
	defer unlock()
	metas, _, err := db.readSegmentIndexLockedInternalByPath(folderPath, false)
	return metas, err
}

func (db *PrefixDB) readSegmentIndexForKeyByPath(folderPath string, key []byte) ([]segmentChunkMeta, error) {
	metas, _, err := db.readSegmentIndexForKeyByPathWithSource(folderPath, key)
	return metas, err
}

func (db *PrefixDB) readSegmentIndexForKeyByPathWithSource(folderPath string, key []byte) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	entryLock, unlock := db.lockSegmentIndexFolderReadEntry(folderPath)
	defer unlock()
	generation := atomic.LoadUint64(&entryLock.gen)
	if len(key) == 0 {
		return db.readSegmentIndexLockedInternalByPath(folderPath, true)
	}
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return nil, segmentIndexLookupSourceNoCache, err
	}
	// For multi-level indexes, key lookup should prefer level2 shard cache.
	// Returning the full level1 metas slice can be significantly more expensive
	// than selecting one level2 shard and causes slower cache-hit latency.
	if layout.mode != indexLayoutMultiLevel && db.storageIndexCache != nil {
		if metas, ok := db.storageIndexCache.GetByPath(folderPath); ok {
			return metas, segmentIndexLookupSourceL1Cache, nil
		}
	}
	if layout.mode != indexLayoutMultiLevel {
		data := layout.flatData
		if len(data) == 0 {
			indexPath := filepath.Join(folderPath, segmentIndexFileName)
			data, err = db.readSegmentIndexFile(indexPath)
			if err != nil {
				return nil, segmentIndexLookupSourceNoCache, err
			}
		}
		if meta, selectErr := selectFixedFlatSegmentIndexMeta(data, key); selectErr != nil {
			return nil, segmentIndexLookupSourceNoCache, selectErr
		} else if meta != nil {
			return []segmentChunkMeta{*meta}, segmentIndexLookupSourceNoCache, nil
		}
		return db.readSegmentIndexLockedInternalByPath(folderPath, true)
	}
	entry := selectSegmentL1Entry(layout.entries, key)
	if entry == nil {
		// Log detailed information for debugging
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: failed to locate L1 index entry for key - folder=%s key=%x entries_count=%d\n",
			folderPath, key, len(layout.entries))
		// Print key ranges for all L1 entries
		for i, e := range layout.entries {
			fmt.Fprintf(prefixdbLogWriter, "prefixdb DEBUG: L1[%d] MetaID=%d ChunkCount=%d KeyStart=%x\n",
				i, e.MetaID, e.ChunkCount, e.KeyStart)
		}
		return nil, segmentIndexLookupSourceNoCache, fmt.Errorf("%w for folder %s", errSegmentIndexEntryNotFound, folderPath)
	}
	if db.storageIndexCache != nil {
		if metas, ok := db.storageIndexCache.GetLevel2ByPath(folderPath, entry.MetaID, generation); ok {
			if selectSegmentChunkMeta(metas, key) == nil {
				fallbackMetas, fallbackSource, fallbackErr := db.readSegmentIndexLockedInternalByPath(folderPath, true)
				if fallbackErr == nil && selectSegmentChunkMeta(fallbackMetas, key) != nil {
					return fallbackMetas, fallbackSource, nil
				}
			}
			return metas, segmentIndexLookupSourceL2Cache, nil
		}
	}
	metas := make([]segmentChunkMeta, 0, entry.ChunkCount)
	var arena []byte
	data, err := db.readSegmentIndexFile(level2IndexFilePath(folderPath, entry.MetaID))
	if err != nil {
		return nil, segmentIndexLookupSourceNoCache, err
	}
	if err := decodeSegmentIndexBuffer(data, &metas, &arena, false, folderPath); err != nil {
		return nil, segmentIndexLookupSourceNoCache, err
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.AddLevel2ByPath(folderPath, entry.MetaID, generation, metas)
	}
	// Guard against stale/misaligned shard boundaries in multi-level indexes.
	// If the selected L2 shard cannot resolve the key, fall back to a full-index
	// view so boundary keys can still locate the correct chunk.
	if selectSegmentChunkMeta(metas, key) == nil {
		fallbackMetas, fallbackSource, fallbackErr := db.readSegmentIndexLockedInternalByPath(folderPath, true)
		if fallbackErr == nil && selectSegmentChunkMeta(fallbackMetas, key) != nil {
			return fallbackMetas, fallbackSource, nil
		}
	}
	return metas, segmentIndexLookupSourceNoCache, nil
}

func (db *PrefixDB) readSegmentIndexLockedInternal(folderID uint32, useLRU bool) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	return db.readSegmentIndexLockedInternalByPath(db.segmentedFolderPath(folderID), useLRU)
}

func (db *PrefixDB) readSegmentIndexForKey(folderID uint32, key []byte) ([]segmentChunkMeta, error) {
	metas, _, err := db.readSegmentIndexForKeyWithSource(folderID, key)
	return metas, err
}

func (db *PrefixDB) readSegmentIndexForKeyWithSource(folderID uint32, key []byte) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	return db.readSegmentIndexForKeyByPathWithSource(db.segmentedFolderPath(folderID), key)
}

func cloneSegmentChunkMetas(src []segmentChunkMeta) []segmentChunkMeta {
	if len(src) == 0 {
		return nil
	}
	dst := make([]segmentChunkMeta, len(src))
	for i := range src {
		dst[i] = segmentChunkMeta{
			FileName: strings.Clone(src[i].FileName),
		}
		dst[i].KeyStart = cloneBytes(src[i].KeyStart)
	}
	return dst
}

func cloneSegmentIndexLayout(src segmentIndexLayout) segmentIndexLayout {
	dst := segmentIndexLayout{
		mode:       src.mode,
		nextMetaID: src.nextMetaID,
	}
	if len(src.flatData) > 0 {
		dst.flatData = cloneBytes(src.flatData)
	}
	if len(src.entries) == 0 {
		return dst
	}
	dst.entries = make([]segmentIndexL1Entry, len(src.entries))
	for i := range src.entries {
		dst.entries[i] = segmentIndexL1Entry{
			MetaID:     src.entries[i].MetaID,
			ChunkCount: src.entries[i].ChunkCount,
			KeyStart:   cloneBytes(src.entries[i].KeyStart),
		}
	}
	return dst
}

func estimateSegmentIndexLayoutMemory(layout segmentIndexLayout) uint64 {
	total := uint64(unsafe.Sizeof(segmentIndexLayout{}))
	total += uint64(len(layout.flatData))
	total += uint64(len(layout.entries)) * uint64(unsafe.Sizeof(segmentIndexL1Entry{}))
	for i := range layout.entries {
		total += uint64(len(layout.entries[i].KeyStart))
	}
	if total == 0 {
		return 1
	}
	return total
}

func estimateSegmentChunkMetasMemory(metas []segmentChunkMeta) uint64 {
	if len(metas) == 0 {
		return 0
	}
	total := uint64(len(metas)) * uint64(unsafe.Sizeof(segmentChunkMeta{}))
	for i := range metas {
		total += uint64(len(metas[i].FileName))
		total += uint64(len(metas[i].KeyStart))
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
	idx := upperBoundSegmentChunkMetas(metas, key)
	if idx == 0 {
		return nil
	}
	return &metas[idx-1]
}

func upperBoundSegmentIndexL1Entries(entries []segmentIndexL1Entry, key []byte) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		start := entries[mid].KeyStart
		if len(start) == 0 || compareSegmentIndexKeyStarts(start, key) <= 0 {
			lo = mid + 1
			continue
		}
		hi = mid
	}
	return lo
}

func upperBoundSegmentChunkMetas(metas []segmentChunkMeta, key []byte) int {
	lo, hi := 0, len(metas)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		start := metas[mid].KeyStart
		if len(start) == 0 || compareSegmentIndexKeyStarts(start, key) <= 0 {
			lo = mid + 1
			continue
		}
		hi = mid
	}
	return lo
}

func (db *PrefixDB) readSegmentChunkFile(folderID uint32, fileName string) ([]kvPair, *bufferLease, error) {
	return db.readSegmentChunkFileWithUsage(folderID, fileName, diskIOUsageStorageSeparatedLogs)
}

func (db *PrefixDB) readSegmentChunkFileWithUsage(folderID uint32, fileName string, usage diskIOUsage) ([]kvPair, *bufferLease, error) {
	lease, err := db.readSegmentFileBufferWithUsage(folderID, fileName, usage)
	if err != nil {
		return nil, nil, err
	}
	kvCount, err := countChunkEntriesFromTail(lease.Bytes())
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	entries, err := buildPairsFromChunkBuffer(lease.Bytes(), kvCount, nil)
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	return entries, lease, nil
}

func (db *PrefixDB) readSegmentChunkFileWithUsageByPath(folderPath string, fileName string, usage diskIOUsage) ([]kvPair, *bufferLease, error) {
	lease, err := db.readSegmentFileBufferByPathWithUsage(folderPath, fileName, usage)
	if err != nil {
		return nil, nil, err
	}
	kvCount, err := countChunkEntriesFromTail(lease.Bytes())
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	entries, err := buildPairsFromChunkBuffer(lease.Bytes(), kvCount, nil)
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	return entries, lease, nil
}

func (db *PrefixDB) readSegmentFileBufferByPathWithUsage(folderPath string, fileName string, usage diskIOUsage) (*bufferLease, error) {
	fullPath := filepath.Join(folderPath, fileName)
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
	sr := io.NewSectionReader(f, 0, size)
	if _, err := io.ReadFull(sr, buf[:intSize]); err != nil {
		putDataBuffer(buf)
		return nil, err
	}
	db.addDiskRead(usage, intSize)
	return newBufferLease(buf[:intSize]), nil
}

func (db *PrefixDB) segmentChunkFileSizeByPath(folderPath string, fileName string) (int64, error) {
	fullPath := filepath.Join(folderPath, fileName)
	f, err := db.openCachedReadOnlyFile(fullPath)
	if err != nil {
		return 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *PrefixDB) readSegmentedChunkToCacheStreamingByPath(folderPath string, fileName string, accountKey []byte, storageKey []byte, failure *segmentedStorageReadFailure) ([]byte, *segmentedStorageReadFailure, *bufferLease, error) {
	lease, err := db.readSegmentFileBufferByPathWithUsage(folderPath, fileName, diskIOUsageStorageSeparatedLogs)
	if err != nil {
		failure.reason = "segment-chunk-read-failed"
		return nil, failure, nil, err
	}
	buf := lease.Bytes()
	if len(buf) == 0 {
		lease.Release()
		failure.reason = "segment-chunk-empty"
		return nil, failure, nil, nil
	}

	cache := db.storageCache
	prefetchLimit := db.storageGetCacheCount
	pending := make([]kvPair, 0, prefetchLimit)
	for cursor := len(buf); cursor > 0; {
		if cursor < segmentedChunkEntryHeaderSize {
			lease.Release()
			failure.reason = "segment-chunk-corrupted"
			return nil, failure, nil, nil
		}
		footer := buf[cursor-segmentedChunkEntryHeaderSize : cursor]
		klen := int(readUint16BE(footer[:2]))
		vlen := int(readUint16BE(footer[2:4]))
		recordDataLen := klen + vlen
		recordStart := cursor - segmentedChunkEntryHeaderSize - recordDataLen
		if recordStart < 0 {
			lease.Release()
			failure.reason = "segment-chunk-corrupted"
			return nil, failure, nil, nil
		}

		entryBuf := buf[recordStart : cursor-segmentedChunkEntryHeaderSize]
		key := entryBuf[:klen]
		var value []byte
		if vlen > 0 {
			value = entryBuf[klen:recordDataLen]
		}
		if prefetchLimit > 0 && len(pending) < prefetchLimit {
			pending = append(pending, kvPair{key: cloneBytes(key), val: cloneBytes(value)})
		}
		if bytes.Equal(key, storageKey) {
			if cache != nil {
				for i := range pending {
					db.addStorageCacheValue(accountKey, pending[i].key, pending[i].val, true)
				}
			}
			if value == nil {
				if cache != nil {
					db.addStorageCacheValue(accountKey, storageKey, nil, false)
				}
				failure.reason = "segment-chunk-tombstone"
				return nil, failure, lease, nil
			}
			result := append([]byte(nil), value...)
			if cache != nil {
				db.addStorageCacheValue(accountKey, storageKey, result, false)
			}
			return result, nil, lease, nil
		}
		cursor = recordStart
	}

	if cache != nil {
		db.addStorageCacheValue(accountKey, storageKey, nil, false)
	}
	failure.reason = "segment-chunk-key-not-found"
	return nil, failure, lease, nil
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
	return normalizeStorageEntries(entries)
}

func (db *PrefixDB) readAccountStorageValue(accountKey, storageKey []byte) ([]byte, bool, *segmentedStorageReadFailure, error) {
	if len(accountKey) == 0 {
		return nil, false, nil, nil
	}
	if db.isAccountStorageFolderManaged(accountKey) {
		folderPath := db.segmentedFolderPathForAccount(accountKey)
		val, failure, err := db.readSegmentedChunkToCacheByPath(folderPath, accountKey, storageKey)
		if err != nil {
			if shouldFallbackMissingFolderRead(err) {
				db.clearAccountStorageFolder(accountKey)
			} else {
				return nil, false, failure, err
			}
		} else if val != nil {
			return val, true, nil, nil
		} else {
			return nil, false, failure, nil
		}
	}

	cacheInfo, err := db.resolveAccountStoragePointer(accountKey)
	if err != nil {
		return nil, false, nil, err
	}

	if cacheInfo.storageFileID == 0 {
		return nil, false, nil, nil
	}

	if isSegmentedStorage(cacheInfo.storageFileID) {
		if !isAccountNamedSegmentedStorage(cacheInfo.storageFileID) {
			return nil, false, nil, errors.New("legacy segmented storage pointers are no longer supported")
		}
		val, failure := db.readSegmentedChunkToCache(cacheInfo.storageFileID, accountKey, storageKey)
		if val == nil {
			return nil, false, failure, nil
		}
		return val, true, nil, nil

	} else {
		if cacheInfo.storageSize == 0 || cacheInfo.storageOffset < 0 {
			storagePath, _ := db.storagePathByFileID(cacheInfo.storageFileID)
			db.logLargeLogReadFailure(accountKey, storageKey, storagePath, cacheInfo.storageFileID, cacheInfo.storageOffset, cacheInfo.storageSize, "invalid-account-entry-pointer", nil)
			return nil, false, nil, nil
		}
		val := db.readStorageSegmentFile(cacheInfo.storageFileID, cacheInfo.storageOffset, cacheInfo.storageSize, accountKey, storageKey)
		if val == nil {
			return nil, false, nil, nil
		}
		return val, true, nil, nil
	}
}

func (db *PrefixDB) logLargeLogReadFailure(accountKey, storageKey []byte, filePath string, fileID uint32, offset int64, size uint64, reason string, err error) {
	dir, file := splitLogPath(filePath)
	if err != nil {
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: failed to read large log via account entry account=%x storage=%x dir=%s file=%s fileID=%d offset=%d size=%d reason=%s err=%v\n", accountKey, storageKey, dir, file, fileID, offset, size, reason, err)
		return
	}
	fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: failed to read large log via account entry account=%x storage=%x dir=%s file=%s fileID=%d offset=%d size=%d reason=%s\n", accountKey, storageKey, dir, file, fileID, offset, size, reason)
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
	start := time.Now()
	node, fromCache, err := db.getNodeWithSource(accountKey)
	recordTrieStorageGetBreakdownStep(&db.trieStorageAccountEntryStats, fromCache, time.Since(start))
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
	start := time.Now()
	defer func() {
		recordTrieStorageGetBreakdownStep(&db.trieStorageKVStats, false, time.Since(start))
	}()
	p, _ := db.storagePathByFileID(fileID)

	f, err := db.openCachedReadOnlyFile(p)
	if err != nil {
		db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "open-storage-file", err)
		return nil
	}

	if size == 0 {
		db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "empty-storage-size", nil)
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
			db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "read-storage-file", err)
			putDataBuffer(buf)
			return nil
		}
		read += n
		db.addDiskRead(diskIOUsageStorageCommonLogs, n)
	}
	if read != total {
		db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "short-storage-read", io.ErrUnexpectedEOF)
		putDataBuffer(buf)
		return nil
	}
	buf = buf[:total]

	if db.storageCache != nil && len(accountKey) > 0 && len(storageKey) > 0 {
		payload, kvCount, parseErr := parseSegmentBuffer(buf)
		if parseErr != nil {
			db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "corrupted-storage-segment", parseErr)
		} else {
			cursor := 0
			payloadLen := len(payload)
			hit := false
			malformed := false
			count := 0
			for i := 0; i < kvCount; i++ {
				if cursor+segmentedChunkEntryHeaderSize > payloadLen {
					malformed = true
					break
				}
				header := payload[cursor : cursor+segmentedChunkEntryHeaderSize]
				klen := int(readUint16BE(header[:2]))
				vlen := int(readUint16BE(header[2:4]))
				cursor += segmentedChunkEntryHeaderSize
				totalLen := klen + vlen
				if cursor+totalLen > payloadLen {
					malformed = true
					break
				}
				keyRaw := payload[cursor : cursor+klen]
				key := keyRaw
				if bytes.HasPrefix(key, storageKey) {
					var value []byte
					if vlen > 0 {
						value = payload[cursor+klen : cursor+totalLen]
					}
					if bytes.Equal(key, storageKey) {
						if value == nil {
							ret = nil
							db.addStorageCacheValue(accountKey, key, nil, false)
						} else {
							ret = append([]byte(nil), value...)
							db.addStorageCacheValue(accountKey, key, value, false)
						}
						hit = true
					}
					if hit && count < 16 {
						if value == nil {
							db.addStorageCacheValue(accountKey, key, nil, !bytes.Equal(key, storageKey))
						} else {
							db.addStorageCacheValue(accountKey, key, value, !bytes.Equal(key, storageKey))
						}
						count++
					}
				}
				cursor += totalLen
			}
			if malformed {
				db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "corrupted-storage-segment", io.ErrUnexpectedEOF)
			}
			if !hit && !malformed {
				db.addStorageCacheValue(accountKey, storageKey, nil, false)
			}
		}
	}
	putDataBuffer(buf)
	return ret
}

func (db *PrefixDB) readStorageSegmentPayload(fileID uint32, offset int64, size uint64) ([]byte, int, *bufferLease, error) {
	if isSegmentedStorage(fileID) {
		if isAccountNamedSegmentedStorage(fileID) {
			return nil, 0, nil, fmt.Errorf("account-named segmented storage requires account-key folder context")
		}
		return nil, 0, nil, errors.New("legacy segmented storage pointers are no longer supported")
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

	payload, kvCount, err := parseSegmentBuffer(buf)
	if err != nil {
		putDataBuffer(buf)
		return nil, 0, nil, err
	}
	return payload, kvCount, newBufferLease(buf), nil

}

func parseSegmentBuffer(buf []byte) ([]byte, int, error) {
	kvCount, err := countPayloadEntriesWithHeaderSize(buf, segmentedChunkEntryHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	return buf, kvCount, nil
}

func countPayloadEntriesWithHeaderSize(payload []byte, headerSize int) (int, error) {
	if headerSize != segmentedChunkEntryHeaderSize {
		return 0, fmt.Errorf("unsupported segmented chunk header size: %d", headerSize)
	}
	cursor := 0
	payloadLen := len(payload)
	count := 0
	for cursor < payloadLen {
		if cursor+headerSize > payloadLen {
			return 0, io.ErrUnexpectedEOF
		}
		header := payload[cursor : cursor+headerSize]
		klen := int(readUint16BE(header[:2]))
		vlen := int(readUint16BE(header[2:4]))
		cursor += headerSize
		totalLen := klen + vlen
		if cursor+totalLen > payloadLen {
			return 0, io.ErrUnexpectedEOF
		}
		cursor += totalLen
		count++
	}
	return count, nil
}

func countChunkEntriesFromTail(buf []byte) (int, error) {
	cursor := len(buf)
	count := 0
	for cursor > 0 {
		if cursor < segmentedChunkEntryHeaderSize {
			return 0, io.ErrUnexpectedEOF
		}
		footer := buf[cursor-segmentedChunkEntryHeaderSize : cursor]
		klen := int(readUint16BE(footer[:2]))
		vlen := int(readUint16BE(footer[2:4]))
		recordSize := segmentedChunkEntryHeaderSize + klen + vlen
		if recordSize > cursor {
			return 0, io.ErrUnexpectedEOF
		}
		cursor -= recordSize
		count++
	}
	return count, nil
}

func buildPairsFromPayload(payload []byte, kvCount int, headerSize int, dst []kvPair) ([]kvPair, error) {
	if kvCount <= 0 {
		return dst[:0], nil
	}
	if headerSize != segmentedChunkEntryHeaderSize {
		return nil, fmt.Errorf("unsupported segmented chunk header size: %d", headerSize)
	}

	if cap(dst) < kvCount {
		dst = make([]kvPair, kvCount)
	}
	entries := dst[:kvCount]
	cursor := 0
	payloadLen := len(payload)

	var klen, vlen int
	for i := 0; i < kvCount; i++ {
		if cursor+headerSize > payloadLen {
			return nil, io.ErrUnexpectedEOF
		}
		header := payload[cursor : cursor+headerSize]
		klen = int(readUint16BE(header[:2]))
		vlen = int(readUint16BE(header[2:4]))
		cursor += headerSize
		totalLen := klen + vlen
		if cursor+totalLen > payloadLen {
			return nil, io.ErrUnexpectedEOF
		}
		var val []byte
		if vlen > 0 {
			val = payload[cursor+klen : cursor+totalLen]
		}
		entries[i] = kvPair{key: payload[cursor : cursor+klen], val: val}
		cursor += totalLen
	}

	return entries, nil
}

func buildPairsFromChunkBuffer(payload []byte, kvCount int, dst []kvPair) ([]kvPair, error) {
	if kvCount < 0 {
		var err error
		kvCount, err = countChunkEntriesFromTail(payload)
		if err != nil {
			return nil, err
		}
	}
	if kvCount <= 0 {
		return dst[:0], nil
	}

	if cap(dst) < kvCount {
		dst = make([]kvPair, kvCount)
	}
	entries := dst[:kvCount]
	cursor := len(payload)
	payloadLen := len(payload)

	var klen, vlen int
	for i := kvCount - 1; i >= 0; i-- {
		if cursor < segmentedChunkEntryHeaderSize {
			return nil, io.ErrUnexpectedEOF
		}
		header := payload[cursor-segmentedChunkEntryHeaderSize : cursor]
		klen = int(readUint16BE(header[:2]))
		vlen = int(readUint16BE(header[2:4]))
		totalLen := klen + vlen
		dataStart := cursor - segmentedChunkEntryHeaderSize - totalLen
		if dataStart < 0 || dataStart > payloadLen {
			return nil, io.ErrUnexpectedEOF
		}
		var val []byte
		if vlen > 0 {
			val = payload[dataStart+klen : dataStart+totalLen]
		}
		entries[i] = kvPair{
			key: payload[dataStart : dataStart+klen],
			// vlen==0 is a tombstone delete; preserve it as nil
			// so cache/read paths treat it as not-found.
			val: val,
		}
		cursor = dataStart
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
	entries, err := buildPairsFromPayload(payload, kvCount, segmentedChunkEntryHeaderSize, nil)
	if err != nil {
		if backing != nil {
			backing.Release()
		}
		return nil, nil, err
	}
	return entries, backing, nil
}

func (db *PrefixDB) GetStorageCount(accountKey []byte) (int, uint64, error) {
	if db.isAccountStorageFolderManaged(accountKey) {
		folderPath := db.segmentedFolderPathForAccount(accountKey)
		metas, err := db.readSegmentIndexNoCacheByPath(folderPath)
		if err != nil {
			return 0, 0, err
		}
		count := 0
		var total uint64
		for i := range metas {
			entries, backing, err := db.readSegmentChunkFileWithUsageByPath(folderPath, metas[i].FileName, diskIOUsageStorageSeparatedLogs)
			if err != nil {
				return 0, 0, err
			}
			if backing != nil {
				total += uint64(len(backing.Bytes()))
				backing.Release()
			} else {
				info, statErr := os.Stat(filepath.Join(folderPath, metas[i].FileName))
				if statErr != nil {
					return 0, 0, statErr
				}
				total += uint64(info.Size())
			}
			count += len(entries)
		}
		return count, total, nil
	}
	node, err := db.getNode(accountKey)
	if err != nil {
		return 0, 0, err
	}
	if node == nil || node.storageFileID == 0 {
		return 0, 0, nil
	}
	if isSegmentedStorage(node.storageFileID) {
		if isAccountNamedSegmentedStorage(node.storageFileID) {
			return 0, 0, nil
		}
		return 0, 0, errors.New("legacy segmented storage pointers are no longer supported")
	}

	p, _ := db.storagePathByFileID(node.storageFileID)

	f, err := db.openCachedReadOnlyFile(p)
	if err != nil {
		return 0, 0, err
	}

	if node.storageSize == 0 {
		return 0, 0, nil
	}

	buf := make([]byte, int(node.storageSize))
	n, err := f.ReadAt(buf, node.storageOffset)
	if err != nil && err != io.EOF {
		return 0, 0, err
	}
	db.addDiskRead(diskIOUsageStorageCommonLogs, n)
	buf = buf[:n]
	_, kvCount, parseErr := parseSegmentBuffer(buf)
	if parseErr != nil {
		return 0, 0, parseErr
	}
	return kvCount, node.storageSize, nil

}

// storagePathByFileID returns the storage file path, whether it's hot storage, and the real file ID.
func (db *PrefixDB) storagePathByFileID(fileID uint32) (path string, realID uint32) {
	if isSegmentedStorage(fileID) {
		return "", 0
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

func (db *PrefixDB) storagePrefetchPendingCount() int {
	if !analysisStatsEnabled || db == nil {
		return 0
	}
	return int(atomic.LoadUint64(&db.storagePrefetchTrackedCount))
}

func (db *PrefixDB) recordStoragePrefetchAdd(cacheKey string, value []byte) {
	if db == nil || !shouldSampleStoragePrefetchKey(cacheKey) {
		return
	}
	atomic.AddUint64(&db.trieStoragePrefetchStats.addCount, 1)
	if value == nil {
		atomic.AddUint64(&db.trieStoragePrefetchStats.addNilCount, 1)
	} else {
		atomic.AddUint64(&db.trieStoragePrefetchStats.addBytes, uint64(len(value)))
	}
	db.storagePrefetchMu.Lock()
	if db.storagePrefetchPending == nil {
		db.storagePrefetchPending = make(map[string]struct{})
	}
	if _, exists := db.storagePrefetchPending[cacheKey]; !exists {
		db.storagePrefetchPending[cacheKey] = struct{}{}
		atomic.AddUint64(&db.storagePrefetchTrackedCount, 1)
	}
	db.storagePrefetchMu.Unlock()
}

func (db *PrefixDB) clearStoragePrefetch(cacheKey string) {
	if !analysisStatsEnabled || db == nil || cacheKey == "" {
		return
	}
	if atomic.LoadUint64(&db.storagePrefetchTrackedCount) == 0 {
		return
	}
	db.storagePrefetchMu.Lock()
	if len(db.storagePrefetchPending) == 0 {
		db.storagePrefetchMu.Unlock()
		return
	}
	if _, ok := db.storagePrefetchPending[cacheKey]; ok {
		delete(db.storagePrefetchPending, cacheKey)
		atomic.AddUint64(&db.storagePrefetchTrackedCount, ^uint64(0))
		atomic.AddUint64(&db.trieStoragePrefetchStats.clearCount, 1)
	}
	db.storagePrefetchMu.Unlock()
}

func (db *PrefixDB) noteStoragePrefetchHit(cacheKey string, value interface{}) {
	if !analysisStatsEnabled || db == nil || cacheKey == "" {
		return
	}
	if atomic.LoadUint64(&db.storagePrefetchTrackedCount) == 0 {
		return
	}
	db.storagePrefetchMu.Lock()
	if len(db.storagePrefetchPending) == 0 {
		db.storagePrefetchMu.Unlock()
		return
	}
	if _, ok := db.storagePrefetchPending[cacheKey]; !ok {
		db.storagePrefetchMu.Unlock()
		return
	}
	delete(db.storagePrefetchPending, cacheKey)
	atomic.AddUint64(&db.storagePrefetchTrackedCount, ^uint64(0))
	db.storagePrefetchMu.Unlock()
	atomic.AddUint64(&db.trieStoragePrefetchStats.hitCount, 1)
	if value == nil {
		atomic.AddUint64(&db.trieStoragePrefetchStats.hitNilCount, 1)
		return
	}
	if valueBytes, ok := value.([]byte); ok {
		atomic.AddUint64(&db.trieStoragePrefetchStats.hitBytes, uint64(len(valueBytes)))
	}
}

func (db *PrefixDB) addStorageCacheValueByKey(cacheKey string, value []byte, prefetched bool) {
	if db == nil || db.storageCache == nil || cacheKey == "" {
		return
	}
	if prefetched {
		db.recordStoragePrefetchAdd(cacheKey, value)
	} else {
		db.clearStoragePrefetch(cacheKey)
	}
	db.storageCache.Add(cacheKey, value)
}

func (db *PrefixDB) addStorageCacheValue(accountKey, storageKey, value []byte, prefetched bool) {
	if db == nil || len(accountKey) == 0 || len(storageKey) == 0 {
		return
	}
	db.addStorageCacheValueByKey(db.storageCacheKey(accountKey, storageKey), value, prefetched)
}

func (db *PrefixDB) removeStorageCacheValue(accountKey, storageKey []byte) {
	if db == nil || db.storageCache == nil || len(accountKey) == 0 || len(storageKey) == 0 {
		return
	}
	cacheKey := db.storageCacheKey(accountKey, storageKey)
	db.clearStoragePrefetch(cacheKey)
	db.storageCache.Remove(cacheKey)
}

func (db *PrefixDB) syncStorageCacheEntries(accountKey []byte, kvs []kvPair) {
	if db == nil || db.storageCache == nil || len(accountKey) == 0 || len(kvs) == 0 {
		return
	}
	for _, kv := range kvs {
		cacheKey := db.storageCacheKey(accountKey, kv.key)
		if kv.val == nil {
			db.addStorageCacheValueByKey(cacheKey, nil, false)
			continue
		}
		db.addStorageCacheValueByKey(cacheKey, kv.val, false)
	}
}

func (db *PrefixDB) refreshManagedFolderStorageCache(folderPath string) error {
	accountKey, ok := db.managedAccountKeyForFolderPath(folderPath)
	if !ok || db.storageCache == nil {
		return nil
	}
	metas, err := db.readSegmentIndexNoCacheByPath(folderPath)
	if err != nil {
		return err
	}
	for _, meta := range metas {
		entries, backing, err := db.readSegmentChunkFileWithUsageByPath(folderPath, meta.FileName, diskIOUsageStorageGC)
		if err != nil {
			return err
		}
		db.syncStorageCacheEntries(accountKey, entries)
		if backing != nil {
			backing.Release()
		}
	}
	return nil
}

func isSegmentedStorage(fileID uint32) bool {
	return fileID&segmentedStorageFlag != 0
}

func (db *PrefixDB) invalidateSegmentIndexLayoutForPath(folderPath string) {
	if db.storageIndexCache != nil {
		db.storageIndexCache.RemoveLayoutByPath(folderPath)
	}
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
		pending := make(map[string][]storageGCJob)
		active := make(map[string]struct{})
		batchDone := make(chan string, storageGCQueueCapacity(db.gcWorkers))
		launchBatch := func(folderPath string) {
			jobs := pending[folderPath]
			if len(jobs) == 0 {
				return
			}
			if _, exists := active[folderPath]; exists {
				return
			}
			delete(pending, folderPath)
			active[folderPath] = struct{}{}
			batchWait.Add(1)
			go func(path string, jobs []storageGCJob) {
				defer batchWait.Done()
				db.processStorageGCBatch(jobs)
				batchDone <- path
			}(folderPath, jobs)
		}
		launchAllReady := func() {
			for folderPath := range pending {
				launchBatch(folderPath)
			}
		}
		drainQueue := func() {
			for {
				select {
				case job := <-db.storageGCQueue:
					pending[job.folderPath] = append(pending[job.folderPath], job)
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
				pending[job.folderPath] = append(pending[job.folderPath], job)
				drainQueue()
				launchAllReady()
			case folderPath := <-batchDone:
				delete(active, folderPath)
				launchBatch(folderPath)
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

func (db *PrefixDB) maybeScheduleStorageGC(folderPath string, meta *segmentChunkMeta, backing *bufferLease) {
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
	if db.storageGCQueue == nil {
		release()
		return
	}
	// ChunkSize is no longer tracked in meta - get from filesystem
	info, err := os.Stat(filepath.Join(folderPath, meta.FileName))
	if err != nil {
		release()
		return
	}
	chunkSize := uint64(info.Size())
	if chunkSize <= uint64(db.segmentedChunkTriggerSize()) {
		release()
		return
	}
	job := storageGCJob{folderPath: folderPath, fileName: meta.FileName, backing: backing}
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
		fmt.Printf("storage GC failed for folder %s file %s: %v\n", job.folderPath, job.fileName, err)
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
		fmt.Printf("storage GC batch failed for folder %s jobs %d: %v\n", jobs[0].folderPath, len(jobs), err)
	}
}

func (db *PrefixDB) runStorageGCBatch(jobs []storageGCJob) error {
	if len(jobs) == 0 {
		return nil
	}
	folderPath := jobs[0].folderPath
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
	// Prefer cached index snapshot on GC reads to reduce repetitive disk IO.
	metas, gen0, err := db.readSegmentIndexWithGenByPath(folderPath, true)
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
		if j.folderPath != folderPath || j.fileName == "" {
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
			kvCount, pErr := countChunkEntriesFromTail(backings[backingIdx].Bytes())
			if pErr == nil {
				entries := borrowStorageEntries(kvCount)
				if decoded, decErr := buildPairsFromChunkBuffer(backings[backingIdx].Bytes(), kvCount, entries); decErr == nil {
					preloaded = decoded
					preloadBacking = backings[backingIdx]
					backings[backingIdx] = nil
				} else {
					releaseStorageEntries(entries)
				}
			}
		}

		chunkMetas, nextOrd2, err := db.rewriteChunkWithDedupToNewFiles(folderPath, metas[idx], nil, nextOrd, preloaded, preloadBacking)
		if preloaded != nil {
			releaseStorageEntries(preloaded)
		}
		if err != nil {
			return err
		}
		nextOrd = nextOrd2
		replacements[job.fileName] = chunkMetas
	}

	if len(replacements) == 0 {
		return nil
	}

	// Phase 2: commit by updating the index once.
	// Re-read metas so we don't clobber concurrent index updates (e.g., another GC job).
	genNow := db.segmentIndexGenerationLocked(folderPath)
	latest := metas
	if genNow != gen0 {
		var latestGen uint64
		latest, latestGen, err = db.readSegmentIndexWithGenByPath(folderPath, true)
		_ = latestGen
		if err != nil {
			return err
		}
	}

	// Build updated index by applying all replacements
	changed := false
	updated := make([]segmentChunkMeta, 0, len(latest))
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
	applied, err := db.writeSegmentIndexIncrementalGC(folderPath, latest, replacements)
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	committed, err := db.readSegmentIndexNoCacheByPath(folderPath)
	if err != nil {
		return err
	}
	db.refreshSegmentIndexCacheByPath(folderPath, committed)
	if err := db.refreshManagedFolderStorageCache(folderPath); err != nil {
		return err
	}
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
	// Prefer cached index snapshot on GC reads to reduce repetitive disk IO.
	metas, gen0, err := db.readSegmentIndexWithGenByPath(job.folderPath, true)
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
	folderPath := job.folderPath

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
		kvCount, err := countChunkEntriesFromTail(job.backing.Bytes())
		if err == nil {
			entries := borrowStorageEntries(kvCount)
			if decoded, decErr := buildPairsFromChunkBuffer(job.backing.Bytes(), kvCount, entries); decErr == nil {
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

	chunkMetas, nextOrd2, err := db.rewriteChunkWithDedupToNewFiles(folderPath, metas[idx], nil, nextOrd, preloaded, preloadBacking)
	if preloaded != nil {
		releaseStorageEntries(preloaded)
	}
	if err != nil {
		return err
	}
	_ = nextOrd2

	// Phase 2: commit by updating the index to point to the new files.
	// Re-read metas so we don't clobber concurrent index updates (e.g., another GC job).
	genNow := db.segmentIndexGenerationLocked(job.folderPath)
	latest := metas
	if genNow != gen0 {
		var latestGen uint64
		latest, latestGen, err = db.readSegmentIndexWithGenByPath(job.folderPath, true)
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
	replacements := map[string][]segmentChunkMeta{job.fileName: chunkMetas}
	applied, err := db.writeSegmentIndexIncrementalGC(folderPath, latest, replacements)
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	committed, err := db.readSegmentIndexNoCacheByPath(folderPath)
	if err != nil {
		return err
	}
	db.refreshSegmentIndexCacheByPath(job.folderPath, committed)
	if err := db.refreshManagedFolderStorageCache(job.folderPath); err != nil {
		return err
	}
	// Option B: do NOT delete the original chunk file. It becomes garbage and can be cleaned later.
	return nil
}

// reserveChunkFileName tries to reserve a unique %04d.dat name by creating the destination
// path with O_EXCL. The created file is a placeholder and will be replaced atomically by writeChunkFile.
func reserveChunkFileName(folderPath string, startOrdinal int) (name string, nextOrdinal int, err error) {
	ord := startOrdinal
	for {
		candidate := chunkFileNameForOrdinal(uint32(ord))
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
func (db *PrefixDB) rewriteChunkWithDedupToNewFiles(folderPath string, meta segmentChunkMeta, additions []kvPair, startOrdinal int, existing []kvPair, backing *bufferLease) ([]segmentChunkMeta, int, error) {
	var err error
	var bytesWritten uint64
	if existing == nil {
		existing, backing, err = db.readSegmentChunkFileWithUsageByPath(folderPath, meta.FileName, diskIOUsageStorageGC)
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
		addUint64Stat(&db.GCCount, 1)
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
			FileName: name,
			KeyStart: cloneBytes(chunk[0].key),
		})
	}
	addUint64Stat(&db.GCCount, 1)
	addUint64Stat(&db.GCWriteBytes, bytesWritten)
	return result, ordinal, nil
}

func (db *PrefixDB) InsertAccountHashPebble(accountHash []byte, pebbleKey []byte) error {
	return db.accountHashKeyPebble.Put(accountHash, pebbleKey)
}

func (db *PrefixDB) readSegmentedChunkToCache(fileID uint32, accountKey []byte, storageKey []byte) ([]byte, *segmentedStorageReadFailure) {
	if !isAccountNamedSegmentedStorage(fileID) {
		return nil, nil
	}
	if len(accountKey) == 0 {
		return nil, nil
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	val, failure, err := db.readSegmentedChunkToCacheByPath(folderPath, accountKey, storageKey)
	if err != nil {
		if shouldFallbackMissingFolderRead(err) {
			db.clearAccountStorageFolder(accountKey)
		}
		return nil, failure
	}
	if val != nil {
		db.markAccountStorageFolder(accountKey)
	}
	return val, failure
}

func (db *PrefixDB) readSegmentedChunkToCacheByPath(folderPath string, accountKey []byte, storageKey []byte) ([]byte, *segmentedStorageReadFailure, error) {
	unlock := db.lockSegmentIndexFolder(folderPath)
	var gcMeta *segmentChunkMeta
	var gcBacking *bufferLease
	defer func() {
		unlock()
		if gcMeta != nil {
			db.maybeScheduleStorageGC(folderPath, gcMeta, gcBacking)
			gcBacking = nil
		}
	}()
	failure := &segmentedStorageReadFailure{folderPath: folderPath, indexFile: segmentIndexFileName}
	indexStart := time.Now()
	metas, segmentIndexSource, err := db.readSegmentIndexForKeyByPathWithSource(folderPath, storageKey)
	if len(metas) > 0 {
		duration := time.Since(indexStart)
		recordTrieStorageGetBreakdownStep(&db.trieStorageSegmentIndexStats, segmentIndexSource.fromCache(), duration)
		recordTrieStorageSegmentIndexLayer(segmentIndexSource, duration, &db.trieStorageSegmentIndexLayerStats)
	}
	if err != nil {
		if errors.Is(err, errSegmentIndexEntryNotFound) {
			failure.reason = "segment-index-entry-not-found"
			return nil, failure, nil
		}
		failure.reason = "segment-index-read-failed"
		return nil, failure, err
	}
	if len(metas) == 0 {
		failure.reason = "segment-index-empty"
		return nil, failure, nil
	}
	meta := selectSegmentChunkMeta(metas, storageKey)
	if meta == nil {
		failure.reason = "segment-chunk-meta-not-found"
		// Log detailed information for debugging
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: failed to locate chunk for storage key - account=%x storage=%x folder=%s metas_count=%d\n",
			accountKey, storageKey, folderPath, len(metas))
		// Print key ranges for all metas
		for i, m := range metas {
			fmt.Fprintf(prefixdbLogWriter, "prefixdb DEBUG: chunk[%d] file=%s KeyStart=%x\n",
				i, m.FileName, m.KeyStart)
		}
		return nil, failure, nil
	}
	failure.chunkFile = meta.FileName
	if db.testSegmentedReadHook != nil {
		db.testSegmentedReadHook(folderPath, *meta)
	}
	chunkStart := time.Now()
	defer func() {
		recordTrieStorageGetBreakdownStep(&db.trieStorageKVStats, false, time.Since(chunkStart))
	}()
	gcMeta = meta
	value, readFailure, backing, err := db.readSegmentedChunkToCacheStreamingByPath(folderPath, meta.FileName, accountKey, storageKey, failure)
	gcBacking = backing
	return value, readFailure, err
}

func (db *PrefixDB) MigrateLegacySegmentIndexFormats() error {
	return errors.New("legacy segment index formats are no longer supported")
}

func (db *PrefixDB) rebuildSegmentIndexFilesLocked() error {
	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		folderPath := filepath.Join(db.storageDir, entry.Name())
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if _, err := os.Stat(indexPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		lockEntry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
		metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
		if err != nil {
			unlock()
			return err
		}
		if err := db.writeSegmentIndexLocked(folderPath, metas, lockEntry); err != nil {
			unlock()
			return err
		}
		db.invalidateSegmentIndexLayoutForPath(folderPath)
		db.refreshSegmentIndexCacheByPathLocked(folderPath, metas)
		unlock()
	}
	return nil
}

func (db *PrefixDB) RebuildSegmentIndexFiles() error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	return db.rebuildSegmentIndexFilesLocked()
}

func (db *PrefixDB) UpgradeSegmentIndexFiles() error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	return db.rebuildSegmentIndexFilesLocked()
}

// GCCollectGarbageChunks removes chunk files that are not referenced by the current
// segment index for the given folderID.
//
// This is an explicit, offline-style cleanup helper for the "Option B" GC strategy
// where old chunk files are intentionally left behind as garbage.
// It does not modify the index and serializes only with operations on the same folder.
func (db *PrefixDB) GCCollectGarbageChunks(folderID uint32) (int, error) {
	if db == nil || folderID == 0 {
		return 0, nil
	}
	folderPath := db.segmentedFolderPath(folderID)

	_, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()

	metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
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
		if strings.HasSuffix(name, ".dat.tmp") {
			if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return deleted, err
			}
			deleted++
			continue
		}

		// Only consider *.dat files.
		if !strings.HasSuffix(name, ".dat") {
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
		folderPath := filepath.Join(db.storageDir, entry.Name())
		lockEntry, unlock := db.lockSegmentIndexFolderEntry(folderPath)

		metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
		if err != nil {
			unlock()
			return err
		}
		if len(metas) == 0 {
			unlock()
			continue
		}
		allEntries := make([]kvPair, 0)
		for _, meta := range metas {
			entries, backing, err := db.readSegmentChunkFileWithUsageByPath(folderPath, meta.FileName, diskIOUsageStorageGC)
			if err != nil {
				unlock()
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
				fileName := chunkFileNameForOrdinal(uint32(i))
				_, err := db.writeChunkFileWithUsage(folderPath, fileName, chunk, diskIOUsageStorageGC)
				if err != nil {
					unlock()
					return err
				}
				updated = append(updated, segmentChunkMeta{
					FileName: fileName,
					KeyStart: cloneBytes(chunk[0].key),
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
				unlock()
				return err
			}
		}

		if err := db.writeSegmentIndexLocked(folderPath, updated, lockEntry); err != nil {
			unlock()
			return err
		}
		db.invalidateSegmentIndexLayoutForPath(folderPath)
		db.refreshSegmentIndexCacheByPathLocked(folderPath, updated)
		if accountKey, ok := db.managedAccountKeyForFolderPath(folderPath); ok {
			db.syncStorageCacheEntries(accountKey, allEntries)
		}
		unlock()
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

// RunPostLoadGC performs the full compaction steps expected after bulk load.
// It always sweeps all node files with unsorted data and all segmented storage folders.
func (db *PrefixDB) RunPostLoadGC() error {
	db.writeMutex.Lock()
	if count := db.prefixTree.CompactAllNodeFiles(); count < 0 {
		db.writeMutex.Unlock()
		return fmt.Errorf("prefix tree GC failed")
	}
	db.writeMutex.Unlock()
	if err := db.GCAllStorageChunkFiles(); err != nil {
		return err
	}
	return nil
}
