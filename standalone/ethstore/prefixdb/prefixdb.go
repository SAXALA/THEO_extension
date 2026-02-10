package prefixdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	lru "github.com/hashicorp/golang-lru"
)

const storageMaxFileSize int64 = 1 << 30 // 1GB

const (
	storageSegmentThreshold           = 1 * 64 * 1024  // per account before folder split
	storageChunkSize                  = 1 * 64 * 1024  // target size of each chunk file
	segmentedChunkHardLimit           = 1 * 128 * 1024 // hard cap for individual chunk files
	storageGCTriggerSize              = segmentedChunkHardLimit
	segmentedStorageFlag       uint32 = 1 << 31
	segmentedDirNamePrefix            = "storage_seg_"
	segmentIndexFileName              = "index.meta"
	segmentIndexCacheThreshold        = 1 * 1024 // cache indexes larger than 1KB
	segmentIndexCacheCapacity         = 64       // number of large index folders retained in memory
)

const (
	segmentIndexMultiLevelThreshold = 16 * 1024
	segmentIndexLevel2TargetSize    = 4 * 1024
	segmentIndexLevel2MaxSize       = 8 * 1024
	segmentIndexMultiLevelMagic     = 0x4d4c4958 // 'MLIX'
	segmentIndexFormatVersion       = 1
)

const segmentIndexLevel2Pattern = "index.meta.l2.%08d"

const chunkNormalizationThreshold uint64 = 256 // chunk files over this size have been appended

const ()

type DatabaseType int

const (
	StateDB DatabaseType = iota
	SnapshotDB
)

func (dbType DatabaseType) String() string {
	switch dbType {
	case StateDB:
		return "StateDB"
	case SnapshotDB:
		return "SnapshotDB"
	default:
		return "UnknownDB"
	}
}

type KeyType int

const (
	TrieAccount KeyType = iota // TrieAccount
	TrieStorage                // TrieStorage
	TrieCode                   // Code
	TASnapshot                 // SnapshotAccount
	TSSnapshot                 // SnapshotStorage
)

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

type storageCacheEntry map[string][]byte

type PrefixDB struct {
	databaseType DatabaseType
	prefixTree   *PrefixTree
	accountFile  *os.File
	// slotFile    *os.File

	nodeCache *NodeCache
	batch     *WriteBatch
	// triePath             string       // path to the prefix tree file
	accountHashKeyPebble *PebbleStore // pebble store for account hash key index
	// hashIndex  hashIndex to aviod hash collision
	writeMutex sync.Mutex // mutex for writeCommit

	storageBufLock sync.Mutex
	storageChunk   storageChunkBuffer

	storageDir       string
	storageCurFile   *os.File
	storageCurFileID uint32
	storageCurSize   int64
	storageBuf       storageOpBuffer
	segmentDirSeq    uint32

	// a index file maybe accessed frequently
	storageIndexFolderId uint32
	storageIndexMetas    []segmentChunkMeta
	storageIndexCache    *lru.Cache
	storageIndexReusable bool
	storageIndexArena    []byte

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

	storageCache *lru.Cache
	// for debug
	totalOps   uint64
	cachedOps  uint64
	timeOnRead time.Duration
	readCount  uint64
	sortedOps  int
}

// SerializedTrieNode
type SerializedTrieNode struct {
	Path        string
	IsLeaf      bool
	SlotIndices []int
	Offset      int64
}

/**
 * NewPrefixDB creates a new PrefixDB instance.
 */
func NewPrefixDB(dirpath string, databaseType DatabaseType) (*PrefixDB, error) {
	fmt.Println(databaseType.String() + " prefixDB Initializing...")
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
	}

	// Ensure base directory exists
	if err := os.MkdirAll(cfg.BaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base dir: %v", err)
	}

	// pebblePath := filepath.Join("/mnt/ssd/ethstore/index/accountHash_key_pebble")

	// Resolve paths
	accountFilePath := resolvePath(cfg.BaseDir, cfg.AccountDir)
	triePath := resolvePath(cfg.BaseDir, cfg.TrieDir)
	pebblePath := resolvePath(cfg.BaseDir, cfg.PebblePath)
	storageDir := resolvePath(cfg.BaseDir, cfg.StorageDir)

	// Ensure directories exist
	if err := os.MkdirAll(filepath.Dir(accountFilePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create account dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(triePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create trie dir: %v", err)
	}

	accountFile, err := os.OpenFile(accountFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("failed to open normal account file")
	}

	db := &PrefixDB{
		accountFile:  accountFile,
		batch:        NewWriteBatch(cfg.WriteBatchSize),
		writeMutex:   sync.Mutex{},
		storageDir:   storageDir,
		databaseType: databaseType,
	}

	if err := os.MkdirAll(db.storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage dir: %v", err)
	}
	if err := db.openOrCreateStorageFile(); err != nil {
		return nil, fmt.Errorf("failed to init storage shard: %v", err)
	}

	nodeCache, err := NewNodeCache(cfg.MaxCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to init node cache: %v", err)
	}
	db.nodeCache = nodeCache

	prefixTree, err := NewPrefixTree(db, dirpath, db.databaseType)
	if err != nil {
		return nil, fmt.Errorf("failed to create prefix tree: %v", err)
	}

	db.prefixTree = prefixTree

	db.accountHashKeyPebble, err = NewPebbleStore(pebblePath, 0, 0, "", false)
	if err != nil {
		return nil, fmt.Errorf("failed to create PebbleStore: %v", err)
	}

	indexCache, err := lru.New(segmentIndexCacheCapacity)
	if err != nil {
		return nil, fmt.Errorf("failed to init segment index cache: %v", err)
	}
	db.storageIndexCache = indexCache

	storageCache, err := lru.New(4096)
	if err != nil {
		return nil, fmt.Errorf("failed to init storage cache: %v", err)
	}
	db.storageCache = storageCache

	db.startStorageGCWorker()

	db.batch.EnableAutoCommit(db, 1024) // enable auto commit with a threshold of 1024 operations

	fmt.Println(databaseType.String() + " prefixDB Initialized.")
	return db, nil
}

func (db *PrefixDB) Get(key []byte, accountKey []byte) ([]byte, bool, error) {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return nil, false, err
	}

	switch keyType {
	case TrieAccount, TASnapshot:
		cacheKey := bytesToString(key)
		if entry, ok := db.nodeCache.Get(cacheKey); ok && entry.Value != nil {
			return entry.Value, true, nil
		}

		if db.batch != nil {
			if value, _, ok := db.batch.get(key); ok {
				return value, true, nil
			}
		}

		node, err := db.getNode(key)
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
		return value, true, nil

	case TrieStorage, TSSnapshot:
		switch db.databaseType {
		case StateDB:
			if accountKey == nil {
				fmt.Printf("Parent account key not found for %x\n", key)
				return nil, false, nil
			}
		case SnapshotDB:
			accountKey = key[1:33] // first 32 bytes is prefix
			accountKey = append([]byte{'a'}, accountKey...)
		}

		if value, ok := db.storageCache.Get(bytesToString(key)); ok {
			valueBytes := value.([]byte)
			return valueBytes, true, nil
		}

		value, ok, err := db.ensureAccountStorageBuffered(accountKey, key)
		if err != nil {
			fmt.Println("Error ensuring account storage buffered:", err)
			return nil, false, err
		}
		if ok {
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
	case TrieAccount, TASnapshot:
		cacheKey := bytesToString(key)
		var stroageInfo StorageInfo
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			stroageInfo = entry.StorageInfo
			db.nodeCache.UpdateValue(cacheKey, value)
		} else {
			node, err := db.getNode(key)
			if err != nil {
				return err
			}
			if node != nil {
				stroageInfo = StorageInfo{
					storageFileID: node.storageFileID,
					storageOffset: node.storageOffset,
					storageSize:   node.storageSize,
				}
				db.nodeCache.StoreMetadata(cacheKey, node.offset, stroageInfo)
				db.nodeCache.UpdateValue(cacheKey, value)
			}
		}
		if db.batch != nil {
			db.batch.add(key, value, stroageInfo.storageFileID, stroageInfo.storageOffset, stroageInfo.storageSize, ValueModified)
		}

	case TrieStorage, TSSnapshot:
		// db.totalOps++
		// if db.totalOps%10000 == 0 {
		// 	fmt.Printf("Total Ops: %d, Cached Ops: %d, Sorted Ops: %d, Read Count: %d, Time on Read: %s\n",
		// 		db.totalOps, db.cachedOps, db.sortedOps, db.readCount, db.timeOnRead)
		// }
		if db.storageCache != nil {
			db.storageCache.Remove(bytesToString(key))
		}
		switch db.databaseType {
		case StateDB:
			if accountKey == nil {
				fmt.Printf("Parent account key not found for %x\n", key)
				return nil
			}
		case SnapshotDB:
			accountKey = key[1:33]                          // first 32 bytes is prefix
			accountKey = append([]byte{'a'}, accountKey...) // add prefix 'a' for account key
		}
		return db.bufferStorageMutation(accountKey, key, value)
	}
	return nil
}

func (db *PrefixDB) Has(key []byte, accountKey []byte) (bool, error) {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return false, err
	}

	switch keyType {
	case TrieAccount, TASnapshot:
		cacheKey := bytesToString(key)
		if _, ok := db.nodeCache.Get(cacheKey); ok {
			return true, nil
		}

		if db.batch != nil {
			if _, _, ok := db.batch.get(key); ok {
				return true, nil
			}
		}

		node, err := db.getNode(key)
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
		return true, nil
	case TrieStorage, TSSnapshot:
		switch db.databaseType {
		case StateDB:
			if accountKey == nil {
				fmt.Printf("Parent account key not found for %x\n", key)
				return false, nil
			}
		case SnapshotDB:
			accountKey = key[1:33] // first 32 bytes is prefix
			accountKey = append([]byte{'a'}, accountKey...)
		}
		if _, ok := db.storageCache.Get(bytesToString(key)); ok {
			return true, nil
		}
		_, ok, err := db.ensureAccountStorageBuffered(accountKey, key)
		if err != nil {
			fmt.Println("Error ensuring account storage buffered:", err)
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
	case TrieAccount, TASnapshot:
		if db.batch != nil {
			db.batch.delete(key)
		}
		if db.nodeCache != nil {
			db.nodeCache.Delete(bytesToString(key))
		}
		return db.storeNode(key, &TrieNode{
			storageFileID: 0,
			storageOffset: 0,
			offset:        0,
			storageSize:   0,
		})

	case TrieStorage, TSSnapshot:
		// db.totalOps++
		// if db.totalOps%10000 == 0 {
		// 	fmt.Printf("Total Ops: %d, Cached Ops: %d, Sorted Ops: %d, Read Count: %d, Time on Read: %s\n",
		// 		db.totalOps, db.cachedOps, db.sortedOps, db.readCount, db.timeOnRead)
		// }

		if db.storageCache != nil {
			db.storageCache.Remove(bytesToString(key))
		}
		switch db.databaseType {
		case StateDB:
			if accountKey == nil {
				fmt.Printf("Parent account key not found for %x\n", key)
				return nil
			}
		case SnapshotDB:
			accountKey = key[1:33] // first 32 bytes is prefix
			accountKey = append([]byte{'a'}, accountKey...)
		}
		return db.bufferStorageMutation(accountKey, key, nil)

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
		if db.batch != nil {
			_ = db.batch.updateStoragePointer(stringToBytes(buf.accountKey), StorageInfo{})
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
		if db.batch != nil {
			_ = db.batch.updateStoragePointer(stringToBytes(buf.accountKey), StorageInfo{
				storageFileID: fileID,
				storageOffset: off,
				storageSize:   sz,
			})
		}
	}
	db.invalidateStorageBuffer(buf.accountKey)
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

	_, err := file.ReadAt(header, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read header at offset %d: %v", offset, err)
	}

	keySize := int(uint16(header[0])<<8 | uint16(header[1]))
	valueSize := int(uint16(header[2])<<8 | uint16(header[3]))

	totalSize := keySize + valueSize

	combinedData := getDataBuffer(totalSize)
	defer putDataBuffer(combinedData)

	_, err = file.ReadAt(combinedData, offset+4)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read combined data at offset %d: %v", offset+6, err)
	}

	value := make([]byte, valueSize)
	copy(value, combinedData[keySize:totalSize])

	return value, nil
}

func (db *PrefixDB) Close() error {
	errs := []error{}

	db.stopStorageGCWorker()

	if err := db.flushStorageBuffer(); err != nil {
		errs = append(errs, fmt.Errorf("failed to flush storage buffer: %v", err))
	}

	db.releaseStorageBuffer()

	if db.nodeCache != nil {
		db.nodeCache.Close()
	}

	// if db.storageCache != nil {
	// 	db.storageCache.Close()
	// }

	// forbid further writes to the database
	if db.batch != nil {
		db.batch.DisableAutoCommit()

		// wait for any ongoing background commit to finish
		if db.batch.bgCommit {
			db.batch.DisableBackgroundCommit()
		}
	}

	if db.batch != nil {
		if len(db.batch.operations) > 0 {
			if err := db.WriteCommit(db.batch); err != nil {
				fmt.Printf("Error committing batch operations: %v\n", err)
			}
		}
	}

	if err := db.prefixTree.Close(); err != nil {
		return fmt.Errorf("failed to close prefix tree: %v", err)
	}

	// if err := db.SaveSlotIndex(); err != nil {
	// 	return fmt.Errorf("failed to save prefix tree: %v", err)
	// }

	if err := db.accountFile.Sync(); err != nil {
		// Check if file is already closed
		if !errors.Is(err, os.ErrClosed) {
			errs = append(errs, fmt.Errorf("failed to sync account file: %v", err))
		}
	}

	// if err := db.slotFile.Sync(); err != nil {
	// 	errs = append(errs, fmt.Errorf("failed to sync slot file: %v", err))
	// }

	if err := db.accountFile.Close(); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
	}

	// if err := db.slotFile.Close(); err != nil {
	// 	errs = append(errs, err)
	// }

	db.nodeCache = nil
	db.batch = nil

	if db.storageCurFile != nil {
		_ = db.storageCurFile.Sync()
		_ = db.storageCurFile.Close()
		db.storageCurFile = nil
	}
	// db.accountHashKeyPebble = nil

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
	case 'c':
		return TrieCode, nil
	case 'a':
		return TASnapshot, nil
	case 'o':
		return TSSnapshot, nil
	default:
	}
	return -1, errors.New("unknown key type")
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
	// accountHashStr := hex.EncodeToString(accountHash)
	// item, err := db.memcache.Get(accountHashStr)
	// if err != nil {
	// 	if err == memcache.ErrCacheMiss {
	// 		return nil // account not found in cache
	// 	}
	// 	fmt.Printf("Error retrieving account key from mem cache: %v\n", err)
	// 	return nil
	// }
	// return item.Value
}

func (db *PrefixDB) storeNode(key []byte, node *TrieNode) error {
	return db.prefixTree.Put(key, node.offset, node.storageFileID, node.storageOffset, node.storageSize)
}

func (db *PrefixDB) getNode(key []byte) (*TrieNode, error) {
	cacheKey := bytesToString(key)
	if entry, ok := db.nodeCache.Get(cacheKey); ok {
		if entry.AccountOffset != 0 || entry.StorageInfo.storageFileID != 0 || entry.Value != nil {
			return &TrieNode{
				storageFileID: entry.StorageInfo.storageFileID,
				storageOffset: entry.StorageInfo.storageOffset,
				storageSize:   entry.StorageInfo.storageSize,
				offset:        entry.AccountOffset,
			}, nil
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
	db.nodeCache.StoreMetadata(cacheKey, node.offset, StorageInfo{
		storageFileID: node.storageFileID,
		storageOffset: node.storageOffset,
		storageSize:   node.storageSize,
	})
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
	if size <= storageSegmentThreshold {
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
		if err := os.WriteFile(fullPath, seg, 0644); err != nil {
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
		if chunkSize+sz > storageChunkSize && len(chunk) > 0 {
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

// filterDeletedPairs drops tombstoned entries (values set to nil) before persistence.
func filterDeletedPairs(kvs []kvPair) []kvPair {
	if len(kvs) == 0 {
		return kvs
	}
	out := kvs[:0]
	for _, kv := range kvs {
		if kv.val == nil {
			continue
		}
		out = append(out, kv)
	}
	return out
}

func (db *PrefixDB) updateSegmentedStorage(existingFileID uint32, kvs []kvPair) (uint32, int64, uint64, error) {
	folderID := existingFileID & ^segmentedStorageFlag
	metas, err := db.readSegmentIndex(folderID)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(metas) == 0 {
		return 0, 0, 0, fmt.Errorf("segment index missing for folder %d", folderID)
	}
	buckets := partitionEntriesByChunks(metas, kvs)
	folderPath := db.segmentedFolderPath(folderID)
	updated := make([]segmentChunkMeta, 0, len(metas)+len(kvs)/64+1)
	for idx, meta := range metas {
		additions := buckets[idx]
		if len(additions) == 0 {
			updated = append(updated, meta)
			continue
		}
		chunkMetas, err := db.mutateSegmentChunk(folderPath, meta, additions)
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
	name := fmt.Sprintf("chunk_%04d.dat", a.next)
	a.next++
	return name
}

func parseChunkOrdinal(name string) int {
	var idx int
	if _, err := fmt.Sscanf(name, "chunk_%04d.dat", &idx); err == nil {
		return idx
	}
	return -1
}

func (db *PrefixDB) mutateSegmentChunk(folderPath string, meta segmentChunkMeta, additions []kvPair) ([]segmentChunkMeta, error) {
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
	seg, release, _, err := db.serializeStorageSegment(additions)
	if err != nil {
		return err
	}
	defer release()
	data := seg[4:]
	if _, err := f.WriteAt(data, currentSize); err != nil {
		return err
	}
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

func (db *PrefixDB) rewriteChunkWithDedup(folderID uint32, folderPath string, meta segmentChunkMeta, additions []kvPair, allocator *chunkFileAllocator, existing []kvPair, backing *bufferLease) ([]segmentChunkMeta, error) {
	var err error
	if existing == nil {
		existing, backing, err = db.readSegmentChunkFile(folderID, meta.FileName)
		if err != nil {
			return nil, err
		}
	}
	if backing != nil {
		defer backing.Release()
	}
	merged := mergeAndDedupPairs(existing, additions)
	if len(merged) == 0 {
		fullPath := filepath.Join(folderPath, meta.FileName)
		if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		return nil, nil
	}
	chunks := splitEntriesBySize(merged, segmentedChunkHardLimit)
	result := make([]segmentChunkMeta, 0, len(chunks))
	var chunkSize int
	for idx, chunk := range chunks {
		name := meta.FileName
		if idx > 0 {
			name = allocator.nextName()
		}
		if chunkSize, err = db.writeChunkFile(folderPath, name, chunk); err != nil {
			return nil, err
		}
		result = append(result, segmentChunkMeta{
			FileName:  name,
			KeyStart:  cloneBytes(chunk[0].key),
			KeyEnd:    cloneBytes(chunk[len(chunk)-1].key),
			KVCount:   uint32(len(chunk)),
			ChunkSize: uint64(chunkSize),
		})
	}
	return result, nil
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

func splitEntriesBySize(entries []kvPair, limit int64) [][]kvPair {
	if len(entries) == 0 {
		return nil
	}
	chunks := make([][]kvPair, 0, len(entries)/64+1)
	start := 0
	var size int64 = 4
	for i := 0; i < len(entries); i++ {
		entrySize := int64(6 + len(entries[i].key) + len(entries[i].val))
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

func (db *PrefixDB) writeChunkFile(folderPath, fileName string, entries []kvPair) (int, error) {
	seg, release, chunkSize, err := db.serializeStorageSegment(entries)
	if err != nil {
		return 0, err
	}
	defer release()
	fullPath := filepath.Join(folderPath, fileName)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, err
	}
	if _, err := f.Write(seg); err != nil {
		f.Close()
		return 0, err
	}
	return chunkSize, f.Close()
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

func level2IndexFilePath(folderPath string, metaID uint32) string {
	return filepath.Join(folderPath, fmt.Sprintf(segmentIndexLevel2Pattern, metaID))
}

func estimateSegmentEntrySize(meta segmentChunkMeta) int {
	return 2 + len(meta.FileName) + 2 + len(meta.KeyStart) + 2 + len(meta.KeyEnd) + 4 + 8
}

func estimateSegmentIndexSize(metas []segmentChunkMeta) int {
	total := 4
	for _, meta := range metas {
		total += estimateSegmentEntrySize(meta)
	}
	return total
}

func encodeSegmentChunkMetas(metas []segmentChunkMeta) ([]byte, error) {
	buf := make([]byte, 0, estimateSegmentIndexSize(metas))
	var tmp32 [4]byte
	var tmp64 [8]byte
	writeUint32BE(tmp32[:], uint32(len(metas)))
	buf = append(buf, tmp32[:]...)
	for _, meta := range metas {
		var err error
		if buf, err = appendVarBytes(buf, []byte(meta.FileName)); err != nil {
			return nil, err
		}
		if buf, err = appendVarBytes(buf, meta.KeyStart); err != nil {
			return nil, err
		}
		if buf, err = appendVarBytes(buf, meta.KeyEnd); err != nil {
			return nil, err
		}
		writeUint32BE(tmp32[:], meta.KVCount)
		buf = append(buf, tmp32[:]...)
		writeUint64BE(tmp64[:], meta.ChunkSize)
		buf = append(buf, tmp64[:]...)
	}
	return buf, nil
}

func writeFileIfChanged(path string, data []byte) error {
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, data) {
			return nil
		}
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
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
			chunkPath := filepath.Join(chunkDir, string(nameBytes))
			info, err := os.Stat(chunkPath)
			if err != nil {
				return err
			}
			chunkSize = uint64(info.Size())
		} else {
			return io.ErrUnexpectedEOF
		}
		meta := segmentChunkMeta{
			FileName:  string(nameBytes),
			KVCount:   kvCount,
			ChunkSize: chunkSize,
		}
		meta.KeyStart = cloneIntoArena(arena, start)
		meta.KeyEnd = cloneIntoArena(arena, end)
		*metas = append(*metas, meta)
	}
	return nil
}

func (db *PrefixDB) loadSegmentIndexLayout(folderPath string) (segmentIndexLayout, error) {
	if db.storageIndexLayoutReady && db.storageIndexLayoutPath == folderPath {
		return db.storageIndexLayoutCache, nil
	}
	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	data, err := os.ReadFile(indexPath)
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
	db.storageIndexLayoutPath = folderPath
	db.storageIndexLayoutCache = layout
	db.storageIndexLayoutReady = true
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
	if len(metas) == 0 {
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := os.Remove(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
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
		if err := writeFileIfChanged(indexPath, buf); err != nil {
			return err
		}
		return removeLevel2IndexFiles(folderPath, nil)
	}
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return err
	}
	if layout.mode != indexLayoutMultiLevel {
		layout = segmentIndexLayout{mode: indexLayoutMultiLevel, nextMetaID: 1}
	}
	groups := splitSegmentMetas(metas)
	if len(groups) == 0 {
		groups = [][]segmentChunkMeta{metas}
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
	if err := removeLevel2IndexFiles(folderPath, keep); err != nil {
		return err
	}
	newEntries := make([]segmentIndexL1Entry, 0, len(groups))
	for idx, group := range groups {
		buf, err := encodeSegmentChunkMetas(group)
		if err != nil {
			return err
		}
		path := level2IndexFilePath(folderPath, idAssignments[idx])
		if err := writeFileIfChanged(path, buf); err != nil {
			return err
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
		if err := writeFileIfChanged(indexPath, buf); err != nil {
			return err
		}
	}
	return nil
}

func (db *PrefixDB) invalidateSegmentIndexCache(folderID uint32) {
	if folderID == 0 {
		return
	}
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
	if db.storageIndexFolderId == folderID && len(db.storageIndexMetas) > 0 {
		return db.storageIndexMetas, nil
	}
	if db.storageIndexCache != nil {
		if cached, ok := db.storageIndexCache.Get(folderID); ok {
			if metas, ok := cached.([]segmentChunkMeta); ok {
				db.storageIndexFolderId = folderID
				db.storageIndexMetas = metas
				db.storageIndexReusable = false
				db.storageIndexArena = nil
				return metas, nil
			}
		}
	}
	folderPath := db.segmentedFolderPath(folderID)
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return nil, err
	}
	var metas []segmentChunkMeta
	var arena []byte
	if layout.mode == indexLayoutMultiLevel {
		total := 0
		for _, entry := range layout.entries {
			total += int(entry.ChunkCount)
		}
		if db.storageIndexReusable && cap(db.storageIndexMetas) >= total {
			metas = db.storageIndexMetas[:0]
		} else {
			metas = make([]segmentChunkMeta, 0, total)
		}
		if cap(db.storageIndexArena) > 0 {
			arena = db.storageIndexArena[:0]
		}
		for idx, entry := range layout.entries {
			data, err := os.ReadFile(level2IndexFilePath(folderPath, entry.MetaID))
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
			data, err = os.ReadFile(indexPath)
			if err != nil {
				return nil, err
			}
		}
		if db.storageIndexReusable {
			metas = db.storageIndexMetas[:0]
		} else {
			metas = nil
		}
		if cap(db.storageIndexArena) > 0 {
			arena = db.storageIndexArena[:0]
		}
		if err := decodeSegmentIndexBuffer(data, &metas, &arena, false, folderPath); err != nil {
			return nil, err
		}
	}
	db.storageIndexFolderId = folderID
	db.storageIndexMetas = metas
	db.storageIndexReusable = true
	db.storageIndexArena = arena
	estimatedSize := estimateSegmentIndexSize(metas)
	if estimatedSize >= segmentIndexCacheThreshold && db.storageIndexCache != nil {
		db.storageIndexCache.Add(folderID, cloneSegmentChunkMetas(metas))
	}
	return metas, nil
}

func (db *PrefixDB) readSegmentIndexForKey(folderID uint32, key []byte) ([]segmentChunkMeta, error) {
	if len(key) == 0 {
		return db.readSegmentIndex(folderID)
	}
	if db.storageIndexFolderId == folderID && len(db.storageIndexMetas) > 0 {
		return db.storageIndexMetas, nil
	}
	if db.storageIndexCache != nil {
		if cached, ok := db.storageIndexCache.Get(folderID); ok {
			if metas, ok := cached.([]segmentChunkMeta); ok {
				db.storageIndexFolderId = folderID
				db.storageIndexMetas = metas
				db.storageIndexReusable = false
				db.storageIndexArena = nil
				return metas, nil
			}
		}
	}
	folderPath := db.segmentedFolderPath(folderID)
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return nil, err
	}
	if layout.mode != indexLayoutMultiLevel {
		return db.readSegmentIndex(folderID)
	}
	entry := selectSegmentL1Entry(layout.entries, key)
	if entry == nil {
		return nil, fmt.Errorf("segment index entry not found for folder %d", folderID)
	}
	if db.storageIndexPartialFolder == folderID && db.storageIndexPartialMetaID == entry.MetaID && len(db.storageIndexPartialMetas) > 0 {
		return db.storageIndexPartialMetas, nil
	}
	metas := db.storageIndexPartialMetas
	arena := db.storageIndexPartialArena
	if !db.storageIndexPartialReusable {
		metas = nil
		arena = nil
	}
	data, err := os.ReadFile(level2IndexFilePath(folderPath, entry.MetaID))
	if err != nil {
		return nil, err
	}
	if err := decodeSegmentIndexBuffer(data, &metas, &arena, false, folderPath); err != nil {
		return nil, err
	}
	db.storageIndexPartialFolder = folderID
	db.storageIndexPartialMetaID = entry.MetaID
	db.storageIndexPartialMetas = metas
	db.storageIndexPartialArena = arena
	db.storageIndexPartialReusable = true
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
			FileName:  src[i].FileName,
			KVCount:   src[i].KVCount,
			ChunkSize: src[i].ChunkSize,
		}
		dst[i].KeyStart = cloneBytes(src[i].KeyStart)
		dst[i].KeyEnd = cloneBytes(src[i].KeyEnd)
	}
	return dst
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

func (db *PrefixDB) readSegmentedChunk(fileID uint32, storageKey []byte) ([]kvPair, *bufferLease, *segmentChunkMeta, error) {
	folderID := fileID & ^segmentedStorageFlag
	metas, err := db.readSegmentIndexForKey(folderID, storageKey)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(metas) == 0 {
		return nil, nil, nil, nil
	}
	meta := selectSegmentChunkMeta(metas, storageKey)
	if meta == nil {
		return nil, nil, nil, nil
	}
	payload, kvCount, backing, err := db.readSegmentChunkPayload(folderID, meta.FileName)
	if err != nil {
		return nil, nil, nil, err
	}
	entries, err := db.buildStorageEntries(payload, kvCount)
	if err != nil {
		if backing != nil {
			backing.Release()
		}
		return nil, nil, nil, err
	}
	entries = db.maybeNormalizeChunkEntries(entries, meta)
	var gcBacking *bufferLease
	if backing != nil {
		gcBacking = backing.Retain()
	}
	db.maybeScheduleStorageGC(folderID, meta, gcBacking)
	return entries, backing, meta, nil
}

func (db *PrefixDB) readSegmentChunkFile(folderID uint32, fileName string) ([]kvPair, *bufferLease, error) {
	lease, err := db.readSegmentFileBuffer(folderID, fileName)
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
	lease, err := db.readSegmentFileBuffer(folderID, fileName)
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
	fullPath := filepath.Join(db.segmentedFolderPath(folderID), fileName)
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
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
	if _, err := io.ReadFull(f, buf[:intSize]); err != nil {
		putDataBuffer(buf)
		return nil, err
	}
	return newBufferLease(buf[:intSize]), nil
}

func (db *PrefixDB) maybeNormalizeChunkEntries(entries []kvPair, meta *segmentChunkMeta) []kvPair {
	if len(entries) < 2 || meta == nil {
		return entries
	}
	if meta.ChunkSize <= chunkNormalizationThreshold {
		return entries
	}
	return normalizeStorageEntries(entries)
}

// ensureAccountStorageBuffered loads (and optionally returns) the storage chunk entry for storageKey
// so repeated GET/Has calls over the same account avoid redundant chunk scans.
func (db *PrefixDB) ensureAccountStorageBuffered(accountKey, storageKey []byte) ([]byte, bool, error) {
	if len(accountKey) == 0 {
		return nil, false, nil
	}
	ak := bytesToString(accountKey)

	db.storageBufLock.Lock()
	if db.storageChunk.accountKey == ak && db.storageChunk.covers(ak, storageKey) {
		value, ok := db.storageChunk.lookup(storageKey)
		db.storageBufLock.Unlock()
		return value, ok, nil
	}
	db.storageBufLock.Unlock()

	cacheInfo, err := db.resolveAccountStoragePointer(ak, accountKey)
	if err != nil {
		return nil, false, err
	}

	if cacheInfo.storageFileID == 0 {
		db.adoptStorageBuffer(ak, nil, nil)
		return nil, false, nil
	}

	var (
		storage []kvPair
		backing *bufferLease
	)
	if isSegmentedStorage(cacheInfo.storageFileID) {
		val := db.readSegmentedChunkToCache(cacheInfo.storageFileID, storageKey)
		if val == nil {
			return nil, false, nil
		}
		return val, true, nil

	} else {
		storage, backing, err = db.readStorageSegmentToMap(cacheInfo.storageFileID, cacheInfo.storageOffset, cacheInfo.storageSize)
	}

	if err != nil {
		if backing != nil {
			backing.Release()
		}
		return nil, false, err
	}

	var (
		value []byte
		found bool
	)
	if len(storageKey) > 0 && len(storage) > 0 {
		if idx, ok := binarySearchKVPairs(storage, storageKey); ok {
			value = storage[idx].val
			found = true
		}
	}

	db.adoptStorageBuffer(ak, storage, backing)
	return value, found, nil
}

func (db *PrefixDB) adoptStorageBuffer(accountKey string, entries []kvPair, backing *bufferLease) {
	db.storageBufLock.Lock()
	defer db.storageBufLock.Unlock()
	db.storageChunk.adopt(accountKey, entries, backing)
}

func (db *PrefixDB) borrowStorageEntries(count int) []kvPair {
	if count <= 0 {
		return nil
	}
	db.storageBufLock.Lock()
	entries := db.storageChunk.borrowEntries(count)
	db.storageBufLock.Unlock()
	return entries
}

func (db *PrefixDB) releaseStorageEntries(entries []kvPair) {
	if entries == nil {
		return
	}
	db.storageBufLock.Lock()
	db.storageChunk.returnEntries(entries)
	db.storageBufLock.Unlock()
}

func (db *PrefixDB) buildStorageEntries(payload []byte, kvCount int) ([]kvPair, error) {
	if kvCount == 0 {
		return nil, nil
	}
	entries := db.borrowStorageEntries(kvCount)
	decoded, err := buildPairsFromPayload(payload, kvCount, entries)
	if err != nil {
		db.releaseStorageEntries(entries)
		return nil, err
	}
	return decoded, nil
	// return normalizeStorageEntries(decoded), nil
}

func (db *PrefixDB) invalidateStorageBuffer(accountKey string) {
	if accountKey == "" {
		return
	}
	db.storageBufLock.Lock()
	db.storageChunk.invalidate(accountKey)
	db.storageBufLock.Unlock()
}

func (db *PrefixDB) releaseStorageBuffer() {
	db.storageBufLock.Lock()
	db.storageChunk.reset()
	db.storageBufLock.Unlock()
}

func normalizeStorageEntries(entries []kvPair) []kvPair {
	if len(entries) <= 1 {
		return entries
	}
	keys := make([]string, len(entries))
	lastIdx := make(map[string]int, len(entries))
	for i := range entries {
		keyStr := string(entries[i].key)
		keys[i] = keyStr
		lastIdx[keyStr] = i
	}
	out := entries[:0]
	for i := range entries {
		if lastIdx[keys[i]] != i {
			continue
		}
		out = append(out, entries[i])
	}
	sortKVPairs(out)
	return out
}

func (db *PrefixDB) resolveAccountStoragePointer(accountKey string, keyBytes []byte) (StorageInfo, error) {
	if entry, ok := db.nodeCache.Get(accountKey); ok && entry.StorageInfo.storageFileID != 0 {
		return entry.StorageInfo, nil
	}

	node, err := db.getNode(keyBytes)
	if err != nil {
		return StorageInfo{}, err
	}

	if node != nil && node.storageFileID != 0 {
		cacheInfo := StorageInfo{
			storageFileID: node.storageFileID,
			storageOffset: node.storageOffset,
			storageSize:   node.storageSize,
		}
		db.nodeCache.StoreMetadata(accountKey, node.offset, cacheInfo)
		return cacheInfo, nil
	}

	return StorageInfo{}, nil
}

func (db *PrefixDB) readStorageSegmentToMap(fileID uint32, offset int64, size uint64) ([]kvPair, *bufferLease, error) {
	if isSegmentedStorage(fileID) {
		return nil, nil, fmt.Errorf("file %d references segmented storage", fileID)
	}
	p, _ := db.storagePathByFileID(fileID)

	f, err := os.Open(p)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	if size == 0 {
		return []kvPair{}, nil, nil
	}

	total := int(size)
	buf := getDataBuffer(total)
	read := 0
	for read < total {
		n, err := f.ReadAt(buf[read:total], offset+int64(read))
		if err != nil {
			if err == io.EOF && read+n == total {
				read += n
				break
			}
			putDataBuffer(buf)
			return nil, nil, err
		}
		read += n
	}
	if read != total {
		putDataBuffer(buf)
		return nil, nil, io.ErrUnexpectedEOF
	}
	buf = buf[:total]

	payload, kvCount, err := parseSegmentBuffer(buf)
	if err != nil {
		putDataBuffer(buf)
		return nil, nil, err
	}
	entries, err := db.buildStorageEntries(payload, kvCount)
	if err != nil {
		putDataBuffer(buf)
		return nil, nil, err
	}
	db.sortedOps += len(entries)
	return entries, newBufferLease(buf), nil
}

func (db *PrefixDB) readStorageSegmentPayload(fileID uint32, offset int64, size uint64) ([]byte, int, *bufferLease, error) {
	if isSegmentedStorage(fileID) {
		return db.readSegmentedStoragePayload(fileID)
	}
	p, _ := db.storagePathByFileID(fileID)
	f, err := os.Open(p)
	if err != nil {
		return nil, 0, nil, err
	}
	defer f.Close()
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
				break
			}
			putDataBuffer(buf)
			return nil, 0, nil, err
		}
		read += n
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
	metas, err := db.readSegmentIndex(folderID)
	if err != nil {
		return nil, 0, nil, err
	}
	if len(metas) == 0 {
		return nil, 0, nil, nil
	}
	totalCount := 0
	totalSize := 0
	type chunkData struct {
		payload []byte
		count   int
		backing *bufferLease
	}
	pieces := make([]chunkData, 0, len(metas))
	for _, meta := range metas {
		payload, count, backing, err := db.readSegmentChunkPayload(folderID, meta.FileName)
		if err != nil {
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
		entries[i] = kvPair{
			key: payload[cursor : cursor+klen],
			val: payload[cursor+klen : cursor+totalLen],
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

	f, err := os.Open(p)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	if node.storageSize == 0 {
		return 0, 0, nil
	}

	// just read kv count
	buf := make([]byte, 10)

	n, err := f.ReadAt(buf, node.storageOffset)
	if err != nil && err != io.EOF {
		return 0, 0, err
	}
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

func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
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
	db.storageGCQueue = make(chan storageGCJob, 64)
	db.storageGCInFlight = make(map[string]struct{})
	db.storageGCStop = make(chan struct{})
	db.storageGCWait.Add(1)
	go func() {
		defer db.storageGCWait.Done()
		for {
			select {
			case job := <-db.storageGCQueue:
				db.processStorageGCJob(job)
			case <-db.storageGCStop:
				for {
					select {
					case job := <-db.storageGCQueue:
						db.processStorageGCJob(job)
					default:
						return
					}
				}
			}
		}
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
	if db.storageGCQueue == nil || meta.ChunkSize <= storageGCTriggerSize {
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
	defer db.finishStorageGCJob(job)
	if err := db.runStorageGCJob(job); err != nil {
		fmt.Printf("storage GC failed for folder %d file %s: %v\n", job.folderID, job.fileName, err)
	}
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
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	metas, err := db.readSegmentIndex(job.folderID)
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
	allocator := newChunkFileAllocator(metas)
	var (
		preloaded      []kvPair
		preloadBacking *bufferLease
	)
	if job.backing != nil {
		payload, kvCount, err := parseSegmentBuffer(job.backing.Bytes())
		if err == nil {
			entries := db.borrowStorageEntries(kvCount)
			if decoded, decErr := buildPairsFromPayload(payload, kvCount, entries); decErr == nil {
				preloaded = decoded
				preloadBacking = job.backing
				job.backing = nil
			} else {
				db.releaseStorageEntries(entries)
			}
		}
		if job.backing != nil {
			job.backing.Release()
			job.backing = nil
		}
	}
	chunkMetas, err := db.rewriteChunkWithDedup(job.folderID, folderPath, metas[idx], nil, allocator, preloaded, preloadBacking)
	if preloaded != nil {
		db.releaseStorageEntries(preloaded)
	}
	if err != nil {
		return err
	}
	updated := make([]segmentChunkMeta, 0, len(metas)-1+len(chunkMetas))
	updated = append(updated, metas[:idx]...)
	if len(chunkMetas) > 0 {
		updated = append(updated, chunkMetas...)
	}
	if idx+1 < len(metas) {
		updated = append(updated, metas[idx+1:]...)
	}
	if err := db.writeSegmentIndex(folderPath, updated); err != nil {
		return err
	}
	db.invalidateSegmentIndexCache(job.folderID)
	return nil
}

func (db *PrefixDB) InsertAccountHashPebble(accountHash []byte, pebbleKey []byte) error {
	return db.accountHashKeyPebble.Put(accountHash, pebbleKey)
}

func (db *PrefixDB) readSegmentedChunkToCache(fileID uint32, storageKey []byte) []byte {
	folderID := fileID & ^segmentedStorageFlag
	metas, err := db.readSegmentIndexForKey(folderID, storageKey)
	if err != nil || len(metas) == 0 {
		return nil
	}
	meta := selectSegmentChunkMeta(metas, storageKey)
	if meta == nil {
		return nil
	}
	lease, err := db.readSegmentFileBuffer(folderID, meta.FileName)
	if err != nil {
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
	res := []byte{}
	buf = buf[4:]
	cursor := 0
	bufLen := len(buf)
	var klen, vlen int
	hit := false
	var count int
	for i := 0; i < kvCount; i++ {
		if cursor+6 > bufLen {
			lease.Release()
			return res
		}
		header := buf[cursor : cursor+6]
		klen = int(header[0])<<8 | int(header[1])
		vlen = int(header[2])<<24 | int(header[3])<<16 | int(header[4])<<8 | int(header[5])
		cursor += 6
		totalLen := klen + vlen
		if cursor+totalLen > bufLen {
			lease.Release()
			return res
		}
		key := buf[cursor : cursor+klen]
		if bytes.HasPrefix(key, storageKey) {
			value := buf[cursor+klen : cursor+totalLen]
			if bytes.Equal(key, storageKey) {
				res = append([]byte(nil), value...)
				hit = true
			}
			if hit {
				valueCopy := append([]byte(nil), value...)
				db.storageCache.Add(string(key), valueCopy)
				count++
			}
			// if count >= 512 {
			// 	break
			// }
		}
		cursor += totalLen
	}
	// fmt.Println(count, " / ", kvCount)
	db.maybeScheduleStorageGC(folderID, meta, lease.Retain())
	lease.Release()
	return res
}

func (db *PrefixDB) UpgradeSegmentIndexFiles() error {
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
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		info, err := os.Stat(indexPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if info.Size() <= int64(segmentIndexMultiLevelThreshold) {
			continue
		}
		metas, err := db.readSegmentIndex(folderID)
		if err != nil {
			return err
		}
		if err := db.writeSegmentIndex(folderPath, metas); err != nil {
			return err
		}
		db.invalidateSegmentIndexCache(folderID)
	}
	return nil
}
