package prefixdb

import (
	"bytes"
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

	memcache "github.com/bradfitz/gomemcache/memcache"
	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

const MAX_CACHE_SIZE = 65535 // maximum cache size
const BUFFER_SIZE = 8192     // buffer size for file operations
const SLOT_SIZE = 8 * 1024   // size of each slot
const SLOT_NUM = 1024
const METADATA_SPACE = 1024 * 1024

const storageMaxFileSize int64 = 1 << 30 // 1GB

const (
	storageSegmentThreshold = 4 * 1024 * 1024 // 4MB per account before folder split
	storageChunkSize        = 4 * 1024 * 1024 // target size of each chunk file
	segmentedChunkHardLimit = 8 * 1024 * 1024 // hard cap for individual chunk files
)

const (
	segmentedStorageFlag   uint32 = 1 << 31
	segmentedDirNamePrefix        = "storage_seg_"
	segmentIndexFileName          = "index.meta"
)

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
	trieFile  *os.File
	indexfile string
	nodeCache *NodeCache
	slotCache *SlotCache
	batch     *WriteBatch
	// triePath             string       // path to the prefix tree file
	accountHashKeyPebble *PebbleStore // pebble store for account hash key index
	// hashIndex  hashIndex to aviod hash collision
	accountHashKeyIndex sync.Map // index for account keys
	memcache            *memcache.Client
	writeMutex          sync.Mutex // mutex for writeCommit

	storageDir       string
	storageCurFile   *os.File
	storageCurFileID uint32
	storageCurSize   int64
	storageBuf       storageOpBuffer
	segmentDirSeq    uint32

	// for debug
	totalOps   uint64
	cachedOps  uint64
	timeOnRead time.Duration
	readCount  uint64
	sortedOps  int
}

// SerializedTrieNode修改为直接存储完整路径
type SerializedTrieNode struct {
	Path        string // 完整的路径字符串
	IsLeaf      bool
	SlotIndices []int
	Offset      int64
}

/**
 * NewPrefixDB creates a new PrefixDB instance.
 */
func NewPrefixDB(dirpath string) (*PrefixDB, error) {
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
	slotIndexFile := resolvePath(cfg.BaseDir, cfg.SlotIndexFile)

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

	trieFile, err := os.OpenFile(triePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("failed to open prefix tree file")
	}

	db := &PrefixDB{
		accountFile:         accountFile,
		trieFile:            trieFile,
		batch:               NewWriteBatch(cfg.WriteBatchSize),
		writeMutex:          sync.Mutex{},
		indexfile:           slotIndexFile,
		accountHashKeyIndex: sync.Map{},
		storageDir:          storageDir,
	}

	if err := os.MkdirAll(db.storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage dir: %v", err)
	}
	if err := db.openOrCreateStorageFile(); err != nil {
		return nil, fmt.Errorf("failed to init storage shard: %v", err)
	}

	db.nodeCache = NewNodeCache(cfg.MaxCacheSize, db)
	db.slotCache = NewSlotCache(cfg.SlotCacheSize, db)

	prefixTree, err := NewPrefixTree(db, dirpath)
	if err != nil {
		return nil, fmt.Errorf("failed to create prefix tree: %v", err)
	}

	db.prefixTree = prefixTree

	db.accountHashKeyPebble, err = NewPebbleStore(pebblePath, 0, 0, "", false)
	if err != nil {
		return nil, fmt.Errorf("failed to create PebbleStore: %v", err)
	}

	db.memcache = memcache.New(cfg.MemcacheAddr)

	db.batch.EnableAutoCommit(db, 1024) // enable auto commit with a threshold of 1024 operations

	return db, nil
}

func (db *PrefixDB) Get(key []byte) ([]byte, bool, error) {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return nil, false, err
	}

	switch keyType {
	case TrieAccount:
		// check in cache
		var value []byte
		var ok bool
		if value, _, ok = db.nodeCache.Get(bytesToString(key)); ok {
			return value, true, nil
		}

		// check in batch
		if db.batch != nil {
			if value, _, ok = db.batch.get(key); ok {
				return value, true, nil
			}
		}

		node, err := db.getNode(key)

		if err != nil {
			return nil, false, err
		}
		if node == nil {
			fmt.Printf("Account key %s not found in index\n", string(key))
			return nil, false, nil
		}
		value, err = db.readFromFile(node.offset)
		if err != nil {
			return nil, false, err
		}

		// add to cache and cache path of the node
		db.nodeCache.Put(string(key), value, CacheInfo{
			storageFileID: node.storageFileID,
			storageOffset: node.storageOffset,
			storageSize:   node.storageSize,
		}, 0)
		// db.nodeCache.AsyncCachePathToNode(string(key), db)
		return value, true, nil

	case TrieStorage:
		// db.totalOps++

		// if db.totalOps%10000 == 0 {
		// 	fmt.Printf("Total Ops: %d, Cached Ops: %d, Sorted Ops: %d, Read Count: %d, Time on Read: %s\n",
		// 		db.totalOps, db.cachedOps, db.sortedOps, db.readCount, db.timeOnRead)
		// }

		accountKey := db.GetParentAccountKey(key)
		if accountKey == nil {
			fmt.Printf("Parent account key not found for %x\n", key)
			return nil, false, nil
		}
		err := db.ensureAccountStorageCached(accountKey, key)
		if err != nil {
			fmt.Println("Error ensuring account storage cached:", err)
			return nil, false, err
		}
		if value, ok := db.slotCache.Get(bytesToString(accountKey), key); ok {
			return value, true, nil
		} else {
			return nil, false, nil
		}
	default:
		return nil, false, errors.New("unknown key type")
	}
}

func (db *PrefixDB) Put(key, value []byte) error {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}

	switch keyType {
	case TrieAccount:
		// isContract := db.isContractAccount(value)
		// check accountIndex
		// var ok bool
		// if _, _, ok = db.nodeCache.Get(bytesToString(key)); !ok {
		// 	if _, _, ok = db.batch.get(key); !ok {
		// 		// node, err := db.getNode(key)
		// 		// if err != nil {
		// 		// 	return err
		// 		// }
		// 		// if node != nil {
		// 		// 	cacheInfo = CacheInfo{
		// 		// 		storageFileID:    node.storageFileID,
		// 		// 		storageOffset:    node.storageOffset,
		// 		// 		storageSize:      node.storageSize,
		// 		// 		hotStorageOffset: node.hotStorageOffset,
		// 		// 		hotStorageSize:   node.hotStorageSize,
		// 		// 	}
		// 		// }
		// 	}
		// }

		db.nodeCache.UpdateValue(bytesToString(key), value, 1)
		// db.nodeCache.AsyncCachePathToNode(string(key), db)

	case TrieStorage:
		// db.totalOps++
		// if db.totalOps%10000 == 0 {
		// 	fmt.Printf("Total Ops: %d, Cached Ops: %d, Sorted Ops: %d, Read Count: %d, Time on Read: %s\n",
		// 		db.totalOps, db.cachedOps, db.sortedOps, db.readCount, db.timeOnRead)
		// }
		accountKey := db.GetParentAccountKey(key)
		if accountKey == nil {
			fmt.Printf("Parent account key not found for %x\n", key)
			return nil
		}
		return db.bufferStorageMutation(accountKey, key, value)
	}
	return nil
}

func (db *PrefixDB) Has(key []byte) (bool, error) {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return false, err
	}

	switch keyType {
	case TrieAccount:
		// check in cache
		var value []byte
		var cacheInfo CacheInfo
		var ok bool
		if value, cacheInfo, ok = db.nodeCache.Get(bytesToString(key)); ok {
			return true, nil
		}

		// check in batch
		if db.batch != nil {
			if value, cacheInfo, ok = db.batch.get(key); ok {
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
		// return true, nil
		value, err = db.readFromFile(node.offset)
		if err != nil {
			return false, err
		}

		// add to cache and cache path of the node
		db.nodeCache.Put(string(key), value, cacheInfo, 0)
		// db.nodeCache.AsyncCachePathToNode(string(key), db)
		return true, nil
	case TrieStorage:
		// db.totalOps++
		// if db.totalOps%10000 == 0 {
		// 	fmt.Printf("Total Ops: %d, Cached Ops: %d, Sorted Ops: %d, Read Count: %d, Time on Read: %s\n",
		// 		db.totalOps, db.cachedOps, db.sortedOps, db.readCount, db.timeOnRead)
		// }

		accountKey := db.GetParentAccountKey(key)
		if accountKey == nil {
			fmt.Printf("Parent account key not found for %x\n", key)
			return false, nil
		}

		err := db.ensureAccountStorageCached(accountKey, key)
		if err != nil {
			fmt.Println("Error ensuring account storage cached:", err)
			return false, err
		}
		if _, ok := db.slotCache.Get(bytesToString(accountKey), key); ok {
			return true, nil
		} else {
			return false, nil
		}
	default:
		return false, errors.New("unknown key type")
	}
}

func (db *PrefixDB) Delete(key []byte) error {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}

	switch keyType {
	case TrieAccount:
		var ok bool
		if _, _, ok = db.nodeCache.Get(bytesToString(key)); !ok {
			if _, _, ok = db.batch.get(key); !ok {
				node, err := db.getNode(key)
				if err != nil {
					return err
				}
				if node == nil {
					fmt.Printf("Account key %s not found in index\n", string(key))
					return nil
				}
			} else {
				db.batch.delete(key)
			}
		} else {
			db.nodeCache.Delete(bytesToString(key))
		}

		// delete node
		db.storeNode(key, &TrieNode{
			storageFileID: 0,
			storageOffset: 0,
			offset:        0,
			storageSize:   0,
		})
		// db.accountIndex.delete(string(key))

	case TrieStorage:
		db.totalOps++
		if db.totalOps%10000 == 0 {
			fmt.Printf("Total Ops: %d, Cached Ops: %d, Sorted Ops: %d, Read Count: %d, Time on Read: %s\n",
				db.totalOps, db.cachedOps, db.sortedOps, db.readCount, db.timeOnRead)
		}

		accountKey := db.GetParentAccountKey(key)
		if accountKey == nil {
			return nil
		}
		return db.bufferStorageMutation(accountKey, key, nil)

	default:
		return errors.New("unknown key type")
	}

	return nil
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
	node, err := db.getNode([]byte(buf.accountKey))
	if err != nil {
		return err
	}
	var (
		accOff         int64
		existingFileID uint32
		existingOffset int64
		existingSize   uint64
	)
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
		db.nodeCache.UpdateStoragePointer(buf.accountKey, CacheInfo{})
	} else {
		fileID, off, sz, err := db.persistStorageEntries(buf.storagekvs, existingFileID, existingOffset, existingSize)
		if err != nil {
			return err
		}
		if err := db.prefixTree.Put([]byte(buf.accountKey), accOff, fileID, off, sz); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(buf.accountKey, CacheInfo{
			storageFileID: fileID,
			storageOffset: off,
			storageSize:   sz,
		})
	}
	if db.slotCache != nil {
		db.slotCache.Invalidate(buf.accountKey)
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

	if err := db.flushStorageBuffer(); err != nil {
		errs = append(errs, fmt.Errorf("failed to flush storage buffer: %v", err))
	}

	if db.nodeCache != nil {
		db.nodeCache.Close()
	}
	if db.slotCache != nil {
		db.slotCache.Close()
	}

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
	db.slotCache = nil
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

// SaveSlotIndex saves the current prefix tree to a file.
func (db *PrefixDB) SaveSlotIndex() error {
	// write accountIndex to file
	file, err := os.Create(db.indexfile)
	if err != nil {
		return fmt.Errorf("failed to create the prefix tree file: %v", err)
	}
	defer file.Close()

	return nil
}

// LoadSlotIndex loads the prefix tree from a file.
func (db *PrefixDB) LoadSlotIndex() error {
	file, err := os.OpenFile(db.indexfile, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open the prefix tree file: %v", err)
	}
	defer file.Close()

	// check if the file is empty
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat the prefix tree file: %v", err)
	}
	if fileInfo.Size() == 0 {
		// empty file, nothing to load
		return errors.New("empty prefix tree file")
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
	nodeInfo, found, err := db.prefixTree.Get(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &TrieNode{
		storageFileID: nodeInfo.storageFileID,
		storageOffset: nodeInfo.storageOffset,
		storageSize:   nodeInfo.storageSize,
		offset:        nodeInfo.accountOffset,
	}, nil
}

func (pdb *PrefixDB) SaveTree() error {
	return pdb.prefixTree.SaveToFile(pdb.prefixTree.trieFile)
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
func (db *PrefixDB) serializeStorageSegment(kvs []kvPair) ([]byte, error) {
	est := 4
	for _, v := range kvs {
		est += 6 + len(v.key) + len(v.val)
	}

	buf := make([]byte, 0, est)
	tmp := make([]byte, 6)
	//kv count
	writeUint32BE(tmp[0:4], uint32(len(kvs)))
	buf = append(buf, tmp[0:4]...)
	for _, v := range kvs {
		if len(v.key) > 0xFFFF {
			return nil, fmt.Errorf("key too large: %d", len(v.key))
		}
		writeUint16BE(tmp[:2], uint16(len(v.key)))
		writeUint32BE(tmp[2:6], uint32(len(v.val)))
		buf = append(buf, tmp[:6]...)
		buf = append(buf, []byte(v.key)...)
		buf = append(buf, v.val...)
	}
	return buf, nil
}

// appendStorageSegment appends a serialized storage segment to the storage file and returns its file ID, offset, and size.
func (db *PrefixDB) appendStorageSegment(kvs []kvPair) (fileID uint32, offset int64, size uint64, err error) {
	seg, err := db.serializeStorageSegment(kvs)
	if err != nil {
		return 0, 0, 0, err
	}
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
	kvs = dedupSortedKVPairs(kvs)
	if len(kvs) == 0 {
		return 0, 0, 0, nil
	}
	if isSegmentedStorage(existingFileID) {
		return db.updateSegmentedStorage(existingFileID, kvs)
	}
	merged := kvs
	var existingBacking []byte
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
		defer putDataBuffer(existingBacking)
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
		seg, err := db.serializeStorageSegment(chunk)
		if err != nil {
			return err
		}
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
	allocator := newChunkFileAllocator(metas)
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

func (db *PrefixDB) mutateSegmentChunk(folderID uint32, folderPath string, meta segmentChunkMeta, additions []kvPair, allocator *chunkFileAllocator) ([]segmentChunkMeta, error) {
	if len(additions) == 0 {
		return []segmentChunkMeta{meta}, nil
	}
	chunkPath := filepath.Join(folderPath, meta.FileName)
	currentSize := int64(meta.ChunkSize)
	appendBytes := payloadSize(additions)
	needRewrite := appendBytes == 0
	if !needRewrite {
		predicted := currentSize + appendBytes
		if predicted > segmentedChunkHardLimit {
			needRewrite = true
		}
		if len(meta.KeyEnd) > 0 && bytes.Compare(additions[0].key, meta.KeyEnd) <= 0 {
			needRewrite = true
		}
		if len(meta.KeyStart) > 0 && bytes.Compare(additions[len(additions)-1].key, meta.KeyStart) < 0 {
			needRewrite = true
		}
	}
	if !needRewrite {
		metaCopy := meta
		if err := db.appendChunkFile(chunkPath, metaCopy.KVCount, additions, currentSize); err != nil {
			return nil, err
		}
		metaCopy.KVCount += uint32(len(additions))
		metaCopy.ChunkSize += uint64(appendBytes)
		adjustMetaRange(&metaCopy, additions)
		return []segmentChunkMeta{metaCopy}, nil
	}
	return db.rewriteChunkWithDedup(folderID, folderPath, meta, additions, allocator)
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
	seg, err := db.serializeStorageSegment(additions)
	if err != nil {
		return err
	}
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

func (db *PrefixDB) rewriteChunkWithDedup(folderID uint32, folderPath string, meta segmentChunkMeta, additions []kvPair, allocator *chunkFileAllocator) ([]segmentChunkMeta, error) {
	existing, backing, err := db.readSegmentChunkFile(folderID, meta.FileName)
	if err != nil {
		return nil, err
	}
	if backing != nil {
		defer putDataBuffer(backing)
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
	for idx, chunk := range chunks {
		name := meta.FileName
		if idx > 0 {
			name = allocator.nextName()
		}
		if err := db.writeChunkFile(folderPath, name, chunk); err != nil {
			return nil, err
		}
		chunkSize := uint64(estimateSegmentSize(chunk))
		result = append(result, segmentChunkMeta{
			FileName:  name,
			KeyStart:  cloneBytes(chunk[0].key),
			KeyEnd:    cloneBytes(chunk[len(chunk)-1].key),
			KVCount:   uint32(len(chunk)),
			ChunkSize: chunkSize,
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
			chunk := make([]kvPair, i-start)
			copy(chunk, entries[start:i])
			chunks = append(chunks, chunk)
			start = i
			size = 4
		}
		size += entrySize
	}
	if start < len(entries) {
		chunk := make([]kvPair, len(entries)-start)
		copy(chunk, entries[start:])
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

func (db *PrefixDB) writeChunkFile(folderPath, fileName string, entries []kvPair) error {
	seg, err := db.serializeStorageSegment(entries)
	if err != nil {
		return err
	}
	fullPath := filepath.Join(folderPath, fileName)
	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(seg); err != nil {
		f.Close()
		return err
	}
	return f.Close()
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

func (db *PrefixDB) writeSegmentIndex(folderPath string, metas []segmentChunkMeta) error {
	buf := make([]byte, 0, 64*len(metas))
	var tmp32 [4]byte
	var tmp64 [8]byte
	writeUint32BE(tmp32[:], uint32(len(metas)))
	buf = append(buf, tmp32[:]...)
	for _, meta := range metas {
		var err error
		if buf, err = appendVarBytes(buf, []byte(meta.FileName)); err != nil {
			return err
		}
		if buf, err = appendVarBytes(buf, meta.KeyStart); err != nil {
			return err
		}
		if buf, err = appendVarBytes(buf, meta.KeyEnd); err != nil {
			return err
		}
		writeUint32BE(tmp32[:], meta.KVCount)
		buf = append(buf, tmp32[:]...)
		writeUint64BE(tmp64[:], meta.ChunkSize)
		buf = append(buf, tmp64[:]...)
	}
	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	return os.WriteFile(indexPath, buf, 0644)
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
	indexPath := filepath.Join(db.segmentedFolderPath(folderID), segmentIndexFileName)
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid segment index: %s", indexPath)
	}
	count := int(readUint32BE(data[:4]))
	cursor := 4
	metas := make([]segmentChunkMeta, 0, count)
	for i := 0; i < count; i++ {
		nameBytes, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return nil, err
		}
		cursor += n
		start, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return nil, err
		}
		cursor += n
		end, n, err := readVarBytes(data[cursor:])
		if err != nil {
			return nil, err
		}
		cursor += n
		if cursor+4 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		kvCount := readUint32BE(data[cursor : cursor+4])
		cursor += 4
		var chunkSize uint64
		if cursor+8 <= len(data) {
			chunkSize = readUint64BE(data[cursor : cursor+8])
			cursor += 8
		} else {
			// backward compatibility: derive size from the actual chunk file if not stored in index
			chunkPath := filepath.Join(db.segmentedFolderPath(folderID), string(nameBytes))
			info, statErr := os.Stat(chunkPath)
			if statErr != nil {
				return nil, statErr
			}
			chunkSize = uint64(info.Size())
		}
		metas = append(metas, segmentChunkMeta{
			FileName:  string(nameBytes),
			KeyStart:  cloneBytes(start),
			KeyEnd:    cloneBytes(end),
			KVCount:   kvCount,
			ChunkSize: chunkSize,
		})
	}
	return metas, nil
}

func readVarBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	ln := int(readUint16BE(buf[:2]))
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
	for i := range metas {
		startOK := len(metas[i].KeyStart) == 0 || bytes.Compare(key, metas[i].KeyStart) >= 0
		endOK := len(metas[i].KeyEnd) == 0 || bytes.Compare(key, metas[i].KeyEnd) <= 0
		if startOK && endOK {
			return &metas[i]
		}
		if len(metas[i].KeyEnd) > 0 && bytes.Compare(key, metas[i].KeyEnd) < 0 {
			return &metas[i]
		}
	}
	return &metas[len(metas)-1]
}

func (db *PrefixDB) readSegmentedChunk(fileID uint32, storageKey []byte) ([]kvPair, []byte, *segmentChunkMeta, error) {
	folderID := fileID & ^segmentedStorageFlag
	metas, err := db.readSegmentIndex(folderID)
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
	entries, err := buildPairsFromPayload(payload, kvCount)
	if err != nil {
		if backing != nil {
			putDataBuffer(backing)
		}
		return nil, nil, nil, err
	}
	return entries, backing, meta, nil
}

func (db *PrefixDB) readSegmentChunkFile(folderID uint32, fileName string) ([]kvPair, []byte, error) {
	buf, err := db.readSegmentFileBuffer(folderID, fileName)
	if err != nil {
		return nil, nil, err
	}
	payload, kvCount, err := parseSegmentBuffer(buf)
	if err != nil {
		putDataBuffer(buf)
		return nil, nil, err
	}
	entries, err := buildPairsFromPayload(payload, kvCount)
	if err != nil {
		putDataBuffer(buf)
		return nil, nil, err
	}
	return entries, buf, nil
}

func (db *PrefixDB) readSegmentChunkPayload(folderID uint32, fileName string) ([]byte, int, []byte, error) {
	buf, err := db.readSegmentFileBuffer(folderID, fileName)
	if err != nil {
		return nil, 0, nil, err
	}
	payload, kvCount, err := parseSegmentBuffer(buf)
	if err != nil {
		putDataBuffer(buf)
		return nil, 0, nil, err
	}
	return payload, kvCount, buf, nil
}

func (db *PrefixDB) readSegmentFileBuffer(folderID uint32, fileName string) ([]byte, error) {
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
	return buf[:intSize], nil
}

// ensureAccountStorageCached ensures that the storage map for an account is loaded into the slot cache.
func (db *PrefixDB) ensureAccountStorageCached(accountKey, storageKey []byte) error {
	ak := string(accountKey)
	if db.slotCache.AccountHasKey(ak, storageKey) {
		return nil
	}

	loadIntoCache := func(cacheInfo CacheInfo) error {
		if cacheInfo.storageFileID == 0 {
			db.slotCache.PutAccount(ak, nil, nil, nil)
			return nil
		}

		start := time.Now()
		var (
			storage []kvPair
			backing []byte
			err     error
			meta    *SlotCacheMeta
		)

		if isSegmentedStorage(cacheInfo.storageFileID) {
			var chunkMeta *segmentChunkMeta
			storage, backing, chunkMeta, err = db.readSegmentedChunk(cacheInfo.storageFileID, storageKey)
			if chunkMeta != nil {
				meta = &SlotCacheMeta{
					Segmented: true,
					KeyStart:  chunkMeta.KeyStart,
					KeyEnd:    chunkMeta.KeyEnd,
				}
			} else if err == nil {
				meta = &SlotCacheMeta{Segmented: true}
			}
		} else {
			storage, backing, err = db.readStorageSegmentToMap(cacheInfo.storageFileID, cacheInfo.storageOffset, cacheInfo.storageSize)
		}

		end := time.Now()
		db.readCount++
		db.timeOnRead += end.Sub(start)

		if err != nil {
			if backing != nil {
				putDataBuffer(backing)
			}
			return err
		}

		db.slotCache.PutAccount(ak, storage, backing, meta)
		return nil
	}

	if _, cacheInfo, ok := db.nodeCache.Get(ak); ok && cacheInfo.storageFileID != 0 {
		return loadIntoCache(cacheInfo)
	}

	node, err := db.getNode(accountKey)
	if err != nil {
		return err
	}

	if node != nil && node.storageFileID != 0 {
		cacheInfo := CacheInfo{
			storageFileID: node.storageFileID,
			storageOffset: node.storageOffset,
			storageSize:   node.storageSize,
		}
		db.nodeCache.UpdateStoragePointer(ak, cacheInfo)
		return loadIntoCache(cacheInfo)
	}

	db.slotCache.PutAccount(ak, nil, nil, nil)
	return nil
}

func (db *PrefixDB) readStorageSegmentToMap(fileID uint32, offset int64, size uint64) ([]kvPair, []byte, error) {
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
	entries, err := buildPairsFromPayload(payload, kvCount)
	if err != nil {
		putDataBuffer(buf)
		return nil, nil, err
	}
	db.sortedOps += len(entries)
	return entries, buf, nil
}

func (db *PrefixDB) readStorageSegmentPayload(fileID uint32, offset int64, size uint64) ([]byte, int, []byte, error) {
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
	return payload, kvCount, buf, nil

}

func (db *PrefixDB) readSegmentedStoragePayload(fileID uint32) ([]byte, int, []byte, error) {
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
		backing []byte
	}
	pieces := make([]chunkData, 0, len(metas))
	for _, meta := range metas {
		payload, count, backing, err := db.readSegmentChunkPayload(folderID, meta.FileName)
		if err != nil {
			for _, piece := range pieces {
				if piece.backing != nil {
					putDataBuffer(piece.backing)
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
		putDataBuffer(piece.backing)
	}
	merged = merged[:totalSize]
	return merged, totalCount, merged, nil
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

func buildPairsFromPayload(payload []byte, kvCount int) ([]kvPair, error) {
	if kvCount <= 0 {
		return nil, nil
	}
	entries := make([]kvPair, kvCount)
	cursor := 0
	limit := len(payload)
	for i := 0; i < kvCount; i++ {
		if cursor+6 > limit {
			return nil, io.ErrUnexpectedEOF
		}
		klen := int(readUint16BE(payload[cursor : cursor+2]))
		vlen := int(readUint32BE(payload[cursor+2 : cursor+6]))
		cursor += 6
		if klen < 0 || vlen < 0 {
			return nil, fmt.Errorf("invalid kv lens: k=%d v=%d", klen, vlen)
		}
		need := klen + vlen
		if need < 0 || cursor+need > limit {
			return nil, io.ErrUnexpectedEOF
		}
		key := payload[cursor : cursor+klen]
		cursor += klen
		val := payload[cursor : cursor+vlen]
		cursor += vlen
		entries[i] = kvPair{key: key, val: val}
	}
	return entries, nil
}

func (db *PrefixDB) readStorageSegmentPairs(fileID uint32, offset int64, size uint64) ([]kvPair, []byte, error) {
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
			putDataBuffer(backing)
		}
		return nil, nil, nil
	}
	entries, err := buildPairsFromPayload(payload, kvCount)
	if err != nil {
		if backing != nil {
			putDataBuffer(backing)
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

	// node, err := db.getNode(accountKey)
	// if err != nil {
	// 	return 0, err
	// }
	// if node == nil || node.storageFileID == 0 {
	// 	return 0, nil
	// }

	// if node.storageSize == 0 {
	// 	return 0, nil
	// }

	// if isSegmentedStorage(node.storageFileID) {
	// 	folderID := node.storageFileID & ^segmentedStorageFlag
	// 	metas, err := db.readSegmentIndex(folderID)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	var total uint32
	// 	for _, meta := range metas {
	// 		total += meta.KVCount
	// 	}
	// 	return total, nil
	// }

	// // read segment head to get kvcount without loading entire payload
	// p, _ := db.storagePathByFileID(node.storageFileID)
	// f, err := os.Open(p)
	// if err != nil {
	// 	return 0, err
	// }
	// defer f.Close()

	// offset := node.storageOffset

	// head := make([]byte, 4)
	// if _, err := f.ReadAt(head, offset); err != nil {
	// 	return 0, err
	// }
	// return readUint32BE(head), nil

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
