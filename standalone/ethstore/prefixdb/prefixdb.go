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

const storageMaxFileSize int64 = 1 << 30      // 1GB
const hotStorageMaxFileSize int64 = 128 << 20 // 128MB
const hotFileIDMask uint32 = 0x80000000       // fileID with highest bit set
const hotFileMagic uint32 = 0x48535446        // 'HSTF' for hot storage files
const hotSegMagic uint32 = 0x48534753         // 'HGSS' for hot storage segments

type hotFileHeader struct {
	Magic    uint32
	Version  uint16
	Flags    uint16
	Reserved [8]byte
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
	accountKey     string
	payload        []byte
	payloadBacking []byte
	payloadCount   int
	pending        []byte
	pendingCount   int
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

	hotStorageDir string
	hotCurMu      sync.Mutex
	hotCurFile    *os.File
	hotCurFileID  uint32
	hotCurSize    int64 // current size of the hot storage file
	hotGCStop     chan struct{}
	hotFileLocks  sync.Map // key: real hot file id(uint32), val: *sync.Mutex

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
	hotStorageDir := resolvePath(cfg.BaseDir, cfg.HotStorageDir)
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
		hotStorageDir:       hotStorageDir,
		hotGCStop:           make(chan struct{}),
	}

	if err := os.MkdirAll(db.storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage dir: %v", err)
	}
	if err := db.openOrCreateStorageFile(); err != nil {
		return nil, fmt.Errorf("failed to init storage shard: %v", err)
	}

	if err := os.MkdirAll(db.hotStorageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create hot storage dir: %v", err)
	}
	if err := db.openOrCreateHotFile(); err != nil {
		return nil, fmt.Errorf("failed to init hot storage shard: %v", err)
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
			storageFileID:    node.storageFileID,
			storageOffset:    node.storageOffset,
			storageSize:      node.storageSize,
			hotStorageOffset: node.hotStorageOffset,
			hotStorageSize:   node.hotStorageSize,
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

		// if data, ok := db.slotCache.GetAccount(bytesToString(accountKey)); ok {
		// 	if index, found := binarySearchKVPairs(data, key); found {
		// 		db.cachedOps++
		// 		return data[index].val, true, nil
		// 	}
		// 	return nil, false, nil
		// }

		readType, err := db.ensureAccountStorageCached(accountKey, false)
		if err != nil {
			fmt.Println("Error ensuring account storage cached:", err)
			return nil, false, err
		}
		if value, ok := db.slotCache.Get(bytesToString(accountKey), key); ok {
			return value, true, nil
		} else {
			if readType != readFileTypeCold {
				_, err := db.ensureAccountStorageCached(accountKey, true)
				if err != nil {
					fmt.Println("Error ensuring account storage cached from hot:", err)
					return nil, false, err
				}
				if value, ok := db.slotCache.Get(bytesToString(accountKey), key); ok {
					return value, true, nil
				}
			}
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

		readtype, err := db.ensureAccountStorageCached(accountKey, false)
		if err != nil {
			fmt.Println("Error ensuring account storage cached:", err)
			return false, err
		}
		if _, ok := db.slotCache.Get(bytesToString(accountKey), key); ok {
			return true, nil
		} else {
			if readtype == readFileTypeHot {
				_, err := db.ensureAccountStorageCached(accountKey, true)
				if err != nil {
					fmt.Println("Error ensuring account storage cached from hot:", err)
					return false, err
				}
				if _, ok := db.slotCache.Get(bytesToString(accountKey), key); ok {
					return true, nil
				}
			}
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
			storageFileID:    0,
			storageOffset:    0,
			offset:           0,
			storageSize:      0,
			hotStorageOffset: 0,
			hotStorageSize:   0,
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

	if err := db.ensureStorageBuffer(accountKey); err != nil {
		return err
	}
	return db.storageBuf.appendOperation(storageKey, value)
}

func (db *PrefixDB) ensureStorageBuffer(accountKey []byte) error {
	accountStr := string(accountKey)
	if db.storageBuf.accountKey == accountStr {
		return nil
	}
	if db.storageBuf.accountKey != "" {
		if err := db.flushStorageBuffer(); err != nil {
			return err
		}
	}
	db.storageBuf.reset()
	db.storageBuf.accountKey = accountStr
	return db.populateStoragePayload(accountStr, accountKey)
}

func (db *PrefixDB) populateStoragePayload(accountStr string, accountKey []byte) error {
	if len(accountStr) == 0 {
		return nil
	}
	var fileID uint32
	var offset int64
	var size uint64
	if _, cacheInfo, ok := db.nodeCache.Get(accountStr); ok {
		fileID = cacheInfo.storageFileID
		offset = cacheInfo.storageOffset
		size = cacheInfo.storageSize
	} else {
		node, err := db.getNode(accountKey)
		if err != nil {
			return err
		}
		if node != nil {
			fileID = node.storageFileID
			offset = node.storageOffset
			size = node.storageSize
		}
	}
	if fileID == 0 || size == 0 {
		return nil
	}
	payload, count, backing, err := db.readStorageSegmentPayload(fileID, offset, size)
	if err != nil {
		return err
	}
	db.storageBuf.payload = payload
	db.storageBuf.payloadBacking = backing
	db.storageBuf.payloadCount = count
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
	var accOff int64
	if node != nil {
		accOff = node.offset
	}
	total := buf.payloadCount + buf.pendingCount
	var final []kvPair
	if total > 0 {
		rawEntries := kvPairEntryPool.Get().([]kvPair)
		if cap(rawEntries) < total {
			kvPairEntryPool.Put(rawEntries[:0])
			rawEntries = make([]kvPair, 0, total)
		}
		entries := rawEntries[:0]
		defer kvPairEntryPool.Put(entries[:0])
		appendStream := func(data []byte, count int, zeroIsDelete bool) error {
			if count == 0 || len(data) == 0 {
				return nil
			}
			return walkKVStream(data, count, func(k, v []byte) error {
				if zeroIsDelete && len(v) == 0 {
					entries = append(entries, kvPair{key: k, val: nil})
					return nil
				}
				entries = append(entries, kvPair{key: k, val: v})
				return nil
			})
		}
		if err := appendStream(buf.payload, buf.payloadCount, false); err != nil {
			return err
		}
		if err := appendStream(buf.pending, buf.pendingCount, true); err != nil {
			return err
		}
		if len(entries) > 0 {
			sortKVPairs(entries)
			write := 0
			for i := 0; i < len(entries); {
				j := i + 1
				for ; j < len(entries) && bytes.Equal(entries[i].key, entries[j].key); j++ {
				}
				last := entries[j-1]
				if last.val != nil {
					entries[write] = last
					write++
				}
				i = j
			}
			if write > 0 {
				final = entries[:write]
			}
		}
	}
	if len(final) == 0 {
		if err := db.prefixTree.Put([]byte(buf.accountKey), accOff, 0, 0, 0, 0, 0); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(buf.accountKey, CacheInfo{})
	} else {
		fileID, off, sz, err := db.appendStorageSegment(final)
		if err != nil {
			return err
		}
		if err := db.prefixTree.Put([]byte(buf.accountKey), accOff, fileID, off, sz, 0, 0); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(buf.accountKey, CacheInfo{
			storageFileID:    fileID,
			storageOffset:    off,
			storageSize:      sz,
			hotStorageOffset: 0, // hot storage is unvalidated on storage update
			hotStorageSize:   0,
		})
	}
	if db.slotCache != nil {
		db.slotCache.Invalidate(buf.accountKey)
	}
	buf.reset()
	return nil
}

func walkKVStream(data []byte, count int, fn func(key, val []byte) error) error {
	cursor := 0
	for i := 0; i < count; i++ {
		if cursor+6 > len(data) {
			return io.ErrUnexpectedEOF
		}
		klen := int(readUint16BE(data[cursor : cursor+2]))
		vlen := int(readUint32BE(data[cursor+2 : cursor+6]))
		cursor += 6
		if klen < 0 || vlen < 0 || cursor+klen+vlen > len(data) {
			return io.ErrUnexpectedEOF
		}
		key := data[cursor : cursor+klen]
		cursor += klen
		val := data[cursor : cursor+vlen]
		cursor += vlen
		if err := fn(key, val); err != nil {
			return err
		}
	}
	return nil
}

func (sb *storageOpBuffer) appendOperation(key, value []byte) error {
	if len(key) > 0xFFFF {
		return fmt.Errorf("storage key too large: %d", len(key))
	}
	if len(value) > 0xFFFFFFFF {
		return fmt.Errorf("storage value too large: %d", len(value))
	}
	var hdr [6]byte
	writeUint16BE(hdr[0:2], uint16(len(key)))
	writeUint32BE(hdr[2:6], uint32(len(value)))
	sb.pending = append(sb.pending, hdr[:]...)
	sb.pending = append(sb.pending, key...)
	sb.pending = append(sb.pending, value...)
	sb.pendingCount++
	return nil
}

func (sb *storageOpBuffer) reset() {
	if sb.payloadBacking != nil {
		putDataBuffer(sb.payloadBacking)
	}
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
	largeBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 1024*1024) // 1MB
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
		sort.Slice(entries, func(i, j int) bool {
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
		buffer = largeBufferPool.Get().([]byte)
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
		largeBufferPool.Put(buf[:capacity])
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

	if db.hotGCStop != nil {
		select {
		case <-db.hotGCStop:
			// already closed
		default:
			close(db.hotGCStop)
		}
	}
	if db.hotCurFile != nil {
		_ = db.hotCurFile.Sync()
		_ = db.hotCurFile.Close()
		db.hotCurFile = nil
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
	return db.prefixTree.Put(key, node.offset, node.storageFileID, node.storageOffset, node.storageSize, node.hotStorageOffset, node.hotStorageSize)
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
		storageFileID:    nodeInfo.storageFileID,
		storageOffset:    nodeInfo.storageOffset,
		storageSize:      nodeInfo.storageSize,
		offset:           nodeInfo.accountOffset,
		hotStorageOffset: nodeInfo.hotStorageOffset,
		hotStorageSize:   nodeInfo.hotStorageSize,
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
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var id uint32
		n, _ := fmt.Sscanf(e.Name(), "storage_%08d.dat", &id)
		if n == 1 && id > maxID {
			maxID = id
		}
	}
	tryID := maxID
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

// // flushAccountEntry writes one account's storage map as a contiguous segment and updates PrefixTree
// func (db *PrefixDB) flushAccountEntry(accountKey string, data []kvPair) error {
// 	var node *TrieNode
// 	var err error

// 	node, err = db.getNode([]byte(accountKey))
// 	if err != nil {
// 		return err
// 	}
// 	var accOff int64
// 	if node != nil {
// 		accOff = node.offset
// 	}

// 	if len(data) == 0 {
// 		if err := db.prefixTree.Put([]byte(accountKey), accOff, 0, 0, 0); err != nil {
// 			return err
// 		}
// 		db.nodeCache.UpdateStoragePointer(accountKey, 0, 0, 0)
// 		return nil
// 	}

// 	fileID, off, sz, err := db.appendStorageSegment(data)
// 	if err != nil {
// 		return err
// 	}
// 	if err := db.prefixTree.Put([]byte(accountKey), accOff, fileID, off, sz); err != nil {
// 		return err
// 	}
// 	db.nodeCache.UpdateStoragePointer(accountKey, fileID, off, sz)
// 	return nil
// }

type readFileType int

const (
	other readFileType = iota
	readFileTypeCold
	readFileTypeHot
)

// ensureAccountStorageCached ensures that the storage map for an account is loaded into the slot cache.
func (db *PrefixDB) ensureAccountStorageCached(accountKey []byte, readColdForce bool) (readFileType, error) {

	var readType readFileType

	ak := string(accountKey)
	if _, ok := db.slotCache.GetAccount(ak); ok && !readColdForce {
		return 0, nil
	}

	tryRead := func(cacheInfo CacheInfo) ([]kvPair, []byte, error) {
		_, isHot, _ := db.storagePathByFileID(cacheInfo.storageFileID)

		start := time.Now()

		var pairs []kvPair
		var backing []byte
		var err error
		if !isHot {
			readType = readFileTypeCold
			pairs, backing, err = db.readStorageSegmentToMap(cacheInfo.storageFileID, cacheInfo.storageOffset, cacheInfo.storageSize)
		} else {
			if readColdForce {
				readType = readFileTypeCold
				cacheInfo.storageFileID = cacheInfo.storageFileID & ^hotFileIDMask // force read from cold storage
				pairs, backing, err = db.readStorageSegmentToMap(cacheInfo.storageFileID, cacheInfo.storageOffset, cacheInfo.storageSize)
				if err == nil {
					return pairs, backing, nil
				}
			}
			readType = readFileTypeHot
			pairs, backing, err = db.readStorageSegmentToMap(cacheInfo.storageFileID, int64(cacheInfo.hotStorageOffset), uint64(cacheInfo.hotStorageSize))
		}

		end := time.Now()
		db.readCount++
		db.timeOnRead += end.Sub(start)
		fmt.Println("read time: "+end.Sub(start).String()+"sorted ops:", len(pairs))
		if err != nil {
			if backing != nil {
				putDataBuffer(backing)
			}
			return nil, nil, err
		}
		return pairs, backing, nil
	}

	if _, cacheInfo, ok := db.nodeCache.Get(ak); ok && cacheInfo.storageFileID != 0 {
		if m, backing, err := tryRead(cacheInfo); err == nil {
			db.slotCache.PutAccount(ak, m, backing)
			return readType, nil
		} else {
			return readType, err
		}
	}

	node, err := db.getNode(accountKey)
	if err != nil {
		return readType, err
	}

	if node != nil && node.storageFileID != 0 {
		db.nodeCache.UpdateStoragePointer(ak, CacheInfo{
			storageFileID:    node.storageFileID,
			storageOffset:    node.storageOffset,
			storageSize:      node.storageSize,
			hotStorageOffset: node.hotStorageOffset,
			hotStorageSize:   node.hotStorageSize,
		})
		if m, backing, err := tryRead(CacheInfo{
			storageFileID:    node.storageFileID,
			storageOffset:    node.storageOffset,
			storageSize:      node.storageSize,
			hotStorageOffset: node.hotStorageOffset,
			hotStorageSize:   node.hotStorageSize,
		}); err == nil {
			db.slotCache.PutAccount(ak, m, backing)
			return readType, nil
		} else {
			return readType, err
		}
	}

	db.slotCache.PutAccount(ak, nil, nil)
	return readType, nil
}

func (db *PrefixDB) readStorageSegmentToMap(fileID uint32, offset int64, size uint64) ([]kvPair, []byte, error) {
	p, isHot, _ := db.storagePathByFileID(fileID)
	if isHot {
	}

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

	var payload []byte
	var kvCount int

	if len(buf) < 4 {
		putDataBuffer(buf)
		return nil, nil, fmt.Errorf("segment too small")
	}
	kvCount = int(readUint32BE(buf[:4]))
	payload = buf[4:]

	if kvCount < 0 {
		putDataBuffer(buf)
		return nil, nil, fmt.Errorf("invalid kv count: %d", kvCount)
	}
	if kvCount == 0 {
		return []kvPair{}, buf, nil
	}

	entries := make([]kvPair, 0, kvCount)
	cursor := 0
	remaining := len(payload)
	for i := 0; i < kvCount; i++ {
		if remaining < 6 {
			putDataBuffer(buf)
			return nil, nil, io.ErrUnexpectedEOF
		}
		klen := int(readUint16BE(payload[cursor : cursor+2]))
		vlen := int(readUint32BE(payload[cursor+2 : cursor+6]))
		cursor += 6
		remaining -= 6
		if klen < 0 || vlen < 0 {
			putDataBuffer(buf)
			return nil, nil, fmt.Errorf("invalid kv lens: k=%d v=%d", klen, vlen)
		}
		need := klen + vlen
		if remaining < need {
			putDataBuffer(buf)
			return nil, nil, io.ErrUnexpectedEOF
		}
		key := payload[cursor : cursor+klen]
		cursor += klen
		remaining -= klen
		val := payload[cursor : cursor+vlen]
		cursor += vlen
		remaining -= vlen
		entries = append(entries, kvPair{key: key, val: val})
	}

	// sortKVPairs(entries)
	db.sortedOps += len(entries)

	return entries, buf, nil
}

func (db *PrefixDB) readStorageSegmentPayload(fileID uint32, offset int64, size uint64) ([]byte, int, []byte, error) {
	p, isHot, _ := db.storagePathByFileID(fileID)
	if isHot {
		return nil, 0, nil, fmt.Errorf("readStorageSegmentPayload not supported for hot storage")
	}
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

func (db *PrefixDB) GetStorageCount(accountKey []byte) (uint32, error) {
	node, err := db.getNode(accountKey)
	if err != nil {
		return 0, err
	}
	if node == nil || node.storageFileID == 0 {
		return 0, nil
	}

	if node.storageSize == 0 {
		return 0, nil
	}

	// read segment head to get kvcount without loading entire payload
	p, _, _ := db.storagePathByFileID(node.storageFileID)
	f, err := os.Open(p)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	offset := node.storageOffset

	head := make([]byte, 4)
	if _, err := f.ReadAt(head, offset); err != nil {
		return 0, err
	}
	return readUint32BE(head), nil

}

// storagePathByFileID returns the storage file path, whether it's hot storage, and the real file ID.
func (db *PrefixDB) storagePathByFileID(fileID uint32) (path string, isHot bool, realID uint32) {
	isHot = (fileID & hotFileIDMask) != 0
	realID = fileID & ^hotFileIDMask
	if isHot {
		return filepath.Join(db.hotStorageDir, fmt.Sprintf("hot_%08d.dat", realID)), true, realID
	}
	return filepath.Join(db.storageDir, fmt.Sprintf("storage_%08d.dat", realID)), false, realID
}

// openOrCreateHotFile opens the latest hot storage file or creates a new one if necessary.
func (db *PrefixDB) openOrCreateHotFile() error {
	entries, err := os.ReadDir(db.hotStorageDir)
	if err != nil {
		return fmt.Errorf("failed to read hot storage directory: %v", err)
	}
	var maxID uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var id uint32
		n, _ := fmt.Sscanf(e.Name(), "hot_%08d.dat", &id)
		if n == 1 && id > maxID {
			maxID = id
		}
	}
	tryID := maxID
	path := func(id uint32) string { return filepath.Join(db.hotStorageDir, fmt.Sprintf("hot_%08d.dat", id)) }

	if tryID > 0 {
		p := path(tryID)
		file, err := os.OpenFile(p, os.O_RDWR, 0644)
		if err == nil {
			fi, _ := file.Stat()
			if fi != nil && fi.Size() >= int64(binary.Size(hotFileHeader{})) && fi.Size() < hotStorageMaxFileSize {
				db.hotCurFile = file
				db.hotCurFileID = tryID
				db.hotCurSize = fi.Size()
				return nil
			}
			file.Close()
		}
	}

	newID := maxID + 1
	p := path(newID)
	file, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create hot storage file: %v", err)
	}
	// write header
	hdr := hotFileHeader{Magic: hotFileMagic, Version: 1, Flags: 0}
	if err := binary.Write(file, binary.BigEndian, &hdr); err != nil {
		file.Close()
		return fmt.Errorf("failed to write hot header: %v", err)
	}
	db.hotCurFile = file
	db.hotCurFileID = newID
	db.hotCurSize = int64(binary.Size(hotFileHeader{}))
	return nil
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

func (pdb *PrefixDB) flushAccessedStorageEntries(accountKey []byte, kvs []kvPair) error {
	if len(kvs) == 0 {
		return nil
	}

	// locate trie node to update storage metadata
	node, err := pdb.getNode(accountKey)
	if err != nil {
		return fmt.Errorf("failed to get trie node: %w", err)
	}
	if node == nil {
		return fmt.Errorf("trie node not found for key %x", accountKey)
	}

	baseFileID := node.storageFileID & ^hotFileIDMask
	hotFileID := baseFileID | hotFileIDMask
	filePath := filepath.Join(pdb.hotStorageDir, fmt.Sprintf("hot_%08d.dat", baseFileID))

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open hot storage file %s: %w", filePath, err)
	}
	defer file.Close()

	// ensure file has a valid header before appending data
	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat hot storage file %s: %w", filePath, err)
	}
	if fi.Size() == 0 {
		hdr := hotFileHeader{Magic: hotFileMagic, Version: 1, Flags: 0}
		if err := binary.Write(file, binary.BigEndian, &hdr); err != nil {
			return fmt.Errorf("failed to initialize hot storage file %s: %w", filePath, err)
		}
	}

	// serialize data into [kvCount][keyLen][valLen][key][val]...
	est := 4
	for _, v := range kvs {
		est += 6 + len(v.key) + len(v.val)
	}

	buf := make([]byte, 0, est)
	tmp := make([]byte, 6)
	writeUint32BE(tmp[0:4], uint32(len(kvs)))
	buf = append(buf, tmp[0:4]...)
	for _, v := range kvs {
		if len(v.key) > 0xFFFF {
			return fmt.Errorf("key too large: %d", len(v.key))
		}
		writeUint16BE(tmp[:2], uint16(len(v.key)))
		writeUint32BE(tmp[2:6], uint32(len(v.val)))
		buf = append(buf, tmp[:6]...)
		buf = append(buf, v.key...)
		buf = append(buf, v.val...)
	}

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of hot storage file %s: %w", filePath, err)
	}
	if offset+int64(len(buf)) > hotStorageMaxFileSize {
		return fmt.Errorf("hot storage file %s exceeded max size", filePath)
	}
	if _, err := file.Write(buf); err != nil {
		return fmt.Errorf("failed to write to hot storage file %s: %w", filePath, err)
	}

	node.storageFileID = hotFileID
	node.hotStorageOffset = uint64(offset)
	node.hotStorageSize = uint32(len(buf))

	// get from nodecache and update
	// if _, cacheInfo, ok := pdb.nodeCache.Get(string(accountKey));ok{

	// }

	if err := pdb.prefixTree.Put(accountKey, node.offset, node.storageFileID, node.storageOffset, node.storageSize, node.hotStorageOffset, node.hotStorageSize); err != nil {
		return fmt.Errorf("failed to update prefix tree with hot storage pointer: %w", err)
	}

	if pdb.nodeCache != nil {
		pdb.nodeCache.UpdateStoragePointer(string(accountKey), CacheInfo{
			storageFileID:    node.storageFileID,
			storageOffset:    node.storageOffset,
			storageSize:      node.storageSize,
			hotStorageOffset: node.hotStorageOffset,
			hotStorageSize:   node.hotStorageSize,
		})
	}

	return nil
}
