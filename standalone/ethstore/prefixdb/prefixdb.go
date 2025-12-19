package prefixdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
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
const hotFlagNeedsGC uint16 = 0x0001          // flag indicating the file needs garbage collection
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
)

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

type PrefixDB struct {
	prefixTree  *PrefixTree
	accountFile *os.File
	// slotFile    *os.File
	trieFile    *os.File
	indexfile   string
	nodeCache   *NodeCache
	slotCache   *SlotCache
	slotManager *SlotManager
	batch       *WriteBatch
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

	hotStorageDir string
	hotCurFile    *os.File
	hotCurFileID  uint32
	hotCurSize    int64 // current size of the hot storage file
	hotGCStop     chan struct{}
	hotFileLocks  sync.Map // key: real hot file id(uint32), val: *sync.Mutex
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
		slotManager:         NewSlotManager(SLOT_NUM, SLOT_SIZE),
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

	go db.hotGCWorker(2 * time.Minute)

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
		if value, _, _, _, ok := db.nodeCache.Get(bytesToString(key)); ok {
			return value, true, nil
		}

		// check in batch
		if db.batch != nil {
			if value, _, _, _, ok := db.batch.get(key); ok {
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
		value, err := db.readFromFile(node.offset, TrieAccount)
		if err != nil {
			return nil, false, err
		}

		// add to cache and cache path of the node
		db.nodeCache.Put(string(key), value, node.storageFileID, node.storageOffset, node.storageSize, 0)
		db.nodeCache.AsyncCachePathToNode(string(key), db)
		return value, true, nil

	case TrieStorage:
		accountKey := db.getParentAccountKey(key)
		if accountKey == nil {
			fmt.Printf("Parent account key not found for %x\n", key)
			return nil, false, nil
			// return nil, false, errors.New("parent account not found")
		}

		if data, ok := db.slotCache.GetAccount(bytesToString(accountKey)); ok {
			if value, exists := data[bytesToString(key)]; exists {
				return value, true, nil
			} else {
				return nil, false, nil
			}
		}

		m, err := db.ensureAccountStorageCached(accountKey)
		if err != nil {
			fmt.Println("Error ensuring account storage cached:", err)
			return nil, false, err
		}
		if value, exists := m[bytesToString(key)]; exists {
			return value, true, nil
		}
	default:
		return nil, false, errors.New("unknown key type")
	}
	return nil, false, errors.New("key not found")
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
		var storageFileID uint32 = 0
		var storageOffset int64 = 0
		var storageSize uint32 = 0
		var ok bool
		if _, storageFileID, storageOffset, storageSize, ok = db.nodeCache.Get(bytesToString(key)); !ok {
			if _, storageFileID, storageOffset, storageSize, ok = db.batch.get(key); !ok {
				// not found in cache or batch, get from prefix tree
				// node, err := db.getNode(key)
				// if err != nil {
				// 	return fmt.Errorf("failed to get node for key %s: %v", string(key), err)
				// }
				// if node == nil {
				// 	// new account
				// 	storageFileID = 0
				// 	storageOffset = 0
				// 	storageSize = 0
				// }
				// storageFileID = node.storageFileID
				// storageOffset = node.storageOffset
				// storageSize = node.storageSize
			}
		}
		db.nodeCache.Put(string(key), value, storageFileID, storageOffset, storageSize, 1)
		// db.nodeCache.AsyncCachePathToNode(string(key), db)

	case TrieStorage:
		accountKey := db.getParentAccountKey(key)
		if accountKey == nil {
			fmt.Printf("Parent account key not found for %x\n", key)
			return nil
		}
		// ensure the account's storage is cached
		if _, err := db.ensureAccountStorageCached(accountKey); err != nil {
			return err
		}
		// update in slot cache
		db.slotCache.UpdateKey(string(accountKey), string(key), value)
		return nil
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
		if _, _, _, _, ok := db.nodeCache.Get(bytesToString(key)); ok {
			return true, nil
		}

		// check in batch
		if db.batch != nil {
			if _, _, _, _, ok := db.batch.get(key); ok {
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
		value, err := db.readFromFile(node.offset, TrieAccount)
		if err != nil {
			return false, err
		}

		// add to cache and cache path of the node
		db.nodeCache.Put(string(key), value, node.storageFileID, node.storageOffset, node.storageSize, 0)
		db.nodeCache.AsyncCachePathToNode(string(key), db)
		return true, nil
	case TrieStorage:
		accountKey := db.getParentAccountKey(key)
		if accountKey == nil {
			return false, nil
		}
		m, err := db.ensureAccountStorageCached(accountKey)
		if err != nil {
			return false, err
		}
		_, ok := m[bytesToString(key)]
		return ok, nil

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
		if _, _, _, _, ok = db.nodeCache.Get(bytesToString(key)); !ok {
			if _, _, _, _, ok = db.batch.get(key); !ok {
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
		})
		// db.accountIndex.delete(string(key))

	case TrieStorage:
		accountKey := db.getParentAccountKey(key)
		if accountKey == nil {
			return nil
		}
		m, err := db.ensureAccountStorageCached(accountKey)
		if err != nil {
			return err
		}
		if _, ok := m[string(key)]; ok {
			// delete
			delete(m, string(key))
			db.slotCache.UpdateKey(string(accountKey), string(key), nil)
			delete(m, string(key))
		}
		return nil

	default:
		return errors.New("unknown key type")
	}

	return nil
}

// func (db *PrefixDB) createNode(key []byte) (*TrieNode, error) {
// 	db.nodesMutex.Lock()
// 	defer db.nodesMutex.Unlock()

// 	keyStr := string(key)
// 	if _, exists := db.nodes[keyStr]; !exists {
// 		db.nodes[keyStr] = &TrieNode{
// 			children:    make(map[byte]*TrieNode), // 可以保留为空映射
// 			slotIndices: nil,
// 			offset:      0,
// 			isValid:     false,
// 		}
// 	}

// 	return db.nodes[keyStr], nil
// }

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
)

// getDataBuffer返回适当大小的缓冲区
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
	}
}

func (db *PrefixDB) readFromFile(offset int64, keyType KeyType) ([]byte, error) {
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

	keySize := int(binary.BigEndian.Uint16(header[0:2]))
	valueSize := int(binary.BigEndian.Uint16(header[2:4]))

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

	if db.slotCache != nil {
		db.slotCache.FlushAll()
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

	errs := []error{}

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

	encoder := gob.NewEncoder(file)

	// save slot manager state
	db.slotManager.lock.Lock()
	defer db.slotManager.lock.Unlock()

	if err := encoder.Encode(db.slotManager.slotNum); err != nil {
		return fmt.Errorf("failed to encode slot num: %v", err)
	}
	if err := encoder.Encode(db.slotManager.slotSize); err != nil {
		return fmt.Errorf("failed to encode slot size: %v", err)
	}
	if err := encoder.Encode(db.slotManager.usedSlots); err != nil {
		return fmt.Errorf("failed to encode used slots: %v", err)
	}
	if err := encoder.Encode(db.slotManager.freeSlots); err != nil {
		return fmt.Errorf("failed to encode free slots: %v", err)
	}

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

	decoder := gob.NewDecoder(file)

	slotInfoLoaded := false

	var slotNum int
	if err := decoder.Decode(&slotNum); err == nil {
		var slotSize int
		if err := decoder.Decode(&slotSize); err == nil {
			if slotSize == db.slotManager.slotSize {
				db.slotManager.lock.Lock()

				var usedSlots map[int]struct{}
				if err := decoder.Decode(&usedSlots); err == nil {
					var freeSlots map[int]struct{}
					if err := decoder.Decode(&freeSlots); err == nil {
						db.slotManager.slotNum = slotNum
						db.slotManager.usedSlots = usedSlots
						db.slotManager.freeSlots = freeSlots
						slotInfoLoaded = true
					}
				}

				db.slotManager.lock.Unlock()
			}
		}
	}

	if !slotInfoLoaded {
		//fmt.Println("slot manager state not loaded, reinitializing from trie file")
		// db.markUsedSlots()
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

// isContractAccount checks if the value represents a contract account
func (db *PrefixDB) isContractAccount(value []byte) bool {
	if len(value) == 0 {
		return false
	}

	var rawNode []interface{}
	if err := rlp.DecodeBytes(value, &rawNode); err != nil {
		var account StateAccount
		if err := rlp.DecodeBytes(value, &account); err == nil {
			emptyCodeHash := make([]byte, common.HashLength)
			return !bytes.Equal(account.CodeHash, emptyCodeHash)
		}
		return false
	}
	var accountRLP []byte
	switch len(rawNode) {
	case 2:
		firstItem, ok := rawNode[0].([]byte)
		if !ok || len(firstItem) == 0 {
			// fmt.Println("无效的节点格式")
			return false
		}
		prefix := firstItem[0] >> 4
		if prefix == 2 || prefix == 3 {
			if valBytes, ok := rawNode[1].([]byte); ok {
				accountRLP = valBytes
			} else {
				return false
			}
		} else {
			return false
		}

	case 17:
		if valBytes, ok := rawNode[16].([]byte); ok && len(valBytes) > 0 {
			accountRLP = valBytes
		} else {
			return false
		}

	default:
		// fmt.Printf("未知节点格式: %d个元素\n", len(rawNode))
		return false
	}

	if len(accountRLP) == 0 {
		// fmt.Println("节点中没有账户数据")
		return false
	}

	kind, _, _, err := rlp.Split(accountRLP)
	if err != nil || kind != rlp.List {
		// fmt.Printf("账户数据不是有效的RLP列表: %v\n", err)
		return false
	}

	var account StateAccount
	if err := db.decodeAccountRLP(accountRLP, &account); err != nil {
		// fmt.Printf("解码账户数据失败: %v\n", err)
		return false
	}

	emptyCodeHash := make([]byte, common.HashLength)
	if !bytes.Equal(account.CodeHash, emptyCodeHash) {
		return true
	}

	emptyRoot := common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
	return account.Root != emptyRoot
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
	default:
	}
	return -1, errors.New("unknown key type")
}

// getParentAccountKey retrieves the parent account key from a given (code or storage)key.
func (db *PrefixDB) getParentAccountKey(key []byte) []byte {
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
	offset, storageFileID, storageOffset, storageSize, found, err := db.prefixTree.Get(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &TrieNode{
		storageFileID: storageFileID,
		storageOffset: storageOffset,
		storageSize:   storageSize,
		offset:        offset,
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
func (db *PrefixDB) serializeStorageSegment(kvs map[string][]byte) ([]byte, error) {
	est := 4
	for k, v := range kvs {
		est += 4 + len(k) + len(v)
	}

	buf := make([]byte, 0, est)
	tmp := make([]byte, 6)
	//kv count
	binary.BigEndian.PutUint32(tmp[0:4], uint32(len(kvs)))
	buf = append(buf, tmp[0:4]...)
	for k, v := range kvs {
		if len(k) > 0xFFFF {
			return nil, fmt.Errorf("key too large: %d", len(k))
		}
		binary.BigEndian.PutUint16(tmp[:2], uint16(len(k)))
		binary.BigEndian.PutUint32(tmp[2:6], uint32(len(v)))
		buf = append(buf, tmp[:6]...)
		buf = append(buf, []byte(k)...)
		buf = append(buf, v...)
	}
	return buf, nil
}

// appendStorageSegment appends a serialized storage segment to the storage file and returns its file ID, offset, and size.
func (db *PrefixDB) appendStorageSegment(kvs map[string][]byte) (fileID uint32, offset int64, size uint32, err error) {
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
	return db.storageCurFileID, offset, uint32(need), nil
}

// flushAccountEntry writes one account's storage map as a contiguous segment and updates PrefixTree
func (db *PrefixDB) flushAccountEntry(accountKey string, data map[string][]byte) error {
	if len(data) == 0 {
		return nil
	}

	var node *TrieNode
	var err error
	node, err = db.getNode([]byte(accountKey))

	if err != nil {
		return err
	}
	var accOff int64
	var prevFileID uint32
	if node != nil {
		accOff = node.offset
		prevFileID = node.storageFileID
	}

	isUpdate := (node != nil && (node.storageFileID != 0 || node.storageOffset != 0))

	// cold segment: new account or no previous segment
	if !isUpdate || (prevFileID == 0) {
		fileID, off, sz, err := db.appendStorageSegment(data)
		if err != nil {
			return err
		}
		if err := db.prefixTree.Put([]byte(accountKey), accOff, fileID, off, sz); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(accountKey, fileID, off, sz)
		return nil
	}

	// update existing segment in hot storage
	seg, err := db.serializeHotStorageSegment(accountKey, data)
	if err != nil {
		return err
	}

	if (prevFileID & hotFileIDMask) == 0 {
		// if old segment is in cold file: append new hot file
		newFID, newOff, sz, err := db.appendHotStorageSegment(accountKey, data)
		if err != nil {
			return err
		}
		_ = db.markHotFileNeedsGC(newFID)
		if err := db.prefixTree.Put([]byte(accountKey), accOff, newFID, newOff, sz); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(accountKey, newFID, newOff, sz)
		return nil
	}

	// old segment is in hot file: append to the same file
	newFID, newOff, sz, err := db.appendHotToSameFile(prevFileID, seg)
	if err != nil {
		return err
	}
	if err := db.prefixTree.Put([]byte(accountKey), accOff, newFID, newOff, sz); err != nil {
		return err
	}
	db.nodeCache.UpdateStoragePointer(accountKey, newFID, newOff, sz)
	return nil
}

// ensureAccountStorageCached ensures that the storage map for an account is loaded into the slot cache.
func (db *PrefixDB) ensureAccountStorageCached(accountKey []byte) (map[string][]byte, error) {
	ak := string(accountKey)
	if data, ok := db.slotCache.GetAccount(ak); ok {
		return data, nil
	}

	tryRead := func(fid uint32, off int64, sz uint32) (map[string][]byte, error) {
		if fid == 0 {
			return map[string][]byte{}, nil
		}
		return db.readStorageSegmentToMap(fid, off, sz)
	}

	if _, fid, off, sz, ok := db.nodeCache.Get(ak); ok && fid != 0 {
		if m, err := tryRead(fid, off, sz); err == nil {
			db.slotCache.PutAccount(ak, m)
			return m, nil
		} else if !isShortRead(err) {
			return nil, err
		}
	}

	node, err := db.getNode(accountKey)
	if err != nil {
		return nil, err
	}

	if node != nil && node.storageFileID != 0 {
		db.nodeCache.UpdateStoragePointer(ak, node.storageFileID, node.storageOffset, node.storageSize)
		if m, err := tryRead(node.storageFileID, node.storageOffset, node.storageSize); err == nil {
			db.slotCache.PutAccount(ak, m)
			return m, nil
		} else if !isShortRead(err) {
			return nil, err
		}
	}

	empty := map[string][]byte{}
	db.slotCache.PutAccount(ak, empty)
	return empty, nil
}

// valueArenaBuckets defines byte slice pools for different size buckets.
var valueArenaBuckets = []struct {
	max  int
	pool sync.Pool
}{
	{8 << 10, sync.Pool{New: func() interface{} { return make([]byte, 8<<10) }}},
	{64 << 10, sync.Pool{New: func() interface{} { return make([]byte, 64<<10) }}},
	{512 << 10, sync.Pool{New: func() interface{} { return make([]byte, 512<<10) }}},
}

// getValueArena retrieves a byte slice of at least 'need' bytes from the appropriate pool.
func getValueArena(need int) ([]byte, func([]byte)) {
	if need == 0 {
		return nil, func([]byte) {}
	}
	for i := range valueArenaBuckets {
		if need <= valueArenaBuckets[i].max {
			buf := valueArenaBuckets[i].pool.Get().([]byte)
			idx := i
			return buf[:need], func(b []byte) {
				if b == nil {
					return
				}
				max := valueArenaBuckets[idx].max
				if cap(b) < max {
					max = cap(b)
				}
				valueArenaBuckets[idx].pool.Put(b[:max])
			}
		}
	}
	return make([]byte, need), func([]byte) {}
}

func (db *PrefixDB) readStorageSegmentToMap(fileID uint32, offset int64, size uint32) (map[string][]byte, error) {
	p, isHot, _ := db.storagePathByFileID(fileID)
	if isHot {
		lock := db.getHotFileLock(fileID)
		lock.RLock()
		defer lock.RUnlock()
	}

	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if size == 0 {
		return map[string][]byte{}, nil
	}

	buf := getDataBuffer(int(size))
	defer putDataBuffer(buf)
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	buf = buf[:n]

	if !isHot {
		if len(buf) < 4 {
			return nil, fmt.Errorf("segment too small")
		}
		kvCount := binary.BigEndian.Uint32(buf[:4])
		data := buf[4:]

		var parsed uint32
		var totalV int
		var totalK int
		tmp := data
		for parsed = 0; parsed < kvCount; parsed++ {
			if len(tmp) < 6 {
				break
			}
			klen := int(binary.BigEndian.Uint16(tmp[:2]))
			vlen := int(binary.BigEndian.Uint32(tmp[2:6]))
			tmp = tmp[6:]
			if len(tmp) < klen+vlen {
				break
			}
			tmp = tmp[klen+vlen:]
			totalV += vlen
			totalK += klen
		}

		out := make(map[string][]byte, int(parsed))
		arena, release := getValueArena(totalV)
		defer release(arena)
		ap := 0

		keyArena := make([]byte, totalK)
		kp := 0

		for i := uint32(0); i < parsed; i++ {
			klen := int(binary.BigEndian.Uint16(data[:2]))
			vlen := int(binary.BigEndian.Uint32(data[2:6]))
			data = data[6:]

			copy(keyArena[kp:kp+klen], data[:klen])
			k := bytesToString(keyArena[kp:kp+klen])
			kp += klen
			data = data[klen:]

			copy(arena[ap:ap+vlen], data[:vlen])
			val := arena[ap : ap+vlen : ap+vlen]
			out[k] = val

			ap += vlen
			data = data[vlen:]
		}
		return out, nil
	}

	if len(buf) < 10 {
		return nil, io.ErrUnexpectedEOF
	}
	if binary.BigEndian.Uint32(buf[0:4]) != hotSegMagic {
		return nil, fmt.Errorf("invalid hot segment magic")
	}
	acctLen := int(binary.BigEndian.Uint16(buf[4:6]))
	kvCount := binary.BigEndian.Uint32(buf[6:10])

	cur := 10 + acctLen
	if cur > len(buf) {
		return nil, io.ErrUnexpectedEOF
	}

	var parsed uint32
	var totalV int
	var totalK int
	scan := cur
	for parsed = 0; parsed < kvCount; parsed++ {
		if scan+6 > len(buf) {
			break
		}
		klen := int(binary.BigEndian.Uint16(buf[scan : scan+2]))
		vlen := int(binary.BigEndian.Uint32(buf[scan+2 : scan+6]))
		scan += 6
		if scan+klen+vlen > len(buf) {
			break
		}
		scan += klen + vlen
		totalV += vlen
		totalK += klen
	}

	out := make(map[string][]byte, int(parsed))
	arena, release := getValueArena(totalV)
	defer release(arena)
	ap := 0

	keyArena := make([]byte, totalK)
	kp := 0

	for i := uint32(0); i < parsed; i++ {
		klen := int(binary.BigEndian.Uint16(buf[cur : cur+2]))
		vlen := int(binary.BigEndian.Uint32(buf[cur+2 : cur+6]))
		cur += 6

		copy(keyArena[kp:kp+klen], buf[cur : cur+klen])
		k := bytesToString(keyArena[kp:kp+klen])
		kp += klen
		cur += klen

		copy(arena[ap:ap+vlen], buf[cur:cur+vlen])
		val := arena[ap : ap+vlen : ap+vlen]
		out[k] = val

		ap += vlen
		cur += vlen
	}
	return out, nil
}
func isShortRead(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
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

// ensureHotCapacity ensures that there is enough space in the current hot storage file, creating a new one if necessary.
func (db *PrefixDB) ensureHotCapacity(need int64) error {
	if need > hotStorageMaxFileSize-int64(binary.Size(hotFileHeader{})) {
		return errors.New("need size larger than hotStorageMaxFileSize")
	}
	if db.hotCurFile == nil {
		return db.openOrCreateHotFile()
	}
	if db.hotCurSize+need > hotStorageMaxFileSize {
		_ = db.hotCurFile.Close()
		db.hotCurFile = nil
		db.hotCurSize = 0
		db.hotCurFileID++
		p := filepath.Join(db.hotStorageDir, fmt.Sprintf("hot_%08d.dat", db.hotCurFileID))
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		hdr := hotFileHeader{Magic: hotFileMagic, Version: 1, Flags: 0}
		if err := binary.Write(f, binary.BigEndian, &hdr); err != nil {
			f.Close()
			return fmt.Errorf("failed to write hot header: %v", err)
		}
		db.hotCurFile = f
		db.hotCurSize = int64(binary.Size(hotFileHeader{}))
	}
	return nil
}

// appendHotSegment appends a segment to the hot storage file and returns its file ID, offset, and size.
func (db *PrefixDB) appendHotToSameFile(targetFileID uint32, seg []byte) (fileID uint32, offset int64, size uint32, err error) {
	if (targetFileID & hotFileIDMask) == 0 {
		return 0, 0, 0, errors.New("target is not hot file")
	}
	lock := db.getHotFileLock(targetFileID)
	lock.Lock()
	defer lock.Unlock()

	_, _, realID := db.storagePathByFileID(targetFileID)
	p := filepath.Join(db.hotStorageDir, fmt.Sprintf("hot_%08d.dat", realID))
	f, err := os.OpenFile(p, os.O_RDWR, 0644)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()

	hdrSize := int64(binary.Size(hotFileHeader{}))
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, 0, err
	}

	var hdr hotFileHeader
	if fi.Size() < hdrSize {
		// init header
		hdr = hotFileHeader{Magic: hotFileMagic, Version: 1, Flags: 0}
		hdr.Flags |= hotFlagNeedsGC
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return 0, 0, 0, err
		}
		if err := binary.Write(f, binary.BigEndian, &hdr); err != nil {
			return 0, 0, 0, fmt.Errorf("failed to init hot header: %v", err)
		}
		offset = hdrSize
	} else {
		// read header, check magic and set needsGC flag
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return 0, 0, 0, err
		}
		if err := binary.Read(f, binary.BigEndian, &hdr); err != nil {
			return 0, 0, 0, fmt.Errorf("read hot header failed: %v", err)
		}
		if hdr.Magic != hotFileMagic {
			return 0, 0, 0, fmt.Errorf("invalid hot file magic")
		}
		if (hdr.Flags & hotFlagNeedsGC) == 0 {
			hdr.Flags |= hotFlagNeedsGC
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return 0, 0, 0, err
			}
			if err := binary.Write(f, binary.BigEndian, &hdr); err != nil {
				return 0, 0, 0, err
			}
		}
		// get end offset
		if off, err := f.Seek(0, io.SeekEnd); err != nil {
			return 0, 0, 0, err
		} else {
			offset = off
		}
	}

	// append segment
	if _, err := f.WriteAt(seg, offset); err != nil {
		return 0, 0, 0, err
	}
	return targetFileID, offset, uint32(len(seg)), nil
}

// markHotFileNeedsGC marks a hot storage file as needing garbage collection by setting a flag in its header.
func (db *PrefixDB) markHotFileNeedsGC(fileID uint32) error {
	if (fileID & hotFileIDMask) == 0 {
		return nil
	}
	_, _, realID := db.storagePathByFileID(fileID)
	p := filepath.Join(db.hotStorageDir, fmt.Sprintf("hot_%08d.dat", realID))
	f, err := os.OpenFile(p, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	var hdr hotFileHeader
	if err := binary.Read(f, binary.BigEndian, &hdr); err != nil {
		return fmt.Errorf("read hot header failed: %v", err)
	}
	if hdr.Magic != hotFileMagic {
		return fmt.Errorf("invalid hot file magic")
	}
	if (hdr.Flags & hotFlagNeedsGC) != 0 {
		return nil
	}
	hdr.Flags |= hotFlagNeedsGC
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return binary.Write(f, binary.BigEndian, &hdr)
}

// serializeHotStorageSegment serializes a hot storage segment with an account key and key-value pairs.
func (db *PrefixDB) serializeHotStorageSegment(accountKey string, kvs map[string][]byte) ([]byte, error) {
	acct := []byte(accountKey)
	if len(acct) > 0xFFFF {
		return nil, fmt.Errorf("account key too large: %d", len(acct))
	}
	est := 4 + 2 + 4 + len(acct)
	for k, v := range kvs {
		est += 6 + len(k) + len(v)
	}
	buf := make([]byte, 0, est)
	tmp := make([]byte, 10)
	binary.BigEndian.PutUint32(tmp[0:4], hotSegMagic)
	binary.BigEndian.PutUint16(tmp[4:6], uint16(len(acct)))
	binary.BigEndian.PutUint32(tmp[6:10], uint32(len(kvs)))
	buf = append(buf, tmp[:10]...)
	buf = append(buf, acct...)
	for k, v := range kvs {
		if len(k) > 0xFFFF {
			return nil, fmt.Errorf("key too large: %d", len(k))
		}
		binary.BigEndian.PutUint16(tmp[:2], uint16(len(k)))
		binary.BigEndian.PutUint32(tmp[2:6], uint32(len(v)))
		buf = append(buf, tmp[:6]...)
		buf = append(buf, []byte(k)...)
		buf = append(buf, v...)
	}
	return buf, nil
}

// appendHotStorageSegment appends a serialized hot storage segment to the hot storage file and returns its file ID, offset, and size.
func (db *PrefixDB) appendHotStorageSegment(accountKey string, kvs map[string][]byte) (fileID uint32, offset int64, size uint32, err error) {
	seg, err := db.serializeHotStorageSegment(accountKey, kvs)
	if err != nil {
		return 0, 0, 0, err
	}
	need := int64(len(seg))
	if err := db.ensureHotCapacity(need); err != nil {
		return 0, 0, 0, err
	}
	offset = db.hotCurSize
	if _, err := db.hotCurFile.WriteAt(seg, offset); err != nil {
		return 0, 0, 0, err
	}
	db.hotCurSize += need
	return (db.hotCurFileID | hotFileIDMask), offset, uint32(need), nil
}

// getHotFileLock retrieves or creates a mutex for synchronizing access to a specific hot storage file.
func (db *PrefixDB) getHotFileLock(fileID uint32) *sync.RWMutex {
	realID := fileID & ^hotFileIDMask
	if v, ok := db.hotFileLocks.Load(realID); ok {
		return v.(*sync.RWMutex)
	}
	mu := &sync.RWMutex{}
	actual, _ := db.hotFileLocks.LoadOrStore(realID, mu)
	return actual.(*sync.RWMutex)
}

// hotGCWorker is a background goroutine that periodically performs garbage collection on hot storage files.
func (db *PrefixDB) hotGCWorker(interval time.Duration) {
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-db.hotGCStop:
			return
		case <-tk.C:
			_ = db.gcHotFilesOnce()
		}
	}
}

// gcHotFilesOnce performs garbage collection on all hot storage files that are marked as needing GC.
func (db *PrefixDB) gcHotFilesOnce() error {
	entries, err := os.ReadDir(db.hotStorageDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var id uint32
		n, _ := fmt.Sscanf(e.Name(), "hot_%08d.dat", &id)
		if n != 1 {
			continue
		}
		if err := db.gcHotFileInPlace(id); err != nil {
			fmt.Printf("hot in-place GC error for %08d: %v\n", id, err)
		}
	}
	return nil
}

// gcHotFileInPlace performs in-place garbage collection on a specific hot storage file.
func (db *PrefixDB) gcHotFileInPlace(realID uint32) error {
	fileID := realID | hotFileIDMask
	lock := db.getHotFileLock(fileID)
	lock.Lock()
	defer lock.Unlock()

	p := filepath.Join(db.hotStorageDir, fmt.Sprintf("hot_%08d.dat", realID))
	f, err := os.OpenFile(p, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var hdr hotFileHeader
	if err := binary.Read(f, binary.BigEndian, &hdr); err != nil {
		return fmt.Errorf("read hot header failed: %v", err)
	}
	if hdr.Magic != hotFileMagic {
		return fmt.Errorf("invalid hot file magic")
	}
	if (hdr.Flags & hotFlagNeedsGC) == 0 {
		return nil
	}

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	headerSize := int64(binary.Size(hdr))
	if fi.Size() < headerSize {
		return fmt.Errorf("hot file truncated")
	}
	totalDataSize := fi.Size() - headerSize
	if totalDataSize == 0 {
		hdr.Flags &^= hotFlagNeedsGC
		if _, err := f.Seek(0, io.SeekStart); err == nil {
			_ = binary.Write(f, binary.BigEndian, &hdr)
		}
		return nil
	}

	data := make([]byte, totalDataSize)
	if _, err := f.ReadAt(data, headerSize); err != nil && err != io.EOF {
		return fmt.Errorf("bulk read failed: %v", err)
	}

	type segMeta struct {
		accountKey string
		start      int64
		size       int64
	}
	lastSeg := make(map[string]segMeta, 1024)

	pos := 0
	for pos < len(data) {
		//read segment header
		if pos+10 > len(data) {
			break
		}
		magic := binary.BigEndian.Uint32(data[pos : pos+4])
		if magic != hotSegMagic {
			break
		}
		acctLen := int(binary.BigEndian.Uint16(data[pos+4 : pos+6]))
		kvCount := binary.BigEndian.Uint32(data[pos+6 : pos+10])
		segStartAbs := headerSize + int64(pos)

		cur := pos + 10
		if cur+acctLen > len(data) {
			break
		}
		accountKeyBytes := data[cur : cur+acctLen]
		cur += acctLen

		segSize := int64(10 + acctLen)

		// skip kvs
		for i := uint32(0); i < kvCount; i++ {
			if cur+6 > len(data) {
				cur = len(data)
				break
			}
			klen := int(binary.BigEndian.Uint16(data[cur : cur+2]))
			vlen := int(binary.BigEndian.Uint32(data[cur+2 : cur+6]))
			cur += 6
			if cur+klen+vlen > len(data) {
				cur = len(data)
				break
			}
			cur += klen + vlen
			segSize += int64(6 + klen + vlen)
		}

		acctKey := string(accountKeyBytes)
		lastSeg[acctKey] = segMeta{
			accountKey: acctKey,
			start:      segStartAbs,
			size:       segSize,
		}

		// move to next segment
		pos += int(segSize)
	}

	// compact segments
	keeps := make([]segMeta, 0, len(lastSeg))
	for _, s := range lastSeg {
		keeps = append(keeps, s)
	}
	sort.Slice(keeps, func(i, j int) bool { return keeps[i].start < keeps[j].start })

	writePos := headerSize
	for _, s := range keeps {
		if s.start == writePos {
			// already in place, just update index
			accNode, _ := db.getNode([]byte(s.accountKey))
			var accOff int64
			if accNode != nil {
				accOff = accNode.offset
			}
			if err := db.prefixTree.Put([]byte(s.accountKey), accOff, fileID, writePos, uint32(s.size)); err != nil {
				return fmt.Errorf("update prefix tree failed: %v", err)
			}
			db.nodeCache.UpdateStoragePointer(s.accountKey, fileID, writePos, uint32(s.size))
			writePos += s.size
			continue
		}

		// move segment data
		segBuf := make([]byte, s.size)
		if _, err := f.ReadAt(segBuf, s.start); err != nil {
			return fmt.Errorf("read segment failed: %v", err)
		}
		if _, err := f.WriteAt(segBuf, writePos); err != nil {
			return fmt.Errorf("write segment failed: %v", err)
		}

		// update index
		accNode, _ := db.getNode([]byte(s.accountKey))
		var accOff int64
		if accNode != nil {
			accOff = accNode.offset
		}
		if err := db.prefixTree.Put([]byte(s.accountKey), accOff, fileID, writePos, uint32(s.size)); err != nil {
			return fmt.Errorf("update prefix tree failed: %v", err)
		}
		db.nodeCache.UpdateStoragePointer(s.accountKey, fileID, writePos, uint32(s.size))

		writePos += s.size
	}

	// truncate file
	if err := f.Truncate(writePos); err != nil {
		return fmt.Errorf("truncate hot file failed: %v", err)
	}

	// clear needsGC flag
	hdr.Flags &^= hotFlagNeedsGC
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, &hdr); err != nil {
		return fmt.Errorf("write header failed: %v", err)
	}
	_ = f.Sync()
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
