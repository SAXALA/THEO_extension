package ssPrefixdb

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
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

const MAX_CACHE_SIZE = 1024   // maximum cache size
const BUFFER_SIZE = 8192      // buffer size for file operations
const SLOT_SIZE = 1024 * 1024 // size of each slot
const SLOT_NUM = 1024
const avgKVPairsPerSlot = 50 // average number of key-value pairs per slot

type KeyType int

const (
	TASnapshot KeyType = iota //
	TSSnapshot                //
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

type TrieNode struct {
	slotIndices []int // additional slot indices for contract account
	offset      int64 // offset in the file
}

type accountIndex struct {
	accountOffset map[string]int64 // index for account keys
	indexLock     sync.RWMutex
}

func (ai *accountIndex) get(key string) (int64, bool) {
	ai.indexLock.RLock()
	defer ai.indexLock.RUnlock()
	offset, exists := ai.accountOffset[key]
	if !exists {
		return -1, false
	}
	return offset, true
}

func (ai *accountIndex) put(key string, offset int64) {
	ai.indexLock.Lock()
	defer ai.indexLock.Unlock()
	ai.accountOffset[key] = offset
}

func (ai *accountIndex) set(key string, offset int64) {
	ai.indexLock.Lock()
	defer ai.indexLock.Unlock()
	if _, exists := ai.accountOffset[key]; !exists {
		ai.accountOffset[key] = offset
	}
}

func (ai *accountIndex) delete(key string) {
	ai.indexLock.Lock()
	defer ai.indexLock.Unlock()
	delete(ai.accountOffset, key)
}

func NewAccountIndex() *accountIndex {
	return &accountIndex{
		accountOffset: make(map[string]int64),
		indexLock:     sync.RWMutex{},
	}
}

type SSPrefixDB struct {
	accountIndex accountIndex
	accountFile  *os.File
	slotFile     *os.File
	nodeCache    *NodeCache
	slotCache    *SlotCache
	slotManager  *SlotManager
	batch        *WriteBatch
	triePath     string // path to the prefix tree file
}

type SerializedTrieNode struct {
	Path        string
	SlotIndices []int
	Offset      int64
}

/**
 * NewSSPrefixDB creates a new PrefixDB instance.
 * NAfilePath: path to the normal account file
 * CAfilePath: path to the contract account file
 * cacheSize: fixed size of the in-memory cache
 */
func NewSSPrefixDB(dirPath string) (*SSPrefixDB, error) {

	NAfilePath := filepath.Join(dirPath, "ssprefixdb", "account")
	CAfilePath := filepath.Join(dirPath, "ssprefixdb", "storage")
	triePath := filepath.Join(dirPath, "ssprefixdb", "trie")

	naFilePath, err := os.OpenFile(NAfilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("failed to open account file")
	}
	cafilePath, err := os.OpenFile(CAfilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("failed to open storage file")
	}

	// get the path for the prefix tree file (same directory as normal account file)

	db := &SSPrefixDB{
		accountIndex: *NewAccountIndex(),
		accountFile:  naFilePath,
		slotFile:     cafilePath,
		nodeCache:    NewNodeCache(MAX_CACHE_SIZE),
		batch:        NewWriteBatch(),
		slotManager:  NewSlotManager(SLOT_NUM, SLOT_SIZE),
		triePath:     triePath,
	}

	db.slotCache = NewSlotCache(1, db)

	db.batch.EnableAutoCommit(db, 1000) // enable auto commit with a threshold of 1000 operations

	// try to load the persisted prefix tree
	if err := db.LoadTrie(); err != nil {
		// if loading fails, use an empty prefix tree (already initialized in the constructor)
		fmt.Printf("unable to load ssprefix tree, using empty tree: %v\n", err)
	} else {
		// mark the used slot
		db.markUsedSlots()
	}

	return db, nil
}

func (db *SSPrefixDB) Get(key []byte) ([]byte, bool, error) {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return nil, false, err
	}

	switch keyType {
	case TASnapshot:
		// check in cache
		if value, _, ok := db.nodeCache.Get(string(key)); ok {
			return value, true, nil
		}

		// check in batch
		if db.batch != nil {
			if value, _, ok := db.batch.get(key); ok {
				return value, true, nil
			}
		}

		offset, exists := db.accountIndex.get(string(key))
		if !exists {
			return nil, false, nil
		}
		value, slotIndices, err := db.readFromFile(offset, TASnapshot)
		if err != nil {
			return nil, false, err
		}

		// add to cache and cache path of the node
		db.nodeCache.Put(string(key), value, slotIndices)
		db.nodeCache.CachePathToNode(string(key), db)
		return value, true, nil

	case TSSnapshot:
		accountKey := db.GetParentAccountKey(key)
		if accountKey == nil {
			return nil, false, errors.New("parent account not found")
		}

		var slotIndices []int64
		var ok bool
		value := []byte{}
		if _, slotIndices, ok = db.nodeCache.Get(string(accountKey)); !ok {
			if value, slotIndices, ok = db.batch.get(accountKey); !ok {
				offset, exists := db.accountIndex.get(string(accountKey))
				if !exists {
					return nil, false, errors.New("parent account not found in index")
				}
				value, slotIndices, err = db.readFromFile(offset, TASnapshot)
			}
			db.nodeCache.Put(string(accountKey), value, slotIndices)
		}

		// check in batch
		if len(slotIndices) > 0 {
			for _, slotIndex := range slotIndices {
				if value, ok := db.batch.getBySlotIndex(slotIndex, key); ok {
					return value, true, nil
				}
			}
		} else {
			return nil, false, errors.New("no slot allocated")
		}

		keyStr := string(key)

		// search in all associated slots
		for _, slotIdx := range slotIndices {
			//cache check
			if slotData, exists := db.slotCache.Get(slotIdx); exists {
				if value, ok := slotData[keyStr]; ok {
					return value, true, nil
				}
			} else {
				// load slot data from disk
				slotData, err := db.loadSlot(slotIdx)
				if err == nil {
					db.slotCache.Put(slotIdx, slotData)
					if value, exists := slotData[keyStr]; exists {
						return value, true, nil
					}
				}
			}
		}
	default:
		return nil, false, errors.New("unknown key type")
	}
	return nil, false, errors.New("key not found")
}

func (db *SSPrefixDB) Put(key, value []byte) error {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}

	switch keyType {
	case TASnapshot:
		var slotIndices []int64
		if offset, exists := db.accountIndex.get(string(key)); exists {
			// update existing account
			var ok bool
			if _, slotIndices, ok = db.nodeCache.Get(string(key)); !ok {
				if _, slotIndices, ok = db.batch.get(key); !ok {
					// not in batch,but offset is 1, is writing to the file
					if offset == 1 {
						db.batch.CommitBatch()
						offset, _ = db.accountIndex.get(string(key))
					}
					_, slotIndices, err = db.readFromFile(offset, TASnapshot)
				}
			}
			if err != nil {
				return fmt.Errorf("failed to read account data: %v", err)
			}
		} else {
			slotIndices = []int64{}
			db.accountIndex.put(string(key), 1) // set a dummy offset, will be updated later
		}

		db.nodeCache.Put(string(key), value, slotIndices)
		db.batch.add(key, value, slotIndices)
		db.nodeCache.CachePathToNode(string(key), db)

	case TSSnapshot:
		accountKey := db.GetParentAccountKey(key)
		if accountKey == nil {
			return errors.New("parent account not found")
		}

		var slotIndices []int64
		var ok bool
		accountValue := []byte{}
		if accountValue, slotIndices, ok = db.nodeCache.Get(string(accountKey)); !ok {
			if accountValue, slotIndices, ok = db.batch.get(accountKey); !ok {
				offset, exists := db.accountIndex.get(string(accountKey))
				if !exists {
					return errors.New("parent account not found in index")
				}
				accountValue, slotIndices, err = db.readFromFile(offset, TASnapshot)
				if err != nil {
					return fmt.Errorf("failed to read account data: %v", err)
				}
			}
		}

		if len(slotIndices) <= 0 {
			newSlotIdx := db.slotManager.getEmptySlot()
			if newSlotIdx == -1 {
				return errors.New("no empty slot available for contract account")
			}

			slotIndices = []int64{int64(newSlotIdx)}

			if len(accountValue) == 0 {
				offset, exists := db.accountIndex.get(string(accountKey))
				if !exists {
					return errors.New("parent account not found in index")
				}
				accountValue, _, err = db.readFromFile(offset, TASnapshot)
				if err != nil {
					return fmt.Errorf("failed to read account value: %v", err)
				}
			}

			db.nodeCache.Put(string(accountKey), accountValue, slotIndices)
			db.batch.add(accountKey, accountValue, slotIndices)
		}

		keyStr := string(key)
		entrySize := 4 + len(key) + len(value)
		slotFound := false

		if len(slotIndices) > 0 {
			slotIdx := slotIndices[len(slotIndices)-1]
			var slotData map[string][]byte
			var slotSize int
			var exists bool
			if slotData, exists = db.slotCache.Get(slotIdx); exists {
				slotSize = db.slotManager.getSlotUsedSize(int(slotIdx))
			} else if slotData, exists = db.batch.getSlot(slotIdx); exists {
				slotSize = db.slotManager.getSlotUsedSize(int(slotIdx))
			} else {
				var err error
				if slotData, err = db.loadSlot(slotIdx); err != nil {
					// continue
				}
				slotSize = db.calculateSlotSize(slotData)
				db.slotManager.setSlotUsedSize(int(slotIdx), slotSize)
			}

			if slotSize+entrySize <= SLOT_SIZE {
				slotData[keyStr] = value
				db.slotCache.Put(slotIdx, slotData)
				db.slotCache.MarkSlotModified(slotIdx)
				db.slotManager.updateUsedSize(int(slotIdx), entrySize)
				slotFound = true
				break
			}
		}

		// no slot found with enough space
		if !slotFound {
			newSlotIdx := db.slotManager.getEmptySlot()
			if newSlotIdx == -1 {
				return errors.New("no empty slot available for expanding contract account")
			}

			slotIndices = append(slotIndices, int64(newSlotIdx))

			newSlotData := make(map[string][]byte)
			newSlotData[keyStr] = value

			db.slotCache.Put(int64(newSlotIdx), newSlotData)
			db.slotCache.MarkSlotModified(int64(newSlotIdx))

			db.nodeCache.Put(string(accountKey), value, slotIndices)
			db.batch.add(key, value, slotIndices)

			db.slotManager.updateUsedSize(newSlotIdx, entrySize)
		}
	}
	return nil
}

func (db *SSPrefixDB) calculateSlotSize(slotData map[string][]byte) int {
	size := 0
	for k, v := range slotData {
		size += 4 + len(k) + len(v)
	}
	return size
}

func (db *SSPrefixDB) Has(key []byte) (bool, error) {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return false, err
	}

	switch keyType {
	case TASnapshot:
		// check in accountIndex
		if _, exists := db.accountIndex.get(string(key)); exists {
			return true, nil
		} else {
			return false, nil
		}

	case TSSnapshot:
		accountKey := db.GetParentAccountKey(key)
		if accountKey == nil {
			return false, errors.New("parent account not found")
		}

		var slotIndices []int64
		var ok bool
		value := []byte{}
		if _, slotIndices, ok = db.nodeCache.Get(string(accountKey)); !ok {
			if value, slotIndices, ok = db.batch.get(accountKey); !ok {
				offset, exists := db.accountIndex.get(string(accountKey))
				if !exists {
					return false, errors.New("parent account not found in index")
				}
				value, slotIndices, err = db.readFromFile(offset, TASnapshot)
			}
			db.nodeCache.Put(string(accountKey), value, slotIndices)
		}

		// check in batch
		if len(slotIndices) > 0 {
			for _, slotIndex := range slotIndices {
				if _, ok := db.batch.getBySlotIndex(slotIndex, key); ok {
					return true, nil
				}
			}
		} else {
			return false, errors.New("no slot allocated")
		}

		keyStr := string(key)

		// search in all associated slots
		for _, slotIdx := range slotIndices {
			//cache check
			if slotData, exists := db.slotCache.Get(slotIdx); exists {
				if _, ok := slotData[keyStr]; ok {
					return true, nil
				}
			} else {
				// load slot data from disk
				slotData, err := db.loadSlot(slotIdx)
				if err == nil {
					db.slotCache.Put(slotIdx, slotData)
					if _, exists := slotData[keyStr]; exists {
						return true, nil
					}
				}
			}
		}
		return false, nil
	default:
		return false, errors.New("unknown key type")
	}
}

// func (db *SSPrefixDB) findNode(key []byte) (*TrieNode, error) {
// 	db.nodesMutex.RLock()
// 	defer db.nodesMutex.RUnlock()

// 	node, exists := db.nodes[string(key)]
// 	if !exists {
// 		return nil, nil
// 	}
// 	return node, nil
// }

// // deleteNodeFromTrie removes a node from the trie structure
// func (db *SSPrefixDB) deleteNodeFromTrie(key []byte) {
// 	db.nodesMutex.Lock()
// 	defer db.nodesMutex.Unlock()

// 	delete(db.nodes, string(key))
// }

func (db *SSPrefixDB) Delete(key []byte) error {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}

	switch keyType {
	case TASnapshot:
		var slotIndices []int64
		var ok bool
		if _, slotIndices, ok = db.nodeCache.Get(string(key)); !ok {
			if _, slotIndices, ok = db.batch.get(key); !ok {
				offset, exists := db.accountIndex.get(string(key))
				if !exists {
					return nil // key does not exist, nothing to delete
				}
				_, slotIndices, err = db.readFromFile(offset, TASnapshot)
			}
		}

		if len(slotIndices) > 0 {
			for _, slotIdx := range slotIndices {
				db.slotCache.Delete(slotIdx)
				db.slotManager.releaseSlot(int(slotIdx), db.slotFile)
			}
		}

		db.nodeCache.Delete(string(key))
		db.batch.delete(key)
		db.accountIndex.delete(string(key))

	case TSSnapshot:
		accountKey := db.GetParentAccountKey(key)
		if accountKey == nil {
			return errors.New("parent account not found")
		}

		var slotIndices []int64
		var ok bool
		value := []byte{}
		if _, slotIndices, ok = db.nodeCache.Get(string(accountKey)); !ok {
			if value, slotIndices, ok = db.batch.get(accountKey); !ok {
				offset, exists := db.accountIndex.get(string(accountKey))
				if !exists {
					return errors.New("parent account not found in index")
				}
				value, slotIndices, err = db.readFromFile(offset, TASnapshot)
			}
			db.nodeCache.Put(string(accountKey), value, slotIndices)
		}

		for _, slotIdx := range slotIndices {
			if slotData, exists := db.slotCache.Get(slotIdx); exists {
				delete(slotData, string(key))
				db.slotCache.MarkSlotModified(slotIdx)
				db.slotCache.Put(slotIdx, slotData)
			} else {
				slotData, err := db.loadSlot(slotIdx)
				if err != nil {
					return err
				}
				delete(slotData, string(key))
				db.slotCache.MarkSlotModified(slotIdx)
				db.slotCache.Put(slotIdx, slotData)
			}
		}
	default:
		return errors.New("unknown key type")
	}

	return nil
}

// func (db *SSPrefixDB) createNode(key []byte) (*TrieNode, error) {
// 	db.nodesMutex.Lock()
// 	defer db.nodesMutex.Unlock()

// 	keyStr := string(key)
// 	if _, exists := db.nodes[keyStr]; !exists {
// 		db.nodes[keyStr] = &TrieNode{
// 			slotIndices: nil,
// 			offset:      0,
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
	dataBufferPools = &sync.Map{}
)

// getDataBuffer returns a buffer of the specified size from the pool.
func getDataBuffer(size int) []byte {
	if size <= 1024 {
		pool, _ := dataBufferPools.LoadOrStore(1024, &sync.Pool{
			New: func() interface{} {
				return make([]byte, 1024)
			},
		})
		buf := pool.(*sync.Pool).Get().([]byte)
		return buf[:size]
	}

	bufSize := 1
	for bufSize < size {
		bufSize *= 2
	}

	poolI, _ := dataBufferPools.LoadOrStore(bufSize, &sync.Pool{
		New: func() interface{} {
			return make([]byte, bufSize)
		},
	})
	pool := poolI.(*sync.Pool)
	buf := pool.Get().([]byte)
	return buf[:size]
}

// putDataBuffer returns a buffer to the pool for reuse.
func putDataBuffer(buf []byte) {
	if buf == nil {
		return
	}

	bufSize := 1024
	if cap(buf) > bufSize {
		bufSize = 1
		for bufSize < cap(buf) {
			bufSize *= 2
		}
	}

	if poolI, ok := dataBufferPools.Load(bufSize); ok {
		pool := poolI.(*sync.Pool)
		pool.Put(buf[:cap(buf)])
	}
}

func (db *SSPrefixDB) readFromFile(offset int64, keyType KeyType) ([]byte, []int64, error) {
	// Read from the appropriate file based on account type
	var file *os.File
	if keyType == TASnapshot {
		file = db.accountFile
	} else {
		file = db.slotFile
	}

	header := headerPool.Get().([]byte)
	defer headerPool.Put(header)

	if cap(header) < 6 {
		header = make([]byte, 6)
	} else {
		header = header[:6]
	}

	_, err := file.ReadAt(header, offset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read header at offset %d: %v", offset, err)
	}

	slotIndicesSize := binary.BigEndian.Uint16(header[0:2])
	keySize := int(binary.BigEndian.Uint16(header[2:4]))
	valueSize := int(binary.BigEndian.Uint16(header[4:6]))

	kvSize := keySize + valueSize
	kvData := getDataBuffer(kvSize)
	defer putDataBuffer(kvData)

	_, err = file.ReadAt(kvData, offset+6)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read key-value data at offset %d: %v", offset+6, err)
	}

	value := make([]byte, valueSize)
	copy(value, kvData[keySize:keySize+valueSize])

	var slotIndices []int64
	if slotIndicesSize > 0 {
		slotDataOffset := offset + 6 + int64(kvSize)
		slotDataSize := int(slotIndicesSize)

		slotData := getDataBuffer(slotDataSize)
		defer putDataBuffer(slotData)

		_, err = file.ReadAt(slotData, slotDataOffset)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read slot data at offset %d: %v", slotDataOffset, err)
		}

		slotCount := slotDataSize / 8
		slotIndices = make([]int64, slotCount)
		for i := 0; i < slotCount; i++ {
			slotIndices[i] = int64(binary.BigEndian.Uint64(slotData[i*8 : (i+1)*8]))
		}
	}

	return value, slotIndices, nil
}

func (db *SSPrefixDB) Close() error {

	db.nodeCache.Close()

	db.slotCache.FlushModifiedSlots()
	// forbid further writes to the database
	if db.batch != nil {
		db.batch.DisableAutoCommit()

		// wait for any ongoing background commit to finish
		if db.batch.bgCommit {
			db.batch.DisableBackgroundCommit()
		}
	}

	if db.batch != nil && len(db.batch.operations) > 0 {
		_ = db.WriteCommit(db.batch)
	}

	if err := db.SaveTrie(); err != nil {
		return fmt.Errorf("failed to save prefix tree: %v", err)
	}

	if err := db.accountFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync account file: %v", err)
	}

	if err := db.slotFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync slot file: %v", err)
	}

	if err := db.accountFile.Close(); err != nil {
		return err
	}

	if err := db.slotFile.Close(); err != nil {
		return err
	}

	// clear pools
	dataBufferPools.Range(func(key, value interface{}) bool {
		pool := value.(*sync.Pool)
		pool.New = nil // clear the new function to prevent memory leaks
		return true
	})

	db.nodeCache = nil
	db.slotCache = nil
	db.batch = nil

	return nil
}

// SaveTrie saves the current prefix tree to a file.
func (db *SSPrefixDB) SaveTrie() error {
	// write accountIndex to file
	file, err := os.Create(db.triePath)
	if err != nil {
		return fmt.Errorf("failed to create the prefix tree file: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	db.accountIndex.indexLock.RLock()
	indexSize := len(db.accountIndex.accountOffset)

	if err := encoder.Encode(indexSize); err != nil {
		db.accountIndex.indexLock.RUnlock()
		return fmt.Errorf("failed to encode index size: %v", err)
	}

	keys := make([]string, 0, indexSize)
	offsets := make([]int64, 0, indexSize)

	for key, offset := range db.accountIndex.accountOffset {
		keys = append(keys, key)
		offsets = append(offsets, offset)
	}
	db.accountIndex.indexLock.RUnlock()

	for i, key := range keys {
		offset := offsets[i]

		// encode key
		if err := encoder.Encode(key); err != nil {
			return fmt.Errorf("failed to encode key %s: %v", key, err)
		}

		// encode offset
		if err := encoder.Encode(offset); err != nil {
			return fmt.Errorf("failed to encode offset for key %s: %v", key, err)
		}
	}

	// save slot manager state
	db.slotManager.lock.Lock()
	defer db.slotManager.lock.Unlock()

	if err := encoder.Encode(db.slotManager.slotNum); err != nil {
		return fmt.Errorf("failed to encode slot num: %v", err)
	}

	if err := encoder.Encode(db.slotManager.slotSize); err != nil {
		return fmt.Errorf("failed to encode slot size: %v", err)
	}

	if err := encoder.Encode(db.slotManager.usedSizes); err != nil {
		return fmt.Errorf("failed to encode used sizes: %v", err)
	}

	if err := encoder.Encode(db.slotManager.freeSlots); err != nil {
		return fmt.Errorf("failed to encode free slots: %v", err)
	}

	return nil
}

// LoadTrie loads the prefix tree from a file.
func (db *SSPrefixDB) LoadTrie() error {
	file, err := os.OpenFile(db.triePath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open the prefix tree file: %v", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	var indexSize int
	if err := decoder.Decode(&indexSize); err != nil {
		return fmt.Errorf("failed to decode index size: %v", err)
	}

	db.accountIndex.indexLock.Lock()
	defer db.accountIndex.indexLock.Unlock()

	db.accountIndex.accountOffset = make(map[string]int64, indexSize)

	for i := 0; i < indexSize; i++ {

		var key string
		if err := decoder.Decode(&key); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode key at index %d: %v", i, err)
		}
		var offset int64
		if err := decoder.Decode(&offset); err != nil {
			return fmt.Errorf("failed to decode offset at index %d: %v", i, err)
		}
		db.accountIndex.accountOffset[key] = offset
	}

	slotInfoLoaded := false

	var slotNum int
	if err := decoder.Decode(&slotNum); err == nil {
		var slotSize int
		if err := decoder.Decode(&slotSize); err == nil {
			if slotSize == db.slotManager.slotSize {
				db.slotManager.lock.Lock()

				var usedSizes []int
				if err := decoder.Decode(&usedSizes); err == nil {
					var freeSlots []int
					if err := decoder.Decode(&freeSlots); err == nil {
						db.slotManager.slotNum = slotNum
						db.slotManager.usedSizes = usedSizes
						db.slotManager.freeSlots = freeSlots
						slotInfoLoaded = true
					}
				}

				db.slotManager.lock.Unlock()
			}
		}
	}

	if !slotInfoLoaded {
		fmt.Println("slot manager state not loaded, reinitializing from slot file")
		db.markUsedSlots()
	}
	return nil
}

// ExtractAccountData extracts account data from the value of a TrieAccount node.
func (db *SSPrefixDB) ExtractAccountData(key, value []byte) (*StateAccount, error) {
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
func (db *SSPrefixDB) decodeAccountRLP(accountRLP []byte, account *StateAccount) error {
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

func (db *SSPrefixDB) loadSlot(index int64) (map[string][]byte, error) {

	buf := getDataBuffer(SLOT_SIZE)
	defer putDataBuffer(buf)

	appendOnlyKVPairs := make(map[string][]byte, avgKVPairsPerSlot)

	offset := index * SLOT_SIZE
	n, err := db.slotFile.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("loadSlot error: %v", err)
	}

	data := buf[:n]
	position := 0

	for position+4 <= len(data) {

		if data[position] == 0 && data[position+1] == 0 {
			break
		}

		keySize := int(uint16(data[position])<<8 | uint16(data[position+1]))
		valueSize := int(uint16(data[position+2])<<8 | uint16(data[position+3]))

		totalSize := 4 + keySize + valueSize
		if position+totalSize > len(data) {
			break
		}

		key := string(data[position+4 : position+4+keySize])

		value := make([]byte, valueSize)
		copy(value, data[position+4+keySize:position+4+keySize+valueSize])

		appendOnlyKVPairs[key] = value
		position += totalSize
	}

	return appendOnlyKVPairs, nil
}

func (db *SSPrefixDB) saveSlot(index int64, slot *Slot) {
	offset := int64(index * SLOT_SIZE)
	buf := make([]byte, SLOT_SIZE)

	// Serialize appendOnlyPart map into the buffer
	data := buf[:SLOT_SIZE]
	for key, value := range slot.appendOnlyPart {
		entry, _ := db.ConvertKV([]byte(key), value)
		if len(entry) > len(data) {
			break
		}
		copy(data, entry)
		data = data[len(entry):]
	}

	// Write the slot back to the file
	_, err := db.slotFile.WriteAt(buf, offset)
	if err != nil {
		fmt.Printf("saveSlot index %d  error: %v\n", index, err)
	}
	// clear the append-only part
	for k := range slot.appendOnlyPart {
		delete(slot.appendOnlyPart, k)
	}

}

// Convert key-value pair to byte array: <key size (short) + value size (short) + key + value>
func (db *SSPrefixDB) ConvertKV(key, value []byte) ([]byte, error) {

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
func (db *SSPrefixDB) isContractAccount(value []byte) bool {
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
	if account.Root != emptyRoot {
		return true
	}

	return false
}

// getKeyType determines the type of key based on its prefix.
func (db *SSPrefixDB) getKeyType(key []byte) (KeyType, error) {
	if len(key) == 0 {
		return -1, errors.New("invalid key")
	}

	prefix := key[0]
	switch prefix {
	case 'a':
		return TASnapshot, nil
	case 'o':
		return TSSnapshot, nil
	default:
	}
	return -1, errors.New("unknown key type")
}

// GetParentAccountKey retrieves the parent account key from a given (code or storage)key.
func (db *SSPrefixDB) GetParentAccountKey(key []byte) []byte {
	if len(key) < 21 {
		return nil
	}
	accountHash := key[1:33]
	// accountHashHex := common.Bytes2Hex(accountHash)

	snapshotKey := append([]byte{'a'}, accountHash...)
	return snapshotKey
}

// markUsedSlots marks all used slots in the prefix tree.
func (db *SSPrefixDB) markUsedSlots() {
	// get all account keys and their offsets
	db.accountIndex.indexLock.RLock()
	keys := make([]string, 0, len(db.accountIndex.accountOffset))
	offsets := make([]int64, 0, len(db.accountIndex.accountOffset))

	for k, off := range db.accountIndex.accountOffset {
		keys = append(keys, k)
		offsets = append(offsets, off)
	}
	db.accountIndex.indexLock.RUnlock()

	for i, key := range keys {
		offset := offsets[i]
		if offset <= 0 {
			continue
		}

		_, slotIndices, exists := db.nodeCache.Get(key)
		if !exists {
			// 从文件读取
			_, slotIndices, err := db.readFromFile(offset, TASnapshot)
			if err != nil || len(slotIndices) == 0 {
				continue
			}

			for _, slotIdx := range slotIndices {
				if slotIdx >= 0 {
					db.slotManager.setSlotUsedSize(int(slotIdx), 0)

					slotData, err := db.loadSlot(slotIdx)
					if err == nil {
						size := db.calculateSlotSize(slotData)
						db.slotManager.setSlotUsedSize(int(slotIdx), size)
					}
				}
			}
		} else {
			for _, slotIdx := range slotIndices {
				if slotIdx >= 0 {
					db.slotManager.setSlotUsedSize(int(slotIdx), 0)

					slotData, err := db.loadSlot(slotIdx)
					if err == nil {
						size := db.calculateSlotSize(slotData)
						db.slotManager.setSlotUsedSize(int(slotIdx), size)
					}
				}
			}
		}
	}
}
