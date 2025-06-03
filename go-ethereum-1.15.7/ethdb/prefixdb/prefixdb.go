package prefixdb

import (
	"errors"
	"io"
	"os"
	"sync"
)

const MAX_CACHE_SIZE = 1024        // maximum cache size
const BUFFER_SIZE = 8192           // buffer size for file operations
const SLOT_SIZE = 1024 * 1024      // size of each slot
const SLOT_NUM = 1024              // number of slots
const APPENDONLY_SIZE = 512 * 1024 // size of append-only part in each slot

type AccountType int

const (
	// Account types
	NormalAccount AccountType = iota
	ContractAccount
)

type TrieNode struct {
	children    map[byte]*TrieNode // children nodes
	value       []byte             // value of the node
	slotIndex   int                // slot index for contract account
	offset      int64              // for normal account, offset in the file,for contract account, offset in the slot
	isLeaf      bool               // is leaf node
	accountType AccountType        // account type
	refCount    int                // reference counter for internal nodes

}

type PrefixDB struct {
	root                *TrieNode
	normalAccountFile   *os.File
	contractAccountFile *os.File
	cache               map[string][]byte // in-memory cache
	cacheLock           sync.RWMutex
	cacheSize           int            // fixed size of the cache
	cacheFreq           map[string]int // frequency of access for each key
	slotManager         *SlotManager
	batch               *WriteBatch
}

/**
 * NewPrefixDB creates a new PrefixDB instance.
 * NAfilePath: path to the normal account file
 * CAfilePath: path to the contract account file
 * cacheSize: fixed size of the in-memory cache
 */
func NewPrefixDB(NAfilePath string, CAfilePath string, cacheSize int) (*PrefixDB, error) {
	naFilePath, err := os.OpenFile(NAfilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("failed to open normal account file")
	}
	cafilePath, err := os.OpenFile(CAfilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("failed to open contract account file")
	}

	return &PrefixDB{
		root:                &TrieNode{children: make(map[byte]*TrieNode)},
		normalAccountFile:   naFilePath,
		contractAccountFile: cafilePath,
		cache:               make(map[string][]byte),
		cacheSize:           cacheSize,
		cacheFreq:           make(map[string]int),
		slotManager:         NewSlotManager(SLOT_NUM, SLOT_SIZE),
		batch:               NewWriteBatch(),
	}, nil
}

func (db *PrefixDB) Read(key []byte) ([]byte, error) {
	// Check in-memory cache
	db.cacheLock.RLock()
	if value, ok := db.cache[string(key)]; ok {
		db.cacheLock.RUnlock()
		db.incrementCacheFreq(string(key))
		return value, nil
	}
	db.cacheLock.RUnlock()
	// Check in batch
	if db.batch != nil {
		if value, ok := db.batch.get(key); ok {
			// db.incrementCacheFreq(string(key))
			return value, nil
		}
	}

	// search in the prefix tree
	node, err := db.findNode(key)
	if err != nil || node == nil || !node.isLeaf {
		return nil, errors.New("key not found")
	}

	switch node.accountType {
	// NormalAccount:
	case NormalAccount:
		value, err := db.readFromFile(node.offset, node.accountType)
		if err != nil {
			return nil, err
		}
		db.addToCache(string(key), value, true)
		return value, nil
	// ContractAccount:
	case ContractAccount:
		slotIndex := node.slotIndex
		appendOnlyKVPairs, err := db.loadSlot(slotIndex)
		if err != nil {
			return nil, err
		}
		// Add the all valid Pair to cache
		for k, v := range appendOnlyKVPairs {
			db.addToCache(k, v, false)
		}

		if value, exists := appendOnlyKVPairs[string(key)]; exists {
			go db.performGC(appendOnlyKVPairs, slotIndex)
			return value, nil
		}

	default:
		return nil, errors.New("key not found")
	}
	return nil, errors.New("key not found")
}

func (db *PrefixDB) Write(key, value []byte) error {
	// Determine account type
	accountType, err := db.getAccountType(key)
	if err != nil {
		return err
	}

	node, err := db.createNode(key)
	node.accountType = accountType
	if err != nil {
		return err
	}
	// Add the operation to the batch
	db.batch.add(key, value, accountType)
	// Add to cache
	db.addToCache(string(key), value, false)
	return nil
}

func (db *PrefixDB) Delete(key []byte) error {
	// search in the prefix tree
	node, err := db.findNode(key)
	if err != nil || node == nil || !node.isLeaf {
		return errors.New("key not found")
	}

	// delete
	node.isLeaf = false
	return db.writeToFile(node.offset, nil, nil, node.accountType)
}

func (db *PrefixDB) findNode(key []byte) (*TrieNode, error) {
	current := db.root
	for _, b := range key {
		if next, ok := current.children[b]; ok {
			current = next
		} else {
			return nil, nil
		}
	}
	return current, nil
}

func (db *PrefixDB) createNode(key []byte) (*TrieNode, error) {
	current := db.root
	for _, b := range key {
		if _, ok := current.children[b]; !ok {
			current.children[b] = &TrieNode{
				children:    make(map[byte]*TrieNode),
				slotIndex:   0,
				offset:      0,
				isLeaf:      true,
				accountType: NormalAccount,
				refCount:    0,
			}
		}
		current = current.children[b]
	}
	return current, nil
}

// func (db *PrefixDB) allocateOffset(accountType AccountType) int64 {
// 	// Allocate offset based on account type
// 	var file *os.File
// 	if accountType == ContractAccount {
// 		file = db.contractAccountFile
// 	} else {
// 		file = db.normalAccountFile
// 	}
// 	stat, _ := file.Stat()
// 	return stat.Size()
// }

func (db *PrefixDB) readFromFile(offset int64, accountType AccountType) ([]byte, error) {
	// Read from the appropriate file based on account type
	var file *os.File
	if accountType == ContractAccount {
		file = db.contractAccountFile
	} else {
		file = db.normalAccountFile
	}

	// Read the key-value format: <key_size (short) + value_size (short) + key + value>
	header := make([]byte, 4)
	_, err := file.ReadAt(header, offset)
	if err != nil {
		return nil, err
	}

	keySize := int(header[0])<<8 | int(header[1])
	valueSize := int(header[2])<<8 | int(header[3])

	data := make([]byte, keySize+valueSize+4)
	_, err = file.ReadAt(data, offset+4)
	if err != nil {
		return nil, err
	}

	return data[keySize:], nil
}

func (db *PrefixDB) writeToFile(offset int64, key, value []byte, accountType AccountType) error {
	// Write to the appropriate file based on account type
	var file *os.File
	if accountType == ContractAccount {
		file = db.contractAccountFile
	} else {
		file = db.normalAccountFile
	}

	formattedData, _ := db.ConvertKV(key, value)
	_, err := file.WriteAt(formattedData, offset)
	return err
}

// Write the normal account data to the file
func (db *PrefixDB) writeNAToFile(key, value []byte) error {

	file := db.normalAccountFile
	formattedData, _ := db.ConvertKV(key, value)
	offset, _ := file.Seek(0, io.SeekEnd)
	_, err := file.WriteAt(formattedData, offset)
	return err
}

func (db *PrefixDB) Close() error {
	// Close both files
	if err := db.normalAccountFile.Close(); err != nil {
		return err
	}
	if err := db.contractAccountFile.Close(); err != nil {
		return err
	}
	return nil
}

func (db *PrefixDB) SetCacheSize(newSize int) error {
	db.cacheLock.Lock()
	defer db.cacheLock.Unlock()

	switch {
	case newSize < 0 || newSize > MAX_CACHE_SIZE:
		return errors.New("cache size out of range")
	case newSize == db.cacheSize:
		return nil
	case newSize < db.cacheSize:
		// clear cache
		for i := 0; i < db.cacheSize-newSize; i++ {
			// find the least frequently used key
			lfuKey := db.findLFUKey()
			delete(db.cache, lfuKey)
			delete(db.cacheFreq, lfuKey)
		}
		db.cacheSize = newSize
		return nil
	case newSize > db.cacheSize:
		db.cacheSize = newSize
		return nil
	default:
		return errors.New("unexpected error")
	}
}

func (db *PrefixDB) getAccountType(key []byte) (AccountType, error) {
	// Check if the value is nil
	if key == nil {
		return NormalAccount, errors.New("value is nil")
	}

	// the prefix of key is "a" --normal account
	// the prefix of key is "c" --contract account
	switch key[0] {
	case 'a':
		return NormalAccount, nil
	case 'c':
		return ContractAccount, nil
	default:
		return NormalAccount, errors.New("unknown account type")
	}
}

func (db *PrefixDB) getSlotIndex(key []byte) int {
	// Get the slot index from the prefix tree
	node, err := db.findNode(key)
	if err != nil || node == nil {
		return -1
	}
	return node.slotIndex
}

func (db *PrefixDB) loadSlot(index int) (map[string][]byte, error) {
	// Load the slot from the contract account file
	offset := int64(index * SLOT_SIZE)
	buf := make([]byte, SLOT_SIZE)
	appendOnlyKVPairs := make(map[string][]byte)

	// Read the slot from the file
	_, err := db.contractAccountFile.ReadAt(buf, offset)
	if err != nil {
		return appendOnlyKVPairs, errors.New("loadSlot error") // Return empty maps on error
	}

	// Parse the append-only part
	data := buf[:APPENDONLY_SIZE]
	for len(data) > 0 {
		if data[0] == 0 && data[1] == 0 {
			break
		}
		// Read key size and value size
		keySize := int(data[0])<<8 | int(data[1])
		valueSize := int(data[2])<<8 | int(data[3])
		// Read key and value
		key := string(data[4 : 4+keySize])
		value := data[4+keySize : 4+keySize+valueSize]
		data = data[4+keySize+valueSize:]

		// Update appendOnlyKVPairs with the latest value for the key
		appendOnlyKVPairs[key] = value
	}

	return appendOnlyKVPairs, nil
}

func (db *PrefixDB) saveSlot(index int, slot *Slot) {
	offset := int64(index * SLOT_SIZE)
	buf := make([]byte, SLOT_SIZE)

	// Serialize appendOnlyPart map into the buffer
	data := buf[:APPENDONLY_SIZE]
	for key, value := range slot.appendOnlyPart {
		entry, _ := db.ConvertKV([]byte(key), value)
		if len(entry) > len(data) {
			break
		}
		copy(data, entry)
		data = data[len(entry):]
	}

	// Write the slot back to the file
	_, err := db.contractAccountFile.WriteAt(buf, offset)
	if err != nil {
		panic("failed to save slot: " + err.Error())
	}
	// clear the append-only part
	for k := range slot.appendOnlyPart {
		delete(slot.appendOnlyPart, k)
	}

}

func (db *PrefixDB) addToCache(key string, value []byte, loadPath bool) {
	db.cacheLock.Lock()
	defer db.cacheLock.Unlock()

	// If the key already exists, update the value and increment its frequency
	if _, exists := db.cache[key]; exists {
		db.cache[key] = value
		db.incrementCacheFreq(key)
		return
	}

	// If the cache is full, evict the least frequently used entry
	if len(db.cache) >= db.cacheSize {
		lfuKey := db.findLFUKey()
		db.evictFromCache(lfuKey)
	}

	// Add the new key-value pair to the cache
	db.cache[key] = value
	db.cacheFreq[key] = 1

	// Optionally cache the path to the node
	if loadPath {
		db.cachePathToNode(key)
	}
}

func (db *PrefixDB) cachePathToNode(key string) {
	currentKey := key
	for len(currentKey) > 0 {
		if _, exists := db.cache[currentKey]; !exists {
			node, err := db.findNode([]byte(currentKey))
			if err != nil || node == nil {
				panic("node not found")
			}
			db.cache[currentKey], err = db.readFromFile(node.offset+int64(SLOT_SIZE*node.slotIndex), node.accountType)
			if err != nil {
				panic("failed to read from file")
			}
			db.cacheFreq[currentKey] = 1
			node.refCount++
		}
		currentKey = currentKey[:len(currentKey)-1] // Reduce key length
	}
}

func (db *PrefixDB) evictFromCache(key string) {
	// Remove the key from the cache
	delete(db.cache, key)
	delete(db.cacheFreq, key)

	// Remove sibling nodes if they are not required by other nodes
	node, _ := db.findNode([]byte(key))
	if node != nil {
		for siblingKey, siblingNode := range node.children {
			if siblingNode != nil && siblingNode.isLeaf {
				if siblingNode.refCount <= 1 {
					delete(db.cache, string(siblingKey))
					delete(db.cacheFreq, string(siblingKey))
				} else {
					siblingNode.refCount--
				}
			}
		}
	}

	// Evict the path to the node if not used by other nodes
	db.evictPathToNode(key)
}

func (db *PrefixDB) evictPathToNode(key string) {
	currentKey := key
	for len(currentKey) > 0 {
		if parentNode, _ := db.findNode([]byte(currentKey)); parentNode != nil {
			if parentNode.refCount <= 1 {
				delete(db.cache, string(currentKey))
				delete(db.cacheFreq, string(currentKey))
			} else {
				parentNode.refCount--
			}
		}
		currentKey = currentKey[:len(currentKey)-1] // Reduce key length
	}
}

func (db *PrefixDB) incrementCacheFreq(key string) {
	// Increment the access frequency of the key
	if freq, exists := db.cacheFreq[key]; exists {
		db.cacheFreq[key] = freq + 1
	}

	// Increment reference counter for internal nodes
	node, _ := db.findNode([]byte(key))
	if node != nil {
		node.refCount++
	}
}

func (db *PrefixDB) findLFUKey() string {
	// Find the key with the lowest frequency
	var lfuKey string
	minFreq := int(^uint(0) >> 1) // Max int value
	for key, freq := range db.cacheFreq {
		if freq < minFreq {
			minFreq = freq
			lfuKey = key
		}
	}
	return lfuKey
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

func (db *PrefixDB) performGC(appendOnlyKVPairs map[string][]byte, oldSlotIndex int) {
	//  Write valid KV pairs to a new slot
	newSlotIndex := db.slotManager.getEmptySlot()
	if newSlotIndex == -1 {
		panic("No available slot")
	}

	newSlot := &Slot{
		appendOnlyPart: make(map[string][]byte),
	}

	newSlot.appendOnlyPart = appendOnlyKVPairs

	// Save the new slot
	db.saveSlot(newSlotIndex, newSlot)

	// Update the prefix tree
	offset := 0
	for k := range appendOnlyKVPairs {
		node, _ := db.findNode([]byte(k))
		if node != nil {
			node.offset = int64(offset)
			node.slotIndex = newSlotIndex
			offset += len(k) + len(appendOnlyKVPairs[k]) + 4
		}
	}

	// Free the old slot
	db.slotManager.lock.Lock()
	defer db.slotManager.lock.Unlock()
	db.slotManager.slotStatus[oldSlotIndex] = false
	db.slotManager.releaseSlot(oldSlotIndex, db.contractAccountFile)
}

func (db *PrefixDB) setOffset(key []byte, offset int64) {
	// Set the offset for the key in the prefix tree
	node, _ := db.findNode(key)
	if node != nil {
		node.offset = offset
	}
}

func (db *PrefixDB) setSlotIndex(key []byte, slotIndex int) {
	// Set the slot index for the key in the prefix tree
	node, _ := db.findNode(key)
	if node != nil {
		node.slotIndex = slotIndex
	}
}
