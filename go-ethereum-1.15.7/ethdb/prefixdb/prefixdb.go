package prefixdb

import (
	"bytes"
	"errors"
	"os"
	"sync"
)

const maxCacheSize = 1024         // maximum cache size
const BufferSize = 8192           // buffer size for file operations
const slotSize = 1024 * 1024      // size of each slot
const slotNum = 1024              // number of slots
const appendOnlySize = 512 * 1024 // size of append-only part in each slot

type accountType int

const (
	// Account types
	NormalAccount accountType = iota
	ContractAccount
)

type TrieNode struct {
	children    map[byte]*TrieNode // children nodes
	value       []byte             // value of the node
	offset      int64              // for normal account, offset in the file,for contract account, offset in the slot
	length      int                // length of the value
	isLeaf      bool               // is leaf node
	accountType accountType        // account type
}

type Slot struct {
	appendOnlyPart []byte            // append-only part
	accessedPart   map[string][]byte // sorted part
}

type PrefixDB struct {
	root                *TrieNode
	normalAccountFile   *os.File
	contractAccountFile *os.File
	cache               map[string][]byte // in-memory cache
	cacheLock           sync.RWMutex
	cacheSize           int            // fixed size of the cache
	cacheFreq           map[string]int // frequency of access for each key
	slotOffsets         []int64        // offsets for each slot
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

	// Initialize slot offsets
	slotOffsets := make([]int64, slotNum)
	for i := range slotOffsets {
		slotOffsets[i] = int64(i) * slotSize
	}

	return &PrefixDB{
		root:                &TrieNode{children: make(map[byte]*TrieNode)},
		normalAccountFile:   naFilePath,
		contractAccountFile: cafilePath,
		cache:               make(map[string][]byte),
		cacheSize:           cacheSize,
		cacheFreq:           make(map[string]int),
		slotOffsets:         slotOffsets,
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

	// search in the prefix tree
	node, err := db.findNode(key)
	if err != nil || node == nil || !node.isLeaf {
		return nil, errors.New("key not found")
	}

	switch node.accountType {
	case NormalAccount:
		value, err := db.readFromFile(node.offset, node.accountType, node.length)
		if err != nil {
			return nil, err
		}
		db.addToCache(string(key), value)
		return value, nil
	case ContractAccount:
		slotIndex := db.getSlotIndex(key)
		slot := db.loadSlot(slotIndex)

		if value, exists := slot.accessedPart[string(key)]; exists {
			// add all accessed part to cache
			for k, v := range slot.accessedPart {
				db.addToCache(k, v)
			}
			return value, nil
		}

		// read from append-only part
		value, err := db.readFromFile(int64(slotIndex*slotSize)+node.offset, node.accountType, node.length)
		if err != nil {
			return nil, err
		}
		db.addToCache(string(key), value)
		slot.accessedPart[string(key)] = value
		db.saveSlot(slotIndex, slot)
		return value, nil
	default:
		return nil, errors.New("key not found")
	}
}

func (db *PrefixDB) Write(key, value []byte) error {
	// determine account type
	accountType, err := db.getAccountType(key)
	if err != nil {
		return err
	}

	node, err := db.createNode(key)
	if err != nil {
		return err
	}
	switch accountType {
	case NormalAccount:
		// normal account
		offset := db.allocateOffset(accountType)
		if err := db.writeToFile(offset, key, value, accountType); err != nil {
			return err
		}
		node.offset = offset
		node.length = len(value)
	case ContractAccount:
		// smart contract account
		slotIndex := db.getSlotIndex(key)
		slot := db.loadSlot(slotIndex)

		// append-only part
		slot.appendOnlyPart = append(slot.appendOnlyPart, "key:\""+string(key)+"\"value:\""+string(value)+"\""...)
		node.offset = int64(slotIndex)*slotSize + int64(len(slot.appendOnlyPart))
		if len(slot.appendOnlyPart) > appendOnlySize {
			// if append-only part exceeds size
			//
		}
		node.length = len(value)

		if _, exists := slot.accessedPart[string(key)]; exists {
			slot.accessedPart[string(key)] = value
		}
		db.saveSlot(slotIndex, slot)
	}
	// Add the value to the cache
	db.addToCache(string(key), value)
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
				children: make(map[byte]*TrieNode),
			}
		}
		current = current.children[b]
	}
	return current, nil
}

func (db *PrefixDB) allocateOffset(accountType accountType) int64 {
	// Allocate offset based on account type
	var file *os.File
	if accountType == ContractAccount {
		file = db.contractAccountFile
	} else {
		file = db.normalAccountFile
	}
	stat, _ := file.Stat()
	return stat.Size()
}

func (db *PrefixDB) readFromFile(offset int64, accountType accountType, length int) ([]byte, error) {
	// Read from the appropriate file based on account type
	var file *os.File
	if accountType == ContractAccount {
		file = db.contractAccountFile
	} else {
		file = db.normalAccountFile
	}
	buf := make([]byte, length)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (db *PrefixDB) writeToFile(offset int64, key, value []byte, accountType accountType) error {
	// Write to the appropriate file based on account type
	var file *os.File
	if accountType == ContractAccount {
		file = db.contractAccountFile
	} else {
		file = db.normalAccountFile
	}

	// Construct the key-value format: key:"<key>"value:"<value>"
	formattedData := append([]byte("key:\""), key...)
	formattedData = append(formattedData, []byte("\"value:\"")...)
	formattedData = append(formattedData, value...)
	formattedData = append(formattedData, '"')

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
	case newSize < 0 || newSize > maxCacheSize:
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

func (db *PrefixDB) getAccountType(key []byte) (accountType, error) {
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
	// calculate the slot index based on the key
	if len(key) == 0 {
		return 0
	}
	return int(key[0]) % slotNum
}

func (db *PrefixDB) loadSlot(index int) *Slot {
	// load the slot from the contract account file
	offset := db.slotOffsets[index]
	buf := make([]byte, slotSize)

	_, err := db.contractAccountFile.ReadAt(buf, offset)
	if err != nil {
		return &Slot{
			appendOnlyPart: []byte{},
			accessedPart:   make(map[string][]byte),
		}
	}

	slot := &Slot{
		appendOnlyPart: buf[:appendOnlySize],
		accessedPart:   make(map[string][]byte),
	}

	sortedData := buf[appendOnlySize:]
	for len(sortedData) > 0 {
		// key value pair in file :"key:\"<key>\"value:\"<value>\""
		keyStart := bytes.Index(sortedData, []byte("key:\""))
		if keyStart == -1 {
			break
		}
		keyStart += len("key:\"")
		keyEnd := bytes.Index(sortedData[keyStart:], []byte("\""))
		if keyEnd == -1 {
			break
		}
		keyEnd += keyStart

		valueStart := bytes.Index(sortedData[keyEnd:], []byte("value:\""))
		if valueStart == -1 {
			break
		}
		valueStart += keyEnd + len("value:\"")
		valueEnd := bytes.Index(sortedData[valueStart:], []byte("\""))
		if valueEnd == -1 {
			break
		}
		valueEnd += valueStart

		key := string(sortedData[keyStart:keyEnd])
		value := sortedData[valueStart:valueEnd]
		slot.accessedPart[key] = value
		// Move to the next entry
		sortedData = sortedData[valueEnd+1:]
	}

	return slot
}

func (db *PrefixDB) saveSlot(index int, slot *Slot) {

	offset := db.slotOffsets[index]
	buf := make([]byte, slotSize)

	copy(buf[:appendOnlySize], slot.appendOnlyPart)

	sortedData := buf[appendOnlySize:]
	for key, value := range slot.accessedPart {
		entry := append([]byte("key:\""), key...)
		entry = append(entry, []byte("\"value:\"")...)
		entry = append(entry, value...)
		entry = append(entry, '"')

		if len(entry) > len(sortedData) {
			break
		}

		// write the entry to the buffer
		copy(sortedData, entry)
		sortedData = sortedData[len(entry):]
	}

	// Write the slot back to the file
	_, err := db.contractAccountFile.WriteAt(buf, offset)
	if err != nil {
		panic("failed to save slot: " + err.Error())
	}
	// delete the accessed part
	for key := range slot.accessedPart {
		delete(slot.accessedPart, key)
	}
	// clear the append-only part
	slot.appendOnlyPart = nil
}

func (db *PrefixDB) addToCache(key string, value []byte) {
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
		delete(db.cache, lfuKey)
		delete(db.cacheFreq, lfuKey)
	}

	// Add the new key-value pair to the cache
	db.cache[key] = value
	db.cacheFreq[key] = 1
}

func (db *PrefixDB) incrementCacheFreq(key string) {
	// Increment the access frequency of the key
	if freq, exists := db.cacheFreq[key]; exists {
		db.cacheFreq[key] = freq + 1
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
