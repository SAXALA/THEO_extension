package prefixdb

import (
	"bufio"
	"encoding/gob"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

const MAX_CACHE_SIZE = 1024   // maximum cache size
const BUFFER_SIZE = 8192      // buffer size for file operations
const SLOT_SIZE = 1024 * 1024 // size of each slot
const SLOT_NUM = 1024
const accountHashIndexFileName = "/mnt/tmp/index/accountHash_accountKey_index.txt"

// number of slots
const APPENDONLY_SIZE = 512 * 1024 // size of append-only part in each slot
const TRIE_FILE_NAME = "trie.dat"  // prefix tree file names

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

type TrieNode struct {
	children    map[byte]*TrieNode // children nodes
	value       []byte             // value of the node
	slotIndex   int                // slot index for contract account
	offset      int64              // for normal account, offset in the file,for contract account, offset in the slot
	isLeaf      bool               // is leaf node
	accountType AccountType        // account type
}

type PrefixDB struct {
	root        *TrieNode
	accountFile *os.File
	slotFile    *os.File
	nodeCache   *NodeCache
	slotCache   *SlotCache
	slotManager *SlotManager
	batch       *WriteBatch
	triePath    string // path to the prefix tree file
}

// SerializedTrieNode is used for serializing TrieNode to file.
type SerializedTrieNode struct {
	IsLeaf      bool
	AccountType AccountType
	SlotIndex   int
	Offset      int64
	Value       []byte
	Children    map[byte][]byte
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

	// get the path for the prefix tree file (same directory as normal account file)
	triePath := filepath.Join(filepath.Dir(NAfilePath), TRIE_FILE_NAME)

	db := &PrefixDB{
		root:        &TrieNode{children: make(map[byte]*TrieNode)},
		accountFile: naFilePath,
		slotFile:    cafilePath,
		nodeCache:   NewNodeCache(cacheSize),
		slotCache:   NewSlotCache(50), // default slot cache size
		slotManager: NewSlotManager(SLOT_NUM, SLOT_SIZE),
		batch:       NewWriteBatch(),
		triePath:    triePath,
	}

	// try to load the persisted prefix tree
	if err := db.LoadTrie(); err != nil {
		// if loading fails, use an empty prefix tree (already initialized in the constructor)
		fmt.Printf("unable to load prefix tree, using empty tree: %v\n", err)
	}

	return db, nil
}

func (db *PrefixDB) Read(key []byte) ([]byte, error) {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return nil, err
	}

	// check in cache
	if value, ok := db.nodeCache.Get(string(key)); ok {
		return value, nil
	}

	// check in batch
	if db.batch != nil {
		if value, ok := db.batch.get(key); ok {
			return value, nil
		}
	}

	switch keyType {
	case TrieAccount:
		node, err := db.findNode(key)
		if err != nil || node == nil || !node.isLeaf {
			return nil, errors.New("key not found")
		}

		value, err := db.readFromFile(node.offset, TrieAccount)
		if err != nil {
			return nil, err
		}

		// add to cache and cache path of the node
		db.nodeCache.Put(string(key), value)
		db.nodeCache.CachePathToNode(string(key), db)
		return value, nil

	case TrieStorage, TrieCode:
		accountKey := db.getParentAccountKey(key)
		if accountKey == nil {
			return nil, errors.New("parent account not found")
		}
		accountNode, err := db.findNode(accountKey)
		if err != nil || accountNode == nil || accountNode.slotIndex <= 0 {
			return nil, errors.New("account node not found or no slot allocated")
		}

		slotIndex := accountNode.slotIndex

		// check slot cache
		if slotData, exists := db.slotCache.Get(slotIndex); exists {
			if value, ok := slotData[string(key)]; ok {
				return value, nil
			}
		} else {
			// load slot
			slotData, err := db.loadSlot(slotIndex)
			if err != nil {
				return nil, err
			}

			// add to slot cache
			db.slotCache.Put(slotIndex, slotData)
			if value, exists := slotData[string(key)]; exists {
				return value, nil
			}
		}

	default:
		return nil, errors.New("unknown key type")
	}
	return nil, errors.New("key not found")
}

func (db *PrefixDB) Write(key, value []byte) error {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}

	switch keyType {
	case TrieAccount:
		existingNode, _ := db.findNode(key)
		isNewNode := existingNode == nil
		var node *TrieNode
		if isNewNode {
			var err error
			node, err = db.createNode(key)
			if err != nil {
				return err
			}
		} else {
			node = existingNode
		}

		// judge if the account is a contract account
		isContract := db.isContractAccount(value)

		// 如果是已有账户（不是新账户）并且是智能合约账户
		if !isNewNode && isContract && node.slotIndex > 0 {
			// 检查该账户的 slot 是否已经加载到内存中
			_, exists := db.slotCache.Get(node.slotIndex)
			if !exists {
				// 如果未加载到内存，则加载 slot
				slotData, err := db.loadSlot(node.slotIndex)
				if err == nil && slotData != nil {
					// 加载成功后放入 slotCache
					db.slotCache.Put(node.slotIndex, slotData)
				}
			} else {
				// 如果已经加载到内存中，直接更新 slotData
				slotData, _ := db.slotCache.Get(node.slotIndex)
				slotData[string(key)] = value
				db.slotCache.Put(node.slotIndex, slotData)
			}
		}

		// if the account is a contract account and has no slot allocated, allocate one
		if isContract && (isNewNode || node.slotIndex <= 0) {
			slotIndex := db.slotManager.getEmptySlot()
			if slotIndex == -1 {
				return errors.New("no empty slot available")
			}
			node.slotIndex = slotIndex

			// 将智能合约账户数据放入slotCache
			slotData := make(map[string][]byte)
			slotData[string(key)] = value
			db.slotCache.Put(slotIndex, slotData)
		} else if !isContract {
			// 普通账户放入nodeCache
			db.nodeCache.Put(string(key), value)
		}

		db.batch.add(key, value, TrieAccount, node.slotIndex)

	case TrieStorage, TrieCode:
		accountKey := db.getParentAccountKey(key)
		if accountKey == nil {
			return errors.New("parent account not found")
		}

		accountNode, err := db.findNode(accountKey)
		if err != nil || accountNode == nil || accountNode.slotIndex <= 0 {
			return errors.New("account node not found or no slot allocated")
		}

		slotIndex := accountNode.slotIndex

		// check if the slot is in cache
		if slotData, exists := db.slotCache.Get(slotIndex); exists {
			slotData[string(key)] = value
			db.slotCache.Put(slotIndex, slotData)
		} else {
			var slotData map[string][]byte
			var err error
			slotData, err = db.loadSlot(slotIndex)
			if err != nil {
				slotData = make(map[string][]byte)
			}
			slotData[string(key)] = value
			db.slotCache.Put(slotIndex, slotData)
		}

		db.batch.add(key, value, keyType, slotIndex)
	}

	return nil
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
			}
		}
		current = current.children[b]
	}
	return current, nil
}

func (db *PrefixDB) readFromFile(offset int64, keyType KeyType) ([]byte, error) {
	// Read from the appropriate file based on account type
	var file *os.File
	if keyType == TrieStorage || keyType == TrieCode {
		file = db.slotFile
	} else {
		file = db.accountFile
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

func (db *PrefixDB) writeToFile(offset int64, key, value []byte, keyType KeyType) error {
	// Write to the appropriate file based on account type
	var file *os.File
	if keyType == TrieStorage || keyType == TrieCode {
		file = db.slotFile
	} else {
		file = db.accountFile
	}

	formattedData, _ := db.ConvertKV(key, value)
	_, err := file.WriteAt(formattedData, offset)
	return err
}

func (db *PrefixDB) Close() error {
	// Save the prefix tree to file before closing
	if err := db.SaveTrie(); err != nil {
		return fmt.Errorf("failed to save prefix tree: %v", err)
	}

	// Close both files
	if err := db.accountFile.Close(); err != nil {
		return err
	}
	if err := db.slotFile.Close(); err != nil {
		return err
	}
	return nil
}

// SaveTrie saves the current prefix tree to a file.
func (db *PrefixDB) SaveTrie() error {
	file, err := os.Create(db.triePath)
	if err != nil {
		return fmt.Errorf("创建前缀树文件失败: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	// serialize and save the root node
	return db.saveNode(encoder, db.root, []byte{})
}

// saveNode recursively serializes a node and its children
func (db *PrefixDB) saveNode(encoder *gob.Encoder, node *TrieNode, path []byte) error {
	if node == nil {
		return nil
	}

	// create a data structure for serializing the node
	serialNode := SerializedTrieNode{
		IsLeaf:      node.isLeaf,
		AccountType: node.accountType,
		SlotIndex:   node.slotIndex,
		Offset:      node.offset,
		Value:       node.value,
		Children:    make(map[byte][]byte),
	}

	// record child node paths
	for b := range node.children {
		childPath := append(append([]byte{}, path...), b)
		serialNode.Children[b] = childPath
	}

	// write the current node
	if err := encoder.Encode(path); err != nil {
		return err
	}
	if err := encoder.Encode(serialNode); err != nil {
		return err
	}

	// recursively save all child nodes
	for b, child := range node.children {
		childPath := append(append([]byte{}, path...), b)
		if err := db.saveNode(encoder, child, childPath); err != nil {
			return err
		}
	}

	return nil
}

// LoadTrie loads the prefix tree from a file.
func (db *PrefixDB) LoadTrie() error {
	file, err := os.Open(db.triePath)
	if err != nil {
		return fmt.Errorf("failed to open the prefix tree file: %v", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	// reset the prefix tree
	db.root = &TrieNode{children: make(map[byte]*TrieNode)}

	// temporary storage for all nodes
	nodes := make(map[string]*TrieNode)
	nodes[""] = db.root // root node path is an empty string

	// continuously read until the end of the file
	for {
		var path []byte
		err := decoder.Decode(&path)
		if err != nil {
			// reaching the end of the file is expected
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		var serialNode SerializedTrieNode
		if err := decoder.Decode(&serialNode); err != nil {
			return err
		}

		// restore the node and its position in the tree
		pathStr := string(path)
		if pathStr == "" {
			// special handling for the root node
			db.root.isLeaf = serialNode.IsLeaf
			db.root.accountType = serialNode.AccountType
			db.root.slotIndex = serialNode.SlotIndex
			db.root.offset = serialNode.Offset
			db.root.value = serialNode.Value
		} else {
			// create or get the current node
			currentNode := db.createNodeForPath(path)
			currentNode.isLeaf = serialNode.IsLeaf
			currentNode.accountType = serialNode.AccountType
			currentNode.slotIndex = serialNode.SlotIndex
			currentNode.offset = serialNode.Offset
			currentNode.value = serialNode.Value
			nodes[pathStr] = currentNode
		}
	}

	return nil
}

// createNodeForPath ceates a node for the given path in the prefix tree.
func (db *PrefixDB) createNodeForPath(path []byte) *TrieNode {
	current := db.root
	for _, b := range path {
		if _, ok := current.children[b]; !ok {
			current.children[b] = &TrieNode{
				children: make(map[byte]*TrieNode),
			}
		}
		current = current.children[b]
	}
	return current
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

	// Split the content into fields
	// The expected format is: [nonce, balance, storageRoot, codeHash]
	var fields [][]byte
	rest := content

	for i := 0; i < 4; i++ {
		if len(rest) == 0 {
			return fmt.Errorf("not enough fields in RLP data, expected 4, got %d", i)
		}

		var fieldValue []byte

		_, fieldValue, rest, err = rlp.Split(content)
		if err != nil {
			return fmt.Errorf("failed to split field %d: %v", i, err)
		}

		fields = append(fields, fieldValue)
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

func (db *PrefixDB) getSlotIndex(key []byte) int {
	// Get the slot index from the prefix tree
	node, err := db.findNode(key)
	if err != nil || node == nil {
		return -1
	}
	return node.slotIndex
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

func (db *PrefixDB) loadSlot(index int) (map[string][]byte, error) {
	// Load the slot from the contract account file
	offset := int64(index * SLOT_SIZE)
	buf := make([]byte, SLOT_SIZE)
	appendOnlyKVPairs := make(map[string][]byte)

	// Read the slot from the file
	_, err := db.slotFile.ReadAt(buf, offset)
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
	_, err := db.slotFile.WriteAt(buf, offset)
	if err != nil {
		panic("failed to save slot: " + err.Error())
	}
	// clear the append-only part
	for k := range slot.appendOnlyPart {
		delete(slot.appendOnlyPart, k)
	}

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
	db.slotManager.releaseSlot(oldSlotIndex, db.slotFile)
}

func (db *PrefixDB) setOffset(key []byte, offset int64) {
	// Set the offset for the key in the prefix tree
	node, _ := db.findNode(key)
	if node != nil {
		node.offset = offset
	}
}

// isContractAccount checks
func (db *PrefixDB) isContractAccount(value []byte) bool {
	// RLP decode the value to check if it is a contract account
	if len(value) == 0 {
		return false
	}
	var rawNode []interface{}
	if err := rlp.DecodeBytes(value, &rawNode); err != nil {
		return false // decoding failed, not a contract account
	}
	// A contract account is identified by having a code hash or a non-empty code
	if len(rawNode) < 4 {
		return false // not enough fields to be a contract account
	}
	if codeHash, ok := rawNode[3].([]byte); ok && len(codeHash) == common.HashLength {
		return true // has a valid code hash
	}
	if code, ok := rawNode[2].([]byte); ok && len(code) > 0 {
		return true // has non-empty code
	}
	// Check if the value is too long to be a contract account
	if len(value) > 128 {
		return true // if the value is longer than 128 bytes, treat it as a contract
	}
	// Otherwise, it is not a contract account
	return false
}

// getKeyType determines the type of key based on its prefix.
func (db *PrefixDB) getKeyType(key []byte) (KeyType, error) {
	if len(key) == 0 {
		return -1, errors.New("invalid key")
	}

	prefix := key[0]
	switch prefix {
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
	accountHashHex := common.Bytes2Hex(accountHash)

	file, err := os.Open(accountHashIndexFileName)
	if err != nil {
		fmt.Println("failed to open account hash index file:", err)
		return nil
	}
	defer file.Close()

	// read the account hash index file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		parts := strings.Split(line, " Key: ")
		if len(parts) != 2 {
			continue
		}

		hashPart := parts[0]
		hashStart := strings.Index(hashPart, "\"")
		hashEnd := strings.LastIndex(hashPart, "\"")
		if hashStart == -1 || hashEnd == -1 || hashStart >= hashEnd {
			continue
		}

		fileAccountHashHex := hashPart[hashStart+1 : hashEnd]

		if strings.EqualFold(fileAccountHashHex, accountHashHex) {
			keyPart := parts[1]
			keyStart := strings.Index(keyPart, "\"")
			keyEnd := strings.LastIndex(keyPart, "\"")
			if keyStart == -1 || keyEnd == -1 || keyStart >= keyEnd {
				continue
			}

			accountKeyHex := keyPart[keyStart+1 : keyEnd]
			return common.Hex2Bytes(accountKeyHex)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("error reading account hash index file:", err)
	}

	return nil
}
