package prefixdb

import (
	"errors"
	"fmt"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

const MAX_CACHE_SIZE = 1024   // maximum cache size
const BUFFER_SIZE = 8192      // buffer size for file operations
const SLOT_SIZE = 1024 * 1024 // size of each slot
const SLOT_NUM = 1024

// number of slots
const APPENDONLY_SIZE = 512 * 1024 // size of append-only part in each slot

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

	// slot cache
	// slotCache      map[int]map[string][]byte // slotIndex -> (key -> value)
	// slotCacheLock  sync.RWMutex
	// slotCacheSize  int           // maximum number of slots in cache
	// slotAccessTime map[int]int64 // records the last access time of each slot
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
		root:        &TrieNode{children: make(map[byte]*TrieNode)},
		accountFile: naFilePath,
		slotFile:    cafilePath,
		nodeCache:   NewNodeCache(cacheSize),
		slotCache:   NewSlotCache(50), // 默认slot缓存大小为50
		slotManager: NewSlotManager(SLOT_NUM, SLOT_SIZE),
		batch:       NewWriteBatch(),
	}, nil
}

func (db *PrefixDB) Read(key []byte) ([]byte, error) {
	keyType, err := db.getKeyType(key)
	if err != nil {
		return nil, err
	}

	// 检查节点缓存 - O(1)时间复杂度
	if value, ok := db.nodeCache.Get(string(key)); ok {
		return value, nil
	}

	// 检查批处理缓存
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

		// 添加到缓存 - O(1)时间复杂度
		db.nodeCache.Put(string(key), value)
		// 缓存路径上的所有节点
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

		// 检查slot缓存 - O(1)时间复杂度
		if slotData, exists := db.slotCache.Get(slotIndex); exists {
			if value, ok := slotData[string(key)]; ok {
				return value, nil
			}
		} else {
			// 从文件加载slot
			slotData, err := db.loadSlot(slotIndex)
			if err != nil {
				return nil, err
			}

			// 添加到缓存 - O(1)时间复杂度
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

		node, err := db.createNode(key)
		if err != nil {
			return err
		}

		// if it's a new node,give it a slot index
		if db.isContractAccount(value) && (isNewNode || node.slotIndex <= 0) {
			slotIndex := db.slotManager.getEmptySlot()
			if slotIndex == -1 {
				return errors.New("no empty slot available")
			}
			node.slotIndex = slotIndex
		}

		db.batch.add(key, value, TrieAccount)

		// 添加到缓存 - O(1)时间复杂度
		db.nodeCache.Put(string(key), value)

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

		// 检查slot是否在缓存中 - O(1)时间复杂度
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

		db.batch.add(key, value, keyType)
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
	// Close both files
	if err := db.accountFile.Close(); err != nil {
		return err
	}
	if err := db.slotFile.Close(); err != nil {
		return err
	}
	return nil
}

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

func (db *PrefixDB) decodeAccountRLP(accountRLP []byte, account *StateAccount) error {
	// Decode the RLP encoded account data
	fields, err := rlp.Split(accountRLP)
	if err != nil {
		return err
	}

	// decode nonce
	if nonce, err := rlp.ParseUint64(fields[0]); err != nil {
		return fmt.Errorf("invalid nonce: %v", err)
	} else {
		account.Nonce = nonce
	}

	// decode balance
	if balance, err := rlp.ParseBig(fields[1]); err != nil {
		return fmt.Errorf("invalid balance: %v", err)
	} else {
		account.Balance = balance
	}

	// decode storage root
	if len(fields[2]) != common.HashLength+1 {
		return fmt.Errorf("invalid storage root length: %d", len(fields[2])-1)
	}
	copy(account.Root[:], fields[2][1:])

	// decode code hash
	if len(fields[3]) == common.HashLength+1 {
		account.CodeHash = make([]byte, common.HashLength)
		copy(account.CodeHash, fields[3][1:])
	} else {
		return fmt.Errorf("invalid code hash length: %d", len(fields[3])-1)
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

// 删除不再需要的方法
// cachePathToNode和incrementRefCount方法已移至NodeCache中

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

// 辅助方法：判断是否为合约账户
func (db *PrefixDB) isContractAccount(value []byte) bool {
	// 这里可以根据实际情况实现
	// 简单实现：检查value是否超过一定大小或包含特定字段
	return len(value) > 128 // 假设大于128字节的value可能是合约账户
}

// 辅助方法：获取键类型
func (db *PrefixDB) getKeyType(key []byte) (KeyType, error) {
	if key == nil || len(key) == 0 {
		return -1, errors.New("invalid key")
	}

	// 根据key的前缀判断类型
	prefix := key[0]
	switch prefix {
	case 'A': // 假设A开头的是TrieAccount
		return TrieAccount, nil
	case 'S': // 假设S开头的是TrieStorage
		return TrieStorage, nil
	case 'C': // 假设C开头的是TrieCode
		return TrieCode, nil
	default:
	}
	return -1, errors.New("unknown key type")
}

// 辅助方法：从Storage或Code的key中提取Account key
func (db *PrefixDB) getParentAccountKey(key []byte) []byte {
	// 假设key格式为：前缀(1字节) + 账户地址(20字节) + 其他数据
	if len(key) < 21 {
		return nil
	}

	// 提取账户地址并构造账户key
	accountAddr := key[1:21]
	return append([]byte{'A'}, accountAddr...)
}
