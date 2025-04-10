package prefixdb

import (
	"bytes"
	"errors"
	"os"
	"sync"
)

const maxCacheDepth = 16 // 最大缓存深度
const BufferSize = 8192  // 缓冲区大小

type accountType int

const (
	// 账户类型
	NormalAccount accountType = iota
	ContractAccount
)

type TrieNode struct {
	children map[byte]*TrieNode // 子节点
	value    []byte             // 存储的值
	offset   int64              // 文件中的偏移量
	isLeaf   bool               // 是否为叶子节点
}

type PrefixDB struct {
	root       *TrieNode
	file       *os.File
	cache      map[string][]byte // 内存缓存
	cacheLock  sync.RWMutex
	cacheDepth int // 可缓存的深度

}

func NewPrefixDB(filePath string, cacheDepth int) (*PrefixDB, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &PrefixDB{
		root:       &TrieNode{children: make(map[byte]*TrieNode)},
		file:       file,
		cache:      make(map[string][]byte),
		cacheDepth: cacheDepth,
	}, nil
}

func (db *PrefixDB) Read(key []byte) ([]byte, error) {
	// Check in-memory cache
	db.cacheLock.RLock()
	if value, ok := db.cache[string(key)]; ok {
		db.cacheLock.RUnlock()
		return value, nil
	}
	db.cacheLock.RUnlock()

	// 遍历前缀树查找节点
	node, err := db.findNode(key)
	if err != nil || node == nil || !node.isLeaf {
		return nil, errors.New("key not found")
	}

	// 从文件中读取值
	return db.readFromFile(node.offset)
}

func (db *PrefixDB) Write(key, value []byte) error {
	// 查找前缀树节点
	node, err := db.findNode(key)
	if err == nil && node != nil && node.isLeaf {
		// 键已存在，更新文件中的值
		if err := db.writeToFile(node.offset, value); err != nil {
			return err
		}
		// 更新内存缓存
		if len(key) <= db.cacheDepth {
			db.cacheLock.Lock()
			db.cache[string(key)] = value
			db.cacheLock.Unlock()
		}
		return nil
	}

	// 遍历或创建前缀树节点
	node, err = db.createNode(key)
	if err != nil {
		return err
	}

	// 写入文件
	if node.offset == 0 {
		node.offset = db.allocateOffset()
	}
	if err := db.writeToFile(node.offset, value); err != nil {
		return err
	}
	node.isLeaf = true

	// 更新内存缓存
	if len(key) <= db.cacheDepth {
		db.cacheLock.Lock()
		db.cache[string(key)] = value
		db.cacheLock.Unlock()
	}
	return nil
}

func (db *PrefixDB) Delete(key []byte) error {
	// 查找前缀树节点
	node, err := db.findNode(key)
	if err != nil || node == nil || !node.isLeaf {
		return errors.New("key not found")
	}

	// 标记为删除
	node.isLeaf = false
	return db.writeToFile(node.offset, nil)
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

func (db *PrefixDB) allocateOffset() int64 {
	// 默认从文件末尾开始分配偏移量。待改进，如何找到文件中那些之前被删除的节点的位置
	stat, _ := db.file.Stat()
	return stat.Size()
}

func (db *PrefixDB) readFromFile(offset int64) ([]byte, error) {
	buf := make([]byte, BufferSize)
	_, err := db.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return bytes.Trim(buf, "\x00"), nil
}

func (db *PrefixDB) writeToFile(offset int64, value []byte) error {
	if value == nil {
		value = make([]byte, BufferSize) // 标记为已删除
	}
	_, err := db.file.WriteAt(value, offset)
	return err
}

func (db *PrefixDB) Close() error {
	return db.file.Close()
}

func (db *PrefixDB) SetCacheDepth(newDepth int) error {
	db.cacheLock.Lock()
	defer db.cacheLock.Unlock()

	switch {
	case newDepth < 0 || newDepth > maxCacheDepth:
		return errors.New("cache depth out of range")
	case newDepth == db.cacheDepth:
		return nil
	case newDepth < db.cacheDepth:
		// 清除缓存中超过新深度的key
		for k := range db.cache {
			if len(k) > newDepth {
				delete(db.cache, k)
			}
		}
		db.cacheDepth = newDepth
		return nil
	case newDepth > db.cacheDepth:
		// 扩展缓存深度并加载磁盘中满足条件的数据
		for key, node := range db.collectNodesWithinDepth(db.root, nil, newDepth) {
			if _, ok := db.cache[key]; !ok {
				value, err := db.readFromFile(node.offset)
				if err == nil {
					db.cache[key] = value
				}
			}
		}
		db.cacheDepth = newDepth
		return nil
	default:
		return errors.New("unexpected error")
	}
}

func (db *PrefixDB) collectNodesWithinDepth(node *TrieNode, prefix []byte, depth int) map[string]*TrieNode {
	if depth == 0 || node == nil {
		return nil
	}
	nodes := make(map[string]*TrieNode)
	if node.isLeaf {
		nodes[string(prefix)] = node
	}
	for key, child := range node.children {
		childNodes := db.collectNodesWithinDepth(child, append(prefix, key), depth-1)
		for k, v := range childNodes {
			nodes[k] = v
		}
	}
	return nodes
}
