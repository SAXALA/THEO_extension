package prefixdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	lru "github.com/hashicorp/golang-lru"
)

const (
	MaxPrefixDepth = 6                                  // the maximum depth of the prefix tree
	NodeEntrySize  = 64                                 // the fixed size of each node entry in the file: 1 (key length) + 32 (key) + 4 (startSlotindex) + 4 (slotNum) + 8 (offset) + 15 (padding)
	FileNodeMagic  = 0x50544E46                         // "PTNF" - file node magic number
	MaxKeySize     = 32                                 // maximum key size in bytes
	FilterSize     = 16 * (MaxKeySize - MaxPrefixDepth) // bloom filter size
	TreeFileMagic  = 0x50545246                         // "PTRF" - prefix tree file magic number
	maxCacheFiles  = 1024
)

type NodeType byte

const (
	NormalNode NodeType = 0 // in-memory normal node
	FileNode   NodeType = 1 // file node
)

const (
	BloomFileMagic   = 0x42464C31
	BloomFileVersion = 1
)

const lockStripes = 4096

type stripedRWLocks struct {
	stripes [lockStripes]sync.RWMutex
}

func (s *stripedRWLocks) pick(key []byte) *sync.RWMutex {
	h := fnv.New32a()
	_, _ = h.Write(key)
	idx := h.Sum32() & (lockStripes - 1)
	return &s.stripes[idx]
}

// TrieNode
type TrieNode struct {
	nodeType NodeType           // node type
	children map[byte]*TrieNode // child nodes
	isLeaf   bool               // whether it's a leaf node

	// startSlotindex int   // the starting slot index
	// slotNum        int   // number of slots
	offset int64 // in the account file

	storageFileID uint32
	storageOffset int64
	storageSize   uint32

	fileID string // file name
}

// PrefixTree
type PrefixTree struct {
	root        *TrieNode // root
	lock        sync.RWMutex
	maxDepth    int
	db          *PrefixDB
	fileNodeDir string
	trieFile    string

	bucketPrefixLength int

	//for bloom filter
	filterLock  sync.RWMutex
	filterCache map[string]*bloom.BloomFilter
	bloomFile   string

	// for background merging
	mergeLock     sync.Mutex
	mergeStop     chan struct{}
	mergeWait     sync.WaitGroup
	mergeInterval time.Duration // merging interval

	fileCache *lru.Cache

	fileStripeLocks stripedRWLocks // striped locks for file operations
}

// FileNodeHeader  file node header structure
type FileNodeHeader struct {
	Magic              uint32 // file magic number
	Version            uint16 // file version
	SortedEntryCount   uint32
	UnsortedEntryCount uint32
	Reserved           [8]byte
}

type bloomsFileHeader struct {
	Magic    uint32
	Version  uint16
	Count    uint32
	Reserved [8]byte
}

type bloomEntryHeader struct {
	FileIDLen uint16
	DataLen   uint32
}

// NewPrefixTree
func NewPrefixTree(db *PrefixDB, dirPath string) (*PrefixTree, error) {
	fmt.Println("Initializing Prefix Tree...")
	fileNodeDir := filepath.Join(dirPath, "prefixdb", "filenodes")
	if err := os.MkdirAll(fileNodeDir, 0755); err != nil {
		return nil, fmt.Errorf("creat node file path failed: %w", err)
	}

	fileCache, err := lru.NewWithEvict(maxCacheFiles, func(key interface{}, value interface{}) {
		if file, ok := value.(*os.File); ok {
			file.Close()
		}
	})
	if err != nil {
		return nil, fmt.Errorf("create file cache failed: %w", err)
	}

	pt := &PrefixTree{
		root: &TrieNode{
			nodeType: NormalNode,
			children: make(map[byte]*TrieNode),
		},
		maxDepth:    MaxPrefixDepth,
		db:          db,
		fileNodeDir: fileNodeDir,
		// bucketPrefixLength: MaxPrefixDepth - 1,
		filterCache:   make(map[string]*bloom.BloomFilter),
		mergeStop:     make(chan struct{}),
		mergeInterval: 10 * time.Minute,
		fileCache:     fileCache,
		bloomFile:     filepath.Join(dirPath, "prefixdb", "filters.bf"),
	}
	pt.startMergeWorker()

	// load existing prefix tree file if exists
	pt.trieFile = filepath.Join(dirPath, "prefixdb", "trie")

	if _, err := os.Stat(pt.trieFile); err == nil {
		if err := pt.LoadFromFile(pt.trieFile); err != nil {
			fmt.Printf("Warning: Failed to load prefix tree from file: %v\n", err)
		}
	}
	pt.bucketPrefixLength = pt.maxDepth

	// load existing file node filters
	if err := pt.loadAllFiltersFromSingleFile(); err != nil {
		pt.loadAllFileNodeFilters()

		pt.saveAllFiltersToSingleFile()

	}
	fmt.Println("Prefix Tree initialized.")
	return pt, nil
}

func (pt *PrefixTree) getFileNodePath(prefix []byte) string {
	fileName := fmt.Sprintf("%x.node", prefix)
	return filepath.Join(pt.fileNodeDir, fileName)
}

// getBucketID returns the bucket ID for a given key
func (pt *PrefixTree) getBucketID(key []byte) string {
	if len(key) < pt.bucketPrefixLength {
		paddedKey := make([]byte, pt.bucketPrefixLength)
		copy(paddedKey, key)
		return fmt.Sprintf("bucket_%x.node", paddedKey[:pt.bucketPrefixLength])
	}

	// use the first bucketPrefixLength bytes as the bucket ID
	return fmt.Sprintf("bucket_%x.node", key[:pt.bucketPrefixLength])
}

// encodeNodeEntry encode node information into a fixed-size entry
// [0]        : keyLen (1B)
// [1..32]    : key (max 32B，padded with zeros if shorter)
// [33..40]   : accountOffset (8B)
// [41..44]   : storageFileID (4B)
// [45..52]   : storageOffset (8B)
// [53..56]	  : storageSize (4B)
// [53..63]   : eserved
func encodeNodeEntry(key []byte, accountOffset int64, storageFileID uint32, storageOffset int64, storageSize uint32) []byte {
	entry := make([]byte, NodeEntrySize)

	keyLen := len(key)
	if keyLen > MaxKeySize {
		keyLen = MaxKeySize
	}
	entry[0] = byte(keyLen)

	copy(entry[1:1+keyLen], key)
	// account offset
	binary.BigEndian.PutUint64(entry[33:41], uint64(accountOffset))
	// storage file id
	binary.BigEndian.PutUint32(entry[41:45], storageFileID)
	// storage offset
	binary.BigEndian.PutUint64(entry[45:53], uint64(storageOffset))
	// storage size
	binary.BigEndian.PutUint32(entry[53:57], storageSize)

	return entry
}

type nodeEntryDecoded struct {
	key           []byte
	accountOffset int64
	storageFileID uint32
	storageOffset int64
	storageSize   uint32
}

func decodeNodeEntry(entry []byte) nodeEntryDecoded {
	res := nodeEntryDecoded{}
	if len(entry) < NodeEntrySize {
		return res
	}
	keyLen := int(entry[0])
	if keyLen > MaxKeySize {
		keyLen = MaxKeySize
	}
	res.key = make([]byte, keyLen)
	copy(res.key, entry[1:1+keyLen])

	res.accountOffset = int64(binary.BigEndian.Uint64(entry[33:41]))
	res.storageFileID = binary.BigEndian.Uint32(entry[41:45])
	res.storageOffset = int64(binary.BigEndian.Uint64(entry[45:53]))
	res.storageSize = binary.BigEndian.Uint32(entry[53:57])
	return res
}

func (pt *PrefixTree) Get(key []byte) (accountOffset int64, storageFileID uint32, storageOffset int64, storageSize uint32, found bool, err error) {
	pt.lock.RLock()
	defer pt.lock.RUnlock()
	if len(key) == 0 {
		return 0, 0, 0, 0, false, errors.New("key cannot be empty")
	}
	currentNode := pt.root
	depth := 0
	for depth < len(key) && depth < pt.maxDepth {
		if currentNode.nodeType == FileNode {
			return pt.getFromFileNode(currentNode.fileID, key[depth:])
		}
		nextNode, exists := currentNode.children[key[depth]]
		if !exists {
			return 0, 0, 0, 0, false, nil
		}
		currentNode = nextNode
		depth++
	}
	if len(key) == pt.maxDepth || depth == len(key) {
		if currentNode.isLeaf {
			return currentNode.offset, currentNode.storageFileID, currentNode.storageOffset, currentNode.storageSize, true, nil
		}
		return 0, 0, 0, 0, false, nil
	}
	if depth == pt.maxDepth && currentNode.nodeType == FileNode {
		return pt.getFromFileNode(currentNode.fileID, key)
	}
	return 0, 0, 0, 0, false, nil
}

// Put inserts or updates a key in the prefix tree
func (pt *PrefixTree) Put(key []byte, accountOffset int64, storageFileID uint32, storageOffset int64, storageSize uint32) error {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	currentNode := pt.root
	depth := 0
	for depth < len(key) && depth < pt.maxDepth {
		if currentNode.nodeType == FileNode {
			return pt.putIntoFileNode(currentNode.fileID, key[depth:], accountOffset, storageFileID, storageOffset, storageSize)
		}
		if _, exists := currentNode.children[key[depth]]; !exists {
			currentNode.children[key[depth]] = &TrieNode{
				nodeType:      NormalNode,
				storageFileID: 0,
				storageOffset: 0,
				storageSize:   0,
				children:      make(map[byte]*TrieNode),
			}
		}
		currentNode = currentNode.children[key[depth]]
		depth++
	}
	if depth == pt.maxDepth {
		currentNode.nodeType = FileNode
		prefix := key[:pt.maxDepth]
		currentNode.fileID = pt.getBucketID(prefix)
		if len(key) == pt.maxDepth {
			currentNode.isLeaf = true
			currentNode.offset = accountOffset
			if storageFileID != 0 || storageOffset != 0 || storageSize != 0 {
				currentNode.storageFileID = storageFileID
				currentNode.storageOffset = storageOffset
				currentNode.storageSize = storageSize
			}
		}
		return pt.putIntoFileNode(currentNode.fileID, key, accountOffset, storageFileID, storageOffset, storageSize)
	}
	currentNode.isLeaf = true
	currentNode.offset = accountOffset
	if storageFileID != 0 || storageOffset != 0 || storageSize != 0 {
		currentNode.storageFileID = storageFileID
		currentNode.storageOffset = storageOffset
		currentNode.storageSize = storageSize
	}
	return nil
}

func (pt *PrefixTree) putIntoFileNode(fileID string, key []byte, accountOffset int64, storageFileID uint32, storageOffset int64, storageSize uint32) error {
	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.Lock()
	defer fl.Unlock()

	filePath := filepath.Join(pt.fileNodeDir, fileID)
	filter := pt.getOrCreateFilter(fileID)
	if filter != nil {
		filter.Add(key)
		pt.filterLock.Lock()
		pt.filterCache[fileID] = filter
		pt.filterLock.Unlock()
	}
	var file *os.File
	var header FileNodeHeader
	var err error

	if _, err = os.Stat(filePath); os.IsNotExist(err) {
		file, err = pt.getOrCreateFileHandle(fileID, os.O_RDWR|os.O_CREATE)
		if err != nil {
			return fmt.Errorf("create file failed: %w", err)
		}
		header = FileNodeHeader{Magic: FileNodeMagic, Version: 2}
		if err := binary.Write(file, binary.BigEndian, &header); err != nil {
			file.Close()
			return fmt.Errorf("write header failed: %w", err)
		}
	} else {
		file, err = pt.getOrCreateFileHandle(fileID, os.O_RDWR)
		if err != nil {
			return fmt.Errorf("open file failed: %w", err)
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			file.Close()
			return fmt.Errorf("seek failed: %w", err)
		}
		if err := binary.Read(file, binary.BigEndian, &header); err != nil {
			file.Close()
			return fmt.Errorf("read header failed: %w", err)
		}
		if header.Magic != FileNodeMagic {
			file.Close()
			return errors.New("invalid file node magic")
		}
	}

	writeOffset := int64(binary.Size(header)) + int64(header.SortedEntryCount+header.UnsortedEntryCount)*NodeEntrySize
	entryData := encodeNodeEntry(key, accountOffset, storageFileID, storageOffset, storageSize)
	if _, err := file.WriteAt(entryData, writeOffset); err != nil {
		return fmt.Errorf("write entry failed: %w", err)
	}
	header.UnsortedEntryCount++
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek start failed: %w", err)
	}
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("update header failed: %w", err)
	}
	return nil
}

// Delete deletes a key from the prefix tree
func (pt *PrefixTree) Delete(key []byte) (bool, error) {
	pt.lock.Lock()
	defer pt.lock.Unlock()

	if len(key) == 0 {
		return false, errors.New("key cannot be empty")
	}

	currentNode := pt.root
	depth := 0
	path := make([]*TrieNode, 0)
	pathBytes := make([]byte, 0)

	for depth < len(key) && depth < pt.maxDepth {
		path = append(path, currentNode)
		pathBytes = append(pathBytes, key[depth])

		if currentNode.nodeType == FileNode {
			return pt.deleteFromFileNode(currentNode.fileID, key)
		}

		nextNode, exists := currentNode.children[key[depth]]
		if !exists {
			return false, nil
		}

		currentNode = nextNode
		depth++
	}
	if depth == pt.maxDepth && currentNode.nodeType == FileNode {
		return pt.deleteFromFileNode(currentNode.fileID, key[depth:])
	}

	if currentNode.isLeaf && currentNode.nodeType == NormalNode {
		if depth == len(key) {

			currentNode.isLeaf = false
			currentNode.storageFileID = 0
			currentNode.storageOffset = 0
			currentNode.offset = 0

			for i := len(path) - 1; i >= 0; i-- {
				parentNode := path[i]
				childByte := pathBytes[i]

				childNode := parentNode.children[childByte]
				if !childNode.isLeaf && len(childNode.children) == 0 {
					delete(parentNode.children, childByte)
				} else {
					break
				}
			}

			return true, nil
		}
	}
	return false, nil
}

// deleteFromFileNode deletes a key from a file node
func (pt *PrefixTree) deleteFromFileNode(fileID string, key []byte) (bool, error) {
	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.Lock()
	defer fl.Unlock()

	file, err := pt.getOrCreateFileHandle(fileID, os.O_RDWR)
	if err != nil {
		return false, fmt.Errorf("open node file failed: %w", err)
	}
	var header FileNodeHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return false, fmt.Errorf("read file header failed: %w", err)
	}
	if header.Magic != FileNodeMagic {
		return false, errors.New("invalid file node magic number")
	}
	total := header.SortedEntryCount + header.UnsortedEntryCount
	entries := make([]nodeEntryDecoded, 0, total)
	found := false
	for i := uint32(0); i < total; i++ {
		entryData := make([]byte, NodeEntrySize)
		if _, err := file.ReadAt(entryData, int64(binary.Size(header))+int64(i)*NodeEntrySize); err != nil {
			return false, fmt.Errorf("read entry failed : %w", err)
		}
		dec := decodeNodeEntry(entryData)
		if !bytes.Equal(dec.key, key) {
			entries = append(entries, dec)
		} else {
			found = true
		}
	}
	if !found {
		return false, nil
	}
	// rebuild filter, sort
	filter := bloom.NewWithEstimates(FilterSize, 0.05)
	for _, e := range entries {
		filter.Add(e.key)
	}
	pt.filterLock.Lock()
	pt.filterCache[fileID] = filter
	pt.filterLock.Unlock()

	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("reset file pointer failed: %w", err)
	}
	header.SortedEntryCount = uint32(len(entries))
	header.UnsortedEntryCount = 0
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return false, fmt.Errorf("write file header failed : %w", err)
	}
	for _, e := range entries {
		entryData := encodeNodeEntry(e.key, e.accountOffset, e.storageFileID, e.storageOffset, e.storageSize)
		if _, err := file.Write(entryData); err != nil {
			return false, fmt.Errorf("failed to write entry: %w", err)
		}
	}
	newSize := int64(binary.Size(header)) + int64(len(entries))*NodeEntrySize
	if err := file.Truncate(newSize); err != nil {
		return false, fmt.Errorf("fail to Truncate file : %w", err)
	}
	return true, nil
}

// StartMergeWorker starts the background merge worker
func (pt *PrefixTree) startMergeWorker() {
	pt.mergeWait.Add(1)
	go func() {
		defer pt.mergeWait.Done()
		ticker := time.NewTicker(pt.mergeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				pt.mergeAllFileNodes()
			case <-pt.mergeStop:
				return
			}
		}
	}()
}

// Close closes the prefix tree and stops the merge worker
func (pt *PrefixTree) Close() error {
	select {
	case <-pt.mergeStop:
		// already closed
	default:
		close(pt.mergeStop)
	}
	pt.mergeWait.Wait()

	pt.ForceMerge()

	if pt.fileCache != nil {
		pt.fileCache.Purge()
	}

	if err := pt.saveAllFiltersToSingleFile(); err != nil {
		fmt.Printf("Warning: Failed to save bloom filters: %v\n", err)
	}

	if err := pt.SaveToFile(pt.trieFile); err != nil {
		fmt.Printf("Warning: Failed to save prefix tree to file: %v\n", err)
	}

	pt.filterLock.Lock()
	pt.filterCache = nil
	pt.filterLock.Unlock()

	return nil
}

func (pt *PrefixTree) getFromFileNode(fileID string, remainingKey []byte) (accountOffset int64, storageFileID uint32, storageOffset int64, storageSize uint32, found bool, err error) {
	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.RLock()
	defer fl.RUnlock()

	filter := pt.getOrCreateFilter(fileID)
	pt.filterLock.RLock()
	if filter != nil && !filter.Test(remainingKey) {
		pt.filterLock.RUnlock()
		return 0, 0, 0, 0, false, nil
	}
	pt.filterLock.RUnlock()

	file, err := pt.getOrCreateFileHandle(fileID, os.O_RDWR)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, 0, 0, false, nil
		}
		return 0, 0, 0, 0, false, fmt.Errorf("open file failed: %w", err)
	}
	// file.Seek(0, io.SeekStart)

	var header FileNodeHeader
	headerSize := int64(binary.Size(header))
	hdrBuf := make([]byte, headerSize)
	if _, err := file.ReadAt(hdrBuf, 0); err != nil {
		return 0, 0, 0, 0, false, fmt.Errorf("read header failed: %w", err)
	}
	if err := binary.Read(bytes.NewReader(hdrBuf), binary.BigEndian, &header); err != nil {
		return 0, 0, 0, 0, false, fmt.Errorf("decode header failed: %w", err)
	}
	if header.Magic != FileNodeMagic {
		return 0, 0, 0, 0, false, fmt.Errorf("invalid file node magic (got 0x%X, file=%s)", header.Magic, fileID)
	}
	isNonZero := func(fid uint32, off int64) bool { return fid != 0 && off != 0 }

	var zeroHit *nodeEntryDecoded
	if header.UnsortedEntryCount > 0 {
		unsortedBase := int64(binary.Size(header)) + int64(header.SortedEntryCount)*NodeEntrySize
		totalSize := int64(header.UnsortedEntryCount) * NodeEntrySize
		buf := make([]byte, totalSize)
		if _, err := file.ReadAt(buf, unsortedBase); err != nil && err != io.EOF {
			return 0, 0, 0, 0, false, fmt.Errorf("read unsorted bulk failed: %w", err)
		}
		for i := uint32(0); i < header.UnsortedEntryCount; i++ {
			idx := header.UnsortedEntryCount - 1 - i
			offsetInBuf := int64(idx) * NodeEntrySize
			if offsetInBuf+NodeEntrySize > int64(len(buf)) {
				break
			}
			dec := decodeNodeEntry(buf[offsetInBuf : offsetInBuf+NodeEntrySize])
			if bytes.Equal(dec.key, remainingKey) {
				if isNonZero(dec.storageFileID, dec.storageOffset) {
					if zeroHit == nil {
						return dec.accountOffset, dec.storageFileID, dec.storageOffset, dec.storageSize, true, nil
					}
					return zeroHit.accountOffset, dec.storageFileID, dec.storageOffset, dec.storageSize, true, nil
				}
				if zeroHit == nil {
					tmp := dec
					zeroHit = &tmp
				}
			}
		}
	}
	// if zeroHit != nil {
	// 	return zeroHit.accountOffset, zeroHit.storageFileID, zeroHit.storageOffset, zeroHit.storageSize, true, nil
	// }

	if header.SortedEntryCount > 0 {
		sortedBase := int64(binary.Size(header))
		sortedSize := int64(header.SortedEntryCount) * NodeEntrySize
		sortedBuf := make([]byte, sortedSize)
		if _, err := file.ReadAt(sortedBuf, sortedBase); err != nil && err != io.EOF {
			return 0, 0, 0, 0, false, fmt.Errorf("read sorted bulk failed: %w", err)
		}

		getKeyAt := func(idx uint32) []byte {
			start := int64(idx) * NodeEntrySize
			keyLen := int(sortedBuf[start])
			if keyLen > MaxKeySize {
				keyLen = MaxKeySize
			}
			return sortedBuf[start+1 : start+1+int64(keyLen)]
		}

		low, high := uint32(0), header.SortedEntryCount-1
		for low <= high {
			mid := (low + high) / 2
			k := getKeyAt(mid)
			cmp := bytes.Compare(k, remainingKey)
			if cmp == 0 {

				start := int64(mid) * NodeEntrySize
				dec := decodeNodeEntry(sortedBuf[start : start+NodeEntrySize])
				if isNonZero(dec.storageFileID, dec.storageOffset) {
					return dec.accountOffset, dec.storageFileID, dec.storageOffset, dec.storageSize, true, nil
				}
				if zeroHit != nil {
					return zeroHit.accountOffset, zeroHit.storageFileID, zeroHit.storageOffset, zeroHit.storageSize, true, nil
				}
				return dec.accountOffset, dec.storageFileID, dec.storageOffset, dec.storageSize, true, nil
			} else if cmp < 0 {
				low = mid + 1
			} else {
				if mid == 0 {
					break
				}
				high = mid - 1
			}
		}
	}
	if zeroHit != nil {
		return zeroHit.accountOffset, zeroHit.storageFileID, zeroHit.storageOffset, zeroHit.storageSize, true, nil
	}
	return 0, 0, 0, 0, false, nil
}

// MergeAllFileNodes merges all file nodes in the directory
func (pt *PrefixTree) mergeAllFileNodes() {
	pt.mergeLock.Lock()
	defer pt.mergeLock.Unlock()

	files, err := os.ReadDir(pt.fileNodeDir)
	if err != nil {
		fmt.Printf("Error reading file node directory: %v\n", err)
		return
	}

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".node" {
			filePath := filepath.Join(pt.fileNodeDir, file.Name())
			if err := pt.mergeFileNode(filePath); err != nil {
				fmt.Printf("Error merging file node %s: %v\n", filePath, err)
			}
		}
	}
}

func (pt *PrefixTree) mergeFileNode(filePath string) error {
	fileID := filepath.Base(filePath)
	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.Lock()
	defer fl.Unlock()

	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open file failed: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return err
	}
	var header FileNodeHeader
	if err := binary.Read(f, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("read file header failed: %w", err)
	}
	if header.Magic != FileNodeMagic || header.UnsortedEntryCount == 0 {
		return nil
	}

	hdrSize := int64(binary.Size(header))
	total := header.SortedEntryCount + header.UnsortedEntryCount
	totalBytes := int64(total) * NodeEntrySize
	buf := make([]byte, totalBytes)
	if _, err := f.ReadAt(buf, hdrSize); err != nil && err != io.EOF {
		return fmt.Errorf("bulk read failed: %w", err)
	}

	m := make(map[string]nodeEntryDecoded, total)
	isNonZero := func(e nodeEntryDecoded) bool { return e.storageFileID != 0 || e.storageOffset != 0 }

	// sorted part
	sortedBytes := int64(header.SortedEntryCount) * NodeEntrySize
	sortedSlice := buf[:sortedBytes]
	for i := uint32(0); i < header.SortedEntryCount; i++ {
		start := int64(i) * NodeEntrySize
		dec := decodeNodeEntry(sortedSlice[start : start+NodeEntrySize])
		if len(dec.key) > 0 {
			m[string(dec.key)] = dec
		}
	}
	// unsorted part
	unsortedSlice := buf[sortedBytes:]
	for i := uint32(0); i < header.UnsortedEntryCount; i++ {
		start := int64(i) * NodeEntrySize
		dec := decodeNodeEntry(unsortedSlice[start : start+NodeEntrySize])
		if len(dec.key) == 0 {
			continue
		}
		k := string(dec.key)
		if old, ok := m[k]; ok && isNonZero(old) && !isNonZero(dec) {
			dec.storageFileID = old.storageFileID
			dec.storageOffset = old.storageOffset
			dec.storageSize = old.storageSize
		}
		m[k] = dec
	}

	entries := make([]nodeEntryDecoded, 0, len(m))
	filter := bloom.NewWithEstimates(uint(FilterSize), 0.05)
	for _, e := range m {
		entries = append(entries, e)
		filter.Add(e.key)
	}

	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })

	tmp := filePath + ".tmp"
	newHdr := FileNodeHeader{
		Magic:            FileNodeMagic,
		Version:          2,
		SortedEntryCount: uint32(len(entries)),
	}
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create tmp file failed: %w", err)
	}
	if err := binary.Write(tf, binary.BigEndian, &newHdr); err != nil {
		tf.Close()
		return fmt.Errorf("write tmp header failed: %w", err)
	}
	for _, e := range entries {
		if _, err := tf.Write(encodeNodeEntry(e.key, e.accountOffset, e.storageFileID, e.storageOffset, e.storageSize)); err != nil {
			tf.Close()
			return fmt.Errorf("write tmp entry failed: %w", err)
		}
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return fmt.Errorf("fsync tmp file failed: %w", err)
	}
	if err := tf.Close(); err != nil {
		return fmt.Errorf("close tmp file failed: %w", err)
	}
	if err := os.Rename(tmp, filePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename failed: %w", err)
	}
	if dirf, err := os.Open(filepath.Dir(filePath)); err == nil {
		_ = dirf.Sync()
		_ = dirf.Close()
	}

	pt.filterLock.Lock()
	pt.filterCache[fileID] = filter
	pt.filterLock.Unlock()
	pt.dropFileHandles(fileID)
	return nil
}

func (pt *PrefixTree) dropFileHandles(fileID string) {
	if pt.fileCache == nil {
		return
	}
	flags := []int{os.O_RDONLY, os.O_RDWR, os.O_RDWR | os.O_CREATE}
	for _, flag := range flags {
		key := fmt.Sprintf("%s|%d", fileID, flag)
		if v, ok := pt.fileCache.Get(key); ok {
			if f, _ := v.(*os.File); f != nil {
				_ = f.Close()
			}
			pt.fileCache.Remove(key)
		}
	}
}

// ForceMerge
func (pt *PrefixTree) ForceMerge() error {
	pt.mergeAllFileNodes()
	return nil
}

// SetMergeInterval
func (pt *PrefixTree) SetMergeInterval(interval time.Duration) {
	pt.mergeInterval = interval
}

// loadAllFileNodeFilters loads bloom filters for all existing file nodes
func (pt *PrefixTree) loadAllFileNodeFilters() {
	files, err := os.ReadDir(pt.fileNodeDir)
	if err != nil {
		fmt.Printf("Error reading file node directory: %v\n", err)
		return
	}

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".node" {
			fileID := file.Name()
			pt.getOrCreateFilter(fileID)
		}
	}
}

// buildFilterFromFile builds a bloom filter from the keys in the specified file
func (pt *PrefixTree) getOrCreateFilter(fileID string) *bloom.BloomFilter {
	pt.filterLock.RLock()
	filter, exists := pt.filterCache[fileID]
	pt.filterLock.RUnlock()

	if exists {
		return filter
	}

	pt.filterLock.Lock()
	defer pt.filterLock.Unlock()

	// check again after acquiring write lock
	filter, exists = pt.filterCache[fileID]
	if exists {
		return filter
	}

	// load from file
	filePath := filepath.Join(pt.fileNodeDir, fileID)
	filter = pt.buildFilterFromFile(filePath)

	// cache it
	pt.filterCache[fileID] = filter
	return filter
}

func (pt *PrefixTree) buildFilterFromFile(filePath string) *bloom.BloomFilter {
	file, err := os.Open(filePath)
	if err != nil {
		// fmt.Printf("Error opening file %s: %v\n", filePath, err)
		return bloom.NewWithEstimates(uint(FilterSize), 0.05)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("Error stating file %s: %v\n", filePath, err)
		return bloom.NewWithEstimates(uint(FilterSize), 0.05)
	}
	if fileInfo.Size() == 0 {
		// fmt.Printf("File %s is empty\n", filePath)
		return bloom.NewWithEstimates(uint(FilterSize), 0.05)
	}

	header := FileNodeHeader{}
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		fmt.Printf("Error reading header from file %s: %v\n", filePath, err)
		return bloom.NewWithEstimates(uint(FilterSize), 0.05)
	}

	if header.Magic != FileNodeMagic {
		fmt.Printf("Invalid magic number in file %s\n", filePath)
		return bloom.NewWithEstimates(uint(FilterSize), 0.05)
	}

	// count total entries
	total := header.SortedEntryCount + header.UnsortedEntryCount
	if total == 0 {
		return bloom.NewWithEstimates(uint(FilterSize), 0.05)
	}
	filter := bloom.NewWithEstimates(uint(total), 0.05)

	// read entries
	headerSize := int64(binary.Size(header))
	for i := uint32(0); i < header.SortedEntryCount; i++ {
		entryData := make([]byte, NodeEntrySize)
		offset := headerSize + int64(i)*NodeEntrySize
		if _, err := file.ReadAt(entryData, offset); err != nil {
			break
		}
		dec := decodeNodeEntry(entryData)
		if len(dec.key) > 0 {
			filter.Add(dec.key)
		}
	}
	for i := uint32(0); i < header.UnsortedEntryCount; i++ {
		entryData := make([]byte, NodeEntrySize)
		offset := headerSize + int64(header.SortedEntryCount+i)*NodeEntrySize
		if _, err := file.ReadAt(entryData, offset); err != nil {
			break
		}
		dec := decodeNodeEntry(entryData)
		if len(dec.key) > 0 {
			filter.Add(dec.key)
		}
	}

	return filter
}

// rebuildFilter rebuilds the bloom filter for a specific file node
func (pt *PrefixTree) RebuildFilter(fileID string) error {
	filePath := filepath.Join(pt.fileNodeDir, fileID)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("file node does not exist: %s", fileID)
	}

	filter := pt.buildFilterFromFile(filePath)

	pt.filterLock.Lock()
	pt.filterCache[fileID] = filter
	pt.filterLock.Unlock()

	return nil
}

// TreeFileHeader tree file header structure
type TreeFileHeader struct {
	Magic     uint32 // file magic number
	Version   uint16 // file version
	NodeCount uint32 // number of nodes
	MaxDepth  uint16 // maximum depth of the tree
	Reserved  [8]byte
}

// NodeRecord tree node record structure
type NodeRecord struct {
	NodeType      byte
	IsLeaf        byte
	ChildCount    uint16
	Offset        int64
	StorageFileID uint32
	StorageOffset int64
	StorageSize   uint64
	FileIDLength  byte
	FileID        [255]byte
}

// SaveToFile saves the prefix tree to a file
func (pt *PrefixTree) SaveToFile(filePath string) error {
	pt.lock.RLock()
	defer pt.lock.RUnlock()

	// 确保目录存在
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建目录失败: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer file.Close()

	// 写入文件头
	header := TreeFileHeader{
		Magic:     TreeFileMagic,
		Version:   1,
		NodeCount: 0, // 稍后更新
		MaxDepth:  uint16(pt.maxDepth),
	}

	headerPos := int64(0)
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("写入文件头失败: %w", err)
	}

	// 使用BFS遍历树并写入节点
	nodeCount := uint32(0)
	nodeQueue := []*TrieNode{pt.root}

	for len(nodeQueue) > 0 {
		node := nodeQueue[0]
		nodeQueue = nodeQueue[1:]

		// 准备节点记录
		record := NodeRecord{
			NodeType:      byte(node.nodeType),
			IsLeaf:        0,
			ChildCount:    uint16(len(node.children)),
			Offset:        node.offset,
			StorageFileID: node.storageFileID,
			StorageOffset: node.storageOffset,
			FileIDLength:  0,
		}

		if node.isLeaf {
			record.IsLeaf = 1
		}

		// 如果是文件节点，存储fileID
		if node.nodeType == FileNode && node.fileID != "" {
			fileIDBytes := []byte(node.fileID)
			record.FileIDLength = byte(len(fileIDBytes))
			if record.FileIDLength > 255 {
				record.FileIDLength = 255
			}
			copy(record.FileID[:], fileIDBytes[:record.FileIDLength])
		}

		// 写入节点记录
		if err := binary.Write(file, binary.BigEndian, &record); err != nil {
			return fmt.Errorf("写入节点记录失败: %w", err)
		}

		// 写入子节点索引字节
		if len(node.children) > 0 {
			childBytes := make([]byte, len(node.children))
			i := 0
			childNodes := make([]*TrieNode, 0, len(node.children))

			for b, childNode := range node.children {
				childBytes[i] = b
				childNodes = append(childNodes, childNode)
				i++
			}

			// 确保按字节值排序子节点，以保持序列化结果一致性
			sort.Slice(childBytes, func(i, j int) bool {
				return childBytes[i] < childBytes[j]
			})

			if _, err := file.Write(childBytes); err != nil {
				return fmt.Errorf("写入子节点索引失败: %w", err)
			}

			// 对应排序的子节点加入队列
			for _, b := range childBytes {
				nodeQueue = append(nodeQueue, node.children[b])
			}
		}

		nodeCount++
	}

	// 更新文件头中的节点计数
	if _, err := file.Seek(headerPos, io.SeekStart); err != nil {
		return fmt.Errorf("文件定位失败: %w", err)
	}

	header.NodeCount = nodeCount
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("更新文件头失败: %w", err)
	}

	return nil
}

// LoadFromFile loads the prefix tree from a file
func (pt *PrefixTree) LoadFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// read file header
	var header TreeFileHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("读取文件头失败: %w", err)
	}

	// validate header
	if header.Magic != TreeFileMagic {
		return fmt.Errorf("无效的文件魔数")
	}

	if header.Version != 1 {
		return fmt.Errorf("不支持的文件版本: %d", header.Version)
	}

	pt.maxDepth = int(header.MaxDepth)

	//no nodes to load
	if header.NodeCount == 0 {
		return nil
	}

	// read root node
	var rootRecord NodeRecord
	if err := binary.Read(file, binary.BigEndian, &rootRecord); err != nil {
		return fmt.Errorf("读取根节点记录失败: %w", err)
	}

	// create root node
	pt.root = &TrieNode{
		nodeType:      NodeType(rootRecord.NodeType),
		children:      make(map[byte]*TrieNode),
		isLeaf:        rootRecord.IsLeaf == 1,
		storageFileID: rootRecord.StorageFileID,
		storageOffset: rootRecord.StorageOffset,
		offset:        rootRecord.Offset,
	}

	if rootRecord.FileIDLength > 0 {
		pt.root.fileID = string(rootRecord.FileID[:rootRecord.FileIDLength])
		if rootRecord.NodeType == byte(FileNode) {
			go pt.getOrCreateFilter(pt.root.fileID)
		}
	}

	// read root node's child indices
	// use ChildCount from NodeRecord instead of len(children)
	childIndices := make([]byte, rootRecord.ChildCount)
	if rootRecord.ChildCount > 0 {
		if _, err := file.Read(childIndices); err != nil {
			return fmt.Errorf("read child indices failed: %w", err)
		}
	}

	// BFS to read and construct the rest of the tree
	nodeQueue := []*TrieNode{pt.root}
	childIndicesQueue := [][]byte{childIndices}
	processedNodes := uint32(1) // count root node

	for len(nodeQueue) > 0 && len(childIndicesQueue) > 0 && processedNodes < header.NodeCount {
		currentNode := nodeQueue[0]
		nodeQueue = nodeQueue[1:]

		currentIndices := childIndicesQueue[0]
		childIndicesQueue = childIndicesQueue[1:]

		// process all child nodes of the current node
		for _, index := range currentIndices {
			if processedNodes >= header.NodeCount {
				break // safety check: ensure we don't process more than total nodes
			}

			var childRecord NodeRecord
			if err := binary.Read(file, binary.BigEndian, &childRecord); err != nil {
				if err == io.EOF {
					return fmt.Errorf("error: file ended unexpectedly while reading node records")
				}
				return fmt.Errorf("read child node record failed: %w", err)
			}

			// create child node
			childNode := &TrieNode{
				nodeType:      NodeType(childRecord.NodeType),
				children:      make(map[byte]*TrieNode),
				isLeaf:        childRecord.IsLeaf == 1,
				storageFileID: childRecord.StorageFileID,
				storageOffset: childRecord.StorageOffset,
				offset:        childRecord.Offset,
			}

			if childRecord.FileIDLength > 0 {
				childNode.fileID = string(childRecord.FileID[:childRecord.FileIDLength])
				if childRecord.NodeType == byte(FileNode) {
					// go pt.getOrCreateFilter(childNode.fileID)
				}
			}

			// add child node to parent
			currentNode.children[index] = childNode

			// read child node's child indices
			childChildIndices := make([]byte, childRecord.ChildCount)
			if childRecord.ChildCount > 0 {
				if _, err := file.Read(childChildIndices); err != nil {
					return fmt.Errorf("read child indices failed: %w", err)
				}
			}

			// add child node and its child indices to the queue
			nodeQueue = append(nodeQueue, childNode)
			childIndicesQueue = append(childIndicesQueue, childChildIndices)

			processedNodes++
		}
	}

	// go pt.loadAllFileNodeFilters()

	return nil
}

// getOrCreateFileHandle gets or creates a cached file handle for a given fileID and flag
func (pt *PrefixTree) getOrCreateFileHandle(fileID string, flag int) (*os.File, error) {
	cacheKey := fmt.Sprintf("%s|%d", fileID, flag)

	// get from cache
	if handle, ok := pt.fileCache.Get(cacheKey); ok {
		return handle.(*os.File), nil
	}

	filePath := filepath.Join(pt.fileNodeDir, fileID)
	file, err := os.OpenFile(filePath, flag, 0644)
	if err != nil {
		return nil, err
	}

	// add to cache
	pt.fileCache.Add(cacheKey, file)
	return file, nil
}

// saveAllFiltersToSingleFile saves all bloom filters to a single file
func (pt *PrefixTree) saveAllFiltersToSingleFile() error {
	pt.filterLock.RLock()
	defer pt.filterLock.RUnlock()

	if pt.bloomFile == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(pt.bloomFile), 0755); err != nil {
		return err
	}
	tmp := pt.bloomFile + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// initialize and write file header
	hdr := bloomsFileHeader{
		Magic:   BloomFileMagic,
		Version: BloomFileVersion,
		Count:   uint32(len(pt.filterCache)),
	}
	if err := binary.Write(f, binary.BigEndian, &hdr); err != nil {
		return err
	}

	// write each bloom filter entry(fileID + data)
	for fileID, bf := range pt.filterCache {
		if bf == nil {
			continue
		}
		data, err := marshalBloom(bf)
		if err != nil {
			// 跳过无法序列化的条目
			continue
		}
		entryHdr := bloomEntryHeader{
			FileIDLen: uint16(len(fileID)),
			DataLen:   uint32(len(data)),
		}
		if err := binary.Write(f, binary.BigEndian, &entryHdr); err != nil {
			return err
		}
		if _, err := f.Write([]byte(fileID)); err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			return err
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}
	return os.Rename(tmp, pt.bloomFile)
}

func (pt *PrefixTree) loadAllFiltersFromSingleFile() error {
	if pt.bloomFile == "" {
		return fmt.Errorf("bloom file not set")
	}
	f, err := os.Open(pt.bloomFile)
	if err != nil {
		return err
	}
	defer f.Close()

	var hdr bloomsFileHeader
	if err := binary.Read(f, binary.BigEndian, &hdr); err != nil {
		return err
	}
	if hdr.Magic != BloomFileMagic || hdr.Version != BloomFileVersion {
		return fmt.Errorf("invalid bloom file header")
	}

	cache := make(map[string]*bloom.BloomFilter, hdr.Count)
	for i := uint32(0); i < hdr.Count; i++ {
		var eh bloomEntryHeader
		if err := binary.Read(f, binary.BigEndian, &eh); err != nil {
			return err
		}
		if eh.FileIDLen == 0 || eh.DataLen == 0 {
			return fmt.Errorf("invalid entry header")
		}
		idBuf := make([]byte, eh.FileIDLen)
		if _, err := io.ReadFull(f, idBuf); err != nil {
			return err
		}
		data := make([]byte, eh.DataLen)
		if _, err := io.ReadFull(f, data); err != nil {
			return err
		}
		bf, err := unmarshalBloom(data)
		if err != nil {
			// skip invalid entries
			continue
		}
		cache[string(idBuf)] = bf
	}

	pt.filterLock.Lock()
	pt.filterCache = cache
	pt.filterLock.Unlock()
	return nil
}

func marshalBloom(bf *bloom.BloomFilter) ([]byte, error) {
	// use WriteTo if available
	if wt, ok := interface{}(bf).(interface {
		WriteTo(io.Writer) (int64, error)
	}); ok {
		var buf bytes.Buffer
		_, err := wt.WriteTo(&buf)
		return buf.Bytes(), err
	}
	// fallback to MarshalBinary
	if mb, ok := interface{}(bf).(interface{ MarshalBinary() ([]byte, error) }); ok {
		return mb.MarshalBinary()
	}
	return nil, fmt.Errorf("bloom filter does not support serialization")
}

func unmarshalBloom(data []byte) (*bloom.BloomFilter, error) {
	bf := &bloom.BloomFilter{}
	// use ReadFrom if available
	if rf, ok := interface{}(bf).(interface {
		ReadFrom(io.Reader) (int64, error)
	}); ok {
		_, err := rf.ReadFrom(bytes.NewReader(data))
		return bf, err
	}
	// fallback to UnmarshalBinary
	if ub, ok := interface{}(bf).(interface{ UnmarshalBinary([]byte) error }); ok {
		if err := ub.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return bf, nil
	}
	return nil, fmt.Errorf("bloom filter does not support deserialization")
}
