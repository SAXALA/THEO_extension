package prefixdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

const (
	MaxPrefixDepth = 7                                  // the maximum depth of the prefix tree
	NodeEntrySize  = 64                                 // the fixed size of each node entry in the file: 1 (key length) + 32 (key) + 4 (startSlotindex) + 4 (slotNum) + 8 (offset) + 15 (padding)
	FileNodeMagic  = 0x50544E46                         // "PTNF" - file node magic number
	MaxKeySize     = 32                                 // maximum key size in bytes
	FilterSize     = 16 * (MaxKeySize - MaxPrefixDepth) // bloom filter size
	TreeFileMagic  = 0x50545246                         // "PTRF" - prefix tree file magic number
)

type NodeType byte

const (
	NormalNode NodeType = 0 // in-memory normal node
	FileNode   NodeType = 1 // file node
)

// TrieNode
type TrieNode struct {
	nodeType NodeType           // node type
	children map[byte]*TrieNode // child nodes
	isLeaf   bool               // whether it's a leaf node

	startSlotindex int   // the starting slot index
	slotNum        int   // number of slots
	offset         int64 // in the account file

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

	//for bloom filter
	filterLock  sync.RWMutex
	filterCache map[string]*bloom.BloomFilter

	// for background merging
	mergeLock     sync.Mutex
	mergeStop     chan struct{}
	mergeWait     sync.WaitGroup
	mergeInterval time.Duration // merging interval
}

// FileNodeHeader  file node header structure
type FileNodeHeader struct {
	Magic              uint32 // file magic number
	Version            uint16 // file version
	SortedEntryCount   uint32
	UnsortedEntryCount uint32
	Reserved           [8]byte
}

// NewPrefixTree
func NewPrefixTree(db *PrefixDB, dirPath string) (*PrefixTree, error) {
	fileNodeDir := filepath.Join(dirPath, "prefixdb", "filenodes")
	if err := os.MkdirAll(fileNodeDir, 0755); err != nil {
		return nil, fmt.Errorf("creat node file path failed: %w", err)
	}

	pt := &PrefixTree{
		root: &TrieNode{
			nodeType: NormalNode,
			children: make(map[byte]*TrieNode),
		},
		maxDepth:      MaxPrefixDepth,
		db:            db,
		fileNodeDir:   fileNodeDir,
		filterCache:   make(map[string]*bloom.BloomFilter),
		mergeStop:     make(chan struct{}),
		mergeInterval: 1 * time.Minute,
	}
	pt.startMergeWorker()

	// load existing prefix tree file if exists
	pt.trieFile = filepath.Join(dirPath, "prefixdb", "trie")

	if _, err := os.Stat(pt.trieFile); err == nil {
		if err := pt.LoadFromFile(pt.trieFile); err != nil {
			fmt.Printf("Warning: Failed to load prefix tree from file: %v\n", err)
		}
	}

	// load existing file node filters
	go pt.loadAllFileNodeFilters()
	return pt, nil
}

func (pt *PrefixTree) getFileNodePath(prefix []byte) string {
	fileName := fmt.Sprintf("%x.node", prefix)
	return filepath.Join(pt.fileNodeDir, fileName)
}

// encodeNodeEntry encode node information into a fixed-size entry
func encodeNodeEntry(key []byte, startSlotindex, slotNum int, offset int64) []byte {
	entry := make([]byte, NodeEntrySize)

	keyLen := len(key)
	if keyLen > MaxKeySize {
		keyLen = MaxKeySize
	}
	entry[0] = byte(keyLen)

	copy(entry[1:1+keyLen], key)

	binary.BigEndian.PutUint32(entry[33:37], uint32(startSlotindex))

	binary.BigEndian.PutUint32(entry[37:41], uint32(slotNum))

	binary.BigEndian.PutUint64(entry[41:49], uint64(offset))

	return entry
}

// decodeNodeEntry decode a fixed-size entry into node information
func decodeNodeEntry(entry []byte) ([]byte, int, int, int64) {
	if len(entry) < NodeEntrySize {
		return nil, 0, 0, 0
	}
	keyLen := int(entry[0])
	if keyLen > MaxKeySize {
		keyLen = MaxKeySize
	}

	key := make([]byte, keyLen)
	copy(key, entry[1:1+keyLen])

	startSlotindex := int(binary.BigEndian.Uint32(entry[33:37]))

	slotNum := int(binary.BigEndian.Uint32(entry[37:41]))

	offset := int64(binary.BigEndian.Uint64(entry[41:49]))

	return key, startSlotindex, slotNum, offset
}

func (pt *PrefixTree) Get(key []byte) (int, int, int64, bool, error) {
	pt.lock.RLock()
	defer pt.lock.RUnlock()
	if len(key) == 0 {
		return 0, 0, 0, false, errors.New("key cannot be empty")
	}
	currentNode := pt.root
	depth := 0
	for depth < len(key) && depth < pt.maxDepth {
		if currentNode.nodeType == FileNode {
			return pt.getFromFileNode(currentNode.fileID, key[depth:])
		}

		nextNode, exists := currentNode.children[key[depth]]
		if !exists {
			return 0, 0, 0, false, nil
		}

		currentNode = nextNode
		depth++
	}

	if len(key) == pt.maxDepth || depth == len(key) {
		if currentNode.isLeaf {
			return currentNode.startSlotindex, currentNode.slotNum, currentNode.offset, true, nil
		}
		return 0, 0, 0, false, nil
	}
	if depth == pt.maxDepth && currentNode.nodeType == FileNode {
		// check filter first
		pt.filterLock.RLock()
		filter, exists := pt.filterCache[currentNode.fileID]
		pt.filterLock.RUnlock()

		if filter == nil {
			filter = pt.getOrCreateFilter(currentNode.fileID)
		}
		if filter == nil {
			return pt.getFromFileNode(currentNode.fileID, key[depth:])
		}

		if exists && !filter.Test(key[depth:]) {
			return 0, 0, 0, false, nil
		}
		// search in file node
		return pt.getFromFileNode(currentNode.fileID, key[depth:])
	}
	return 0, 0, 0, false, nil
}

// Put inserts or updates a key in the prefix tree
func (pt *PrefixTree) Put(key []byte, startSlotindex, slotNum int, offset int64) error {
	pt.lock.Lock()
	defer pt.lock.Unlock()

	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	currentNode := pt.root
	depth := 0

	for depth < len(key) && depth < pt.maxDepth {
		if currentNode.nodeType == FileNode {
			return pt.putIntoFileNode(currentNode.fileID, key[depth:], startSlotindex, slotNum, offset)
		}

		if _, exists := currentNode.children[key[depth]]; !exists {
			currentNode.children[key[depth]] = &TrieNode{
				nodeType: NormalNode,
				children: make(map[byte]*TrieNode),
			}
		}

		currentNode = currentNode.children[key[depth]]
		depth++
	}

	if depth == pt.maxDepth {
		currentNode.nodeType = FileNode
		prefix := key[:pt.maxDepth]
		currentNode.fileID = fmt.Sprintf("%x.node", prefix)
		if len(key) == pt.maxDepth {
			currentNode.isLeaf = true
			currentNode.startSlotindex = startSlotindex
			currentNode.slotNum = slotNum
			currentNode.offset = offset
			// set fileID based on the prefix
			return nil
		}
		return pt.putIntoFileNode(currentNode.fileID, key[depth:], startSlotindex, slotNum, offset)

	}

	// normal node
	currentNode.isLeaf = true
	currentNode.startSlotindex = startSlotindex
	currentNode.slotNum = slotNum
	currentNode.offset = offset

	return nil
}

func (pt *PrefixTree) putIntoFileNode(fileID string, key []byte, startSlotindex, slotNum int, offset int64) error {
	filePath := filepath.Join(pt.fileNodeDir, fileID)

	filter := pt.getOrCreateFilter(fileID)
	if filter != nil {
		filter.Add(key)
		pt.filterLock.Lock()
		pt.filterCache[fileID] = filter
		pt.filterLock.Unlock()
	}

	// check if file exists, if not create and initialize
	var file *os.File
	var header FileNodeHeader
	var err error

	if _, err = os.Stat(filePath); os.IsNotExist(err) {
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return fmt.Errorf("create file failed: %w", err)
		}

		header = FileNodeHeader{
			Magic:              FileNodeMagic,
			Version:            1,
			SortedEntryCount:   0,
			UnsortedEntryCount: 0,
		}

		if err := binary.Write(file, binary.BigEndian, &header); err != nil {
			file.Close()
			return fmt.Errorf("write new file header failed: %w", err)
		}
	} else {
		// open existing file
		file, err = os.OpenFile(filePath, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("open file failed: %w", err)
		}

		//if file is empty, reinitialize it
		fileInfo, err := file.Stat()
		if err != nil {
			file.Close()
			return fmt.Errorf("stat file failed: %w", err)
		}
		if fileInfo.Size() == 0 {
			file.Close()
			// delete the empty file
			if err := os.Remove(filePath); err != nil {
				return fmt.Errorf("remove empty file failed: %w", err)
			}
			// recreate the file
			file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return fmt.Errorf("recreate file failed: %w", err)
			}
			header = FileNodeHeader{
				Magic:              FileNodeMagic,
				Version:            1,
				SortedEntryCount:   0,
				UnsortedEntryCount: 0,
			}
			if err := binary.Write(file, binary.BigEndian, &header); err != nil {
				file.Close()
				return fmt.Errorf("write new file header failed: %w", err)
			}
		} else {
			if err := binary.Read(file, binary.BigEndian, &header); err != nil {
				file.Close()
				return fmt.Errorf("read file header failed: %w", err)
			}

			if header.Magic != FileNodeMagic {
				file.Close()
				return errors.New("invalid file node magic number")
			}
		}
	}
	defer file.Close()

	// add new entry at the end of the file
	writeOffset := int64(binary.Size(header)) + int64(header.SortedEntryCount+header.UnsortedEntryCount)*NodeEntrySize
	entryData := encodeNodeEntry(key, startSlotindex, slotNum, offset)

	if _, err := file.WriteAt(entryData, writeOffset); err != nil {
		return fmt.Errorf("write new entry failed: %w", err)
	}

	// update header
	header.UnsortedEntryCount++

	// reset file pointer to the beginning
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("reset file pointer failed: %w", err)
	}

	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("update file header failed: %w", err)
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
			return pt.deleteFromFileNode(currentNode.fileID, key[depth:])
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
			currentNode.startSlotindex = 0
			currentNode.slotNum = 0
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
	filePath := filepath.Join(pt.fileNodeDir, fileID)

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return false, fmt.Errorf("open node file failed: %w", err)
	}
	defer file.Close()
	// read and validate header
	var header FileNodeHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return false, fmt.Errorf("read file header failed: %w", err)
	}

	if header.Magic != FileNodeMagic {
		return false, errors.New("invalid file node magic number")
	}

	totalEntries := header.SortedEntryCount + header.UnsortedEntryCount

	// read all entries
	entries := make([]struct {
		key            []byte
		startSlotindex int
		slotNum        int
		offset         int64
	}, 0, totalEntries)

	keyFound := false
	for i := uint32(0); i < totalEntries; i++ {
		entryData := make([]byte, NodeEntrySize)
		if _, err := file.ReadAt(entryData, int64(binary.Size(header))+int64(i)*NodeEntrySize); err != nil {
			return false, fmt.Errorf("read entry failed : %w", err)
		}

		entryKey, entryStartSlot, entrySlotNum, entryOffset := decodeNodeEntry(entryData)

		if !bytes.Equal(entryKey, key) {
			entries = append(entries, struct {
				key            []byte
				startSlotindex int
				slotNum        int
				offset         int64
			}{
				key:            entryKey,
				startSlotindex: entryStartSlot,
				slotNum:        entrySlotNum,
				offset:         entryOffset,
			})
		} else {
			keyFound = true
		}
	}

	if !keyFound {
		return false, nil
	}

	//rebuild filter
	filter := bloom.NewWithEstimates(FilterSize, 0.05)
	for _, entry := range entries {
		filter.Add(entry.key)
	}
	pt.filterLock.Lock()
	pt.filterCache[fileID] = filter
	pt.filterLock.Unlock()

	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("reset file pointer failed: %w", err)
	}

	header.SortedEntryCount = uint32(len(entries))
	header.UnsortedEntryCount = 0
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return false, fmt.Errorf("write file header failed : %w", err)
	}

	for _, entry := range entries {
		entryData := encodeNodeEntry(entry.key, entry.startSlotindex, entry.slotNum, entry.offset)
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
	close(pt.mergeStop)
	pt.mergeWait.Wait()

	pt.ForceMerge()

	if err := pt.SaveToFile(pt.trieFile); err != nil {
		fmt.Printf("Warning: Failed to save prefix tree to file: %v\n", err)
	}

	pt.filterLock.Lock()
	pt.filterCache = nil
	pt.filterLock.Unlock()

	return nil
}

func (pt *PrefixTree) getFromFileNode(fileID string, remainingKey []byte) (int, int, int64, bool, error) {

	// check bloom filter first
	// filter := pt.getOrCreateFilter(fileID)
	// if !filter.Test(remainingKey) {
	// 	// definitely not exist
	// 	return 0, 0, 0, false, nil
	// }

	filePath := filepath.Join(pt.fileNodeDir, fileID)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, 0, false, nil
		}
		return 0, 0, 0, false, fmt.Errorf("open file failed: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return 0, 0, 0, false, fmt.Errorf("stat file failed: %w", err)
	}
	if fileInfo.Size() == 0 {
		return 0, 0, 0, false, nil
	}

	header := FileNodeHeader{}
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return 0, 0, 0, false, fmt.Errorf("read file header failed: %w", err)
	}

	if header.Magic != FileNodeMagic {
		return 0, 0, 0, false, errors.New("invalid file node magic number")
	}

	// search in the sorted part using binary search
	if header.SortedEntryCount > 0 {
		entrySize := NodeEntrySize
		low := int64(0)
		high := int64(header.SortedEntryCount - 1)

		for low <= high {
			mid := (low + high) / 2
			offset := int64(binary.Size(header)) + mid*int64(entrySize)

			entryData := make([]byte, entrySize)
			if _, err := file.ReadAt(entryData, offset); err != nil {
				return 0, 0, 0, false, fmt.Errorf("read item error: %w", err)
			}

			key, startSlotindex, slotNum, valueOffset := decodeNodeEntry(entryData)

			cmp := bytes.Compare(key, remainingKey)
			if cmp == 0 {
				// 找到了
				return startSlotindex, slotNum, valueOffset, true, nil
			} else if cmp < 0 {
				low = mid + 1
			} else {
				high = mid - 1
			}
		}
	}

	// search in the unsorted part linearly
	if header.UnsortedEntryCount > 0 {
		unsortedOffset := int64(binary.Size(header)) + int64(header.SortedEntryCount)*NodeEntrySize

		for i := uint32(0); i < header.UnsortedEntryCount; i++ {
			offset := unsortedOffset + int64(i)*NodeEntrySize

			entryData := make([]byte, NodeEntrySize)
			if _, err := file.ReadAt(entryData, offset); err != nil {
				return 0, 0, 0, false, fmt.Errorf("read unsorted item error: %w", err)
			}

			key, startSlotindex, slotNum, valueOffset := decodeNodeEntry(entryData)

			if bytes.Equal(key, remainingKey) {
				return startSlotindex, slotNum, valueOffset, true, nil
			}
		}
	}

	return 0, 0, 0, false, nil // not found
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
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open file failed: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file failed: %w", err)
	}
	if fileInfo.Size() == 0 {
		return nil
	}

	var header FileNodeHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("read file header failed: %w", err)
	}

	if header.Magic != FileNodeMagic {
		return errors.New("invalid file node magic number")
	}

	if header.UnsortedEntryCount == 0 {
		return nil
	}

	totalEntries := header.SortedEntryCount + header.UnsortedEntryCount
	entries := make(map[string]struct {
		key            []byte
		startSlotindex int
		slotNum        int
		offset         int64
	}, totalEntries)

	headerSize := int64(binary.Size(header))

	for i := uint32(0); i < header.SortedEntryCount; i++ {
		entryData := make([]byte, NodeEntrySize)
		offset := headerSize + int64(i)*NodeEntrySize
		if _, err := file.ReadAt(entryData, offset); err != nil {
			return fmt.Errorf("read sorted entry failed: %w", err)
		}

		key, startSlot, slotNum, valueOffset := decodeNodeEntry(entryData)
		entries[string(key)] = struct {
			key            []byte
			startSlotindex int
			slotNum        int
			offset         int64
		}{
			key:            key,
			startSlotindex: startSlot,
			slotNum:        slotNum,
			offset:         valueOffset,
		}
	}

	for i := uint32(0); i < header.UnsortedEntryCount; i++ {
		entryData := make([]byte, NodeEntrySize)
		offset := headerSize + int64(header.SortedEntryCount+i)*NodeEntrySize
		if _, err := file.ReadAt(entryData, offset); err != nil {
			return fmt.Errorf("read unsorted entry failed: %w", err)
		}

		key, startSlot, slotNum, valueOffset := decodeNodeEntry(entryData)
		entries[string(key)] = struct {
			key            []byte
			startSlotindex int
			slotNum        int
			offset         int64
		}{
			key:            key,
			startSlotindex: startSlot,
			slotNum:        slotNum,
			offset:         valueOffset,
		}
	}

	sortedEntries := make([]struct {
		key            []byte
		startSlotindex int
		slotNum        int
		offset         int64
	}, 0, len(entries))

	fileID := filepath.Base(filePath)
	filter := bloom.NewWithEstimates(uint(FilterSize), 0.05)

	for _, entry := range entries {
		sortedEntries = append(sortedEntries, entry)
		filter.Add(entry.key)
	}
	pt.filterLock.Lock()
	pt.filterCache[fileID] = filter
	pt.filterLock.Unlock()

	// sort entries by key
	sort.Slice(sortedEntries, func(i, j int) bool {
		return bytes.Compare(sortedEntries[i].key, sortedEntries[j].key) < 0
	})

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("reset file pointer failed: %w", err)
	}

	header.SortedEntryCount = uint32(len(sortedEntries))
	header.UnsortedEntryCount = 0

	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("write updated header failed: %w", err)
	}
	for _, entry := range sortedEntries {
		entryData := encodeNodeEntry(entry.key, entry.startSlotindex, entry.slotNum, entry.offset)
		if _, err := file.Write(entryData); err != nil {
			return fmt.Errorf("write sorted entry failed: %w", err)
		}
	}

	currentPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("get file position failed: %w", err)
	}

	if err := file.Truncate(currentPos); err != nil {
		return fmt.Errorf("truncate file failed: %w", err)
	}

	return nil
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

	filter := bloom.NewWithEstimates(uint(FilterSize), 0.05)

	// read all entries and add keys to the filter
	headerSize := int64(binary.Size(header))
	for i := uint32(0); i < uint32(FilterSize); i++ {
		entryData := make([]byte, NodeEntrySize)
		offset := headerSize + int64(i)*NodeEntrySize

		if _, err := file.ReadAt(entryData, offset); err != nil {
			break
		}

		key, _, _, _ := decodeNodeEntry(entryData)
		if len(key) > 0 {
			filter.Add(key)
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
	NodeType       byte
	IsLeaf         byte
	ChildCount     uint16
	StartSlotIndex int32
	SlotNum        int32
	Offset         int64
	FileIDLength   byte
	FileID         [255]byte
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
			NodeType:       byte(node.nodeType),
			IsLeaf:         byte(0),
			ChildCount:     uint16(len(node.children)),
			StartSlotIndex: int32(node.startSlotindex),
			SlotNum:        int32(node.slotNum),
			Offset:         node.offset,
			FileIDLength:   0,
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

	// 读取文件头
	var header TreeFileHeader
	if err := binary.Read(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("读取文件头失败: %w", err)
	}

	// 验证魔数和版本
	if header.Magic != TreeFileMagic {
		return fmt.Errorf("无效的文件魔数")
	}

	if header.Version != 1 {
		return fmt.Errorf("不支持的文件版本: %d", header.Version)
	}

	pt.maxDepth = int(header.MaxDepth)

	// 如果没有节点，直接返回
	if header.NodeCount == 0 {
		return nil
	}

	// 读取根节点记录
	var rootRecord NodeRecord
	if err := binary.Read(file, binary.BigEndian, &rootRecord); err != nil {
		return fmt.Errorf("读取根节点记录失败: %w", err)
	}

	// 创建根节点
	pt.root = &TrieNode{
		nodeType:       NodeType(rootRecord.NodeType),
		children:       make(map[byte]*TrieNode),
		isLeaf:         rootRecord.IsLeaf == 1,
		startSlotindex: int(rootRecord.StartSlotIndex),
		slotNum:        int(rootRecord.SlotNum),
		offset:         rootRecord.Offset,
	}

	if rootRecord.FileIDLength > 0 {
		pt.root.fileID = string(rootRecord.FileID[:rootRecord.FileIDLength])
		if rootRecord.NodeType == byte(FileNode) {
			go pt.getOrCreateFilter(pt.root.fileID)
		}
	}

	// 读取根节点的子节点索引
	// 使用NodeRecord中的ChildCount而不是len(children)
	childIndices := make([]byte, rootRecord.ChildCount)
	if rootRecord.ChildCount > 0 {
		if _, err := file.Read(childIndices); err != nil {
			return fmt.Errorf("读取根节点子节点索引失败: %w", err)
		}
	}

	// BFS 遍历重建树结构
	nodeQueue := []*TrieNode{pt.root}
	childIndicesQueue := [][]byte{childIndices}
	processedNodes := uint32(1) // 已处理根节点

	for len(nodeQueue) > 0 && len(childIndicesQueue) > 0 && processedNodes < header.NodeCount {
		currentNode := nodeQueue[0]
		nodeQueue = nodeQueue[1:]

		currentIndices := childIndicesQueue[0]
		childIndicesQueue = childIndicesQueue[1:]

		// 处理当前节点的所有子节点
		for _, index := range currentIndices {
			if processedNodes >= header.NodeCount {
				break // 安全检查：确保不会处理超过节点总数
			}

			var childRecord NodeRecord
			if err := binary.Read(file, binary.BigEndian, &childRecord); err != nil {
				if err == io.EOF {
					return fmt.Errorf("文件意外结束，缺少节点数据")
				}
				return fmt.Errorf("读取子节点记录失败: %w", err)
			}

			// 创建子节点
			childNode := &TrieNode{
				nodeType:       NodeType(childRecord.NodeType),
				children:       make(map[byte]*TrieNode),
				isLeaf:         childRecord.IsLeaf == 1,
				startSlotindex: int(childRecord.StartSlotIndex),
				slotNum:        int(childRecord.SlotNum),
				offset:         childRecord.Offset,
			}

			if childRecord.FileIDLength > 0 {
				childNode.fileID = string(childRecord.FileID[:childRecord.FileIDLength])
				if childRecord.NodeType == byte(FileNode) {
					go pt.getOrCreateFilter(childNode.fileID)
				}
			}

			// 将子节点添加到父节点
			currentNode.children[index] = childNode

			// 读取子节点的子节点索引
			childChildIndices := make([]byte, childRecord.ChildCount)
			if childRecord.ChildCount > 0 {
				if _, err := file.Read(childChildIndices); err != nil {
					return fmt.Errorf("读取子节点索引失败: %w", err)
				}
			}

			// 将子节点和其子节点索引加入队列，保持同步
			nodeQueue = append(nodeQueue, childNode)
			childIndicesQueue = append(childIndicesQueue, childChildIndices)

			processedNodes++
		}
	}

	// 确保加载所有文件节点的过滤器
	go pt.loadAllFileNodeFilters()

	return nil
}
