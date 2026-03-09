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

	lru "github.com/hashicorp/golang-lru"
)

const (
	MaxPrefixDepth        = 6          // the maximum depth of the prefix tree
	NodeEntrySize         = 76         // (1 + 32 + 8 + 4 + 8 + 8 + 8 + 4 + 3) bytes
	FileNodeMagic         = 0x50544E46 // "PTNF" - file node magic number
	MaxKeySize            = 32         // maximum key size in bytes
	TreeFileMagic         = 0x50545246 // "PTRF" - prefix tree file magic number
	maxCacheFilesHandles  = 65536
	fileNodeCacheCapacity = 64
	maxPooledBufferSize   = 1024 * 1024 // 1MB
	globalFileName        = "global.node"
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
	offset int64 // in the account file

	storageFileID  uint32
	storageOffset  int64
	storageSize    uint64
	storageKVCount uint32

	fileID string // file name
}

type NodeInfo struct {
	key           []byte
	accountOffset int64
	storageFileID uint32
	storageOffset int64
	storageSize   uint64
}

type fileNodeCacheEntry struct {
	hdrBuf []byte
	buf    []byte
}

type gcJob struct {
	fileID string
	state  *gcState
}

type gcState struct {
	done     chan struct{}
	header   FileNodeHeader
	sorted   []byte
	unsorted []byte
}

func (pt *PrefixTree) invalidateFileNodeCache(fileID string) {
	pt.fileNodeCacheMu.Lock()
	defer pt.fileNodeCacheMu.Unlock()
	if pt.fileNodeCache == nil || fileID == "" {
		return
	}
	pt.fileNodeCache.Remove(fileID)
}

func (pt *PrefixTree) setFileNodeCache(fileID string, hdrBuf []byte, buf []byte) {
	pt.fileNodeCacheMu.Lock()
	defer pt.fileNodeCacheMu.Unlock()
	if pt.fileNodeCache == nil || fileID == "" {
		return
	}
	pt.fileNodeCache.Add(fileID, &fileNodeCacheEntry{hdrBuf: hdrBuf, buf: buf})
}

// PrefixTree
type PrefixTree struct {
	lock        sync.RWMutex
	maxDepth    int
	db          *PrefixDB
	fileNodeDir string

	globalFileID       string
	bucketPrefixLength int

	// for background merging
	mergeLock sync.Mutex
	mergeStop chan struct{}
	mergeWait sync.WaitGroup

	fileHandleCache *lru.Cache
	fileNodeCache   *lru.Cache
	fileNodeCacheMu sync.RWMutex
	fileStripeLocks stripedRWLocks // striped locks for file operations

	bufPool sync.Pool

	gcCount int

	gcQueue       chan gcJob
	gcInFlight    map[string]*gcState
	gcWriteBlocks map[string]int
	gcMu          sync.Mutex
}

func (pt *PrefixTree) fileIDForKey(key []byte) string {
	if len(key) < pt.maxDepth {
		return pt.globalFileID
	}
	return pt.getBucketID(key)
}

func (pt *PrefixTree) getGCState(fileID string) *gcState {
	pt.gcMu.Lock()
	defer pt.gcMu.Unlock()
	return pt.gcInFlight[fileID]
}

func (pt *PrefixTree) beginFileMutation(fileID string) func() {
	if fileID == "" {
		return func() {}
	}
	for {
		pt.gcMu.Lock()
		state, running := pt.gcInFlight[fileID]
		if running {
			pt.gcMu.Unlock()
			<-state.done
			continue
		}
		pt.gcWriteBlocks[fileID]++
		pt.gcMu.Unlock()
		break
	}
	return func() {
		pt.gcMu.Lock()
		if remaining := pt.gcWriteBlocks[fileID] - 1; remaining <= 0 {
			delete(pt.gcWriteBlocks, fileID)
		} else {
			pt.gcWriteBlocks[fileID] = remaining
		}
		pt.gcMu.Unlock()
	}
}

func (pt *PrefixTree) maybeScheduleGC(fileID string, header FileNodeHeader, sortedSlice, unsortedSlice []byte) {
	if fileID == "" || len(unsortedSlice) == 0 {
		return
	}
	pt.gcMu.Lock()
	if pt.gcWriteBlocks[fileID] > 0 {
		pt.gcMu.Unlock()
		return
	}
	if _, exists := pt.gcInFlight[fileID]; exists {
		pt.gcMu.Unlock()
		return
	}
	state := &gcState{
		done:   make(chan struct{}),
		header: header,
	}
	state.header.SortedEntryCount = uint32(len(sortedSlice) / NodeEntrySize)
	state.header.UnsortedEntryCount = uint32(len(unsortedSlice) / NodeEntrySize)
	if len(sortedSlice) > 0 {
		state.sorted = make([]byte, len(sortedSlice))
		copy(state.sorted, sortedSlice)
	}
	state.unsorted = make([]byte, len(unsortedSlice))
	copy(state.unsorted, unsortedSlice)
	pt.gcInFlight[fileID] = state
	job := gcJob{fileID: fileID, state: state}
	pt.gcMu.Unlock()

	select {
	case pt.gcQueue <- job:
	default:
		go pt.processGCJob(job)
	}
}

func (pt *PrefixTree) buildGCStateFromFile(fileID string) (*gcState, error) {
	filePath := filepath.Join(pt.fileNodeDir, fileID)
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open filenode failed: %w", err)
	}
	defer f.Close()

	var header FileNodeHeader
	if err := binary.Read(f, binary.BigEndian, &header); err != nil {
		return nil, fmt.Errorf("read filenode header failed: %w", err)
	}
	if header.Magic != FileNodeMagic {
		return nil, fmt.Errorf("invalid filenode magic for %s", fileID)
	}
	if header.SortedEntryCount == 0 && header.UnsortedEntryCount == 0 {
		return nil, nil
	}
	if header.UnsortedEntryCount == 0 {
		return nil, nil
	}
	totalEntries := header.SortedEntryCount + header.UnsortedEntryCount
	dataSize := int64(totalEntries) * NodeEntrySize
	if _, err := f.Seek(int64(binary.Size(header)), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek filenode failed: %w", err)
	}
	dataBuf := make([]byte, dataSize)
	if _, err := io.ReadFull(f, dataBuf); err != nil {
		return nil, fmt.Errorf("read filenode payload failed: %w", err)
	}
	sortedBytes := int(header.SortedEntryCount) * NodeEntrySize
	if sortedBytes > len(dataBuf) {
		sortedBytes = len(dataBuf)
	}
	unsortedOffset := sortedBytes
	unsortedBytes := len(dataBuf) - unsortedOffset
	state := &gcState{
		done:   make(chan struct{}),
		header: header,
	}
	if sortedBytes > 0 {
		state.sorted = append([]byte(nil), dataBuf[:sortedBytes]...)
	}
	if unsortedBytes > 0 {
		state.unsorted = append([]byte(nil), dataBuf[unsortedOffset:]...)
	}
	state.header.SortedEntryCount = uint32(len(state.sorted) / NodeEntrySize)
	state.header.UnsortedEntryCount = uint32(len(state.unsorted) / NodeEntrySize)
	return state, nil
}

func (pt *PrefixTree) processGCJob(job gcJob) {
	if job.state == nil {
		return
	}
	if err := pt.compactFileFromState(job.fileID, job.state); err != nil {
		fmt.Printf("PrefixTree GC failed for %s: %v\n", job.fileID, err)
	}
	pt.finishGC(job.fileID)
}

func (pt *PrefixTree) finishGC(fileID string) {
	pt.gcMu.Lock()
	state, exists := pt.gcInFlight[fileID]
	if exists {
		delete(pt.gcInFlight, fileID)
		close(state.done)
	}
	pt.gcMu.Unlock()
}

func (pt *PrefixTree) compactFileFromState(fileID string, state *gcState) error {
	entries := buildEntriesFromSlices(state.header, state.sorted, state.unsorted)
	filePath := filepath.Join(pt.fileNodeDir, fileID)
	tmp := filePath + ".tmp"
	newHdr := FileNodeHeader{
		Magic:            FileNodeMagic,
		Version:          state.header.Version,
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
		if _, err := tf.Write(encodeNodeEntry(e)); err != nil {
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
	pt.invalidateFileNodeCache(fileID)
	pt.dropFileHandles(fileID)
	pt.gcCount++
	return nil
}

func buildEntriesFromSlices(header FileNodeHeader, sortedSlice, unsortedSlice []byte) []NodeInfo {
	total := header.SortedEntryCount + header.UnsortedEntryCount
	if total == 0 {
		return nil
	}
	m := make(map[string]NodeInfo, total)
	isNonZero := func(e NodeInfo) bool { return e.storageFileID != 0 || e.storageOffset != 0 }
	sortedEntries := int(header.SortedEntryCount)
	for i := 0; i < sortedEntries; i++ {
		start := i * NodeEntrySize
		end := start + NodeEntrySize
		if end > len(sortedSlice) {
			break
		}
		dec := decodeNodeEntry(sortedSlice[start:end])
		if len(dec.key) > 0 {
			m[string(dec.key)] = dec
		}
	}
	unsortedEntries := int(header.UnsortedEntryCount)
	for i := 0; i < unsortedEntries; i++ {
		start := i * NodeEntrySize
		end := start + NodeEntrySize
		if end > len(unsortedSlice) {
			break
		}
		dec := decodeNodeEntry(unsortedSlice[start:end])
		if len(dec.key) == 0 {
			continue
		}
		k := string(dec.key)
		if old, ok := m[k]; ok && isNonZero(old) && !isNonZero(dec) {
			dec.storageFileID = old.storageFileID
			dec.storageOffset = old.storageOffset
			dec.storageSize = old.storageSize
			if dec.accountOffset < old.accountOffset {
				dec.accountOffset = old.accountOffset
			}
		}
		m[k] = dec
	}
	entries := make([]NodeInfo, 0, len(m))
	for _, e := range m {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
	return entries
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
	fmt.Println("Initializing " + " Prefix Tree...")
	fileNodeDir := filepath.Join(dirPath, "prefixdb", "filenodes")
	if err := os.MkdirAll(fileNodeDir, 0755); err != nil {
		return nil, fmt.Errorf("creat node file path failed: %w", err)
	}

	fileCache, err := lru.NewWithEvict(maxCacheFilesHandles, func(key interface{}, value interface{}) {
		if file, ok := value.(*os.File); ok {
			file.Close()
		}
	})
	if err != nil {
		return nil, fmt.Errorf("create file cache failed: %w", err)
	}

	pt := &PrefixTree{
		maxDepth:     MaxPrefixDepth,
		db:           db,
		fileNodeDir:  fileNodeDir,
		globalFileID: globalFileName,
		// bucketPrefixLength: MaxPrefixDepth - 1,
		mergeStop:       make(chan struct{}),
		fileHandleCache: fileCache,
		bufPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, NodeEntrySize*512)
			},
		},
		gcQueue:       make(chan gcJob, 64),
		gcInFlight:    make(map[string]*gcState),
		gcWriteBlocks: make(map[string]int),
	}
	fileNodeCache, err := lru.NewWithEvict(fileNodeCacheCapacity, func(key interface{}, value interface{}) {
		entry, _ := value.(*fileNodeCacheEntry)
		if entry != nil && entry.buf != nil {
			pt.releaseBuf(entry.buf)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("create file node cache failed: %w", err)
	}
	pt.fileNodeCache = fileNodeCache
	pt.startMergeWorker()

	pt.bucketPrefixLength = pt.maxDepth

	pt.gcCount = 0

	fmt.Println(" Prefix Tree initialized.")
	return pt, nil
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
// [53..60]	  : storageSize (8B)
// [73..75]   : reserved (3B)
func encodeNodeEntry(nodeInfo NodeInfo) []byte {
	entry := make([]byte, NodeEntrySize)

	keyLen := len(nodeInfo.key)
	if keyLen > MaxKeySize {
		keyLen = MaxKeySize
	}
	entry[0] = byte(keyLen)

	copy(entry[1:1+keyLen], nodeInfo.key)
	// account offset
	binary.BigEndian.PutUint64(entry[33:41], uint64(nodeInfo.accountOffset))
	// storage file id
	binary.BigEndian.PutUint32(entry[41:45], nodeInfo.storageFileID)
	// storage offset
	binary.BigEndian.PutUint64(entry[45:53], uint64(nodeInfo.storageOffset))
	// storage size
	binary.BigEndian.PutUint64(entry[53:61], nodeInfo.storageSize)

	return entry
}

func decodeNodeEntry(entry []byte) NodeInfo {
	res := NodeInfo{}
	if len(entry) < NodeEntrySize {
		return res
	}
	keyLen := int(entry[0])
	if keyLen > MaxKeySize {
		keyLen = MaxKeySize
	}
	res.key = entry[1 : 1+keyLen]

	res.accountOffset = int64(binary.BigEndian.Uint64(entry[33:41]))
	res.storageFileID = binary.BigEndian.Uint32(entry[41:45])
	res.storageOffset = int64(binary.BigEndian.Uint64(entry[45:53]))
	res.storageSize = binary.BigEndian.Uint64(entry[53:61])
	return res
}

func (pt *PrefixTree) Get(key []byte) (nodeInfo NodeInfo, found bool, err error) {
	pt.lock.RLock()
	defer pt.lock.RUnlock()
	if len(key) == 0 {
		return NodeInfo{}, false, errors.New("key cannot be empty")
	}
	fileID := pt.fileIDForKey(key)

	return pt.getFromFileNode(fileID, key)

}

// Put inserts or updates a key in the prefix tree
func (pt *PrefixTree) Put(key []byte, accountOffset int64, storageFileID uint32, storageOffset int64, storageSize uint64) error {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	fileID := pt.fileIDForKey(key)
	return pt.putIntoFileNode(fileID, key, accountOffset, storageFileID, storageOffset, storageSize)

}

func (pt *PrefixTree) putIntoFileNode(fileID string, key []byte, accountOffset int64, storageFileID uint32, storageOffset int64, storageSize uint64) error {
	release := pt.beginFileMutation(fileID)
	defer release()

	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.Lock()
	defer fl.Unlock()

	// invalidate cache if it matches
	pt.invalidateFileNodeCache(fileID)

	filePath := filepath.Join(pt.fileNodeDir, fileID)
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
	entryData := encodeNodeEntry(NodeInfo{
		key:           key,
		accountOffset: accountOffset,
		storageFileID: storageFileID,
		storageOffset: storageOffset,
		storageSize:   storageSize,
	})
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
	fileID := pt.fileIDForKey(key)
	return pt.deleteFromFileNode(fileID, key)

}

// deleteFromFileNode deletes a key from a file node
func (pt *PrefixTree) deleteFromFileNode(fileID string, key []byte) (bool, error) {
	release := pt.beginFileMutation(fileID)
	defer release()

	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.Lock()
	defer fl.Unlock()

	// invalidate cache if it matches
	pt.invalidateFileNodeCache(fileID)

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
	entries := make([]NodeInfo, 0, total)
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
		entryData := encodeNodeEntry(NodeInfo{
			key:           e.key,
			accountOffset: e.accountOffset,
			storageFileID: e.storageFileID,
			storageOffset: e.storageOffset,
			storageSize:   e.storageSize,
		})
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
		for {
			select {
			case job := <-pt.gcQueue:
				pt.processGCJob(job)
			case <-pt.mergeStop:
				for {
					select {
					case job := <-pt.gcQueue:
						pt.processGCJob(job)
					default:
						return
					}
				}
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

	if pt.fileHandleCache != nil {
		pt.fileHandleCache.Purge()
	}

	return nil
}

func (pt *PrefixTree) getFromFileNode(fileID string, Key []byte) (nodeInfo NodeInfo, found bool, err error) {
	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.RLock()
	scheduleGC := func() {}
	scheduleJob := false
	defer func() {
		fl.RUnlock()
		if scheduleJob {
			scheduleGC()
		}
	}()

	var hdrBuf []byte
	var bigBuf []byte
	var header FileNodeHeader
	var sortedSlice, unsortedSlice []byte

	if state := pt.getGCState(fileID); state != nil {
		header = state.header
		sortedSlice = state.sorted
		unsortedSlice = state.unsorted
	} else {
		cacheLocked := false
		pt.fileNodeCacheMu.RLock()
		if pt.fileNodeCache != nil {
			if raw, ok := pt.fileNodeCache.Get(fileID); ok {
				if entry, _ := raw.(*fileNodeCacheEntry); entry != nil && entry.hdrBuf != nil {
					hdrBuf = entry.hdrBuf
					bigBuf = entry.buf
					cacheLocked = true
				}
			}
		}
		if cacheLocked {
			defer pt.fileNodeCacheMu.RUnlock()
			if err := binary.Read(bytes.NewReader(hdrBuf), binary.BigEndian, &header); err != nil {
				return NodeInfo{}, false, fmt.Errorf("decode header failed: %w", err)
			}
			if header.Magic != FileNodeMagic {
				return NodeInfo{}, false, fmt.Errorf("invalid cached file node magic (got 0x%X, file=%s)", header.Magic, fileID)
			}
		} else {
			pt.fileNodeCacheMu.RUnlock()
			file, err := pt.getOrCreateFileHandle(fileID, os.O_RDWR)
			if err != nil {
				if os.IsNotExist(err) {
					return NodeInfo{}, false, nil
				}
				return NodeInfo{}, false, fmt.Errorf("open file failed: %w", err)
			}

			headerSize := int64(binary.Size(header))
			hdrBuf = make([]byte, headerSize)
			if _, err := file.ReadAt(hdrBuf, 0); err != nil {
				return NodeInfo{}, false, fmt.Errorf("read header failed: %w", err)
			}
			if pt.db != nil {
				pt.db.addReadBytes(len(hdrBuf))
			}
			if err := binary.Read(bytes.NewReader(hdrBuf), binary.BigEndian, &header); err != nil {
				return NodeInfo{}, false, fmt.Errorf("decode header failed: %w", err)
			}
			if header.Magic != FileNodeMagic {
				return NodeInfo{}, false, fmt.Errorf("invalid file node magic (got 0x%X, file=%s)", header.Magic, fileID)
			}

			totalEntries := header.SortedEntryCount + header.UnsortedEntryCount
			if totalEntries > 0 {
				totalDataSize := int64(totalEntries) * NodeEntrySize
				tempBuf := pt.borrowBuf(int(totalDataSize))
				if tempBuf != nil {
					if _, err := file.ReadAt(tempBuf[:totalDataSize], headerSize); err != nil && err != io.EOF {
						pt.releaseBuf(tempBuf)
						return NodeInfo{}, false, fmt.Errorf("read bulk data failed: %w", err)
					}
					if pt.db != nil {
						pt.db.addReadBytes(int(totalDataSize))
					}
					bigBuf = tempBuf[:totalDataSize]
				}
			}

			pt.setFileNodeCache(fileID, hdrBuf, bigBuf)
		}

		totalEntries := header.SortedEntryCount + header.UnsortedEntryCount
		if totalEntries > 0 && bigBuf != nil {
			sortedBytes := int64(header.SortedEntryCount) * NodeEntrySize
			sortedSlice = bigBuf[:sortedBytes]
			unsortedSlice = bigBuf[sortedBytes:]
			if header.UnsortedEntryCount > 0 && header.UnsortedEntryCount >= header.SortedEntryCount {
				scheduleJob = true
				scheduleGC = func() {
					pt.maybeScheduleGC(fileID, header, sortedSlice, unsortedSlice)
				}
			}
		}
	}

	totalEntries := header.SortedEntryCount + header.UnsortedEntryCount
	if totalEntries == 0 {
		return NodeInfo{}, false, nil
	}

	// segmented storage keeps offset 0, so treat fileID alone as validity signal
	isNonZero := func(fid uint32) bool { return fid != 0 }

	var zeroHit *NodeInfo

	// var nodeInfo NodeInfo
	if header.UnsortedEntryCount > 0 && unsortedSlice != nil {
		totalSize := int(header.UnsortedEntryCount) * NodeEntrySize
		for i := uint32(0); i < header.UnsortedEntryCount; i++ {
			idx := header.UnsortedEntryCount - 1 - i
			offsetInBuf := int64(idx) * NodeEntrySize
			if offsetInBuf+NodeEntrySize > int64(totalSize) {
				break
			}
			dec := decodeNodeEntry(unsortedSlice[offsetInBuf : offsetInBuf+NodeEntrySize])
			if bytes.Equal(dec.key, Key) {
				if isNonZero(dec.storageFileID) {
					if zeroHit == nil {
						return dec, true, nil
					}
					return NodeInfo{
						key:           dec.key,
						accountOffset: zeroHit.accountOffset,
						storageFileID: dec.storageFileID,
						storageOffset: dec.storageOffset,
						storageSize:   dec.storageSize,
					}, true, nil
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

	if header.SortedEntryCount > 0 && sortedSlice != nil {
		getKeyAt := func(idx uint32) []byte {
			start := int(idx) * NodeEntrySize
			keyLen := int(sortedSlice[start])
			if keyLen > MaxKeySize {
				keyLen = MaxKeySize
			}
			return sortedSlice[start+1 : start+1+keyLen]
		}

		low, high := uint32(0), header.SortedEntryCount-1
		for low <= high {
			mid := (low + high) / 2
			k := getKeyAt(mid)
			cmp := bytes.Compare(k, Key)
			if cmp == 0 {
				start := int(mid) * NodeEntrySize
				dec := decodeNodeEntry(sortedSlice[start : start+NodeEntrySize])
				if isNonZero(dec.storageFileID) {
					return dec, true, nil
				}
				if zeroHit != nil {
					return *zeroHit, true, nil
				}
				return dec, true, nil
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
		return *zeroHit, true, nil
	}
	return NodeInfo{}, false, nil
}

func (pt *PrefixTree) dropFileHandles(fileID string) {
	if pt.fileHandleCache == nil {
		return
	}
	flags := []int{os.O_RDONLY, os.O_RDWR, os.O_RDWR | os.O_CREATE}
	for _, flag := range flags {
		key := fmt.Sprintf("%s|%d", fileID, flag)
		if v, ok := pt.fileHandleCache.Get(key); ok {
			if f, _ := v.(*os.File); f != nil {
				_ = f.Close()
			}
			pt.fileHandleCache.Remove(key)
		}
	}
}

func (pt *PrefixTree) borrowBuf(size int) []byte {
	if size <= 0 {
		return nil
	}
	v := pt.bufPool.Get()
	if v == nil {
		return make([]byte, size)
	}
	buf := v.([]byte)
	if cap(buf) < size {
		pt.bufPool.Put(buf[:cap(buf)])
		return make([]byte, size)
	}
	return buf[:size]
}

func (pt *PrefixTree) releaseBuf(buf []byte) {
	if buf == nil {
		return
	}
	if cap(buf) > maxPooledBufferSize {
		return
	}
	pt.bufPool.Put(buf[:cap(buf)])
}

// getOrCreateFileHandle gets or creates a cached file handle for a given fileID and flag
func (pt *PrefixTree) getOrCreateFileHandle(fileID string, flag int) (*os.File, error) {
	cacheKey := fmt.Sprintf("%s|%d", fileID, flag)

	// get from cache
	if handle, ok := pt.fileHandleCache.Get(cacheKey); ok {
		return handle.(*os.File), nil
	}

	filePath := filepath.Join(pt.fileNodeDir, fileID)
	file, err := os.OpenFile(filePath, flag, 0644)
	if err != nil {
		return nil, err
	}

	// add to cache
	pt.fileHandleCache.Add(cacheKey, file)
	return file, nil
}

// gc All filenodes
func (pt *PrefixTree) GC() int {
	pt.lock.Lock()
	defer pt.lock.Unlock()

	pt.gcMu.Lock()
	gcJobs := make([]gcJob, 0, len(pt.gcInFlight))
	for fileID, state := range pt.gcInFlight {
		gcJobs = append(gcJobs, gcJob{fileID: fileID, state: state})
	}
	pt.gcMu.Unlock()

	count := 0
	for _, job := range gcJobs {
		if job.state == nil {
			continue
		}
		if err := pt.compactFileFromState(job.fileID, job.state); err != nil {
			fmt.Printf("PrefixTree GC failed for %s: %v\n", job.fileID, err)
			continue
		}
		pt.finishGC(job.fileID)
		count++
	}
	entries, err := os.ReadDir(pt.fileNodeDir)
	if err != nil {
		fmt.Printf("PrefixTree GC scan failed: %v\n", err)
		return count
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileID := entry.Name()
		state, err := pt.buildGCStateFromFile(fileID)
		if err != nil {
			fmt.Printf("PrefixTree GC state build failed for %s: %v\n", fileID, err)
			continue
		}
		if state == nil || state.header.UnsortedEntryCount == 0 {
			continue
		}
		pt.gcMu.Lock()
		if _, exists := pt.gcInFlight[fileID]; exists {
			pt.gcMu.Unlock()
			continue
		}
		pt.gcInFlight[fileID] = state
		pt.gcMu.Unlock()
		if err := pt.compactFileFromState(fileID, state); err != nil {
			fmt.Printf("PrefixTree GC failed for %s: %v\n", fileID, err)
			pt.finishGC(fileID)
			continue
		}
		pt.finishGC(fileID)
		count++
	}
	return count
}
