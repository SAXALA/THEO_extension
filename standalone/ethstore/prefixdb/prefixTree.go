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
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru"
	"github.com/huandu/skiplist"
)

const (
	MaxPrefixDepth                  = 6          // the maximum depth of the prefix tree
	NodeEntrySize                   = 76         // (1 + 32 + 8 + 4 + 8 + 8 + 8 + 4 + 3) bytes
	FileNodeMagic                   = 0x50544E46 // "PTNF" - file node magic number
	MaxKeySize                      = 32         // maximum key size in bytes
	TreeFileMagic                   = 0x50545246 // "PTRF" - prefix tree file magic number
	maxCacheFilesHandles            = 65536
	maxPooledBufferSize             = 1024 * 1024 // 1MB
	globalFileName                  = "global.node"
	defaultNodeFileGCRatioThreshold = 1.0
	maxPrefixTreeGCWorkers          = 128
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
	hdrBuf  []byte
	buf     []byte
	release func([]byte)
	mu      sync.Mutex
	refs    int
	evicted bool
}

func newFileNodeCacheEntry(hdrBuf []byte, buf []byte, release func([]byte)) *fileNodeCacheEntry {
	return &fileNodeCacheEntry{hdrBuf: hdrBuf, buf: buf, release: release, refs: 1}
}

func (e *fileNodeCacheEntry) Retain() bool {
	if e == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.evicted && e.refs == 0 {
		return false
	}
	e.refs++
	return true
}

func (e *fileNodeCacheEntry) Release() {
	if e == nil {
		return
	}
	var buf []byte
	var release func([]byte)
	e.mu.Lock()
	if e.refs > 0 {
		e.refs--
	}
	if e.evicted && e.refs == 0 && e.buf != nil {
		buf = e.buf
		release = e.release
		e.buf = nil
	}
	e.mu.Unlock()
	if release != nil && buf != nil {
		release(buf)
	}
}

func (e *fileNodeCacheEntry) onSharedCacheEvict() {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.evicted = true
	e.mu.Unlock()
	e.Release()
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
	if fileID == pt.globalFileID {
		return
	}
	if pt.sharedCache == nil || fileID == "" {
		return
	}
	pt.sharedCache.Remove(sharedCacheNamespaceFileNode, fileID)
}

func (pt *PrefixTree) setFileNodeCache(fileID string, hdrBuf []byte, buf []byte) {
	if fileID == pt.globalFileID {
		return
	}
	if pt.sharedCache == nil || fileID == "" {
		return
	}
	entry := newFileNodeCacheEntry(hdrBuf, buf, pt.releaseBuf)
	pt.sharedCache.Add(sharedCacheNamespaceFileNode, fileID, entry, estimateFileNodeCacheEntrySize(fileID, entry))
}

func (pt *PrefixTree) getFileNodeCache(fileID string) (*fileNodeCacheEntry, bool) {
	if fileID == pt.globalFileID {
		return nil, false
	}
	if pt.sharedCache == nil || fileID == "" {
		return nil, false
	}
	raw, ok := pt.sharedCache.Get(sharedCacheNamespaceFileNode, fileID)
	if !ok {
		return nil, false
	}
	entry, _ := raw.(*fileNodeCacheEntry)
	if entry == nil || !entry.Retain() {
		return nil, false
	}
	return entry, true
}

func estimateFileNodeCacheEntrySize(fileID string, entry *fileNodeCacheEntry) uint64 {
	if entry == nil {
		return 1
	}
	total := uint64(len(fileID) + len(entry.hdrBuf) + len(entry.buf))
	if total == 0 {
		return 1
	}
	return total
}

func cloneNodeInfo(nodeInfo NodeInfo) NodeInfo {
	cloned := nodeInfo
	if len(nodeInfo.key) > 0 {
		cloned.key = append([]byte(nil), nodeInfo.key...)
	}
	return cloned
}

func mergeNodeInfoForAppend(previous NodeInfo, next NodeInfo) NodeInfo {
	merged := cloneNodeInfo(next)
	if previous.storageFileID != 0 && merged.storageFileID == 0 {
		merged.storageFileID = previous.storageFileID
		merged.storageOffset = previous.storageOffset
		merged.storageSize = previous.storageSize
		if merged.accountOffset < previous.accountOffset {
			merged.accountOffset = previous.accountOffset
		}
	}
	return merged
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

	fileHandleCache    *lru.Cache
	sharedCache        *sharedByteCache
	fileStripeLocks    stripedRWLocks // striped locks for file operations
	globalNodeMu       sync.RWMutex
	globalNodeIndex    *skiplist.SkipList
	globalFile         *os.File
	globalHeader       FileNodeHeader
	globalCommitDepth  int
	globalCommitDirty  bool
	globalCommitBatch  map[string]NodeInfo
	globalNeedsRewrite bool

	// node file access stats (read path)
	fileNodeCacheHits           uint64
	fileNodeCacheMisses         uint64
	fileHandleCacheHits         uint64
	fileHandleCacheMisses       uint64
	nodeFileDiskLoads           uint64 // times we had to read from a node file due to fileNodeCache miss
	nodeFileReadOps             uint64 // number of os.File.ReadAt calls
	nodeFileReadBytes           uint64
	globalFileNodeCacheHits     uint64
	globalFileNodeCacheMisses   uint64
	globalFileHandleCacheHits   uint64
	globalFileHandleCacheMisses uint64
	globalNodeFileDiskLoads     uint64
	globalNodeFileReadOps       uint64
	globalNodeFileReadBytes     uint64

	bufPool sync.Pool

	gcCount int

	gcQueue          chan gcJob
	gcInFlight       map[string]*gcState
	gcWriteBlocks    map[string]int
	gcMu             sync.Mutex
	gcRatioThreshold float64
	gcWorkerCount    int
}

func sanitizeNodeFileGCRatioThreshold(threshold float64) float64 {
	if threshold <= 0 {
		return defaultNodeFileGCRatioThreshold
	}
	return threshold
}

func sanitizePrefixTreeGCWorkerCount(workers int) int {
	if workers <= 0 {
		workers = runtime.NumCPU() / 2
	}
	if workers < 1 {
		workers = 1
	}
	if workers > maxPrefixTreeGCWorkers {
		workers = maxPrefixTreeGCWorkers
	}
	return workers
}

func (pt *PrefixTree) shouldScheduleGC(sortedCount, unsortedCount uint32) bool {
	if unsortedCount == 0 {
		return false
	}
	threshold := sanitizeNodeFileGCRatioThreshold(pt.gcRatioThreshold)
	return float64(unsortedCount)/float64(sortedCount) >= threshold
}

func (pt *PrefixTree) gcWorkerConcurrency() int {
	return sanitizePrefixTreeGCWorkerCount(pt.gcWorkerCount)
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
	if fileID == pt.globalFileID {
		return
	}
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
	if pt.db != nil {
		pt.db.addDiskRead(diskIOUsageNodeFileGC, binary.Size(header))
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
	payloadSize, err := nodeFileStoredPayloadSize(header)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(int64(binary.Size(header)), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek filenode failed: %w", err)
	}
	dataBuf := make([]byte, payloadSize)
	if _, err := io.ReadFull(f, dataBuf); err != nil {
		return nil, fmt.Errorf("read filenode payload failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskRead(diskIOUsageNodeFileGC, len(dataBuf))
	}
	_, sortedSlice, unsortedSlice, err := decodeNodeFilePayload(header, dataBuf)
	if err != nil {
		return nil, fmt.Errorf("decode filenode payload failed: %w", err)
	}
	state := &gcState{
		done:   make(chan struct{}),
		header: header,
	}
	if len(sortedSlice) > 0 {
		state.sorted = append([]byte(nil), sortedSlice...)
	}
	if len(unsortedSlice) > 0 {
		state.unsorted = append([]byte(nil), unsortedSlice...)
	}
	state.header.SortedEntryCount = uint32(len(state.sorted) / NodeEntrySize)
	state.header.UnsortedEntryCount = uint32(len(state.unsorted) / NodeEntrySize)
	state.header.setSortedCompression(false, 0)
	return state, nil
}

func (pt *PrefixTree) processGCJob(job gcJob) {
	if job.state == nil {
		return
	}
	release := pt.db.acquireSharedGCWorker()
	defer release()
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
		Version:          fileNodeVersionBase,
		SortedEntryCount: uint32(len(entries)),
	}
	sortedData := encodeNodeEntries(entries)
	payload, err := encodeNodeFilePayload(&newHdr, sortedData, nil, pt.db != nil && pt.db.nodeFileSortedCompression)
	if err != nil {
		return fmt.Errorf("encode compacted node payload failed: %w", err)
	}
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create tmp file failed: %w", err)
	}
	if err := binary.Write(tf, binary.BigEndian, &newHdr); err != nil {
		tf.Close()
		return fmt.Errorf("write tmp header failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileGC, binary.Size(newHdr))
	}
	if len(payload) > 0 {
		if _, err := tf.Write(payload); err != nil {
			tf.Close()
			return fmt.Errorf("write tmp entry failed: %w", err)
		}
		if pt.db != nil {
			pt.db.addDiskWrite(diskIOUsageNodeFileGC, len(payload))
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
	if fileID == pt.globalFileID {
		pt.globalNodeMu.Lock()
		defer pt.globalNodeMu.Unlock()
		if pt.globalFile != nil {
			_ = pt.globalFile.Close()
			pt.globalFile = nil
		}
		pt.globalHeader = newHdr
		pt.globalNodeIndex = skiplist.New(skiplist.String)
		for _, entry := range entries {
			pt.globalNodeIndex.Set(string(entry.key), cloneNodeInfo(entry))
		}
		file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("reopen global node file failed: %w", err)
		}
		pt.globalFile = file
		pt.gcCount++
		return nil
	}
	var hdrBuf bytes.Buffer
	if err := binary.Write(&hdrBuf, binary.BigEndian, &newHdr); err == nil {
		cacheData := append([]byte(nil), sortedData...)
		pt.setFileNodeCache(fileID, hdrBuf.Bytes(), cacheData)
	} else {
		pt.invalidateFileNodeCache(fileID)
	}
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

func encodeNodeEntries(entries []NodeInfo) []byte {
	if len(entries) == 0 {
		return nil
	}
	buf := make([]byte, 0, len(entries)*NodeEntrySize)
	for _, entry := range entries {
		buf = append(buf, encodeNodeEntry(entry)...)
	}
	return buf
}

func (pt *PrefixTree) loadGlobalNodeIndex() error {
	filePath := filepath.Join(pt.fileNodeDir, pt.globalFileID)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}

	header := FileNodeHeader{Magic: FileNodeMagic, Version: fileNodeVersionBase}
	index := skiplist.New(skiplist.String)
	if stat.Size() == 0 {
		if err := binary.Write(file, binary.BigEndian, &header); err != nil {
			_ = file.Close()
			return fmt.Errorf("write global node header failed: %w", err)
		}
		if pt.db != nil {
			pt.db.addDiskWrite(diskIOUsageNodeFileMutation, binary.Size(header))
		}
	} else {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			_ = file.Close()
			return fmt.Errorf("seek global node failed: %w", err)
		}
		if err := binary.Read(file, binary.BigEndian, &header); err != nil {
			_ = file.Close()
			return fmt.Errorf("read global node header failed: %w", err)
		}
		if pt.db != nil {
			pt.db.addDiskRead(diskIOUsageNodeFileLookup, binary.Size(header))
		}
		if header.Magic != FileNodeMagic {
			_ = file.Close()
			return fmt.Errorf("invalid global node magic")
		}

		payloadSize, err := nodeFileStoredPayloadSize(header)
		if err != nil {
			_ = file.Close()
			return err
		}
		if payloadSize > 0 {
			payload := make([]byte, payloadSize)
			if _, err := io.ReadFull(file, payload); err != nil {
				_ = file.Close()
				return fmt.Errorf("read global node payload failed: %w", err)
			}
			if pt.db != nil {
				pt.db.addDiskRead(diskIOUsageNodeFileLookup, len(payload))
			}
			_, sortedSlice, unsortedSlice, err := decodeNodeFilePayload(header, payload)
			if err != nil {
				_ = file.Close()
				return fmt.Errorf("decode global node payload failed: %w", err)
			}
			entries := buildEntriesFromSlices(header, sortedSlice, unsortedSlice)
			for _, entry := range entries {
				index.Set(string(entry.key), cloneNodeInfo(entry))
			}
		}
	}

	pt.globalNodeMu.Lock()
	if pt.globalFile != nil {
		_ = pt.globalFile.Close()
	}
	pt.globalFile = file
	pt.globalHeader = header
	pt.globalNodeIndex = index
	pt.globalNodeMu.Unlock()
	return nil
}

func (pt *PrefixTree) getFromGlobalNode(key []byte) (NodeInfo, bool, error) {
	pt.globalNodeMu.RLock()
	defer pt.globalNodeMu.RUnlock()
	if pt.globalNodeIndex == nil {
		return NodeInfo{}, false, nil
	}
	elem := pt.globalNodeIndex.Get(bytesToString(key))
	if elem == nil {
		return NodeInfo{}, false, nil
	}
	nodeInfo, ok := elem.Value.(NodeInfo)
	if !ok {
		return NodeInfo{}, false, fmt.Errorf("invalid global node entry type")
	}
	return cloneNodeInfo(nodeInfo), true, nil
}

func (pt *PrefixTree) beginGlobalCommit() {
	pt.globalNodeMu.Lock()
	pt.globalCommitDepth++
	if pt.globalCommitBatch == nil {
		pt.globalCommitBatch = make(map[string]NodeInfo)
	}
	pt.globalNodeMu.Unlock()
}

func (pt *PrefixTree) endGlobalCommit() error {
	pt.globalNodeMu.Lock()
	defer pt.globalNodeMu.Unlock()
	if pt.globalCommitDepth == 0 {
		return nil
	}
	pt.globalCommitDepth--
	if pt.globalCommitDepth > 0 || !pt.globalCommitDirty {
		return nil
	}
	defer func() {
		pt.globalCommitDirty = false
		pt.globalNeedsRewrite = false
		clear(pt.globalCommitBatch)
	}()
	if pt.globalNeedsRewrite {
		return pt.rewriteGlobalNodeFileLocked()
	}
	if len(pt.globalCommitBatch) == 0 {
		return nil
	}
	entries := make([]NodeInfo, 0, len(pt.globalCommitBatch))
	for _, entry := range pt.globalCommitBatch {
		entries = append(entries, cloneNodeInfo(entry))
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
	return pt.appendGlobalNodeEntries(entries)
}

func (pt *PrefixTree) appendGlobalNodeEntry(nodeInfo NodeInfo) error {
	return pt.appendGlobalNodeEntries([]NodeInfo{nodeInfo})
}

func (pt *PrefixTree) appendGlobalNodeEntries(entries []NodeInfo) error {
	if len(entries) == 0 {
		return nil
	}
	if pt.globalFile == nil {
		return errors.New("global node file is not initialized")
	}
	payloadSize, err := nodeFileStoredPayloadSize(pt.globalHeader)
	if err != nil {
		return err
	}
	writeOffset := int64(binary.Size(pt.globalHeader)) + int64(payloadSize)
	buf := make([]byte, 0, len(entries)*NodeEntrySize)
	for _, entry := range entries {
		buf = append(buf, encodeNodeEntry(entry)...)
	}
	if _, err := pt.globalFile.WriteAt(buf, writeOffset); err != nil {
		return fmt.Errorf("write global node entries failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileMutation, len(buf))
	}
	pt.globalHeader.UnsortedEntryCount += uint32(len(entries))
	if _, err := pt.globalFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek global node header failed: %w", err)
	}
	if err := binary.Write(pt.globalFile, binary.BigEndian, &pt.globalHeader); err != nil {
		return fmt.Errorf("update global node header failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileMutation, binary.Size(pt.globalHeader))
	}
	return nil
}

func (pt *PrefixTree) putIntoGlobalFileNode(key []byte, accountOffset int64, storageFileID uint32, storageOffset int64, storageSize uint64) error {
	pt.globalNodeMu.Lock()
	defer pt.globalNodeMu.Unlock()
	if pt.globalNodeIndex == nil {
		pt.globalNodeIndex = skiplist.New(skiplist.String)
	}
	next := NodeInfo{
		key:           append([]byte(nil), key...),
		accountOffset: accountOffset,
		storageFileID: storageFileID,
		storageOffset: storageOffset,
		storageSize:   storageSize,
	}
	if elem := pt.globalNodeIndex.Get(string(key)); elem != nil {
		if previous, ok := elem.Value.(NodeInfo); ok {
			next = mergeNodeInfoForAppend(previous, next)
		}
	}
	pt.globalNodeIndex.Set(string(key), cloneNodeInfo(next))
	if pt.globalCommitDepth > 0 {
		if pt.globalCommitBatch == nil {
			pt.globalCommitBatch = make(map[string]NodeInfo)
		}
		pt.globalCommitBatch[string(key)] = cloneNodeInfo(next)
		pt.globalCommitDirty = true
		return nil
	}
	if err := pt.appendGlobalNodeEntry(next); err != nil {
		return err
	}
	return nil
}

func (pt *PrefixTree) rewriteGlobalNodeFileLocked() error {
	if pt.globalNodeIndex == nil {
		pt.globalNodeIndex = skiplist.New(skiplist.String)
	}
	entries := make([]NodeInfo, 0, pt.globalNodeIndex.Len())
	for elem := pt.globalNodeIndex.Front(); elem != nil; elem = elem.Next() {
		nodeInfo, ok := elem.Value.(NodeInfo)
		if !ok {
			return fmt.Errorf("invalid global node entry type during rewrite")
		}
		entries = append(entries, cloneNodeInfo(nodeInfo))
	}
	filePath := filepath.Join(pt.fileNodeDir, pt.globalFileID)
	tmp := filePath + ".tmp"
	header := FileNodeHeader{Magic: FileNodeMagic, Version: fileNodeVersionBase, SortedEntryCount: uint32(len(entries))}
	sortedData := encodeNodeEntries(entries)
	payload, err := encodeNodeFilePayload(&header, sortedData, nil, pt.db != nil && pt.db.nodeFileSortedCompression)
	if err != nil {
		return fmt.Errorf("encode global node payload failed: %w", err)
	}
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create global node tmp file failed: %w", err)
	}
	if err := binary.Write(tf, binary.BigEndian, &header); err != nil {
		_ = tf.Close()
		return fmt.Errorf("write global node tmp header failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileMutation, binary.Size(header))
	}
	if len(payload) > 0 {
		if _, err := tf.Write(payload); err != nil {
			_ = tf.Close()
			return fmt.Errorf("write global node tmp entry failed: %w", err)
		}
		if pt.db != nil {
			pt.db.addDiskWrite(diskIOUsageNodeFileMutation, len(payload))
		}
	}
	if err := tf.Sync(); err != nil {
		_ = tf.Close()
		return fmt.Errorf("fsync global node tmp file failed: %w", err)
	}
	if err := tf.Close(); err != nil {
		return fmt.Errorf("close global node tmp file failed: %w", err)
	}
	if err := os.Rename(tmp, filePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename global node tmp file failed: %w", err)
	}
	if dirf, err := os.Open(filepath.Dir(filePath)); err == nil {
		_ = dirf.Sync()
		_ = dirf.Close()
	}
	if pt.globalFile != nil {
		_ = pt.globalFile.Close()
	}
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("reopen global node file failed: %w", err)
	}
	pt.globalFile = file
	pt.globalHeader = header
	return nil
}

func (pt *PrefixTree) deleteFromGlobalFileNode(key []byte) (bool, error) {
	pt.globalNodeMu.Lock()
	defer pt.globalNodeMu.Unlock()
	if pt.globalNodeIndex == nil {
		return false, nil
	}
	if pt.globalNodeIndex.Get(string(key)) == nil {
		return false, nil
	}
	pt.globalNodeIndex.Remove(string(key))
	if pt.globalCommitDepth > 0 {
		delete(pt.globalCommitBatch, string(key))
		pt.globalCommitDirty = true
		pt.globalNeedsRewrite = true
		return true, nil
	}
	if err := pt.rewriteGlobalNodeFileLocked(); err != nil {
		return false, err
	}
	return true, nil
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
	sharedCache := newSharedByteCache(segmentIndexCacheCapacityMiB * 1024 * 1024)
	if db != nil && db.sharedCache != nil {
		sharedCache = db.sharedCache
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
		sharedCache:     sharedCache,
		bufPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, NodeEntrySize*512)
			},
		},
		gcQueue:          make(chan gcJob, 64),
		gcInFlight:       make(map[string]*gcState),
		gcWriteBlocks:    make(map[string]int),
		gcRatioThreshold: sanitizeNodeFileGCRatioThreshold(db.nodeFileGCUnsortedRatioThreshold),
		gcWorkerCount:    sanitizePrefixTreeGCWorkerCount(db.gcWorkers),
		globalNodeIndex:  skiplist.New(skiplist.String),
	}
	if err := pt.loadGlobalNodeIndex(); err != nil {
		return nil, fmt.Errorf("load global node index failed: %w", err)
	}
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
	if fileID == pt.globalFileID {
		return pt.putIntoGlobalFileNode(key, accountOffset, storageFileID, storageOffset, storageSize)
	}
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
		header = FileNodeHeader{Magic: FileNodeMagic, Version: fileNodeVersionBase}
		if err := binary.Write(file, binary.BigEndian, &header); err != nil {
			file.Close()
			return fmt.Errorf("write header failed: %w", err)
		}
		if pt.db != nil {
			pt.db.addDiskWrite(diskIOUsageNodeFileMutation, binary.Size(header))
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
		if pt.db != nil {
			pt.db.addDiskRead(diskIOUsageNodeFileMutation, binary.Size(header))
		}
		if header.Magic != FileNodeMagic {
			file.Close()
			return errors.New("invalid file node magic")
		}
	}

	payloadSize, err := nodeFileStoredPayloadSize(header)
	if err != nil {
		return err
	}
	writeOffset := int64(binary.Size(header)) + int64(payloadSize)
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
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileMutation, len(entryData))
	}
	header.UnsortedEntryCount++
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek start failed: %w", err)
	}
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("update header failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileMutation, binary.Size(header))
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
	if fileID == pt.globalFileID {
		return pt.deleteFromGlobalFileNode(key)
	}
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
	if pt.db != nil {
		pt.db.addDiskRead(diskIOUsageNodeFileMutation, binary.Size(header))
	}
	if header.Magic != FileNodeMagic {
		return false, errors.New("invalid file node magic number")
	}
	payloadSize, err := nodeFileStoredPayloadSize(header)
	if err != nil {
		return false, err
	}
	payload := make([]byte, payloadSize)
	if _, err := file.ReadAt(payload, int64(binary.Size(header))); err != nil && err != io.EOF {
		return false, fmt.Errorf("read payload failed : %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskRead(diskIOUsageNodeFileMutation, len(payload))
	}
	_, sortedSlice, unsortedSlice, err := decodeNodeFilePayload(header, payload)
	if err != nil {
		return false, fmt.Errorf("decode payload failed : %w", err)
	}
	total := header.SortedEntryCount + header.UnsortedEntryCount
	entries := make([]NodeInfo, 0, total)
	found := false
	for _, dec := range buildEntriesFromSlices(header, sortedSlice, unsortedSlice) {
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
	payload, err = encodeNodeFilePayload(&header, encodeNodeEntries(entries), nil, pt.db != nil && pt.db.nodeFileSortedCompression)
	if err != nil {
		return false, fmt.Errorf("encode payload failed : %w", err)
	}
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		return false, fmt.Errorf("write file header failed : %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileMutation, binary.Size(header))
	}
	if len(payload) > 0 {
		if _, err := file.Write(payload); err != nil {
			return false, fmt.Errorf("failed to write entry: %w", err)
		}
		if pt.db != nil {
			pt.db.addDiskWrite(diskIOUsageNodeFileMutation, len(payload))
		}
	}
	newSize := int64(binary.Size(header)) + int64(len(payload))
	if err := file.Truncate(newSize); err != nil {
		return false, fmt.Errorf("fail to Truncate file : %w", err)
	}
	return true, nil
}

// StartMergeWorker starts the background merge worker
func (pt *PrefixTree) startMergeWorker() {
	workerCount := pt.gcWorkerConcurrency()
	pt.mergeWait.Add(workerCount)
	for i := 0; i < workerCount; i++ {
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
}

func (pt *PrefixTree) runGCJobsInParallel(jobs []gcJob) int {
	if len(jobs) == 0 {
		return 0
	}
	workerCount := pt.gcWorkerConcurrency()
	if workerCount > len(jobs) {
		workerCount = len(jobs)
	}
	jobCh := make(chan gcJob, len(jobs))
	resultCh := make(chan int, workerCount)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			completed := 0
			for job := range jobCh {
				if job.state == nil {
					continue
				}
				release := pt.db.acquireSharedGCWorker()
				if err := pt.compactFileFromState(job.fileID, job.state); err != nil {
					release()
					fmt.Printf("PrefixTree GC failed for %s: %v\n", job.fileID, err)
					pt.finishGC(job.fileID)
					continue
				}
				release()
				pt.finishGC(job.fileID)
				completed++
			}
			resultCh <- completed
		}()
	}
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)
	wg.Wait()
	close(resultCh)
	total := 0
	for completed := range resultCh {
		total += completed
	}
	return total
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

	fmt.Printf("PrefixTree nodefile stats: fileNodeCache hits=%d misses=%d handleCache hits=%d misses=%d diskLoads=%d readOps=%d readBytes=%d\n",
		atomic.LoadUint64(&pt.fileNodeCacheHits),
		atomic.LoadUint64(&pt.fileNodeCacheMisses),
		atomic.LoadUint64(&pt.fileHandleCacheHits),
		atomic.LoadUint64(&pt.fileHandleCacheMisses),
		atomic.LoadUint64(&pt.nodeFileDiskLoads),
		atomic.LoadUint64(&pt.nodeFileReadOps),
		atomic.LoadUint64(&pt.nodeFileReadBytes),
	)
	fmt.Printf("PrefixTree global.node stats: fileNodeCache hits=%d misses=%d handleCache hits=%d misses=%d diskLoads=%d readOps=%d readBytes=%d\n",
		atomic.LoadUint64(&pt.globalFileNodeCacheHits),
		atomic.LoadUint64(&pt.globalFileNodeCacheMisses),
		atomic.LoadUint64(&pt.globalFileHandleCacheHits),
		atomic.LoadUint64(&pt.globalFileHandleCacheMisses),
		atomic.LoadUint64(&pt.globalNodeFileDiskLoads),
		atomic.LoadUint64(&pt.globalNodeFileReadOps),
		atomic.LoadUint64(&pt.globalNodeFileReadBytes),
	)

	if pt.fileHandleCache != nil {
		pt.fileHandleCache.Purge()
	}
	pt.globalNodeMu.Lock()
	if pt.globalCommitDirty {
		if pt.globalNeedsRewrite {
			if err := pt.rewriteGlobalNodeFileLocked(); err != nil {
				pt.globalNodeMu.Unlock()
				return err
			}
		} else if len(pt.globalCommitBatch) > 0 {
			entries := make([]NodeInfo, 0, len(pt.globalCommitBatch))
			for _, entry := range pt.globalCommitBatch {
				entries = append(entries, cloneNodeInfo(entry))
			}
			sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
			if err := pt.appendGlobalNodeEntries(entries); err != nil {
				pt.globalNodeMu.Unlock()
				return err
			}
		}
		pt.globalCommitDirty = false
		pt.globalNeedsRewrite = false
		clear(pt.globalCommitBatch)
	}
	if pt.globalHeader.UnsortedEntryCount > 0 {
		if err := pt.rewriteGlobalNodeFileLocked(); err != nil {
			pt.globalNodeMu.Unlock()
			return err
		}
	}
	if pt.globalFile != nil {
		_ = pt.globalFile.Close()
		pt.globalFile = nil
	}
	pt.globalNodeMu.Unlock()

	return nil
}

func (pt *PrefixTree) getFromFileNode(fileID string, Key []byte) (nodeInfo NodeInfo, found bool, err error) {
	if fileID == pt.globalFileID {
		return pt.getFromGlobalNode(Key)
	}
	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.RLock()
	scheduleGC := func() {}
	scheduleJob := false
	var releaseCacheEntry func()
	defer func() {
		fl.RUnlock()
		if scheduleJob {
			scheduleGC()
		}
		if releaseCacheEntry != nil {
			releaseCacheEntry()
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
		if entry, ok := pt.getFileNodeCache(fileID); ok {
			releaseCacheEntry = entry.Release
			hdrBuf = entry.hdrBuf
			bigBuf = entry.buf
			atomic.AddUint64(&pt.fileNodeCacheHits, 1)
			if fileID == globalFileName {
				atomic.AddUint64(&pt.globalFileNodeCacheHits, 1)
			}
			if err := binary.Read(bytes.NewReader(hdrBuf), binary.BigEndian, &header); err != nil {
				return NodeInfo{}, false, fmt.Errorf("decode header failed: %w", err)
			}
			if header.Magic != FileNodeMagic {
				return NodeInfo{}, false, fmt.Errorf("invalid cached file node magic (got 0x%X, file=%s)", header.Magic, fileID)
			}
		} else {
			atomic.AddUint64(&pt.fileNodeCacheMisses, 1)
			if fileID == globalFileName {
				atomic.AddUint64(&pt.globalFileNodeCacheMisses, 1)
			}
			atomic.AddUint64(&pt.nodeFileDiskLoads, 1)
			if fileID == globalFileName {
				atomic.AddUint64(&pt.globalNodeFileDiskLoads, 1)
			}
			file, err := pt.getOrCreateFileHandle(fileID, os.O_RDWR)
			if err != nil {
				if os.IsNotExist(err) {
					return NodeInfo{}, false, nil
				}
				return NodeInfo{}, false, fmt.Errorf("open file failed: %w", err)
			}

			headerSize := int64(binary.Size(header))
			hdrBuf = make([]byte, headerSize)
			n, err := file.ReadAt(hdrBuf, 0)
			atomic.AddUint64(&pt.nodeFileReadOps, 1)
			if n > 0 {
				atomic.AddUint64(&pt.nodeFileReadBytes, uint64(n))
				if fileID == globalFileName {
					atomic.AddUint64(&pt.globalNodeFileReadBytes, uint64(n))
				}
			}
			if fileID == globalFileName {
				atomic.AddUint64(&pt.globalNodeFileReadOps, 1)
			}
			if err != nil {
				return NodeInfo{}, false, fmt.Errorf("read header failed: %w", err)
			}
			if pt.db != nil {
				pt.db.addDiskRead(diskIOUsageNodeFileLookup, n)
			}
			if err := binary.Read(bytes.NewReader(hdrBuf), binary.BigEndian, &header); err != nil {
				return NodeInfo{}, false, fmt.Errorf("decode header failed: %w", err)
			}
			if header.Magic != FileNodeMagic {
				return NodeInfo{}, false, fmt.Errorf("invalid file node magic (got 0x%X, file=%s)", header.Magic, fileID)
			}

			payloadSize, err := nodeFileStoredPayloadSize(header)
			if err != nil {
				return NodeInfo{}, false, err
			}
			if payloadSize > 0 {
				tempBuf := pt.borrowBuf(payloadSize)
				if tempBuf != nil {
					n2, err := file.ReadAt(tempBuf[:payloadSize], headerSize)
					atomic.AddUint64(&pt.nodeFileReadOps, 1)
					if n2 > 0 {
						atomic.AddUint64(&pt.nodeFileReadBytes, uint64(n2))
						if fileID == globalFileName {
							atomic.AddUint64(&pt.globalNodeFileReadBytes, uint64(n2))
						}
					}
					if fileID == globalFileName {
						atomic.AddUint64(&pt.globalNodeFileReadOps, 1)
					}
					if err != nil && err != io.EOF {
						pt.releaseBuf(tempBuf)
						return NodeInfo{}, false, fmt.Errorf("read bulk data failed: %w", err)
					}
					if pt.db != nil {
						pt.db.addDiskRead(diskIOUsageNodeFileLookup, n2)
					}
					decodedPayload, _, _, err := decodeNodeFilePayload(header, tempBuf[:payloadSize])
					pt.releaseBuf(tempBuf)
					if err != nil {
						return NodeInfo{}, false, fmt.Errorf("decode bulk data failed: %w", err)
					}
					bigBuf = decodedPayload
				}
			}

			pt.setFileNodeCache(fileID, hdrBuf, bigBuf)
		}

		totalEntries := header.SortedEntryCount + header.UnsortedEntryCount
		if totalEntries > 0 && bigBuf != nil {
			sortedBytes := int(header.SortedEntryCount) * NodeEntrySize
			sortedSlice = bigBuf[:sortedBytes]
			unsortedSlice = bigBuf[sortedBytes:]
			if pt.shouldScheduleGC(header.SortedEntryCount, header.UnsortedEntryCount) {
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
		atomic.AddUint64(&pt.fileHandleCacheHits, 1)
		if fileID == globalFileName {
			atomic.AddUint64(&pt.globalFileHandleCacheHits, 1)
		}
		return handle.(*os.File), nil
	}
	atomic.AddUint64(&pt.fileHandleCacheMisses, 1)
	if fileID == globalFileName {
		atomic.AddUint64(&pt.globalFileHandleCacheMisses, 1)
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

// CompactAllNodeFiles performs a full sweep over all node files and compacts
// every file that still contains an unsorted portion.
func (pt *PrefixTree) CompactAllNodeFiles() int {
	pt.lock.Lock()
	defer pt.lock.Unlock()

	pt.gcMu.Lock()
	pending := make([]*gcState, 0, len(pt.gcInFlight))
	for _, state := range pt.gcInFlight {
		if state != nil {
			pending = append(pending, state)
		}
	}
	pt.gcMu.Unlock()

	count := 0
	for _, state := range pending {
		<-state.done
		count++
	}
	entries, err := os.ReadDir(pt.fileNodeDir)
	if err != nil {
		fmt.Printf("PrefixTree GC scan failed: %v\n", err)
		return count
	}
	jobs := make([]gcJob, 0, len(entries))
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
		jobs = append(jobs, gcJob{fileID: fileID, state: state})
	}
	return count + pt.runGCJobsInParallel(jobs)
}

// GC keeps the historical API name and delegates to the full-sweep compaction.
func (pt *PrefixTree) GC() int {
	return pt.CompactAllNodeFiles()
}
