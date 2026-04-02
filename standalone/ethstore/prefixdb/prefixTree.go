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
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/huandu/skiplist"
)

const (
	MaxPrefixDepth                  = 6          // the maximum depth of the prefix tree
	NodeEntrySize                   = 65         // (1 + 32 + 8 + 4 + 4 + 8 + 8) bytes
	FileNodeMagic                   = 0x50544E46 // "PTNF" - file node magic number
	MaxKeySize                      = 32         // maximum key size in bytes
	TreeFileMagic                   = 0x50545246 // "PTRF" - prefix tree file magic number
	maxCacheFilesHandles            = 65536
	maxPooledBufferSize             = 1024 * 1024 // 1MB
	globalFileName                  = "global.node"
	defaultNodeFileGCRatioThreshold = 1.0
	maxPrefixTreeGCWorkers          = 64
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
	accountOffset uint64 // in the account file
	accountSize   uint32

	storageFileID uint32
	storageOffset uint64
	storageSize   uint64
}

type NodeInfo struct {
	key           []byte
	accountOffset uint64
	accountSize   uint32
	storageFileID uint32
	storageOffset uint64
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

func tombstoneNodeInfo(key []byte) NodeInfo {
	return NodeInfo{key: append([]byte(nil), key...)}
}

func isNodeInfoTombstone(nodeInfo NodeInfo) bool {
	return nodeInfo.accountOffset == 0 && nodeInfo.accountSize == 0 && nodeInfo.storageFileID == 0 && nodeInfo.storageOffset == 0 && nodeInfo.storageSize == 0
}

func hasStorageInfo(nodeInfo NodeInfo) bool {
	return nodeInfo.storageFileID != 0 || nodeInfo.storageOffset != 0 || nodeInfo.storageSize != 0
}

func nodeInfoToTrieNode(nodeInfo NodeInfo) *TrieNode {
	return &TrieNode{
		accountOffset: nodeInfo.accountOffset,
		accountSize:   nodeInfo.accountSize,
		storageFileID: nodeInfo.storageFileID,
		storageOffset: nodeInfo.storageOffset,
		storageSize:   nodeInfo.storageSize,
	}
}

func trieNodeToNodeInfo(key []byte, node *TrieNode) NodeInfo {
	if node == nil {
		return NodeInfo{key: append([]byte(nil), key...)}
	}
	return NodeInfo{
		key:           append([]byte(nil), key...),
		accountOffset: node.accountOffset,
		accountSize:   node.accountSize,
		storageFileID: node.storageFileID,
		storageOffset: node.storageOffset,
		storageSize:   node.storageSize,
	}
}

func mergeNodeInfoForAppend(previous NodeInfo, next NodeInfo) NodeInfo {
	if isNodeInfoTombstone(next) {
		return cloneNodeInfo(next)
	}
	merged := cloneNodeInfo(next)
	if merged.accountSize == 0 && merged.accountOffset == previous.accountOffset {
		merged.accountSize = previous.accountSize
	}
	if hasStorageInfo(previous) && !hasStorageInfo(merged) {
		merged.storageFileID = previous.storageFileID
		merged.storageOffset = previous.storageOffset
		merged.storageSize = previous.storageSize
		if merged.accountOffset < previous.accountOffset {
			merged.accountOffset = previous.accountOffset
			merged.accountSize = previous.accountSize
		}
	}
	return merged
}

func encodeFileNodeHeader(header FileNodeHeader) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, &header); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeFileNodeAndPayload(header FileNodeHeader, payload []byte) ([]byte, error) {
	headerBytes, err := encodeFileNodeHeader(header)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return headerBytes, nil
	}
	fileData := make([]byte, 0, len(headerBytes)+len(payload))
	fileData = append(fileData, headerBytes...)
	fileData = append(fileData, payload...)
	return fileData, nil
}

func nodeFileContainsKey(sortedSlice, unsortedSlice, key []byte) bool {
	unsortedCount := uint32(len(unsortedSlice) / NodeEntrySize)
	if unsortedCount > 0 {
		totalSize := int(unsortedCount) * NodeEntrySize
		for i := uint32(0); i < unsortedCount; i++ {
			idx := unsortedCount - 1 - i
			offsetInBuf := int64(idx) * NodeEntrySize
			if offsetInBuf+NodeEntrySize > int64(totalSize) {
				break
			}
			dec := decodeNodeEntry(unsortedSlice[offsetInBuf : offsetInBuf+NodeEntrySize])
			if bytes.Equal(dec.key, key) {
				return !isNodeInfoTombstone(dec)
			}
		}
	}

	sortedCount := uint32(len(sortedSlice) / NodeEntrySize)
	if sortedCount == 0 {
		return false
	}
	getKeyAt := func(idx uint32) []byte {
		start := int(idx) * NodeEntrySize
		keyLen := int(sortedSlice[start])
		if keyLen > MaxKeySize {
			keyLen = MaxKeySize
		}
		return sortedSlice[start+1 : start+1+keyLen]
	}
	low, high := uint32(0), sortedCount-1
	for low <= high {
		mid := (low + high) / 2
		cmp := bytes.Compare(getKeyAt(mid), key)
		if cmp == 0 {
			start := int(mid) * NodeEntrySize
			dec := decodeNodeEntry(sortedSlice[start : start+NodeEntrySize])
			return !isNodeInfoTombstone(dec)
		}
		if cmp < 0 {
			low = mid + 1
		} else {
			if mid == 0 {
				break
			}
			high = mid - 1
		}
	}
	return false
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

	fileHandleCache   *lru.Cache
	sharedCache       *sharedByteCache
	fileStripeLocks   stripedRWLocks // striped locks for file operations
	globalNodeMu      sync.RWMutex
	globalNodeIndex   *skiplist.SkipList
	globalFile        *os.File
	globalHeader      FileNodeHeader
	globalCommitDepth int
	globalCommitDirty bool
	globalCommitBatch map[string]NodeInfo

	// node file access stats (read path)
	fileNodeCacheHits     uint64
	fileNodeCacheMisses   uint64
	fileHandleCacheHits   uint64
	fileHandleCacheMisses uint64
	nodeFileDiskLoads     uint64 // times we had to read from a node file due to fileNodeCache miss
	nodeFileReadOps       uint64 // number of os.File.ReadAt calls
	nodeFileReadBytes     uint64
	fileNodeUnsortedHits  uint64
	fileNodeUnsortedSum   uint64

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
	return pt.beginFileMutationForState(fileID, nil)
}

func (pt *PrefixTree) beginFileMutationForState(fileID string, allowedState *gcState) func() {
	if fileID == "" {
		return func() {}
	}
	for {
		pt.gcMu.Lock()
		state, running := pt.gcInFlight[fileID]
		if running && state != allowedState {
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

	_, header, payload, err := pt.readWholeNodeFile(fileID, f, diskIOUsageNodeFileGC)
	if err != nil {
		return nil, err
	}
	payloadSize := len(payload)
	if payloadSize <= 0 {
		return nil, nil
	}
	unsortedCount, err := nodeFileInferUnsortedEntryCount(header, payloadSize)
	if err != nil {
		return nil, err
	}
	if header.SortedEntryCount == 0 && unsortedCount == 0 {
		return nil, nil
	}
	if unsortedCount == 0 {
		return nil, nil
	}
	_, sortedSlice, unsortedSlice, err := decodeNodeFilePayload(header, payload)
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
	release := pt.beginFileMutationForState(fileID, state)
	defer release()

	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.Lock()
	defer fl.Unlock()

	pt.invalidateFileNodeCache(fileID)

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
	fileData, err := encodeFileNodeAndPayload(newHdr, payload)
	if err != nil {
		return fmt.Errorf("encode compacted node file failed: %w", err)
	}
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create tmp file failed: %w", err)
	}
	if _, err := tf.Write(fileData); err != nil {
		tf.Close()
		return fmt.Errorf("write tmp file failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileGC, len(fileData))
	}
	if err := tf.Close(); err != nil {
		return fmt.Errorf("close tmp file failed: %w", err)
	}
	if err := os.Rename(tmp, filePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename failed: %w", err)
	}
	pt.dropFileHandles(fileID)
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
	pt.gcCount++
	return nil
}

func buildEntriesFromSlices(header FileNodeHeader, sortedSlice, unsortedSlice []byte) []NodeInfo {
	sortedCount := uint32(len(sortedSlice) / NodeEntrySize)
	unsortedCount := uint32(len(unsortedSlice) / NodeEntrySize)
	total := sortedCount + unsortedCount
	if total == 0 {
		return nil
	}
	m := make(map[string]NodeInfo, total)
	sortedEntries := int(sortedCount)
	for i := 0; i < sortedEntries; i++ {
		start := i * NodeEntrySize
		end := start + NodeEntrySize
		if end > len(sortedSlice) {
			break
		}
		dec := decodeNodeEntry(sortedSlice[start:end])
		if len(dec.key) > 0 && !isNodeInfoTombstone(dec) {
			m[string(dec.key)] = dec
		}
	}
	unsortedEntries := int(unsortedCount)
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
		if isNodeInfoTombstone(dec) {
			delete(m, k)
			continue
		}
		if old, ok := m[k]; ok {
			dec = mergeNodeInfoForAppend(old, dec)
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
		_, decodedHeader, payload, err := pt.readWholeNodeFile(pt.globalFileID, file, diskIOUsageNodeFileLookup)
		if err != nil {
			_ = file.Close()
			return err
		}
		header = decodedHeader
		payloadSize := len(payload)
		if payloadSize > 0 {
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
	commitStart := time.Now()
	pt.globalNodeMu.Lock()
	defer pt.globalNodeMu.Unlock()
	if pt.globalCommitDepth == 0 {
		prefixdbDebugf("PrefixTree endGlobalCommit: no-op elapsed=%s", time.Since(commitStart))
		return nil
	}
	pt.globalCommitDepth--
	if pt.globalCommitDepth > 0 || !pt.globalCommitDirty {
		prefixdbDebugf("PrefixTree endGlobalCommit: skipped dirty=%t depth=%d elapsed=%s",
			pt.globalCommitDirty, pt.globalCommitDepth, time.Since(commitStart))
		return nil
	}
	defer func() {
		pt.globalCommitDirty = false
		clear(pt.globalCommitBatch)
	}()
	if len(pt.globalCommitBatch) == 0 {
		prefixdbDebugf("PrefixTree endGlobalCommit: empty batch elapsed=%s", time.Since(commitStart))
		return nil
	}
	entries := make([]NodeInfo, 0, len(pt.globalCommitBatch))
	for _, entry := range pt.globalCommitBatch {
		entries = append(entries, cloneNodeInfo(entry))
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
	prefixdbDebugf("PrefixTree endGlobalCommit: append start entries=%d", len(entries))
	err := pt.appendGlobalNodeEntries(entries)
	prefixdbDebugf("PrefixTree endGlobalCommit: append done elapsed=%s err=%v", time.Since(commitStart), err)
	return err
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
	info, err := pt.globalFile.Stat()
	if err != nil {
		return err
	}
	writeOffset := info.Size()
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
	return nil
}

func (pt *PrefixTree) putIntoGlobalFileNode(key []byte, accountOffset uint64, accountSize uint32, storageFileID uint32, storageOffset uint64, storageSize uint64) error {
	pt.globalNodeMu.Lock()
	defer pt.globalNodeMu.Unlock()
	if pt.globalNodeIndex == nil {
		pt.globalNodeIndex = skiplist.New(skiplist.String)
	}
	next := NodeInfo{
		key:           append([]byte(nil), key...),
		accountOffset: accountOffset,
		accountSize:   accountSize,
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
	fileData, err := encodeFileNodeAndPayload(header, payload)
	if err != nil {
		return fmt.Errorf("encode global node file failed: %w", err)
	}
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create global node tmp file failed: %w", err)
	}
	if _, err := tf.Write(fileData); err != nil {
		_ = tf.Close()
		return fmt.Errorf("write global node tmp file failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileMutation, len(fileData))
	}
	if err := tf.Close(); err != nil {
		return fmt.Errorf("close global node tmp file failed: %w", err)
	}
	if err := os.Rename(tmp, filePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename global node tmp file failed: %w", err)
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
	tombstone := tombstoneNodeInfo(key)
	if pt.globalCommitDepth > 0 {
		if pt.globalCommitBatch == nil {
			pt.globalCommitBatch = make(map[string]NodeInfo)
		}
		pt.globalCommitBatch[string(key)] = tombstone
		pt.globalCommitDirty = true
		return true, nil
	}
	if err := pt.appendGlobalNodeEntry(tombstone); err != nil {
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
// [1..33]    : key (max 32B，padded with zeros if shorter)
// [33..41]   : accountOffset (8B)
// [41..45]   : accountSize (4B)
// [45..49]   : storageFileID (4B)
// [49..57]   : storageOffset (8B)
// [57..65]   : storageSize (8B)
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
	// account size
	binary.BigEndian.PutUint32(entry[41:45], nodeInfo.accountSize)
	// storage file id
	binary.BigEndian.PutUint32(entry[45:49], nodeInfo.storageFileID)
	// storage offset
	binary.BigEndian.PutUint64(entry[49:57], uint64(nodeInfo.storageOffset))
	// storage size
	binary.BigEndian.PutUint64(entry[57:65], nodeInfo.storageSize)
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

	res.accountOffset = binary.BigEndian.Uint64(entry[33:41])
	res.accountSize = binary.BigEndian.Uint32(entry[41:45])
	res.storageFileID = binary.BigEndian.Uint32(entry[45:49])
	res.storageOffset = binary.BigEndian.Uint64(entry[49:57])
	res.storageSize = binary.BigEndian.Uint64(entry[57:65])
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
func (pt *PrefixTree) Put(key []byte, accountOffset uint64, accountSize uint32, storageFileID uint32, storageOffset uint64, storageSize uint64) error {
	pt.lock.Lock()
	defer pt.lock.Unlock()
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	return pt.putNodeInfosLocked([]NodeInfo{{
		key:           append([]byte(nil), key...),
		accountOffset: accountOffset,
		accountSize:   accountSize,
		storageFileID: storageFileID,
		storageOffset: storageOffset,
		storageSize:   storageSize,
	}})

}

func (pt *PrefixTree) PutNode(key []byte, node *TrieNode) error {
	return pt.PutNodeInfos([]NodeInfo{trieNodeToNodeInfo(key, node)})
}

func (pt *PrefixTree) PutNodeInfos(entries []NodeInfo) error {
	if len(entries) == 0 {
		return nil
	}
	pt.lock.Lock()
	defer pt.lock.Unlock()
	return pt.putNodeInfosLocked(entries)
}

func (pt *PrefixTree) putNodeInfosLocked(entries []NodeInfo) error {
	if len(entries) == 0 {
		return nil
	}
	perFile := make(map[string][]NodeInfo)
	fileOrder := make([]string, 0, len(entries))
	globalEntries := make([]NodeInfo, 0)
	for _, entry := range entries {
		if len(entry.key) == 0 {
			return errors.New("key cannot be empty")
		}
		cloned := cloneNodeInfo(entry)
		fileID := pt.fileIDForKey(cloned.key)
		if fileID == pt.globalFileID {
			globalEntries = append(globalEntries, cloned)
			continue
		}
		if _, ok := perFile[fileID]; !ok {
			fileOrder = append(fileOrder, fileID)
		}
		perFile[fileID] = append(perFile[fileID], cloned)
	}
	if len(globalEntries) > 0 {
		if err := pt.putGlobalNodeInfos(globalEntries); err != nil {
			return err
		}
	}
	sort.Strings(fileOrder)
	for _, fileID := range fileOrder {
		if err := pt.appendFileNodeEntries(fileID, perFile[fileID]); err != nil {
			return err
		}
	}
	return nil
}

func (pt *PrefixTree) putGlobalNodeInfos(entries []NodeInfo) error {
	pt.globalNodeMu.Lock()
	defer pt.globalNodeMu.Unlock()
	if pt.globalNodeIndex == nil {
		pt.globalNodeIndex = skiplist.New(skiplist.String)
	}
	appendEntries := make([]NodeInfo, 0, len(entries))
	for _, entry := range entries {
		next := cloneNodeInfo(entry)
		if elem := pt.globalNodeIndex.Get(string(entry.key)); elem != nil {
			if previous, ok := elem.Value.(NodeInfo); ok {
				next = mergeNodeInfoForAppend(previous, next)
			}
		}
		pt.globalNodeIndex.Set(string(entry.key), cloneNodeInfo(next))
		appendEntries = append(appendEntries, next)
		if pt.globalCommitDepth > 0 {
			if pt.globalCommitBatch == nil {
				pt.globalCommitBatch = make(map[string]NodeInfo)
			}
			pt.globalCommitBatch[string(entry.key)] = cloneNodeInfo(next)
			pt.globalCommitDirty = true
		}
	}
	if pt.globalCommitDepth > 0 {
		return nil
	}
	return pt.appendGlobalNodeEntries(appendEntries)
}

func (pt *PrefixTree) appendFileNodeEntries(fileID string, entries []NodeInfo) error {
	if len(entries) == 0 {
		return nil
	}
	release := pt.beginFileMutation(fileID)
	defer release()

	fl := pt.fileStripeLocks.pick([]byte(fileID))
	fl.Lock()
	defer fl.Unlock()

	pt.invalidateFileNodeCache(fileID)

	file, err := pt.getOrCreateFileHandle(fileID, os.O_RDWR)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("open file failed: %w", err)
		}
		file, err = pt.getOrCreateFileHandle(fileID, os.O_RDWR|os.O_CREATE)
		if err != nil {
			return fmt.Errorf("create file failed: %w", err)
		}
	}

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file failed: %w", err)
	}
	entryData := encodeNodeEntries(entries)
	if info.Size() == 0 {
		header := FileNodeHeader{Magic: FileNodeMagic, Version: fileNodeVersionBase}
		fileData, err := encodeFileNodeAndPayload(header, entryData)
		if err != nil {
			return fmt.Errorf("encode initial file content failed: %w", err)
		}
		if _, err := file.WriteAt(fileData, 0); err != nil {
			return fmt.Errorf("write initial file content failed: %w", err)
		}
		if pt.db != nil {
			pt.db.addDiskWrite(diskIOUsageNodeFileMutation, len(fileData))
		}
		return nil
	} else if info.Size() < int64(binary.Size(FileNodeHeader{})) {
		return fmt.Errorf("invalid file node size %d", info.Size())
	}
	writeOffset := info.Size()
	if _, err := file.WriteAt(entryData, writeOffset); err != nil {
		return fmt.Errorf("write entry failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileMutation, len(entryData))
	}
	return nil
}

func (pt *PrefixTree) putIntoFileNode(fileID string, key []byte, accountOffset uint64, accountSize uint32, storageFileID uint32, storageOffset uint64, storageSize uint64) error {
	if fileID == pt.globalFileID {
		return pt.putIntoGlobalFileNode(key, accountOffset, accountSize, storageFileID, storageOffset, storageSize)
	}
	return pt.appendFileNodeEntries(fileID, []NodeInfo{{
		key:           append([]byte(nil), key...),
		accountOffset: accountOffset,
		accountSize:   accountSize,
		storageFileID: storageFileID,
		storageOffset: storageOffset,
		storageSize:   storageSize,
	}})
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
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("reset file pointer failed: %w", err)
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
	info, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("stat node file failed: %w", err)
	}
	payloadSize := int(info.Size()) - binary.Size(header)
	payload := make([]byte, payloadSize)
	n, err := file.ReadAt(payload, int64(binary.Size(header)))
	if err != nil && err != io.EOF {
		return false, fmt.Errorf("read payload failed : %w", err)
	}
	if n != payloadSize {
		return false, fmt.Errorf("read payload failed : short read got %d want %d: %w", n, payloadSize, io.ErrUnexpectedEOF)
	}
	if pt.db != nil {
		pt.db.addDiskRead(diskIOUsageNodeFileMutation, len(payload))
	}
	_, sortedSlice, unsortedSlice, err := decodeNodeFilePayload(header, payload)
	if err != nil {
		return false, fmt.Errorf("decode payload failed : %w", err)
	}
	if !nodeFileContainsKey(sortedSlice, unsortedSlice, key) {
		return false, nil
	}
	tombstone := encodeNodeEntry(tombstoneNodeInfo(key))
	if _, err := file.WriteAt(tombstone, info.Size()); err != nil {
		return false, fmt.Errorf("write tombstone failed: %w", err)
	}
	if pt.db != nil {
		pt.db.addDiskWrite(diskIOUsageNodeFileMutation, len(tombstone))
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

	if analysisStatsEnabled {
		unsortedHits := atomic.LoadUint64(&pt.fileNodeUnsortedHits)
		avgUnsortedCount := float64(0)
		if unsortedHits > 0 {
			avgUnsortedCount = float64(atomic.LoadUint64(&pt.fileNodeUnsortedSum)) / float64(unsortedHits)
		}
		fmt.Printf("PrefixTree nodefile stats: fileNodeCache hits=%d misses=%d handleCache hits=%d misses=%d diskLoads=%d readOps=%d readBytes=%d unsortedHits=%d avgUnsortedCount=%.2f\n",
			atomic.LoadUint64(&pt.fileNodeCacheHits),
			atomic.LoadUint64(&pt.fileNodeCacheMisses),
			atomic.LoadUint64(&pt.fileHandleCacheHits),
			atomic.LoadUint64(&pt.fileHandleCacheMisses),
			atomic.LoadUint64(&pt.nodeFileDiskLoads),
			atomic.LoadUint64(&pt.nodeFileReadOps),
			atomic.LoadUint64(&pt.nodeFileReadBytes),
			unsortedHits,
			avgUnsortedCount,
		)
	}

	if pt.fileHandleCache != nil {
		pt.fileHandleCache.Purge()
	}
	pt.globalNodeMu.Lock()
	if pt.globalCommitDirty {
		if len(pt.globalCommitBatch) > 0 {
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
		clear(pt.globalCommitBatch)
	}
	if pt.globalFile != nil {
		info, err := pt.globalFile.Stat()
		if err != nil {
			pt.globalNodeMu.Unlock()
			return err
		}
		payloadSize := int(info.Size()) - binary.Size(pt.globalHeader)
		if payloadSize > 0 {
			unsortedCount, inferErr := nodeFileInferUnsortedEntryCount(pt.globalHeader, payloadSize)
			if inferErr != nil {
				pt.globalNodeMu.Unlock()
				return inferErr
			}
			if unsortedCount > 0 {
				if err := pt.rewriteGlobalNodeFileLocked(); err != nil {
					pt.globalNodeMu.Unlock()
					return err
				}
			}
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
			addUint64Stat(&pt.fileNodeCacheHits, 1)
			if err := binary.Read(bytes.NewReader(hdrBuf), binary.BigEndian, &header); err != nil {
				return NodeInfo{}, false, fmt.Errorf("decode header failed: %w", err)
			}
			if header.Magic != FileNodeMagic {
				return NodeInfo{}, false, fmt.Errorf("invalid cached file node magic (got 0x%X, file=%s)", header.Magic, fileID)
			}
		} else {
			addUint64Stat(&pt.fileNodeCacheMisses, 1)
			addUint64Stat(&pt.nodeFileDiskLoads, 1)
			file, err := pt.getOrCreateFileHandle(fileID, os.O_RDWR)
			if err != nil {
				if os.IsNotExist(err) {
					return NodeInfo{}, false, nil
				}
				return NodeInfo{}, false, fmt.Errorf("open file failed: %w", err)
			}
			decoded, readErr := pt.readDecodedFileNode(fileID, file)
			if readErr != nil {
				var retryErr error
				pt.dropFileHandles(fileID)
				pt.invalidateFileNodeCache(fileID)
				decoded, retryErr = pt.readDecodedFileNodeFresh(fileID)
				if retryErr != nil {
					return NodeInfo{}, false, fmt.Errorf("%v; fresh reopen retry failed: %w", readErr, retryErr)
				}
			}
			hdrBuf = decoded.hdrBuf
			header = decoded.header
			bigBuf = decoded.bigBuf

			pt.setFileNodeCache(fileID, hdrBuf, bigBuf)
		}

		sortedCount := uint32(0)
		unsortedCount := uint32(0)
		if bigBuf != nil {
			sortedBytes := int(header.SortedEntryCount) * NodeEntrySize
			sortedSlice = bigBuf[:sortedBytes]
			unsortedSlice = bigBuf[sortedBytes:]
			sortedCount = uint32(len(sortedSlice) / NodeEntrySize)
			unsortedCount = uint32(len(unsortedSlice) / NodeEntrySize)
			if sortedCount+unsortedCount > 0 && pt.shouldScheduleGC(sortedCount, unsortedCount) {
				scheduleJob = true
				scheduleGC = func() {
					headerCopy := header
					headerCopy.SortedEntryCount = sortedCount
					headerCopy.UnsortedEntryCount = unsortedCount
					pt.maybeScheduleGC(fileID, headerCopy, sortedSlice, unsortedSlice)
				}
			}
		}
	}

	sortedCount := uint32(len(sortedSlice) / NodeEntrySize)
	unsortedCount := uint32(len(unsortedSlice) / NodeEntrySize)
	totalEntries := sortedCount + unsortedCount
	if totalEntries == 0 {
		return NodeInfo{}, false, nil
	}

	var latestAccountHit *NodeInfo
	var latestStorageHit *NodeInfo
	findSortedHit := func() *NodeInfo {
		if sortedCount == 0 || sortedSlice == nil {
			return nil
		}
		getKeyAt := func(idx uint32) []byte {
			start := int(idx) * NodeEntrySize
			keyLen := int(sortedSlice[start])
			if keyLen > MaxKeySize {
				keyLen = MaxKeySize
			}
			return sortedSlice[start+1 : start+1+keyLen]
		}

		low, high := uint32(0), sortedCount-1
		for low <= high {
			mid := (low + high) / 2
			k := getKeyAt(mid)
			cmp := bytes.Compare(k, Key)
			if cmp == 0 {
				start := int(mid) * NodeEntrySize
				dec := decodeNodeEntry(sortedSlice[start : start+NodeEntrySize])
				if isNodeInfoTombstone(dec) {
					return nil
				}
				result := dec
				return &result
			} else if cmp < 0 {
				low = mid + 1
			} else {
				if mid == 0 {
					break
				}
				high = mid - 1
			}
		}
		return nil
	}
	mergeForRead := func(base *NodeInfo, overlay *NodeInfo) NodeInfo {
		if overlay == nil {
			if base == nil {
				return NodeInfo{}
			}
			return cloneNodeInfo(*base)
		}
		if base == nil {
			return cloneNodeInfo(*overlay)
		}
		return mergeNodeInfoForAppend(*base, *overlay)
	}

	// var nodeInfo NodeInfo
	if unsortedCount > 0 && unsortedSlice != nil {
		totalSize := int(unsortedCount) * NodeEntrySize
		for i := uint32(0); i < unsortedCount; i++ {
			idx := unsortedCount - 1 - i
			offsetInBuf := int64(idx) * NodeEntrySize
			if offsetInBuf+NodeEntrySize > int64(totalSize) {
				break
			}
			dec := decodeNodeEntry(unsortedSlice[offsetInBuf : offsetInBuf+NodeEntrySize])
			if bytes.Equal(dec.key, Key) {
				addUint64Stat(&pt.fileNodeUnsortedHits, 1)
				addUint64Stat(&pt.fileNodeUnsortedSum, uint64(unsortedCount))
				if isNodeInfoTombstone(dec) {
					return NodeInfo{}, false, nil
				}
				if hasStorageInfo(dec) {
					if latestStorageHit == nil {
						tmp := dec
						latestStorageHit = &tmp
					}
					continue
				}
				if latestAccountHit == nil {
					tmp := dec
					latestAccountHit = &tmp
				}
			}
		}
	}
	sortedHit := findSortedHit()
	if latestStorageHit != nil {
		merged := mergeForRead(sortedHit, latestStorageHit)
		if latestAccountHit != nil {
			merged = mergeForRead(latestAccountHit, &merged)
		}
		return merged, true, nil
	}
	if latestAccountHit != nil {
		merged := mergeForRead(sortedHit, latestAccountHit)
		return merged, true, nil
	}

	if sortedCount > 0 && sortedSlice != nil {
		if sortedHit != nil {
			return *sortedHit, true, nil
		}
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

type decodedFileNode struct {
	hdrBuf []byte
	header FileNodeHeader
	bigBuf []byte
}

func (pt *PrefixTree) readWholeNodeFile(fileID string, file *os.File, usage diskIOUsage) ([]byte, FileNodeHeader, []byte, error) {
	var header FileNodeHeader
	if file == nil {
		return nil, header, nil, fmt.Errorf("nil file handle")
	}
	headerSize := binary.Size(header)
	info, err := file.Stat()
	if err != nil {
		return nil, header, nil, fmt.Errorf("stat node file failed: file=%s: %w", fileID, err)
	}
	if info.Size() < int64(headerSize) {
		return nil, header, nil, fmt.Errorf("invalid node file size %d for %s", info.Size(), fileID)
	}
	totalSize := int(info.Size())
	buf := make([]byte, totalSize)
	n, err := file.ReadAt(buf, 0)
	if err != nil && !(err == io.EOF && n == totalSize) {
		return nil, header, nil, fmt.Errorf("read node file failed: file=%s size=%d: %w", fileID, totalSize, err)
	}
	if n != totalSize {
		return nil, header, nil, fmt.Errorf("read node file failed: file=%s short read got %d want %d: %w", fileID, n, totalSize, io.ErrUnexpectedEOF)
	}
	if pt.db != nil {
		pt.db.addDiskRead(usage, n)
	}
	if err := binary.Read(bytes.NewReader(buf[:headerSize]), binary.BigEndian, &header); err != nil {
		return nil, header, nil, fmt.Errorf("decode header failed: file=%s: %w", fileID, err)
	}
	if header.Magic != FileNodeMagic {
		return nil, header, nil, fmt.Errorf("invalid file node magic (got 0x%X, file=%s)", header.Magic, fileID)
	}
	return buf, header, buf[headerSize:], nil
}

func (pt *PrefixTree) readDecodedFileNode(fileID string, file *os.File) (decodedFileNode, error) {
	var result decodedFileNode
	if file == nil {
		return result, fmt.Errorf("nil file handle")
	}
	headerSize := binary.Size(result.header)
	rawBuf, header, payload, err := pt.readWholeNodeFile(fileID, file, diskIOUsageNodeFileLookup)
	if err != nil {
		return result, err
	}
	addUint64Stat(&pt.nodeFileReadOps, 1)
	addUint64Stat(&pt.nodeFileReadBytes, uint64(len(rawBuf)))
	result.hdrBuf = append(result.hdrBuf[:0], rawBuf[:headerSize]...)
	result.header = header
	payloadSize := len(payload)
	if payloadSize == 0 {
		return result, nil
	}
	decodedPayload, _, _, err := decodeNodeFilePayload(result.header, payload)
	if err != nil {
		return result, fmt.Errorf("decode bulk data failed: file=%s version=%d sorted=%d unsorted=%d payload=%d: %w",
			fileID, result.header.Version, result.header.SortedEntryCount, result.header.UnsortedEntryCount, payloadSize, err)
	}
	if !result.header.sortedCompressed() {
		result.bigBuf = pt.borrowBuf(payloadSize)
		if result.bigBuf == nil {
			return result, fmt.Errorf("borrow payload buffer failed: file=%s payloadSize=%d", fileID, payloadSize)
		}
		copy(result.bigBuf, payload)
		return result, nil
	}
	result.bigBuf = decodedPayload
	return result, nil
}

func (pt *PrefixTree) readDecodedFileNodeFresh(fileID string) (decodedFileNode, error) {
	filePath := filepath.Join(pt.fileNodeDir, fileID)
	file, err := os.Open(filePath)
	if err != nil {
		return decodedFileNode{}, err
	}
	defer file.Close()
	return pt.readDecodedFileNode(fileID, file)
}

// getOrCreateFileHandle gets or creates a cached file handle for a given fileID and flag
func (pt *PrefixTree) getOrCreateFileHandle(fileID string, flag int) (*os.File, error) {
	cacheKey := fmt.Sprintf("%s|%d", fileID, flag)

	// get from cache
	if handle, ok := pt.fileHandleCache.Get(cacheKey); ok {
		addUint64Stat(&pt.fileHandleCacheHits, 1)
		return handle.(*os.File), nil
	}
	addUint64Stat(&pt.fileHandleCacheMisses, 1)

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
