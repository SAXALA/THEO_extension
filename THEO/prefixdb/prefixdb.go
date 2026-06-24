package prefixdb

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	datatypepkg "theo.local/THEO/datatype"
)

const storageMaxFileSize int64 = 1 << 30 // 1GB

const (
	segmentedStorageFlag            uint32 = 1 << 31
	segmentIndexFileName                   = "index.meta"
	bufferLogFileName                      = "buffer.log"
	bufferLogBloomBitCount          uint64 = 1 << 27
	bufferLogBloomHashCount                = 5
	bufferLogMigrationSizeThreshold int64  = 64 * 1024 * 1024
	bufferLogMigrationMaxConcurrent        = 1
	accountFolderBloomBitCount      uint64 = 1 << 20
	segmentIndexCacheThresholdBytes        = 0   // cache all decoded segment indexes
	segmentIndexCacheCapacityMiB           = 64  // total segment-index cache budget in MiB
	defaultStorageGCThreshold              = 2.0 // when chunk file size > chunkSize * threshold, trigger GC for the segment
	storageGCQueueMultiplier               = 8
)

const (
	segmentIndexCompressionMinSize  = 4 * 1024
	segmentIndexKeyStartMaxBytes    = 32
	segmentIndexFixedKeyFieldBytes  = 1 + segmentIndexKeyStartMaxBytes
	segmentIndexFlatEntryBytes      = 4 + segmentIndexFixedKeyFieldBytes
	segmentChunkStreamReadThreshold = 64 * 1024 * 1024
	segmentChunkBufferEntryLimit    = 16
	commitTagRecordSize             = 12
	segmentIndexFlatMagic           = 0x464c4958 // 'FLIX'
	segmentIndexMultiLevelMagic     = 0x4d4c4958 // 'MLIX'
	segmentIndexFormatVersion       = 3
	segmentIndexFlatVersion         = 3
)

const defaultSegmentIndexLevel2Size = 8 * 1024

const segmentIndexLevel2Pattern = "index.meta.l2.%08d"

const ()

var bufferLogSizeBucketLabels = [...]string{
	"<1KiB",
	"1-4KiB",
	"4-16KiB",
	"16-64KiB",
	"64-256KiB",
	"256KiB-1MiB",
	"1-4MiB",
	"4-16MiB",
	"16-64MiB",
	">=64MiB",
}

func bufferLogSizeBucket(size int64) int {
	switch {
	case size < 1<<10:
		return 0
	case size < 4<<10:
		return 1
	case size < 16<<10:
		return 2
	case size < 64<<10:
		return 3
	case size < 256<<10:
		return 4
	case size < 1<<20:
		return 5
	case size < 4<<20:
		return 6
	case size < 16<<20:
		return 7
	case size < 64<<20:
		return 8
	default:
		return 9
	}
}

var prefixdbLogWriter io.Writer = os.Stdout
var prefixdbDebugLogging atomic.Bool

func shouldEmitStorageMissLogForTestsOnly(err error) bool {
	if err != nil {
		return true
	}
	// Keep nil-error miss logs available to tests that assert log content,
	// but suppress them in normal binaries to avoid noisy replay output.
	if strings.HasSuffix(filepath.Base(os.Args[0]), ".test") {
		return true
	}
	return false
}

func SetPrefixDBDebugLogging(enabled bool) {
	prefixdbDebugLogging.Store(enabled)
}

func prefixdbDebugf(format string, args ...interface{}) {
	if !prefixdbDebugLogging.Load() {
		return
	}
	fmt.Fprintf(prefixdbLogWriter, "prefixdb DEBUG: "+format+"\n", args...)
}

var errSegmentIndexEntryNotFound = errors.New("segment index entry not found")

type segmentedStorageReadFailure struct {
	folderPath string
	indexFile  string
	chunkFile  string
	reason     string
}

const storageKeyTrimOffset = 33 // 'O' + 32-byte account hash

type kvPair struct {
	key []byte
	val []byte
}

func appendForwardCommitTag(dst []byte, blockID uint64) []byte {
	if blockID == 0 {
		return dst
	}
	var tag [commitTagRecordSize]byte
	writeUint64BE(tag[segmentedChunkEntryHeaderSize:], blockID)
	return append(dst, tag[:]...)
}

func appendChunkCommitTag(dst []byte, blockID uint64) []byte {
	if blockID == 0 {
		return dst
	}
	var tag [commitTagRecordSize]byte
	writeUint64BE(tag[:8], blockID)
	return append(dst, tag[:]...)
}

func forwardCommitTagBlockID(payload []byte, cursor int) (uint64, bool) {
	if cursor+commitTagRecordSize > len(payload) {
		return 0, false
	}
	if readUint16BE(payload[cursor:cursor+2]) != 0 || readUint16BE(payload[cursor+2:cursor+4]) != 0 {
		return 0, false
	}
	return readUint64BE(payload[cursor+segmentedChunkEntryHeaderSize : cursor+commitTagRecordSize]), true
}

func chunkCommitTagBlockID(payload []byte, footerEnd int) (uint64, bool) {
	if footerEnd < commitTagRecordSize || footerEnd > len(payload) {
		return 0, false
	}
	footer := payload[footerEnd-segmentedChunkEntryHeaderSize : footerEnd]
	if readUint16BE(footer[:2]) != 0 || readUint16BE(footer[2:4]) != 0 {
		return 0, false
	}
	return readUint64BE(payload[footerEnd-commitTagRecordSize : footerEnd-segmentedChunkEntryHeaderSize]), true
}

func lastForwardCommitTagBlockID(payload []byte, allowLeadingPadding bool) (uint64, bool) {
	if allowLeadingPadding && len(payload) > 0 && payload[0] == 0 {
		if blockID, ok := lastForwardCommitTagBlockIDFrom(payload, 1); ok {
			return blockID, true
		}
	}
	return lastForwardCommitTagBlockIDFrom(payload, 0)
}

func lastForwardCommitTagBlockIDFrom(payload []byte, start int) (uint64, bool) {
	cursor := start
	var last uint64
	found := false
	for cursor < len(payload) {
		if cursor+segmentedChunkEntryHeaderSize > len(payload) {
			return last, found
		}
		klen := int(readUint16BE(payload[cursor : cursor+2]))
		vlen := int(readUint16BE(payload[cursor+2 : cursor+4]))
		if klen == 0 && vlen == 0 {
			blockID, ok := forwardCommitTagBlockID(payload, cursor)
			if !ok {
				return last, found
			}
			last = blockID
			found = true
			cursor += commitTagRecordSize
			continue
		}
		cursor += segmentedChunkEntryHeaderSize
		if cursor+klen+vlen > len(payload) {
			return last, found
		}
		cursor += klen + vlen
	}
	return last, found
}

func lastChunkCommitTagBlockID(payload []byte) (uint64, bool) {
	cursor := len(payload)
	for cursor > 0 {
		if cursor < segmentedChunkEntryHeaderSize {
			return 0, false
		}
		footer := payload[cursor-segmentedChunkEntryHeaderSize : cursor]
		klen := int(readUint16BE(footer[:2]))
		vlen := int(readUint16BE(footer[2:4]))
		if klen == 0 && vlen == 0 {
			blockID, ok := chunkCommitTagBlockID(payload, cursor)
			if !ok {
				return 0, false
			}
			return blockID, true
		}
		recordSize := segmentedChunkEntryHeaderSize + klen + vlen
		if recordSize > cursor {
			return 0, false
		}
		cursor -= recordSize
	}
	return 0, false
}

type segmentChunkMeta struct {
	FileName string
	KeyStart []byte
}

const segmentedChunkEntryHeaderSize = 4 // [keyLen u16][valLen u16] trailer stored after [key][val]

type segmentIndexLayoutMode uint8

const (
	indexLayoutFlat segmentIndexLayoutMode = iota
	indexLayoutMultiLevel
)

type segmentIndexL1Entry struct {
	MetaID     uint32
	KeyStart   []byte
	ChunkCount uint32
}

type segmentIndexLayout struct {
	mode       segmentIndexLayoutMode
	entries    []segmentIndexL1Entry
	nextMetaID uint32
	flatData   []byte
}

type accountFolderFilter struct {
	mu    sync.RWMutex
	bits  []uint64
	mask  uint64
	exact map[string]struct{}
}

type bufferLogBloom struct {
	mu    sync.RWMutex
	count []uint8
	mask  uint64
}

type bufferLogEntryRef struct {
	valueOffset int64
	valueLen    int
}

type bufferLogAccountIndex struct {
	path    string
	size    int64
	modTime int64
	device  uint64
	inode   uint64
	entries map[string]bufferLogEntryRef
}

type bufferLogReadAccessInfo struct {
	size           int64
	indexLookedUp  bool
	valueReadNanos uint64
}

func bufferLogFileIdentity(info os.FileInfo) (int64, int64, uint64, uint64) {
	if info == nil {
		return 0, 0, 0, 0
	}
	size := info.Size()
	modTime := info.ModTime().UnixNano()
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat != nil {
		return size, modTime, uint64(stat.Dev), uint64(stat.Ino)
	}
	return size, modTime, 0, 0
}

func (idx *bufferLogAccountIndex) setFileIdentity(info os.FileInfo) {
	if idx == nil {
		return
	}
	idx.size, idx.modTime, idx.device, idx.inode = bufferLogFileIdentity(info)
}

func (idx *bufferLogAccountIndex) matchesFile(info os.FileInfo) bool {
	if idx == nil || info == nil {
		return false
	}
	size, modTime, device, inode := bufferLogFileIdentity(info)
	if idx.size != size {
		return false
	}
	if idx.inode != 0 || inode != 0 {
		return idx.device == device && idx.inode == inode
	}
	return idx.modTime == modTime
}

func newAccountFolderFilter(bitCount uint64) *accountFolderFilter {
	if bitCount == 0 {
		bitCount = accountFolderBloomBitCount
	}
	if bitCount&(bitCount-1) != 0 {
		pow := uint64(1)
		for pow < bitCount {
			pow <<= 1
		}
		bitCount = pow
	}
	return &accountFolderFilter{
		bits:  make([]uint64, bitCount/64),
		mask:  bitCount - 1,
		exact: make(map[string]struct{}),
	}
}

func mixAccountFolderHash(key []byte, seed uint64) uint64 {
	h := seed ^ 0x9e3779b97f4a7c15
	for _, c := range key {
		h ^= uint64(c) + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

func (f *accountFolderFilter) bloomIndexes(accountKey []byte) [3]uint64 {
	h1 := mixAccountFolderHash(accountKey, 0x100000001b3)
	h2 := mixAccountFolderHash(accountKey, 0x84222325cbf29ce4)
	return [3]uint64{h1 & f.mask, (h1 + h2) & f.mask, (h1 + 2*h2) & f.mask}
}

func (f *accountFolderFilter) add(accountKey []byte) {
	if len(accountKey) == 0 {
		return
	}
	idx := f.bloomIndexes(accountKey)
	f.mu.Lock()
	for _, bitIdx := range idx {
		word := bitIdx >> 6
		bit := bitIdx & 63
		f.bits[word] |= uint64(1) << bit
	}
	f.exact[string(accountKey)] = struct{}{}
	f.mu.Unlock()
}

func (f *accountFolderFilter) remove(accountKey []byte) {
	if len(accountKey) == 0 {
		return
	}
	f.mu.Lock()
	delete(f.exact, string(accountKey))
	f.mu.Unlock()
}

func (f *accountFolderFilter) maybeContains(accountKey []byte) bool {
	if len(accountKey) == 0 {
		return false
	}
	idx := f.bloomIndexes(accountKey)
	f.mu.RLock()
	for _, bitIdx := range idx {
		word := bitIdx >> 6
		bit := bitIdx & 63
		if (f.bits[word] & (uint64(1) << bit)) == 0 {
			f.mu.RUnlock()
			return false
		}
	}
	_, ok := f.exact[string(accountKey)]
	f.mu.RUnlock()
	return ok
}

func newBufferLogBloom(bitCount uint64) *bufferLogBloom {
	if bitCount == 0 {
		bitCount = bufferLogBloomBitCount
	}
	if bitCount&(bitCount-1) != 0 {
		pow := uint64(1)
		for pow < bitCount {
			pow <<= 1
		}
		bitCount = pow
	}
	return &bufferLogBloom{
		count: make([]uint8, bitCount),
		mask:  bitCount - 1,
	}
}

func mixBufferLogHash(accountKey, storageKey []byte, seed uint64) uint64 {
	h := seed ^ 0x9e3779b97f4a7c15
	for _, c := range accountKey {
		h ^= uint64(c) + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	}
	for _, c := range storageKey {
		h ^= uint64(c) + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

func (b *bufferLogBloom) bloomIndexes(accountKey, storageKey []byte) [bufferLogBloomHashCount]uint64 {
	h1 := mixBufferLogHash(accountKey, storageKey, 0x100000001b3)
	h2 := mixBufferLogHash(accountKey, storageKey, 0x84222325cbf29ce4)
	var idx [bufferLogBloomHashCount]uint64
	for i := 0; i < bufferLogBloomHashCount; i++ {
		idx[i] = (h1 + uint64(i)*h2) & b.mask
	}
	return idx
}

func (b *bufferLogBloom) add(accountKey, storageKey []byte) {
	if b == nil || len(accountKey) == 0 || len(storageKey) == 0 {
		return
	}
	idx := b.bloomIndexes(accountKey, storageKey)
	b.mu.Lock()
	for _, bitIdx := range idx {
		if b.count[bitIdx] < 0xff {
			b.count[bitIdx]++
		}
	}
	b.mu.Unlock()
}

func (b *bufferLogBloom) remove(accountKey, storageKey []byte) {
	if b == nil || len(accountKey) == 0 || len(storageKey) == 0 {
		return
	}
	idx := b.bloomIndexes(accountKey, storageKey)
	b.mu.Lock()
	for _, bitIdx := range idx {
		if b.count[bitIdx] > 0 {
			b.count[bitIdx]--
		}
	}
	b.mu.Unlock()
}

func (b *bufferLogBloom) maybeContains(accountKey, storageKey []byte) bool {
	if b == nil || len(accountKey) == 0 || len(storageKey) == 0 {
		return false
	}
	idx := b.bloomIndexes(accountKey, storageKey)
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, bitIdx := range idx {
		if b.count[bitIdx] == 0 {
			return false
		}
	}
	return true
}

type storageGCJob struct {
	folderPath     string
	fileName       string
	chunkBuffer    *storageGCChunkBuffer
	lastTagBlockID uint64
}

func (job storageGCJob) key() string {
	return job.folderPath + ":" + job.fileName
}

type storageGCChunkBuffer struct {
	lease *bufferLease
	data  []byte
}

type currentSegmentChunkBufferSource uint8

const (
	currentSegmentChunkBufferSourceRead currentSegmentChunkBufferSource = iota
	currentSegmentChunkBufferSourceGC
)

type currentSegmentChunkBufferEntry struct {
	lease  *bufferLease
	source currentSegmentChunkBufferSource
	lru    *list.Element
}

type currentSegmentChunkBuffer struct {
	mu             sync.RWMutex
	entries        map[string]*currentSegmentChunkBufferEntry
	readLRU        *list.List
	maxReadEntries int
	readEntryCount int
}

func newCurrentSegmentChunkBuffer() *currentSegmentChunkBuffer {
	return &currentSegmentChunkBuffer{
		entries:        make(map[string]*currentSegmentChunkBufferEntry),
		readLRU:        list.New(),
		maxReadEntries: segmentChunkBufferEntryLimit,
	}
}

func segmentChunkBufferKey(folderPath string, fileName string) string {
	return folderPath + "\x00" + fileName
}

func (c *currentSegmentChunkBuffer) GetByPath(folderPath string, fileName string) ([]byte, bool) {
	lease, ok := c.GetLeaseByPath(folderPath, fileName)
	if !ok || lease == nil {
		return nil, false
	}
	buf := cloneBytes(lease.Bytes())
	lease.Release()
	if len(buf) == 0 {
		return nil, false
	}
	return buf, true
}

func (c *currentSegmentChunkBuffer) PeekByPath(folderPath string, fileName string) ([]byte, bool) {
	lease, ok := c.peekLeaseByPath(folderPath, fileName)
	if !ok || lease == nil {
		return nil, false
	}
	buf := cloneBytes(lease.Bytes())
	lease.Release()
	if len(buf) == 0 {
		return nil, false
	}
	return buf, true
}

func (c *currentSegmentChunkBuffer) SetByPath(folderPath string, fileName string, buf []byte) {
	if c == nil {
		return
	}
	if len(buf) == 0 {
		c.invalidateByPath(folderPath, fileName)
		return
	}
	leaseBuf := getDataBuffer(len(buf))
	copy(leaseBuf, buf)
	lease := newBufferLease(leaseBuf[:len(buf)])
	c.SetReadLeaseByPath(folderPath, fileName, lease)
	lease.Release()
}

func (c *currentSegmentChunkBuffer) GetLeaseByPath(folderPath string, fileName string) (*bufferLease, bool) {
	if c == nil || folderPath == "" || fileName == "" {
		return nil, false
	}
	key := segmentChunkBufferKey(folderPath, fileName)
	c.mu.Lock()
	entry := c.entries[key]
	if entry != nil && entry.source == currentSegmentChunkBufferSourceRead {
		c.touchReadEntryLocked(entry)
	}
	var lease *bufferLease
	if entry != nil && entry.lease != nil {
		lease = entry.lease.Retain()
	}
	c.mu.Unlock()
	if lease == nil {
		return nil, false
	}
	return lease, true
}

func (c *currentSegmentChunkBuffer) peekLeaseByPath(folderPath string, fileName string) (*bufferLease, bool) {
	if c == nil || folderPath == "" || fileName == "" {
		return nil, false
	}
	key := segmentChunkBufferKey(folderPath, fileName)
	c.mu.RLock()
	entry := c.entries[key]
	var lease *bufferLease
	if entry != nil && entry.lease != nil {
		lease = entry.lease.Retain()
	}
	c.mu.RUnlock()
	if lease == nil {
		return nil, false
	}
	return lease, true
}

func (c *currentSegmentChunkBuffer) SetReadLeaseByPath(folderPath string, fileName string, lease *bufferLease) {
	if c == nil || folderPath == "" || fileName == "" || lease == nil || len(lease.Bytes()) == 0 {
		return
	}
	key := segmentChunkBufferKey(folderPath, fileName)
	retained := lease.Retain()
	var stale *bufferLease
	var evicted *bufferLease
	c.mu.Lock()
	entry := c.entries[key]
	created := false
	if entry == nil {
		entry = &currentSegmentChunkBufferEntry{}
		c.entries[key] = entry
		created = true
	}
	if entry.source != currentSegmentChunkBufferSourceRead || entry.lru == nil {
		if entry.lru != nil {
			c.readLRU.Remove(entry.lru)
			entry.lru = nil
		}
		if !created && entry.source == currentSegmentChunkBufferSourceRead && c.readEntryCount > 0 {
			c.readEntryCount--
		}
		entry.source = currentSegmentChunkBufferSourceRead
		entry.lru = c.readLRU.PushFront(key)
		c.readEntryCount++
	} else {
		c.touchReadEntryLocked(entry)
	}
	stale = entry.lease
	entry.lease = retained
	if c.maxReadEntries > 0 && c.readEntryCount > c.maxReadEntries {
		evicted = c.evictOneReadLocked(key)
	}
	c.mu.Unlock()
	if stale != nil {
		stale.Release()
	}
	if evicted != nil {
		evicted.Release()
	}
}

func (c *currentSegmentChunkBuffer) SetGCLeaseByPath(folderPath string, fileName string, lease *bufferLease) {
	if c == nil || folderPath == "" || fileName == "" || lease == nil || len(lease.Bytes()) == 0 {
		return
	}
	key := segmentChunkBufferKey(folderPath, fileName)
	retained := lease.Retain()
	var stale *bufferLease
	c.mu.Lock()
	entry := c.entries[key]
	created := false
	if entry == nil {
		entry = &currentSegmentChunkBufferEntry{}
		c.entries[key] = entry
		created = true
	}
	if !created && entry.source == currentSegmentChunkBufferSourceRead {
		if entry.lru != nil {
			c.readLRU.Remove(entry.lru)
			entry.lru = nil
		}
		if c.readEntryCount > 0 {
			c.readEntryCount--
		}
	}
	stale = entry.lease
	entry.lease = retained
	entry.source = currentSegmentChunkBufferSourceGC
	entry.lru = nil
	c.mu.Unlock()
	if stale != nil {
		stale.Release()
	}
}

func (c *currentSegmentChunkBuffer) UpdateExistingByPath(folderPath string, fileName string, lease *bufferLease) bool {
	if c == nil || folderPath == "" || fileName == "" || lease == nil || len(lease.Bytes()) == 0 {
		return false
	}
	key := segmentChunkBufferKey(folderPath, fileName)
	retained := lease.Retain()
	var stale *bufferLease
	updated := false
	c.mu.Lock()
	entry := c.entries[key]
	if entry != nil {
		stale = entry.lease
		entry.lease = retained
		updated = true
		if entry.source == currentSegmentChunkBufferSourceRead {
			c.touchReadEntryLocked(entry)
		}
	}
	c.mu.Unlock()
	if !updated {
		retained.Release()
		return false
	}
	if stale != nil {
		stale.Release()
	}
	return true
}

func (c *currentSegmentChunkBuffer) IsReadEntryByPath(folderPath string, fileName string) bool {
	if c == nil || folderPath == "" || fileName == "" {
		return false
	}
	key := segmentChunkBufferKey(folderPath, fileName)
	c.mu.RLock()
	entry := c.entries[key]
	c.mu.RUnlock()
	return entry != nil && entry.source == currentSegmentChunkBufferSourceRead && entry.lru != nil && entry.lease != nil
}

func (c *currentSegmentChunkBuffer) touchReadEntryLocked(entry *currentSegmentChunkBufferEntry) {
	if c == nil || entry == nil || entry.source != currentSegmentChunkBufferSourceRead {
		return
	}
	if entry.lru == nil {
		return
	}
	c.readLRU.MoveToFront(entry.lru)
}

func (c *currentSegmentChunkBuffer) evictOneReadLocked(skipKey string) *bufferLease {
	if c == nil || c.readLRU == nil || c.readEntryCount == 0 {
		return nil
	}
	for elem := c.readLRU.Back(); elem != nil; elem = elem.Prev() {
		key, _ := elem.Value.(string)
		if key == "" || key == skipKey {
			continue
		}
		entry := c.entries[key]
		if entry == nil || entry.source != currentSegmentChunkBufferSourceRead || entry.lease == nil {
			c.readLRU.Remove(elem)
			continue
		}
		delete(c.entries, key)
		c.readLRU.Remove(elem)
		entry.lru = nil
		if c.readEntryCount > 0 {
			c.readEntryCount--
		}
		return entry.lease
	}
	return nil
}

func (c *currentSegmentChunkBuffer) RemoveGCEntriesByPath(folderPath string, fileNames []string) {
	if c == nil || folderPath == "" || len(fileNames) == 0 {
		return
	}
	var stale []*bufferLease
	c.mu.Lock()
	for _, fileName := range fileNames {
		key := segmentChunkBufferKey(folderPath, fileName)
		entry := c.entries[key]
		if entry == nil || entry.source != currentSegmentChunkBufferSourceGC {
			continue
		}
		if entry.lease != nil {
			stale = append(stale, entry.lease)
		}
		delete(c.entries, key)
	}
	c.mu.Unlock()
	for _, lease := range stale {
		lease.Release()
	}
}

func (c *currentSegmentChunkBuffer) PromoteGCEntriesToReadByPath(folderPath string, fileNames []string) {
	if c == nil || folderPath == "" || len(fileNames) == 0 {
		return
	}
	var evicted []*bufferLease
	c.mu.Lock()
	for _, fileName := range fileNames {
		key := segmentChunkBufferKey(folderPath, fileName)
		entry := c.entries[key]
		if entry == nil || entry.source != currentSegmentChunkBufferSourceGC {
			continue
		}
		entry.source = currentSegmentChunkBufferSourceRead
		entry.lru = c.readLRU.PushFront(key)
		c.readEntryCount++
	}
	for c.maxReadEntries > 0 && c.readEntryCount > c.maxReadEntries {
		if lease := c.evictOneReadLocked(""); lease != nil {
			evicted = append(evicted, lease)
			continue
		}
		break
	}
	c.mu.Unlock()
	for _, lease := range evicted {
		lease.Release()
	}
}

func (c *currentSegmentChunkBuffer) ContainsByPath(folderPath string, fileName string) bool {
	if c == nil || folderPath == "" || fileName == "" {
		return false
	}
	key := segmentChunkBufferKey(folderPath, fileName)
	c.mu.RLock()
	entry := c.entries[key]
	c.mu.RUnlock()
	return entry != nil && entry.lease != nil && len(entry.lease.Bytes()) > 0
}

func (c *currentSegmentChunkBuffer) invalidateByPath(folderPath string, fileName string) {
	if c == nil || folderPath == "" || fileName == "" {
		return
	}
	key := segmentChunkBufferKey(folderPath, fileName)
	var stale *bufferLease
	c.mu.Lock()
	if entry := c.entries[key]; entry != nil {
		if entry.source == currentSegmentChunkBufferSourceRead && entry.lru != nil {
			c.readLRU.Remove(entry.lru)
			entry.lru = nil
			if c.readEntryCount > 0 {
				c.readEntryCount--
			}
		}
		stale = entry.lease
	}
	delete(c.entries, key)
	c.mu.Unlock()
	if stale != nil {
		stale.Release()
	}
}

func (c *currentSegmentChunkBuffer) Close() {
	if c == nil {
		return
	}
	var stale []*bufferLease
	c.mu.Lock()
	for key, entry := range c.entries {
		if entry != nil && entry.lease != nil {
			stale = append(stale, entry.lease)
		}
		delete(c.entries, key)
	}
	c.mu.Unlock()
	for i := range stale {
		stale[i].Release()
	}
}

func newStorageGCChunkBufferFromLease(lease *bufferLease) *storageGCChunkBuffer {
	if lease == nil {
		return nil
	}
	return &storageGCChunkBuffer{lease: lease}
}

func newStorageGCChunkBufferFromBytes(data []byte) *storageGCChunkBuffer {
	if len(data) == 0 {
		return nil
	}
	buf := getDataBuffer(len(data))
	copy(buf, data)
	return &storageGCChunkBuffer{lease: newBufferLease(buf[:len(data)])}
}

func (b *storageGCChunkBuffer) Bytes() []byte {
	if b == nil {
		return nil
	}
	if b.lease != nil {
		return b.lease.Bytes()
	}
	return b.data
}

func (b *storageGCChunkBuffer) Release() {
	if b == nil {
		return
	}
	if b.lease != nil {
		b.lease.Release()
		b.lease = nil
	}
	b.data = nil
}

type AccountType int

const (
	NormalAccount AccountType = iota
	ContractAccount
)

type storageOpBuffer struct {
	accountKey   string
	storagekvs   []kvPair
	pendingCount int
}

type trieStorageGetBreakdownStepStats struct {
	cacheCount   uint64
	cacheNanos   uint64
	noCacheCount uint64
	noCacheNanos uint64
}

type prefixDBNotFoundStats struct {
	accountNodeMissing       uint64
	storageMissingParent     uint64
	storageOverlayTombstone  uint64
	storageCacheTombstone    uint64
	storageBufferTombstone   uint64
	storageAccountMissing    uint64
	storageSegmentMissing    uint64
	storageSegmentIndexEmpty uint64
	storageSegmentIndexMiss  uint64
	storageChunkMetaMiss     uint64
	storageChunkKVNotFound   uint64
	storageChunkTombstone    uint64
	storageChunkReadFailed   uint64
	storageChunkEmpty        uint64
	storageChunkCorrupted    uint64
	storageSegmentIndexRead  uint64
	storageNotFound          uint64
}

func (s *prefixDBNotFoundStats) add(reason string) {
	if !analysisStatsEnabled || s == nil {
		return
	}
	switch reason {
	case "account-not-found":
		atomic.AddUint64(&s.accountNodeMissing, 1)
	case "missing-parent-account":
		atomic.AddUint64(&s.storageMissingParent, 1)
	case "storage-overlay-tombstone":
		atomic.AddUint64(&s.storageOverlayTombstone, 1)
	case "storage-cache-tombstone":
		atomic.AddUint64(&s.storageCacheTombstone, 1)
	case "buffer-log-tombstone":
		atomic.AddUint64(&s.storageBufferTombstone, 1)
	case "account-storage-missing":
		atomic.AddUint64(&s.storageAccountMissing, 1)
	case "segment-index-empty":
		atomic.AddUint64(&s.storageSegmentIndexEmpty, 1)
	case "segment-index-entry-not-found":
		atomic.AddUint64(&s.storageSegmentIndexMiss, 1)
	case "segment-chunk-meta-not-found":
		atomic.AddUint64(&s.storageChunkMetaMiss, 1)
	case "segment-chunk-key-not-found":
		atomic.AddUint64(&s.storageChunkKVNotFound, 1)
	case "segment-chunk-tombstone":
		atomic.AddUint64(&s.storageChunkTombstone, 1)
	case "segment-chunk-read-failed":
		atomic.AddUint64(&s.storageChunkReadFailed, 1)
	case "segment-chunk-empty":
		atomic.AddUint64(&s.storageChunkEmpty, 1)
	case "segment-chunk-corrupted":
		atomic.AddUint64(&s.storageChunkCorrupted, 1)
	case "segment-index-read-failed":
		atomic.AddUint64(&s.storageSegmentIndexRead, 1)
	case "storage-not-found":
		atomic.AddUint64(&s.storageNotFound, 1)
	default:
		atomic.AddUint64(&s.storageSegmentMissing, 1)
	}
}

type trieGetBreakdownStats struct {
	indexLocate trieStorageGetBreakdownStepStats
	ioRead      trieStorageGetBreakdownStepStats
	search      trieStorageGetBreakdownStepStats
}

type segmentIndexLookupSource uint8

const (
	segmentIndexLookupSourceNoCache segmentIndexLookupSource = iota
	segmentIndexLookupSourceL1Cache
	segmentIndexLookupSourceL2Cache
)

func (s segmentIndexLookupSource) fromCache() bool {
	return s == segmentIndexLookupSourceL1Cache || s == segmentIndexLookupSourceL2Cache
}

type trieStorageSegmentIndexLayerStats struct {
	l2CacheCount uint64
	l2CacheNanos uint64
}

type trieStoragePrefetchStats struct {
	addCount    uint64
	addBytes    uint64
	addNilCount uint64
	hitCount    uint64
	hitBytes    uint64
	hitNilCount uint64
	clearCount  uint64
}

type cacheMissCostStats struct {
	count        uint64
	ioOps        uint64
	ioBytes      uint64
	ioNanos      uint64
	computeNanos uint64
}

type cacheMissCostTracker struct {
	ioOps   uint64
	ioBytes uint64
	ioNanos uint64
}

type bufferLogSizeAccessBucketStats struct {
	lookups        uint64
	bloomRejects   uint64
	indexLookups   uint64
	hits           uint64
	tombstoneHits  uint64
	misses         uint64
	errors         uint64
	hitBytes       uint64
	valueReadNanos uint64
}

const storagePrefetchStatsSampleMask uint32 = 0xff

func addUint64Stat(dst *uint64, delta uint64) {
	if !analysisStatsEnabled || dst == nil || delta == 0 {
		return
	}
	atomic.AddUint64(dst, delta)
}

func addDurationStat(count *uint64, nanos *uint64, duration time.Duration) {
	if !analysisStatsEnabled || count == nil || nanos == nil {
		return
	}
	atomic.AddUint64(count, 1)
	if duration > 0 {
		atomic.AddUint64(nanos, uint64(duration))
	}
}

func loadUint64Stat(src *uint64) uint64 {
	if !analysisStatsEnabled || src == nil {
		return 0
	}
	return atomic.LoadUint64(src)
}

func averageMicros(nanos uint64, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return float64(nanos) / float64(count) / 1000.0
}

func averageBytes(bytes uint64, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return float64(bytes) / float64(count)
}

func shouldSampleStoragePrefetchKey(cacheKey string) bool {
	if !analysisStatsEnabled || cacheKey == "" {
		return false
	}
	last := len(cacheKey) - 1
	hash := uint32(len(cacheKey))
	hash = hash*16777619 ^ uint32(cacheKey[0])
	hash = hash*16777619 ^ uint32(cacheKey[len(cacheKey)/2])
	hash = hash*16777619 ^ uint32(cacheKey[last])
	return hash&storagePrefetchStatsSampleMask == 0
}

func storagePrefetchStatsSampleDenominator() uint64 {
	return uint64(storagePrefetchStatsSampleMask) + 1
}

type PrefixDB struct {
	prefixTree  *PrefixTree
	accountFile *os.File
	// slotFile    *os.File

	nodeCache    *NodeCache
	sharedCache  *sharedByteCache
	accountBatch *WriteBatch
	// triePath             string       // path to the prefix tree file
	// hashIndex  hashIndex to aviod hash collision
	writeMutex sync.Mutex // mutex for writeCommit

	// segmentIndexMu protects in-memory segment index caches/layouts (they are mutated on reads).
	segmentIndexMu sync.Mutex
	// segmentIndexFolderLocks serializes segment index operations per folder path.
	segmentIndexFolderLocksMu sync.Mutex
	segmentIndexFolderLocks   map[string]*segmentIndexFolderLock

	storageDir       string
	storageFileMu    sync.Mutex
	storageCurFile   *os.File
	storageCurFileID uint32
	storageCurSize   int64
	fileHandleCache  *fileHandleCache
	storageBuf       storageOpBuffer
	segmentDirSeq    uint32

	// a index file maybe accessed frequently
	storageIndexFolderPath string
	storageIndexMetas      []segmentChunkMeta
	storageIndexCache      *segmentIndexCache
	storageIndexReusable   bool
	storageIndexArena      []byte
	storageGetCacheCount   int

	storageIndexPartialFolderPath string
	storageIndexPartialMetaID     uint32
	storageIndexPartialMetas      []segmentChunkMeta
	storageIndexPartialReusable   bool
	storageIndexPartialArena      []byte

	storageGCQueue    chan storageGCJob
	storageGCInFlight map[string]struct{}
	storageGCStop     chan struct{}
	storageGCWait     sync.WaitGroup
	storageGCMu       sync.Mutex
	gcWorkerLimiter   chan struct{}
	accountFolderSet  *accountFolderFilter

	nodeFileGCUnsortedRatioThreshold float64
	gcWorkers                        int
	nodeFileSortedCompression        bool
	segmentIndexCompression          bool

	storageCache                    *storageValueCache
	currentSegmentChunkBuffer       *currentSegmentChunkBuffer
	stroageCacheSizeLimit           uint64
	storageChunkSize                int
	segmentedChunkHardLimit         int // hard cap for individual chunk files
	segmentIndexLevel2Size          int // target byte budget per L2 index shard; defaults to storageChunkSize
	segmentIndexMultiLevelThreshold int // serialized index bytes threshold for switching to multi-level layout

	// storageBatcher enables BatchPut/BatchCommit for storage-only kvs.
	storageBatch *storageBatcher
	// ParentKeyResolver, when set, is used to resolve a parent account key from
	// a storage key. It is intended to be set by the owning `theo.Database`
	// so that PrefixDB can defer resolution to the higher-level store.
	ParentKeyResolver func([]byte) []byte
	// for debug
	totalOps     uint64
	cachedOps    uint64
	timeOnRead   time.Duration
	readCount    uint64
	sortedOps    int
	GCCount      uint64
	GCWriteBytes uint64

	commitOldKVReadCount uint64
	commitOldKVReadBytes uint64
	totalReadBytes       uint64
	getReadReqCount      uint64
	getReadBytesSum      uint64

	trieStorageCachePairs uint64
	trieStorageCacheBytes uint64
	trieStorageLogPairs   uint64
	trieStorageLogBytes   uint64

	trieAccountGetStats               trieGetBreakdownStats
	trieStorageAccountEntryStats      trieStorageGetBreakdownStepStats
	trieStorageSegmentIndexStats      trieStorageGetBreakdownStepStats
	trieStorageSegmentIndexLayerStats trieStorageSegmentIndexLayerStats
	trieStoragePrefetchStats          trieStoragePrefetchStats
	trieStorageKVStats                trieStorageGetBreakdownStepStats
	accountDataMissStats              cacheMissCostStats
	storageDataMissStats              cacheMissCostStats
	notFoundStats                     prefixDBNotFoundStats
	storagePrefetchMu                 sync.Mutex
	storagePrefetchTrackedCount       uint64
	storagePrefetchPending            map[string]struct{}

	bufferLogMu                     sync.Mutex
	bufferLogBloomLoadedAccounts    map[string]struct{}
	bufferLogBloom                  *bufferLogBloom
	bufferLogIndexMu                sync.RWMutex
	bufferLogIndexes                map[string]*bufferLogAccountIndex
	bufferLogCacheMu                sync.Mutex
	bufferLogCachePath              string
	bufferLogCacheSize              int64
	bufferLogCacheModTime           int64
	bufferLogCacheBuf               []byte
	bufferLogLookupCount            uint64
	bufferLogBloomRejectCount       uint64
	bufferLogHitCount               uint64
	bufferLogHitBytes               uint64
	bufferLogTombstoneHitCount      uint64
	bufferLogMissCount              uint64
	bufferLogErrorCount             uint64
	bufferLogSizeAccessStats        [len(bufferLogSizeBucketLabels)]bufferLogSizeAccessBucketStats
	bufferLogAppendAccountCount     uint64
	bufferLogAppendKVCount          uint64
	bufferLogAppendBytes            uint64
	bufferLogBloomLoadCount         uint64
	bufferLogBloomLoadKVCount       uint64
	bufferLogMigrationCount         uint64
	bufferLogMigratedKVCount        uint64
	bufferLogLookupNanos            uint64
	bufferLogHitLookupCount         uint64
	bufferLogHitLookupNanos         uint64
	bufferLogMissLookupCount        uint64
	bufferLogMissLookupNanos        uint64
	bufferLogBloomCheckCount        uint64
	bufferLogBloomCheckNanos        uint64
	bufferLogIndexLookupCount       uint64
	bufferLogIndexLookupNanos       uint64
	bufferLogIndexBuildCount        uint64
	bufferLogIndexBuildNanos        uint64
	bufferLogValueReadCount         uint64
	bufferLogValueReadNanos         uint64
	bufferLogFullReadCount          uint64
	bufferLogFullReadNanos          uint64
	bufferLogMigrationTotalNanos    uint64
	bufferLogMigrationLockCount     uint64
	bufferLogMigrationLockNanos     uint64
	bufferLogMigrationReadCount     uint64
	bufferLogMigrationReadNanos     uint64
	bufferLogMigrationReadBytes     uint64
	bufferLogMigrationDiskReadBytes uint64
	bufferLogMigrationSortCount     uint64
	bufferLogMigrationSortNanos     uint64
	bufferLogMigrationPrepCount     uint64
	bufferLogMigrationPrepNanos     uint64
	bufferLogMigrationWriteCount    uint64
	bufferLogMigrationWriteNanos    uint64
	bufferLogMigrationWriteOps      uint64
	bufferLogMigrationWriteBytes    uint64
	bufferLogMigrationFinishCount   uint64
	bufferLogMigrationFinishNanos   uint64
	bufferLogMigrationWaitGroup     sync.WaitGroup
	bufferLogMigrationLimiter       chan struct{}
	bufferLogMigrationPending       map[string]struct{}

	// nodeCache access stats (read path)
	nodeCacheLookups uint64
	nodeCacheHits    uint64
	nodeCacheMisses  uint64
	// Served means we returned from nodeCache without consulting PrefixTree/NodeFile.
	nodeCacheServed uint64
	// NodeFile (PrefixTree) access after nodeCache lookup.
	nodeCacheToNodeFile            uint64
	nodeCacheMissToNodeFile        uint64
	nodeCacheHitFallbackToNodeFile uint64
	diskIOStats                    [diskIOUsageCount]diskIOCounters

	testSegmentedReadHook    func(folderPath string, meta segmentChunkMeta)
	testBuildStoragePlanHook func(accountKey string)
}

type diskIOUsage uint8

const (
	diskIOUsageAccountData diskIOUsage = iota
	diskIOUsageNodeFileLookup
	diskIOUsageNodeFileMutation
	diskIOUsageNodeFileGC
	diskIOUsageStorageCommonLogs
	diskIOUsageStorageBufferLogs
	diskIOUsageStorageSeparatedLogs
	diskIOUsageStorageGC
	diskIOUsageStorageSegmentIndex
	diskIOUsageCount
)

var diskIOUsageNames = [...]string{
	"account-data",
	"nodefile-lookup",
	"nodefile-mutation",
	"nodefile-gc",
	"storage-common-logs",
	"storage-buffer-logs",
	"storage-separated-logs",
	"storage-gc",
	"storage-segment-index",
}

type diskIOCounters struct {
	readOps    uint64
	readBytes  uint64
	writeOps   uint64
	writeBytes uint64
}

// SerializedTrieNode
type SerializedTrieNode struct {
	Path        string
	IsLeaf      bool
	SlotIndices []int
	Offset      int64
}

type segmentIndexFolderLock struct {
	mu   sync.RWMutex
	refs int
	gen  uint64
}

/*
*
  - NewPrefixDB creates a new PrefixDB instance.
  - It initializes the necessary files, directories, caches, and workers based on the provided configuration.
    the storageChunkFileSize is in bytes, and cacheSize is in bytes.
*/
func NewPrefixDB(dirpath string, storageChunkFileSize int, totalCacheSizeMiB int, storageGetCacheCount int) (*PrefixDB, error) {
	return NewPrefixDBWithRuntimeOptions(dirpath, storageChunkFileSize, totalCacheSizeMiB, storageGetCacheCount, 0, 0, 0, false, false, 0)
}

func NewPrefixDBWithRuntimeOptions(dirpath string, storageChunkFileSize int, totalCacheSizeMiB int, storageGetCacheCount int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool, fileHandleCacheSize int) (*PrefixDB, error) {
	fmt.Println(dirpath + " prefixDB Initializing...")
	SetPrefixDBDebugLogging(false)
	defaultCfg := DefaultConfig(dirpath)
	// Try to load config from config.json in dirpath
	configPath := filepath.Join(dirpath, "config.json")
	cfg, err := LoadConfig(configPath)
	if err != nil {
		// If config file doesn't exist or fails to load, use default config
		cfg = defaultCfg
	} else {
		// If BaseDir is not set in config, use dirpath
		if cfg.BaseDir == "" {
			cfg.BaseDir = dirpath
		}
		if cfg.AccountDir == "" {
			cfg.AccountDir = defaultCfg.AccountDir
		}
		if cfg.StorageDir == "" {
			cfg.StorageDir = defaultCfg.StorageDir
		}
		if cfg.NodeFileGCUnsortedRatioThreshold <= 0 {
			cfg.NodeFileGCUnsortedRatioThreshold = defaultCfg.NodeFileGCUnsortedRatioThreshold
		}
		if cfg.GCWorkers == 0 {
			cfg.GCWorkers = cfg.NodeFileGCWorkers
		}
		if cfg.StorageGCThreshold == 0 {
			cfg.StorageGCThreshold = defaultCfg.StorageGCThreshold
		}
	}
	if storageGCThreshold > 0 {
		cfg.StorageGCThreshold = storageGCThreshold
	}

	resolvedStorageGCThreshold := sanitizeStorageGCThreshold(cfg.StorageGCThreshold)

	// Ensure base directory exists
	if err := os.MkdirAll(cfg.BaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base dir: %v", err)
	}

	resolvedNodeFileSortedCompression := cfg.NodeFileSortedCompression || nodeFileSortedCompression
	resolvedSegmentIndexCompression := cfg.SegmentIndexCompression || segmentIndexCompression

	// Resolve paths
	accountFilePath := resolvePath(cfg.BaseDir, cfg.AccountDir)
	storageDir := resolvePath(cfg.BaseDir, cfg.StorageDir)

	// Ensure directories exist
	if err := os.MkdirAll(filepath.Dir(accountFilePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create account dir: %v", err)
	}
	accountFile, err := os.OpenFile(accountFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, errors.New("failed to open normal account file")
	}

	resolvedSegmentIndexLevel2Size := resolveSegmentIndexLevel2Size(storageChunkFileSize)

	db := &PrefixDB{
		accountFile:                      accountFile,
		writeMutex:                       sync.Mutex{},
		segmentIndexFolderLocks:          make(map[string]*segmentIndexFolderLock),
		fileHandleCache:                  getGlobalFileHandleCache(fileHandleCacheSize),
		accountFolderSet:                 newAccountFolderFilter(accountFolderBloomBitCount),
		storageDir:                       storageDir,
		storageGetCacheCount:             storageGetCacheCount,
		storageChunkSize:                 storageChunkFileSize,
		segmentedChunkHardLimit:          computeSegmentedChunkHardLimit(storageChunkFileSize, resolvedStorageGCThreshold),
		segmentIndexLevel2Size:           resolvedSegmentIndexLevel2Size,
		segmentIndexMultiLevelThreshold:  resolveSegmentIndexMultiLevelThreshold(resolvedSegmentIndexLevel2Size),
		nodeFileGCUnsortedRatioThreshold: cfg.NodeFileGCUnsortedRatioThreshold,
		gcWorkers:                        cfg.GCWorkers,
		nodeFileSortedCompression:        resolvedNodeFileSortedCompression,
		segmentIndexCompression:          resolvedSegmentIndexCompression,
		bufferLogBloomLoadedAccounts:     make(map[string]struct{}),
		bufferLogBloom:                   newBufferLogBloom(bufferLogBloomBitCount),
		bufferLogIndexes:                 make(map[string]*bufferLogAccountIndex),
		bufferLogMigrationLimiter:        make(chan struct{}, bufferLogMigrationMaxConcurrent),
		bufferLogMigrationPending:        make(map[string]struct{}),
	}
	if nodeFileGCRatioThreshold > 0 {
		db.nodeFileGCUnsortedRatioThreshold = nodeFileGCRatioThreshold
	}
	if gcWorkers > 0 {
		db.gcWorkers = gcWorkers
	}
	db.gcWorkerLimiter = make(chan struct{}, sanitizePrefixTreeGCWorkerCount(db.gcWorkers))

	db.accountBatch = NewWriteBatch(db)
	sharedCacheBudgetMiB := segmentIndexCacheCapacityMiB
	if totalCacheSizeMiB > 0 {
		sharedCacheBudgetMiB = totalCacheSizeMiB
	}
	db.stroageCacheSizeLimit = uint64(sharedCacheBudgetMiB) * 1024 * 1024
	if err := os.MkdirAll(db.storageDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage dir: %v", err)
	}
	if err := db.openOrCreateStorageFile(); err != nil {
		return nil, fmt.Errorf("failed to init storage shard: %v", err)
	}
	if err := db.primeAccountFolderSetFromStorageDir(); err != nil {
		return nil, fmt.Errorf("failed to initialize account folder set: %v", err)
	}

	sharedCache := newSharedByteCache(db.stroageCacheSizeLimit)
	db.sharedCache = sharedCache

	db.nodeCache = newSharedNodeCache(sharedCache)

	prefixTree, err := NewPrefixTree(db, dirpath)
	if err != nil {
		return nil, fmt.Errorf("failed to create prefix tree: %v", err)
	}

	db.prefixTree = prefixTree

	db.storageIndexCache = newSharedSegmentIndexCache(sharedCache)
	db.storageCache = newSharedStorageValueCache(sharedCache)
	db.currentSegmentChunkBuffer = newCurrentSegmentChunkBuffer()

	db.startStorageGCWorker()

	db.initStorageBatcher()

	fmt.Println(dirpath + " prefixDB Initialized.")
	return db, nil
}

// GCWorkerCount returns the effective worker count used by PrefixDB GC.
func (db *PrefixDB) GCWorkerCount() int {
	if db == nil {
		return 0
	}
	return sanitizePrefixTreeGCWorkerCount(db.gcWorkers)
}

func (db *PrefixDB) getAccount(key []byte) ([]byte, bool, error) {
	readBefore := loadUint64Stat(&db.totalReadBytes)
	defer db.finishGetReadStats(readBefore)
	cacheKey := string(key)
	useNodeCache := !db.shouldBypassNodeCache(key)

	if db.accountBatch != nil {
		if value, _, ok := db.accountBatch.get(key); ok {
			return value, true, nil
		}
	}

	cacheLookupStart := time.Now()
	if useNodeCache {
		if entry, ok := db.nodeCache.Get(cacheKey); ok && entry.Value != nil {
			recordTrieStorageGetBreakdownStep(&db.trieAccountGetStats.indexLocate, true, time.Since(cacheLookupStart))
			recordTrieStorageGetBreakdownStep(&db.trieAccountGetStats.ioRead, true, 0)
			recordTrieStorageGetBreakdownStep(&db.trieAccountGetStats.search, true, 0)
			return entry.Value, true, nil
		}
	}
	var missTracker *cacheMissCostTracker
	if useNodeCache {
		missStart := time.Now()
		missTracker = &cacheMissCostTracker{}
		defer func() {
			recordCacheMissCost(&db.accountDataMissStats, time.Since(missStart), missTracker)
		}()
	}

	node, err := db.getAccountNodeWithBreakdown(key, &db.trieAccountGetStats)
	if err != nil {
		db.logAccountKVReadFailure(key, 0, 0, "load-account-node", err)
		return nil, false, err
	}
	if node == nil {
		db.notFoundStats.add("account-not-found")
		db.logAccountKVReadFailure(key, 0, 0, "account-not-found", nil)
		return nil, false, nil
	}
	ioReadStart := time.Now()
	value, err := db.readFromFileWithTracker(node.accountOffset, node.accountSize, missTracker)
	recordTrieStorageGetBreakdownStep(&db.trieAccountGetStats.ioRead, false, time.Since(ioReadStart))
	if err != nil {
		db.logAccountKVReadFailure(key, node.accountOffset, node.accountSize, "read-account-file", err)
		return nil, false, err
	}

	if useNodeCache {
		db.nodeCache.Put(NodeCacheEntry{
			Key:           cacheKey,
			Value:         value,
			AccountOffset: node.accountOffset,
			AccountSize:   node.accountSize,
			StorageInfo: StorageInfo{
				storageFileID: node.storageFileID,
				storageOffset: node.storageOffset,
				storageSize:   node.storageSize,
			},
		})
	}

	return value, true, nil
}

func (db *PrefixDB) Get(dataType datatypepkg.DataType, key []byte, accountKey []byte) ([]byte, bool, error) {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.getAccount(key)
	case datatypepkg.TrieNodeStorageDataType:
		return db.getStorage(key, accountKey)
	default:
		return nil, false, errors.New("unknown data type")
	}
}

func (db *PrefixDB) getStorage(key []byte, accountKey []byte) ([]byte, bool, error) {
	readBefore := loadUint64Stat(&db.totalReadBytes)
	defer db.finishGetReadStats(readBefore)

	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		db.logStorageKVReadFailure(key, accountKey, "normalize-storage-key", err)
		return nil, false, err
	}

	if accountKey == nil {
		db.notFoundStats.add("missing-parent-account")
		db.logStorageKVReadFailure(storageKey, nil, "missing-parent-account", nil)
		return nil, false, nil
	}

	if v, present := db.batchGetOverlayNormalized(storageKey, accountKey); present {
		if v == nil {
			db.notFoundStats.add("storage-overlay-tombstone")
			db.logStorageKVReadFailure(storageKey, accountKey, "storage-overlay-tombstone", nil)
			return nil, false, nil
		}
		return v, true, nil
	}

	storageCacheStart := time.Now()
	cacheKey := db.storageCacheKey(accountKey, storageKey)
	if value, ok := db.storageCache.Get(cacheKey); ok {
		recordTrieStorageGetBreakdownStep(&db.trieStorageKVStats, true, time.Since(storageCacheStart))
		db.noteStoragePrefetchHit(cacheKey, value)
		if value == nil {
			db.notFoundStats.add("storage-cache-tombstone")
			db.logStorageKVReadFailure(storageKey, accountKey, "storage-cache-tombstone", nil)
			return nil, false, nil
		}
		valueBytes := value.([]byte)
		db.addTrieStorageFetchStats(true, valueBytes)
		return valueBytes, true, nil
	}

	if value, found, err := db.readBufferLogValue(accountKey, storageKey); err != nil {
		db.logStorageKVReadFailure(storageKey, accountKey, "buffer-log-read", err)
		return nil, false, err
	} else if found {
		if value == nil {
			db.notFoundStats.add("buffer-log-tombstone")
			db.logStorageKVReadFailure(storageKey, accountKey, "buffer-log-tombstone", nil)
			return nil, false, nil
		}
		return value, true, nil
	}
	missStart := time.Now()
	missTracker := &cacheMissCostTracker{}
	defer func() {
		recordCacheMissCost(&db.storageDataMissStats, time.Since(missStart), missTracker)
	}()

	value, ok, failure, err := db.readAccountStorageValueWithTracker(accountKey, storageKey, missTracker)
	if err != nil {
		if failure != nil {
			db.logSegmentedStorageKVReadFailure(storageKey, accountKey, failure, err)
		} else {
			db.logStorageKVReadFailure(storageKey, accountKey, "read-account-storage", err)
		}
		return nil, false, err
	}
	if ok {
		db.addTrieStorageFetchStats(false, value)
		return value, true, nil
	}
	if failure != nil {
		db.notFoundStats.add(failure.reason)
		db.logSegmentedStorageKVReadFailure(storageKey, accountKey, failure, nil)
	} else {
		db.notFoundStats.add("storage-not-found")
		db.logStorageKVReadFailure(storageKey, accountKey, "storage-not-found", nil)
	}
	return nil, false, nil
}

func splitLogPath(path string) (string, string) {
	if path == "" {
		return "", ""
	}
	return filepath.Dir(path), filepath.Base(path)
}

func (db *PrefixDB) accountReadLogFields(offset uint64, size uint32) (string, string, uint64, uint32) {
	if db == nil || db.accountFile == nil {
		return "", "", offset, size
	}
	dir, file := splitLogPath(db.accountFile.Name())
	return dir, file, offset, size
}

func (db *PrefixDB) storageReadLogFields(accountKey []byte) (string, string, uint32, uint64, uint64) {
	if db == nil || len(accountKey) == 0 {
		return "", "", 0, 0, 0
	}
	if db.isAccountStorageFolderManaged(accountKey) {
		folderPath := db.segmentedFolderPathForAccount(accountKey)
		dir, file := splitLogPath(folderPath)
		return dir, file, segmentedStorageFlag, 0, 0
	}
	node, err := db.getNode(accountKey)
	if err != nil || node == nil {
		return "", "", 0, 0, 0
	}
	if isSegmentedStorage(node.storageFileID) {
		folderPath := db.segmentedFolderPathForAccount(accountKey)
		dir, file := splitLogPath(folderPath)
		return dir, file, node.storageFileID, node.storageOffset, node.storageSize
	}
	path, _ := db.storagePathByFileID(node.storageFileID)
	dir, file := splitLogPath(path)
	return dir, file, node.storageFileID, node.storageOffset, node.storageSize
}

func (db *PrefixDB) logAccountKVReadFailure(key []byte, offset uint64, size uint32, reason string, err error) {
	if !shouldEmitStorageMissLogForTestsOnly(err) {
		return
	}
	dir, file, offset, size := db.accountReadLogFields(offset, size)
	fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: account kv read failed key=%x dir=%s file=%s offset=%d size=%d reason=%s\n", key, dir, file, offset, size, reason)
}

func (db *PrefixDB) logStorageKVReadFailure(storageKey, accountKey []byte, reason string, err error) {
	if !shouldEmitStorageMissLogForTestsOnly(err) {
		return
	}
	dir, file, fileID, offset, size := db.storageReadLogFields(accountKey)
	if err != nil {
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: storage kv read failed account=%x storage=%x dir=%s file=%s fileID=%d offset=%d size=%d reason=%s err=%v\n", accountKey, storageKey, dir, file, fileID, offset, size, reason, err)
		return
	}
	fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: storage kv read failed account=%x storage=%x dir=%s file=%s fileID=%d offset=%d size=%d reason=%s\n", accountKey, storageKey, dir, file, fileID, offset, size, reason)
}

func (db *PrefixDB) logSegmentedStorageKVReadFailure(storageKey, accountKey []byte, failure *segmentedStorageReadFailure, err error) {
	if !shouldEmitStorageMissLogForTestsOnly(err) {
		return
	}
	if failure == nil {
		db.logStorageKVReadFailure(storageKey, accountKey, "storage-not-found", err)
		return
	}
	dir, file := splitLogPath(failure.folderPath)
	indexFile := failure.indexFile
	if indexFile == "" {
		indexFile = segmentIndexFileName
	}
	fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: storage kv read failed account=%x storage=%x dir=%s file=%s fileID=%d offset=0 size=0 mode=folder index=%s chunk=%s reason=%s\n", accountKey, storageKey, dir, file, segmentedStorageFlag, indexFile, failure.chunkFile, failure.reason)
}

func (db *PrefixDB) putAccount(key, value []byte) error {
	cacheKey := string(key)
	var stroageInfo StorageInfo
	if !db.shouldBypassNodeCache(key) {
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			stroageInfo = entry.StorageInfo
			db.nodeCache.UpdateValue(cacheKey, value)
		}
	}
	if db.accountBatch != nil {
		db.accountBatch.add(key, value, stroageInfo.storageFileID, stroageInfo.storageOffset, stroageInfo.storageSize, ValueModified)
	}
	return nil
}

func (db *PrefixDB) putStorage(key, value, accountKey []byte) error {
	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		return err
	}
	if db.storageCache != nil {
		if accountKey != nil {
			db.removeStorageCacheValue(accountKey, storageKey)
		}
	}

	if accountKey == nil {
		fmt.Printf("Parent account key not found for %x\n", key)
		return nil
	}

	return db.bufferStorageMutation(accountKey, storageKey, value)
}

func (db *PrefixDB) Put(dataType datatypepkg.DataType, key, value, accountKey []byte) error {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.putAccount(key, value)
	case datatypepkg.TrieNodeStorageDataType:
		return db.putStorage(key, value, accountKey)
	default:
		return errors.New("unknown data type")
	}
}

func (db *PrefixDB) batchPutAccount(key, value []byte) error {
	cacheKey := string(key)
	var stroageInfo StorageInfo
	if !db.shouldBypassNodeCache(key) {
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			stroageInfo = entry.StorageInfo
			db.nodeCache.UpdateValue(cacheKey, value)
		}
	} else if db.nodeCache != nil {
		db.nodeCache.Delete(cacheKey)
	}
	if db.accountBatch != nil {
		db.accountBatch.add(key, value, stroageInfo.storageFileID, stroageInfo.storageOffset, stroageInfo.storageSize, ValueModified)
	}
	return nil
}

func (db *PrefixDB) BatchPut(dataType datatypepkg.DataType, key, value, accountKey []byte) error {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.batchPutAccount(key, value)
	case datatypepkg.TrieNodeStorageDataType:
		return db.StorageBatchPut(key, value, accountKey)
	default:
		return errors.New("unknown data type")
	}
}

func (db *PrefixDB) BatchCommit() error {
	return db.BatchCommitWithBlockID(0)
}

func (db *PrefixDB) BatchCommitWithBlockID(blockID uint64) (err error) {
	if db.prefixTree != nil {
		db.prefixTree.beginGlobalCommitWithBlockID(blockID)
		defer func() {
			if endErr := db.prefixTree.endGlobalCommit(); err == nil {
				err = endErr
			}
		}()
	}

	var accountOps map[string]WriteOperation
	if db.accountBatch != nil {
		accountOps = db.accountBatch.drainOperations()
	}
	var (
		storageBatch      map[string]map[string][]byte
		storageUnresolved map[string][]byte
		storagePlans      []storageCommitPlan
	)
	if db.storageBatch != nil {
		storageBatch, storageUnresolved = db.storageBatch.drain()
	}

	// Log batch commit statistics
	totalStorageKeys := 0
	for _, perAccount := range storageBatch {
		totalStorageKeys += len(perAccount)
	}
	if len(accountOps) > 0 || totalStorageKeys > 0 || len(storageUnresolved) > 0 {
		prefixdbDebugf("BatchCommit: starting commit - accountOps=%d storageAccounts=%d storageKeys=%d unresolved=%d",
			len(accountOps), len(storageBatch), totalStorageKeys, len(storageUnresolved))
	}

	if len(accountOps) == 0 && len(storageBatch) == 0 && len(storageUnresolved) == 0 {
		return nil
	}
	shouldWaitForStorageGC := false
	commitStart := time.Now()

	db.writeMutex.Lock()
	err = func() error {
		stageStart := time.Now()
		useBufferLogForStorageBatch := blockID != 0
		if useBufferLogForStorageBatch {
			if storageBatch == nil {
				storageBatch = make(map[string]map[string][]byte)
			}
			if err := db.resolveUnresolvedStorageBatch(storageBatch, storageUnresolved); err != nil {
				return err
			}
			if err := db.appendStorageBatchToBufferLogs(storageBatch, blockID); err != nil {
				return err
			}
			prefixdbDebugf("BatchCommit: storage batch appended to buffer logs accounts=%d elapsed=%s", len(storageBatch), time.Since(stageStart))
			if err := db.preserveAccountStoragePointers(accountOps); err != nil {
				return err
			}
		} else {
			storagePlans, err = db.prepareStorageCommitPlans(storageBatch, storageUnresolved, accountOps, blockID)
			if err != nil {
				return err
			}
			stageStart = time.Now()
			if err := db.appendPreparedInlineStorageSegments(storagePlans); err != nil {
				return err
			}
			prefixdbDebugf("BatchCommit: inline storage appends applied elapsed=%s", time.Since(stageStart))
			prefixdbDebugf("BatchCommit: storage plans ready count=%d elapsed=%s", len(storagePlans), time.Since(stageStart))
			if len(storagePlans) > 0 && accountOps != nil {
				stageStart = time.Now()
				for _, plan := range storagePlans {
					op, ok := accountOps[plan.accountKey]
					if !ok || op.value == nil {
						continue
					}
					op.storageFileID = plan.storageInfo.storageFileID
					op.storageOffset = plan.storageInfo.storageOffset
					op.storageSize = plan.storageInfo.storageSize
					accountOps[plan.accountKey] = op
				}
				prefixdbDebugf("BatchCommit: storage pointers merged elapsed=%s", time.Since(stageStart))
			}
		}

		stageStart = time.Now()
		prepared, err := db.prepareAccountCommit(accountOps)
		if err != nil {
			return err
		}
		prefixdbDebugf("BatchCommit: account entries prepared count=%d bytes=%d elapsed=%s",
			len(prepared.order), prepared.totalSize, time.Since(stageStart))

		trieAccountOffset, _ := db.accountFile.Seek(0, io.SeekEnd)
		if trieAccountOffset == 0 {
			trieAccountOffset = 1
		}

		naEntry := make([]byte, 0, prepared.totalSize)
		nodeEntries := make([]NodeInfo, 0, len(prepared.order))
		cacheUpdates := make([]pendingNodeCacheUpdate, 0, len(prepared.order))
		stageStart = time.Now()
		processedAccounts := 0
		for _, key := range prepared.order {
			op := accountOps[key]
			keyBytes := []byte(key)
			if op.modifiedType == None {
				continue
			}
			if op.value == nil {
				nodeEntries = append(nodeEntries, trieNodeToNodeInfo(keyBytes, &TrieNode{}))
				cacheUpdates = append(cacheUpdates, pendingNodeCacheUpdate{key: key, delete: true})
				continue
			}

			entry := prepared.entries[key]
			offset := trieAccountOffset + int64(len(naEntry))
			naEntry = append(naEntry, entry...)

			node := &TrieNode{
				storageFileID: op.storageFileID,
				storageOffset: op.storageOffset,
				storageSize:   op.storageSize,
				accountOffset: uint64(offset),
				accountSize:   uint32(len(entry)),
			}
			nodeEntries = append(nodeEntries, trieNodeToNodeInfo(keyBytes, node))
			cacheUpdates = append(cacheUpdates, pendingNodeCacheUpdate{
				key:           key,
				accountOffset: uint64(offset),
				accountSize:   uint32(len(entry)),
				storageInfo: StorageInfo{
					storageFileID: op.storageFileID,
					storageOffset: op.storageOffset,
					storageSize:   op.storageSize,
				},
			})
			processedAccounts++
			if processedAccounts%10000 == 0 {
				prefixdbDebugf("BatchCommit: account node staging progress=%d/%d elapsed=%s",
					processedAccounts, len(prepared.order), time.Since(stageStart))
			}
		}
		prefixdbDebugf("BatchCommit: account node staging done count=%d totalEntries=%d elapsed=%s",
			processedAccounts, len(nodeEntries), time.Since(stageStart))

		if len(naEntry) > 0 {
			naEntry = appendForwardCommitTag(naEntry, blockID)
			stageStart = time.Now()
			_, err := db.accountFile.WriteAt(naEntry, trieAccountOffset)
			if err != nil {
				return err
			}
			db.addDiskWrite(diskIOUsageAccountData, len(naEntry))
			prefixdbDebugf("BatchCommit: account file append bytes=%d elapsed=%s", len(naEntry), time.Since(stageStart))
		}

		if len(nodeEntries) > 0 {
			stageStart = time.Now()
			if err := db.applyNodeBatch(nodeEntries, cacheUpdates, blockID); err != nil {
				return err
			}
			prefixdbDebugf("BatchCommit: account node writes done count=%d elapsed=%s", len(nodeEntries), time.Since(stageStart))
		}

		if len(storagePlans) > 0 {
			shouldWaitForStorageGC = true
			stageStart = time.Now()
			appliedStoragePlans := 0
			for _, plan := range storagePlans {
				if _, ok := accountOps[plan.accountKey]; ok || plan.skipNodeWrite {
					continue
				}
				appliedStoragePlans++
				if appliedStoragePlans%25 == 0 {
					prefixdbDebugf("BatchCommit: storage pointer writes progress=%d/%d elapsed=%s",
						appliedStoragePlans, len(storagePlans), time.Since(stageStart))
				}
			}
			if err := db.applyStorageCommitPlans(storagePlans, accountOps, false); err != nil {
				return err
			}
			prefixdbDebugf("BatchCommit: storage pointer writes done applied=%d total=%d elapsed=%s",
				appliedStoragePlans, len(storagePlans), time.Since(stageStart))
		}
		prefixdbDebugf("BatchCommit: write phase finished totalElapsed=%s", time.Since(commitStart))
		return nil
	}()
	db.writeMutex.Unlock()
	if err != nil {
		return err
	}
	for _, plan := range storagePlans {
		db.syncStorageCacheEntries([]byte(plan.accountKey), plan.cacheEntries)
	}
	if shouldWaitForStorageGC {
		waitStart := time.Now()
		prefixdbDebugf("BatchCommit: waiting storage GC idle")
		if waitErr := db.waitForStorageGCIdle(); err == nil {
			err = waitErr
		}
		prefixdbDebugf("BatchCommit: storage GC idle wait done elapsed=%s", time.Since(waitStart))
	}
	prefixdbDebugf("BatchCommit: finished totalElapsed=%s", time.Since(commitStart))
	return nil
}

func (db *PrefixDB) hasAccount(key []byte) (bool, error) {
	cacheKey := string(key)
	useNodeCache := !db.shouldBypassNodeCache(key)
	if useNodeCache {
		if _, ok := db.nodeCache.Get(cacheKey); ok {
			return true, nil
		}
	}

	if db.accountBatch != nil {
		if _, _, ok := db.accountBatch.get(key); ok {
			return true, nil
		}
	}

	node, err := db.getAccountNode(key)
	if err != nil {
		return false, err
	}
	if node == nil {
		fmt.Printf("Account key %s not found in index\n", string(key))
		return false, nil
	}
	value, err := db.readFromFile(node.accountOffset, node.accountSize)
	if err != nil {
		return false, err
	}

	if useNodeCache {
		db.nodeCache.Put(NodeCacheEntry{
			Key:           cacheKey,
			Value:         value,
			AccountOffset: node.accountOffset,
			AccountSize:   node.accountSize,
			StorageInfo: StorageInfo{
				storageFileID: node.storageFileID,
				storageOffset: node.storageOffset,
				storageSize:   node.storageSize,
			},
		})
	}

	return true, nil
}

func (db *PrefixDB) Has(dataType datatypepkg.DataType, key []byte, accountKey []byte) (bool, error) {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.hasAccount(key)
	case datatypepkg.TrieNodeStorageDataType:
		return db.hasStorage(key, accountKey)
	default:
		return false, errors.New("unknown data type")
	}
}

func (db *PrefixDB) hasStorage(key []byte, accountKey []byte) (bool, error) {
	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		return false, err
	}

	if accountKey == nil {
		fmt.Printf("Parent account key not found for %x\n", key)
		return false, nil
	}

	if v, present := db.batchGetOverlayNormalized(storageKey, accountKey); present {
		return v != nil, nil
	}

	if v, ok := db.storageCache.Get(db.storageCacheKey(accountKey, storageKey)); ok {
		return v != nil, nil
	}
	_, ok, _, err := db.readAccountStorageValue(accountKey, storageKey)
	if err != nil {
		fmt.Println("Error reading account storage:", err)
		return false, err
	}
	if ok {
		return true, nil
	}
	return false, nil
}

func (db *PrefixDB) deleteAccount(key []byte) error {
	if db.accountBatch != nil {
		db.accountBatch.delete(key)
	}
	if db.nodeCache != nil {
		db.nodeCache.Delete(string(key))
	}
	return db.storeNode(key, &TrieNode{
		storageFileID: 0,
		storageOffset: 0,
		accountOffset: 0,
		accountSize:   0,
		storageSize:   0,
	})
}

func (db *PrefixDB) deleteStorage(key, accountKey []byte) error {
	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		return err
	}

	if db.storageCache != nil {
		if accountKey != nil {
			db.removeStorageCacheValue(accountKey, storageKey)
		}
	}

	if accountKey == nil {
		fmt.Printf("Parent account key not found for %x\n", key)
		return nil
	}

	return db.bufferStorageMutation(accountKey, storageKey, nil)
}

func (db *PrefixDB) Delete(dataType datatypepkg.DataType, key, accountKey []byte) error {
	switch dataType {
	case datatypepkg.TrieNodeAccountDataType:
		return db.deleteAccount(key)
	case datatypepkg.TrieNodeStorageDataType:
		return db.deleteStorage(key, accountKey)
	default:
		return errors.New("unknown data type")
	}
}

func (db *PrefixDB) bufferStorageMutation(accountKey, storageKey, value []byte) error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	accountStr := string(accountKey)
	if db.storageBuf.accountKey != accountStr {
		if db.storageBuf.accountKey != "" {
			if err := db.flushStorageBuffer(); err != nil {
				return err
			}
		}
		db.storageBuf.reset()
		db.storageBuf.accountKey = accountStr
		db.storageBuf.storagekvs = make([]kvPair, 0)
	}
	// Check for duplicate key in the buffer
	for _, existing := range db.storageBuf.storagekvs {
		if string(existing.key) == string(storageKey) {
			fmt.Printf("bufferStorageMutation: duplicate storage key detected in buffer - accountKey=%s storageKey=%x (will be overwritten)\n",
				accountStr, storageKey)
			break
		}
	}
	db.storageBuf.storagekvs = append(db.storageBuf.storagekvs, kvPair{key: storageKey, val: value})
	return nil
}

func (db *PrefixDB) flushStorageBuffer() error {
	buf := &db.storageBuf
	if buf.accountKey == "" {
		return nil
	}
	var (
		accOff         uint64
		accSize        uint32
		existingFileID uint32
		existingOffset uint64
		existingSize   uint64
	)

	node, err := db.getNode([]byte(buf.accountKey))
	if err != nil {
		return err
	}
	if node != nil {
		accOff = node.accountOffset
		accSize = node.accountSize
		existingFileID = node.storageFileID
		existingOffset = node.storageOffset
		existingSize = node.storageSize
	}
	if len(buf.storagekvs) == 0 {
		fmt.Printf("flushStorageBuffer: empty buffer for account - accountKey=%s\n",
			buf.accountKey)
		if err := db.prefixTree.Put([]byte(buf.accountKey), accOff, accSize, 0, 0, 0); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(buf.accountKey, StorageInfo{})
		if db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(buf.accountKey, StorageInfo{})
		}
		return nil
	}
	sortKVPairs(buf.storagekvs)
	fileID, off, sz, err := db.persistStorageEntries([]byte(buf.accountKey), buf.storagekvs, existingFileID, existingOffset, existingSize)
	if err != nil {
		return err
	}
	skipAccountPointerUpdate := shouldSkipAccountEntryPointerUpdate(existingFileID, fileID, off, sz)
	if !skipAccountPointerUpdate {
		if err := db.prefixTree.Put([]byte(buf.accountKey), accOff, accSize, fileID, off, sz); err != nil {
			return err
		}
		db.nodeCache.UpdateStoragePointer(buf.accountKey, StorageInfo{
			storageFileID: fileID,
			storageOffset: off,
			storageSize:   sz,
		})
	}

	// cacheKeyHex := hex.EncodeToString([]byte(buf.accountKey))
	// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", fileID) + ", offset:" + fmt.Sprintf("%d", off) + ", size:" + fmt.Sprintf("%d", sz))

	if db.accountBatch != nil && !skipAccountPointerUpdate {
		_ = db.accountBatch.updateStoragePointer(buf.accountKey, StorageInfo{
			storageFileID: fileID,
			storageOffset: off,
			storageSize:   sz,
		})
	}
	db.syncStorageCacheEntries([]byte(buf.accountKey), buf.storagekvs)
	buf.reset()
	return nil
}

func (sb *storageOpBuffer) reset() {
	*sb = storageOpBuffer{}
}

var (
	headerPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4)
		},
	}
	smallBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 1024)
		},
	}
	mediumBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 32*1024) // 32KB
		},
	}
	oneMBBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 1024*1024) // 1MB
		},
	}
	fourMBBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4*1024*1024) // 4MB
		},
	}

	kvPairScratchPool = sync.Pool{
		New: func() interface{} {
			return make([]kvPair, 0)
		},
	}
	kvPairEntryPool = sync.Pool{
		New: func() interface{} {
			return make([]kvPair, 0, 64)
		},
	}
)

// sortKVPairs performs a stable merge sort on the provided kvPair slice using a pooled buffer.
func sortKVPairs(entries []kvPair) {
	if len(entries) < 2 {
		return
	}

	if len(entries) <= 65536 {
		sort.SliceStable(entries, func(i, j int) bool {
			return bytes.Compare(entries[i].key, entries[j].key) < 0
		})
		return
	}
	buf := kvPairScratchPool.Get().([]kvPair)
	if cap(buf) < len(entries) {
		buf = make([]kvPair, len(entries))
	}
	buf = buf[:len(entries)]
	copy(buf, entries)

	src := buf
	dst := entries
	srcIsEntries := false
	for width := 1; width < len(entries); width <<= 1 {
		for start := 0; start < len(entries); start += 2 * width {
			mid := start + width
			if mid > len(entries) {
				mid = len(entries)
			}
			end := start + 2*width
			if end > len(entries) {
				end = len(entries)
			}
			left := start
			right := mid
			pos := start
			for left < mid && right < end {
				if bytes.Compare(src[left].key, src[right].key) <= 0 {
					dst[pos] = src[left]
					left++
				} else {
					dst[pos] = src[right]
					right++
				}
				pos++
			}
			for left < mid {
				dst[pos] = src[left]
				left++
				pos++
			}
			for right < end {
				dst[pos] = src[right]
				right++
				pos++
			}

		}
		src, dst = dst, src
		srcIsEntries = !srcIsEntries
	}

	if !srcIsEntries {
		copy(entries, src)
	}
	kvPairScratchPool.Put(buf[:0])
}

// getDataBuffer returns a byte slice of the requested size from the appropriate buffer pool.
func getDataBuffer(size int) []byte {
	var buffer []byte
	if size <= 1024 {
		buffer = smallBufferPool.Get().([]byte)
		return buffer[:size]
	} else if size <= 32*1024 {
		buffer = mediumBufferPool.Get().([]byte)
		return buffer[:size]
	} else if size <= 1024*1024 {
		buffer = oneMBBufferPool.Get().([]byte)
		return buffer[:size]
	} else if size <= 4*1024*1024 {
		buffer = fourMBBufferPool.Get().([]byte)
		return buffer[:size]
	}
	return make([]byte, size)
}

func putDataBuffer(buf []byte) {
	if buf == nil {
		return
	}

	capacity := cap(buf)
	switch {
	case capacity <= 1024:
		smallBufferPool.Put(buf[:capacity])
	case capacity <= 32*1024:
		mediumBufferPool.Put(buf[:capacity])
	case capacity <= 1024*1024:
		oneMBBufferPool.Put(buf[:capacity])
	case capacity <= 4*1024*1024:
		fourMBBufferPool.Put(buf[:capacity])
	default:
		// do nothing for large buffers
	}
}

type bufferLease struct {
	buf  []byte
	refs int
	mu   sync.Mutex
}

func newBufferLease(buf []byte) *bufferLease {
	if buf == nil {
		return nil
	}
	return &bufferLease{buf: buf, refs: 1}
}

func (l *bufferLease) Retain() *bufferLease {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	l.refs++
	l.mu.Unlock()
	return l
}

func (l *bufferLease) Release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.refs--
	refs := l.refs
	buf := l.buf
	l.mu.Unlock()
	if refs == 0 {
		putDataBuffer(buf)
	}
}

func (l *bufferLease) Bytes() []byte {
	if l == nil {
		return nil
	}
	return l.buf
}

func readUint16BE(b []byte) uint16 {
	return uint16(b[0])<<8 | uint16(b[1])
}

func readUint32BE(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func readUint64BE(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

func writeUint16BE(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

func writeUint32BE(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func writeUint64BE(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func (tracker *cacheMissCostTracker) addIO(n int, elapsed time.Duration) {
	if !analysisStatsEnabled || tracker == nil {
		return
	}
	tracker.ioOps++
	if n > 0 {
		tracker.ioBytes += uint64(n)
	}
	if elapsed > 0 {
		tracker.ioNanos += uint64(elapsed)
	}
}

func recordCacheMissCost(stats *cacheMissCostStats, total time.Duration, tracker *cacheMissCostTracker) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	atomic.AddUint64(&stats.count, 1)
	if tracker == nil {
		atomic.AddUint64(&stats.computeNanos, uint64(total))
		return
	}
	atomic.AddUint64(&stats.ioOps, tracker.ioOps)
	atomic.AddUint64(&stats.ioBytes, tracker.ioBytes)
	atomic.AddUint64(&stats.ioNanos, tracker.ioNanos)
	computeNanos := uint64(total)
	if tracker.ioNanos < computeNanos {
		computeNanos -= tracker.ioNanos
	} else {
		computeNanos = 0
	}
	atomic.AddUint64(&stats.computeNanos, computeNanos)
}

func printCacheMissCostStats(label string, stats *cacheMissCostStats) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	count := atomic.LoadUint64(&stats.count)
	if count == 0 {
		return
	}
	ioOps := atomic.LoadUint64(&stats.ioOps)
	ioBytes := atomic.LoadUint64(&stats.ioBytes)
	ioNanos := atomic.LoadUint64(&stats.ioNanos)
	computeNanos := atomic.LoadUint64(&stats.computeNanos)
	ioAvgMicros := float64(ioNanos) / float64(count) / 1000.0
	computeAvgMicros := float64(computeNanos) / float64(count) / 1000.0
	ioOpsAvg := float64(ioOps) / float64(count)
	ioBytesAvg := float64(ioBytes) / float64(count)
	fmt.Printf("PrefixDB cache-miss cost [%s]: count=%d ioOps=%d ioBytes=%d ioTotal=%s ioAvg=%0.2fus ioOpsAvg=%0.2f ioBytesAvg=%0.2f computeTotal=%s computeAvg=%0.2fus\n",
		label,
		count,
		ioOps,
		ioBytes,
		time.Duration(ioNanos),
		ioAvgMicros,
		ioOpsAvg,
		ioBytesAvg,
		time.Duration(computeNanos),
		computeAvgMicros,
	)
}

func (db *PrefixDB) readFromFile(offset uint64, accountSize uint32) ([]byte, error) {
	return db.readFromFileWithTracker(offset, accountSize, nil)
}

func (db *PrefixDB) readFromFileWithTracker(offset uint64, accountSize uint32, tracker *cacheMissCostTracker) ([]byte, error) {
	if accountSize == 0 {
		return db.readFromFileLegacyWithTracker(offset, tracker)
	}
	file := db.accountFile
	totalSize := int(accountSize)
	buf := getDataBuffer(totalSize)
	defer putDataBuffer(buf)

	ioStart := time.Now()
	n, err := file.ReadAt(buf[:totalSize], int64(offset))
	tracker.addIO(n, time.Since(ioStart))
	if err != nil && !(err == io.EOF && n == totalSize) {
		return nil, fmt.Errorf("failed to read account entry at offset %d: %v", offset, err)
	}
	if n != totalSize {
		return nil, fmt.Errorf("short account entry read at offset %d: got %d want %d", offset, n, totalSize)
	}
	db.addDiskRead(diskIOUsageAccountData, n)

	if totalSize < 4 {
		return nil, fmt.Errorf("corrupted account entry at offset %d: size %d", offset, totalSize)
	}
	keySize := int(uint16(buf[0])<<8 | uint16(buf[1]))
	valueSize := int(uint16(buf[2])<<8 | uint16(buf[3]))
	if 4+keySize+valueSize != totalSize {
		return nil, fmt.Errorf("corrupted account entry at offset %d: header size %d payload size %d entry size %d", offset, keySize, valueSize, totalSize)
	}

	value := make([]byte, valueSize)
	copy(value, buf[4+keySize:])
	return value, nil
}

func (db *PrefixDB) readFromFileLegacy(offset uint64) ([]byte, error) {
	return db.readFromFileLegacyWithTracker(offset, nil)
}

func (db *PrefixDB) readFromFileLegacyWithTracker(offset uint64, tracker *cacheMissCostTracker) ([]byte, error) {
	var file *os.File
	file = db.accountFile
	header := headerPool.Get().([]byte)
	defer headerPool.Put(header)

	if cap(header) < 6 {
		header = make([]byte, 4)
	} else {
		header = header[:4]
	}

	ioStart := time.Now()
	n, err := file.ReadAt(header, int64(offset))
	tracker.addIO(n, time.Since(ioStart))
	if err != nil {
		return nil, fmt.Errorf("failed to read header at offset %d: %v", offset, err)
	}
	db.addDiskRead(diskIOUsageAccountData, n)

	keySize := int(uint16(header[0])<<8 | uint16(header[1]))
	valueSize := int(uint16(header[2])<<8 | uint16(header[3]))

	totalSize := keySize + valueSize

	combinedData := getDataBuffer(totalSize)
	defer putDataBuffer(combinedData)

	ioStart = time.Now()
	n, err = file.ReadAt(combinedData, int64(offset)+4)
	tracker.addIO(n, time.Since(ioStart))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read combined data at offset %d: %v", offset+6, err)
	}
	db.addDiskRead(diskIOUsageAccountData, n)

	value := make([]byte, valueSize)
	copy(value, combinedData[keySize:totalSize])

	return value, nil
}

func (db *PrefixDB) addCommitOldKVReadStats(pairCount int, bytes uint64) {
	addUint64Stat(&db.commitOldKVReadCount, uint64(pairCount))
	addUint64Stat(&db.commitOldKVReadBytes, bytes)
}

func (db *PrefixDB) addReadBytes(n int) {
	addUint64Stat(&db.totalReadBytes, uint64(n))
}

func (db *PrefixDB) finishGetReadStats(readBefore uint64) {
	if !analysisStatsEnabled || db == nil {
		return
	}
	readAfter := atomic.LoadUint64(&db.totalReadBytes)
	if readAfter >= readBefore {
		atomic.AddUint64(&db.getReadBytesSum, readAfter-readBefore)
	}
	atomic.AddUint64(&db.getReadReqCount, 1)
}

func (db *PrefixDB) addDiskRead(usage diskIOUsage, n int) {
	if !analysisStatsEnabled || db == nil || usage >= diskIOUsageCount {
		return
	}
	atomic.AddUint64(&db.diskIOStats[usage].readOps, 1)
	if n > 0 {
		atomic.AddUint64(&db.diskIOStats[usage].readBytes, uint64(n))
		db.addReadBytes(n)
	}
}

func (db *PrefixDB) addDiskWrite(usage diskIOUsage, n int) {
	if !analysisStatsEnabled || db == nil || usage >= diskIOUsageCount {
		return
	}
	atomic.AddUint64(&db.diskIOStats[usage].writeOps, 1)
	if n > 0 {
		atomic.AddUint64(&db.diskIOStats[usage].writeBytes, uint64(n))
	}
}

func (db *PrefixDB) diskIOStatsSnapshot() [diskIOUsageCount]diskIOCounters {
	var snapshot [diskIOUsageCount]diskIOCounters
	if !analysisStatsEnabled || db == nil {
		return snapshot
	}
	for usage := diskIOUsage(0); usage < diskIOUsageCount; usage++ {
		stats := &db.diskIOStats[usage]
		snapshot[usage] = diskIOCounters{
			readOps:    atomic.LoadUint64(&stats.readOps),
			readBytes:  atomic.LoadUint64(&stats.readBytes),
			writeOps:   atomic.LoadUint64(&stats.writeOps),
			writeBytes: atomic.LoadUint64(&stats.writeBytes),
		}
	}
	return snapshot
}

func diskIOStatsTotalDelta(before, after [diskIOUsageCount]diskIOCounters) diskIOCounters {
	var delta diskIOCounters
	for usage := diskIOUsage(0); usage < diskIOUsageCount; usage++ {
		if after[usage].readOps >= before[usage].readOps {
			delta.readOps += after[usage].readOps - before[usage].readOps
		}
		if after[usage].readBytes >= before[usage].readBytes {
			delta.readBytes += after[usage].readBytes - before[usage].readBytes
		}
		if after[usage].writeOps >= before[usage].writeOps {
			delta.writeOps += after[usage].writeOps - before[usage].writeOps
		}
		if after[usage].writeBytes >= before[usage].writeBytes {
			delta.writeBytes += after[usage].writeBytes - before[usage].writeBytes
		}
	}
	return delta
}

func (db *PrefixDB) readFileWithStats(path string, usage diskIOUsage) ([]byte, error) {
	return db.readFileWithStatsTracked(path, usage, nil)
}

func (db *PrefixDB) readFileWithStatsTracked(path string, usage diskIOUsage, tracker *cacheMissCostTracker) ([]byte, error) {
	f, err := db.openCachedReadOnlyFile(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size < 0 {
		return nil, fmt.Errorf("invalid file size: %s", path)
	}
	if size == 0 {
		return nil, nil
	}
	if size > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("file too large to read into memory: %s", path)
	}
	data := make([]byte, int(size))
	sr := io.NewSectionReader(f, 0, size)
	ioStart := time.Now()
	if _, err := io.ReadFull(sr, data); err != nil {
		tracker.addIO(0, time.Since(ioStart))
		return nil, err
	}
	tracker.addIO(len(data), time.Since(ioStart))
	db.addDiskRead(usage, len(data))
	return data, nil
}

func (db *PrefixDB) writeFileWithStats(path string, data []byte, perm os.FileMode, usage diskIOUsage) error {
	if err := os.WriteFile(path, data, perm); err != nil {
		return err
	}
	db.addDiskWrite(usage, len(data))
	return nil
}

func (db *PrefixDB) printDiskIOStats() {
	if !analysisStatsEnabled || db == nil {
		return
	}
	var totalReadOps, totalReadBytes, totalWriteOps, totalWriteBytes uint64
	for usage := diskIOUsage(0); usage < diskIOUsageCount; usage++ {
		stats := &db.diskIOStats[usage]
		readOps := atomic.LoadUint64(&stats.readOps)
		readBytes := atomic.LoadUint64(&stats.readBytes)
		writeOps := atomic.LoadUint64(&stats.writeOps)
		writeBytes := atomic.LoadUint64(&stats.writeBytes)
		totalReadOps += readOps
		totalReadBytes += readBytes
		totalWriteOps += writeOps
		totalWriteBytes += writeBytes
		if readOps == 0 && readBytes == 0 && writeOps == 0 && writeBytes == 0 {
			continue
		}
		fmt.Printf("PrefixDB disk IO stats [%s]: readOps=%d readBytes=%d writeOps=%d writeBytes=%d\n",
			diskIOUsageNames[usage], readOps, readBytes, writeOps, writeBytes,
		)
	}
	fmt.Printf("PrefixDB disk IO stats [total]: readOps=%d readBytes=%d writeOps=%d writeBytes=%d\n",
		totalReadOps, totalReadBytes, totalWriteOps, totalWriteBytes,
	)
}

func (db *PrefixDB) addTrieStorageFetchStats(fromCache bool, value []byte) {
	if !analysisStatsEnabled || len(value) == 0 {
		return
	}
	valueSize := uint64(len(value))
	if fromCache {
		atomic.AddUint64(&db.trieStorageCachePairs, 1)
		atomic.AddUint64(&db.trieStorageCacheBytes, valueSize)
		return
	}
	atomic.AddUint64(&db.trieStorageLogPairs, 1)
	atomic.AddUint64(&db.trieStorageLogBytes, valueSize)
}

func recordTrieStorageGetBreakdownStep(stats *trieStorageGetBreakdownStepStats, fromCache bool, duration time.Duration) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	nanos := uint64(duration)
	if fromCache {
		atomic.AddUint64(&stats.cacheCount, 1)
		atomic.AddUint64(&stats.cacheNanos, nanos)
		return
	}
	atomic.AddUint64(&stats.noCacheCount, 1)
	atomic.AddUint64(&stats.noCacheNanos, nanos)
}

func trieStorageGetBreakdownHitRatio(cacheCount uint64, noCacheCount uint64) float64 {
	total := cacheCount + noCacheCount
	if total == 0 {
		return 0
	}
	return float64(cacheCount) / float64(total) * 100.0
}

func printTrieStorageGetBreakdownStep(label string, stats *trieStorageGetBreakdownStepStats) {
	printTrieGetBreakdownStep("TrieNodeStorage", label, stats)
}

func printTrieGetBreakdownStep(kind string, label string, stats *trieStorageGetBreakdownStepStats) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	cacheCount := atomic.LoadUint64(&stats.cacheCount)
	cacheNanos := atomic.LoadUint64(&stats.cacheNanos)
	noCacheCount := atomic.LoadUint64(&stats.noCacheCount)
	noCacheNanos := atomic.LoadUint64(&stats.noCacheNanos)
	cacheAvgMicros := 0.0
	if cacheCount > 0 {
		cacheAvgMicros = float64(cacheNanos) / float64(cacheCount) / 1000.0
	}
	noCacheAvgMicros := 0.0
	if noCacheCount > 0 {
		noCacheAvgMicros = float64(noCacheNanos) / float64(noCacheCount) / 1000.0
	}
	cacheHitRatio := trieStorageGetBreakdownHitRatio(cacheCount, noCacheCount)
	fmt.Printf("PrefixDB %s get breakdown [%s]: cacheHitRatio=%0.2f%% cacheCount=%d cacheTotal=%s cacheAvg=%0.2fus noCacheCount=%d noCacheTotal=%s noCacheAvg=%0.2fus\n",
		kind,
		label,
		cacheHitRatio,
		cacheCount,
		time.Duration(cacheNanos),
		cacheAvgMicros,
		noCacheCount,
		time.Duration(noCacheNanos),
		noCacheAvgMicros,
	)
}

func recordTrieStorageSegmentIndexLayer(source segmentIndexLookupSource, duration time.Duration, stats *trieStorageSegmentIndexLayerStats) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	nanos := uint64(duration)
	if source == segmentIndexLookupSourceL2Cache {
		atomic.AddUint64(&stats.l2CacheCount, 1)
		atomic.AddUint64(&stats.l2CacheNanos, nanos)
	}
}

func printTrieStorageSegmentIndexLayerStats(stats *trieStorageSegmentIndexLayerStats) {
	if !analysisStatsEnabled || stats == nil {
		return
	}
	l2Count := atomic.LoadUint64(&stats.l2CacheCount)
	l2Nanos := atomic.LoadUint64(&stats.l2CacheNanos)
	l2AvgMicros := 0.0
	if l2Count > 0 {
		l2AvgMicros = float64(l2Nanos) / float64(l2Count) / 1000.0
	}
	fmt.Printf("PrefixDB TrieNodeStorage segment-index cache layer stats: l2Count=%d l2Total=%s l2Avg=%0.2fus\n",
		l2Count,
		time.Duration(l2Nanos),
		l2AvgMicros,
	)
}

func printPrefixDBNotFoundStats(db *PrefixDB) {
	if !analysisStatsEnabled || db == nil {
		return
	}
	stats := &db.notFoundStats
	total := atomic.LoadUint64(&stats.accountNodeMissing) +
		atomic.LoadUint64(&stats.storageMissingParent) +
		atomic.LoadUint64(&stats.storageOverlayTombstone) +
		atomic.LoadUint64(&stats.storageCacheTombstone) +
		atomic.LoadUint64(&stats.storageBufferTombstone) +
		atomic.LoadUint64(&stats.storageAccountMissing) +
		atomic.LoadUint64(&stats.storageSegmentMissing) +
		atomic.LoadUint64(&stats.storageSegmentIndexEmpty) +
		atomic.LoadUint64(&stats.storageSegmentIndexMiss) +
		atomic.LoadUint64(&stats.storageChunkMetaMiss) +
		atomic.LoadUint64(&stats.storageChunkKVNotFound) +
		atomic.LoadUint64(&stats.storageChunkTombstone) +
		atomic.LoadUint64(&stats.storageChunkReadFailed) +
		atomic.LoadUint64(&stats.storageChunkEmpty) +
		atomic.LoadUint64(&stats.storageChunkCorrupted) +
		atomic.LoadUint64(&stats.storageSegmentIndexRead) +
		atomic.LoadUint64(&stats.storageNotFound)
	if total == 0 {
		return
	}
	fmt.Printf("PrefixDB notfound stats: accountNodeMissing=%d storageMissingParent=%d storageOverlayTombstone=%d storageCacheTombstone=%d storageBufferTombstone=%d storageAccountMissing=%d storageSegmentMissing=%d storageSegmentIndexEmpty=%d storageSegmentIndexMiss=%d storageChunkMetaMiss=%d storageChunkKVNotFound=%d storageChunkTombstone=%d storageChunkReadFailed=%d storageChunkEmpty=%d storageChunkCorrupted=%d storageSegmentIndexRead=%d storageNotFound=%d total=%d\n",
		atomic.LoadUint64(&stats.accountNodeMissing),
		atomic.LoadUint64(&stats.storageMissingParent),
		atomic.LoadUint64(&stats.storageOverlayTombstone),
		atomic.LoadUint64(&stats.storageCacheTombstone),
		atomic.LoadUint64(&stats.storageBufferTombstone),
		atomic.LoadUint64(&stats.storageAccountMissing),
		atomic.LoadUint64(&stats.storageSegmentMissing),
		atomic.LoadUint64(&stats.storageSegmentIndexEmpty),
		atomic.LoadUint64(&stats.storageSegmentIndexMiss),
		atomic.LoadUint64(&stats.storageChunkMetaMiss),
		atomic.LoadUint64(&stats.storageChunkKVNotFound),
		atomic.LoadUint64(&stats.storageChunkTombstone),
		atomic.LoadUint64(&stats.storageChunkReadFailed),
		atomic.LoadUint64(&stats.storageChunkEmpty),
		atomic.LoadUint64(&stats.storageChunkCorrupted),
		atomic.LoadUint64(&stats.storageSegmentIndexRead),
		atomic.LoadUint64(&stats.storageNotFound),
		total,
	)
}

func printSharedCacheLockOpStats(label string, stats sharedCacheLockOpSnapshot) {
	if !analysisStatsEnabled {
		return
	}
	if stats.Count == 0 {
		return
	}
	waitAvgMicros := float64(stats.WaitNanos) / float64(stats.Count) / 1000.0
	holdAvgMicros := float64(stats.HoldNanos) / float64(stats.Count) / 1000.0
	fmt.Printf("PrefixDB shared cache lock stats [%s]: count=%d waitTotal=%s waitAvg=%0.2fus holdTotal=%s holdAvg=%0.2fus\n",
		label,
		stats.Count,
		time.Duration(stats.WaitNanos),
		waitAvgMicros,
		time.Duration(stats.HoldNanos),
		holdAvgMicros,
	)
}

func printSharedCacheLockStats(shared *sharedByteCache) {
	if !analysisStatsEnabled || shared == nil {
		return
	}
	stats := shared.LockStatsSnapshot()
	printSharedCacheLockOpStats("get-touch", stats.GetTouch)
	printSharedCacheLockOpStats("get-notouch", stats.GetNoTouch)
	printSharedCacheLockOpStats("add", stats.Add)
	printSharedCacheLockOpStats("remove", stats.Remove)
	printSharedCacheLockOpStats("namespace", stats.Namespace)
}

func printTrieStoragePrefetchStats(db *PrefixDB) {
	if !analysisStatsEnabled || db == nil {
		return
	}
	addCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.addCount)
	addBytes := atomic.LoadUint64(&db.trieStoragePrefetchStats.addBytes)
	addNilCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.addNilCount)
	hitCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.hitCount)
	hitBytes := atomic.LoadUint64(&db.trieStoragePrefetchStats.hitBytes)
	hitNilCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.hitNilCount)
	clearCount := atomic.LoadUint64(&db.trieStoragePrefetchStats.clearCount)
	pendingCount := db.storagePrefetchPendingCount()
	hitRate := 0.0
	if addCount > 0 {
		hitRate = float64(hitCount) / float64(addCount) * 100.0
	}
	fmt.Printf("PrefixDB storage prefetch stats: sampleRate=1/%d addCount=%d addBytes=%d addNilCount=%d hitCount=%d hitBytes=%d hitNilCount=%d clearCount=%d pendingCount=%d hitRate=%0.2f%%\n",
		storagePrefetchStatsSampleDenominator(),
		addCount,
		addBytes,
		addNilCount,
		hitCount,
		hitBytes,
		hitNilCount,
		clearCount,
		pendingCount,
		hitRate,
	)
}

func (db *PrefixDB) addBufferLogMigrationStats(migratedKVs int) {
	if migratedKVs <= 0 {
		return
	}
	addUint64Stat(&db.bufferLogMigrationCount, 1)
	addUint64Stat(&db.bufferLogMigratedKVCount, uint64(migratedKVs))
}

func (db *PrefixDB) currentBufferLogSizeForStats(accountKey []byte) int64 {
	if !analysisStatsEnabled || db == nil || len(accountKey) == 0 {
		return 0
	}
	account := string(accountKey)
	db.bufferLogIndexMu.RLock()
	if idx := db.bufferLogIndexes[account]; idx != nil && idx.size >= 0 {
		size := idx.size
		db.bufferLogIndexMu.RUnlock()
		return size
	}
	db.bufferLogIndexMu.RUnlock()

	path, err := db.bufferLogPathForAccount(accountKey)
	if err != nil {
		return 0
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func (db *PrefixDB) addBufferLogSizeAccessStats(size int64, indexLookedUp bool, hit bool, tombstone bool, bloomReject bool, errSeen bool, hitBytes int, valueReadNanos uint64) {
	if !analysisStatsEnabled || db == nil {
		return
	}
	bucket := bufferLogSizeBucket(size)
	stats := &db.bufferLogSizeAccessStats[bucket]
	addUint64Stat(&stats.lookups, 1)
	if bloomReject {
		addUint64Stat(&stats.bloomRejects, 1)
	}
	if indexLookedUp {
		addUint64Stat(&stats.indexLookups, 1)
	}
	if errSeen {
		addUint64Stat(&stats.errors, 1)
		return
	}
	if hit {
		if tombstone {
			addUint64Stat(&stats.tombstoneHits, 1)
			return
		}
		addUint64Stat(&stats.hits, 1)
		addUint64Stat(&stats.hitBytes, uint64(hitBytes))
		addUint64Stat(&stats.valueReadNanos, valueReadNanos)
		return
	}
	addUint64Stat(&stats.misses, 1)
}

func printBufferLogStats(db *PrefixDB) {
	if !analysisStatsEnabled || db == nil {
		return
	}
	lookups := loadUint64Stat(&db.bufferLogLookupCount)
	bloomRejects := loadUint64Stat(&db.bufferLogBloomRejectCount)
	hits := loadUint64Stat(&db.bufferLogHitCount)
	hitBytes := loadUint64Stat(&db.bufferLogHitBytes)
	tombstoneHits := loadUint64Stat(&db.bufferLogTombstoneHitCount)
	misses := loadUint64Stat(&db.bufferLogMissCount)
	errors := loadUint64Stat(&db.bufferLogErrorCount)
	appends := loadUint64Stat(&db.bufferLogAppendAccountCount)
	appendedKVs := loadUint64Stat(&db.bufferLogAppendKVCount)
	appendBytes := loadUint64Stat(&db.bufferLogAppendBytes)
	bloomLoads := loadUint64Stat(&db.bufferLogBloomLoadCount)
	bloomLoadKVs := loadUint64Stat(&db.bufferLogBloomLoadKVCount)
	migrations := loadUint64Stat(&db.bufferLogMigrationCount)
	migratedKVs := loadUint64Stat(&db.bufferLogMigratedKVCount)
	lookupNanos := loadUint64Stat(&db.bufferLogLookupNanos)
	hitLookupCount := loadUint64Stat(&db.bufferLogHitLookupCount)
	hitLookupNanos := loadUint64Stat(&db.bufferLogHitLookupNanos)
	missLookupCount := loadUint64Stat(&db.bufferLogMissLookupCount)
	missLookupNanos := loadUint64Stat(&db.bufferLogMissLookupNanos)
	bloomCheckCount := loadUint64Stat(&db.bufferLogBloomCheckCount)
	bloomCheckNanos := loadUint64Stat(&db.bufferLogBloomCheckNanos)
	indexLookupCount := loadUint64Stat(&db.bufferLogIndexLookupCount)
	indexLookupNanos := loadUint64Stat(&db.bufferLogIndexLookupNanos)
	indexBuildCount := loadUint64Stat(&db.bufferLogIndexBuildCount)
	indexBuildNanos := loadUint64Stat(&db.bufferLogIndexBuildNanos)
	valueReadCount := loadUint64Stat(&db.bufferLogValueReadCount)
	valueReadNanos := loadUint64Stat(&db.bufferLogValueReadNanos)
	fullReadCount := loadUint64Stat(&db.bufferLogFullReadCount)
	fullReadNanos := loadUint64Stat(&db.bufferLogFullReadNanos)
	migrationTotalNanos := loadUint64Stat(&db.bufferLogMigrationTotalNanos)
	migrationLockCount := loadUint64Stat(&db.bufferLogMigrationLockCount)
	migrationLockNanos := loadUint64Stat(&db.bufferLogMigrationLockNanos)
	migrationReadCount := loadUint64Stat(&db.bufferLogMigrationReadCount)
	migrationReadNanos := loadUint64Stat(&db.bufferLogMigrationReadNanos)
	migrationReadBytes := loadUint64Stat(&db.bufferLogMigrationReadBytes)
	migrationDiskReadBytes := loadUint64Stat(&db.bufferLogMigrationDiskReadBytes)
	migrationSortCount := loadUint64Stat(&db.bufferLogMigrationSortCount)
	migrationSortNanos := loadUint64Stat(&db.bufferLogMigrationSortNanos)
	migrationPrepCount := loadUint64Stat(&db.bufferLogMigrationPrepCount)
	migrationPrepNanos := loadUint64Stat(&db.bufferLogMigrationPrepNanos)
	migrationWriteCount := loadUint64Stat(&db.bufferLogMigrationWriteCount)
	migrationWriteNanos := loadUint64Stat(&db.bufferLogMigrationWriteNanos)
	migrationWriteOps := loadUint64Stat(&db.bufferLogMigrationWriteOps)
	migrationWriteBytes := loadUint64Stat(&db.bufferLogMigrationWriteBytes)
	migrationFinishCount := loadUint64Stat(&db.bufferLogMigrationFinishCount)
	migrationFinishNanos := loadUint64Stat(&db.bufferLogMigrationFinishNanos)

	totalHits := hits + tombstoneHits
	hitRate := 0.0
	if lookups > 0 {
		hitRate = float64(totalHits) / float64(lookups) * 100.0
	}
	avgHitBytes := 0.0
	if hits > 0 {
		avgHitBytes = float64(hitBytes) / float64(hits)
	}
	avgMigratedKVs := 0.0
	if migrations > 0 {
		avgMigratedKVs = float64(migratedKVs) / float64(migrations)
	}
	avgAppendKVs := 0.0
	if appends > 0 {
		avgAppendKVs = float64(appendedKVs) / float64(appends)
	}

	fmt.Printf("PrefixDB bufferlog stats: lookups=%d bloomRejects=%d hits=%d tombstoneHits=%d misses=%d errors=%d hitRate=%0.2f%% hitBytes=%d avgHitBytes=%0.2f appends=%d appendedKVs=%d appendBytes=%d avgAppendKVs=%0.2f bloomLoads=%d bloomLoadKVs=%d migrations=%d migratedKVs=%d avgMigratedKVs=%0.2f\n",
		lookups,
		bloomRejects,
		hits,
		tombstoneHits,
		misses,
		errors,
		hitRate,
		hitBytes,
		avgHitBytes,
		appends,
		appendedKVs,
		appendBytes,
		avgAppendKVs,
		bloomLoads,
		bloomLoadKVs,
		migrations,
		migratedKVs,
		avgMigratedKVs,
	)
	if lookups > 0 || fullReadCount > 0 {
		fmt.Printf("PrefixDB bufferlog read latency: lookupTotal=%s lookupAvg=%0.2fus hitLookupCount=%d hitLookupTotal=%s hitLookupAvg=%0.2fus missLookupCount=%d missLookupTotal=%s missLookupAvg=%0.2fus bloomChecks=%d bloomTotal=%s bloomAvg=%0.2fus indexLookups=%d indexLookupTotal=%s indexLookupAvg=%0.2fus indexBuilds=%d indexBuildTotal=%s indexBuildAvg=%0.2fus valueReads=%d valueReadTotal=%s valueReadAvg=%0.2fus fullReads=%d fullReadTotal=%s fullReadAvg=%0.2fus\n",
			time.Duration(lookupNanos),
			averageMicros(lookupNanos, lookups),
			hitLookupCount,
			time.Duration(hitLookupNanos),
			averageMicros(hitLookupNanos, hitLookupCount),
			missLookupCount,
			time.Duration(missLookupNanos),
			averageMicros(missLookupNanos, missLookupCount),
			bloomCheckCount,
			time.Duration(bloomCheckNanos),
			averageMicros(bloomCheckNanos, bloomCheckCount),
			indexLookupCount,
			time.Duration(indexLookupNanos),
			averageMicros(indexLookupNanos, indexLookupCount),
			indexBuildCount,
			time.Duration(indexBuildNanos),
			averageMicros(indexBuildNanos, indexBuildCount),
			valueReadCount,
			time.Duration(valueReadNanos),
			averageMicros(valueReadNanos, valueReadCount),
			fullReadCount,
			time.Duration(fullReadNanos),
			averageMicros(fullReadNanos, fullReadCount),
		)
	}
	if migrations > 0 || migrationReadCount > 0 {
		persistNanos := migrationPrepNanos + migrationWriteNanos
		fmt.Printf("PrefixDB bufferlog migration latency: count=%d total=%s avg=%0.2fms lockCount=%d lockTotal=%s lockAvg=%0.2fus readCount=%d readTotal=%s readAvg=%0.2fus inputBytes=%d avgInputBytes=%0.2f diskReadBytes=%d avgDiskReadBytes=%0.2f sortCount=%d sortTotal=%s sortAvg=%0.2fus prepareCount=%d prepareTotal=%s prepareAvg=%0.2fus inlineWriteCount=%d inlineWriteTotal=%s inlineWriteAvg=%0.2fus persistTotal=%s persistAvg=%0.2fus writeOps=%d avgWriteOps=%0.2f writeBytes=%d avgWriteBytes=%0.2f finishCount=%d finishTotal=%s finishAvg=%0.2fus\n",
			migrations,
			time.Duration(migrationTotalNanos),
			averageMicros(migrationTotalNanos, migrations)/1000.0,
			migrationLockCount,
			time.Duration(migrationLockNanos),
			averageMicros(migrationLockNanos, migrationLockCount),
			migrationReadCount,
			time.Duration(migrationReadNanos),
			averageMicros(migrationReadNanos, migrationReadCount),
			migrationReadBytes,
			averageBytes(migrationReadBytes, migrationReadCount),
			migrationDiskReadBytes,
			averageBytes(migrationDiskReadBytes, migrationReadCount),
			migrationSortCount,
			time.Duration(migrationSortNanos),
			averageMicros(migrationSortNanos, migrationSortCount),
			migrationPrepCount,
			time.Duration(migrationPrepNanos),
			averageMicros(migrationPrepNanos, migrationPrepCount),
			migrationWriteCount,
			time.Duration(migrationWriteNanos),
			averageMicros(migrationWriteNanos, migrationWriteCount),
			time.Duration(persistNanos),
			averageMicros(persistNanos, migrationPrepCount),
			migrationWriteOps,
			averageBytes(migrationWriteOps, migrations),
			migrationWriteBytes,
			averageBytes(migrationWriteBytes, migrations),
			migrationFinishCount,
			time.Duration(migrationFinishNanos),
			averageMicros(migrationFinishNanos, migrationFinishCount),
		)
	}
	for bucket, label := range bufferLogSizeBucketLabels {
		stats := &db.bufferLogSizeAccessStats[bucket]
		bucketLookups := loadUint64Stat(&stats.lookups)
		if bucketLookups == 0 {
			continue
		}
		bucketBloomRejects := loadUint64Stat(&stats.bloomRejects)
		bucketIndexLookups := loadUint64Stat(&stats.indexLookups)
		bucketHits := loadUint64Stat(&stats.hits)
		bucketTombstoneHits := loadUint64Stat(&stats.tombstoneHits)
		bucketMisses := loadUint64Stat(&stats.misses)
		bucketErrors := loadUint64Stat(&stats.errors)
		bucketHitBytes := loadUint64Stat(&stats.hitBytes)
		bucketValueReadNanos := loadUint64Stat(&stats.valueReadNanos)
		bucketTotalHits := bucketHits + bucketTombstoneHits
		bucketHitRate := 0.0
		if bucketLookups > 0 {
			bucketHitRate = float64(bucketTotalHits) / float64(bucketLookups) * 100.0
		}
		fmt.Printf("PrefixDB bufferlog size access bucket=%s lookups=%d bloomRejects=%d indexLookups=%d hits=%d tombstoneHits=%d misses=%d errors=%d hitRate=%0.2f%% hitBytes=%d avgHitBytes=%0.2f valueReadTotal=%s valueReadAvg=%0.2fus\n",
			label,
			bucketLookups,
			bucketBloomRejects,
			bucketIndexLookups,
			bucketHits,
			bucketTombstoneHits,
			bucketMisses,
			bucketErrors,
			bucketHitRate,
			bucketHitBytes,
			averageBytes(bucketHitBytes, bucketHits),
			time.Duration(bucketValueReadNanos),
			averageMicros(bucketValueReadNanos, bucketHits),
		)
	}
}

func (db *PrefixDB) Close() error {
	errs := []error{}
	// Flush any pending storage batch writes before tearing down files.
	if db.storageBatch != nil {
		if err := db.StorageBatchCommit(); err != nil {
			// best-effort: keep closing even if batch commit fails
			errs = append(errs, fmt.Errorf("failed to commit storage batch: %v", err))
		}
		db.stopStorageBatcher()
	}

	db.stopStorageGCWorker()
	db.bufferLogMigrationWaitGroup.Wait()

	if analysisStatsEnabled {
		fmt.Printf("PrefixDB GC stats: count=%d writeBytes=%d\n",
			atomic.LoadUint64(&db.GCCount),
			atomic.LoadUint64(&db.GCWriteBytes),
		)
		getReqs := atomic.LoadUint64(&db.getReadReqCount)
		getReadBytes := atomic.LoadUint64(&db.getReadBytesSum)
		avgGetReadBytes := float64(0)
		if getReqs > 0 {
			avgGetReadBytes = float64(getReadBytes) / float64(getReqs)
		}
		fmt.Printf("PrefixDB commit old KV read stats: pairs=%d bytes=%d\n",
			atomic.LoadUint64(&db.commitOldKVReadCount),
			atomic.LoadUint64(&db.commitOldKVReadBytes),
		)
		fmt.Printf("PrefixDB get read stats: requests=%d totalBytes=%d avgBytes=%.2f\n",
			getReqs,
			getReadBytes,
			avgGetReadBytes,
		)
		fmt.Printf("PrefixDB TrieNodeStorage fetch stats: cachePairs=%d cacheBytes=%d logPairs=%d logBytes=%d\n",
			atomic.LoadUint64(&db.trieStorageCachePairs),
			atomic.LoadUint64(&db.trieStorageCacheBytes),
			atomic.LoadUint64(&db.trieStorageLogPairs),
			atomic.LoadUint64(&db.trieStorageLogBytes),
		)
		printTrieGetBreakdownStep("TrieNodeAccount", "index-locate", &db.trieAccountGetStats.indexLocate)
		printTrieGetBreakdownStep("TrieNodeAccount", "io-read", &db.trieAccountGetStats.ioRead)
		printTrieGetBreakdownStep("TrieNodeAccount", "search", &db.trieAccountGetStats.search)
		printTrieStorageGetBreakdownStep("account-entry", &db.trieStorageAccountEntryStats)
		printTrieStorageGetBreakdownStep("segment-index", &db.trieStorageSegmentIndexStats)
		printTrieStorageSegmentIndexLayerStats(&db.trieStorageSegmentIndexLayerStats)
		printCacheMissCostStats("account-data", &db.accountDataMissStats)
		printCacheMissCostStats("storage-data", &db.storageDataMissStats)
		printPrefixDBNotFoundStats(db)
		printSharedCacheLockStats(db.sharedCache)
		printTrieStoragePrefetchStats(db)
		printBufferLogStats(db)
		printTrieStorageGetBreakdownStep("storage-kv-pairs", &db.trieStorageKVStats)
		lookups := atomic.LoadUint64(&db.nodeCacheLookups)
		hits := atomic.LoadUint64(&db.nodeCacheHits)
		misses := atomic.LoadUint64(&db.nodeCacheMisses)
		served := atomic.LoadUint64(&db.nodeCacheServed)
		toNodeFile := atomic.LoadUint64(&db.nodeCacheToNodeFile)
		missToNodeFile := atomic.LoadUint64(&db.nodeCacheMissToNodeFile)
		hitFallbackToNodeFile := atomic.LoadUint64(&db.nodeCacheHitFallbackToNodeFile)
		fallback := uint64(0)
		if hits >= served {
			fallback = hits - served
		}
		fmt.Printf("PrefixDB nodeCache stats: lookups=%d hits=%d misses=%d served=%d fallback=%d toNodeFile=%d missToNodeFile=%d hitFallbackToNodeFile=%d\n",
			lookups, hits, misses, served, fallback, toNodeFile, missToNodeFile, hitFallbackToNodeFile,
		)
		db.printDiskIOStats()
	}

	if err := db.flushStorageBuffer(); err != nil {
		errs = append(errs, fmt.Errorf("failed to flush storage buffer: %v", err))
	}

	if db.nodeCache != nil {
		db.nodeCache.Close()
	}
	if db.currentSegmentChunkBuffer != nil {
		db.currentSegmentChunkBuffer.Close()
	}

	// if db.storageCache != nil {
	// 	db.storageCache.Close()
	// }

	// forbid further writes to the database
	if db.accountBatch != nil {
		db.accountBatch.DisableAutoCommit()

		// wait for any ongoing background commit to finish
		if db.accountBatch.bgCommit {
			db.accountBatch.DisableBackgroundCommit()
		}
	}

	if db.accountBatch != nil {
		if len(db.accountBatch.operations) > 0 {
			if err := db.WriteCommit(db.accountBatch); err != nil {
				fmt.Printf("Error committing batch operations: %v\n", err)
			}
		}
	}

	if err := db.prefixTree.Close(); err != nil {
		return fmt.Errorf("failed to close prefix tree: %v", err)
	}

	if err := db.accountFile.Close(); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
	}

	db.nodeCache = nil
	db.accountBatch = nil

	if db.storageCurFile != nil {
		_ = db.storageCurFile.Close()
		db.storageCurFile = nil
	}
	// db.accountHashKeyPebble = nil
	db.printDiskIOStats()

	if len(errs) > 0 {
		fmt.Printf("Errors occurred during closing: %v\n", errs)
		return errs[0]
	}
	return nil
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

func (db *PrefixDB) normalizeStorageKey(rawKey []byte) ([]byte, error) {

	// Storage keys are expected to include the account-hash prefix: 'O' + 32-byte account hash.
	if len(rawKey) < storageKeyTrimOffset {
		return nil, errors.New("invalid storage key")
	}
	if len(rawKey) == storageKeyTrimOffset {
		// Root storage key marker.
		// IMPORTANT: return a single byte 0x4f (same as 'O'), not the ASCII bytes for "4f".
		// This key is used within an account-scoped storage segment.
		return []byte{0x4f}, nil
	}
	return rawKey[storageKeyTrimOffset:], nil
}

func (db *PrefixDB) storeNode(key []byte, node *TrieNode) error {
	return db.prefixTree.PutNode(key, node)
}

type pendingNodeCacheUpdate struct {
	key           string
	delete        bool
	accountOffset uint64
	accountSize   uint32
	storageInfo   StorageInfo
}

func (db *PrefixDB) applyNodeBatch(entries []NodeInfo, cacheUpdates []pendingNodeCacheUpdate, blockID uint64) error {
	if len(entries) > 0 {
		if db.prefixTree == nil {
			return errors.New("prefix tree not initialized")
		}
		if err := db.prefixTree.PutNodeInfosWithBlockID(entries, blockID); err != nil {
			return err
		}
	}
	if db.nodeCache == nil {
		return nil
	}
	for _, update := range cacheUpdates {
		if update.delete {
			db.nodeCache.Delete(update.key)
			continue
		}
		db.nodeCache.StoreMetadata(update.key, update.accountOffset, update.accountSize, update.storageInfo)
	}
	return nil
}

func (db *PrefixDB) shouldBypassNodeCache(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	return len(key) < MaxPrefixDepth
}

func (db *PrefixDB) getNode(key []byte) (*TrieNode, error) {
	node, _, err := db.getNodeWithSource(key)
	return node, err
}

func (db *PrefixDB) getNodeWithSource(key []byte) (*TrieNode, bool, error) {
	cacheKey := string(key)
	cacheHit := false
	useNodeCache := !db.shouldBypassNodeCache(key)
	if useNodeCache {
		addUint64Stat(&db.nodeCacheLookups, 1)
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			cacheHit = true
			addUint64Stat(&db.nodeCacheHits, 1)
			if entry.StorageInfo.storageFileID != 0 {
				addUint64Stat(&db.nodeCacheServed, 1)
				return nodeInfoToTrieNode(NodeInfo{
					accountOffset: entry.AccountOffset,
					accountSize:   entry.AccountSize,
					storageFileID: entry.StorageInfo.storageFileID,
					storageOffset: entry.StorageInfo.storageOffset,
					storageSize:   entry.StorageInfo.storageSize,
				}), true, nil
			}
		} else {
			addUint64Stat(&db.nodeCacheMisses, 1)
		}
	}

	if useNodeCache {
		addUint64Stat(&db.nodeCacheToNodeFile, 1)
		if cacheHit {
			addUint64Stat(&db.nodeCacheHitFallbackToNodeFile, 1)
		} else {
			addUint64Stat(&db.nodeCacheMissToNodeFile, 1)
		}
	}
	nodeInfo, found, err := db.prefixTree.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	node := nodeInfoToTrieNode(nodeInfo)
	// accountOffset==0 is a tombstone delete for account nodes.
	if node.accountOffset == 0 && node.storageFileID == 0 {
		return nil, false, nil
	}
	if useNodeCache {
		db.nodeCache.StoreMetadata(cacheKey, node.accountOffset, node.accountSize, StorageInfo{
			storageFileID: node.storageFileID,
			storageOffset: node.storageOffset,
			storageSize:   node.storageSize,
		})

		// cacheKeyHex := hex.EncodeToString([]byte(cacheKey))
		// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", node.storageFileID) + ", offset:" + fmt.Sprintf("%d", node.storageOffset) + ", size:" + fmt.Sprintf("%d", node.storageSize))

		if nodeInfoGet, found := db.nodeCache.Get(cacheKey); found {
			if nodeInfoGet.StorageInfo.storageFileID != node.storageFileID {
				fmt.Printf("Metadata store mismatch for key %s: expected file ID %d, got %d\n", string(key), node.storageFileID, nodeInfoGet.StorageInfo.storageFileID)
			}
		} else {
			fmt.Printf("Failed to retrieve metadata for key %s after storing it\n", string(key))
		}
	}
	return node, false, nil
}

func (db *PrefixDB) getAccountNode(key []byte) (*TrieNode, error) {
	return db.getAccountNodeWithBreakdown(key, nil)
}

func (db *PrefixDB) getAccountNodeWithBreakdown(key []byte, breakdown *trieGetBreakdownStats) (*TrieNode, error) {
	cacheKey := string(key)
	cacheHit := false
	useNodeCache := !db.shouldBypassNodeCache(key)
	if useNodeCache {
		addUint64Stat(&db.nodeCacheLookups, 1)
		if entry, ok := db.nodeCache.Get(cacheKey); ok {
			cacheHit = true
			addUint64Stat(&db.nodeCacheHits, 1)
			if entry.AccountOffset != 0 || entry.StorageInfo.storageFileID != 0 || entry.Value != nil {
				recordTrieStorageGetBreakdownStep(func() *trieStorageGetBreakdownStepStats {
					if breakdown == nil {
						return nil
					}
					return &breakdown.indexLocate
				}(), true, 0)
				recordTrieStorageGetBreakdownStep(func() *trieStorageGetBreakdownStepStats {
					if breakdown == nil {
						return nil
					}
					return &breakdown.search
				}(), true, 0)
				addUint64Stat(&db.nodeCacheServed, 1)
				return nodeInfoToTrieNode(NodeInfo{
					accountOffset: entry.AccountOffset,
					accountSize:   entry.AccountSize,
					storageFileID: entry.StorageInfo.storageFileID,
					storageOffset: entry.StorageInfo.storageOffset,
					storageSize:   entry.StorageInfo.storageSize,
				}), nil
			}
		} else {
			addUint64Stat(&db.nodeCacheMisses, 1)
		}
	}

	if useNodeCache {
		addUint64Stat(&db.nodeCacheToNodeFile, 1)
		if cacheHit {
			addUint64Stat(&db.nodeCacheHitFallbackToNodeFile, 1)
		} else {
			addUint64Stat(&db.nodeCacheMissToNodeFile, 1)
		}
	}

	nodeInfo, found, err := db.prefixTree.GetWithBreakdown(key, breakdown)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	node := nodeInfoToTrieNode(nodeInfo)
	// accountOffset==0 is a tombstone delete for account nodes.
	if node.accountOffset == 0 && node.storageFileID == 0 {
		return nil, nil
	}
	if useNodeCache {
		db.nodeCache.StoreMetadata(cacheKey, node.accountOffset, node.accountSize, StorageInfo{
			storageFileID: node.storageFileID,
			storageOffset: node.storageOffset,
			storageSize:   node.storageSize,
		})
	}
	return node, nil
}

func (db *PrefixDB) openOrCreateStorageFile() error {
	db.storageFileMu.Lock()
	defer db.storageFileMu.Unlock()
	return db.openOrCreateStorageFileLocked()
}

func (db *PrefixDB) openOrCreateStorageFileLocked() error {
	// find max FileID
	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		return fmt.Errorf("failed to read storage directory: %v", err)
	}

	var maxID uint32 = 0
	var maxSegmentID uint32 = 0
	for _, e := range entries {
		if e.IsDir() {
			var segID uint32
			if n, _ := fmt.Sscanf(e.Name(), "%08d", &segID); n == 1 && segID > maxSegmentID {
				maxSegmentID = segID
			}
			continue
		}
		var id uint32
		n, _ := fmt.Sscanf(e.Name(), "storage_%08d.dat", &id)
		if n == 1 && id > maxID {
			maxID = id
		}
	}
	tryID := maxID
	if maxSegmentID > db.segmentDirSeq {
		db.segmentDirSeq = maxSegmentID
	}
	path := func(id uint32) string { return filepath.Join(db.storageDir, fmt.Sprintf("storage_%08d.dat", id)) }

	if tryID > 0 {
		p := path(tryID)
		file, err := os.OpenFile(p, os.O_RDWR, 0644)
		if err == nil {
			fi, _ := file.Stat()
			if fi.Size() < storageMaxFileSize && fi != nil {
				db.storageCurFile = file
				db.storageCurFileID = tryID
				db.storageCurSize = fi.Size()
				return nil
			}
			file.Close()
		}
	}

	newID := maxID + 1
	p := path(newID)
	file, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create storage file: %v", err)
	}
	db.storageCurFile = file
	db.storageCurFileID = newID
	db.storageCurSize = 0
	return nil
}

func (db *PrefixDB) ensureStorageCapacity(need int64) error {
	db.storageFileMu.Lock()
	defer db.storageFileMu.Unlock()
	return db.ensureStorageCapacityLocked(need)
}

func (db *PrefixDB) ensureStorageCapacityLocked(need int64) error {
	// if need > storageMaxFileSize {
	// 	return errors.New("need size lager than storageMaxFileSize")
	// }

	if db.storageCurFile == nil {
		return db.openOrCreateStorageFileLocked()
	}
	if db.storageCurSize+need > storageMaxFileSize {
		db.storageCurFile.Close()
		db.storageCurFile = nil
		db.storageCurSize = 0
		db.storageCurFileID++
		p := filepath.Join(db.storageDir, fmt.Sprintf("storage_%08d.dat", db.storageCurFileID))
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		db.storageCurFile = f
	}
	return nil
}

// Common storage segment format: [keyLen u16][valLen u16][key][val]...
func (db *PrefixDB) serializeStorageSegment(kvs []kvPair) ([]byte, func(), int, error) {
	total := 0
	for _, v := range kvs {
		if len(v.key) > 0xFFFF {
			return nil, func() {}, 0, fmt.Errorf("key too large: %d", len(v.key))
		}
		if len(v.val) > 0xFFFF {
			return nil, func() {}, 0, fmt.Errorf("value too large: %d", len(v.val))
		}
		total += segmentedChunkEntryHeaderSize + len(v.key) + len(v.val)
	}

	buf := getDataBuffer(total)
	release := func() {
		putDataBuffer(buf)
	}
	offset := 0
	var header [segmentedChunkEntryHeaderSize]byte

	for _, v := range kvs {
		writeUint16BE(header[:2], uint16(len(v.key)))
		writeUint16BE(header[2:4], uint16(len(v.val)))
		copy(buf[offset:], header[:])
		offset += segmentedChunkEntryHeaderSize
		copy(buf[offset:], v.key)
		offset += len(v.key)
		copy(buf[offset:], v.val)
		offset += len(v.val)
	}
	return buf, release, total, nil
}

// Segmented chunk format: [key][val][keyLen u16][valLen u16]...
func serializeChunkPayload(kvs []kvPair) ([]byte, func(), int, error) {
	total := 0
	for _, v := range kvs {
		if len(v.key) > 0xFFFF {
			return nil, func() {}, 0, fmt.Errorf("key too large: %d", len(v.key))
		}
		if len(v.val) > 0xFFFF {
			return nil, func() {}, 0, fmt.Errorf("value too large for segmented chunk: %d", len(v.val))
		}
		total += segmentedChunkEntryHeaderSize + len(v.key) + len(v.val)
	}

	buf := getDataBuffer(total)
	release := func() {
		putDataBuffer(buf)
	}
	offset := 0
	for _, v := range kvs {
		copy(buf[offset:], v.key)
		offset += len(v.key)
		copy(buf[offset:], v.val)
		offset += len(v.val)
		writeUint16BE(buf[offset:offset+2], uint16(len(v.key)))
		writeUint16BE(buf[offset+2:offset+4], uint16(len(v.val)))
		offset += segmentedChunkEntryHeaderSize
	}
	return buf, release, total, nil
}

// appendStorageSegment appends a serialized storage segment to the storage file and returns its file ID, offset, and size.

func (db *PrefixDB) appendStorageSegment(kvs []kvPair) (fileID uint32, offset uint64, size uint64, err error) {
	seg, release, _, err := db.serializeStorageSegment(kvs)
	if err != nil {
		return 0, 0, 0, err
	}
	defer release()
	need := int64(len(seg))
	db.storageFileMu.Lock()
	defer db.storageFileMu.Unlock()
	if err := db.ensureStorageCapacityLocked(need); err != nil {
		return 0, 0, 0, err
	}
	offset = uint64(db.storageCurSize)
	if _, err := db.storageCurFile.WriteAt(seg, int64(offset)); err != nil {
		return 0, 0, 0, err
	}
	db.addDiskWrite(diskIOUsageStorageCommonLogs, len(seg))
	db.storageCurSize += need
	return db.storageCurFileID, offset, uint64(need), nil
}

func (db *PrefixDB) prepareStorageEntriesForCommit(accountKey []byte, kvs []kvPair, existingFileID uint32, existingOffset uint64, existingSize uint64, blockID uint64) (StorageInfo, []byte, error) {
	if len(kvs) == 0 {
		return StorageInfo{}, nil, nil
	}
	if isSegmentedStorage(existingFileID) {
		kvs = dedupSortedKVPairs(kvs)
		if isAccountNamedSegmentedStorage(existingFileID) {
			fileID, offset, size, err := db.updateAccountNamedSegmentedStorageWithBlockID(accountKey, kvs, blockID)
			if err != nil {
				return StorageInfo{}, nil, err
			}
			return StorageInfo{storageFileID: fileID, storageOffset: offset, storageSize: size}, nil, nil
		}
		return StorageInfo{}, nil, errors.New("legacy segmented storage pointers are no longer supported")
	}
	merged := kvs
	var existingBacking *bufferLease
	if existingFileID != 0 && existingSize > 0 {
		existingEntries, backing, err := db.readStorageSegmentPairs(existingFileID, existingOffset, existingSize)
		if err != nil {
			return StorageInfo{}, nil, err
		}
		db.addCommitOldKVReadStats(len(existingEntries), existingSize)
		if backing != nil {
			existingBacking = backing
		}
		if len(existingEntries) > 0 {
			merged = mergeAndDedupPairs(existingEntries, kvs)
		}
	}
	if existingBacking != nil {
		defer existingBacking.Release()
	}
	if len(merged) == 0 {
		return StorageInfo{}, nil, nil
	}
	if estimateSegmentSize(merged) <= db.storageChunkSize {
		seg, release, _, err := db.serializeStorageSegment(merged)
		if err != nil {
			return StorageInfo{}, nil, err
		}
		defer release()
		payload := append([]byte(nil), seg...)
		payload = appendForwardCommitTag(payload, blockID)
		return StorageInfo{storageSize: uint64(len(payload))}, payload, nil
	}
	fileID, offset, size, err := db.appendSegmentedStorage(accountKey, merged, blockID)
	if err != nil {
		return StorageInfo{}, nil, err
	}
	return StorageInfo{storageFileID: fileID, storageOffset: offset, storageSize: size}, nil, nil
}

func (db *PrefixDB) appendPreparedInlineStorageSegments(plans []storageCommitPlan) error {
	var pendingCount int
	for _, plan := range plans {
		if len(plan.inlineSegment) > 0 {
			pendingCount++
		}
	}
	if pendingCount == 0 {
		return nil
	}

	db.storageFileMu.Lock()
	defer db.storageFileMu.Unlock()

	var (
		batch       []byte
		batchStart  int64
		batchFileID uint32
	)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, err := db.storageCurFile.WriteAt(batch, batchStart); err != nil {
			return err
		}
		db.addDiskWrite(diskIOUsageStorageCommonLogs, len(batch))
		db.storageCurSize = batchStart + int64(len(batch))
		batch = nil
		batchStart = 0
		batchFileID = 0
		return nil
	}

	for idx := range plans {
		payload := plans[idx].inlineSegment
		if len(payload) == 0 {
			continue
		}
		need := int64(len(payload))
		if need <= 0 {
			plans[idx].inlineSegment = nil
			continue
		}
		if len(batch) == 0 {
			if err := db.ensureStorageCapacityLocked(need); err != nil {
				return err
			}
			batchStart = db.storageCurSize
			batchFileID = db.storageCurFileID
		}
		if db.storageCurSize+int64(len(batch))+need > storageMaxFileSize {
			if err := flushBatch(); err != nil {
				return err
			}
			if err := db.ensureStorageCapacityLocked(need); err != nil {
				return err
			}
			batchStart = db.storageCurSize
			batchFileID = db.storageCurFileID
		}

		offset := uint64(batchStart + int64(len(batch)))
		plans[idx].storageInfo.storageFileID = batchFileID
		plans[idx].storageInfo.storageOffset = offset
		plans[idx].storageInfo.storageSize = uint64(len(payload))
		plans[idx].skipNodeWrite = shouldSkipAccountEntryPointerUpdate(
			plans[idx].existingInfo.storageFileID,
			batchFileID,
			offset,
			uint64(len(payload)),
		)
		batch = append(batch, payload...)
		plans[idx].inlineSegment = nil
	}

	return flushBatch()
}

func (db *PrefixDB) persistStorageEntries(accountKey []byte, kvs []kvPair, existingFileID uint32, existingOffset uint64, existingSize uint64) (uint32, uint64, uint64, error) {
	info, inlineSegment, err := db.prepareStorageEntriesForCommit(accountKey, kvs, existingFileID, existingOffset, existingSize, 0)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(inlineSegment) > 0 {
		fileID, offset, size, err := db.appendStorageSegmentRaw(inlineSegment)
		if err != nil {
			return 0, 0, 0, err
		}
		return fileID, offset, size, nil
	}
	return info.storageFileID, info.storageOffset, info.storageSize, nil
}

func (db *PrefixDB) appendStorageSegmentRaw(seg []byte) (fileID uint32, offset uint64, size uint64, err error) {
	need := int64(len(seg))
	db.storageFileMu.Lock()
	defer db.storageFileMu.Unlock()
	if err := db.ensureStorageCapacityLocked(need); err != nil {
		return 0, 0, 0, err
	}
	offset = uint64(db.storageCurSize)
	if _, err := db.storageCurFile.WriteAt(seg, int64(offset)); err != nil {
		return 0, 0, 0, err
	}
	db.addDiskWrite(diskIOUsageStorageCommonLogs, len(seg))
	db.storageCurSize += need
	return db.storageCurFileID, offset, uint64(need), nil
}

func estimateSegmentSize(kvs []kvPair) int {
	total := 0
	for _, kv := range kvs {
		total += segmentedChunkEntryHeaderSize + len(kv.key) + len(kv.val)
	}
	return total
}

func (db *PrefixDB) appendSegmentedStorage(accountKey []byte, kvs []kvPair, blockID uint64) (uint32, uint64, uint64, error) {
	if len(accountKey) == 0 {
		return 0, 0, 0, errors.New("account key required for segmented storage")
	}
	return db.rewriteAccountNamedSegmentedStorageWithBlockID(accountKey, kvs, blockID)
}

func (db *PrefixDB) rewriteAccountNamedSegmentedStorage(accountKey []byte, kvs []kvPair) (uint32, uint64, uint64, error) {
	return db.rewriteAccountNamedSegmentedStorageWithBlockID(accountKey, kvs, 0)
}

func (db *PrefixDB) rewriteAccountNamedSegmentedStorageWithBlockID(accountKey []byte, kvs []kvPair, blockID uint64) (uint32, uint64, uint64, error) {
	if len(accountKey) == 0 {
		return 0, 0, 0, errors.New("account key required for account-named segmented storage")
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry, blockID)
}

func (db *PrefixDB) rewriteAccountNamedSegmentedStorageWithLockHeld(accountKey []byte, kvs []kvPair) (uint32, uint64, uint64, error) {
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry, 0)
}

func (db *PrefixDB) rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath string, accountKey []byte, kvs []kvPair, entry *segmentIndexFolderLock, blockID uint64) (uint32, uint64, uint64, error) {
	var oldMetas []segmentChunkMeta
	if metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath); err == nil {
		oldMetas = cloneSegmentChunkMetas(metas)
	} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errSegmentIndexEntryNotFound) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return 0, 0, 0, err
	}
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		return 0, 0, 0, err
	}
	chunkMetas, err := db.writeSegmentedChunksToFolderWithBlockID(folderPath, kvs, blockID)
	if err != nil {
		return 0, 0, 0, err
	}
	if err := db.writeSegmentIndexLocked(folderPath, chunkMetas, entry); err != nil {
		return 0, 0, 0, err
	}
	keep := make(map[string]struct{}, len(chunkMetas))
	for _, meta := range chunkMetas {
		keep[meta.FileName] = struct{}{}
	}
	if err := removeStaleSegmentChunkFiles(folderPath, keep); err != nil {
		return 0, 0, 0, err
	}
	for _, meta := range oldMetas {
		if _, ok := keep[meta.FileName]; ok {
			// Existing file names may be overwritten with new payloads; drop stale cached data.
			db.removeCachedSegmentChunkEntries(folderPath, meta.FileName)
			continue
		}
		db.removeCachedSegmentChunkEntries(folderPath, meta.FileName)
	}
	db.markAccountStorageFolder(accountKey)
	db.invalidateSegmentIndexLayoutForPath(folderPath)
	return segmentedStorageFlag, 0, 0, nil
}

func (db *PrefixDB) updateAccountNamedSegmentedStorage(accountKey []byte, kvs []kvPair) (uint32, uint64, uint64, error) {
	return db.updateAccountNamedSegmentedStorageWithBlockID(accountKey, kvs, 0)
}

func (db *PrefixDB) updateAccountNamedSegmentedStorageWithBlockID(accountKey []byte, kvs []kvPair, blockID uint64) (uint32, uint64, uint64, error) {
	if len(accountKey) == 0 {
		return 0, 0, 0, errors.New("account key required for account-named segmented storage")
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry, blockID)
		}
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if errors.Is(err, errSegmentIndexEntryNotFound) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, os.ErrNotExist) || !fileExists(indexPath) {
			return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry, blockID)
		}
		return 0, 0, 0, err
	}
	if len(metas) == 0 {
		return db.rewriteAccountNamedSegmentedStorageWithFolderLockHeld(folderPath, accountKey, kvs, entry, blockID)
	}
	orderChanged := false
	metas = normalizeSegmentChunkMetasOrder(metas, &orderChanged)
	allocator := newChunkFileAllocator(metas)
	buckets, unmatched := partitionEntriesByChunks(metas, kvs)
	updated := make([]segmentChunkMeta, 0, len(metas)+len(kvs)/64+1)
	bufferReplacements := make([]committedChunkBufferReplacement, 0)
	indexDirty := orderChanged
	for idx, meta := range metas {
		additions := buckets[idx]
		if len(additions) == 0 {
			updated = append(updated, meta)
			continue
		}
		oldWasRead := db.isReadSegmentChunkCached(folderPath, meta.FileName)
		chunkMetas, newChunks, mutateErr := db.mutateSegmentChunk(folderPath, meta, additions, allocator, blockID)
		if mutateErr != nil {
			return 0, 0, 0, mutateErr
		}
		if len(chunkMetas) == 0 {
			bufferReplacements = append(bufferReplacements, committedChunkBufferReplacement{
				oldFileName: meta.FileName,
				oldWasRead:  oldWasRead,
			})
			indexDirty = true
			continue
		}
		if len(chunkMetas) != 1 || chunkMetas[0].FileName != meta.FileName || !bytes.Equal(chunkMetas[0].KeyStart, meta.KeyStart) {
			if len(chunkMetas) != 1 || chunkMetas[0].FileName != meta.FileName {
				bufferReplacements = append(bufferReplacements, committedChunkBufferReplacement{
					oldFileName: meta.FileName,
					newChunks:   newChunks,
					oldWasRead:  oldWasRead,
				})
			}
			indexDirty = true
		}
		updated = append(updated, chunkMetas...)
	}
	// Handle unmatched KV pairs by creating new chunks
	if len(unmatched) > 0 {
		// Sort unmatched pairs to ensure proper ordering
		sortKVPairs(unmatched)
		// Create new chunks for unmatched pairs using the allocator to avoid filename conflicts
		newChunkMetas, err := db.writeSegmentedChunksToFolderWithAllocatorAndBlockID(folderPath, unmatched, allocator, blockID)
		if err != nil {
			return 0, 0, 0, err
		}
		// Append new chunks to the updated list
		updated = append(updated, newChunkMetas...)
		indexDirty = true
	}
	if indexDirty {
		if err := db.writeSegmentIndexLocked(folderPath, updated, entry); err != nil {
			return 0, 0, 0, err
		}
		db.syncCommittedChunkBufferReplacements(folderPath, bufferReplacements)
		db.invalidateSegmentIndexLayoutForPath(folderPath)
	}
	db.markAccountStorageFolder(accountKey)
	return segmentedStorageFlag, 0, 0, nil
}

func (db *PrefixDB) writeSegmentedChunksToFolder(folderPath string, kvs []kvPair) ([]segmentChunkMeta, error) {
	return db.writeSegmentedChunksToFolderWithBlockID(folderPath, kvs, 0)
}

func (db *PrefixDB) writeSegmentedChunksToFolderWithBlockID(folderPath string, kvs []kvPair, blockID uint64) ([]segmentChunkMeta, error) {
	return db.writeSegmentedChunksToFolderWithAllocatorAndBlockID(folderPath, kvs, nil, blockID)
}

func (db *PrefixDB) writeSegmentedChunksToFolderWithAllocator(folderPath string, kvs []kvPair, allocator *chunkFileAllocator) ([]segmentChunkMeta, error) {
	return db.writeSegmentedChunksToFolderWithAllocatorAndBlockID(folderPath, kvs, allocator, 0)
}

func (db *PrefixDB) writeSegmentedChunksToFolderWithAllocatorAndBlockID(folderPath string, kvs []kvPair, allocator *chunkFileAllocator, blockID uint64) ([]segmentChunkMeta, error) {
	chunkMetas := make([]segmentChunkMeta, 0)
	chunk := make([]kvPair, 0)
	chunkSize := 0
	chunkIdx := 0
	flushChunk := func() error {
		if len(chunk) == 0 {
			return nil
		}
		seg, release, _, err := serializeChunkPayload(chunk)
		if err != nil {
			return err
		}
		defer release()
		payload := append([]byte(nil), seg...)
		payload = appendChunkCommitTag(payload, blockID)
		// Use allocator if provided to generate unique chunk filenames
		var name string
		if allocator != nil {
			name = allocator.nextName()
		} else {
			name = chunkFileNameForOrdinal(uint32(chunkIdx))
		}
		fullPath := filepath.Join(folderPath, name)
		if err := db.writeFileWithStats(fullPath, payload, 0o644, diskIOUsageStorageSeparatedLogs); err != nil {
			return err
		}
		chunkMetas = append(chunkMetas, segmentChunkMeta{
			FileName: name,
			KeyStart: cloneBytes(chunk[0].key),
		})
		chunk = make([]kvPair, 0)
		chunkSize = 0
		chunkIdx++
		return nil
	}
	for _, kv := range kvs {
		sz := segmentedChunkEntryHeaderSize + len(kv.key) + len(kv.val)
		if chunkSize+sz > db.storageChunkSize && len(chunk) > 0 {
			if err := flushChunk(); err != nil {
				return nil, err
			}
		}
		chunk = append(chunk, kv)
		chunkSize += sz
	}
	if err := flushChunk(); err != nil {
		return nil, err
	}
	if len(chunkMetas) == 0 {
		return nil, errors.New("failed to build segmented storage chunks")
	}
	return chunkMetas, nil
}

func dedupSortedKVPairs(kvs []kvPair) []kvPair {
	if len(kvs) < 2 {
		return kvs
	}
	out := kvs[:0]
	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && bytes.Equal(kvs[j].key, kvs[i].key) {
			j++
		}
		out = append(out, kvs[j-1])
		i = j
	}
	return out
}

func (db *PrefixDB) updateSegmentedStorageWithLockHeld(existingFileID uint32, kvs []kvPair) (uint32, uint64, uint64, error) {
	folderID := existingFileID & ^segmentedStorageFlag
	folderPath := db.segmentedFolderPath(folderID)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(metas) == 0 {
		return 0, 0, 0, fmt.Errorf("segment index missing for folder %d", folderID)
	}
	orderChanged := false
	metas = normalizeSegmentChunkMetasOrder(metas, &orderChanged)
	allocator := newChunkFileAllocator(metas)
	buckets, unmatched := partitionEntriesByChunks(metas, kvs)
	updated := make([]segmentChunkMeta, 0, len(metas)+len(kvs)/64+1)
	bufferReplacements := make([]committedChunkBufferReplacement, 0)
	indexDirty := orderChanged
	for idx, meta := range metas {
		additions := buckets[idx]
		if len(additions) == 0 {
			updated = append(updated, meta)
			continue
		}
		oldWasRead := db.isReadSegmentChunkCached(folderPath, meta.FileName)
		chunkMetas, newChunks, err := db.mutateSegmentChunk(folderPath, meta, additions, allocator, 0)
		if err != nil {
			return 0, 0, 0, err
		}
		if len(chunkMetas) == 0 {
			bufferReplacements = append(bufferReplacements, committedChunkBufferReplacement{
				oldFileName: meta.FileName,
				oldWasRead:  oldWasRead,
			})
			indexDirty = true
			continue
		}
		if len(chunkMetas) != 1 || chunkMetas[0].FileName != meta.FileName || !bytes.Equal(chunkMetas[0].KeyStart, meta.KeyStart) {
			if len(chunkMetas) != 1 || chunkMetas[0].FileName != meta.FileName {
				bufferReplacements = append(bufferReplacements, committedChunkBufferReplacement{
					oldFileName: meta.FileName,
					newChunks:   newChunks,
					oldWasRead:  oldWasRead,
				})
			}
			indexDirty = true
		}
		updated = append(updated, chunkMetas...)
	}
	// Handle unmatched KV pairs by creating new chunks
	if len(unmatched) > 0 {
		// Sort unmatched pairs to ensure proper ordering
		sortKVPairs(unmatched)
		// Create new chunks for unmatched pairs using the allocator to avoid filename conflicts
		newChunkMetas, err := db.writeSegmentedChunksToFolderWithAllocator(folderPath, unmatched, allocator)
		if err != nil {
			return 0, 0, 0, err
		}
		// Append new chunks to the updated list
		updated = append(updated, newChunkMetas...)
		indexDirty = true
	}
	if indexDirty {
		if err := db.writeSegmentIndexLocked(folderPath, updated, entry); err != nil {
			return 0, 0, 0, err
		}
		db.syncCommittedChunkBufferReplacements(folderPath, bufferReplacements)
		db.invalidateSegmentIndexLayoutForPath(folderPath)
		db.refreshSegmentIndexCacheByPathLocked(folderPath, updated)
	}
	return existingFileID, 0, 0, nil
}

// partitionEntriesByChunks assigns sorted kvs to chunk ranges using binary search
// on KeyStart boundaries.
func partitionEntriesByChunks(metas []segmentChunkMeta, kvs []kvPair) ([][]kvPair, []kvPair) {
	buckets := make([][]kvPair, len(metas))
	var unmatched []kvPair
	if len(metas) == 0 || len(kvs) == 0 {
		return buckets, unmatched
	}
	idx := 0
	for _, kv := range kvs {
		idx = findChunkIndexForKey(metas, kv.key, idx)
		if idx < 0 {
			unmatched = append(unmatched, kv)
			continue
		}
		buckets[idx] = append(buckets[idx], kv)
	}
	return buckets, unmatched
}

func findChunkIndexForKey(metas []segmentChunkMeta, key []byte, start int) int {
	if len(metas) == 0 {
		return -1
	}
	if len(key) == 0 {
		return 0
	}
	idx := sort.Search(len(metas), func(i int) bool {
		startKey := metas[i].KeyStart
		if len(startKey) == 0 {
			return false
		}
		return compareSegmentIndexKeyStarts(key, startKey) < 0
	})
	if idx == 0 {
		if len(metas[0].KeyStart) == 0 {
			return 0
		}
		if compareSegmentIndexKeyStarts(key, metas[0].KeyStart) < 0 {
			return -1
		}
		return 0
	}
	selected := idx - 1
	if len(metas[selected].KeyStart) == 0 {
		return selected
	}
	if compareSegmentIndexKeyStarts(key, metas[selected].KeyStart) < 0 {
		return -1
	}
	if idx < len(metas) {
		nextStart := metas[idx].KeyStart
		if len(nextStart) > 0 && compareSegmentIndexKeyStarts(key, nextStart) >= 0 {
			return -1
		}
	}
	_ = start
	return selected
}

type chunkFileAllocator struct {
	next int
}

type committedChunkBuffer struct {
	fileName string
	payload  []byte
}

type committedChunkBufferReplacement struct {
	oldFileName string
	newChunks   []committedChunkBuffer
	oldWasRead  bool
}

func newChunkFileAllocator(metas []segmentChunkMeta) *chunkFileAllocator {
	maxIdx := -1
	for _, meta := range metas {
		if idx := parseChunkOrdinal(meta.FileName); idx > maxIdx {
			maxIdx = idx
		}
	}
	return &chunkFileAllocator{next: maxIdx + 1}
}

func (a *chunkFileAllocator) nextName() string {
	name := chunkFileNameForOrdinal(uint32(a.next))
	a.next++
	return name
}

func chunkFileNameForOrdinal(ordinal uint32) string {
	if ordinal < 10000 {
		var b [8]byte
		b[0] = byte('0' + (ordinal/1000)%10)
		b[1] = byte('0' + (ordinal/100)%10)
		b[2] = byte('0' + (ordinal/10)%10)
		b[3] = byte('0' + ordinal%10)
		b[4] = '.'
		b[5] = 'd'
		b[6] = 'a'
		b[7] = 't'
		return string(b[:])
	}
	// Keep behavior compatible with %04d: width is minimum 4, not maximum.
	return strconv.FormatUint(uint64(ordinal), 10) + ".dat"
}

func parseChunkOrdinal(name string) int {
	const suffix = ".dat"
	if len(name) <= len(suffix) {
		return -1
	}
	if name[len(name)-len(suffix):] != suffix {
		return -1
	}
	num := name[:len(name)-len(suffix)]
	if len(num) == 0 {
		return -1
	}
	idx := 0
	for i := 0; i < len(num); i++ {
		c := num[i]
		if c < '0' || c > '9' {
			return -1
		}
		idx = idx*10 + int(c-'0')
	}
	return idx
}

func (db *PrefixDB) mutateSegmentChunk(folderPath string, meta segmentChunkMeta, additions []kvPair, allocator *chunkFileAllocator, blockID uint64) ([]segmentChunkMeta, []committedChunkBuffer, error) {
	if len(additions) == 0 {
		return []segmentChunkMeta{meta}, nil, nil
	}
	chunkPath := filepath.Join(folderPath, meta.FileName)
	info, err := os.Stat(chunkPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			chunkMetas, newChunks, _, rewriteErr := db.rewriteChunkWithDedup(folderPath, meta, additions, allocator, []kvPair{}, nil, blockID)
			if rewriteErr != nil {
				return nil, nil, rewriteErr
			}
			fmt.Printf("prefixdb: recreated missing chunk %s in folder %s during write\n", meta.FileName, folderPath)
			return chunkMetas, newChunks, nil
		}
		return nil, nil, err
	}
	currentSize := info.Size()
	appendBytes := payloadSize(additions)
	if appendBytes == 0 {
		return []segmentChunkMeta{meta}, nil, nil
	}
	if trigger := int64(db.segmentedChunkTriggerSize()); trigger > 0 && currentSize+int64(appendBytes) > trigger {
		chunkMetas, newChunks, _, err := db.rewriteChunkWithDedup(folderPath, meta, additions, allocator, nil, nil, blockID)
		return chunkMetas, newChunks, err
	}
	if err := db.appendChunkFile(chunkPath, additions, currentSize, blockID); err != nil {
		return nil, nil, err
	}
	adjustMetaRange(&meta, additions)
	return []segmentChunkMeta{meta}, nil, nil
}

func (db *PrefixDB) appendChunkFile(path string, additions []kvPair, currentSize int64, blockID uint64) error {
	if len(additions) == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	seg, release, _, err := serializeChunkPayload(additions)
	if err != nil {
		return err
	}
	defer release()
	payload := append([]byte(nil), seg...)
	payload = appendChunkCommitTag(payload, blockID)
	folderPath := filepath.Dir(path)
	fileName := filepath.Base(path)
	var cacheLease *bufferLease
	if db.shouldUseSegmentChunkBufferSlot() {
		if existingLease, ok := db.getCachedSegmentChunkLease(folderPath, fileName); ok {
			existing := existingLease.Bytes()
			if len(existing) == int(currentSize) {
				totalSize := len(existing) + len(payload)
				buf := getDataBuffer(totalSize)
				copy(buf[:len(existing)], existing)
				copy(buf[len(existing):], payload)
				cacheLease = newBufferLease(buf[:totalSize])
			}
			existingLease.Release()
		}
	}
	if _, err := f.WriteAt(payload, currentSize); err != nil {
		if cacheLease != nil {
			cacheLease.Release()
		}
		return err
	}
	db.addDiskWrite(diskIOUsageStorageSeparatedLogs, len(payload))
	if cacheLease != nil {
		if !db.updateExistingChunkBufferLease(folderPath, fileName, cacheLease) {
			db.removeCachedSegmentChunkEntries(folderPath, fileName)
		}
		cacheLease.Release()
	} else {
		db.removeCachedSegmentChunkEntries(folderPath, fileName)
	}
	return nil
}

func adjustMetaRange(meta *segmentChunkMeta, additions []kvPair) {
	if len(additions) == 0 {
		return
	}
	first := additions[0].key
	if len(meta.KeyStart) == 0 || bytes.Compare(first, meta.KeyStart) < 0 {
		meta.KeyStart = cloneBytes(first)
	}
	// Ranges are KeyStart-only.
}

func (db *PrefixDB) rewriteChunkWithDedup(folderPath string, meta segmentChunkMeta, additions []kvPair, allocator *chunkFileAllocator, existing []kvPair, backing *bufferLease, blockID uint64) ([]segmentChunkMeta, []committedChunkBuffer, bool, error) {
	var err error
	if existing == nil {
		existing, backing, err = db.readSegmentChunkFileWithUsageByPathPreferCache(folderPath, meta.FileName, diskIOUsageStorageSeparatedLogs)
		if err != nil {
			return nil, nil, false, err
		}
		// ChunkSize is no longer tracked in meta - get from filesystem if needed
		db.addCommitOldKVReadStats(len(existing), 0)
	}
	if backing != nil {
		defer backing.Release()
	}
	// Chunk files are append-only (see appendChunkFile) so their on-disk kv order is not
	// guaranteed to be sorted. mergeAndDedupPairs assumes sorted inputs; normalize first
	// to avoid dropping keys during GC rewrites.
	if len(existing) > 1 {
		existing = db.maybeNormalizeChunkEntries(existing, &meta)
	}
	merged := mergeAndDedupPairs(existing, additions)
	if len(merged) == 0 {
		return nil, nil, true, nil
	}
	chunks := splitEntriesBySize(merged, db.segmentedChunkTargetSize())
	result := make([]segmentChunkMeta, 0, len(chunks))
	newChunks := make([]committedChunkBuffer, 0, len(chunks))
	if allocator == nil {
		allocator = newChunkFileAllocator([]segmentChunkMeta{meta})
	}
	reuseOldFileName := len(chunks) == 1
	for idx, chunk := range chunks {
		name := meta.FileName
		if !reuseOldFileName || idx > 0 {
			name = allocator.nextName()
		}
		if _, payload, writeErr := db.writeChunkFileWithUsageAndPayload(folderPath, name, chunk, diskIOUsageStorageSeparatedLogs, true, blockID); writeErr != nil {
			return nil, nil, false, writeErr
		} else {
			newChunks = append(newChunks, committedChunkBuffer{fileName: name, payload: payload})
		}
		result = append(result, segmentChunkMeta{
			FileName: name,
			KeyStart: cloneBytes(chunk[0].key),
		})
	}
	return result, newChunks, false, nil
}

func (db *PrefixDB) repairMissingChunkFile(folderID uint32, fileName string) error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	folderPath := db.segmentedFolderPath(folderID)
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
	if err != nil {
		return err
	}
	filtered := make([]segmentChunkMeta, 0, len(metas))
	removed := false
	for _, meta := range metas {
		if meta.FileName == fileName {
			removed = true
			continue
		}
		filtered = append(filtered, meta)
	}
	if !removed {
		return fmt.Errorf("missing chunk %s not referenced in folder %d", fileName, folderID)
	}
	if err := db.writeSegmentIndexLocked(folderPath, filtered, entry); err != nil {
		return err
	}
	db.invalidateSegmentIndexLayoutForPath(folderPath)
	db.refreshSegmentIndexCacheByPathLocked(folderPath, filtered)
	fmt.Printf("prefixdb: repaired missing chunk %s in folder %d\n", fileName, folderID)
	return nil
}

func mergeAndDedupPairs(existing, additions []kvPair) []kvPair {
	merged := make([]kvPair, 0, len(existing)+len(additions))
	i, j := 0, 0
	for i < len(existing) && j < len(additions) {
		cmp := bytes.Compare(existing[i].key, additions[j].key)
		switch {
		case cmp < 0:
			merged = append(merged, existing[i])
			i++
		case cmp > 0:
			merged = append(merged, additions[j])
			j++
		default:
			merged = append(merged, additions[j])
			i++
			j++
		}
	}
	if i < len(existing) {
		merged = append(merged, existing[i:]...)
	}
	if j < len(additions) {
		merged = append(merged, additions[j:]...)
	}
	return merged
}

func splitEntriesBySize(entries []kvPair, limit int) [][]kvPair {
	if len(entries) == 0 {
		return nil
	}
	chunks := make([][]kvPair, 0, len(entries)/64+1)
	start := 0
	var size int
	for i := 0; i < len(entries); i++ {
		entrySize := segmentedChunkEntryHeaderSize + len(entries[i].key) + len(entries[i].val)
		if size+entrySize > limit && i > start {
			chunk := entries[start:i:i]
			chunks = append(chunks, chunk)
			start = i
			size = 0
		}
		size += entrySize
	}
	if start < len(entries) {
		chunk := entries[start:len(entries):len(entries)]
		chunks = append(chunks, chunk)
	}
	return chunks
}

func payloadSize(entries []kvPair) int64 {
	var total int64
	for _, kv := range entries {
		total += int64(segmentedChunkEntryHeaderSize + len(kv.key) + len(kv.val))
	}
	return total
}

func (db *PrefixDB) segmentedChunkTargetSize() int {
	if db != nil && db.storageChunkSize > 0 {
		return db.storageChunkSize
	}
	if db != nil && db.segmentedChunkHardLimit > 0 {
		return db.segmentedChunkHardLimit
	}
	return 16 * 1024
}

func (db *PrefixDB) segmentedChunkTriggerSize() int {
	if db != nil && db.segmentedChunkHardLimit > 0 {
		return db.segmentedChunkHardLimit
	}
	return db.segmentedChunkTargetSize()
}

func sanitizeStorageGCThreshold(threshold float64) float64 {
	if threshold <= 0 {
		return defaultStorageGCThreshold
	}
	return threshold
}

func computeSegmentedChunkHardLimit(storageChunkFileSize int, threshold float64) int {
	if storageChunkFileSize <= 0 {
		return 0
	}
	return int(math.Ceil(float64(storageChunkFileSize) * sanitizeStorageGCThreshold(threshold)))
}

// resolveSegmentIndexLevel2Size returns the L2 index shard byte budget.
// It defaults to storageChunkFileSize so that the segment index page size
// scales together with the chunk file size configured via CLI.
// Falls back to defaultSegmentIndexLevel2Size when storageChunkFileSize <= 0.
func resolveSegmentIndexLevel2Size(storageChunkFileSize int) int {
	if storageChunkFileSize > 0 {
		return storageChunkFileSize
	}
	return defaultSegmentIndexLevel2Size
}

func resolveSegmentIndexMultiLevelThreshold(level2Size int) int {
	resolvedLevel2Size := level2Size
	if resolvedLevel2Size <= 0 {
		resolvedLevel2Size = defaultSegmentIndexLevel2Size
	}
	return resolvedLevel2Size * 2
}

func storageGCQueueCapacity(workers int) int {
	return sanitizePrefixTreeGCWorkerCount(workers) * storageGCQueueMultiplier
}

func (db *PrefixDB) acquireSharedGCWorker() func() {
	if db == nil || db.gcWorkerLimiter == nil {
		return func() {}
	}
	db.gcWorkerLimiter <- struct{}{}
	return func() {
		<-db.gcWorkerLimiter
	}
}

func (db *PrefixDB) writeChunkFile(folderPath, fileName string, entries []kvPair) (int, error) {
	return db.writeChunkFileWithUsage(folderPath, fileName, entries, diskIOUsageStorageSeparatedLogs)
}

func (db *PrefixDB) writeChunkFileWithUsage(folderPath, fileName string, entries []kvPair, usage diskIOUsage) (int, error) {
	chunkSize, _, err := db.writeChunkFileWithUsageAndPayload(folderPath, fileName, entries, usage, false, 0)
	return chunkSize, err
}

func (db *PrefixDB) writeChunkFileWithUsageAndPayload(folderPath, fileName string, entries []kvPair, usage diskIOUsage, capturePayload bool, blockID uint64) (int, []byte, error) {
	seg, release, chunkSize, err := serializeChunkPayload(entries)
	if err != nil {
		return 0, nil, err
	}
	defer release()
	payload := append([]byte(nil), seg...)
	payload = appendChunkCommitTag(payload, blockID)
	fullPath := filepath.Join(folderPath, fileName)
	// Write atomically to avoid readers observing a partially rewritten chunk
	// (GC rewrites truncate and rewrite existing files).
	tmpPath := fullPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, nil, err
	}
	if _, err := f.Write(payload); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return 0, nil, err
	}
	db.addDiskWrite(usage, len(payload))
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return 0, nil, err
	}
	if err := os.Rename(tmpPath, fullPath); err != nil {
		_ = os.Remove(tmpPath)
		return 0, nil, err
	}
	if usage == diskIOUsageStorageGC {
		db.cacheGCChunkSerializedPayload(folderPath, fileName, payload)
	} else {
		db.updateExistingChunkBufferSerializedPayload(folderPath, fileName, payload)
	}
	if capturePayload {
		return len(payload), cloneBytes(payload), nil
	}
	return chunkSize + len(payload) - len(seg), nil, nil
}

func (db *PrefixDB) segmentedFolderPath(id uint32) string {
	return filepath.Join(db.storageDir, fmt.Sprintf("%08d", id))
}

func (db *PrefixDB) segmentedFolderPathForAccount(accountKey []byte) string {
	return filepath.Join(db.storageDir, hex.EncodeToString(accountKey))
}

func (db *PrefixDB) managedAccountKeyForFolderPath(folderPath string) ([]byte, bool) {
	if db == nil {
		return nil, false
	}
	name := filepath.Base(folderPath)
	accountKey, err := hex.DecodeString(name)
	if err != nil {
		return nil, false
	}
	if !db.isAccountStorageFolderManaged(accountKey) {
		return nil, false
	}
	return accountKey, true
}

func isAccountNamedSegmentedStorage(fileID uint32) bool {
	return fileID == segmentedStorageFlag
}

func (db *PrefixDB) markAccountStorageFolder(accountKey []byte) {
	if db == nil || db.accountFolderSet == nil {
		return
	}
	db.accountFolderSet.add(accountKey)
}

func (db *PrefixDB) isAccountStorageFolderManaged(accountKey []byte) bool {
	if db == nil || db.accountFolderSet == nil {
		return false
	}
	return db.accountFolderSet.maybeContains(accountKey)
}

func (db *PrefixDB) clearAccountStorageFolder(accountKey []byte) {
	if db == nil || db.accountFolderSet == nil {
		return
	}
	db.accountFolderSet.remove(accountKey)
}

func shouldFallbackMissingFolderRead(err error) bool {
	return err != nil && errors.Is(err, os.ErrNotExist)
}

func (db *PrefixDB) primeAccountFolderSetFromStorageDir() error {
	if db == nil || db.accountFolderSet == nil {
		return nil
	}
	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		accountKey, decodeErr := hex.DecodeString(name)
		if decodeErr != nil || len(accountKey) == 0 {
			continue
		}
		db.markAccountStorageFolder(accountKey)
	}
	return nil
}

func shouldSkipAccountEntryPointerUpdate(existingFileID uint32, fileID uint32, off uint64, size uint64) bool {
	return isAccountNamedSegmentedStorage(existingFileID) && isAccountNamedSegmentedStorage(fileID) && off == 0 && size == 0
}

func (db *PrefixDB) lockSegmentIndexFolder(folderPath string) func() {
	_, unlock := db.lockSegmentIndexFolderReadEntry(folderPath)
	return unlock
}

func (db *PrefixDB) lockSegmentIndexFolderWrite(folderPath string) func() {
	_, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	return unlock
}

func (db *PrefixDB) readSegmentIndexNoCacheByPathLocked(folderPath string) ([]segmentChunkMeta, error) {
	metas, _, err := db.readSegmentIndexLockedInternalByPath(folderPath, false)
	return metas, err
}

func (db *PrefixDB) lockSegmentIndexFolderReadEntry(folderPath string) (*segmentIndexFolderLock, func()) {
	db.segmentIndexFolderLocksMu.Lock()
	if db.segmentIndexFolderLocks == nil {
		db.segmentIndexFolderLocks = make(map[string]*segmentIndexFolderLock)
	}
	entry := db.segmentIndexFolderLocks[folderPath]
	if entry == nil {
		entry = &segmentIndexFolderLock{}
		db.segmentIndexFolderLocks[folderPath] = entry
	}
	entry.refs++
	db.segmentIndexFolderLocksMu.Unlock()

	entry.mu.RLock()
	return entry, func() {
		entry.mu.RUnlock()
		db.segmentIndexFolderLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(db.segmentIndexFolderLocks, folderPath)
		}
		db.segmentIndexFolderLocksMu.Unlock()
	}
}

func (db *PrefixDB) lockSegmentIndexFolderEntry(folderPath string) (*segmentIndexFolderLock, func()) {
	db.segmentIndexFolderLocksMu.Lock()
	if db.segmentIndexFolderLocks == nil {
		db.segmentIndexFolderLocks = make(map[string]*segmentIndexFolderLock)
	}
	entry := db.segmentIndexFolderLocks[folderPath]
	if entry == nil {
		entry = &segmentIndexFolderLock{}
		db.segmentIndexFolderLocks[folderPath] = entry
	}
	entry.refs++
	db.segmentIndexFolderLocksMu.Unlock()

	entry.mu.Lock()
	return entry, func() {
		entry.mu.Unlock()
		db.segmentIndexFolderLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(db.segmentIndexFolderLocks, folderPath)
		}
		db.segmentIndexFolderLocksMu.Unlock()
	}
}

func (db *PrefixDB) segmentIndexGenerationLocked(folderPath string) uint64 {
	entry, unlock := db.lockSegmentIndexFolderReadEntry(folderPath)
	gen := atomic.LoadUint64(&entry.gen)
	unlock()
	return gen
}

func (db *PrefixDB) bumpSegmentIndexGenerationLocked(entry *segmentIndexFolderLock) {
	if entry == nil {
		return
	}
	atomic.AddUint64(&entry.gen, 1)
}

func (db *PrefixDB) readSegmentIndexWithGenByPath(folderPath string, useLRU bool) ([]segmentChunkMeta, uint64, error) {
	entry, unlock := db.lockSegmentIndexFolderReadEntry(folderPath)
	defer unlock()
	gen := atomic.LoadUint64(&entry.gen)
	metas, _, err := db.readSegmentIndexLockedInternalByPath(folderPath, useLRU)
	return metas, gen, err
}

func (db *PrefixDB) readSegmentIndexWithGen(folderID uint32, useLRU bool) ([]segmentChunkMeta, uint64, error) {
	return db.readSegmentIndexWithGenByPath(db.segmentedFolderPath(folderID), useLRU)
}

func level2IndexFilePath(folderPath string, metaID uint32) string {
	return filepath.Join(folderPath, fmt.Sprintf(segmentIndexLevel2Pattern, metaID))
}

func segmentChunkMetaCanUseCompactEncoding(meta segmentChunkMeta) bool {
	if len(meta.KeyStart) > segmentIndexKeyStartMaxBytes {
		return false
	}
	return parseChunkOrdinal(meta.FileName) >= 0
}

func canUseCompactSegmentEncoding(metas []segmentChunkMeta) bool {
	for _, meta := range metas {
		if !segmentChunkMetaCanUseCompactEncoding(meta) {
			return false
		}
	}
	return true
}

func estimateSegmentEntrySize(meta segmentChunkMeta) int {
	if segmentChunkMetaCanUseCompactEncoding(meta) {
		return segmentIndexFlatEntryBytes
	}
	return 2 + len(meta.FileName) + 2 + len(meta.KeyStart)
}

func estimateSegmentIndexSize(metas []segmentChunkMeta) int {
	total := 4
	if canUseCompactSegmentEncoding(metas) {
		total = 12
	}
	for _, meta := range metas {
		total += estimateSegmentEntrySize(meta)
	}
	return total
}

func encodeSegmentChunkMetas(metas []segmentChunkMeta) ([]byte, error) {
	buf := make([]byte, 0, estimateSegmentIndexSize(metas))
	var tmp32 [4]byte
	if !canUseCompactSegmentEncoding(metas) {
		return nil, fmt.Errorf("segment index requires compact encoding compatible metas")
	}
	writeUint32BE(tmp32[:], segmentIndexFlatMagic)
	buf = append(buf, tmp32[:]...)
	var tmp16 [2]byte
	writeUint16BE(tmp16[:], segmentIndexFlatVersion)
	buf = append(buf, tmp16[:]...)
	buf = append(buf, 0, 0)
	writeUint32BE(tmp32[:], uint32(len(metas)))
	buf = append(buf, tmp32[:]...)
	for _, meta := range metas {
		ordinal := parseChunkOrdinal(meta.FileName)
		writeUint32BE(tmp32[:], uint32(ordinal))
		buf = append(buf, tmp32[:]...)
		var err error
		if buf, err = appendFixedSegmentIndexKeyStart(buf, meta.KeyStart); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func writeFileIfChanged(db *PrefixDB, path string, data []byte) error {
	fi, err := os.Stat(path)
	if err == nil {
		// Fast path: if sizes differ, content differs.
		if fi.Size() == int64(len(data)) {
			same, cmpErr := fileContentEqualsBytes(db, path, data)
			if cmpErr == nil && same {
				return nil
			}
		}
	} else if !os.IsNotExist(err) {
		// Preserve prior behavior: on read/stat errors, fall back to writing.
	}
	return writeFileAtomic(db, path, data)
}

func (db *PrefixDB) encodeSegmentIndexFileData(data []byte) ([]byte, error) {
	if db == nil || !db.segmentIndexCompression || len(data) <= segmentIndexCompressionMinSize {
		return data, nil
	}
	return encodeCompressedMetadataBlock(data)
}

func (db *PrefixDB) decodeSegmentIndexFileData(path string, data []byte) ([]byte, error) {
	raw, _, err := maybeDecodeCompressedMetadataBlock(data)
	if err != nil {
		return nil, fmt.Errorf("decode compressed segment index %s failed: %w", path, err)
	}
	return raw, nil
}

func (db *PrefixDB) readSegmentIndexFile(path string) ([]byte, error) {
	return db.readSegmentIndexFileWithTracker(path, nil)
}

func (db *PrefixDB) readSegmentIndexFileWithTracker(path string, tracker *cacheMissCostTracker) ([]byte, error) {
	data, err := db.readFileWithStatsTracked(path, diskIOUsageStorageSegmentIndex, tracker)
	if err != nil {
		return nil, err
	}
	return db.decodeSegmentIndexFileData(path, data)
}

func (db *PrefixDB) writeSegmentIndexFileIfChanged(path string, data []byte) error {
	encoded, err := db.encodeSegmentIndexFileData(data)
	if err != nil {
		return err
	}
	return writeFileIfChanged(db, path, encoded)
}

func (db *PrefixDB) writeSegmentIndexFileAtomic(path string, data []byte) error {
	encoded, err := db.encodeSegmentIndexFileData(data)
	if err != nil {
		return err
	}
	return writeFileAtomic(db, path, encoded)
}

func fileContentEqualsBytes(db *PrefixDB, path string, data []byte) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// Compare in fixed-size chunks to avoid allocating a full copy of the file.
	var buf [32 * 1024]byte
	offset := 0
	for offset < len(data) {
		need := len(data) - offset
		if need > len(buf) {
			need = len(buf)
		}
		if _, err := io.ReadFull(f, buf[:need]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return false, nil
			}
			return false, err
		}
		if db != nil {
			db.addDiskRead(diskIOUsageStorageSegmentIndex, need)
		}
		if !bytes.Equal(buf[:need], data[offset:offset+need]) {
			return false, nil
		}
		offset += need
	}

	// Ensure the file doesn't contain extra bytes (handles races between Stat/Open).
	if n, err := f.Read(buf[:1]); n > 0 {
		if db != nil {
			db.addDiskRead(diskIOUsageStorageSegmentIndex, n)
		}
		return false, nil
	} else if err == io.EOF {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func writeFileAtomic(db *PrefixDB, path string, data []byte) error {
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	if db != nil {
		db.addDiskWrite(diskIOUsageStorageSegmentIndex, len(data))
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if db != nil && db.fileHandleCache != nil {
		db.fileHandleCache.InvalidatePath(path)
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func segmentChunkMetaEqual(a, b segmentChunkMeta) bool {
	if a.FileName != b.FileName {
		return false
	}
	return bytes.Equal(a.KeyStart, b.KeyStart)
}

func compareSegmentIndexKeyStarts(a, b []byte) int {
	return bytes.Compare(a, b)
}

var zeroSegmentIndexKeyPadding [segmentIndexKeyStartMaxBytes]byte

func appendFixedSegmentIndexKeyStart(buf []byte, key []byte) ([]byte, error) {
	if len(key) > segmentIndexKeyStartMaxBytes {
		return nil, fmt.Errorf("segment key start too large: %d", len(key))
	}
	buf = append(buf, byte(len(key)))
	if len(key) > 0 {
		buf = append(buf, key...)
	}
	if pad := segmentIndexKeyStartMaxBytes - len(key); pad > 0 {
		buf = append(buf, zeroSegmentIndexKeyPadding[:pad]...)
	}
	return buf, nil
}

func decodeFixedSegmentIndexKeyStart(data []byte) ([]byte, int, error) {
	if len(data) < segmentIndexFixedKeyFieldBytes {
		return nil, 0, io.ErrUnexpectedEOF
	}
	keyLen := int(data[0])
	if keyLen > segmentIndexKeyStartMaxBytes {
		return nil, 0, fmt.Errorf("invalid fixed segment key length %d", keyLen)
	}
	if keyLen == 0 {
		return nil, segmentIndexFixedKeyFieldBytes, nil
	}
	return data[1 : 1+keyLen], segmentIndexFixedKeyFieldBytes, nil
}

func compareSearchKeyToEncodedFixedSegmentIndexKey(search []byte, encoded []byte) (int, error) {
	if len(encoded) < segmentIndexFixedKeyFieldBytes {
		return 0, io.ErrUnexpectedEOF
	}
	keyLen := int(encoded[0])
	if keyLen > segmentIndexKeyStartMaxBytes {
		return 0, fmt.Errorf("invalid fixed segment key length %d", keyLen)
	}
	keyData := encoded[1 : 1+keyLen]
	return compareSegmentIndexKeyStarts(search, keyData), nil
}

func flatSegmentIndexCount(data []byte) (int, uint16, error) {
	if len(data) < 12 {
		return 0, 0, fmt.Errorf("corrupted compact segment index header")
	}
	if binary.BigEndian.Uint32(data[:4]) != segmentIndexFlatMagic {
		return 0, 0, fmt.Errorf("unsupported segment index format")
	}
	version := binary.BigEndian.Uint16(data[4:6])
	if version != segmentIndexFlatVersion {
		return 0, 0, fmt.Errorf("unsupported flat index version %d", version)
	}
	count := int(binary.BigEndian.Uint32(data[8:12]))
	return count, version, nil
}

func flatSegmentIndexEntryOffset(version uint16, idx int) (int, error) {
	if idx < 0 {
		return 0, fmt.Errorf("invalid flat segment index entry index %d", idx)
	}
	if version != segmentIndexFlatVersion {
		return 0, fmt.Errorf("unsupported flat index version %d", version)
	}
	return 12 + idx*segmentIndexFlatEntryBytes, nil
}

func flatSegmentIndexMetaAt(data []byte, version uint16, idx int) (segmentChunkMeta, error) {
	offset, err := flatSegmentIndexEntryOffset(version, idx)
	if err != nil {
		return segmentChunkMeta{}, err
	}
	if offset+segmentIndexFlatEntryBytes > len(data) {
		return segmentChunkMeta{}, io.ErrUnexpectedEOF
	}
	ordinal := readUint32BE(data[offset : offset+4])
	keyStart, _, err := decodeFixedSegmentIndexKeyStart(data[offset+4 : offset+4+segmentIndexFixedKeyFieldBytes])
	if err != nil {
		return segmentChunkMeta{}, err
	}
	return segmentChunkMeta{FileName: chunkFileNameForOrdinal(ordinal), KeyStart: keyStart}, nil
}

func selectFixedFlatSegmentIndexMeta(data []byte, key []byte) (*segmentChunkMeta, error) {
	count, version, err := flatSegmentIndexCount(data)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	idx := sort.Search(count, func(i int) bool {
		offset := 12 + i*segmentIndexFlatEntryBytes + 4
		cmp, cmpErr := compareSearchKeyToEncodedFixedSegmentIndexKey(key, data[offset:offset+segmentIndexFixedKeyFieldBytes])
		if cmpErr != nil {
			return true
		}
		return cmp < 0
	})
	if idx == 0 {
		cmp, cmpErr := compareSearchKeyToEncodedFixedSegmentIndexKey(key, data[16:16+segmentIndexFixedKeyFieldBytes])
		if cmpErr != nil {
			return nil, cmpErr
		}
		if cmp < 0 {
			return nil, nil
		}
		meta, err := flatSegmentIndexMetaAt(data, version, 0)
		if err != nil {
			return nil, err
		}
		return &meta, nil
	}
	selectedIdx := idx - 1
	selected, err := flatSegmentIndexMetaAt(data, version, selectedIdx)
	if err != nil {
		return nil, err
	}
	if compareSegmentIndexKeyStarts(key, selected.KeyStart) < 0 {
		return nil, nil
	}
	if idx < count {
		nextOffset := 12 + idx*segmentIndexFlatEntryBytes + 4
		cmp, cmpErr := compareSearchKeyToEncodedFixedSegmentIndexKey(key, data[nextOffset:nextOffset+segmentIndexFixedKeyFieldBytes])
		if cmpErr != nil {
			return nil, cmpErr
		}
		if cmp >= 0 {
			return nil, nil
		}
	}
	return &selected, nil
}

func lessSegmentChunkMeta(a, b segmentChunkMeta) bool {
	cmp := compareSegmentIndexKeyStarts(a.KeyStart, b.KeyStart)
	if cmp != 0 {
		return cmp < 0
	}
	return a.FileName < b.FileName
}

func isSegmentChunkMetasOrdered(metas []segmentChunkMeta) bool {
	for i := 1; i < len(metas); i++ {
		if lessSegmentChunkMeta(metas[i], metas[i-1]) {
			return false
		}
	}
	return true
}

func normalizeSegmentChunkMetasOrder(metas []segmentChunkMeta, changed *bool) []segmentChunkMeta {
	if len(metas) <= 1 {
		if changed != nil {
			*changed = false
		}
		return metas
	}
	if isSegmentChunkMetasOrdered(metas) {
		if changed != nil {
			*changed = false
		}
		return metas
	}
	sorted := cloneSegmentChunkMetas(metas)
	sort.Slice(sorted, func(i, j int) bool {
		return lessSegmentChunkMeta(sorted[i], sorted[j])
	})
	if changed != nil {
		*changed = true
	}
	return sorted
}

func segmentChunkMetasEqual(a, b []segmentChunkMeta) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !segmentChunkMetaEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func removeLevel2IndexFilesByIDs(folderPath string, ids []uint32) error {
	for _, id := range ids {
		full := level2IndexFilePath(folderPath, id)
		if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func removeStaleLevel2IndexFiles(folderPath string, oldEntries []segmentIndexL1Entry, keep map[uint32]struct{}) error {
	if len(oldEntries) == 0 {
		return nil
	}
	toDelete := make([]uint32, 0, len(oldEntries))
	for _, entry := range oldEntries {
		if keep != nil {
			if _, ok := keep[entry.MetaID]; ok {
				continue
			}
		}
		toDelete = append(toDelete, entry.MetaID)
	}
	return removeLevel2IndexFilesByIDs(folderPath, toDelete)
}

func removeStaleSegmentChunkFiles(folderPath string, keep map[string]struct{}) error {
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if parseChunkOrdinal(name) < 0 {
			continue
		}
		if keep != nil {
			if _, ok := keep[name]; ok {
				continue
			}
		}
		full := filepath.Join(folderPath, name)
		if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func removeLevel2IndexFiles(folderPath string, keep map[uint32]struct{}) error {
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var metaID uint32
		if _, err := fmt.Sscanf(entry.Name(), "index.meta.l2.%08d", &metaID); err != nil {
			continue
		}
		if keep != nil {
			if _, ok := keep[metaID]; ok {
				continue
			}
		}
		full := filepath.Join(folderPath, entry.Name())
		if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (db *PrefixDB) splitSegmentMetas(metas []segmentChunkMeta) [][]segmentChunkMeta {
	if len(metas) == 0 {
		return nil
	}
	groups := make([][]segmentChunkMeta, 0, len(metas)/16+1)
	groupStart := 0
	groupSize := 4
	level2Size := db.segmentIndexLevel2Size
	for i, meta := range metas {
		entrySize := estimateSegmentEntrySize(meta)
		if groupSize+entrySize > level2Size && i > groupStart {
			groups = append(groups, metas[groupStart:i])
			groupStart = i
			groupSize = 4
		}
		groupSize += entrySize
		if groupSize >= level2Size {
			groups = append(groups, metas[groupStart:i+1])
			groupStart = i + 1
			groupSize = 4
		}
	}
	if groupStart < len(metas) {
		groups = append(groups, metas[groupStart:])
	}
	return groups
}

func selectSegmentL1Entry(entries []segmentIndexL1Entry, key []byte) *segmentIndexL1Entry {
	if len(entries) == 0 {
		return nil
	}
	if len(key) == 0 {
		return &entries[0]
	}
	idx := upperBoundSegmentIndexL1Entries(entries, key)
	if idx == 0 {
		return nil
	}
	return &entries[idx-1]
}

func decodeSegmentIndexBuffer(data []byte, metas *[]segmentChunkMeta, arena *[]byte, appendExisting bool, chunkDir string) error {
	count, version, err := flatSegmentIndexCount(data)
	if err != nil {
		return err
	}
	cursor := 12
	if count == 0 {
		if !appendExisting {
			*metas = (*metas)[:0]
			*arena = (*arena)[:0]
		}
		return nil
	}
	if !appendExisting {
		if cap(*metas) < count {
			*metas = make([]segmentChunkMeta, 0, count)
		} else {
			*metas = (*metas)[:0]
		}
		*arena = (*arena)[:0]
	}
	needed := len(*metas) + count
	if cap(*metas) < needed {
		newCap := needed
		if newCap < 2*cap(*metas) {
			newCap = 2 * cap(*metas)
		}
		buf := make([]segmentChunkMeta, len(*metas), newCap)
		copy(buf, *metas)
		*metas = buf
	}
	if version != segmentIndexFlatVersion {
		return fmt.Errorf("unsupported flat index version %d", version)
	}
	for i := 0; i < count; i++ {
		if cursor+segmentIndexFlatEntryBytes > len(data) {
			return io.ErrUnexpectedEOF
		}
		fileName := chunkFileNameForOrdinal(readUint32BE(data[cursor : cursor+4]))
		cursor += 4
		start, n, err := decodeFixedSegmentIndexKeyStart(data[cursor : cursor+segmentIndexFixedKeyFieldBytes])
		if err != nil {
			return err
		}
		cursor += n
		meta := segmentChunkMeta{FileName: fileName, KeyStart: start}
		_ = chunkDir
		*metas = append(*metas, meta)
	}
	return nil
}

func (db *PrefixDB) loadSegmentIndexLayoutWithSource(folderPath string) (segmentIndexLayout, segmentIndexLookupSource, error) {
	return db.loadSegmentIndexLayoutWithSourceAndTracker(folderPath, nil)
}

func (db *PrefixDB) loadSegmentIndexLayoutWithSourceAndTracker(folderPath string, tracker *cacheMissCostTracker) (segmentIndexLayout, segmentIndexLookupSource, error) {
	if db.storageIndexCache != nil {
		if layout, ok := db.storageIndexCache.GetLayoutByPath(folderPath); ok {
			if layout.mode == indexLayoutMultiLevel {
				return layout, segmentIndexLookupSourceL1Cache, nil
			}
			return layout, segmentIndexLookupSourceL2Cache, nil
		}
	}

	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	data, err := db.readSegmentIndexFileWithTracker(indexPath, tracker)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return segmentIndexLayout{mode: indexLayoutFlat, nextMetaID: 1}, segmentIndexLookupSourceNoCache, nil
		}
		return segmentIndexLayout{}, segmentIndexLookupSourceNoCache, err
	}
	if len(data) < 4 {
		return segmentIndexLayout{}, segmentIndexLookupSourceNoCache, fmt.Errorf("invalid segment index: %s", indexPath)
	}
	var layout segmentIndexLayout
	if binary.BigEndian.Uint32(data[:4]) != segmentIndexMultiLevelMagic {
		layout = segmentIndexLayout{mode: indexLayoutFlat, nextMetaID: 1, flatData: data}
	} else {
		layout, err = parseMultiLevelLayout(data)
		if err != nil {
			return segmentIndexLayout{}, segmentIndexLookupSourceNoCache, err
		}
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.AddLayoutByPath(folderPath, layout)
	}
	return layout, segmentIndexLookupSourceNoCache, nil
}

func (db *PrefixDB) loadSegmentIndexLayout(folderPath string) (segmentIndexLayout, error) {
	layout, _, err := db.loadSegmentIndexLayoutWithSourceAndTracker(folderPath, nil)
	return layout, err
}

func parseMultiLevelLayout(data []byte) (segmentIndexLayout, error) {
	if len(data) < 16 {
		return segmentIndexLayout{}, fmt.Errorf("corrupted multi-level index header")
	}
	layout := segmentIndexLayout{mode: indexLayoutMultiLevel}
	cursor := 4
	version := binary.BigEndian.Uint16(data[cursor : cursor+2])
	cursor += 2
	if version != segmentIndexFormatVersion {
		return segmentIndexLayout{}, fmt.Errorf("unsupported index meta version %d", version)
	}
	cursor += 2 // reserved
	layout.nextMetaID = readUint32BE(data[cursor : cursor+4])
	cursor += 4
	count := int(readUint32BE(data[cursor : cursor+4]))
	cursor += 4
	layout.entries = make([]segmentIndexL1Entry, 0, count)
	for i := 0; i < count; i++ {
		if cursor+8 > len(data) {
			return segmentIndexLayout{}, io.ErrUnexpectedEOF
		}
		metaID := readUint32BE(data[cursor : cursor+4])
		chunkCount := readUint32BE(data[cursor+4 : cursor+8])
		cursor += 8
		if cursor+segmentIndexFixedKeyFieldBytes > len(data) {
			return segmentIndexLayout{}, io.ErrUnexpectedEOF
		}
		start, n, err := decodeFixedSegmentIndexKeyStart(data[cursor : cursor+segmentIndexFixedKeyFieldBytes])
		if err != nil {
			return segmentIndexLayout{}, err
		}
		cursor += n
		layout.entries = append(layout.entries, segmentIndexL1Entry{
			MetaID:     metaID,
			KeyStart:   start,
			ChunkCount: chunkCount,
		})
	}
	if layout.nextMetaID == 0 {
		layout.nextMetaID = uint32(len(layout.entries)) + 1
	}
	// Read-path hardening: keep top-level entries ordered even if on-disk
	// layout was produced by an older/buggy writer. Key lookup relies on
	// binary search and requires monotonic KeyStart order.
	layout.entries = normalizeSegmentIndexL1EntriesOrder(layout.entries, nil)
	return layout, nil
}

func encodeTopLevelIndex(layout segmentIndexLayout) ([]byte, error) {
	if layout.mode != indexLayoutMultiLevel {
		return nil, fmt.Errorf("invalid layout mode")
	}
	buf := make([]byte, 0, 32+len(layout.entries)*48)
	var tmp32 [4]byte
	writeUint32BE(tmp32[:], segmentIndexMultiLevelMagic)
	buf = append(buf, tmp32[:]...)
	var tmp16 [2]byte
	writeUint16BE(tmp16[:], segmentIndexFormatVersion)
	buf = append(buf, tmp16[:]...)
	buf = append(buf, 0, 0)
	writeUint32BE(tmp32[:], layout.nextMetaID)
	buf = append(buf, tmp32[:]...)
	writeUint32BE(tmp32[:], uint32(len(layout.entries)))
	buf = append(buf, tmp32[:]...)
	for _, entry := range layout.entries {
		writeUint32BE(tmp32[:], entry.MetaID)
		buf = append(buf, tmp32[:]...)
		writeUint32BE(tmp32[:], entry.ChunkCount)
		buf = append(buf, tmp32[:]...)
		var err error
		if buf, err = appendFixedSegmentIndexKeyStart(buf, entry.KeyStart); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func layoutEntriesEqual(a, b []segmentIndexL1Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].MetaID != b[i].MetaID || a[i].ChunkCount != b[i].ChunkCount {
			return false
		}
		if !bytes.Equal(a[i].KeyStart, b[i].KeyStart) {
			return false
		}
	}
	return true
}

func lessSegmentIndexL1Entry(a, b segmentIndexL1Entry) bool {
	cmp := compareSegmentIndexKeyStarts(a.KeyStart, b.KeyStart)
	if cmp != 0 {
		return cmp < 0
	}
	if a.MetaID != b.MetaID {
		return a.MetaID < b.MetaID
	}
	return a.ChunkCount < b.ChunkCount
}

func isSegmentIndexL1EntriesOrdered(entries []segmentIndexL1Entry) bool {
	for i := 1; i < len(entries); i++ {
		if lessSegmentIndexL1Entry(entries[i], entries[i-1]) {
			return false
		}
	}
	return true
}

func normalizeSegmentIndexL1EntriesOrder(entries []segmentIndexL1Entry, changed *bool) []segmentIndexL1Entry {
	if len(entries) <= 1 {
		if changed != nil {
			*changed = false
		}
		return entries
	}
	if isSegmentIndexL1EntriesOrdered(entries) {
		if changed != nil {
			*changed = false
		}
		return entries
	}
	sorted := make([]segmentIndexL1Entry, len(entries))
	for i := range entries {
		sorted[i] = segmentIndexL1Entry{
			MetaID:     entries[i].MetaID,
			ChunkCount: entries[i].ChunkCount,
			KeyStart:   cloneBytes(entries[i].KeyStart),
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return lessSegmentIndexL1Entry(sorted[i], sorted[j])
	})
	if changed != nil {
		*changed = true
	}
	return sorted
}

func (db *PrefixDB) writeSegmentIndex(folderPath string, metas []segmentChunkMeta) error {
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	return db.writeSegmentIndexLocked(folderPath, metas, entry)
}

func (db *PrefixDB) writeSegmentIndexLocked(folderPath string, metas []segmentChunkMeta, entry *segmentIndexFolderLock) error {
	// Writers must observe the on-disk top-level layout so external rewrites of
	// index.meta cannot leave us reusing stale cached ordering information.
	db.invalidateSegmentIndexLayoutForPath(folderPath)
	metas = normalizeSegmentChunkMetasOrder(metas, nil)
	// Capture the previous layout so we can remove stale L2 files without scanning
	// the whole folder (which may contain many *.dat files).
	prevLayout, _ := db.loadSegmentIndexLayout(folderPath)
	var prevEntries []segmentIndexL1Entry
	if prevLayout.mode == indexLayoutMultiLevel {
		prevEntries = prevLayout.entries
	}
	if len(metas) == 0 {
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := os.Remove(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		db.invalidateSegmentIndexLayoutForPath(folderPath)
		db.bumpSegmentIndexGenerationLocked(entry)
		db.refreshSegmentIndexCacheByPathLocked(folderPath, nil)
		if len(prevEntries) > 0 {
			return removeStaleLevel2IndexFiles(folderPath, prevEntries, nil)
		}
		return removeLevel2IndexFiles(folderPath, nil)
	}
	serializedSize := estimateSegmentIndexSize(metas)
	if serializedSize <= db.segmentIndexMultiLevelThreshold {
		buf, err := encodeSegmentChunkMetas(metas)
		if err != nil {
			return err
		}
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := db.writeSegmentIndexFileIfChanged(indexPath, buf); err != nil {
			return err
		}
		if db.storageIndexCache != nil {
			db.storageIndexCache.AddLayoutByPath(folderPath, segmentIndexLayout{mode: indexLayoutFlat, nextMetaID: 1, flatData: cloneBytes(buf)})
		}
		db.bumpSegmentIndexGenerationLocked(entry)
		db.refreshSegmentIndexCacheByPathLocked(folderPath, metas)
		if len(prevEntries) > 0 {
			return removeStaleLevel2IndexFiles(folderPath, prevEntries, nil)
		}
		return removeLevel2IndexFiles(folderPath, nil)
	}
	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return err
	}
	oldEntries := layout.entries
	if layout.mode != indexLayoutMultiLevel {
		oldEntries = nil
		layout = segmentIndexLayout{mode: indexLayoutMultiLevel, nextMetaID: 1}
	}
	var reuseLayoutGrouping bool
	var groupOffsets []int
	// Try to reuse existing L2 grouping if it matches the current meta count.
	var groups [][]segmentChunkMeta
	if layout.mode == indexLayoutMultiLevel && len(layout.entries) > 0 {
		sum := 0
		for _, entry := range layout.entries {
			sum += int(entry.ChunkCount)
		}
		if sum == len(metas) {
			groups = make([][]segmentChunkMeta, 0, len(layout.entries))
			groupOffsets = make([]int, 0, len(layout.entries))
			off := 0
			for _, entry := range layout.entries {
				cnt := int(entry.ChunkCount)
				groupOffsets = append(groupOffsets, off)
				groups = append(groups, metas[off:off+cnt])
				off += cnt
			}
			reuseLayoutGrouping = true
		}
	}
	if len(groups) == 0 {
		groups = db.splitSegmentMetas(metas)
		if len(groups) == 0 {
			groups = [][]segmentChunkMeta{metas}
		}
		reuseLayoutGrouping = false
		groupOffsets = nil
	}
	nextID := layout.nextMetaID
	if nextID == 0 {
		nextID = 1
	}
	idAssignments := make([]uint32, len(groups))
	for i := range groups {
		if i < len(layout.entries) {
			idAssignments[i] = layout.entries[i].MetaID
		}
		if idAssignments[i] == 0 {
			idAssignments[i] = nextID
			nextID++
		}
	}
	keep := make(map[uint32]struct{}, len(idAssignments))
	for _, id := range idAssignments {
		keep[id] = struct{}{}
	}
	newEntries := make([]segmentIndexL1Entry, 0, len(groups))
	for idx, group := range groups {
		path := level2IndexFilePath(folderPath, idAssignments[idx])
		_ = reuseLayoutGrouping
		_ = groupOffsets
		buf, err := encodeSegmentChunkMetas(group)
		if err != nil {
			return err
		}
		// Incremental write: only update this L2 index file when payload changed.
		if err := db.writeSegmentIndexFileIfChanged(path, buf); err != nil {
			return err
		}
		entry := segmentIndexL1Entry{
			MetaID:     idAssignments[idx],
			ChunkCount: uint32(len(group)),
		}
		entry.KeyStart = cloneBytes(group[0].KeyStart)
		newEntries = append(newEntries, entry)
	}
	newLayout := segmentIndexLayout{
		mode:       indexLayoutMultiLevel,
		entries:    newEntries,
		nextMetaID: nextID,
	}
	topOrderChanged := false
	newLayout.entries = normalizeSegmentIndexL1EntriesOrder(newLayout.entries, &topOrderChanged)
	needTopUpdate := layout.mode != indexLayoutMultiLevel || !layoutEntriesEqual(layout.entries, newLayout.entries) || layout.nextMetaID != newLayout.nextMetaID || topOrderChanged
	if needTopUpdate {
		buf, err := encodeTopLevelIndex(newLayout)
		if err != nil {
			return err
		}
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := db.writeSegmentIndexFileIfChanged(indexPath, buf); err != nil {
			return err
		}
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.AddLayoutByPath(folderPath, newLayout)
	}
	// Even if the top-level layout didn't change, L2 files may have been rewritten.
	db.bumpSegmentIndexGenerationLocked(entry)
	db.refreshSegmentIndexCacheByPathLocked(folderPath, metas)
	// Remove only those L2 files that were previously referenced but are no longer
	// needed. This avoids scanning the whole folder (which can be huge).
	if len(oldEntries) > 0 {
		if err := removeStaleLevel2IndexFiles(folderPath, oldEntries, keep); err != nil {
			return err
		}
	}
	return nil
}

func buildLayoutGroupsFromMetas(layout segmentIndexLayout, metas []segmentChunkMeta) ([][]segmentChunkMeta, bool) {
	if layout.mode != indexLayoutMultiLevel || len(layout.entries) == 0 {
		return nil, false
	}
	sum := 0
	for _, entry := range layout.entries {
		sum += int(entry.ChunkCount)
	}
	if sum != len(metas) {
		return nil, false
	}
	groups := make([][]segmentChunkMeta, 0, len(layout.entries))
	off := 0
	for _, entry := range layout.entries {
		cnt := int(entry.ChunkCount)
		groups = append(groups, metas[off:off+cnt])
		off += cnt
	}
	return groups, true
}

func applyGroupReplacement(group []segmentChunkMeta, oldFileName string, replacement []segmentChunkMeta) ([]segmentChunkMeta, bool) {
	idx := -1
	for i := range group {
		if group[i].FileName == oldFileName {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, false
	}
	updated := make([]segmentChunkMeta, 0, len(group)-1+len(replacement))
	updated = append(updated, group[:idx]...)
	if len(replacement) > 0 {
		updated = append(updated, replacement...)
	}
	if idx+1 < len(group) {
		updated = append(updated, group[idx+1:]...)
	}
	return updated, true
}

func buildUpdatedSegmentChunkMetas(metas []segmentChunkMeta, replacements map[string][]segmentChunkMeta) ([]segmentChunkMeta, bool) {
	if len(replacements) == 0 {
		return cloneSegmentChunkMetas(metas), false
	}
	updated := make([]segmentChunkMeta, 0, len(metas))
	changed := false
	for i := range metas {
		if repl, ok := replacements[metas[i].FileName]; ok {
			changed = true
			if len(repl) > 0 {
				updated = append(updated, cloneSegmentChunkMetas(repl)...)
			}
			continue
		}
		updated = append(updated, metas[i])
	}
	return updated, changed
}

func (db *PrefixDB) writeSegmentIndexIncrementalGC(folderPath string, latest []segmentChunkMeta, replacements map[string][]segmentChunkMeta) (bool, error) {
	if len(replacements) == 0 {
		return true, nil
	}
	entry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()
	return db.writeSegmentIndexIncrementalGCLocked(folderPath, latest, replacements, entry)
}

func (db *PrefixDB) writeSegmentIndexIncrementalGCLocked(folderPath string, latest []segmentChunkMeta, replacements map[string][]segmentChunkMeta, entry *segmentIndexFolderLock) (bool, error) {
	if len(replacements) == 0 {
		return true, nil
	}

	layout, err := db.loadSegmentIndexLayout(folderPath)
	if err != nil {
		return false, err
	}
	current, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
	if err != nil {
		return false, err
	}
	groups, ok := buildLayoutGroupsFromMetas(layout, current)
	if !ok {
		updated, changed := buildUpdatedSegmentChunkMetas(current, replacements)
		if !changed {
			return false, nil
		}
		if err := db.writeSegmentIndexLocked(folderPath, updated, entry); err != nil {
			return false, err
		}
		return true, nil
	}

	affected := make(map[int]struct{}, len(replacements))
	for oldFileName, replacement := range replacements {
		found := false
		for groupIdx := range groups {
			updated, hit := applyGroupReplacement(groups[groupIdx], oldFileName, replacement)
			if !hit {
				continue
			}
			groups[groupIdx] = updated
			affected[groupIdx] = struct{}{}
			found = true
			break
		}
		if !found {
			updated, changed := buildUpdatedSegmentChunkMetas(current, replacements)
			if !changed {
				return false, nil
			}
			if err := db.writeSegmentIndexLocked(folderPath, updated, entry); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	oldEntries := layout.entries
	newEntries := make([]segmentIndexL1Entry, 0, len(layout.entries))
	keep := make(map[uint32]struct{}, len(layout.entries))
	for idx := range groups {
		if len(groups[idx]) == 0 {
			continue
		}
		if _, touch := affected[idx]; touch {
			buf, err := encodeSegmentChunkMetas(groups[idx])
			if err != nil {
				return false, err
			}
			if err := db.writeSegmentIndexFileIfChanged(level2IndexFilePath(folderPath, layout.entries[idx].MetaID), buf); err != nil {
				return false, err
			}
		}
		e := segmentIndexL1Entry{
			MetaID:     layout.entries[idx].MetaID,
			ChunkCount: uint32(len(groups[idx])),
			KeyStart:   cloneBytes(groups[idx][0].KeyStart),
		}
		newEntries = append(newEntries, e)
		keep[e.MetaID] = struct{}{}
	}

	newLayout := segmentIndexLayout{mode: indexLayoutMultiLevel, entries: newEntries, nextMetaID: layout.nextMetaID}
	topOrderChanged := false
	newLayout.entries = normalizeSegmentIndexL1EntriesOrder(newLayout.entries, &topOrderChanged)
	needTopUpdate := !layoutEntriesEqual(layout.entries, newLayout.entries) || layout.nextMetaID != newLayout.nextMetaID || topOrderChanged
	if needTopUpdate {
		buf, err := encodeTopLevelIndex(newLayout)
		if err != nil {
			return false, err
		}
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if err := db.writeSegmentIndexFileIfChanged(indexPath, buf); err != nil {
			return false, err
		}
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.AddLayoutByPath(folderPath, newLayout)
	}

	db.bumpSegmentIndexGenerationLocked(entry)
	if len(oldEntries) > 0 {
		if err := removeStaleLevel2IndexFiles(folderPath, oldEntries, keep); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (db *PrefixDB) invalidateSegmentIndexCacheByPath(folderPath string) {
	unlock := db.lockSegmentIndexFolderWrite(folderPath)
	defer unlock()
	if folderPath == "" {
		return
	}
	db.segmentIndexMu.Lock()
	defer db.segmentIndexMu.Unlock()
	if db.storageIndexFolderPath == folderPath {
		db.storageIndexFolderPath = ""
		db.storageIndexMetas = nil
		db.storageIndexReusable = true
		db.storageIndexArena = nil
	}
	if db.storageIndexPartialFolderPath == folderPath {
		db.storageIndexPartialFolderPath = ""
		db.storageIndexPartialMetaID = 0
		db.storageIndexPartialMetas = nil
		db.storageIndexPartialReusable = true
		db.storageIndexPartialArena = nil
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.RemoveByPath(folderPath)
		db.storageIndexCache.RemoveLayoutByPath(folderPath)
	}
}

func (db *PrefixDB) invalidateSegmentIndexCache(folderID uint32) {
	db.invalidateSegmentIndexCacheByPath(db.segmentedFolderPath(folderID))
}

func (db *PrefixDB) refreshSegmentIndexCacheByPath(folderPath string, metas []segmentChunkMeta) {
	unlock := db.lockSegmentIndexFolderWrite(folderPath)
	defer unlock()
	db.refreshSegmentIndexCacheByPathLocked(folderPath, metas)
}

func (db *PrefixDB) refreshSegmentIndexCacheByPathLocked(folderPath string, metas []segmentChunkMeta) {
	if folderPath == "" {
		return
	}
	cloned := cloneSegmentChunkMetas(metas)
	db.segmentIndexMu.Lock()
	defer db.segmentIndexMu.Unlock()
	if db.storageIndexFolderPath == folderPath {
		db.storageIndexFolderPath = folderPath
		db.storageIndexMetas = cloneSegmentChunkMetas(cloned)
		db.storageIndexReusable = true
		db.storageIndexArena = nil
	}
	if db.storageIndexPartialFolderPath == folderPath {
		db.storageIndexPartialFolderPath = ""
		db.storageIndexPartialMetaID = 0
		db.storageIndexPartialMetas = nil
		db.storageIndexPartialReusable = true
		db.storageIndexPartialArena = nil
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.AddByPathNoClone(folderPath, cloned)
	}
}

func (db *PrefixDB) refreshSegmentIndexCache(folderID uint32, metas []segmentChunkMeta) {
	db.refreshSegmentIndexCacheByPath(db.segmentedFolderPath(folderID), metas)
}

func appendVarBytes(buf []byte, data []byte) ([]byte, error) {
	if len(data) > 0xFFFF {
		return buf, fmt.Errorf("segment meta field too large: %d", len(data))
	}
	var hdr [2]byte
	writeUint16BE(hdr[:], uint16(len(data)))
	buf = append(buf, hdr[:]...)
	buf = append(buf, data...)
	return buf, nil
}

func (db *PrefixDB) readSegmentIndexLockedInternalByPath(folderPath string, useLRU bool) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	return db.readSegmentIndexLockedInternalByPathWithTracker(folderPath, useLRU, nil)
}

func (db *PrefixDB) readSegmentIndexLockedInternalByPathWithTracker(folderPath string, useLRU bool, tracker *cacheMissCostTracker) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	if useLRU && db.storageIndexCache != nil {
		if metas, ok := db.storageIndexCache.GetByPath(folderPath); ok {
			return metas, segmentIndexLookupSourceL2Cache, nil
		}
	}
	layout, layoutSource, err := db.loadSegmentIndexLayoutWithSourceAndTracker(folderPath, tracker)
	if err != nil {
		return nil, segmentIndexLookupSourceNoCache, err
	}
	var metas []segmentChunkMeta
	if layout.mode == indexLayoutMultiLevel {
		total := 0
		for _, entry := range layout.entries {
			total += int(entry.ChunkCount)
		}
		metas = make([]segmentChunkMeta, 0, total)
		var arena []byte
		for idx, entry := range layout.entries {
			data, err := db.readSegmentIndexFileWithTracker(level2IndexFilePath(folderPath, entry.MetaID), tracker)
			if err != nil {
				return nil, segmentIndexLookupSourceNoCache, err
			}
			appendExisting := idx != 0
			if err := decodeSegmentIndexBuffer(data, &metas, &arena, appendExisting, folderPath); err != nil {
				return nil, segmentIndexLookupSourceNoCache, err
			}
		}
	} else {
		data := layout.flatData
		if len(data) == 0 {
			indexPath := filepath.Join(folderPath, segmentIndexFileName)
			data, err = db.readSegmentIndexFileWithTracker(indexPath, tracker)
			if err != nil {
				return nil, segmentIndexLookupSourceNoCache, err
			}
		}
		metas = nil
		var arena []byte
		if err := decodeSegmentIndexBuffer(data, &metas, &arena, false, folderPath); err != nil {
			return nil, segmentIndexLookupSourceNoCache, err
		}
	}
	estimatedSize := estimateSegmentIndexSize(metas)
	if useLRU && estimatedSize >= segmentIndexCacheThresholdBytes && db.storageIndexCache != nil {
		db.storageIndexCache.AddByPathNoClone(folderPath, metas)
	}
	if layoutSource != segmentIndexLookupSourceNoCache {
		return metas, layoutSource, nil
	}
	return metas, segmentIndexLookupSourceNoCache, nil
}

func (db *PrefixDB) readSegmentIndexNoCache(folderID uint32) ([]segmentChunkMeta, error) {
	unlock := db.lockSegmentIndexFolder(db.segmentedFolderPath(folderID))
	defer unlock()
	metas, _, err := db.readSegmentIndexLockedInternalByPath(db.segmentedFolderPath(folderID), false)
	return metas, err
}

func (db *PrefixDB) readSegmentIndexNoCacheByPath(folderPath string) ([]segmentChunkMeta, error) {
	unlock := db.lockSegmentIndexFolder(folderPath)
	defer unlock()
	metas, _, err := db.readSegmentIndexLockedInternalByPath(folderPath, false)
	return metas, err
}

func (db *PrefixDB) readSegmentIndexForKeyByPath(folderPath string, key []byte) ([]segmentChunkMeta, error) {
	metas, _, err := db.readSegmentIndexForKeyByPathWithSource(folderPath, key)
	return metas, err
}

func (db *PrefixDB) readSegmentIndexForKeyByPathWithSource(folderPath string, key []byte) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	return db.readSegmentIndexForKeyByPathWithSourceAndTracker(folderPath, key, nil)
}

func (db *PrefixDB) readSegmentIndexForKeyByPathWithSourceAndTracker(folderPath string, key []byte, tracker *cacheMissCostTracker) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	entryLock, unlock := db.lockSegmentIndexFolderReadEntry(folderPath)
	defer unlock()
	return db.readSegmentIndexForKeyByPathWithSourceAndTrackerLocked(folderPath, key, tracker, entryLock)
}

func (db *PrefixDB) readSegmentIndexForKeyByPathWithSourceAndTrackerLocked(folderPath string, key []byte, tracker *cacheMissCostTracker, entryLock *segmentIndexFolderLock) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	if entryLock == nil {
		return db.readSegmentIndexForKeyByPathWithSourceAndTracker(folderPath, key, tracker)
	}
	generation := atomic.LoadUint64(&entryLock.gen)
	if len(key) == 0 {
		return db.readSegmentIndexLockedInternalByPathWithTracker(folderPath, true, tracker)
	}
	layout, layoutSource, err := db.loadSegmentIndexLayoutWithSourceAndTracker(folderPath, tracker)
	if err != nil {
		return nil, segmentIndexLookupSourceNoCache, err
	}
	// For multi-level indexes, key lookup should prefer level2 shard cache.
	// Returning the full level1 metas slice can be significantly more expensive
	// than selecting one level2 shard and causes slower cache-hit latency.
	if layout.mode != indexLayoutMultiLevel && db.storageIndexCache != nil {
		if metas, ok := db.storageIndexCache.GetByPath(folderPath); ok {
			return metas, segmentIndexLookupSourceL2Cache, nil
		}
	}
	if layout.mode != indexLayoutMultiLevel {
		data := layout.flatData
		if len(data) == 0 {
			indexPath := filepath.Join(folderPath, segmentIndexFileName)
			data, err = db.readSegmentIndexFileWithTracker(indexPath, tracker)
			if err != nil {
				return nil, segmentIndexLookupSourceNoCache, err
			}
		}
		if meta, selectErr := selectFixedFlatSegmentIndexMeta(data, key); selectErr != nil {
			return nil, segmentIndexLookupSourceNoCache, selectErr
		} else if meta != nil {
			return []segmentChunkMeta{*meta}, layoutSource, nil
		}
		metas, source, readErr := db.readSegmentIndexLockedInternalByPathWithTracker(folderPath, true, tracker)
		if readErr != nil {
			return nil, source, readErr
		}
		if source == segmentIndexLookupSourceNoCache && layoutSource != segmentIndexLookupSourceNoCache {
			source = layoutSource
		}
		return metas, source, nil
	}
	entry := selectSegmentL1Entry(layout.entries, key)
	if entry == nil {
		// Log detailed information for debugging
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: failed to locate L1 index entry for key - folder=%s key=%x entries_count=%d\n",
			folderPath, key, len(layout.entries))
		// Print key ranges for all L1 entries
		for i, e := range layout.entries {
			fmt.Fprintf(prefixdbLogWriter, "prefixdb DEBUG: L1[%d] MetaID=%d ChunkCount=%d KeyStart=%x\n",
				i, e.MetaID, e.ChunkCount, e.KeyStart)
		}
		return nil, segmentIndexLookupSourceNoCache, fmt.Errorf("%w for folder %s", errSegmentIndexEntryNotFound, folderPath)
	}
	if db.storageIndexCache != nil {
		if metas, ok := db.storageIndexCache.GetLevel2ByPath(folderPath, entry.MetaID, generation); ok {
			if selectSegmentChunkMeta(metas, key) == nil {
				fallbackMetas, fallbackSource, fallbackErr := db.readSegmentIndexLockedInternalByPath(folderPath, true)
				if fallbackErr == nil && selectSegmentChunkMeta(fallbackMetas, key) != nil {
					return fallbackMetas, fallbackSource, nil
				}
			}
			return metas, segmentIndexLookupSourceL2Cache, nil
		}
	}
	metas := make([]segmentChunkMeta, 0, entry.ChunkCount)
	var arena []byte
	data, err := db.readSegmentIndexFile(level2IndexFilePath(folderPath, entry.MetaID))
	if err != nil {
		return nil, segmentIndexLookupSourceNoCache, err
	}
	if err := decodeSegmentIndexBuffer(data, &metas, &arena, false, folderPath); err != nil {
		return nil, segmentIndexLookupSourceNoCache, err
	}
	if db.storageIndexCache != nil {
		db.storageIndexCache.AddLevel2ByPathNoClone(folderPath, entry.MetaID, generation, metas)
	}
	// Guard against stale/misaligned shard boundaries in multi-level indexes.
	// If the selected L2 shard cannot resolve the key, fall back to a full-index
	// view so boundary keys can still locate the correct chunk.
	if selectSegmentChunkMeta(metas, key) == nil {
		fallbackMetas, fallbackSource, fallbackErr := db.readSegmentIndexLockedInternalByPath(folderPath, true)
		if fallbackErr == nil && selectSegmentChunkMeta(fallbackMetas, key) != nil {
			return fallbackMetas, fallbackSource, nil
		}
	}
	if layoutSource != segmentIndexLookupSourceNoCache {
		return metas, layoutSource, nil
	}
	return metas, segmentIndexLookupSourceNoCache, nil
}

func (db *PrefixDB) readSegmentIndexLockedInternal(folderID uint32, useLRU bool) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	return db.readSegmentIndexLockedInternalByPath(db.segmentedFolderPath(folderID), useLRU)
}

func (db *PrefixDB) readSegmentIndexForKey(folderID uint32, key []byte) ([]segmentChunkMeta, error) {
	metas, _, err := db.readSegmentIndexForKeyWithSource(folderID, key)
	return metas, err
}

func (db *PrefixDB) readSegmentIndexForKeyWithSource(folderID uint32, key []byte) ([]segmentChunkMeta, segmentIndexLookupSource, error) {
	return db.readSegmentIndexForKeyByPathWithSource(db.segmentedFolderPath(folderID), key)
}

func cloneSegmentChunkMetas(src []segmentChunkMeta) []segmentChunkMeta {
	if len(src) == 0 {
		return nil
	}
	dst := make([]segmentChunkMeta, len(src))
	for i := range src {
		dst[i] = segmentChunkMeta{
			FileName: strings.Clone(src[i].FileName),
		}
		dst[i].KeyStart = cloneBytes(src[i].KeyStart)
	}
	return dst
}

func cloneSegmentIndexLayout(src segmentIndexLayout) segmentIndexLayout {
	dst := segmentIndexLayout{
		mode:       src.mode,
		nextMetaID: src.nextMetaID,
	}
	if len(src.flatData) > 0 {
		dst.flatData = cloneBytes(src.flatData)
	}
	if len(src.entries) == 0 {
		return dst
	}
	dst.entries = make([]segmentIndexL1Entry, len(src.entries))
	for i := range src.entries {
		dst.entries[i] = segmentIndexL1Entry{
			MetaID:     src.entries[i].MetaID,
			ChunkCount: src.entries[i].ChunkCount,
			KeyStart:   cloneBytes(src.entries[i].KeyStart),
		}
	}
	return dst
}

func estimateSegmentIndexLayoutMemory(layout segmentIndexLayout) uint64 {
	total := uint64(unsafe.Sizeof(segmentIndexLayout{}))
	total += uint64(len(layout.flatData))
	total += uint64(len(layout.entries)) * uint64(unsafe.Sizeof(segmentIndexL1Entry{}))
	for i := range layout.entries {
		total += uint64(len(layout.entries[i].KeyStart))
	}
	if total == 0 {
		return 1
	}
	return total
}

func estimateSegmentChunkMetasMemory(metas []segmentChunkMeta) uint64 {
	if len(metas) == 0 {
		return 0
	}
	total := uint64(len(metas)) * uint64(unsafe.Sizeof(segmentChunkMeta{}))
	for i := range metas {
		total += uint64(len(metas[i].FileName))
		total += uint64(len(metas[i].KeyStart))
	}
	return total
}

func readVarBytes(buf []byte) ([]byte, int, error) {
	if len(buf) < 2 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	ln := int(buf[0])<<8 | int(buf[1])
	if len(buf) < 2+ln {
		return nil, 0, io.ErrUnexpectedEOF
	}
	return buf[2 : 2+ln], 2 + ln, nil
}

func selectSegmentChunkMeta(metas []segmentChunkMeta, key []byte) *segmentChunkMeta {
	if len(metas) == 0 {
		return nil
	}
	if len(key) == 0 {
		return &metas[0]
	}
	idx := upperBoundSegmentChunkMetas(metas, key)
	if idx == 0 {
		return nil
	}
	return &metas[idx-1]
}

func upperBoundSegmentIndexL1Entries(entries []segmentIndexL1Entry, key []byte) int {
	lo, hi := 0, len(entries)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		start := entries[mid].KeyStart
		if len(start) == 0 || compareSegmentIndexKeyStarts(start, key) <= 0 {
			lo = mid + 1
			continue
		}
		hi = mid
	}
	return lo
}

func upperBoundSegmentChunkMetas(metas []segmentChunkMeta, key []byte) int {
	lo, hi := 0, len(metas)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		start := metas[mid].KeyStart
		if len(start) == 0 || compareSegmentIndexKeyStarts(start, key) <= 0 {
			lo = mid + 1
			continue
		}
		hi = mid
	}
	return lo
}

func (db *PrefixDB) readSegmentChunkFile(folderID uint32, fileName string) ([]kvPair, *bufferLease, error) {
	return db.readSegmentChunkFileWithUsage(folderID, fileName, diskIOUsageStorageSeparatedLogs)
}

func (db *PrefixDB) readSegmentChunkFileWithUsage(folderID uint32, fileName string, usage diskIOUsage) ([]kvPair, *bufferLease, error) {
	lease, err := db.readSegmentFileBufferWithUsage(folderID, fileName, usage)
	if err != nil {
		return nil, nil, err
	}
	kvCount, err := countChunkEntriesFromTail(lease.Bytes())
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	entries, err := buildPairsFromChunkBuffer(lease.Bytes(), kvCount, nil)
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	return entries, lease, nil
}

func (db *PrefixDB) readSegmentChunkFileWithUsageByPath(folderPath string, fileName string, usage diskIOUsage) ([]kvPair, *bufferLease, error) {
	lease, err := db.readSegmentFileBufferByPathWithUsage(folderPath, fileName, usage)
	if err != nil {
		return nil, nil, err
	}
	kvCount, err := countChunkEntriesFromTail(lease.Bytes())
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	entries, err := buildPairsFromChunkBuffer(lease.Bytes(), kvCount, nil)
	if err != nil {
		lease.Release()
		return nil, nil, err
	}
	return entries, lease, nil
}

func (db *PrefixDB) readSegmentChunkFileWithUsageByPathPreferCache(folderPath string, fileName string, usage diskIOUsage) ([]kvPair, *bufferLease, error) {
	if lease, ok := db.getCachedSegmentChunkLease(folderPath, fileName); ok {
		kvCount, err := countChunkEntriesFromTail(lease.Bytes())
		if err != nil {
			lease.Release()
			return nil, nil, err
		}
		entries, err := buildPairsFromChunkBuffer(lease.Bytes(), kvCount, nil)
		if err != nil {
			lease.Release()
			return nil, nil, err
		}
		return entries, lease, nil
	}
	return db.readSegmentChunkFileWithUsageByPath(folderPath, fileName, usage)
}

func (db *PrefixDB) readSegmentFileBufferByPathWithUsage(folderPath string, fileName string, usage diskIOUsage) (*bufferLease, error) {
	return db.readSegmentFileBufferByPathWithUsageAndTracker(folderPath, fileName, usage, nil)
}

func (db *PrefixDB) readSegmentFileBufferByPathWithUsageAndTracker(folderPath string, fileName string, usage diskIOUsage, tracker *cacheMissCostTracker) (*bufferLease, error) {
	fullPath := filepath.Join(folderPath, fileName)
	f, err := db.openCachedReadOnlyFile(fullPath)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return nil, fmt.Errorf("empty segment chunk: %s", fullPath)
	}
	if size > int64(^uint32(0)) {
		return nil, fmt.Errorf("segment chunk too large: %s", fullPath)
	}
	intSize := int(size)
	buf := getDataBuffer(intSize)
	sr := io.NewSectionReader(f, 0, size)
	ioStart := time.Now()
	if _, err := io.ReadFull(sr, buf[:intSize]); err != nil {
		tracker.addIO(0, time.Since(ioStart))
		putDataBuffer(buf)
		return nil, err
	}
	tracker.addIO(intSize, time.Since(ioStart))
	db.addDiskRead(usage, intSize)
	return newBufferLease(buf[:intSize]), nil
}

func (db *PrefixDB) segmentChunkFileSizeByPath(folderPath string, fileName string) (int64, error) {
	fullPath := filepath.Join(folderPath, fileName)
	f, err := db.openCachedReadOnlyFile(fullPath)
	if err != nil {
		return 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *PrefixDB) readSegmentedChunkToCacheStreamingByPath(folderPath string, fileName string, accountKey []byte, storageKey []byte, failure *segmentedStorageReadFailure) ([]byte, *segmentedStorageReadFailure, *bufferLease, error) {
	return db.readSegmentedChunkToCacheStreamingByPathWithTracker(folderPath, fileName, accountKey, storageKey, failure, nil)
}

func (db *PrefixDB) readSegmentedChunkToCacheStreamingByPathWithTracker(folderPath string, fileName string, accountKey []byte, storageKey []byte, failure *segmentedStorageReadFailure, tracker *cacheMissCostTracker) ([]byte, *segmentedStorageReadFailure, *bufferLease, error) {
	lease, err := db.readSegmentFileBufferByPathWithUsageAndTracker(folderPath, fileName, diskIOUsageStorageSeparatedLogs, tracker)
	if err != nil {
		failure.reason = "segment-chunk-read-failed"
		return nil, failure, nil, err
	}
	buf := lease.Bytes()
	if len(buf) == 0 {
		lease.Release()
		failure.reason = "segment-chunk-empty"
		return nil, failure, nil, nil
	}

	cache := db.storageCache
	prefetchLimit := db.storageGetCacheCount
	if prefetchLimit == 0 {
		db.cacheSegmentChunkLease(folderPath, fileName, lease)
		value, readFailure := db.readSegmentedChunkBufferToCache(buf, accountKey, storageKey, failure)
		return value, readFailure, lease, nil
	}
	pending := make([]kvPair, 0, prefetchLimit)
	for cursor := len(buf); cursor > 0; {
		if cursor < segmentedChunkEntryHeaderSize {
			lease.Release()
			failure.reason = "segment-chunk-corrupted"
			return nil, failure, nil, nil
		}
		footer := buf[cursor-segmentedChunkEntryHeaderSize : cursor]
		klen := int(readUint16BE(footer[:2]))
		vlen := int(readUint16BE(footer[2:4]))
		if klen == 0 && vlen == 0 {
			if _, ok := chunkCommitTagBlockID(buf, cursor); !ok {
				lease.Release()
				failure.reason = "segment-chunk-corrupted"
				return nil, failure, nil, nil
			}
			cursor -= commitTagRecordSize
			continue
		}
		recordDataLen := klen + vlen
		recordStart := cursor - segmentedChunkEntryHeaderSize - recordDataLen
		if recordStart < 0 {
			lease.Release()
			failure.reason = "segment-chunk-corrupted"
			return nil, failure, nil, nil
		}

		entryBuf := buf[recordStart : cursor-segmentedChunkEntryHeaderSize]
		key := entryBuf[:klen]
		var value []byte
		if vlen > 0 {
			value = entryBuf[klen:recordDataLen]
		}
		if prefetchLimit > 0 && len(pending) < prefetchLimit {
			pending = append(pending, kvPair{key: cloneBytes(key), val: cloneBytes(value)})
		}
		if bytes.Equal(key, storageKey) {
			if cache != nil {
				for i := range pending {
					db.addStorageCacheValue(accountKey, pending[i].key, pending[i].val, true)
				}
			}
			if value == nil {
				if cache != nil {
					db.addStorageCacheValue(accountKey, storageKey, nil, false)
				}
				failure.reason = "segment-chunk-tombstone"
				return nil, failure, lease, nil
			}
			result := append([]byte(nil), value...)
			if cache != nil {
				db.addStorageCacheValue(accountKey, storageKey, result, false)
			}
			return result, nil, lease, nil
		}
		cursor = recordStart
	}

	if cache != nil {
		db.addStorageCacheValue(accountKey, storageKey, nil, false)
	}
	failure.reason = "segment-chunk-key-not-found"
	return nil, failure, lease, nil
}

func (db *PrefixDB) readSegmentFileBufferWithUsage(folderID uint32, fileName string, usage diskIOUsage) (*bufferLease, error) {
	fullPath := filepath.Join(db.segmentedFolderPath(folderID), fileName)
	f, err := db.openCachedReadOnlyFile(fullPath)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return nil, fmt.Errorf("empty segment chunk: %s", fullPath)
	}
	if size > int64(^uint32(0)) {
		return nil, fmt.Errorf("segment chunk too large: %s", fullPath)
	}
	intSize := int(size)
	buf := getDataBuffer(intSize)
	// NOTE: file handles may be reused via fileHandleCache. Do not rely on the
	// shared file offset (Read/Seek). Use a ReaderAt-based reader to always read
	// from offset 0.
	sr := io.NewSectionReader(f, 0, size)
	if _, err := io.ReadFull(sr, buf[:intSize]); err != nil {
		putDataBuffer(buf)
		return nil, err
	}
	db.addDiskRead(usage, intSize)
	return newBufferLease(buf[:intSize]), nil
}

func (db *PrefixDB) maybeNormalizeChunkEntries(entries []kvPair, meta *segmentChunkMeta) []kvPair {
	if len(entries) < 2 || meta == nil {
		return entries
	}
	return normalizeTailScannedStorageEntries(entries)
}

func (db *PrefixDB) readAccountStorageValue(accountKey, storageKey []byte) ([]byte, bool, *segmentedStorageReadFailure, error) {
	return db.readAccountStorageValueWithTracker(accountKey, storageKey, nil)
}

func (db *PrefixDB) readAccountStorageValueWithTracker(accountKey, storageKey []byte, tracker *cacheMissCostTracker) ([]byte, bool, *segmentedStorageReadFailure, error) {
	if len(accountKey) == 0 {
		return nil, false, nil, nil
	}
	if db.isAccountStorageFolderManaged(accountKey) {
		folderPath := db.segmentedFolderPathForAccount(accountKey)
		val, failure, err := db.readSegmentedChunkToCacheByPathWithTracker(folderPath, accountKey, storageKey, tracker)
		if err != nil {
			if shouldFallbackMissingFolderRead(err) {
				db.clearAccountStorageFolder(accountKey)
			} else {
				return nil, false, failure, err
			}
		} else if val != nil {
			return val, true, nil, nil
		} else {
			return nil, false, failure, nil
		}
	}

	cacheInfo, err := db.resolveAccountStoragePointer(accountKey)
	if err != nil {
		return nil, false, nil, err
	}

	if cacheInfo.storageFileID == 0 {
		return nil, false, nil, nil
	}

	if isSegmentedStorage(cacheInfo.storageFileID) {
		if !isAccountNamedSegmentedStorage(cacheInfo.storageFileID) {
			return nil, false, nil, errors.New("legacy segmented storage pointers are no longer supported")
		}
		val, failure := db.readSegmentedChunkToCache(cacheInfo.storageFileID, accountKey, storageKey)
		if val == nil {
			return nil, false, failure, nil
		}
		return val, true, nil, nil

	} else {
		if cacheInfo.storageSize == 0 {
			storagePath, _ := db.storagePathByFileID(cacheInfo.storageFileID)
			db.logLargeLogReadFailure(accountKey, storageKey, storagePath, cacheInfo.storageFileID, cacheInfo.storageOffset, cacheInfo.storageSize, "invalid-account-entry-pointer", nil)
			return nil, false, nil, nil
		}
		val := db.readStorageSegmentFileWithTracker(cacheInfo.storageFileID, cacheInfo.storageOffset, cacheInfo.storageSize, accountKey, storageKey, tracker)
		if val == nil {
			return nil, false, nil, nil
		}
		return val, true, nil, nil
	}
}

func (db *PrefixDB) logLargeLogReadFailure(accountKey, storageKey []byte, filePath string, fileID uint32, offset uint64, size uint64, reason string, err error) {
	dir, file := splitLogPath(filePath)
	if err != nil {
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: failed to read large log via account entry account=%x storage=%x dir=%s file=%s fileID=%d offset=%d size=%d reason=%s err=%v\n", accountKey, storageKey, dir, file, fileID, offset, size, reason, err)
		return
	}
	fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: failed to read large log via account entry account=%x storage=%x dir=%s file=%s fileID=%d offset=%d size=%d reason=%s\n", accountKey, storageKey, dir, file, fileID, offset, size, reason)
}

func borrowStorageEntries(count int) []kvPair {
	if count <= 0 {
		return nil
	}
	if buf := kvPairEntryPool.Get(); buf != nil {
		entries := buf.([]kvPair)
		if cap(entries) >= count {
			return entries[:count]
		}
	}
	return make([]kvPair, count)
}

func releaseStorageEntries(entries []kvPair) {
	if entries == nil {
		return
	}
	for i := range entries {
		entries[i] = kvPair{}
	}
	kvPairEntryPool.Put(entries[:0])
}

func normalizeStorageEntries(entries []kvPair) []kvPair {
	if len(entries) <= 1 {
		return entries
	}
	// Fast path: if the chunk is already sorted, we can avoid map allocations.
	sorted := true
	strictlyIncreasing := true
	for i := 1; i < len(entries); i++ {
		cmp := bytes.Compare(entries[i-1].key, entries[i].key)
		if cmp > 0 {
			sorted = false
			strictlyIncreasing = false
			break
		}
		if cmp == 0 {
			strictlyIncreasing = false
		}
	}
	if sorted {
		if strictlyIncreasing {
			return entries
		}
		// Sorted with duplicates: keep the last entry for each key.
		out := entries[:0]
		for i := 0; i < len(entries); {
			j := i + 1
			for j < len(entries) && bytes.Equal(entries[j].key, entries[i].key) {
				j++
			}
			out = append(out, entries[j-1])
			i = j
		}
		return out
	}

	// General path: last write wins (append order), then sort for binary search.
	// Use unsafe byte->string conversion to avoid per-key allocations.
	lastIdx := make(map[string]int, len(entries))
	for i := range entries {
		lastIdx[bytesToString(entries[i].key)] = i
	}
	out := entries[:0]
	for i := range entries {
		if lastIdx[bytesToString(entries[i].key)] != i {
			continue
		}
		out = append(out, entries[i])
	}
	sortKVPairs(out)
	return out
}

func normalizeTailScannedStorageEntries(entries []kvPair) []kvPair {
	if len(entries) <= 1 {
		return entries
	}
	// Chunk files are scanned from tail to head, so the first duplicate is the
	// newest version. This is the opposite order of forward append logs.
	firstIdx := make(map[string]int, len(entries))
	for i := range entries {
		key := bytesToString(entries[i].key)
		if _, ok := firstIdx[key]; !ok {
			firstIdx[key] = i
		}
	}
	if len(firstIdx) == len(entries) {
		sortKVPairs(entries)
		return entries
	}
	out := entries[:0]
	for i := range entries {
		if firstIdx[bytesToString(entries[i].key)] != i {
			continue
		}
		out = append(out, entries[i])
	}
	sortKVPairs(out)
	return out
}

func (db *PrefixDB) resolveAccountStoragePointer(accountKey []byte) (StorageInfo, error) {
	start := time.Now()
	node, fromCache, err := db.getNodeWithSource(accountKey)
	recordTrieStorageGetBreakdownStep(&db.trieStorageAccountEntryStats, fromCache, time.Since(start))
	if err != nil {
		return StorageInfo{}, err
	}

	if node != nil && node.storageFileID != 0 {
		cacheInfo := StorageInfo{
			storageFileID: node.storageFileID,
			storageOffset: node.storageOffset,
			storageSize:   node.storageSize,
		}
		return cacheInfo, nil
	}
	return StorageInfo{}, nil
}

func (db *PrefixDB) readStorageSegmentFile(fileID uint32, offset uint64, size uint64, accountKey, storageKey []byte) []byte {
	return db.readStorageSegmentFileWithTracker(fileID, offset, size, accountKey, storageKey, nil)
}

func (db *PrefixDB) readStorageSegmentFileWithTracker(fileID uint32, offset uint64, size uint64, accountKey, storageKey []byte, tracker *cacheMissCostTracker) []byte {
	if isSegmentedStorage(fileID) {
		return nil
	}
	start := time.Now()
	defer func() {
		recordTrieStorageGetBreakdownStep(&db.trieStorageKVStats, false, time.Since(start))
	}()
	p, _ := db.storagePathByFileID(fileID)

	f, err := db.openCachedReadOnlyFile(p)
	if err != nil {
		db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "open-storage-file", err)
		return nil
	}

	if size == 0 {
		db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "empty-storage-size", nil)
		return nil
	}

	total := int(size)
	buf := getDataBuffer(total)
	read := 0
	var ret []byte
	for read < total {
		ioStart := time.Now()
		n, err := f.ReadAt(buf[read:total], int64(offset)+int64(read))
		tracker.addIO(n, time.Since(ioStart))
		if err != nil {
			if err == io.EOF && read+n == total {
				read += n
				db.addDiskRead(diskIOUsageStorageCommonLogs, n)
				break
			}
			db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "read-storage-file", err)
			putDataBuffer(buf)
			return nil
		}
		read += n
		db.addDiskRead(diskIOUsageStorageCommonLogs, n)
	}
	if read != total {
		db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "short-storage-read", io.ErrUnexpectedEOF)
		putDataBuffer(buf)
		return nil
	}
	buf = buf[:total]

	if db.storageCache != nil && len(accountKey) > 0 && len(storageKey) > 0 {
		payload, kvCount, parseErr := parseSegmentBuffer(buf)
		if parseErr != nil {
			db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "corrupted-storage-segment", parseErr)
		} else {
			cursor := 0
			payloadLen := len(payload)
			hit := false
			malformed := false
			count := 0
			for i := 0; i < kvCount; i++ {
				if cursor+segmentedChunkEntryHeaderSize > payloadLen {
					malformed = true
					break
				}
				header := payload[cursor : cursor+segmentedChunkEntryHeaderSize]
				klen := int(readUint16BE(header[:2]))
				vlen := int(readUint16BE(header[2:4]))
				if klen == 0 && vlen == 0 {
					if _, ok := forwardCommitTagBlockID(payload, cursor); !ok {
						malformed = true
						break
					}
					cursor += commitTagRecordSize
					i--
					continue
				}
				cursor += segmentedChunkEntryHeaderSize
				totalLen := klen + vlen
				if cursor+totalLen > payloadLen {
					malformed = true
					break
				}
				keyRaw := payload[cursor : cursor+klen]
				key := keyRaw
				if bytes.HasPrefix(key, storageKey) {
					var value []byte
					if vlen > 0 {
						value = payload[cursor+klen : cursor+totalLen]
					}
					if bytes.Equal(key, storageKey) {
						if value == nil {
							ret = nil
							db.addStorageCacheValue(accountKey, key, nil, false)
						} else {
							ret = append([]byte(nil), value...)
							db.addStorageCacheValue(accountKey, key, value, false)
						}
						hit = true
					}
					if hit && count < 16 {
						if value == nil {
							db.addStorageCacheValue(accountKey, key, nil, !bytes.Equal(key, storageKey))
						} else {
							db.addStorageCacheValue(accountKey, key, value, !bytes.Equal(key, storageKey))
						}
						count++
					}
				}
				cursor += totalLen
			}
			if malformed {
				db.logLargeLogReadFailure(accountKey, storageKey, p, fileID, offset, size, "corrupted-storage-segment", io.ErrUnexpectedEOF)
			}
			if !hit && !malformed {
				db.addStorageCacheValue(accountKey, storageKey, nil, false)
			}
		}
	}
	putDataBuffer(buf)
	return ret
}

func (db *PrefixDB) readStorageSegmentPayload(fileID uint32, offset uint64, size uint64) ([]byte, int, *bufferLease, error) {
	return db.readStorageSegmentPayloadWithTracker(fileID, offset, size, nil)
}

func (db *PrefixDB) readStorageSegmentPayloadWithTracker(fileID uint32, offset uint64, size uint64, tracker *cacheMissCostTracker) ([]byte, int, *bufferLease, error) {
	if isSegmentedStorage(fileID) {
		if isAccountNamedSegmentedStorage(fileID) {
			return nil, 0, nil, fmt.Errorf("account-named segmented storage requires account-key folder context")
		}
		return nil, 0, nil, errors.New("legacy segmented storage pointers are no longer supported")
	}
	p, _ := db.storagePathByFileID(fileID)
	f, err := db.openCachedReadOnlyFile(p)
	if err != nil {
		return nil, 0, nil, err
	}
	if size == 0 {
		return nil, 0, nil, nil
	}
	total := int(size)
	buf := getDataBuffer(total)
	read := 0
	for read < total {
		ioStart := time.Now()
		n, err := f.ReadAt(buf[read:total], int64(offset)+int64(read))
		tracker.addIO(n, time.Since(ioStart))
		if err != nil {
			if err == io.EOF && read+n == total {
				read += n
				db.addDiskRead(diskIOUsageStorageCommonLogs, n)
				break
			}
			putDataBuffer(buf)
			return nil, 0, nil, err
		}
		read += n
		db.addDiskRead(diskIOUsageStorageCommonLogs, n)
	}
	if read != total {
		putDataBuffer(buf)
		return nil, 0, nil, io.ErrUnexpectedEOF
	}
	buf = buf[:total]

	payload, kvCount, err := parseSegmentBuffer(buf)
	if err != nil {
		putDataBuffer(buf)
		return nil, 0, nil, err
	}
	return payload, kvCount, newBufferLease(buf), nil

}

func parseSegmentBuffer(buf []byte) ([]byte, int, error) {
	kvCount, err := countPayloadEntriesWithHeaderSize(buf, segmentedChunkEntryHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	return buf, kvCount, nil
}

func countPayloadEntriesWithHeaderSize(payload []byte, headerSize int) (int, error) {
	if headerSize != segmentedChunkEntryHeaderSize {
		return 0, fmt.Errorf("unsupported segmented chunk header size: %d", headerSize)
	}
	cursor := 0
	payloadLen := len(payload)
	count := 0
	for cursor < payloadLen {
		if cursor+headerSize > payloadLen {
			return 0, io.ErrUnexpectedEOF
		}
		header := payload[cursor : cursor+headerSize]
		klen := int(readUint16BE(header[:2]))
		vlen := int(readUint16BE(header[2:4]))
		if klen == 0 && vlen == 0 {
			if _, ok := forwardCommitTagBlockID(payload, cursor); !ok {
				return 0, io.ErrUnexpectedEOF
			}
			cursor += commitTagRecordSize
			continue
		}
		cursor += headerSize
		totalLen := klen + vlen
		if cursor+totalLen > payloadLen {
			return 0, io.ErrUnexpectedEOF
		}
		cursor += totalLen
		count++
	}
	return count, nil
}

func countChunkEntriesFromTail(buf []byte) (int, error) {
	cursor := len(buf)
	count := 0
	for cursor > 0 {
		if cursor < segmentedChunkEntryHeaderSize {
			return 0, io.ErrUnexpectedEOF
		}
		footer := buf[cursor-segmentedChunkEntryHeaderSize : cursor]
		klen := int(readUint16BE(footer[:2]))
		vlen := int(readUint16BE(footer[2:4]))
		if klen == 0 && vlen == 0 {
			if _, ok := chunkCommitTagBlockID(buf, cursor); !ok {
				return 0, io.ErrUnexpectedEOF
			}
			cursor -= commitTagRecordSize
			continue
		}
		recordSize := segmentedChunkEntryHeaderSize + klen + vlen
		if recordSize > cursor {
			return 0, io.ErrUnexpectedEOF
		}
		cursor -= recordSize
		count++
	}
	return count, nil
}

func buildPairsFromPayload(payload []byte, kvCount int, headerSize int, dst []kvPair) ([]kvPair, error) {
	if kvCount <= 0 {
		return dst[:0], nil
	}
	if headerSize != segmentedChunkEntryHeaderSize {
		return nil, fmt.Errorf("unsupported segmented chunk header size: %d", headerSize)
	}

	if cap(dst) < kvCount {
		dst = make([]kvPair, kvCount)
	}
	entries := dst[:kvCount]
	cursor := 0
	payloadLen := len(payload)

	var klen, vlen int
	for i := 0; i < kvCount; i++ {
		if cursor+headerSize > payloadLen {
			return nil, io.ErrUnexpectedEOF
		}
		header := payload[cursor : cursor+headerSize]
		klen = int(readUint16BE(header[:2]))
		vlen = int(readUint16BE(header[2:4]))
		if klen == 0 && vlen == 0 {
			if _, ok := forwardCommitTagBlockID(payload, cursor); !ok {
				return nil, io.ErrUnexpectedEOF
			}
			cursor += commitTagRecordSize
			i--
			continue
		}
		cursor += headerSize
		totalLen := klen + vlen
		if cursor+totalLen > payloadLen {
			return nil, io.ErrUnexpectedEOF
		}
		var val []byte
		if vlen > 0 {
			val = payload[cursor+klen : cursor+totalLen]
		}
		entries[i] = kvPair{key: payload[cursor : cursor+klen], val: val}
		cursor += totalLen
	}

	return entries, nil
}

func buildPairsFromChunkBuffer(payload []byte, kvCount int, dst []kvPair) ([]kvPair, error) {
	if kvCount < 0 {
		var err error
		kvCount, err = countChunkEntriesFromTail(payload)
		if err != nil {
			return nil, err
		}
	}
	if kvCount <= 0 {
		return dst[:0], nil
	}

	if cap(dst) < kvCount {
		dst = make([]kvPair, 0, kvCount)
	}
	entries := dst[:0]
	cursor := len(payload)
	payloadLen := len(payload)

	var klen, vlen int
	for cursor > 0 {
		if cursor < segmentedChunkEntryHeaderSize {
			return nil, io.ErrUnexpectedEOF
		}
		header := payload[cursor-segmentedChunkEntryHeaderSize : cursor]
		klen = int(readUint16BE(header[:2]))
		vlen = int(readUint16BE(header[2:4]))
		if klen == 0 && vlen == 0 {
			if _, ok := chunkCommitTagBlockID(payload, cursor); !ok {
				return nil, io.ErrUnexpectedEOF
			}
			cursor -= commitTagRecordSize
			continue
		}
		totalLen := klen + vlen
		dataStart := cursor - segmentedChunkEntryHeaderSize - totalLen
		if dataStart < 0 || dataStart > payloadLen {
			return nil, io.ErrUnexpectedEOF
		}
		var val []byte
		if vlen > 0 {
			val = payload[dataStart+klen : dataStart+totalLen]
		}
		entries = append(entries, kvPair{
			key: payload[dataStart : dataStart+klen],
			// vlen==0 is a tombstone delete; preserve it as nil
			// so cache/read paths treat it as not-found.
			val: val,
		})
		cursor = dataStart
	}
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}

	return entries, nil
}

func (db *PrefixDB) readStorageSegmentPairs(fileID uint32, offset uint64, size uint64) ([]kvPair, *bufferLease, error) {
	if isSegmentedStorage(fileID) {
		return nil, nil, fmt.Errorf("file %d references segmented storage", fileID)
	}
	if size == 0 {
		return nil, nil, nil
	}
	payload, kvCount, backing, err := db.readStorageSegmentPayload(fileID, offset, size)
	if err != nil {
		return nil, nil, err
	}
	if kvCount == 0 {
		if backing != nil {
			backing.Release()
		}
		return nil, nil, nil
	}
	entries, err := buildPairsFromPayload(payload, kvCount, segmentedChunkEntryHeaderSize, nil)
	if err != nil {
		if backing != nil {
			backing.Release()
		}
		return nil, nil, err
	}
	return entries, backing, nil
}

func (db *PrefixDB) GetStorageCount(accountKey []byte) (int, uint64, error) {
	if db.isAccountStorageFolderManaged(accountKey) {
		folderPath := db.segmentedFolderPathForAccount(accountKey)
		metas, err := db.readSegmentIndexNoCacheByPath(folderPath)
		if err != nil {
			return 0, 0, err
		}
		count := 0
		var total uint64
		for i := range metas {
			entries, backing, err := db.readSegmentChunkFileWithUsageByPath(folderPath, metas[i].FileName, diskIOUsageStorageSeparatedLogs)
			if err != nil {
				return 0, 0, err
			}
			if backing != nil {
				total += uint64(len(backing.Bytes()))
				backing.Release()
			} else {
				info, statErr := os.Stat(filepath.Join(folderPath, metas[i].FileName))
				if statErr != nil {
					return 0, 0, statErr
				}
				total += uint64(info.Size())
			}
			count += len(entries)
		}
		return count, total, nil
	}
	node, err := db.getNode(accountKey)
	if err != nil {
		return 0, 0, err
	}
	if node == nil || node.storageFileID == 0 {
		return 0, 0, nil
	}
	if isSegmentedStorage(node.storageFileID) {
		if isAccountNamedSegmentedStorage(node.storageFileID) {
			return 0, 0, nil
		}
		return 0, 0, errors.New("legacy segmented storage pointers are no longer supported")
	}

	p, _ := db.storagePathByFileID(node.storageFileID)

	f, err := db.openCachedReadOnlyFile(p)
	if err != nil {
		return 0, 0, err
	}

	if node.storageSize == 0 {
		return 0, 0, nil
	}

	buf := make([]byte, int(node.storageSize))
	n, err := f.ReadAt(buf, int64(node.storageOffset))
	if err != nil && err != io.EOF {
		return 0, 0, err
	}
	db.addDiskRead(diskIOUsageStorageCommonLogs, n)
	buf = buf[:n]
	_, kvCount, parseErr := parseSegmentBuffer(buf)
	if parseErr != nil {
		return 0, 0, parseErr
	}
	return kvCount, node.storageSize, nil

}

// storagePathByFileID returns the storage file path, whether it's hot storage, and the real file ID.
func (db *PrefixDB) storagePathByFileID(fileID uint32) (path string, realID uint32) {
	if isSegmentedStorage(fileID) {
		return "", 0
	}
	realID = fileID
	return filepath.Join(db.storageDir, fmt.Sprintf("storage_%08d.dat", realID)), realID
}

func (db *PrefixDB) bufferLogPathForAccount(accountKey []byte) (string, error) {
	if len(accountKey) == 0 {
		return "", errors.New("account key required for buffer log")
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	return filepath.Join(folderPath, bufferLogFileName), nil
}

func (db *PrefixDB) appendBufferLogEntries(accountKey []byte, kvs []kvPair, blockID uint64) error {
	if len(accountKey) == 0 || len(kvs) == 0 {
		return nil
	}
	if db.bufferLogBloom != nil {
		for _, kv := range kvs {
			if len(kv.key) == 0 {
				continue
			}
			db.bufferLogBloom.add(accountKey, kv.key)
		}
	}
	db.markBufferLogBloomLoaded(accountKey)
	path, err := db.bufferLogPathForAccount(accountKey)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	appendOffset := int64(0)
	if info, err := os.Stat(path); err == nil {
		appendOffset = info.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	seg, release, _, err := db.serializeStorageSegment(kvs)
	if err != nil {
		return err
	}
	defer release()
	payload := append([]byte(nil), seg...)
	payload = appendForwardCommitTag(payload, blockID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(payload); err != nil {
		return err
	}
	info, statErr := f.Stat()
	if statErr != nil {
		return statErr
	}
	db.addDiskWrite(diskIOUsageStorageBufferLogs, len(payload))
	addUint64Stat(&db.bufferLogAppendAccountCount, 1)
	addUint64Stat(&db.bufferLogAppendKVCount, uint64(len(kvs)))
	addUint64Stat(&db.bufferLogAppendBytes, uint64(len(payload)))
	db.updateBufferLogIndexForAppend(accountKey, path, appendOffset, info, kvs)
	db.maybeScheduleBufferLogMigration(accountKey, path, appendOffset+int64(len(payload)))
	return nil
}

func (db *PrefixDB) updateBufferLogIndexForAppend(accountKey []byte, path string, appendOffset int64, info os.FileInfo, kvs []kvPair) {
	if db == nil || len(accountKey) == 0 || len(kvs) == 0 {
		return
	}
	account := string(accountKey)
	db.bufferLogIndexMu.Lock()
	if db.bufferLogIndexes == nil {
		db.bufferLogIndexes = make(map[string]*bufferLogAccountIndex)
	}
	idx := db.bufferLogIndexes[account]
	if idx == nil || idx.path != path {
		idx = &bufferLogAccountIndex{path: path, entries: make(map[string]bufferLogEntryRef, len(kvs))}
		db.bufferLogIndexes[account] = idx
	}
	cursor := appendOffset
	for _, kv := range kvs {
		recordLen := int64(segmentedChunkEntryHeaderSize + len(kv.key) + len(kv.val))
		if len(kv.key) > 0 {
			idx.entries[string(kv.key)] = bufferLogEntryRef{
				valueOffset: cursor + int64(segmentedChunkEntryHeaderSize+len(kv.key)),
				valueLen:    len(kv.val),
			}
		}
		cursor += recordLen
	}
	idx.setFileIdentity(info)
	db.bufferLogIndexMu.Unlock()
}

func (db *PrefixDB) readBufferLogFileWithStats(path string) ([]byte, os.FileInfo, error) {
	start := time.Now()
	defer func() {
		if db != nil {
			addDurationStat(&db.bufferLogFullReadCount, &db.bufferLogFullReadNanos, time.Since(start))
		}
	}()
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	size := info.Size()
	if size < 0 {
		return nil, nil, fmt.Errorf("invalid file size: %s", path)
	}
	if size == 0 {
		return nil, info, nil
	}
	if size > int64(int(^uint(0)>>1)) {
		return nil, nil, fmt.Errorf("file too large to read into memory: %s", path)
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(io.NewSectionReader(f, 0, size), data); err != nil {
		return nil, nil, err
	}
	db.addDiskRead(diskIOUsageStorageBufferLogs, len(data))
	return data, info, nil
}

func openBufferLogReadFile(path string) (*os.File, os.FileInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return f, info, nil
}

func (db *PrefixDB) readBufferLogEntries(accountKey []byte) ([]kvPair, uint64, error) {
	path, err := db.bufferLogPathForAccount(accountKey)
	if err != nil {
		return nil, 0, err
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			return nil, 0, nil
		}
		return nil, 0, statErr
	}
	modTime := info.ModTime().UnixNano()
	fileSize := info.Size()
	var data []byte
	db.bufferLogCacheMu.Lock()
	if db.bufferLogCachePath == path && db.bufferLogCacheSize == fileSize && db.bufferLogCacheModTime == modTime && len(db.bufferLogCacheBuf) > 0 {
		data = db.bufferLogCacheBuf
		db.bufferLogCacheMu.Unlock()
	} else {
		db.bufferLogCacheMu.Unlock()
		var readInfo os.FileInfo
		data, readInfo, err = db.readBufferLogFileWithStats(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, 0, nil
			}
			return nil, 0, err
		}
		if len(data) == 0 {
			return nil, 0, nil
		}
		if readInfo != nil {
			fileSize = readInfo.Size()
			modTime = readInfo.ModTime().UnixNano()
		}
		db.bufferLogCacheMu.Lock()
		db.bufferLogCachePath = path
		db.bufferLogCacheSize = fileSize
		db.bufferLogCacheModTime = modTime
		db.bufferLogCacheBuf = data
		db.bufferLogCacheMu.Unlock()
	}
	if len(data) == 0 {
		return nil, 0, nil
	}
	payload, kvCount, err := parseSegmentBuffer(data)
	if err != nil {
		return nil, uint64(len(data)), err
	}
	entries, err := buildPairsFromPayload(payload, kvCount, segmentedChunkEntryHeaderSize, nil)
	if err != nil {
		return nil, uint64(len(data)), err
	}
	entries = normalizeStorageEntries(entries)
	return entries, uint64(len(data)), nil
}

func (db *PrefixDB) buildBufferLogIndexForAccount(accountKey []byte) (*bufferLogAccountIndex, int, error) {
	start := time.Now()
	defer func() {
		if db != nil {
			addDurationStat(&db.bufferLogIndexBuildCount, &db.bufferLogIndexBuildNanos, time.Since(start))
		}
	}()
	path, err := db.bufferLogPathForAccount(accountKey)
	if err != nil {
		return nil, 0, err
	}
	data, info, err := db.readBufferLogFileWithStats(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &bufferLogAccountIndex{path: path, entries: make(map[string]bufferLogEntryRef)}, 0, nil
		}
		return nil, 0, err
	}
	index := &bufferLogAccountIndex{path: path, entries: make(map[string]bufferLogEntryRef)}
	index.setFileIdentity(info)
	cursor := 0
	count := 0
	for cursor < len(data) {
		if cursor+segmentedChunkEntryHeaderSize > len(data) {
			return nil, 0, io.ErrUnexpectedEOF
		}
		header := data[cursor : cursor+segmentedChunkEntryHeaderSize]
		klen := int(readUint16BE(header[:2]))
		vlen := int(readUint16BE(header[2:4]))
		if klen == 0 && vlen == 0 {
			if _, ok := forwardCommitTagBlockID(data, cursor); !ok {
				return nil, 0, io.ErrUnexpectedEOF
			}
			cursor += commitTagRecordSize
			continue
		}
		recordStart := cursor
		cursor += segmentedChunkEntryHeaderSize
		totalLen := klen + vlen
		if cursor+totalLen > len(data) {
			return nil, 0, io.ErrUnexpectedEOF
		}
		key := data[cursor : cursor+klen]
		if len(key) > 0 {
			index.entries[string(key)] = bufferLogEntryRef{
				valueOffset: int64(recordStart + segmentedChunkEntryHeaderSize + klen),
				valueLen:    vlen,
			}
		}
		cursor += totalLen
		count++
	}
	return index, count, nil
}

func (db *PrefixDB) getBufferLogIndex(accountKey []byte) (*bufferLogAccountIndex, int, bool, error) {
	if db == nil || len(accountKey) == 0 {
		return nil, 0, false, nil
	}
	account := string(accountKey)
	db.bufferLogIndexMu.RLock()
	idx := db.bufferLogIndexes[account]
	db.bufferLogIndexMu.RUnlock()
	if idx != nil {
		return idx, 0, false, nil
	}
	built, entryCount, err := db.buildBufferLogIndexForAccount(accountKey)
	if err != nil {
		return nil, 0, false, err
	}
	db.bufferLogIndexMu.Lock()
	if db.bufferLogIndexes == nil {
		db.bufferLogIndexes = make(map[string]*bufferLogAccountIndex)
	}
	if existing := db.bufferLogIndexes[account]; existing != nil {
		db.bufferLogIndexMu.Unlock()
		return existing, 0, false, nil
	}
	db.bufferLogIndexes[account] = built
	db.bufferLogIndexMu.Unlock()
	return built, entryCount, true, nil
}

func (db *PrefixDB) readBufferLogValueByIndex(accountKey, storageKey []byte) ([]byte, bool, bufferLogReadAccessInfo, error) {
	for attempt := 0; attempt < 2; attempt++ {
		var access bufferLogReadAccessInfo
		indexStart := time.Now()
		idx, _, _, err := db.getBufferLogIndex(accountKey)
		addDurationStat(&db.bufferLogIndexLookupCount, &db.bufferLogIndexLookupNanos, time.Since(indexStart))
		access.indexLookedUp = true
		if err != nil {
			if db.resetStaleBufferLogState(accountKey, "", err) && attempt == 0 {
				continue
			}
			if isStaleBufferLogReadError(err) {
				return nil, false, access, nil
			}
			return nil, false, access, err
		}
		if idx == nil || len(idx.entries) == 0 {
			return nil, false, access, nil
		}
		access.size = idx.size
		ref, ok := idx.entries[string(storageKey)]
		if !ok {
			return nil, false, access, nil
		}
		if ref.valueLen == 0 {
			return nil, true, access, nil
		}
		f, info, err := openBufferLogReadFile(idx.path)
		if err != nil {
			if db.resetStaleBufferLogState(accountKey, idx.path, err) && attempt == 0 {
				continue
			}
			if isStaleBufferLogReadError(err) {
				return nil, false, access, nil
			}
			return nil, false, access, err
		}
		if !idx.matchesFile(info) {
			_ = f.Close()
			db.resetBufferLogState(accountKey)
			if attempt == 0 {
				continue
			}
			return nil, false, access, nil
		}
		access.size = info.Size()
		value := make([]byte, ref.valueLen)
		readStart := time.Now()
		n, err := f.ReadAt(value, ref.valueOffset)
		valueReadDuration := time.Since(readStart)
		access.valueReadNanos = uint64(valueReadDuration)
		addDurationStat(&db.bufferLogValueReadCount, &db.bufferLogValueReadNanos, valueReadDuration)
		_ = f.Close()
		if err != nil {
			if db.resetStaleBufferLogState(accountKey, idx.path, err) && attempt == 0 {
				continue
			}
			if isStaleBufferLogReadError(err) {
				return nil, false, access, nil
			}
			return nil, false, access, err
		}
		if n != ref.valueLen {
			if attempt == 0 {
				db.resetBufferLogState(accountKey)
				db.invalidateReadOnlyFileHandle(idx.path)
				continue
			}
			return nil, false, access, nil
		}
		db.addDiskRead(diskIOUsageStorageBufferLogs, n)
		return value, true, access, nil
	}
	return nil, false, bufferLogReadAccessInfo{}, nil
}

func (db *PrefixDB) readBufferLogValue(accountKey, storageKey []byte) ([]byte, bool, error) {
	if len(accountKey) == 0 || len(storageKey) == 0 {
		return nil, false, nil
	}
	lookupStart := time.Now()
	recordLookup := func(found bool) {
		elapsed := time.Since(lookupStart)
		addUint64Stat(&db.bufferLogLookupNanos, uint64(elapsed))
		if found {
			addDurationStat(&db.bufferLogHitLookupCount, &db.bufferLogHitLookupNanos, elapsed)
			return
		}
		addDurationStat(&db.bufferLogMissLookupCount, &db.bufferLogMissLookupNanos, elapsed)
	}
	addUint64Stat(&db.bufferLogLookupCount, 1)
	if db.bufferLogBloom != nil {
		bloomStart := time.Now()
		maybeContains := db.bufferLogBloom.maybeContains(accountKey, storageKey)
		addDurationStat(&db.bufferLogBloomCheckCount, &db.bufferLogBloomCheckNanos, time.Since(bloomStart))
		if !maybeContains {
			loaded, err := db.ensureBufferLogBloomForAccount(accountKey)
			if err != nil {
				addUint64Stat(&db.bufferLogErrorCount, 1)
				recordLookup(false)
				return nil, false, err
			}
			bloomStart = time.Now()
			maybeContains = loaded && db.bufferLogBloom.maybeContains(accountKey, storageKey)
			addDurationStat(&db.bufferLogBloomCheckCount, &db.bufferLogBloomCheckNanos, time.Since(bloomStart))
			if !maybeContains {
				addUint64Stat(&db.bufferLogBloomRejectCount, 1)
				addUint64Stat(&db.bufferLogMissCount, 1)
				db.addBufferLogSizeAccessStats(db.currentBufferLogSizeForStats(accountKey), false, false, false, true, false, 0, 0)
				recordLookup(false)
				return nil, false, nil
			}
		}
	}
	val, found, access, err := db.readBufferLogValueByIndex(accountKey, storageKey)
	if err != nil {
		addUint64Stat(&db.bufferLogErrorCount, 1)
		db.addBufferLogSizeAccessStats(access.size, access.indexLookedUp, false, false, false, true, 0, access.valueReadNanos)
		recordLookup(false)
		return nil, false, err
	}
	if !found {
		addUint64Stat(&db.bufferLogMissCount, 1)
		db.addBufferLogSizeAccessStats(access.size, access.indexLookedUp, false, false, false, false, 0, access.valueReadNanos)
		recordLookup(false)
		return nil, false, nil
	}
	if val == nil {
		addUint64Stat(&db.bufferLogTombstoneHitCount, 1)
		db.addBufferLogSizeAccessStats(access.size, access.indexLookedUp, true, true, false, false, 0, access.valueReadNanos)
		recordLookup(true)
		return nil, true, nil
	}
	addUint64Stat(&db.bufferLogHitCount, 1)
	addUint64Stat(&db.bufferLogHitBytes, uint64(len(val)))
	db.addBufferLogSizeAccessStats(access.size, access.indexLookedUp, true, false, false, false, len(val), access.valueReadNanos)
	recordLookup(true)
	return val, true, nil
}

func (db *PrefixDB) ensureBufferLogBloomForAccount(accountKey []byte) (bool, error) {
	if db == nil || db.bufferLogBloom == nil || len(accountKey) == 0 {
		return false, nil
	}
	if db.isBufferLogBloomLoaded(accountKey) {
		return true, nil
	}
	path, err := db.bufferLogPathForAccount(accountKey)
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			db.markBufferLogBloomLoaded(accountKey)
			return true, nil
		}
		return false, err
	}
	idx, entryCount, built, err := db.getBufferLogIndex(accountKey)
	if err != nil {
		return false, err
	}
	if idx == nil || len(idx.entries) == 0 {
		db.markBufferLogBloomLoaded(accountKey)
		return true, nil
	}
	for key := range idx.entries {
		db.bufferLogBloom.add(accountKey, []byte(key))
	}
	db.markBufferLogBloomLoaded(accountKey)
	if built {
		addUint64Stat(&db.bufferLogBloomLoadCount, 1)
		addUint64Stat(&db.bufferLogBloomLoadKVCount, uint64(entryCount))
	}
	return true, nil
}

func (db *PrefixDB) isBufferLogBloomLoaded(accountKey []byte) bool {
	if db == nil || len(accountKey) == 0 {
		return false
	}
	db.bufferLogMu.Lock()
	_, ok := db.bufferLogBloomLoadedAccounts[string(accountKey)]
	db.bufferLogMu.Unlock()
	return ok
}

func (db *PrefixDB) markBufferLogBloomLoaded(accountKey []byte) {
	if db == nil || len(accountKey) == 0 {
		return
	}
	db.bufferLogMu.Lock()
	if db.bufferLogBloomLoadedAccounts == nil {
		db.bufferLogBloomLoadedAccounts = make(map[string]struct{})
	}
	db.bufferLogBloomLoadedAccounts[string(accountKey)] = struct{}{}
	db.bufferLogMu.Unlock()
}

func (db *PrefixDB) unmarkBufferLogBloomLoaded(accountKey []byte) {
	if db == nil || len(accountKey) == 0 {
		return
	}
	db.bufferLogMu.Lock()
	delete(db.bufferLogBloomLoadedAccounts, string(accountKey))
	db.bufferLogMu.Unlock()
}

func (db *PrefixDB) resetBufferLogState(accountKey []byte) {
	if db == nil || len(accountKey) == 0 {
		return
	}
	db.unmarkBufferLogBloomLoaded(accountKey)
	db.removeBufferLogIndex(accountKey)
}

func (db *PrefixDB) resetStaleBufferLogState(accountKey []byte, path string, err error) bool {
	if !isStaleBufferLogReadError(err) {
		return false
	}
	db.resetBufferLogState(accountKey)
	db.invalidateReadOnlyFileHandle(path)
	return true
}

func isStaleBufferLogReadError(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (db *PrefixDB) removeBufferLogIndex(accountKey []byte) {
	if db == nil || len(accountKey) == 0 {
		return
	}
	db.bufferLogIndexMu.Lock()
	delete(db.bufferLogIndexes, string(accountKey))
	db.bufferLogIndexMu.Unlock()
}

func (db *PrefixDB) invalidateReadOnlyFileHandle(path string) {
	if db == nil || db.fileHandleCache == nil || path == "" {
		return
	}
	db.fileHandleCache.InvalidatePath(path)
}

func (db *PrefixDB) maybeScheduleBufferLogMigration(accountKey []byte, path string, fileSize int64) {
	if db == nil || len(accountKey) == 0 || fileSize < bufferLogMigrationSizeThreshold {
		return
	}
	account := string(accountKey)
	db.bufferLogMu.Lock()
	if db.bufferLogMigrationPending == nil {
		db.bufferLogMigrationPending = make(map[string]struct{})
	}
	if _, pending := db.bufferLogMigrationPending[account]; pending {
		db.bufferLogMu.Unlock()
		return
	}
	select {
	case db.bufferLogMigrationLimiter <- struct{}{}:
		db.bufferLogMigrationPending[account] = struct{}{}
	default:
		db.bufferLogMu.Unlock()
		return
	}
	db.bufferLogMu.Unlock()

	accountCopy := append([]byte(nil), accountKey...)
	db.bufferLogMigrationWaitGroup.Add(1)
	go func() {
		defer db.bufferLogMigrationWaitGroup.Done()
		defer func() {
			db.bufferLogMu.Lock()
			delete(db.bufferLogMigrationPending, account)
			db.bufferLogMu.Unlock()
			<-db.bufferLogMigrationLimiter
		}()
		if err := db.migrateBufferLogForAccount(accountCopy, nil); err != nil {
			prefixdbDebugf("buffer log async migrate failed account=%x path=%s err=%v", accountCopy, path, err)
		}
	}()
}

func (db *PrefixDB) migrateBufferLogForAccount(accountKey []byte, bufferEntries []kvPair) error {
	if len(accountKey) == 0 {
		return nil
	}
	migrationStart := time.Now()
	lockStart := time.Now()
	db.writeMutex.Lock()
	addDurationStat(&db.bufferLogMigrationLockCount, &db.bufferLogMigrationLockNanos, time.Since(lockStart))
	defer db.writeMutex.Unlock()

	readStart := time.Now()
	readIOBefore := db.diskIOStatsSnapshot()
	entries, readBytes, err := db.readBufferLogEntries(accountKey)
	readIOAfter := db.diskIOStatsSnapshot()
	readIODelta := diskIOStatsTotalDelta(readIOBefore, readIOAfter)
	addDurationStat(&db.bufferLogMigrationReadCount, &db.bufferLogMigrationReadNanos, time.Since(readStart))
	addUint64Stat(&db.bufferLogMigrationReadBytes, readBytes)
	addUint64Stat(&db.bufferLogMigrationDiskReadBytes, readIODelta.readBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if len(bufferEntries) == 0 {
				return nil
			}
		} else {
			return err
		}
	}
	if len(entries) > 0 {
		bufferEntries = entries
	}
	if len(bufferEntries) == 0 {
		return nil
	}
	accountKeyStr := string(accountKey)
	node, err := db.getNode(accountKey)
	if err != nil {
		return err
	}
	var accOff uint64
	var accSize uint32
	if node != nil {
		accOff = node.accountOffset
		accSize = node.accountSize
	}

	if len(bufferEntries) > 1 {
		sortStart := time.Now()
		sortKVPairs(bufferEntries)
		bufferEntries = dedupSortedKVPairs(bufferEntries)
		addDurationStat(&db.bufferLogMigrationSortCount, &db.bufferLogMigrationSortNanos, time.Since(sortStart))
	}

	var existingFileID uint32
	var existingOffset uint64
	var existingSize uint64
	if node != nil {
		existingFileID = node.storageFileID
		existingOffset = node.storageOffset
		existingSize = node.storageSize
	}
	writeBefore := db.diskIOStatsSnapshot()
	prepareStart := time.Now()
	info, inlineSegment, err := db.prepareStorageEntriesForCommit(accountKey, bufferEntries, existingFileID, existingOffset, existingSize, 0)
	addDurationStat(&db.bufferLogMigrationPrepCount, &db.bufferLogMigrationPrepNanos, time.Since(prepareStart))
	if err != nil {
		return err
	}
	if len(inlineSegment) > 0 {
		writeStart := time.Now()
		fileID, offset, size, err := db.appendStorageSegmentRaw(inlineSegment)
		addDurationStat(&db.bufferLogMigrationWriteCount, &db.bufferLogMigrationWriteNanos, time.Since(writeStart))
		if err != nil {
			return err
		}
		info = StorageInfo{storageFileID: fileID, storageOffset: offset, storageSize: size}
	}
	recordSuccessfulMigration := func(migratedKVs int) {
		writeAfter := db.diskIOStatsSnapshot()
		writeDelta := diskIOStatsTotalDelta(writeBefore, writeAfter)
		addUint64Stat(&db.bufferLogMigrationWriteOps, writeDelta.writeOps)
		addUint64Stat(&db.bufferLogMigrationWriteBytes, writeDelta.writeBytes)
		db.addBufferLogMigrationStats(migratedKVs)
		addUint64Stat(&db.bufferLogMigrationTotalNanos, uint64(time.Since(migrationStart)))
	}
	if info.storageFileID == 0 && info.storageSize == 0 {
		finishStart := time.Now()
		if err := db.prefixTree.Put(accountKey, accOff, accSize, 0, 0, 0); err != nil {
			addDurationStat(&db.bufferLogMigrationFinishCount, &db.bufferLogMigrationFinishNanos, time.Since(finishStart))
			return err
		}
		db.nodeCache.UpdateStoragePointer(accountKeyStr, StorageInfo{})
		if db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(accountKeyStr, StorageInfo{})
		}
		db.clearAccountStorageFolder(accountKey)
		if err := db.removeBufferLogForAccount(accountKey); err != nil {
			addDurationStat(&db.bufferLogMigrationFinishCount, &db.bufferLogMigrationFinishNanos, time.Since(finishStart))
			return err
		}
		addDurationStat(&db.bufferLogMigrationFinishCount, &db.bufferLogMigrationFinishNanos, time.Since(finishStart))
		recordSuccessfulMigration(len(bufferEntries))
		return nil
	}
	finishStart := time.Now()
	if !shouldSkipAccountEntryPointerUpdate(existingFileID, info.storageFileID, info.storageOffset, info.storageSize) {
		if err := db.prefixTree.Put(accountKey, accOff, accSize, info.storageFileID, info.storageOffset, info.storageSize); err != nil {
			addDurationStat(&db.bufferLogMigrationFinishCount, &db.bufferLogMigrationFinishNanos, time.Since(finishStart))
			return err
		}
		db.nodeCache.UpdateStoragePointer(accountKeyStr, info)
		if db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(accountKeyStr, info)
		}
	}
	if err := db.removeBufferLogForAccount(accountKey); err != nil {
		addDurationStat(&db.bufferLogMigrationFinishCount, &db.bufferLogMigrationFinishNanos, time.Since(finishStart))
		return err
	}
	db.syncStorageCacheEntries(accountKey, bufferEntries)
	addDurationStat(&db.bufferLogMigrationFinishCount, &db.bufferLogMigrationFinishNanos, time.Since(finishStart))
	recordSuccessfulMigration(len(bufferEntries))
	return nil
}

func (db *PrefixDB) removeBufferLogForAccount(accountKey []byte) error {
	path, err := db.bufferLogPathForAccount(accountKey)
	if err != nil {
		return err
	}
	db.invalidateReadOnlyFileHandle(path)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	db.invalidateReadOnlyFileHandle(path)
	db.bufferLogCacheMu.Lock()
	if db.bufferLogCachePath == path {
		db.bufferLogCachePath = ""
		db.bufferLogCacheSize = 0
		db.bufferLogCacheModTime = 0
		db.bufferLogCacheBuf = nil
	}
	db.bufferLogCacheMu.Unlock()
	db.resetBufferLogState(accountKey)
	return nil
}

func findLastForwardCommitTagEnd(payload []byte, allowLeadingPadding bool) (int64, bool) {
	if allowLeadingPadding && len(payload) > 0 && payload[0] == 0 {
		if end, ok := findLastForwardCommitTagEndFrom(payload, 1); ok {
			return end, true
		}
	}
	return findLastForwardCommitTagEndFrom(payload, 0)
}

func findLastForwardCommitTagEndFrom(payload []byte, start int) (int64, bool) {
	cursor := start
	var end int64
	found := false
	for cursor < len(payload) {
		if cursor+segmentedChunkEntryHeaderSize > len(payload) {
			return end, found
		}
		klen := int(readUint16BE(payload[cursor : cursor+2]))
		vlen := int(readUint16BE(payload[cursor+2 : cursor+4]))
		if klen == 0 && vlen == 0 {
			if _, ok := forwardCommitTagBlockID(payload, cursor); !ok {
				return end, found
			}
			cursor += commitTagRecordSize
			end = int64(cursor)
			found = true
			continue
		}
		cursor += segmentedChunkEntryHeaderSize
		if cursor+klen+vlen > len(payload) {
			return end, found
		}
		cursor += klen + vlen
	}
	return end, found
}

func findLastChunkCommitTagEnd(payload []byte) (int64, bool) {
	cursor := len(payload)
	for cursor > 0 {
		if cursor < segmentedChunkEntryHeaderSize {
			break
		}
		footer := payload[cursor-segmentedChunkEntryHeaderSize : cursor]
		klen := int(readUint16BE(footer[:2]))
		vlen := int(readUint16BE(footer[2:4]))
		if klen == 0 && vlen == 0 {
			if _, ok := chunkCommitTagBlockID(payload, cursor); ok {
				return int64(cursor), true
			}
			break
		}
		recordSize := segmentedChunkEntryHeaderSize + klen + vlen
		if recordSize > cursor {
			break
		}
		cursor -= recordSize
	}
	for cursor := len(payload); cursor >= commitTagRecordSize; cursor-- {
		if blockID, ok := chunkCommitTagBlockID(payload, cursor); ok && blockID != 0 {
			return int64(cursor), true
		}
	}
	return 0, false
}

func trimFileAfterLastCommitTag(path string, chunkFormat bool, allowLeadingPadding bool) (bool, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	var (
		end int64
		ok  bool
	)
	if chunkFormat {
		end, ok = findLastChunkCommitTagEnd(payload)
	} else {
		end, ok = findLastForwardCommitTagEnd(payload, allowLeadingPadding)
	}
	if !ok {
		return false, nil
	}
	if end < int64(len(payload)) {
		if err := os.Truncate(path, end); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (db *PrefixDB) TrimLogsAfterCommitTag(blockID uint64) error {
	if db == nil || blockID == 0 {
		return nil
	}
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	if db.accountFile != nil {
		if err := db.accountFile.Sync(); err != nil {
			return fmt.Errorf("sync account log before recovery trim: %w", err)
		}
		if _, err := trimFileAfterLastCommitTag(db.accountFile.Name(), false, true); err != nil {
			return fmt.Errorf("trim account log: %w", err)
		}
	}

	db.storageFileMu.Lock()
	defer db.storageFileMu.Unlock()
	if db.storageCurFile != nil {
		_ = db.storageCurFile.Sync()
	}

	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		path := filepath.Join(db.storageDir, entry.Name())
		if entry.IsDir() {
			chunkEntries, readErr := os.ReadDir(path)
			if readErr != nil {
				return readErr
			}
			for _, chunkEntry := range chunkEntries {
				if chunkEntry.IsDir() {
					continue
				}
				chunkPath := filepath.Join(path, chunkEntry.Name())
				if chunkEntry.Name() == bufferLogFileName {
					if _, trimErr := trimFileAfterLastCommitTag(chunkPath, false, false); trimErr != nil {
						return fmt.Errorf("trim buffer log %s: %w", chunkPath, trimErr)
					}
					if accountKey, decodeErr := hex.DecodeString(entry.Name()); decodeErr == nil {
						db.resetBufferLogState(accountKey)
					}
					db.bufferLogCacheMu.Lock()
					if db.bufferLogCachePath == chunkPath {
						db.bufferLogCachePath = ""
						db.bufferLogCacheSize = 0
						db.bufferLogCacheModTime = 0
						db.bufferLogCacheBuf = nil
					}
					db.bufferLogCacheMu.Unlock()
					continue
				}
				if !strings.HasSuffix(chunkEntry.Name(), ".dat") {
					continue
				}
				if _, trimErr := trimFileAfterLastCommitTag(chunkPath, true, false); trimErr != nil {
					return fmt.Errorf("trim storage chunk %s: %w", chunkPath, trimErr)
				}
				db.removeCachedSegmentChunkEntries(path, chunkEntry.Name())
			}
			continue
		}
		var fileID uint32
		if n, _ := fmt.Sscanf(entry.Name(), "storage_%08d.dat", &fileID); n != 1 {
			continue
		}
		trimmed, trimErr := trimFileAfterLastCommitTag(path, false, false)
		if trimErr != nil {
			return fmt.Errorf("trim storage log %s: %w", path, trimErr)
		}
		if trimmed && fileID == db.storageCurFileID {
			if info, statErr := os.Stat(path); statErr == nil {
				db.storageCurSize = info.Size()
			}
		}
	}
	if db.prefixTree != nil {
		if err := db.prefixTree.TrimNodeFilesAfterCommitTag(blockID); err != nil {
			return fmt.Errorf("trim prefix tree node files: %w", err)
		}
	}
	if db.nodeCache != nil {
		db.nodeCache.Clear()
	}
	return nil
}

func (db *PrefixDB) openCachedReadOnlyFile(path string) (*os.File, error) {
	if db != nil && db.fileHandleCache != nil {
		return db.fileHandleCache.Open(path, os.O_RDONLY)
	}
	return os.Open(path)
}

func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (db *PrefixDB) storageCacheKey(accountKey, storageKey []byte) string {
	// Unambiguous, binary-safe composite key:
	//   [u32 accountKeyLen (big-endian)] [accountKey bytes] [storageKey bytes]
	// This avoids collisions even if accountKey/storageKey contain '\x00' bytes.
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(accountKey)))

	var b strings.Builder
	b.Grow(4 + len(accountKey) + len(storageKey))
	_, _ = b.Write(lenBuf[:])
	_, _ = b.Write(accountKey)
	_, _ = b.Write(storageKey)
	return b.String()
}

func (db *PrefixDB) storagePrefetchPendingCount() int {
	if !analysisStatsEnabled || db == nil {
		return 0
	}
	return int(atomic.LoadUint64(&db.storagePrefetchTrackedCount))
}

func (db *PrefixDB) recordStoragePrefetchAdd(cacheKey string, value []byte) {
	if db == nil || !shouldSampleStoragePrefetchKey(cacheKey) {
		return
	}
	atomic.AddUint64(&db.trieStoragePrefetchStats.addCount, 1)
	if value == nil {
		atomic.AddUint64(&db.trieStoragePrefetchStats.addNilCount, 1)
	} else {
		atomic.AddUint64(&db.trieStoragePrefetchStats.addBytes, uint64(len(value)))
	}
	db.storagePrefetchMu.Lock()
	if db.storagePrefetchPending == nil {
		db.storagePrefetchPending = make(map[string]struct{})
	}
	if _, exists := db.storagePrefetchPending[cacheKey]; !exists {
		db.storagePrefetchPending[cacheKey] = struct{}{}
		atomic.AddUint64(&db.storagePrefetchTrackedCount, 1)
	}
	db.storagePrefetchMu.Unlock()
}

func (db *PrefixDB) clearStoragePrefetch(cacheKey string) {
	if !analysisStatsEnabled || db == nil || cacheKey == "" {
		return
	}
	if atomic.LoadUint64(&db.storagePrefetchTrackedCount) == 0 {
		return
	}
	db.storagePrefetchMu.Lock()
	if len(db.storagePrefetchPending) == 0 {
		db.storagePrefetchMu.Unlock()
		return
	}
	if _, ok := db.storagePrefetchPending[cacheKey]; ok {
		delete(db.storagePrefetchPending, cacheKey)
		atomic.AddUint64(&db.storagePrefetchTrackedCount, ^uint64(0))
		atomic.AddUint64(&db.trieStoragePrefetchStats.clearCount, 1)
	}
	db.storagePrefetchMu.Unlock()
}

func (db *PrefixDB) noteStoragePrefetchHit(cacheKey string, value interface{}) {
	if !analysisStatsEnabled || db == nil || cacheKey == "" {
		return
	}
	if atomic.LoadUint64(&db.storagePrefetchTrackedCount) == 0 {
		return
	}
	db.storagePrefetchMu.Lock()
	if len(db.storagePrefetchPending) == 0 {
		db.storagePrefetchMu.Unlock()
		return
	}
	if _, ok := db.storagePrefetchPending[cacheKey]; !ok {
		db.storagePrefetchMu.Unlock()
		return
	}
	delete(db.storagePrefetchPending, cacheKey)
	atomic.AddUint64(&db.storagePrefetchTrackedCount, ^uint64(0))
	db.storagePrefetchMu.Unlock()
	atomic.AddUint64(&db.trieStoragePrefetchStats.hitCount, 1)
	if value == nil {
		atomic.AddUint64(&db.trieStoragePrefetchStats.hitNilCount, 1)
		return
	}
	if valueBytes, ok := value.([]byte); ok {
		atomic.AddUint64(&db.trieStoragePrefetchStats.hitBytes, uint64(len(valueBytes)))
	}
}

func (db *PrefixDB) addStorageCacheValueByKey(cacheKey string, value []byte, prefetched bool) {
	if db == nil || db.storageCache == nil || cacheKey == "" {
		return
	}
	if prefetched {
		db.recordStoragePrefetchAdd(cacheKey, value)
	} else {
		db.clearStoragePrefetch(cacheKey)
	}
	db.storageCache.Add(cacheKey, value)
}

func (db *PrefixDB) addStorageCacheValue(accountKey, storageKey, value []byte, prefetched bool) {
	if db == nil || len(accountKey) == 0 || len(storageKey) == 0 {
		return
	}
	db.addStorageCacheValueByKey(db.storageCacheKey(accountKey, storageKey), value, prefetched)
}

func (db *PrefixDB) removeStorageCacheValue(accountKey, storageKey []byte) {
	if db == nil || db.storageCache == nil || len(accountKey) == 0 || len(storageKey) == 0 {
		return
	}
	cacheKey := db.storageCacheKey(accountKey, storageKey)
	db.clearStoragePrefetch(cacheKey)
	db.storageCache.Remove(cacheKey)
}

func (db *PrefixDB) syncStorageCacheEntries(accountKey []byte, kvs []kvPair) {
	if db == nil || db.storageCache == nil || len(accountKey) == 0 || len(kvs) == 0 {
		return
	}
	for _, kv := range kvs {
		cacheKey := db.storageCacheKey(accountKey, kv.key)
		if kv.val == nil {
			db.addStorageCacheValueByKey(cacheKey, nil, false)
			continue
		}
		db.addStorageCacheValueByKey(cacheKey, kv.val, false)
	}
}

func (db *PrefixDB) refreshManagedFolderStorageCache(folderPath string) error {
	accountKey, ok := db.managedAccountKeyForFolderPath(folderPath)
	if !ok || db.storageCache == nil {
		return nil
	}
	metas, err := db.readSegmentIndexNoCacheByPath(folderPath)
	if err != nil {
		return err
	}
	for _, meta := range metas {
		entries, backing, err := db.readSegmentChunkFileWithUsageByPath(folderPath, meta.FileName, diskIOUsageStorageGC)
		if err != nil {
			return err
		}
		db.syncStorageCacheEntries(accountKey, entries)
		if backing != nil {
			backing.Release()
		}
	}
	return nil
}

func (db *PrefixDB) shouldUseSegmentChunkBufferSlot() bool {
	return db != nil && db.storageGetCacheCount == 0 && db.currentSegmentChunkBuffer != nil
}

func (db *PrefixDB) getCachedSegmentChunkLease(folderPath string, fileName string) (*bufferLease, bool) {
	if !db.shouldUseSegmentChunkBufferSlot() {
		return nil, false
	}
	return db.currentSegmentChunkBuffer.GetLeaseByPath(folderPath, fileName)
}

func (db *PrefixDB) getCachedSegmentChunkBuffer(folderPath string, fileName string) ([]byte, bool) {
	if !db.shouldUseSegmentChunkBufferSlot() {
		return nil, false
	}
	return db.currentSegmentChunkBuffer.GetByPath(folderPath, fileName)
}

func (db *PrefixDB) peekCachedSegmentChunkBuffer(folderPath string, fileName string) ([]byte, bool) {
	if !db.shouldUseSegmentChunkBufferSlot() {
		return nil, false
	}
	return db.currentSegmentChunkBuffer.PeekByPath(folderPath, fileName)
}

func (db *PrefixDB) isSegmentChunkCached(folderPath string, fileName string) bool {
	if !db.shouldUseSegmentChunkBufferSlot() {
		return false
	}
	return db.currentSegmentChunkBuffer.ContainsByPath(folderPath, fileName)
}

func (db *PrefixDB) cacheSegmentChunkBuffer(folderPath string, fileName string, buf []byte) {
	if !db.shouldUseSegmentChunkBufferSlot() {
		return
	}
	db.currentSegmentChunkBuffer.SetByPath(folderPath, fileName, buf)
}

func (db *PrefixDB) cacheSegmentChunkLease(folderPath string, fileName string, lease *bufferLease) {
	if !db.shouldUseSegmentChunkBufferSlot() || lease == nil {
		return
	}
	db.currentSegmentChunkBuffer.SetReadLeaseByPath(folderPath, fileName, lease)
}

func (db *PrefixDB) updateExistingChunkBufferLease(folderPath string, fileName string, lease *bufferLease) bool {
	if !db.shouldUseSegmentChunkBufferSlot() || lease == nil {
		return false
	}
	return db.currentSegmentChunkBuffer.UpdateExistingByPath(folderPath, fileName, lease)
}

func (db *PrefixDB) updateExistingChunkBufferSerializedPayload(folderPath string, fileName string, payload []byte) bool {
	if !db.shouldUseSegmentChunkBufferSlot() || len(payload) == 0 {
		return false
	}
	buf := getDataBuffer(len(payload))
	copy(buf, payload)
	lease := newBufferLease(buf[:len(payload)])
	updated := db.updateExistingChunkBufferLease(folderPath, fileName, lease)
	lease.Release()
	return updated
}

func (db *PrefixDB) cacheGCChunkSerializedPayload(folderPath string, fileName string, payload []byte) {
	if !db.shouldUseSegmentChunkBufferSlot() || len(payload) == 0 {
		return
	}
	buf := getDataBuffer(len(payload))
	copy(buf, payload)
	lease := newBufferLease(buf[:len(payload)])
	db.cacheGCChunkLease(folderPath, fileName, lease)
	lease.Release()
}

func (db *PrefixDB) cacheGCChunkLease(folderPath string, fileName string, lease *bufferLease) {
	if !db.shouldUseSegmentChunkBufferSlot() || lease == nil {
		return
	}
	db.currentSegmentChunkBuffer.SetGCLeaseByPath(folderPath, fileName, lease)
}

func (db *PrefixDB) promoteGCChunkBuffersToRead(folderPath string, fileNames []string) {
	if !db.shouldUseSegmentChunkBufferSlot() || len(fileNames) == 0 {
		return
	}
	db.currentSegmentChunkBuffer.PromoteGCEntriesToReadByPath(folderPath, fileNames)
}

func (db *PrefixDB) removeGCChunkBuffers(folderPath string, fileNames []string) {
	if !db.shouldUseSegmentChunkBufferSlot() || len(fileNames) == 0 {
		return
	}
	db.currentSegmentChunkBuffer.RemoveGCEntriesByPath(folderPath, fileNames)
}

func (db *PrefixDB) isReadSegmentChunkCached(folderPath string, fileName string) bool {
	if !db.shouldUseSegmentChunkBufferSlot() {
		return false
	}
	return db.currentSegmentChunkBuffer.IsReadEntryByPath(folderPath, fileName)
}

func (db *PrefixDB) removeCachedSegmentChunkEntries(folderPath string, fileName string) {
	if !db.shouldUseSegmentChunkBufferSlot() {
		return
	}
	db.currentSegmentChunkBuffer.SetByPath(folderPath, fileName, nil)
}

func (db *PrefixDB) syncCommittedChunkBufferReplacements(folderPath string, replacements []committedChunkBufferReplacement) {
	if !db.shouldUseSegmentChunkBufferSlot() || len(replacements) == 0 {
		return
	}
	for _, replacement := range replacements {
		if replacement.oldFileName != "" {
			db.removeCachedSegmentChunkEntries(folderPath, replacement.oldFileName)
		}
		if !replacement.oldWasRead {
			continue
		}
		for _, chunk := range replacement.newChunks {
			if len(chunk.payload) == 0 {
				db.removeCachedSegmentChunkEntries(folderPath, chunk.fileName)
				continue
			}
			db.cacheSegmentChunkBuffer(folderPath, chunk.fileName, chunk.payload)
		}
	}
}

func (db *PrefixDB) readSegmentedChunkBufferToCache(buf []byte, accountKey []byte, storageKey []byte, failure *segmentedStorageReadFailure) ([]byte, *segmentedStorageReadFailure) {
	cache := db.storageCache
	for cursor := len(buf); cursor > 0; {
		if cursor < segmentedChunkEntryHeaderSize {
			failure.reason = "segment-chunk-corrupted"
			return nil, failure
		}
		footer := buf[cursor-segmentedChunkEntryHeaderSize : cursor]
		klen := int(readUint16BE(footer[:2]))
		vlen := int(readUint16BE(footer[2:4]))
		if klen == 0 && vlen == 0 {
			if _, ok := chunkCommitTagBlockID(buf, cursor); !ok {
				failure.reason = "segment-chunk-corrupted"
				return nil, failure
			}
			cursor -= commitTagRecordSize
			continue
		}
		recordDataLen := klen + vlen
		recordStart := cursor - segmentedChunkEntryHeaderSize - recordDataLen
		if recordStart < 0 {
			failure.reason = "segment-chunk-corrupted"
			return nil, failure
		}

		entryBuf := buf[recordStart : cursor-segmentedChunkEntryHeaderSize]
		key := entryBuf[:klen]
		if bytes.Equal(key, storageKey) {
			if vlen == 0 {
				if cache != nil {
					db.addStorageCacheValue(accountKey, storageKey, nil, false)
				}
				failure.reason = "segment-chunk-tombstone"
				return nil, failure
			}
			result := cloneBytes(entryBuf[klen:recordDataLen])
			if cache != nil {
				db.addStorageCacheValue(accountKey, storageKey, result, false)
			}
			return result, nil
		}
		cursor = recordStart
	}
	if cache != nil {
		db.addStorageCacheValue(accountKey, storageKey, nil, false)
	}
	failure.reason = "segment-chunk-key-not-found"
	return nil, failure
}

func isSegmentedStorage(fileID uint32) bool {
	return fileID&segmentedStorageFlag != 0
}

func (db *PrefixDB) invalidateSegmentIndexLayoutForPath(folderPath string) {
	if db.storageIndexCache != nil {
		db.storageIndexCache.RemoveLayoutByPath(folderPath)
	}
}

func (db *PrefixDB) startStorageGCWorker() {
	if db.storageGCQueue != nil {
		return
	}
	db.storageGCQueue = make(chan storageGCJob, storageGCQueueCapacity(db.gcWorkers))
	db.storageGCInFlight = make(map[string]struct{})
	db.storageGCStop = make(chan struct{})
	db.storageGCWait.Add(1)
	go func() {
		defer db.storageGCWait.Done()
		var batchWait sync.WaitGroup
		pending := make(map[string][]storageGCJob)
		active := make(map[string]struct{})
		batchDone := make(chan string, storageGCQueueCapacity(db.gcWorkers))
		launchBatch := func(folderPath string) {
			jobs := pending[folderPath]
			if len(jobs) == 0 {
				return
			}
			if _, exists := active[folderPath]; exists {
				return
			}
			delete(pending, folderPath)
			active[folderPath] = struct{}{}
			batchWait.Add(1)
			go func(path string, jobs []storageGCJob) {
				defer batchWait.Done()
				db.processStorageGCBatch(jobs)
				batchDone <- path
			}(folderPath, jobs)
		}
		launchAllReady := func() {
			for folderPath := range pending {
				launchBatch(folderPath)
			}
		}
		drainQueue := func() {
			for {
				select {
				case job := <-db.storageGCQueue:
					pending[job.folderPath] = append(pending[job.folderPath], job)
				default:
					return
				}
			}
		}
		stopRequested := false
		for {
			if stopRequested && len(active) == 0 {
				launchAllReady()
				if len(active) == 0 && len(pending) == 0 {
					break
				}
			}
			select {
			case job := <-db.storageGCQueue:
				pending[job.folderPath] = append(pending[job.folderPath], job)
				drainQueue()
				launchAllReady()
			case folderPath := <-batchDone:
				delete(active, folderPath)
				launchBatch(folderPath)
			case <-db.storageGCStop:
				stopRequested = true
				drainQueue()
				launchAllReady()
			}
		}
		batchWait.Wait()
	}()
}

func (db *PrefixDB) stopStorageGCWorker() {
	if db.storageGCStop == nil {
		return
	}
	select {
	case <-db.storageGCStop:
	default:
		close(db.storageGCStop)
	}
	db.storageGCWait.Wait()
	db.storageGCStop = nil
	db.storageGCQueue = nil
	db.storageGCInFlight = nil
}

func (db *PrefixDB) isStorageGCIdle() bool {
	queued, inFlight := db.storageGCStatus()
	return queued == 0 && inFlight == 0
}

func (db *PrefixDB) storageGCStatus() (queued int, inFlight int) {
	if db == nil {
		return 0, 0
	}
	if db.storageGCQueue != nil {
		queued = len(db.storageGCQueue)
	}
	db.storageGCMu.Lock()
	inFlight = len(db.storageGCInFlight)
	db.storageGCMu.Unlock()
	return queued, inFlight
}

func (db *PrefixDB) maybeScheduleStorageGC(folderPath string, meta *segmentChunkMeta, backing *bufferLease, chunkData []byte) {
	release := func() {
		if backing != nil {
			backing.Release()
			backing = nil
		}
	}
	if db == nil || meta == nil || meta.FileName == "" {
		release()
		return
	}
	if db.storageGCQueue == nil {
		release()
		return
	}
	// Prefer already-loaded bytes to avoid an extra filesystem stat on the read path.
	chunkSize := 0
	if len(chunkData) > 0 {
		chunkSize = len(chunkData)
	} else if backing != nil {
		chunkSize = len(backing.Bytes())
	}
	if chunkSize == 0 {
		release()
		return
	}
	if chunkSize <= db.segmentedChunkTriggerSize() {
		release()
		return
	}
	job := storageGCJob{folderPath: folderPath, fileName: meta.FileName}
	if len(chunkData) > 0 {
		job.lastTagBlockID, _ = lastChunkCommitTagBlockID(chunkData)
	} else if backing != nil {
		job.lastTagBlockID, _ = lastChunkCommitTagBlockID(backing.Bytes())
	}
	key := job.key()
	db.storageGCMu.Lock()
	if db.storageGCInFlight == nil {
		db.storageGCMu.Unlock()
		release()
		return
	}
	if _, exists := db.storageGCInFlight[key]; exists {
		db.storageGCMu.Unlock()
		release()
		return
	}
	db.storageGCInFlight[key] = struct{}{}
	db.storageGCMu.Unlock()
	if len(chunkData) > 0 {
		job.chunkBuffer = newStorageGCChunkBufferFromBytes(chunkData)
	} else if backing != nil {
		job.chunkBuffer = newStorageGCChunkBufferFromLease(backing)
		backing = nil
	}

	select {
	case db.storageGCQueue <- job:
	default:
		go db.processStorageGCJob(job)
	}
}

func (db *PrefixDB) processStorageGCJob(job storageGCJob) {
	release := db.acquireSharedGCWorker()
	defer release()
	defer db.finishStorageGCJob(job)
	if err := db.runStorageGCJob(job); err != nil {
		fmt.Printf("storage GC failed for folder %s file %s: %v\n", job.folderPath, job.fileName, err)
	}
}

func (db *PrefixDB) processStorageGCBatch(jobs []storageGCJob) {
	if len(jobs) == 0 {
		return
	}
	release := db.acquireSharedGCWorker()
	defer release()
	for i := range jobs {
		job := jobs[i]
		defer db.finishStorageGCJob(job)
	}
	if err := db.runStorageGCBatch(jobs); err != nil {
		// Print one summary line to avoid log spam.
		fmt.Printf("storage GC batch failed for folder %s jobs %d: %v\n", jobs[0].folderPath, len(jobs), err)
	}
}

func collectReplacementChunkFileNames(replacements map[string][]segmentChunkMeta) []string {
	if len(replacements) == 0 {
		return nil
	}
	fileNames := make([]string, 0, len(replacements))
	seen := make(map[string]struct{}, len(replacements))
	for _, metas := range replacements {
		for _, meta := range metas {
			if meta.FileName == "" {
				continue
			}
			if _, ok := seen[meta.FileName]; ok {
				continue
			}
			seen[meta.FileName] = struct{}{}
			fileNames = append(fileNames, meta.FileName)
		}
	}
	return fileNames
}

func (db *PrefixDB) runStorageGCBatch(jobs []storageGCJob) error {
	if len(jobs) == 0 {
		return nil
	}
	folderPath := jobs[0].folderPath
	chunkBuffers := make([]*storageGCChunkBuffer, len(jobs))
	for i := range jobs {
		chunkBuffers[i] = jobs[i].chunkBuffer
	}
	defer func() {
		for i := range chunkBuffers {
			if chunkBuffers[i] != nil {
				chunkBuffers[i].Release()
			}
		}
	}()

	// Phase 1: read index once and rewrite multiple target chunks into new files.
	// Prefer cached index snapshot on GC reads to reduce repetitive disk IO.
	metas, gen0, err := db.readSegmentIndexWithGenByPath(folderPath, true)
	if err != nil {
		return err
	}
	if len(metas) == 0 {
		return nil
	}

	// Deduplicate by fileName within this batch.
	seen := make(map[string]struct{}, len(jobs))
	unique := make([]storageGCJob, 0, len(jobs))
	uniqueIdx := make([]int, 0, len(jobs))
	for i := range jobs {
		j := jobs[i]
		if j.folderPath != folderPath || j.fileName == "" {
			continue
		}
		if _, ok := seen[j.fileName]; ok {
			continue
		}
		seen[j.fileName] = struct{}{}
		unique = append(unique, j)
		uniqueIdx = append(uniqueIdx, i)
	}
	if len(unique) == 0 {
		return nil
	}

	maxOrd := -1
	for i := range metas {
		if ord := parseChunkOrdinal(metas[i].FileName); ord > maxOrd {
			maxOrd = ord
		}
	}
	nextOrd := maxOrd + 1

	// replacements maps old fileName -> new metas (may be nil to delete from index).
	replacements := make(map[string][]segmentChunkMeta, len(unique))
	lruBacked := make(map[string]bool, len(unique))

	for u := range unique {
		job := unique[u]
		// Find the meta for this chunk in the snapshot.
		idx := -1
		for i := range metas {
			if metas[i].FileName == job.fileName {
				idx = i
				break
			}
		}
		if idx == -1 {
			continue
		}
		lruBacked[job.fileName] = db.isReadSegmentChunkCached(folderPath, job.fileName)

		var (
			preloaded      []kvPair
			lastTagBlockID = job.lastTagBlockID
		)
		backingIdx := uniqueIdx[u]
		if chunkBuffers[backingIdx] != nil {
			payload := chunkBuffers[backingIdx].Bytes()
			if lastTagBlockID == 0 {
				lastTagBlockID, _ = lastChunkCommitTagBlockID(payload)
			}
			kvCount, pErr := countChunkEntriesFromTail(payload)
			if pErr == nil {
				entries := borrowStorageEntries(kvCount)
				if decoded, decErr := buildPairsFromChunkBuffer(payload, kvCount, entries); decErr == nil {
					preloaded = decoded
				} else {
					releaseStorageEntries(entries)
				}
			}
		}

		chunkMetas, nextOrd2, err := db.rewriteChunkWithDedupToNewFiles(folderPath, metas[idx], nil, nextOrd, preloaded, nil, lastTagBlockID)
		if preloaded != nil {
			releaseStorageEntries(preloaded)
		}
		if err != nil {
			return err
		}
		nextOrd = nextOrd2
		replacements[job.fileName] = chunkMetas
	}

	if len(replacements) == 0 {
		return nil
	}
	gcFileNames := collectReplacementChunkFileNames(replacements)

	// Phase 2: commit by updating the index once.
	// Re-read metas so we don't clobber concurrent index updates (e.g., another GC job).
	genNow := db.segmentIndexGenerationLocked(folderPath)
	latest := metas
	if genNow != gen0 {
		var latestGen uint64
		latest, latestGen, err = db.readSegmentIndexWithGenByPath(folderPath, true)
		_ = latestGen
		if err != nil {
			return err
		}
	}

	// Build updated index by applying all replacements
	changed := false
	updated := make([]segmentChunkMeta, 0, len(latest))
	for i := range latest {
		if repl, ok := replacements[latest[i].FileName]; ok {
			changed = true
			if len(repl) > 0 {
				updated = append(updated, repl...)
			}
			continue
		}
		updated = append(updated, latest[i])
	}
	if !changed {
		// All targeted chunks disappeared/changed concurrently; new chunks are left as garbage.
		db.removeGCChunkBuffers(folderPath, gcFileNames)
		return nil
	}
	applied, err := db.writeSegmentIndexIncrementalGC(folderPath, latest, replacements)
	if err != nil {
		db.removeGCChunkBuffers(folderPath, gcFileNames)
		return err
	}
	if !applied {
		db.removeGCChunkBuffers(folderPath, gcFileNames)
		return nil
	}
	committed, err := db.readSegmentIndexNoCacheByPath(folderPath)
	if err != nil {
		db.removeGCChunkBuffers(folderPath, gcFileNames)
		return err
	}
	db.refreshSegmentIndexCacheByPath(folderPath, committed)
	for oldFileName, metas := range replacements {
		files := collectReplacementChunkFileNames(map[string][]segmentChunkMeta{oldFileName: metas})
		if lruBacked[oldFileName] {
			db.promoteGCChunkBuffersToRead(folderPath, files)
		} else {
			db.removeGCChunkBuffers(folderPath, files)
		}
	}
	for oldFileName := range replacements {
		db.removeCachedSegmentChunkEntries(folderPath, oldFileName)
	}
	if err := db.refreshManagedFolderStorageCache(folderPath); err != nil {
		return err
	}
	return nil
}

func (db *PrefixDB) finishStorageGCJob(job storageGCJob) {
	db.storageGCMu.Lock()
	if db.storageGCInFlight != nil {
		delete(db.storageGCInFlight, job.key())
	}
	db.storageGCMu.Unlock()
}

func (db *PrefixDB) runStorageGCJob(job storageGCJob) error {
	defer func() {
		if job.chunkBuffer != nil {
			job.chunkBuffer.Release()
		}
	}()
	// Phase 1: build rewritten chunk(s) into NEW files (do not overwrite old fileName).
	// This allows concurrent readers to keep using the old index+old chunk file safely.
	// Prefer cached index snapshot on GC reads to reduce repetitive disk IO.
	metas, gen0, err := db.readSegmentIndexWithGenByPath(job.folderPath, true)
	if err != nil {
		return err
	}
	if len(metas) == 0 {
		return nil
	}
	idx := -1
	for i, meta := range metas {
		if meta.FileName == job.fileName {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil
	}
	folderPath := job.folderPath

	// Start allocating new chunk ordinals after the current max.
	maxOrd := -1
	for i := range metas {
		if ord := parseChunkOrdinal(metas[i].FileName); ord > maxOrd {
			maxOrd = ord
		}
	}
	nextOrd := maxOrd + 1

	var (
		preloaded      []kvPair
		lastTagBlockID = job.lastTagBlockID
	)
	if job.chunkBuffer != nil {
		payload := job.chunkBuffer.Bytes()
		if lastTagBlockID == 0 {
			lastTagBlockID, _ = lastChunkCommitTagBlockID(payload)
		}
		kvCount, err := countChunkEntriesFromTail(payload)
		if err == nil {
			entries := borrowStorageEntries(kvCount)
			if decoded, decErr := buildPairsFromChunkBuffer(payload, kvCount, entries); decErr == nil {
				preloaded = decoded
			} else {
				releaseStorageEntries(entries)
			}
		}
	}

	chunkMetas, nextOrd2, err := db.rewriteChunkWithDedupToNewFiles(folderPath, metas[idx], nil, nextOrd, preloaded, nil, lastTagBlockID)
	if preloaded != nil {
		releaseStorageEntries(preloaded)
	}
	if err != nil {
		return err
	}
	_ = nextOrd2
	gcFileNames := collectReplacementChunkFileNames(map[string][]segmentChunkMeta{job.fileName: chunkMetas})
	oldWasLRU := db.isReadSegmentChunkCached(folderPath, job.fileName)

	// Phase 2: commit by updating the index to point to the new files.
	// Re-read metas so we don't clobber concurrent index updates (e.g., another GC job).
	genNow := db.segmentIndexGenerationLocked(job.folderPath)
	latest := metas
	if genNow != gen0 {
		var latestGen uint64
		latest, latestGen, err = db.readSegmentIndexWithGenByPath(job.folderPath, true)
		_ = latestGen
		if err != nil {
			return err
		}
	}
	idx2 := -1
	for i := range latest {
		if latest[i].FileName == job.fileName {
			idx2 = i
			break
		}
	}
	if idx2 == -1 {
		// Someone else already removed/replaced it; leave the newly written chunks as garbage.
		db.removeGCChunkBuffers(folderPath, gcFileNames)
		return nil
	}
	updated := make([]segmentChunkMeta, 0, len(latest)-1+len(chunkMetas))
	updated = append(updated, latest[:idx2]...)
	if len(chunkMetas) > 0 {
		updated = append(updated, chunkMetas...)
	}
	if idx2+1 < len(latest) {
		updated = append(updated, latest[idx2+1:]...)
	}
	replacements := map[string][]segmentChunkMeta{job.fileName: chunkMetas}
	applied, err := db.writeSegmentIndexIncrementalGC(folderPath, latest, replacements)
	if err != nil {
		db.removeGCChunkBuffers(folderPath, gcFileNames)
		return err
	}
	if !applied {
		db.removeGCChunkBuffers(folderPath, gcFileNames)
		return nil
	}
	committed, err := db.readSegmentIndexNoCacheByPath(folderPath)
	if err != nil {
		db.removeGCChunkBuffers(folderPath, gcFileNames)
		return err
	}
	db.refreshSegmentIndexCacheByPath(job.folderPath, committed)
	if oldWasLRU {
		db.promoteGCChunkBuffersToRead(job.folderPath, gcFileNames)
	} else {
		db.removeGCChunkBuffers(job.folderPath, gcFileNames)
	}
	db.removeCachedSegmentChunkEntries(job.folderPath, job.fileName)
	if err := db.refreshManagedFolderStorageCache(job.folderPath); err != nil {
		return err
	}
	// Option B: do NOT delete the original chunk file. It becomes garbage and can be cleaned later.
	return nil
}

// reserveChunkFileName tries to reserve a unique %04d.dat name by creating the destination
// path with O_EXCL. The created file is a placeholder and will be replaced atomically by writeChunkFile.
func reserveChunkFileName(folderPath string, startOrdinal int) (name string, nextOrdinal int, err error) {
	ord := startOrdinal
	for {
		candidate := chunkFileNameForOrdinal(uint32(ord))
		fullPath := filepath.Join(folderPath, candidate)
		f, openErr := os.OpenFile(fullPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if openErr == nil {
			_ = f.Close()
			return candidate, ord + 1, nil
		}
		if errors.Is(openErr, os.ErrExist) {
			ord++
			continue
		}
		return "", ord, openErr
	}
}

// rewriteChunkWithDedupToNewFiles rewrites a chunk with deduplication and splits by target size,
// writing results into NEW chunk files (never overwriting meta.FileName). It returns the new metas
// and the next suggested ordinal.
func (db *PrefixDB) rewriteChunkWithDedupToNewFiles(folderPath string, meta segmentChunkMeta, additions []kvPair, startOrdinal int, existing []kvPair, backing *bufferLease, lastTagBlockID uint64) ([]segmentChunkMeta, int, error) {
	var err error
	var bytesWritten uint64
	if existing == nil {
		existing, backing, err = db.readSegmentChunkFileWithUsageByPathPreferCache(folderPath, meta.FileName, diskIOUsageStorageGC)
		if err != nil {
			return nil, startOrdinal, err
		}
	}
	if lastTagBlockID == 0 && backing != nil {
		lastTagBlockID, _ = lastChunkCommitTagBlockID(backing.Bytes())
	}
	if backing != nil {
		defer backing.Release()
	}
	if len(existing) > 1 {
		existing = db.maybeNormalizeChunkEntries(existing, &meta)
	}
	merged := mergeAndDedupPairs(existing, additions)
	if len(merged) == 0 {
		// Nothing left; caller should remove from index. Original file is left as garbage.
		addUint64Stat(&db.GCCount, 1)
		return nil, startOrdinal, nil
	}
	chunks := splitEntriesBySize(merged, db.segmentedChunkTargetSize())
	result := make([]segmentChunkMeta, 0, len(chunks))
	ordinal := startOrdinal
	reserved := make([]string, 0, len(chunks))
	defer func() {
		// Best-effort cleanup of placeholders on early error. Any successfully written chunk
		// files or index-less placeholders are safe to leave as garbage.
		if err == nil {
			return
		}
		for _, name := range reserved {
			_ = os.Remove(filepath.Join(folderPath, name))
		}
	}()

	for _, chunk := range chunks {
		name, next, rErr := reserveChunkFileName(folderPath, ordinal)
		if rErr != nil {
			err = rErr
			return nil, startOrdinal, rErr
		}
		reserved = append(reserved, name)
		ordinal = next
		chunkSize, _, wErr := db.writeChunkFileWithUsageAndPayload(folderPath, name, chunk, diskIOUsageStorageGC, false, lastTagBlockID)
		if wErr != nil {
			err = wErr
			return nil, startOrdinal, wErr
		}
		bytesWritten += uint64(chunkSize)
		result = append(result, segmentChunkMeta{
			FileName: name,
			KeyStart: cloneBytes(chunk[0].key),
		})
	}
	addUint64Stat(&db.GCCount, 1)
	addUint64Stat(&db.GCWriteBytes, bytesWritten)
	return result, ordinal, nil
}

func (db *PrefixDB) readSegmentedChunkToCache(fileID uint32, accountKey []byte, storageKey []byte) ([]byte, *segmentedStorageReadFailure) {
	return db.readSegmentedChunkToCacheWithTracker(fileID, accountKey, storageKey, nil)
}

func (db *PrefixDB) readSegmentedChunkToCacheWithTracker(fileID uint32, accountKey []byte, storageKey []byte, tracker *cacheMissCostTracker) ([]byte, *segmentedStorageReadFailure) {
	if !isAccountNamedSegmentedStorage(fileID) {
		return nil, nil
	}
	if len(accountKey) == 0 {
		return nil, nil
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	val, failure, err := db.readSegmentedChunkToCacheByPathWithTracker(folderPath, accountKey, storageKey, tracker)
	if err != nil {
		if shouldFallbackMissingFolderRead(err) {
			db.clearAccountStorageFolder(accountKey)
		}
		return nil, failure
	}
	if val != nil {
		db.markAccountStorageFolder(accountKey)
	}
	return val, failure
}

func (db *PrefixDB) readSegmentedChunkToCacheByPath(folderPath string, accountKey []byte, storageKey []byte) ([]byte, *segmentedStorageReadFailure, error) {
	return db.readSegmentedChunkToCacheByPathWithTracker(folderPath, accountKey, storageKey, nil)
}

func (db *PrefixDB) readSegmentedChunkToCacheByPathWithTracker(folderPath string, accountKey []byte, storageKey []byte, tracker *cacheMissCostTracker) ([]byte, *segmentedStorageReadFailure, error) {
	entryLock, unlock := db.lockSegmentIndexFolderReadEntry(folderPath)
	var gcMeta *segmentChunkMeta
	var gcBacking *bufferLease
	var gcChunkData []byte
	chunkFromCache := false
	defer func() {
		unlock()
		if gcMeta != nil {
			db.maybeScheduleStorageGC(folderPath, gcMeta, gcBacking, gcChunkData)
			gcBacking = nil
			gcChunkData = nil
		}
	}()
	failure := &segmentedStorageReadFailure{folderPath: folderPath, indexFile: segmentIndexFileName}
	indexStart := time.Now()
	metas, segmentIndexSource, err := db.readSegmentIndexForKeyByPathWithSourceAndTrackerLocked(folderPath, storageKey, tracker, entryLock)
	if len(metas) > 0 {
		duration := time.Since(indexStart)
		recordTrieStorageGetBreakdownStep(&db.trieStorageSegmentIndexStats, segmentIndexSource.fromCache(), duration)
		recordTrieStorageSegmentIndexLayer(segmentIndexSource, duration, &db.trieStorageSegmentIndexLayerStats)
	}
	if err != nil {
		if errors.Is(err, errSegmentIndexEntryNotFound) {
			failure.reason = "segment-index-entry-not-found"
			return nil, failure, nil
		}
		failure.reason = "segment-index-read-failed"
		return nil, failure, err
	}
	if len(metas) == 0 {
		failure.reason = "segment-index-empty"
		return nil, failure, nil
	}
	meta := selectSegmentChunkMeta(metas, storageKey)
	if meta == nil {
		failure.reason = "segment-chunk-meta-not-found"
		// Log detailed information for debugging
		fmt.Fprintf(prefixdbLogWriter, "prefixdb ERROR: failed to locate chunk for storage key - account=%x storage=%x folder=%s metas_count=%d\n",
			accountKey, storageKey, folderPath, len(metas))
		// Print key ranges for all metas
		for i, m := range metas {
			fmt.Fprintf(prefixdbLogWriter, "prefixdb DEBUG: chunk[%d] file=%s KeyStart=%x\n",
				i, m.FileName, m.KeyStart)
		}
		return nil, failure, nil
	}
	failure.chunkFile = meta.FileName
	if db.testSegmentedReadHook != nil {
		db.testSegmentedReadHook(folderPath, *meta)
	}
	chunkStart := time.Now()
	defer func() {
		recordTrieStorageGetBreakdownStep(&db.trieStorageKVStats, chunkFromCache, time.Since(chunkStart))
	}()
	gcMeta = meta
	if cachedLease, ok := db.getCachedSegmentChunkLease(folderPath, meta.FileName); ok {
		chunkFromCache = true
		gcBacking = cachedLease
		cachedBuf := cachedLease.Bytes()
		value, readFailure := db.readSegmentedChunkBufferToCache(cachedBuf, accountKey, storageKey, failure)
		return value, readFailure, nil
	}
	value, readFailure, backing, err := db.readSegmentedChunkToCacheStreamingByPathWithTracker(folderPath, meta.FileName, accountKey, storageKey, failure, tracker)
	gcBacking = backing
	return value, readFailure, err
}

func (db *PrefixDB) MigrateLegacySegmentIndexFormats() error {
	return errors.New("legacy segment index formats are no longer supported")
}

func (db *PrefixDB) rebuildSegmentIndexFilesLocked() error {
	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		folderPath := filepath.Join(db.storageDir, entry.Name())
		indexPath := filepath.Join(folderPath, segmentIndexFileName)
		if _, err := os.Stat(indexPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		lockEntry, unlock := db.lockSegmentIndexFolderEntry(folderPath)
		metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
		if err != nil {
			unlock()
			return err
		}
		if err := db.writeSegmentIndexLocked(folderPath, metas, lockEntry); err != nil {
			unlock()
			return err
		}
		db.refreshSegmentIndexCacheByPathLocked(folderPath, metas)
		unlock()
	}
	return nil
}

func (db *PrefixDB) RebuildSegmentIndexFiles() error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	return db.rebuildSegmentIndexFilesLocked()
}

func (db *PrefixDB) UpgradeSegmentIndexFiles() error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	return db.rebuildSegmentIndexFilesLocked()
}

// GCCollectGarbageChunks removes chunk files that are not referenced by the current
// segment index for the given folderID.
//
// This is an explicit, offline-style cleanup helper for the "Option B" GC strategy
// where old chunk files are intentionally left behind as garbage.
// It does not modify the index and serializes only with operations on the same folder.
func (db *PrefixDB) GCCollectGarbageChunks(folderID uint32) (int, error) {
	if db == nil || folderID == 0 {
		return 0, nil
	}
	folderPath := db.segmentedFolderPath(folderID)

	_, unlock := db.lockSegmentIndexFolderEntry(folderPath)
	defer unlock()

	metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
	if err != nil {
		return 0, err
	}
	referenced := make(map[string]struct{}, len(metas))
	for i := range metas {
		if metas[i].FileName != "" {
			referenced[metas[i].FileName] = struct{}{}
		}
	}

	entries, err := os.ReadDir(folderPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}

	deleted := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		fullPath := filepath.Join(folderPath, name)

		// Remove leftover temp files from atomic chunk writes.
		if strings.HasSuffix(name, ".dat.tmp") {
			if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return deleted, err
			}
			deleted++
			continue
		}

		// Only consider *.dat files.
		if !strings.HasSuffix(name, ".dat") {
			continue
		}
		if _, ok := referenced[name]; ok {
			continue
		}
		if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return deleted, err
		}
		deleted++
	}

	return deleted, nil
}

// GCAllStorageChunkFiles runs a full sweep GC for all segmented storage chunk files.
// It rewrites every chunk file with deduplication and splits by target chunk size,
// then updates index metadata for each segmented folder.
func (db *PrefixDB) GCAllStorageChunkFiles() error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	fmt.Println("start GC for all segmented storage chunk files")

	entries, err := os.ReadDir(db.storageDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		folderPath := filepath.Join(db.storageDir, entry.Name())
		lockEntry, unlock := db.lockSegmentIndexFolderEntry(folderPath)

		metas, err := db.readSegmentIndexNoCacheByPathLocked(folderPath)
		if err != nil {
			unlock()
			return err
		}
		if len(metas) == 0 {
			unlock()
			continue
		}
		type taggedKVPair struct {
			pair      kvPair
			commitTag uint64
		}
		type taggedChunk struct {
			entries   []kvPair
			commitTag uint64
		}
		allEntries := make([]taggedKVPair, 0)
		for _, meta := range metas {
			entries, backing, err := db.readSegmentChunkFileWithUsageByPath(folderPath, meta.FileName, diskIOUsageStorageGC)
			if err != nil {
				unlock()
				return err
			}
			var sourceTagBlockID uint64
			if backing != nil {
				if tagBlockID, ok := lastChunkCommitTagBlockID(backing.Bytes()); ok {
					sourceTagBlockID = tagBlockID
				}
			}
			for _, entry := range entries {
				keyCopy := append([]byte(nil), entry.key...)
				var valCopy []byte
				if entry.val != nil {
					valCopy = append([]byte(nil), entry.val...)
				}
				allEntries = append(allEntries, taggedKVPair{pair: kvPair{key: keyCopy, val: valCopy}, commitTag: sourceTagBlockID})
			}
			if backing != nil {
				backing.Release()
			}
		}

		if len(allEntries) > 1 {
			sort.SliceStable(allEntries, func(i, j int) bool {
				return bytes.Compare(allEntries[i].pair.key, allEntries[j].pair.key) < 0
			})
			out := allEntries[:0]
			for i := 0; i < len(allEntries); {
				j := i + 1
				for j < len(allEntries) && bytes.Equal(allEntries[j].pair.key, allEntries[i].pair.key) {
					j++
				}
				out = append(out, allEntries[j-1])
				i = j
			}
			allEntries = out
		}

		updated := make([]segmentChunkMeta, 0, len(metas))
		keep := make(map[string]struct{})
		if len(allEntries) > 0 {
			chunks := make([]taggedChunk, 0, len(allEntries)/64+1)
			limit := db.segmentedChunkTargetSize()
			current := make([]kvPair, 0)
			currentSize := 0
			var currentTag uint64
			flushChunk := func() {
				if len(current) == 0 {
					return
				}
				chunks = append(chunks, taggedChunk{entries: current, commitTag: currentTag})
				current = nil
				currentSize = 0
				currentTag = 0
			}
			for _, entry := range allEntries {
				entrySize := segmentedChunkEntryHeaderSize + len(entry.pair.key) + len(entry.pair.val)
				if currentSize+entrySize > limit && len(current) > 0 {
					flushChunk()
				}
				current = append(current, entry.pair)
				currentSize += entrySize
				if entry.commitTag > currentTag {
					currentTag = entry.commitTag
				}
			}
			flushChunk()
			for i, chunk := range chunks {
				fileName := chunkFileNameForOrdinal(uint32(i))
				_, _, err := db.writeChunkFileWithUsageAndPayload(folderPath, fileName, chunk.entries, diskIOUsageStorageGC, false, chunk.commitTag)
				if err != nil {
					unlock()
					return err
				}
				updated = append(updated, segmentChunkMeta{
					FileName: fileName,
					KeyStart: cloneBytes(chunk.entries[0].key),
				})
				keep[fileName] = struct{}{}
			}
		}

		for _, meta := range metas {
			if _, ok := keep[meta.FileName]; ok {
				continue
			}
			fullPath := filepath.Join(folderPath, meta.FileName)
			if err := os.Remove(fullPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				unlock()
				return err
			}
		}

		if err := db.writeSegmentIndexLocked(folderPath, updated, lockEntry); err != nil {
			unlock()
			return err
		}
		db.refreshSegmentIndexCacheByPathLocked(folderPath, updated)
		if accountKey, ok := db.managedAccountKeyForFolderPath(folderPath); ok {
			plainEntries := make([]kvPair, 0, len(allEntries))
			for _, entry := range allEntries {
				plainEntries = append(plainEntries, entry.pair)
			}
			db.syncStorageCacheEntries(accountKey, plainEntries)
		}
		unlock()
	}
	fmt.Println("Completed GC for all segmented storage chunk files")
	return nil
}

func (db *PrefixDB) GCPrefixTree() error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	if count := db.prefixTree.GC(); count >= 0 {
		return nil
	}
	return fmt.Errorf("prefix tree GC failed")
}

// RunPostLoadGC performs the full compaction steps expected after bulk load.
// It always sweeps all node files with unsorted data and all segmented storage folders.
func (db *PrefixDB) RunPostLoadGC() error {
	db.writeMutex.Lock()
	if count := db.prefixTree.CompactAllNodeFiles(); count < 0 {
		db.writeMutex.Unlock()
		return fmt.Errorf("prefix tree GC failed")
	}
	db.writeMutex.Unlock()
	if err := db.GCAllStorageChunkFiles(); err != nil {
		return err
	}
	return nil
}
