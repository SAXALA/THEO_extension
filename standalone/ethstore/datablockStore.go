package ethstore

import (
	"bufio"
	"bytes" // Added bytes import
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort" // Add this import
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// Added testing import
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/huandu/skiplist" // Using a third-party skiplist library
)

const (
	BlockdataFileName      = "headerdata.log"
	BlockindexMapFileName  = "blockindex.map"
	BlockindexMetaFileName = "blockindex.meta"
	defaultRecentN         = 6000 // Default number of recent blocks to keep indexed in memory
	offsetSize             = 8    // Size of uint64 for offsets
	blockIDSize            = 8    // Assuming block ID is uint64
	keyLenSize             = 4    // Size of uint32 for key length
	valueLenSize           = 4    // Size of uint32 for value length
	indexEntrySize         = blockIDSize + offsetSize + offsetSize
	indexMetaSize          = 16 // minBlockID (8 bytes) + maxBlockID (8 bytes)
	// TombstoneMarker is a special value to mark deletion
	TombstoneMarker   = "_D_"
	initialBufferSize = 4096  // Initial buffer size for writers
	IgnoredThreshold  = 10000 // Threshold for ignoring very old blocks in Get (e.g., if blockID is more than 10k behind latestBlockID, treat as non-existent)
)

type baolDiskIOUsage uint8

const (
	baolDiskIOUsageBootstrap baolDiskIOUsage = iota
	baolDiskIOUsageDataQuery
	baolDiskIOUsageDataMutation
	baolDiskIOUsageIndexLookup
	baolDiskIOUsageIndexMutation
	baolDiskIOUsageIndexMeta
	baolDiskIOUsageCount
)

var baolDiskIOUsageNames = [...]string{
	"bootstrap",
	"data-query",
	"data-mutation",
	"index-lookup",
	"index-mutation",
	"index-meta",
}

var errRequestedFutureBlock = errors.New("requested future block")

type baolDiskIOCounters struct {
	readOps    uint64
	readBytes  uint64
	writeOps   uint64
	writeBytes uint64
}

type baolGetBreakdownStepStats struct {
	cacheCount   uint64
	cacheNanos   uint64
	noCacheCount uint64
	noCacheNanos uint64
}

// BlockAppendOnlyLog implements the append-only log store with skiplist indexing for recent blocks.
type BlockAppendOnlyLog struct {
	dirPath string
	log     log.Logger

	dataFilePath  string
	dataFile      *os.File
	dataWriter    *bufio.Writer
	currentOffset int64 // Current end offset of the data file

	indexMapFilePath  string
	indexMapFile      *os.File
	indexMetaFilePath string
	blockIndex        map[uint64]blockIndexEntry // In-memory cache of block offsets
	minBlockID        uint64                     // Minimum block ID in the log (0 means empty)
	latestBlockID     uint64

	// Skiplist index for recent N blocks
	recentN          int
	recentBlocks     []uint64            // Ordered list of recent block IDs (most recent last)
	skiplistIndex    *skiplist.SkipList  // Key: string (key), Value: *kvPointer
	indexedBlocks    map[uint64]struct{} // Set of block IDs currently in the skiplist
	indexedBlockKeys map[uint64][]string // Keys contributed by each indexed block

	indexBuffer      []blockIndexEntry // Buffer for batching index writes
	indexBufferMu    sync.Mutex        // Mutex for index buffer
	indexBufferSize  int               // Size threshold for flushing index buffer
	indexBufferFlush chan struct{}     // Channel to signal index buffer flush
	backgroundDone   chan struct{}     // Channel to signal background flush goroutine exit

	mu     sync.RWMutex
	closed bool

	diskIOStats [baolDiskIOUsageCount]baolDiskIOCounters

	getInMemoryIndexStats baolGetBreakdownStepStats
	getDiskIndexStats     baolGetBreakdownStepStats
	getDiskDataStats      baolGetBreakdownStepStats

	iteratorMemoryIndexStats baolGetBreakdownStepStats
	iteratorBlockIndexStats  baolGetBreakdownStepStats
	iteratorKeyReadStats     baolGetBreakdownStepStats
	iteratorValueReadStats   baolGetBreakdownStepStats

	// opCount   uint64 // for debugging
	// failedOps uint64 // for debugging
}

// NewAppendOnlyLog creates or opens an append-only log store.
func NewBlockAppendOnlyLog(dirPath string, recentN int, logger log.Logger) (*BlockAppendOnlyLog, error) {
	if recentN <= 0 {
		recentN = defaultRecentN
	}
	if logger == nil {
		logger = log.New() // Use a default logger if none provided
	}

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}

	dataFilePath := filepath.Join(dirPath, BlockdataFileName)
	indexMapFilePath := filepath.Join(dirPath, BlockindexMapFileName)
	indexMetaFilePath := filepath.Join(dirPath, BlockindexMetaFileName)
	// Open data file for appending
	dataFile, err := os.OpenFile(dataFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file %s: %w", dataFilePath, err)
	}
	fi, err := dataFile.Stat()
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to stat data file %s: %w", dataFilePath, err)
	}
	currentOffset := fi.Size()

	// Open index map file for reading/writing
	indexMapFile, err := os.OpenFile(indexMapFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to open index map file %s: %w", indexMapFilePath, err)
	}

	baol := &BlockAppendOnlyLog{
		dirPath:           dirPath,
		log:               logger.New("module", "appendlog", "path", dirPath),
		dataFilePath:      dataFilePath,
		dataFile:          dataFile,
		dataWriter:        bufio.NewWriterSize(dataFile, initialBufferSize),
		currentOffset:     currentOffset,
		indexMapFilePath:  indexMapFilePath,
		indexMapFile:      indexMapFile,
		indexMetaFilePath: indexMetaFilePath,
		blockIndex:        make(map[uint64]blockIndexEntry),
		minBlockID:        0,
		recentN:           recentN,
		recentBlocks:      make([]uint64, 0, recentN), // Initialize empty, will be populated below
		skiplistIndex:     skiplist.New(skiplist.String),
		indexedBlocks:     make(map[uint64]struct{}), // Initialize empty, will be populated below
		indexedBlockKeys:  make(map[uint64][]string),
		indexBuffer:       make([]blockIndexEntry, 0, recentN/2),
		indexBufferSize:   recentN / 2,
		indexBufferFlush:  make(chan struct{}, 1),
		backgroundDone:    make(chan struct{}),

		// opCount:   0,
		// failedOps: 0,
	}

	go baol.backgroundFlush()

	// Load index metadata (minBlockID, maxBlockID)
	if err := baol.loadIndexMeta(); err != nil {
		baol.Close()
		return nil, fmt.Errorf("failed to load index metadata: %w", err)
	}

	// Load existing block index map
	if err := baol.loadBlockIndex(); err != nil {
		baol.Close()
		return nil, fmt.Errorf("failed to load block index: %w", err)
	}

	// Determine the actual N most recent blocks from all loaded blockIndex entries.
	allLoadedBlockIDs := make([]uint64, 0, len(baol.blockIndex))
	for id := range baol.blockIndex {
		allLoadedBlockIDs = append(allLoadedBlockIDs, id)
	}
	sort.Slice(allLoadedBlockIDs, func(i, j int) bool {
		return allLoadedBlockIDs[i] < allLoadedBlockIDs[j] // Sort oldest to newest
	})

	// Populate recentBlocks and indexedBlocks with the true N most recent blocks.
	// aol.recentBlocks and aol.indexedBlocks were already initialized as empty.
	startIdx := 0
	if len(allLoadedBlockIDs) > baol.recentN {
		startIdx = len(allLoadedBlockIDs) - baol.recentN
	}

	for i := startIdx; i < len(allLoadedBlockIDs); i++ {
		blockID := allLoadedBlockIDs[i]
		baol.recentBlocks = append(baol.recentBlocks, blockID) // These are the N most recent, oldest of N to newest of N
		baol.indexedBlocks[blockID] = struct{}{}
	}
	// aol.recentBlocks is now correctly populated and sorted (oldest of recentN to newest of recentN).
	// aol.indexedBlocks now correctly reflects this set.

	// Rebuild skiplist for the last N blocks from the index
	// This call will now use the correctly populated aol.recentBlocks and aol.indexedBlocks.
	if err := baol.rebuildSkiplist(); err != nil {
		baol.Close()
		return nil, fmt.Errorf("failed to rebuild skiplist index: %w", err)
	}

	baol.log.Info("AppendOnlyLog initialized", "dataSize", common.StorageSize(currentOffset),
		"indexedBlocks", len(baol.indexedBlocks), "recentBlocksTracked", len(baol.recentBlocks))
	baol.releaseBootstrapResources()
	return baol, nil
}

func (baol *BlockAppendOnlyLog) releaseBootstrapResources() {
	// Release blockIndex - queries will fall back to disk via getBlockIndexEntry()
	baol.blockIndex = nil

	// Keep indexedBlocks - it's small (just block IDs) and needed for fast skiplist lookup
	// baol.indexedBlocks is retained

	// Release recentBlocks and indexedBlockKeys - will be rebuilt on write if needed
	baol.recentBlocks = nil
	baol.indexedBlockKeys = nil

	baol.log.Info("Released bootstrap resources",
		"skiplistKeys", baol.skiplistIndex.Len(),
		"indexedBlocks", len(baol.indexedBlocks))
}

func (baol *BlockAppendOnlyLog) ensureWriteTrackingInitialized() {
	// indexedBlocks is preserved after releaseBootstrapResources()
	// Only rebuild recentBlocks and indexedBlockKeys if needed
	if baol.recentBlocks != nil && baol.indexedBlockKeys != nil {
		return
	}

	blockSet := make(map[uint64]struct{})
	blockKeys := make(map[uint64][]string)
	for e := baol.skiplistIndex.Front(); e != nil; e = e.Next() {
		ptr := e.Value.(*kvPointer)
		blockSet[ptr.BlockID] = struct{}{}
		key := e.Key().(string)
		blockKeys[ptr.BlockID] = append(blockKeys[ptr.BlockID], key)
	}

	blockIDs := make([]uint64, 0, len(blockSet))
	for id := range blockSet {
		blockIDs = append(blockIDs, id)
	}
	sort.Slice(blockIDs, func(i, j int) bool { return blockIDs[i] < blockIDs[j] })
	if len(blockIDs) > baol.recentN {
		blockIDs = blockIDs[len(blockIDs)-baol.recentN:]
	}

	baol.recentBlocks = make([]uint64, len(blockIDs))
	copy(baol.recentBlocks, blockIDs)

	// Only rebuild indexedBlocks if it was actually released (nil)
	if baol.indexedBlocks == nil {
		baol.indexedBlocks = make(map[uint64]struct{}, len(blockIDs))
		for _, id := range blockIDs {
			baol.indexedBlocks[id] = struct{}{}
		}
	}

	baol.indexedBlockKeys = blockKeys
}

func (baol *BlockAppendOnlyLog) findBlockIndexEntryOnDisk(blockID uint64) (blockIndexEntry, bool, error) {
	// Check if blockID is in valid range
	if blockID < baol.minBlockID {
		return blockIndexEntry{}, false, nil
	}

	f := baol.indexMapFile
	if f == nil {
		return blockIndexEntry{}, false, ErrClosed
	}

	buf := make([]byte, indexEntrySize)
	// Use compact storage: position based on offset from minBlockID
	pos := int64(blockID-baol.minBlockID) * int64(indexEntrySize)
	n, readErr := f.ReadAt(buf, pos)
	baol.addDiskRead(baolDiskIOUsageIndexLookup, n)
	if readErr != nil {
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			return blockIndexEntry{}, false, nil
		}
		return blockIndexEntry{}, false, readErr
	}
	if n != len(buf) {
		return blockIndexEntry{}, false, nil
	}

	entry := blockIndexEntry{
		BlockID:     binary.BigEndian.Uint64(buf[0:blockIDSize]),
		StartOffset: int64(binary.BigEndian.Uint64(buf[blockIDSize : blockIDSize+offsetSize])),
		EndOffset:   int64(binary.BigEndian.Uint64(buf[blockIDSize+offsetSize:])),
	}

	if entry.BlockID != blockID {
		return blockIndexEntry{}, false, nil
	}
	if entry.BlockID == 0 && entry.StartOffset == 0 && entry.EndOffset == 0 {
		return blockIndexEntry{}, false, nil
	}
	if entry.EndOffset < entry.StartOffset {
		return blockIndexEntry{}, false, nil
	}

	return entry, true, nil
}

// Path returns the data directory of the append-only log.
func (baol *BlockAppendOnlyLog) Path() string {
	return baol.dirPath
}

// RecentN returns the number of recent blocks indexed in the skiplist.
func (baol *BlockAppendOnlyLog) RecentN() int {
	return baol.recentN
}

// loadIndexMeta loads the minimum and maximum block IDs from the metadata file.
func (baol *BlockAppendOnlyLog) loadIndexMeta() error {
	file, err := os.Open(baol.indexMetaFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			// No metadata file yet, this is a new log
			baol.minBlockID = 0
			return nil
		}
		return fmt.Errorf("failed to open index meta file: %w", err)
	}
	defer file.Close()

	buf := make([]byte, indexMetaSize)
	n, err := io.ReadFull(file, buf)
	baol.addDiskRead(baolDiskIOUsageIndexMeta, n)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Empty or incomplete file, treat as new
			baol.minBlockID = 0
			return nil
		}
		return fmt.Errorf("failed to read index meta: %w", err)
	}
	if n != indexMetaSize {
		baol.minBlockID = 0
		return nil
	}

	baol.minBlockID = binary.BigEndian.Uint64(buf[0:8])
	// maxBlockID is stored but not loaded into memory since latestBlockID will be updated from index
	baol.log.Debug("Loaded index metadata", "minBlockID", baol.minBlockID)
	return nil
}

// saveIndexMeta saves the minimum and maximum block IDs to the metadata file.
func (baol *BlockAppendOnlyLog) saveIndexMeta() error {
	file, err := os.OpenFile(baol.indexMetaFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open index meta file for writing: %w", err)
	}
	defer file.Close()

	buf := make([]byte, indexMetaSize)
	binary.BigEndian.PutUint64(buf[0:8], baol.minBlockID)
	binary.BigEndian.PutUint64(buf[8:16], baol.latestBlockID)

	if _, err := file.Write(buf); err != nil {
		return fmt.Errorf("failed to write index meta: %w", err)
	}
	baol.addDiskWrite(baolDiskIOUsageIndexMeta, len(buf))

	baol.log.Debug("Saved index metadata", "minBlockID", baol.minBlockID, "maxBlockID", baol.latestBlockID)
	return nil
}

func (baol *BlockAppendOnlyLog) addDiskRead(usage baolDiskIOUsage, n int) {
	if baol == nil || usage >= baolDiskIOUsageCount {
		return
	}
	atomic.AddUint64(&baol.diskIOStats[usage].readOps, 1)
	if n > 0 {
		atomic.AddUint64(&baol.diskIOStats[usage].readBytes, uint64(n))
	}
}

func (baol *BlockAppendOnlyLog) addDiskWrite(usage baolDiskIOUsage, n int) {
	if baol == nil || usage >= baolDiskIOUsageCount {
		return
	}
	atomic.AddUint64(&baol.diskIOStats[usage].writeOps, 1)
	if n > 0 {
		atomic.AddUint64(&baol.diskIOStats[usage].writeBytes, uint64(n))
	}
}

func (baol *BlockAppendOnlyLog) readAtWithStats(file *os.File, buf []byte, offset int64, usage baolDiskIOUsage) (int, error) {
	n, err := file.ReadAt(buf, offset)
	baol.addDiskRead(usage, n)
	return n, err
}

func (baol *BlockAppendOnlyLog) writeAtWithStats(file *os.File, buf []byte, offset int64, usage baolDiskIOUsage) (int, error) {
	n, err := file.WriteAt(buf, offset)
	baol.addDiskWrite(usage, n)
	return n, err
}

func (baol *BlockAppendOnlyLog) printDiskIOStats() {
	if baol == nil {
		return
	}
	var totalReadOps, totalReadBytes, totalWriteOps, totalWriteBytes uint64
	for usage := baolDiskIOUsage(0); usage < baolDiskIOUsageCount; usage++ {
		stats := &baol.diskIOStats[usage]
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
		fmt.Printf("BlockStore disk IO stats [%s]: readOps=%d readBytes=%d writeOps=%d writeBytes=%d\n",
			baolDiskIOUsageNames[usage], readOps, readBytes, writeOps, writeBytes,
		)
	}
	fmt.Printf("BlockStore disk IO stats [total]: readOps=%d readBytes=%d writeOps=%d writeBytes=%d\n",
		totalReadOps, totalReadBytes, totalWriteOps, totalWriteBytes,
	)
}

func (baol *BlockAppendOnlyLog) BootstrapReadBytes() uint64 {
	if baol == nil {
		return 0
	}
	return atomic.LoadUint64(&baol.diskIOStats[baolDiskIOUsageBootstrap].readBytes)
}

func recordBAOLGetBreakdownStep(stats *baolGetBreakdownStepStats, fromCache bool, duration time.Duration) {
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

func printBAOLGetBreakdownStep(label string, stats *baolGetBreakdownStepStats) {
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
	fmt.Printf("BlockStore get breakdown [%s]: cacheCount=%d cacheTotal=%s cacheAvg=%0.2fus noCacheCount=%d noCacheTotal=%s noCacheAvg=%0.2fus\n",
		label,
		cacheCount,
		time.Duration(cacheNanos),
		cacheAvgMicros,
		noCacheCount,
		time.Duration(noCacheNanos),
		noCacheAvgMicros,
	)
}

func baolBreakdownTotalNanos(stats ...*baolGetBreakdownStepStats) uint64 {
	if !analysisStatsEnabled {
		return 0
	}
	var total uint64
	for _, stat := range stats {
		if stat == nil {
			continue
		}
		total += atomic.LoadUint64(&stat.cacheNanos)
		total += atomic.LoadUint64(&stat.noCacheNanos)
	}
	return total
}

func printBAOLBreakdownSummary(label string, parts map[string]*baolGetBreakdownStepStats) {
	if !analysisStatsEnabled || len(parts) == 0 {
		return
	}
	totalNanos := baolBreakdownTotalNanos(func() []*baolGetBreakdownStepStats {
		stats := make([]*baolGetBreakdownStepStats, 0, len(parts))
		for _, stat := range parts {
			stats = append(stats, stat)
		}
		return stats
	}()...)
	if totalNanos == 0 {
		fmt.Printf("BlockStore %s summary: total=0s\n", label)
		return
	}

	orderedLabels := make([]string, 0, len(parts))
	for name := range parts {
		orderedLabels = append(orderedLabels, name)
	}
	sort.Strings(orderedLabels)

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("BlockStore %s summary: total=%s", label, time.Duration(totalNanos)))
	for _, name := range orderedLabels {
		stat := parts[name]
		partNanos := atomic.LoadUint64(&stat.cacheNanos) + atomic.LoadUint64(&stat.noCacheNanos)
		share := 0.0
		if totalNanos > 0 {
			share = float64(partNanos) * 100 / float64(totalNanos)
		}
		builder.WriteString(fmt.Sprintf(" %s=%s(%.2f%%)", name, time.Duration(partNanos), share))
	}
	fmt.Println(builder.String())
}

// loadBlockIndex reads the index map file into memory using compact storage based on minBlockID.
func (baol *BlockAppendOnlyLog) loadBlockIndex() error {
	baol.indexMapFile.Seek(0, io.SeekStart)
	reader := bufio.NewReader(baol.indexMapFile)
	buf := make([]byte, indexEntrySize)
	latestBlock := uint64(0)
	inferredMin := uint64(0)
	slotIndex := uint64(0)
	verifyCompactSlots := baol.minBlockID != 0

	for {
		n, err := io.ReadFull(reader, buf)
		baol.addDiskRead(baolDiskIOUsageBootstrap, n)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("error reading index map file: %w", err)
		}
		if n != len(buf) {
			baol.log.Warn("Incomplete record found in index map file", "bytesRead", n)
			break
		}

		entry := blockIndexEntry{
			BlockID:     binary.BigEndian.Uint64(buf[0:blockIDSize]),
			StartOffset: int64(binary.BigEndian.Uint64(buf[blockIDSize : blockIDSize+offsetSize])),
			EndOffset:   int64(binary.BigEndian.Uint64(buf[blockIDSize+offsetSize:])),
		}

		// Calculate expected blockID based on slot position and minBlockID
		expectedBlockID := baol.minBlockID + slotIndex
		slotIndex++

		// Skip invalid entries.
		// Keep blockID==0 entries when they have a valid offset range.
		if entry.EndOffset < entry.StartOffset {
			continue
		}
		if entry.BlockID == 0 && entry.StartOffset == 0 && entry.EndOffset == 0 {
			continue
		}

		// Verify blockID matches expected position when metadata provides minBlockID.
		if verifyCompactSlots && entry.BlockID != expectedBlockID {
			baol.log.Warn("Block ID mismatch in index file",
				"expected", expectedBlockID, "got", entry.BlockID, "slot", slotIndex-1)
			continue
		}

		if inferredMin == 0 || entry.BlockID < inferredMin {
			inferredMin = entry.BlockID
		}

		baol.blockIndex[entry.BlockID] = entry
		if entry.BlockID > latestBlock {
			latestBlock = entry.BlockID
		}
	}

	if baol.minBlockID == 0 && inferredMin > 0 {
		baol.minBlockID = inferredMin
	}
	baol.latestBlockID = latestBlock
	baol.log.Debug("Loaded block index", "entries", len(baol.blockIndex),
		"minBlockID", baol.minBlockID, "latestBlockID", baol.latestBlockID)
	return nil
}

// rebuildSkiplist populates the skiplist index for the actual N most recent blocks found.
func (baol *BlockAppendOnlyLog) rebuildSkiplist() error {
	// aol.mu must be held (WLock) by the caller if called outside of NewAppendOnlyLog initialization.
	// NewAppendOnlyLog calls this before aol is accessible externally, so no concurrent access.
	// updateRecentBlocks calls this under a WLock.

	// The source of truth for which blocks should be in the skiplist is aol.recentBlocks.
	// We just need to iterate over them (oldest to newest) and populate the skiplist.

	// Create a sorted copy of aol.recentBlocks (oldest to newest) to iterate over.
	// This ensures that if a key appears in multiple recent blocks, the one from the
	// newest block (processed last) will be what ends up in the skiplist.
	blocksToIndex := make([]uint64, len(baol.recentBlocks))
	copy(blocksToIndex, baol.recentBlocks)
	sort.Slice(blocksToIndex, func(i, j int) bool {
		return blocksToIndex[i] < blocksToIndex[j] // Sort oldest to newest
	})

	// oldSkiplist := aol.skiplistIndex
	baol.skiplistIndex = skiplist.New(skiplist.String)
	baol.indexedBlockKeys = make(map[uint64][]string)

	baol.log.Debug("Rebuilding skiplist index", "blocksToScan", blocksToIndex)

	newIndexBlocks := make(map[uint64]struct{})

	for _, blockID := range blocksToIndex {
		// Ensure this block is still supposed to be indexed.
		// This check is somewhat redundant if blocksToIndex is derived from aol.recentBlocks
		// and aol.indexedBlocks is kept in sync, but good for safety.
		if _, stillIndexed := baol.indexedBlocks[blockID]; !stillIndexed {
			baol.log.Warn("Block in blocksToIndex for rebuild is no longer in baol.indexedBlocks", "blockID", blockID)
			continue
		}

		indexEntry, ok := baol.getBlockIndexEntry(blockID)
		if !ok {
			baol.log.Error("Block ID from recentBlocks list not found in main blockIndex during skiplist rebuild", "blockID", blockID)
			// This indicates a serious inconsistency.
			// Depending on desired robustness, could return error or try to continue.
			return fmt.Errorf("inconsistency: block %d in recentBlocks not in blockIndex", blockID)
		}

		// Read all entries for this block and add to skiplist
		if err := baol.readAndIndexBlockFrom(indexEntry); err != nil {
			return fmt.Errorf("failed to read/index main part of block %d: %w", blockID, err)
		}
		newIndexBlocks[blockID] = struct{}{}
	}

	baol.indexedBlocks = newIndexBlocks

	baol.log.Debug("Skiplist rebuild complete", "indexedKeys", baol.skiplistIndex.Len(),
		"currentRecentBlocks", baol.recentBlocks)
	return nil
}

func (baol *BlockAppendOnlyLog) recordIndexedKey(blockID uint64, key string) {
	if baol.indexedBlockKeys == nil {
		baol.indexedBlockKeys = make(map[uint64][]string)
	}
	baol.indexedBlockKeys[blockID] = append(baol.indexedBlockKeys[blockID], key)
}

func buildKVPointerForSkiplist(blockID uint64, entryOffset int64, key, value string) *kvPointer {
	ptr := &kvPointer{
		Offset:   entryOffset,
		ValueLen: uint32(len(value)),
		BlockID:  blockID,
	}
	if GetDataTypeFromKey([]byte(key)) == HeaderDataType {
		ptr.InlineValue = []byte(value)
		ptr.HasInlineValue = true
	}
	return ptr
}

func (baol *BlockAppendOnlyLog) setSkiplistEntry(blockID uint64, key string, ptr *kvPointer) {
	baol.skiplistIndex.Set(key, ptr)
	baol.recordIndexedKey(blockID, key)
}

func (baol *BlockAppendOnlyLog) fillMissingBlocksUntil(targetBlockID uint64) error {
	if targetBlockID <= baol.latestBlockID+1 {
		return nil
	}

	for missingID := baol.latestBlockID + 1; missingID < targetBlockID; missingID++ {
		if baol.minBlockID == 0 {
			baol.minBlockID = missingID
		}

		entry := blockIndexEntry{
			BlockID:     missingID,
			StartOffset: baol.currentOffset,
			EndOffset:   baol.currentOffset,
		}
		if err := baol.writeIndexEntry(baol.indexMapFile, entry); err != nil {
			return fmt.Errorf("failed to append gap index entry for block %d: %w", missingID, err)
		}

		baol.latestBlockID = missingID
		baol.updateRecentBlocks(missingID)
	}

	if err := baol.flushIndexBufferWithBlockID(baol.minBlockID); err != nil {
		return fmt.Errorf("failed to flush gap index buffer: %w", err)
	}

	if err := baol.saveIndexMeta(); err != nil {
		return fmt.Errorf("failed to save index metadata after gap fill: %w", err)
	}

	return nil
}

// Append adds a batch of key-value pairs for a given block ID.
// It ensures atomicity for the block: either all pairs are written or none.
// It updates the block index map and the skiplist if the block is recent.
func (baol *BlockAppendOnlyLog) Append(blockID uint64, kvs map[string]string) error {
	baol.mu.Lock()
	defer baol.mu.Unlock()

	if baol.closed {
		return fmt.Errorf("block append-only log is closed")
	}
	baol.ensureWriteTrackingInitialized()
	// isFirstAppend checks if this is the very first operation on a completely empty log.
	isFirstAppend := baol.latestBlockID == 0 && baol.currentOffset == 0

	// Monotonicity checks:
	// 1. If blockID is 0, it's only allowed if it's the first append on an empty log.
	// if blockID == 0 && !isFirstAppend {
	// 	return fmt.Errorf("block ID 0 can only be used for the first append on an empty log; current latest is %d, and this is not the first append", aol.latestBlockID)
	// }
	// 2. If blockID is not 0 (or it is 0 and isFirstAppend), it must be greater than the current latestBlockID.
	//    (The case blockID == 0 && isFirstAppend means latestBlockID is also 0, so 0 <= 0 is true, but it's allowed).
	if !(blockID == 0 && isFirstAppend) && blockID <= baol.latestBlockID {
		if _, exists := baol.getBlockIndexEntry(blockID); exists {
			if blockID < baol.latestBlockID {
				baol.log.Debug("Duplicate historical block ID append ignored", "blockID", blockID, "latestBlockID", baol.latestBlockID)
				return nil
			}
			baol.log.Debug("Appending more kvs to latest block", "blockID", blockID)
		} else {
			return fmt.Errorf("non-monotonic block ID: current latest %d, got %d", baol.latestBlockID, blockID)
		}
	}

	if !isFirstAppend && blockID > baol.latestBlockID+1 {
		if err := baol.fillMissingBlocksUntil(blockID); err != nil {
			return err
		}
	}

	// if blockID == 0 && isFirstAppend {
	// 	return fmt.Errorf("non-monotonic block ID: current latest %d, got %d", baol.latestBlockID, blockID)
	// }
	if len(kvs) == 0 {
		// If kvs is empty, this append operation should generally be a no-op
		// in terms of advancing the log or writing data.

		if blockID == 0 && isFirstAppend { // Allowed: block 0, empty kvs, first append
			baol.log.Debug("Append(0, empty_kvs) on empty log: No operation performed, latestBlockID remains 0.", "blockID", blockID)
			// No index entry, no latestBlockID update.
			return nil
		}

		// If blockID > aol.latestBlockID and kvs is empty, the test implies it's a no-op
		// regarding latestBlockID. We also won't write an index entry for it to be consistent.
		// This condition is implicitly true if we passed the monotonicity check and len(kvs) == 0
		// and it's not the (blockID == 0 && isFirstAppend) case.
		baol.log.Debug("Append called with empty KVS for a new block ID. No operation performed, latestBlockID not advanced.", "blockID", blockID, "latestBlockID", baol.latestBlockID)
		return nil
	}

	var startOffset int64
	// --- Logic for non-empty KVS starts here ---
	existingEntry, exists := baol.getBlockIndexEntry(blockID)
	if exists {
		startOffset = existingEntry.EndOffset
	} else {
		// New block, append at current end offset
		startOffset = baol.currentOffset
	}
	blockDataBuf := new(bytes.Buffer)

	// Serialize all entries for the block into the buffer

	if err := baol.writeLogEntries(blockDataBuf, blockID, kvs); err != nil {
		return fmt.Errorf("failed to serialize entry for block %d: %w", blockID, err)
	}

	blockBytes := blockDataBuf.Bytes()
	n, err := baol.dataWriter.Write(blockBytes)
	if err != nil {
		baol.log.Error("Failed to write block data to buffer", "blockID", blockID, "error", err)
		return fmt.Errorf("failed to write block %d data: %w", blockID, err)
	}
	baol.addDiskWrite(baolDiskIOUsageDataMutation, n)
	if n != len(blockBytes) {
		baol.log.Error("Incomplete write for block data", "blockID", blockID, "written", n, "expected", len(blockBytes))
		return fmt.Errorf("incomplete write for block %d data", blockID)
	}
	if err := baol.dataWriter.Flush(); err != nil {
		baol.log.Error("Failed to flush data writer", "blockID", blockID, "error", err)
		return fmt.Errorf("failed to flush data writer for block %d: %w", blockID, err)
	}

	endOffset := startOffset + int64(n)
	baol.currentOffset = endOffset

	indexEntry := blockIndexEntry{
		BlockID: blockID,
		StartOffset: func() int64 {
			if exists {
				return existingEntry.StartOffset
			}
			return startOffset
		}(),
		EndOffset: endOffset,
	}

	if err := baol.writeIndexEntry(baol.indexMapFile, indexEntry); err != nil {
		baol.log.Crit("Failed to write block index entry to file after writing data!", "blockID", blockID, "error", err)
		return fmt.Errorf("CRITICAL: failed to write index entry for block %d: %w", blockID, err)
	}

	if blockID > baol.latestBlockID {
		// Update minBlockID on first block
		if baol.minBlockID == 0 {
			baol.minBlockID = blockID
			baol.log.Info("Set minBlockID for first block", "minBlockID", baol.minBlockID)
		}

		baol.latestBlockID = blockID
		baol.updateRecentBlocks(blockID)

		// Save metadata after updating block range
		if err := baol.saveIndexMeta(); err != nil {
			baol.log.Error("Failed to save index metadata", "error", err)
			// Non-critical error, continue
		}
	}

	if _, isIndexed := baol.indexedBlocks[blockID]; isIndexed {
		baol.log.Debug("Indexing new block in skiplist", "blockID", blockID)
		reader := bytes.NewReader(blockBytes) // Using bytes.NewReader
		entryPos := startOffset
		for reader.Len() > 0 {
			entry, bytesReadThisEntry, errRead := baol.readLogEntry(reader)
			if errRead == io.EOF {
				break
			}
			if errRead != nil {
				baol.log.Error("Failed to decode entry while indexing new block", "blockID", blockID, "error", errRead)
				return fmt.Errorf("failed to decode entry for skiplist indexing block %d: %w", blockID, errRead)
			}
			ptr := buildKVPointerForSkiplist(entry.BlockID, entryPos, entry.Key, entry.Value)
			baol.setSkiplistEntry(entry.BlockID, entry.Key, ptr)
			entryPos += bytesReadThisEntry
		}
	}

	// Flush index buffer to persist entries (like Delete and AppendToNewBlock do)
	if err := baol.flushIndexBufferWithBlockID(baol.minBlockID); err != nil {
		baol.log.Error("Failed to flush index buffer", "blockID", blockID, "error", err)
		return fmt.Errorf("failed to flush index buffer for block %d: %w", blockID, err)
	}

	baol.evictOldEntries()
	return nil
}

// updateRecentBlocks adds the new block ID and removes the oldest if the limit is exceeded.
func (baol *BlockAppendOnlyLog) updateRecentBlocks(newBlockID uint64) {
	if baol.recentBlocks == nil {
		baol.recentBlocks = make([]uint64, 0, baol.recentN)
	}
	if baol.indexedBlocks == nil {
		baol.indexedBlocks = make(map[uint64]struct{})
	}
	if baol.indexedBlockKeys == nil {
		baol.indexedBlockKeys = make(map[uint64][]string)
	}

	baol.recentBlocks = append(baol.recentBlocks, newBlockID)
	baol.indexedBlocks[newBlockID] = struct{}{}

	if len(baol.recentBlocks) > baol.recentN {
		// Remove the oldest block from index
		oldestBlockID := baol.recentBlocks[0]
		baol.recentBlocks = baol.recentBlocks[1:] // Shift slice
		delete(baol.indexedBlocks, oldestBlockID)

		baol.log.Debug("Evicting oldest block from skiplist index", "blockID", oldestBlockID)

		// Remove keys belonging *only* to the evicted block from the skiplist.
		baol.evictOldBlockFromSkiplist(oldestBlockID)
	}
}

// evictOldEntries is called after new data is appended and recent blocks are updated.
// The primary mechanism for skiplist eviction is `rebuildSkiplist`, which is
// called by `updateRecentBlocks` when the oldest block in `recentBlocks` is removed.
func (baol *BlockAppendOnlyLog) evictOldEntries() {
	// Ensure lock is held if operations were to be performed, matching original intent.
	// For example, if using a library that provides aol.mu.AssertHeld()
	// aol.mu.AssertHeld()

	// The core logic for ensuring the skiplist only contains recentN blocks
	// is handled by `updateRecentBlocks` calling `rebuildSkiplist`.
	// If `updateRecentBlocks` has run and potentially triggered `rebuildSkiplist`,
	// the skiplist should already be in the correct state.
	baol.log.Debug("evictOldEntries: Called. Skiplist state is managed by rebuildSkiplist.",
		"latestBlockID", baol.latestBlockID,
		"recentN", baol.recentN,
		"numRecentBlocksTracked", len(baol.recentBlocks),
		"numIndexedBlocks", len(baol.indexedBlocks))

	// The previous logic in this function was flawed because:
	// 1. It attempted to access a non-existent 'Entries' field on 'blockIndexEntry'.
	// 2. It was largely redundant with the 'rebuildSkiplist' mechanism, which correctly
	//    rebuilds the index based on the 'recentN' most recent blocks.
	//
	// No explicit key removal or other operations are needed here if 'rebuildSkiplist'
	// (triggered by 'updateRecentBlocks') is functioning as intended.
}

// Get retrieves the latest value for a key from the indexed recent blocks,
// or by scanning older blocks if not found in recent ones.
// Returns the value and true if found, or "", false otherwise.
// Handles tombstones (returns "", true for deleted keys).
func (baol *BlockAppendOnlyLog) Get(key string) (string, bool, error) {
	baol.mu.RLock()
	defer baol.mu.RUnlock()
	if baol.closed {
		return "", false, fmt.Errorf("append-only log is closed")
	}

	// check in buffer first

	// Check if the key is in the skiplist index
	inMemoryIndexStart := time.Now()
	element := baol.skiplistIndex.Get(key)
	if element != nil {
		recordBAOLGetBreakdownStep(&baol.getInMemoryIndexStats, true, time.Since(inMemoryIndexStart))
		pointer := element.Value.(*kvPointer)

		diskDataStart := time.Now()
		valueBytes, dataFromCache, err := baol.readValueBytesFromPointerWithSource(pointer)
		recordBAOLGetBreakdownStep(&baol.getDiskDataStats, dataFromCache, time.Since(diskDataStart))
		if err != nil {
			baol.log.Error("Get: Failed to read entry via pointer", "key", key, "offset", pointer.Offset, "blockID", pointer.BlockID, "error", err)
			return "", false, fmt.Errorf("failed to read entry for key %s: %w", key, err)
		}

		value := BytesToString(valueBytes)
		if value == TombstoneMarker {
			return "", true, nil // Key was explicitly deleted
		}
		return value, true, nil // Key found in skiplist
	}
	recordBAOLGetBreakdownStep(&baol.getInMemoryIndexStats, false, time.Since(inMemoryIndexStart))

	// Key not in skiplist - check if we can determine blockID from key
	dataType := GetDataTypeFromKey([]byte(key))
	if blockID, ok := ParseBlockNumberFromKey([]byte(key), dataType); ok {
		// For keys with embedded block numbers (Header, BlockBody, BlockReceipts)
		if blockID > baol.latestBlockID {
			return "", false, fmt.Errorf("Get: requested blockID %d > latestBlockID %d: %w", blockID, baol.latestBlockID, errRequestedFutureBlock)
			// return "", false, nil
		} else if baol.latestBlockID-blockID > IgnoredThreshold {
			return "", true, nil
		}
		// Check if this block is in the skiplist
		if baol.indexedBlocks != nil {
			if _, isIndexed := baol.indexedBlocks[blockID]; isIndexed {
				// Block is in skiplist but key not found - key doesn't exist
				return "", false, nil
			}
		}

		// Block not in skiplist - read from disk via index
		diskIndexStart := time.Now()
		mainEntry, okMain, indexFromCache, err := baol.getBlockIndexEntryWithSource(blockID)
		recordBAOLGetBreakdownStep(&baol.getDiskIndexStats, indexFromCache, time.Since(diskIndexStart))
		if err != nil {
			return "", false, fmt.Errorf("failed to lookup block index %d: %w", blockID, err)
		}
		if okMain {
			diskDataStart := time.Now()
			if val, found, err := baol.findKeyInOneBlock(baol.dataFile, mainEntry, key); err != nil {
				recordBAOLGetBreakdownStep(&baol.getDiskDataStats, false, time.Since(diskDataStart))
				return "", false, fmt.Errorf("failed to scan block %d from disk: %w", blockID, err)
			} else if found {
				recordBAOLGetBreakdownStep(&baol.getDiskDataStats, false, time.Since(diskDataStart))
				if val == TombstoneMarker {
					return "", true, nil
				}
				return val, true, nil
			}
			recordBAOLGetBreakdownStep(&baol.getDiskDataStats, false, time.Since(diskDataStart))
		}
		// Block not found in index either
		return "", false, nil
	}

	// For keys without embedded block numbers, only check skiplist
	// (we can't determine which block to scan without a full scan)
	return "", false, nil
}

// Delete marks a key as deleted by appending a tombstone entry for it
// associated with the next logical block ID.
// Note: This implementation creates a new block for the deletion.
// A more sophisticated approach might allow adding tombstones to the current latest block
// if it's mutable or batching deletions.
func (baol *BlockAppendOnlyLog) Delete(key string) error {

	baol.mu.Lock()
	defer baol.mu.Unlock()

	if baol.closed {
		return fmt.Errorf("append-only log is closed")
	}
	baol.ensureWriteTrackingInitialized()

	dataType := GetDataTypeFromKey([]byte(key))
	blockID, _ := ParseBlockNumberFromKey([]byte(key), dataType)
	if baol.latestBlockID > 0 && baol.latestBlockID-blockID > IgnoredThreshold {
		return nil
	}
	// Determine next block ID
	blockIDForDelete := baol.latestBlockID
	if blockIDForDelete == 0 {
		blockIDForDelete = 1 // First block
	} else {
		blockIDForDelete++ // Next block
	}

	startOffset := baol.currentOffset
	blockDataBuf := new(bytes.Buffer)

	if err := baol.writeLogEntry(blockDataBuf, blockIDForDelete, key, TombstoneMarker); err != nil {
		return fmt.Errorf("failed to serialize tombstone entry for block %d, key %s: %w", blockIDForDelete, key, err)
	}

	blockBytes := blockDataBuf.Bytes()
	n, err := baol.dataWriter.Write(blockBytes)
	if err != nil {
		baol.log.Error("Failed to write tombstone block data to buffer", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to write tombstone block %d data: %w", blockIDForDelete, err)
	}
	baol.addDiskWrite(baolDiskIOUsageDataMutation, n)
	if n != len(blockBytes) {
		baol.log.Error("Incomplete write for tombstone block data", "blockID", blockIDForDelete, "written", n, "expected", len(blockBytes))
		return fmt.Errorf("incomplete write for tombstone block %d data", blockIDForDelete)
	}

	endOffset := startOffset + int64(n)
	baol.currentOffset = endOffset

	indexEntry := blockIndexEntry{
		BlockID:     blockIDForDelete,
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
	baol.latestBlockID = blockIDForDelete // Update latestBlockID

	if err := baol.writeIndexEntry(baol.indexMapFile, indexEntry); err != nil {
		baol.log.Crit("Failed to write block index entry for tombstone to file!", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("CRITICAL: failed to write index entry for tombstone block %d: %w", blockIDForDelete, err)
	}

	if err := baol.dataWriter.Flush(); err != nil {
		baol.log.Error("Failed to flush data writer after tombstone", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to flush data writer for tombstone block %d: %w", blockIDForDelete, err)
	}
	if err := baol.flushIndexBuffer(); err != nil {
		baol.log.Error("Failed to flush index map buffer after tombstone", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to flush index map buffer for tombstone block %d: %w", blockIDForDelete, err)
	}

	baol.updateRecentBlocks(blockIDForDelete) // This will add the new block
	if _, isIndexed := baol.indexedBlocks[blockIDForDelete]; isIndexed {
		baol.log.Debug("Indexing tombstone in skiplist", "blockID", blockIDForDelete, "key", key)
		// Add tombstone to skiplist
		ptr := buildKVPointerForSkiplist(blockIDForDelete, startOffset, key, TombstoneMarker)
		baol.setSkiplistEntry(blockIDForDelete, key, ptr)
	}

	baol.log.Info("Appended tombstone for key", "key", key, "blockID", blockIDForDelete)
	return nil
}

// DeleteByPrefixInBlock identifies all keys in a specific targetBlockID that start with the given prefix
// and appends tombstone entries for them in a new block.
// If the target block doesn't exist, an error is returned.
// If no such keys are found (e.g., block is empty, no keys match prefix, or all matching keys are already tombstones),
// it's a no-op and returns nil.
func (baol *BlockAppendOnlyLog) DeleteByPrefixInBlock(targetBlockID uint64, prefix string) error {
	var blockData []byte
	var readErr error

	baol.mu.RLock()
	if baol.closed {
		baol.mu.RUnlock()
		return fmt.Errorf("append-only log is closed")
	}

	indexEntry, ok := baol.getBlockIndexEntry(targetBlockID)
	if !ok {
		baol.mu.RUnlock()
		return fmt.Errorf("target block ID %d not found in index", targetBlockID)
	}

	size := indexEntry.EndOffset - indexEntry.StartOffset
	if size <= 0 {
		baol.mu.RUnlock()
		baol.log.Debug("Target block for prefix deletion is empty", "blockID", targetBlockID, "prefix", prefix)
		return nil // Empty block, no-op
	}

	blockData = make([]byte, size)
	_, readErr = baol.readAtWithStats(baol.dataFile, blockData, indexEntry.StartOffset, baolDiskIOUsageDataQuery)
	baol.mu.RUnlock() // Release RLock after reading data (or attempting to)

	if readErr != nil {
		return fmt.Errorf("failed to read block data for target block %d (offset %d): %w", targetBlockID, indexEntry.StartOffset, readErr)
	}

	// Parse block data to get raw key-value pairs
	kvsInBlock := make(map[string]string)
	reader := bytes.NewReader(blockData)
	for reader.Len() > 0 {
		entry, _, err := baol.readLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// This error means the block itself is corrupted or unreadable in part.
			return fmt.Errorf("failed to decode entry in target block %d during prefix deletion scan: %w", targetBlockID, err)
		}
		kvsInBlock[entry.Key] = entry.Value // Store raw value, including any existing tombstones
	}

	keysToTombstone := make(map[string]string)
	for key, value := range kvsInBlock {
		if strings.HasPrefix(key, prefix) && value != TombstoneMarker {
			keysToTombstone[key] = TombstoneMarker
		}
	}

	if len(keysToTombstone) == 0 {
		baol.log.Debug("No non-tombstoned keys found with prefix in target block for deletion",
			"targetBlockID", targetBlockID, "prefix", prefix)
		return nil // No keys to delete, or all were already tombstones
	}

	baol.log.Info("Identified keys for prefix deletion",
		"targetBlockID", targetBlockID, "prefix", prefix, "count", len(keysToTombstone))

	// AppendToNewBlock handles its own locking.
	// It will create a new block with the tombstones.
	newBlockID, err := baol.AppendToNewBlock(keysToTombstone)
	if err != nil {
		return fmt.Errorf("failed to append tombstones for prefix deletion (target block %d, prefix '%s'): %w", targetBlockID, prefix, err)
	}

	baol.log.Info("Successfully appended tombstones for prefix deletion",
		"targetBlockID", targetBlockID, "prefix", prefix, "tombstoneBlockID", newBlockID, "count", len(keysToTombstone))
	return nil
}

// GetByBlock retrieves all key-value pairs for a specific block ID.
func (baol *BlockAppendOnlyLog) GetByBlock(blockID uint64) (map[string]string, error) {
	baol.mu.RLock()
	defer baol.mu.RUnlock()

	if baol.closed {
		return nil, fmt.Errorf("append-only log is closed")
	}

	kvs := make(map[string]string)

	// main
	if indexEntry, ok := baol.getBlockIndexEntry(blockID); ok {
		size := indexEntry.EndOffset - indexEntry.StartOffset
		if size > 0 {
			blockData := make([]byte, size)
			if _, err := baol.readAtWithStats(baol.dataFile, blockData, indexEntry.StartOffset, baolDiskIOUsageDataQuery); err != nil {
				return nil, fmt.Errorf("failed to read block data for %d from offset %d: %w", blockID, indexEntry.StartOffset, err)
			}
			reader := bytes.NewReader(blockData)
			for reader.Len() > 0 {
				entry, _, err := baol.readLogEntry(reader)
				if err == io.EOF {
					break
				}
				if err != nil {
					baol.log.Error("Failed to decode entry in block during GetByBlock", "blockID", blockID, "error", err)
					return kvs, fmt.Errorf("failed to decode entry in block %d: %w", blockID, err)
				}
				if entry.Value == TombstoneMarker {
					kvs[entry.Key] = "__DELETED__"
				} else {
					kvs[entry.Key] = entry.Value
				}
			}
		}
	}
	return kvs, nil
}

// writeLogEntry serializes a single log entry to the writer.
// Format: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
func (baol *BlockAppendOnlyLog) writeLogEntry(w io.Writer, blockID uint64, key, value string) error {
	keyBytes := []byte(key)
	valueBytes := []byte(value)
	keyLen := uint32(len(keyBytes))
	valueLen := uint32(len(valueBytes))

	buf := make([]byte, blockIDSize+keyLenSize+valueLenSize+len(keyBytes)+len(valueBytes))
	binary.BigEndian.PutUint64(buf[0:blockIDSize], blockID)
	binary.BigEndian.PutUint32(buf[blockIDSize:blockIDSize+keyLenSize], keyLen)
	binary.BigEndian.PutUint32(buf[blockIDSize+keyLenSize:], valueLen)
	offset := blockIDSize + keyLenSize + valueLenSize
	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)
	copy(buf[offset:], valueBytes)
	_, err := w.Write(buf)
	return err
}

// writeLogEntriess serializes some log entries to the writer.
// Format: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
func (baol *BlockAppendOnlyLog) writeLogEntries(w io.Writer, blockID uint64, kvs map[string]string) error {
	if len(kvs) == 0 {
		return nil
	}
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		pi := baolWritePriority(keys[i])
		pj := baolWritePriority(keys[j])
		if pi != pj {
			return pi < pj
		}
		return keys[i] < keys[j]
	})

	size := 0
	for _, key := range keys {
		value := kvs[key]
		size += blockIDSize + keyLenSize + valueLenSize + len(key) + len(value)
	}
	buf := make([]byte, size)
	offset := 0
	for _, key := range keys {
		value := kvs[key]
		kb := []byte(key)
		vb := []byte(value)
		keyLen := uint32(len(kb))
		valueLen := uint32(len(vb))
		binary.BigEndian.PutUint64(buf[offset:], blockID)
		binary.BigEndian.PutUint32(buf[offset+blockIDSize:], keyLen)
		binary.BigEndian.PutUint32(buf[offset+blockIDSize+keyLenSize:], valueLen)
		offset += blockIDSize + keyLenSize + valueLenSize
		copy(buf[offset:], kb)
		offset += len(kb)
		copy(buf[offset:], vb)
		offset += len(vb)
	}
	_, err := w.Write(buf)
	return err
}

func baolWritePriority(key string) int {
	dt := GetDataTypeFromKey(StringToBytes(key))
	switch dt {
	case HeaderDataType:
		return 0
	case BlockBodyDataType:
		return 1
	case BlockReceiptsDataType:
		return 2
	default:
		return 3
	}
}

// readLogEntry deserializes a single log entry from the reader.
// Returns the entry, bytes read, and error.
func (baol *BlockAppendOnlyLog) readLogEntry(r io.Reader) (*logEntry, int64, error) {
	headerBuf := make([]byte, blockIDSize+keyLenSize+valueLenSize)
	n, err := io.ReadFull(r, headerBuf)
	bytesRead := int64(n)
	if err != nil {
		if err == io.EOF && bytesRead == 0 {
			return nil, 0, io.EOF
		}
		return nil, bytesRead, fmt.Errorf("failed reading entry header: %w", err)
	}

	entry := &logEntry{}
	entry.BlockID = binary.BigEndian.Uint64(headerBuf[0:blockIDSize])
	keyLen := binary.BigEndian.Uint32(headerBuf[blockIDSize : blockIDSize+keyLenSize])
	valueLen := binary.BigEndian.Uint32(headerBuf[blockIDSize+keyLenSize:])

	keyBytes := make([]byte, keyLen)
	n, err = io.ReadFull(r, keyBytes)
	bytesRead += int64(n)
	if err != nil {
		return nil, bytesRead, fmt.Errorf("failed reading key (len %d): %w", keyLen, err)
	}
	entry.Key = string(keyBytes)

	valueBytes := make([]byte, valueLen)
	n, err = io.ReadFull(r, valueBytes)
	bytesRead += int64(n)
	if err != nil {
		return nil, bytesRead, fmt.Errorf("failed reading value (len %d): %w", valueLen, err)
	}
	entry.Value = string(valueBytes)

	return entry, bytesRead, nil
}

// writeIndexEntry appends a block index entry to the index map file.
func (baol *BlockAppendOnlyLog) writeIndexEntry(w io.Writer, entry blockIndexEntry) error {
	if w == nil {
		return baol.bufferIndexEntry(entry)
	}

	if f, ok := w.(*os.File); ok && (f == nil || (baol.indexMapFile != nil && f == baol.indexMapFile)) {
		return baol.bufferIndexEntry(entry)
	}

	buf := make([]byte, blockIDSize+offsetSize+offsetSize)
	binary.BigEndian.PutUint64(buf[0:blockIDSize], entry.BlockID)
	binary.BigEndian.PutUint64(buf[blockIDSize:blockIDSize+offsetSize], uint64(entry.StartOffset))
	binary.BigEndian.PutUint64(buf[blockIDSize+offsetSize:], uint64(entry.EndOffset))

	_, err := w.Write(buf)
	if err != nil {
		return err
	}
	// if indexWriter, ok := w.(*bufio.Writer); ok {
	// 	return indexWriter.Flush()
	// }
	// if f, ok := w.(*os.File); ok {
	// 	return f.Sync()
	// }
	return nil
}

// AppendToNewBlock adds a batch of key-value pairs to a new, automatically assigned block ID.
// If kvs is empty, no block is written, aol.latestBlockID is returned (or 0 if log was empty), and no error.
func (baol *BlockAppendOnlyLog) AppendToNewBlock(kvs map[string]string) (uint64, error) {
	baol.mu.Lock()
	defer baol.mu.Unlock()

	if baol.closed {
		return 0, fmt.Errorf("append-only log is closed")
	}
	baol.ensureWriteTrackingInitialized()

	if len(kvs) == 0 {
		baol.log.Debug("AppendToNewBlock called with empty KVS, no operation performed")
		if baol.isLogEmptyInitial() {
			return 0, nil // Log is empty, no block ID assigned yet
		}
		return baol.latestBlockID, nil // Return current latest, no new block created
	}

	newBlockID := baol.latestBlockID + 1
	if baol.isLogEmptyInitial() {
		newBlockID = 1
	}

	startOffset := baol.currentOffset
	blockDataBuf := new(bytes.Buffer)
	if err := baol.writeLogEntries(blockDataBuf, newBlockID, kvs); err != nil {
		return 0, fmt.Errorf("failed to serialize entries for new block %d: %w", newBlockID, err)
	}

	blockBytes := blockDataBuf.Bytes()
	n, err := baol.dataWriter.Write(blockBytes)
	if err != nil {
		baol.log.Error("Failed to write new block data to buffer", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to write new block %d data: %w", newBlockID, err)
	}
	baol.addDiskWrite(baolDiskIOUsageDataMutation, n)
	if n != len(blockBytes) {
		baol.log.Error("Incomplete write for new block data", "assignedBlockID", newBlockID, "written", n, "expected", len(blockBytes))
		return 0, fmt.Errorf("incomplete write for new block %d data", newBlockID)
	}

	endOffset := startOffset + int64(n)
	baol.currentOffset = endOffset

	indexEntry := blockIndexEntry{
		BlockID:     newBlockID,
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
	// Update minBlockID on first block
	if baol.minBlockID == 0 {
		baol.minBlockID = newBlockID
		baol.log.Info("Set minBlockID for first block", "minBlockID", baol.minBlockID)
	}

	baol.latestBlockID = newBlockID

	if err := baol.writeIndexEntry(baol.indexMapFile, indexEntry); err != nil {
		baol.log.Crit("Failed to write block index entry to file for new block!", "assignedBlockID", newBlockID, "error", err)
		// This is critical. Data is written but index isn't. Consider how to handle.
		return 0, fmt.Errorf("CRITICAL: failed to write index entry for new block %d: %w", newBlockID, err)
	}

	if err := baol.dataWriter.Flush(); err != nil {
		baol.log.Error("Failed to flush data writer after new block", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to flush data writer for new block %d: %w", newBlockID, err)
	}
	if err := baol.flushIndexBuffer(); err != nil {
		baol.log.Error("Failed to flush index map buffer after new block", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to flush index map buffer for new block %d: %w", newBlockID, err)
	}

	// Save metadata after successful append
	if err := baol.saveIndexMeta(); err != nil {
		baol.log.Error("Failed to save index metadata", "error", err)
		// Non-critical error, continue
	}

	baol.updateRecentBlocks(newBlockID) // Manages recentBlocks and indexedBlocks
	if _, isIndexed := baol.indexedBlocks[newBlockID]; isIndexed {
		baol.log.Debug("Indexing new block in skiplist (AppendToNewBlock)", "blockID", newBlockID)
		reader := bytes.NewReader(blockBytes)
		entryPos := startOffset
		for reader.Len() > 0 {
			entry, bytesRead, readErr := baol.readLogEntry(reader)
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				baol.log.Error("Failed to decode entry while indexing new block (AppendToNewBlock)", "blockID", newBlockID, "error", readErr)
				// Data is persisted, but skiplist might be inconsistent for this new block.
				return 0, fmt.Errorf("failed to decode entry for skiplist indexing new block %d: %w", newBlockID, readErr)
			}
			ptr := buildKVPointerForSkiplist(entry.BlockID, entryPos, entry.Key, entry.Value)
			baol.setSkiplistEntry(entry.BlockID, entry.Key, ptr)
			entryPos += bytesRead
		}
	}

	// evictOldEntries is implicitly handled by updateRecentBlocks if it calls rebuildSkiplist.
	// No explicit call to aol.evictOldEntries() needed here if updateRecentBlocks is comprehensive.

	return newBlockID, nil
}

// isLogEmptyInitial checks if the log is completely empty (no blocks indexed).
// Caller must hold aol.mu.
func (baol *BlockAppendOnlyLog) isLogEmptyInitial() bool {
	return baol.latestBlockID == 0 && len(baol.blockIndex) == 0
}

// readValueBytesFromPointer reads the raw value bytes from the data file for a given kvPointer.
// This is used by the iterator to get values from the skiplist pointers.
// Assumes aol.mu is RLocked by the caller if called during skiplist iteration.
// Reading from aol.dataFile with ReadAt is safe concurrently.
func (baol *BlockAppendOnlyLog) readValueBytesFromPointer(pointer *kvPointer) ([]byte, error) {
	value, _, err := baol.readValueBytesFromPointerWithSource(pointer)
	return value, err
}

func (baol *BlockAppendOnlyLog) readValueBytesFromPointerWithSource(pointer *kvPointer) ([]byte, bool, error) {
	if pointer.HasInlineValue {
		return pointer.InlineValue, true, nil
	}

	// logEntry format on disk: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
	// pointer.Offset points to the start of this logEntry.
	// pointer.ValueLen is the length of the string form of the value (can be TombstoneMarker).

	// We need to determine the key's length to correctly calculate the value's starting offset.
	// The keyLen field is located after the blockID field.
	f, headerSize, keyLen, valueLen, err := baol.readHeaderAndLocate(pointer)
	if err != nil {
		return nil, false, err
	}

	valueBytes := make([]byte, valueLen)
	valueOffset := pointer.Offset + int64(headerSize) + int64(keyLen)
	if _, err := baol.readAtWithStats(f, valueBytes, valueOffset, baolDiskIOUsageDataQuery); err != nil {
		return nil, false, fmt.Errorf("ReadAt for value failed at offset %d (len %d): %w", valueOffset, valueLen, err)
	}
	return valueBytes, false, nil
}

// Close flushes buffers and closes open files.
func (baol *BlockAppendOnlyLog) Close() error {
	baol.mu.Lock()
	if baol.closed {
		baol.mu.Unlock()
		return ErrClosed // Or your specific error for already closed
	}
	baol.closed = true

	flushSignal := baol.indexBufferFlush
	backgroundDone := baol.backgroundDone
	minBlockID := baol.minBlockID
	baol.mu.Unlock()

	var errs []error // Using a slice to collect multiple errors

	if flushSignal != nil {
		close(flushSignal)
	}

	if backgroundDone != nil {
		select {
		case <-backgroundDone:
		case <-time.After(2 * time.Second):
			errs = append(errs, fmt.Errorf("background flush goroutine exit timeout"))
		}
	}

	// Flush any remaining buffered index entries
	if err := baol.flushIndexBufferWithBlockID(minBlockID); err != nil {
		errs = append(errs, fmt.Errorf("failed to flush index buffer on close: %w", err))
	}

	if baol.dataWriter != nil {
		if err := baol.dataWriter.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("failed to flush data writer on close: %w", err))
		}
	}

	if baol.dataFile != nil {
		if err := baol.dataFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close data file: %w", err))
		}
		baol.dataFile = nil // Mark as closed
	}

	if baol.indexMapFile != nil {
		if err := baol.indexMapFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close index map file: %w", err))
		}
		baol.indexMapFile = nil
	}

	baol.printDiskIOStats()
	printBAOLGetBreakdownStep("in-memory-index-fetch", &baol.getInMemoryIndexStats)
	printBAOLGetBreakdownStep("disk-index-fetch", &baol.getDiskIndexStats)
	printBAOLGetBreakdownStep("disk-data-fetch", &baol.getDiskDataStats)
	printBAOLGetBreakdownStep("range-memory-index-seek", &baol.iteratorMemoryIndexStats)
	printBAOLGetBreakdownStep("range-block-index-fetch", &baol.iteratorBlockIndexStats)
	printBAOLGetBreakdownStep("range-key-read", &baol.iteratorKeyReadStats)
	printBAOLGetBreakdownStep("range-value-fetch", &baol.iteratorValueReadStats)
	printBAOLBreakdownSummary("range breakdown",
		map[string]*baolGetBreakdownStepStats{
			"memory-index-seek": &baol.iteratorMemoryIndexStats,
			"block-index-fetch": &baol.iteratorBlockIndexStats,
			"key-read":          &baol.iteratorKeyReadStats,
			"value-fetch":       &baol.iteratorValueReadStats,
		})

	// Combine errors if any occurred
	if len(errs) > 0 {
		// In Go 1.20+, you can use errors.Join(errs...)
		// For older versions, you might return a custom error type or a formatted string.
		// For simplicity, returning the first error or a generic message:
		// This is a basic way; consider a more robust error aggregation if needed.
		var sb strings.Builder
		sb.WriteString("errors during close: ")
		for i, e := range errs {
			if i > 0 {
				sb.WriteString("; ")
			}
			sb.WriteString(e.Error())
		}
		return errors.New(sb.String())
	}
	return nil
}

func (baol *BlockAppendOnlyLog) evictOldBlockFromSkiplist(oldestBlockID uint64) {
	baol.log.Debug("Evicting oldest block from skiplist index", "blockID", oldestBlockID)

	keys := baol.indexedBlockKeys[oldestBlockID]
	if len(keys) == 0 {
		baol.log.Debug("No tracked keys for block during eviction; falling back to full scan", "blockID", oldestBlockID)
		for e := baol.skiplistIndex.Front(); e != nil; e = e.Next() {
			ptr := e.Value.(*kvPointer)
			if ptr.BlockID != oldestBlockID {
				continue
			}
			key := e.Key().(string)
			baol.skiplistIndex.Remove(key)
			baol.log.Debug("Removed key (fallback) from skiplist during eviction", "key", key, "evictedBlockID", oldestBlockID)
		}
		delete(baol.indexedBlockKeys, oldestBlockID)
		return
	}

	removed := 0
	for _, key := range keys {
		el := baol.skiplistIndex.Get(key)
		if el == nil {
			continue
		}
		ptr := el.Value.(*kvPointer)
		if ptr.BlockID != oldestBlockID {
			continue
		}
		baol.skiplistIndex.Remove(key)
		removed++
		baol.log.Debug("Removed key from skiplist during eviction", "key", key, "evictedBlockID", oldestBlockID)
	}
	delete(baol.indexedBlockKeys, oldestBlockID)
	if removed == 0 {
		baol.log.Debug("Tracked keys remained due to newer versions", "blockID", oldestBlockID, "tracked", len(keys))
	}
}

func (baol *BlockAppendOnlyLog) backgroundFlush() {
	defer close(baol.backgroundDone)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case _, ok := <-baol.indexBufferFlush:
			if !ok {
				return
			}
			_ = baol.flushIndexBuffer()
		case <-ticker.C:
			_ = baol.flushIndexBuffer()
		}
	}
}

// add an entry to the index buffer and trigger flush if needed
func (baol *BlockAppendOnlyLog) bufferIndexEntry(entry blockIndexEntry) error {
	baol.indexBufferMu.Lock()
	defer baol.indexBufferMu.Unlock()

	baol.indexBuffer = append(baol.indexBuffer, entry)

	// Trigger flush if buffer exceeds threshold
	if len(baol.indexBuffer) >= baol.indexBufferSize {
		select {
		case baol.indexBufferFlush <- struct{}{}:
		default:
		}
	}
	return nil
}

func (baol *BlockAppendOnlyLog) restoreIndexBuffer(entries []blockIndexEntry) {
	if len(entries) == 0 {
		return
	}
	baol.indexBufferMu.Lock()
	defer baol.indexBufferMu.Unlock()
	baol.indexBuffer = append(entries, baol.indexBuffer...)
}

func (baol *BlockAppendOnlyLog) flushIndexBuffer() error {
	return baol.flushIndexBufferWithBlockID(baol.minBlockID)
}

func (baol *BlockAppendOnlyLog) flushIndexBufferWithBlockID(minBlockID uint64) error {
	baol.indexBufferMu.Lock()

	if len(baol.indexBuffer) == 0 {
		baol.indexBufferMu.Unlock()
		return nil
	}

	entries := make([]blockIndexEntry, len(baol.indexBuffer))
	copy(entries, baol.indexBuffer)

	baol.indexBuffer = baol.indexBuffer[:0]
	baol.indexBufferMu.Unlock()

	f := baol.indexMapFile
	if f == nil {
		baol.restoreIndexBuffer(entries)
		return ErrClosed
	}

	for _, entry := range entries {
		buf := make([]byte, indexEntrySize)
		binary.BigEndian.PutUint64(buf[0:blockIDSize], entry.BlockID)
		binary.BigEndian.PutUint64(buf[blockIDSize:blockIDSize+offsetSize], uint64(entry.StartOffset))
		binary.BigEndian.PutUint64(buf[blockIDSize+offsetSize:], uint64(entry.EndOffset))

		if entry.BlockID < minBlockID {
			baol.log.Warn("Skipping index entry with blockID < minBlockID",
				"blockID", entry.BlockID, "minBlockID", minBlockID)
			continue
		}

		// Use compact storage: position based on offset from minBlockID.
		// minBlockID can be 0 for valid block-0 based datasets.
		pos := int64(entry.BlockID-minBlockID) * int64(indexEntrySize)
		if _, err := baol.writeAtWithStats(f, buf, pos, baolDiskIOUsageIndexMutation); err != nil {
			baol.log.Error("Failed to write index entry during flush", "error", err)
			baol.restoreIndexBuffer(entries)
			return err
		}
	}

	baol.log.Debug("Successfully flushed index buffer", "entries", len(entries))
	return nil
}

func (baol *BlockAppendOnlyLog) FlushIndexBuffer() error {
	return baol.flushIndexBuffer()
}

// FlushDataAndIndex explicitly flushes current block data and index updates.
// It is used by replay block boundaries to avoid relying on background flushes.
func (baol *BlockAppendOnlyLog) FlushDataAndIndex() error {
	baol.mu.RLock()
	defer baol.mu.RUnlock()

	if baol.closed {
		return fmt.Errorf("append-only log is closed")
	}
	if baol.dataWriter != nil {
		if err := baol.dataWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush data writer: %w", err)
		}
	}
	if err := baol.flushIndexBufferWithBlockID(baol.minBlockID); err != nil {
		return fmt.Errorf("failed to flush index buffer: %w", err)
	}
	if err := baol.saveIndexMeta(); err != nil {
		return fmt.Errorf("failed to save index metadata: %w", err)
	}
	return nil
}

// getBlockIndexEntry retrieves the block index entry for a given block ID.
func (baol *BlockAppendOnlyLog) getBlockIndexEntry(blockID uint64) (blockIndexEntry, bool) {
	entry, ok, _, err := baol.getBlockIndexEntryWithSource(blockID)
	if err != nil {
		baol.log.Error("Failed to lookup block index entry from disk", "blockID", blockID, "error", err)
		return blockIndexEntry{}, false
	}
	if ok {
		return entry, true
	}
	return blockIndexEntry{}, false
}

func (baol *BlockAppendOnlyLog) getBlockIndexEntryWithSource(blockID uint64) (blockIndexEntry, bool, bool, error) {
	if baol.blockIndex != nil {
		entry, ok := baol.blockIndex[blockID]
		if ok {
			return entry, true, true, nil
		}
	}
	baol.indexBufferMu.Lock()
	for _, entry := range baol.indexBuffer {
		if entry.BlockID == blockID {
			baol.indexBufferMu.Unlock()
			return entry, true, true, nil
		}
	}
	baol.indexBufferMu.Unlock()

	entry, ok, err := baol.findBlockIndexEntryOnDisk(blockID)
	if err != nil {
		return blockIndexEntry{}, false, false, err
	}
	if ok {
		return entry, true, false, nil
	}
	return blockIndexEntry{}, false, false, nil
}

// readAndIndexBlockFrom reads a block from the data file (main or late) based on the index entry
func (baol *BlockAppendOnlyLog) readAndIndexBlockFrom(indexEntry blockIndexEntry) error {
	size := indexEntry.EndOffset - indexEntry.StartOffset
	if size <= 0 {
		return nil
	}

	f := baol.dataFile

	blockData := make([]byte, size)
	if _, err := baol.readAtWithStats(f, blockData, indexEntry.StartOffset, baolDiskIOUsageBootstrap); err != nil {
		return fmt.Errorf("failed to read block data for %d from offset %d: %w", indexEntry.BlockID, indexEntry.StartOffset, err)
	}

	reader := bytes.NewReader(blockData)
	currentPos := indexEntry.StartOffset

	for reader.Len() > 0 {
		entryOffset := currentPos
		entry, bytesRead, err := baol.readLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to decode entry in block %d: %w", indexEntry.BlockID, err)
		}
		currentPos += bytesRead

		ptr := buildKVPointerForSkiplist(entry.BlockID, entryOffset, entry.Key, entry.Value)
		// Insert into skiplist
		baol.setSkiplistEntry(entry.BlockID, entry.Key, ptr)
	}
	return nil
}

// findKeyInOneBlock searches for a key within a specific block defined by the index entry.
func (baol *BlockAppendOnlyLog) findKeyInOneBlock(f *os.File, indexEntry blockIndexEntry, key string) (string, bool, error) {
	size := indexEntry.EndOffset - indexEntry.StartOffset
	if size <= 0 {
		return "", false, nil
	}
	blockData := make([]byte, size)
	if _, err := baol.readAtWithStats(f, blockData, indexEntry.StartOffset, baolDiskIOUsageDataQuery); err != nil {
		return "", false, fmt.Errorf("Get: failed to read data for block %d: %w", indexEntry.BlockID, err)
	}
	reader := bytes.NewReader(blockData)
	for reader.Len() > 0 {
		entry, _, err := baol.readLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", false, fmt.Errorf("Get: failed to decode entry in block %d: %w", indexEntry.BlockID, err)
		}
		if entry.Key == key {
			return entry.Value, true, nil
		}
	}

	reader.Seek(0, io.SeekStart)

	for reader.Len() > 0 {
		entry, _, err := baol.readLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", false, fmt.Errorf("Get: failed to decode entry in block %d: %w", indexEntry.BlockID, err)
		}
		if entry.Key == key {
			return entry.Value, true, nil
		}
	}
	return "", false, nil
}

// readHeaderAndLocate reads the header at the given pointer and determines which data file it belongs to.
func (baol *BlockAppendOnlyLog) readHeaderAndLocate(pointer *kvPointer) (*os.File, int, uint32, uint32, error) {
	headerSize := blockIDSize + keyLenSize + valueLenSize
	headerBytes := make([]byte, headerSize)

	// use BlockID to locate the correct data file
	if pointer.BlockID != 0 {
		if mainEntry, ok := baol.getBlockIndexEntry(pointer.BlockID); ok {
			if pointer.Offset >= mainEntry.StartOffset && pointer.Offset+int64(headerSize) <= mainEntry.EndOffset {
				if _, err := baol.readAtWithStats(baol.dataFile, headerBytes, pointer.Offset, baolDiskIOUsageDataQuery); err == nil {
					keyLen := binary.BigEndian.Uint32(headerBytes[blockIDSize : blockIDSize+keyLenSize])
					valLen := binary.BigEndian.Uint32(headerBytes[blockIDSize+keyLenSize:])
					return baol.dataFile, headerSize, keyLen, valLen, nil
				}
			}
		}
	}
	if _, err := baol.readAtWithStats(baol.dataFile, headerBytes, pointer.Offset, baolDiskIOUsageDataQuery); err == nil {
		keyLen := binary.BigEndian.Uint32(headerBytes[blockIDSize : blockIDSize+keyLenSize])
		valLen := binary.BigEndian.Uint32(headerBytes[blockIDSize+keyLenSize:])
		return baol.dataFile, headerSize, keyLen, valLen, nil
	}
	return nil, 0, 0, 0, fmt.Errorf("failed to detect data file for pointer offset %d (blockID %d)", pointer.Offset, pointer.BlockID)
}

type baolStreamIterator struct {
	baol *BlockAppendOnlyLog
	prefix []byte

	minBlockID uint64
	maxBlockID uint64
	lowerBound []byte
	seekKey    []byte
	seekExact  bool

	curBlockID      uint64
	curBlockEntry   blockIndexEntry
	hasCurrentBlock bool
	nextOffset      int64

	// Prefetched next KV (filled by prefetchNext)
	hasNextKV bool
	nextKey   []byte
	nextValue []byte

	// Current KV (visible via Key/Value)
	key   []byte
	value []byte

	err error
}

// NewIterator returns a streaming iterator over KV pairs in the data log,
// in on-disk order (block by block, entry by entry), honoring the same
// prefix/start lower-bound semantics as PebbleStore.NewIterator.
//
// To position the iterator, it only reads each candidate entry's header and key.
// The value bytes are fetched only after the first in-range KV is identified.
func (baol *BlockAppendOnlyLog) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	baol.mu.RLock()
	if baol.closed {
		baol.mu.RUnlock()
		return &errorIterator{err: ErrClosed}
	}
	minBlockID := baol.minBlockID
	maxBlockID := baol.latestBlockID
	baol.mu.RUnlock()

	it := &baolStreamIterator{
		baol:       baol,
		prefix:     bytes.Clone(prefix),
		minBlockID: minBlockID,
		maxBlockID: maxBlockID,
	}
	if len(start) > 0 {
		if len(prefix) > 0 && !bytes.HasPrefix(start, prefix) {
			it.lowerBound = make([]byte, len(prefix)+len(start))
			copy(it.lowerBound, prefix)
			copy(it.lowerBound[len(prefix):], start)
		} else {
			it.lowerBound = bytes.Clone(start)
		}
	} else if len(prefix) > 0 {
		it.lowerBound = bytes.Clone(prefix)
	}
	it.seekKey = bytes.Clone(it.lowerBound)

	// Determine start block ID.
	startBlockID := uint64(0)
	if len(it.lowerBound) > 0 {
		dt := GetDataTypeFromKey(it.lowerBound)
		if blockID, ok := ParseBlockNumberFromKey(it.lowerBound, dt); ok {
			startBlockID = blockID
		} else {
			// If the lower bound does not encode a block number, fall back to the
			// beginning of the log and filter entries lazily while streaming.
			startBlockID = minBlockID
		}
	} else {
		startBlockID = minBlockID
	}

	if startBlockID == 0 {
		if maxBlockID == 0 {
			// Empty log.
			return it
		}
		// Fallback for legacy datasets where minBlockID isn't set.
		startBlockID = 1
	}
	if minBlockID != 0 && startBlockID < minBlockID {
		startBlockID = minBlockID
	}
	if startBlockID > maxBlockID {
		// Start beyond current range => empty iterator.
		return it
	}

	it.curBlockID = startBlockID
	return it
}

func (it *baolStreamIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.hasNextKV {
		it.prefetchNext()
		if it.err != nil || !it.hasNextKV {
			return false
		}
	}
	// Publish prefetched KV as current.
	it.key = it.nextKey
	it.value = it.nextValue
	it.seekKey = bytes.Clone(it.key)
	it.seekExact = true
	it.hasNextKV = false
	it.nextKey = nil
	it.nextValue = nil
	return true
}

func (it *baolStreamIterator) Error() error { return it.err }

func (it *baolStreamIterator) Key() []byte { return it.key }

func (it *baolStreamIterator) Value() []byte { return it.value }

func (it *baolStreamIterator) Release() {
	it.baol = nil
	it.key = nil
	it.value = nil
	it.nextKey = nil
	it.nextValue = nil
}

func (it *baolStreamIterator) prefetchNext() {
	if it.baol == nil {
		return
	}
	if it.curBlockID == 0 {
		return
	}
	for {
		if it.curBlockID > it.maxBlockID {
			it.hasNextKV = false
			return
		}

		if handled := it.prefetchFromMemoryIndex(); handled {
			return
		}

		// Load current block index entry if needed.
		if !it.hasCurrentBlock {
			indexLookupStart := time.Now()
			it.baol.mu.RLock()
			entry, ok := it.baol.getBlockIndexEntry(it.curBlockID)
			closed := it.baol.closed
			it.baol.mu.RUnlock()
			recordBAOLGetBreakdownStep(&it.baol.iteratorBlockIndexStats, false, time.Since(indexLookupStart))
			if closed {
				it.err = ErrClosed
				it.hasNextKV = false
				return
			}
			if !ok || entry.EndOffset <= entry.StartOffset {
				it.curBlockID++
				continue
			}
			it.curBlockEntry = entry
			it.hasCurrentBlock = true
			it.nextOffset = entry.StartOffset
		}

		// End of this block? move to next.
		if it.nextOffset >= it.curBlockEntry.EndOffset {
			it.curBlockID++
			it.hasCurrentBlock = false
			continue
		}

		key, keyLen, valueLen, bytesRead, err := it.readKeyAt(it.nextOffset, it.curBlockEntry.EndOffset)
		if err != nil {
			it.err = err
			it.hasNextKV = false
			return
		}
		if it.keyBeforeSeek(key) {
			it.nextOffset += bytesRead
			continue
		}
		if len(it.prefix) > 0 && !bytes.HasPrefix(key, it.prefix) {
			it.nextOffset += bytesRead
			continue
		}
		value, err := it.readValueAt(it.nextOffset, keyLen, valueLen)
		if err != nil {
			it.err = err
			it.hasNextKV = false
			return
		}
		it.nextOffset += bytesRead
		it.nextKey = key
		it.nextValue = value
		it.hasNextKV = true
		return
	}
}

func (it *baolStreamIterator) prefetchFromMemoryIndex() bool {
	if it.baol == nil || it.curBlockID == 0 {
		return false
	}

	seekStart := time.Now()
	it.baol.mu.RLock()
	if it.baol.closed {
		it.baol.mu.RUnlock()
		it.err = ErrClosed
		it.hasNextKV = false
		return true
	}
	if it.baol.indexedBlocks == nil {
		it.baol.mu.RUnlock()
		return false
	}
	if _, ok := it.baol.indexedBlocks[it.curBlockID]; !ok {
		it.baol.mu.RUnlock()
		return false
	}

	var elem *skiplist.Element
	if len(it.seekKey) > 0 {
		elem = it.baol.skiplistIndex.Find(string(it.seekKey))
		if it.seekExact && elem != nil && elem.Key().(string) == string(it.seekKey) {
			elem = elem.Next()
		}
	} else {
		elem = it.baol.skiplistIndex.Front()
	}

	for elem != nil {
		keyStr := elem.Key().(string)
		keyBytes := StringToBytes(keyStr)
		if len(it.prefix) > 0 && !bytes.HasPrefix(keyBytes, it.prefix) {
			if bytes.Compare(keyBytes, it.prefix) < 0 {
				elem = elem.Next()
				continue
			}
			recordBAOLGetBreakdownStep(&it.baol.iteratorMemoryIndexStats, true, time.Since(seekStart))
			it.baol.mu.RUnlock()
			it.hasNextKV = false
			return true
		}

		ptr := elem.Value.(*kvPointer)
		if ptr.BlockID < it.curBlockID {
			elem = elem.Next()
			continue
		}
		if ptr.BlockID > it.maxBlockID {
			recordBAOLGetBreakdownStep(&it.baol.iteratorMemoryIndexStats, true, time.Since(seekStart))
			it.baol.mu.RUnlock()
			it.hasNextKV = false
			return true
		}

		recordBAOLGetBreakdownStep(&it.baol.iteratorMemoryIndexStats, true, time.Since(seekStart))
		valueStart := time.Now()
		value, fromCache, err := it.baol.readValueBytesFromPointerWithSource(ptr)
		recordBAOLGetBreakdownStep(&it.baol.iteratorValueReadStats, fromCache, time.Since(valueStart))
		it.baol.mu.RUnlock()
		if err != nil {
			it.err = err
			it.hasNextKV = false
			return true
		}

		it.nextKey = append(it.nextKey[:0], keyBytes...)
		it.nextValue = value
		it.hasNextKV = true
		it.curBlockID = ptr.BlockID
		it.hasCurrentBlock = false
		return true
	}

	recordBAOLGetBreakdownStep(&it.baol.iteratorMemoryIndexStats, true, time.Since(seekStart))
	it.baol.mu.RUnlock()
	it.hasNextKV = false
	return true
}

func (it *baolStreamIterator) keyBeforeSeek(key []byte) bool {
	if len(it.seekKey) == 0 {
		return false
	}
	cmp := bytes.Compare(key, it.seekKey)
	return cmp < 0 || (it.seekExact && cmp == 0)
}

func (it *baolStreamIterator) readKeyAt(offset int64, blockEnd int64) ([]byte, int64, int64, int64, error) {
	const headerSize = int64(blockIDSize + keyLenSize + valueLenSize)
	if offset+headerSize > blockEnd {
		return nil, 0, 0, 0, fmt.Errorf("corrupted entry header at offset %d: remaining=%d < headerSize=%d", offset, blockEnd-offset, headerSize)
	}

	readStart := time.Now()
	hdr := make([]byte, headerSize)
	if _, err := it.baol.readAtWithStats(it.baol.dataFile, hdr, offset, baolDiskIOUsageDataQuery); err != nil {
		recordBAOLGetBreakdownStep(&it.baol.iteratorKeyReadStats, false, time.Since(readStart))
		return nil, 0, 0, 0, err
	}

	keyLenU32 := binary.BigEndian.Uint32(hdr[blockIDSize : blockIDSize+keyLenSize])
	valueLenU32 := binary.BigEndian.Uint32(hdr[blockIDSize+keyLenSize:])
	keyLen := int64(keyLenU32)
	valueLen := int64(valueLenU32)
	maxInt := int64(int(^uint(0) >> 1))
	if keyLen > maxInt || valueLen > maxInt {
		return nil, 0, 0, 0, fmt.Errorf("entry too large at offset %d: keyLen=%d valueLen=%d", offset, keyLen, valueLen)
	}
	entrySize := headerSize + keyLen + valueLen
	if offset+entrySize > blockEnd {
		return nil, 0, 0, 0, fmt.Errorf("corrupted entry at offset %d: entrySize=%d exceeds blockEnd=%d", offset, entrySize, blockEnd)
	}

	key := make([]byte, int(keyLen))
	if _, err := it.baol.readAtWithStats(it.baol.dataFile, key, offset+headerSize, baolDiskIOUsageDataQuery); err != nil {
		recordBAOLGetBreakdownStep(&it.baol.iteratorKeyReadStats, false, time.Since(readStart))
		return nil, 0, 0, 0, err
	}
	recordBAOLGetBreakdownStep(&it.baol.iteratorKeyReadStats, false, time.Since(readStart))
	return key, keyLen, valueLen, entrySize, nil
}

func (it *baolStreamIterator) readValueAt(offset int64, keyLen int64, valueLen int64) ([]byte, error) {
	const headerSize = int64(blockIDSize + keyLenSize + valueLenSize)
	readStart := time.Now()
	value := make([]byte, int(valueLen))
	if _, err := it.baol.readAtWithStats(it.baol.dataFile, value, offset+headerSize+keyLen, baolDiskIOUsageDataQuery); err != nil {
		recordBAOLGetBreakdownStep(&it.baol.iteratorValueReadStats, false, time.Since(readStart))
		return nil, err
	}
	recordBAOLGetBreakdownStep(&it.baol.iteratorValueReadStats, false, time.Since(readStart))
	return value, nil
}
