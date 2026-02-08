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
	"time"

	// Added testing import
	"github.com/ethereum/go-ethereum/common" // Added common import
	"github.com/ethereum/go-ethereum/log"
	"github.com/huandu/skiplist" // Using a third-party skiplist library
)

const (
	BlockdataFileName     = "headerdata.log"
	BlockindexMapFileName = "blockindex.map"
	defaultRecentN        = 100 // Default number of recent blocks to keep indexed in memory
	offsetSize            = 8   // Size of uint64 for offsets
	blockIDSize           = 8   // Assuming block ID is uint64
	keyLenSize            = 4   // Size of uint32 for key length
	valueLenSize          = 4   // Size of uint32 for value length
	// TombstoneMarker is a special value to mark deletion
	TombstoneMarker    = "_D_"
	initialBufferSize  = 4096  // Initial buffer size for writers
	SkipDistanceForGet = 10000 // If the requested blockID is more than this distance behind latestBlockID, return

)

// BlockAppendOnlyLog implements the append-only log store with skiplist indexing for recent blocks.
type BlockAppendOnlyLog struct {
	dirPath string
	log     log.Logger

	dataFilePath  string
	dataFile      *os.File
	dataWriter    *bufio.Writer
	currentOffset int64 // Current end offset of the data file

	indexMapFilePath string
	indexMapFile     *os.File
	blockIndex       map[uint64]blockIndexEntry // In-memory cache of block offsets
	latestBlockID    uint64

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

	mu     sync.RWMutex
	closed bool

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
		dirPath:          dirPath,
		log:              logger.New("module", "appendlog", "path", dirPath),
		dataFilePath:     dataFilePath,
		dataFile:         dataFile,
		dataWriter:       bufio.NewWriterSize(dataFile, initialBufferSize),
		currentOffset:    currentOffset,
		indexMapFilePath: indexMapFilePath,
		indexMapFile:     indexMapFile,
		blockIndex:       make(map[uint64]blockIndexEntry),
		recentN:          recentN,
		recentBlocks:     make([]uint64, 0, recentN), // Initialize empty, will be populated below
		skiplistIndex:    skiplist.New(skiplist.String),
		indexedBlocks:    make(map[uint64]struct{}), // Initialize empty, will be populated below
		indexedBlockKeys: make(map[uint64][]string),
		indexBuffer:      make([]blockIndexEntry, 0, recentN/2),
		indexBufferSize:  recentN / 2,
		indexBufferFlush: make(chan struct{}, 1),

		// opCount:   0,
		// failedOps: 0,
	}

	go baol.backgroundFlush()

	// Load existing block index map
	if err := baol.loadBlockIndex(); err != nil {
		baol.Close()
		return nil, fmt.Errorf("failed to load block index: %w", err)
	}

	// if err := baol.loadSkiplistIndex(); err != nil {
	// 	baol.log.Warn("Failed to load skiplist index, will rebuild", "error", err)
	// }

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
	return baol, nil
}

// Path returns the data directory of the append-only log.
func (baol *BlockAppendOnlyLog) Path() string {
	return baol.dirPath
}

// RecentN returns the number of recent blocks indexed in the skiplist.
func (baol *BlockAppendOnlyLog) RecentN() int {
	return baol.recentN
}

// loadBlockIndex reads the index map file into memory.
func (baol *BlockAppendOnlyLog) loadBlockIndex() error {
	baol.indexMapFile.Seek(0, io.SeekStart) // Go to the beginning
	reader := bufio.NewReader(baol.indexMapFile)
	buf := make([]byte, blockIDSize+offsetSize+offsetSize)
	latestBlock := uint64(0)

	for {
		n, err := io.ReadFull(reader, buf)
		if err == io.EOF {
			break // End of file
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("error reading index map file: %w", err)
		}
		if n != len(buf) {
			// Should not happen with properly written file unless corrupted
			baol.log.Warn("Incomplete record found in index map file", "bytesRead", n)
			break
		}

		entry := blockIndexEntry{
			BlockID:     binary.BigEndian.Uint64(buf[0:blockIDSize]),
			StartOffset: int64(binary.BigEndian.Uint64(buf[blockIDSize : blockIDSize+offsetSize])),
			EndOffset:   int64(binary.BigEndian.Uint64(buf[blockIDSize+offsetSize:])),
		}
		baol.blockIndex[entry.BlockID] = entry
		if entry.BlockID > latestBlock {
			latestBlock = entry.BlockID
		}
		// Removed logic that attempted to populate aol.recentBlocks here.
		// It will be correctly populated in NewAppendOnlyLog after this function returns.
	}
	baol.latestBlockID = latestBlock
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
	// for el := oldSkiplist.Front(); el != nil; el = el.Next() {
	// 	key := el.Key().(string)
	// 	if isHeaderKey(key) && aol.skiplistIndex.Get(key) == nil {
	// 		ptr := el.Value.(*kvPointer)

	// 		valueBytes, err := aol.readValueBytesFromPointer(ptr)
	// 		if err == nil && string(valueBytes) != TombstoneMarker {
	// 			aol.headerIndex.Set(key, ptr)
	// 			aol.log.Debug("Added evicted header key to headerIndex during rebuild", "key", key)
	// 		}
	// 	}
	// }

	baol.log.Debug("Skiplist rebuild complete", "indexedKeys", baol.skiplistIndex.Len(),
		"currentRecentBlocks", baol.recentBlocks)
	return nil
}

// readAndIndexBlock reads all log entries for a given block and adds/updates them in the skiplist.
func (baol *BlockAppendOnlyLog) readAndIndexBlock(indexEntry blockIndexEntry) error {
	size := indexEntry.EndOffset - indexEntry.StartOffset
	if size <= 0 {
		return nil // Empty block
	}

	blockData := make([]byte, size)
	_, err := baol.dataFile.ReadAt(blockData, indexEntry.StartOffset)
	if err != nil {
		return fmt.Errorf("failed to read block data for %d from offset %d: %w", indexEntry.BlockID, indexEntry.StartOffset, err)
	}

	reader := bytes.NewReader(blockData) // Using bytes.NewReader
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

		// Add or update the key in the skiplist
		// The latest occurrence of a key within the indexed blocks wins.
		ptr := &kvPointer{
			Offset:   entryOffset,
			ValueLen: uint32(len(entry.Value)), // Store length for faster Get
			BlockID:  entry.BlockID,            // Store block ID for reference
		}
		baol.setSkiplistEntry(entry.BlockID, entry.Key, ptr)
	}
	return nil
}

func (baol *BlockAppendOnlyLog) recordIndexedKey(blockID uint64, key string) {
	if baol.indexedBlockKeys == nil {
		baol.indexedBlockKeys = make(map[uint64][]string)
	}
	baol.indexedBlockKeys[blockID] = append(baol.indexedBlockKeys[blockID], key)
}

func (baol *BlockAppendOnlyLog) setSkiplistEntry(blockID uint64, key string, ptr *kvPointer) {
	baol.skiplistIndex.Set(key, ptr)
	baol.recordIndexedKey(blockID, key)
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
	// isFirstAppend checks if this is the very first operation on a completely empty log.
	isFirstAppend := baol.latestBlockID == 0 && len(baol.blockIndex) == 0

	// Monotonicity checks:
	// 1. If blockID is 0, it's only allowed if it's the first append on an empty log.
	// if blockID == 0 && !isFirstAppend {
	// 	return fmt.Errorf("block ID 0 can only be used for the first append on an empty log; current latest is %d, and this is not the first append", aol.latestBlockID)
	// }
	// 2. If blockID is not 0 (or it is 0 and isFirstAppend), it must be greater than the current latestBlockID.
	//    (The case blockID == 0 && isFirstAppend means latestBlockID is also 0, so 0 <= 0 is true, but it's allowed).
	if !(blockID == 0 && isFirstAppend) && blockID < baol.latestBlockID {
		return fmt.Errorf("non-monotonic block ID: current latest %d, got %d", baol.latestBlockID, blockID)
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
	existingEntry, exists := baol.blockIndex[blockID]
	if exists {
		startOffset = existingEntry.EndOffset
	} else {
		// New block, append at current end offset
		startOffset = baol.currentOffset
	}
	// startOffset = aol.currentOffset
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
	baol.blockIndex[blockID] = indexEntry
	// baol.latestBlockID = blockID // This is correct for non-empty appends

	if err := baol.writeIndexEntry(baol.indexMapFile, indexEntry); err != nil {
		baol.log.Crit("Failed to write block index entry to file after writing data!", "blockID", blockID, "error", err)
		// Attempt to revert in-memory changes on critical failure
		delete(baol.blockIndex, blockID)
		// Reverting latestBlock
		// and this error path is considered critical and rare.
		return fmt.Errorf("CRITICAL: failed to write index entry for block %d: %w", blockID, err)
	}

	if blockID > baol.latestBlockID {
		baol.latestBlockID = blockID
		baol.updateRecentBlocks(blockID)
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
			ptr := &kvPointer{
				Offset:   entryPos,
				ValueLen: uint32(len(entry.Value)),
				BlockID:  entry.BlockID, // Store block ID for reference
			}
			baol.setSkiplistEntry(entry.BlockID, entry.Key, ptr)
			entryPos += bytesReadThisEntry
		}
	}

	baol.evictOldEntries()
	return nil
}

// updateRecentBlocks adds the new block ID and removes the oldest if the limit is exceeded.
func (baol *BlockAppendOnlyLog) updateRecentBlocks(newBlockID uint64) {
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
	element := baol.skiplistIndex.Get(key)
	if element != nil {
		pointer := element.Value.(*kvPointer)

		valueBytes, err := baol.readValueBytesFromPointer(pointer)
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

	// Check if the key is in the headerIndex
	// Header keys are special and stored in a separate skiplist.
	dataType := GetDataTypeFromKey([]byte(key))
	if blockID, ok := parseBlockNumberFromKey([]byte(key), dataType); ok {
		if blockID > baol.latestBlockID {
			// keyhex := hex.EncodeToString([]byte(key))
			// return "", false, fmt.Errorf("Get: needed blockID lager than aol.lastBlockID, key: %s", keyhex)
			return "", false, fmt.Errorf("Get: needed blockID larger than baol.latestBlockID")
		}
		if baol.latestBlockID-blockID > SkipDistanceForGet {
			return "", true, nil
		}
		if baol.latestBlockID-blockID > uint64(baol.recentN) {
			if _, isIndexed := baol.indexedBlocks[blockID]; !isIndexed {
				if mainEntry, okMain := baol.getBlockIndexEntry(blockID); okMain {
					if val, found, err := baol.findKeyInOneBlock(baol.dataFile, mainEntry, key); err != nil {
						return "", false, err
					} else if found {
						if val == TombstoneMarker {
							return "", true, nil
						}
						return val, true, nil
					}
					return "", false, nil
				}
			}
			return "", false, nil
		}
		return "", false, nil
	}

	return "", false, nil

	//    Iterate all blockIndex entries from newest to oldest.
	allBlockIDs := make([]uint64, 0, len(baol.blockIndex))
	for id := range baol.blockIndex {
		allBlockIDs = append(allBlockIDs, id)
	}
	// Sort from newest to oldest to find the most recent version of the key
	sort.Slice(allBlockIDs, func(i, j int) bool {
		return allBlockIDs[i] > allBlockIDs[j]
	})

	for _, blockIDToScan := range allBlockIDs {
		// If this block IS covered by the skiplist index, then the skiplist.Get() above
		// would have found the key's latest state if it originated from this block or a newer
		// skiplist-indexed block. So, we can skip re-scanning its data here.
		if _, isIndexed := baol.indexedBlocks[blockIDToScan]; isIndexed {
			continue
		}

		// This block is older and not covered by the skiplist. Scan its data.
		indexEntry, ok := baol.getBlockIndexEntry(blockIDToScan) // Still under RLock
		if !ok {
			baol.log.Error("Get: Block ID from allBlockIDs not found in blockIndex", "blockID", blockIDToScan)
			continue // Should not happen
		}

		size := indexEntry.EndOffset - indexEntry.StartOffset
		if size <= 0 {
			continue // Empty block
		}

		blockData := make([]byte, size)
		// dataFile.ReadAt is thread-safe and suitable for use under RLock
		_, err := baol.dataFile.ReadAt(blockData, indexEntry.StartOffset)
		if err != nil {
			baol.log.Error("Get: Failed to read block data for older block", "blockID", blockIDToScan, "key", key, "error", err)
			// Depending on desired behavior, might continue to try other blocks or return error.
			// For now, return error as it indicates a potential issue reading data.
			return "", false, fmt.Errorf("Get: failed to read data for block %d: %w", blockIDToScan, err)
		}

		if blockIDToScan == 0 {
			fmt.Println("key:", key, "keyType:", dataType, "mark")
		}

		reader := bytes.NewReader(blockData)
		// Iterate through entries in this specific block's data
		for reader.Len() > 0 {
			entry, _, readErr := baol.readLogEntry(reader)
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				baol.log.Error("Get: Failed to decode entry in older block", "blockID", blockIDToScan, "key", key, "error", readErr)
				return "", false, fmt.Errorf("Get: failed to decode entry in block %d: %w", blockIDToScan, readErr)
			}

			if entry.Key == key {
				// Found the key in this older block. Since we are iterating newest to oldest,
				// this is the most recent version not covered by the skiplist.
				if entry.Value == TombstoneMarker {
					return "", true, nil // Found tombstone
				}
				return entry.Value, true, nil // Found value
			}
		}
	}

	// Key not found in skiplist or any older blocks
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

	if baol.latestBlockID != 0 {
		return nil
	}

	blockIDForDelete := baol.latestBlockID + 1
	if len(baol.blockIndex) == 0 && baol.latestBlockID == 0 {
		blockIDForDelete = 1
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
	baol.blockIndex[blockIDForDelete] = indexEntry
	baol.latestBlockID = blockIDForDelete // Update latestBlockID

	if err := baol.writeIndexEntry(baol.indexMapFile, indexEntry); err != nil {
		baol.log.Crit("Failed to write block index entry for tombstone to file!", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("CRITICAL: failed to write index entry for tombstone block %d: %w", blockIDForDelete, err)
	}

	if err := baol.dataWriter.Flush(); err != nil {
		baol.log.Error("Failed to flush data writer after tombstone", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to flush data writer for tombstone block %d: %w", blockIDForDelete, err)
	}
	if err := baol.dataFile.Sync(); err != nil {
		baol.log.Error("Failed to sync data file after tombstone", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to sync data file for tombstone block %d: %w", blockIDForDelete, err)
	}
	if err := baol.indexMapFile.Sync(); err != nil {
		baol.log.Error("Failed to sync index map file after tombstone", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to sync index map file for tombstone block %d: %w", blockIDForDelete, err)
	}

	baol.updateRecentBlocks(blockIDForDelete) // This will add the new block
	if _, isIndexed := baol.indexedBlocks[blockIDForDelete]; isIndexed {
		baol.log.Debug("Indexing tombstone in skiplist", "blockID", blockIDForDelete, "key", key)
		// Add tombstone to skiplist
		ptr := &kvPointer{
			Offset:   startOffset, // Offset of this specific logEntry (tombstone)
			ValueLen: uint32(len(TombstoneMarker)),
			BlockID:  blockIDForDelete, // Store block ID for reference
		}
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
	_, readErr = baol.dataFile.ReadAt(blockData, indexEntry.StartOffset)
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
			if _, err := baol.dataFile.ReadAt(blockData, indexEntry.StartOffset); err != nil {
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

	buf := make([]byte, blockIDSize+keyLenSize+valueLenSize)
	binary.BigEndian.PutUint64(buf[0:blockIDSize], blockID)
	binary.BigEndian.PutUint32(buf[blockIDSize:blockIDSize+keyLenSize], keyLen)
	binary.BigEndian.PutUint32(buf[blockIDSize+keyLenSize:], valueLen)

	if _, err := w.Write(buf); err != nil {
		return err
	}
	if _, err := w.Write(keyBytes); err != nil {
		return err
	}
	if _, err := w.Write(valueBytes); err != nil {
		return err
	}
	return nil
}

// writeLogEntriess serializes some log entries to the writer.
// Format: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
func (baol *BlockAppendOnlyLog) writeLogEntries(w io.Writer, blockID uint64, kvs map[string]string) error {
	size := 0
	for key, value := range kvs {
		size += blockIDSize + keyLenSize + valueLenSize + len(key) + len(value)
	}
	buf := make([]byte, size)
	offset := 0
	for key, value := range kvs {
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
	if f, ok := w.(*os.File); ok && f == baol.indexMapFile {
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

// persistIndexMap writes the current block index map to the index map file.
func (baol *BlockAppendOnlyLog) persistIndexMap() error {
	baol.indexMapFile.Seek(0, io.SeekStart)
	writer := bufio.NewWriter(baol.indexMapFile)

	for _, entry := range baol.blockIndex {
		if err := baol.writeIndexEntry(writer, entry); err != nil {
			return fmt.Errorf("failed to write index entry for block %d: %w", entry.BlockID, err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush index map writer: %w", err)
	}

	if err := baol.indexMapFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index map file: %w", err)
	}

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

	for key, value := range kvs {
		if err := baol.writeLogEntry(blockDataBuf, newBlockID, key, value); err != nil {
			return 0, fmt.Errorf("failed to serialize entry for new block %d, key %s: %w", newBlockID, key, err)
		}
	}

	blockBytes := blockDataBuf.Bytes()
	n, err := baol.dataWriter.Write(blockBytes)
	if err != nil {
		baol.log.Error("Failed to write new block data to buffer", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to write new block %d data: %w", newBlockID, err)
	}
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
	baol.blockIndex[newBlockID] = indexEntry
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
	if err := baol.dataFile.Sync(); err != nil {
		baol.log.Error("Failed to sync data file after new block", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to sync data file for new block %d: %w", newBlockID, err)
	}
	if err := baol.indexMapFile.Sync(); err != nil {
		baol.log.Error("Failed to sync index map file after new block", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to sync index map file for new block %d: %w", newBlockID, err)
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
			ptr := &kvPointer{
				Offset:   entryPos,
				ValueLen: uint32(len(entry.Value)),
				BlockID:  newBlockID, // Store block ID for reference
			}
			baol.setSkiplistEntry(entry.BlockID, entry.Key, ptr)
			entryPos += bytesRead
		}
	}

	// evictOldEntries is implicitly handled by updateRecentBlocks if it calls rebuildSkiplist.
	// No explicit call to aol.evictOldEntries() needed here if updateRecentBlocks is comprehensive.

	return newBlockID, nil
}

// getLatestBlockID returns the latest block ID known to the log.
// Caller must hold aol.mu if consistency with a subsequent write is needed.
func (baol *BlockAppendOnlyLog) getLatestBlockID() uint64 {
	return baol.latestBlockID
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
	// logEntry format on disk: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
	// pointer.Offset points to the start of this logEntry.
	// pointer.ValueLen is the length of the string form of the value (can be TombstoneMarker).

	// We need to determine the key's length to correctly calculate the value's starting offset.
	// The keyLen field is located after the blockID field.
	f, headerSize, keyLen, valueLen, err := baol.readHeaderAndLocate(pointer)
	if err != nil {
		return nil, err
	}

	valueBytes := make([]byte, valueLen)
	valueOffset := pointer.Offset + int64(headerSize) + int64(keyLen)
	if _, err := f.ReadAt(valueBytes, valueOffset); err != nil {
		return nil, fmt.Errorf("ReadAt for value failed at offset %d (len %d): %w", valueOffset, valueLen, err)
	}
	return valueBytes, nil
}

// Close flushes buffers and closes open files.
func (baol *BlockAppendOnlyLog) Close() error {
	baol.mu.Lock()
	defer baol.mu.Unlock()

	if baol.closed {
		return ErrClosed // Or your specific error for already closed
	}
	baol.closed = true

	var errs []error // Using a slice to collect multiple errors

	select {
	case baol.indexBufferFlush <- struct{}{}:

	default:

	}

	flushDone := make(chan struct{})
	go func() {
		// Wait for the flush to complete
		time.Sleep(500 * time.Millisecond)
		close(flushDone)
	}()

	select {
	case <-flushDone:

	case <-time.After(2 * time.Second):
		fmt.Println("Warning: Timeout waiting for background flush goroutine to exit")
		errs = append(errs, fmt.Errorf("background flush goroutine exit timeout"))
	}

	// Flush any remaining buffered index entries
	if err := baol.flushIndexBuffer(); err != nil {
		errs = append(errs, fmt.Errorf("failed to flush index buffer on close: %w", err))
	}

	if baol.dataWriter != nil {
		if err := baol.dataWriter.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("failed to flush data writer on close: %w", err))
		}
	}

	// Persist the final state of the index map.
	// This ensures that even if the last Append's persistIndexMap had issues
	// or if there were no appends since the last persist, the current map is written.
	if err := baol.persistIndexMap(); err != nil {
		errs = append(errs, fmt.Errorf("failed to persist index map on close: %w", err))
	}

	if baol.dataFile != nil {
		// Sync data file before closing, to ensure all writes are flushed.
		if err := baol.dataFile.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("failed to sync data file on close: %w", err))
		}
		if err := baol.dataFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close data file: %w", err))
		}
		baol.dataFile = nil // Mark as closed
	}

	if baol.indexMapFile != nil {
		if err := baol.indexMapFile.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("failed to sync index map file on close: %w", err))
		}
		if err := baol.indexMapFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close index map file: %w", err))
		}
		baol.indexMapFile = nil
	}

	if baol.indexMapFile != nil {
		if err := baol.indexMapFile.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("failed to sync index map file on close: %w", err))
		}
		if err := baol.indexMapFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close index map file: %w", err))
		}
		baol.indexMapFile = nil
	}

	close(baol.indexBufferFlush)
	baol.indexBufferFlush = nil

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
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-baol.indexBufferFlush:
			baol.flushIndexBuffer()
		case <-ticker.C:
			baol.flushIndexBuffer()
		}

		baol.mu.RLock()
		closed := baol.closed
		baol.mu.RUnlock()

		if closed {
			return
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

func (baol *BlockAppendOnlyLog) flushIndexBuffer() error {
	baol.indexBufferMu.Lock()

	if len(baol.indexBuffer) == 0 {
		baol.indexBufferMu.Unlock()
		return nil
	}

	entries := make([]blockIndexEntry, len(baol.indexBuffer))
	copy(entries, baol.indexBuffer)

	baol.indexBuffer = baol.indexBuffer[:0]
	baol.indexBufferMu.Unlock()

	writer := bufio.NewWriter(baol.indexMapFile)
	for _, entry := range entries {
		buf := make([]byte, blockIDSize+offsetSize+offsetSize)
		binary.BigEndian.PutUint64(buf[0:blockIDSize], entry.BlockID)
		binary.BigEndian.PutUint64(buf[blockIDSize:blockIDSize+offsetSize], uint64(entry.StartOffset))
		binary.BigEndian.PutUint64(buf[blockIDSize+offsetSize:], uint64(entry.EndOffset))

		if _, err := writer.Write(buf); err != nil {
			baol.log.Error("Failed to write index entry during flush", "error", err)
			return err
		}
	}

	// Flush and sync
	if err := writer.Flush(); err != nil {
		baol.log.Error("Failed to flush index buffer writer", "error", err)
		return err
	}

	if err := baol.indexMapFile.Sync(); err != nil {
		baol.log.Error("Failed to sync index file after buffer flush", "error", err)
		return err
	}

	baol.log.Debug("Successfully flushed index buffer", "entries", len(entries))
	return nil
}

func (baol *BlockAppendOnlyLog) FlushIndexBuffer() error {
	return baol.flushIndexBuffer()
}

// getBlockIndexEntry retrieves the block index entry for a given block ID.
func (baol *BlockAppendOnlyLog) getBlockIndexEntry(blockID uint64) (blockIndexEntry, bool) {

	entry, ok := baol.blockIndex[blockID]
	if ok {
		return entry, true
	}
	baol.indexBufferMu.Lock()
	for _, entry := range baol.indexBuffer {
		if entry.BlockID == blockID {
			baol.indexBufferMu.Unlock()
			return entry, true
		}
	}
	baol.indexBufferMu.Unlock()
	return blockIndexEntry{}, false
}

// readAndIndexBlockFrom reads a block from the data file (main or late) based on the index entry
func (baol *BlockAppendOnlyLog) readAndIndexBlockFrom(indexEntry blockIndexEntry) error {
	size := indexEntry.EndOffset - indexEntry.StartOffset
	if size <= 0 {
		return nil
	}

	f := baol.dataFile

	blockData := make([]byte, size)
	if _, err := f.ReadAt(blockData, indexEntry.StartOffset); err != nil {
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

		ptr := &kvPointer{
			Offset:   entryOffset,
			ValueLen: uint32(len(entry.Value)),
			BlockID:  entry.BlockID,
		}
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
	if _, err := f.ReadAt(blockData, indexEntry.StartOffset); err != nil {
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
		if mainEntry, ok := baol.blockIndex[pointer.BlockID]; ok {
			if pointer.Offset >= mainEntry.StartOffset && pointer.Offset+int64(headerSize) <= mainEntry.EndOffset {
				if _, err := baol.dataFile.ReadAt(headerBytes, pointer.Offset); err == nil {
					keyLen := binary.BigEndian.Uint32(headerBytes[blockIDSize : blockIDSize+keyLenSize])
					valLen := binary.BigEndian.Uint32(headerBytes[blockIDSize+keyLenSize:])
					return baol.dataFile, headerSize, keyLen, valLen, nil
				}
			}
		}
	}
	if _, err := baol.dataFile.ReadAt(headerBytes, pointer.Offset); err == nil {
		keyLen := binary.BigEndian.Uint32(headerBytes[blockIDSize : blockIDSize+keyLenSize])
		valLen := binary.BigEndian.Uint32(headerBytes[blockIDSize+keyLenSize:])
		return baol.dataFile, headerSize, keyLen, valLen, nil
	}
	return nil, 0, 0, 0, fmt.Errorf("failed to detect data file for pointer offset %d (blockID %d)", pointer.Offset, pointer.BlockID)
}
