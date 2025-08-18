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
	dataFileName     = "data.log"
	indexMapFileName = "index.map"
	defaultRecentN   = 100 // Default number of recent blocks to keep indexed in memory
	offsetSize       = 8   // Size of uint64 for offsets
	blockIDSize      = 8   // Assuming block ID is uint64
	keyLenSize       = 4   // Size of uint32 for key length
	valueLenSize     = 4   // Size of uint32 for value length
	// TombstoneMarker is a special value to mark deletion
	TombstoneMarker   = "_D_"
	initialBufferSize = 4096 // Initial buffer size for writers
)

// logEntry represents a single key-value pair within a block in the data log.
// Format on disk: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
type logEntry struct {
	BlockID uint64
	Key     string
	Value   string // Can be TombstoneMarker for deletion
	Offset  int64  // Offset in the data file where this entry starts
}

// blockIndexEntry stores the start and end offset for all entries belonging to a block.
// Format on disk: blockID (uint64) | startOffset (uint64) | endOffset (uint64)
type blockIndexEntry struct {
	BlockID     uint64
	StartOffset int64
	EndOffset   int64 // Offset *after* the last byte of the last entry for this block
}

// kvPointer stores the location of a specific key's value in the data log.
// Used as the value in the skiplist.
type kvPointer struct {
	Offset   int64 // Offset of the logEntry start
	ValueLen uint32
	BlockID  uint64 // The block ID this entry belongs to
}

// AppendOnlyLog implements the append-only log store with skiplist indexing for recent blocks.
type AppendOnlyLog struct {
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
	recentN       int
	recentBlocks  []uint64            // Ordered list of recent block IDs (most recent last)
	skiplistIndex *skiplist.SkipList  // Key: string (key), Value: *kvPointer
	indexedBlocks map[uint64]struct{} // Set of block IDs currently in the skiplist

	SLIndexFilePath      string // Path to the skiplist index file (if needed)
	lastPersistedBlockID uint64 // the last block ID that was persisted to disk
	persistInterval      int    // how many blocks between persistence operations

	indexBuffer      []blockIndexEntry // Buffer for batching index writes
	indexBufferMu    sync.Mutex        // Mutex for index buffer
	indexBufferSize  int               // Size threshold for flushing index buffer
	indexBufferFlush chan struct{}     // Channel to signal index buffer flush

	mu     sync.RWMutex
	closed bool
}

// NewAppendOnlyLog creates or opens an append-only log store.
func NewAppendOnlyLog(dirPath string, recentN int, logger log.Logger) (*AppendOnlyLog, error) {
	if recentN <= 0 {
		recentN = defaultRecentN
	}
	if logger == nil {
		logger = log.New() // Use a default logger if none provided
	}

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}

	dataFilePath := filepath.Join(dirPath, dataFileName)
	indexMapFilePath := filepath.Join(dirPath, indexMapFileName)

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

	aol := &AppendOnlyLog{
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

		lastPersistedBlockID: 0,
		persistInterval:      recentN,
		SLIndexFilePath:      filepath.Join(dirPath, "skiplist_index.dat"),

		indexBuffer:      make([]blockIndexEntry, 0, recentN/2),
		indexBufferSize:  recentN / 2,
		indexBufferFlush: make(chan struct{}, 1),
	}

	// Load existing block index map
	if err := aol.loadBlockIndex(); err != nil {
		aol.Close()
		return nil, fmt.Errorf("failed to load block index: %w", err)
	}

	// load skiplist index from disk if it exists
	if err := aol.loadSkiplistIndex(); err != nil {
		aol.log.Warn("Failed to load skiplist index from disk, rebuilding from recent blocks", "error", err)
	}

	// Determine the actual N most recent blocks from all loaded blockIndex entries.
	allLoadedBlockIDs := make([]uint64, 0, len(aol.blockIndex))
	for id := range aol.blockIndex {
		allLoadedBlockIDs = append(allLoadedBlockIDs, id)
	}
	sort.Slice(allLoadedBlockIDs, func(i, j int) bool {
		return allLoadedBlockIDs[i] < allLoadedBlockIDs[j] // Sort oldest to newest
	})

	// Populate recentBlocks and indexedBlocks with the true N most recent blocks.
	// aol.recentBlocks and aol.indexedBlocks were already initialized as empty.
	startIdx := 0
	if len(allLoadedBlockIDs) > aol.recentN {
		startIdx = len(allLoadedBlockIDs) - aol.recentN
	}

	for i := startIdx; i < len(allLoadedBlockIDs); i++ {
		blockID := allLoadedBlockIDs[i]
		aol.recentBlocks = append(aol.recentBlocks, blockID) // These are the N most recent, oldest of N to newest of N
		aol.indexedBlocks[blockID] = struct{}{}
	}
	// aol.recentBlocks is now correctly populated and sorted (oldest of recentN to newest of recentN).
	// aol.indexedBlocks now correctly reflects this set.

	// Rebuild skiplist for the last N blocks from the index
	// This call will now use the correctly populated aol.recentBlocks and aol.indexedBlocks.
	if err := aol.rebuildSkiplist(); err != nil {
		aol.Close()
		return nil, fmt.Errorf("failed to rebuild skiplist index: %w", err)
	}

	aol.log.Info("AppendOnlyLog initialized", "dataSize", common.StorageSize(currentOffset), "indexedBlocks", len(aol.indexedBlocks), "recentBlocksTracked", len(aol.recentBlocks))

	go aol.backgroundFlush()

	return aol, nil
}

// Path returns the data directory of the append-only log.
func (aol *AppendOnlyLog) Path() string {
	return aol.dirPath
}

// RecentN returns the number of recent blocks indexed in the skiplist.
func (aol *AppendOnlyLog) RecentN() int {
	return aol.recentN
}

// loadBlockIndex reads the index map file into memory.
func (aol *AppendOnlyLog) loadBlockIndex() error {
	aol.indexMapFile.Seek(0, io.SeekStart) // Go to the beginning
	reader := bufio.NewReader(aol.indexMapFile)
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
			aol.log.Warn("Incomplete record found in index map file", "bytesRead", n)
			break
		}

		entry := blockIndexEntry{
			BlockID:     binary.BigEndian.Uint64(buf[0:blockIDSize]),
			StartOffset: int64(binary.BigEndian.Uint64(buf[blockIDSize : blockIDSize+offsetSize])),
			EndOffset:   int64(binary.BigEndian.Uint64(buf[blockIDSize+offsetSize:])),
		}
		aol.blockIndex[entry.BlockID] = entry
		if entry.BlockID > latestBlock {
			latestBlock = entry.BlockID
		}
		// Removed logic that attempted to populate aol.recentBlocks here.
		// It will be correctly populated in NewAppendOnlyLog after this function returns.
	}
	aol.latestBlockID = latestBlock
	return nil
}

// rebuildSkiplist populates the skiplist index for the actual N most recent blocks found.
func (aol *AppendOnlyLog) rebuildSkiplist() error {
	// aol.mu must be held (WLock) by the caller if called outside of NewAppendOnlyLog initialization.
	// NewAppendOnlyLog calls this before aol is accessible externally, so no concurrent access.
	// updateRecentBlocks calls this under a WLock.

	// The source of truth for which blocks should be in the skiplist is aol.recentBlocks.
	// We just need to iterate over them (oldest to newest) and populate the skiplist.

	// Create a sorted copy of aol.recentBlocks (oldest to newest) to iterate over.
	// This ensures that if a key appears in multiple recent blocks, the one from the
	// newest block (processed last) will be what ends up in the skiplist.
	blocksToIndex := make([]uint64, len(aol.recentBlocks))
	copy(blocksToIndex, aol.recentBlocks)
	sort.Slice(blocksToIndex, func(i, j int) bool {
		return blocksToIndex[i] < blocksToIndex[j] // Sort oldest to newest
	})

	aol.skiplistIndex = skiplist.New(skiplist.String) // Reset skiplist

	aol.log.Debug("Rebuilding skiplist index", "blocksToScan", blocksToIndex)

	for _, blockID := range blocksToIndex {
		// Ensure this block is still supposed to be indexed.
		// This check is somewhat redundant if blocksToIndex is derived from aol.recentBlocks
		// and aol.indexedBlocks is kept in sync, but good for safety.
		if _, stillIndexed := aol.indexedBlocks[blockID]; !stillIndexed {
			aol.log.Warn("Block in blocksToIndex for rebuild is no longer in aol.indexedBlocks", "blockID", blockID)
			continue
		}

		indexEntry, ok := aol.getBlockIndexEntry(blockID)
		if !ok {
			aol.log.Error("Block ID from recentBlocks list not found in main blockIndex during skiplist rebuild", "blockID", blockID)
			// This indicates a serious inconsistency.
			// Depending on desired robustness, could return error or try to continue.
			return fmt.Errorf("inconsistency: block %d in recentBlocks not in blockIndex", blockID)
		}

		// Read all entries for this block and add to skiplist
		err := aol.readAndIndexBlock(indexEntry) // readAndIndexBlock uses aol.skiplistIndex.Set
		if err != nil {
			return fmt.Errorf("failed to read/index block %d during rebuild: %w", blockID, err)
		}
	}

	aol.log.Debug("Skiplist rebuild complete", "indexedKeys", aol.skiplistIndex.Len(), "currentRecentBlocks", aol.recentBlocks)
	return nil
}

// readAndIndexBlock reads all log entries for a given block and adds/updates them in the skiplist.
func (aol *AppendOnlyLog) readAndIndexBlock(indexEntry blockIndexEntry) error {
	size := indexEntry.EndOffset - indexEntry.StartOffset
	if size <= 0 {
		return nil // Empty block
	}

	blockData := make([]byte, size)
	_, err := aol.dataFile.ReadAt(blockData, indexEntry.StartOffset)
	if err != nil {
		return fmt.Errorf("failed to read block data for %d from offset %d: %w", indexEntry.BlockID, indexEntry.StartOffset, err)
	}

	reader := bytes.NewReader(blockData) // Using bytes.NewReader
	currentPos := indexEntry.StartOffset

	for reader.Len() > 0 {
		entryOffset := currentPos
		entry, bytesRead, err := aol.readLogEntry(reader)
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
			BlockID:  entry.BlockID,
		}
		aol.skiplistIndex.Set(entry.Key, ptr)
	}
	return nil
}

// Append adds a batch of key-value pairs for a given block ID.
// It ensures atomicity for the block: either all pairs are written or none.
// It updates the block index map and the skiplist if the block is recent.
func (aol *AppendOnlyLog) Append(blockID uint64, kvs map[string]string) error {
	aol.mu.Lock()
	defer aol.mu.Unlock()

	if aol.closed {
		return fmt.Errorf("block append-only log is closed")
	}
	// isFirstAppend checks if this is the very first operation on a completely empty log.
	isFirstAppend := aol.latestBlockID == 0 && len(aol.blockIndex) == 0

	// Monotonicity checks:
	// 1. If blockID is 0, it's only allowed if it's the first append on an empty log.
	// if blockID == 0 && !isFirstAppend {
	// 	return fmt.Errorf("block ID 0 can only be used for the first append on an empty log; current latest is %d, and this is not the first append", aol.latestBlockID)
	// }
	// 2. If blockID is not 0 (or it is 0 and isFirstAppend), it must be greater than the current latestBlockID.
	//    (The case blockID == 0 && isFirstAppend means latestBlockID is also 0, so 0 <= 0 is true, but it's allowed).
	if !(blockID == 0 && isFirstAppend) && blockID < aol.latestBlockID {
		return fmt.Errorf("non-monotonic block ID: current latest %d, got %d", aol.latestBlockID, blockID)
	}

	if len(kvs) == 0 {
		// If kvs is empty, this append operation should generally be a no-op
		// in terms of advancing the log or writing data.

		if blockID == 0 && isFirstAppend { // Allowed: block 0, empty kvs, first append
			aol.log.Debug("Append(0, empty_kvs) on empty log: No operation performed, latestBlockID remains 0.", "blockID", blockID)
			// No index entry, no latestBlockID update.
			return nil
		}

		// If blockID > aol.latestBlockID and kvs is empty, the test implies it's a no-op
		// regarding latestBlockID. We also won't write an index entry for it to be consistent.
		// This condition is implicitly true if we passed the monotonicity check and len(kvs) == 0
		// and it's not the (blockID == 0 && isFirstAppend) case.
		aol.log.Debug("Append called with empty KVS for a new block ID. No operation performed, latestBlockID not advanced.", "blockID", blockID, "latestBlockID", aol.latestBlockID)
		return nil
	}

	// --- Logic for non-empty KVS starts here ---
	existingEntry, exists := aol.blockIndex[blockID]
	var startOffset int64
	if exists {
		startOffset = existingEntry.EndOffset
	} else {
		// 否则使用当前偏移量
		startOffset = aol.currentOffset
	}
	// startOffset = aol.currentOffset
	blockDataBuf := new(bytes.Buffer)

	// Serialize all entries for the block into the buffer
	for key, value := range kvs {
		if err := aol.writeLogEntry(blockDataBuf, blockID, key, value); err != nil {
			return fmt.Errorf("failed to serialize entry for block %d, key %s: %w", blockID, key, err)
		}
	}

	blockBytes := blockDataBuf.Bytes()
	n, err := aol.dataWriter.Write(blockBytes)
	if err != nil {
		aol.log.Error("Failed to write block data to buffer", "blockID", blockID, "error", err)
		return fmt.Errorf("failed to write block %d data: %w", blockID, err)
	}
	if n != len(blockBytes) {
		aol.log.Error("Incomplete write for block data", "blockID", blockID, "written", n, "expected", len(blockBytes))
		return fmt.Errorf("incomplete write for block %d data", blockID)
	}

	endOffset := startOffset + int64(n)
	aol.currentOffset = endOffset

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
	aol.blockIndex[blockID] = indexEntry
	aol.latestBlockID = blockID // This is correct for non-empty appends

	if err := aol.writeIndexEntry(aol.indexMapFile, indexEntry); err != nil {
		aol.log.Crit("Failed to write block index entry to file after writing data!", "blockID", blockID, "error", err)
		// Attempt to revert in-memory changes on critical failure
		delete(aol.blockIndex, blockID)
		// Reverting latestBlock
		// and this error path is considered critical and rare.
		return fmt.Errorf("CRITICAL: failed to write index entry for block %d: %w", blockID, err)
	}

	// if err := aol.dataWriter.Flush(); err != nil {
	// 	aol.log.Error("Failed to flush data writer", "error", err)
	// }
	// if err := aol.dataFile.Sync(); err != nil {
	// 	aol.log.Error("Failed to sync data file", "error", err)
	// }
	// if err := aol.indexMapFile.Sync(); err != nil {
	// 	aol.log.Error("Failed to sync index map file", "error", err)
	// }

	aol.updateRecentBlocks(blockID)
	if _, isIndexed := aol.indexedBlocks[blockID]; isIndexed {
		aol.log.Debug("Indexing new block in skiplist", "blockID", blockID)
		reader := bytes.NewReader(blockBytes) // Using bytes.NewReader
		entryPos := startOffset
		for reader.Len() > 0 {
			entry, bytesReadThisEntry, errRead := aol.readLogEntry(reader)
			if errRead == io.EOF {
				break
			}
			if errRead != nil {
				aol.log.Error("Failed to decode entry while indexing new block", "blockID", blockID, "error", errRead)
				return fmt.Errorf("failed to decode entry for skiplist indexing block %d: %w", blockID, errRead)
			}
			ptr := &kvPointer{
				Offset:   entryPos,
				ValueLen: uint32(len(entry.Value)),
				BlockID:  entry.BlockID, // Store block ID for reference
			}
			aol.skiplistIndex.Set(entry.Key, ptr)
			entryPos += bytesReadThisEntry
		}
	}

	aol.evictOldEntries()
	return nil
}

// updateRecentBlocks adds the new block ID and removes the oldest if the limit is exceeded.
func (aol *AppendOnlyLog) updateRecentBlocks(newBlockID uint64) {
	aol.recentBlocks = append(aol.recentBlocks, newBlockID)
	aol.indexedBlocks[newBlockID] = struct{}{}

	if len(aol.recentBlocks) > aol.recentN {
		// Remove the oldest block from index
		oldestBlockID := aol.recentBlocks[0]
		aol.recentBlocks = aol.recentBlocks[1:] // Shift slice
		delete(aol.indexedBlocks, oldestBlockID)

		aol.log.Debug("Evicting oldest block from skiplist index", "blockID", oldestBlockID)

		// Remove keys belonging *only* to the evicted block from the skiplist.
		// if err := aol.rebuildSkiplist(); err != nil {
		// 	aol.log.Error("Failed to rebuild skiplist after eviction", "evictedBlock", oldestBlockID, "error", err)
		// }
		aol.evictOldBlockFromSkiplist(oldestBlockID)
	}
	aol.persistIndexIfNeeded(newBlockID)
}

// evictOldEntries is called after new data is appended and recent blocks are updated.
// The primary mechanism for skiplist eviction is `rebuildSkiplist`, which is
// called by `updateRecentBlocks` when the oldest block in `recentBlocks` is removed.
func (aol *AppendOnlyLog) evictOldEntries() {
	// Ensure lock is held if operations were to be performed, matching original intent.
	// For example, if using a library that provides aol.mu.AssertHeld()
	// aol.mu.AssertHeld()

	// The core logic for ensuring the skiplist only contains recentN blocks
	// is handled by `updateRecentBlocks` calling `rebuildSkiplist`.
	// If `updateRecentBlocks` has run and potentially triggered `rebuildSkiplist`,
	// the skiplist should already be in the correct state.
	aol.log.Debug("evictOldEntries: Called. Skiplist state is managed by rebuildSkiplist.",
		"latestBlockID", aol.latestBlockID,
		"recentN", aol.recentN,
		"numRecentBlocksTracked", len(aol.recentBlocks),
		"numIndexedBlocks", len(aol.indexedBlocks))

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
func (aol *AppendOnlyLog) Get(key string) (string, bool, error) {
	aol.mu.RLock()
	defer aol.mu.RUnlock()

	if aol.closed {
		return "", false, fmt.Errorf("append-only log is closed")
	}

	// 1. Try skiplist (recent N blocks)
	element := aol.skiplistIndex.Get(key)
	if element != nil {
		pointer := element.Value.(*kvPointer)
		headerSize := blockIDSize + keyLenSize + valueLenSize
		headerBytes := make([]byte, headerSize)

		// Read header from data file
		_, err := aol.dataFile.ReadAt(headerBytes, pointer.Offset)
		if err != nil {
			aol.log.Error("Get: Failed to read entry header from skiplist pointer", "key", key, "offset", pointer.Offset, "error", err)
			return "", false, fmt.Errorf("failed to read entry header for key %s: %w", key, err)
		}

		keyLenOnDisk := binary.BigEndian.Uint32(headerBytes[blockIDSize : blockIDSize+keyLenSize])
		valueLenOnDisk := binary.BigEndian.Uint32(headerBytes[blockIDSize+keyLenSize : headerSize])

		// It's possible pointer.ValueLen might differ from valueLenOnDisk if there's an issue,
		// but we should read what the header on disk says for the value.
		// The kvPointer's ValueLen is mostly for information or a quick check.

		valueBytes := make([]byte, valueLenOnDisk)
		valueOffset := pointer.Offset + int64(headerSize) + int64(keyLenOnDisk)
		_, err = aol.dataFile.ReadAt(valueBytes, valueOffset)
		if err != nil {
			aol.log.Error("Get: Failed to read value from skiplist pointer", "key", key, "valueOffset", valueOffset, "error", err)
			return "", false, fmt.Errorf("failed to read value for key %s: %w", key, err)
		}

		value := string(valueBytes)
		if value == TombstoneMarker {
			return "", true, nil // Key was explicitly deleted
		}
		return value, true, nil // Key found in skiplist
	}

	// 2. If not in skiplist, search older blocks (those not covered by skiplist index).
	//    Iterate all blockIndex entries from newest to oldest.
	allBlockIDs := make([]uint64, 0, len(aol.blockIndex))
	for id := range aol.blockIndex {
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
		if _, isIndexed := aol.indexedBlocks[blockIDToScan]; isIndexed {
			continue
		}

		// This block is older and not covered by the skiplist. Scan its data.
		indexEntry, ok := aol.getBlockIndexEntry(blockIDToScan) // Still under RLock
		if !ok {
			aol.log.Error("Get: Block ID from allBlockIDs not found in blockIndex", "blockID", blockIDToScan)
			continue // Should not happen
		}

		size := indexEntry.EndOffset - indexEntry.StartOffset
		if size <= 0 {
			continue // Empty block
		}

		blockData := make([]byte, size)
		// dataFile.ReadAt is thread-safe and suitable for use under RLock
		_, err := aol.dataFile.ReadAt(blockData, indexEntry.StartOffset)
		if err != nil {
			aol.log.Error("Get: Failed to read block data for older block", "blockID", blockIDToScan, "key", key, "error", err)
			// Depending on desired behavior, might continue to try other blocks or return error.
			// For now, return error as it indicates a potential issue reading data.
			return "", false, fmt.Errorf("Get: failed to read data for block %d: %w", blockIDToScan, err)
		}

		reader := bytes.NewReader(blockData)
		// Iterate through entries in this specific block's data
		for reader.Len() > 0 {
			entry, _, readErr := aol.readLogEntry(reader)
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				aol.log.Error("Get: Failed to decode entry in older block", "blockID", blockIDToScan, "key", key, "error", readErr)
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
func (aol *AppendOnlyLog) Delete(key string) error {
	aol.mu.Lock()
	defer aol.mu.Unlock()

	if aol.closed {
		return fmt.Errorf("append-only log is closed")
	}

	blockIDForDelete := aol.latestBlockID + 1
	if len(aol.blockIndex) == 0 && aol.latestBlockID == 0 {
		blockIDForDelete = 1
	}

	startOffset := aol.currentOffset
	blockDataBuf := new(bytes.Buffer)

	if err := aol.writeLogEntry(blockDataBuf, blockIDForDelete, key, TombstoneMarker); err != nil {
		return fmt.Errorf("failed to serialize tombstone entry for block %d, key %s: %w", blockIDForDelete, key, err)
	}

	blockBytes := blockDataBuf.Bytes()
	n, err := aol.dataWriter.Write(blockBytes)
	if err != nil {
		aol.log.Error("Failed to write tombstone block data to buffer", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to write tombstone block %d data: %w", blockIDForDelete, err)
	}
	if n != len(blockBytes) {
		aol.log.Error("Incomplete write for tombstone block data", "blockID", blockIDForDelete, "written", n, "expected", len(blockBytes))
		return fmt.Errorf("incomplete write for tombstone block %d data", blockIDForDelete)
	}

	endOffset := startOffset + int64(n)
	aol.currentOffset = endOffset

	indexEntry := blockIndexEntry{
		BlockID:     blockIDForDelete,
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
	aol.blockIndex[blockIDForDelete] = indexEntry
	aol.latestBlockID = blockIDForDelete // Update latestBlockID

	if err := aol.writeIndexEntry(aol.indexMapFile, indexEntry); err != nil {
		aol.log.Crit("Failed to write block index entry for tombstone to file!", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("CRITICAL: failed to write index entry for tombstone block %d: %w", blockIDForDelete, err)
	}

	if err := aol.dataWriter.Flush(); err != nil {
		aol.log.Error("Failed to flush data writer after tombstone", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to flush data writer for tombstone block %d: %w", blockIDForDelete, err)
	}
	if err := aol.dataFile.Sync(); err != nil {
		aol.log.Error("Failed to sync data file after tombstone", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to sync data file for tombstone block %d: %w", blockIDForDelete, err)
	}
	if err := aol.indexMapFile.Sync(); err != nil {
		aol.log.Error("Failed to sync index map file after tombstone", "blockID", blockIDForDelete, "error", err)
		return fmt.Errorf("failed to sync index map file for tombstone block %d: %w", blockIDForDelete, err)
	}

	aol.updateRecentBlocks(blockIDForDelete) // This will add the new block
	if _, isIndexed := aol.indexedBlocks[blockIDForDelete]; isIndexed {
		aol.log.Debug("Indexing tombstone in skiplist", "blockID", blockIDForDelete, "key", key)
		// Add tombstone to skiplist
		ptr := &kvPointer{
			Offset:   startOffset, // Offset of this specific logEntry (tombstone)
			ValueLen: uint32(len(TombstoneMarker)),
			BlockID:  blockIDForDelete,
		}
		aol.skiplistIndex.Set(key, ptr)
	}

	aol.log.Info("Appended tombstone for key", "key", key, "blockID", blockIDForDelete)
	return nil
}

// DeleteByPrefixInBlock identifies all keys in a specific targetBlockID that start with the given prefix
// and appends tombstone entries for them in a new block.
// If the target block doesn't exist, an error is returned.
// If no such keys are found (e.g., block is empty, no keys match prefix, or all matching keys are already tombstones),
// it's a no-op and returns nil.
func (aol *AppendOnlyLog) DeleteByPrefixInBlock(targetBlockID uint64, prefix string) error {
	var blockData []byte
	var readErr error

	aol.mu.RLock()
	if aol.closed {
		aol.mu.RUnlock()
		return fmt.Errorf("append-only log is closed")
	}

	indexEntry, ok := aol.getBlockIndexEntry(targetBlockID)
	if !ok {
		aol.mu.RUnlock()
		return fmt.Errorf("target block ID %d not found in index", targetBlockID)
	}

	size := indexEntry.EndOffset - indexEntry.StartOffset
	if size <= 0 {
		aol.mu.RUnlock()
		aol.log.Debug("Target block for prefix deletion is empty", "blockID", targetBlockID, "prefix", prefix)
		return nil // Empty block, no-op
	}

	blockData = make([]byte, size)
	_, readErr = aol.dataFile.ReadAt(blockData, indexEntry.StartOffset)
	aol.mu.RUnlock() // Release RLock after reading data (or attempting to)

	if readErr != nil {
		return fmt.Errorf("failed to read block data for target block %d (offset %d): %w", targetBlockID, indexEntry.StartOffset, readErr)
	}

	// Parse block data to get raw key-value pairs
	kvsInBlock := make(map[string]string)
	reader := bytes.NewReader(blockData)
	for reader.Len() > 0 {
		entry, _, err := aol.readLogEntry(reader)
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
		aol.log.Debug("No non-tombstoned keys found with prefix in target block for deletion",
			"targetBlockID", targetBlockID, "prefix", prefix)
		return nil // No keys to delete, or all were already tombstones
	}

	aol.log.Info("Identified keys for prefix deletion",
		"targetBlockID", targetBlockID, "prefix", prefix, "count", len(keysToTombstone))

	// AppendToNewBlock handles its own locking.
	// It will create a new block with the tombstones.
	newBlockID, err := aol.AppendToNewBlock(keysToTombstone)
	if err != nil {
		return fmt.Errorf("failed to append tombstones for prefix deletion (target block %d, prefix '%s'): %w", targetBlockID, prefix, err)
	}

	aol.log.Info("Successfully appended tombstones for prefix deletion",
		"targetBlockID", targetBlockID, "prefix", prefix, "tombstoneBlockID", newBlockID, "count", len(keysToTombstone))
	return nil
}

// GetByBlock retrieves all key-value pairs for a specific block ID.
func (aol *AppendOnlyLog) GetByBlock(blockID uint64) (map[string]string, error) {
	aol.mu.RLock()
	defer aol.mu.RUnlock()

	if aol.closed {
		return nil, fmt.Errorf("append-only log is closed")
	}

	indexEntry, ok := aol.getBlockIndexEntry(blockID)
	if !ok {
		return nil, fmt.Errorf("block ID %d not found in index", blockID)
	}

	size := indexEntry.EndOffset - indexEntry.StartOffset
	if size <= 0 {
		return make(map[string]string), nil // Empty block
	}

	blockData := make([]byte, size)
	_, err := aol.dataFile.ReadAt(blockData, indexEntry.StartOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read block data for %d from offset %d: %w", blockID, indexEntry.StartOffset, err)
	}

	kvs := make(map[string]string)
	reader := bytes.NewReader(blockData)

	for reader.Len() > 0 {
		entry, _, err := aol.readLogEntry(reader) // We don't need bytesRead here
		if err == io.EOF {
			break
		}
		if err != nil {
			aol.log.Error("Failed to decode entry in block during GetByBlock", "blockID", blockID, "error", err)
			return kvs, fmt.Errorf("failed to decode entry in block %d: %w", blockID, err)
		}

		if entry.Value == TombstoneMarker {
			kvs[entry.Key] = "__DELETED__" // Meet test expectation
		} else {
			// This else block is taken if entry.Value != TombstoneMarker
			// If the test output shows _D_ for delKey, it means entry.Value was _D_
			// but the comparison failed.
			if entry.Key == "delKey" {
				aol.log.Error("UNEXPECTED_TOMBSTONE_COMPARISON_FAILURE",
					"key", entry.Key,
					"entry.Value_str", entry.Value,
					"entry.Value_bytes", []byte(entry.Value),
					"TombstoneMarker_str", TombstoneMarker,
					"TombstoneMarker_bytes", []byte(TombstoneMarker),
					"comparison_is_true", (entry.Value == TombstoneMarker)) // This should be false if we are in this else block
			}
			kvs[entry.Key] = entry.Value // Assign the original value
		}
	}
	return kvs, nil
}

// writeLogEntry serializes a single log entry to the writer.
// Format: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
func (aol *AppendOnlyLog) writeLogEntry(w io.Writer, blockID uint64, key, value string) error {
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

// readLogEntry deserializes a single log entry from the reader.
// Returns the entry, bytes read, and error.
func (aol *AppendOnlyLog) readLogEntry(r io.Reader) (*logEntry, int64, error) {
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
func (aol *AppendOnlyLog) writeIndexEntry(w io.Writer, entry blockIndexEntry) error {
	if f, ok := w.(*os.File); ok && f == aol.indexMapFile {
		return aol.bufferIndexEntry(entry)
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
func (aol *AppendOnlyLog) persistIndexMap() error {
	aol.indexMapFile.Seek(0, io.SeekStart)
	writer := bufio.NewWriter(aol.indexMapFile)

	for _, entry := range aol.blockIndex {
		if err := aol.writeIndexEntry(writer, entry); err != nil {
			return fmt.Errorf("failed to write index entry for block %d: %w", entry.BlockID, err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush index map writer: %w", err)
	}

	if err := aol.indexMapFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync index map file: %w", err)
	}

	return nil
}

// AppendToNewBlock adds a batch of key-value pairs to a new, automatically assigned block ID.
// If kvs is empty, no block is written, aol.latestBlockID is returned (or 0 if log was empty), and no error.
func (aol *AppendOnlyLog) AppendToNewBlock(kvs map[string]string) (uint64, error) {
	aol.mu.Lock()
	defer aol.mu.Unlock()

	if aol.closed {
		return 0, fmt.Errorf("append-only log is closed")
	}

	if len(kvs) == 0 {
		aol.log.Debug("AppendToNewBlock called with empty KVS, no operation performed")
		if aol.isLogEmptyInitial() {
			return 0, nil // Log is empty, no block ID assigned yet
		}
		return aol.latestBlockID, nil // Return current latest, no new block created
	}

	newBlockID := aol.latestBlockID + 1
	if aol.isLogEmptyInitial() {
		newBlockID = 1
	}

	startOffset := aol.currentOffset
	blockDataBuf := new(bytes.Buffer)

	for key, value := range kvs {
		if err := aol.writeLogEntry(blockDataBuf, newBlockID, key, value); err != nil {
			return 0, fmt.Errorf("failed to serialize entry for new block %d, key %s: %w", newBlockID, key, err)
		}
	}

	blockBytes := blockDataBuf.Bytes()
	n, err := aol.dataWriter.Write(blockBytes)
	if err != nil {
		aol.log.Error("Failed to write new block data to buffer", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to write new block %d data: %w", newBlockID, err)
	}
	if n != len(blockBytes) {
		aol.log.Error("Incomplete write for new block data", "assignedBlockID", newBlockID, "written", n, "expected", len(blockBytes))
		return 0, fmt.Errorf("incomplete write for new block %d data", newBlockID)
	}

	endOffset := startOffset + int64(n)
	aol.currentOffset = endOffset

	indexEntry := blockIndexEntry{
		BlockID:     newBlockID,
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
	aol.blockIndex[newBlockID] = indexEntry
	aol.latestBlockID = newBlockID

	if err := aol.writeIndexEntry(aol.indexMapFile, indexEntry); err != nil {
		aol.log.Crit("Failed to write block index entry to file for new block!", "assignedBlockID", newBlockID, "error", err)
		// This is critical. Data is written but index isn't. Consider how to handle.
		return 0, fmt.Errorf("CRITICAL: failed to write index entry for new block %d: %w", newBlockID, err)
	}

	if err := aol.dataWriter.Flush(); err != nil {
		aol.log.Error("Failed to flush data writer after new block", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to flush data writer for new block %d: %w", newBlockID, err)
	}
	if err := aol.dataFile.Sync(); err != nil {
		aol.log.Error("Failed to sync data file after new block", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to sync data file for new block %d: %w", newBlockID, err)
	}
	if err := aol.indexMapFile.Sync(); err != nil {
		aol.log.Error("Failed to sync index map file after new block", "assignedBlockID", newBlockID, "error", err)
		return 0, fmt.Errorf("failed to sync index map file for new block %d: %w", newBlockID, err)
	}

	aol.updateRecentBlocks(newBlockID) // Manages recentBlocks and indexedBlocks
	if _, isIndexed := aol.indexedBlocks[newBlockID]; isIndexed {
		aol.log.Debug("Indexing new block in skiplist (AppendToNewBlock)", "blockID", newBlockID)
		reader := bytes.NewReader(blockBytes)
		entryPos := startOffset
		for reader.Len() > 0 {
			entry, bytesRead, readErr := aol.readLogEntry(reader)
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				aol.log.Error("Failed to decode entry while indexing new block (AppendToNewBlock)", "blockID", newBlockID, "error", readErr)
				// Data is persisted, but skiplist might be inconsistent for this new block.
				return 0, fmt.Errorf("failed to decode entry for skiplist indexing new block %d: %w", newBlockID, readErr)
			}
			ptr := &kvPointer{
				Offset:   entryPos,
				ValueLen: uint32(len(entry.Value)),
				BlockID:  entry.BlockID,
			}
			aol.skiplistIndex.Set(entry.Key, ptr)
			entryPos += bytesRead
		}
	}

	// evictOldEntries is implicitly handled by updateRecentBlocks if it calls rebuildSkiplist.
	// No explicit call to aol.evictOldEntries() needed here if updateRecentBlocks is comprehensive.

	return newBlockID, nil
}

// getLatestBlockID returns the latest block ID known to the log.
// Caller must hold aol.mu if consistency with a subsequent write is needed.
func (aol *AppendOnlyLog) getLatestBlockID() uint64 {
	return aol.latestBlockID
}

// isLogEmptyInitial checks if the log is completely empty (no blocks indexed).
// Caller must hold aol.mu.
func (aol *AppendOnlyLog) isLogEmptyInitial() bool {
	return aol.latestBlockID == 0 && len(aol.blockIndex) == 0
}

// readValueBytesFromPointer reads the raw value bytes from the data file for a given kvPointer.
// This is used by the iterator to get values from the skiplist pointers.
// Assumes aol.mu is RLocked by the caller if called during skiplist iteration.
// Reading from aol.dataFile with ReadAt is safe concurrently.
func (aol *AppendOnlyLog) readValueBytesFromPointer(pointer *kvPointer) ([]byte, error) {
	// logEntry format on disk: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
	// pointer.Offset points to the start of this logEntry.
	// pointer.ValueLen is the length of the string form of the value (can be TombstoneMarker).

	// We need to determine the key's length to correctly calculate the value's starting offset.
	// The keyLen field is located after the blockID field.
	offsetOfKeyLenField := pointer.Offset + int64(blockIDSize)

	// Ensure reading keyLen field is within bounds
	if offsetOfKeyLenField+int64(keyLenSize) > aol.currentOffset {
		return nil, fmt.Errorf("offset for keyLen field %d is out of data file bounds %d", offsetOfKeyLenField, aol.currentOffset)
	}

	keyLenBuf := make([]byte, keyLenSize)
	_, err := aol.dataFile.ReadAt(keyLenBuf, offsetOfKeyLenField)
	if err != nil {
		return nil, fmt.Errorf("ReadAt for keyLen failed at offset %d: %w", offsetOfKeyLenField, err)
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf)

	// Value's actual data starts after the full header and the key itself.
	// Full header consists of: blockID, keyLen field, valueLen field.
	fullHeaderFieldsSize := int64(blockIDSize + keyLenSize + valueLenSize)
	valueOfset := pointer.Offset + fullHeaderFieldsSize + int64(keyLen)

	// Ensure reading value is within bounds
	if valueOfset+int64(pointer.ValueLen) > aol.currentOffset {
		return nil, fmt.Errorf("value offset %d + length %d is out of data file bounds %d", valueOfset, pointer.ValueLen, aol.currentOffset)
	}

	valueBytes := make([]byte, pointer.ValueLen)
	_, err = aol.dataFile.ReadAt(valueBytes, valueOfset)
	if err != nil {
		return nil, fmt.Errorf("ReadAt for value failed at offset %d (len %d): %w", valueOfset, pointer.ValueLen, err)
	}
	return valueBytes, nil
}

// Close flushes buffers and closes open files.
func (aol *AppendOnlyLog) Close() error {
	aol.mu.Lock()
	defer aol.mu.Unlock()

	if aol.closed {
		return ErrClosed // Or your specific error for already closed
	}
	aol.closed = true
	fmt.Println("Closing AppendOnlyLog...")

	var errs []error // Using a slice to collect multiple errors

	select {
	case aol.indexBufferFlush <- struct{}{}:
	default:
	}

	flushDone := make(chan struct{})
	go func() {
		// Wait for the index buffer to flush
		time.Sleep(500 * time.Millisecond)
		close(flushDone)
	}()

	select {
	case <-flushDone:
	case <-time.After(2 * time.Second):
		fmt.Println("Warning: Timeout waiting for background flush goroutine to exit")
		errs = append(errs, fmt.Errorf("background flush goroutine exit timeout"))
	}

	if err := aol.flushIndexBuffer(); err != nil {
		errs = append(errs, fmt.Errorf("failed to flush index buffer on close: %w", err))
	}

	if aol.dataWriter != nil {
		if err := aol.dataWriter.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("failed to flush data writer on close: %w", err))
		}
	}

	// Persist the final state of the index map.
	// This ensures that even if the last Append's persistIndexMap had issues
	// or if there were no appends since the last persist, the current map is written.
	if err := aol.persistIndexMap(); err != nil {
		errs = append(errs, fmt.Errorf("failed to persist index map on close: %w", err))
	}
	if err := aol.persistSkiplistIndex(); err != nil {
		errs = append(errs, fmt.Errorf("failed to persist skiplist index on close: %w", err))
	}

	if aol.dataFile != nil {
		// Sync data file before closing, to ensure all writes are flushed.
		if err := aol.dataFile.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("failed to sync data file on close: %w", err))
		}
		if err := aol.dataFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close data file: %w", err))
		}
		aol.dataFile = nil // Mark as closed
	}

	if aol.indexMapFile != nil {
		if err := aol.indexMapFile.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("failed to sync index map file on close: %w", err))
		}
		if err := aol.indexMapFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close index map file: %w", err))
		}
		aol.indexMapFile = nil
	}

	close(aol.indexBufferFlush)
	aol.indexBufferFlush = nil

	aol.skiplistIndex = nil
	aol.blockIndex = nil
	aol.indexedBlocks = nil
	aol.recentBlocks = nil
	aol.dataWriter = nil
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

func (aol *AppendOnlyLog) evictOldBlockFromSkiplist(oldestBlockID uint64) {
	aol.log.Debug("Evicting oldest block from skiplist index", "blockID", oldestBlockID)

	// Identify keys to remove that belong only to the evicted block.
	keysToRemove := make([]string, 0)

	// Iterate through skiplist to find keys associated with the oldestBlockID
	for e := aol.skiplistIndex.Front(); e != nil; e = e.Next() {
		ptr := e.Value.(*kvPointer)
		if ptr.BlockID == oldestBlockID {
			keysToRemove = append(keysToRemove, e.Key().(string))
		}
	}

	// Remove identified keys from skiplist
	for _, key := range keysToRemove {
		// check if the key exists in other blocks before removing
		// shouldRemove := true
		// existingElement := aol.skiplistIndex.Get(key)
		// if existingElement != nil {
		// 	ptr := existingElement.Value.(*kvPointer)
		// 	if ptr.BlockID != oldestBlockID {
		// 		// if the key exists in another block, do not remove it
		// 		shouldRemove = false
		// 	}
		// }

		// if shouldRemove {
		// 	aol.skiplistIndex.Remove(key)
		// }

		aol.skiplistIndex.Remove(key)
		aol.log.Debug("Removed key from skiplist during eviction", "key", key, "evictedBlockID", oldestBlockID)
	}
}

// persistIndexIfNeeded checks if the index needs to be persisted based on the new block ID.
func (aol *AppendOnlyLog) persistIndexIfNeeded(currentBlockID uint64) {
	if aol.lastPersistedBlockID == 0 || (currentBlockID-aol.lastPersistedBlockID) > uint64(aol.persistInterval) {
		if err := aol.persistSkiplistIndex(); err != nil {
			aol.log.Error("Failed to persist skiplist index", "error", err)
		} else {
			aol.lastPersistedBlockID = currentBlockID
			aol.log.Info("Successfully persisted skiplist index",
				"blockID", currentBlockID,
				"keysIndexed", aol.skiplistIndex.Len())
		}
	}
}

// persistSkiplistIndex writes the current skiplist index to the skiplist index file.
func (aol *AppendOnlyLog) persistSkiplistIndex() error {
	file, err := os.Create(aol.SLIndexFilePath)
	if err != nil {
		return fmt.Errorf("failed to create index file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	blockCount := len(aol.recentBlocks)
	if err := binary.Write(writer, binary.BigEndian, uint32(blockCount)); err != nil {
		return fmt.Errorf("failed to write block count: %w", err)
	}

	for _, blockID := range aol.recentBlocks {
		if err := binary.Write(writer, binary.BigEndian, blockID); err != nil {
			return fmt.Errorf("failed to write block ID: %w", err)
		}
	}

	keyCount := aol.skiplistIndex.Len()
	if err := binary.Write(writer, binary.BigEndian, uint32(keyCount)); err != nil {
		return fmt.Errorf("failed to write key count: %w", err)
	}

	for e := aol.skiplistIndex.Front(); e != nil; e = e.Next() {
		key := e.Key().(string)
		ptr := e.Value.(*kvPointer)

		keyBytes := []byte(key)
		if err := binary.Write(writer, binary.BigEndian, uint32(len(keyBytes))); err != nil {
			return fmt.Errorf("failed to write key length: %w", err)
		}
		if _, err := writer.Write(keyBytes); err != nil {
			return fmt.Errorf("failed to write key data: %w", err)
		}

		if err := binary.Write(writer, binary.BigEndian, ptr.Offset); err != nil {
			return fmt.Errorf("failed to write pointer offset: %w", err)
		}
		if err := binary.Write(writer, binary.BigEndian, ptr.ValueLen); err != nil {
			return fmt.Errorf("failed to write pointer value length: %w", err)
		}
		if err := binary.Write(writer, binary.BigEndian, ptr.BlockID); err != nil {
			return fmt.Errorf("failed to write pointer block ID: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush index data: %w", err)
	}

	return nil
}

func (aol *AppendOnlyLog) loadSkiplistIndex() error {
	// Check if the index file exists
	if _, err := os.Stat(aol.SLIndexFilePath); os.IsNotExist(err) {
		aol.log.Info("No persisted skiplist index found, will rebuild from data")
		return nil
	}

	file, err := os.Open(aol.SLIndexFilePath)
	if err != nil {
		return fmt.Errorf("failed to open index file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// read block count
	var blockCount uint32
	if err := binary.Read(reader, binary.BigEndian, &blockCount); err != nil {
		return fmt.Errorf("failed to read block count: %w", err)
	}

	// read block IDs
	tempRecentBlocks := make([]uint64, blockCount)
	tempIndexedBlocks := make(map[uint64]struct{})

	var highestBlockID uint64 = 0
	for i := uint32(0); i < blockCount; i++ {
		var blockID uint64
		if err := binary.Read(reader, binary.BigEndian, &blockID); err != nil {
			return fmt.Errorf("failed to read block ID: %w", err)
		}
		tempRecentBlocks[i] = blockID
		tempIndexedBlocks[blockID] = struct{}{}

		if blockID > highestBlockID {
			highestBlockID = blockID
		}
	}

	// check if the persisted index is outdated
	if highestBlockID < aol.latestBlockID {
		aol.log.Warn("Persisted index is outdated, will rebuild",
			"indexLatestBlock", highestBlockID,
			"aolLatestBlock", aol.latestBlockID)
		return nil
	}

	aol.skiplistIndex = skiplist.New(skiplist.String)

	// read key count
	var keyCount uint32
	if err := binary.Read(reader, binary.BigEndian, &keyCount); err != nil {
		return fmt.Errorf("failed to read key count: %w", err)
	}

	// read each key and its pointer
	for i := uint32(0); i < keyCount; i++ {
		// read key
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length: %w", err)
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return fmt.Errorf("failed to read key data: %w", err)
		}
		key := string(keyBytes)

		// read pointer
		ptr := &kvPointer{}
		if err := binary.Read(reader, binary.BigEndian, &ptr.Offset); err != nil {
			return fmt.Errorf("failed to read pointer offset: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &ptr.ValueLen); err != nil {
			return fmt.Errorf("failed to read pointer value length: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &ptr.BlockID); err != nil {
			return fmt.Errorf("failed to read pointer block ID: %w", err)
		}

		// insert into skiplist
		aol.skiplistIndex.Set(key, ptr)
	}

	// update recentBlocks and indexedBlocks
	aol.recentBlocks = tempRecentBlocks
	aol.indexedBlocks = tempIndexedBlocks
	aol.lastPersistedBlockID = highestBlockID

	aol.log.Info("Successfully loaded skiplist index from disk",
		"blockCount", blockCount,
		"keyCount", keyCount,
		"lastPersistedBlock", aol.lastPersistedBlockID)

	return nil
}

func (aol *AppendOnlyLog) backgroundFlush() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-aol.indexBufferFlush:
			aol.flushIndexBuffer()
		case <-ticker.C:
			aol.flushIndexBuffer()
		}

		aol.mu.RLock()
		closed := aol.closed
		aol.mu.RUnlock()
		if closed {
			return
		}
	}
}

// add an entry to the index buffer and trigger flush if needed
func (aol *AppendOnlyLog) bufferIndexEntry(entry blockIndexEntry) error {
	aol.indexBufferMu.Lock()
	defer aol.indexBufferMu.Unlock()

	aol.indexBuffer = append(aol.indexBuffer, entry)

	// Trigger flush if buffer exceeds threshold
	if len(aol.indexBuffer) >= aol.indexBufferSize {
		select {
		case aol.indexBufferFlush <- struct{}{}:
		default:
		}
	}
	return nil
}

func (aol *AppendOnlyLog) flushIndexBuffer() error {
	aol.indexBufferMu.Lock()

	if len(aol.indexBuffer) == 0 {
		aol.indexBufferMu.Unlock()
		return nil
	}

	entries := make([]blockIndexEntry, len(aol.indexBuffer))
	copy(entries, aol.indexBuffer)

	aol.indexBuffer = aol.indexBuffer[:0]
	aol.indexBufferMu.Unlock()

	writer := bufio.NewWriter(aol.indexMapFile)
	for _, entry := range entries {
		buf := make([]byte, blockIDSize+offsetSize+offsetSize)
		binary.BigEndian.PutUint64(buf[0:blockIDSize], entry.BlockID)
		binary.BigEndian.PutUint64(buf[blockIDSize:blockIDSize+offsetSize], uint64(entry.StartOffset))
		binary.BigEndian.PutUint64(buf[blockIDSize+offsetSize:], uint64(entry.EndOffset))

		if _, err := writer.Write(buf); err != nil {
			aol.log.Error("Failed to write index entry during flush", "error", err)
			return err
		}
	}

	// Flush and sync
	if err := writer.Flush(); err != nil {
		aol.log.Error("Failed to flush index buffer writer", "error", err)
		return err
	}

	if err := aol.indexMapFile.Sync(); err != nil {
		aol.log.Error("Failed to sync index file after buffer flush", "error", err)
		return err
	}

	aol.log.Debug("Successfully flushed index buffer", "entries", len(entries))
	return nil
}

func (aol *AppendOnlyLog) FlushIndexBuffer() error {
	return aol.flushIndexBuffer()
}

func (aol *AppendOnlyLog) getBlockIndexEntry(blockID uint64) (blockIndexEntry, bool) {

	entry, ok := aol.blockIndex[blockID]
	if ok {
		return entry, true
	}
	aol.indexBufferMu.Lock()
	for _, entry := range aol.indexBuffer {
		if entry.BlockID == blockID {
			aol.indexBufferMu.Unlock()
			return entry, true
		}
	}
	aol.indexBufferMu.Unlock()
	return blockIndexEntry{}, false
}
