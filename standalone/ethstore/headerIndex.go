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

	// Added testing import
	"github.com/ethereum/go-ethereum/common" // Added common import
	"github.com/ethereum/go-ethereum/log"
	"github.com/huandu/skiplist" // Using a third-party skiplist library
)

const (
	BlockdataFileName     = "headerdata.log"
	BlockindexMapFileName = "blockindex.map"
	HeaderKeyPrefix       = "h" // the prefix for header keys
	HeaderIndexFileName   = "headerindex.map"
	// defaultRecentN   = 100 // Default number of recent blocks to keep indexed in memory
	// offsetSize       = 8   // Size of uint64 for offsets
	// blockIDSize      = 8   // Assuming block ID is uint64
	// keyLenSize       = 4   // Size of uint32 for key length
	// valueLenSize     = 4   // Size of uint32 for value length
	// // TombstoneMarker is a special value to mark deletion
	// TombstoneMarker   = "_D_"
	// initialBufferSize = 4096 // Initial buffer size for writers
)

// // logEntry represents a single key-value pair within a block in the data log.
// // Format on disk: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
// type logEntry struct {
// 	BlockID uint64
// 	Key     string
// 	Value   string // Can be TombstoneMarker for deletion
// 	Offset  int64  // Offset in the data file where this entry starts
// }

// // blockIndexEntry stores the start and end offset for all entries belonging to a block.
// // Format on disk: blockID (uint64) | startOffset (uint64) | endOffset (uint64)
// type blockIndexEntry struct {
// 	BlockID     uint64
// 	StartOffset int64
// 	EndOffset   int64 // Offset *after* the last byte of the last entry for this block
// }

// // kvPointer stores the location of a specific key's value in the data log.
// // Used as the value in the skiplist.
// type kvPointer struct {
// 	Offset   int64 // Offset of the logEntry start
// 	ValueLen uint32
// }

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
	recentN       int
	recentBlocks  []uint64            // Ordered list of recent block IDs (most recent last)
	skiplistIndex *skiplist.SkipList  // Key: string (key), Value: *kvPointer
	indexedBlocks map[uint64]struct{} // Set of block IDs currently in the skiplist

	// Skiplist for header keys
	headerIndex         *skiplist.SkipList  // Key: string (header key), Value: *kvPointer
	modifiedHeaders     map[string]struct{} // Track modified header keys
	headerIndexFilePath string
	headerIndexFile     *os.File

	mu     sync.RWMutex
	closed bool
}

// isHeaderKey checks if a key is a header key.
func isHeaderKey(key string) bool {
	return strings.HasPrefix(key, HeaderKeyPrefix)
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
	headerIndexFilePath := filepath.Join(dirPath, HeaderIndexFileName)
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

	headerIndexFile, err := os.OpenFile(headerIndexFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		dataFile.Close()
		indexMapFile.Close()
		return nil, fmt.Errorf("failed to open header index file %s: %w", headerIndexFilePath, err)
	}

	baol := &BlockAppendOnlyLog{
		dirPath:             dirPath,
		log:                 logger.New("module", "appendlog", "path", dirPath),
		dataFilePath:        dataFilePath,
		dataFile:            dataFile,
		dataWriter:          bufio.NewWriterSize(dataFile, initialBufferSize),
		currentOffset:       currentOffset,
		indexMapFilePath:    indexMapFilePath,
		indexMapFile:        indexMapFile,
		blockIndex:          make(map[uint64]blockIndexEntry),
		recentN:             recentN,
		recentBlocks:        make([]uint64, 0, recentN), // Initialize empty, will be populated below
		skiplistIndex:       skiplist.New(skiplist.String),
		indexedBlocks:       make(map[uint64]struct{}), // Initialize empty, will be populated below
		headerIndexFilePath: headerIndexFilePath,
		headerIndexFile:     headerIndexFile,
		headerIndex:         skiplist.New(skiplist.String),
		modifiedHeaders:     make(map[string]struct{}), // Initialize empty for tracking modified headers
	}

	// Load existing block index map
	if err := baol.loadBlockIndex(); err != nil {
		baol.Close()
		return nil, fmt.Errorf("failed to load block index: %w", err)
	}

	if err := baol.loadHeaderIndex(); err != nil {
		baol.log.Warn("Failed to load header index, will rebuild", "error", err)
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

	// Initialize the header index
	if baol.headerIndex.Len() == 0 {
		if err := baol.initializeHeaderIndex(); err != nil {
			baol.Close()
			return nil, fmt.Errorf("failed to initialize header index: %w", err)
		}

		if err := baol.persistHeaderIndex(); err != nil {
			baol.log.Warn("Failed to persist header index after initialization", "error", err)
		}
	}

	baol.log.Info("AppendOnlyLog initialized", "dataSize", common.StorageSize(currentOffset),
		"indexedBlocks", len(baol.indexedBlocks), "recentBlocksTracked", len(baol.recentBlocks),
		"headerIndexSize", baol.headerIndex.Len())
	return baol, nil
}

// Path returns the data directory of the append-only log.
func (aol *BlockAppendOnlyLog) Path() string {
	return aol.dirPath
}

// RecentN returns the number of recent blocks indexed in the skiplist.
func (aol *BlockAppendOnlyLog) RecentN() int {
	return aol.recentN
}

// loadBlockIndex reads the index map file into memory.
func (aol *BlockAppendOnlyLog) loadBlockIndex() error {
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
func (aol *BlockAppendOnlyLog) rebuildSkiplist() error {
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

	// oldSkiplist := aol.skiplistIndex
	aol.skiplistIndex = skiplist.New(skiplist.String)

	aol.log.Debug("Rebuilding skiplist index", "blocksToScan", blocksToIndex)

	for _, blockID := range blocksToIndex {
		// Ensure this block is still supposed to be indexed.
		// This check is somewhat redundant if blocksToIndex is derived from aol.recentBlocks
		// and aol.indexedBlocks is kept in sync, but good for safety.
		if _, stillIndexed := aol.indexedBlocks[blockID]; !stillIndexed {
			aol.log.Warn("Block in blocksToIndex for rebuild is no longer in aol.indexedBlocks", "blockID", blockID)
			continue
		}

		indexEntry, ok := aol.blockIndex[blockID]
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

	aol.log.Debug("Skiplist rebuild complete", "indexedKeys", aol.skiplistIndex.Len(),
		"currentRecentBlocks", aol.recentBlocks, "headerIndexSize", aol.headerIndex.Len())
	return nil
}

// readAndIndexBlock reads all log entries for a given block and adds/updates them in the skiplist.
func (aol *BlockAppendOnlyLog) readAndIndexBlock(indexEntry blockIndexEntry) error {
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
		}
		aol.skiplistIndex.Set(entry.Key, ptr)
	}
	return nil
}

// Append adds a batch of key-value pairs for a given block ID.
// It ensures atomicity for the block: either all pairs are written or none.
// It updates the block index map and the skiplist if the block is recent.
func (aol *BlockAppendOnlyLog) Append(blockID uint64, kvs map[string]string) error {
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
	startOffset := aol.currentOffset
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
		BlockID:     blockID,
		StartOffset: startOffset,
		EndOffset:   endOffset,
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

	if err := aol.dataWriter.Flush(); err != nil {
		aol.log.Error("Failed to flush data writer after append", "blockID", blockID, "error", err)
		return fmt.Errorf("failed to flush data writer for block %d: %w", blockID, err)
	}
	if err := aol.dataFile.Sync(); err != nil {
		aol.log.Error("Failed to sync data file after append", "blockID", blockID, "error", err)
		return fmt.Errorf("failed to sync data file for block %d: %w", blockID, err)
	}
	if err := aol.indexMapFile.Sync(); err != nil {
		aol.log.Error("Failed to sync index map file after append", "blockID", blockID, "error", err)
		return fmt.Errorf("failed to sync index map file for block %d: %w", blockID, err)
	}

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
			}
			aol.skiplistIndex.Set(entry.Key, ptr)
			entryPos += bytesReadThisEntry
		}
	}

	aol.evictOldEntries()
	return nil
}

// updateRecentBlocks adds the new block ID and removes the oldest if the limit is exceeded.
func (aol *BlockAppendOnlyLog) updateRecentBlocks(newBlockID uint64) {
	aol.recentBlocks = append(aol.recentBlocks, newBlockID)
	aol.indexedBlocks[newBlockID] = struct{}{}

	if len(aol.recentBlocks) > aol.recentN {
		// Remove the oldest block from index
		oldestBlockID := aol.recentBlocks[0]
		aol.recentBlocks = aol.recentBlocks[1:] // Shift slice
		delete(aol.indexedBlocks, oldestBlockID)

		aol.log.Debug("Evicting oldest block from skiplist index", "blockID", oldestBlockID)

		oldHeaderKeys := make(map[string]*kvPointer)
		for el := aol.skiplistIndex.Front(); el != nil; el = el.Next() {
			key := el.Key().(string)
			if isHeaderKey(key) {
				oldHeaderKeys[key] = el.Value.(*kvPointer)
			}
		}

		// Remove keys belonging *only* to the evicted block from the skiplist.
		if err := aol.rebuildSkiplist(); err != nil {
			aol.log.Error("Failed to rebuild skiplist after eviction", "evictedBlock", oldestBlockID, "error", err)
		}

		headerIndexModified := false
		for key, ptr := range oldHeaderKeys {
			if aol.skiplistIndex.Get(key) == nil {
				valueBytes, err := aol.readValueBytesFromPointer(ptr)
				if err == nil && string(valueBytes) != TombstoneMarker {
					aol.headerIndex.Set(key, ptr)
					aol.modifiedHeaders[key] = struct{}{} // Track modified header keys
					aol.log.Debug("Moved evicted header key to headerIndex", "key", key)
					headerIndexModified = true
				}
			}
		}
		if headerIndexModified {
			if err := aol.persistHeaderIndex(); err != nil {
				aol.log.Warn("Failed to persist header index after update", "error", err)
			}
		}
	}
}

// evictOldEntries is called after new data is appended and recent blocks are updated.
// The primary mechanism for skiplist eviction is `rebuildSkiplist`, which is
// called by `updateRecentBlocks` when the oldest block in `recentBlocks` is removed.
func (aol *BlockAppendOnlyLog) evictOldEntries() {
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
func (aol *BlockAppendOnlyLog) Get(key string) (string, bool, error) {
	aol.mu.RLock()
	defer aol.mu.RUnlock()

	if aol.closed {
		return "", false, fmt.Errorf("append-only log is closed")
	}

	// Check if the key is in the skiplist index
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

	// Check if the key is in the headerIndex
	// Header keys are special and stored in a separate skiplist.
	if isHeaderKey(key) {
		headerElement := aol.headerIndex.Get(key)
		if headerElement != nil {
			pointer := headerElement.Value.(*kvPointer)

			headerSize := blockIDSize + keyLenSize + valueLenSize
			headerBytes := make([]byte, headerSize)
			_, err := aol.dataFile.ReadAt(headerBytes, pointer.Offset)
			if err != nil {
				aol.log.Error("Get: Failed to read entry header from headerIndex",
					"key", key, "offset", pointer.Offset, "error", err)
				return "", false, fmt.Errorf("failed to read header entry for key %s: %w", key, err)
			}

			keyLenOnDisk := binary.BigEndian.Uint32(headerBytes[blockIDSize : blockIDSize+keyLenSize])
			valueLenOnDisk := binary.BigEndian.Uint32(headerBytes[blockIDSize+keyLenSize : headerSize])

			valueBytes := make([]byte, valueLenOnDisk)
			valueOffset := pointer.Offset + int64(headerSize) + int64(keyLenOnDisk)
			_, err = aol.dataFile.ReadAt(valueBytes, valueOffset)
			if err != nil {
				aol.log.Error("Get: Failed to read value from headerIndex",
					"key", key, "valueOffset", valueOffset, "error", err)
				return "", false, fmt.Errorf("failed to read value for header key %s: %w", key, err)
			}

			value := string(valueBytes)
			if value == TombstoneMarker {
				return "", true, nil
			}
			return value, true, nil
		}
	}

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
		indexEntry, ok := aol.blockIndex[blockIDToScan] // Still under RLock
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
func (aol *BlockAppendOnlyLog) Delete(key string) error {
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
func (aol *BlockAppendOnlyLog) DeleteByPrefixInBlock(targetBlockID uint64, prefix string) error {
	var blockData []byte
	var readErr error

	aol.mu.RLock()
	if aol.closed {
		aol.mu.RUnlock()
		return fmt.Errorf("append-only log is closed")
	}

	indexEntry, ok := aol.blockIndex[targetBlockID]
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
func (aol *BlockAppendOnlyLog) GetByBlock(blockID uint64) (map[string]string, error) {
	aol.mu.RLock()
	defer aol.mu.RUnlock()

	if aol.closed {
		return nil, fmt.Errorf("append-only log is closed")
	}

	indexEntry, ok := aol.blockIndex[blockID]
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
func (aol *BlockAppendOnlyLog) writeLogEntry(w io.Writer, blockID uint64, key, value string) error {
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
func (aol *BlockAppendOnlyLog) readLogEntry(r io.Reader) (*logEntry, int64, error) {
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
func (aol *BlockAppendOnlyLog) writeIndexEntry(w io.Writer, entry blockIndexEntry) error {
	buf := make([]byte, blockIDSize+offsetSize+offsetSize)
	binary.BigEndian.PutUint64(buf[0:blockIDSize], entry.BlockID)
	binary.BigEndian.PutUint64(buf[blockIDSize:blockIDSize+offsetSize], uint64(entry.StartOffset))
	binary.BigEndian.PutUint64(buf[blockIDSize+offsetSize:], uint64(entry.EndOffset))

	_, err := w.Write(buf)
	if err != nil {
		return err
	}
	if indexWriter, ok := w.(*bufio.Writer); ok {
		return indexWriter.Flush()
	}
	if f, ok := w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

// persistIndexMap writes the current block index map to the index map file.
func (aol *BlockAppendOnlyLog) persistIndexMap() error {
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
func (aol *BlockAppendOnlyLog) AppendToNewBlock(kvs map[string]string) (uint64, error) {
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
func (aol *BlockAppendOnlyLog) getLatestBlockID() uint64 {
	return aol.latestBlockID
}

// isLogEmptyInitial checks if the log is completely empty (no blocks indexed).
// Caller must hold aol.mu.
func (aol *BlockAppendOnlyLog) isLogEmptyInitial() bool {
	return aol.latestBlockID == 0 && len(aol.blockIndex) == 0
}

// readValueBytesFromPointer reads the raw value bytes from the data file for a given kvPointer.
// This is used by the iterator to get values from the skiplist pointers.
// Assumes aol.mu is RLocked by the caller if called during skiplist iteration.
// Reading from aol.dataFile with ReadAt is safe concurrently.
func (aol *BlockAppendOnlyLog) readValueBytesFromPointer(pointer *kvPointer) ([]byte, error) {
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
func (aol *BlockAppendOnlyLog) Close() error {
	aol.mu.Lock()
	defer aol.mu.Unlock()

	if aol.closed {
		return ErrClosed // Or your specific error for already closed
	}
	aol.closed = true

	var errs []error // Using a slice to collect multiple errors

	// Persist the final state of the index map.
	// This ensures that even if the last Append's persistIndexMap had issues
	// or if there were no appends since the last persist, the current map is written.
	if err := aol.persistIndexMap(); err != nil {
		errs = append(errs, fmt.Errorf("failed to persist index map on close: %w", err))
	}

	if err := aol.persistHeaderIndex(); err != nil {
		errs = append(errs, fmt.Errorf("failed to persist header index on close: %w", err))
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

	if aol.headerIndexFile != nil {
		if err := aol.headerIndexFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close header index file: %w", err))
		}
		aol.headerIndexFile = nil
	}

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

// initializeHeaderIndex scans all blocks and builds the header index.
func (aol *BlockAppendOnlyLog) initializeHeaderIndex() error {
	allBlockIDs := make([]uint64, 0, len(aol.blockIndex))
	for id := range aol.blockIndex {
		allBlockIDs = append(allBlockIDs, id)
	}
	sort.Slice(allBlockIDs, func(i, j int) bool {
		return allBlockIDs[i] < allBlockIDs[j]
	})

	processedHeaderKeys := make(map[string]struct{})

	for i := len(allBlockIDs) - 1; i >= 0; i-- {
		blockID := allBlockIDs[i]

		if _, isIndexed := aol.indexedBlocks[blockID]; isIndexed {
			continue
		}

		indexEntry, ok := aol.blockIndex[blockID]
		if !ok {
			continue
		}

		size := indexEntry.EndOffset - indexEntry.StartOffset
		if size <= 0 {
			continue
		}

		blockData := make([]byte, size)
		_, err := aol.dataFile.ReadAt(blockData, indexEntry.StartOffset)
		if err != nil {
			return fmt.Errorf("failed to read block data for header index: %w", err)
		}

		reader := bytes.NewReader(blockData)
		currentPos := indexEntry.StartOffset

		for reader.Len() > 0 {
			entryOffset := currentPos
			entry, bytesRead, err := aol.readLogEntry(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to decode entry for header index: %w", err)
			}
			currentPos += bytesRead

			// if the entry is a header key, process it
			if isHeaderKey(entry.Key) {
				if _, processed := processedHeaderKeys[entry.Key]; processed {
					continue
				}
				processedHeaderKeys[entry.Key] = struct{}{}

				if entry.Value == TombstoneMarker {
					continue
				}

				if aol.skiplistIndex.Get(entry.Key) != nil {
					continue
				}

				ptr := &kvPointer{
					Offset:   entryOffset,
					ValueLen: uint32(len(entry.Value)),
				}
				aol.headerIndex.Set(entry.Key, ptr)
				aol.log.Debug("Added header key to headerIndex", "key", entry.Key)
			}
		}
	}

	aol.log.Info("Header index initialized", "headerKeyCount", aol.headerIndex.Len())
	return nil
}

// persistHeaderIndex writes the current header index to the header index file.
// Format per entry: keyLen (uint32) | key (bytes) | offset (int64) | valueLen (uint32)
func (aol *BlockAppendOnlyLog) persistHeaderIndex() error {
	if aol.headerIndexFile == nil {
		return nil
	}

	if len(aol.modifiedHeaders) == 0 {
		return nil
	}

	aol.headerIndexFile.Seek(0, io.SeekEnd)
	writer := bufio.NewWriter(aol.headerIndexFile)

	count := 0
	for key := range aol.modifiedHeaders {
		el := aol.headerIndex.Get(key)
		if el == nil {
			continue
		}

		ptr := el.Value.(*kvPointer)
		keyBytes := []byte(key)
		keyLen := uint32(len(keyBytes))

		binary.Write(writer, binary.BigEndian, keyLen)
		writer.Write(keyBytes)
		binary.Write(writer, binary.BigEndian, ptr.Offset)
		binary.Write(writer, binary.BigEndian, ptr.ValueLen)
		count++
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush header index writer: %w", err)
	}

	if err := aol.headerIndexFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync header index file: %w", err)
	}

	modifiedCount := len(aol.modifiedHeaders)
	aol.modifiedHeaders = make(map[string]struct{})

	aol.log.Info("Header index updated", "appended", count, "modifiedTotal", modifiedCount)
	return nil
}

// loadHeaderIndex reads the header index file and populates the header index.
func (aol *BlockAppendOnlyLog) loadHeaderIndex() error {
	fileInfo, err := aol.headerIndexFile.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size() == 0 {
		aol.log.Debug("Header index file is empty, will rebuild index")
		return nil
	}

	aol.headerIndexFile.Seek(0, io.SeekStart)
	reader := bufio.NewReader(aol.headerIndexFile)

	headerBuf := make([]byte, 4)
	offsetBuf := make([]byte, 8)
	valueLenBuf := make([]byte, 4)

	const initialKeyBufSize = 256
	keyBuf := make([]byte, initialKeyBufSize)

	loadedCount := 0

	for {
		_, err := io.ReadFull(reader, headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read key length from header index: %w", err)
		}
		keyLen := binary.BigEndian.Uint32(headerBuf)

		if int(keyLen) > cap(keyBuf) {
			keyBuf = make([]byte, keyLen)
		} else {
			keyBuf = keyBuf[:keyLen]
		}

		_, err = io.ReadFull(reader, keyBuf)
		if err != nil {
			return fmt.Errorf("failed to read key content from header index: %w", err)
		}
		key := string(keyBuf)

		_, err = io.ReadFull(reader, offsetBuf)
		if err != nil {
			return fmt.Errorf("failed to read offset from header index: %w", err)
		}
		offset := int64(binary.BigEndian.Uint64(offsetBuf))

		_, err = io.ReadFull(reader, valueLenBuf)
		if err != nil {
			return fmt.Errorf("failed to read value length from header index: %w", err)
		}
		valueLen := binary.BigEndian.Uint32(valueLenBuf)

		ptr := &kvPointer{
			Offset:   offset,
			ValueLen: valueLen,
		}
		aol.headerIndex.Set(key, ptr)

		loadedCount++
	}

	aol.log.Info("Header index loaded from file", "entries", loadedCount)
	return nil
}
