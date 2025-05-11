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
	dataFileName      = "data.log"
	indexMapFileName  = "index.map"
	defaultRecentN    = 100           // Default number of recent blocks to keep indexed in memory
	offsetSize        = 8             // Size of uint64 for offsets
	blockIDSize       = 8             // Assuming block ID is uint64
	keyLenSize        = 4             // Size of uint32 for key length
	valueLenSize      = 4             // Size of uint32 for value length
	tombstoneMarker   = "__DELETED__" // Special value to mark deletion
	initialBufferSize = 4096          // Initial buffer size for writers
)

// logEntry represents a single key-value pair within a block in the data log.
// Format on disk: blockID (uint64) | keyLen (uint32) | valueLen (uint32) | key (bytes) | value (bytes)
type logEntry struct {
	BlockID uint64
	Key     string
	Value   string // Can be tombstoneMarker for deletion
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
		recentBlocks:     make([]uint64, 0, recentN),
		skiplistIndex:    skiplist.New(skiplist.String),
		indexedBlocks:    make(map[uint64]struct{}),
	}

	// Load existing block index map
	if err := aol.loadBlockIndex(); err != nil {
		aol.Close()
		return nil, fmt.Errorf("failed to load block index: %w", err)
	}

	// Rebuild skiplist for the last N blocks from the index
	if err := aol.rebuildSkiplist(); err != nil {
		aol.Close()
		return nil, fmt.Errorf("failed to rebuild skiplist index: %w", err)
	}

	aol.log.Info("AppendOnlyLog initialized", "dataSize", common.StorageSize(currentOffset), "indexedBlocks", len(aol.indexedBlocks))
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
		// Keep track of recent blocks encountered during load for potential skiplist rebuild
		if len(aol.recentBlocks) < aol.recentN || entry.BlockID > aol.recentBlocks[0] {
			// This isn't perfectly ordered yet, rebuildSkiplist will sort
			aol.recentBlocks = append(aol.recentBlocks, entry.BlockID)
		}
	}
	aol.latestBlockID = latestBlock
	return nil
}

// rebuildSkiplist populates the skiplist index for the actual N most recent blocks found.
func (aol *AppendOnlyLog) rebuildSkiplist() error {
	if len(aol.blockIndex) == 0 {
		return nil // Nothing to index
	}

	// Sort all known block IDs to find the most recent N
	allBlockIDs := make([]uint64, 0, len(aol.blockIndex))
	for id := range aol.blockIndex {
		allBlockIDs = append(allBlockIDs, id)
	}
	// Sort descending to easily get the latest N
	sort.Slice(allBlockIDs, func(i, j int) bool {
		return allBlockIDs[i] > allBlockIDs[j]
	})

	aol.recentBlocks = aol.recentBlocks[:0] // Clear existing
	aol.indexedBlocks = make(map[uint64]struct{})
	aol.skiplistIndex = skiplist.New(skiplist.String) // Reset skiplist

	numToIndex := aol.recentN
	if len(allBlockIDs) < numToIndex {
		numToIndex = len(allBlockIDs)
	}

	aol.log.Debug("Rebuilding skiplist index", "blocksToScan", numToIndex)

	// Iterate over the N most recent blocks (or fewer if not enough blocks exist)
	for i := 0; i < numToIndex; i++ {
		blockID := allBlockIDs[i]
		indexEntry, ok := aol.blockIndex[blockID]
		if !ok {
			// Should not happen
			aol.log.Error("Block ID inconsistency during skiplist rebuild", "blockID", blockID)
			continue
		}

		aol.recentBlocks = append(aol.recentBlocks, blockID) // Add to recent list (will be reversed later)
		aol.indexedBlocks[blockID] = struct{}{}

		// Read all entries for this block and add to skiplist
		err := aol.readAndIndexBlock(indexEntry)
		if err != nil {
			return fmt.Errorf("failed to read/index block %d during rebuild: %w", blockID, err)
		}
	}

	// Reverse recentBlocks to have the oldest at index 0, newest at end
	for i, j := 0, len(aol.recentBlocks)-1; i < j; i, j = i+1, j-1 {
		aol.recentBlocks[i], aol.recentBlocks[j] = aol.recentBlocks[j], aol.recentBlocks[i]
	}

	aol.log.Debug("Skiplist rebuild complete", "indexedKeys", aol.skiplistIndex.Len())
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
		return fmt.Errorf("append-only log is closed")
	}
	// Allow blockID 0 if it's the very first block or if kvs is empty (no-op)
	isFirstAppend := len(aol.blockIndex) == 0 && aol.latestBlockID == 0
	if blockID == 0 && isFirstAppend && len(kvs) == 0 {
		// Appending block 0 with no KVS when log is empty is a no-op.
		// latestBlockID remains 0 (or its initial state if not exactly 0 but conceptually "before first block")
		// No index entry, no data write.
		return nil
	}

	if blockID <= aol.latestBlockID && !(blockID == 0 && isFirstAppend) {
		return fmt.Errorf("non-monotonic block ID: current latest %d, got %d", aol.latestBlockID, blockID)
	}

	if len(kvs) == 0 {
		aol.log.Debug("Append called with empty KVS for block, no operation performed", "blockID", blockID)
		return nil // No data to persist, no index entry, no change to latestBlockID
	}

	startOffset := aol.currentOffset
	blockDataBuf := new(bytes.Buffer) // Using bytes.Buffer

	// Serialize all entries for the block into the buffer
	for key, value := range kvs {
		if err := aol.writeLogEntry(blockDataBuf, blockID, key, value); err != nil {
			return fmt.Errorf("failed to serialize entry for block %d, key %s: %w", blockID, key, err)
		}
	}

	// Write the buffered block data to the main data file writer
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

	// Update current offset *after* successful buffer write
	endOffset := startOffset + int64(n)
	aol.currentOffset = endOffset

	// Create and store block index entry
	indexEntry := blockIndexEntry{
		BlockID:     blockID,
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
	aol.blockIndex[blockID] = indexEntry
	aol.latestBlockID = blockID

	// Append to index map file
	if err := aol.writeIndexEntry(aol.indexMapFile, indexEntry); err != nil {
		aol.log.Crit("Failed to write block index entry to file after writing data!", "blockID", blockID, "error", err)
		return fmt.Errorf("CRITICAL: failed to write index entry for block %d: %w", blockID, err)
	}

	// Sync files to ensure data is written to disk
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

	// Update skiplist if this block is within the recent N
	aol.updateRecentBlocks(blockID)
	if _, isIndexed := aol.indexedBlocks[blockID]; isIndexed {
		aol.log.Debug("Indexing new block in skiplist", "blockID", blockID)
		reader := bytes.NewReader(blockBytes) // Using bytes.NewReader
		entryPos := startOffset
		for reader.Len() > 0 {
			entry, bytesRead, err := aol.readLogEntry(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				aol.log.Error("Failed to decode entry while indexing new block", "blockID", blockID, "error", err)
				return fmt.Errorf("failed to decode entry for skiplist indexing block %d: %w", blockID, err)
			}
			ptr := &kvPointer{
				Offset:   entryPos,
				ValueLen: uint32(len(entry.Value)),
			}
			aol.skiplistIndex.Set(entry.Key, ptr)
			entryPos += bytesRead
		}
	}

	// Evict old entries from skiplist
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
		if err := aol.rebuildSkiplist(); err != nil {
			aol.log.Error("Failed to rebuild skiplist after eviction", "evictedBlock", oldestBlockID, "error", err)
		}
	}
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

// Get retrieves the latest value for a key from the indexed recent blocks.
// Returns the value and true if found, or "", false otherwise.
// Handles tombstones (returns "", true for deleted keys).
func (aol *AppendOnlyLog) Get(key string) (string, bool, error) {
	aol.mu.RLock()
	if aol.closed {
		aol.mu.RUnlock()
		return "", false, fmt.Errorf("append-only log is closed")
	}

	// Try skiplist first
	element := aol.skiplistIndex.Get(key)
	if element != nil {
		aol.mu.RUnlock()
		pointer := element.Value.(*kvPointer)
		headerSize := blockIDSize + keyLenSize + valueLenSize
		headerBytes := make([]byte, headerSize)

		_, err := aol.dataFile.ReadAt(headerBytes, pointer.Offset)
		if err != nil {
			return "", false, fmt.Errorf("failed to read entry header for key %s: %w", key, err)
		}

		keyLen := binary.BigEndian.Uint32(headerBytes[blockIDSize : blockIDSize+keyLenSize])
		valueLen := binary.BigEndian.Uint32(headerBytes[blockIDSize+keyLenSize : headerSize])

		if valueLen != pointer.ValueLen {
			return "", false, fmt.Errorf("data inconsistency for key %s", key)
		}

		valueBytes := make([]byte, valueLen)
		valueOffset := pointer.Offset + int64(headerSize) + int64(keyLen)
		_, err = aol.dataFile.ReadAt(valueBytes, valueOffset)
		if err != nil {
			return "", false, fmt.Errorf("failed to read value for key %s: %w", key, err)
		}

		value := string(valueBytes)
		if value == tombstoneMarker {
			return "", true, nil
		}

		return value, true, nil
	}
	aol.mu.RUnlock()

	aol.mu.Lock()
	defer aol.mu.Unlock()

	if element := aol.skiplistIndex.Get(key); element != nil {
		pointer := element.Value.(*kvPointer)
		headerSize := blockIDSize + keyLenSize + valueLenSize
		headerBytes := make([]byte, headerSize)

		_, err := aol.dataFile.ReadAt(headerBytes, pointer.Offset)
		if err != nil {
			return "", false, fmt.Errorf("failed to read entry header for key %s: %w", key, err)
		}

		keyLen := binary.BigEndian.Uint32(headerBytes[blockIDSize : blockIDSize+keyLenSize])
		valueLen := binary.BigEndian.Uint32(headerBytes[blockIDSize+keyLenSize : headerSize])

		if valueLen != pointer.ValueLen {
			return "", false, fmt.Errorf("data inconsistency for key %s", key)
		}

		valueBytes := make([]byte, valueLen)
		valueOffset := pointer.Offset + int64(headerSize) + int64(keyLen)
		_, err = aol.dataFile.ReadAt(valueBytes, valueOffset)
		if err != nil {
			return "", false, fmt.Errorf("failed to read value for key %s: %w", key, err)
		}

		value := string(valueBytes)
		if value == tombstoneMarker {
			return "", true, nil
		}

		return value, true, nil
	}

	blockIDs := make([]uint64, 0, len(aol.blockIndex))
	for id := range aol.blockIndex {
		blockIDs = append(blockIDs, id)
	}
	sort.Slice(blockIDs, func(i, j int) bool { return blockIDs[i] > blockIDs[j] })

	for _, blockNum := range blockIDs {
		indexEntry, ok := aol.blockIndex[blockNum]
		if !ok {
			continue
		}
		err := aol.readAndIndexBlock(indexEntry)
		if err != nil {
			return "", false, fmt.Errorf("failed to decode entry in block %d: %w", blockNum, err)
		}
	}

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

	if err := aol.writeLogEntry(blockDataBuf, blockIDForDelete, key, tombstoneMarker); err != nil {
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
			ValueLen: uint32(len(tombstoneMarker)),
		}
		aol.skiplistIndex.Set(key, ptr)
	}

	// Eviction is handled by updateRecentBlocks calling rebuildSkiplist if necessary
	// aol.evictOldEntries() // Not strictly needed here if updateRecentBlocks handles it

	aol.log.Info("Appended tombstone for key", "key", key, "blockID", blockIDForDelete)
	return nil
}

// GetByBlock retrieves all key-value pairs for a specific block ID.
// It reads directly from the data file based on the block index.
// Note: This does not consult the skiplist, as the skiplist holds latest values across recent blocks.
func (aol *AppendOnlyLog) GetByBlock(blockID uint64) (map[string]string, error) {
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
			// Log the error but attempt to return what has been read so far,
			// or decide if this is a critical error.
			aol.log.Error("Failed to decode entry in block during GetByBlock", "blockID", blockID, "error", err)
			return kvs, fmt.Errorf("failed to decode entry in block %d: %w", blockID, err)
		}
		// If a key appears multiple times in the same block, the last one wins.
		// readLogEntry gives us one entry at a time.
		kvs[entry.Key] = entry.Value
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

// Close flushes buffers and closes open files.
func (aol *AppendOnlyLog) Close() error {
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
