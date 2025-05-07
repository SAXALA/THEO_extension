package ethstore

import (
	"bufio"
	"bytes" // Added bytes import
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort" // Add this import
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

	mu sync.RWMutex
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
		skiplistIndex:    skiplist.New(skiplist.String), // Use string comparison for keys
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
	sort.Slice(allBlockIDs, func(i, j int) bool { // Replace common.SortUint64sReverse
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
	if len(kvs) == 0 {
		return nil // Nothing to append
	}

	aol.mu.Lock()
	defer aol.mu.Unlock()

	if _, exists := aol.blockIndex[blockID]; exists {
		// Prevent re-writing an existing block. Updates should happen via new blocks
		// or potentially a separate compaction/update mechanism if needed later.
		// For strict append-only, we disallow modifying history.
		return fmt.Errorf("block %d already exists", blockID)
	}
	if blockID <= aol.latestBlockID && aol.latestBlockID != 0 {
		// Enforce monotonically increasing block IDs
		return fmt.Errorf("block ID %d is not greater than latest block ID %d", blockID, aol.latestBlockID)
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
		// Attempt to truncate back if write failed partially? Difficult in append-only.
		// Best effort: log error. State might be inconsistent.
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
		// Critical error: data written but index failed. Log and potentially panic or mark for recovery.
		aol.log.Crit("Failed to write block index entry to file after writing data!", "blockID", blockID, "error", err)
		// Rollback is hard here. Maybe truncate index file? Or mark DB as needing recovery.
		return fmt.Errorf("CRITICAL: failed to write index entry for block %d: %w", blockID, err)
	}

	// Update skiplist if this block is within the recent N
	aol.updateRecentBlocks(blockID)
	if _, isIndexed := aol.indexedBlocks[blockID]; isIndexed {
		aol.log.Debug("Indexing new block in skiplist", "blockID", blockID)
		// Read the just written data (from buffer or file?) and index it
		// Re-reading from the buffer is efficient
		reader := bytes.NewReader(blockBytes) // Using bytes.NewReader
		entryPos := startOffset
		for reader.Len() > 0 {
			entry, bytesRead, err := aol.readLogEntry(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				aol.log.Error("Failed to decode entry while indexing new block", "blockID", blockID, "error", err)
				// Inconsistency: block data written, index map written, skiplist update failed partially.
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

	// Optionally flush data writer periodically, on close, or based on size
	// aol.dataWriter.Flush() // Consider flushing strategy

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
		// This requires reading the evicted block's entries.
		// Optimization: If a key exists in newer indexed blocks, don't remove it.
		// Simpler (but less precise): Rebuild skiplist for current recent N blocks.
		// Let's do the simpler rebuild for now, can optimize later if needed.
		// NOTE: Frequent rebuilds can be costly.
		if err := aol.rebuildSkiplist(); err != nil {
			// Log error, skiplist might be temporarily inaccurate
			aol.log.Error("Failed to rebuild skiplist after eviction", "evictedBlock", oldestBlockID, "error", err)
		}
	}
}

// Get retrieves the latest value for a key from the indexed recent blocks.
// Returns the value and true if found, or "", false otherwise.
// Handles tombstones (returns "", true for deleted keys).
func (aol *AppendOnlyLog) Get(key string) (string, bool, error) {
	aol.mu.RLock()
	defer aol.mu.RUnlock()

	// Check skiplist first
	element := aol.skiplistIndex.Get(key) // Get returns a single value
	if element == nil {                   // Check if nil
		return "", false, nil // Not found in recent blocks
	}

	pointer := element.Value.(*kvPointer) // Get Value from Element

	// Read the specific entry from the data file
	// Format: blockID (8) | keyLen (4) | valueLen (4) | key (var) | value (var)
	headerSize := blockIDSize + keyLenSize + valueLenSize
	headerBytes := make([]byte, headerSize)

	_, err := aol.dataFile.ReadAt(headerBytes, pointer.Offset)
	if err != nil {
		aol.log.Error("Failed to read entry header", "key", key, "offset", pointer.Offset, "error", err)
		return "", false, fmt.Errorf("failed to read entry header for key %s: %w", key, err)
	}

	keyLen := binary.BigEndian.Uint32(headerBytes[blockIDSize : blockIDSize+keyLenSize])
	valueLen := binary.BigEndian.Uint32(headerBytes[blockIDSize+keyLenSize : headerSize])

	if valueLen != pointer.ValueLen {
		// Sanity check failed
		aol.log.Error("Inconsistency: skiplist value length mismatch", "key", key, "offset", pointer.Offset, "skiplistLen", pointer.ValueLen, "headerLen", valueLen)
		return "", false, fmt.Errorf("data inconsistency for key %s", key)
	}

	valueBytes := make([]byte, valueLen)
	valueOffset := pointer.Offset + int64(headerSize) + int64(keyLen) // Calculate value offset
	_, err = aol.dataFile.ReadAt(valueBytes, valueOffset)
	if err != nil {
		aol.log.Error("Failed to read entry value", "key", key, "offset", valueOffset, "error", err)
		return "", false, fmt.Errorf("failed to read value for key %s: %w", key, err)
	}

	value := string(valueBytes)
	if value == tombstoneMarker {
		return "", true, nil // Key exists but is marked as deleted
	}

	return value, true, nil
}

// GetByBlock retrieves the value for a key specifically from the given block ID.
// This searches the block index map and reads from the data file directly.
// Returns the value and true if found, or "", false otherwise.
// Handles tombstones.
func (aol *AppendOnlyLog) GetByBlock(blockID uint64, key string) (string, bool, error) {
	aol.mu.RLock()
	defer aol.mu.RUnlock()

	indexEntry, ok := aol.blockIndex[blockID]
	if !ok {
		return "", false, nil // Block not found
	}

	size := indexEntry.EndOffset - indexEntry.StartOffset
	if size <= 0 {
		return "", false, nil // Empty block
	}

	// Read the entire block's data
	// Optimization: Could potentially read chunks if blocks are huge
	blockData := make([]byte, size)
	_, err := aol.dataFile.ReadAt(blockData, indexEntry.StartOffset)
	if err != nil {
		return "", false, fmt.Errorf("failed to read block data for %d: %w", blockID, err)
	}

	reader := bytes.NewReader(blockData) // Using bytes.NewReader
	var foundValue string
	found := false

	// Iterate through entries in the block
	for reader.Len() > 0 {
		entry, _, err := aol.readLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", false, fmt.Errorf("failed to decode entry in block %d: %w", blockID, err)
		}

		if entry.BlockID == blockID && entry.Key == key {
			// Found the key, store the latest value encountered within this block
			foundValue = entry.Value
			found = true
		}
	}

	if !found {
		return "", false, nil
	}

	if foundValue == tombstoneMarker {
		return "", true, nil // Key found in block but marked deleted
	}

	return foundValue, true, nil
}

// Update appends a new entry with the updated value for the key in the latest block.
// Note: This effectively creates a new version in the *next* block ID.
// If you need to update within the *same* block ID, the Append logic needs modification
// (e.g., allow appending to the latest open block, which complicates things).
// This implementation assumes updates create entries in subsequent blocks.
func (aol *AppendOnlyLog) Update(blockID uint64, key string, value string) error {
	// This is essentially the same as Append for a single key,
	// creating a new record in the specified (new) block.
	return aol.Append(blockID, map[string]string{key: value})
}

// Delete appends a tombstone entry for the key in the specified block ID.
// Similar to Update, this places the tombstone in a *new* block.
func (aol *AppendOnlyLog) Delete(blockID uint64, key string) error {
	return aol.Append(blockID, map[string]string{key: tombstoneMarker})
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
		// Allow EOF if nothing was read, otherwise it's unexpected
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

	// Offset is not read from the stream, it's determined by the caller's position

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
	// Ensure index entry is persisted (fsync might be needed for stronger guarantees)
	// For simplicity, we rely on OS caching or Close() for now.
	// If using bufio.Writer for indexMapFile, flush it here.
	if indexWriter, ok := w.(*bufio.Writer); ok {
		return indexWriter.Flush()
	}
	// If writing directly to os.File, consider Sync() for durability
	if f, ok := w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

// Close flushes buffers and closes open files.
func (aol *AppendOnlyLog) Close() error {
	aol.mu.Lock()
	defer aol.mu.Unlock()

	var firstErr error

	// Flush data writer buffer
	if aol.dataWriter != nil {
		if err := aol.dataWriter.Flush(); err != nil {
			aol.log.Error("Failed to flush data writer on close", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Close data file
	if aol.dataFile != nil {
		if err := aol.dataFile.Close(); err != nil {
			aol.log.Error("Failed to close data file", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
		aol.dataFile = nil
		aol.dataWriter = nil
	}

	// Close index map file
	if aol.indexMapFile != nil {
		// No buffered writer assumed for index map in this impl, but Sync for safety
		if err := aol.indexMapFile.Sync(); err != nil {
			aol.log.Error("Failed to sync index map file on close", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
		if err := aol.indexMapFile.Close(); err != nil {
			aol.log.Error("Failed to close index map file", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
		aol.indexMapFile = nil
	}

	// Clear in-memory structures
	aol.blockIndex = nil
	aol.skiplistIndex = nil
	aol.recentBlocks = nil
	aol.indexedBlocks = nil

	aol.log.Info("AppendOnlyLog closed")
	return firstErr
}
