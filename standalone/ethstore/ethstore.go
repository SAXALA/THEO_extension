package ethstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"context"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
)

// Define custom errors to replace ethdb's if they are undefined
var (
	ErrClosed     = errors.New("database closed")
	ErrNotFound   = errors.New("not found")
	ErrCompaction = errors.New("compaction error") // Example, if you need more
)

var aolHandledDataTypes = map[DataType]bool{
	HeaderDataType:                    true,
	HeaderNumberDataType:              true,
	BlockBodyDataType:                 true,
	BlockReceiptsDataType:             true,
	TransactionLookupMetadataDataType: true,
}

const aolDeleteTombstone = "__AOL_DELETED__"

// txLookupRLP is a local struct definition for RLP decoding TransactionLookupMetadata.
type txLookupRLP struct {
	BlockHash   common.Hash
	BlockNumber uint64
	TxIndex     uint64
}

// parseBlockNumberFromKey tries to parse the block number from the key structure
// for data types where it's expected (e.g., Header, BlockBody, BlockReceipts).
// Key format is assumed to be: prefix (1 byte) + num (8 bytes) + ...
func parseBlockNumberFromKey(key []byte, dataType DataType) (uint64, bool) {
	switch dataType {
	case HeaderDataType, BlockBodyDataType, BlockReceiptsDataType:
		if len(key) >= 9 { // 1 byte prefix + 8 bytes for uint64
			return binary.BigEndian.Uint64(key[1:9]), true
		}
	}
	return 0, false
}

// parseBlockNumberFromValue tries to parse the block number from the value structure
// for data types like HeaderNumber (value is block number) or TransactionLookupMetadata (value is RLP encoded).
func parseBlockNumberFromValue(value []byte, dataType DataType, logger log.Logger) (uint64, bool) {
	switch dataType {
	case HeaderNumberDataType: // Value is num (uint64 big endian)
		if len(value) == 8 { // Must be exactly 8 bytes for uint64
			return binary.BigEndian.Uint64(value), true
		}
		if logger != nil {
			logger.Warn("Invalid value length for HeaderNumber to parse blockID", "len", len(value))
		}
	case TransactionLookupMetadataDataType: // Value is rlp([blockhash, blocknum, txindex])
		var entry txLookupRLP
		if err := rlp.DecodeBytes(value, &entry); err == nil {
			return entry.BlockNumber, true
		} else if logger != nil {
			logger.Warn("Failed to RLP decode TransactionLookupMetadata to parse blockID", "err", err)
		}
	}
	return 0, false
}

// Database is a persistent key-value store based on the append-only log store.
type Database struct {
	fn  string         // filename/directory for reporting
	aol *AppendOnlyLog // Underlying append-only log store
	db  *PebbleStore   // Pebble store for non-AOL data

	diskSizeGauge *metrics.Gauge // Gauge for tracking the size of all the data in the database

	quitLock sync.RWMutex    // Mutex protecting the quit channel and the closed flag
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database
	closed   bool            // keep track of whether we're Closed

	log log.Logger // Contextual logger tracking the database path
}

// New returns a wrapped EthStore object using AppendOnlyLog.
// The namespace is the prefix that the metrics reporting should use.
// cache and handles parameters might be less relevant for AppendOnlyLog,
// but recentN (number of blocks to index) becomes important.
func New(dirPath string, recentN int, namespace string, readonly bool) (*Database, error) {
	logger := log.New("database", dirPath)

	db := &Database{
		fn:       dirPath, // Use directory path now
		log:      logger,
		quitChan: make(chan chan error),
	}

	// Initialize the AppendOnlyLog store
	if recentN <= 0 {
		// Use defaultRecentN from blockStore.go (implicitly, as it's used in NewAppendOnlyLog)
		// Or explicitly pass it if needed: recentN = defaultRecentN
	}
	logger.Info("Initializing AppendOnlyLog store", "recentN", recentN) // recentN will be default if <= 0

	appendLog, err := NewAppendOnlyLog(dirPath, recentN, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize append-only log: %w", err)
	}
	db.aol = appendLog

	// Initialize Pebble store for non-AOL data
	pebblePath := filepath.Join(dirPath, "pebble")
	logger.Info("Initializing Pebble store", "path", pebblePath)
	pebbleStore, err := NewPebbleStore(pebblePath)
	if err != nil {
		// Close AOL if Pebble initialization fails
		appendLog.Close()
		return nil, fmt.Errorf("failed to initialize pebble store: %w", err)
	}
	db.db = pebbleStore

	// Initialize metrics
	db.diskSizeGauge = metrics.GetOrRegisterGauge(namespace+"disk/size", nil)

	return db, nil
}

// Close stops the metrics collection and closes all io accesses to the underlying key-value store.
func (d *Database) Close() error {
	d.quitLock.Lock()
	defer d.quitLock.Unlock()
	if d.closed {
		return ErrClosed // If you want to signal it was already closed
	}
	d.closed = true
	if d.quitChan != nil {
		errc := make(chan error)
		d.quitChan <- errc
		// Handle potential error from metrics shutdown if it existed
		select {
		case err := <-errc:
			if err != nil {
				d.log.Error("Metrics collection failed", "err", err)
			}
		case <-time.After(1 * time.Second): // Add timeout
			d.log.Warn("Timeout waiting for metrics shutdown")
		}
		close(d.quitChan) // Close the channel itself
		d.quitChan = nil
	}
	// First close Pebble store
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			d.log.Error("Failed to close Pebble store", "err", err)
			// Continue trying to close AOL, but remember Pebble's error
			if aolErr := d.aol.Close(); aolErr != nil {
				return fmt.Errorf("failed to close stores: pebble: %v, aol: %v", err, aolErr)
			}
			return fmt.Errorf("failed to close Pebble store: %v", err)
		}
	}
	// Then close AOL
	return d.aol.Close()
}

// Has retrieves if a key is present in the key-value store.
func (d *Database) Has(key []byte) (bool, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return false, ErrClosed
	}
	
	dataType := GetDataTypeFromKey(key)

	if aolHandledDataTypes[dataType] {
		if d.aol == nil {
			return false, fmt.Errorf("AOL is not initialized, cannot check key %x (type %s)", key, DataTypeStrings[dataType])
			}
		
			// First check if the key exists in AOL
		valStr, exists, err := d.aol.Get(string(key))
		if err != nil {
			return false, err
		}
		if exists {
			// If it's a deletion marker, consider the key non-existent
			if valStr == aolDeleteTombstone {
				return false, nil
			}
			return true, nil
		}
		// If the key doesn't exist in AOL, continue to look in Pebble
	}

	// Check if the key exists in Pebble
	_, err := d.db.Get(key)
	if err == nil {
		return true, nil // Key exists
	} else if err == pebble.ErrNotFound {
		return false, nil // Key doesn't exist
	}
	return false, err // Other error occurred
}

// Get retrieves the given key if it's present in the key-value store.
func (d *Database) Get(key []byte) ([]byte, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	
	dataType := GetDataTypeFromKey(key)

	if aolHandledDataTypes[dataType] {
		if d.aol == nil {
			return nil, fmt.Errorf("AOL is not initialized, cannot get key %x (type %s)", key, DataTypeStrings[dataType])
			}
		
			// First try to get from AOL
		valStr, exists, err := d.aol.Get(string(key))
		if err != nil {
			return nil, err
		}
		if exists {
			// If it's a deletion marker, return not found error
			if valStr == aolDeleteTombstone {
				return nil, ErrNotFound
			}
			// Return the found value
			return []byte(valStr), nil
		}
		// Key doesn't exist in AOL, continue to look in Pebble
		d.log.Trace("Key not found in AOL, checking Pebble", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
	}

	// Try to get from Pebble
	value, err := d.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, ErrNotFound // Convert to EthStore specific ErrNotFound
		}
		return nil, err
	}
	return value, nil
}

// Put stores the given key-value pair.
// If the key belongs to specific types (Header, HeaderNumber, etc.), it's stored in the Append-Only Log (AOL).
// Otherwise, it's stored in the underlying key-value database.
func (d *Database) Put(key []byte, value []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()

	if d.closed {
		return ErrClosed
	}

	dataType := GetDataTypeFromKey(key)

	if aolHandledDataTypes[dataType] {
		if d.aol == nil {
			return fmt.Errorf("AOL is not initialized, cannot store key %x (type %s)", key, DataTypeStrings[dataType])
		}
		var blockID uint64
		var foundBlockID bool

		// Try to get blockID from key
		blockID, foundBlockID = parseBlockNumberFromKey(key, dataType)

		// If not found in key, try from value (for HeaderNumber, TxLookup)
		if !foundBlockID {
			blockID, foundBlockID = parseBlockNumberFromValue(value, dataType, d.log)
		}

		if foundBlockID {
			kvs := map[string]string{string(key): string(value)}
			err := d.aol.Append(blockID, kvs)
			if err != nil {
				return fmt.Errorf("aol append failed for key %x (type %s, blockID %d): %w", key, DataTypeStrings[dataType], blockID, err)
			}
			d.log.Trace("Stored key via AOL", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType], "blockID", blockID)
			return nil // Data stored in AOL
		}
		// If blockID couldn't be determined for an AOL-handled type.
		return fmt.Errorf("could not determine blockID for AOL-handled type %s for key %x; storage via AOL failed", DataTypeStrings[dataType], key)
	}

	// Default: store non-AOL data in Pebble
	if d.db == nil {
		return fmt.Errorf("Pebble store is not initialized, cannot store non-AOL key %x (type %s)", key, DataTypeStrings[dataType])
	}
	err := d.db.Put(key, value)
	if err != nil {
		return fmt.Errorf("pebble put failed for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
	}
	d.log.Trace("Stored key via Pebble", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
	return nil
}

// Delete removes the given key.
// If the key belongs to specific types and can be handled by AOL (e.g., Header),
// a tombstone record is appended to the AOL.
// Otherwise, it's deleted from the underlying key-value database.
// Deletion for types like HeaderNumber and TransactionLookupMetadata via AOL is not supported
// with this method as blockID cannot be derived from the key alone.
func (d *Database) Delete(key []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	
	if d.closed {
		return ErrClosed
	}
	
	dataType := GetDataTypeFromKey(key)

	if aolHandledDataTypes[dataType] {
		if d.aol == nil {
			return fmt.Errorf("AOL is not initialized, cannot delete key %x (type %s)", key, DataTypeStrings[dataType])
		}
		var blockID uint64
		var foundBlockID bool

		// Try to get blockID from key (works for Header, BlockBody, BlockReceipts)
		blockID, foundBlockID = parseBlockNumberFromKey(key, dataType)

		if !foundBlockID {
			d.log.Warn("AOL delete for type not supported as blockID cannot be derived from key alone", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
			return fmt.Errorf("cannot determine blockID from key for AOL-handled type %s (key %x) during delete; AOL delete not supported for this type via this path", DataTypeStrings[dataType], key)
		}

		kvs := map[string]string{string(key): aolDeleteTombstone}
		err := d.aol.Append(blockID, kvs)
		if err != nil {
			return fmt.Errorf("aol append (delete tombstone) failed for key %x (type %s, blockID %d): %w", key, DataTypeStrings[dataType], blockID, err)
		}
		d.log.Trace("Stored delete tombstone via AOL", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType], "blockID", blockID)
		return nil // Deletion marker stored in AOL
	}

	// Default: delete from Pebble
	if d.db == nil {
		return fmt.Errorf("Pebble store is not initialized, cannot delete non-AOL key %x (type %s)", key, DataTypeStrings[dataType])
	}
	err := d.db.Delete(key)
	if err != nil {
		return fmt.Errorf("pebble delete failed for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
	}
	d.log.Trace("Deleted key via Pebble", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
	return nil
}

// DeleteRange removes all keys between start and end (exclusive of end).
// WARNING: This is a placeholder implementation. True range deletion is complex
// for an append-only log and might not be fully supported or may require
// a different approach (e.g., marking a range as deleted for future compaction).
func (d *Database) DeleteRange(start, end []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return ErrClosed
	}
	d.log.Warn("DeleteRange is not efficiently implemented for AppendOnlyLog; this is a placeholder.", "start", string(start), "end", string(end))
	return nil
}

// Path returns the path to the database directory.
func (d *Database) Path() string {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return "" // Or handle appropriately
	}
	return d.aol.Path()
}

// Retrieve retrieves the value for a key from the database.
// The key should not contain the type prefix.
func (d *Database) Retrieve(ctx context.Context, dataType DataType, key []byte) ([]byte, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	
	// Get the prefix for the current data type
	var prefix []byte
	for _, keyInfo := range keyPrefixes {
		if keyInfo.DataType == dataType {
			prefix = []byte{keyInfo.Prefix}
			break
		}
	}
	if prefix == nil {
		return nil, fmt.Errorf("unknown data type: %d", dataType)
	}
	
	// Construct the complete key (add prefix)
	fullKey := append(prefix, key...)
	
	// Call the Get method to retrieve data (Get method already handles AOL and Pebble logic)
	return d.Get(fullKey)
}

// RetrieveByPrefix retrieves an iterator for keys starting with a given prefix.
// The prefix should not contain the type prefix.
func (d *Database) RetrieveByPrefix(ctx context.Context, dataType DataType, prefix []byte) (ethdb.Iterator, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	
	// Get the prefix for the data type
	var typePrefix byte
	for _, keyInfo := range keyPrefixes {
		if keyInfo.DataType == dataType {
			typePrefix = keyInfo.Prefix
			break
		}
	}
	
	// Build the complete prefix (type prefix + user prefix)
	fullPrefix := append([]byte{typePrefix}, prefix...)
	
	// Create iterator options for Pebble
	iterOpts := &pebble.IterOptions{
		LowerBound: fullPrefix,
		UpperBound: append(append([]byte{}, fullPrefix...), 0xFF), // Set upper bound to the maximum possible value for the prefix
	}
	
	// Create Pebble iterator
	pebbleIter := d.db.NewIterator(iterOpts)
	
	// Wrap as an iterator that implements the ethdb.Iterator interface
	return &ethdbIterator{
		iter:      pebbleIter,
		typePrefix: typePrefix,
		prefixLen: len(fullPrefix),
	}, nil
}

// ethdbIterator is a wrapper implementing the ethdb.Iterator interface for Pebble iterator
type ethdbIterator struct {
	iter      *pebble.Iterator
	typePrefix byte
	prefixLen int
	valid     bool
	err       error
}

// Next moves to the next entry
func (it *ethdbIterator) Next() bool {
	// Move to the first element on the first call to Next()
	if !it.valid {
		it.valid = true
		return it.iter.First()
	}
	// Subsequent calls move to the next element
	return it.iter.Next()
}

// Error returns the iterator's error
func (it *ethdbIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.iter.Error()
}

// Key returns the key of the current entry, removing the type prefix
func (it *ethdbIterator) Key() []byte {
	if !it.iter.Valid() {
		return nil
	}
	key := it.iter.Key()
	// Return the key after removing the type prefix
	if len(key) > 1 && key[0] == it.typePrefix {
		return key[1:]
	}
	return key
}

// Value returns the value of the current entry
func (it *ethdbIterator) Value() []byte {
	if !it.iter.Valid() {
		return nil
	}
	return it.iter.Value()
}

// Release releases the iterator resources
func (it *ethdbIterator) Release() {
	it.iter.Close()
}

// --- Methods below need implementation or removal ---

// NewBatch creates a write-only database batch object.
func (d *Database) NewBatch() ethdb.Batch {
	d.log.Warn("NewBatch is not implemented for AppendOnlyLog; returning nil.")
	return nil // Placeholder
}

// NewBatchWithSize creates a write-only database batch object with pre-allocated buffer size.
func (d *Database) NewBatchWithSize(size int) ethdb.Batch {
	d.log.Warn("NewBatchWithSize is not implemented for AppendOnlyLog; returning nil.")
	return nil // Placeholder
}

// NewIterator creates a binary-alphabetical iterator over a subset of database content.
func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	d.log.Warn("NewIterator is not implemented for AppendOnlyLog; returning nil.")
	return nil // Placeholder
}

// Stat returns a particular internal stat of the database.
func (d *Database) Stat() (string, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return "", ErrClosed
	}
	return fmt.Sprintf("ethstore(path=%s, recentN=%d)", d.aol.Path(), d.aol.RecentN()), nil
}

// Compact flattens the underlying data store for the given key range.
func (d *Database) Compact(start []byte, limit []byte) error {
	d.log.Warn("Compact operation may not be applicable or is handled differently by AppendOnlyLog")
	return nil // Or return an error if not supported
}

// batch is a wrapper around AppendOnlyLog operations potentially for a single block.
// Needs complete redesign.
type batch struct {
	db      *Database
	blockID uint64            // The block ID for this batch
	kvs     map[string]string // Key-value pairs for the batch
	size    int               // Approximate size
	lock    sync.Mutex
}

// iterator is a wrapper around AppendOnlyLog data access.
// Needs complete redesign. Might only iterate over skiplist or require full scan.
type iterator struct {
	db *Database
	// ... fields for iteration state ...
}
