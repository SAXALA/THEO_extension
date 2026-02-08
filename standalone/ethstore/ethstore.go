package ethstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tinoryj/EthStore/standalone/ethstore/prefixdb"
)

// backendStats tracks operation counts and durations
type backendStats struct {
	ops   uint64
	durNs uint64
}

// errorIterator is an ethdb.Iterator that always returns an error or represents an invalid state.
type errorIterator struct {
	err error
}

// Next implements ethdb.Iterator, always returns false for errorIterator.
func (it *errorIterator) Next() bool { return false }

// Error implements ethdb.Iterator, returns the predefined error.
func (it *errorIterator) Error() error { return it.err }

// Key implements ethdb.Iterator, returns nil for errorIterator.
func (it *errorIterator) Key() []byte { return nil }

// Value implements ethdb.Iterator, returns nil for errorIterator.
func (it *errorIterator) Value() []byte { return nil }

// Release implements ethdb.Iterator, is a no-op for errorIterator.
func (it *errorIterator) Release() {}

// errorBatch is an ethdb.Batch that always returns an error.
// It's used when a real batch cannot be created (e.g., DB is closed).
type errorBatch struct {
	err error
}

// Put implements ethdb.Batch and returns the predefined error.
func (b *errorBatch) Put(key []byte, value []byte) error { return b.err }

// Delete implements ethdb.Batch and returns the predefined error.
func (b *errorBatch) Delete(key []byte) error { return b.err }

// ValueSize implements ethdb.Batch and returns 0.
func (b *errorBatch) ValueSize() int { return 0 }

// Write implements ethdb.Batch and returns the predefined error.
func (b *errorBatch) Write() error { return b.err }

// Reset implements ethdb.Batch and is a no-op.
func (b *errorBatch) Reset() {}

// Replay implements ethdb.Batch and returns the predefined error.
func (b *errorBatch) Replay(w ethdb.KeyValueWriter) error { return b.err }

var AolHandledDataTypes = map[DataType]bool{
	HeaderDataType:        true,
	BlockBodyDataType:     true,
	BlockReceiptsDataType: true,
	// TransactionLookupMetadataDataType: true,
	// HeaderNumberDataType:              true,
}

var PrefixDBHandledDataTypes = map[DataType]bool{
	TrieNodeAccountDataType: true,
	TrieNodeStorageDataType: true,
}

var SSPrefixdbHandledDataTypes = map[DataType]bool{
	SnapshotAccountDataType: true,
	SnapshotStorageDataType: true,
}

const aolDeleteTombstone = "__AOL_DELETED__"

// txLookupRLP is a local struct definition for RLP decoding TransactionLookupMetadata.
type txLookupRLP struct {
	BlockHash   common.Hash
	BlockNumber uint64
	TxIndex     uint64
}

// ParseBlockNumberFromKey tries to parse the block number from the key structure
// for data types where it's expected (e.g., Header, BlockBody, BlockReceipts).
// Key format is assumed to be: prefix (1 byte) + num (8 bytes) + ...
func ParseBlockNumberFromKey(key []byte, dataType DataType) (uint64, bool) {
	switch dataType {
	case HeaderDataType, BlockBodyDataType, BlockReceiptsDataType:
		if len(key) >= 9 { // 1 byte prefix + 8 bytes for uint64
			return binary.BigEndian.Uint64(key[1:9]), true
		}
	}
	return 0, false
}

// ParseBlockNumberFromKey tries to parse the block number from the key structure
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
func ParseBlockNumberFromValue(value []byte, dataType DataType) (uint64, bool) {
	switch dataType {
	case HeaderNumberDataType: // Value is num (uint64 big endian)
		if len(value) == 8 { // Must be exactly 8 bytes for uint64
			return binary.BigEndian.Uint64(value), true
		}

	case TransactionLookupMetadataDataType: // Value is rlp([blockhash, blocknum, txindex])
		if len(value) == 4 {
			return uint64(binary.BigEndian.Uint32(value)), true
		}
		var entry txLookupRLP
		if err := rlp.DecodeBytes(value, &entry); err == nil {
			return entry.BlockNumber, true
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
		if len(value) == 4 {
			return uint64(binary.BigEndian.Uint32(value)), true
		}
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
	fn         string                // filename/directory for reporting
	txIndexAol *TxIndexAppendOnlyLog // Underlying append-only log store
	db         *PebbleStore          // Pebble store for non-AOL data
	statepdb   *prefixdb.PrefixDB    // world state PrefixDB
	snappdb    *prefixdb.PrefixDB    // snapshot PrefixDB
	blockAol   *BlockAppendOnlyLog

	diskSizeGauge *metrics.Gauge // Gauge for tracking the size of all the data in the database

	quitLock sync.RWMutex    // Mutex protecting the quit channel and the closed flag
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database
	closed   bool            // keep track of whether we're Closed

	log log.Logger // Contextual logger tracking the database path

	baolkvs            map[string]string // Temporary storage for BlockAppendOnlyLog key-values during operations
	baolLatestBlock    uint64            // Temporary storage for latest block number during operations
	txIndexkvs         map[string]string // Temporary storage for TxIndexAppendOnlyLog key-values during operations
	txIndexLatestBlock uint64            // Temporary storage for latest block number during operations
	accountKey         []byte            // Temporary storage for account key during operations
	enableTxlookupData bool
}

// New returns a wrapped EthStore object using TxIndexAppendOnlyLog.
// The namespace is the prefix that the metrics reporting should use.
// cache and handles parameters might be less relevant for TxIndexAppendOnlyLog,
// but recentN (number of blocks to index) becomes important.
func New(dirPath string, recentN int, namespace string, readonly bool, enableTxlookupData bool) (*Database, error) {
	logger := log.New("database", dirPath)

	dirPathState := dirPath + "_state"
	statePrefixdb, err := prefixdb.NewPrefixDB(dirPathState, prefixdb.StateDB)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize prefixdb: %w", err)
	}

	dirPathSnapshot := dirPath + "_snapshot"
	snapshotPrefixdb, err := prefixdb.NewPrefixDB(dirPathSnapshot, prefixdb.SnapshotDB)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize prefixdb: %w", err)
	}
	db := &Database{
		fn:                 dirPath, // Use directory path now
		log:                logger,
		quitChan:           make(chan chan error, 1),
		statepdb:           statePrefixdb,
		snappdb:            snapshotPrefixdb,
		enableTxlookupData: enableTxlookupData,
	}

	// Initialize the TxIndexAppendOnlyLog store
	if recentN <= 0 {
		// Use defaultRecentN from blockStore.go (implicitly, as it's used in NewAppendOnlyLog)
		// Or explicitly pass it if needed: recentN = defaultRecentN
	}
	logger.Info("Initializing TxIndexAppendOnlyLog store", "recentN", recentN) // recentN will be default if <= 0

	appendLog, err := NewAppendOnlyLog(dirPath+"/aol", recentN, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize append-only log: %w", err)
	}
	db.txIndexAol = appendLog
	db.txIndexkvs = make(map[string]string)

	// Initialize BlockAppendOnlyLog
	baol, err := NewBlockAppendOnlyLog(dirPath+"/aol", recentN, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize block append-only log: %w", err)
	}
	db.blockAol = baol
	db.baolkvs = make(map[string]string)

	// Initialize Pebble store for non-AOL data
	pebblePath := dirPath + "_pebble"
	logger.Info("Initializing Pebble store", "path", pebblePath)
	// Pass 0 for cache and handles to use default values defined in NewPebbleStore.
	// Pass through namespace and readonly from the New function's parameters.

	pebbleStore, err := NewPebbleStore(pebblePath, 0, 0, namespace, readonly)
	if err != nil {
		// Close AOL if Pebble initialization fails
		db.statepdb.Close()
		db.snappdb.Close()
		appendLog.Close()
		baol.Close()
		return nil, fmt.Errorf("failed to initialize pebble store: %w", err)
	}
	db.db = pebbleStore

	// Initialize metrics
	db.diskSizeGauge = metrics.GetOrRegisterGauge(namespace+"disk/size", nil)

	// go func() {
	// 	for errc := range db.quitChan {
	// 		errc <- nil
	// 		return
	// 	}
	// }()

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

	if d.statepdb != nil {
		if err := d.statepdb.Close(); err != nil {
			d.log.Error("Failed to close state prefixDB", "err", err)
		}
	}

	if d.snappdb != nil {
		if err := d.snappdb.Close(); err != nil {
			d.log.Error("Failed to close snapshot prefixDB", "err", err)
		}
	}

	// Close the TxIndexAppendOnlyLog store
	if d.txIndexAol != nil {
		if err := d.txIndexAol.Close(); err != nil {
			d.log.Error("Failed to close TxIndexAppendOnlyLog", "err", err)
			return fmt.Errorf("failed to close TxIndexAppendOnlyLog: %w", err)
		}
	}
	// Close the BlockAppendOnlyLog
	if d.blockAol != nil {
		if err := d.blockAol.Close(); err != nil {
			d.log.Error("Failed to close BlockAppendOnlyLog", "err", err)
			return fmt.Errorf("failed to close BlockAppendOnlyLog: %w", err)
		}
	}
	// First close Pebble store
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			d.log.Error("Failed to close Pebble store", "err", err)
			// Continue trying to close AOL, but remember Pebble's error
			if aolErr := d.txIndexAol.Close(); aolErr != nil {
				return fmt.Errorf("failed to close stores: pebble: %v, aol: %v", err, aolErr)
			}

			if baolErr := d.blockAol.Close(); baolErr != nil {
				return fmt.Errorf("failed to close BlockAppendOnlyLog: %v", baolErr)
			}
			return fmt.Errorf("failed to close Pebble store: %v", err)
		}
	}
	// Then close AOL
	return nil
}

// Has retrieves if a key is present in the key-value store.
func (d *Database) Has(key []byte) (bool, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return false, ErrClosed
	}
	dataType := GetDataTypeFromKey(key)
	if dataType == TransactionLookupMetadataDataType {
		if !d.enableTxlookupData {
			return true, nil
		} else {
			// fmt.Printf("EthStore.Get Pebble: key=%x\n", key)
			_, err := d.db.Get(key)

			if err != nil {
				if err == pebble.ErrNotFound {
					return false, ErrNotFound // Convert to EthStore specific ErrNotFound
				}
				return false, err
			}
			return true, nil
		}
	}

	if AolHandledDataTypes[dataType] {
		if d.txIndexAol == nil {
			return false, fmt.Errorf("AOL is not initialized, cannot check key %x (type %s)", key, DataTypeStrings[dataType])
		}
		var valStr string
		var exists bool
		var err error

		if valStr, exists = d.baolkvs[string(key)]; exists {
			return true, nil
		}
		valStr, exists, err = d.blockAol.Get(string(key))
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
		return false, ErrNotFound
		// If the key doesn't exist in AOL, continue to look in Pebble
	} else if PrefixDBHandledDataTypes[dataType] {
		if d.statepdb == nil {
			return false, fmt.Errorf("PrefixDB is not initialized, cannot check key %x (type %s)", key, DataTypeStrings[dataType])
		}
		// Check if the key exists in PrefixDB
		exists, err := d.statepdb.Has(key, d.accountKey)
		if err != nil {
			return false, fmt.Errorf("failed to check key %x in PrefixDB: %w", key, err)
		}
		return exists, nil
	} else if SSPrefixdbHandledDataTypes[dataType] {
		if d.snappdb == nil {
			return false, fmt.Errorf("SSPrefixDB is not initialized, cannot check key %x(type %s)", key, DataTypeStrings[dataType])
		}
		// Check if the key exists in SSPrefixDB
		exists, err := d.snappdb.Has(key, d.accountKey)
		if err != nil {
			return false, fmt.Errorf("failed to check key %x in SSPrefixDB: %w", key, err)
		}
		return exists, nil
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
	if dataType == TransactionLookupMetadataDataType {
		if !d.enableTxlookupData {
			return nil, nil
		} else {
			// fmt.Printf("EthStore.Get Pebble: key=%x\n", key)
			value, err := d.db.Get(key)

			if err != nil {
				if err == pebble.ErrNotFound {
					return nil, ErrNotFound // Convert to EthStore specific ErrNotFound
				}
				return nil, err
			}
			return value, nil
		}
	}
	if AolHandledDataTypes[dataType] {
		if d.txIndexAol == nil {
			return nil, fmt.Errorf("AOL is not initialized, cannot get key %x (type %s)", key, DataTypeStrings[dataType])
		}
		var valStr string
		var exists bool
		var err error

		if valStr, exists = d.baolkvs[string(key)]; exists {
			// Return the found value from temporary storage
			return []byte(valStr), nil
		}
		valStr, exists, err = d.blockAol.Get(string(key))
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
		} else {
			return nil, ErrNotFound
		}
	} else if PrefixDBHandledDataTypes[dataType] {
		if d.statepdb == nil {

			return nil, fmt.Errorf("PrefixDB is not initialized, cannot get key %x (type %s)", key, DataTypeStrings[dataType])
		}

		// Try to get from PrefixDB

		value, exists, err := d.statepdb.Get(key, d.accountKey)

		if err != nil {
			return nil, fmt.Errorf("failed to get key %x from PrefixDB: %w", key, err)
		}
		if !exists {
			return nil, ErrNotFound // Key not found in PrefixDB
		}
		if value == nil {
			return nil, fmt.Errorf("key %x found in PrefixDB but value is nil", key)
		}
		// Log the found key in PrefixDB
		d.log.Trace("Key found in PrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
		return value, nil // Return the found value
	} else if SSPrefixdbHandledDataTypes[dataType] {
		if d.snappdb == nil {
			return nil, fmt.Errorf("SSPrefixDB is not initialized, cannot get key %x (type %s)", key, DataTypeStrings[dataType])
		}
		// Try to get from SSPrefixDB

		value, exists, err := d.snappdb.Get(key, d.accountKey)

		if err != nil {
			return nil, fmt.Errorf("failed to get key %x from SSPrefixDB: %w", key, err)
		}
		if !exists {
			return nil, ErrNotFound // Key not found in SSPrefixDB
		}
		if value == nil {
			return nil, fmt.Errorf("key %x found in SSPrefixDB but value is nil", key)
		}
		// Log the found key in SSPrefixDB
		d.log.Trace("Key found in SSPrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
		return value, nil // Return the found value
	}

	// Try to get from Pebble

	// fmt.Printf("EthStore.Get Pebble: key=%x\n", key)
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
	if dataType == TransactionLookupMetadataDataType {
		if !d.enableTxlookupData {
			return nil
		} else {
			// fmt.Printf("EthStore.Put Pebble: key=%x\n", key)
			err := d.db.Put(key, value)
			if err != nil {
				return fmt.Errorf("pebble put failed for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
			}
			d.log.Trace("Stored key via Pebble", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
			return nil
		}
	}

	if AolHandledDataTypes[dataType] {
		if d.txIndexAol == nil {
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
			var err error
			if blockID > d.baolLatestBlock {
				if d.baolLatestBlock != 0 {
					err = d.blockAol.Append(d.baolLatestBlock, d.baolkvs)
				}
				d.baolLatestBlock = blockID
				d.baolkvs = make(map[string]string, 4)
				d.baolkvs[string(key)] = string(value)
				if err != nil {
					return fmt.Errorf("baol append failed for key %x (type %s, blockID %d): %w", key, DataTypeStrings[dataType], blockID-1, err)
				}
				d.log.Trace("Stored key via BlockAppendOnlyLog", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType], "blockID", blockID)
				return nil // Data stored in AOL
			} else {
				d.baolkvs[string(key)] = string(value)
				return nil // Data queued for AOL
			}
		}
		// If blockID couldn't be determined for an AOL-handled type.
		return fmt.Errorf("could not determine blockID for AOL-handled type %s for key %x; storage via AOL failed", DataTypeStrings[dataType], key)
	} else if PrefixDBHandledDataTypes[dataType] {
		if d.statepdb == nil {
			return fmt.Errorf("PrefixDB is not initialized, cannot store key %x (type %s)", key, DataTypeStrings[dataType])
		}
		// Store in PrefixDB

		err := d.statepdb.Put(key, value, d.accountKey)

		if err != nil {
			return fmt.Errorf("failed to put key %x in PrefixDB (type %s): %w", key, DataTypeStrings[dataType], err)
		}
		d.log.Trace("Stored key via PrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
		return nil // Data stored in PrefixDB
	} else if SSPrefixdbHandledDataTypes[dataType] {
		if d.snappdb == nil {
			return fmt.Errorf("SSPrefixDB is not initialized, cannot store key %x (type %s)", key, DataTypeStrings[dataType])
		}
		// Store in SSPrefixDB

		err := d.snappdb.Put(key, value, d.accountKey)

		if err != nil {
			return fmt.Errorf("failed to put key %x in SSPrefixDB (type %s): %w", key, DataTypeStrings[dataType], err)
		}
		d.log.Trace("Stored key via SSPrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
		return nil // Data stored in SSPrefixDB
	}

	// Default: store non-AOL data in Pebble
	if d.db == nil {
		return fmt.Errorf("Pebble store is not initialized, cannot store non-AOL key %x (type %s)", key, DataTypeStrings[dataType])
	}

	// fmt.Printf("EthStore.Put Pebble: key=%x\n", key)
	// fmt.Printf("EthStore.Put Pebble: key=%x\n", key)
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
	if dataType == TransactionLookupMetadataDataType {
		if !d.enableTxlookupData {
			return nil
		} else {
			err := d.db.Delete(key)

			if err != nil {
				return fmt.Errorf("pebble delete failed for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
			}
			d.log.Trace("Deleted key via Pebble", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
			return nil
		}
	}

	if AolHandledDataTypes[dataType] {
		if d.txIndexAol == nil {
			return fmt.Errorf("AOL is not initialized, cannot delete key %x (type %s)", key, DataTypeStrings[dataType])
		}
		var err error
		err = d.blockAol.Delete(string(key))
		if err != nil {
			return fmt.Errorf("baol append (delete tombstone) failed for key %x (type %s, blockID): %w", key, DataTypeStrings[dataType], err)
		}
		d.log.Trace("Stored delete tombstone via BlockAppendOnlyLog", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
		// Successfully stored deletion marker in AOL
		return nil // Deletion marker stored in AOL
	} else if PrefixDBHandledDataTypes[dataType] {
		if d.statepdb == nil {
			return fmt.Errorf("PrefixDB is not initialized, cannot delete key %x (type %s)", key, DataTypeStrings[dataType])
		}
		// Delete from PrefixDB
		err := d.statepdb.Delete(key, d.accountKey)
		if err != nil {
			return fmt.Errorf("failed to delete key %x from PrefixDB (type %s): %w", key, DataTypeStrings[dataType], err)
		}
		d.log.Trace("Deleted key via PrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
		return nil // Data deleted from PrefixDB
	} else if SSPrefixdbHandledDataTypes[dataType] {
		if d.snappdb == nil {
			return fmt.Errorf("SSPrefixDB is not initialized, cannot delete key %x (type %s)", key, DataTypeStrings[dataType])
		}
		// Delete from SSPrefixDB
		err := d.snappdb.Delete(key, d.accountKey)
		if err != nil {
			return fmt.Errorf("failed to delete key %x from SSPrefixDB (type %s): %w", key, DataTypeStrings[dataType], err)
		}
		d.log.Trace("Deleted key via SSPrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
		return nil // Data deleted from SSPrefixDB
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
	// Delegate to PebbleStore for now, as most range deletions in tests target Pebble data.
	// TODO: Implement range deletion for AOL and PrefixDB if needed.
	if d.db != nil {
		return d.db.DeleteRange(start, end)
	}
	d.log.Warn("DeleteRange called but PebbleStore is nil")
	return nil
}

// Path returns the path to the database directory.
func (d *Database) Path() string {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return "" // Or handle appropriately
	}
	return d.txIndexAol.Path()
}

// ethdbIterator is a wrapper implementing the ethdb.Iterator interface for Pebble iterator
type ethdbIterator struct {
	iter       *pebble.Iterator
	typePrefix byte
	prefixLen  int
	valid      bool
	err        error
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

// iterator is a wrapper implementing the ethdb.Iterator interface for iterating over keys in the Database (primarily for AOL data)
type iterator struct {
	db     *Database
	prefix []byte
	start  []byte
	keys   [][]byte
	pos    int
	err    error // Added err field
}

// NewIterator creates a binary-alphabetical iterator over a subset of database content.
// If the prefix indicates an AOL-handled data type, an AOL-specific iterator is returned.
// Otherwise, the call is delegated to the underlying PebbleStore.
func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	d.quitLock.RLock() // Lock for reading d.closed and accessing d.db/d.txIndexAol
	defer d.quitLock.RUnlock()

	if d.closed {
		d.log.Warn("NewIterator called on closed database")
		return &errorIterator{err: ErrClosed}
	}

	dataType := GetDataTypeFromKey(prefix) // Assuming GetDataTypeFromKey is defined elsewhere

	if AolHandledDataTypes[dataType] {
		// This is an AOL-handled type, use the 'iterator' struct.
		d.log.Trace("Creating new iterator for AOL", "prefix", common.Bytes2Hex(prefix), "start", common.Bytes2Hex(start), "dataType", DataTypeStrings[dataType])
		iter := &iterator{
			db:     d,
			prefix: prefix,
			start:  start,
			keys:   make([][]byte, 0),
			pos:    -1,
		}
		// The init() method for this AOL iterator needs to be fully implemented
		// to correctly load keys from the TxIndexAppendOnlyLog.
		iter.init()
		return iter
	}

	// Not an AOL-handled type, delegate to PebbleStore.
	if d.db == nil {
		d.log.Error("Pebble store (d.db) not initialized, cannot create iterator for non-AOL type", "prefix", common.Bytes2Hex(prefix), "dataType", DataTypeStrings[dataType])
		return &errorIterator{err: errors.New("internal pebble store not initialized for iterator")}
	}

	d.log.Trace("Delegating NewIterator to PebbleStore", "prefix", common.Bytes2Hex(prefix), "start", common.Bytes2Hex(start), "dataType", DataTypeStrings[dataType])
	// PebbleStore's NewIterator is expected to return an ethdb.Iterator (specifically, a *pebbleIterator).
	// It handles its own internal errors by setting an initErr field in the returned iterator.
	return d.db.NewIterator(prefix, start)
}

// init initializes the iterator, loading keys that match the prefix and start.
// For the AOL-specific iterator, this method needs to scan relevant AOL files,
// filter keys by prefix and start, consider tombstones, and sort them if necessary.
// WARNING: The current implementation is a placeholder and does not load data from AOL.
func (it *iterator) init() {
	// Ensure it.db and it.db.txIndexAol are valid before proceeding with AOL-specific logic
	if it.db == nil {
		it.err = errors.New("iterator: database not initialized")
		return
	}
	// The RLock/RUnlock for db.closed and aol access should be managed here if init performs direct aol operations.
	// For now, we assume NewIterator holds the lock during this call.
	// If init becomes asynchronous or complex, it needs its own locking.

	// If this iterator is for AOL:
	if AolHandledDataTypes[GetDataTypeFromKey(it.prefix)] {
		if it.db.txIndexAol == nil {
			it.err = errors.New("iterator: AOL not initialized in database for AOL-specific iterator")
			it.keys = make([][]byte, 0)
			it.pos = -1
			return
		}
		// --- BEGIN AOL Key Loading Logic (Placeholder) ---
		// This section requires significant implementation:
		// 1. Identify relevant AOL segment files based on potential block ranges or timestamps if applicable.
		// 2. Read records from these segment files.
		// 3. For each record:
		//    a. Deserialize the key-value pairs.
		//    b. Check if a key matches `it.prefix`.
		//    c. If `it.start` is provided, ensure the key is >= `it.start`.
		//    d. Handle `aolDeleteTombstone`: if a key is marked deleted, it should not be included.
		//    e. Store valid keys. Keys might need to be unique (latest version wins).
		// 4. Sort the collected keys if order is not guaranteed by the reading process.
		//
		// Example (very simplified, conceptual):
		// allAOLKeysAndValues := it.db.txIndexAol.GetAllMatchingPrefix(it.prefix) // This function doesn't exist, needs to be built
		// for key, value := range allAOLKeysAndValues {
		//    if bytes.HasPrefix(key, it.prefix) && (it.start == nil || bytes.Compare(key, it.start) >= 0) {
		//        if !bytes.Equal(value, []byte(aolDeleteTombstone)) { // Check value if tombstones are stored as values
		//            it.keys = append(it.keys, key)
		//        }
		//    }
		// }
		// sort.Slice(it.keys, func(i, j int) bool { return bytes.Compare(it.keys[i], it.keys[j]) < 0 })
		// --- END AOL Key Loading Logic (Placeholder) ---
		it.db.log.Debug("AOL iterator init called (data loading logic is a placeholder)", "prefix", common.Bytes2Hex(it.prefix), "start", common.Bytes2Hex(it.start))
		it.keys = make([][]byte, 0) // Initialize as empty until fully implemented
		it.pos = -1
		// To signal that this part is not done, you might set an error:
		// it.err = errors.New("AOL iterator data loading not implemented in init()")
	} else {
		// This 'iterator' struct should ideally not be initialized for non-AOL types if delegation occurs.
		// However, if NewIterator somehow created this 'iterator' instance before deciding to delegate,
		// this path might be hit. Setting an error or ensuring it's a no-op is safest.
		it.err = errors.New("iterator.init() called for a non-AOL type; this should have been delegated to PebbleStore")
		it.keys = make([][]byte, 0)
		it.pos = -1
	}
}

// Next moves to the next key
func (it *iterator) Next() bool { // Receiver changed to *iterator
	if it.err != nil {
		return false
	}
	// This iterates over the 'keys' slice, which 'init' is supposed to populate.
	// Since 'init' currently leaves 'keys' empty, this will effectively be an empty iterator.
	if it.pos+1 < len(it.keys) {
		it.pos++
		return true
	}
	return false
}

// Error returns any accumulated error
func (it *iterator) Error() error { // Receiver changed to *iterator
	return it.err
}

// Key returns the key of the current entry
func (it *iterator) Key() []byte { // Receiver changed to *iterator
	if it.pos < 0 || it.pos >= len(it.keys) {
		return nil
	}
	return it.keys[it.pos]
}

// Value returns the value of the current entry
func (it *iterator) Value() []byte { // Receiver changed to *iterator
	if it.pos < 0 || it.pos >= len(it.keys) {
		if it.err == nil && len(it.keys) > 0 { // Avoid error if keys was empty from start
			it.err = errors.New("iterator: invalid position for Value()")
		}
		return nil
	}
	key := it.keys[it.pos]
	if key == nil {
		if it.err == nil {
			it.err = errors.New("iterator: current key is nil")
		}
		return nil
	}
	// Fetches from the main Database.Get, which handles AOL/Pebble dispatch
	value, err := it.db.Get(key)
	if err != nil {
		it.err = err
		return nil
	}
	return value
}

// Release releases associated resources
func (it *iterator) Release() { // Receiver changed to *iterator
	it.keys = nil
	it.pos = -1
	// it.err = nil // Optional: clear error on release
}

// NewBatch creates a write-only database batch object that operates on the underlying Pebble store.
// Operations on this batch will NOT be routed to the TxIndexAppendOnlyLog.
func (d *Database) NewBatch() ethdb.Batch {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()

	// if d.closed {
	// 	d.log.Error("NewBatch called on closed database")
	// 	return &errorBatch{err: ErrClosed}
	// }
	if d.db == nil {
		d.log.Error("Pebble store (d.db) not initialized, cannot create batch")
		return &errorBatch{err: errors.New("internal pebble store not initialized")}
	}
	d.log.Trace("Creating new batch via PebbleStore component")
	return d.db.NewBatch()
}

// NewBatchWithSize creates a write-only database batch object with pre-allocated buffer size
// that operates on the underlying Pebble store.
// Operations on this batch will NOT be routed to the TxIndexAppendOnlyLog.
func (d *Database) NewBatchWithSize(size int) ethdb.Batch {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()

	// if d.closed {
	// 	d.log.Error("NewBatchWithSize called on closed database")
	// 	return &errorBatch{err: ErrClosed}
	// }
	if d.db == nil {
		d.log.Error("Pebble store (d.db) not initialized, cannot create batch with size")
		return &errorBatch{err: errors.New("internal pebble store not initialized")}
	}
	d.log.Trace("Creating new batch with size via PebbleStore component", "size", size)
	return d.db.NewBatchWithSize(size)
}

// Stat returns a particular internal stat of the database.
func (d *Database) Stat() (string, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return "", ErrClosed
	}
	return fmt.Sprintf("ethstore(path=%s, recentN=%d)", d.txIndexAol.Path(), d.txIndexAol.RecentN()), nil
}

// Compact flattens the underlying data store for the given key range.
func (d *Database) Compact(start []byte, limit []byte) error {
	d.log.Warn("Compact operation may not be applicable or is handled differently by TxIndexAppendOnlyLog")
	return nil // Or return an error if not supported
}

func (d *Database) CloseAol() error {
	d.quitLock.Lock()
	defer d.quitLock.Unlock()
	if d.txIndexAol != nil {
		if err := d.txIndexAol.Close(); err != nil {
			d.log.Error("Failed to close TxIndexAppendOnlyLog", "err", err)
			return fmt.Errorf("failed to close TxIndexAppendOnlyLog: %w", err)
		}
		d.txIndexAol = nil // Clear the reference after closing
	}
	if d.blockAol != nil {
		if err := d.blockAol.Close(); err != nil {
			d.log.Error("Failed to close BlockAppendOnlyLog", "err", err)
			return fmt.Errorf("failed to close BlockAppendOnlyLog: %w", err)
		}
		d.blockAol = nil // Clear the reference after closing
	}

	return nil
}

func (d *Database) SetAccountKey(accountKey []byte) error {
	d.accountKey = accountKey
	return nil
}

func (d *Database) GetParentAccountKey(key []byte) []byte {
	return d.statepdb.GetParentAccountKey(key)
}

func (d *Database) InsertAccountHashPebble(key []byte, accounthash []byte) error {
	return d.statepdb.InsertAccountHashPebble(accounthash, key)
}
