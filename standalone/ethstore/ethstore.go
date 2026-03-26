package ethstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tinoryj/EthStore/standalone/ethstore/pebblestore"
	"github.com/tinoryj/EthStore/standalone/ethstore/prefixdb"
)

type trieStorageGetBreakdownStepStats struct {
	cacheCount   uint64
	cacheNanos   uint64
	noCacheCount uint64
	noCacheNanos uint64
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

func isNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound) || errors.Is(err, pebble.ErrNotFound)
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

// DeleteRange implements ethdb.Batch and returns the predefined error.
func (b *errorBatch) DeleteRange(start []byte, end []byte) error { return b.err }

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
	// HeaderNumberDataType: true,
}

var PrefixDBHandledDataTypes = map[DataType]bool{
	TrieNodeAccountDataType: true,
	TrieNodeStorageDataType: true,
}

func (d *Database) resolvePrefixDBAccountKey(key []byte, dataType DataType, requirePresent bool) ([]byte, error) {
	if dataType != TrieNodeStorageDataType {
		return nil, nil
	}
	if len(key) < 33 {
		return nil, fmt.Errorf("invalid storage key %x", key)
	}
	accountKey := d.GetParentAccountKey(key[1:33])
	if requirePresent && accountKey == nil {
		return nil, fmt.Errorf("failed to derive account key for key %x", key)
	}
	return accountKey, nil
}

const aolDeleteTombstone = "__AOL_DELETED__"

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

// ParseBlockNumberFromValue tries to parse the block number from the value structure
// for data types like HeaderNumber (value is block number) or TransactionLookupMetadata (value is RLP encoded).
func ParseBlockNumberFromValue(value []byte, dataType DataType, logger log.Logger) (uint64, bool) {
	if dataType == HeaderNumberDataType && len(value) == 8 { // Must be exactly 8 bytes for uint64
		return binary.BigEndian.Uint64(value), true
	}
	if logger != nil {
		logger.Warn("Invalid value length for HeaderNumber to parse blockID", "len", len(value))
	}
	return 0, false
}

// Database is a persistent key-value store based on the append-only log store.
type Database struct {
	fn       string                   // filename/directory for reporting
	pebble   *pebblestore.PebbleStore // Pebble store for non-AOL data
	statepdb *prefixdb.PrefixDB       // world state PrefixDB
	blockAol *BlockAppendOnlyLog

	accountHashKeyCache *accountHashToKeyCache // in-memory cache: accountHash(32) -> accountKey(max64)

	diskSizeGauge metrics.Gauge // Gauge for tracking the size of all the data in the database

	quitLock sync.RWMutex    // Mutex protecting the quit channel and the closed flag
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database
	closed   bool            // keep track of whether we're Closed

	log log.Logger // Contextual logger tracking the database path

	baolkvs         map[string]string // Temporary storage for BlockAppendOnlyLog key-values during operations
	baolLatestBlock uint64            // Temporary storage for latest block number during operations
	accountKey      []byte            // Temporary storage for account key during operations

	trieStorageAccountPathStats trieStorageGetBreakdownStepStats
}

// The namespace is the prefix that the metrics reporting should use.
func New(dirPath string, recentN int, namespace string, readonly bool, chunkFileSize int, prefixTreeCacheSize uint64, contractCachePrefetchCount int) (*Database, error) {
	return NewWithPrefixGCSettings(dirPath, recentN, namespace, readonly, chunkFileSize, int(prefixTreeCacheSize/(1024*1024)), contractCachePrefetchCount, 0, 0, 0, false, false)
}

// NewWithPrefixCacheSettings creates Database with a single shared PrefixDB
// cache budget in MiB. All PrefixDB caches share this total budget.
// Use <=0 values to fallback to the default shared cache size.
func NewWithPrefixCacheSettings(dirPath string, recentN int, namespace string, readonly bool, chunkFileSize int, totalCacheSizeMiB int, contractCachePrefetchCount int) (*Database, error) {
	return NewWithPrefixGCSettings(dirPath, recentN, namespace, readonly, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, 0, 0, 0, false, false)
}

func NewWithPrefixGCSettings(dirPath string, recentN int, namespace string, readonly bool, chunkFileSize int, totalCacheSizeMiB int, contractCachePrefetchCount int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool) (*Database, error) {
	return NewWithPrefixGCAndFileHandlesSettings(dirPath, recentN, namespace, readonly, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, 0)
}

func NewWithPrefixGCAndFileHandlesSettings(dirPath string, recentN int, namespace string, readonly bool, chunkFileSize int, totalCacheSizeMiB int, contractCachePrefetchCount int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool, prefixdbHandles int) (*Database, error) {
	return NewWithPrefixGCAndStoreSettings(dirPath, recentN, namespace, readonly, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, prefixdbHandles, 0, 0)
}

func NewWithPrefixGCAndStoreSettings(dirPath string, recentN int, namespace string, readonly bool, chunkFileSize int, totalCacheSizeMiB int, contractCachePrefetchCount int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool, prefixdbHandles int, pebbleCache int, pebbleHandles int) (*Database, error) {
	logger := log.New("database", dirPath)

	dirPathState := dirPath + "_state"
	statePrefixdb, err := prefixdb.NewPrefixDBWithRuntimeOptions(dirPathState, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, prefixdbHandles)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize prefixdb: %w", err)
	}
	db := &Database{
		fn:                  dirPath, // Use directory path now
		log:                 logger,
		quitChan:            make(chan chan error, 1),
		statepdb:            statePrefixdb,
		accountHashKeyCache: newAccountHashToKeyCache(defaultAccountHashToKeyCacheCapacity),
	}
	// Let PrefixDB resolve parent account keys from full storage keys.
	// PrefixDB passes the original storage key ('O' + 32-byte account hash + slot),
	// while Database.GetParentAccountKey expects only the 32-byte account hash.
	statePrefixdb.ParentKeyResolver = func(storageKey []byte) []byte {
		if len(storageKey) < 33 {
			return nil
		}
		return db.GetParentAccountKey(storageKey[1:33])
	}
	// Initialize BlockAppendOnlyLog
	baol, err := NewBlockAppendOnlyLog(dirPath+"_aol", recentN, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize block append-only log: %w", err)
	}
	db.blockAol = baol
	db.baolkvs = make(map[string]string)

	// Initialize Pebble store for non-AOL data
	pebblePath := dirPath + "_pebble"
	logger.Info("Initializing Pebble store", "path", pebblePath)

	pebbleStore, err := pebblestore.NewPebbleStore(pebblePath, pebbleCache, pebbleHandles, namespace, readonly)
	if err != nil {
		// Close AOL if Pebble initialization fails
		db.statepdb.Close()
		baol.Close()
		return nil, fmt.Errorf("failed to initialize pebble store: %w", err)
	}
	db.pebble = pebbleStore

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

// NewStateOnlyWithPrefixCacheSettings opens PrefixDB-only Database with a
// single shared cache budget in MiB.
func NewStateOnlyWithPrefixCacheSettings(stateDir string, chunkFileSize int, totalCacheSizeMiB int, contractCachePrefetchCount int) (*Database, error) {
	return NewStateOnlyWithPrefixGCSettings(stateDir, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, 0, 0, 0, false, false)
}

func NewStateOnlyWithPrefixGCSettings(stateDir string, chunkFileSize int, totalCacheSizeMiB int, contractCachePrefetchCount int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool) (*Database, error) {
	return NewStateOnlyWithPrefixGCAndFileHandlesSettings(stateDir, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, 0)
}

func NewStateOnlyWithPrefixGCAndFileHandlesSettings(stateDir string, chunkFileSize int, totalCacheSizeMiB int, contractCachePrefetchCount int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool, prefixdbHandles int) (*Database, error) {
	logger := log.New("database", stateDir)
	statePrefixdb, err := prefixdb.NewPrefixDBWithRuntimeOptions(stateDir, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, prefixdbHandles)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize prefixdb (state-only): %w", err)
	}

	db := &Database{
		fn:                  stateDir,
		log:                 logger,
		statepdb:            statePrefixdb,
		accountHashKeyCache: newAccountHashToKeyCache(defaultAccountHashToKeyCacheCapacity),
	}
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
	d.printTrieNodeStorageGetBreakdown()

	if d.statepdb != nil {
		if err := d.statepdb.Close(); err != nil {
			d.log.Error("Failed to close state prefixDB", "err", err)
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
	if d.pebble != nil {
		if err := d.pebble.Close(); err != nil {
			d.log.Error("Failed to close Pebble store", "err", err)
			return fmt.Errorf("failed to close Pebble store: %v", err)
		}
	}
	// Then close AOL
	return nil
}

// Has retrieves if a key is present in the key-value store.
func (d *Database) Has(key []byte) (bool, error) {
	return d.HasWithDataType(key, GetDataTypeFromKey(key))
}

func (d *Database) HasWithDataType(key []byte, dataType DataType) (bool, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return false, ErrClosed
	}

	if AolHandledDataTypes[dataType] {
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
		accountKey, err := d.resolvePrefixDBAccountKey(key, dataType, false)
		if err != nil {
			return false, fmt.Errorf("failed to resolve PrefixDB key context for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
		}
		exists, err := d.statepdb.Has(dataType, key, accountKey)
		if err != nil {
			return false, fmt.Errorf("failed to check key %x in PrefixDB: %w", key, err)
		}
		return exists, nil
	}
	// Check if the key exists in Pebble
	_, err := d.pebble.Get(key)
	if err == nil {
		return true, nil // Key exists
	} else if isNotFoundError(err) {
		return false, nil // Key doesn't exist
	}
	return false, err // Other error occurred
}

// Get retrieves the given key if it's present in the key-value store.
func (d *Database) Get(key []byte, dataType DataType) ([]byte, error) {
	if AolHandledDataTypes[dataType] {
		return d.GetFromAOL(key)
	}
	if PrefixDBHandledDataTypes[dataType] {
		return d.GetFromPrefixDB(key, dataType)
	}
	return d.GetFromPebble(key)
}

// GetFromAOL reads key from AOL path only.
func (d *Database) GetFromAOL(key []byte) ([]byte, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	var valStr string
	var exists bool
	var err error

	if valStr, exists = d.baolkvs[string(key)]; exists {
		return []byte(valStr), nil
	}
	valStr, exists, err = d.blockAol.Get(string(key))
	if err != nil {
		return nil, err
	}
	if exists {
		if valStr == aolDeleteTombstone {
			return nil, ErrNotFound
		}
		return []byte(valStr), nil
	}
	return nil, ErrNotFound
}

// GetFromPrefixDB reads key from PrefixDB path only.
func (d *Database) GetFromPrefixDB(key []byte, dataType DataType) ([]byte, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	if d.statepdb == nil {
		return nil, fmt.Errorf("PrefixDB is not initialized, cannot get key %x (type %s)", key, DataTypeStrings[dataType])
	}
	accountKey, err := d.resolvePrefixDBAccountKey(key, dataType, false)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve PrefixDB key context for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
	}
	value, exists, err := d.statepdb.Get(dataType, key, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %x from PrefixDB: %w", key, err)
	}
	if !exists {
		return nil, ErrNotFound
	}
	d.log.Trace("Key found in PrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
	return value, nil
}

// GetFromPebble reads key from Pebble path only.
func (d *Database) GetFromPebble(key []byte) ([]byte, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	value, err := d.pebble.Get(key)
	if err != nil {
		if isNotFoundError(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return value, nil
}

// Put stores the given key-value pair.
// If the key belongs to specific types (Header, HeaderNumber, etc.), it's stored in the Append-Only Log (AOL).
// Otherwise, it's stored in the underlying key-value database.
func (d *Database) Put(key []byte, value []byte) error {
	return d.PutWithDataType(key, value, GetDataTypeFromKey(key))
}

func (d *Database) PutWithDataType(key []byte, value []byte, dataType DataType) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()

	if d.closed {
		return ErrClosed
	}
	if AolHandledDataTypes[dataType] {
		var blockID uint64
		var foundBlockID bool

		// Try to get blockID from key
		blockID, foundBlockID = ParseBlockNumberFromKey(key, dataType)

		// If not found in key, try from value (for HeaderNumber)
		if !foundBlockID {
			blockID, foundBlockID = ParseBlockNumberFromValue(value, dataType, d.log)
		}
		if foundBlockID {
			if blockID > d.baolLatestBlock {
				if d.blockAol == nil {
					return fmt.Errorf("block append-only log is not initialized")
				}
				if d.baolkvs == nil {
					d.baolkvs = make(map[string]string, 4)
				}
				if d.baolLatestBlock != 0 {
					if err := d.blockAol.Append(d.baolLatestBlock, d.baolkvs); err != nil {
						return fmt.Errorf("baol append failed for key %x (type %s, blockID %d): %w", key, DataTypeStrings[dataType], blockID-1, err)
					}
				}
				// Reuse the map to avoid per-block allocations.
				clear(d.baolkvs)
				d.baolLatestBlock = blockID
				d.baolkvs[string(key)] = string(value)
				d.log.Trace("Stored key via BlockAppendOnlyLog", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType], "blockID", blockID)
				return nil // Data stored in AOL
			} else {
				if d.baolkvs == nil {
					d.baolkvs = make(map[string]string, 4)
				}
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
		accountKey, err := d.resolvePrefixDBAccountKey(key, dataType, true)
		if err != nil {
			return fmt.Errorf("failed to resolve PrefixDB key context for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
		}
		err = d.statepdb.Put(dataType, key, value, accountKey)

		if err != nil {
			return fmt.Errorf("failed to put key %x in PrefixDB (type %s): %w", key, DataTypeStrings[dataType], err)
		}
		d.log.Trace("Stored key via PrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
		return nil // Data stored in PrefixDB
	}
	// Default: store non-AOL data in Pebble
	if d.pebble == nil {
		return fmt.Errorf("Pebble store is not initialized, cannot store non-AOL key %x (type %s)", key, DataTypeStrings[dataType])
	}

	// fmt.Printf("EthStore.Put Pebble: key=%x\n", key)
	// fmt.Printf("EthStore.Put Pebble: key=%x\n", key)
	err := d.pebble.Put(key, value)

	if err != nil {
		return fmt.Errorf("pebble put failed for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
	}
	d.log.Trace("Stored key via Pebble", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
	return nil
}

func (d *Database) BatchPut(key []byte, value []byte, dataType DataType) error {
	if PrefixDBHandledDataTypes[dataType] {
		return d.BatchPutToPrefixDB(key, value, dataType)
	}
	if AolHandledDataTypes[dataType] {
		return d.BatchPutToAOL(key, value, dataType)
	}
	return nil
}

// BatchPutToPrefixDB writes to PrefixDB batch directly.
func (d *Database) BatchPutToPrefixDB(key []byte, value []byte, dataType DataType) error {
	if d.statepdb == nil {
		return fmt.Errorf("PrefixDB is not initialized, cannot batch put key %x (type %s)", key, DataTypeStrings[dataType])
	}
	accountKey, err := d.resolvePrefixDBAccountKey(key, dataType, false)
	if err != nil {
		return fmt.Errorf("failed to resolve PrefixDB key context for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
	}
	err = d.statepdb.BatchPut(dataType, key, value, accountKey)
	if err != nil {
		return fmt.Errorf("failed to batch put account key %x in PrefixDB: %w", key, err)
	}
	d.log.Trace("Batch stored key via PrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
	return nil
}

// BatchPutToAOL writes to AOL batch directly.
func (d *Database) BatchPutToAOL(key []byte, value []byte, dataType DataType) error {
	var blockID uint64
	var foundBlockID bool

	blockID, foundBlockID = ParseBlockNumberFromKey(key, dataType)
	if !foundBlockID {
		blockID, foundBlockID = ParseBlockNumberFromValue(value, dataType, d.log)
	}
	if !foundBlockID {
		return fmt.Errorf("could not determine blockID for AOL-handled type %s for key %x; storage via AOL failed", DataTypeStrings[dataType], key)
	}
	if d.blockAol == nil {
		return fmt.Errorf("block append-only log is not initialized")
	}
	if d.baolkvs == nil {
		d.baolkvs = make(map[string]string, 4)
	}
	if d.baolLatestBlock == 0 {
		d.baolLatestBlock = blockID
	}
	if blockID != d.baolLatestBlock {
		return fmt.Errorf("aol batch spans multiple blocks without commit: current=%d incoming=%d", d.baolLatestBlock, blockID)
	}
	d.baolkvs[string(key)] = string(value)
	d.log.Trace("Buffered key for BlockAppendOnlyLog", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType], "blockID", blockID)
	return nil
}

// CommitAOLBatch appends buffered AOL kvs as one block and performs explicit
// data+index flush for durability. This is intended to be called at block
// boundaries by the replay pipeline.
func (d *Database) CommitAOLBatch() error {
	if d.blockAol == nil {
		return fmt.Errorf("block append-only log is not initialized")
	}
	if d.baolLatestBlock == 0 || len(d.baolkvs) == 0 {
		return nil
	}
	if err := d.blockAol.Append(d.baolLatestBlock, d.baolkvs); err != nil {
		return fmt.Errorf("baol append failed for block %d: %w", d.baolLatestBlock, err)
	}
	if err := d.blockAol.FlushDataAndIndex(); err != nil {
		return fmt.Errorf("baol explicit flush failed for block %d: %w", d.baolLatestBlock, err)
	}
	clear(d.baolkvs)
	d.baolLatestBlock = 0
	return nil
}

func (d *Database) PrefixdbBatchCommit() error {

	if d.statepdb == nil {
		return fmt.Errorf("PrefixDB is not initialized, cannot commit batch")
	}
	err := d.statepdb.BatchCommit()
	if err != nil {
		return fmt.Errorf("failed to commit batch for PrefixDB: %w", err)
	}
	d.log.Trace("Committed batch for PrefixDB", "prefix")

	return nil
}

// Delete removes the given key.
// If the key belongs to specific types and can be handled by AOL (e.g., Header),
// a tombstone record is appended to the AOL.
// Otherwise, it's deleted from the underlying key-value database.
// Deletion for types like HeaderNumber and TransactionLookupMetadata via AOL is not supported
// with this method as blockID cannot be derived from the key alone.
func (d *Database) Delete(key []byte) error {
	return d.DeleteWithDataType(key, GetDataTypeFromKey(key))
}

func (d *Database) DeleteWithDataType(key []byte, dataType DataType) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()

	if d.closed {
		return ErrClosed
	}
	if AolHandledDataTypes[dataType] {
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
		accountKey, err := d.resolvePrefixDBAccountKey(key, dataType, false)
		if err != nil {
			return fmt.Errorf("failed to resolve PrefixDB key context for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
		}
		err = d.statepdb.Delete(dataType, key, accountKey)
		if err != nil {
			return fmt.Errorf("failed to delete key %x from PrefixDB (type %s): %w", key, DataTypeStrings[dataType], err)
		}
		d.log.Trace("Deleted key via PrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
		return nil // Data deleted from PrefixDB
	}
	// Default: delete from Pebble
	if d.pebble == nil {
		return fmt.Errorf("Pebble store is not initialized, cannot delete non-AOL key %x (type %s)", key, DataTypeStrings[dataType])
	}

	err := d.pebble.Delete(key)

	if err != nil {
		return fmt.Errorf("pebble delete failed for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
	}
	d.log.Trace("Deleted key via Pebble", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
	return nil
}

func (d *Database) BatchDelete(key []byte, dataType DataType) error {
	if PrefixDBHandledDataTypes[dataType] {
		return d.BatchDeleteFromPrefixDB(key, dataType)
	}
	if AolHandledDataTypes[dataType] {
		return d.BatchDeleteFromAOL(key, dataType)
	}
	return nil

}

// BatchDeleteFromPrefixDB deletes from PrefixDB batch directly.
func (d *Database) BatchDeleteFromPrefixDB(key []byte, dataType DataType) error {
	if d.statepdb == nil {
		return fmt.Errorf("PrefixDB is not initialized, cannot batch delete key %x (type %s)", key, DataTypeStrings[dataType])
	}
	accountKey, err := d.resolvePrefixDBAccountKey(key, dataType, false)
	if err != nil {
		return fmt.Errorf("failed to resolve PrefixDB key context for key %x (type %s): %w", key, DataTypeStrings[dataType], err)
	}
	err = d.statepdb.BatchPut(dataType, key, nil, accountKey)
	if err != nil {
		return fmt.Errorf("failed to batch delete key %x from PrefixDB (type %s): %w", key, DataTypeStrings[dataType], err)
	}
	d.log.Trace("Batch deleted key via PrefixDB", "key", common.Bytes2Hex(key), "type", DataTypeStrings[dataType])
	return nil
}

// BatchDeleteFromAOL keeps existing replay behavior (no-op for AOL batch delete).
func (d *Database) BatchDeleteFromAOL(_ []byte, _ DataType) error {
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
	if d.pebble == nil {
		d.log.Warn("DeleteRange called but PebbleStore is nil")
		return nil
	}

	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}

	batch := d.pebble.NewBatch()
	iter := d.pebble.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		if start != nil && bytes.Compare(key, start) < 0 {
			continue
		}
		if end != nil && bytes.Compare(key, end) >= 0 {
			continue
		}
		if err := batch.Delete(key); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return batch.Write()
}

// Path returns the path to the database directory.
func (d *Database) Path() string {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return "" // Or handle appropriately
	}
	return d.fn
}

// NewIterator creates a binary-alphabetical iterator over a subset of database content.
// If the prefix indicates an AOL-handled data type, an AOL-specific iterator is returned.
// Otherwise, the call is delegated to the underlying PebbleStore.
func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	d.quitLock.RLock() // Lock for reading d.closed and accessing d.pebble
	defer d.quitLock.RUnlock()

	if d.closed {
		d.log.Warn("NewIterator called on closed database")
		return &errorIterator{err: ErrClosed}
	}

	dataType := GetDataTypeFromKey(prefix) // Assuming GetDataTypeFromKey is defined elsewhere

	if AolHandledDataTypes[dataType] {
		// This is an AOL-handled type, use the 'iterator' struct.
		d.log.Trace("Creating new iterator for AOL", "prefix", common.Bytes2Hex(prefix), "start", common.Bytes2Hex(start), "dataType", DataTypeStrings[dataType])
		return d.blockAol.NewIterator(prefix)
	}

	// Not an AOL-handled type, delegate to PebbleStore.
	if d.pebble == nil {
		d.log.Error("Pebble store (d.pebble) not initialized, cannot create iterator for non-AOL type", "prefix", common.Bytes2Hex(prefix), "dataType", DataTypeStrings[dataType])
		return &errorIterator{err: errors.New("internal pebble store not initialized for iterator")}
	}

	d.log.Trace("Delegating NewIterator to PebbleStore", "prefix", common.Bytes2Hex(prefix), "start", common.Bytes2Hex(start), "dataType", DataTypeStrings[dataType])
	// PebbleStore's NewIterator is expected to return an ethdb.Iterator (specifically, a *pebbleIterator).
	// It handles its own internal errors by setting an initErr field in the returned iterator.
	return d.pebble.NewIterator(prefix, start)
}

// NewPebbleIterator creates an iterator using the underlying Pebble store directly.
// This is useful for replay workloads that want to exercise Pebble iterator behavior
// and intentionally bypass EthStore's higher-level routing (e.g., AOL-specific iterators).
func (d *Database) NewPebbleIterator(prefix []byte, start []byte) ethdb.Iterator {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()

	if d.closed {
		d.log.Warn("NewPebbleIterator called on closed database")
		return &errorIterator{err: ErrClosed}
	}
	if d.pebble == nil {
		d.log.Error("Pebble store (d.pebble) not initialized, cannot create pebble iterator", "prefix", common.Bytes2Hex(prefix))
		return &errorIterator{err: errors.New("internal pebble store not initialized for pebble iterator")}
	}
	return d.pebble.NewIterator(prefix, start)
}

// NewBatch creates a write-only database batch object that operates on the underlying Pebble store.
func (d *Database) NewBatch() ethdb.Batch {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()

	// if d.closed {
	// 	d.log.Error("NewBatch called on closed database")
	// 	return &errorBatch{err: ErrClosed}
	// }
	if d.pebble == nil {
		d.log.Error("Pebble store (d.pebble) not initialized, cannot create batch")
		return &errorBatch{err: errors.New("internal pebble store not initialized")}
	}
	d.log.Trace("Creating new batch via PebbleStore component")
	return d.pebble.NewBatch()
}

// NewBatchWithSize creates a write-only database batch object with pre-allocated buffer size
// that operates on the underlying Pebble store.
func (d *Database) NewBatchWithSize(size int) ethdb.Batch {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()

	// if d.closed {
	// 	d.log.Error("NewBatchWithSize called on closed database")
	// 	return &errorBatch{err: ErrClosed}
	// }
	if d.pebble == nil {
		d.log.Error("Pebble store (d.pebble) not initialized, cannot create batch with size")
		return &errorBatch{err: errors.New("internal pebble store not initialized")}
	}
	d.log.Trace("Creating new batch with size via PebbleStore component", "size", size)
	return d.pebble.NewBatchWithSize(size)
}

// Stat returns a particular internal stat of the database.
func (d *Database) Stat() (string, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return "", ErrClosed
	}
	return fmt.Sprintf("ethstore(path=%s, recentN=%d)", d.fn, 0), nil
}

// Compact flattens the underlying data store for the given key range.
func (d *Database) Compact(start []byte, limit []byte) error {
	d.log.Warn("Compact operation may not be applicable or is handled differently")
	return nil // Or return an error if not supported
}

// SyncKeyValue flushes pending writes to durable storage.
func (d *Database) SyncKeyValue() error {
	d.quitLock.Lock()
	defer d.quitLock.Unlock()

	if d.closed {
		return ErrClosed
	}

	if d.baolLatestBlock != 0 && len(d.baolkvs) > 0 && d.blockAol != nil {
		if err := d.blockAol.Append(d.baolLatestBlock, d.baolkvs); err != nil {
			return fmt.Errorf("failed to sync AOL writes: %w", err)
		}
		// Reuse the map to avoid allocations on every SyncKeyValue.
		clear(d.baolkvs)
	}

	if d.statepdb != nil {
		if err := d.statepdb.BatchCommit(); err != nil {
			return fmt.Errorf("failed to sync PrefixDB batch: %w", err)
		}
	}

	if d.pebble != nil {
		if err := d.pebble.Flush(); err != nil && err != ErrClosed {
			return fmt.Errorf("failed to flush pebble: %w", err)
		}
	}

	return nil
}

func (d *Database) CloseAol() error {
	d.quitLock.Lock()
	defer d.quitLock.Unlock()
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
	start := time.Now()
	// key is expected to be accountHash (fixed 32 bytes)
	if len(key) != 32 {
		recordTrieStorageGetBreakdownStep(&d.trieStorageAccountPathStats, false, time.Since(start))
		return nil
	}

	// Fast path: in-memory cache
	if d.accountHashKeyCache != nil {
		var tmp [64]byte
		if n, ok := d.accountHashKeyCache.Get(key, &tmp); ok {
			d.accountKey = append(d.accountKey[:0], tmp[:n]...)
			recordTrieStorageGetBreakdownStep(&d.trieStorageAccountPathStats, true, time.Since(start))
			return d.accountKey
		}
	}

	// Fallback: resolve via Pebble + PrefixDB index
	if d.pebble == nil {
		recordTrieStorageGetBreakdownStep(&d.trieStorageAccountPathStats, false, time.Since(start))
		return nil
	}
	val, err := d.pebble.Get(key)
	if err != nil {
		recordTrieStorageGetBreakdownStep(&d.trieStorageAccountPathStats, false, time.Since(start))
		d.log.Error("Failed to get parent account key", "key", key, "err", err)
		return nil
	}

	if d.accountHashKeyCache != nil {
		d.accountHashKeyCache.Put(key, val)
	}
	recordTrieStorageGetBreakdownStep(&d.trieStorageAccountPathStats, false, time.Since(start))
	return val
}

func (d *Database) printTrieNodeStorageGetBreakdown() {
	if !analysisStatsEnabled {
		return
	}
	step := &d.trieStorageAccountPathStats
	cacheCountNum := atomic.LoadUint64(&step.cacheCount)
	cacheNanos := atomic.LoadUint64(&step.cacheNanos)
	noCacheCount := atomic.LoadUint64(&step.noCacheCount)
	noCacheNanos := atomic.LoadUint64(&step.noCacheNanos)
	cacheAvgMicros := 0.0
	if cacheCountNum > 0 {
		cacheAvgMicros = float64(cacheNanos) / float64(cacheCountNum) / 1000.0
	}
	noCacheAvgMicros := 0.0
	if noCacheCount > 0 {
		noCacheAvgMicros = float64(noCacheNanos) / float64(noCacheCount) / 1000.0
	}
	fmt.Printf("EthStore TrieNodeStorage get breakdown [account-hash->account-path]: cacheCount=%d cacheTotal=%s cacheAvg=%0.2fus noCacheCount=%d noCacheTotal=%s noCacheAvg=%0.2fus\n",
		cacheCountNum,
		time.Duration(cacheNanos),
		cacheAvgMicros,
		noCacheCount,
		time.Duration(noCacheNanos),
		noCacheAvgMicros,
	)
}

func (d *Database) GCPrefixTree() error {
	if d.statepdb == nil {
		return fmt.Errorf("PrefixDB is not initialized, cannot perform GC on prefix tree storage")
	}

	err := d.statepdb.GCPrefixTree()
	if err != nil {
		return fmt.Errorf("failed to perform GC on prefix tree storage: %w", err)
	}

	err = d.statepdb.GCAllStorageChunkFiles()
	if err != nil {
		return fmt.Errorf("failed to perform GC on prefix tree storage: %w", err)
	}
	d.log.Trace("Performed GC on prefix tree storage")
	return nil
}

func (d *Database) RunPostLoadGC() error {
	if d.statepdb == nil {
		return fmt.Errorf("PrefixDB is not initialized, cannot perform post-load GC")
	}
	if err := d.statepdb.RunPostLoadGC(); err != nil {
		return fmt.Errorf("failed to perform post-load GC on prefix tree storage: %w", err)
	}
	d.log.Trace("Performed post-load GC on prefix tree storage")
	return nil
}

func (d *Database) UpgradeSegmentIndexFiles() error {
	if d.statepdb == nil {
		return fmt.Errorf("PrefixDB is not initialized, cannot upgrade segment index files")
	}
	if err := d.statepdb.UpgradeSegmentIndexFiles(); err != nil {
		return fmt.Errorf("failed to upgrade segment index files: %w", err)
	}
	d.log.Trace("Upgraded segment index files")
	return nil
}

func (d *Database) InsertAccountHashPebble(key []byte, accounthash []byte) error {
	if d.accountHashKeyCache != nil {
		d.accountHashKeyCache.Put(accounthash, key)
	}
	return d.pebble.Put(accounthash, key)
}
