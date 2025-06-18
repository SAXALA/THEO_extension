package ethstore

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

const (
	// minCache is the minimum amount of memory in megabytes to allocate to pebble
	// read and write caching, split half and half.
	minCache = 16

	// minHandles is the minimum number of files handles to allocate to the open
	// database files.
	minHandles = 16

	// metricsGatheringInterval specifies the interval to retrieve pebble database
	// compaction, io and pause stats to report to the user.
	metricsGatheringInterval = 3 * time.Second

	// degradationWarnInterval specifies how often warning should be printed if the
	// leveldb database cannot keep up with requested writes.
	degradationWarnInterval = time.Minute

	// Default values for NewPebbleStore parameters if not specified (i.e., passed as zero value)
	defaultCacheValue      = 64  // Default cache size in MB
	defaultHandlesValue    = 256 // Default number of file handles
	defaultNamespaceValue  = "pebble" // Default namespace for metrics if an empty string is provided
)

// PebbleStore implements the Store interface using PebbleDB
type PebbleStore struct {
	fn string      // filename for reporting
	db *pebble.DB  // Underlying pebble storage engine
	quitChan chan chan error // Quit channel to stop metrics collection

	compCount    int32 // Compaction count
	compTime     int32 // Compaction time
	writePause   int32 // Write pause count
	writeStall   int32 // Write stall count
	diskReadBytes  uint64 // Disk read counter
	diskWriteBytes uint64 // Disk write counter
	
	log log.Logger // Contextual logger
}

// panicLogger implementation that just panics on errors
type panicLogger struct{}

func (l panicLogger) Infof(format string, args ...interface{})  {}
func (l panicLogger) Fatalf(format string, args ...interface{}) { panic(fmt.Sprintf(format, args...)) }

func (d *PebbleStore) onCompactionBegin(info pebble.CompactionInfo) {
	atomic.AddInt32(&d.compCount, 1)
}

func (d *PebbleStore) onCompactionEnd(info pebble.CompactionInfo) {
	atomic.AddInt32(&d.compTime, 1)
}

func (d *PebbleStore) onWriteStallBegin(info pebble.WriteStallBeginInfo) {
	atomic.AddInt32(&d.writePause, 1)
}

func (d *PebbleStore) onWriteStallEnd() {
	atomic.AddInt32(&d.writeStall, 1)
}

// meter periodically retrieves internal pebble counters and reports them to metrics
func (d *PebbleStore) meter(interval time.Duration, namespace string) {
	compactions := metrics.NewRegisteredGauge(namespace+"pebble/compactions", nil)
	compactionTime := metrics.NewRegisteredGauge(namespace+"pebble/compaction/time", nil)
	writePauses := metrics.NewRegisteredGauge(namespace+"pebble/write/pauses", nil)
	writeStalls := metrics.NewRegisteredGauge(namespace+"pebble/write/stalls", nil)
	diskReads := metrics.NewRegisteredGauge(namespace+"pebble/disk/reads", nil)
	diskWrites := metrics.NewRegisteredGauge(namespace+"pebble/disk/writes", nil)

	// Iterate ad infinitum and collect stats
	for {
		// Retrieve the database stats
		compactions.Update(int64(atomic.LoadInt32(&d.compCount)))
		compactionTime.Update(int64(atomic.LoadInt32(&d.compTime)))
		writePauses.Update(int64(atomic.LoadInt32(&d.writePause)))
		writeStalls.Update(int64(atomic.LoadInt32(&d.writeStall)))
		diskReads.Update(int64(atomic.LoadUint64(&d.diskReadBytes)))
		diskWrites.Update(int64(atomic.LoadUint64(&d.diskWriteBytes)))

		select {
		case errc := <-d.quitChan:
			// Quit requesting, stop hammering the database
			errc <- nil
			return
		case <-time.After(interval):
			// Timeout, gather a new set of stats
		}
	}
}

// NewPebbleStore creates a new database instance
func NewPebbleStore(file string, cache int, handles int, namespace string, readonly bool) (*PebbleStore, error) {
	// Apply default values if zero values are provided by the caller
	if cache == 0 {
		cache = defaultCacheValue
	}
	if handles == 0 {
		handles = defaultHandlesValue
	}
	if namespace == "" {
		namespace = defaultNamespaceValue
	}
	// For 'readonly', its zero value 'false' is typically the desired default.

	// Ensure we have some minimal caching and file guarantees
	// These checks apply after defaults have been set for zero-value inputs.
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}
	logger := log.New("database", file)

	// Open the db and recover any potential corruptions
	db := &PebbleStore{
		fn: file,
		log: logger,
		quitChan: make(chan chan error),
	}

	opt := &pebble.Options{
		Cache:                       pebble.NewCache(int64(cache * 1024 * 1024)),
		MaxOpenFiles:               handles,
		BytesPerSync:               4 * 1024 * 1024,
		DisableWAL:                false,
		L0CompactionThreshold:     2,
		L0StopWritesThreshold:     1000,
		LBaseMaxBytes:             64 * 1024 * 1024,
		MemTableSize:              32 * 1024 * 1024,
		MemTableStopWritesThreshold: 4,
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		},
		EventListener: &pebble.EventListener{
			CompactionBegin: db.onCompactionBegin,
			CompactionEnd:   db.onCompactionEnd,
			WriteStallBegin: db.onWriteStallBegin,
			WriteStallEnd:   db.onWriteStallEnd,
		},
		ReadOnly: readonly,
		Logger:   panicLogger{},
		// NewIter field removed as it's not a valid pebble.Options field
	}

	var err error
	if db.db, err = pebble.Open(file, opt); err != nil {
		return nil, err
	}

	// Start metrics collection
	if namespace != "" { // This will now use defaultNamespaceValue if the original namespace was empty
		go db.meter(metricsGatheringInterval, namespace)
	}
	return db, nil
}

// Close implements the Store interface
func (d *PebbleStore) Close() error {
	if d.quitChan != nil {
		errc := make(chan error)
		d.quitChan <- errc
		if err := <-errc; err != nil {
			return err
		}
		d.quitChan = nil
	}
	return d.db.Close()
}

// Has implements the Store interface
func (d *PebbleStore) Has(key []byte) (bool, error) {
	_, closer, err := d.db.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// Get implements the Store interface
func (d *PebbleStore) Get(key []byte) ([]byte, error) {
	value, closer, err := d.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	ret := make([]byte, len(value))
	copy(ret, value)
	closer.Close()
	return ret, nil
}

// Put implements the Store interface
func (d *PebbleStore) Put(key []byte, value []byte) error {
	return d.db.Set(key, value, pebble.Sync)
}

// Delete implements the Store interface
func (d *PebbleStore) Delete(key []byte) error {
	return d.db.Delete(key, pebble.Sync)
}

// pebbleIterator implements the ethdb.Iterator interface for PebbleDB.
type pebbleIterator struct {
	db      *PebbleStore     // Reference to the store for context (e.g., logging)
	iter    *pebble.Iterator // The underlying Pebble iterator
	prefix  []byte           // The prefix for iteration, if any
	initErr error            // Error that occurred during iterator creation
	// start []byte          // The start key for iteration (handled by IterOptions.LowerBound)
	// No explicit 'valid' field; pebble.Iterator manages its own validity.
}

// Next moves the iterator to the next key/value pair.
// It returns false if the iterator is exhausted.
func (it *pebbleIterator) Next() bool {
	if it.initErr != nil || it.iter == nil {
		return false
	}
	// Loop to find the next key that matches the prefix (if a prefix is specified).
	for it.iter.Next() {
		if it.prefix == nil || bytes.HasPrefix(it.iter.Key(), it.prefix) {
			return true // Found a valid key (either no prefix, or key matches prefix)
		}
		// Key does not match prefix, continue to the next key in the underlying iterator.
	}
	return false // Underlying iterator is exhausted or no more keys match the prefix.
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error.
func (it *pebbleIterator) Error() error {
	if it.initErr != nil {
		return it.initErr
	}
	if it.iter == nil {
		return nil // Or some specific error indicating iterator was not initialized/already released
	}
	return it.iter.Error()
}

// Key returns the key of the current key/value pair, or nil if done.
// The caller should not modify the contents of the returned slice.
func (it *pebbleIterator) Key() []byte {
	if it.initErr != nil || it.iter == nil || !it.iter.Valid() {
		return nil
	}
	// ethdb.Iterator contract implies the returned slice's contents should not be modified by future Next calls.
	// Pebble's iterator key slice is valid until the next mutation call on the iterator.
	// To be safe, we make a copy.
	key := it.iter.Key()
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return keyCopy
}

// Value returns the value of the current key/value pair, or nil if done.
// The caller should not modify the contents of the returned slice.
func (it *pebbleIterator) Value() []byte {
	if it.initErr != nil || it.iter == nil || !it.iter.Valid() {
		return nil
	}
	// Similar to Key(), copy the value to adhere to ethdb.Iterator contract.
	val := it.iter.Value()
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without error.
func (it *pebbleIterator) Release() {
	if it.iter != nil {
		it.iter.Close()
		it.iter = nil // Prevent multiple closes on the same pebble.Iterator and mark as released
	}
}

type pebbleBatch struct {
	db   *PebbleStore
	b    *pebble.Batch
	size int
}

// NewBatch implements the Store interface
func (d *PebbleStore) NewBatch() ethdb.Batch {
	return &pebbleBatch{db: d, b: d.db.NewBatch()}
}

// NewBatchWithSize implements the Store interface
func (d *PebbleStore) NewBatchWithSize(size int) ethdb.Batch {
	return &pebbleBatch{db: d, b: d.db.NewBatchWithSize(size)}
}

// DeleteRange implements the Store interface
func (d *PebbleStore) DeleteRange(start []byte, end []byte) error {
	return d.db.DeleteRange(start, end, pebble.Sync)
}

// Put inserts the given value into the batch for later committing.
func (b *pebbleBatch) Put(key []byte, value []byte) error {
	if b.b == nil {
		return fmt.Errorf("pebble batch not initialized")
	}
	if err := b.b.Set(key, value, nil); err != nil {
		return err
	}
	b.size += len(key) + len(value)
	return nil
}

// Delete inserts the key removal into the batch for later committing.
func (b *pebbleBatch) Delete(key []byte) error {
	if b.b == nil {
		return fmt.Errorf("pebble batch not initialized")
	}
	if err := b.b.Delete(key, nil); err != nil {
		return err
	}
	b.size += len(key)
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *pebbleBatch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b *pebbleBatch) Write() error {
	if b.b == nil {
		return fmt.Errorf("pebble batch not initialized")
	}
	// Using pebble.Sync for consistency with PebbleStore direct Put/Delete.
	// Pebble's default is pebble.NoSync for Commit unless WriteOptions are passed.
	return b.b.Commit(pebble.Sync)
}

// Reset resets the batch for reuse.
func (b *pebbleBatch) Reset() {
	if b.b != nil {
		b.b.Reset()
	}
	b.size = 0
}

// Replay replays the batch contents over another KeyValueWriter.
func (b *pebbleBatch) Replay(w ethdb.KeyValueWriter) error {
	if b.b == nil {
		return fmt.Errorf("pebble batch not initialized")
	}
	reader := b.b.Reader()
	for {
		kind, k, v, ok, err := reader.Next()
		if !ok {
			if err != nil {
				return err // Error during iteration
			}
			return nil // End of iteration
		}

		switch kind {
		case pebble.InternalKeyKindSet:
			if err = w.Put(k, v); err != nil {
				return err
			}
		case pebble.InternalKeyKindDelete:
			if err = w.Delete(k); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unhandled pebble batch operation kind: %v", kind)
		}
	}
}

// NewIterator creates a new iterator over the store.
func (d *PebbleStore) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	opts := &pebble.IterOptions{}
	var lowerBound []byte

	if start != nil {
		lowerBound = start
	} else if prefix != nil {
		lowerBound = prefix
	}
	// If both start and prefix are nil, lowerBound remains nil, iterating from the DB start.

	if lowerBound != nil {
		opts.LowerBound = lowerBound
	}

	// For true prefix iteration, an UpperBound should be set.
	// E.g. prefix "foo" -> UpperBound "fop".
	// This is an optimization; without it, our Next() method filters.
	// if prefix != nil {
	//    upperBound := calculatePrefixUpperBound(prefix) // calculatePrefixUpperBound would be a helper
	//    if upperBound != nil {
	//        opts.UpperBound = upperBound
	//    }
	// }

	underlyingIter, err := d.db.NewIter(opts)
	if err != nil {
		// Log the error or handle it as per application's error strategy
		// d.log.Error("Failed to create Pebble iterator", "err", err)
		return &pebbleIterator{
			db:      d,
			iter:    nil,
			prefix:  prefix,
			initErr: err,
		}
	}

	// The pebble iterator is created. The first call to Next() will position it.
	// If a prefix is used, our pebbleIterator.Next() will filter.
	return &pebbleIterator{
		db:     d,
		iter:   underlyingIter,
		prefix: prefix,
		// start: start, // Storing start is not strictly needed as LowerBound handles it
	}
}

// calculatePrefixUpperBound is a helper to get the upper bound for a prefix.
// For example, prefix "foo" -> upper bound "fop".
// This ensures the iterator stops after the last key with the given prefix.
// func calculatePrefixUpperBound(prefix []byte) []byte {
// 	if len(prefix) == 0 {
// 		return nil // No prefix, no specific upper bound from it.
// 	}
// 	// Create a copy to modify
// 	upperBound := make([]byte, len(prefix))
// 	copy(upperBound, prefix)
// 	// Iterate from the rightmost byte
// 	for i := len(upperBound) - 1; i >= 0; i-- {
// 		if upperBound[i] < 0xff {
// 			upperBound[i]++ // Increment the byte
// 			return upperBound[:i+1] // Return the modified prefix up to this byte
// 		}
// 		// If byte is 0xff, it "overflows" to 0x00 in this position,
// 		// and we continue to the next byte to the left (the "carry").
// 	}
// 	// If all bytes were 0xff, it means this prefix is like "\xff\xff\xff".
// 	// There's no key with this prefix that's greater than it by just incrementing.
// 	// In this specific case, an upper bound is not easily formed this way to exclude others.
// 	// Pebble handles this by iterating up to keys that no longer share the prefix.
// 	// For simplicity, our Next() method will filter by prefix, so an explicit UpperBound
// 	// for strict prefix matching might not be strictly necessary if performance is not critical.
// 	// However, for large ranges, providing an UpperBound to Pebble is much more efficient.
// 	return nil // Returning nil means iterate to the end (if no other bounds) or rely on Next filtering.
// }
