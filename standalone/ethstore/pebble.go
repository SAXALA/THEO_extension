package ethstore

import (
	"errors"
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
		// Add this line to provide an iterator for PebbleStore
		NewIter: func(o *pebble.IterOptions) (*pebble.Iterator, error) { return nil, errors.New("PebbleStore.NewIter not implemented") },
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

// NewIterator creates a new iterator over the store.
// This is a placeholder and needs to be implemented correctly.
func (d *PebbleStore) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	// This is a non-functional placeholder. 
	// A proper implementation would use d.db.NewIter or similar, 
	// and wrap the pebble.Iterator in a type that satisfies ethdb.Iterator.
	return nil 
}

type batch struct {
	db   *PebbleStore
	b    *pebble.Batch
	size int
}

func (b *pebbleBatch) Put(key, value []byte) error {
	b.size += len(value)
	return b.b.Set(key, value, nil)
}

func (b *pebbleBatch) Delete(key []byte) error {
	return b.b.Delete(key, nil)
}

func (b *pebbleBatch) ValueSize() int {
	return b.size
}

func (b *pebbleBatch) Write() error {
	return b.b.Commit(pebble.Sync)
}

func (b *pebbleBatch) Reset() {
	b.b.Reset()
	b.size = 0
}

// Replay replays the pebbleBatch on a separate database instance
func (b *pebbleBatch) Replay(w ethdb.KeyValueWriter) error {
	reader := b.b.Reader()
	for {
		kind, key, value, ok, err := reader.Next() // Corrected to 5 variables
		if err != nil { // Added error check
			return err
		}
		if !ok {
			break
		}
		switch kind {
		case pebble.InternalKeyKindSet:
			if err := w.Put(key, value); err != nil {
				return err
			}
		case pebble.InternalKeyKindDelete:
			if err := w.Delete(key); err != nil {
				return err
			}
		}
	}
	return nil
}
