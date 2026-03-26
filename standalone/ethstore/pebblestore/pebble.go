// Package pebblestore provides the single authoritative PebbleDB-backed
// key-value store implementation shared by the ethstore and prefixdb packages.
package pebblestore

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

// Sentinel errors returned by PebbleStore methods.
var (
	ErrClosed   = errors.New("database closed")
	ErrNotFound = errors.New("not found")
)

const (
	metricsGatheringInterval = 3 * time.Second
	minCache                = 16
	minHandles              = 16
	defaultCacheValue        = 16
	defaultHandlesValue      = 32768
	defaultNamespaceValue    = "pebble"
)

// PebbleStore is a PebbleDB-backed key-value store used both as an internal
// component of ethstore (alongside AOL and PrefixDB) and as a standalone
// baseline for comparison experiments.
type PebbleStore struct {
	fn             string
	db             *pebble.DB
	quitChan       chan chan error
	compCount      int32
	compTime       int32
	writePause     int32
	writeStall     int32
	diskReadBytes  uint64
	diskWriteBytes uint64
	log            log.Logger
	writeOptions   *pebble.WriteOptions
}

type panicLogger struct{}

func (l panicLogger) Infof(format string, args ...interface{})  {}
func (l panicLogger) Fatalf(format string, args ...interface{}) { panic(fmt.Sprintf(format, args...)) }

func (d *PebbleStore) onCompactionBegin(info pebble.CompactionInfo) { atomic.AddInt32(&d.compCount, 1) }
func (d *PebbleStore) onCompactionEnd(info pebble.CompactionInfo)   { atomic.AddInt32(&d.compTime, 1) }
func (d *PebbleStore) onWriteStallBegin(info pebble.WriteStallBeginInfo) {
	atomic.AddInt32(&d.writePause, 1)
}
func (d *PebbleStore) onWriteStallEnd() { atomic.AddInt32(&d.writeStall, 1) }

func (d *PebbleStore) meter(interval time.Duration, namespace string) {
	compactions := metrics.NewRegisteredGauge(namespace+"pebble/compactions", nil)
	compactionTime := metrics.NewRegisteredGauge(namespace+"pebble/compaction/time", nil)
	writePauses := metrics.NewRegisteredGauge(namespace+"pebble/write/pauses", nil)
	writeStalls := metrics.NewRegisteredGauge(namespace+"pebble/write/stalls", nil)
	diskReads := metrics.NewRegisteredGauge(namespace+"pebble/disk/reads", nil)
	diskWrites := metrics.NewRegisteredGauge(namespace+"pebble/disk/writes", nil)
	for {
		compactions.Update(int64(atomic.LoadInt32(&d.compCount)))
		compactionTime.Update(int64(atomic.LoadInt32(&d.compTime)))
		writePauses.Update(int64(atomic.LoadInt32(&d.writePause)))
		writeStalls.Update(int64(atomic.LoadInt32(&d.writeStall)))
		diskReads.Update(int64(atomic.LoadUint64(&d.diskReadBytes)))
		diskWrites.Update(int64(atomic.LoadUint64(&d.diskWriteBytes)))
		select {
		case errc := <-d.quitChan:
			errc <- nil
			return
		case <-time.After(interval):
		}
	}
}

// NewPebbleStore opens (or creates) a PebbleDB database at the given path.
func NewPebbleStore(file string, cache int, handles int, namespace string, readonly bool) (*PebbleStore, error) {
	if cache <= 0 {
		cache = defaultCacheValue
	}
	if handles <= 0 {
		handles = defaultHandlesValue
	}
	if cache < minCache {
		cache = minCache
	}
	if handles < minHandles {
		handles = minHandles
	}
	if namespace == "" {
		namespace = defaultNamespaceValue
	}

	logger := log.New("database", file)
	logger.Info("Allocated cache and file handles", "cache", common.StorageSize(cache*1024*1024), "handles", handles)

	maxMemTableSize := (1<<31)<<(^uint(0)>>63) - 1
	memTableLimit := 2
	memTableSize := 4 * 1024 * 1024
	if memTableSize >= maxMemTableSize {
		memTableSize = maxMemTableSize - 1
	}

	db := &PebbleStore{
		fn:           file,
		log:          logger,
		quitChan:     make(chan chan error),
		writeOptions: &pebble.WriteOptions{Sync: true},
	}
	opt := &pebble.Options{
		Cache:        pebble.NewCache(int64(cache * 1024 * 1024)),
		MaxOpenFiles: handles,
		MemTableSize: uint64(memTableSize),
		MemTableStopWritesThreshold: memTableLimit,
		MaxConcurrentCompactions: func() int {
			return runtime.NumCPU() / 2
		},
		L0CompactionThreshold:       4,
		L0StopWritesThreshold:       1000,
		L0CompactionFileThreshold:   1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		BytesPerSync:                512 << 10, // 512 KB
		DisableWAL:                  false,
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
	}
	opt.Experimental.ReadSamplingMultiplier = -1 // disable seek compaction as in Geth

	// Log all options values at startup
	logger.Info("Pebble options configured",
		"Cache", common.StorageSize(cache*1024*1024),
		"MaxOpenFiles", opt.MaxOpenFiles,
		"MemTableSize", common.StorageSize(opt.MemTableSize),
		"MemTableStopWritesThreshold", opt.MemTableStopWritesThreshold,
		"L0CompactionThreshold", opt.L0CompactionThreshold,
		"L0StopWritesThreshold", opt.L0StopWritesThreshold,
		"L0CompactionFileThreshold", opt.L0CompactionFileThreshold,
		"LBaseMaxBytes", common.StorageSize(opt.LBaseMaxBytes),
		"BytesPerSync", common.StorageSize(opt.BytesPerSync),
		"DisableWAL", opt.DisableWAL,
		"ReadOnly", opt.ReadOnly,
		"Levels", len(opt.Levels),
		"ReadSamplingMultiplier", opt.Experimental.ReadSamplingMultiplier,
	)
	for i, level := range opt.Levels {
		logger.Info("Pebble level options",
			"level", i,
			"TargetFileSize", common.StorageSize(level.TargetFileSize),
			"FilterPolicy", fmt.Sprintf("%T", level.FilterPolicy),
			"Compression", level.Compression,
		)
	}

	var err error
	if db.db, err = pebble.Open(file, opt); err != nil {
		return nil, err
	}
	go db.meter(metricsGatheringInterval, namespace)
	return db, nil
}

func (d *PebbleStore) Close() error {
	if d.quitChan == nil {
		return nil
	}

	// Log metrics before closing
	metrics := d.db.Metrics()
	d.log.Info("Pebble metrics on close",
		"diskSpaceUsage", common.StorageSize(metrics.DiskSpaceUsage()),
		"uptime", metrics.Uptime,
		"numVirtual", metrics.NumVirtual(),
		"readAmp", metrics.ReadAmp(),
	)

	// Log keys metrics
	d.log.Info("Pebble keys metrics on close",
		"rangeKeySetsCount", metrics.Keys.RangeKeySetsCount,
		"tombstoneCount", metrics.Keys.TombstoneCount,
		"missizedTombstonesCount", metrics.Keys.MissizedTombstonesCount,
	)

	// Log memtable metrics
	d.log.Info("Pebble memtable metrics on close",
		"size", common.StorageSize(metrics.MemTable.Size),
		"count", metrics.MemTable.Count,
		"zombieSize", common.StorageSize(metrics.MemTable.ZombieSize),
		"zombieCount", metrics.MemTable.ZombieCount,
	)

	// Log table metrics
	d.log.Info("Pebble table metrics on close",
		"obsoleteSize", common.StorageSize(metrics.Table.ObsoleteSize),
		"obsoleteCount", metrics.Table.ObsoleteCount,
		"zombieSize", common.StorageSize(metrics.Table.ZombieSize),
		"zombieCount", metrics.Table.ZombieCount,
		"backingTableCount", metrics.Table.BackingTableCount,
		"backingTableSize", common.StorageSize(metrics.Table.BackingTableSize),
	)

	// Log WAL metrics
	d.log.Info("Pebble WAL metrics on close",
		"files", metrics.WAL.Files,
		"obsoleteFiles", metrics.WAL.ObsoleteFiles,
		"size", common.StorageSize(metrics.WAL.Size),
		"physicalSize", common.StorageSize(metrics.WAL.PhysicalSize),
		"bytesIn", common.StorageSize(metrics.WAL.BytesIn),
		"bytesWritten", common.StorageSize(metrics.WAL.BytesWritten),
	)

	// Log compaction metrics
	d.log.Info("Pebble compaction metrics on close",
		"count", metrics.Compact.Count,
		"defaultCount", metrics.Compact.DefaultCount,
		"deleteOnlyCount", metrics.Compact.DeleteOnlyCount,
		"elisionOnlyCount", metrics.Compact.ElisionOnlyCount,
		"moveCount", metrics.Compact.MoveCount,
		"readCount", metrics.Compact.ReadCount,
		"rewriteCount", metrics.Compact.RewriteCount,
		"multiLevelCount", metrics.Compact.MultiLevelCount,
		"estimatedDebt", common.StorageSize(metrics.Compact.EstimatedDebt),
		"inProgressBytes", common.StorageSize(metrics.Compact.InProgressBytes),
		"numInProgress", metrics.Compact.NumInProgress,
		"markedFiles", metrics.Compact.MarkedFiles,
		"duration", metrics.Compact.Duration,
	)

	// Log flush metrics
	d.log.Info("Pebble flush metrics on close",
		"count", metrics.Flush.Count,
		"numInProgress", metrics.Flush.NumInProgress,
		"asIngestCount", metrics.Flush.AsIngestCount,
		"asIngestTableCount", metrics.Flush.AsIngestTableCount,
		"asIngestBytes", common.StorageSize(metrics.Flush.AsIngestBytes),
	)

	// Log ingest metrics
	d.log.Info("Pebble ingest metrics on close",
		"count", metrics.Ingest.Count,
	)

	// Log snapshot metrics
	d.log.Info("Pebble snapshot metrics on close",
		"count", metrics.Snapshots.Count,
		"earliestSeqNum", metrics.Snapshots.EarliestSeqNum,
		"pinnedKeys", metrics.Snapshots.PinnedKeys,
		"pinnedSize", common.StorageSize(metrics.Snapshots.PinnedSize),
	)

	// Log block cache metrics
	d.log.Info("Pebble block cache metrics on close",
		"hits", metrics.BlockCache.Hits,
		"misses", metrics.BlockCache.Misses,
		"hitRate", fmt.Sprintf("%.2f%%", float64(metrics.BlockCache.Hits)/float64(metrics.BlockCache.Hits+metrics.BlockCache.Misses+1)*100),
	)

	// Log table cache metrics
	d.log.Info("Pebble table cache metrics on close",
		"hits", metrics.TableCache.Hits,
		"misses", metrics.TableCache.Misses,
		"hitRate", fmt.Sprintf("%.2f%%", float64(metrics.TableCache.Hits)/float64(metrics.TableCache.Hits+metrics.TableCache.Misses+1)*100),
	)

	// Log level-specific metrics
	for i := 0; i < len(metrics.Levels); i++ {
		level := metrics.Levels[i]
		if level.NumFiles == 0 && level.Size == 0 {
			continue
		}
		d.log.Info("Pebble level metrics on close",
			"level", i,
			"sublevels", level.Sublevels,
			"numFiles", level.NumFiles,
			"numVirtualFiles", level.NumVirtualFiles,
			"size", common.StorageSize(level.Size),
			"virtualSize", common.StorageSize(level.VirtualSize),
			"score", level.Score,
			"bytesIn", common.StorageSize(level.BytesIn),
			"bytesIngested", common.StorageSize(level.BytesIngested),
			"bytesMoved", common.StorageSize(level.BytesMoved),
			"bytesRead", common.StorageSize(level.BytesRead),
			"bytesCompacted", common.StorageSize(level.BytesCompacted),
			"bytesFlushed", common.StorageSize(level.BytesFlushed),
			"tablesCompacted", level.TablesCompacted,
			"tablesFlushed", level.TablesFlushed,
			"tablesIngested", level.TablesIngested,
			"tablesMoved", level.TablesMoved,
		)
	}

	errc := make(chan error)
	d.quitChan <- errc
	if err := <-errc; err != nil {
		return err
	}
	d.quitChan = nil
	return d.db.Close()
}

func (d *PebbleStore) Has(key []byte) (int, bool, error) {
	if d.quitChan == nil {
		return -1, false, ErrClosed
	}
	value, closer, err := d.db.Get(key)
	if err == pebble.ErrNotFound {
		return -1, false, nil
	}
	if err != nil {
		return -1, false, err
	}
	n := len(value)
	closer.Close()
	return n, true, nil
}

func (d *PebbleStore) Get(key []byte) ([]byte, error) {
	if d.quitChan == nil {
		return nil, ErrClosed
	}
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

func (d *PebbleStore) Put(key []byte, value []byte) error {
	if d.quitChan == nil {
		return ErrClosed
	}
	return d.db.Set(key, value, d.writeOptions)
}

func (d *PebbleStore) Delete(key []byte) error {
	if d.quitChan == nil {
		return ErrClosed
	}
	return d.db.Delete(key, d.writeOptions)
}

func (d *PebbleStore) DeleteRange(start, end []byte) error {
	return d.db.DeleteRange(start, end, d.writeOptions)
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

type pebbleIterator struct {
	db      *PebbleStore
	iter    *pebble.Iterator
	prefix  []byte
	initErr error
	moved   bool
}

func (it *pebbleIterator) Next() bool {
	if it.initErr != nil || it.iter == nil {
		return false
	}
	var valid bool
	if !it.moved {
		it.moved = true
		valid = it.iter.First()
	} else {
		valid = it.iter.Next()
	}
	if !valid {
		return false
	}
	if it.prefix != nil && !bytes.HasPrefix(it.iter.Key(), it.prefix) {
		return false
	}
	return true
}

func (it *pebbleIterator) Error() error {
	if it.initErr != nil {
		return it.initErr
	}
	if it.iter == nil {
		return nil
	}
	return it.iter.Error()
}

func (it *pebbleIterator) Key() []byte {
	if it.initErr != nil || it.iter == nil || !it.iter.Valid() {
		return nil
	}
	k := it.iter.Key()
	kc := make([]byte, len(k))
	copy(kc, k)
	return kc
}

func (it *pebbleIterator) Value() []byte {
	if it.initErr != nil || it.iter == nil || !it.iter.Valid() {
		return nil
	}
	v := it.iter.Value()
	vc := make([]byte, len(v))
	copy(vc, v)
	return vc
}

func (it *pebbleIterator) Release() {
	if it.iter != nil {
		it.iter.Close()
		it.iter = nil
	}
}

func (d *PebbleStore) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	opts := &pebble.IterOptions{}
	var lowerBound []byte
	if len(start) > 0 {
		if len(prefix) > 0 && !bytes.HasPrefix(start, prefix) {
			lowerBound = make([]byte, len(prefix)+len(start))
			copy(lowerBound, prefix)
			copy(lowerBound[len(prefix):], start)
		} else {
			lowerBound = start
		}
	} else if len(prefix) > 0 {
		lowerBound = prefix
	}
	if lowerBound != nil {
		opts.LowerBound = lowerBound
	}
	underlyingIter, err := d.db.NewIter(opts)
	if err != nil {
		return &pebbleIterator{db: d, prefix: prefix, initErr: err}
	}
	return &pebbleIterator{db: d, iter: underlyingIter, prefix: prefix}
}

func (d *PebbleStore) GetIterator() (*pebble.Iterator, error) {
	return d.db.NewIter(nil)
}

// Flush flushes any in-memory data to disk. Returns ErrClosed if the store is
// already closed.
func (d *PebbleStore) Flush() error {
	if d.quitChan == nil {
		return ErrClosed
	}
	return d.db.Flush()
}

// ---------------------------------------------------------------------------
// Batch
// ---------------------------------------------------------------------------

type pebbleBatch struct {
	mu   sync.Mutex
	db   *PebbleStore
	b    *pebble.Batch
	kvs  map[string][]byte
	size int
}

// BatchGet returns the pending value for key (read-your-writes overlay).
func (b *pebbleBatch) BatchGet(key []byte) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.kvs[string(key)]
	if !ok {
		return nil, false
	}
	if v == nil {
		return nil, true
	}
	vc := make([]byte, len(v))
	copy(vc, v)
	return vc, true
}

func (d *PebbleStore) NewBatch() ethdb.Batch {
	return &pebbleBatch{db: d, b: d.db.NewBatch(), kvs: make(map[string][]byte)}
}
func (d *PebbleStore) NewBatchWithSize(size int) ethdb.Batch {
	return &pebbleBatch{db: d, b: d.db.NewBatchWithSize(size), kvs: make(map[string][]byte)}
}

// maxBatchKey is used as the upper bound when DeleteRange is called with a nil
// end key, mirroring go-ethereum's ethdb.MaximumKey (32 × 0xff).
var maxBatchKey = bytes.Repeat([]byte{0xff}, 32)

func (b *pebbleBatch) Put(key []byte, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	// Write to underlying batch first so that Replay can see this operation.
	if err := b.b.Set(key, value, nil); err != nil {
		return err
	}
	// Also mirror into kvs for the read-your-writes BatchGet overlay.
	vc := make([]byte, len(value))
	copy(vc, value)
	b.kvs[string(key)] = vc
	b.size += len(key) + len(value)
	return nil
}

func (b *pebbleBatch) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.b == nil {
		return fmt.Errorf("pebble batch not initialized")
	}
	// Write to underlying batch first so that Replay can see this operation.
	if err := b.b.Delete(key, nil); err != nil {
		return err
	}
	b.kvs[string(key)] = nil
	b.size += len(key)
	return nil
}

func (b *pebbleBatch) DeleteRange(start, end []byte) error {
	// Pebble requires end > start. Treat nil end as "delete to the maximum key"
	// (mirrors go-ethereum ethdb.MaximumKey = 32 × 0xff).
	if end == nil {
		end = maxBatchKey
	}
	if err := b.b.DeleteRange(start, end, nil); err != nil {
		return err
	}
	b.size += len(start) + len(end)
	return nil
}

func (b *pebbleBatch) ValueSize() int { return b.size }

func (b *pebbleBatch) Write() (err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			err = ErrClosed
		}
	}()
	if b.b == nil {
		return fmt.Errorf("pebble batch not initialized")
	}
	// All Put/Delete/DeleteRange ops have already been applied to b.b as they
	// were called; just commit the batch.
	return b.b.Commit(b.db.writeOptions)
}

func (b *pebbleBatch) Reset() {
	if b.b != nil {
		b.b.Reset()
	}
	b.size = 0
	b.kvs = make(map[string][]byte)
}

func (b *pebbleBatch) Replay(w ethdb.KeyValueWriter) error {
	if b.b == nil {
		return fmt.Errorf("pebble batch not initialized")
	}
	reader := b.b.Reader()
	for {
		kind, k, v, ok, err := reader.Next()
		if !ok {
			return err
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
		case pebble.InternalKeyKindRangeDelete:
			// k is start key, v is end key for range deletions.
			type rangeDeleter interface {
				DeleteRange(start, end []byte) error
			}
			if rd, ok := w.(rangeDeleter); ok {
				if err = rd.DeleteRange(k, v); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("writer does not implement DeleteRange, cannot replay range deletion")
			}
		default:
			return fmt.Errorf("unhandled pebble batch operation kind: %v", kind)
		}
	}
}
