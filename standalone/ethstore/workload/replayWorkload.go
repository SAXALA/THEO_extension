package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	// Please replace "ethstore_module" with the actual module path defined in your ethstore/go.mod file

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	chainkvdb "github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/ethdb"
	"github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/iterator"
	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
	prefixdb "github.com/tinoryj/EthStore/standalone/ethstore/prefixdb"
)

type opType int

const (
	opGet opType = iota
	opPut
	opDelete
	opNewIterator
	opIteratorNext
)

type DBType int

const (
	AOL DBType = iota
	PrefixDB
	Pebble
	allDBTypes
)

// opRegex is compiled once at init time and reused across all replayTrace calls.
var opRegex = regexp.MustCompile(`OPType:\s*(\w+)(?:,\s*key:\s*([0-9a-fA-F]+),\s*size:\s*(\d+)(?:,\s*value:\s*([0-9a-fA-F]+),\s*size:\s*(\d+))?)?(?:,\s*size:\s*(\d+))?(?:,\s*prefix:\s*([0-9a-fA-F]+),\s*start key:\s*([0-9a-fA-F]*))?`)

var opTypeNames = map[opType]string{
	opGet: "Get",
	opPut:          "Put",
	opDelete:       "Delete",
	opNewIterator:  "NewIterator",
	opIteratorNext: "IteratorNext",
}

type latencyHistogram struct {
	boundsNs   []int64
	counts     []int64
	totalCount int64
	totalNs    int64
	minNs      int64
	maxNs      int64
}

func newLatencyHistogram() *latencyHistogram {
	boundsNs := make([]int64, 0, 60)
	// 1-10 us, step 1 us (9 buckets)
	for i := int64(1); i < 10; i++ {
		boundsNs = append(boundsNs, i*1000)
	}
	// 10-100 us, step 10 us (9 buckets)
	for i := int64(10); i < 100; i += 10 {
		boundsNs = append(boundsNs, i*1000)
	}
	// 100-1000 us, step 100 us (9 buckets)
	for i := int64(100); i < 1000; i += 100 {
		boundsNs = append(boundsNs, i*1000)
	}
	// 1-10 ms, step 1ms (9 buckets)
	for i := int64(1); i < 10; i++ {
		boundsNs = append(boundsNs, i*1000*1000)
	}
	// 10-100 ms, step 10ms (9 buckets)
	for i := int64(10); i < 100; i += 10 {
		boundsNs = append(boundsNs, i*1000*1000)
	}
	// 100-1000 ms, step 100ms (9 buckets)
	for i := int64(100); i < 1000; i += 100 {
		boundsNs = append(boundsNs, i*1000*1000)
	}

	return &latencyHistogram{
		boundsNs: boundsNs,
		counts:   make([]int64, len(boundsNs)+1),
		minNs:    int64(^uint64(0) >> 1),
		maxNs:    0,
	}
}

func (h *latencyHistogram) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	ns := d.Nanoseconds()
	if ns < h.minNs {
		h.minNs = ns
	}
	if ns > h.maxNs {
		h.maxNs = ns
	}
	h.totalCount++
	h.totalNs += ns
	idx := sort.Search(len(h.boundsNs), func(i int) bool {
		return ns <= h.boundsNs[i]
	})
	if idx >= len(h.counts) {
		idx = len(h.counts) - 1
	}
	h.counts[idx]++
}

func (h *latencyHistogram) avg() time.Duration {
	if h.totalCount == 0 {
		return 0
	}
	return time.Duration(h.totalNs / h.totalCount)
}

func (h *latencyHistogram) percentile(p float64) time.Duration {
	if h.totalCount == 0 {
		return 0
	}
	target := int64(math.Ceil(float64(h.totalCount) * p / 100.0))
	if target < 1 {
		target = 1
	}
	var cum int64
	for i, c := range h.counts {
		cum += c
		if cum >= target {
			if i < len(h.boundsNs) {
				return time.Duration(h.boundsNs[i])
			}
			return time.Duration(h.maxNs)
		}
	}
	return time.Duration(h.maxNs)
}

func (h *latencyHistogram) histogramLines() []string {
	if h.totalCount == 0 {
		return nil
	}
	lines := make([]string, 0, len(h.counts))
	total := float64(h.totalCount)
	for i, c := range h.counts {
		if c == 0 {
			continue
		}
		var label string
		if i < len(h.boundsNs) {
			label = "<=" + formatDurationCompact(time.Duration(h.boundsNs[i]))
		} else {
			label = ">" + formatDurationCompact(time.Duration(h.boundsNs[len(h.boundsNs)-1]))
		}
		pct := float64(c) / total * 100.0
		lines = append(lines, fmt.Sprintf("%-14s %12d (%.2f%%)", label, c, pct))
	}
	return lines
}

func formatDurationCompact(d time.Duration) string {
	ns := d.Nanoseconds()
	switch {
	case ns < 1000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1000000:
		return fmt.Sprintf("%.3fus", float64(ns)/1000.0)
	case ns < 1000000000:
		return fmt.Sprintf("%.3fms", float64(ns)/1000000.0)
	default:
		return fmt.Sprintf("%.3fs", float64(ns)/1000000000.0)
	}
}

func opTypeName(op opType) string {
	if name, ok := opTypeNames[op]; ok {
		return name
	}
	return fmt.Sprintf("opType(%d)", op)
}

func dataTypeName(dt ethstore.DataType) string {
	if name, ok := ethstore.DataTypeStrings[dt]; ok {
		return name
	}
	return fmt.Sprintf("DataType(%d)", dt)
}

// classifyDataType returns the stats bucket label for a given data type:
// "AOL", "PrefixDB", or the raw data-type name for pebble-handled types.
func classifyDataType(dt ethstore.DataType) string {
	if ethstore.AolHandledDataTypes[dt] {
		return "AOL"
	}
	if ethstore.PrefixDBHandledDataTypes[dt] {
		return "PrefixDB"
	}
	return dataTypeName(dt)
}

func reportLatencyStats(stats map[string]map[opType]*latencyHistogram) {
	if len(stats) == 0 {
		return
	}
	dataTypes := make([]string, 0, len(stats))
	for dt := range stats {
		dataTypes = append(dataTypes, dt)
	}
	sort.Slice(dataTypes, func(i, j int) bool {
		return dataTypes[i] < dataTypes[j]
	})

	for _, dt := range dataTypes {
		opMap := stats[dt]
		ops := make([]opType, 0, len(opMap))
		for op := range opMap {
			ops = append(ops, op)
		}
		sort.Slice(ops, func(i, j int) bool {
			return ops[i] < ops[j]
		})

		for _, op := range ops {
			hist := opMap[op]
			if hist.totalCount == 0 {
				continue
			}
			totalSec := float64(hist.totalNs) / 1000000000.0
			throughputK := 0.0
			if totalSec > 0 {
				throughputK = float64(hist.totalCount) / totalSec / 1000.0
			}
			fmt.Printf("\n[Latency] dataType=%s op=%s count=%d throughput=%.3f K ops/s avg=%s p50=%s p75=%s p90=%s p95=%s p99=%s p99.99=%s p99.999=%s\n",
				dt,
				opTypeName(op),
				hist.totalCount,
				throughputK,
				formatDurationCompact(hist.avg()),
				formatDurationCompact(hist.percentile(50.0)),
				formatDurationCompact(hist.percentile(75.0)),
				formatDurationCompact(hist.percentile(90.0)),
				formatDurationCompact(hist.percentile(95.0)),
				formatDurationCompact(hist.percentile(99.0)),
				formatDurationCompact(hist.percentile(99.99)),
				formatDurationCompact(hist.percentile(99.999)),
			)
			fmt.Println("Histogram (<= upper bound):")
			for _, line := range hist.histogramLines() {
				fmt.Printf("  %s\n", line)
			}
		}
	}
}

func reportHistogramSummary(label string, hist *latencyHistogram) {
	if hist == nil || hist.totalCount == 0 {
		fmt.Printf("\n[Latency] %s: no samples\n", label)
		return
	}
	min := time.Duration(hist.minNs)
	max := time.Duration(hist.maxNs)
	fmt.Printf("\n[Latency] %s count=%d avg=%s p50=%s p75=%s p90=%s p95=%s p99=%s p99.99=%s p99.999=%s min=%s max=%s\n",
		label,
		hist.totalCount,
		formatDurationCompact(hist.avg()),
		formatDurationCompact(hist.percentile(50.0)),
		formatDurationCompact(hist.percentile(75.0)),
		formatDurationCompact(hist.percentile(90.0)),
		formatDurationCompact(hist.percentile(95.0)),
		formatDurationCompact(hist.percentile(99.0)),
		formatDurationCompact(hist.percentile(99.99)),
		formatDurationCompact(hist.percentile(99.999)),
		formatDurationCompact(min),
		formatDurationCompact(max),
	)
}

type replayConfig struct {
	LoadDataDir                  string `json:"loadDataDir"`
	AolDataFile                  string `json:"aolDataFile"`
	AccountHashIndexSourceDir    string `json:"accountHashIndexSourceDir"`
	AccountHashIndexTargetDir    string `json:"accountHashIndexTargetDir"`
	PebbleAuxDir                 string `json:"pebbleAuxDir"`
	TraceFile                    string `json:"traceFile"`
	TraceFileNocache             string `json:"traceFileNocache"`
	TraceFileNoCacheWithSnapshot string `json:"traceFileNoCacheWithSnapshot"`
	EthStoreDir                  string `json:"ethstoreDir"`
	PebbleDBDir            		 string `json:"pebbleDir"`
	ChainKVDir           		 string `json:"chainKVDir"`
}

func loadReplayConfig(path string) (replayConfig, error) {
	var cfg replayConfig
	file, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

type chainKVLDB struct {
	db            *chainkvdb.LDBDatabase
	useState      bool
	statePrefixes [][]byte
}

func NewChainKVLDB(path string, cache int, handles int, useState bool, statePrefixes []string) (*chainKVLDB, error) {
	db, err := chainkvdb.NewLDBDatabase(path, cache, handles)
	if err != nil {
		return nil, fmt.Errorf("failed to open chainkv database: %w", err)
	}

	prefixes := make([][]byte, 0, len(statePrefixes))
	for _, prefix := range statePrefixes {
		prefix = strings.TrimSpace(prefix)
		if prefix == "" {
			continue
		}
		prefixes = append(prefixes, []byte(prefix))
	}

	return &chainKVLDB{
		db:            db,
		useState:      useState,
		statePrefixes: prefixes,
	}, nil
}

func (c *chainKVLDB) useStateForKey(key []byte) bool {
	if c.useState {
		return true
	}
	for _, prefix := range c.statePrefixes {
		if bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (c *chainKVLDB) Put(key, value []byte) error {
	if c.useStateForKey(key) {
		return c.db.Put_s(key, value)
	}
	return c.db.Put(key, value)
}

func (c *chainKVLDB) Get(key []byte) ([]byte, error) {
	if c.useStateForKey(key) {
		return c.db.Get_s(key)
	}
	return c.db.Get(key)
}

func (c *chainKVLDB) Delete(key []byte) error {
	return c.db.Delete(key)
}

func (c *chainKVLDB) NewBatch() chainkvdb.Batch {
	return c.db.NewBatch()
}

func (c *chainKVLDB) BatchPut(batch chainkvdb.Batch, key, value []byte) error {
	if c.useStateForKey(key) {
		return batch.Put_s(key, value)
	}
	return batch.Put(key, value)
}

func (c *chainKVLDB) BatchDelete(batch chainkvdb.Batch, key []byte) error {
	if c.useStateForKey(key) {
		return batch.Put_s(key, nil)
	}
	return batch.Put(key, nil)
}

func (c *chainKVLDB) NewIterator() iterator.Iterator {
	return c.db.NewIterator()
}

func (c *chainKVLDB) IteratorNext(it iterator.Iterator) (key, value []byte, valid bool) {
	if it.Next() {
		return it.Key(), it.Value(), true
	}
	return nil, nil, false
}

func (c *chainKVLDB) BatchCommit(batch chainkvdb.Batch) error {
	return batch.Write()
}

func (c *chainKVLDB) Close() {
	if c.db != nil && c.db.LDB() != nil {
		_ = c.db.LDB().Close()
	}
}

func chainKVLoadData(db *chainKVLDB, dataFile string, limit int) error {
	file, err := os.Open(dataFile)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	count := 0
	startTime := time.Now()

	for limit == 0 || count < limit {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			if readErr == io.EOF {
				if line == "" {
					break
				}
			} else {
				return fmt.Errorf("error reading data file: %w", readErr)
			}
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if readErr == io.EOF {
				break
			}
			continue
		}

		parts := strings.Split(line, ", Value :")
		if len(parts) != 2 {
			log.Printf("无法解析行: %s", line)
			continue
		}

		keyPart := strings.TrimPrefix(parts[0], "Key: ")
		valuePart := strings.TrimSpace(parts[1])
		keyBytes, err := hex.DecodeString(keyPart)
		if err != nil {
			return fmt.Errorf("failed to decode key: %w", err)
		}
		valueBytes, err := hex.DecodeString(valuePart)
		if err != nil {
			return fmt.Errorf("failed to decode value: %w", err)
		}

		if err := db.Put(keyBytes, valueBytes); err != nil {
			return fmt.Errorf("failed to put key-value: %w", err)
		}
		count++
		if count%100000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(count) / elapsed.Seconds()
			fmt.Printf("Loaded %d entries (%.2f ops/sec)\n", count, rate)
		}

		if readErr == io.EOF {
			break
		}
	}

	elapsed := time.Since(startTime)
	rate := float64(count) / elapsed.Seconds()
	fmt.Printf("Loaded %d entries in %v (%.2f ops/sec)\n", count, elapsed, rate)
	return nil
}

func runLoadData(cfg replayConfig, backend string, ldChunkFileSize int, ldCacheSize int, ckvCache int, ckvHandles int, ckvUseState bool, ckvStateKeyPrefixes string, ckvLoadLimit int) error {
	switch {
	case strings.EqualFold(backend, "chainkv"):
		dbDir := cfg.ChainKVDir
		if dbDir == "" {
			dbDir = cfg.EthStoreDir
		}
		if dbDir == "" {
			return fmt.Errorf("ld with chainkv backend requires chainKVDatabaseDir or databaseDir in config")
		}
		dataFile := cfg.LoadDataDir
		if dataFile == "" {
			return fmt.Errorf("ld with chainkv backend requires loadDataDir in config")
		}
		var prefixes []string
		if strings.TrimSpace(ckvStateKeyPrefixes) != "" {
			prefixes = strings.Split(ckvStateKeyPrefixes, ",")
		}
		ckv, openErr := NewChainKVLDB(dbDir, ckvCache, ckvHandles, ckvUseState, prefixes)
		if openErr != nil {
			return fmt.Errorf("failed to open chainkv database: %w", openErr)
		}
		defer ckv.Close()
		if loadErr := chainKVLoadData(ckv, dataFile, ckvLoadLimit); loadErr != nil {
			return fmt.Errorf("chainkv load failed: %w", loadErr)
		}
		return nil
	case strings.EqualFold(backend, "pebble"):
		if cfg.PebbleDBDir == "" {
			return fmt.Errorf("ld with pebble backend requires pebbleDir in config")
		}
		if cfg.LoadDataDir == "" {
			return fmt.Errorf("ld with pebble backend requires loadDataDir in config")
		}
		if err := loadbaselineData(cfg.PebbleDBDir, cfg.LoadDataDir); err != nil {
			return fmt.Errorf("pebble load failed: %w", err)
		}
		return nil
	default:
		if cfg.EthStoreDir == "" {
			return fmt.Errorf("ld with ethstore backend requires databaseDir in config")
		}
		if cfg.LoadDataDir == "" {
			return fmt.Errorf("ld with ethstore backend requires loadDataDir in config")
		}
		if cfg.PebbleAuxDir == "" {
			return fmt.Errorf("ld with ethstore backend requires pebbleAuxDir in config")
		}
		if cfg.AccountHashIndexSourceDir == "" || cfg.AccountHashIndexTargetDir == "" {
			return fmt.Errorf("ld with ethstore backend requires accountHashIndexSourceDir and accountHashIndexTargetDir in config")
		}
		if err := loadAccount(cfg.EthStoreDir, cfg.LoadDataDir, cfg.PebbleAuxDir, cfg.AccountHashIndexSourceDir, cfg.AccountHashIndexTargetDir, ldChunkFileSize, ldCacheSize); err != nil {
			return fmt.Errorf("ethstore load failed: %w", err)
		}

		aolDataFile := strings.TrimSpace(cfg.AolDataFile)
		if aolDataFile == "" {
			return fmt.Errorf("ld with ethstore backend requires aolDataFile in config")
		}
		if err := loadAol(cfg.EthStoreDir, aolDataFile); err != nil {
			return fmt.Errorf("ethstore aol load failed: %w", err)
		}
		return nil
	}
}


// ---------------------------------------------------------------------------
// Read-your-writes overlay helper
// ---------------------------------------------------------------------------

// batchReader is satisfied by pebbleBatch (and Database's batch) which expose
// a read-your-writes overlay for pending mutations.
type batchReader interface {
	BatchGet(key []byte) ([]byte, bool)
}

// getter is a minimal store read interface used by getWithPebbleBatchOverlay.
type getter interface {
	Get(key []byte) ([]byte, error)
}

// getWithPebbleBatchOverlay returns the value for key by checking the pending
// batch first (read-your-writes semantics).  When batch is nil or does not
// implement batchReader it falls back to store.Get.
// A nil value returned by BatchGet means the key is deleted in the batch,
// which is reported as ethstore.ErrNotFound.
func getWithPebbleBatchOverlay(store getter, batch ethdb.Batch, key []byte) ([]byte, error) {
	if batch != nil {
		if br, ok := batch.(batchReader); ok {
			if val, found := br.BatchGet(key); found {
				if val == nil {
					return nil, ethstore.ErrNotFound
				}
				return val, nil
			}
		}
	}
	return store.Get(key)
}

// ---------------------------------------------------------------------------
// Unified replay interface
// ---------------------------------------------------------------------------

// replayIter is a simplified iterator interface used by the unified replay loop.
type replayIter interface {
	Next() bool
	Value() []byte
	Release()
}

// noopIter is a placeholder for when an iterator cannot be created
// (e.g., EthStore skips 'O'-prefix iterators).
type noopIter struct{}

func (noopIter) Next() bool    { return false }
func (noopIter) Value() []byte { return nil }
func (noopIter) Release()      {}

// ethdbIterWrapper wraps ethdb.Iterator to satisfy replayIter.
type ethdbIterWrapper struct{ ethdb.Iterator }

// chainKVIterWrapper wraps iterator.Iterator to satisfy replayIter while
// also capturing the value returned by chainKVLDB.IteratorNext.
type chainKVIterWrapper struct {
	db      *chainKVLDB
	it      iterator.Iterator
	lastVal []byte
}

func (w *chainKVIterWrapper) Next() bool {
	_, val, ok := w.db.IteratorNext(w.it)
	w.lastVal = val
	return ok
}
func (w *chainKVIterWrapper) Value() []byte { return w.lastVal }
func (w *chainKVIterWrapper) Release()      { w.it.Release() }

// replayBackend abstracts the three storage backends for the unified replay loop.
type replayBackend interface {
	// Name returns a short human-readable backend identifier.
	Name() string
	// Get reads key, consulting any pending batch first if applicable.
	Get(key []byte) ([]byte, error)
	// StagePut stages a put within the current block batch.
	StagePut(key, value []byte, dataType ethstore.DataType) error
	// StageDelete stages a delete within the current block batch.
	StageDelete(key []byte, dataType ethstore.DataType) error
	// CommitBlock commits all staged operations for the current block.
	CommitBlock() error
	// NewIterator creates a new iterator for prefix/start.
	// May return a noopIter when the backend cannot iterate over that prefix.
	NewIterator(prefix, start []byte) replayIter
	// SkipByDBType returns true when this op should be skipped based on the
	// dbType filter.
	SkipByDBType(dataType ethstore.DataType, dbType DBType) bool
	// PrintCommitStats prints backend-specific commit-latency histograms.
	PrintCommitStats()
	// Close releases backend resources.
	Close()
}

// skipByDBType is a shared helper used by pebble-based backends.
func skipByDBType(dt ethstore.DataType, dbType DBType) bool {
	switch dbType {
	case AOL:
		return !ethstore.AolHandledDataTypes[dt]
	case PrefixDB:
		return !ethstore.PrefixDBHandledDataTypes[dt]
	case Pebble:
		return ethstore.AolHandledDataTypes[dt] || ethstore.PrefixDBHandledDataTypes[dt]
	}
	return false
}

// ---------------------------------------------------------------------------
// pebbleBaselineReplayBackend – wraps *ethstore.PebbleStore
// ---------------------------------------------------------------------------

type pebbleBaselineReplayBackend struct {
	store      *ethstore.PebbleStore
	batch      ethdb.Batch
	commitHist *latencyHistogram
}

func newPebbleBaselineReplayBackend(dir string) (*pebbleBaselineReplayBackend, error) {
	store, err := ethstore.NewPebbleStore(dir, 0, 0, "", false)
	if err != nil {
		return nil, fmt.Errorf("newPebbleBaselineReplayBackend: open store: %w", err)
	}
	return &pebbleBaselineReplayBackend{store: store, commitHist: newLatencyHistogram()}, nil
}

func (b *pebbleBaselineReplayBackend) Name() string { return "baseline-pebble" }
func (b *pebbleBaselineReplayBackend) Close()       { b.store.Close() }

func (b *pebbleBaselineReplayBackend) SkipByDBType(dt ethstore.DataType, dbType DBType) bool {
	return skipByDBType(dt, dbType)
}
func (b *pebbleBaselineReplayBackend) Get(key []byte) ([]byte, error) {
	return getWithPebbleBatchOverlay(b.store, b.batch, key)
}
func (b *pebbleBaselineReplayBackend) ensureBatch() ethdb.Batch {
	if b.batch == nil {
		b.batch = b.store.NewBatch()
	}
	return b.batch
}
func (b *pebbleBaselineReplayBackend) StagePut(key, value []byte, _ ethstore.DataType) error {
	return b.ensureBatch().Put(key, value)
}
func (b *pebbleBaselineReplayBackend) StageDelete(key []byte, _ ethstore.DataType) error {
	return b.ensureBatch().Delete(key)
}
func (b *pebbleBaselineReplayBackend) CommitBlock() error {
	if b.batch == nil {
		return nil
	}
	start := time.Now()
	err := b.batch.Write()
	b.commitHist.observe(time.Since(start))
	b.batch = nil
	return err
}
func (b *pebbleBaselineReplayBackend) NewIterator(prefix, start []byte) replayIter {
	return ethdbIterWrapper{b.store.NewIterator(prefix, start)}
}
func (b *pebbleBaselineReplayBackend) PrintCommitStats() {
	reportHistogramSummary("baseline-pebble commit (Batch.Write)", b.commitHist)
}

// ---------------------------------------------------------------------------
// ethstoreReplayBackend – wraps *ethstore.Database
// ---------------------------------------------------------------------------

type ethstoreReplayBackend struct {
	store              *ethstore.Database
	pebbleBatch        ethdb.Batch
	prefixdbDirty      bool
	prefixdbCommitHist *latencyHistogram
	pebbleCommitHist   *latencyHistogram
	blockTotalHist     *latencyHistogram
}

func newEthstoreReplayBackend(dir string, cacheCount int) (*ethstoreReplayBackend, error) {
	store, err := ethstore.New(dir, 6000, "put_test", false, 16*1024, 12*1024*1024, cacheCount)
	if err != nil {
		return nil, fmt.Errorf("newEthstoreReplayBackend: open store: %w", err)
	}
	return &ethstoreReplayBackend{
		store:              store,
		prefixdbCommitHist: newLatencyHistogram(),
		pebbleCommitHist:   newLatencyHistogram(),
		blockTotalHist:     newLatencyHistogram(),
	}, nil
}

func (b *ethstoreReplayBackend) Name() string { return "ethstore" }
func (b *ethstoreReplayBackend) Close()       { b.store.Close() }
func (b *ethstoreReplayBackend) SkipByDBType(dt ethstore.DataType, dbType DBType) bool {
	return skipByDBType(dt, dbType)
}
func (b *ethstoreReplayBackend) Get(key []byte) ([]byte, error) {
	return getWithPebbleBatchOverlay(b.store, b.pebbleBatch, key)
}
func (b *ethstoreReplayBackend) ensurePebbleBatch() ethdb.Batch {
	if b.pebbleBatch == nil {
		b.pebbleBatch = b.store.NewBatch()
	}
	return b.pebbleBatch
}
func (b *ethstoreReplayBackend) StagePut(key, value []byte, dataType ethstore.DataType) error {
	if ethstore.AolHandledDataTypes[dataType] || ethstore.PrefixDBHandledDataTypes[dataType] {
		err := b.store.BatchPut(key, value, dataType)
		if err == nil {
			b.prefixdbDirty = true
		}
		return err
	}
	return b.ensurePebbleBatch().Put(key, value)
}
func (b *ethstoreReplayBackend) StageDelete(key []byte, dataType ethstore.DataType) error {
	if ethstore.AolHandledDataTypes[dataType] || ethstore.PrefixDBHandledDataTypes[dataType] {
		return b.store.BatchDelete(key, dataType)
	}
	return b.ensurePebbleBatch().Delete(key)
}
func (b *ethstoreReplayBackend) CommitBlock() error {
	blockStart := time.Now()
	if b.prefixdbDirty {
		start := time.Now()
		if err := b.store.PrefixdbBatchCommit('O'); err != nil {
			return err
		}
		b.prefixdbCommitHist.observe(time.Since(start))
		b.prefixdbDirty = false
	}
	if b.pebbleBatch != nil {
		start := time.Now()
		if err := b.pebbleBatch.Write(); err != nil {
			return err
		}
		b.pebbleCommitHist.observe(time.Since(start))
		b.pebbleBatch = nil
	}
	b.blockTotalHist.observe(time.Since(blockStart))
	return nil
}
func (b *ethstoreReplayBackend) NewIterator(prefix, start []byte) replayIter {
	if len(prefix) > 0 && prefix[0] == 'O' {
		return noopIter{}
	}
	return ethdbIterWrapper{b.store.NewIterator(prefix, start)}
}
func (b *ethstoreReplayBackend) PrintCommitStats() {
	reportHistogramSummary("ethstore commit (PrefixdbBatchCommit)", b.prefixdbCommitHist)
	reportHistogramSummary("ethstore commit (pebble Batch.Write)", b.pebbleCommitHist)
	reportHistogramSummary("ethstore commit (block total)", b.blockTotalHist)
}

// ---------------------------------------------------------------------------
// chainKVReplayBackend – wraps *chainKVLDB
// ---------------------------------------------------------------------------

type chainKVReplayBackend struct {
	db            *chainKVLDB
	stateBatch    chainkvdb.Batch
	nonStateBatch chainkvdb.Batch
	commitHist    *latencyHistogram
}

func newChainKVReplayBackend(dbDir string, cache, handles int, useState bool, statePrefixes []string) (*chainKVReplayBackend, error) {
	db, err := NewChainKVLDB(dbDir, cache, handles, useState, statePrefixes)
	if err != nil {
		return nil, fmt.Errorf("newChainKVReplayBackend: open db: %w", err)
	}
	return &chainKVReplayBackend{db: db, commitHist: newLatencyHistogram()}, nil
}

func (b *chainKVReplayBackend) Name() string { return "chainkv" }
func (b *chainKVReplayBackend) Close()       { b.db.Close() }

func (b *chainKVReplayBackend) SkipByDBType(dt ethstore.DataType, dbType DBType) bool {
	return skipByDBType(dt, dbType)
}
func (b *chainKVReplayBackend) Get(key []byte) ([]byte, error)                  { return b.db.Get(key) }

func (b *chainKVReplayBackend) StagePut(key, value []byte, _ ethstore.DataType) error {
	if b.db.useStateForKey(key) {
		if b.stateBatch == nil {
			b.stateBatch = b.db.NewBatch()
		}
		return b.db.BatchPut(b.stateBatch, key, value)
	}
	if b.nonStateBatch == nil {
		b.nonStateBatch = b.db.NewBatch()
	}
	return b.db.BatchPut(b.nonStateBatch, key, value)
}
func (b *chainKVReplayBackend) StageDelete(key []byte, _ ethstore.DataType) error {
	if b.db.useStateForKey(key) {
		if b.stateBatch == nil {
			b.stateBatch = b.db.NewBatch()
		}
		return b.db.BatchDelete(b.stateBatch, key)
	}
	if b.nonStateBatch == nil {
		b.nonStateBatch = b.db.NewBatch()
	}
	return b.db.BatchDelete(b.nonStateBatch, key)
}
func (b *chainKVReplayBackend) CommitBlock() error {
	if b.stateBatch != nil {
		start := time.Now()
		if err := b.db.BatchCommit(b.stateBatch); err != nil {
			fmt.Printf("state batch commit failed: %v\n", err)
		}
		b.commitHist.observe(time.Since(start))
		b.stateBatch = nil
	}
	if b.nonStateBatch != nil {
		start := time.Now()
		if err := b.db.BatchCommit(b.nonStateBatch); err != nil {
			fmt.Printf("non-state batch commit failed: %v\n", err)
		}
		b.commitHist.observe(time.Since(start))
		b.nonStateBatch = nil
	}
	return nil
}
func (b *chainKVReplayBackend) NewIterator(_, _ []byte) replayIter {
	return &chainKVIterWrapper{db: b.db, it: b.db.NewIterator()}
}
func (b *chainKVReplayBackend) PrintCommitStats() {
	reportHistogramSummary("chainkv commit (Batch.Write)", b.commitHist)
}

// ---------------------------------------------------------------------------
// replayTrace – unified replay loop
// ---------------------------------------------------------------------------

// replayTrace replays a workload trace file against the given backend.
// dbType controls which key types are replayed across all backends.
func replayTrace(backend replayBackend, traceFile string, maxOps int64, dbType DBType) {
	fmt.Printf("[%s] Replaying trace from %s\n", backend.Name(), traceFile)
	file, err := os.Open(traceFile)
	if err != nil {
		log.Fatalf("replayTrace: failed to open trace file: %v", err)
	}
	defer file.Close()

	var (
		totalTime          time.Duration
		counter            int64
		lineCounter        int64
		logicReadSize      int64
		logicWriteSize     int64
		stopAtNextBlockEnd bool
		lastIterDataType   ethstore.DataType = ethstore.DataType(-1)
	)
	stats := make(map[string]map[opType]*latencyHistogram)
	recordOp := func(kvTypeStr string, op opType, elapsed time.Duration) {
		if _, ok := stats[kvTypeStr]; !ok {
			stats[kvTypeStr] = make(map[opType]*latencyHistogram)
		}
		if _, ok := stats[kvTypeStr][op]; !ok {
			stats[kvTypeStr][op] = newLatencyHistogram()
		}
		stats[kvTypeStr][op].observe(elapsed)
	}

	reader := bufio.NewReader(file)
	var iter replayIter
	defer func() {
		if iter != nil {
			iter.Release()
		}
	}()

	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			if readErr == io.EOF {
				if line == "" {
					break
				}
			} else {
				log.Printf("error reading trace file: %v", readErr)
				break
			}
		}
		line = strings.TrimSpace(line)
		lineCounter++

		if strings.Contains(line, "Processing block (end)") {
			if commitErr := backend.CommitBlock(); commitErr != nil {
				fmt.Printf("[%s] block commit failed at line %d: %v\n",
					backend.Name(), lineCounter, commitErr)
				break
			}
			if stopAtNextBlockEnd {
				fmt.Printf("[%s] Reached max ops %d; stopping at block boundary (line %d).\n",
					backend.Name(), maxOps, lineCounter)
				break
			}
			continue
		}
		if !strings.Contains(line, "OPType:") {
			continue
		}
		matches := opRegex.FindStringSubmatch(line)
		if len(matches) < 2 {
			continue
		}

		opTypeStr := matches[1]
		keyHex := ""
		var keyBytes []byte
		dataType := ethstore.DataType(-1)
		if len(matches) >= 3 && matches[2] != "" {
			keyHex = matches[2]
			keyBytes, err = hex.DecodeString(keyHex)
			if err != nil {
				continue
			}
			if len(keyBytes) > 0 {
				dataType = ethstore.GetDataTypeFromKey(keyBytes)
			}
		}
		var valueHex string
		var valueBytes []byte
		if len(matches) >= 6 && matches[4] != "" {
			valueHex = matches[4]
			valueBytes, err = hex.DecodeString(valueHex)
			if err != nil && valueHex != "" {
				continue
			}
		}
		var iterPrefixBytes, iterStartBytes []byte
		if len(matches) >= 9 && matches[7] != "" {
			iterPrefixBytes, err = hex.DecodeString(matches[7])
			if err != nil {
				continue
			}
			dataType = ethstore.GetDataTypeFromKey(iterPrefixBytes)
			lastIterDataType = dataType
			if matches[8] != "" {
				iterStartBytes, err = hex.DecodeString(matches[8])
				if err != nil {
					continue
				}
			}
		}

		counter++
		if counter%10000 == 0 {
			fmt.Printf("\r[%s] ops=%d time=%.2fs read=%d write=%d",
				backend.Name(), counter, totalTime.Seconds(), logicReadSize, logicWriteSize)
		}
		if maxOps > 0 && counter >= maxOps && !stopAtNextBlockEnd {
			stopAtNextBlockEnd = true
			fmt.Printf("\n[%s] Reached max ops %d; waiting for next block boundary.\n",
				backend.Name(), maxOps)
		}

		kvTypeStr := classifyDataType(dataType)

		if backend.SkipByDBType(dataType, dbType) {
			continue
		}

		var op opType
		switch opTypeStr {
		case "Get":
			op = opGet
		case "Put", "BatchPut":
			op = opPut
		case "Delete", "BatchDelete":
			op = opDelete
		case "NewIterator":
			op = opNewIterator
		case "IteratorNext":
			dataType = lastIterDataType
			op = opIteratorNext
			kvTypeStr = classifyDataType(dataType)
		default:
			continue
		}

		start := time.Now()
		var opErr error
		switch op {
		case opGet:
			val, getErr := backend.Get(keyBytes)
			opErr = getErr
			if opErr == nil {
				logicReadSize += int64(len(val))
			}
		case opPut:
			if len(keyBytes) == 0 {
				continue
			}
			opErr = backend.StagePut(keyBytes, valueBytes, dataType)
			if opErr == nil {
				logicWriteSize += int64(len(keyBytes) + len(valueBytes))
			}
		case opDelete:
			if len(keyBytes) == 0 {
				continue
			}
			opErr = backend.StageDelete(keyBytes, dataType)
			if opErr == nil {
				logicWriteSize += int64(len(keyBytes))
			}
		case opNewIterator:
			if iter != nil {
				iter.Release()
			}
			iter = backend.NewIterator(iterPrefixBytes, iterStartBytes)
		case opIteratorNext:
			if iter != nil {
				if ok := iter.Next(); !ok {
					iter.Release()
					iter = nil
				} else {
					logicReadSize += int64(len(iter.Value()))
				}
			}
		}
		elapsed := time.Since(start)
		totalTime += elapsed
		if opErr != nil {
			fmt.Printf("[%s] op %s failed for key %s: %v\n",
				backend.Name(), opTypeStr, keyHex, opErr)
		}
		recordOp(kvTypeStr, op, elapsed)
		if readErr == io.EOF {
			break
		}
	}

	if commitErr := backend.CommitBlock(); commitErr != nil {
		fmt.Printf("[%s] final commit failed: %v\n", backend.Name(), commitErr)
	}
	fmt.Printf("\n[%s] Replay finished. ops=%d time=%.2fs read=%d write=%d\n",
		backend.Name(), counter, totalTime.Seconds(), logicReadSize, logicWriteSize)
	reportLatencyStats(stats)
	backend.PrintCommitStats()
}

func main() {
	configPath := flag.String("config", "replay_config.json", "Path to replay config JSON")
	mode := flag.String("mode", "re", "Mode of operation: ld/re/rb")
	backend := flag.String("backend", "ethstore", "Backend for ld/re mode: ethstore, chainkv, or pebble")
	maxOps := flag.Int64("max-ops", 100*1000*1000, "Max operations to replay, 0 means no limit")
	ldChunkFileSize := flag.Int("ld-chunk-file-size", 0, "Chunk file size for ld mode")
	ldCacheSize := flag.Int("ld-cache-size", 0, "Cache size for ld mode")
	ckvCache := flag.Int("ckv-cache", 16, "ChainKV cache size in MB")
	ckvHandles := flag.Int("ckv-handles", 128, "ChainKV number of file handles")
	ckvUseState := flag.Bool("ckv-state", true, "ChainKV use state-specific operations (Put_s/Get_s)")
	ckvStateKeyPrefixes := flag.String("ckv-state-key-prefixes", "", "ChainKV comma-separated key prefixes routed to Put_s/Get_s")
	ckvLoadLimit := flag.Int("ckv-limit", 0, "ChainKV load limit, 0 means no limit")
	DBTypeStr := flag.String("db-type", "allDBtypes", "Database type for replay: prefixdb, pebble, or aol")
	replayTraceFile := flag.String("trace-file", "Cache", "Path to trace file for recording")
	cacheCount := flag.Int("cache-count", 16, "Number of entries to cache for storage chunk get")
	flag.Parse()

	cfg, err := loadReplayConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config %s: %v", *configPath, err)
	}

	dbType := allDBTypes
	switch strings.ToLower(strings.TrimSpace(*DBTypeStr)) {
	case "prefixdb":
		*DBTypeStr = "prefixdb"
		dbType = PrefixDB
	case "pebble":
		*DBTypeStr = "pebble"
		dbType = Pebble
	case "aol":
		*DBTypeStr = "aol"
		dbType = AOL
	case "alldbtypes", "all_db_types", "all":
		*DBTypeStr = "allDBTypes"
		dbType = allDBTypes
	default:
		log.Fatalf("invalid -db-type %q (expected: prefixdb, pebble, aol, all)", *DBTypeStr)
	}

	var traceFile string
	switch strings.ToLower(strings.TrimSpace(*replayTraceFile)) {
	case "cache":
		traceFile = cfg.TraceFile
	case "nocache":
		traceFile = cfg.TraceFileNocache
	case "nocache_snap":
		traceFile = cfg.TraceFileNoCacheWithSnapshot
	default:
		log.Fatalf("invalid -trace-file %q (expected: cache, nocache, nocache_snap)", *replayTraceFile)
	}

	go func() {
		// Start the HTTP server for pprof profiling
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	switch *mode {
	case "ld":
		if err := runLoadData(cfg, *backend, *ldChunkFileSize, *ldCacheSize, *ckvCache, *ckvHandles, *ckvUseState, *ckvStateKeyPrefixes, *ckvLoadLimit); err != nil {
			log.Fatalf("ld failed: %v", err)
		}
	case "re":
		if strings.EqualFold(*backend, "chainkv") {
			dbDir := cfg.ChainKVDir
			if dbDir == "" {
				dbDir = cfg.EthStoreDir
			}
			if dbDir == "" {
				log.Fatal("re with chainkv backend requires chainKVDatabaseDir or databaseDir in config")
			}
			var prefixes []string
			if strings.TrimSpace(*ckvStateKeyPrefixes) != "" {
				prefixes = strings.Split(*ckvStateKeyPrefixes, ",")
			}
			ckvBackend, ckvErr := newChainKVReplayBackend(dbDir, *ckvCache, *ckvHandles, *ckvUseState, prefixes)
			if ckvErr != nil {
				log.Fatalf("re: failed to open chainkv backend: %v", ckvErr)
			}
			defer ckvBackend.Close()
			replayTrace(ckvBackend, traceFile, *maxOps, dbType)
		} else if strings.EqualFold(*backend, "pebble") {
			pbBackend, pbErr := newPebbleBaselineReplayBackend(cfg.PebbleDBDir)
			if pbErr != nil {
				log.Fatalf("rb: failed to open pebble baseline backend: %v", pbErr)
			}
			defer pbBackend.Close()
			replayTrace(pbBackend, traceFile, *maxOps, dbType)
		} else {
			ethBackend, ethErr := newEthstoreReplayBackend(cfg.EthStoreDir, *cacheCount)
			if ethErr != nil {
				log.Fatalf("re: failed to open ethstore backend: %v", ethErr)
			}
			defer ethBackend.Close()
			replayTrace(ethBackend, traceFile, *maxOps, dbType)
		}
	default:
		log.Fatalf("unknown mode %q, use ld or re", *mode)
	}
}

func loadbaselineData(pebbleDir string, dataFile string) error {
	tempDir := pebbleDir
	store, err := ethstore.NewPebbleStore(tempDir, 0, 0, "", false)
	if err != nil {
		return fmt.Errorf("failed to create PebbleStore: %w", err)
	}
	defer store.Close()

	testFilePath := dataFile

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		return fmt.Errorf("failed to open test file: %w", err)
	}
	defer file.Close()

	var totalTime time.Duration
	counter := 0
	reader := bufio.NewReader(file)

	for {
		counter++
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break // End of file reached
		}

		// line format: "key: xxxxxx, value: yyyy"
		line = line[:len(line)-1] // Remove the newline character

		parts := strings.Split(line, ", Value :")
		if len(parts) != 2 {
			log.Printf("无法解析行: %s", line)
			continue
		}
		keyPart := strings.TrimPrefix(parts[0], "Key: ")
		valuePart := strings.TrimSpace(parts[1])

		// Convert key and value to byte slices
		keyBytes := []byte(keyPart)

		valueBytes := []byte(valuePart)

		keyBytes, err = hex.DecodeString(string(keyBytes))
		if err != nil {
			return fmt.Errorf("failed to decode key: %w", err)
		}
		valueBytes, err = hex.DecodeString(string(valueBytes))
		if err != nil {
			return fmt.Errorf("failed to decode value: %w", err)
		}

		// Perform the Put operation
		startTime := time.Now()
		err = store.Put(keyBytes, valueBytes)
		endTime := time.Now()
		totalTime += endTime.Sub(startTime)
		if err != nil {
			return fmt.Errorf("put operation failed for key %s: %w", keyPart, err)
		}
		// Verify the value was stored correctly
		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %f s", counter, totalTime.Seconds())
		}
	}
	fmt.Printf("\nTotal Put operations: %d, Total time: %f s\n", counter, totalTime.Seconds())
	return nil
}

// load all data from the key-value file into EthStore
func loadData(dataBaseDir string, dataFile string) {
	ethStoreDir := dataBaseDir
	store, err := ethstore.New(ethStoreDir, 1000, "put_test", false, 64*1024, 512*1024*1024, 16)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	// Read key-value pairs from the test file
	file, err := os.Open(dataFile)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	var totalTime time.Duration
	counter := 0
	reader := bufio.NewReader(file)

	//isSaveTrie := false

	for {

		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break // End of file reached
		}

		// line format: "key: xxxxxx, value: yyyy"
		line = line[:len(line)-1] // Remove the newline character

		parts := strings.Split(line, ", Value :")
		if len(parts) != 2 {
			log.Printf("无法解析行: %s", line)
			continue
		}
		keyPart := strings.TrimPrefix(parts[0], "Key: ")
		valuePart := strings.TrimSpace(parts[1])

		// Convert key and value to byte slices
		keyBytes := []byte(keyPart)

		valueBytes := []byte(valuePart)

		keyBytes, err = hex.DecodeString(string(keyBytes))
		if err != nil {
			log.Fatalf("Failed to decode key: %v", err)
		}

		if keyBytes[0] == 'a' || keyBytes[0] == 'o' {
			continue
		}

		valueBytes, err = hex.DecodeString(string(valueBytes))
		if err != nil {
			log.Fatalf("Failed to decode value: %v", err)
		}
		start := time.Now()
		store.Put(keyBytes, valueBytes)
		end := time.Now()

		totalTime += end.Sub(start)
		counter++
		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %f s", counter, totalTime.Seconds())
		}
	}
	fmt.Printf("\nTotal Put operations: %d, Total time: %f s\n", counter, totalTime.Seconds())
}

func loadAccount(databaseDir string, dataFile string, pebbleDir string, accountHashIndexSourceDir string, accountHashIndexTargetDir string, chunkFileSize int, cacheSize int) error {
	var dir string
	chunkFileSizeStr := strconv.Itoa(chunkFileSize/1024) + "KB"

	dir = databaseDir + "/database_statedb" + chunkFileSizeStr

	// dir = databaseDir + "/database_state"

	pdb, err := prefixdb.NewPrefixDB(dir, chunkFileSize, uint64(cacheSize), 16)
	if err != nil {
		return fmt.Errorf("failed to create PrefixDB: %w", err)
	}
	defer pdb.Close()

	dbPath := strings.TrimSpace(pebbleDir)
	if dbPath == "" {
		return fmt.Errorf("pebble aux dir is required for loadAccount")
	}
	ps, err := ethstore.NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		return fmt.Errorf("failed to create PebbleStore instance: %w", err)
	}
	defer ps.Close()

	testFilePath := strings.TrimSpace(dataFile)
	if testFilePath == "" {
		return fmt.Errorf("load data file is required for loadAccount")
	}

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		return fmt.Errorf("failed to open test file: %w", err)
	}
	defer file.Close()

	var totalTime time.Duration
	counter := 0
	reader := bufio.NewReader(file)

	//isSaveTrie := false

	for {

		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break // End of file reached
		}

		// line format: "key: xxxxxx, value: yyyy"
		line = line[:len(line)-1] // Remove the newline character

		counter++

		if counter > 2000000000 {
			break
		}

		parts := strings.Split(line, ", Value :")
		if len(parts) != 2 {
			log.Printf("无法解析行: %s", line)
			continue
		}
		keyPart := strings.TrimPrefix(parts[0], "Key: ")
		valuePart := strings.TrimSpace(parts[1])

		// Convert key and value to byte slices
		keyBytes := []byte(keyPart)

		valueBytes := []byte(valuePart)

		keyBytes, err = hex.DecodeString(string(keyBytes))
		if err != nil {
			return fmt.Errorf("failed to decode key: %w", err)
		}
		valueBytes, err = hex.DecodeString(string(valueBytes))
		if err != nil {
			return fmt.Errorf("failed to decode value: %w", err)
		}
		var accountKey []byte

		if keyBytes[0] != 'O' && keyBytes[0] != 'A' {
			continue
		}

		// Perform the Put operation
		if keyBytes[0] == 'O' {
			accountKey = pdb.GetParentAccountKey(keyBytes)
			if accountKey == nil {
				Key, _, err := findKeyValuePair(keyPart[2:66], ps)
				if Key == "" || err != nil {
					fmt.Printf("Failed to get parent account key for key %s\n", keyPart)
					continue
				}
				accountKey = []byte(Key)
				pdb.InsertAccountHashPebble(keyBytes[1:33], accountKey)
			}
			// accountKey = nil
		}
		startTime := time.Now()
		err = pdb.Put(keyBytes, valueBytes, accountKey)

		endTime := time.Now()
		totalTime += endTime.Sub(startTime)

		// value, ok, err := pdb.Get(keyBytes, accountKey)

		if err != nil {
			// err = pdb.Put(keyBytes, valueBytes)
			fmt.Printf("Get operation failed for key %s: %v ", keyPart, err)
			continue
		}
		// if !ok {
		// 	fmt.Printf("Key %s not found in PrefixDB ", keyPart)
		// 	continue
		// }
		// if !bytes.Equal(value, valueBytes) {
		// 	fmt.Println("counter:", counter)
		// 	// log.Printf("Value mismatch for key %s: expected %x, got %x", keyPart, valueBytes, value)
		// }
		// if err != nil {
		// 	log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		// }
		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %f s", counter, totalTime.Seconds())
		}
	}

	// pdb.SaveTrie()
	pdb.GCPrefixTree()
	if err := insertAccountHashindexTopebble(accountHashIndexSourceDir, accountHashIndexTargetDir); err != nil {
		return fmt.Errorf("failed to sync accountHash index to pebble: %w", err)
	}
	fmt.Printf("\nTotal Put operations: %d, Total time: %f s\n", counter, totalTime.Seconds())
	return nil
}

func loadAol(dataBaseDir string, notxFile string) error {

	store, err := ethstore.New(dataBaseDir, 6000, "put_test", false, 16*1024, 12*1024*1024, 16)
	if err != nil {
		return fmt.Errorf("failed to create EthStore instance: %w", err)
	}
	defer store.Close()
	fmt.Println("Start aol put test...")

	// Read key-value pairs from the test file
	notxfile, err := os.Open(notxFile)
	if err != nil {
		return fmt.Errorf("failed to open aol data file: %w", err)
	}
	defer notxfile.Close()

	var totalTime time.Duration
	counter := 0
	notxreader := bufio.NewReader(notxfile)

	for {

		line, err := notxreader.ReadString('\n')
		if err == io.EOF {
			break
		}

		parts := strings.Split(string(line), "\tvalue:")
		if len(parts) != 2 {
			// log.Printf("无法解析行: %s", line)
			continue
		}
		counter++
		keyPart := strings.TrimPrefix(parts[0], "key: ")
		valuePart := strings.TrimSpace(parts[1])

		// Convert key and value to byte slices
		keyBytes := []byte(keyPart)

		valueBytes := []byte(valuePart)

		keyBytes, err = hex.DecodeString(string(keyBytes))
		if err != nil {
			return fmt.Errorf("failed to decode key: %w", err)
		}
		valueBytes, err = hex.DecodeString(string(valueBytes))
		if err != nil {
			return fmt.Errorf("failed to decode value: %w", err)
		}

		// Perform the Put operation
		startTime := time.Now()
		err = store.Put(keyBytes, valueBytes)

		// value, err := store.Get(keyBytes)
		// if !bytes.Equal(value, valueBytes) {
		// 	log.Printf("Value mismatch for key %s: expected %x, got %x", keyPart, valueBytes, value)
		// }

		endTime := time.Now()
		totalTime += endTime.Sub(startTime)
		if err != nil {
			return fmt.Errorf("aol put operation failed for key %s: %w", keyPart, err)
		}
		// Verify the value was stored correctly

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %d ns", counter, totalTime.Nanoseconds())
		}
	}

	log.Printf("Total Put operations: %d, Total time: %d ns", counter, totalTime.Nanoseconds())
	log.Println("Put test completed.")
	return nil
}

func insertAccountHashindexTopebble(sourcePebblePath string, targetPebbleDir string) error {
	// insert all kvs in hashKeyPebble into memCache
	fmt.Println("Building memcache from pebble store...")
	pebblePath := strings.TrimSpace(sourcePebblePath)
	if pebblePath == "" {
		return fmt.Errorf("account hash index source dir is required")
	}

	accountHashKeyPebble, err := ethstore.NewPebbleStore(pebblePath, 0, 0, "", false)
	if err != nil {
		return fmt.Errorf("failed to open pebble store: %v", err)
	}
	defer accountHashKeyPebble.Close()

	dir := strings.TrimSpace(targetPebbleDir)
	if dir == "" {
		return fmt.Errorf("account hash index target dir is required")
	}
	db, err := ethstore.NewPebbleStore(dir, 0, 0, "", false)
	if err != nil {
		return fmt.Errorf("failed to create pebble store: %v", err)
	}
	defer db.Close()

	iter, err := accountHashKeyPebble.GetIterator()
	if err != nil {
		return fmt.Errorf("failed to get iterator from pebble store: %v", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		if err != nil {
			return fmt.Errorf("failed to set item in memcache: %v", err)
		}
		_, err := db.Get(key)
		if err != nil {
			return fmt.Errorf("failed to check key existence in pebble store: %v", err)
		}
		err = db.Put(key, value)
		if err != nil {
			return fmt.Errorf("failed to put item into pebble store: %v", err)
		}
	}
	fmt.Println("Finished inserting account hash key values.")

	return nil
}

func replayPebble(tempDir string, testFilePath string) {
	tempDir = strings.TrimSpace(tempDir)
	testFilePath = strings.TrimSpace(testFilePath)
	if tempDir == "" || testFilePath == "" {
		log.Fatalf("replayPebble requires non-empty tempDir and testFilePath")
	}
	store, err := ethstore.NewPebbleStore(tempDir, 0, 0, "", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	var totalTime time.Duration
	counter := 0
	reader := bufio.NewReader(file)

	for {
		counter++
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break // End of file reached
		}

		// line format: "key: xxxxxx, value: yyyy"
		line = line[:len(line)-1] // Remove the newline character

		parts := strings.Split(line, ", Value :")
		if len(parts) != 2 {
			log.Printf("无法解析行: %s", line)
			continue
		}
		keyPart := strings.TrimPrefix(parts[0], "Key: ")
		valuePart := strings.TrimSpace(parts[1])

		// Convert key and value to byte slices
		keyBytes := []byte(keyPart)

		valueBytes := []byte(valuePart)
		keyBytes, err = hex.DecodeString(string(keyBytes))
		if err != nil {
			log.Fatalf("Failed to decode key: %v", err)
		}
		valueBytes, err = hex.DecodeString(string(valueBytes))
		if err != nil {
			log.Fatalf("Failed to decode value: %v", err)
		}

		// Perform the Put operation
		startTime := time.Now()
		value, err := store.Get(keyBytes)
		endTime := time.Now()

		totalTime += endTime.Sub(startTime)

		if !bytes.Equal(value, valueBytes) {
			log.Printf("Value mismatch for key %s: expected %x, got %x", keyPart, valueBytes, value)
		}
		if err != nil {
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}
		// Verify the value was stored correctly

		if counter%100000 == 0 {
			fmt.Printf("\rtest: %d, use time: %d ns", counter, totalTime.Nanoseconds())
		}

	}
}

func loadPebble(dirPath string, testFilePath string) {
	dirPath = strings.TrimSpace(dirPath)
	testFilePath = strings.TrimSpace(testFilePath)
	if dirPath == "" || testFilePath == "" {
		log.Fatalf("loadPebble requires non-empty dirPath and testFilePath")
	}
	fmt.Println("Start load pebble...")
	pdb, err := ethstore.NewPebbleStore(dirPath, 0, 0, "pebble_load", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer pdb.Close()

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	var totalTime time.Duration
	counter := 0
	reader := bufio.NewReader(file)

	fmt.Println("start load pebble")
	for {

		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break // End of file reached
		}

		// line format: "key: xxxxxx, value: yyyy"
		line = line[:len(line)-1] // Remove the newline character

		parts := strings.Split(line, ", Value :")
		if len(parts) != 2 {
			log.Printf("无法解析行: %s", line)
			continue
		}
		keyPart := strings.TrimPrefix(parts[0], "Key: ")
		valuePart := strings.TrimSpace(parts[1])

		// Convert key and value to byte slices
		keyBytes := []byte(keyPart)

		valueBytes := []byte(valuePart)

		keyBytes, err = hex.DecodeString(string(keyBytes))
		if err != nil {
			log.Fatalf("Failed to decode key: %v", err)
		}
		valueBytes, err = hex.DecodeString(string(valueBytes))
		if err != nil {
			log.Fatalf("Failed to decode value: %v", err)
		}
		DataType := ethstore.GetDataTypeFromKey(keyBytes)
		if !ethstore.AolHandledDataTypes[DataType] && !ethstore.PrefixDBHandledDataTypes[DataType] {
			// Perform the Put operation
			startTime := time.Now()
			err = pdb.Put(keyBytes, valueBytes)
			endTime := time.Now()
			totalTime += endTime.Sub(startTime)
			counter++
			if err != nil {
				log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
			}
			if counter%100000 == 0 {
				fmt.Printf("\rPut test: %d, use time: %f s", counter, totalTime.Seconds())
			}
		}

	}
	fmt.Printf("\nTotal Put operations: %d, Total time: %f s\n", counter, totalTime.Seconds())
}

const TrieNodeAccountPrefix = "41"

type node interface {
	isNode()
}

type fullNode struct {
	Children [16]node
}

type shortNode struct {
	Key    []byte
	Val    node
	isLeaf bool
}

type valueNode []byte

func (f fullNode) isNode()  {}
func (s shortNode) isNode() {}
func (v valueNode) isNode() {}

func findKeyValuePair(accountHashHex string, ps *ethstore.PebbleStore) (string, string, error) {
	targetPath, err := hexToNibbles(accountHashHex)
	if err != nil {
		return "", "", fmt.Errorf("无效的十六进制哈希: %v", err)
	}

	finalPath, finalValue, err := findRecursive(targetPath, 0, ps)
	if err != nil {
		return "", "", err
	}

	finalDBKey := accountTrieNodeKey(finalPath)
	// decodedValue, err := decodeAccountValue(finalValue)
	// if err != nil {
	// 	return finalDBKey, finalValue, fmt.Errorf("找到键值对，但解码失败: %v", err)
	// }
	// finalValue = fmt.Sprintf("%s (解码: %s)", finalValue, decodedValue)
	return finalDBKey, finalValue, nil
}

func hexToNibbles(h string) ([]byte, error) {
	if len(h)%2 != 0 {
		h = "0" + h
	}
	bytes, err := hex.DecodeString(h)
	if err != nil {
		return nil, err
	}
	nibbles := make([]byte, 0, len(bytes)*2)
	for _, b := range bytes {
		nibbles = append(nibbles, b>>4)
		nibbles = append(nibbles, b&0x0F)
	}
	return nibbles, nil
}

func findRecursive(path []byte, pos int, ps *ethstore.PebbleStore) ([]byte, string, error) {
	// fmt.Printf("递归步骤:\n")
	// fmt.Printf("  - 当前已走路径 (逻辑): %x\n", path[:pos])
	// fmt.Printf("  - 剩余待查路径: %x\n", path[pos:])
	if pos > len(path) {
		// fmt.Println("  - 到达路径末尾，返回空结果")
		return nil, "", fmt.Errorf("到达路径末尾，没有指定分支")
	}

	dbKey := accountTrieNodeKey(path[:pos])

	decode, err := hex.DecodeString(dbKey)
	if err != nil {
		return nil, "", fmt.Errorf("无法解码数据库键 %s: %v", dbKey, err)
	}

	value, err := ps.Get(decode)
	if err != nil {
		// fmt.Printf("无法从数据库中获取键 %s: %v\n", dbKey, err)
		return findRecursive(path, pos+1, ps)
		// return nil, "", fmt.Errorf("无法从数据库中获取键 %s: %v", dbKey, err)
	}

	n, err := decodeNode(value)
	if err != nil {
		return nil, "", err
	}
	// fmt.Printf("  - 解码节点为: %T\n", n)

	switch node := n.(type) {
	case *shortNode:
		if !node.isLeaf {
			// fmt.Printf("  - ShortNode不是叶子节点，继续查找...\n")
			if pos+len(node.Key) >= len(path) {
				return nil, "", fmt.Errorf("到达ShortNode但路径已耗尽，没有指定分支,accountHash: %s", encodePath(path, true))
			}
			// 继续查找剩余路径
			return findRecursive(path, pos+len(node.Key), ps)
		}
		remainBytes := encodePath(path[pos:], true)
		if _, isValue := node.Val.(valueNode); isValue && len(remainBytes) == len(node.Key) {
			// fmt.Printf(" 节点key(字节) %x, 待查询的剩余路径 %x\n", node.Key, remainBytes)
			// fmt.Println("  - ShortNode包含ValueNode，路径完全匹配。查找成功!")

			return path[:pos], "", nil //暂时不返回value
			// return path[:pos], value, nil
		}

		if len(remainBytes) < len(node.Key) || !bytes.Equal(remainBytes[:len(node.Key)], node.Key) {
			return nil, "", fmt.Errorf("路径不匹配: 节点key(字节) %x, 待查询的剩余路径  %x", node.Key, path[pos:])
		}
		// fmt.Printf("  - ShortNode匹配路径前缀(字节): %x\n", node.Key)
		return findRecursive(path, pos+len(node.Key), ps)

	case *fullNode:
		if pos >= len(path) {
			return nil, "", fmt.Errorf("到达FullNode但路径已耗尽，没有指定分支")
		}
		// nibble := path[pos]
		// fmt.Printf("  - FullNode选择分支: %x\n", nibble)
		return findRecursive(path, pos+1, ps)
	}

	return nil, "", fmt.Errorf("未知的节点类型或逻辑错误")
}

func accountTrieNodeKey(path []byte) string {
	return TrieNodeAccountPrefix + hex.EncodeToString(path)
}

func decodeNode(value []byte) (node, error) {
	valBytes := value
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) >= 2 && trimmed[0] == '0' && (trimmed[1] == 'x' || trimmed[1] == 'X') {
		trimmed = trimmed[2:]
	}
	if len(trimmed) > 0 && isHexBytes(trimmed) {
		decoded := make([]byte, hex.DecodedLen(len(trimmed)))
		if _, err := hex.Decode(decoded, trimmed); err == nil {
			valBytes = decoded
		}
	}

	var decoded []interface{}
	if err := rlp.DecodeBytes(valBytes, &decoded); err != nil {
		return valueNode(valBytes), nil
	}

	switch len(decoded) {
	case 2:
		keyBytes, ok := decoded[0].([]byte)
		if !ok {
			return nil, fmt.Errorf("无效的shortNode键类型")
		}

		_, isLeaf := decodePath(keyBytes)

		// fmt.Printf("  - 解码为: shortNode (是叶子节点: %v)\n", isLeaf)

		var value node
		switch v := decoded[1].(type) {
		case []byte:
			value = valueNode(v)
		case []interface{}:
			if len(v) == 17 {
				value = &fullNode{}
			} else if len(v) == 2 {
				value = &shortNode{}
			}
		}

		return &shortNode{Key: keyBytes, Val: value, isLeaf: isLeaf}, nil

	case 17:
		// fmt.Println("  - 解码为: fullNode")
		// 检查fullNode第17个元素是否有value
		if len(decoded) == 17 {
			if val, ok := decoded[16].([]byte); ok && len(val) > 0 {
				fmt.Printf("  - fullNode第17项为value: %x\n", val)
			}
		}
		return &fullNode{}, nil
	}

	return nil, fmt.Errorf("未知的节点编码格式，解码后长度为 %d", len(decoded))
}

func isHexBytes(data []byte) bool {
	if len(data)%2 != 0 {
		return false
	}
	for _, b := range data {
		switch {
		case b >= '0' && b <= '9':
		case b >= 'a' && b <= 'f':
		case b >= 'A' && b <= 'F':
		default:
			return false
		}
	}
	return true
}

// encodePath 将 nibble 路径编码为压缩的字节数组
func encodePath(nibbles []byte, terminator bool) []byte {
	oddLen := len(nibbles)%2 != 0

	// 构造 prefix
	var flags byte
	if terminator {
		flags |= 0x20
	}
	if oddLen {
		flags |= 0x10
	}

	var encoded []byte
	if oddLen {
		// 前缀低4位放入第一个 nibble
		prefix := flags | (nibbles[0] & 0x0F)
		encoded = append([]byte{prefix}, packNibbles(nibbles[1:])...)
	} else {
		// 低4位为0
		prefix := flags
		encoded = append([]byte{prefix}, packNibbles(nibbles)...)
	}

	return encoded
}

// packNibbles 将 nibble 数组每两个合并成一个 byte
func packNibbles(nibbles []byte) []byte {
	out := make([]byte, (len(nibbles)+1)/2)
	for i := 0; i < len(nibbles); i++ {
		if i%2 == 0 {
			out[i/2] = nibbles[i] << 4
		} else {
			out[i/2] |= nibbles[i] & 0x0F
		}
	}
	return out
}

// decodePath 将编码后的字节数组解码为 nibble 路径和 terminator 标志
func decodePath(encoded []byte) (nibbles []byte, terminator bool) {
	if len(encoded) == 0 {
		return nil, false
	}

	prefix := encoded[0]
	terminator = (prefix & 0x20) != 0
	oddLen := (prefix & 0x10) != 0

	unpacked := unpackToNibbles(encoded[1:])

	if oddLen {
		// 低4位是第一个 nibble
		nibbles = append([]byte{prefix & 0x0F}, unpacked...)
	} else {
		nibbles = unpacked
	}
	return
}

// unpackToNibbles 将字节数组还原为 nibble 数组
func unpackToNibbles(bytes []byte) []byte {
	nibbles := make([]byte, 0, len(bytes)*2)
	for _, b := range bytes {
		nibbles = append(nibbles, b>>4)
		nibbles = append(nibbles, b&0x0F)
	}
	return nibbles
}
