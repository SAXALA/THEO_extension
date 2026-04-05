package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	// Please replace "ethstore_module" with the actual module path defined in your ethstore/go.mod file

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	chainkvdb "github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/ethdb"
	"github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/iterator"
	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
	"github.com/tinoryj/EthStore/standalone/ethstore/pebblestore"
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

// blockMarkerRegex parses trace lines like:
// "Processing block (start), ID: 20500000" / "Processing block (end), ID: 20500000".
var blockMarkerRegex = regexp.MustCompile(`Processing block \((start|end)\),\s*ID:\s*(\d+)`)

var opTypeNames = map[opType]string{
	opGet:          "Get",
	opPut:          "Put",
	opDelete:       "Delete",
	opNewIterator:  "NewIterator",
	opIteratorNext: "IteratorNext",
}

func logPutProgressSeconds(counter int, totalTime time.Duration) {
	fmt.Printf("Put test: %d, use time: %f s\n", counter, totalTime.Seconds())
}

func logPutProgressNanos(counter int, totalTime time.Duration) {
	fmt.Printf("Put test: %d, use time: %d ns\n", counter, totalTime.Nanoseconds())
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
	boundsNs := make([]int64, 0, 900)
	// Keep sub-1ms very dense to improve short-latency percentile accuracy.
	appendRange := func(start, end, step int64) {
		for v := start; v <= end; v += step {
			boundsNs = append(boundsNs, v)
		}
	}

	us := int64(1000)
	ms := int64(1000 * 1000)
	s := int64(1000 * 1000 * 1000)

	// 1us - 100us, step 0.5us
	appendRange(1*us, 100*us, 500)
	// 100us - 1ms, step 2us
	appendRange(102*us, 1*ms, 2*us)

	// >=1ms keeps practical granularity and memory overhead balanced.
	// 1ms - 10ms, step 100us
	appendRange(1*ms+100*us, 10*ms, 100*us)
	// 10ms - 100ms, step 1ms
	appendRange(11*ms, 100*ms, 1*ms)
	// 100ms - 1s, step 10ms
	appendRange(110*ms, 1*s, 10*ms)
	// 1s - 10s, step 100ms
	appendRange(1*s+100*ms, 10*s, 100*ms)
	// 10s - 60s, step 1s
	appendRange(11*s, 60*s, 1*s)

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
		return "BlockData"
	}
	if ethstore.PrefixDBHandledDataTypes[dt] {
		return "StateData"
	}
	return "OtherData"
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

func reportGlobalLatencyStats(stats map[opType]*latencyHistogram) {
	if len(stats) == 0 {
		return
	}
	ops := make([]opType, 0, len(stats))
	for op := range stats {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool {
		return ops[i] < ops[j]
	})

	for _, op := range ops {
		hist := stats[op]
		if hist.totalCount == 0 {
			continue
		}
		totalSec := float64(hist.totalNs) / 1000000000.0
		throughputK := 0.0
		if totalSec > 0 {
			throughputK = float64(hist.totalCount) / totalSec / 1000.0
		}
		fmt.Printf("\n[Latency][Global] op=%s count=%d throughput=%.3f K ops/s avg=%s p50=%s p75=%s p90=%s p95=%s p99=%s p99.99=%s p99.999=%s\n",
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

func reportReplayReadStats(title string, success map[string]int64, miss map[string]int64, successTotal int64, missTotal int64) {
	fmt.Printf("[%s][Global] success=%d notfound=%d\n", title, successTotal, missTotal)
	keys := make([]string, 0, len(success)+len(miss))
	seen := make(map[string]struct{})
	for k := range success {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	for k := range miss {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("[%s] dataType=%s success=%d notfound=%d\n", title, k, success[k], miss[k])
	}
}

func mibToBytes(sizeMiB int) int {
	if sizeMiB <= 0 {
		return 0
	}
	return sizeMiB * 1024 * 1024
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
	TraceFile                    string `json:"traceFile"`
	TraceFileNocache             string `json:"traceFileNocache"`
	TraceFileNoCacheWithSnapshot string `json:"traceFileNoCacheWithSnapshot"`
	EthStoreDir                  string `json:"ethstoreDir"`
	PebbleDBDir                  string `json:"pebbleDir"`
	ChainKVDir                   string `json:"chainKVDir"`
	AccountHashKeyPebbleDir      string `json:"accountHashKeyPebbleDir"`
	LoadedEthstoreDir            string `json:"loadedEthStoreDir"`
}

type prefixdbLoadStage string

const (
	prefixdbLoadStageAll     prefixdbLoadStage = "all"
	prefixdbLoadStageAccount prefixdbLoadStage = "account"
	prefixdbLoadStageStorage prefixdbLoadStage = "storage"
)

func parsePrefixDBLoadStage(raw string) (prefixdbLoadStage, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "all":
		return prefixdbLoadStageAll, nil
	case "account":
		return prefixdbLoadStageAccount, nil
	case "storage":
		return prefixdbLoadStageStorage, nil
	default:
		return "", fmt.Errorf("invalid prefixdb load stage %q (expected: all, account, storage)", raw)
	}
}

func formatPrefixDBStateDirName(chunkFileSize int) string {
	return "database_statedb" + strconv.Itoa(chunkFileSize/1024) + "KB"
}

func resolvePrefixDBStateDir(databaseDir string, explicitStateDir string, chunkFileSize int, stage prefixdbLoadStage) (string, error) {
	if stateDir := strings.TrimSpace(explicitStateDir); stateDir != "" {
		return stateDir, nil
	}
	if stage == prefixdbLoadStageStorage {
		return "", fmt.Errorf("prefixdb storage load requires -prefixdb-state-dir pointing to an account-loaded statedb directory")
	}
	baseDir := strings.TrimSpace(databaseDir)
	if baseDir == "" {
		return "", fmt.Errorf("loadPrefixDB requires non-empty databaseDir (loadedEthStoreDir)")
	}
	return filepath.Join(baseDir, formatPrefixDBStateDirName(chunkFileSize)), nil
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

func NewChainKVLDB(path string, cache int, handles int, useState bool) (*chainKVLDB, error) {
	db, err := chainkvdb.NewLDBDatabase(path, cache, handles)
	if err != nil {
		return nil, fmt.Errorf("failed to open chainkv database: %w", err)
	}
	return &chainKVLDB{
		db:       db,
		useState: useState,
	}, nil
}

func (c *chainKVLDB) useStateForDataType(dataType ethstore.DataType) bool {
	return c.useState && ethstore.PrefixDBHandledDataTypes[dataType]
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

		dataType := ethstore.GetDataTypeFromKey(keyBytes)
		if db.useStateForDataType(dataType) {
			err = db.db.Put_s(keyBytes, valueBytes)
		} else {
			err = db.db.Put(keyBytes, valueBytes)
		}
		if err != nil {
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

func runLoadData(cfg replayConfig, backend string, contractChunkFileSizeBytes int, totalCacheSizeMiB int, prefixdbHandles int, ckvCache int, ckvHandles int, pebbleCache int, pebbleHandles int, ckvUseState bool, ckvLoadLimit int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool, prefixdbStage prefixdbLoadStage, prefixdbStateDir string) error {
	switch {
	case strings.EqualFold(backend, "chainkv"):
		ckv, openErr := NewChainKVLDB(cfg.ChainKVDir, ckvCache, ckvHandles, ckvUseState)
		if openErr != nil {
			return fmt.Errorf("failed to open chainkv database: %w", openErr)
		}
		defer ckv.Close()
		if loadErr := chainKVLoadData(ckv, cfg.ChainKVDir, ckvLoadLimit); loadErr != nil {
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
		if err := pebbleDBLoadData(cfg.PebbleDBDir, cfg.LoadDataDir, pebbleCache, pebbleHandles); err != nil {
			return fmt.Errorf("pebble load failed: %w", err)
		}
		return nil
	case strings.EqualFold(backend, "ethstore"):
		if cfg.EthStoreDir == "" {
			return fmt.Errorf("ld with ethstore backend requires databaseDir in config")
		}
		if cfg.LoadDataDir == "" {
			return fmt.Errorf("ld with ethstore backend requires loadDataDir in config")
		}

		// laod block store
		aolDataFile := strings.TrimSpace(cfg.AolDataFile)
		if aolDataFile == "" {
			return fmt.Errorf("ld with ethstore backend requires aolDataFile in config")
		}
		if err := loadBlockStore(cfg.EthStoreDir, aolDataFile, contractChunkFileSizeBytes, totalCacheSizeMiB, pebbleCache, pebbleHandles, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression); err != nil {
			return fmt.Errorf("ethstore aol load failed: %w", err)
		}
		// load contract account and storage
		if err := loadPrefixdbAndPebble(cfg.EthStoreDir, cfg.LoadDataDir, contractChunkFileSizeBytes, totalCacheSizeMiB, prefixdbHandles, pebbleCache, pebbleHandles, 32, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression); err != nil {
			return fmt.Errorf("ethstore account load failed: %w", err)
		}
	case strings.EqualFold(backend, "prefixdb"):
		if strings.TrimSpace(cfg.LoadedEthstoreDir) == "" {
			return fmt.Errorf("ld with prefixdb backend requires loadedEthStoreDir in config")
		}
		if strings.TrimSpace(cfg.LoadDataDir) == "" {
			return fmt.Errorf("ld with prefixdb backend requires loadDataDir in config")
		}
		if err := loadPrefixDB(cfg.LoadedEthstoreDir, prefixdbStateDir, cfg.LoadDataDir, cfg.AccountHashKeyPebbleDir, prefixdbStage, contractChunkFileSizeBytes, totalCacheSizeMiB, prefixdbHandles, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression); err != nil {
			return fmt.Errorf("prefixdb load failed: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown backend: %s", backend)
	}
	return nil
}

func runGC(backend string, contractCachePrefetchCount int, gcStateDir string, chunkFileSize int, totalCacheSizeMiB int, prefixdbHandles int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool) error {
	if !strings.EqualFold(backend, "ethstore") {
		return fmt.Errorf("gc mode currently supports ethstore backend only")
	}
	stateDir := strings.TrimSpace(gcStateDir)
	if stateDir == "" {
		return fmt.Errorf("gc mode requires -gc-state-dir")
	}
	store, err := ethstore.NewStateOnlyWithPrefixGCAndFileHandlesSettings(stateDir, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, prefixdbHandles)
	if err != nil {
		return fmt.Errorf("gc: failed to open state db: %w", err)
	}
	defer store.Close()

	start := time.Now()
	if err := store.GCPrefixTree(); err != nil {
		return fmt.Errorf("gc: failed to run state db GC: %w", err)
	}
	fmt.Printf("ethstore state db GC finished in %s\n", time.Since(start))
	return nil
}

func runUpgradeIndex(backend string, upgradeStateDir string, chunkFileSize int, totalCacheSizeMiB int, contractCachePrefetchCount int, prefixdbHandles int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool) error {
	if !strings.EqualFold(backend, "ethstore") {
		return fmt.Errorf("upgrade-index mode currently supports ethstore backend only")
	}
	stateDir := strings.TrimSpace(upgradeStateDir)
	if stateDir == "" {
		return fmt.Errorf("upgrade-index mode requires -upgrade-state-dir")
	}
	store, err := ethstore.NewStateOnlyWithPrefixGCAndFileHandlesSettings(stateDir, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, prefixdbHandles)
	if err != nil {
		return fmt.Errorf("upgrade-index: failed to open state db: %w", err)
	}
	defer store.Close()

	start := time.Now()
	if err := store.UpgradeSegmentIndexFiles(); err != nil {
		return fmt.Errorf("upgrade-index: failed to upgrade segment index files: %w", err)
	}
	fmt.Printf("ethstore segment index upgrade finished in %s\n", time.Since(start))
	return nil
}

// ---------------------------------------------------------------------------
// Read-your-writes overlay helper
// ---------------------------------------------------------------------------

// batchReader is satisfied by pebbleBatch (and Database's batch) which expose
// a read-your-writes overlay for pending mutations.
type batchReader interface {
	BatchGet(key []byte) ([]byte, bool)
}

func ensureQueryableBatch(batch ethdb.Batch, owner string) error {
	if batch == nil {
		return fmt.Errorf("%s: nil pebble batch", owner)
	}
	if _, ok := batch.(batchReader); !ok {
		return fmt.Errorf("%s: batch does not implement BatchGet; require map-based queryable batch", owner)
	}
	return nil
}

// getWithPebbleBatchOverlay returns the value for key by checking the pending
// batch first (read-your-writes semantics).  When batch is nil or does not
// implement batchReader it falls back to fallbackGet.
// A nil value returned by BatchGet means the key is deleted in the batch,
// which is reported as ethstore.ErrNotFound.
func getWithPebbleBatchOverlay(batch ethdb.Batch, key []byte, fallbackGet func() ([]byte, error)) ([]byte, error) {
	if batch != nil {
		if br, ok := batch.(batchReader); ok {
			if val, found := br.BatchGet(key); found {
				if val == nil {
					return nil, ethstore.ErrNotFound
				}
				return val, nil
			}
		} else {
			return nil, fmt.Errorf("pebble batch must be map-based queryable (BatchGet)")
		}
	}
	return fallbackGet()
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
	it      iterator.Iterator
	lastVal []byte
}

func (w *chainKVIterWrapper) Next() bool {
	if !w.it.Next() {
		w.lastVal = nil
		return false
	}
	w.lastVal = w.it.Value()
	return true
}
func (w *chainKVIterWrapper) Value() []byte { return w.lastVal }
func (w *chainKVIterWrapper) Release()      { w.it.Release() }

// replayBackend abstracts the three storage backends for the unified replay loop.
type replayBackend interface {
	// Name returns a short human-readable backend identifier.
	Name() string
	// Get reads key, consulting pending batch when applicable.
	Get(key []byte, dataType ethstore.DataType) ([]byte, error)
	// StagePut stages a put within the current block batch.
	StagePut(key, value []byte, dataType ethstore.DataType) error
	// StageDelete stages a delete within the current block batch.
	StageDelete(key []byte, dataType ethstore.DataType) error
	// CommitBlock commits all staged operations for the current block.
	CommitBlock() error
	// NewIterator creates a new iterator for prefix/start.
	// May return a noopIter when the backend cannot iterate over that prefix.
	NewIterator(prefix, start []byte) replayIter
	// PrintCommitStats prints backend-specific commit-latency histograms.
	PrintCommitStats()
	// Close releases backend resources.
	Close()
}

func shouldSkipByDBType(dt ethstore.DataType, dbType DBType) bool {
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
// pebbleBaselineReplayBackend – wraps *pebblestore.PebbleStore
// ---------------------------------------------------------------------------

type pebbleBaselineReplayBackend struct {
	store      *pebblestore.PebbleStore
	batch      ethdb.Batch
	commitHist *latencyHistogram
}

func newPebbleBaselineReplayBackend(dir string, cache int, handles int) (*pebbleBaselineReplayBackend, error) {
	store, err := pebblestore.NewPebbleStore(dir, cache, handles, "", false)
	if err != nil {
		return nil, fmt.Errorf("newPebbleBaselineReplayBackend: open store: %w", err)
	}
	return &pebbleBaselineReplayBackend{store: store, commitHist: newLatencyHistogram()}, nil
}

func (b *pebbleBaselineReplayBackend) Name() string { return "baseline-pebble" }
func (b *pebbleBaselineReplayBackend) Close()       { b.store.Close() }

func (b *pebbleBaselineReplayBackend) Get(key []byte, _ ethstore.DataType) ([]byte, error) {
	return getWithPebbleBatchOverlay(b.batch, key, func() ([]byte, error) {
		return b.store.Get(key)
	})
}
func (b *pebbleBaselineReplayBackend) ensureBatch() (ethdb.Batch, error) {
	if b.batch == nil {
		b.batch = b.store.NewBatch()
	}
	if err := ensureQueryableBatch(b.batch, "pebbleBaselineReplayBackend"); err != nil {
		return nil, err
	}
	return b.batch, nil
}
func (b *pebbleBaselineReplayBackend) StagePut(key, value []byte, _ ethstore.DataType) error {
	batch, err := b.ensureBatch()
	if err != nil {
		return err
	}
	return batch.Put(key, value)
}
func (b *pebbleBaselineReplayBackend) StageDelete(key []byte, _ ethstore.DataType) error {
	batch, err := b.ensureBatch()
	if err != nil {
		return err
	}
	return batch.Delete(key)
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
	blockdbCommitHist  *latencyHistogram
	prefixdbCommitHist *latencyHistogram
	pebbleCommitHist   *latencyHistogram
	blockTotalHist     *latencyHistogram
}

func newEthstoreReplayBackend(dir string, contractCachePrefetchCount int, chunkFileSize int, totalCacheSizeMiB int, prefixdbHandles int, pebbleCache int, pebbleHandles int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool) (*ethstoreReplayBackend, error) {
	store, err := ethstore.NewWithPrefixGCAndStoreSettings(dir, 6000, "put_test", false, chunkFileSize, totalCacheSizeMiB, contractCachePrefetchCount, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, prefixdbHandles, pebbleCache, pebbleHandles)
	if err != nil {
		return nil, fmt.Errorf("newEthstoreReplayBackend: open store: %w", err)
	}
	return &ethstoreReplayBackend{
		store:              store,
		prefixdbCommitHist: newLatencyHistogram(),
		pebbleCommitHist:   newLatencyHistogram(),
		blockdbCommitHist:  newLatencyHistogram(),
		blockTotalHist:     newLatencyHistogram(),
	}, nil
}

func (b *ethstoreReplayBackend) Name() string { return "ethstore" }
func (b *ethstoreReplayBackend) Close()       { b.store.Close() }
func (b *ethstoreReplayBackend) Get(key []byte, dataType ethstore.DataType) ([]byte, error) {
	if ethstore.AolHandledDataTypes[dataType] {
		return b.store.GetFromAOL(key)
	}
	if ethstore.PrefixDBHandledDataTypes[dataType] {
		return b.store.GetFromPrefixDB(key, dataType)
	}
	return getWithPebbleBatchOverlay(b.pebbleBatch, key, func() ([]byte, error) {
		return b.store.GetFromPebble(key)
	})
}

func (b *ethstoreReplayBackend) ensurePebbleBatch() (ethdb.Batch, error) {
	if b.pebbleBatch == nil {
		b.pebbleBatch = b.store.NewBatch()
	}
	if err := ensureQueryableBatch(b.pebbleBatch, "ethstoreReplayBackend"); err != nil {
		return nil, err
	}
	return b.pebbleBatch, nil
}

func (b *ethstoreReplayBackend) StagePut(key, value []byte, dataType ethstore.DataType) error {
	if ethstore.AolHandledDataTypes[dataType] {
		return b.store.BatchPutToAOL(key, value, dataType)
	}
	if ethstore.PrefixDBHandledDataTypes[dataType] {
		err := b.store.BatchPutToPrefixDB(key, value, dataType)
		if err == nil {
			b.prefixdbDirty = true
		}
		return err
	}
	batch, err := b.ensurePebbleBatch()
	if err != nil {
		return err
	}
	return batch.Put(key, value)
}

func (b *ethstoreReplayBackend) StageDelete(key []byte, dataType ethstore.DataType) error {
	if ethstore.AolHandledDataTypes[dataType] {
		return b.store.BatchDeleteFromAOL(key, dataType)
	}
	if ethstore.PrefixDBHandledDataTypes[dataType] {
		err := b.store.BatchDeleteFromPrefixDB(key, dataType)
		if err == nil {
			b.prefixdbDirty = true
		}
		return err
	}
	batch, err := b.ensurePebbleBatch()
	if err != nil {
		return err
	}
	return batch.Delete(key)
}

func (b *ethstoreReplayBackend) CommitBlock() error {
	blockStart := time.Now()
	start := time.Now()
	err := b.store.CommitAOLBatch()
	b.blockdbCommitHist.observe(time.Since(start))
	if err != nil {
		return err
	}
	if b.prefixdbDirty {
		start := time.Now()
		if err := b.store.PrefixdbBatchCommit(); err != nil {
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
	return ethdbIterWrapper{b.store.NewIterator(prefix, start)}
}

func (b *ethstoreReplayBackend) PrintCommitStats() {
	reportHistogramSummary("EthStore commit (Block store)", b.blockdbCommitHist)
	reportHistogramSummary("ethstore commit (State store)", b.prefixdbCommitHist)
	reportHistogramSummary("ethstore commit (PebbleDB)", b.pebbleCommitHist)
	reportHistogramSummary("ethstore commit (Total)", b.blockTotalHist)
}

// ---------------------------------------------------------------------------
// chainKVReplayBackend – wraps *chainKVLDB
// ---------------------------------------------------------------------------

type chainKVReplayBackend struct {
	db              *chainKVLDB
	stateBatch      chainkvdb.Batch
	nonStateBatch   chainkvdb.Batch
	statePending    map[string][]byte
	nonStatePending map[string][]byte
	commitHist      *latencyHistogram
}

func newChainKVReplayBackend(dbDir string, cache, handles int, useState bool) (*chainKVReplayBackend, error) {
	db, err := NewChainKVLDB(dbDir, cache, handles, useState)
	if err != nil {
		return nil, fmt.Errorf("newChainKVReplayBackend: open db: %w", err)
	}
	return &chainKVReplayBackend{db: db, commitHist: newLatencyHistogram()}, nil
}

func (b *chainKVReplayBackend) Name() string { return "chainkv" }
func (b *chainKVReplayBackend) Close()       { b.db.Close() }

func (b *chainKVReplayBackend) pendingOverlay(dataType ethstore.DataType) map[string][]byte {
	if b.db.useStateForDataType(dataType) {
		if b.statePending == nil {
			b.statePending = make(map[string][]byte)
		}
		return b.statePending
	}
	if b.nonStatePending == nil {
		b.nonStatePending = make(map[string][]byte)
	}
	return b.nonStatePending
}

func (b *chainKVReplayBackend) Get(key []byte, dataType ethstore.DataType) ([]byte, error) {
	if val, found := b.pendingOverlay(dataType)[string(key)]; found {
		if val == nil {
			return nil, ethstore.ErrNotFound
		}
		return append([]byte(nil), val...), nil
	}
	if b.db.useStateForDataType(dataType) {
		return b.db.db.Get_s(key)
	}
	return b.db.db.Get(key)
}

func (b *chainKVReplayBackend) StagePut(key, value []byte, dataType ethstore.DataType) error {
	b.pendingOverlay(dataType)[string(key)] = append([]byte(nil), value...)
	if b.db.useStateForDataType(dataType) {
		if b.stateBatch == nil {
			b.stateBatch = b.db.db.NewBatch()
		}
		return b.stateBatch.Put_s(key, value)
	}
	if b.nonStateBatch == nil {
		b.nonStateBatch = b.db.db.NewBatch()
	}
	return b.nonStateBatch.Put(key, value)
}
func (b *chainKVReplayBackend) StageDelete(key []byte, dataType ethstore.DataType) error {
	b.pendingOverlay(dataType)[string(key)] = nil
	if b.db.useStateForDataType(dataType) {
		if b.stateBatch == nil {
			b.stateBatch = b.db.db.NewBatch()
		}
		return b.stateBatch.Put_s(key, nil)
	}
	if b.nonStateBatch == nil {
		b.nonStateBatch = b.db.db.NewBatch()
	}
	return b.nonStateBatch.Put(key, nil)
}
func (b *chainKVReplayBackend) CommitBlock() error {
	if b.stateBatch != nil {
		start := time.Now()
		if err := b.stateBatch.Write(); err != nil {
			fmt.Printf("state batch commit failed: %v\n", err)
		}
		b.commitHist.observe(time.Since(start))
		b.stateBatch = nil
		b.statePending = nil
	}
	if b.nonStateBatch != nil {
		start := time.Now()
		if err := b.nonStateBatch.Write(); err != nil {
			fmt.Printf("non-state batch commit failed: %v\n", err)
		}
		b.commitHist.observe(time.Since(start))
		b.nonStateBatch = nil
		b.nonStatePending = nil
	}
	return nil
}
func (b *chainKVReplayBackend) NewIterator(_, _ []byte) replayIter {
	return &chainKVIterWrapper{it: b.db.db.NewIterator()}
}
func (b *chainKVReplayBackend) PrintCommitStats() {
	reportHistogramSummary("chainkv commit (Batch.Write)", b.commitHist)
}

// ---------------------------------------------------------------------------
// replayTrace – unified replay loop
// ---------------------------------------------------------------------------

// replayTrace replays a workload trace file against the given backend.
// dbType controls which key types are replayed across all backends.
func replayTrace(backend replayBackend, traceFile string, maxOps int64, dbType DBType, startBlockID int64, endBlockID int64, commitBlockInterval int64) {
	fmt.Printf("[%s] Replaying trace from %s\n", backend.Name(), traceFile)
	if commitBlockInterval <= 0 {
		log.Fatalf("replayTrace: invalid commit block interval %d", commitBlockInterval)
	}
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
		traceBytesRead     int64
		stopAtNextBlockEnd bool
		committedAtExit    bool
		replayStarted      bool
		pendingBlocks      int64
		lastIterDataType   ethstore.DataType = ethstore.DataType(-1)
	)
	if startBlockID <= 0 {
		replayStarted = true
	}
	stats := make(map[string]map[opType]*latencyHistogram)
	globalStats := make(map[opType]*latencyHistogram)
	getSuccessByType := make(map[string]int64)
	getNotFoundByType := make(map[string]int64)
	iterNextSuccessByType := make(map[string]int64)
	iterNextEndByType := make(map[string]int64)
	var (
		getSuccessTotal      int64
		getNotFoundTotal     int64
		iterNextSuccessTotal int64
		iterNextEndTotal     int64
	)
	recordOp := func(kvTypeStr string, op opType, elapsed time.Duration) {
		if _, ok := stats[kvTypeStr]; !ok {
			stats[kvTypeStr] = make(map[opType]*latencyHistogram)
		}
		if _, ok := stats[kvTypeStr][op]; !ok {
			stats[kvTypeStr][op] = newLatencyHistogram()
		}
		stats[kvTypeStr][op].observe(elapsed)
		if _, ok := globalStats[op]; !ok {
			globalStats[op] = newLatencyHistogram()
		}
		globalStats[op].observe(elapsed)
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
		traceBytesRead += int64(len(line) + 1) // +1 for newline character

		if marker := blockMarkerRegex.FindStringSubmatch(line); len(marker) == 3 {
			markerType := marker[1]
			markerID, parseErr := strconv.ParseInt(marker[2], 10, 64)
			if parseErr == nil {
				if markerType == "start" && !replayStarted && markerID == startBlockID {
					replayStarted = true
					fmt.Printf("[%s] Replay window started at block ID %d (line %d).\n",
						backend.Name(), markerID, lineCounter)
				}
				if markerType == "end" && replayStarted {
					pendingBlocks++
					if pendingBlocks >= commitBlockInterval {
						if commitErr := backend.CommitBlock(); commitErr != nil {
							fmt.Printf("[%s] block commit failed at line %d: %v\n",
								backend.Name(), lineCounter, commitErr)
							break
						}
						pendingBlocks = 0
					}
					if endBlockID > 0 && markerID == endBlockID {
						committedAtExit = pendingBlocks == 0
						fmt.Printf("[%s] Replay window ended at block ID %d (line %d).\n",
							backend.Name(), markerID, lineCounter)
						break
					}
					if stopAtNextBlockEnd {
						committedAtExit = pendingBlocks == 0
						fmt.Printf("[%s] Reached max ops %d; stopping at block boundary (line %d).\n",
							backend.Name(), maxOps, lineCounter)
						break
					}
				}
			}
			continue
		}
		if !replayStarted {
			if readErr == io.EOF {
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
			if matches[8] != "" {
				iterStartBytes, err = hex.DecodeString(matches[8])
				if err != nil {
					continue
				}
			}
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
			lastIterDataType = dataType
		case "IteratorNext":
			op = opIteratorNext
			dataType = lastIterDataType
		default:
			continue
		}

		if shouldSkipByDBType(dataType, dbType) {
			continue
		}
		kvTypeStr := classifyDataType(dataType)

		counter++
		if counter%10000 == 0 {
			fmt.Printf("\r[%s] ops=%d time=%.2fs read=%d write=%d\n",
				backend.Name(), counter, totalTime.Seconds(), logicReadSize, logicWriteSize)
		}
		if maxOps > 0 && counter >= maxOps && !stopAtNextBlockEnd {
			stopAtNextBlockEnd = true
			fmt.Printf("\n[%s] Reached max ops %d; waiting for next block boundary.\n",
				backend.Name(), maxOps)
		}

		//debug
		// testKeyHex := "4fc01cf44b5fd388621bce9fca946de503c1f9fa5c34765867954352ad3baec0080f01"
		// testKeyBytes, _ := hex.DecodeString(testKeyHex)
		// val, err := backend.Get(testKeyBytes, ethstore.TrieNodeStorageDataType)
		// if err != nil && !errors.Is(err, ethstore.ErrNotFound) {
		// 	fmt.Printf("Debug: Get error for key %s: %v\n", testKeyHex, err)
		// } else if err == nil {
		// 	fmt.Printf("Debug: Get success for key %s: value length %d\n", testKeyHex, len(val))
		// }

		start := time.Now()
		var opErr error
		switch op {
		case opGet:
			val, getErr := backend.Get(keyBytes, dataType)
			opErr = getErr
			if opErr == nil {
				logicReadSize += int64(len(val))
				getSuccessByType[kvTypeStr]++
				getSuccessTotal++
			} else if errors.Is(opErr, ethstore.ErrNotFound) {
				getNotFoundByType[kvTypeStr]++
				getNotFoundTotal++
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
					iterNextEndByType[kvTypeStr]++
					iterNextEndTotal++
				} else {
					logicReadSize += int64(len(iter.Value()))
					iterNextSuccessByType[kvTypeStr]++
					iterNextSuccessTotal++
				}
			} else {
				iterNextEndByType[kvTypeStr]++
				iterNextEndTotal++
			}
		}
		elapsed := time.Since(start)
		totalTime += elapsed
		// comment to disable operation failed output
		// if opErr != nil {
		// 	if dataType != ethstore.SnapshotAccountDataType && dataType != ethstore.SnapshotStorageDataType {
		// 		fmt.Printf("[%s] op %s failed for key %s: %v\n",
		// 			backend.Name(), opTypeStr, keyHex, opErr)
		// 	}
		// }
		recordOp(kvTypeStr, op, elapsed)
		if readErr == io.EOF {
			break
		}
	}

	if !committedAtExit || pendingBlocks > 0 {
		if commitErr := backend.CommitBlock(); commitErr != nil {
			fmt.Printf("[%s] final commit failed: %v\n", backend.Name(), commitErr)
		}
	}
	fmt.Printf("\n[%s] Replay finished. ops=%d time=%.2fs throughput=%.2f ops/s read=%d write=%d\n",
		backend.Name(), counter, totalTime.Seconds(), float64(counter)/totalTime.Seconds(), logicReadSize, logicWriteSize)
	fmt.Printf("\n[%s] Trace file bytes read: %d bytes (%.2f GiB)\n",
		backend.Name(), traceBytesRead, float64(traceBytesRead)/1024/1024/1024)
	reportLatencyStats(stats)
	reportGlobalLatencyStats(globalStats)
	reportReplayReadStats("GetStats", getSuccessByType, getNotFoundByType, getSuccessTotal, getNotFoundTotal)
	reportReplayReadStats("IteratorNextStats", iterNextSuccessByType, iterNextEndByType, iterNextSuccessTotal, iterNextEndTotal)
	backend.PrintCommitStats()
}

func printRuntimeArgsSnapshot(
	mode string,
	backend string,
	configPath string,
	maxOps int64,
	startBlockID int64,
	endBlockID int64,
	commitBlockInterval int64,
	dbTypeStr string,
	traceFileSelector string,
	resolvedTraceFile string,
	contractChunkFileSizeBytes int,
	totalCacheSizeMiB int,
	prefixdbHandles int,
	contractCachePrefetchCount int,
	nodeFileGCRatioThreshold float64,
	gcWorkers int,
	storageGCThreshold float64,
	nodeFileSortedCompression bool,
	segmentIndexCompression bool,
	ckvCache int,
	ckvHandles int,
	pebbleCache int,
	pebbleHandles int,
	ckvUseState bool,
	ckvLoadLimit int,
	gcStateDir string,
	prefixdbLoadStage string,
	prefixdbStateDir string,
) {
	fmt.Println("==== replayWorkload args ====")
	fmt.Printf("argv=%s\n", strings.Join(os.Args, " "))
	fmt.Printf("mode=%s\n", mode)
	fmt.Printf("backend=%s\n", backend)
	fmt.Printf("config=%s\n", configPath)
	fmt.Printf("max_ops=%d\n", maxOps)
	fmt.Printf("start_block_id=%d\n", startBlockID)
	fmt.Printf("end_block_id=%d\n", endBlockID)
	fmt.Printf("commit_block_interval=%d\n", commitBlockInterval)
	fmt.Printf("db_type=%s\n", dbTypeStr)
	fmt.Printf("trace_file_selector=%s\n", traceFileSelector)
	fmt.Printf("trace_file_resolved=%s\n", resolvedTraceFile)

	if strings.EqualFold(backend, "ethstore") {
		fmt.Printf("state_cache_prefetch_count=%d\n", contractCachePrefetchCount)
		fmt.Printf("contract_chunk_file_mib=%d\n", contractChunkFileSizeBytes)
		fmt.Printf("total_cache_size_mib=%d\n", totalCacheSizeMiB)
		fmt.Printf("prefixdb_handles=%d\n", prefixdbHandles)
		fmt.Printf("pebble_cache=%d\n", pebbleCache)
		fmt.Printf("pebble_handles=%d\n", pebbleHandles)
		fmt.Printf("node_file_gc_unsorted_ratio_threshold=%g\n", nodeFileGCRatioThreshold)
		fmt.Printf("gc_workers=%d\n", gcWorkers)
		fmt.Printf("storage_gc_threshold=%g\n", storageGCThreshold)
		fmt.Printf("node_file_sorted_compression=%t\n", nodeFileSortedCompression)
		fmt.Printf("segment_index_compression=%t\n", segmentIndexCompression)
	} else if strings.EqualFold(backend, "prefixdb") {
		fmt.Printf("contract_chunk_file_mib=%d\n", contractChunkFileSizeBytes)
		fmt.Printf("total_cache_size_mib=%d\n", totalCacheSizeMiB)
		fmt.Printf("prefixdb_handles=%d\n", prefixdbHandles)
		fmt.Printf("prefixdb_load_stage=%s\n", prefixdbLoadStage)
		fmt.Printf("prefixdb_state_dir=%s\n", prefixdbStateDir)
		fmt.Printf("node_file_gc_unsorted_ratio_threshold=%g\n", nodeFileGCRatioThreshold)
		fmt.Printf("gc_workers=%d\n", gcWorkers)
		fmt.Printf("storage_gc_threshold=%g\n", storageGCThreshold)
		fmt.Printf("node_file_sorted_compression=%t\n", nodeFileSortedCompression)
		fmt.Printf("segment_index_compression=%t\n", segmentIndexCompression)
	} else if strings.EqualFold(backend, "chainkv") {
		fmt.Printf("ckv_cache=%d\n", ckvCache)
		fmt.Printf("ckv_handles=%d\n", ckvHandles)
		fmt.Printf("ckv_state=%t\n", ckvUseState)
		fmt.Printf("ckv_limit=%d\n", ckvLoadLimit)
	} else if strings.EqualFold(backend, "pebble") {
		fmt.Printf("pebble_cache=%d\n", pebbleCache)
		fmt.Printf("pebble_handles=%d\n", pebbleHandles)
	}

	if mode == "gc" {
		fmt.Printf("gc_state_dir=%s\n", gcStateDir)
	}
	if mode == "upgrade-index" {
		fmt.Printf("upgrade_state_dir=%s\n", gcStateDir)
	}
	fmt.Println("=============================")
}

func normalizeLegacyBoolFlagArgs(args []string, boolFlags map[string]struct{}) []string {
	if len(args) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(args))
	normalized = append(normalized, args[0])
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if _, ok := boolFlags[arg]; ok && i+1 < len(args) {
			if boolValue, err := strconv.ParseBool(args[i+1]); err == nil {
				normalized = append(normalized, fmt.Sprintf("%s=%t", arg, boolValue))
				i++
				continue
			}
		}
		normalized = append(normalized, arg)
	}
	return normalized
}

func main() {
	os.Args = normalizeLegacyBoolFlagArgs(os.Args, map[string]struct{}{
		"-ckv-state":                    {},
		"-node-file-sorted-compression": {},
		"-segment-index-compression":    {},
	})

	configPath := flag.String("config", "replay_config.json", "Path to replay config JSON")
	mode := flag.String("mode", "re", "Mode of operation: ld/re/gc/upgrade-index")
	backend := flag.String("backend", "ethstore", "Backend for ld/re mode: ethstore, chainkv, or pebble")
	maxOps := flag.Int64("max-ops", 100*1000*1000, "Max operations to replay, 0 means no limit")
	startBlockID := flag.Int64("start-block-id", 0, "Replay start block ID (0 means from beginning)")
	endBlockID := flag.Int64("end-block-id", 0, "Replay end block ID (0 means no early stop by block ID)")
	commitBlockInterval := flag.Int64("commit-block-interval", 1, "Commit staged writes every N completed blocks during replay")
	contractChunkFileSizeBytes := flag.Int("contract-chunk-file-size-bytes", 0, "Chunk file size for ld mode in bytes (0 means use default)")
	totalCacheSizeMiB := flag.Int("total-cache-size-mib", 0, "Total shared PrefixDB cache size for ld/re/gc in MiB (0 means use default)")
	prefixdbHandles := flag.Int("prefixdb-handles", 0, "PrefixDB number of cached file handles (0 means use default)")
	ckvCache := flag.Int("ckv-cache", 16, "ChainKV cache size in MB")
	ckvHandles := flag.Int("ckv-handles", 1048576, "ChainKV number of file handles")
	pebbleCache := flag.Int("pebble-cache", 16, "Pebble cache size in MB")
	pebbleHandles := flag.Int("pebble-handles", 1048576, "Pebble number of file handles")
	ckvUseState := flag.Bool("ckv-state", true, "ChainKV use state-specific operations (Put_s/Get_s)")
	ckvLoadLimit := flag.Int("ckv-limit", 0, "ChainKV load limit, 0 means no limit")
	DBTypeStr := flag.String("db-type", "allDBtypes", "Database type for replay: prefixdb, pebble, or aol")
	replayTraceFile := flag.String("trace-file", "Cache", "Path to trace file for recording")
	contractCachePrefetchCount := flag.Int("cache-count", 16, "Number of entries to cache for storage chunk get")
	nodeFileGCRatioThreshold := flag.Float64("node-file-gc-unsorted-ratio-threshold", 1.0, "Trigger PrefixTree node-file GC when unsorted_count/sorted_count reaches this ratio")
	gcWorkers := flag.Int("gc-workers", 0, "Shared GC workers for node-file GC and storage GC (0 means auto)")
	legacyNodeFileGCWorkers := flag.Int("node-file-gc-workers", 0, "Deprecated alias for -gc-workers")
	storageGCThreshold := flag.Float64("storage-gc-threshold", 2.0, "Trigger segmented storage GC when chunk_file_size >= target_chunk_size * threshold")
	nodeFileSortedCompression := flag.Bool("node-file-sorted-compression", true, "Enable zstd compression for node file sorted payload")
	segmentIndexCompression := flag.Bool("segment-index-compression", true, "Enable zstd compression for segment index files")
	gcStateDir := flag.String("gc-state-dir", "", "State DB directory for gc mode (direct path, no copy)")
	upgradeStateDir := flag.String("upgrade-state-dir", "", "State DB directory for upgrade-index mode (direct path, no copy)")
	prefixdbLoadStageRaw := flag.String("prefixdb-load-stage", "all", "PrefixDB load stage for ld mode: all, account, storage")
	prefixdbStateDir := flag.String("prefixdb-state-dir", "", "Target PrefixDB statedb directory for prefixdb ld mode; required for storage stage")
	flag.Parse()
	resolvedPrefixDBLoadStage, err := parsePrefixDBLoadStage(*prefixdbLoadStageRaw)
	if err != nil {
		log.Fatal(err)
	}
	resolvedGCWorkers := *gcWorkers
	if resolvedGCWorkers <= 0 {
		resolvedGCWorkers = *legacyNodeFileGCWorkers
	}
	if *startBlockID > 0 && *endBlockID > 0 && *endBlockID < *startBlockID {
		log.Fatalf("invalid block window: -end-block-id (%d) must be >= -start-block-id (%d)", *endBlockID, *startBlockID)
	}
	if *commitBlockInterval <= 0 {
		log.Fatalf("invalid -commit-block-interval %d (must be >= 1)", *commitBlockInterval)
	}

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

	printRuntimeArgsSnapshot(
		*mode,
		*backend,
		*configPath,
		*maxOps,
		*startBlockID,
		*endBlockID,
		*commitBlockInterval,
		*DBTypeStr,
		*replayTraceFile,
		traceFile,
		*contractChunkFileSizeBytes,
		*totalCacheSizeMiB,
		*prefixdbHandles,
		*contractCachePrefetchCount,
		*nodeFileGCRatioThreshold,
		resolvedGCWorkers,
		*storageGCThreshold,
		*nodeFileSortedCompression,
		*segmentIndexCompression,
		*ckvCache,
		*ckvHandles,
		*pebbleCache,
		*pebbleHandles,
		*ckvUseState,
		*ckvLoadLimit,
		func() string {
			if *mode == "upgrade-index" && strings.TrimSpace(*upgradeStateDir) != "" {
				return *upgradeStateDir
			}
			return *gcStateDir
		}(),
		string(resolvedPrefixDBLoadStage),
		*prefixdbStateDir,
	)

	go func() {
		// Start the HTTP server for pprof profiling
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	// For quick debugging
	// ethBackend, ethErr := newEthstoreReplayBackend(cfg.EthStoreDir, *contractCachePrefetchCount, *contractChunkFileSizeBytes, *totalCacheSizeMiB, *prefixdbHandles, *pebbleCache, *pebbleHandles, *nodeFileGCRatioThreshold, resolvedGCWorkers, *storageGCThreshold, *nodeFileSortedCompression, *segmentIndexCompression)
	// if ethErr != nil {
	// 	log.Fatalf("re: failed to open ethstore backend: %v", ethErr)
	// }
	// defer ethBackend.Close()
	// replayTrace(ethBackend, traceFile, *maxOps, dbType, *startBlockID, *endBlockID, *commitBlockInterval)
	// return

	switch *mode {
	case "ld":
		if err := runLoadData(cfg, *backend, *contractChunkFileSizeBytes, *totalCacheSizeMiB, *prefixdbHandles, *ckvCache, *ckvHandles, *pebbleCache, *pebbleHandles, *ckvUseState, *ckvLoadLimit, *nodeFileGCRatioThreshold, resolvedGCWorkers, *storageGCThreshold, *nodeFileSortedCompression, *segmentIndexCompression, resolvedPrefixDBLoadStage, *prefixdbStateDir); err != nil {
			log.Fatalf("ld failed: %v", err)
		}
	case "re":
		if strings.EqualFold(*backend, "chainkv") {
			ckvBackend, ckvErr := newChainKVReplayBackend(cfg.ChainKVDir, *ckvCache, *ckvHandles, *ckvUseState)
			if ckvErr != nil {
				log.Fatalf("re: failed to open chainkv backend: %v", ckvErr)
			}
			defer ckvBackend.Close()
			replayTrace(ckvBackend, traceFile, *maxOps, dbType, *startBlockID, *endBlockID, *commitBlockInterval)
		} else if strings.EqualFold(*backend, "pebble") {
			pbBackend, pbErr := newPebbleBaselineReplayBackend(cfg.PebbleDBDir, *pebbleCache, *pebbleHandles)
			if pbErr != nil {
				log.Fatalf("rb: failed to open pebble baseline backend: %v", pbErr)
			}
			defer pbBackend.Close()
			replayTrace(pbBackend, traceFile, *maxOps, dbType, *startBlockID, *endBlockID, *commitBlockInterval)
		} else {
			ethBackend, ethErr := newEthstoreReplayBackend(cfg.EthStoreDir, *contractCachePrefetchCount, *contractChunkFileSizeBytes, *totalCacheSizeMiB, *prefixdbHandles, *pebbleCache, *pebbleHandles, *nodeFileGCRatioThreshold, resolvedGCWorkers, *storageGCThreshold, *nodeFileSortedCompression, *segmentIndexCompression)
			if ethErr != nil {
				log.Fatalf("re: failed to open ethstore backend: %v", ethErr)
			}
			defer ethBackend.Close()
			replayTrace(ethBackend, traceFile, *maxOps, dbType, *startBlockID, *endBlockID, *commitBlockInterval)
		}
	case "gc":
		if err := runGC(*backend, *contractCachePrefetchCount, *gcStateDir, *contractChunkFileSizeBytes, *totalCacheSizeMiB, *prefixdbHandles, *nodeFileGCRatioThreshold, resolvedGCWorkers, *storageGCThreshold, *nodeFileSortedCompression, *segmentIndexCompression); err != nil {
			log.Fatalf("gc failed: %v", err)
		}
	case "upgrade-index":
		if err := runUpgradeIndex(*backend, *upgradeStateDir, *contractChunkFileSizeBytes, *totalCacheSizeMiB, *contractCachePrefetchCount, *prefixdbHandles, *nodeFileGCRatioThreshold, resolvedGCWorkers, *storageGCThreshold, *nodeFileSortedCompression, *segmentIndexCompression); err != nil {
			log.Fatalf("upgrade-index failed: %v", err)
		}
	default:
		log.Fatalf("unknown mode %q, use ld/re/gc/upgrade-index", *mode)
	}
}

func pebbleDBLoadData(pebbleDir string, dataFile string, pebbleCache int, pebbleHandles int) error {
	tempDir := pebbleDir
	store, err := pebblestore.NewPebbleStore(tempDir, pebbleCache, pebbleHandles, "", false)
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
			logPutProgressSeconds(counter, totalTime)
		}
	}
	fmt.Printf("\nTotal Put operations: %d, Total time: %f s\n", counter, totalTime.Seconds())
	return nil
}

// load all data from the key-value file into EthStore
func loadPrefixdbAndPebble(dataBaseDir string, loadDataDir string, contractChunkFileSizeBytes int, totalCacheSizeMiB int, prefixdbHandles int, pebbleCache int, pebbleHandles int, contractCachePrefetchCount int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool) error {
	store, err := ethstore.NewWithPrefixGCAndStoreSettings(dataBaseDir, 6000, "put_test", false, contractChunkFileSizeBytes, totalCacheSizeMiB, contractCachePrefetchCount, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, prefixdbHandles, pebbleCache, pebbleHandles)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	// Read key-value pairs from the test file
	file, err := os.Open(loadDataDir)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	var totalTime time.Duration
	counter := 0
	reader := bufio.NewReader(file)

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

		if !ethstore.PrefixDBHandledDataTypes[ethstore.GetDataTypeFromKey(keyBytes)] {
			continue
		}

		keyBytes, err = hex.DecodeString(string(keyBytes))
		if err != nil {
			log.Fatalf("Failed to decode key: %v", err)
		}

		valueBytes, err = hex.DecodeString(string(valueBytes))
		if err != nil {
			log.Fatalf("Failed to decode value: %v", err)
		}

		dataType := ethstore.GetDataTypeFromKey(keyBytes)
		if ethstore.AolHandledDataTypes[dataType] {
			continue
		}

		start := time.Now()
		err = store.PutWithDataType(keyBytes, valueBytes, dataType)
		end := time.Now()

		totalTime += end.Sub(start)
		counter++
		if counter%100000 == 0 {
			logPutProgressSeconds(counter, totalTime)
		}
	}
	if err := store.RunPostLoadGC(); err != nil {
		log.Fatalf("Failed to run post-load GC: %v", err)
	}
	fmt.Printf("\nTotal Put operations: %d, Total time: %f s\n", counter, totalTime.Seconds())
	return nil
}

func loadPrefixDB(databaseDir string, explicitStateDir string, dataFile string, pebbleDir string, stage prefixdbLoadStage, chunkFileSize int, cacheSize int, prefixdbHandles int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool) error {
	dir, err := resolvePrefixDBStateDir(databaseDir, explicitStateDir, chunkFileSize, stage)
	if err != nil {
		return err
	}

	pdb, err := prefixdb.NewPrefixDBWithRuntimeOptions(dir, chunkFileSize, cacheSize, 16, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, prefixdbHandles)
	if err != nil {
		return fmt.Errorf("failed to create PrefixDB: %w", err)
	}
	defer pdb.Close()

	var acccuntHashKeyPebble *pebblestore.PebbleStore
	if stage == prefixdbLoadStageAll || stage == prefixdbLoadStageStorage {
		if len(pebbleDir) == 0 {
			pebbleDir = "/mnt/ssd/ethstore/index/accountHash_key_pebble"
		}
		dbPath := strings.TrimSpace(pebbleDir)
		if dbPath == "" {
			return fmt.Errorf("pebble aux dir is required for prefixdb %s load", stage)
		}
		acccuntHashKeyPebble, err = pebblestore.NewPebbleStore(dbPath, 0, 0, "", false)
		if err != nil {
			return fmt.Errorf("failed to create PebbleStore instance: %w", err)
		}
		defer acccuntHashKeyPebble.Close()
		pdb.ParentKeyResolver = func(storageKey []byte) []byte {
			accountKey, resolveErr := resolvePrefixDBLoadAccountKey(acccuntHashKeyPebble, storageKey)
			if resolveErr != nil {
				return nil
			}
			return accountKey
		}
	}

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
	processedCount := 0
	deferredStorageCount := 0
	deferredStorageSamples := make([]string, 0, 4)
	skippedCount := 0
	targetPrefixSeen := false
	reader := bufio.NewReader(file)

	//isSaveTrie := false

	for {

		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read load data line: %w", err)
		}
		if err == io.EOF && len(line) == 0 {
			break
		}

		// line format: "key: xxxxxx, value: yyyy"
		line = strings.TrimRight(line, "\r\n")
		if len(line) == 0 {
			if err == io.EOF {
				break
			}
			continue
		}

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
		dataType := ethstore.GetDataTypeFromKey(keyBytes)
		var accountKey []byte

		if keyBytes[0] != 'O' && keyBytes[0] != 'A' {
			continue
		}
		if stage == prefixdbLoadStageAccount && keyBytes[0] != 'A' {
			if targetPrefixSeen {
				break
			}
			skippedCount++
			continue
		}
		if stage == prefixdbLoadStageStorage && keyBytes[0] != 'O' {
			if targetPrefixSeen {
				break
			}
			skippedCount++
			continue
		}
		targetPrefixSeen = true
		processedCount++

		// Perform the Put operation
		if keyBytes[0] == 'O' {
			accountKey, err = resolvePrefixDBLoadAccountKey(acccuntHashKeyPebble, keyBytes)
			if err != nil {
				return fmt.Errorf("failed to resolve account key for storage key %x: %w", keyBytes, err)
			}
			if accountKey == nil {
				deferredStorageCount++
				if len(deferredStorageSamples) < cap(deferredStorageSamples) {
					deferredStorageSamples = append(deferredStorageSamples, fmt.Sprintf("%x", keyBytes))
				}
			}
		}
		startTime := time.Now()
		err = pdb.BatchPut(dataType, keyBytes, valueBytes, accountKey)

		endTime := time.Now()
		totalTime += endTime.Sub(startTime)

		if err != nil {
			fmt.Printf("Get operation failed for key %s: %v ", keyPart, err)
			continue
		}
		if counter%100000 == 0 {
			logPutProgressSeconds(counter, totalTime)
			if err := pdb.BatchCommit(); err != nil {
				return fmt.Errorf("failed to commit PrefixDB batch at row %d: %w", counter, err)
			}
		}
		if err == io.EOF {
			break
		}
	}
	if err := pdb.BatchCommit(); err != nil {
		return fmt.Errorf("failed to finalize PrefixDB batch commit: %w", err)
	}
	if deferredStorageCount > 0 {
		return fmt.Errorf("loadPrefixDB deferred %d storage entries with unresolved account keys; sample keys: %v", deferredStorageCount, deferredStorageSamples)
	}

	if err := pdb.RunPostLoadGC(); err != nil {
		return fmt.Errorf("failed to run post-load GC: %w", err)
	}
	fmt.Printf("\nPrefixDB %s load target=%s processed=%d skipped=%d total time: %f s\n", stage, dir, processedCount, skippedCount, totalTime.Seconds())
	return nil
}

func resolvePrefixDBLoadAccountKey(index interface{ Get([]byte) ([]byte, error) }, storageKey []byte) ([]byte, error) {
	if len(storageKey) == 0 || storageKey[0] != 'O' {
		return nil, nil
	}
	if len(storageKey) < 33 {
		return nil, fmt.Errorf("invalid storage key %x", storageKey)
	}
	accountKey, err := index.Get(storageKey[1:33])
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) || errors.Is(err, ethstore.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return accountKey, nil
}

func loadBlockStore(dataBaseDir string, notxFile string, chunkFileSize int, totalCacheSizeMiB int, pebbleCache int, pebbleHandles int, nodeFileGCRatioThreshold float64, gcWorkers int, storageGCThreshold float64, nodeFileSortedCompression bool, segmentIndexCompression bool) error {
	store, err := ethstore.NewWithPrefixGCAndStoreSettings(dataBaseDir, 6000, "put_test", false, chunkFileSize, totalCacheSizeMiB, 16, nodeFileGCRatioThreshold, gcWorkers, storageGCThreshold, nodeFileSortedCompression, segmentIndexCompression, 0, pebbleCache, pebbleHandles)
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
			logPutProgressNanos(counter, totalTime)
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

	accountHashKeyPebble, err := pebblestore.NewPebbleStore(pebblePath, 0, 0, "", false)
	if err != nil {
		return fmt.Errorf("failed to open pebble store: %v", err)
	}
	defer accountHashKeyPebble.Close()

	dir := strings.TrimSpace(targetPebbleDir)
	if dir == "" {
		return fmt.Errorf("account hash index target dir is required")
	}
	db, err := pebblestore.NewPebbleStore(dir, 0, 0, "", false)
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

func loadPebble(dirPath string, testFilePath string) {
	dirPath = strings.TrimSpace(dirPath)
	testFilePath = strings.TrimSpace(testFilePath)
	if dirPath == "" || testFilePath == "" {
		log.Fatalf("loadPebble requires non-empty dirPath and testFilePath")
	}
	fmt.Println("Start load pebble...")
	pdb, err := pebblestore.NewPebbleStore(dirPath, 0, 0, "pebble_load", false)
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
				logPutProgressSeconds(counter, totalTime)
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

func findKeyValuePair(accountHashHex string, ps *pebblestore.PebbleStore) (string, string, error) {
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

func findRecursive(path []byte, pos int, ps *pebblestore.PebbleStore) ([]byte, string, error) {
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
