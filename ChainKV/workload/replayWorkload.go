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
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb"
	chainkvdb "github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/ethdb"
	"github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/iterator"
)

// Config holds the configuration for workload replay
type Config struct {
	DatabaseDir       string `json:"databaseDir"`
	LoadDataDir       string `json:"loadDataDir"`
	BaselinePebbleDir string `json:"baselinePebbleDir"`
	TraceFile         string `json:"traceFile"`
	TraceFileNocache  string `json:"traceFileNocache"`
}

type opType int

const (
	opGet opType = iota
	opHas
	opPut
	opDelete
	opNewIterator
	opIteratorNext
	opNewBatch
	opBatchPut
	opBatchPutCommit
	opBatchDelete
	opNewBatchWithSize
	opGetBatchValueSize
)

var opTypeNames = map[opType]string{
	opGet:               "Get",
	opHas:               "Has",
	opPut:               "Put",
	opDelete:            "Delete",
	opNewIterator:       "NewIterator",
	opIteratorNext:      "IteratorNext",
	opNewBatch:          "NewBatch",
	opBatchPut:          "BatchPut",
	opBatchPutCommit:    "BatchPutCommit",
	opBatchDelete:       "BatchDelete",
	opNewBatchWithSize:  "NewBatchWithSize",
	opGetBatchValueSize: "GetBatchValueSize",
}

// chainKVLDB wraps ChainKV's LDBDatabase to satisfy kvStore.
type chainKVLDB struct {
	db            *chainkvdb.LDBDatabase
	useState      bool     // if true, always use Put_s/Get_s
	statePrefixes [][]byte // key prefixes that should use Put_s/Get_s
}

// NewChainKVLDB creates a new ChainKV database instance
func NewChainKVLDB(path string, cache int, handles int, useState bool, statePrefixes []string) (*chainKVLDB, error) {
	db, err := chainkvdb.NewLDBDatabase(path, cache, handles)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
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

// Put writes a key-value pair to the database
func (c *chainKVLDB) Put(key, value []byte) error {
	if c.useStateForKey(key) {
		return c.db.Put_s(key, value)
	}
	return c.db.Put(key, value)
}

// Get retrieves a value for the given key
func (c *chainKVLDB) Get(key []byte) ([]byte, error) {
	if c.useStateForKey(key) {
		return c.db.Get_s(key)
	}
	return c.db.Get(key)
}

// Delete removes a key from the database
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
		return batch.Put_s(key, nil) // ChainKV's Batch does not have Delete_s, use Put_s with nil value to indicate deletion
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

// Close closes the database
func (c *chainKVLDB) Close() {
	if c.db != nil && c.db.LDB() != nil {
		// Close the underlying LevelDB directly to avoid nil pointer issues
		_ = c.db.LDB().Close()
	}
}

// loadConfig loads configuration from JSON file
func loadConfig(configPath string) (*Config, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}
	return &config, nil
}

// loadData loads key-value pairs from a file
func loadData(db *chainKVLDB, dataFile string, limit int) error {
	file, err := os.Open(dataFile)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	count := 0
	startTime := time.Now()

	for limit == 0 || count < limit {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if line == "" {
					break
				}
			} else {
				return fmt.Errorf("error reading data file: %w", err)
			}
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if err == io.EOF {
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

		if err := db.Put(keyBytes, valueBytes); err != nil {
			return fmt.Errorf("failed to put key-value: %w", err)
		}
		count++
		if count%100000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(count) / elapsed.Seconds()
			fmt.Printf("Loaded %d entries (%.2f ops/sec)\n", count, rate)
		}

		if err == io.EOF {
			break
		}
	}

	elapsed := time.Since(startTime)
	rate := float64(count) / elapsed.Seconds()
	fmt.Printf("Loaded %d entries in %v (%.2f ops/sec)\n", count, elapsed, rate)
	return nil
}

// benchmarkOperations performs basic benchmark operations
func benchmarkOperations(db *chainKVLDB, numOps int) {
	fmt.Printf("\n=== Running Benchmark (%d operations) ===\n", numOps)

	// Benchmark PUT operations
	startTime := time.Now()
	for i := 0; i < numOps; i++ {
		key := []byte(fmt.Sprintf("bench_key_%d", i))
		value := []byte(fmt.Sprintf("bench_value_%d", i))
		if err := db.Put(key, value); err != nil {
			log.Printf("PUT error: %v", err)
		}
	}
	putDuration := time.Since(startTime)
	putRate := float64(numOps) / putDuration.Seconds()
	fmt.Printf("PUT: %d ops in %v (%.2f ops/sec)\n", numOps, putDuration, putRate)

	// Benchmark GET operations
	startTime = time.Now()
	hits := 0
	for i := 0; i < numOps; i++ {
		key := []byte(fmt.Sprintf("bench_key_%d", i))
		if _, err := db.Get(key); err == nil {
			hits++
		}
	}
	getDuration := time.Since(startTime)
	getRate := float64(numOps) / getDuration.Seconds()
	fmt.Printf("GET: %d ops in %v (%.2f ops/sec, %d hits)\n", numOps, getDuration, getRate, hits)
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
	boundsNs := []int64{
		1000, 2000, 5000, 10000, 20000, 50000,
		100000, 200000, 500000,
		1000000, 2000000, 5000000,
		10000000, 20000000, 50000000,
		100000000, 200000000, 500000000,
		1000000000, 2000000000, 5000000000,
		10000000000,
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

func dataTypeName(dt leveldb.DataType) string {
	if name, ok := leveldb.DataTypeStrings[dt]; ok {
		return name
	}
	return fmt.Sprintf("DataType(%d)", dt)
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
			fmt.Printf("\n[Latency] dataType=%s op=%s count=%d throughput=%.3f K ops/s avg=%s p50=%s p75=%s p90=%s p95=%s p99=%s p99.99=%s\n",
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
			)
			fmt.Println("Histogram (<= upper bound):")
			for _, line := range hist.histogramLines() {
				fmt.Printf("  %s\n", line)
			}
		}
	}
}

type replayConfig struct {
	DatabaseDir       string `json:"databaseDir"`
	LoadDataDir       string `json:"loadDataDir"`
	BaselinePebbleDir string `json:"baselinePebbleDir"`
	TraceFile         string `json:"traceFile"`
	TraceFileNocache  string `json:"traceFileNocache"`
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

func replayTrace(db *chainKVLDB, traceFile string, maxOps int64) error {
	file, err := os.Open(traceFile)
	if err != nil {
		return fmt.Errorf("failed to open trace file: %w", err)
	}
	defer file.Close()

	// Support both formats:
	// - OPType: Put, key: ..., size: ..., value: ..., size: ...
	// - OPType: BatchPutCommit
	// - OPType: NewBatchWithSize, size: <batch-bytes>
	// - OPType: NewIterator, prefix: <hex>, start key: <hex or empty>
	opRegex := regexp.MustCompile(`OPType:\s*(\w+)(?:,\s*key:\s*([0-9a-fA-F]+),\s*size:\s*(\d+)(?:,\s*value:\s*([0-9a-fA-F]+),\s*size:\s*(\d+))?)?(?:,\s*size:\s*(\d+))?(?:,\s*prefix:\s*([0-9a-fA-F]+),\s*start key:\s*([0-9a-fA-F]*))?`)

	var totalTime time.Duration
	var counter int64
	reader := bufio.NewReader(file)

	var logicReadSize int64 = 0
	var logicWriteSize int64 = 0

	// var oldop string

	var lineCounter int64 = 0
	type batchEntry struct {
		prefix byte
		seq    int64
		batch  ethdb.Batch
	}
	// 分离state/non-state的batch
	var stateBatch chainkvdb.Batch
	var nonStateBatch chainkvdb.Batch
	var stateBatchActive bool
	var nonStateBatchActive bool

	var iterator iterator.Iterator
	defer func() {
		if iterator != nil {
			iterator.Release()
		}
	}()
	var lastIterDataType leveldb.DataType = -1
	var stats = make(map[string]map[opType]*latencyHistogram)
	const batchCommitDataType leveldb.DataType = -1
	recordOp := func(op opType, dt leveldb.DataType, elapsed time.Duration) {
		var dtStr string
		if leveldb.AolHandledDataTypes[dt] {
			dtStr = "aol"
		} else if leveldb.PrefixDBHandledDataTypes[dt] {
			dtStr = "prefixdb"
		} else {
			dtStr = "pebble"
		}
		if _, ok := stats[dtStr]; !ok {
			stats[dtStr] = make(map[opType]*latencyHistogram)
		}
		if _, ok := stats[dtStr][op]; !ok {
			stats[dtStr][op] = newLatencyHistogram()
		}
		stats[dtStr][op].observe(elapsed)
	}

	fmt.Println("Start replaying baseline trace...")
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("End of file reached")
				break
			}
			log.Printf("error reading trace file: %v", err)
			break
		}

		line = strings.TrimSpace(line)
		lineCounter++

		// 检查是否为区块结束行，遇到则commit batch
		if strings.Contains(line, "Processing block (end)") && strings.Contains(line, "geth:") {
			if stateBatchActive && stateBatch != nil {
				start := time.Now()
				err := db.BatchCommit(stateBatch)
				elapsed := time.Since(start)
				totalTime += elapsed
				recordOp(opBatchPutCommit, batchCommitDataType, elapsed)
				if err != nil {
					fmt.Printf("State batch commit failed at block end: %v\n", err)
				}
				stateBatch = nil
				stateBatchActive = false
			}
			if nonStateBatchActive && nonStateBatch != nil {
				start := time.Now()
				err := db.BatchCommit(nonStateBatch)
				elapsed := time.Since(start)
				totalTime += elapsed
				recordOp(opBatchPutCommit, batchCommitDataType, elapsed)
				if err != nil {
					fmt.Printf("Non-state batch commit failed at block end: %v\n", err)
				}
				nonStateBatch = nil
				nonStateBatchActive = false
			}
			continue
		}

		// 跳过无关行
		if strings.Contains(line, "Global log file opened successfully") || !strings.Contains(line, "OPType:") {
			continue
		}

		matches := opRegex.FindStringSubmatch(line)
		if len(matches) < 2 {
			continue
		}

		opTypeStr := matches[1]
		currentDataType := leveldb.DataType(-1)
		keyHex := ""
		keySize := 0
		keyBytes := []byte{}
		if len(matches) >= 3 && matches[2] != "" {
			keyHex = matches[2]
			if len(matches) >= 4 && matches[3] != "" {
				fmt.Sscanf(matches[3], "%d", &keySize)
			}
			keyBytes, err = hex.DecodeString(keyHex)
			if err != nil {
				log.Printf("line %d: failed to decode key hex %q: %v", lineCounter, keyHex, err)
				continue
			}
			if len(keyBytes) > 0 {
				currentDataType = leveldb.GetDataTypeFromKey(keyBytes)
			}
		}

		var valueHex string
		var valueSize int
		var valueBytes []byte
		if len(matches) >= 6 && matches[4] != "" {
			valueHex = matches[4]
			fmt.Sscanf(matches[5], "%d", &valueSize)
			valueBytes, err = hex.DecodeString(valueHex)
			if err != nil {
				log.Printf("line %d: failed to decode value hex for key %q: %v", lineCounter, keyHex, err)
				continue
			}
			if len(valueBytes) != valueSize {
				fmt.Printf("Warning: Parsed value size %d does not match expected size %d at line %d\n", len(valueBytes), valueSize, lineCounter)
			}
		}

		// var batchSize int
		// if len(matches) >= 7 && matches[6] != "" {
		// 	if v, parseErr := strconv.ParseInt(matches[6], 10, 0); parseErr == nil && v > 0 {
		// 		maxInt := int64(int(^uint(0) >> 1))
		// 		if v > maxInt {
		// 			batchSize = int(maxInt)
		// 		} else {
		// 			batchSize = int(v)
		// 		}
		// 	}
		// }

		var iterPrefixBytes []byte
		// var iterStartBytes []byte
		if len(matches) >= 9 && matches[7] != "" {
			iterPrefixBytes, err = hex.DecodeString(matches[7])
			if err != nil {
				log.Printf("line %d: failed to decode iterator prefix %q: %v", lineCounter, matches[7], err)
				continue
			}
			currentDataType = leveldb.GetDataTypeFromKey(iterPrefixBytes)
			// if matches[8] != "" {
			// 	iterStartBytes, err = hex.DecodeString(matches[8])
			// 	if err != nil {
			// 		continue
			// 	}
			// }
		}

		var op opType
		start := time.Now()
		var opErr error
		shouldRecord := true

		switch opTypeStr {
		case "Get":
			op = opGet
			var value []byte
			value, opErr = db.Get(keyBytes)
			if opErr == nil {
				logicReadSize += int64(len(value))
			}
		case "Has":
			op = opHas
			var value []byte
			value, opErr = db.Get(keyBytes)
			if opErr == nil {
				logicReadSize += int64(len(value))
			}
		case "Put":
			op = opPut
			opErr = db.Put(keyBytes, valueBytes)
			if opErr == nil {
				logicWriteSize += int64(len(keyBytes) + len(valueBytes))
			}
		case "Delete":
			op = opDelete
			opErr = db.Delete(keyBytes)
			if opErr == nil {
				logicWriteSize += int64(len(keyBytes))
			}
		case "NewBatch":
			op = opNewBatch
			if !stateBatchActive {
				stateBatch = db.NewBatch()
				stateBatchActive = true
			}
			if !nonStateBatchActive {
				nonStateBatch = db.NewBatch()
				nonStateBatchActive = true
			}
		case "BatchPut":
			if len(keyBytes) == 0 {
				break
			}
			op = opBatchPut
			if db.useStateForKey(keyBytes) {
				if !stateBatchActive || stateBatch == nil {
					stateBatch = db.NewBatch()
					stateBatchActive = true
				}
				opErr = db.BatchPut(stateBatch, keyBytes, valueBytes)
			} else {
				if !nonStateBatchActive || nonStateBatch == nil {
					nonStateBatch = db.NewBatch()
					nonStateBatchActive = true
				}
				opErr = db.BatchPut(nonStateBatch, keyBytes, valueBytes)
			}
			if opErr == nil {
				logicWriteSize += int64(len(keyBytes) + len(valueBytes))
			}
		case "BatchPutCommit":
			// 不再在这里commit，交由区块结束行处理
			shouldRecord = false
		case "BatchDelete":
			if len(keyBytes) == 0 {
				break
			}
			op = opBatchDelete
			if db.useStateForKey(keyBytes) {
				if !stateBatchActive || stateBatch == nil {
					stateBatch = db.NewBatch()
					stateBatchActive = true
				}
				opErr = db.BatchDelete(stateBatch, keyBytes)
			} else {
				if !nonStateBatchActive || nonStateBatch == nil {
					nonStateBatch = db.NewBatch()
					nonStateBatchActive = true
				}
				opErr = db.BatchDelete(nonStateBatch, keyBytes)
			}
			if opErr == nil {
				logicWriteSize += int64(len(keyBytes))
			}
		case "NewBatchWithSize":
			op = opNewBatchWithSize
			if !stateBatchActive {
				stateBatch = db.NewBatch()
				stateBatchActive = true
			}
			if !nonStateBatchActive {
				nonStateBatch = db.NewBatch()
				nonStateBatchActive = true
			}
		// case "GetBatchValueSize":
		// 	// 忽略
		case "NewIterator":
			op = opNewIterator
			if iterator != nil {
				iterator.Release()
			}
			iterator = db.NewIterator()
			lastIterDataType = currentDataType
		case "IteratorNext":
			currentDataType = lastIterDataType
			op = opIteratorNext
			if iterator == nil {
				opErr = fmt.Errorf("IteratorNext without active iterator at line %d", lineCounter)
			} else {
				var valid bool
				var key []byte
				var value []byte
				key, value, valid = db.IteratorNext(iterator)
				if !valid {
					iterator.Release()
					iterator = nil
				} else {
					logicReadSize += int64(len(key) + len(value))
				}
			}
		default:
			shouldRecord = false
		}

		end := time.Now()
		elapsed := end.Sub(start)
		totalTime += elapsed

		if opErr != nil {
			if len(keyBytes) > 0 && (keyBytes[0] != 'o' && keyBytes[0] != 'a') {
				fmt.Printf("Operation %s failed for key %s: %v\n", opTypeStr, keyHex, opErr)
			}
		}
		counter++
		if shouldRecord {
			recordOp(op, currentDataType, elapsed)
		}
		if counter%10000 == 0 {
			fmt.Printf("\rProcessed %d operations, total time: %f s, logic read size: %d, logic write size: %d", counter, totalTime.Seconds(), logicReadSize, logicWriteSize)
		}
		if maxOps > 0 && counter >= maxOps {
			fmt.Printf("Reached max operations %d, stopping replay.\n", maxOps)
			fmt.Println("logic read size: "+strconv.FormatInt(logicReadSize, 10), ", logic write size: ", strconv.FormatInt(logicWriteSize, 10))
			break
		}
	}
	fmt.Printf("\nFinished replaying trace. Total operations: %d, total time: %f s, logic read size: %d, logic write size: %d\n", counter, totalTime.Seconds(), logicReadSize, logicWriteSize)
	reportLatencyStats(stats)
	return nil

}
func main() {
	configPath := flag.String("config", "replay_config.json", "Path to configuration file")
	mode := flag.String("mode", "re", "Mode of operation: ld (load data), re (replay trace)")
	cache := flag.Int("cache", 256, "Cache size in MB")
	handles := flag.Int("handles", 128, "Number of file handles")
	useState := flag.Bool("state", true, "Use state-specific operations (Put_s/Get_s)")
	stateKeyPrefixes := flag.String("state-key-prefixes", "", "Comma-separated key prefixes to route to Put_s/Get_s")
	loadLimit := flag.Int("limit", 0, "Limit number of entries to load (0 = no limit)")
	// benchmark := flag.Int("bench", 0, "Run benchmark with N operations")
	maxOps := flag.Int64("max-ops", 100*1000*1000, "Max operations to replay, 0 means no limit")
	flag.Parse()

	fmt.Println("ChainKV Workload Replay Tool")
	fmt.Println("=============================")

	// Load configuration
	config, err := loadConfig(*configPath)
	if err != nil {
		log.Printf("Warning: Could not load config: %v", err)
		config = &Config{}
	}

	// Determine database path
	dbDirectory := config.DatabaseDir
	if dbDirectory == "" {
		dbDirectory = config.DatabaseDir
	}
	if dbDirectory == "" {
		log.Fatal("Database path must be specified via -db flag or config file")
	}

	fmt.Printf("Database path: %s\n", dbDirectory)
	fmt.Printf("Cache size: %d MB\n", *cache)
	fmt.Printf("File handles: %d\n", *handles)
	fmt.Printf("State mode: %v\n", *useState)
	fmt.Printf("State key prefixes: %s\n", *stateKeyPrefixes)

	// Open database
	fmt.Println("\nOpening ChainKV database...")
	var prefixes []string
	if strings.TrimSpace(*stateKeyPrefixes) != "" {
		prefixes = strings.Split(*stateKeyPrefixes, ",")
	}

	db, err := NewChainKVLDB(dbDirectory, *cache, *handles, *useState, prefixes)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	fmt.Println("Database opened successfully!")

	switch *mode {
	case "ld":
		fmt.Printf("\nLoading data from: %s\n", config.LoadDataDir)
		if err := loadData(db, config.LoadDataDir, *loadLimit); err != nil {
			log.Fatalf("Failed to load data: %v", err)
		}
	case "re":
		if config.TraceFile == "" {
			log.Fatal("Replay mode requires trace file specified in config")
		}
		replayTrace(db, config.TraceFile, *maxOps)
	default:
		log.Fatalf("Unknown mode: %s. Supported modes are 'ld' and 're'", *mode)
	}

	// Load data if specified
	// if *loadFile != "" {
	// 	fmt.Printf("\nLoading data from: %s\n", *loadFile)
	// 	if err := loadData(db, *loadFile, *loadLimit); err != nil {
	// 		log.Fatalf("Failed to load data: %v", err)
	// 	}
	// }

	// replayTrace(db, config.TraceFile, *maxOps)
	// Run benchmark if specified
	// if *benchmark > 0 {
	// 	benchmarkOperations(db, *benchmark)
	// }

	// // If no operations specified, show usage
	// if *loadFile == "" && *benchmark == 0 {
	// 	fmt.Println("\nNo operations specified. Use -load or -bench flags.")
	// 	fmt.Println("\nUsage examples:")
	// 	fmt.Println("  # Load data from file")
	// 	fmt.Println("  ./replayWorkload -db /path/to/db -load data.txt")
	// 	fmt.Println("\n  # Run benchmark")
	// 	fmt.Println("  ./replayWorkload -db /path/to/db -bench 10000")
	// 	fmt.Println("\n  # Use state-specific operations")
	// 	fmt.Println("  ./replayWorkload -db /path/to/db -state -bench 10000")
	// }

	fmt.Println("\nDone!")
}
