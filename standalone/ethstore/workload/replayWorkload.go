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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	// Please replace "ethstore_module" with the actual module path defined in your ethstore/go.mod file

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
	prefixdb "github.com/tinoryj/EthStore/standalone/ethstore/prefixdb"
)

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

type DBType int

const (
	AOL DBType = iota
	PrefixDB
	Pebble
	allDBTypes
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

func dataTypeName(dt ethstore.DataType) string {
	if name, ok := ethstore.DataTypeStrings[dt]; ok {
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

func reportHistogramSummary(label string, hist *latencyHistogram) {
	if hist == nil || hist.totalCount == 0 {
		fmt.Printf("\n[Latency] %s: no samples\n", label)
		return
	}
	min := time.Duration(hist.minNs)
	max := time.Duration(hist.maxNs)
	fmt.Printf("\n[Latency] %s count=%d avg=%s p50=%s p95=%s p99=%s min=%s max=%s\n",
		label,
		hist.totalCount,
		formatDurationCompact(hist.avg()),
		formatDurationCompact(hist.percentile(50.0)),
		formatDurationCompact(hist.percentile(95.0)),
		formatDurationCompact(hist.percentile(99.0)),
		formatDurationCompact(min),
		formatDurationCompact(max),
	)
}

type replayConfig struct {
	DatabaseDir          string `json:"databaseDir"`
	LoadDataDir          string `json:"loadDataDir"`
	BaselinePebbleDir    string `json:"baselinePebbleDir"`
	TraceFile            string `json:"traceFile"`
	TraceFileNocache     string `json:"traceFileNocache"`
	TraceFileNoCacheSnap string `json:"traceFileNoCacheSnap"`
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

func main() {
	configPath := flag.String("config", "replay_config.json", "Path to replay config JSON")
	mode := flag.String("mode", "re", "Mode of operation: ld (load data), re (replay trace), o (other), lb (load baseline), rb (replay baseline)")
	maxOps := flag.Int64("max-ops", 100*1000*1000, "Max operations to replay, 0 means no limit")
	ldChunkFileSize := flag.Int("ld-chunk-file-size", 0, "Chunk file size for ld mode")
	ldCacheSize := flag.Int("ld-cache-size", 0, "Cache size for ld mode")
	DBTypeStr := flag.String("db-type", "allDBtypes", "Database type for replay: prefixdb, pebble, or aol")
	replayTraceFile := flag.String("trace-file", "Cache", "Path to trace file for recording")
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
		traceFile = cfg.TraceFileNoCacheSnap
	default:
		log.Fatalf("invalid -trace-file %q (expected: cache, nocache, nocache_snap)", *replayTraceFile)
	}

	go func() {
		// Start the HTTP server for pprof profiling
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	// runtime.SetMutexProfileFraction(5)

	// TestPrefixGet()
	// loadPebble()
	// loadAccount(cfg.DatabaseDir, 64*1024, 512*1024*1024)
	// insertAccountHashindexTopebble()
	// repalceAccountHashToAccountKey()

	// recordTraceStorage(traceFile)
	// recordTraceBlock(traceFileNocache)
	// replayTraceAccount(cfg.DatabaseDir, cfg.TraceFile)

	// time.Sleep(5 * time.Second)
	// loadbaselineData(cfg.BaselinePebbleDir, cfg.LoadDataDir)
	// replaybaselineTrace(cfg.BaselinePebbleDir, cfg.TraceFile, *maxOps)
	replayTrace(cfg.DatabaseDir, cfg.TraceFile, *maxOps, allDBTypes)
	// notxFile := "/mnt/ssd/ethstore/database/aol/print_all_output.txt"
	// loadAol(cfg.DatabaseDir, notxFile)
	return

	otherRunner := recordTraceStorage // change here when you need a different 'o' workload

	switch *mode {
	case "ld":
		loadAccount(cfg.DatabaseDir, *ldChunkFileSize, *ldCacheSize)
		// loadData(cfg.DatabaseDir, cfg.LoadDataDir)
		// notxFile := "/mnt/ssd/ethstore/database/aol/print_all_output.txt"
		// loadAol(cfg.DatabaseDir, notxFile, txFile)
	case "re":
		replayTrace(cfg.DatabaseDir, traceFile, *maxOps, dbType)
	case "ra":
		replayTraceAccount(cfg.DatabaseDir, traceFile)
	case "o":
		otherRunner(traceFile)
	case "lb":
		loadbaselineData(cfg.BaselinePebbleDir, cfg.LoadDataDir)
	case "rb":
		replaybaselineTrace(cfg.BaselinePebbleDir, traceFile, *maxOps, dbType)
	default:
		log.Fatalf("unknown mode %q, use ld, re, o, lb, or rb", *mode)
	}
}

func loadbaselineData(pebbleDir string, dataFile string) {
	tempDir := pebbleDir
	store, err := ethstore.NewPebbleStore(tempDir, 0, 0, "", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	testFilePath := dataFile

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
		err = store.Put(keyBytes, valueBytes)
		endTime := time.Now()
		totalTime += endTime.Sub(startTime)
		if err != nil {
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}
		// Verify the value was stored correctly
		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %f s", counter, totalTime.Seconds())
		}
	}
}

func replaybaselineTrace(baselinePebbleDir string, traceFile string, maxOps int64, dbType DBType) {
	fmt.Printf("Replaying baseline trace from file %s using Pebble store at %s\n", traceFile, baselinePebbleDir)
	dir := baselinePebbleDir
	store, err := ethstore.NewPebbleStore(dir, 0, 0, "", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	testFilePath := traceFile

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	// Support both formats:
	// - OPType: Put, key: ..., size: ..., value: ..., size: ...
	// - OPType: BatchPutCommit (commit is deferred until a block end marker)
	// - geth: ... Processing block (end), ID: ..., hash: ... (commit all pending batches)
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
	var nextBatchRequested bool
	var nextBatchSize int
	var pebbleBatch ethdb.Batch
	var stopAtNextBlockEnd bool
	var blockCommitCounter int64
	var lastKeyPrefix byte
	var hasLastKeyPrefix bool
	var lastIterDataType ethstore.DataType
	pebbleCommitHist := newLatencyHistogram()

	ensurePebbleBatch := func() ethdb.Batch {
		if pebbleBatch != nil {
			return pebbleBatch
		}
		if nextBatchRequested {
			if nextBatchSize > 0 {
				pebbleBatch = store.NewBatchWithSize(nextBatchSize)
			} else {
				pebbleBatch = store.NewBatch()
			}
			nextBatchRequested = false
			nextBatchSize = 0
			return pebbleBatch
		}
		pebbleBatch = store.NewBatch()
		return pebbleBatch
	}

	commitBlock := func() error {
		if pebbleBatch == nil {
			blockCommitCounter++
			return nil
		}
		start := time.Now()
		err := pebbleBatch.Write()
		pebbleCommitHist.observe(time.Since(start))
		pebbleBatch = nil
		blockCommitCounter++
		return err
	}

	var dataType ethstore.DataType
	var it ethdb.Iterator
	var stats = make(map[string]map[opType]*latencyHistogram)
	defer func() {
		if it != nil {
			it.Release()
		}
	}()
	fmt.Println("Start replaying baseline trace...")
	for {
		// read line
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

		// Block boundary marker: commit all pending batches when a block ends.
		if strings.Contains(line, "Processing block (end), ID:") {
			if err := commitBlock(); err != nil {
				fmt.Printf("Batch commit on block end failed at trace line %d: %v\n", lineCounter, err)
			} else if stopAtNextBlockEnd {
				fmt.Printf("Reached max operations %d earlier; stopping at next block end (trace line %d).\n", maxOps, lineCounter)
				break
			}
			continue
		}

		// 跳过非操作行
		if strings.Contains(line, "Global log file opened successfully") || !strings.Contains(line, "OPType:") {
			continue
		}

		matches := opRegex.FindStringSubmatch(line)
		if len(matches) < 2 {
			// fmt.Printf("无法解析行: %s\n", line)
			continue
		}

		opTypeStr := matches[1]
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
				// 无效的键，跳过
				continue
			}
			if len(keyBytes) > 0 {
				lastKeyPrefix = keyBytes[0]
				hasLastKeyPrefix = true
				dataType = ethstore.GetDataTypeFromKey(keyBytes)
			}
		}

		// 检查是否有值部分
		var valueHex string
		var valueSize int
		if len(matches) >= 6 && matches[4] != "" {
			valueHex = matches[4]
			fmt.Sscanf(matches[5], "%d", &valueSize)
		}

		// 解析 NewBatchWithSize 中的 batch 大小
		var batchSize int
		if len(matches) >= 7 && matches[6] != "" {
			if v, parseErr := strconv.ParseInt(matches[6], 10, 0); parseErr == nil && v > 0 {
				maxInt := int64(int(^uint(0) >> 1))
				if v > maxInt {
					batchSize = int(maxInt)
				} else {
					batchSize = int(v)
				}
			}
		}

		// 解析 NewIterator 的 prefix/start key（start key 可能为空）
		var iterPrefixBytes []byte
		var iterStartBytes []byte
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
			// fmt.Printf("\rProcessed %d operations, total time: %f s", counter, totalTime.Seconds()
			fmt.Printf("\rProcessed %d operations, total time: %f s, logic read size: %d, logic write size: %d", counter, totalTime.Seconds(), logicReadSize, logicWriteSize)
		}
		// throughput
		if maxOps > 0 && counter >= maxOps {
			if !stopAtNextBlockEnd {
				stopAtNextBlockEnd = true
				fmt.Printf("Reached max operations %d; will continue until next block end then stop.\n", maxOps)
				fmt.Println("logic read size: "+strconv.FormatInt(logicReadSize, 10), ", logic write size: ", strconv.FormatInt(logicWriteSize, 10))
			}
		}

		switch dbType {
		case AOL:
			if !ethstore.AolHandledDataTypes[dataType] {
				continue
			}
		case PrefixDB:
			if !ethstore.PrefixDBHandledDataTypes[dataType] {
				continue
			}
		case Pebble:
			if ethstore.AolHandledDataTypes[dataType] || ethstore.PrefixDBHandledDataTypes[dataType] {
				continue
			}
		case allDBTypes:
		default:
		}

		var op opType
		switch opTypeStr {
		case "Get":
			op = opGet
		case "Has":
			op = opHas
		case "Put":
			op = opPut
		case "BatchPut":
			op = opBatchPut
		case "Delete":
			op = opDelete
		case "NewBatch":
			op = opNewBatch
		case "NewBatchWithSize":
			op = opNewBatchWithSize
		case "GetBatchValueSize":
			op = opGetBatchValueSize
		case "BatchPutCommit":
			op = opBatchPutCommit
		case "BatchDelete":
			op = opBatchDelete
		case "NewIterator":
			op = opNewIterator
		case "IteratorNext":
			dataType = lastIterDataType
			op = opIteratorNext
		default:
			// 未知操作，跳过
			fmt.Printf("Unknown operation '%s' at line %d\n", opTypeStr, counter)
			continue
		}
		valueBytes, decodeErr := hex.DecodeString(valueHex)
		if decodeErr != nil && valueHex != "" {
			// 无效的值，跳过
			continue
		}

		// 执行操作并计时
		var value []byte
		var size int
		start := time.Now()
		var opErr error

		switch op {
		case opGet:
			value, opErr = store.Get(keyBytes)
			size = len(value)
		case opHas:
			size, _, opErr = store.Has(keyBytes)
		case opPut:
			opErr = store.Put(keyBytes, valueBytes)
		case opDelete:
			opErr = store.Delete(keyBytes)
		case opNewBatch:
			nextBatchRequested = true
			nextBatchSize = 0
		case opBatchPut:
			if len(keyBytes) == 0 {
				break
			}
			b := ensurePebbleBatch()
			opErr = b.Put(keyBytes, valueBytes)
		case opBatchPutCommit:
			// Commit is deferred until we observe a block end marker.
			// (See "Processing block (end)" handling above.)
			opErr = nil
		case opBatchDelete:
			if len(keyBytes) == 0 {
				break
			}
			b := ensurePebbleBatch()
			opErr = b.Delete(keyBytes)
		case opNewBatchWithSize:
			nextBatchRequested = true
			if batchSize > 0 {
				nextBatchSize = batchSize
			} else {
				fmt.Printf("Invalid batch size %d at line %d, using default NewBatch\n", batchSize, counter)
				nextBatchSize = 0
			}
		case opGetBatchValueSize:
			if hasLastKeyPrefix {
				_ = lastKeyPrefix
			}
			if pebbleBatch != nil {
				size = pebbleBatch.ValueSize()
			}
		case opNewIterator:
			if it != nil {
				it.Release()
				it = nil
			}
			it = store.NewIterator(iterPrefixBytes, iterStartBytes)
		case opIteratorNext:
			if it != nil {
				_ = it.Next()
			}
		}

		end := time.Now()
		elapsed := end.Sub(start)
		totalTime += elapsed

		var kvTypeStr string
		if ethstore.AolHandledDataTypes[dataType] {
			kvTypeStr = "AOL"
		} else if ethstore.PrefixDBHandledDataTypes[dataType] {
			kvTypeStr = "PrefixDB"
		} else {
			kvTypeStr = "Pebble"
		}

		if opErr != nil {
			if keyBytes[0] != 'o' && keyBytes[0] != 'a' {
				fmt.Printf("Operation %s failed for key %s: %v\n", opTypeStr, keyHex, opErr)
			}
		}

		// 在 switch 外部统一处理统计
		if opErr == nil {
			switch op {
			case opGet, opHas:
				logicReadSize += int64(size)
			case opPut, opBatchPut:
				logicWriteSize += int64(len(keyBytes) + len(valueBytes))
			case opDelete, opBatchDelete:
				logicWriteSize += int64(len(keyBytes))
			}
		}

		if _, ok := stats[kvTypeStr]; !ok {
			stats[kvTypeStr] = make(map[opType]*latencyHistogram)
		}
		if _, ok := stats[kvTypeStr][op]; !ok {
			stats[kvTypeStr][op] = newLatencyHistogram()
		}
		stats[kvTypeStr][op].observe(elapsed)

	}
	if err := commitBlock(); err != nil {
		fmt.Printf("Final batch commit failed: %v\n", err)
	}
	reportLatencyStats(stats)
	reportHistogramSummary("baseline-pebble commit (Batch.Write)", pebbleCommitHist)
}

// load all data from the key-value file into EthStore
func loadData(dataBaseDir string, dataFile string) {
	// ethStoreDir := "/mnt/ssd/ethstore/database/prefixdb"
	ethStoreDir := dataBaseDir
	store, err := ethstore.New(ethStoreDir, 1000, "put_test", false, 64*1024, 512*1024*1024)
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

func loadAccount(databaseDir string, chunkFileSize int, cacheSize int) {
	var dir string
	chunkFileSizeStr := strconv.Itoa(chunkFileSize/1024) + "KB"

	dir = databaseDir + "/database_statedb" + chunkFileSizeStr

	// dir = databaseDir + "/database_state"

	pdb, err := prefixdb.NewPrefixDB(dir, chunkFileSize, uint64(cacheSize))
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer pdb.Close()

	dbPath := "/mnt/ssd2/pebble"
	ps, err := ethstore.NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		fmt.Printf("Failed to create PebbleStore instance: %v\n", err)
		return
	}

	testFilePath := "/mnt/ssd/ethstore/20500000_key_value_pairs.txt"

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
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

		counter++

		// if counter < 375799415 {
		// 	continue
		// }

		// if counter < 560917527 {
		// 	continue
		// }

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
			log.Fatalf("Failed to decode key: %v", err)
		}
		valueBytes, err = hex.DecodeString(string(valueBytes))
		if err != nil {
			log.Fatalf("Failed to decode value: %v", err)
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
	fmt.Printf("\nTotal Put operations: %d, Total time: %f s\n", counter, totalTime.Seconds())
}

func replaySSPut() {
	// tempDir := "/mnt/ssd/ethstore/database/prefixdb"
	dirPath := "/mnt/ssd/ethstore/database"
	pdb, err := prefixdb.NewPrefixDB(dirPath, 64*1024, 3538944)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer pdb.Close()

	testFilePath := "/mnt/ssd/ethstore/20500000_key_value_pairs.txt"

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	var totalTime time.Duration
	counter := 0
	reader := bufio.NewReader(file)
	// isstore := false

	for {
		counter++
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break // End of file reached
		}

		if counter < 1967893668 {
			continue // Skip the first 1967893668 lines
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

		if keyBytes[0] != 'a' && keyBytes[0] != 'o' {
			continue
		}

		// if !isstore && keyBytes[0] == 'o' {
		// 	pdb.SaveTrie()
		// 	isstore = true
		// }

		var accountKey []byte
		if keyBytes[0] == 'o' {
			accountKey = pdb.GetParentAccountKey(keyBytes)
		}

		// Perform the Put operation
		startTime := time.Now()
		err = pdb.Put(keyBytes, valueBytes, accountKey)
		endTime := time.Now()
		totalTime += endTime.Sub(startTime)
		if err != nil {
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}
		// Verify the value was stored correctly

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %d ns", counter, totalTime.Nanoseconds())
		}

	}
}

func loadAol(dataBaseDir string, notxFile string) {

	store, err := ethstore.New(dataBaseDir, 6000, "put_test", false, 16*1024, 12*1024*1024)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()
	fmt.Println("Start aol put test...")

	// Read key-value pairs from the test file
	notxfile, err := os.Open(notxFile)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
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
			log.Fatalf("Failed to decode key: %v", err)
		}
		valueBytes, err = hex.DecodeString(string(valueBytes))
		if err != nil {
			log.Fatalf("Failed to decode value: %v", err)
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
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}
		// Verify the value was stored correctly

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %d ns", counter, totalTime.Nanoseconds())
		}
	}

	log.Printf("Total Put operations: %d, Total time: %d ns", counter, totalTime.Nanoseconds())
	log.Println("Put test completed.")
	// store.CloseAol()
}

func TestPrefixGet() {
	dirPath := "/mnt/ssd2/ethstore/database_state"
	pd, err := prefixdb.NewPrefixDB(dirPath, 64*1024, 3538944)
	if err != nil {
		log.Fatalf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()

	keyhex := "4f63d618e1fc15bd7e9d3579f3f8f7e186b02be609aa45700e5ca81cd8c52c945200000e0200"

	pdkey, err := hex.DecodeString(keyhex)
	if err != nil {
		log.Fatalf("Failed to decode key hex: %v", err)
	}

	var accountKey []byte
	if pdkey[0] == 'O' {
		accountKey = pd.GetParentAccountKey(pdkey)
	}

	startTime := time.Now()
	value, ok, err := pd.Get(pdkey, accountKey)
	endTime := time.Now()
	if err != nil {
		log.Fatalf("Get operation failed: %v", err)
	}
	if !ok {
		log.Fatalf("Key not found in PrefixDB")
	}
	fmt.Printf("Value: %x\n", value)
	fmt.Printf("Get operation took %f seconds\n", endTime.Sub(startTime).Seconds())
}

func testPdbPerformance() {
	tempDir := "/mnt/ssd/ethstore/testDB"
	store, err := ethstore.New(tempDir, 10, "put_test", false, 64*1024, 512*1024*1024)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	testFilePath := "/mnt/ssd/ethstore/20500000_key_value_pairs.txt"

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
		err = store.Put(keyBytes, valueBytes)
		endTime := time.Now()
		totalTime += endTime.Sub(startTime)
		if err != nil {
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}
		// Verify the value was stored correctly

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %f s", counter, totalTime.Seconds())
		}
	}
}

func testAolPreformance() {
	tempDir := "/mnt/ssd/ethstore/testDB"
	store, err := ethstore.New(tempDir, 100, "put_test", false, 64*1024, 512*1024*1024)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	notxFile := "/mnt/ssd/ethstore/sortAol/nontxlookup_sorted.dat"

	// Read key-value pairs from the test file
	notxfile, err := os.Open(notxFile)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer notxfile.Close()

	var totalTime time.Duration
	counter := 0
	notxreader := bufio.NewReader(notxfile)

	for {

		line, err := notxreader.ReadString('\n')
		if err == io.EOF {
			break // End of file reached
		}

		// line format: "key: xxxxxx, value: yyyy"
		line = line[:len(line)-1] // Remove the newline character

		parts := strings.Split(line, ", Value:")
		if len(parts) != 2 {
			// log.Printf("无法解析行: %s", line)
			continue
		}
		counter++
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
		err = store.Put(keyBytes, valueBytes)
		endTime := time.Now()
		totalTime += endTime.Sub(startTime)
		if err != nil {
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}
		// Verify the value was stored correctly

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %f ns", counter, totalTime.Seconds())
		}
	}

	fmt.Println("total put:", counter, " use time:", totalTime.Seconds(), " s", " avg time:", float64(counter)/totalTime.Seconds(), " s")
	log.Println("Put test completed.")
}

func TestPebblePreformance() {
	tempDir := "/mnt/ssd/ethstore/testDB/pebble"
	store, err := ethstore.NewPebbleStore(tempDir, 0, 0, "TestPebblePut", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	testFilePath := "/mnt/ssd/ethstore/20500000_key_value_pairs.txt"

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

		startTime := time.Now()
		err = store.Put(keyBytes, valueBytes)
		endTime := time.Now()
		totalTime += endTime.Sub(startTime)
		if err != nil {
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d, use time: %f s", counter, totalTime.Seconds())
		}
	}
}

func TestGetParentKey() {
	dirpath := "/mnt/ssd/ethstore/database"
	pd, err := prefixdb.NewPrefixDB(dirpath, 64*1024, 3538944)
	if err != nil {
		fmt.Printf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()

	// SK_1 := []byte("610000019759ea326fa019a55bda5dff44477be6e1d9c48db950e3fe07a0ba671e")
	// SV_1 := []byte("f8440180a0665081a76be9ad792eec7ba0b7819e48a97cd6ab5210cae849c1ea4777ba9b6aa029164acf9a06c22bbe9da20100d94116c6ef93f44a5b58ebd6e1954c3bf436df")
	// SK_1, err = hex.DecodeString(string(SK_1))
	// SV_1, err = hex.DecodeString(string(SV_1))

	// pd.Put(SK_1, SV_1)

	Key1 := []byte("4f37d65eaa92c6bc4c13a5ec45527f0c18ea8932588728769ec7aecfe6d9f32e42")
	Value1 := []byte("f91111111")

	parentKey1 := pd.GetParentAccountKey(Key1)

	Key1, err = hex.DecodeString(string(Key1))
	Value1, err = hex.DecodeString(string(Value1))
	pd.Put(Key1, Value1, parentKey1)
	fmt.Print("Parent Key1: ", hex.EncodeToString(parentKey1), "\n")
	if !bytes.Equal(parentKey1, Key1[:len(Key1)-2]) {
		fmt.Printf("Expected parent key for Key1 to be %x, got %x\n", Key1[:len(Key1)-2], parentKey1)
	} else {
		fmt.Println("Parent key test passed.")
	}
}

func replayTrace(dataBaseDir string, traceFileDir string, maxOps int64, dbType DBType) {
	tempDir := dataBaseDir
	store, err := ethstore.New(tempDir, 6000, "put_test", false, 16*1024, 12*1024*1024)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	// store.GCPrefixTreeStorage()

	// dbPath := "/mnt/ssd2/pebble"

	// ps, err := ethstore.NewPebbleStore(dbPath, 0, 0, "", false)
	// if err != nil {
	// 	fmt.Printf("Failed to create PebbleStore instance: %v\n", err)
	// 	return
	// }

	testFilePath := traceFileDir

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	// Support both formats:
	// - OPType: Put, key: ..., size: ..., value: ..., size: ...
	// - OPType: BatchPutCommit (commit is deferred until a block end marker)
	// - geth: ... Processing block (end), ID: ..., hash: ... (commit all pending batches)
	// - OPType: NewBatchWithSize, size: <batch-bytes>
	// - OPType: NewIterator, prefix: <hex>, start key: <hex or empty>
	opRegex := regexp.MustCompile(`OPType:\s*(\w+)(?:,\s*key:\s*([0-9a-fA-F]+),\s*size:\s*(\d+)(?:,\s*value:\s*([0-9a-fA-F]+),\s*size:\s*(\d+))?)?(?:,\s*size:\s*(\d+))?(?:,\s*prefix:\s*([0-9a-fA-F]+),\s*start key:\s*([0-9a-fA-F]*))?`)

	var totalTime time.Duration
	var counter int64
	var linecount int64
	reader := bufio.NewReader(file)
	stats := make(map[string]map[opType]*latencyHistogram)

	var nextBatchRequested bool
	var nextBatchSize int
	var pebbleBatch ethdb.Batch
	var prefixdbDirty bool
	var stopAtNextBlockEnd bool
	var blockCommitCounter int64
	var lastIterDataType ethstore.DataType
	prefixdbCommitHist := newLatencyHistogram()
	pebbleCommitHist := newLatencyHistogram()
	blockCommitTotalHist := newLatencyHistogram()

	ensurePebbleBatch := func() ethdb.Batch {
		if pebbleBatch != nil {
			return pebbleBatch
		}
		if nextBatchRequested {
			if nextBatchSize > 0 {
				pebbleBatch = store.NewBatchWithSize(nextBatchSize)
			} else {
				pebbleBatch = store.NewBatch()
			}
			nextBatchRequested = false
			nextBatchSize = 0
			return pebbleBatch
		}
		pebbleBatch = store.NewBatch()
		return pebbleBatch
	}

	commitBlock := func() error {
		// One block-end commit consists of at most two commits:
		// 1) PrefixDB (TrieStorage) batch commit
		// 2) Pebble batch write
		blockStart := time.Now()
		if prefixdbDirty {
			start := time.Now()
			if err := store.PrefixdbBatchCommit('O'); err != nil {
				return err
			}
			prefixdbCommitHist.observe(time.Since(start))
			prefixdbDirty = false
		}
		if pebbleBatch != nil {
			start := time.Now()
			if err := pebbleBatch.Write(); err != nil {
				return err
			}
			pebbleCommitHist.observe(time.Since(start))
			pebbleBatch = nil
		}
		blockCommitTotalHist.observe(time.Since(blockStart))
		blockCommitCounter++
		return nil
	}

	var iter ethdb.Iterator
	defer func() {
		if iter != nil {
			iter.Release()
		}
	}()

	fmt.Println("start replay")
	for {
		// read line
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
		linecount++
		// if linecount < 954178 {
		// 	continue
		// }

		// Block boundary marker: commit all pending batches when a block ends.
		if strings.Contains(line, "Processing block (end), ID:") {
			if err := commitBlock(); err != nil {
				fmt.Printf("Batch commit on block end failed: %v\n", err)
				break
			} else if stopAtNextBlockEnd {
				fmt.Printf("Reached max operations %d earlier; stopping at next block end.\n", maxOps)
				break
			}
			continue
		}

		// 跳过非操作行
		if strings.Contains(line, "Global log file opened successfully") ||
			!strings.Contains(line, "OPType:") {
			continue
		}

		matches := opRegex.FindStringSubmatch(line)
		if len(matches) == 0 {
			continue
		}

		opTypeStr := matches[1]
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
				// 无效的键，跳过
				continue
			}
			// if len(keyBytes) > 0 && keyBytes[0] == 'O' {
			// 	accountKey := store.GetParentAccountKey(keyBytes)
			// 	if accountKey == nil {
			// 		Key, _, err := findKeyValuePair(keyHex[2:66], ps)
			// 		if Key == "" || err != nil {
			// 			fmt.Printf("Failed to get parent account key for key %s\n", keyHex)
			// 			continue
			// 		}
			// 		accountKey = []byte(Key)
			// 		store.InsertAccountHashPebble(accountKey, keyBytes[1:33])
			// 	}
			// 	if err := store.SetAccountKey(accountKey); err != nil {
			// 		fmt.Printf("SetAccountKey failed for key %s: %v\n", keyHex, err)
			// 		break
			// 	}
			// }
		}
		// 检查是否有值部分
		var valueHex string
		var valueSize int
		var valueBytes []byte
		if len(matches) >= 6 && matches[4] != "" {
			valueHex = matches[4]
			fmt.Sscanf(matches[5], "%d", &valueSize)
			valueBytes, err = hex.DecodeString(valueHex)
			if err != nil && valueHex != "" {
				continue
			}
		}

		var batchSize int
		if len(matches) >= 7 && matches[6] != "" {
			if v, parseErr := strconv.ParseInt(matches[6], 10, 0); parseErr == nil && v > 0 {
				maxInt := int64(int(^uint(0) >> 1))
				if v > maxInt {
					batchSize = int(maxInt)
				} else {
					batchSize = int(v)
				}
			}
		}
		dataType := ethstore.GetDataTypeFromKey(keyBytes)
		var iterPrefixBytes []byte
		var iterStartBytes []byte
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

		if len(keyBytes) == 0 && len(iterPrefixBytes) > 0 {
			dataType = ethstore.GetDataTypeFromKey(iterPrefixBytes)
		}

		counter++
		if counter%10000 == 0 {
			fmt.Printf("\rProcessed %d operations, total time: %f s, blockCommits=%d, dirty(prefixdb=%v, pebble=%v)",
				counter, totalTime.Seconds(), blockCommitCounter, prefixdbDirty, pebbleBatch != nil)
		}

		if maxOps > 0 && counter >= maxOps {
			if !stopAtNextBlockEnd {
				stopAtNextBlockEnd = true
				fmt.Printf("Reached max operations %d; will continue until next block end then stop.\n", maxOps)
			}
		}

		switch dbType {
		case AOL:
			if !ethstore.AolHandledDataTypes[dataType] {
				continue
			}
		case PrefixDB:
			if !ethstore.PrefixDBHandledDataTypes[dataType] {
				continue
			}
		case Pebble:
			if ethstore.AolHandledDataTypes[dataType] || ethstore.PrefixDBHandledDataTypes[dataType] {
				continue
			}
		case allDBTypes:
		default:
		}

		// testKeyHex := "4f4e881af4e10baec755ed1eff3857514735d4e6e34f188bd6afdc48f4b70933830e040c"
		// testKey, _ := hex.DecodeString(testKeyHex)
		// val, err := store.Get(testKey)
		// if err != nil {
		// 	fmt.Printf("Error getting test key %s: %v\n", testKeyHex, err)
		// } else {
		// 	fmt.Printf("Successfully got test key %s, value: %x\n", testKeyHex, val)
		// }

		var op opType
		switch opTypeStr {
		case "Get":
			op = opGet
		case "Has":
			op = opHas
		case "Put":
			op = opPut
		case "Delete", "BatchDelete":
			if opTypeStr == "BatchDelete" {
				op = opBatchDelete
			} else {
				op = opDelete
			}
		case "BatchPut":
			op = opBatchPut
		case "BatchPutCommit":
			op = opBatchPutCommit
		case "NewBatch":
			op = opNewBatch
		case "NewBatchWithSize":
			op = opNewBatchWithSize
		case "GetBatchValueSize":
			op = opGetBatchValueSize
		case "NewIterator":
			op = opNewIterator
		case "IteratorNext":
			dataType = lastIterDataType
			op = opIteratorNext

		default:
			// 未知操作，跳过
			fmt.Printf("Unknown operation '%s' at line %d\n", opTypeStr, counter)
			continue
		}

		if op == opBatchPut {
			if ethstore.AolHandledDataTypes[dataType] || (len(keyBytes) > 0 && keyBytes[0] == 'A') {
				op = opPut
			}
		}
		// 执行操作并计时
		start := time.Now()
		var opErr error
		switch op {
		case opGet:
			_, opErr = store.Get(keyBytes)
		case opHas:
			_, opErr = store.Has(keyBytes)
		case opPut:
			opErr = store.Put(keyBytes, valueBytes)
		case opDelete:
			opErr = store.Delete(keyBytes)
		case opBatchPut:
			if len(keyBytes) == 0 {
				break
			}
			if keyBytes[0] == 'O' {
				opErr = store.BatchPut(keyBytes, valueBytes)
				if opErr == nil {
					prefixdbDirty = true
				}
			} else {
				b := ensurePebbleBatch()
				opErr = b.Put(keyBytes, valueBytes)
			}
		case opBatchDelete:
			if len(keyBytes) == 0 {
				break
			}
			if keyBytes[0] == 'O' {
				// NOTE: There is no PrefixDB batch-delete API here; keep existing behavior.
				opErr = store.Delete(keyBytes)
			} else {
				b := ensurePebbleBatch()
				opErr = b.Delete(keyBytes)
			}
		case opBatchPutCommit:
			// Commit is deferred until we observe a block end marker.
			// (See "Processing block (end)" handling above.)
			opErr = nil
		case opNewBatch:
			nextBatchRequested = true
			nextBatchSize = 0
		case opNewBatchWithSize:
			nextBatchRequested = true
			if batchSize > 0 {
				nextBatchSize = batchSize
			} else {
				nextBatchSize = 0
			}
		case opGetBatchValueSize:
			if pebbleBatch != nil {
				_ = pebbleBatch.ValueSize()
			}
		case opNewIterator:
			if iter != nil {
				iter.Release()
				iter = nil
			}
			// EthStore's iterator routing may involve non-Pebble backends (e.g., AOL) and PrefixDB has no iterator.
			// For replayTrace we only exercise Pebble iterator behavior; skip TrieStorage (PrefixDB) prefixes.
			if len(iterPrefixBytes) > 0 && (iterPrefixBytes[0] == 'O') {
				iter = nil
				break
			}
			iter = store.NewIterator(iterPrefixBytes, iterStartBytes)
		case opIteratorNext:
			if iter != nil {
				_ = iter.Next()
			}
		}

		end := time.Now()
		elapsed := end.Sub(start)
		totalTime += elapsed

		var kvTypeStr string
		if ethstore.AolHandledDataTypes[dataType] {
			kvTypeStr = "AOL"
		} else if ethstore.PrefixDBHandledDataTypes[dataType] {
			kvTypeStr = "PrefixDB"
		} else {
			kvTypeStr = "Pebble"
		}

		if _, ok := stats[kvTypeStr]; !ok {
			stats[kvTypeStr] = make(map[opType]*latencyHistogram)
		}
		if _, ok := stats[kvTypeStr][op]; !ok {
			stats[kvTypeStr][op] = newLatencyHistogram()
		}
		stats[kvTypeStr][op].observe(elapsed)
		if opErr != nil {
			//fmt.Printf("Operation %s failed : %v\n", opTypeStr, opErr)
			if keyBytes[0] != 'o' && keyBytes[0] != 'a' {
				fmt.Printf("linecount: %d Operation %s failed for key %s: %v\n", linecount, opTypeStr, keyHex, opErr)
			}
		}
	}
	if err := commitBlock(); err != nil {
		fmt.Printf("Final batch commit failed: %v\n", err)
	}

	fmt.Printf("\nFinished replaying trace file '%s'. Total operations: %d, total time: %f s\n", traceFileDir, counter, totalTime.Seconds())
	fmt.Println("\nReporting latency statistics...")
	reportLatencyStats(stats)
	reportHistogramSummary("replayTrace commit (PrefixdbBatchCommit)", prefixdbCommitHist)
	reportHistogramSummary("replayTrace commit (pebble Batch.Write)", pebbleCommitHist)
	reportHistogramSummary("replayTrace commit (block total)", blockCommitTotalHist)
}

func insertAccountHashindexTopebble() error {
	// insert all kvs in hashKeyPebble into memCache
	fmt.Println("Building memcache from pebble store...")
	pebblePath := "/mnt/ramdisk/accountHash_key_pebble"

	accountHashKeyPebble, err := ethstore.NewPebbleStore(pebblePath, 0, 0, "", false)
	if err != nil {
		return fmt.Errorf("failed to open pebble store: %v", err)
	}

	dir := "/mnt/ssd2/ethstore/database_pebble"
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

	if accountHashKeyPebble != nil {
		if err := accountHashKeyPebble.Close(); err != nil {
			log.Printf("failed to close pebble store: %v", err)
		}
	}

	if db != nil {
		if err := db.Close(); err != nil {
			log.Printf("failed to close pebble store: %v", err)
		}
	}

	return nil
}

func replayPebble() {
	tempDir := "/mnt/tmp/block-20500000-backup/execution/data/geth/chaindata"
	store, err := ethstore.NewPebbleStore(tempDir, 0, 0, "", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	testFilePath := "/mnt/ssd/ethstore/20500000_key_value_pairs.txt"

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

func recordTraceStorage(traceFileDir string) {
	file, err := os.Open(traceFileDir)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	baseDir := "traceNoCache"
	if strings.Contains(traceFileDir, "withcache") {
		baseDir = "traceCache"
	}
	fmt.Println("Start record trace storage data...", baseDir)

	// 定义操作类型常量
	const (
		OpGet    = 0
		OpPut    = 1
		OpDelete = 2
	)

	createBufferedCSV := func(name string) (*os.File, *bufio.Writer) {
		path := "/mnt/ssd2/ethstore/motivationdata/" + baseDir + "_" + name + ".csv"
		f, err := os.Create(path)
		if err != nil {
			log.Fatalf("Failed to create file: %v", err)
		}
		writer := bufio.NewWriter(f)
		writer.WriteString("linecount,Key,count\n")
		return f, writer
	}

	// TrieNodeAccount
	fTAg, wTAg := createBufferedCSV("trieNodeAccount_get")
	fTAp, wTAp := createBufferedCSV("trieNodeAccount_put")
	fTAd, wTAd := createBufferedCSV("trieNodeAccount_delete")
	defer func() {
		wTAg.Flush()
		wTAp.Flush()
		wTAd.Flush()
		fTAg.Close()
		fTAp.Close()
		fTAd.Close()
	}()

	// Storage
	fStorageg, wStorageg := createBufferedCSV("storage_get")
	fStoragep, wStoragep := createBufferedCSV("storage_put")
	fStoraged, wStoraged := createBufferedCSV("storage_delete")
	defer func() {
		wStorageg.Flush()
		wStoragep.Flush()
		wStoraged.Flush()
		fStorageg.Close()
		fStoragep.Close()
		fStoraged.Close()
	}()

	// SnapshotAccount
	fSAg, wSAg := createBufferedCSV("snapshotAccount_get")
	fSAp, wSAp := createBufferedCSV("snapshotAccount_put")
	fSAd, wSAd := createBufferedCSV("snapshotAccount_delete")
	defer func() {
		wSAg.Flush()
		wSAp.Flush()
		wSAd.Flush()
		fSAg.Close()
		fSAp.Close()
		fSAd.Close()
	}()

	// SnapshotStorage
	fSSg, wSSg := createBufferedCSV("snapshotStorage_get")
	fSSp, wSSp := createBufferedCSV("snapshotStorage_put")
	fSSd, wSSd := createBufferedCSV("snapshotStorage_delete")
	defer func() {
		wSSg.Flush()
		wSSp.Flush()
		wSSd.Flush()
		fSSg.Close()
		fSSp.Close()
		fSSd.Close()
	}()

	// 辅助选择函数
	getWriter := func(op int, g, p, d *bufio.Writer) *bufio.Writer {
		switch op {
		case OpPut:
			return p
		case OpDelete:
			return d
		default:
			return g
		}
	}

	// 状态变量
	var oldStorageHash string
	var oldStorageOp int
	var storageCount int
	var storageStartLine int

	var oldSnapshotHash string
	var oldSnapshotOp int
	var snapshotCount int
	var snapshotStartLine int

	var optypeSet = make(map[string]struct{})

	linecount := 0
	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		linecount++

		if !strings.Contains(line, "OPType: ") {
			continue
		}

		opIdx := strings.Index(line, "OPType: ")
		opPart := line[opIdx+8:]
		commaIdx := strings.Index(opPart, ",")
		if commaIdx == -1 {
			continue
		}
		opTypeStr := opPart[:commaIdx]

		if _, exists := optypeSet[opTypeStr]; !exists {
			optypeSet[opTypeStr] = struct{}{}
			// fmt.Printf("Found operation type: %s\n", opTypeStr)
		}

		var currentOp int
		switch opTypeStr {
		case "Put", "BatchPut":
			currentOp = OpPut
		case "Delete", "BatchDelete":
			currentOp = OpDelete
		case "Get", "Has", "NewIterator", "IteratorNext":
			currentOp = OpGet
		default:
			continue
		}
		var keyHex string
		// 迭代器可能使用 "prefix: "，普通操作使用 "key: "
		if opTypeStr == "NewIterator" {
			if pIdx := strings.Index(line, "prefix: "); pIdx != -1 {
				keyPart := line[pIdx+8:]
				keyHex = keyPart[:strings.Index(keyPart, ",")]
			}
		} else {
			if kIdx := strings.Index(line, "key: "); kIdx != -1 {
				keyPart := line[kIdx+5:]
				keyHex = keyPart[:strings.Index(keyPart, ",")]
			}
		}
		keyBytes, _ := hex.DecodeString(keyHex[:2])

		switch keyBytes[0] {
		case 'A': // TrieNodeAccount
			w := getWriter(currentOp, wTAg, wTAp, wTAd)
			w.WriteString(fmt.Sprintf("%d,%s,1\n", linecount, keyHex))

		case 'O': // Storage (MPT)
			if len(keyHex) < 66 {
				continue
			}
			storageHash := keyHex[2:66]
			if oldStorageHash == storageHash && oldStorageOp == currentOp {
				storageCount++
			} else {
				if oldStorageHash != "" {
					w := getWriter(oldStorageOp, wStorageg, wStoragep, wStoraged)
					w.WriteString(fmt.Sprintf("%d,%s,%d\n", storageStartLine, oldStorageHash, storageCount))
				}
				storageCount = 1
				storageStartLine = linecount
				oldStorageHash = storageHash
				oldStorageOp = currentOp
			}

		case 'a': // SnapshotAccount
			if len(keyHex) < 66 {
				fmt.Println(line)
				continue
			}
			w := getWriter(currentOp, wSAg, wSAp, wSAd)
			w.WriteString(fmt.Sprintf("%d,%s,1\n", linecount, keyHex[2:66]))

		case 'o': // SnapshotStorage
			if len(keyHex) < 66 {
				continue
			}
			snapshotHash := keyHex[2:66]
			if oldSnapshotHash == snapshotHash && oldSnapshotOp == currentOp {
				snapshotCount++
			} else {
				if oldSnapshotHash != "" {
					w := getWriter(oldSnapshotOp, wSSg, wSSp, wSSd)
					w.WriteString(fmt.Sprintf("%d,%s,%d\n", snapshotStartLine, oldSnapshotHash, snapshotCount))
				}
				snapshotCount = 1
				snapshotStartLine = linecount
				oldSnapshotHash = snapshotHash
				oldSnapshotOp = currentOp
			}
		}
	}
	if oldStorageHash != "" {
		w := getWriter(oldStorageOp, wStorageg, wStoragep, wStoraged)
		w.WriteString(fmt.Sprintf("%d,%s,%d\n", storageStartLine, oldStorageHash, storageCount))
	}

	if oldSnapshotHash != "" {
		w := getWriter(oldSnapshotOp, wSSg, wSSp, wSSd)
		w.WriteString(fmt.Sprintf("%d,%s,%d\n", snapshotStartLine, oldSnapshotHash, snapshotCount))
	}
	for opType := range optypeSet {
		fmt.Printf("Detected operation type: %s\n", opType)
	}
	fmt.Println("Trace storage data recording completed.")
}

func recordTraceBlock(traceFileDir string) {
	fmt.Println("Start recode trace block data...")
	testFilePath := traceFileDir

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	var baseDir string
	if strings.Contains(traceFileDir, "withcache") {
		baseDir = "traceWithCache"
	} else {
		baseDir = "traceNoCache"
	}

	opRegex := regexp.MustCompile(`OPType: (\w+), key: ([0-9a-fA-F]+), size: (\d+)(?:, value: ([0-9a-fA-F]+), size: (\d+))?`)
	reader := bufio.NewReader(file)

	outFileGet, err := os.Create("../log/" + baseDir + "_get_tx_ops.csv")
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outFileGet.Close()
	fmt.Fprintf(outFileGet, "opType,block_data_key,lastPutID,opID,distance,dataType\n")

	outFilePut, err := os.Create("../log/" + baseDir + "_put_tx_ops.csv")
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outFilePut.Close()
	fmt.Fprintf(outFilePut, "block_data_key,distance\n")

	//newBlockID := uint64(20500009)
	// newBlockID := uint64(20499865) //noCache
	newBlockID := uint64(20499568) // Cache
	for {
		// read line
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("End of file reached")
				break
			}
			log.Printf("error reading trace file: %v", err)
			break
		}

		line = strings.TrimSpace(line)

		// 跳过非操作行
		if strings.Contains(line, "Global log file opened successfully") ||
			!strings.Contains(line, "OPType:") {
			continue
		}

		matches := opRegex.FindStringSubmatch(line)
		if len(matches) < 4 {
			// 无法解析的行，跳过
			continue
		}

		opTypeStr := matches[1]
		keyHex := matches[2]
		keySize := 0
		fmt.Sscanf(matches[3], "%d", &keySize)

		// 检查是否有值部分
		var valueHex string
		var valueSize int
		if len(matches) >= 6 && matches[4] != "" {
			valueHex = matches[4]
			fmt.Sscanf(matches[5], "%d", &valueSize)
		}

		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			// 无效的键，跳过
			continue
		}

		valueBytes, err := hex.DecodeString(valueHex)
		if err != nil && valueHex != "" {
			// 无效的值，跳过
			continue
		}

		dataType := ethstore.GetDataTypeFromKey(keyBytes)
		if ethstore.AolHandledDataTypes[dataType] {
			if dataType != ethstore.TransactionLookupMetadataDataType {
				continue
			} else {
				var blockID uint64
				var ok bool
				blockID, ok = ethstore.ParseBlockNumberFromKey(keyBytes, dataType)
				if ok != true && len(valueBytes) > 0 {
					blockID, ok = ethstore.ParseBlockNumberFromValue(valueBytes, dataType)
					if err != nil {
						continue
					}
				} else if ok != true && blockID == 0 {
					continue
				}
				switch opTypeStr {
				case "Delete", "BatchDelete":
					distance := int64(newBlockID - blockID)
					fmt.Fprintf(outFileGet, "%s,%s,%d,%d,%d,%s\n", opTypeStr, keyHex, newBlockID, blockID, distance, ethstore.DataTypeStrings[dataType])
				case "Put", "BatchPut":
					if blockID > newBlockID {
						newBlockID = blockID
						// fmt.Fprintf(outFilePut, "%d, ", blockID)
					} else if blockID < newBlockID {
						// fmt.Fprintf(outFilePut, "put old block")
						// fmt.Println("put old block")
					}

				}
			}
		}

	}

}

func replayTraceAccount(dataBaseDir string, traceFileDir string) {
	tempDir := dataBaseDir + "_state"
	store, err := prefixdb.NewPrefixDB(tempDir, 16*1024, 12*1024*1024)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	dbPath := "/mnt/ssd2/pebble"
	ps, err := ethstore.NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		fmt.Printf("Failed to create PebbleStore instance: %v\n", err)
		return
	}

	testFilePath := traceFileDir

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	// Support both formats:
	// - OPType: Put, key: ..., size: ..., value: ..., size: ...
	// - OPType: BatchPutCommit
	opRegex := regexp.MustCompile(`OPType:\s*(\w+)(?:,\s*key:\s*([0-9a-fA-F]+),\s*size:\s*(\d+)(?:,\s*value:\s*([0-9a-fA-F]+),\s*size:\s*(\d+))?)?`)

	var totalTime time.Duration
	counter := 0
	reader := bufio.NewReader(file)

	commitBlock := func() error {
		store.BatchCommit()
		return nil
	}
	var memStats runtime.MemStats

	// var oldop string
	// var oldPre string
	var lineCount int64
	// store.GCPrefixTree()
	// store.GCAllStorageChunkFiles()
	// return
	fmt.Println("start replay")
	for {
		// read line
		line, err := reader.ReadString('\n')
		eof := false
		if err != nil {
			if err == io.EOF {
				// last line without trailing '\n'
				eof = true
				if len(line) == 0 {
					fmt.Println("End of file reached")
					break
				}
			} else {
				fmt.Printf("error reading trace file: %v\n", err)
				continue
			}
		}
		lineCount++

		line = strings.TrimSpace(line)

		// 跳过非操作行
		if strings.Contains(line, "Global log file opened successfully") ||
			!strings.Contains(line, "OPType:") {
			if eof {
				fmt.Println("End of file reached")
				break
			}
			continue
		}

		// Block boundary marker: commit all pending batches when a block ends.
		if strings.Contains(line, "Processing block (end), ID:") {
			if err := commitBlock(); err != nil {
				fmt.Printf("Batch commit on block end failed at trace line %d: %v\n", lineCount, err)
			}
			continue
		}

		matches := opRegex.FindStringSubmatch(line)
		if len(matches) == 0 {
			// 无法解析的行，跳过
			if eof {
				fmt.Println("End of file reached")
				break
			}
			continue
		}

		opTypeStr := matches[1]
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
				// 无效的键，跳过
				continue
			}
			if len(keyHex) > 0 && (keyBytes[0] == 'O' || keyBytes[0] == 'A') {
			} else {
				continue
			}
		}

		// if (oldPre != "4f" && opTypeStr == "BatchPutCommit") || opTypeStr == "NewBatch" || (oldPre == "4f" && oldop == "Get" && opTypeStr == "BatchPutCommit") {
		// 	continue
		// }

		// 检查是否有值部分
		// var valueHex string
		// var valueSize int

		// keyBytes[0] == 'O' storagekvs
		// keyBytes[0] == 'A' accountkvs
		// keyBytes[0] == 'a' accountsnapshotkvs
		// keyBytes[0] == 'o' storagesnapshotkvs
		// keyBytes[0] == 'c' codekvs

		// if oldop != opTypeStr {
		// 	if oldop == "Get" && opTypeStr != "Get" {
		// 		fmt.Println("op changed to "+"Put"+" count: "+fmt.Sprintf("%d", counter)+" use time: ", totalTime.Seconds(), "s")
		// 	} else if oldop != "Get" && opTypeStr == "Get" {
		// 		err := store.BatchCommit()
		// 		if err != nil {
		// 			fmt.Printf("BatchCommit failed: %v\n", err)
		// 		} else {
		// 			fmt.Printf("BatchCommit success at line %d, total time: %f s\n", lineCount, totalTime.Seconds())
		// 		}
		// 		fmt.Println("op changed to "+opTypeStr+" count: "+fmt.Sprintf("%d", counter)+" use time: ", totalTime.Seconds(), "s")
		// 	}
		// 	oldop = opTypeStr

		// }

		var accountKey []byte
		if len(keyBytes) > 0 && keyBytes[0] == 'O' {
			if accountKey = store.GetParentAccountKey(keyBytes); accountKey == nil {
				Key, _, err := findKeyValuePair(keyHex[2:66], ps)
				if Key == "" || err != nil {
					fmt.Printf("Failed to get parent account key for key %s\n", keyHex)
					continue
				}
				accountKey, err = hex.DecodeString(Key)
				if err != nil {
					fmt.Printf("Failed to decode parent account key for key %s: %v\n", keyHex, err)
					continue
				}
				store.InsertAccountHashPebble(keyBytes[1:33], accountKey)
			}
		}

		var valueBytes []byte
		if len(matches) >= 6 && matches[4] != "" {
			valueHex := matches[4]
			valueBytes, err = hex.DecodeString(valueHex)
			if err != nil {
				// 无效的值，跳过
				continue
			}
		}

		// if opTypeStr == "Get" {

		// } else {
		// 	continue
		// }
		// Perform the operation
		testKeyhex := "4105020b03010e000d"
		testKeyBytes, _ := hex.DecodeString(testKeyhex)
		testAccountKey := store.GetParentAccountKey(testKeyBytes)
		val, yes, err := store.Get(testKeyBytes, testAccountKey)
		if err != nil {
			fmt.Printf("Test Get operation failed for key %s: %v\n", testKeyhex, err)
		} else if !yes {
			fmt.Printf("Test Get operation: key %s not found\n", testKeyhex)
		} else {
			fmt.Printf("Test Get operation succeeded for key %s, value: %x\n", testKeyhex, val)
		}

		startTime := time.Now()
		var opErr error
		var ok bool
		switch opTypeStr {
		case "Get":
			_, ok, opErr = store.Get(keyBytes, accountKey)
		case "Has":
			_, opErr = store.Has(keyBytes, accountKey)
		case "Put":
			opErr = store.Put(keyBytes, valueBytes, accountKey)
		case "BatchPut":
			if keyBytes[0] == 'O' {
				opErr = store.BatchPut(keyBytes, valueBytes, accountKey)
			} else {
				opErr = store.Put(keyBytes, valueBytes, accountKey)
			}
		// case "BatchPutCommit":
		// 	opErr = store.BatchCommit()
		case "Delete", "BatchDelete":
			opErr = store.Delete(keyBytes, accountKey)
		default:
			// 未知操作，跳过
			// fmt.Printf("Unknown operation '%s' at line %d\n", opTypeStr, counter)
			continue
		}

		endTime := time.Now()
		totalTime += endTime.Sub(startTime)
		if !ok && (opTypeStr == "Get" || opTypeStr == "Has") {
			fmt.Printf("Get operation: key %s not found\n", keyHex)
		}
		if opErr != nil {
			fmt.Printf("Operation %s failed : %v\n", opTypeStr, opErr)
			// fmt.Printf("Operation %s failed for key %s: %v\n", opTypeStr, keyHex, opErr)
		}
		counter++
		if counter%10000 == 0 {
			fmt.Printf("\rProcessed %d operations, total time: %f s", counter, totalTime.Seconds())
			runtime.ReadMemStats(&memStats)
			fmt.Printf("GC 次数: %d", memStats.NumGC)
		}

		if eof {
			fmt.Println("End of file reached")
			break
		}
	}
}

func loadPebble() {
	fmt.Println("Start load pebble...")
	// tempDir := "/mnt/ssd/ethstore/database/prefixdb"
	dirPath := "/mnt/ssd2/ethstore/database_pebble"
	pdb, err := ethstore.NewPebbleStore(dirPath, 0, 0, "pebble_load", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer pdb.Close()

	testFilePath := "/mnt/ssd/ethstore/20500000_key_value_pairs.txt"

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

func repalceAccountHashToAccountKey() {
	fmt.Println("Start replace account hash to account key...")
	PebblePath := "/mnt/ssd/ethstore/index/accountHash_key_pebble"
	store, err := ethstore.NewPebbleStore(PebblePath, 0, 0, "replace_accounthash_to_accountkey", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	dbPath := "/mnt/ssd2/pebble"

	ps, err := ethstore.NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		fmt.Printf("Failed to create PebbleStore instance: %v\n", err)
		return
	}
	defer ps.Close()

	inputFilePath := "/mnt/ssd2/ethstore/motivationdata/26_2_7/traceCache_snapshotStorage_get_ave.csv"

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatalf("Failed to open input file: %v", err)
	}
	defer inputFile.Close()

	outputFilePath := "/mnt/ssd2/ethstore/motivationdata/26_2_7/traceCache_snapshotStorage_get_ave_accountKey.csv"

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outputFile.Close()

	reader := bufio.NewReader(inputFile)
	// file: accountHash,avg,other...
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}

		//skip first line
		if strings.HasPrefix(line, "Key") {
			_, err = outputFile.WriteString(line)
			if err != nil {
				log.Fatalf("Failed to write to output file: %v", err)
			}
			continue
		}
		accountHashHex := strings.Split(line, ",")[0]
		accountHash, err := hex.DecodeString(accountHashHex)
		if err != nil {
			log.Fatalf("Failed to decode account hash: %v", err)
		}
		key, err := store.Get(accountHash)
		var accountKey []byte
		if err != nil {
			Key, _, err := findKeyValuePair(accountHashHex, ps)
			if err != nil {
				accountKey = accountHash
			} else {
				accountKey, err = hex.DecodeString(Key)
				if err != nil {
					log.Fatalf("Failed to decode account key from DB key: %v", err)
				}
				// store into pebble
				err = store.Put(accountHash, accountKey)
				if err != nil {
					log.Fatalf("Failed to put account hash and key into pebble: %v", err)
				}
			}
		} else {
			accountKey = key
		}
		newLine := strings.Replace(line, accountHashHex, hex.EncodeToString(accountKey), 1)
		_, err = outputFile.WriteString(newLine)
		if err != nil {
			log.Fatalf("Failed to write to output file: %v", err)
		}
	}
}

func recordAccount(traceFileDir string) {
	fmt.Println("Start replay trace store Account test...")

	file, err := os.Open(traceFileDir)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	baseDir := "traceNoCache"
	if strings.Contains(traceFileDir, "withcache") {
		baseDir = "traceCache"
	}

	createBufferedCSV := func(name string) (*os.File, *bufio.Writer) {
		f, err := os.Create("../log/" + baseDir + "_" + name + ".csv")
		if err != nil {
			log.Fatalf("Failed to create file: %v", err)
		}
		writer := bufio.NewWriter(f)
		writer.WriteString("accountKey\n")
		return f, writer
	}

	fGet, wGet := createBufferedCSV("account_get_ops")
	fPut, wPut := createBufferedCSV("account_put_ops")

	defer func() {
		wGet.Flush()
		wPut.Flush()
		fGet.Close()
		fPut.Close()
	}()

	reader := bufio.NewReader(file)
	linecount := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading at line %d: %v\n", linecount, err)
			break
		}
		linecount++

		if !strings.Contains(line, "OPType: ") {
			continue
		}

		opIdx := strings.Index(line, "OPType: ")
		keyIdx := strings.Index(line, "key: ")
		if opIdx == -1 || keyIdx == -1 {
			continue
		}

		// 解析 OPType (截取到逗号)
		opPart := line[opIdx+8:]
		opEnd := strings.Index(opPart, ",")
		if opEnd == -1 {
			continue
		}
		opTypeStr := opPart[:opEnd]

		// 解析 Key (截取到逗号)
		keyPart := line[keyIdx+5:]
		keyEnd := strings.Index(keyPart, ",")
		var keyHex string
		if keyEnd == -1 {

			keyHex = strings.TrimSpace(keyPart)
		} else {
			keyHex = keyPart[:keyEnd]
		}

		if len(keyHex) < 2 || keyHex[0] != '4' || keyHex[1] != '1' {
			continue
		}

		var target *bufio.Writer
		switch opTypeStr {
		case "Has", "Get", "Delete", "BatchDelete":
			target = wGet
		case "Put", "BatchPut":
			target = wPut
		default:
			continue
		}

		target.WriteString(keyHex)
		target.WriteByte('\n')
	}

	fmt.Printf("Finished. Total lines processed: %d\n", linecount)
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
