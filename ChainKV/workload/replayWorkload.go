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

	chainkvdb "github.com/tinoryj/EthStore/ChainKV/leveldb/ethdb"

	// Please replace "ethstore_module" with the actual module path defined in your ethstore/go.mod file

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/ethereum/go-ethereum/rlp"
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
)

var opTypeNames = map[opType]string{
	opGet:          "Get",
	opHas:          "Has",
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

// kvStore abstracts all KV operations we need.
type kvStore interface {
	Get([]byte) ([]byte, error)
	Put([]byte, []byte) error
	Delete([]byte) error
	Has([]byte) (int, bool, error) // returns size, found, error (matching PebbleStore)
	Close() error
}

// pebbleStoreAdapter wraps PebbleStore to satisfy kvStore.
type pebbleStoreAdapter struct {
	store *ethstore.PebbleStore
}

func (p *pebbleStoreAdapter) Get(key []byte) ([]byte, error) {
	return p.store.Get(key)
}

func (p *pebbleStoreAdapter) Put(key []byte, value []byte) error {
	return p.store.Put(key, value)
}

func (p *pebbleStoreAdapter) Delete(key []byte) error {
	return p.store.Delete(key)
}

func (p *pebbleStoreAdapter) Has(key []byte) (int, bool, error) {
	return p.store.Has(key)
}

func (p *pebbleStoreAdapter) Close() error {
	return p.store.Close()
}

// chainKVLDB wraps ChainKV's LDBDatabase to satisfy kvStore.
type chainKVLDB struct {
	db       *chainkvdb.LDBDatabase
	useState bool // if true, use Put_s/Get_s for state data; else use Put/Get
}

func (c *chainKVLDB) Get(key []byte) ([]byte, error) {
	if c.useState {
		return c.db.Get_s(key)
	}
	return c.db.Get(key)
}

func (c *chainKVLDB) Put(key []byte, value []byte) error {
	if c.useState {
		return c.db.Put_s(key, value)
	}
	return c.db.Put(key, value)
}

func (c *chainKVLDB) Delete(key []byte) error {
	return c.db.Delete(key)
}

func (c *chainKVLDB) Has(key []byte) (int, bool, error) {
	val, err := c.Get(key)
	if err != nil {
		return 0, false, err
	}
	return len(val), true, nil
}

func (c *chainKVLDB) Close() error {
	c.db.Close()
	return nil
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

func reportLatencyStats(stats map[ethstore.DataType]map[opType]*latencyHistogram) {
	if len(stats) == 0 {
		return
	}
	dataTypes := make([]ethstore.DataType, 0, len(stats))
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
				dataTypeName(dt),
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

func main() {
	configPath := flag.String("config", "replay_config.json", "Path to replay config JSON")
	go func() {
		// Start the HTTP server for pprof profiling
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	cfg, err := loadReplayConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config %s: %v", *configPath, err)
	}

	// TestPrefixGet()
	// loadPebble()
	// loadAccount()
	// repalceAccountHashToAccountKey()

	// recordTraceStorage(traceFile)
	// recordAccount(traceFileNocache)
	// recordTraceBlock(traceFileNocache)
	replayTraceAccount(cfg.DatabaseDir, cfg.TraceFile)
	// replayTrace(databaseDir, traceFile)

	// time.Sleep(5 * time.Second)
	// loadbaselineData(cfg.BaselinePebbleDir, cfg.LoadDataDir)
	// replaybaselineTrace(cfg.BaselinePebbleDir, cfg.TraceFile, 6000000)
	// replayTrace(databaseDir, traceFileNocache)
	return
	mode := flag.String("mode", "re", "Mode of operation: ld (load data), re (replay trace), o (other), lb (load baseline), rb (replay baseline)")
	maxOps := flag.Int64("max-ops", 100*1000*1000, "Max operations to replay, 0 means no limit")
	flag.Parse()

	otherRunner := recordTraceStorage // change here when you need a different 'o' workload

	switch *mode {
	case "ld":
		loadData(cfg.DatabaseDir, cfg.LoadDataDir)
		// notxFile := "/mnt/ssd/ethstore/sortAol/nontxlookup_sorted.dat"
		// txFile := "/mnt/ssd/ethstore/sortAol/txlookup_sorted.dat"
		// loadAol(cfg.DatabaseDir, notxFile, txFile)
	case "re":
		replayTrace(cfg.DatabaseDir, cfg.TraceFile, *maxOps)
		replayTrace(cfg.DatabaseDir, cfg.TraceFileNocache, *maxOps)
	case "o":
		otherRunner(cfg.TraceFile)
	case "lb":
		loadbaselineData(cfg.BaselinePebbleDir, cfg.LoadDataDir)
	case "rb":
		replaybaselineTrace(cfg.BaselinePebbleDir, cfg.TraceFile, *maxOps)
	default:
		log.Fatalf("unknown mode %q, use ld, re, o, lb, or rb", *mode)
	}
}

func loadbaselineData(pebbleDir string, dataFile string) {
	tempDir := pebbleDir
	useChainKV := os.Getenv("USE_CHAINKV") == "1"
	if override := os.Getenv("CHAINKV_DB_PATH"); override != "" {
		tempDir = override
	}
	store, err := openStore(tempDir, useChainKV, false)
	if err != nil {
		log.Fatalf("Failed to create store instance: %v", err)
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

func replaybaselineTrace(baselinePebbleDir string, traceFile string, maxOps int64) {
	dir := baselinePebbleDir
	useChainKV := os.Getenv("USE_CHAINKV") == "1"
	if override := os.Getenv("CHAINKV_DB_PATH"); override != "" {
		dir = override
	}
	store, err := openStore(dir, useChainKV, false)
	if err != nil {
		log.Fatalf("Failed to create store instance: %v", err)
	}
	defer store.Close()

	testFilePath := traceFile

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	opRegex := regexp.MustCompile(`OPType: (\w+), key: ([0-9a-fA-F]+), size: (\d+)(?:, value: ([0-9a-fA-F]+), size: (\d+))?`)

	var totalTime time.Duration
	var counter int64
	reader := bufio.NewReader(file)

	var logicReadSize int64 = 0
	var logicWriteSize int64 = 0

	var oldop string

	fmt.Println("Start replaying baseline trace...")
	for {
		// read line
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("End of file reached")
			}
			fmt.Errorf("error reading trace file: %v", err)
		}

		line = strings.TrimSpace(line)

		// 跳过非操作行
		if strings.Contains(line, "Global log file opened successfully") || !strings.Contains(line, "OPType:") {
			continue
		}

		matches := opRegex.FindStringSubmatch(line)
		if len(matches) < 4 {
			// fmt.Printf("无法解析行: %s\n", line)
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

		if keyBytes[0] != 'O' {
			continue
		}

		if oldop != opTypeStr {
			if oldop == "BatchPut" && opTypeStr == "BatchDelete" {

			} else {
				oldop = opTypeStr
				fmt.Println("\nop changed to " + opTypeStr)
			}
		}

		var op opType
		switch opTypeStr {
		case "Get":
			op = opGet
		case "Has":
			op = opHas
		case "Put", "BatchPut":
			op = opPut
		case "Delete", "BatchDelete":
			op = opDelete
		case "NewBatch":
			op = opNewBatch
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
		}

		end := time.Now()
		totalTime += end.Sub(start)

		if opErr != nil {
			fmt.Printf("Operation %s failed for key %s: %v\n", opTypeStr, keyHex, opErr)
		}
		counter++
		switch op {
		case opGet, opHas:
			logicReadSize += int64(size)
		case opPut:
			logicWriteSize += int64(keySize) + int64(valueSize)
		case opDelete:
			logicWriteSize += int64(keySize)
		}

		if counter%10000 == 0 {
			// fmt.Printf("\rProcessed %d operations, total time: %f s", counter, totalTime.Seconds())
			fmt.Printf("\rProcessed %d operations, total time: %f s, logic read size: %d, logic write size: %d", counter, totalTime.Seconds(), logicReadSize, logicWriteSize)
		}
		if maxOps > 0 && counter >= maxOps {
			fmt.Printf("Reached max operations %d, stopping replay.\n", maxOps)
			fmt.Println("logic read size: "+strconv.FormatInt(logicReadSize, 10), ", logic write size: ", strconv.FormatInt(logicWriteSize, 10))
			break
		}
	}

}

// load all data from the key-value file into EthStore
func loadData(dataBaseDir string, dataFile string) {
	// ethStoreDir := "/mnt/ssd/ethstore/database/prefixdb"
	ethStoreDir := dataBaseDir
	store, err := ethstore.New(ethStoreDir, 1000, "put_test", false, true)
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

		if keyBytes[0] == 'a' && keyBytes[0] == 'o' {
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

func loadAccount() {
	tempDir := "/mnt/ssd2/ethstore/database_state"
	//dirPath := "/mnt/ssd2/ethstore/database_snapshot"
	pdb, err := prefixdb.NewPrefixDB(tempDir, prefixdb.StateDB)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer pdb.Close()

	dbPath := "/mnt/ssd2/pebble"
	useChainKV := os.Getenv("USE_CHAINKV") == "1"
	if override := os.Getenv("CHAINKV_DB_PATH"); override != "" {
		dbPath = override
	}
	ps, err := openLookupStore(dbPath, useChainKV)
	if err != nil {
		fmt.Printf("Failed to create lookup store instance: %v\n", err)
		return
	}
	defer ps.Close()
	// spdb, err := prefixdb.NewPrefixDB(dirPath)
	// if err != nil {
	// 	log.Fatalf("Failed to create EthStore instance: %v", err)
	// }
	// defer spdb.Close()

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

		switch keyBytes[0] {
		case 'A', 'O':
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

			//value, ok, err := pdb.Get(keyBytes)

			// kvcount, size, err := pdb.GetStorageCount(keyBytes)
			// fmt.Printf("%s, %d, %d\n", keyPart, kvcount, size)

			endTime := time.Now()
			totalTime += endTime.Sub(startTime)

			// if err != nil {
			// 	// err = pdb.Put(keyBytes, valueBytes)
			// 	fmt.Printf("Get operation failed for key %s: %v ", keyPart, err)
			// 	continue
			// }
			// if !ok {
			// 	fmt.Printf("Key %s not found in PrefixDB ", keyPart)
			// 	continue
			// }
			// if !bytes.Equal(value, valueBytes) {
			// 	fmt.Println("counter:", counter)
			// 	// log.Printf("Value mismatch for key %s: expected %x, got %x", keyPart, valueBytes, value)
			// }

			if err != nil {
				log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
			}

			if counter%100000 == 0 {
				fmt.Printf("\rPut test: %d, use time: %f s", counter, totalTime.Seconds())
			}
			// case 'a', 'o':
			// 	// Perform the Put operation
			// 	startTime := time.Now()
			// 	err = spdb.Put(keyBytes, valueBytes)
			// 	endTime := time.Now()
			// 	totalTime += endTime.Sub(startTime)
			// 	if err != nil {
			// 		log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
			// 	}
			// 	// Verify the value was stored correctly

			// 	if counter%100000 == 0 {
			// 		fmt.Printf("\rPut test: %d, use time: %d ns", counter, totalTime.Nanoseconds())
			// 	}
		}

	}

	// test get from the beginning of the file
	// read the file again
	// file.Seek(0, io.SeekStart)

	// pdb.SaveTrie()
	fmt.Printf("\nTotal Put operations: %d, Total time: %f s\n", counter, totalTime.Seconds())
}

func replaySSPut() {
	// tempDir := "/mnt/ssd/ethstore/database/prefixdb"
	dirPath := "/mnt/ssd/ethstore/database"
	pdb, err := prefixdb.NewPrefixDB(dirPath, prefixdb.SnapshotDB)
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

func loadAol(dataBaseDir string, notxFile string, txFile string) {

	store, err := ethstore.New(dataBaseDir, 200, "put_test", false, true)
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

	txfile, err := os.Open(txFile)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer txfile.Close()

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

	txreader := bufio.NewReader(txfile)
	counter = 0
	for {

		line, err := txreader.ReadString('\n')
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
			fmt.Printf("\rPut test: %d, use time: %d ns", counter, totalTime.Nanoseconds())
		}
	}
	log.Printf("Total Put operations: %d, Total time: %d ns", counter, totalTime.Nanoseconds())
	log.Println("Put test completed.")
	// store.CloseAol()
}

func TestPrefixGet() {
	dirPath := "/mnt/ssd2/ethstore/database_state"
	pd, err := prefixdb.NewPrefixDB(dirPath, prefixdb.StateDB)
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
	store, err := ethstore.New(tempDir, 10, "put_test", false, true)
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
	store, err := ethstore.New(tempDir, 100, "put_test", false, true)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	notxFile := "/mnt/ssd/ethstore/sortAol/nontxlookup_sorted.dat"
	txFile := "/mnt/ssd/ethstore/sortAol/txlookup_sorted.dat"

	// Read key-value pairs from the test file
	notxfile, err := os.Open(notxFile)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer notxfile.Close()

	txfile, err := os.Open(txFile)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer txfile.Close()

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

	txreader := bufio.NewReader(txfile)
	counter = 0
	for {
		line, err := txreader.ReadString('\n')
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

		if counter%100000 < 10 {
			fmt.Printf("\rPut test: %d, use time: %f s", counter, totalTime.Seconds())
		}
	}
	log.Printf("Total Put operations: %d, Total time: %f s", counter, totalTime.Seconds())
	log.Println("Put test completed.")
}

func TestPebblePreformance() {
	tempDir := "/mnt/ssd/ethstore/testDB/pebble"
	useChainKV := os.Getenv("USE_CHAINKV") == "1"
	if override := os.Getenv("CHAINKV_DB_PATH"); override != "" {
		tempDir = override
	}
	store, err := openStore(tempDir, useChainKV, false)
	if err != nil {
		log.Fatalf("Failed to create store instance: %v", err)
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
	pd, err := prefixdb.NewPrefixDB(dirpath, prefixdb.StateDB)
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

func replayTrace(dataBaseDir string, traceFileDir string, maxOps int64) {
	tempDir := dataBaseDir
	store, err := ethstore.New(tempDir, 8000, "put_test", false, true)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	dbPath := "/mnt/ssd2/pebble"
	useChainKV := os.Getenv("USE_CHAINKV") == "1"
	if override := os.Getenv("CHAINKV_DB_PATH"); override != "" {
		dbPath = override
	}
	ps, err := openLookupStore(dbPath, useChainKV)
	if err != nil {
		fmt.Printf("Failed to create lookup store instance: %v\n", err)
		return
	}
	defer ps.Close()

	testFilePath := traceFileDir

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	opRegex := regexp.MustCompile(`OPType: (\w+), key: ([0-9a-fA-F]+), size: (\d+)(?:, value: ([0-9a-fA-F]+), size: (\d+))?`)

	var totalTime time.Duration
	var counter int64
	reader := bufio.NewReader(file)
	stats := make(map[ethstore.DataType]map[opType]*latencyHistogram)

	var oldIterationKey []byte
	IterationCount := 0

	fmt.Println("start replay")
	for {
		// read line
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("End of file reached")
			}
			fmt.Errorf("error reading trace file: %v", err)
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

		dataType := ethstore.GetDataTypeFromKey(keyBytes)
		if !ethstore.AolHandledDataTypes[dataType] {
			continue
		}

		// keyBytes[0] == 'O' storagekvs
		// keyBytes[0] == 'A' accountkvs
		// keyBytes[0] == 'a' accountsnapshotkvs
		// keyBytes[0] == 'o' storagesnapshotkvs
		// keyBytes[0] == 'c' codekvs

		if keyBytes[0] == 'O' {
			accountKey := store.GetParentAccountKey(keyBytes)
			if accountKey == nil {
				Key, _, err := findKeyValuePair(keyHex[2:66], ps)
				if Key == "" || err != nil {
					fmt.Printf("Failed to get parent account key for key %s\n", keyHex)
					continue
				}
				accountKey = []byte(Key)
				store.InsertAccountHashPebble(accountKey, keyBytes[1:33])
			}
			if err := store.SetAccountKey(accountKey); err != nil {
				fmt.Printf("SetAccountKey failed for key %s: %v\n", keyHex, err)
				break
			}
		}

		var op opType
		doStoreOp := true
		switch opTypeStr {
		case "Get":
			op = opGet
		case "Has":
			op = opHas
		case "Put", "BatchPut":
			op = opPut
		case "Delete", "BatchDelete":
			op = opDelete
		case "NewIterator":
			oldIterationKey = keyBytes
			IterationCount = 0
			op = opNewIterator
			doStoreOp = false
		case "IteratorNext":
			if oldIterationKey == nil {
				fmt.Printf("IteratorNext without NewIterator at line %d\n", counter)
				continue
			}
			IterationCount++
			op = opIteratorNext
			doStoreOp = false
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
		start := time.Now()
		var opErr error
		if doStoreOp {
			switch op {
			case opGet:
				_, opErr = store.Get(keyBytes)
			case opHas:
				_, opErr = store.Has(keyBytes)
			case opPut:
				opErr = store.Put(keyBytes, valueBytes)
			case opDelete:
				opErr = store.Delete(keyBytes)
			}
		}
		end := time.Now()
		elapsed := end.Sub(start)
		totalTime += elapsed
		if _, ok := stats[dataType]; !ok {
			stats[dataType] = make(map[opType]*latencyHistogram)
		}
		if _, ok := stats[dataType][op]; !ok {
			stats[dataType][op] = newLatencyHistogram()
		}
		stats[dataType][op].observe(elapsed)
		if opErr != nil {
			//fmt.Printf("Operation %s failed : %v\n", opTypeStr, opErr)
			fmt.Printf("Operation %s failed for key %s: %v\n", opTypeStr, keyHex, opErr)
		}
		counter++
		if counter%10000 == 0 {
			fmt.Printf("\rProcessed %d operations, total time: %f s", counter, totalTime.Seconds())
		}

		if maxOps > 0 && counter >= maxOps {
			fmt.Printf("Reached max operations %d, stopping replay.\n", maxOps)
			break
		}

	}

	fmt.Println("\nReplay completed. Reporting latency statistics...")
	reportLatencyStats(stats)
}

func buildMemCache() error {
	// insert all kvs in hashKeyPebble into memCache
	fmt.Println("Building memcache from pebble store...")
	pebblePath := "/mnt/ssd/ethstore/index/accountHash_key_pebble"

	accountHashKeyPebble, err := ethstore.NewPebbleStore(pebblePath, 0, 0, "", false)
	if err != nil {
		return fmt.Errorf("failed to open pebble store: %v", err)
	}

	mc := memcache.New("127.0.0.1:11211")
	iter, err := accountHashKeyPebble.GetIterator()
	if err != nil {
		return fmt.Errorf("failed to get iterator from pebble store: %v", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		err = mc.Set(&memcache.Item{Key: hex.EncodeToString(key), Value: value, Expiration: 0})
		if err != nil {
			return fmt.Errorf("failed to set item in memcache: %v", err)
		}

		// test get item from memcache
		item, err := mc.Get(hex.EncodeToString(key))
		if err != nil {
			return fmt.Errorf("failed to get item from memcache: %v", err)
		}
		if !bytes.Equal(item.Value, value) {
			return fmt.Errorf("value mismatch for key %s: expected %x, got %x", hex.EncodeToString(key), value, item.Value)
		}
	}
	fmt.Println("Memcache build complete.")

	if accountHashKeyPebble != nil {
		if err := accountHashKeyPebble.Close(); err != nil {
			fmt.Errorf("failed to close pebble store: %v", err)
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
			fmt.Errorf("error reading trace file: %v", err)
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
	tempDir := dataBaseDir
	store, err := prefixdb.NewPrefixDB(tempDir, prefixdb.StateDB)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	dbPath := "/mnt/ssd2/pebble"
	useChainKV := os.Getenv("USE_CHAINKV") == "1"
	if override := os.Getenv("CHAINKV_LOOKUP_PATH"); override != "" {
		dbPath = override
	}
	ps, err := openLookupStore(dbPath, useChainKV)
	if err != nil {
		fmt.Printf("Failed to create lookup store instance: %v\n", err)
		return
	}
	defer ps.Close()

	testFilePath := traceFileDir

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	opRegex := regexp.MustCompile(`OPType: (\w+), key: ([0-9a-fA-F]+), size: (\d+)(?:, value: ([0-9a-fA-F]+), size: (\d+))?`)

	var totalTime time.Duration
	counter := 0
	reader := bufio.NewReader(file)

	var memStats runtime.MemStats

	var oldop string
	// store.GCPrefixTree()
	store.UpgradeSegmentIndexFiles()
	fmt.Println("start replay")
	for {
		// read line
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("End of file reached")
			}
			fmt.Errorf("error reading trace file: %v", err)
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
		// var valueHex string
		// var valueSize int
		keyBytes, err := hex.DecodeString(keyHex)
		if err != nil {
			// 无效的键，跳过
			continue
		}
		// keyBytes[0] == 'O' storagekvs
		// keyBytes[0] == 'A' accountkvs
		// keyBytes[0] == 'a' accountsnapshotkvs
		// keyBytes[0] == 'o' storagesnapshotkvs
		// keyBytes[0] == 'c' codekvs
		if keyBytes[0] == 'O' {
		} else {
			continue
		}

		var accountKey []byte
		if keyBytes[0] == 'O' {
			if accountKey = store.GetParentAccountKey(keyBytes); accountKey == nil {
				Key, _, err := findKeyValuePair(keyHex[2:66], ps)
				if Key == "" || err != nil {
					fmt.Printf("Failed to get parent account key for key %s\n", keyHex)
					continue
				}
				accountKey = []byte(Key)
				store.InsertAccountHashPebble(accountKey, keyBytes[1:33])
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

		if oldop != opTypeStr {
			if oldop == "BatchPut" && opTypeStr == "BatchDelete" {

			} else {
				oldop = opTypeStr
				fmt.Println("\nop changed to " + opTypeStr)
			}
		}

		// Perform the operation
		startTime := time.Now()
		var opErr error
		var ok bool
		switch opTypeStr {
		case "Get":
			_, ok, opErr = store.Get(keyBytes, accountKey)
		case "Has":
			_, opErr = store.Has(keyBytes, accountKey)
		case "Put", "BatchPut":
			opErr = store.Put(keyBytes, valueBytes, accountKey)
		case "Delete", "BatchDelete":
			opErr = store.Delete(keyBytes, accountKey)
		default:
			// 未知操作，跳过
			fmt.Printf("Unknown operation '%s' at line %d\n", opTypeStr, counter)
			continue
		}

		endTime := time.Now()
		totalTime += endTime.Sub(startTime)
		if !ok && opTypeStr == "Get" {
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
	}
}

func loadPebble() {
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
		if !ethstore.AolHandledDataTypes[DataType] && !ethstore.PrefixDBHandledDataTypes[DataType] && !ethstore.SSPrefixdbHandledDataTypes[DataType] {
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
	useChainKV := os.Getenv("USE_CHAINKV") == "1"
	if override := os.Getenv("CHAINKV_DB_PATH"); override != "" {
		PebblePath = override
	}
	store, err := openStore(PebblePath, useChainKV, false)
	if err != nil {
		log.Fatalf("Failed to create store instance: %v", err)
	}
	defer store.Close()

	dbPath := "/mnt/ssd2/pebble"
	if override := os.Getenv("CHAINKV_LOOKUP_PATH"); override != "" {
		dbPath = override
	}
	ps, err := openLookupStore(dbPath, useChainKV)
	if err != nil {
		fmt.Printf("Failed to create lookup store instance: %v\n", err)
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

func findKeyValuePair(accountHashHex string, ps kvGetter) (string, string, error) {
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

func findRecursive(path []byte, pos int, ps kvGetter) ([]byte, string, error) {
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
		return nil, "", fmt.Errorf("无法从数据库中获取键 %s: %v", dbKey, err)
		return findRecursive(path, pos+1, ps)
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

// openStore selects either Pebble or ChainKV, returning the unified kvStore interface.
// useChainKV is toggled via env USE_CHAINKV=1. useState indicates whether to use Put_s/Get_s (for ChainKV state data).
func openStore(path string, useChainKV bool, useState bool) (kvStore, error) {
	if useChainKV {
		db, err := chainkvdb.NewLDBDatabase2(path, 0, 0)
		if err != nil {
			return nil, err
		}
		return &chainKVLDB{db: db, useState: useState}, nil
	}
	// For Pebble, useState flag doesn't matter (single LSM)
	store, err := ethstore.NewPebbleStore(path, 0, 0, "", false)
	if err != nil {
		return nil, err
	}
	return &pebbleStoreAdapter{store: store}, nil
}

// openLookupStore selects either the default PebbleStore or ChainKV's SGC-enabled LevelDB for read-only access.
// useChainKV is toggled via env USE_CHAINKV=1 to avoid breaking existing runs.
func openLookupStore(path string, useChainKV bool) (kvGetter, error) {
	if useChainKV {
		db, err := chainkvdb.NewLDBDatabase2(path, 0, 0)
		if err != nil {
			return nil, err
		}
		return &chainKVLDB{db: db, useState: false}, nil
	}
	return ethstore.NewPebbleStore(path, 0, 0, "", false)
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
