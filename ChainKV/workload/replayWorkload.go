package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "net/http/pprof"

	chainkvdb "github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/ethdb"
)

// Config holds the configuration for workload replay
type Config struct {
	DatabaseDir         string `json:"databaseDir"`
	LoadDataDir         string `json:"loadDataDir"`
	BaselinePebbleDir   string `json:"baselinePebbleDir"`
	TraceFile           string `json:"traceFile"`
	TraceFileNocache    string `json:"traceFileNocache"`
}

// chainKVLDB wraps ChainKV's LDBDatabase to satisfy kvStore.
type chainKVLDB struct {
	db       *chainkvdb.LDBDatabase
	useState bool // if true, use Put_s/Get_s for state data; else use Put/Get
}

// NewChainKVLDB creates a new ChainKV database instance
func NewChainKVLDB(path string, cache int, handles int, useState bool) (*chainKVLDB, error) {
	db, err := chainkvdb.NewLDBDatabase(path, cache, handles)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return &chainKVLDB{
		db:       db,
		useState: useState,
	}, nil
}

// Put writes a key-value pair to the database
func (c *chainKVLDB) Put(key, value []byte) error {
	if c.useState {
		return c.db.Put_s(key, value)
	}
	return c.db.Put(key, value)
}

// Get retrieves a value for the given key
func (c *chainKVLDB) Get(key []byte) ([]byte, error) {
	if c.useState {
		return c.db.Get_s(key)
	}
	return c.db.Get(key)
}

// Delete removes a key from the database
func (c *chainKVLDB) Delete(key []byte) error {
	return c.db.Delete(key)
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

	scanner := bufio.NewScanner(file)
	count := 0
	startTime := time.Now()

	for scanner.Scan() && (limit == 0 || count < limit) {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}

		key := []byte(parts[0])
		value := []byte(parts[1])

		if err := db.Put(key, value); err != nil {
			return fmt.Errorf("failed to put key-value: %w", err)
		}

		count++
		if count%10000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(count) / elapsed.Seconds()
			fmt.Printf("Loaded %d entries (%.2f ops/sec)\n", count, rate)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading data file: %w", err)
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

func main() {
	configPath := flag.String("config", "replay_config.json", "Path to configuration file")
	dbPath := flag.String("db", "", "Path to database (overrides config)")
	cache := flag.Int("cache", 256, "Cache size in MB")
	handles := flag.Int("handles", 128, "Number of file handles")
	useState := flag.Bool("state", false, "Use state-specific operations (Put_s/Get_s)")
	loadFile := flag.String("load", "", "Load data from file")
	loadLimit := flag.Int("limit", 0, "Limit number of entries to load (0 = no limit)")
	benchmark := flag.Int("bench", 0, "Run benchmark with N operations")
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
	dbDirectory := *dbPath
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

	// Open database
	fmt.Println("\nOpening ChainKV database...")
	db, err := NewChainKVLDB(dbDirectory, *cache, *handles, *useState)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	fmt.Println("Database opened successfully!")

	// Load data if specified
	if *loadFile != "" {
		fmt.Printf("\nLoading data from: %s\n", *loadFile)
		if err := loadData(db, *loadFile, *loadLimit); err != nil {
			log.Fatalf("Failed to load data: %v", err)
		}
	}

	// Run benchmark if specified
	if *benchmark > 0 {
		benchmarkOperations(db, *benchmark)
	}

	// If no operations specified, show usage
	if *loadFile == "" && *benchmark == 0 {
		fmt.Println("\nNo operations specified. Use -load or -bench flags.")
		fmt.Println("\nUsage examples:")
		fmt.Println("  # Load data from file")
		fmt.Println("  ./replayWorkload -db /path/to/db -load data.txt")
		fmt.Println("\n  # Run benchmark")
		fmt.Println("  ./replayWorkload -db /path/to/db -bench 10000")
		fmt.Println("\n  # Use state-specific operations")
		fmt.Println("  ./replayWorkload -db /path/to/db -state -bench 10000")
	}

	fmt.Println("\nDone!")
}
