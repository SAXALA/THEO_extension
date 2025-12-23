package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"strings"
	"time"

	// Please replace "ethstore_module" with the actual module path defined in your ethstore/go.mod file

	"github.com/bradfitz/gomemcache/memcache"
	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
	prefixdb "github.com/tinoryj/EthStore/standalone/ethstore/prefixdb"
)

type opType int

const (
	opGet opType = iota
	opHas
	opPut
	opDelete
)

func main() {
	mode := flag.String("mode", "re", "Mode of operation: ld (load data), re (replay trace), o (other), lb (load baseline), rb (replay baseline)")
	flag.Parse()

	otherRunner := TestGetParentKey // change here when you need a different 'o' workload

	go func() {
		// Start the HTTP server for pprof profiling
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	switch *mode {
	case "ld":
		loadData()
	case "re":
		replayTrace()
	case "o":
		otherRunner()
	case "lb":
		loadbaselineData()
	case "rb":
		replaybaseline()
	default:
		log.Fatalf("unknown mode %q, use ld, re, o, lb, or rb", *mode)
	}
}

func loadbaselineData() {
	tempDir := "/mnt/ssd/ethstore/baseline/pebble"
	store, err := ethstore.NewPebbleStore(tempDir, 0, 0, "", false)
	// store, err := ethstore.New(tempDir, 10, "put_test", false)
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

func replaybaseline() {
	baselinePebbleDir := "/mnt/ssd/ethstore/baseline/pebble"
	store, err := ethstore.NewPebbleStore(baselinePebbleDir, 0, 0, "", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	testFilePath := "/mnt/tmp/geth-trace-withcache-merged-block-20500000-21500000"

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
		//|| keyBytes[0] == 'O'
		if keyBytes[0] == 'A' || keyBytes[0] == 'O' {

		} else {
			continue
		}

		// if keyBytes[0] == 'a' || keyBytes[0] == 'o' || keyBytes[0] == 'c' {
		// 	continue
		// }

		// keyStr := hex.EncodeToString(keyBytes)

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

		end := time.Now()
		totalTime += end.Sub(start)
		counter++
		if opErr != nil {
			fmt.Printf("Operation %s failed for key %s: %v\n", opTypeStr, keyHex, opErr)
		}
		if counter%10000 == 0 {
			fmt.Printf("\rProcessed %d operations, total time: %f s", counter, totalTime.Seconds())
		}
	}
}

// load all data from the key-value file into EthStore
func loadData() {
	// ethStoreDir := "/mnt/ssd/ethstore/database/prefixdb"
	ethStoreDir := "/mnt/ssd/ethstore/database"
	store, err := ethstore.New(ethStoreDir, 1000, "put_test", false)
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
	// tempDir := "/mnt/ssd/ethstore/database/prefixdb"
	dirPath := "/mnt/ssd/ethstore/database"
	pdb, err := prefixdb.NewPrefixDB(dirPath)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer pdb.Close()

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

		if counter < 375799415 {
			continue
		}

		// if counter > 375799415 && !isSaveTrie {
		// 	pdb.SaveTree()
		// time.Sleep(5 * time.Minute) //make sure all batchs are committed
		// 	isSaveTrie = true
		// 	// continue
		// }

		if counter > 1966138022 {
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

		switch keyBytes[0] {
		case 'A', 'O':
			// Perform the Put operation
			startTime := time.Now()
			// err = pdb.Put(keyBytes, valueBytes)

			value, ok, err := pdb.Get(keyBytes)

			endTime := time.Now()
			totalTime += endTime.Sub(startTime)

			if err != nil {
				err = pdb.Put(keyBytes, valueBytes)
				fmt.Printf("Get operation failed for key %s: %v", keyPart, err)
				continue
			}
			if !ok {
				fmt.Printf("Key %s not found in PrefixDB", keyPart)
				continue
			}
			if !bytes.Equal(value, valueBytes) {

				fmt.Println("counter:", counter)
				// log.Printf("Value mismatch for key %s: expected %x, got %x", keyPart, valueBytes, value)
			}
			// if err != nil {
			// 	log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
			// }

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

		// if counter%500000 == 0 {
		// 	break
		// }
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
	pdb, err := prefixdb.NewPrefixDB(dirPath)
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

		// Perform the Put operation
		startTime := time.Now()
		err = pdb.Put(keyBytes, valueBytes)
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

func repPlayAolPut() {

	tempDir := "/mnt/ssd/ethstore/database"
	store, err := ethstore.New(tempDir, 200, "put_test", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()
	fmt.Println("Start aol put test...")
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
	dirPath := "/mnt/ssd/ethstore/database"
	pd, err := prefixdb.NewPrefixDB(dirPath)
	if err != nil {
		log.Fatalf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()

	testFilePath := "/mnt/ssd/ethstore/20500000_key_value_pairs.txt"

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

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

		if keyBytes[0] != 'O' {
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

		// Perform the Put operation
		value, ok, err := pd.Get(keyBytes)
		if err != nil {
			log.Fatalf("Get operation failed for key %s: %v", keyPart, err)
		}
		if !ok {
			log.Printf("Key %s not found in PrefixDB", keyPart)
			continue
		}
		if !bytes.Equal(value, valueBytes) {
			log.Printf("Value mismatch for key %s: expected %x, got %x", keyPart, valueBytes, value)
		}
		if err != nil {
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}
		// Verify the value was stored correctly

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d", counter)
		}

	}
}

func testPdbPerformance() {
	tempDir := "/mnt/ssd/ethstore/testDB"
	store, err := ethstore.New(tempDir, 10, "put_test", false)
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
	store, err := ethstore.New(tempDir, 100, "put_test", false)
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
	pd, err := prefixdb.NewPrefixDB(dirpath)
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
	pd.Put(Key1, Value1)
	fmt.Print("Parent Key1: ", hex.EncodeToString(parentKey1), "\n")
	if !bytes.Equal(parentKey1, Key1[:len(Key1)-2]) {
		fmt.Printf("Expected parent key for Key1 to be %x, got %x\n", Key1[:len(Key1)-2], parentKey1)
	} else {
		fmt.Println("Parent key test passed.")
	}
}

func replayTrace() {
	tempDir := "/mnt/ssd/ethstore/database"
	store, err := ethstore.New(tempDir, 1000, "put_test", false)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer store.Close()

	testFilePath := "/mnt/tmp/geth-trace-withcache-merged-block-20500000-21500000"

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
		//|| keyBytes[0] == 'O'
		if keyBytes[0] == 'O' {
		} else {
			continue
		}

		// if keyBytes[0] == 'a' || keyBytes[0] == 'o' || keyBytes[0] == 'c' {
		// 	continue
		// }

		// keyStr := hex.EncodeToString(keyBytes)

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

		end := time.Now()
		totalTime += end.Sub(start)
		counter++
		if opErr != nil {
			fmt.Printf("Operation %s failed for key %s: %v\n", opTypeStr, keyHex, opErr)
		}
		if counter%10000 == 0 {
			fmt.Printf("\rProcessed %d operations, total time: %f s", counter, totalTime.Seconds())
		}
	}
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
