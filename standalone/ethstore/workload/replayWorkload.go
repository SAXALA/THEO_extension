package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	// Please replace "ethstore_module" with the actual module path defined in your ethstore/go.mod file

	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
	prefixdb "github.com/tinoryj/EthStore/standalone/ethstore/prefixdb"
	"github.com/tinoryj/EthStore/standalone/ethstore/ssPrefixdb"
)

func main() {
	// traceFilePath := flag.String("tracefile", "", "Path to the workload trace file. (e.g., /path/to/your/trace.log)")
	// dbPath := flag.String("dbpath", "./ethstore_data", "Path to the EthStore database directory.")
	// flag.Parse()

	// if *traceFilePath == "" {
	// 	log.Fatal("Error: Trace file path must be provided using -tracefile flag.")
	// }
	// if *dbPath == "" {
	// 	log.Fatal("Error: EthStore database directory path must be provided using -dbpath flag.")
	// }

	// db, err := ethstore.New(*dbPath, 0, "replay_workload", false)
	// if err != nil {
	// 	log.Fatalf("Failed to create EthStore instance (path: %s): %v", *dbPath, err)
	// }
	// defer func() {
	// 	log.Println("Closing EthStore...")
	// 	if errClose := db.Close(); errClose != nil {
	// 		log.Printf("Failed to close EthStore: %v", errClose)
	// 	}
	// }()
	// log.Printf("EthStore instance initialized at %s", *dbPath)

	// file, err := os.Open(*traceFilePath)
	// if err != nil {
	// 	log.Fatalf("Failed to open trace file '%s': %v", *traceFilePath, err)
	// }
	// defer file.Close()

	// scanner := bufio.NewScanner(file)
	// lineNum := 0
	// opRegex := regexp.MustCompile(`OPType: (\\w+), key: ([0-9a-fA-F]+)`)

	// log.Printf("Starting replay of trace file: %s", *traceFilePath)

	// for scanner.Scan() {
	// 	lineNum++
	// 	line := scanner.Text()
	// 	if strings.Contains(line, "Global log file opened successfully") || !strings.Contains(line, "OPType:") {
	// 		continue
	// 	}

	// 	matches := opRegex.FindStringSubmatch(line)
	// 	if len(matches) < 3 {
	// 		log.Printf("Warning: Could not parse OPType and key from line %d: %s", lineNum, line)
	// 		continue
	// 	}

	// 	opType := matches[1]
	// 	keyHex := matches[2]

	// 	keyBytes, err := hex.DecodeString(keyHex)
	// 	if err != nil {
	// 		log.Printf("Warning: Failed to decode hex key '%s' (line %d): %v", keyHex, lineNum, err)
	// 		continue
	// 	}

	// 	switch opType {
	// 	case "Get":
	// 		value, errGet := db.Get(keyBytes)
	// 		if errGet != nil {
	// 			log.Printf("EthStore Get operation (key: %s): Error: %v", keyHex, errGet)
	// 		} else {
	// 			log.Printf("EthStore Get operation (key: %s): Success, value (hex): %s", keyHex, hex.EncodeToString(value))
	// 		}
	// 	case "Has":
	// 		exists, errHas := db.Has(keyBytes)
	// 		if errHas != nil {
	// 			log.Printf("EthStore Has operation (key: %s): Error: %v", keyHex, errHas)
	// 		} else {
	// 			log.Printf("EthStore Has operation (key: %s): Success, exists: %t", keyHex, exists)
	// 		}
	// 	default:
	// 		log.Printf("Warning: Unknown OPType '%s' (line %d, key: %s)", opType, lineNum, keyHex)
	// 	}
	// }

	// if err := scanner.Err(); err != nil {
	// 	log.Fatalf("Error reading trace file: %v", err)
	// }

	// log.Println("Trace replay completed.")

	go func() {
		// Start the HTTP server for pprof profiling
		log.Println(http.ListenAndServe(":6061", nil))
	}()
	// repalyPut()
	repalyAccountPut()
	// repPalyAolPut()
	repalySSPut()

	// testPdbPerformance()
	// testAolProformance()
	// TestPebblePreformance()
	// TestGetParentKey()

}

func repalyPut() {
	tempDir := "/mnt/ssd/ethstore/database"
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
			fmt.Printf("\rPut test: %d, use time: %d ns", counter, totalTime.Nanoseconds())
		}

	}
}

func repalyAccountPut() {
	// tempDir := "/mnt/ssd/ethstore/database/prefixdb"
	dirPath := "/mnt/ssd/ethstore/database"
	pdb, err := prefixdb.NewPrefixDB(dirPath)
	if err != nil {
		log.Fatalf("Failed to create EthStore instance: %v", err)
	}
	defer pdb.Close()

	// spdb, err := ssPrefixdb.NewSSPrefixDB(dirPath)
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

		switch keyBytes[0] {
		case 'A', 'O', 'c':
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
}

func repalySSPut() {
	// tempDir := "/mnt/ssd/ethstore/database/prefixdb"
	dirPath := "/mnt/ssd/ethstore/database"
	pdb, err := ssPrefixdb.NewSSPrefixDB(dirPath)
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

func repPalyAolPut() {

	tempDir := "/mnt/ssd/ethstore/database"
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
		counter++
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

	txreader := bufio.NewReader(txfile)
	counter = 0
	for {
		counter++
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
	// notxreader := bufio.NewReader(notxfile)

	// for {
	// 	counter++
	// 	line, err := notxreader.ReadString('\n')
	// 	if err == io.EOF {
	// 		break // End of file reached
	// 	}

	// 	// line format: "key: xxxxxx, value: yyyy"
	// 	line = line[:len(line)-1] // Remove the newline character

	// 	parts := strings.Split(line, ", Value:")
	// 	if len(parts) != 2 {
	// 		// log.Printf("无法解析行: %s", line)
	// 		continue
	// 	}
	// 	keyPart := strings.TrimPrefix(parts[0], "Key: ")
	// 	valuePart := strings.TrimSpace(parts[1])

	// 	// Convert key and value to byte slices
	// 	keyBytes := []byte(keyPart)

	// 	valueBytes := []byte(valuePart)

	// 	keyBytes, err = hex.DecodeString(string(keyBytes))
	// 	if err != nil {
	// 		log.Fatalf("Failed to decode key: %v", err)
	// 	}
	// 	valueBytes, err = hex.DecodeString(string(valueBytes))
	// 	if err != nil {
	// 		log.Fatalf("Failed to decode value: %v", err)
	// 	}

	// 	// Perform the Put operation
	// 	startTime := time.Now()
	// 	err = store.Put(keyBytes, valueBytes)
	// 	endTime := time.Now()
	// 	totalTime += endTime.Sub(startTime)
	// 	if err != nil {
	// 		log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
	// 	}
	// 	// Verify the value was stored correctly

	// 	if counter%100000 == 0 {
	// 		fmt.Printf("\rPut test: %d, use time: %d ns", counter, totalTime.Nanoseconds())
	// 	}
	// }

	txreader := bufio.NewReader(txfile)
	counter = 0
	for {
		counter++
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
	pd, err := ssPrefixdb.NewSSPrefixDB(dirpath)
	if err != nil {
		fmt.Printf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()

	SK_1 := []byte("610000019759ea326fa019a55bda5dff44477be6e1d9c48db950e3fe07a0ba671e")
	SV_1 := []byte("f8440180a0665081a76be9ad792eec7ba0b7819e48a97cd6ab5210cae849c1ea4777ba9b6aa029164acf9a06c22bbe9da20100d94116c6ef93f44a5b58ebd6e1954c3bf436df")
	SK_1, err = hex.DecodeString(string(SK_1))
	SV_1, err = hex.DecodeString(string(SV_1))

	pd.Put(SK_1, SV_1)

	Key1 := []byte("6f0000019759ea326fa019a55bda5dff44477be6e1d9c48db950e3fe07a0ba671e290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563")
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
