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
	"path/filepath"
	"sort"
	"strings"

	// Please replace "ethstore_module" with the actual module path defined in your ethstore/go.mod file

	"github.com/ethereum/go-ethereum/common"
	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
	"github.com/tinoryj/EthStore/standalone/ethstore/prefixdb"
)

func main() {
	// sortAol()
	// buildAccountHashPebble()
	// TestPrefixGet()

	// filtterTASS()

	go func() {
		// Start the HTTP server for pprof profiling
		log.Println(http.ListenAndServe(":6062", nil))
	}()

	TestMemCache()
}

func repalyPut() {
	go func() {
		// Start the HTTP server for pprof profiling
		log.Println(http.ListenAndServe(":6060", nil))
	}()
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
		err = store.Put(keyBytes, valueBytes)
		if err != nil {
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}
		// Verify the value was stored correctly

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d", counter)
		}

	}
}

type KeyValuePair struct {
	Key   []byte
	Value []byte
}

func sortAol() {
	go func() {
		// Start the HTTP server for pprof profiling
		log.Println(http.ListenAndServe(":6061", nil))
	}()
	aolDir := "/mnt/ssd/ethstore/sortAol"

	txLookupFile, err := os.Create(filepath.Join(aolDir, "txlookup_sorted.dat"))
	if err != nil {
		log.Fatalf("Failed to create txlookup output file: %v", err)
	}
	defer txLookupFile.Close()

	// nonTxLookupFile, err := os.Create(filepath.Join(aolDir, "nontxlookup_sorted.dat"))
	// if err != nil {
	// 	log.Fatalf("Failed to create non-txlookup output file: %v", err)
	// }
	// defer nonTxLookupFile.Close()

	testFilePath := "/mnt/ssd/ethstore/20500000_key_value_pairs.txt"

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	counter := 0
	reader := bufio.NewReader(file)

	txLookupData := make(map[uint64][]KeyValuePair)
	// nonTxLookupData := make(map[uint64][]KeyValuePair)
	var blockID uint64
	var foundBlockID bool

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
		dataType := ethstore.GetDataTypeFromKey(keyBytes)

		if ethstore.AolHandledDataTypes[dataType] {
			// Try to get blockID from key
			blockID, foundBlockID = ethstore.ParseBlockNumberFromKey(keyBytes, dataType)
			// If not found in key, try from value (for HeaderNumber, TxLookup)
			if !foundBlockID {
				blockID, foundBlockID = ethstore.ParseBlockNumberFromValue(valueBytes, dataType)
			}

			if !foundBlockID {
				fmt.Printf("not supported as blockID cannot be derived from key or value alone, key: %s,value: %s, type: %s\n", common.Bytes2Hex(keyBytes), common.Bytes2Hex(valueBytes), ethstore.DataTypeStrings[dataType])
				continue
			}

			pair := KeyValuePair{
				Key:   keyBytes,
				Value: valueBytes,
			}

			if dataType == ethstore.TransactionLookupMetadataDataType {
				if _, exists := txLookupData[blockID]; !exists {
					txLookupData[blockID] = make([]KeyValuePair, 0)
				}
				txLookupData[blockID] = append(txLookupData[blockID], pair)
			}
			// } else {
			// 	if _, exists := nonTxLookupData[blockID]; !exists {
			// 		nonTxLookupData[blockID] = make([]KeyValuePair, 0)
			// 	}
			// 	nonTxLookupData[blockID] = append(nonTxLookupData[blockID], pair)
			// }
		}
		if err != nil {
			log.Fatalf("Put operation failed for key %s: %v", keyPart, err)
		}
		// Verify the value was stored correctly

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d", counter)
		}
	}

	allBlockIDs := make([]uint64, 0)

	// 收集所有唯一的区块ID
	blockIDSet := make(map[uint64]struct{})
	for blockID := range txLookupData {
		blockIDSet[blockID] = struct{}{}
	}
	// for blockID := range nonTxLookupData {
	// 	blockIDSet[blockID] = struct{}{}
	// }

	// 转换为切片并排序
	for blockID := range blockIDSet {
		allBlockIDs = append(allBlockIDs, blockID)
	}
	sort.Slice(allBlockIDs, func(i, j int) bool {
		return allBlockIDs[i] < allBlockIDs[j]
	})

	txCount, nonTxCount := 0, 0

	for _, blockID := range allBlockIDs {
		// 处理交易查询数据
		if pairs, exists := txLookupData[blockID]; exists && len(pairs) > 0 {
			// 写入区块ID标记行
			txLookupFile.WriteString(fmt.Sprintf("# BlockID: %d\n", blockID))

			// 写入该区块的所有键值对
			for _, pair := range pairs {
				keyHex := hex.EncodeToString(pair.Key)
				valueHex := hex.EncodeToString(pair.Value)
				line := fmt.Sprintf("Key: %s, Value: %s\n", keyHex, valueHex)
				txLookupFile.WriteString(line)
				txCount++
			}
		}

		// // 处理非交易查询数据
		// if pairs, exists := nonTxLookupData[blockID]; exists && len(pairs) > 0 {
		// 	// 写入区块ID标记行
		// 	nonTxLookupFile.WriteString(fmt.Sprintf("# BlockID: %d\n", blockID))

		// 	// 写入该区块的所有键值对
		// 	for _, pair := range pairs {
		// 		keyHex := hex.EncodeToString(pair.Key)
		// 		valueHex := hex.EncodeToString(pair.Value)
		// 		line := fmt.Sprintf("Key: %s, Value: %s\n", keyHex, valueHex)
		// 		nonTxLookupFile.WriteString(line)
		// 		nonTxCount++
		// 	}
		// }

		if blockID%1000 == 0 {
			fmt.Printf("\r处理区块ID: %d", blockID)
		}
	}

	fmt.Printf("\n完成！处理了 %d 个区块\n", len(allBlockIDs))
	fmt.Printf("交易查询数据: %d 条记录\n", txCount)
	fmt.Printf("非交易查询数据: %d 条记录\n", nonTxCount)
	fmt.Printf("输出文件保存在: %s\n", aolDir)
}

func buildAccountHashPebble() {
	fmt.Println("Building accountHashPebble")
	dbPath := "/mnt/ssd/ethstore/index/accountHash_key_pebble"

	ps, err := ethstore.NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		fmt.Printf("Failed to create PebbleStore instance: %v\n", err)
		return
	}
	defer ps.Close()

	testFilePath := "/mnt/ssd/ethstore/index/hash_key_index"

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		fmt.Printf("Failed to open test file: %v\n", err)
		return
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

		// Remove the newline character
		line = strings.TrimSpace(line)

		// line format: "accountHash: xxxxxx, Key: yyyy"
		parts := strings.Split(line, " Key: ")
		if len(parts) != 2 {
			fmt.Printf("无法解析行: %s\n", line)
			continue
		}

		// Extract accountHash and Key
		accountHashPart := strings.TrimPrefix(parts[0], "accountHash: ")
		keyPart := strings.TrimSpace(parts[1])

		// Convert accountHash and Key to byte slices
		accountHashBytes, err := hex.DecodeString(strings.Trim(accountHashPart, `"`))
		if err != nil {
			fmt.Printf("Failed to decode accountHash: %v\n", err)
			continue
		}

		keyBytes, err := hex.DecodeString(strings.Trim(keyPart, `"`))
		if err != nil {
			fmt.Printf("Failed to decode Key: %v\n", err)
			continue
		}

		// Perform the Put operation
		if len(keyBytes) > 0 && keyBytes[0] == 'A' {
			err = ps.Put(accountHashBytes, keyBytes)
			if err != nil {
				fmt.Printf("Put operation failed for accountHash %s and Key %s: %v\n", accountHashPart, keyPart, err)
				continue
			}
		}

		// Print progress
		if counter%100000 == 0 {
			fmt.Printf("\rProcessed lines: %d", counter)
		}
	}
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

// 将'o' 开头的键输出到另一个文件
func filtterTASS() {
	testFilePath := "/mnt/ssd/ethstore/20500000_key_value_pairs.txt"
	outputFilePath := "/mnt/ssd/ethstore/TASS.txt"

	// Read key-value pairs from the test file
	file, err := os.Open(testFilePath)
	if err != nil {
		log.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outputFile.Close()

	counter := 0
	reader := bufio.NewReader(file)

	for {
		counter++
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break // End of file reached
		}

		if counter < 2223547411 {
			continue // Skip the first 1967893668 lines
		}

		// line format: "Key: xxxxxx, value: yyyy"
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

		if keyBytes[0] != 'o' {
			continue
		}

		outputFile.WriteString("Key: " + hex.EncodeToString(keyBytes) + ", Value: " + hex.EncodeToString(valueBytes) + "\n")

		if counter%100000 == 0 {
			fmt.Printf("\rPut test: %d", counter)
		}

	}
	fmt.Println("\nFinished filtering TASS keys.")
}

func TestMemCache() {
	SK_1 := []byte("4f000001b1d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d03101")
	SK_1, _ = hex.DecodeString(string(SK_1))

	AK_1 := []byte("410000000000010b")
	AV_1 := []byte("f8669d31d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d031b846f8440180a02e90fa6e0dd972de88c3d7365b293f8fb67afadb98ba5c58cac1e1ee8ce47d12a0bafa57ebfbfd24de79a762fec12871b565cd7da7206993a55cae3f2a3476aae3")
	AK_1, _ = hex.DecodeString(string(AK_1))
	AV_1, _ = hex.DecodeString(string(AV_1))

	str := string(SK_1)
	fmt.Printf("SK_1: %s\n", str)

	dirPath := "/mnt/ssd/ethstore/database"
	pd, err := prefixdb.NewPrefixDB(dirPath)
	if err != nil {
		fmt.Printf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()

	// pd.Put(AK_1, AV_1)

	// SV_1 := []byte("SV_1_value")
	value, got, err := pd.Get(SK_1)
	if err != nil || !got {
		fmt.Printf("Get operation failed for SK_1: %v, got: %t\n", err, got)
	}
	if value == nil {
		fmt.Println("Value for SK_1 is nil")
	}
	fmt.Printf("Value for SK_1: %x\n", value)
}
