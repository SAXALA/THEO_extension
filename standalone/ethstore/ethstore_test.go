// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethstore

import (
	// For profiling
	"encoding/hex"
	"fmt"
	"os" // Added for os.MkdirTemp or t.TempDir()

	// For path manipulation
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"

	"github.com/stretchr/testify/assert"  // Popular assertion library
	"github.com/stretchr/testify/require" // For assertions that should fail the test immediately
)

const testPrefixCacheSizeBytes = 16 * 1024 * 1024

// keyValueStoreAdapter bridges Database.Get(key, dataType) to ethdb.KeyValueStore.Get(key).
type keyValueStoreAdapter struct {
	*Database
}

func (a keyValueStoreAdapter) Get(key []byte) ([]byte, error) {
	return a.Database.Get(key, GetDataTypeFromKey(key))
}

func TestEthStore(t *testing.T) {
	t.Run("DatabaseSuite", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			// Use a temporary directory for the database path
			tempDir := t.TempDir() // Go 1.15+ feature for creating temp dirs

			// Use the actual New constructor from ethstore.go
			// Parameters for New: dirPath string, recentN int, namespace string, readonly bool
			db, err := New(tempDir, 10, "testdb", false, 0, testPrefixCacheSizeBytes, 16)
			if err != nil {
				t.Fatalf("Failed to create database with New: %v", err)
			}
			return keyValueStoreAdapter{Database: db}
		})
	})
}

func BenchmarkEthStore(b *testing.B) {
	dbtest.BenchDatabaseSuite(b, func() ethdb.KeyValueStore {
		tempDir, err := os.MkdirTemp("", "ethstore_bench_")
		if err != nil {
			b.Fatalf("Failed to create temp dir for benchmark: %v", err)
		}

		db, err := New(tempDir, 10, "benchdb", false, 0, testPrefixCacheSizeBytes, 16)
		if err != nil {
			b.Fatalf("Failed to create database with New for benchmark: %v", err)
		}
		return keyValueStoreAdapter{Database: db}
	})
}

// TestEthStore_Lifecycle tests the Open function and the lifecycle of the Database wrapper.
func TestEthStore_Lifecycle(t *testing.T) {
	t.Run("OpenInMemoryAndWrap", func(t *testing.T) {
		tempDir := t.TempDir()
		// Call the actual New constructor
		// For "in-memory" like behavior with AppendOnlyLog, we still need a path.
		store, err := New(tempDir, 10, "lifecycle_mem_test", false, 0, testPrefixCacheSizeBytes, 16)
		require.NoError(t, err, "New with temp path should not fail")
		require.NotNil(t, store, "Opened store should not be nil")

		err = store.Close()
		assert.NoError(t, err, "store.Close() for temp DB should not fail")
	})

	t.Run("OpenPersistentWrapAndReopen", func(t *testing.T) {
		tempDir := t.TempDir()
		dbPath := tempDir // AppendOnlyLog uses the directory directly

		store1, err := New(dbPath, 10, "lifecycle_persist_test", false, 0, testPrefixCacheSizeBytes, 16)
		require.NoError(t, err, "New with a new path should not fail")
		require.NotNil(t, store1, "Opened store1 should not be nil")

		testKey := []byte("greeting")
		testValue := []byte("hello world")
		// Assuming Put requires a block ID, this test needs adjustment
		// For now, this will use the placeholder block ID in your Put method
		err = store1.Put(testKey, testValue)
		require.NoError(t, err, "store1.Put operation should not fail")

		retrievedValue, err := store1.Get(testKey, GetDataTypeFromKey(testKey))
		require.NoError(t, err, "store1.Get operation should not fail")
		assert.Equal(t, testValue, retrievedValue, "Retrieved value should match the put value")

		err = store1.Close()
		assert.NoError(t, err, "store1.Close() should not fail")

		store2, err := New(dbPath, 10, "lifecycle_persist_test", false, 0, testPrefixCacheSizeBytes, 16) // Reopen
		require.NoError(t, err, "Reopening persistent DB should not fail")
		require.NotNil(t, store2, "Reopened store2 should not be nil")

		retrievedValueAfterReopen, err := store2.Get(testKey, GetDataTypeFromKey(testKey))
		require.NoError(t, err, "store2.Get after reopen should not fail")
		assert.Equal(t, testValue, retrievedValueAfterReopen, "Value should persist after reopen")

		err = store2.Close()
		assert.NoError(t, err, "store2.Close() after reopen should not fail")
	})
}

// TestEthStore_SpecificBlockOperations tests block-specific operations,
// assuming your ethstore.Database struct has such methods (e.g., from blockStore.go logic).
func TestEthStore_SpecificBlockOperations(t *testing.T) {
	tempDir := t.TempDir()
	store, err := New(tempDir, 10, "specific_ops_test", false, 0, testPrefixCacheSizeBytes, 16) // Using temp dir
	require.NoError(t, err)
	require.NotNil(t, store)
	defer store.Close() // Ensure the database is closed at the end

	t.Run("StoreAndRetrieveBlock", func(t *testing.T) {
		t.Log("Skipping TestEthStore_SpecificBlockOperations/StoreAndRetrieveBlock: Implement this test with actual block operation methods from ethstore.Database.")
	})
}

func TestPutAndGet(t *testing.T) {
	t.Skip("Skipping TestPutAndGet because it requires an external file")
	/*
		go func() {
			// Start the HTTP server for pprof profiling
			log.Println(http.ListenAndServe(":6060", nil))
		}()
		tempDir := t.TempDir()
		store, err := New(tempDir, 10, "put_test", false)
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()

		testFilePath := "/mnt/tmp/20500000_key_value_pairs.txt"

		// Read key-value pairs from the test file
		file, err := os.Open(testFilePath)
		require.NoError(t, err, "Failed to open test file")
		defer file.Close()

		counter := 0
		reader := bufio.NewReader(file)
		for {
			counter++
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				break // End of file reached
			}
			require.NoError(t, err, "Error reading line from test file")

			// line format: "key: xxxxxx, value: yyyy"
			line = line[:len(line)-1] // Remove the newline character

			parts := strings.Split(line, ", Value :")
			if len(parts) != 2 {
				t.Logf("无法解析行: %s", line)
				continue
			}
			keyPart := strings.TrimPrefix(parts[0], "Key: ")
			valuePart := strings.TrimSpace(parts[1])

			require.NotEmpty(t, keyPart, "Key should not be empty")
			require.NotEmpty(t, valuePart, "Value should not be empty")

			require.NoError(t, err, "Error parsing line from test file")
			// Convert key and value to byte slices
			keyBytes := []byte(keyPart)
			valueBytes := []byte(valuePart)
			// Perform the Put operation
			err = store.Put(keyBytes, valueBytes)
			require.NoError(t, err, "Put operation should not fail")
			// Verify the value was stored correctly
			// fmt.Printf("\rPut test: %d", counter)

			if err != nil {
				t.Fatalf("Get operation failed for key %s: %v", keyPart, err)
			}
		}
		require.NoError(t, err, "Error scanning test file")
		t.Log("Put test completed successfully")
	*/
}

func TestOther(t *testing.T) {
	tempDir := t.TempDir()
	store, err := New(tempDir, 10, "put_test", false, 0, testPrefixCacheSizeBytes, 16)
	require.NoError(t, err)
	require.NotNil(t, store)
	defer store.Close()

	key := []byte("48000000708550f340a1297eefe721a3b0631d8dc4cc5a3462abaeef1a79726f6b")
	value := []byte("0000000000547d05")

	value, err = hex.DecodeString(string(value))
	if err != nil {
		t.Fatalf("Failed to decode value: %v", err)
	}

	err = store.Put(key, value)
	if err != nil {
		t.Fatalf("Put operation failed for key %s: %v", key, err)
	}
	retrievedValue, err := store.Get(key, GetDataTypeFromKey(key))
	if err != nil {
		t.Fatalf("Get operation failed for key %s: %v", key, err)
	}
	if string(retrievedValue) != string(value) {
		t.Fatalf("Retrieved value does not match: got %s, want %s", retrievedValue, value)
	}
}

func TestPraseBlockID(t *testing.T) {
	key1 := []byte("6cfffffffc657b6c0441aafe2ef195d1828d23c133828713edb9e88ba8f931092a")
	value1 := []byte("0137ead2")

	key1, _ = hex.DecodeString(string(key1))
	value1, _ = hex.DecodeString(string(value1))

	dataType := GetDataTypeFromKey(key1)

	blockID, foundBlockID := ParseBlockNumberFromKey(key1, dataType)

	// If not found in key, try from value (for HeaderNumber)
	if !foundBlockID {
		blockID, foundBlockID = ParseBlockNumberFromValue(value1, dataType, nil)
	}

	fmt.Print("blockID: ", blockID, " foundBlockID: ", foundBlockID, " dataType: ", DataTypeStrings[dataType], "\n")
}
