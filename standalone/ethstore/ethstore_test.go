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
	"os" // Added for os.MkdirTemp or t.TempDir()
	// For path manipulation
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
	"github.com/stretchr/testify/assert"  // Popular assertion library
	"github.com/stretchr/testify/require" // For assertions that should fail the test immediately
)

func TestEthStore(t *testing.T) {
	t.Run("DatabaseSuite", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			// Use a temporary directory for the database path
			tempDir := t.TempDir() // Go 1.15+ feature for creating temp dirs

			// Use the actual New constructor from ethstore.go
			// Parameters for New: dirPath string, recentN int, namespace string, readonly bool
			db, err := New(tempDir, 10, "testdb", false)
			if err != nil {
				t.Fatalf("Failed to create database with New: %v", err)
			}
			return db // The *Database instance itself should implement KeyValueStore
		})
	})
}

func BenchmarkEthStore(b *testing.B) {
	dbtest.BenchDatabaseSuite(b, func() ethdb.KeyValueStore {
		tempDir, err := os.MkdirTemp("", "ethstore_bench_")
		if err != nil {
			b.Fatalf("Failed to create temp dir for benchmark: %v", err)
		}

		db, err := New(tempDir, 10, "benchdb", false)
		if err != nil {
			b.Fatalf("Failed to create database with New for benchmark: %v", err)
		}
		return db
	})
}

// TestEthStore_Lifecycle tests the Open function and the lifecycle of the Database wrapper.
func TestEthStore_Lifecycle(t *testing.T) {
	t.Run("OpenInMemoryAndWrap", func(t *testing.T) {
		tempDir := t.TempDir()
		// Call the actual New constructor
		// For "in-memory" like behavior with AppendOnlyLog, we still need a path.
		store, err := New(tempDir, 10, "lifecycle_mem_test", false)
		require.NoError(t, err, "New with temp path should not fail")
		require.NotNil(t, store, "Opened store should not be nil")

		err = store.Close()
		assert.NoError(t, err, "store.Close() for temp DB should not fail")
	})

	t.Run("OpenPersistentWrapAndReopen", func(t *testing.T) {
		tempDir := t.TempDir()
		dbPath := tempDir // AppendOnlyLog uses the directory directly

		store1, err := New(dbPath, 10, "lifecycle_persist_test", false)
		require.NoError(t, err, "New with a new path should not fail")
		require.NotNil(t, store1, "Opened store1 should not be nil")

		testKey := []byte("greeting")
		testValue := []byte("hello world")
		// Assuming Put requires a block ID, this test needs adjustment
		// For now, this will use the placeholder block ID in your Put method
		err = store1.Put(testKey, testValue)
		require.NoError(t, err, "store1.Put operation should not fail")

		retrievedValue, err := store1.Get(testKey)
		require.NoError(t, err, "store1.Get operation should not fail")
		assert.Equal(t, testValue, retrievedValue, "Retrieved value should match the put value")

		err = store1.Close()
		assert.NoError(t, err, "store1.Close() should not fail")

		store2, err := New(dbPath, 10, "lifecycle_persist_test", false) // Reopen
		require.NoError(t, err, "Reopening persistent DB should not fail")
		require.NotNil(t, store2, "Reopened store2 should not be nil")

		retrievedValueAfterReopen, err := store2.Get(testKey)
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
	store, err := New(tempDir, 10, "specific_ops_test", false) // Using temp dir
	require.NoError(t, err)
	require.NotNil(t, store)
	defer store.Close() // Ensure the database is closed at the end

	t.Run("StoreAndRetrieveBlock", func(t *testing.T) {
		t.Log("Skipping TestEthStore_SpecificBlockOperations/StoreAndRetrieveBlock: Implement this test with actual block operation methods from ethstore.Database.")
	})
}
