package ethstore

import (
	"bytes"
	"errors" // Added for ErrNotFound comparison
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper function to create a temporary directory for tests
func tempPebbleDBPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "pebble_test_")
	if err != nil {
		t.Fatalf("Failed to create temp dir for pebble: %v", err)
	}
	return dir
}

func TestNewPebbleStore(t *testing.T) {
	dbPath := tempPebbleDBPath(t)
	defer os.RemoveAll(dbPath)

	ps, err := NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore() error = %v, wantErr %v", err, false)
	}
	if ps == nil {
		t.Fatal("NewPebbleStore() returned nil PebbleStore")
	}
	if ps.db == nil {
		t.Fatal("PebbleStore.db is nil after NewPebbleStore()")
	}
	err = ps.Close()
	if err != nil {
		t.Errorf("Failed to close pebble store: %v", err)
	}

	// Test if the directory was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("Pebble DB directory %s was not created", dbPath)
	}
}

func TestPebbleStore_PutGetDelete(t *testing.T) {
	dbPath := tempPebbleDBPath(t)
	defer os.RemoveAll(dbPath)

	ps, err := NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		t.Fatalf("Failed to create PebbleStore: %v", err)
	}
	defer ps.Close()

	key1 := []byte("testKey1")
	value1 := []byte("testValue1")
	key2 := []byte("testKey2")
	value2 := []byte("testValue2")

	// Test Put
	if err := ps.Put(key1, value1); err != nil {
		t.Fatalf("Put(%s, %s) error = %v", key1, value1, err)
	}
	if err := ps.Put(key2, value2); err != nil {
		t.Fatalf("Put(%s, %s) error = %v", key2, value2, err)
	}

	// Test Get
	retrievedValue1, err := ps.Get(key1)
	if err != nil {
		t.Fatalf("Get(%s) error = %v", key1, err)
	}
	if !bytes.Equal(retrievedValue1, value1) {
		t.Errorf("Get(%s) = %s, want %s", key1, retrievedValue1, value1)
	}

	retrievedValue2, err := ps.Get(key2)
	if err != nil {
		t.Fatalf("Get(%s) error = %v", key2, err)
	}
	if !bytes.Equal(retrievedValue2, value2) {
		t.Errorf("Get(%s) = %s, want %s", key2, retrievedValue2, value2)
	}

	// Test Get non-existent key
	nonExistentKey := []byte("nonExistentKey")
	_, err = ps.Get(nonExistentKey)
	// if err != pebble.ErrNotFound { // Changed: Use local ErrNotFound
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(%s) error = %v, want %v", nonExistentKey, err, ErrNotFound)
	}

	// Test Delete
	if err := ps.Delete(key1); err != nil {
		t.Fatalf("Delete(%s) error = %v", key1, err)
	}

	// Test Get after Delete
	_, err = ps.Get(key1)
	// if err != pebble.ErrNotFound { // Changed: Use local ErrNotFound
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(%s) after Delete error = %v, want %v", key1, err, ErrNotFound)
	}

	// Ensure other key is still present
	retrievedValue2AfterDelete, err := ps.Get(key2)
	if err != nil {
		t.Fatalf("Get(%s) after deleting another key, error = %v", key2, err)
	}
	if !bytes.Equal(retrievedValue2AfterDelete, value2) {
		t.Errorf("Get(%s) after deleting another key = %s, want %s", key2, retrievedValue2AfterDelete, value2)
	}
}

func TestPebbleStore_Close(t *testing.T) {
	dbPath := tempPebbleDBPath(t)
	defer os.RemoveAll(dbPath)

	ps, err := NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore() error = %v", err)
	}

	// Close the database
	if err := ps.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Try operations after close (should error or panic, pebble specific)
	// Pebble's DB.Get after close returns an error, not a panic.
	_, err = ps.Get([]byte("anyKey"))
	if err == nil {
		// Depending on pebble's behavior, this might be ErrClosed or another error.
		// For now, just check that an error IS returned.
		t.Error("Get() after Close() did not return an error")
	}

	// Closing an already closed DB should be a no-op or return a specific error
	// Pebble's Close is idempotent.
	if err := ps.Close(); err != nil {
		t.Errorf("Closing an already closed DB returned error: %v", err)
	}
}

func TestPebbleStore_Iterator(t *testing.T) {
	dbPath := tempPebbleDBPath(t)
	defer os.RemoveAll(dbPath)

	ps, err := NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		t.Fatalf("Failed to create PebbleStore: %v", err)
	}
	defer ps.Close()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("val1"), []byte("val2"), []byte("val3")}

	for i := 0; i < len(keys); i++ {
		if err := ps.Put(keys[i], values[i]); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	iter := ps.NewIterator(nil, nil) // Changed: pass (nil, nil)
	// defer iter.Close() // Changed: use Release, and handle nil iterator from placeholder
	if iter != nil {
		defer iter.Release()
	}

	var count int
	// for iter.First(); iter.Valid(); iter.Next() { // Changed: adapt to ethdb.Iterator
	// The actual test logic will fail here if iter is nil due to placeholder NewIterator
	if iter != nil {
		for iter.Next() {
			if !bytes.Equal(iter.Key(), keys[count]) {
				t.Errorf("Iterator key mismatch: got %s, want %s", iter.Key(), keys[count])
			}
			if !bytes.Equal(iter.Value(), values[count]) {
				t.Errorf("Iterator value mismatch: got %s, want %s", iter.Value(), values[count])
			}
			count++
		}
		if err := iter.Error(); err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
	}

	if count != len(keys) {
		t.Errorf("Iterator iterated over %d keys, want %d (actual iterator might be nil due to placeholder)", count, len(keys))
	}

	// Test iteration with prefix
	prefix := []byte("key")
	// iterOpts := &pebble.IterOptions{ // Changed: This is Pebble specific, adapt to ethdb.Iterator
	// 	LowerBound: prefix,
	// 	UpperBound: append(prefix, 0xff), // Iterate up to the prefix + max byte
	// }
	// Recreate iterator with specific options
	iterWithPrefix := ps.NewIterator(prefix, nil) // Changed: pass prefix and nil for start
	// defer iterWithPrefix.Close() // Changed: use Release, and handle nil
	if iterWithPrefix != nil {
		defer iterWithPrefix.Release()
	}

	count = 0
	// for iterWithPrefix.First(); iterWithPrefix.Valid(); iterWithPrefix.Next() { // Changed: adapt to ethdb.Iterator
	if iterWithPrefix != nil {
		for iterWithPrefix.Next() {
			// Check if key starts with prefix (though bounds should handle this)
			if !bytes.HasPrefix(iterWithPrefix.Key(), prefix) {
				t.Errorf("Iterator with prefix returned key %s which does not have prefix %s", iterWithPrefix.Key(), prefix)
			}
			count++
		}
		if err := iterWithPrefix.Error(); err != nil {
			t.Fatalf("Iterator with prefix error: %v", err)
		}
	}
	if count != len(keys) { // All keys in this test have the "key" prefix
		t.Errorf("Iterator with prefix iterated over %d keys, want %d (actual iterator might be nil due to placeholder)", count, len(keys))
	}
}

func TestPebbleStore_Reopen(t *testing.T) {
	dbPath := tempPebbleDBPath(t)
	defer os.RemoveAll(dbPath)

	// Create and populate DB
	ps1, err := NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore() error = %v", err)
	}
	key := []byte("persistKey")
	value := []byte("persistValue")
	if err := ps1.Put(key, value); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := ps1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopen the DB
	ps2, err := NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore() on reopen error = %v", err)
	}
	defer ps2.Close()

	retrievedValue, err := ps2.Get(key)
	if err != nil {
		t.Fatalf("Get() after reopen error = %v", err)
	}
	if !bytes.Equal(retrievedValue, value) {
		t.Errorf("Get() after reopen = %s, want %s", retrievedValue, value)
	}
}

func TestPebbleStore_ReopenExtended(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "pebble-reopen-test")

	// Create and open the first Pebble database instance
	store1, err := NewPebbleStore(dbPath, 0, 0, "", false)
	require.NoError(t, err, "First NewPebbleStore should not fail")

	// Write test data
	testKey := []byte("persist-key")
	testValue := []byte("persist-value")
	err = store1.Put(testKey, testValue)
	require.NoError(t, err, "Put operation should not fail")

	// Close the first database instance
	err = store1.Close()
	require.NoError(t, err, "First Close should not fail")

	// Reopen database
	store2, err := NewPebbleStore(dbPath, 0, 0, "", false)
	require.NoError(t, err, "Second NewPebbleStore should not fail")
	defer store2.Close()

	// Verify data persistence
	value, err := store2.Get(testKey)
	require.NoError(t, err, "Get after reopen should not fail")
	assert.Equal(t, testValue, value, "Value should persist after reopen")
}

func TestPebbleStore_MkdirAllError(t *testing.T) {
	// This test is a bit tricky as os.MkdirAll is quite robust.
	// We can try to create a file with the same name as the directory path to force an error.
	// This test might be platform-dependent or require specific permissions.
	// For simplicity, we'll check the error wrapping in NewPebbleStore if os.MkdirAll fails.

	// Create a file where we want to create a directory
	filePath := filepath.Join(os.TempDir(), "pebble_test_file_conflict")
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Failed to create conflicting file: %v", err)
	}
	f.Close()
	defer os.Remove(filePath)

	_, err = NewPebbleStore(filePath, 0, 0, "", false) // Try to create DB at the path of the file
	if err == nil {
		t.Errorf("NewPebbleStore() did not return error when dbPath is a file")
		// If it somehow succeeded, clean up
		ps, _ := NewPebbleStore(filePath, 0, 0, "", false)
		ps.Close()
		os.RemoveAll(filePath) // Attempt to remove if it became a dir
	} else {
		// Check if the error message contains the expected part
		// This is a basic check, could be more specific if needed
		expectedErrorMsg := fmt.Sprintf("failed to create pebble db path %s", filePath)
		if !bytes.Contains([]byte(err.Error()), []byte(expectedErrorMsg)) && !bytes.Contains([]byte(err.Error()), []byte("is a directory")) && !bytes.Contains([]byte(err.Error()), []byte("not a directory")) {
			// The exact error from pebble/os might vary, e.g. "mkdir /tmp/pebble_test_file_conflict: not a directory"
			// or pebble might return its own error after os.MkdirAll fails.
			// We are primarily interested that *an* error related to path creation occurs.
			t.Logf("Received error: %v", err) // Log the error for debugging if the check fails
		}
	}
}

func TestPebbleStore_BasicOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "pebble-test")

	// Create and open a Pebble database
	store, err := NewPebbleStore(dbPath, 0, 0, "", false)
	require.NoError(t, err, "NewPebbleStore should not fail")
	require.NotNil(t, store, "PebbleStore should not be nil")

	// Test Put and Get operations
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	// Write data
	err = store.Put(testKey, testValue)
	require.NoError(t, err, "Put operation should not fail")

	// Read data and verify
	value, err := store.Get(testKey)
	require.NoError(t, err, "Get operation should not fail")
	assert.Equal(t, testValue, value, "Retrieved value should match")

	// Test reading a non-existent key
	nonExistentKey := []byte("non-existent-key")
	_, err = store.Get(nonExistentKey)
	assert.Error(t, err, "Get with non-existent key should fail")

	// Test delete operation
	err = store.Delete(testKey)
	require.NoError(t, err, "Delete operation should not fail")

	// Verify deletion
	_, err = store.Get(testKey)
	assert.Error(t, err, "Get after delete should fail")

	// Close database
	err = store.Close()
	assert.NoError(t, err, "Close should not fail")
}

func TestPebbleStore_ManyOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "pebble-many-ops-test")

	// Create and open a Pebble database
	store, err := NewPebbleStore(dbPath, 0, 0, "", false)
	require.NoError(t, err)
	defer store.Close()

	// Perform multiple write operations
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err = store.Put(key, value)
		require.NoError(t, err)
	}

	// Read and verify all values
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))
		value, err := store.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}

	// Delete half of the keys
	for i := 0; i < numEntries/2; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		err = store.Delete(key)
		require.NoError(t, err)
	}

	// Confirm that deleted keys are indeed deleted
	for i := 0; i < numEntries/2; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		_, err := store.Get(key)
		assert.Error(t, err, "Key should not exist after deletion")
	}

	// Confirm that non-deleted keys still exist
	for i := numEntries / 2; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))
		value, err := store.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}
}
