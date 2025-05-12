package ethstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPebbleIntegration tests Pebble storage integration in EthStore
func TestPebbleIntegration(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()

	// Create and initialize EthStore
	store, err := New(tempDir, 10, "test", false)
	require.NoError(t, err, "Failed to create EthStore")
	defer store.Close()

	// Check if Pebble store is properly initialized
	assert.NotNil(t, store.db, "Pebble store should be initialized")

	// Ensure Pebble database folder exists
	pebblePath := filepath.Join(tempDir, "pebble")
	_, err = os.Stat(pebblePath)
	assert.NoError(t, err, "Pebble directory should exist")
}

// TestNonAOLData tests storage and retrieval of non-AOL data types
func TestNonAOLData(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()

	// Create and initialize EthStore
	store, err := New(tempDir, 10, "test", false)
	require.NoError(t, err, "Failed to create EthStore")
	defer store.Close()

	// Create a key for a non-AOL data type (e.g., using AccountDataType)
	keyPrefix := byte(GetPrefixByDataType(AccountDataType))
	key := append([]byte{keyPrefix}, []byte("test-account-key")...)
	value := []byte("test-account-value")

	// Store data
	err = store.Put(key, value)
	require.NoError(t, err, "Failed to Put non-AOL data")

	// Retrieve data and validate
	retrievedValue, err := store.Get(key)
	require.NoError(t, err, "Failed to Get non-AOL data")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match")

	// Test existence
	exists, err := store.Has(key)
	require.NoError(t, err, "Failed to check existence")
	assert.True(t, exists, "Key should exist")

	// Test deletion
	err = store.Delete(key)
	require.NoError(t, err, "Failed to Delete non-AOL data")

	// Verify deletion
	exists, err = store.Has(key)
	require.NoError(t, err, "Failed to check existence after deletion")
	assert.False(t, exists, "Key should not exist after deletion")
}

// TestPebbleRetrieve tests the Retrieve method for non-AOL data
func TestPebbleRetrieve(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()

	// Create and initialize EthStore
	store, err := New(tempDir, 10, "test", false)
	require.NoError(t, err, "Failed to create EthStore")
	defer store.Close()

	// Create test data
	plainKey := []byte("test-retrieve-key")
	value := []byte("test-retrieve-value")
	dataType := AccountDataType // Non-AOL data type

	// Store data via Retrieve method (need to add type prefix)
	ctx := context.Background()
	keyPrefix := byte(GetPrefixByDataType(dataType))
	fullKey := append([]byte{keyPrefix}, plainKey...)

	err = store.Put(fullKey, value)
	require.NoError(t, err, "Failed to Put data for Retrieve test")

	// Use Retrieve method to get data
	retrievedValue, err := store.Retrieve(ctx, dataType, plainKey)
	require.NoError(t, err, "Retrieve should not fail")
	assert.Equal(t, value, retrievedValue, "Retrieved value should match")
}

// TestPebbleRetrieveByPrefix tests the RetrieveByPrefix method for non-AOL data
func TestPebbleRetrieveByPrefix(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()

	// Create and initialize EthStore
	store, err := New(tempDir, 10, "test", false)
	require.NoError(t, err, "Failed to create EthStore")
	defer store.Close()

	// Create test data with the same prefix
	prefix := []byte("prefix-")
	dataType := AccountDataType // Non-AOL data type
	keyPrefix := byte(GetPrefixByDataType(dataType))

	// Store multiple key-value pairs
	numEntries := 5
	for i := 0; i < numEntries; i++ {
		plainKey := append([]byte{}, prefix...)
		plainKey = append(plainKey, []byte(fmt.Sprintf("%03d", i))...)
		fullKey := append([]byte{keyPrefix}, plainKey...)
		value := []byte(fmt.Sprintf("value-%d", i))

		err = store.Put(fullKey, value)
		require.NoError(t, err, "Failed to Put data for RetrieveByPrefix test")
	}

	// Use RetrieveByPrefix to fetch data
	ctx := context.Background()
	iter, err := store.RetrieveByPrefix(ctx, dataType, prefix)
	require.NoError(t, err, "RetrieveByPrefix should not fail")
	defer iter.Release()

	// Validate iterator results
	count := 0
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Verify that the key has the correct prefix (excluding the type prefix, which the iterator has already handled)
		assert.True(t, len(key) >= len(prefix), "Key should contain prefix")
		assert.Equal(t, prefix, key[:len(prefix)], "Key should start with prefix")

		// Validate value
		expectedValue := []byte(fmt.Sprintf("value-%d", count))
		assert.Equal(t, expectedValue, value, "Value should match expected")

		count++
	}
	require.NoError(t, iter.Error(), "Iterator should not have errors")
	assert.Equal(t, numEntries, count, "Iterator should return all entries")
}

// TestMixedAOLAndPebble tests mixed usage of AOL and Pebble storage
func TestMixedAOLAndPebble(t *testing.T) {
	// Create a temporary directory
	tempDir := t.TempDir()

	// Create and initialize EthStore
	store, err := New(tempDir, 10, "test", false)
	require.NoError(t, err, "Failed to create EthStore")
	defer store.Close()

	// Create a key-value pair for AOL data type (e.g., using HeaderDataType)
	headerKeyPrefix := byte(GetPrefixByDataType(HeaderDataType))
	headerNumber := uint64(12345)
	headerNumBytes := make([]byte, 8)
	// Using BigEndian to convert uint64 to bytes
	// binary.BigEndian.PutUint64(headerNumBytes, headerNumber)
	// Simplified implementation, using fixed values
	headerNumBytes = []byte{0, 0, 0, 0, 0, 0, 48, 57} // Equivalent to 12345
	headerKey := append([]byte{headerKeyPrefix}, headerNumBytes...)
	headerValue := []byte("test-header-value")

	// Create a key-value pair for non-AOL data type
	accountKeyPrefix := byte(GetPrefixByDataType(AccountDataType))
	accountKey := append([]byte{accountKeyPrefix}, []byte("test-account-key")...)
	accountValue := []byte("test-account-value")

	// Store AOL data
	err = store.Put(headerKey, headerValue)
	require.NoError(t, err, "Failed to Put AOL data")

	// Store non-AOL data
	err = store.Put(accountKey, accountValue)
	require.NoError(t, err, "Failed to Put non-AOL data")

	// Retrieve and validate AOL data
	retrievedHeaderValue, err := store.Get(headerKey)
	require.NoError(t, err, "Failed to Get AOL data")
	assert.Equal(t, headerValue, retrievedHeaderValue, "Retrieved AOL value should match")

	// Retrieve and validate non-AOL data
	retrievedAccountValue, err := store.Get(accountKey)
	require.NoError(t, err, "Failed to Get non-AOL data")
	assert.Equal(t, accountValue, retrievedAccountValue, "Retrieved non-AOL value should match")

	// Validate deletion operation
	err = store.Delete(accountKey)
	require.NoError(t, err, "Failed to Delete non-AOL data")

	_, err = store.Get(accountKey)
	assert.Error(t, err, "Should return error after deletion")
	assert.Equal(t, ErrNotFound, err, "Should return ErrNotFound after deletion")
}
