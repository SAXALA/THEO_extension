package ethstore

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestAOL(t *testing.T, recentN int) (*TxIndexAppendOnlyLog, string) {
	dir := t.TempDir()
	logger := log.New()
	aol, err := NewAppendOnlyLog(dir, recentN, logger)
	require.NoError(t, err)
	return aol, dir
}

func TestNewAppendOnlyLog(t *testing.T) {
	aol, dir := setupTestAOL(t, 10)
	defer aol.Close()

	assert.Equal(t, dir, aol.Path())
	assert.Equal(t, 10, aol.RecentN())
	assert.NotNil(t, aol)
}

func TestAppendAndGet(t *testing.T) {
	aol, _ := setupTestAOL(t, 10)
	defer aol.Close()

	kvs := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	err := aol.Append(1, kvs)
	require.NoError(t, err)

	val, found, err := aol.Get("key1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", val)

	val, found, err = aol.Get("key2")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value2", val)

	val, found, err = aol.Get("key3")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, "", val)
}

func TestAppendToNewBlock(t *testing.T) {
	aol, _ := setupTestAOL(t, 10)
	defer aol.Close()

	kvs := map[string]string{
		"key1": "value1",
	}

	blockID, err := aol.AppendToNewBlock(kvs)
	require.NoError(t, err)
	assert.Greater(t, blockID, uint64(0))

	val, found, err := aol.Get("key1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", val)
}

func TestDelete(t *testing.T) {
	aol, _ := setupTestAOL(t, 10)
	defer aol.Close()

	kvs := map[string]string{
		"key1": "value1",
	}
	err := aol.Append(1, kvs)
	require.NoError(t, err)

	err = aol.Delete("key1")
	require.NoError(t, err)

	val, found, err := aol.Get("key1")
	require.NoError(t, err)
	assert.True(t, found) // Found but it's a tombstone, so Get returns "", true?
	// Wait, let's check Get implementation.
	// Get returns "", true for deleted keys (tombstones).
	assert.Equal(t, "", val)
}

func TestGetByBlock(t *testing.T) {
	aol, _ := setupTestAOL(t, 10)
	defer aol.Close()

	kvs := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	err := aol.Append(1, kvs)
	require.NoError(t, err)

	retrievedKvs, err := aol.GetByBlock(1)
	require.NoError(t, err)
	assert.Equal(t, kvs, retrievedKvs)
}

func TestDeleteByPrefixInBlock(t *testing.T) {
	aol, _ := setupTestAOL(t, 10)
	defer aol.Close()

	kvs := map[string]string{
		"prefix_1": "value1",
		"prefix_2": "value2",
		"other_1":  "value3",
	}
	err := aol.Append(1, kvs)
	require.NoError(t, err)

	err = aol.DeleteByPrefixInBlock(1, "prefix_")
	require.NoError(t, err)

	// prefix_1 should be deleted
	val, found, err := aol.Get("prefix_1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "", val)

	// prefix_2 should be deleted
	val, found, err = aol.Get("prefix_2")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "", val)

	// other_1 should exist
	val, found, err = aol.Get("other_1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value3", val)
}

func TestReopen(t *testing.T) {
	dir := t.TempDir()
	logger := log.New()

	// Open and write
	aol, err := NewAppendOnlyLog(dir, 10, logger)
	require.NoError(t, err)

	kvs := map[string]string{"key1": "value1"}
	err = aol.Append(1, kvs)
	require.NoError(t, err)

	err = aol.Close()
	require.NoError(t, err)

	// Reopen
	aol2, err := NewAppendOnlyLog(dir, 10, logger)
	require.NoError(t, err)
	defer aol2.Close()

	val, found, err := aol2.Get("key1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", val)
}

func TestRecentNEviction(t *testing.T) {
	recentN := 2
	aol, _ := setupTestAOL(t, recentN)
	defer aol.Close()

	// Append 3 blocks
	for i := 1; i <= 3; i++ {
		kvs := map[string]string{
			fmt.Sprintf("key%d", i): fmt.Sprintf("value%d", i),
		}
		err := aol.Append(uint64(i), kvs)
		require.NoError(t, err)
	}

	// key1 (block 1) should be evicted from skiplist (memory), but still accessible from disk
	val, found, err := aol.Get("key1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", val)

	// key3 (block 3) should be in skiplist
	val, found, err = aol.Get("key3")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value3", val)
}

func TestFlushIndexBuffer(t *testing.T) {
	aol, _ := setupTestAOL(t, 10)
	defer aol.Close()

	kvs := map[string]string{"key1": "value1"}
	err := aol.Append(1, kvs)
	require.NoError(t, err)

	err = aol.FlushIndexBuffer()
	require.NoError(t, err)
}

func TestLargeData(t *testing.T) {
	aol, _ := setupTestAOL(t, 10)
	defer aol.Close()

	kvs := make(map[string]string)
	for i := 0; i < 1000; i++ {
		kvs[strconv.Itoa(i)] = "value" + strconv.Itoa(i)
	}

	err := aol.Append(1, kvs)
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		val, found, err := aol.Get(strconv.Itoa(i))
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "value"+strconv.Itoa(i), val)
	}
}
