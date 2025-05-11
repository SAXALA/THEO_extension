package ethstore

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/log"
)

// Helper function to create a temporary directory for testing
func setupTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "appendlog_test_") // Changed pattern slightly for clarity
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// Helper function to clean up the temporary directory
func cleanupTestDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Errorf("Failed to remove temp dir %s: %v", dir, err)
	}
}

// NewAppendOnlyLog is a test helper to create and initialize an AppendOnlyLog instance.
// It calls the actual NewAppendOnlyLog constructor from blockStore.go
// and fails the test if initialization is unsuccessful.
func NewAppendOnlyLogTest(t *testing.T, dir string, recentN int) (*AppendOnlyLog, func()) {
	t.Helper()

	testLogger := log.New()

	aol, err := NewAppendOnlyLog(dir, recentN, testLogger)
	if err != nil {
		t.Fatalf("Failed to create NewAppendOnlyLog(dir=%q, recentN=%d, logger): %v", dir, recentN, err)
	}
	if aol == nil {
		t.Fatalf("NewAppendOnlyLog(dir=%q, recentN=%d, logger) returned nil instance without error", dir, recentN)
	}
	// Return aol and the cleanup function that closes it
	return aol, func() {
		if err := aol.Close(); err != nil {
			t.Errorf("Failed to close AppendOnlyLog in cleanup: %v", err)
		}
	}
}

func TestAppendOnlyLog_NewAndClose(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	// Correctly capture and use the cleanup function
	aol, cleanup := NewAppendOnlyLogTest(t, dir, 10)
	defer cleanup() // This will call aol.Close()

	if aol == nil {
		t.Fatal("AppendOnlyLog instance is nil")
	}

	// Check if files were created
	if _, err := os.Stat(filepath.Join(dir, dataFileName)); os.IsNotExist(err) {
		t.Errorf("Data file %s was not created", dataFileName)
	}
	if _, err := os.Stat(filepath.Join(dir, indexMapFileName)); os.IsNotExist(err) {
		t.Errorf("Index map file %s was not created", indexMapFileName)
	}
}

func TestAppendOnlyLog_AppendAndGet(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)
	// Correctly capture and use the cleanup function
	aol, cleanup := NewAppendOnlyLogTest(t, dir, 10)
	defer cleanup()

	// Block 1
	kvs1 := map[string]string{"key1": "value1", "key2": "value2"}
	err := aol.Append(1, kvs1)
	if err != nil {
		t.Fatalf("Append block 1 failed: %v", err)
	}

	// Block 2
	kvs2 := map[string]string{"key3": "value3", "key1": "value1_updated"} // Update key1
	err = aol.Append(2, kvs2)
	if err != nil {
		t.Fatalf("Append block 2 failed: %v", err)
	}

	// Test Get (should retrieve latest values from indexed blocks)
	tests := []struct {
		key        string
		wantValue  string
		wantExists bool
		wantErr    bool
	}{
		{"key1", "value1_updated", true, false},
		{"key2", "value2", true, false},
		{"key3", "value3", true, false},
		{"key_nonexistent", "", false, false},
	}

	for _, tt := range tests {
		t.Run("Get_"+tt.key, func(t *testing.T) {
			val, exists, err := aol.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get(%q) error = %v, wantErr %v", tt.key, err, tt.wantErr)
				return
			}
			if exists != tt.wantExists {
				t.Errorf("Get(%q) exists = %v, wantExists %v", tt.key, exists, tt.wantExists)
			}
			if val != tt.wantValue {
				t.Errorf("Get(%q) value = %q, wantValue %q", tt.key, val, tt.wantValue)
			}
		})
	}
}

func TestAppendOnlyLog_GetByBlock(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)
	// Correctly capture and use the cleanup function, pass dir
	aol, cleanup := NewAppendOnlyLogTest(t, dir, 5)
	defer cleanup()

	kvs1 := map[string]string{"k1": "v1", "k2": "v2"}
	if err := aol.Append(1, kvs1); err != nil {
		t.Fatalf("Failed to append block 1: %v", err)
	}
	kvs2 := map[string]string{"k1": "v1_new", "k3": "v3"} // k1 updated in block 2
	if err := aol.Append(2, kvs2); err != nil {
		t.Fatalf("Failed to append block 2: %v", err)
	}

	tests := []struct {
		name          string
		blockID       uint64
		expectedKVs   map[string]string
		expectErr     bool
		expectedError string
	}{
		{
			name:        "get block 1",
			blockID:     1,
			expectedKVs: kvs1,
			expectErr:   false,
		},
		{
			name:        "get block 2",
			blockID:     2,
			expectedKVs: kvs2,
			expectErr:   false,
		},
		{
			name:          "get non-existent block",
			blockID:       3,
			expectedKVs:   nil,
			expectErr:     true,
			expectedError: "block ID 3 not found in index",
		},
		{
			name:          "get block 0 (non-existent)",
			blockID:       0,
			expectedKVs:   nil,
			expectErr:     true,
			expectedError: "block ID 0 not found in index",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Correct call to GetByBlock (1 argument, 2 return values)
			retrievedKVs, err := aol.GetByBlock(tt.blockID)
			if tt.expectErr {
				if err == nil {
					t.Errorf("Expected error %q, got nil", tt.expectedError)
				} else if !strings.Contains(err.Error(), tt.expectedError) {
					t.Errorf("Expected error containing %q, got %q", tt.expectedError, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("aol.GetByBlock(%d) failed: %v", tt.blockID, err)
			}

			if !reflect.DeepEqual(retrievedKVs, tt.expectedKVs) {
				t.Errorf("GetByBlock(%d) = %v, want %v", tt.blockID, retrievedKVs, tt.expectedKVs)
			}
		})
	}
}

func TestAppendOnlyLog_Delete(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)
	// Correctly capture and use the cleanup function, pass dir
	aol, cleanup := NewAppendOnlyLogTest(t, dir, 5)
	defer cleanup()

	if err := aol.Append(1, map[string]string{"delKey": "initialValue", "otherKey": "otherValue"}); err != nil {
		t.Fatalf("Failed to append block 1: %v", err)
	}

	// Correct call to Delete (1 argument)
	if err := aol.Delete("delKey"); err != nil {
		t.Fatalf("Failed to delete 'delKey': %v", err)
	}

	val, exists, err := aol.Get("delKey")
	if err != nil {
		t.Fatalf("Error getting 'delKey' after deletion: %v", err)
	}
	if !exists {
		t.Errorf("'delKey' should exist (as tombstone), but Get returned exists=false")
	}
	if val != "" {
		t.Errorf("Expected value for 'delKey' to be empty (tombstone), got '%s'", val)
	}

	// Correct call to GetByBlock
	kvsBlock1, err1 := aol.GetByBlock(1)
	if err1 != nil {
		t.Fatalf("GetByBlock(1) failed: %v", err1)
	}
	expectedKVsBlock1 := map[string]string{"delKey": "initialValue", "otherKey": "otherValue"}
	if !reflect.DeepEqual(kvsBlock1, expectedKVsBlock1) {
		t.Errorf("GetByBlock(1) after delete = %v, want %v", kvsBlock1, expectedKVsBlock1)
	}

	// Correct call to GetByBlock
	kvsBlock2, err2 := aol.GetByBlock(2) // Block 2 should contain the tombstone
	if err2 != nil {
		t.Fatalf("GetByBlock(2) failed: %v", err2)
	}
	expectedKVsBlock2 := map[string]string{"delKey": "__DELETED__"}
	if !reflect.DeepEqual(kvsBlock2, expectedKVsBlock2) {
		t.Errorf("GetByBlock(2) after delete = %v, want %v", kvsBlock2, expectedKVsBlock2)
	}

	// Correct call to Delete
	if err := aol.Delete("nonExistentKey"); err != nil {
		t.Fatalf("Failed to delete 'nonExistentKey': %v", err)
	}
}

func TestAppendOnlyLog_PersistenceAndReopen(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	// Correctly capture and use the cleanup function for aol1
	aol1, cleanup1 := NewAppendOnlyLogTest(t, dir, 2)
	// ... appends to aol1 ...
	if err := aol1.Append(1, map[string]string{"k_common": "vc1", "k1": "v1_b1"}); err != nil {
		t.Fatalf("Failed to append block 1: %v", err)
	}
	if err := aol1.Append(2, map[string]string{"k_common": "vc2", "k1": "v1_b2", "k2": "v2_b2"}); err != nil {
		t.Fatalf("Failed to append block 2: %v", err)
	}
	if err := aol1.Append(3, map[string]string{"k_common": "vc3", "k3": "v3_b3"}); err != nil {
		t.Fatalf("Failed to append block 3: %v", err)
	}

	cleanup1() // Explicitly call cleanup (which includes Close) for aol1 before reopening

	// Correctly capture and use the cleanup function for aol2
	aol2, cleanup2 := NewAppendOnlyLogTest(t, dir, 2)
	defer cleanup2()

	tests := []struct {
		name          string
		blockID       uint64
		key           string
		expectedValue string
		expectFound   bool
		expectedKVs   map[string]string
	}{
		{name: "Get k_common after reopen", key: "k_common", expectedValue: "vc3", expectFound: true},
		{name: "Get k1 after reopen", key: "k1", expectedValue: "v1_b2", expectFound: true},
		{name: "Get k2 after reopen", key: "k2", expectedValue: "v2_b2", expectFound: true},
		{name: "Get k3 after reopen", key: "k3", expectedValue: "v3_b3", expectFound: true},
		{name: "Get non-existent key after reopen", key: "kx", expectedValue: "", expectFound: false},
		{name: "GetByBlock 1 after reopen", blockID: 1, expectedKVs: map[string]string{"k_common": "vc1", "k1": "v1_b1"}},
		{name: "GetByBlock 2 after reopen", blockID: 2, expectedKVs: map[string]string{"k_common": "vc2", "k1": "v1_b2", "k2": "v2_b2"}},
		{name: "GetByBlock 3 after reopen", blockID: 3, expectedKVs: map[string]string{"k_common": "vc3", "k3": "v3_b3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.key != "" {
				val, exists, err := aol2.Get(tt.key)
				if err != nil {
					t.Fatalf("aol2.Get(%q) failed: %v", tt.key, err)
				}
				if exists != tt.expectFound {
					t.Errorf("aol2.Get(%q) exists = %v, want %v", tt.key, exists, tt.expectFound)
				}
				if val != tt.expectedValue {
					t.Errorf("aol2.Get(%q) value = %q, wantValue %q", tt.key, val, tt.expectedValue)
				}
			} else {
				// Correct call to GetByBlock
				retrievedKVs, err := aol2.GetByBlock(tt.blockID)
				if err != nil {
					t.Fatalf("aol2.GetByBlock(%d) failed: %v", tt.blockID, err)
				}
				if !reflect.DeepEqual(retrievedKVs, tt.expectedKVs) {
					t.Errorf("aol2.GetByBlock(%d) = %v, want %v", tt.blockID, retrievedKVs, tt.expectedKVs)
				}
			}
		})
	}
}

func TestAppendOnlyLog_SkiplistEviction(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)
	// Correctly capture and use the cleanup function
	aol, cleanup := NewAppendOnlyLogTest(t, dir, 2)
	defer cleanup()

	// Block 1
	if err := aol.Append(1, map[string]string{"k1": "v1_1"}); err != nil {
		t.Fatal(err)
	}
	// Check k1 is indexed (should be in block 1's data, accessible via skiplist)
	val1, exists1, err1 := aol.Get("k1")
	if err1 != nil {
		t.Fatalf("Error getting k1 after block 1: %v", err1)
	}
	if !exists1 || val1 != "v1_1" {
		kvs, _ := aol.GetByBlock(1)
		t.Fatalf("k1 should be indexed after block 1. Get k1: val='%s', exists=%v. Block 1 KVs: %v. Skiplist len: %d", val1, exists1, kvs, aol.skiplistIndex.Len())
	}

	// Block 2
	if err := aol.Append(2, map[string]string{"k2": "v2_2"}); err != nil {
		t.Fatal(err)
	}
	// Check k1 and k2 are indexed
	val2, exists2, err2 := aol.Get("k2")
	if err2 != nil {
		t.Fatalf("Error getting k2 after block 2: %v", err2)
	}
	if !exists2 || val2 != "v2_2" {
		t.Fatalf("k2 should be indexed after block 2. Get k2: val='%s', exists=%v", val2, exists2)
	}

	// Block 3 (should evict block 1 from index)
	if err := aol.Append(3, map[string]string{"k3": "v3_3"}); err != nil {
		t.Fatal(err)
	}

	// k1 should now be evicted from skiplist as block 1 is older than recentN=2
	// (latest is 3, recent are 3, 2. Block 1 is out)
	valEvicted, existsEvicted, errEvicted := aol.Get("k1")
	if errEvicted != nil {
		t.Fatalf("Error getting k1 after block 3 (expected eviction): %v", errEvicted)
	}
	if existsEvicted {
		t.Errorf("k1 should be evicted from skiplist after block 3, but Get found it (val='%s').", valEvicted)
	}

	// k2 (from block 2) should still be in skiplist
	val2After, exists2After, err2After := aol.Get("k2")
	if err2After != nil {
		t.Fatalf("Error getting k2 after block 3: %v", err2After)
	}
	if !exists2After || val2After != "v2_2" {
		t.Fatalf("k2 should still be indexed after block 3. Get k2: val='%s', exists=%v", val2After, exists2After)
	}
}

func TestAppendOnlyLog_AppendErrors(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)
	// Correctly capture and use the cleanup function
	aol, cleanup := NewAppendOnlyLogTest(t, dir, 10)
	defer cleanup()

	// Block 1 - OK
	if err := aol.Append(1, map[string]string{"k1": "v1"}); err != nil {
		t.Fatal(err)
	}

	// Error: Append duplicate block ID
	err := aol.Append(1, map[string]string{"k_new": "v_new"})
	if err == nil {
		t.Error("Expected error when appending duplicate block ID, got nil")
	}

	// Error: Append non-monotonic block ID
	err = aol.Append(0, map[string]string{"k_old": "v_old"})
	if err == nil {
		t.Error("Expected error when appending non-monotonic block ID, got nil")
	}

	// Append empty map - should be no-op
	err = aol.Append(2, map[string]string{})
	if err != nil {
		t.Errorf("Append empty map failed: %v", err)
	}
	if aol.latestBlockID != 1 { // latestBlockID should not have changed
		t.Errorf("latestBlockID changed after appending empty map: got %d, want 1", aol.latestBlockID)
	}
}
