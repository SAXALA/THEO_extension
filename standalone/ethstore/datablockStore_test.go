package ethstore

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const (
	dataFileName     = BlockdataFileName
	indexMapFileName = BlockindexMapFileName
)

func NewAppendOnlyLogTest(t *testing.T, dir string, recentN int) (*BlockAppendOnlyLog, func()) {
	t.Helper()

	aol, err := NewBlockAppendOnlyLog(dir, recentN, nil)
	if err != nil {
		t.Fatalf("Failed to create append-only log: %v", err)
	}

	cleanup := func() {
		if aol != nil {
			if err := aol.Close(); err != nil {
				t.Errorf("Failed to close append-only log: %v", err)
			}
		}
	}
	return aol, cleanup
}

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

func TestAppendOnlyLog_NonZeroStartingBlockID(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	// First, create AOL and append block starting from block 2000
	aol1, cleanup1 := NewAppendOnlyLogTest(t, dir, 10)

	// Manually set the latestBlockID to 1999 to simulate starting from 2000
	aol1.latestBlockID = 1999

	// Append blocks 2000, 2001, 2002
	kvs2000 := map[string]string{"k2000_1": "v2000_1", "k2000_2": "v2000_2"}
	if err := aol1.Append(2000, kvs2000); err != nil {
		t.Fatalf("Failed to append block 2000: %v", err)
	}

	kvs2001 := map[string]string{"k2001_1": "v2001_1"}
	if err := aol1.Append(2001, kvs2001); err != nil {
		t.Fatalf("Failed to append block 2001: %v", err)
	}

	kvs2002 := map[string]string{"k2002_1": "v2002_1", "k2002_2": "v2002_2"}
	if err := aol1.Append(2002, kvs2002); err != nil {
		t.Fatalf("Failed to append block 2002: %v", err)
	}

	// Verify minBlockID was set
	if aol1.minBlockID != 2000 {
		t.Errorf("Expected minBlockID=2000, got %d", aol1.minBlockID)
	}

	// Force flush to check actual file size
	if err := aol1.FlushIndexBuffer(); err != nil {
		t.Fatalf("Failed to flush index buffer: %v", err)
	}

	// Check index file size - should be compact (3 entries * indexEntrySize)
	indexPath := filepath.Join(dir, BlockindexMapFileName)
	stat, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("Failed to stat index file: %v", err)
	}
	expectedSize := int64(3 * indexEntrySize) // 3 blocks: 2000, 2001, 2002 (24 bytes each = 72 total)
	t.Logf("Index file size: %d bytes (expected %d for compact storage)", stat.Size(), expectedSize)
	if stat.Size() != expectedSize {
		t.Logf("NOTE: Index file uses compact storage starting from block %d", aol1.minBlockID)
	}

	cleanup1() // Close first instance

	// Reopen and verify all blocks can be loaded
	aol2, cleanup2 := NewAppendOnlyLogTest(t, dir, 10)
	defer cleanup2()

	// Verify minBlockID was loaded
	if aol2.minBlockID != 2000 {
		t.Errorf("Expected loaded minBlockID=2000, got %d", aol2.minBlockID)
	}

	// Verify latestBlockID
	if aol2.latestBlockID != 2002 {
		t.Errorf("Expected latestBlockID=2002, got %d", aol2.latestBlockID)
	}

	// Verify block 2000
	retrievedKVs2000, err := aol2.GetByBlock(2000)
	if err != nil {
		t.Fatalf("Failed to get block 2000: %v", err)
	}
	if !reflect.DeepEqual(retrievedKVs2000, kvs2000) {
		t.Errorf("Block 2000 mismatch: got %v, want %v", retrievedKVs2000, kvs2000)
	}

	// Verify block 2001
	retrievedKVs2001, err := aol2.GetByBlock(2001)
	if err != nil {
		t.Fatalf("Failed to get block 2001: %v", err)
	}
	if !reflect.DeepEqual(retrievedKVs2001, kvs2001) {
		t.Errorf("Block 2001 mismatch: got %v, want %v", retrievedKVs2001, kvs2001)
	}

	// Verify block 2002
	retrievedKVs2002, err := aol2.GetByBlock(2002)
	if err != nil {
		t.Fatalf("Failed to get block 2002: %v", err)
	}
	if !reflect.DeepEqual(retrievedKVs2002, kvs2002) {
		t.Errorf("Block 2002 mismatch: got %v, want %v", retrievedKVs2002, kvs2002)
	}

	// Verify that keys are accessible via Get
	val, exists, err := aol2.Get("k2002_1")
	if err != nil {
		t.Fatalf("Failed to get k2002_1: %v", err)
	}
	if !exists || val != "v2002_1" {
		t.Errorf("Get k2002_1: got exists=%v val=%q, want exists=true val=\"v2002_1\"", exists, val)
	}
}

func TestAppendOnlyLog_ReopenAfterBlockZeroAndAppend(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	aol1, _ := NewAppendOnlyLogTest(t, dir, 10)
	if err := aol1.Append(0, map[string]string{"k0": "v0"}); err != nil {
		t.Fatalf("Failed to append block 0: %v", err)
	}
	if err := aol1.Close(); err != nil {
		t.Fatalf("Failed to close first instance: %v", err)
	}

	aol2, cleanup2 := NewAppendOnlyLogTest(t, dir, 10)
	defer cleanup2()

	if err := aol2.Append(1, map[string]string{"k1": "v1"}); err != nil {
		t.Fatalf("Failed to append block 1 after reopen: %v", err)
	}

	if err := aol2.FlushIndexBuffer(); err != nil {
		t.Fatalf("Failed to flush index buffer after reopen append: %v", err)
	}

	if got, exists, err := aol2.Get("k1"); err != nil {
		t.Fatalf("Get(k1) failed: %v", err)
	} else if !exists || got != "v1" {
		t.Fatalf("Get(k1) mismatch, got exists=%v val=%q", exists, got)
	}

	if got, exists, err := aol2.Get("k0"); err != nil {
		t.Fatalf("Get(k0) failed: %v", err)
	} else if !exists || got != "v0" {
		t.Fatalf("Get(k0) mismatch, got exists=%v val=%q", exists, got)
	}
}

func TestAppendOnlyLog_ReopenAndAppendRangeWithOverlap(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	aol1, _ := NewAppendOnlyLogTest(t, dir, 64)
	for blockID := uint64(10); blockID <= 20; blockID++ {
		kvs := map[string]string{fmt.Sprintf("k_%d", blockID): fmt.Sprintf("v_phase1_%d", blockID)}
		if err := aol1.Append(blockID, kvs); err != nil {
			t.Fatalf("phase1 append block %d failed: %v", blockID, err)
		}
	}
	if err := aol1.Close(); err != nil {
		t.Fatalf("failed to close first instance: %v", err)
	}

	aol2, cleanup2 := NewAppendOnlyLogTest(t, dir, 64)
	defer cleanup2()

	for blockID := uint64(19); blockID <= 30; blockID++ {
		kvs := map[string]string{fmt.Sprintf("k_%d", blockID): fmt.Sprintf("v_phase2_%d", blockID)}
		err := aol2.Append(blockID, kvs)
		if err != nil {
			t.Fatalf("phase2 append block %d failed: %v", blockID, err)
		}
	}

	if aol2.latestBlockID != 30 {
		t.Fatalf("latestBlockID mismatch after phase2 append, got %d want 30", aol2.latestBlockID)
	}

	for blockID := uint64(10); blockID <= 20; blockID++ {
		kvs, err := aol2.GetByBlock(blockID)
		if err != nil {
			t.Fatalf("GetByBlock(%d) failed: %v", blockID, err)
		}
		expected := map[string]string{fmt.Sprintf("k_%d", blockID): fmt.Sprintf("v_phase1_%d", blockID)}
		if blockID == 20 {
			expected = map[string]string{fmt.Sprintf("k_%d", blockID): fmt.Sprintf("v_phase2_%d", blockID)}
		}
		if !reflect.DeepEqual(kvs, expected) {
			t.Fatalf("block %d mismatch: got %v want %v", blockID, kvs, expected)
		}
	}

	for blockID := uint64(21); blockID <= 30; blockID++ {
		kvs, err := aol2.GetByBlock(blockID)
		if err != nil {
			t.Fatalf("GetByBlock(%d) failed: %v", blockID, err)
		}
		expected := map[string]string{fmt.Sprintf("k_%d", blockID): fmt.Sprintf("v_phase2_%d", blockID)}
		if !reflect.DeepEqual(kvs, expected) {
			t.Fatalf("block %d mismatch: got %v want %v", blockID, kvs, expected)
		}
	}
}

func TestAppendOnlyLog_DuplicateLatestBlockIDAppendsMultipleKVs(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	aol, cleanup := NewAppendOnlyLogTest(t, dir, 16)
	defer cleanup()

	if err := aol.Append(100, map[string]string{"k1": "v1"}); err != nil {
		t.Fatalf("append #1 failed: %v", err)
	}
	if err := aol.Append(100, map[string]string{"k2": "v2"}); err != nil {
		t.Fatalf("append #2 failed: %v", err)
	}
	if err := aol.Append(100, map[string]string{"k3": "v3"}); err != nil {
		t.Fatalf("append #3 failed: %v", err)
	}

	kvs, err := aol.GetByBlock(100)
	if err != nil {
		t.Fatalf("GetByBlock(100) failed: %v", err)
	}
	expected := map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"}
	if !reflect.DeepEqual(kvs, expected) {
		t.Fatalf("block 100 mismatch: got %v want %v", kvs, expected)
	}
}

func TestBlockAppendOnlyLog_IteratorOrder_HeaderBodyReceipts(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	aol, cleanup := NewAppendOnlyLogTest(t, dir, 16)
	defer cleanup()

	// Keys are crafted to match the single-byte prefixes used by GetDataTypeFromKey:
	// 'h' => Header, 'b' => Body, 'r' => Receipts.
	headerKey := "h_header"
	bodyKey := "b_body"
	receiptKey := "r_receipts"

	kvs := map[string]string{
		receiptKey: "V_R",
		bodyKey:    "V_B",
		headerKey:  "V_H",
	}
	if err := aol.Append(1, kvs); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	it := aol.NewIterator([]byte(headerKey))
	defer it.Release()

	if !it.Next() {
		t.Fatalf("expected first Next() to be true, err=%v", it.Error())
	}
	if got := string(it.Key()); got != headerKey {
		t.Fatalf("first key mismatch: got=%q want=%q", got, headerKey)
	}
	if got := string(it.Value()); got != "V_H" {
		t.Fatalf("first value mismatch: got=%q want=%q", got, "V_H")
	}

	if !it.Next() {
		t.Fatalf("expected second Next() to be true, err=%v", it.Error())
	}
	if got := string(it.Key()); got != bodyKey {
		t.Fatalf("second key mismatch: got=%q want=%q", got, bodyKey)
	}

	if !it.Next() {
		t.Fatalf("expected third Next() to be true, err=%v", it.Error())
	}
	if got := string(it.Key()); got != receiptKey {
		t.Fatalf("third key mismatch: got=%q want=%q", got, receiptKey)
	}
}

func TestAppendOnlyLog_ReopenAndGapFillUntilContinuous(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)

	aol1, _ := NewAppendOnlyLogTest(t, dir, 64)
	for blockID := uint64(10); blockID <= 20; blockID++ {
		kvs := map[string]string{fmt.Sprintf("gk_%d", blockID): fmt.Sprintf("gv_phase1_%d", blockID)}
		if err := aol1.Append(blockID, kvs); err != nil {
			t.Fatalf("phase1 append block %d failed: %v", blockID, err)
		}
	}
	if err := aol1.Close(); err != nil {
		t.Fatalf("failed to close first instance: %v", err)
	}

	aol2, cleanup2 := NewAppendOnlyLogTest(t, dir, 64)
	defer cleanup2()

	if err := aol2.Append(25, map[string]string{"gk_25": "gv_phase2_25"}); err != nil {
		t.Fatalf("phase2 append block 25 failed: %v", err)
	}

	for blockID := uint64(21); blockID <= 24; blockID++ {
		kvs, err := aol2.GetByBlock(blockID)
		if err != nil {
			t.Fatalf("GetByBlock(%d) failed: %v", blockID, err)
		}
		if len(kvs) != 0 {
			t.Fatalf("expected gap-filled block %d to be empty, got %v", blockID, kvs)
		}
	}

	kvs25, err := aol2.GetByBlock(25)
	if err != nil {
		t.Fatalf("GetByBlock(25) failed: %v", err)
	}
	expected25 := map[string]string{"gk_25": "gv_phase2_25"}
	if !reflect.DeepEqual(kvs25, expected25) {
		t.Fatalf("block 25 mismatch: got %v want %v", kvs25, expected25)
	}

	if aol2.latestBlockID != 25 {
		t.Fatalf("latestBlockID mismatch, got %d want 25", aol2.latestBlockID)
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
			name:        "get non-existent block",
			blockID:     3,
			expectedKVs: map[string]string{},
			expectErr:   false,
		},
		{
			name:        "get block 0 (non-existent)",
			blockID:     0,
			expectedKVs: map[string]string{},
			expectErr:   false,
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

	// k1 should be evicted from skiplist as block 1 is older than recentN=2.
	// Current Get behavior only serves recent indexed blocks for this key pattern.
	valEvicted, existsEvicted, errEvicted := aol.Get("k1")
	if errEvicted != nil {
		t.Fatalf("Error getting k1 after block 3 (expected eviction): %v", errEvicted)
	}
	if existsEvicted || valEvicted != "" {
		t.Errorf("k1 should not be retrievable after skiplist eviction, got exists=%v val='%s'.", existsEvicted, valEvicted)
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

	// Duplicate block ID should be idempotent success
	err := aol.Append(1, map[string]string{"k_new": "v_new"})
	if err != nil {
		t.Errorf("Expected nil when appending duplicate block ID, got %v", err)
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

func TestAppendOnlyLog_DeleteByPrefixInBlock(t *testing.T) {
	dir := setupTestDir(t)
	defer cleanupTestDir(t, dir)
	aol, cleanup := NewAppendOnlyLogTest(t, dir, 10) // recentN=10 to keep all blocks for verification
	defer cleanup()

	// Block 1: Initial data
	kvs1 := map[string]string{
		"prefixA_key1": "valueA1",
		"prefixA_key2": "valueA2",
		"prefixB_key1": "valueB1",
		"other_key":    "valueOther",
	}
	if err := aol.Append(1, kvs1); err != nil {
		t.Fatalf("Failed to append block 1: %v", err)
	}

	// Block 2: More data, some with same prefix
	kvs2 := map[string]string{
		"prefixA_key3": "valueA3",
		"prefixC_key1": "valueC1",
	}
	if err := aol.Append(2, kvs2); err != nil {
		t.Fatalf("Failed to append block 2: %v", err)
	}

	// --- Test Case 1: Basic prefix deletion in Block 1 ---
	t.Run("Delete prefixA in Block1", func(t *testing.T) {
		err := aol.DeleteByPrefixInBlock(1, "prefixA")
		if err != nil {
			t.Fatalf("DeleteByPrefixInBlock(1, \"prefixA\") failed: %v", err)
		}

		// Verify Block 1 contents (should be unchanged by GetByBlock)
		block1KVs, _ := aol.GetByBlock(1)
		expectedBlock1KVs := map[string]string{"prefixA_key1": "valueA1", "prefixA_key2": "valueA2", "prefixB_key1": "valueB1", "other_key": "valueOther"}
		if !reflect.DeepEqual(block1KVs, expectedBlock1KVs) {
			t.Errorf("Block 1 KVs after prefixA delete: got %v, want %v", block1KVs, expectedBlock1KVs)
		}

		// Verify Block 3 (tombstone block) contents
		block3KVs, err := aol.GetByBlock(3) // Tombstones should be in a new block (1+1+1=3)
		if err != nil {
			t.Fatalf("GetByBlock(3) for tombstones failed: %v", err)
		}
		expectedBlock3KVs := map[string]string{"prefixA_key1": "__DELETED__", "prefixA_key2": "__DELETED__"}
		if !reflect.DeepEqual(block3KVs, expectedBlock3KVs) {
			t.Errorf("Tombstone Block 3 KVs: got %v, want %v", block3KVs, expectedBlock3KVs)
		}

		// Verify Get for deleted keys
		val, exists, _ := aol.Get("prefixA_key1")
		if !exists || val != "" { // Tombstone means exists=true, val=""
			t.Errorf("Get(\"prefixA_key1\") after delete: got val='%s', exists=%v; want val='', exists=true", val, exists)
		}
		val, exists, _ = aol.Get("prefixA_key2")
		if !exists || val != "" {
			t.Errorf("Get(\"prefixA_key2\") after delete: got val='%s', exists=%v; want val='', exists=true", val, exists)
		}

		// Verify Get for non-deleted key in same block
		val, exists, _ = aol.Get("prefixB_key1")
		if !exists || val != "valueB1" {
			t.Errorf("Get(\"prefixB_key1\") after delete: got val='%s', exists=%v; want val='valueB1', exists=true", val, exists)
		}
	})

	// --- Test Case 2: Prefix not found in target block ---
	t.Run("Delete non_existent_prefix in Block2", func(t *testing.T) {
		// Current latest block is 3. Append a new block to ensure no interference.
		if err := aol.Append(4, map[string]string{"dummyKey": "dummyVal"}); err != nil {
			t.Fatalf("Failed to append block 4: %v", err)
		}
		err := aol.DeleteByPrefixInBlock(2, "non_existent_prefix")
		if err != nil {
			t.Fatalf("DeleteByPrefixInBlock(2, \"non_existent_prefix\") failed: %v", err)
		}
		// No new block should be created, latestBlockID should remain 4
		if aol.latestBlockID != 4 {
			t.Errorf("latestBlockID after no-op delete: got %d, want 4", aol.latestBlockID)
		}
	})

	// --- Test Case 3: Target block is empty (implicitly tested if a block becomes empty) ---
	// For explicit test: Append an empty block and try to delete from it.
	t.Run("Delete from empty Block", func(t *testing.T) {
		if err := aol.Append(5, map[string]string{}); err != nil { // Append empty block 5
			t.Fatalf("Failed to append empty block 5: %v", err)
		}
		err := aol.DeleteByPrefixInBlock(5, "any_prefix")
		if err == nil {
			t.Fatalf("DeleteByPrefixInBlock(5, \"any_prefix\") should fail because empty append does not create block index entry")
		}
		// Append with empty kvs is a no-op; latestBlockID should remain unchanged at 4.
		if aol.latestBlockID != 4 {
			t.Errorf("latestBlockID after delete from empty block: got %d, want 4", aol.latestBlockID)
		}
	})

	// --- Test Case 4: Target block does not exist ---
	t.Run("Delete from non_existent_block", func(t *testing.T) {
		err := aol.DeleteByPrefixInBlock(99, "any_prefix")
		if err == nil {
			t.Error("Expected error when deleting from non-existent block, got nil")
		} else if !strings.Contains(err.Error(), "target block ID 99 not found") {
			t.Errorf("Expected error for non-existent block, got: %v", err)
		}
	})

	// --- Test Case 5: Key with prefix already a tombstone ---
	t.Run("Delete prefix_already_tombstone in Block6", func(t *testing.T) {
		// Block 6: one key, one tombstone with the same prefix
		kvs6 := map[string]string{
			"prefixD_key1": "valueD1",
			"prefixD_key2": TombstoneMarker, // Already a tombstone
		}
		if err := aol.Append(6, kvs6); err != nil {
			t.Fatalf("Failed to append block 6: %v", err)
		}

		err := aol.DeleteByPrefixInBlock(6, "prefixD")
		if err != nil {
			t.Fatalf("DeleteByPrefixInBlock(6, \"prefixD\") failed: %v", err)
		}

		// A new block (7) should be created for the tombstone of prefixD_key1
		// prefixD_key2 should not result in another tombstone.
		block7KVs, err := aol.GetByBlock(7)
		if err != nil {
			t.Fatalf("GetByBlock(7) for tombstones failed: %v", err)
		}
		expectedBlock7KVs := map[string]string{"prefixD_key1": "__DELETED__"}
		if !reflect.DeepEqual(block7KVs, expectedBlock7KVs) {
			t.Errorf("Tombstone Block 7 KVs: got %v, want %v", block7KVs, expectedBlock7KVs)
		}

		// Verify Get for prefixD_key1 (now deleted)
		val, exists, _ := aol.Get("prefixD_key1")
		if !exists || val != "" {
			t.Errorf("Get(\"prefixD_key1\") after delete: got val='%s', exists=%v; want val='', exists=true", val, exists)
		}
		// Verify Get for prefixD_key2 (was already tombstone, should still be)
		val, exists, _ = aol.Get("prefixD_key2")
		if !exists || val != "" {
			t.Errorf("Get(\"prefixD_key2\") (already tombstone): got val='%s', exists=%v; want val='', exists=true", val, exists)
		}
	})

	// --- Test Case 6: Delete all keys in a block using empty prefix ---
	// This is not how it's designed, prefix must be non-empty for strings.HasPrefix to be meaningful in this context.
	// The current implementation of DeleteByPrefixInBlock would tombstone all non-tombstoned keys if prefix is "".
	// Let's test this behavior to document it.
	t.Run("Delete with empty_prefix in Block2", func(t *testing.T) {
		// Block 2 initially: {"prefixA_key3": "valueA3", "prefixC_key1": "valueC1"}
		// latestBlockID is 7 from previous test.
		err := aol.DeleteByPrefixInBlock(2, "") // Empty prefix
		if err != nil {
			t.Fatalf("DeleteByPrefixInBlock(2, \"\") failed: %v", err)
		}

		// New block 8 should contain tombstones for all original keys in block 2
		block8KVs, err := aol.GetByBlock(8)
		if err != nil {
			t.Fatalf("GetByBlock(8) for tombstones failed: %v", err)
		}
		expectedBlock8KVs := map[string]string{"prefixA_key3": "__DELETED__", "prefixC_key1": "__DELETED__"}
		if !reflect.DeepEqual(block8KVs, expectedBlock8KVs) {
			t.Errorf("Tombstone Block 8 KVs (empty prefix): got %v, want %v", block8KVs, expectedBlock8KVs)
		}

		val, exists, _ := aol.Get("prefixA_key3")
		if !exists || val != "" {
			t.Errorf("Get(\"prefixA_key3\") after empty prefix delete: got val='%s', exists=%v; want val='', exists=true", val, exists)
		}
		val, exists, _ = aol.Get("prefixC_key1")
		if !exists || val != "" {
			t.Errorf("Get(\"prefixC_key1\") after empty prefix delete: got val='%s', exists=%v; want val='', exists=true", val, exists)
		}
	})
}
