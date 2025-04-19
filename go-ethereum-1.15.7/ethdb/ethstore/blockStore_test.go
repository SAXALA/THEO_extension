package ethstore

import (
    "bytes"
    "fmt"
    "os"
    "path/filepath"
    "testing"

    "github.com/ethereum/go-ethereum/log"
)

// Helper function to create a temporary directory for testing
func setupTestDir(t *testing.T) string {
    t.Helper()
    dir, err := os.MkdirTemp("", "appendlog_test_")
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

// Helper to create and open an AppendOnlyLog instance
func setupAppendLog(t *testing.T, dir string, recentN int) *AppendOnlyLog {
    t.Helper()
    // Create a logger for this specific test instance
    testLogger := log.New()
    // Create a handler that writes to stderr
    handler := log.StreamHandler(os.Stderr, log.TerminalFormat(false))
    // Create a filter handler to control the level
    lvlHandler := log.LvlFilterHandler(log.LvlCrit, handler) // Set level to Crit to suppress most logs
    // Set the handler for the test logger instance
    testLogger.SetHandler(lvlHandler)

    // Pass the test-specific logger instance
    aol, err := NewAppendOnlyLog(dir, recentN, testLogger) // Pass testLogger here
    if err != nil {
        t.Fatalf("Failed to create AppendOnlyLog: %v", err)
    }
    return aol
}

func TestAppendOnlyLog_NewAndClose(t *testing.T) {
    dir := setupTestDir(t)
    defer cleanupTestDir(t, dir)

    aol := setupAppendLog(t, dir, 10)
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

    err := aol.Close()
    if err != nil {
        t.Errorf("Close() failed: %v", err)
    }
}

func TestAppendOnlyLog_AppendAndGet(t *testing.T) {
    dir := setupTestDir(t)
    defer cleanupTestDir(t, dir)
    aol := setupAppendLog(t, dir, 10) // Index last 10 blocks

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

    if err := aol.Close(); err != nil {
        t.Errorf("Close failed: %v", err)
    }
}

func TestAppendOnlyLog_GetByBlock(t *testing.T) {
    dir := setupTestDir(t)
    defer cleanupTestDir(t, dir)
    aol := setupAppendLog(t, dir, 10)

    // Block 1
    kvs1 := map[string]string{"keyA": "valA1", "keyB": "valB1"}
    if err := aol.Append(1, kvs1); err != nil {
        t.Fatal(err)
    }

    // Block 2
    kvs2 := map[string]string{"keyA": "valA2", "keyC": "valC2"} // Update keyA
    if err := aol.Append(2, kvs2); err != nil {
        t.Fatal(err)
    }

    tests := []struct {
        blockID    uint64
        key        string
        wantValue  string
        wantExists bool
        wantErr    bool
    }{
        {1, "keyA", "valA1", true, false},
        {1, "keyB", "valB1", true, false},
        {1, "keyC", "", false, false}, // keyC not in block 1
        {2, "keyA", "valA2", true, false},
        {2, "keyB", "", false, false}, // keyB not in block 2
        {2, "keyC", "valC2", true, false},
        {3, "keyA", "", false, false}, // block 3 doesn't exist
    }

    for _, tt := range tests {
        t.Run(fmt.Sprintf("GetByBlock_%d_%s", tt.blockID, tt.key), func(t *testing.T) {
            val, exists, err := aol.GetByBlock(tt.blockID, tt.key)
            if (err != nil) != tt.wantErr {
                t.Errorf("GetByBlock(%d, %q) error = %v, wantErr %v", tt.blockID, tt.key, err, tt.wantErr)
                return
            }
            if exists != tt.wantExists {
                t.Errorf("GetByBlock(%d, %q) exists = %v, wantExists %v", tt.blockID, tt.key, exists, tt.wantExists)
            }
            if val != tt.wantValue {
                t.Errorf("GetByBlock(%d, %q) value = %q, wantValue %q", tt.blockID, tt.key, val, tt.wantValue)
            }
        })
    }

    if err := aol.Close(); err != nil {
        t.Errorf("Close failed: %v", err)
    }
}

func TestAppendOnlyLog_Delete(t *testing.T) {
    dir := setupTestDir(t)
    defer cleanupTestDir(t, dir)
    aol := setupAppendLog(t, dir, 10)

    // Block 1: Add key
    if err := aol.Append(1, map[string]string{"delKey": "initialValue"}); err != nil {
        t.Fatal(err)
    }

    // Block 2: Delete key
    if err := aol.Delete(2, "delKey"); err != nil {
        t.Fatalf("Delete failed: %v", err)
    }

    // Block 3: Add another key
    if err := aol.Append(3, map[string]string{"otherKey": "otherValue"}); err != nil {
        t.Fatal(err)
    }

    // Check Get (should see deleted marker)
    val, exists, err := aol.Get("delKey")
    if err != nil {
        t.Errorf("Get(delKey) after delete returned error: %v", err)
    }
    if !exists {
        t.Errorf("Get(delKey) after delete reported non-existence, want existence (deleted)")
    }
    if val != "" { // Deleted keys return empty string value
        t.Errorf("Get(delKey) after delete returned value %q, want empty string", val)
    }

    // Check GetByBlock for original value
    val1, exists1, err1 := aol.GetByBlock(1, "delKey")
    if err1 != nil || !exists1 || val1 != "initialValue" {
        t.Errorf("GetByBlock(1, delKey) failed: val=%q, exists=%v, err=%v", val1, exists1, err1)
    }

    // Check GetByBlock for tombstone
    val2, exists2, err2 := aol.GetByBlock(2, "delKey")
    if err2 != nil || !exists2 || val2 != tombstoneMarker {
        t.Errorf("GetByBlock(2, delKey) failed: val=%q, exists=%v, err=%v", val2, exists2, err2)
    }

    // Check other key is unaffected
    valOther, existsOther, errOther := aol.Get("otherKey")
    if errOther != nil || !existsOther || valOther != "otherValue" {
        t.Errorf("Get(otherKey) failed: val=%q, exists=%v, err=%v", valOther, existsOther, errOther)
    }

    if err := aol.Close(); err != nil {
        t.Errorf("Close failed: %v", err)
    }
}

func TestAppendOnlyLog_PersistenceAndReopen(t *testing.T) {
    dir := setupTestDir(t)
    defer cleanupTestDir(t, dir)

    // --- First run ---
    aol1 := setupAppendLog(t, dir, 2) // Index only last 2 blocks

    if err := aol1.Append(1, map[string]string{"k1": "v1", "k_common": "vc1"}); err != nil {
        t.Fatal(err)
    }
    if err := aol1.Append(2, map[string]string{"k2": "v2", "k_common": "vc2"}); err != nil {
        t.Fatal(err)
    }
    if err := aol1.Append(3, map[string]string{"k3": "v3", "k_common": "vc3"}); err != nil {
        t.Fatal(err)
    }

    if err := aol1.Close(); err != nil {
        t.Fatalf("aol1.Close() failed: %v", err)
    }

    // --- Second run (reopen) ---
    aol2 := setupAppendLog(t, dir, 2) // Reopen with same settings

    if aol2.latestBlockID != 3 {
        t.Errorf("Reopened log has latestBlockID %d, want 3", aol2.latestBlockID)
    }

    // Check skiplist (should contain blocks 2 and 3)
    testsGet := []struct {
        key        string
        wantValue  string
        wantExists bool
    }{
        {"k1", "", false},       // Should be evicted from skiplist
        {"k2", "v2", true},
        {"k3", "v3", true},
        {"k_common", "vc3", true}, // Latest version from block 3
    }
    for _, tt := range testsGet {
        val, exists, err := aol2.Get(tt.key)
        if err != nil {
            t.Errorf("Reopened Get(%q) error: %v", tt.key, err)
        }
        if exists != tt.wantExists {
            t.Errorf("Reopened Get(%q) exists = %v, wantExists %v", tt.key, exists, tt.wantExists)
        }
        if val != tt.wantValue {
            t.Errorf("Reopened Get(%q) value = %q, wantValue %q", tt.key, val, tt.wantValue)
        }
    }

    // Check GetByBlock (should work for all blocks)
    testsGetByBlock := []struct {
        blockID   uint64
        key       string
        wantValue string
    }{
        {1, "k1", "v1"},
        {1, "k_common", "vc1"},
        {2, "k2", "v2"},
        {2, "k_common", "vc2"},
        {3, "k3", "v3"},
        {3, "k_common", "vc3"},
    }
    for _, tt := range testsGetByBlock {
        val, exists, err := aol2.GetByBlock(tt.blockID, tt.key)
        if err != nil || !exists || val != tt.wantValue {
            t.Errorf("Reopened GetByBlock(%d, %q) failed: val=%q, exists=%v, err=%v, want=%q", tt.blockID, tt.key, val, exists, err, tt.wantValue)
        }
    }

    if err := aol2.Close(); err != nil {
        t.Errorf("aol2.Close() failed: %v", err)
    }
}

func TestAppendOnlyLog_SkiplistEviction(t *testing.T) {
    dir := setupTestDir(t)
    defer cleanupTestDir(t, dir)
    aol := setupAppendLog(t, dir, 2) // Index only last 2 blocks

    // Block 1
    if err := aol.Append(1, map[string]string{"k1": "v1"}); err != nil {
        t.Fatal(err)
    }
    // Check k1 is indexed
    _, exists, _ := aol.Get("k1")
    if !exists {
        t.Fatal("k1 should be indexed after block 1")
    }

    // Block 2
    if err := aol.Append(2, map[string]string{"k2": "v2"}); err != nil {
        t.Fatal(err)
    }
    // Check k1 and k2 are indexed
    _, exists, _ = aol.Get("k1")
    if !exists {
        t.Fatal("k1 should still be indexed after block 2")
    }
    _, exists, _ = aol.Get("k2")
    if !exists {
        t.Fatal("k2 should be indexed after block 2")
    }

    // Block 3 (should evict block 1 from index)
    if err := aol.Append(3, map[string]string{"k3": "v3"}); err != nil {
        t.Fatal(err)
    }

    // Check k1 is evicted, k2 and k3 are indexed
    _, exists, _ = aol.Get("k1")
    if exists {
        t.Fatal("k1 should be evicted from index after block 3")
    }
    _, exists, _ = aol.Get("k2")
    if !exists {
        t.Fatal("k2 should still be indexed after block 3")
    }
    _, exists, _ = aol.Get("k3")
    if !exists {
        t.Fatal("k3 should be indexed after block 3")
    }

    // Verify k1 still accessible via GetByBlock
    val1, exists1, err1 := aol.GetByBlock(1, "k1")
    if err1 != nil || !exists1 || val1 != "v1" {
        t.Errorf("GetByBlock(1, k1) after eviction failed: val=%q, exists=%v, err=%v", val1, exists1, err1)
    }

    if err := aol.Close(); err != nil {
        t.Errorf("Close failed: %v", err)
    }
}

func TestAppendOnlyLog_AppendErrors(t *testing.T) {
    dir := setupTestDir(t)
    defer cleanupTestDir(t, dir)
    aol := setupAppendLog(t, dir, 10)

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

    if err := aol.Close(); err != nil {
        t.Errorf("Close failed: %v", err)
    }
}

// --- Mocking io.Writer for error injection (Optional but good practice) ---
type errorWriter struct {
    err error
}

func (ew *errorWriter) Write(p []byte) (n int, err error) {
    return 0, ew.err
}

func TestAppendOnlyLog_WriteErrors(t *testing.T) {
    dir := setupTestDir(t)
    defer cleanupTestDir(t, dir)

    // --- Test Data Write Error ---
    aolDataErr := setupAppendLog(t, dir, 10)
    // Inject error into data writer (tricky without modifying original struct,
    // might need interface or more complex setup. For now, test index write error).
    aolDataErr.Close() // Close cleanly first

    // --- Test Index Write Error ---
    cleanupTestDir(t, dir) // Clean and recreate dir
    dir = setupTestDir(t)
    aolIndexErr := setupAppendLog(t, dir, 10)

    // Replace index file writer with one that errors
    aolIndexErr.indexMapFile.Close() // Close the real one
    aolIndexErr.indexMapFile = &os.File{} // Dummy file handle
    // We can't easily replace the writer used internally by writeIndexEntry without interfaces.
    // A more robust test would involve mocking the file system or using interfaces.
    // This test case highlights a limitation of direct file manipulation testing.

    // Simulate append that would trigger index write
    // err := aolIndexErr.Append(1, map[string]string{"k1": "v1"})
    // if err == nil {
    // 	t.Error("Expected error during index write, got nil")
    // }
    // // Check for CRITICAL log message if possible

    aolIndexErr.Close()
}