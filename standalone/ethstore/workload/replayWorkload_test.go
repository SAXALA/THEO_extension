package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
	datatypepkg "github.com/tinoryj/EthStore/standalone/ethstore/datatype"
	"github.com/tinoryj/EthStore/standalone/ethstore/pebblestore"
	prefixdb "github.com/tinoryj/EthStore/standalone/ethstore/prefixdb"
)

type fakeReplayBackend struct {
	puts    [][]byte
	deletes [][]byte
	gets    [][]byte
	commits int
	dirty   bool
	getFunc func([]byte, ethstore.DataType) ([]byte, error)
}

func (b *fakeReplayBackend) Name() string { return "fake" }

func (b *fakeReplayBackend) Get(key []byte, dataType ethstore.DataType) ([]byte, error) {
	b.gets = append(b.gets, append([]byte(nil), key...))
	if b.getFunc != nil {
		return b.getFunc(key, dataType)
	}
	return nil, ethstore.ErrNotFound
}

func (b *fakeReplayBackend) StagePut(key, _ []byte, _ ethstore.DataType) error {
	b.puts = append(b.puts, append([]byte(nil), key...))
	b.dirty = true
	return nil
}

func (b *fakeReplayBackend) StageDelete(key []byte, _ ethstore.DataType) error {
	b.deletes = append(b.deletes, append([]byte(nil), key...))
	b.dirty = true
	return nil
}

func (b *fakeReplayBackend) CommitBlock() error {
	if b.dirty {
		b.commits++
		b.dirty = false
	}
	return nil
}

func (b *fakeReplayBackend) NewIterator(_, _ []byte) replayIter { return noopIter{} }
func (b *fakeReplayBackend) PrintCommitStats()                  {}
func (b *fakeReplayBackend) Close()                             {}

func writeTraceFile(t *testing.T, lines ...string) string {
	t.Helper()
	tracePath := filepath.Join(t.TempDir(), "trace.log")
	content := ""
	for _, line := range lines {
		content += line + "\n"
	}
	if err := os.WriteFile(tracePath, []byte(content), 0o644); err != nil {
		t.Fatalf("write trace file failed: %v", err)
	}
	return tracePath
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	origStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe failed: %v", err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = origStdout
	}()

	outCh := make(chan string, 1)
	go func() {
		buf, readErr := io.ReadAll(reader)
		if readErr != nil {
			outCh <- fmt.Sprintf("read stdout failed: %v", readErr)
			return
		}
		outCh <- string(buf)
	}()

	fn()
	_ = writer.Close()
	return <-outCh
}

func TestDescribeEthstoreOpenedStores(t *testing.T) {
	tests := []struct {
		name   string
		dbType DBType
		want   string
	}{
		{name: "aol", dbType: AOL, want: "aol only"},
		{name: "prefixdb", dbType: PrefixDB, want: "prefixdb+pebble"},
		{name: "pebble", dbType: Pebble, want: "all"},
		{name: "all", dbType: allDBTypes, want: "all"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := describeEthstoreOpenedStores(tt.dbType); got != tt.want {
				t.Fatalf("describeEthstoreOpenedStores(%v) = %q, want %q", tt.dbType, got, tt.want)
			}
		})
	}
}

type fakeGetterStore struct {
	value   []byte
	err     error
	getCall int
}

type fakeAccountKeyLookup struct {
	value []byte
	err   error
	seen  []byte
}

func (f *fakeAccountKeyLookup) Get(key []byte) ([]byte, error) {
	f.seen = append([]byte(nil), key...)
	if f.err != nil {
		return nil, f.err
	}
	if f.value == nil {
		return nil, pebble.ErrNotFound
	}
	return append([]byte(nil), f.value...), nil
}

func (s *fakeGetterStore) Get(key []byte) ([]byte, error) {
	s.getCall++
	if s.err != nil {
		return nil, s.err
	}
	if s.value == nil {
		return nil, ethstore.ErrNotFound
	}
	ret := make([]byte, len(s.value))
	copy(ret, s.value)
	return ret, nil
}

func TestGetWithPebbleBatchOverlay_FallbackToStoreWhenBatchMiss(t *testing.T) {
	store := &fakeGetterStore{value: []byte("db-value")}
	got, err := getWithPebbleBatchOverlay(nil, []byte("k"), func() ([]byte, error) {
		return store.Get([]byte("k"))
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if string(got) != "db-value" {
		t.Fatalf("unexpected value: %s", string(got))
	}
	if store.getCall != 1 {
		t.Fatalf("expected store.Get call once, got %d", store.getCall)
	}
}

func makeAOLTestKey(block uint64) []byte {
	key := make([]byte, 9)
	key[0] = 'h'
	binary.BigEndian.PutUint64(key[1:], block)
	return key
}

func TestEthstoreReplayBackendAOLStagePutDoesNotMarkPrefixDBDirty(t *testing.T) {
	backend, err := newEthstoreReplayBackend(filepath.Join(t.TempDir(), "ethstore"), allDBTypes, 0, 8*1024, 16, 0, 16, 16, 0, 0, 0, false, false)
	if err != nil {
		t.Fatalf("newEthstoreReplayBackend failed: %v", err)
	}
	defer backend.Close()

	if err := backend.StagePut(makeAOLTestKey(1), []byte("header-value"), ethstore.HeaderDataType); err != nil {
		t.Fatalf("StagePut failed: %v", err)
	}
	if backend.prefixdbDirty {
		t.Fatal("expected AOL-only StagePut to avoid marking PrefixDB dirty")
	}
}

func TestEthstoreReplayBackendAOLStageDeleteDoesNotMarkPrefixDBDirty(t *testing.T) {
	backend, err := newEthstoreReplayBackend(filepath.Join(t.TempDir(), "ethstore"), allDBTypes, 0, 8*1024, 16, 0, 16, 16, 0, 0, 0, false, false)
	if err != nil {
		t.Fatalf("newEthstoreReplayBackend failed: %v", err)
	}
	defer backend.Close()

	if err := backend.StageDelete(makeAOLTestKey(1), ethstore.HeaderDataType); err != nil {
		t.Fatalf("StageDelete failed: %v", err)
	}
	if backend.prefixdbDirty {
		t.Fatal("expected AOL-only StageDelete to avoid marking PrefixDB dirty")
	}
}

func TestNewEthstoreReplayBackendPrefixDBSkipsAOLInitialization(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "ethstore")
	brokenAOLDataPath := filepath.Join(baseDir+"_aol", ethstore.BlockdataFileName)
	if err := os.MkdirAll(brokenAOLDataPath, 0o755); err != nil {
		t.Fatalf("create broken AOL path failed: %v", err)
	}

	backend, err := newEthstoreReplayBackend(baseDir, PrefixDB, 0, 8*1024, 16, 0, 16, 16, 0, 0, 0, false, false)
	if err != nil {
		t.Fatalf("expected prefixdb replay backend to skip AOL init, got %v", err)
	}
	defer backend.Close()

	accountKey := append([]byte{'A'}, bytes.Repeat([]byte{0x11}, 32)...)
	if err := backend.StagePut(accountKey, []byte("account"), ethstore.TrieNodeAccountDataType); err != nil {
		t.Fatalf("StagePut account failed: %v", err)
	}
	if err := backend.CommitBlock(); err != nil {
		t.Fatalf("CommitBlock failed: %v", err)
	}
	got, err := backend.Get(accountKey, ethstore.TrieNodeAccountDataType)
	if err != nil {
		t.Fatalf("Get account failed: %v", err)
	}
	if string(got) != "account" {
		t.Fatalf("unexpected account value: %q", string(got))
	}
}

func TestRunRecoveryReportsStartupTimingsAndSkipsReplay(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "ethstore")
	cfg := replayConfig{EthStoreDir: baseDir}

	output := captureStdout(t, func() {
		if err := runRecovery(cfg, allDBTypes, 16, 0, 16, 0, 16, 16, 0, 0, 0, false, false); err != nil {
			t.Fatalf("runRecovery failed: %v", err)
		}
	})

	for _, want := range []string{
		"[ethstore] opened stores: all",
		"[recovery] pebbledb startup=",
		"[recovery] state store startup=",
		"[recovery] block store startup=",
		"read_bytes=",
		"[recovery] state store full read=",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected recovery output to contain %q, got: %s", want, output)
		}
	}
	if strings.Contains(output, "Replaying trace") {
		t.Fatalf("recovery should not replay trace, got output: %s", output)
	}
	if strings.Contains(output, "Trace file bytes read") {
		t.Fatalf("recovery should exit before replay summaries, got output: %s", output)
	}
}

func TestFullReadRegularFilesReadsRegularFiles(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	files := map[string]string{
		filepath.Join(root, "a.bin"):   "alpha",
		filepath.Join(root, "b.bin"):   "beta",
		filepath.Join(nested, "c.bin"): "gamma",
	}
	wantBytes := int64(0)
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile %s failed: %v", path, err)
		}
		wantBytes += int64(len(content))
	}

	stats, err := fullReadRegularFiles(root, 2)
	if err != nil {
		t.Fatalf("fullReadRegularFiles failed: %v", err)
	}
	if stats.fileCount != len(files) {
		t.Fatalf("file count mismatch: got %d want %d", stats.fileCount, len(files))
	}
	if stats.bytesRead != wantBytes {
		t.Fatalf("bytes read mismatch: got %d want %d", stats.bytesRead, wantBytes)
	}
}

func TestNewEthstoreReplayBackendPrefixDBUsesPebbleForStorageAccountLookup(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "ethstore")
	brokenAOLDataPath := filepath.Join(baseDir+"_aol", ethstore.BlockdataFileName)
	if err := os.MkdirAll(brokenAOLDataPath, 0o755); err != nil {
		t.Fatalf("create broken AOL path failed: %v", err)
	}

	accountHash := bytes.Repeat([]byte{0x22}, 32)
	accountKey := append([]byte{'A'}, bytes.Repeat([]byte{0x33}, 32)...)
	storageKey := append(append([]byte{'O'}, accountHash...), 0x01, 0x02, 0x03)

	auxStore, err := pebblestore.NewPebbleStore(baseDir+"_pebble", 0, 0, "", false)
	if err != nil {
		t.Fatalf("open auxiliary pebble failed: %v", err)
	}
	if err := auxStore.Put(accountHash, accountKey); err != nil {
		auxStore.Close()
		t.Fatalf("seed account hash mapping failed: %v", err)
	}
	if err := auxStore.Close(); err != nil {
		t.Fatalf("close auxiliary pebble failed: %v", err)
	}

	backend, err := newEthstoreReplayBackend(baseDir, PrefixDB, 0, 8*1024, 16, 0, 16, 16, 0, 0, 0, false, false)
	if err != nil {
		t.Fatalf("expected prefixdb replay backend to open with sibling pebble, got %v", err)
	}
	defer backend.Close()

	if err := backend.StagePut(accountKey, []byte("account"), ethstore.TrieNodeAccountDataType); err != nil {
		t.Fatalf("StagePut account failed: %v", err)
	}
	if err := backend.StagePut(storageKey, []byte("storage"), ethstore.TrieNodeStorageDataType); err != nil {
		t.Fatalf("StagePut storage failed: %v", err)
	}
	if err := backend.CommitBlock(); err != nil {
		t.Fatalf("CommitBlock failed: %v", err)
	}

	got, err := backend.Get(storageKey, ethstore.TrieNodeStorageDataType)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if string(got) != "storage" {
		t.Fatalf("unexpected storage value: %q", string(got))
	}
}

func TestNewEthstoreReplayBackendAOLSkipsStateAndPebbleInitialization(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "ethstore")
	brokenStatePath := filepath.Join(baseDir+"_state", "prefixdb")
	if err := os.MkdirAll(brokenStatePath, 0o755); err != nil {
		t.Fatalf("create broken state path failed: %v", err)
	}
	brokenPebblePath := filepath.Join(baseDir+"_pebble", "CURRENT")
	if err := os.MkdirAll(brokenPebblePath, 0o755); err != nil {
		t.Fatalf("create broken pebble path failed: %v", err)
	}

	backend, err := newEthstoreReplayBackend(baseDir, AOL, 0, 8*1024, 16, 0, 16, 16, 0, 0, 0, false, false)
	if err != nil {
		t.Fatalf("expected aol replay backend to skip state/pebble init, got %v", err)
	}
	defer backend.Close()

	if err := backend.StagePut(makeAOLTestKey(1), []byte("header-value"), ethstore.HeaderDataType); err != nil {
		t.Fatalf("StagePut failed: %v", err)
	}
	if err := backend.CommitBlock(); err != nil {
		t.Fatalf("CommitBlock failed: %v", err)
	}
	got, err := backend.Get(makeAOLTestKey(1), ethstore.HeaderDataType)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != "header-value" {
		t.Fatalf("unexpected value: %q", string(got))
	}
}

func TestResolvePrefixDBLoadAccountKeyNotFoundIsDeferred(t *testing.T) {
	accountHash := bytes.Repeat([]byte{0x42}, 32)
	storageKey := append(append([]byte{'O'}, accountHash...), 0x01, 0x08, 0x0b)
	lookup := &fakeAccountKeyLookup{err: pebble.ErrNotFound}

	accountKey, err := resolvePrefixDBLoadAccountKey(lookup, storageKey)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if accountKey != nil {
		t.Fatalf("expected nil account key on deferred resolution, got %x", accountKey)
	}
	if !bytes.Equal(lookup.seen, accountHash) {
		t.Fatalf("unexpected account hash lookup: got %x want %x", lookup.seen, accountHash)
	}
}

func TestNormalizeLegacyBoolFlagArgs(t *testing.T) {
	args := []string{
		"replayWorkload",
		"-mode", "re",
		"-node-file-sorted-compression", "false",
		"-segment-index-compression", "true",
		"-ckv-state", "false",
		"-trace-file", "cache",
	}

	normalized := normalizeLegacyBoolFlagArgs(args, map[string]struct{}{
		"-ckv-state":                    {},
		"-node-file-sorted-compression": {},
		"-segment-index-compression":    {},
	})

	got := strings.Join(normalized, " ")
	want := strings.Join([]string{
		"replayWorkload",
		"-mode", "re",
		"-node-file-sorted-compression=false",
		"-segment-index-compression=true",
		"-ckv-state=false",
		"-trace-file", "cache",
	}, " ")
	if got != want {
		t.Fatalf("unexpected normalized args:\n got: %s\nwant: %s", got, want)
	}
}

func TestLoadPrefixDBFinalBatchCommitPersistsTailEntries(t *testing.T) {
	tempDir := t.TempDir()
	auxDir := filepath.Join(tempDir, "account-hash-pebble")
	auxStore, err := pebblestore.NewPebbleStore(auxDir, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore failed: %v", err)
	}
	accountKey := []byte{'A', 0x01, 0x02, 0x03, 0x04}
	accountHash := bytes.Repeat([]byte{0x7a}, 32)
	if err := auxStore.Put(accountHash, accountKey); err != nil {
		auxStore.Close()
		t.Fatalf("auxStore.Put failed: %v", err)
	}
	if err := auxStore.Close(); err != nil {
		t.Fatalf("auxStore.Close failed: %v", err)
	}

	accountValue := []byte("account-value")
	storageValue := []byte("storage-value")
	storageKey := append(append([]byte{'O'}, accountHash...), 0x04, 0x05, 0x06)
	dataFile := filepath.Join(tempDir, "load.txt")
	content := fmt.Sprintf("Key: %x, Value : %x\nKey: %x, Value : %x\n", accountKey, accountValue, storageKey, storageValue)
	if err := os.WriteFile(dataFile, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	stateDir := filepath.Join(tempDir, formatPrefixDBStateDirName(8*1024))
	if err := loadPrefixDB(tempDir, "", dataFile, auxDir, prefixdbLoadStageAccount, 8*1024, 16, 0, 0, 0, 0, false, false); err != nil {
		t.Fatalf("account loadPrefixDB failed: %v", err)
	}
	if err := loadPrefixDB(tempDir, stateDir, dataFile, auxDir, prefixdbLoadStageStorage, 8*1024, 16, 0, 0, 0, 0, false, false); err != nil {
		t.Fatalf("storage loadPrefixDB failed: %v", err)
	}

	dbDir := stateDir
	reopened, err := prefixdb.NewPrefixDBWithRuntimeOptions(dbDir, 8*1024, 16, 16, 0, 0, 0, false, false, 0)
	if err != nil {
		t.Fatalf("reopen PrefixDB failed: %v", err)
	}
	defer reopened.Close()

	gotAccount, found, err := reopened.Get(datatypepkg.TrieNodeAccountDataType, accountKey, nil)
	if err != nil {
		t.Fatalf("Get account failed: %v", err)
	}
	if !found {
		t.Fatal("expected account entry to exist after final BatchCommit")
	}
	if !bytes.Equal(gotAccount, accountValue) {
		t.Fatalf("unexpected account value: got %q want %q", gotAccount, accountValue)
	}

	gotStorage, found, err := reopened.Get(datatypepkg.TrieNodeStorageDataType, storageKey, accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if !found {
		t.Fatal("expected storage entry to exist after final BatchCommit")
	}
	if !bytes.Equal(gotStorage, storageValue) {
		t.Fatalf("unexpected storage value: got %q want %q", gotStorage, storageValue)
	}
}

func TestLoadPrefixDBProcessesLastLineWithoutTrailingNewline(t *testing.T) {
	tempDir := t.TempDir()
	auxDir := filepath.Join(tempDir, "account-hash-pebble")
	auxStore, err := pebblestore.NewPebbleStore(auxDir, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore failed: %v", err)
	}
	accountKey := []byte{'A', 0x09, 0x08, 0x07, 0x06}
	accountHash := bytes.Repeat([]byte{0x4a}, 32)
	if err := auxStore.Put(accountHash, accountKey); err != nil {
		auxStore.Close()
		t.Fatalf("auxStore.Put failed: %v", err)
	}
	if err := auxStore.Close(); err != nil {
		t.Fatalf("auxStore.Close failed: %v", err)
	}

	accountValue := []byte("account-value")
	storageValue := []byte("storage-value-without-newline")
	storageKey := append(append([]byte{'O'}, accountHash...), 0x0a, 0x0b, 0x0c)
	dataFile := filepath.Join(tempDir, "load-no-trailing-newline.txt")
	content := fmt.Sprintf("Key: %x, Value : %x\nKey: %x, Value : %x", accountKey, accountValue, storageKey, storageValue)
	if err := os.WriteFile(dataFile, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	stateDir := filepath.Join(tempDir, formatPrefixDBStateDirName(8*1024))
	if err := loadPrefixDB(tempDir, "", dataFile, auxDir, prefixdbLoadStageAccount, 8*1024, 16, 0, 0, 0, 0, false, false); err != nil {
		t.Fatalf("account loadPrefixDB failed: %v", err)
	}
	if err := loadPrefixDB(tempDir, stateDir, dataFile, auxDir, prefixdbLoadStageStorage, 8*1024, 16, 0, 0, 0, 0, false, false); err != nil {
		t.Fatalf("storage loadPrefixDB failed: %v", err)
	}

	dbDir := stateDir
	reopened, err := prefixdb.NewPrefixDBWithRuntimeOptions(dbDir, 8*1024, 16, 16, 0, 0, 0, false, false, 0)
	if err != nil {
		t.Fatalf("reopen PrefixDB failed: %v", err)
	}
	defer reopened.Close()

	gotStorage, found, err := reopened.Get(datatypepkg.TrieNodeStorageDataType, storageKey, accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if !found {
		t.Fatal("expected storage entry from final line without trailing newline")
	}
	if !bytes.Equal(gotStorage, storageValue) {
		t.Fatalf("unexpected storage value: got %q want %q", gotStorage, storageValue)
	}
}

func TestLoadPrefixDBFailsWhenStorageAccountKeyCannotBeResolved(t *testing.T) {
	tempDir := t.TempDir()
	auxDir := filepath.Join(tempDir, "account-hash-pebble")
	auxStore, err := pebblestore.NewPebbleStore(auxDir, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore failed: %v", err)
	}
	if err := auxStore.Close(); err != nil {
		t.Fatalf("auxStore.Close failed: %v", err)
	}

	missingHash := bytes.Repeat([]byte{0x5b}, 32)
	storageKey := append(append([]byte{'O'}, missingHash...), 0x01, 0x02, 0x03)
	dataFile := filepath.Join(tempDir, "load-missing-account-key.txt")
	content := fmt.Sprintf("Key: %x, Value : %x\n", storageKey, []byte("storage-value"))
	if err := os.WriteFile(dataFile, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	stateDir := filepath.Join(tempDir, formatPrefixDBStateDirName(8*1024))
	err = loadPrefixDB(tempDir, stateDir, dataFile, auxDir, prefixdbLoadStageStorage, 8*1024, 16, 0, 0, 0, 0, false, false)
	if err == nil {
		t.Fatal("expected loadPrefixDB to fail when storage account key cannot be resolved")
	}
	if !strings.Contains(err.Error(), "deferred") || !strings.Contains(err.Error(), "unresolved account keys") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadPrefixDBStorageRequiresExplicitStateDir(t *testing.T) {
	tempDir := t.TempDir()
	dataFile := filepath.Join(tempDir, "load-storage-only.txt")
	storageKey := append(append([]byte{'O'}, bytes.Repeat([]byte{0x11}, 32)...), 0x01)
	content := fmt.Sprintf("Key: %x, Value : %x\n", storageKey, []byte("storage-value"))
	if err := os.WriteFile(dataFile, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	err := loadPrefixDB(tempDir, "", dataFile, filepath.Join(tempDir, "aux"), prefixdbLoadStageStorage, 8*1024, 16, 0, 0, 0, 0, false, false)
	if err == nil {
		t.Fatal("expected storage stage to require explicit state dir")
	}
	if !strings.Contains(err.Error(), "requires -prefixdb-state-dir") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadPrefixDBAccountStopsAfterLeavingAccountPrefixRange(t *testing.T) {
	tempDir := t.TempDir()
	dataFile := filepath.Join(tempDir, "load-account-stop-early.txt")
	accountKey := []byte{'A', 0x01, 0x02, 0x03}
	storageKey := append(append([]byte{'O'}, bytes.Repeat([]byte{0x22}, 32)...), 0x01)
	lateAccountKey := []byte{'A', 0x09, 0x08, 0x07}
	content := fmt.Sprintf(
		"Key: %x, Value : %x\nKey: %x, Value : %x\nKey: %x, Value : %x\n",
		accountKey, []byte("account-value"),
		storageKey, []byte("storage-value"),
		lateAccountKey, []byte("late-account-value"),
	)
	if err := os.WriteFile(dataFile, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := loadPrefixDB(tempDir, "", dataFile, filepath.Join(tempDir, "aux"), prefixdbLoadStageAccount, 8*1024, 16, 0, 0, 0, 0, false, false); err != nil {
		t.Fatalf("account loadPrefixDB failed: %v", err)
	}

	stateDir := filepath.Join(tempDir, formatPrefixDBStateDirName(8*1024))
	reopened, err := prefixdb.NewPrefixDBWithRuntimeOptions(stateDir, 8*1024, 16, 16, 0, 0, 0, false, false, 0)
	if err != nil {
		t.Fatalf("reopen PrefixDB failed: %v", err)
	}
	defer reopened.Close()

	gotAccount, found, err := reopened.Get(datatypepkg.TrieNodeAccountDataType, accountKey, nil)
	if err != nil {
		t.Fatalf("Get account failed: %v", err)
	}
	if !found || !bytes.Equal(gotAccount, []byte("account-value")) {
		t.Fatalf("unexpected first account value: found=%v value=%q", found, gotAccount)
	}

	_, found, err = reopened.Get(datatypepkg.TrieNodeAccountDataType, lateAccountKey, nil)
	if err != nil {
		t.Fatalf("Get late account failed: %v", err)
	}
	if found {
		t.Fatal("expected loader to stop after leaving account prefix range")
	}
}

func TestLoadPebbleLoadsOnlyNonAOLAndNonPrefixDBEntries(t *testing.T) {
	tempDir := t.TempDir()
	pebbleDir := filepath.Join(tempDir, "runtime-pebble")
	auxDir := filepath.Join(tempDir, "account-hash-pebble")
	dataFile := filepath.Join(tempDir, "load-pebble.txt")
	aolKey := []byte{'h', 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	accountKey := []byte{'A', 0x01, 0x02, 0x03}
	nonPrefixKey := []byte{'m', 0xaa, 0xbb, 0xcc}
	auxKey := []byte{0xde, 0xad, 0xbe, 0xef}
	auxValue := []byte("account-hash-index")
	content := fmt.Sprintf(
		"Key: %x, Value : %x\nKey: %x, Value : %x\nKey: %x, Value : %x\n",
		aolKey, []byte("aol-value"),
		accountKey, []byte("account-value"),
		nonPrefixKey, []byte("pebble-value"),
	)
	if err := os.WriteFile(dataFile, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	auxStore, err := pebblestore.NewPebbleStore(auxDir, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore aux failed: %v", err)
	}
	if err := auxStore.Put(auxKey, auxValue); err != nil {
		auxStore.Close()
		t.Fatalf("auxStore.Put failed: %v", err)
	}
	if err := auxStore.Close(); err != nil {
		t.Fatalf("auxStore.Close failed: %v", err)
	}

	if err := loadEthStorePebble(pebbleDir, dataFile, auxDir, 0, 0, false); err != nil {
		t.Fatalf("loadPebble failed: %v", err)
	}

	store, err := pebblestore.NewPebbleStore(pebbleDir, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore reopen failed: %v", err)
	}
	defer store.Close()

	got, err := store.Get(nonPrefixKey)
	if err != nil {
		t.Fatalf("expected non-prefix entry in pebble store: %v", err)
	}
	if !bytes.Equal(got, []byte("pebble-value")) {
		t.Fatalf("unexpected non-prefix value: got %q want %q", got, []byte("pebble-value"))
	}

	gotAux, err := store.Get(auxKey)
	if err != nil {
		t.Fatalf("expected account hash index entry in runtime pebble: %v", err)
	}
	if !bytes.Equal(gotAux, auxValue) {
		t.Fatalf("unexpected account hash index value: got %q want %q", gotAux, auxValue)
	}

	if _, err := store.Get(aolKey); !errors.Is(err, pebblestore.ErrNotFound) {
		t.Fatalf("expected AOL entry to be skipped by loadPebble, got: %v", err)
	}
	if _, err := store.Get(accountKey); !errors.Is(err, pebblestore.ErrNotFound) {
		t.Fatalf("expected PrefixDB entry to be skipped by loadPebble, got: %v", err)
	}
}

func TestGetWithPebbleBatchOverlay_BatchPutAndDeletePrecedence(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "pebble-overlay-workload-test")
	ps, err := pebblestore.NewPebbleStore(dbPath, 0, 0, "", false)
	if err != nil {
		t.Fatalf("NewPebbleStore failed: %v", err)
	}
	defer ps.Close()
	key := []byte("k1")
	if err := ps.Put(key, []byte("db-value")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	batch := ps.NewBatch()
	if err := batch.Put(key, []byte("batch-value")); err != nil {
		t.Fatalf("batch put failed: %v", err)
	}
	got, err := getWithPebbleBatchOverlay(batch, key, func() ([]byte, error) {
		return ps.Get(key)
	})
	if err != nil {
		t.Fatalf("unexpected err on batch hit: %v", err)
	}
	if string(got) != "batch-value" {
		t.Fatalf("expected batch value, got: %s", string(got))
	}
	if err := batch.Delete(key); err != nil {
		t.Fatalf("batch delete failed: %v", err)
	}
	_, err = getWithPebbleBatchOverlay(batch, key, func() ([]byte, error) {
		return ps.Get(key)
	})
	if !errors.Is(err, ethstore.ErrNotFound) {
		t.Fatalf("expected ErrNotFound from batch tombstone, got: %v", err)
	}
}

func TestChainKVReplayBackendGetMapsStateNotFound(t *testing.T) {
	backend, err := newChainKVReplayBackend(filepath.Join(t.TempDir(), "chainkv"), 16, 16, true)
	if err != nil {
		t.Fatalf("newChainKVReplayBackend failed: %v", err)
	}
	defer backend.Close()

	_, err = backend.Get([]byte{0x01, 0x02, 0x03}, ethstore.TrieNodeAccountDataType)
	if !errors.Is(err, ethstore.ErrNotFound) {
		t.Fatalf("expected ethstore.ErrNotFound from chainkv state get miss, got: %v", err)
	}
}

func TestChainKVReplayBackendStageDeleteWritesTombstone(t *testing.T) {
	backend, err := newChainKVReplayBackend(filepath.Join(t.TempDir(), "chainkv"), 16, 16, true)
	if err != nil {
		t.Fatalf("newChainKVReplayBackend failed: %v", err)
	}
	defer backend.Close()

	key := []byte{0xaa, 0xbb, 0xcc}
	if err := backend.StagePut(key, []byte("value"), ethstore.TrieNodeAccountDataType); err != nil {
		t.Fatalf("StagePut failed: %v", err)
	}
	if err := backend.CommitBlock(); err != nil {
		t.Fatalf("CommitBlock after put failed: %v", err)
	}
	got, err := backend.Get(key, ethstore.TrieNodeAccountDataType)
	if err != nil {
		t.Fatalf("Get after put failed: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("unexpected value after put: %q", got)
	}

	if err := backend.StageDelete(key, ethstore.TrieNodeAccountDataType); err != nil {
		t.Fatalf("StageDelete failed: %v", err)
	}
	if err := backend.CommitBlock(); err != nil {
		t.Fatalf("CommitBlock after delete failed: %v", err)
	}
	_, err = backend.Get(key, ethstore.TrieNodeAccountDataType)
	if !errors.Is(err, ethstore.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after tombstone delete, got: %v", err)
	}
}

func TestChainKVReplayBackendNewIteratorHonorsPrefixAndStart(t *testing.T) {
	backend, err := newChainKVReplayBackend(filepath.Join(t.TempDir(), "chainkv"), 16, 16, false)
	if err != nil {
		t.Fatalf("newChainKVReplayBackend failed: %v", err)
	}
	defer backend.Close()

	seed := []struct {
		key   []byte
		value []byte
	}{
		{key: []byte("p:1"), value: []byte("value-1")},
		{key: []byte("p:2"), value: []byte("value-2")},
		{key: []byte("q:1"), value: []byte("value-q")},
	}
	for _, kv := range seed {
		if err := backend.StagePut(kv.key, kv.value, ethstore.HeaderDataType); err != nil {
			t.Fatalf("StagePut failed for %q: %v", kv.key, err)
		}
	}
	if err := backend.CommitBlock(); err != nil {
		t.Fatalf("CommitBlock failed: %v", err)
	}

	it := backend.NewIterator([]byte("p:"), []byte("2"))
	defer it.Release()

	if !it.Next() {
		t.Fatal("expected iterator to return the prefixed start key")
	}
	if got := string(it.Value()); got != "value-2" {
		t.Fatalf("unexpected first iterator value: got %q want %q", got, "value-2")
	}
	if it.Next() {
		t.Fatal("expected iterator to stop at prefix boundary")
	}
}

func TestReplayTrace_StartBlockSkipsEarlierBlocks(t *testing.T) {
	backend := &fakeReplayBackend{}
	traceFile := writeTraceFile(t,
		"Processing block (start), ID: 100",
		"OPType: Put, key: 01, size: 1, value: aa, size: 1",
		"Processing block (end), ID: 100",
		"Processing block (start), ID: 101",
		"OPType: Put, key: 02, size: 1, value: bb, size: 1",
		"Processing block (end), ID: 101",
	)

	replayTrace(backend, traceFile, 0, allDBTypes, 101, 0, 1)

	if len(backend.puts) != 1 {
		t.Fatalf("expected 1 put after start block filter, got %d", len(backend.puts))
	}
	if got := string(backend.puts[0]); got != string([]byte{0x02}) {
		t.Fatalf("expected only block 101 key to be replayed, got %x", backend.puts[0])
	}
	if backend.commits != 1 {
		t.Fatalf("expected 1 commit after start block filter, got %d", backend.commits)
	}
}

func TestReplayTrace_EndBlockStopsAfterCommit(t *testing.T) {
	backend := &fakeReplayBackend{}
	traceFile := writeTraceFile(t,
		"Processing block (start), ID: 200",
		"OPType: Put, key: 0a, size: 1, value: aa, size: 1",
		"Processing block (end), ID: 200",
		"Processing block (start), ID: 201",
		"OPType: Put, key: 0b, size: 1, value: bb, size: 1",
		"Processing block (end), ID: 201",
	)

	replayTrace(backend, traceFile, 0, allDBTypes, 0, 200, 1)

	if len(backend.puts) != 1 {
		t.Fatalf("expected replay to stop after end block, got %d puts", len(backend.puts))
	}
	if got := string(backend.puts[0]); got != string([]byte{0x0a}) {
		t.Fatalf("expected only block 200 key to be replayed, got %x", backend.puts[0])
	}
	if backend.commits != 1 {
		t.Fatalf("expected matching end block to be committed once, got %d", backend.commits)
	}
}

func TestReplayTrace_StartAndEndOnSameBlock(t *testing.T) {
	backend := &fakeReplayBackend{}
	traceFile := writeTraceFile(t,
		"Processing block (start), ID: 299",
		"OPType: Put, key: 09, size: 1, value: aa, size: 1",
		"Processing block (end), ID: 299",
		"Processing block (start), ID: 300",
		"OPType: Put, key: 0c, size: 1, value: cc, size: 1",
		"Processing block (end), ID: 300",
		"Processing block (start), ID: 301",
		"OPType: Put, key: 0d, size: 1, value: dd, size: 1",
		"Processing block (end), ID: 301",
	)

	replayTrace(backend, traceFile, 0, allDBTypes, 300, 300, 1)

	if len(backend.puts) != 1 {
		t.Fatalf("expected exactly one block to replay, got %d puts", len(backend.puts))
	}
	if got := string(backend.puts[0]); got != string([]byte{0x0c}) {
		t.Fatalf("expected only block 300 key to be replayed, got %x", backend.puts[0])
	}
	if backend.commits != 1 {
		t.Fatalf("expected exactly one commit for start=end block, got %d", backend.commits)
	}
}

func TestReplayTrace_CommitBlockInterval(t *testing.T) {
	backend := &fakeReplayBackend{}
	traceFile := writeTraceFile(t,
		"Processing block (start), ID: 400",
		"OPType: Put, key: 0a, size: 1, value: aa, size: 1",
		"Processing block (end), ID: 400",
		"Processing block (start), ID: 401",
		"OPType: Put, key: 0b, size: 1, value: bb, size: 1",
		"Processing block (end), ID: 401",
		"Processing block (start), ID: 402",
		"OPType: Put, key: 0c, size: 1, value: cc, size: 1",
		"Processing block (end), ID: 402",
	)

	replayTrace(backend, traceFile, 0, allDBTypes, 0, 0, 2)

	if backend.commits != 2 {
		t.Fatalf("expected 2 commits for interval=2 across 3 blocks, got %d", backend.commits)
	}
	if len(backend.puts) != 3 {
		t.Fatalf("expected 3 puts to replay, got %d", len(backend.puts))
	}
}

func TestReplayTrace_CountsOnlyActuallyReplayedOpsButTracksTraceBytesRead(t *testing.T) {
	backend := &fakeReplayBackend{}
	traceFile := writeTraceFile(t,
		"Processing block (start), ID: 500",
		"OPType: Put, key: , size: 0, value: aa, size: 1",
		"OPType: Put, key: 0a, size: 1, value: bb, size: 1",
		"Processing block (end), ID: 500",
	)

	traceContent, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("read trace file failed: %v", err)
	}

	output := captureStdout(t, func() {
		replayTrace(backend, traceFile, 0, allDBTypes, 0, 0, 1)
	})

	if len(backend.puts) != 1 {
		t.Fatalf("expected only valid put to be replayed, got %d puts", len(backend.puts))
	}

	replaySummaryRE := regexp.MustCompile(`Replay finished\. ops=(\d+) `)
	match := replaySummaryRE.FindStringSubmatch(output)
	if len(match) != 2 {
		t.Fatalf("replay summary not found in output: %s", output)
	}
	if match[1] != "1" {
		t.Fatalf("expected replay ops to count only actually replayed ops, got %s; output=%s", match[1], output)
	}

	traceBytesRE := regexp.MustCompile(`Trace file bytes read: (\d+) bytes`)
	match = traceBytesRE.FindStringSubmatch(output)
	if len(match) != 2 {
		t.Fatalf("trace bytes summary not found in output: %s", output)
	}
	if got, want := match[1], fmt.Sprintf("%d", len(traceContent)); got != want {
		t.Fatalf("expected trace bytes to reflect bytes actually read from trace file, got %s want %s; output=%s", got, want, output)
	}
}

func TestReplayTrace_ReportsGetOtherErrors(t *testing.T) {
	backend := &fakeReplayBackend{
		getFunc: func(_ []byte, _ ethstore.DataType) ([]byte, error) {
			return nil, errors.New("boom")
		},
	}
	traceFile := writeTraceFile(t,
		"Processing block (start), ID: 600",
		"OPType: Get, key: 41, size: 1",
		"Processing block (end), ID: 600",
	)

	output := captureStdout(t, func() {
		replayTrace(backend, traceFile, 0, allDBTypes, 0, 0, 1)
	})
	if !strings.Contains(output, "[GetStats][Global] success=0 notfound=0") {
		t.Fatalf("expected zero success/notfound in GetStats, output=%s", output)
	}
	if !strings.Contains(output, "[GetOtherErrorStats][Global] count=1") {
		t.Fatalf("expected GetOtherErrorStats global count, output=%s", output)
	}
	if !strings.Contains(output, "[GetOtherErrorStats] dataType=StateData count=1") {
		t.Fatalf("expected GetOtherErrorStats by data type, output=%s", output)
	}
	if !strings.Contains(output, "[GetOtherErrorStats] error=\"boom\" count=1") {
		t.Fatalf("expected GetOtherErrorStats by error cause, output=%s", output)
	}
}
