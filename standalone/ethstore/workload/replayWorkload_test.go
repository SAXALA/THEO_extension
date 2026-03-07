package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
	"github.com/tinoryj/EthStore/standalone/ethstore/pebblestore"
)

type fakeReplayBackend struct {
	puts    [][]byte
	deletes [][]byte
	gets    [][]byte
	commits int
	dirty   bool
}

func (b *fakeReplayBackend) Name() string { return "fake" }

func (b *fakeReplayBackend) Get(key []byte, _ ethstore.DataType) ([]byte, error) {
	b.gets = append(b.gets, append([]byte(nil), key...))
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

type fakeGetterStore struct {
	value   []byte
	err     error
	getCall int
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

	replayTrace(backend, traceFile, 0, allDBTypes, 101, 0)

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

	replayTrace(backend, traceFile, 0, allDBTypes, 0, 200)

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

	replayTrace(backend, traceFile, 0, allDBTypes, 300, 300)

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
