package main

import (
	"errors"
	"path/filepath"
	"testing"

	ethstore "github.com/tinoryj/EthStore/standalone/ethstore"
)

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
	got, err := getWithPebbleBatchOverlay(store, nil, []byte("k"))
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
	ps, err := ethstore.NewPebbleStore(dbPath, 0, 0, "", false)
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
	got, err := getWithPebbleBatchOverlay(ps, batch, key)
	if err != nil {
		t.Fatalf("unexpected err on batch hit: %v", err)
	}
	if string(got) != "batch-value" {
		t.Fatalf("expected batch value, got: %s", string(got))
	}
	if err := batch.Delete(key); err != nil {
		t.Fatalf("batch delete failed: %v", err)
	}
	_, err = getWithPebbleBatchOverlay(ps, batch, key)
	if !errors.Is(err, ethstore.ErrNotFound) {
		t.Fatalf("expected ErrNotFound from batch tombstone, got: %v", err)
	}
}
