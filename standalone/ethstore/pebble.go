package ethstore

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
)

// PebbleStore wraps the Pebble DB instance.
type PebbleStore struct {
	db *pebble.DB
}

// NewPebbleStore creates or opens a Pebble database at the given path.
func NewPebbleStore(dbPath string) (*PebbleStore, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create pebble db path %s: %w", dbPath, err)
	}

	opts := &pebble.Options{}
	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %w", dbPath, err)
	}
	return &PebbleStore{db: db}, nil
}

// Put stores a key-value pair.
func (ps *PebbleStore) Put(key, value []byte) error {
	return ps.db.Set(key, value, pebble.Sync)
}

// Get retrieves a value by key. Returns pebble.ErrNotFound if the key does not exist.
func (ps *PebbleStore) Get(key []byte) ([]byte, error) {
	value, closer, err := ps.db.Get(key)
	if err != nil {
		return nil, err // Handles pebble.ErrNotFound
	}
	defer closer.Close()

	// Need to copy the value, as the underlying buffer is only valid until closer.Close()
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	return valCopy, nil
}

// Delete removes a key-value pair.
func (ps *PebbleStore) Delete(key []byte) error {
	return ps.db.Delete(key, pebble.Sync)
}

// Close closes the Pebble database.
func (ps *PebbleStore) Close() error {
	if ps.db != nil {
		return ps.db.Close()
	}
	return nil
}

// NewIterator creates a new iterator over the Pebble store.
// The caller is responsible for closing the iterator.
func (ps *PebbleStore) NewIterator(opts *pebble.IterOptions) *pebble.Iterator {
	return ps.db.NewIter(opts)
}
