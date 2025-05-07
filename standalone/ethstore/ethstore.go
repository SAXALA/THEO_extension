// Copyright 2023 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package ethstore implements the key-value database layer based on an append-only log store.
package ethstore

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
)

// Define custom errors to replace ethdb's if they are undefined
var (
	ErrClosed     = errors.New("database closed")
	ErrNotFound   = errors.New("not found")
	ErrCompaction = errors.New("compaction error") // Example, if you need more
)

// Database is a persistent key-value store based on the append-only log store.
type Database struct {
	fn  string         // filename/directory for reporting
	aol *AppendOnlyLog // Underlying append-only log store

	diskSizeGauge *metrics.Gauge // Gauge for tracking the size of all the data in the database

	quitLock sync.RWMutex    // Mutex protecting the quit channel and the closed flag
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database
	closed   bool            // keep track of whether we're Closed

	log log.Logger // Contextual logger tracking the database path
}

// New returns a wrapped EthStore object using AppendOnlyLog.
// The namespace is the prefix that the metrics reporting should use.
// cache and handles parameters might be less relevant for AppendOnlyLog,
// but recentN (number of blocks to index) becomes important.
func New(dirPath string, recentN int, namespace string, readonly bool) (*Database, error) {
	logger := log.New("database", dirPath)

	db := &Database{
		fn:       dirPath, // Use directory path now
		log:      logger,
		quitChan: make(chan chan error),
	}

	// Initialize the AppendOnlyLog store
	if recentN <= 0 {
		// Use defaultRecentN from blockStore.go (implicitly, as it's used in NewAppendOnlyLog)
		// Or explicitly pass it if needed: recentN = defaultRecentN
	}
	logger.Info("Initializing AppendOnlyLog store", "recentN", recentN) // recentN will be default if <= 0

	appendLog, err := NewAppendOnlyLog(dirPath, recentN, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize append-only log: %w", err)
	}
	db.aol = appendLog

	// Initialize metrics
	db.diskSizeGauge = metrics.GetOrRegisterGauge(namespace+"disk/size", nil)

	return db, nil
}

// Close stops the metrics collection and closes all io accesses to the underlying key-value store.
func (d *Database) Close() error {
	d.quitLock.Lock()
	defer d.quitLock.Unlock()
	if d.closed {
		return ErrClosed // If you want to signal it was already closed
	}
	d.closed = true
	if d.quitChan != nil {
		errc := make(chan error)
		d.quitChan <- errc
		// Handle potential error from metrics shutdown if it existed
		select {
		case err := <-errc:
			if err != nil {
				d.log.Error("Metrics collection failed", "err", err)
			}
		case <-time.After(1 * time.Second): // Add timeout
			d.log.Warn("Timeout waiting for metrics shutdown")
		}
		close(d.quitChan) // Close the channel itself
		d.quitChan = nil
	}
	return d.aol.Close()
}

// Has retrieves if a key is present in the key-value store.
func (d *Database) Has(key []byte) (bool, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return false, ErrClosed // Use custom ErrClosed
	}
	_, exists, err := d.aol.Get(string(key))
	// Note: aol.Get returns true for exists even if it's a tombstone.
	// This matches the typical Has behavior (key exists, even if deleted).
	return exists, err
}

// Get retrieves the given key if it's present in the key-value store.
func (d *Database) Get(key []byte) ([]byte, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return nil, ErrClosed // Use custom ErrClosed
	}
	valStr, exists, err := d.aol.Get(string(key))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound // Use custom ErrNotFound
	}
	// Check if the value is a tombstone marker from AppendOnlyLog
	if valStr == tombstoneMarker {
		return nil, ErrNotFound // Treat tombstone as not found for Get
	}
	return []byte(valStr), nil
}

// Put inserts the given value into the key-value store.
// WARNING: This requires a block ID. Using a placeholder '0' which is likely incorrect.
// This needs a proper mechanism to determine the block ID.
func (d *Database) Put(key []byte, value []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return ErrClosed // Use custom ErrClosed
	}
	// TODO: Determine the correct block ID. Using 0 as a placeholder.
	// This might require getting the latest block ID from aol and incrementing,
	// or receiving it via context or another mechanism.
	blockID := uint64(0) // <<< PLACEHOLDER - NEEDS PROPER IMPLEMENTATION
	d.log.Warn("Using placeholder blockID for Put operation", "blockID", blockID, "key", string(key))
	return d.aol.Append(blockID, map[string]string{string(key): string(value)})
}

// Delete removes the key from the key-value store.
// WARNING: This requires a block ID. Using a placeholder '0' which is likely incorrect.
// This needs a proper mechanism to determine the block ID.
func (d *Database) Delete(key []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return ErrClosed // Use custom ErrClosed
	}
	// TODO: Determine the correct block ID. Using 0 as a placeholder.
	blockID := uint64(0) // <<< PLACEHOLDER - NEEDS PROPER IMPLEMENTATION
	d.log.Warn("Using placeholder blockID for Delete operation", "blockID", blockID, "key", string(key))
	return d.aol.Delete(blockID, string(key))
}

// DeleteRange removes all keys between start and end (exclusive of end).
// WARNING: This is a placeholder implementation. True range deletion is complex
// for an append-only log and might not be fully supported or may require
// a different approach (e.g., marking a range as deleted for future compaction).
func (d *Database) DeleteRange(start, end []byte) error {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return ErrClosed
	}
	d.log.Warn("DeleteRange is not efficiently implemented for AppendOnlyLog; this is a placeholder.", "start", string(start), "end", string(end))
	// A proper implementation would need to iterate keys and write tombstones,
	// which is non-trivial for an append-only structure without iterating all data
	// or having a comprehensive index of all keys.
	// For now, returning nil to satisfy the interface.
	// Consider returning an "unsupported operation" error if this functionality
	// is critical and cannot be properly implemented.
	return nil
}

// Path returns the path to the database directory.
func (d *Database) Path() string {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return "" // Or handle appropriately
	}
	return d.aol.Path()
}

// --- Methods below need implementation or removal ---

// NewBatch creates a write-only database batch object.
func (d *Database) NewBatch() ethdb.Batch {
	d.log.Warn("NewBatch is not implemented for AppendOnlyLog; returning nil.")
	return nil // Placeholder
}

// NewBatchWithSize creates a write-only database batch object with pre-allocated buffer size.
func (d *Database) NewBatchWithSize(size int) ethdb.Batch {
	d.log.Warn("NewBatchWithSize is not implemented for AppendOnlyLog; returning nil.")
	return nil // Placeholder
}

// NewIterator creates a binary-alphabetical iterator over a subset of database content.
func (d *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	d.log.Warn("NewIterator is not implemented for AppendOnlyLog; returning nil.")
	return nil // Placeholder
}

// Stat returns a particular internal stat of the database.
// The ethdb.KeyValueStore interface expects Stat() (string, error)
// If your previous Stat(string) was meant to get specific stats,
// you'll need to decide what general stat this should return,
// or how to adapt. For now, let's return a simple path or status.
func (d *Database) Stat() (string, error) {
	d.quitLock.RLock()
	defer d.quitLock.RUnlock()
	if d.closed {
		return "", ErrClosed
	}
	// Example: return the path or a general status message.
	// The original ethdb.Database.Stat returns "ldb" or "mem"
	// You might return something like "ethstore(appendonlylog)"
	return fmt.Sprintf("ethstore(path=%s, recentN=%d)", d.aol.Path(), d.aol.RecentN()), nil
}

// Compact flattens the underlying data store for the given key range.
func (d *Database) Compact(start []byte, limit []byte) error {
	// Compaction logic is internal to AppendOnlyLog or needs a separate mechanism.
	// This method might not be directly applicable.
	d.log.Warn("Compact operation may not be applicable or is handled differently by AppendOnlyLog")
	return nil // Or return an error if not supported
}

// batch is a wrapper around AppendOnlyLog operations potentially for a single block.
// Needs complete redesign.
type batch struct {
	db      *Database
	blockID uint64            // The block ID for this batch
	kvs     map[string]string // Key-value pairs for the batch
	size    int               // Approximate size
	lock    sync.Mutex
}

// iterator is a wrapper around AppendOnlyLog data access.
// Needs complete redesign. Might only iterate over skiplist or require full scan.
type iterator struct {
	db *Database
	// ... fields for iteration state ...
}
