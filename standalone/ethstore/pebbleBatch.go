package ethstore

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/ethdb"
)

type ModifiedType int

const (
	None ModifiedType = iota
	ValueModified
)

type WriteBatch struct {
	operations map[string]WriteOperation
	rangeOps   []DeleteRangeOperation
	// slotBatch  map[int]map[string][]byte
	lock       sync.Mutex
	autoCommit bool // enable auto commit feature
	threshold  int  // the threshold for auto commit
	valueSize  int
	db         *PebbleStore

	// for background commit
	commitQueue chan *WriteBatch // the queue for commit batches
	quitCh      chan struct{}    // the quit channel for background commit
	wg          sync.WaitGroup   // wait group for  shutdown
	bgCommit    bool             // enable background commit
}

type WriteOperation struct {
	value         []byte
	modifiedType  ModifiedType // type of modification (0: None, 1: value changed)
	storageFileID uint32       // ID of the storage file for the account
	storageOffset int64        // offset in the file for the account
	storageSize   uint64       // size of the stored data
}

type DeleteRangeOperation struct {
	start []byte
	end   []byte
}

func inDeleteRange(key []byte, rop DeleteRangeOperation) bool {
	if rop.start != nil && bytes.Compare(key, rop.start) < 0 {
		return false
	}
	if rop.end != nil && bytes.Compare(key, rop.end) >= 0 {
		return false
	}
	return true
}

type StorageInfo struct {
	storageFileID uint32
	storageOffset int64
	storageSize   uint64
}

func NewWriteBatch(threshold int) *WriteBatch {
	return &WriteBatch{
		operations: make(map[string]WriteOperation),

		autoCommit:  true,
		threshold:   threshold,                   //dafault threshold for auto commit
		commitQueue: make(chan *WriteBatch, 100), // buffered channel for commit batches
		quitCh:      make(chan struct{}),
		bgCommit:    false,
	}
}

func (wb *WriteBatch) EnableAutoCommit(db *PebbleStore, threshold int) {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	wb.autoCommit = true
	wb.db = db
	if threshold > 0 {
		wb.threshold = threshold
	}
}

// EnableBackgroundCommit
func (wb *WriteBatch) EnableBackgroundCommit(db *PebbleStore) {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	if !wb.bgCommit {
		wb.db = db
		wb.bgCommit = true
		wb.wg.Add(1)

		// start background commit goroutine
		go wb.processCommitQueue()
	}
}

// DisableBackgroundCommit
func (wb *WriteBatch) DisableBackgroundCommit() {
	wb.lock.Lock()
	bgCommit := wb.bgCommit
	wb.lock.Unlock()

	if bgCommit {
		close(wb.quitCh)
		wb.wg.Wait()

		wb.lock.Lock()
		wb.bgCommit = false
		wb.quitCh = make(chan struct{})
		wb.lock.Unlock()
	}
}

// processCommitQueue
func (wb *WriteBatch) processCommitQueue() {
	defer wb.wg.Done()

	for {
		select {
		case batch := <-wb.commitQueue:
			if batch != nil && wb.db != nil {
				err := wb.db.WriteCommit(batch)
				if err != nil {
					fmt.Printf("Error in queue commit: %v\n", err)
				}
			}
		case <-wb.quitCh:
			// operation to perform during shutdown
			for {
				select {
				case batch := <-wb.commitQueue:
					if batch != nil && wb.db != nil {
						err := wb.db.WriteCommit(batch)
						if err != nil {
							fmt.Printf("Error in shutdown commit: %v\n", err)
						}
					}
				default:
					return
				}
			}
		}
	}
}

func (wb *WriteBatch) DisableAutoCommit() {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	wb.autoCommit = false
}

// SetThreshold sets the threshold for auto commit
func (wb *WriteBatch) SetThreshold(threshold int) {
	if threshold > 0 {
		wb.lock.Lock()
		defer wb.lock.Unlock()

		wb.threshold = threshold
	}
}

// checkAndCommit
func (wb *WriteBatch) checkAndCommit() {
	wb.lock.Lock()
	totalOps := len(wb.operations) + len(wb.rangeOps)
	needCommit := wb.autoCommit && wb.db != nil && totalOps >= wb.threshold

	var batchToCommit *WriteBatch
	if needCommit {
		batchToCommit = &WriteBatch{
			operations: make(map[string]WriteOperation, len(wb.operations)),

			db: wb.db,
		}

		batchToCommit.operations = wb.operations
		if len(wb.rangeOps) > 0 {
			batchToCommit.rangeOps = make([]DeleteRangeOperation, len(wb.rangeOps))
			copy(batchToCommit.rangeOps, wb.rangeOps)
		}
		wb.operations = make(map[string]WriteOperation)
		wb.rangeOps = nil
		wb.valueSize = 0
	}
	wb.lock.Unlock()

	if needCommit {
		if wb.bgCommit {
			select {
			case wb.commitQueue <- batchToCommit:
			default:
				if wb.db != nil {
					err := wb.db.WriteCommit(batchToCommit)
					if err != nil {
						fmt.Printf("Error in background commit: %v\n", err)
					}
				}
			}
		} else {
			if wb.db != nil {
				err := wb.db.WriteCommit(batchToCommit)
				if err != nil {
					fmt.Printf("Error in commit: %v\n", err)
				}
			}
		}
	}
}

func (wb *WriteBatch) CommitBatch() error {
	wb.lock.Lock()

	if wb.db == nil {
		wb.lock.Unlock()
		return errors.New("database instance not available")
	}

	batchToCommit := &WriteBatch{
		operations: make(map[string]WriteOperation, len(wb.operations)),
		db:         wb.db,
	}

	batchToCommit.operations = wb.operations
	if len(wb.rangeOps) > 0 {
		batchToCommit.rangeOps = make([]DeleteRangeOperation, len(wb.rangeOps))
		copy(batchToCommit.rangeOps, wb.rangeOps)
	}

	wb.operations = make(map[string]WriteOperation)
	wb.rangeOps = nil
	wb.valueSize = 0

	wb.lock.Unlock()

	if wb.bgCommit {
		select {
		case wb.commitQueue <- batchToCommit:
			return nil
		default:
			return wb.db.WriteCommit(batchToCommit)
		}
	} else {
		return wb.db.WriteCommit(batchToCommit)
	}
}

func (wb *WriteBatch) add(key, value []byte, storageFileID uint32, storageOffset int64, storageSize uint64, modifiedType ModifiedType) {
	wb.lock.Lock()
	wb.operations[string(key)] = WriteOperation{value: value, storageFileID: storageFileID, storageOffset: storageOffset, storageSize: storageSize, modifiedType: modifiedType}
	wb.valueSize += len(key) + len(value)
	wb.lock.Unlock()

	wb.checkAndCommit()
}

// delete marks a key for deletion in the batch
func (wb *WriteBatch) delete(key []byte) {
	wb.lock.Lock()
	wb.operations[string(key)] = WriteOperation{value: nil, storageFileID: 0, storageOffset: 0, storageSize: 0, modifiedType: 1}
	wb.valueSize += len(key)
	wb.lock.Unlock()

	wb.checkAndCommit()
}

func (wb *WriteBatch) get(key []byte) ([]byte, StorageInfo, bool) {
	wb.lock.Lock()
	defer wb.lock.Unlock()
	for _, rop := range wb.rangeOps {
		if inDeleteRange(key, rop) {
			return nil, StorageInfo{}, false
		}
	}
	op, exists := wb.operations[string(key)]
	if exists && op.value == nil {
		return nil, StorageInfo{}, false
	}
	return op.value, StorageInfo{
		storageFileID: op.storageFileID,
		storageOffset: op.storageOffset,
		storageSize:   op.storageSize,
	}, exists
}

func (wb *WriteBatch) updateStoragePointer(key string, cacheInfo StorageInfo) error {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	op, exists := wb.operations[key]
	if !exists || op.value == nil {
		return errors.New("key not found in batch or marked for deletion")
	}

	op.storageFileID = cacheInfo.storageFileID
	op.storageOffset = cacheInfo.storageOffset
	op.storageSize = cacheInfo.storageSize
	wb.operations[key] = op
	return nil
}

func (wb *WriteBatch) updateValue(key []byte, value []byte) error {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	op, exists := wb.operations[string(key)]
	if !exists || op.value == nil {
		return errors.New("key not found in batch or marked for deletion")
	}

	op.value = value
	wb.operations[string(key)] = op
	wb.valueSize += len(value)
	return nil
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}
	k := append([]byte(nil), key...)
	var v []byte
	if value == nil || len(value) == 0 {
		v = make([]byte, 0)
	} else {
		v = append([]byte(nil), value...)
	}
	wb.add(k, v, 0, 0, 0, ValueModified)
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}
	wb.delete(append([]byte(nil), key...))
	return nil
}

func (wb *WriteBatch) DeleteRange(start []byte, end []byte) error {
	var startCopy, endCopy []byte
	if start != nil {
		startCopy = append([]byte(nil), start...)
	}
	if end != nil {
		endCopy = append([]byte(nil), end...)
	}
	if startCopy != nil && endCopy != nil && bytes.Compare(startCopy, endCopy) >= 0 {
		return nil
	}

	wb.lock.Lock()
	wb.rangeOps = append(wb.rangeOps, DeleteRangeOperation{
		start: startCopy,
		end:   endCopy,
	})
	wb.valueSize += len(startCopy) + len(endCopy)
	wb.lock.Unlock()

	wb.checkAndCommit()
	return nil
}

func (wb *WriteBatch) ValueSize() int {
	wb.lock.Lock()
	defer wb.lock.Unlock()
	return wb.valueSize
}

func (wb *WriteBatch) Write() error {
	return wb.CommitBatch()
}

func (wb *WriteBatch) Reset() {
	wb.lock.Lock()
	defer wb.lock.Unlock()
	wb.operations = make(map[string]WriteOperation)
	wb.rangeOps = nil
	wb.valueSize = 0
}

func (wb *WriteBatch) Replay(w ethdb.KeyValueWriter) error {
	wb.lock.Lock()
	opsCopy := make(map[string]WriteOperation, len(wb.operations))
	for key, op := range wb.operations {
		opsCopy[key] = op
	}
	ranges := make([]DeleteRangeOperation, len(wb.rangeOps))
	copy(ranges, wb.rangeOps)
	wb.lock.Unlock()

	if len(ranges) > 0 {
		type rangeDeleter interface {
			DeleteRange(start []byte, end []byte) error
		}
		rd, ok := w.(rangeDeleter)
		if !ok {
			return errors.New("Replay target does not support DeleteRange")
		}
		for _, rop := range ranges {
			if err := rd.DeleteRange(rop.start, rop.end); err != nil {
				return err
			}
		}
	}

	for key, op := range opsCopy {
		if op.modifiedType == None {
			continue
		}
		if op.value == nil {
			if err := w.Delete([]byte(key)); err != nil {
				return err
			}
			continue
		}
		if err := w.Put([]byte(key), op.value); err != nil {
			return err
		}
	}
	return nil
}

// Commit writes all operations in the batch to pebble through a native pebble.Batch.
func (db *PebbleStore) WriteCommit(batch *WriteBatch) error {
	if db == nil || db.db == nil || db.quitChan == nil {
		return ErrClosed
	}
	if batch == nil || (len(batch.operations) == 0 && len(batch.rangeOps) == 0) {
		return nil
	}

	operations := batch.operations
	rangeOps := batch.rangeOps
	batch.operations = nil
	batch.rangeOps = nil

	pb := db.db.NewBatch()
	defer pb.Close()

	for _, rop := range rangeOps {
		if rop.start != nil && rop.end != nil && bytes.Compare(rop.start, rop.end) >= 0 {
			continue
		}
		if rop.start == nil && rop.end == nil {
			iter, err := db.db.NewIter(nil)
			if err != nil {
				return err
			}
			for iter.First(); iter.Valid(); iter.Next() {
				if err := pb.Delete(append([]byte(nil), iter.Key()...), nil); err != nil {
					iter.Close()
					return err
				}
			}
			if err := iter.Close(); err != nil {
				return err
			}
			continue
		}
		if err := pb.DeleteRange(rop.start, rop.end, nil); err != nil {
			return err
		}
	}

	for key, op := range operations {
		switch op.modifiedType {
		case None:
			continue
		case ValueModified:
			keyBytes := []byte(key)
			if op.value == nil {
				if err := pb.Delete(keyBytes, nil); err != nil {
					return err
				}
				continue
			}
			if err := pb.Set(keyBytes, op.value, nil); err != nil {
				return err
			}
		}
	}

	return pb.Commit(pebble.Sync)
}
