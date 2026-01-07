package prefixdb

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

type WriteBatch struct {
	operations map[string]WriteOperation
	// slotBatch  map[int]map[string][]byte
	lock       sync.Mutex
	autoCommit bool // enable auto commit feature
	threshold  int  // the threshold for auto commit
	db         *PrefixDB

	// for background commit
	commitQueue chan *WriteBatch // the queue for commit batches
	quitCh      chan struct{}    // the quit channel for background commit
	wg          sync.WaitGroup   // wait group for  shutdown
	bgCommit    bool             // enable background commit
}

type WriteOperation struct {
	value         []byte
	modifiedType  ModifiedType // type of modification (0: None, 1: value changed, 2: slotIndices changed)
	storageFileID uint32       // ID of the storage file for the account
	storageOffset int64        // offset in the file for the account
	storageSize   uint64       // size of the stored data
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

func (wb *WriteBatch) EnableAutoCommit(db *PrefixDB, threshold int) {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	wb.autoCommit = true
	wb.db = db
	if threshold > 0 {
		wb.threshold = threshold
	}
}

// EnableBackgroundCommit
func (wb *WriteBatch) EnableBackgroundCommit(db *PrefixDB) {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	if !wb.bgCommit {
		wb.db = db
		wb.bgCommit = true
		wb.wg.Add(1)

		// 启动后台处理线程
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
			// 处理剩余的任务
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
	totalOps := len(wb.operations)
	needCommit := wb.autoCommit && wb.db != nil && totalOps >= wb.threshold

	var batchToCommit *WriteBatch
	if needCommit {
		batchToCommit = &WriteBatch{
			operations: make(map[string]WriteOperation, len(wb.operations)),

			db: wb.db,
		}

		batchToCommit.operations = wb.operations
		wb.operations = make(map[string]WriteOperation)
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

	wb.operations = make(map[string]WriteOperation)

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
	wb.lock.Unlock()

	wb.checkAndCommit()
}

// delete marks a key for deletion in the batch
func (wb *WriteBatch) delete(key []byte) {
	wb.lock.Lock()
	// Use nil value to indicate deletion
	if wb.operations[string(key)].value == nil {
		// If the key is already marked for deletion, do nothing
		wb.lock.Unlock()
		return
	}
	wb.operations[string(key)] = WriteOperation{value: nil, storageFileID: 0, storageOffset: 0, storageSize: 0, modifiedType: 0}
	wb.lock.Unlock()

	wb.checkAndCommit()
}

func (wb *WriteBatch) get(key []byte) ([]byte, CacheInfo, bool) {
	wb.lock.Lock()
	defer wb.lock.Unlock()
	op, exists := wb.operations[string(key)]
	if exists && op.value == nil {
		return nil, CacheInfo{}, false
	}
	return op.value, CacheInfo{
		storageFileID: op.storageFileID,
		storageOffset: op.storageOffset,
		storageSize:   op.storageSize,
	}, exists
}

func (wb *WriteBatch) updateStoragePointer(key []byte, cacheInfo CacheInfo) error {
	wb.lock.Lock()
	defer wb.lock.Unlock()

	op, exists := wb.operations[string(key)]
	if !exists || op.value == nil {
		return errors.New("key not found in batch or marked for deletion")
	}

	op.storageFileID = cacheInfo.storageFileID
	op.storageOffset = cacheInfo.storageOffset
	op.storageSize = cacheInfo.storageSize
	wb.operations[string(key)] = op
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
	return nil
}

// Commit writes all operations in the batch to the database
func (db *PrefixDB) WriteCommit(batch *WriteBatch) error {
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()

	operations := batch.operations
	// slotBatch := batch.slotBatch

	batch.operations = nil

	var NAEntry = make([]byte, 0)
	trieAccountOffset, _ := db.accountFile.Seek(0, io.SeekEnd)

	if trieAccountOffset == 0 {
		trieAccountOffset = 1 // Ensure we start writing at a non-zero offset
	}

	// Process
	for key, op := range operations {
		keyBytes := []byte(key)
		switch op.modifiedType {
		case None:
			// No changes, skip
			continue
		case ValueModified:
			entry, err := db.ConvertKV(keyBytes, op.value)
			if err != nil {
				return err
			}

			NAEntry = append(NAEntry, entry...)
			trieAccountOffset += int64(len(entry))

			node := &TrieNode{
				storageFileID: op.storageFileID,
				storageOffset: op.storageOffset,
				storageSize:   op.storageSize,
				offset:        trieAccountOffset - int64(len(entry)),
			}

			if err := db.storeNode(keyBytes, node); err != nil {
				return err
			}
		}
	}

	if len(NAEntry) > 0 {
		_, err := db.accountFile.WriteAt(NAEntry, trieAccountOffset-int64(len(NAEntry)))
		if err != nil {
			return err
		}
	}
	return nil
}
