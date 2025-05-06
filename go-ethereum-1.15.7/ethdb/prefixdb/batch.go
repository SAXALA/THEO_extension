package prefixdb

import (
	"sync"
)

type WriteBatch struct {
	operations map[string]WriteOperation
	lock       sync.Mutex
}

type WriteOperation struct {
	value       []byte
	position    int64
	accountType AccountType
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		operations: make(map[string]WriteOperation),
	}
}

func (wb *WriteBatch) add(key, value []byte, position int64, accountType AccountType) {
	wb.lock.Lock()
	defer wb.lock.Unlock()
	wb.operations[string(key)] = WriteOperation{value: value, position: position, accountType: accountType}
}

func (wb *WriteBatch) get(key []byte) ([]byte, bool) {
	wb.lock.Lock()
	defer wb.lock.Unlock()
	op, exists := wb.operations[string(key)]
	return op.value, exists
}

// Commit writes all operations in the batch to the database
func (db *PrefixDB) WriteCommit(batch *WriteBatch) error {
	batch.lock.Lock()
	defer batch.lock.Unlock()

	// Write all operations to the database
	for key, op := range batch.operations {
		if err := db.writeToFile(op.position, []byte(key), op.value, op.accountType); err != nil {
			return err
		}
	}
	// Clear the batch after committing
	batch.operations = nil
	return nil
}
