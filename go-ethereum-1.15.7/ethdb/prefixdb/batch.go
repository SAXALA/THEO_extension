package prefixdb

import (
	"errors"
	"io"
	"sync"
)

type WriteBatch struct {
	operations map[string]WriteOperation
	lock       sync.Mutex
}

type WriteOperation struct {
	value       []byte
	accountType AccountType
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		operations: make(map[string]WriteOperation),
	}
}

func (wb *WriteBatch) add(key, value []byte, accountType AccountType) {
	wb.lock.Lock()
	defer wb.lock.Unlock()
	wb.operations[string(key)] = WriteOperation{value: value, accountType: accountType}
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

	var NAEntry []byte
	var CAEntry []byte
	NAOffset, _ := db.normalAccountFile.Seek(0, io.SeekEnd)
	// Write all operations to the database
	for key, op := range batch.operations {
		entry, _ := db.ConvertKV([]byte(key), op.value)
		slotIndex := db.slotManager.getEmptySlot()
		switch op.accountType {

		case NormalAccount:
			NAEntry = append(NAEntry, entry...)
			db.setOffset([]byte(key), NAOffset)
			NAOffset += int64(len(entry))

		case ContractAccount:
			if len(CAEntry)+len(entry) > db.slotManager.slotSize {
				// If the current entry exceeds the slot size, write the current CAEntry to the database
				slotIndex = db.slotManager.getEmptySlot()
				if slotIndex == -1 {
					return errors.New("no empty slot available")
				}
				// Pad the CAEntry with zeros to fill the slot
				// padding := make([]byte, db.slotManager.slotSize-len(CAEntry))
				// CAEntry = append(CAEntry, padding...)
				db.contractAccountFile.WriteAt(CAEntry, int64((slotIndex)*db.slotManager.slotSize))
				CAEntry = nil
			}
			CAEntry = append(CAEntry, entry...)
			db.setSlotIndex([]byte(key), slotIndex)
		default:
			return errors.New("unknown account type")
		}
	}
	// Write the remaining CAEntry to the file
	if len(CAEntry) > 0 {
		slotIndex := db.slotManager.getEmptySlot()
		if slotIndex == -1 {
			return errors.New("no empty slot available")
		}
		// Pad the remaining CAEntry with zeros to fill the slot
		// padding := make([]byte, db.slotManager.slotSize-len(CAEntry))
		// CAEntry = append(CAEntry, padding...)
		db.contractAccountFile.WriteAt(CAEntry, int64((slotIndex)*db.slotManager.slotSize))
		CAEntry = nil
	}
	// Write the NAEntry to the file
	if len(NAEntry) > 0 {
		_, err := db.normalAccountFile.WriteAt(NAEntry, NAOffset-int64(len(NAEntry)))
		if err != nil {
			return err
		}
	}
	// Clear the batch after committing
	batch.operations = nil
	return nil
}
