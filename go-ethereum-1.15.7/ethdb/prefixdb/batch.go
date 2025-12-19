package prefixdb

import (
	"io"
	"sync"
)

type WriteBatch struct {
	operations map[string]WriteOperation
	lock       sync.Mutex
}

type WriteOperation struct {
	value       []byte
	accountType KeyType
	slotIndex   int // New field to record the slotIndex for the key-value pair
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		operations: make(map[string]WriteOperation),
	}
}

func (wb *WriteBatch) add(key, value []byte, accountType KeyType, slotIndex int) {
	wb.lock.Lock()
	defer wb.lock.Unlock()
	wb.operations[string(key)] = WriteOperation{value: value, accountType: accountType, slotIndex: slotIndex}
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

	// Store pending write data for each slot
	slotEntries := make(map[int][]byte)

	trieAccountffset, _ := db.accountFile.Seek(0, io.SeekEnd)

	// Process operations by type
	for key, op := range batch.operations {
		entry, _ := db.ConvertKV([]byte(key), op.value)

		switch op.accountType {
		case TrieAccount:
			// 使用slotIndex判断账户类型
			if op.slotIndex != 0 {
				// 智能合约账户，写入对应的slot
				if _, exists := slotEntries[op.slotIndex]; !exists {
					slotEntries[op.slotIndex] = make([]byte, 0)
				}
				slotEntries[op.slotIndex] = append(slotEntries[op.slotIndex], entry...)
			} else {
				// 普通账户写入账户文件
				NAEntry = append(NAEntry, entry...)
				db.setOffset([]byte(key), trieAccountffset)
				trieAccountffset += int64(len(entry))
			}

		case TrieStorage, TrieCode:
			// Use the recorded slotIndex directly, no need to lookup again
			if op.slotIndex <= 0 {
				continue // Skip invalid entries
			}

			// Add to the pending write data for the corresponding slot
			if _, exists := slotEntries[op.slotIndex]; !exists {
				slotEntries[op.slotIndex] = make([]byte, 0)
			}
			slotEntries[op.slotIndex] = append(slotEntries[op.slotIndex], entry...)
		}
	}

	// Write normal account data
	if len(NAEntry) > 0 {
		_, err := db.accountFile.WriteAt(NAEntry, trieAccountffset-int64(len(NAEntry)))
		if err != nil {
			return err
		}
	}

	// Write Storage and Code data to respective slots
	for slotIndex, entryData := range slotEntries {
		// First get slot data from cache - O(1) time complexity
		var slotData map[string][]byte
		var exists bool

		slotData, exists = db.slotCache.Get(slotIndex)
		if !exists {
			// If not in cache, load from file
			var err error
			slotData, err = db.loadSlot(slotIndex)
			if err != nil {
				slotData = make(map[string][]byte)
			}
		}

		// Parse entryData and update slotData
		data := entryData
		for len(data) > 0 && len(data) >= 4 {
			keySize := int(data[0])<<8 | int(data[1])
			valueSize := int(data[2])<<8 | int(data[3])

			if 4+keySize+valueSize > len(data) {
				break
			}

			key := string(data[4 : 4+keySize])
			value := data[4+keySize : 4+keySize+valueSize]
			slotData[key] = value

			data = data[4+keySize+valueSize:]
		}

		// Write to slot in append-only mode
		slot := &Slot{
			appendOnlyPart: slotData,
		}
		db.saveSlot(slotIndex, slot)

		// Update cache - O(1) time complexity
		db.slotCache.Put(slotIndex, slotData)
	}

	// Clear batch
	batch.operations = make(map[string]WriteOperation)
	return nil
}
