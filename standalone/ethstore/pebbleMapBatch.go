package ethstore

import (
	"sync"
)

type pebbleMapBatch struct {
	mu          sync.Mutex
	unresolved  map[string][]byte
	pebbleBatch *pebbleBatch
}

func newPebbleMapBatch() *pebbleMapBatch {
	return &pebbleMapBatch{
		unresolved: make(map[string][]byte),
	}
}

func (sb *pebbleMapBatch) reset() {
	sb.mu.Lock()
	sb.unresolved = make(map[string][]byte)
	sb.mu.Unlock()
}

func (sb *pebbleMapBatch) put(Key, value []byte) {
	sb.mu.Lock()
	keyStr := string(Key)
	if value == nil {
		sb.unresolved[keyStr] = nil
		sb.mu.Unlock()
		return
	}
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	sb.unresolved[keyStr] = valCopy
	sb.mu.Unlock()
}

func (sb *pebbleMapBatch) get(Key []byte) ([]byte, bool) {
	keyStr := string(Key)
	sb.mu.Lock()
	value, ok := sb.unresolved[keyStr]
	sb.mu.Unlock()
	return value, ok
}

func (sb *pebbleMapBatch) getAll() map[string][]byte {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	result := make(map[string][]byte)
	for k, v := range sb.unresolved {
		if v == nil {
			result[k] = nil
		}
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		result[k] = valCopy
	}
	return result
}
