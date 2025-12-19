package prefixdb

import (
	"errors"
	"os"
	"sync"
)

type Slot struct {
	appendOnlyPart map[string][]byte // append-only part
	// accessedPart   map[string][]byte // sorted part
}

type SlotManager struct {
	slotStatus []bool // true means the slot is full
	usedSizes  []int
	lock       sync.Mutex
	slotSize   int
	slotNum    int
}

func NewSlotManager(slotNum int, slotSize int) *SlotManager {
	return &SlotManager{
		slotStatus: make([]bool, slotNum),
		usedSizes:  make([]int, slotNum),
		slotSize:   slotSize,
		slotNum:    slotNum,
	}
}

func (s *SlotManager) getEmptySlot() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	for i, status := range s.usedSizes {
		if status == 0 {
			return i
		}
	}
	return -1
}

func (s *SlotManager) updateUsedSize(slotIndex int, size int) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if slotIndex < 0 || slotIndex >= s.slotNum {
		return errors.New("invalid slot index")
	}

	if s.usedSizes[slotIndex]+size > APPENDONLY_SIZE {
		return errors.New("slot is full")
	}

	s.usedSizes[slotIndex] += size
	return nil
}

func (s *SlotManager) releaseSlot(slotIndex int, file *os.File) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if slotIndex >= 0 && slotIndex < s.slotNum {
		// Clear the slot content in the file
		offset := int64(slotIndex * s.slotSize)
		emptyData := make([]byte, s.slotSize)
		_, err := file.WriteAt(emptyData, offset)
		if err != nil {
			panic("failed to clear slot content: " + err.Error())
		}

		// Reset slot status and used size
		s.slotStatus[slotIndex] = false
		s.usedSizes[slotIndex] = 0
	}
}

func (s *SlotManager) setSlotStatus(slotIndex int, status bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if slotIndex >= 0 && slotIndex < s.slotNum {
		s.slotStatus[slotIndex] = status
	}
}
