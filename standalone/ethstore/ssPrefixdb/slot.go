package ssPrefixdb

import (
	"errors"
	"os"
	"sync"
)

type Slot struct {
	appendOnlyPart map[string][]byte // append-only part
	// usedSize       int               // used size in the slot
}

type SlotManager struct {
	usedSizes []int // used size in each slot
	lock      sync.Mutex
	slotSize  int
	slotNum   int
	freeSlots []int // cache of free slots for quick access
}

func NewSlotManager(slotNum int, slotSize int) *SlotManager {
	sm := &SlotManager{
		usedSizes: make([]int, slotNum),
		slotSize:  slotSize,
		slotNum:   slotNum,
		freeSlots: make([]int, 0, slotNum),
	}

	for i := 0; i < slotNum; i++ {
		sm.freeSlots = append(sm.freeSlots, i)
	}

	return sm
}

func (s *SlotManager) getEmptySlot() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.freeSlots) > 0 {
		lastIndex := len(s.freeSlots) - 1
		slotIndex := s.freeSlots[lastIndex]
		s.freeSlots = s.freeSlots[:lastIndex]

		s.usedSizes[slotIndex] = 0
		return slotIndex
	}

	return s.expandSlots()
}

func (s *SlotManager) expandSlots() int {
	// Expand the slots by a certain strategy
	currentSize := s.slotNum
	expandSize := currentSize / 2
	if expandSize < 100 {
		expandSize = 100
	}

	newSize := currentSize + expandSize

	newUsedSizes := make([]int, newSize)
	copy(newUsedSizes, s.usedSizes)
	s.usedSizes = newUsedSizes

	// Initialize new free slots
	for i := currentSize + 1; i < newSize; i++ {
		s.freeSlots = append(s.freeSlots, i)
	}

	s.slotNum = newSize

	firstNewIndex := currentSize
	s.usedSizes[firstNewIndex] = 0

	return firstNewIndex
}

func (s *SlotManager) getSlotUsedSize(slotIndex int) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	if slotIndex < 0 || slotIndex >= s.slotNum {
		return SLOT_SIZE
	}
	return s.usedSizes[slotIndex]
}

func (s *SlotManager) updateUsedSize(slotIndex int, size int) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if slotIndex < 0 || slotIndex >= s.slotNum {
		return errors.New("invalid slot index")
	}

	if s.usedSizes[slotIndex]+size > SLOT_SIZE {
		return errors.New("slot is full")
	}

	s.usedSizes[slotIndex] += size
	return nil
}

func (s *SlotManager) setSlotUsedSize(slotIndex int, size int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if slotIndex >= 0 && slotIndex < s.slotNum {
		s.usedSizes[slotIndex] = size
	}
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

		s.usedSizes[slotIndex] = 0
		s.freeSlots = append(s.freeSlots, slotIndex)
	}
}

// Check if a slot is free
func (s *SlotManager) isSlotFree(slotIndex int) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if slotIndex < 0 || slotIndex >= s.slotNum {
		return false
	}

	for _, idx := range s.freeSlots {
		if idx == slotIndex {
			return true
		}
	}
	return false
}
