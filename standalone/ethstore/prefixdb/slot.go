package prefixdb

import (
	"fmt"
	"os"
	"sort"
	"sync"
)

type Slot struct {
	appendOnlyPart map[string][]byte // append-only part
	// usedSize       int               // used size in the slot
}

type SlotManager struct {
	// usedSizes []int // used size in each slot
	lock      sync.Mutex
	slotSize  int
	slotNum   int
	usedSlots map[int]struct{}
	freeSlots map[int]struct{} // cache of free slots for quick access
}

func NewSlotManager(slotNum int, slotSize int) *SlotManager {
	sm := &SlotManager{
		// usedSizes: make([]int, slotNum),
		slotSize:  slotSize,
		slotNum:   slotNum,
		usedSlots: make(map[int]struct{}),
		freeSlots: make(map[int]struct{}, slotNum),
	}

	for i := 0; i < slotNum; i++ {
		sm.freeSlots[i] = struct{}{}
	}

	return sm
}

func (s *SlotManager) getEmptySlot() int {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.freeSlots) > 0 {
		for slotIndex := range s.freeSlots {
			delete(s.freeSlots, slotIndex)
			s.usedSlots[slotIndex] = struct{}{}
			return slotIndex
		}
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

	//newUsedSizes := make([]int, newSize)
	//copy(newUsedSizes, s.usedSizes)
	//s.usedSizes = newUsedSizes

	// Initialize new free slots
	for i := currentSize; i < newSize; i++ {
		s.freeSlots[i] = struct{}{}
	}

	s.slotNum = newSize

	firstNewIndex := currentSize
	//s.usedSizes[firstNewIndex] = 0

	return firstNewIndex
}

// // Resize 调整SlotManager的大小，确保有足够空间
// func (s *SlotManager) Resize(newSize int) {
// 	s.lock.Lock()
// 	defer s.lock.Unlock()

// 	if newSize > len(s.usedSizes) {
// 		newUsedSizes := make([]int, newSize)
// 		copy(newUsedSizes, s.usedSizes)
// 		s.usedSizes = newUsedSizes
// 		s.slotNum = newSize
// 	}
// }

// func (s *SlotManager) getSlotUsedSize(slotIndex int) int {
// 	s.lock.Lock()
// 	defer s.lock.Unlock()

// 	if slotIndex < 0 || slotIndex >= s.slotNum {
// 		return SLOT_SIZE
// 	}
// 	return s.usedSizes[slotIndex]
// }

// func (s *SlotManager) updateUsedSize(slotIndex int, size int) error {
// 	s.lock.Lock()
// 	defer s.lock.Unlock()

// 	if slotIndex < 0 || slotIndex >= s.slotNum {
// 		return errors.New("invalid slot index")
// 	}

// 	if s.usedSizes[slotIndex]+size > SLOT_SIZE {
// 		return errors.New("slot is full")
// 	}

// 	s.usedSizes[slotIndex] += size
// 	return nil
// }

// func (s *SlotManager) setSlotUsedSize(slotIndex int, size int) {
// 	s.lock.Lock()
// 	defer s.lock.Unlock()

// 	if slotIndex >= 0 && slotIndex < s.slotNum {
// 		s.usedSizes[slotIndex] = size
// 	}
// }

func (s *SlotManager) releaseSlot(slotIndex int, file *os.File) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if slotIndex >= 0 && slotIndex < s.slotNum {
		// Clear the slot content in the file
		// offset := int64(slotIndex * s.slotSize)
		// emptyData := make([]byte, s.slotSize)
		// _, err := file.WriteAt(emptyData, offset)
		// if err != nil {
		// 	panic("failed to clear slot content: " + err.Error())
		// }

		delete(s.usedSlots, slotIndex)
		s.freeSlots[slotIndex] = struct{}{}
	}
}

// Check if a slot is free
func (s *SlotManager) isSlotFree(slotIndex int) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if slotIndex < 0 || slotIndex >= s.slotNum {
		return false
	}

	_, used := s.usedSlots[slotIndex]
	return !used
}

func (s *SlotManager) getAdjSlot(currentSlot int) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	nextSlot := currentSlot + 1
	if nextSlot >= s.slotNum {
		// no more slots available,expand the slots
		s.expandSlots()
	}

	if _, used := s.usedSlots[nextSlot]; used {
		return -1
	}

	if _, free := s.freeSlots[nextSlot]; free {
		delete(s.freeSlots, nextSlot)
		s.usedSlots[nextSlot] = struct{}{}
		return nextSlot
	}

	return -1
}

func (s *SlotManager) findContFreeSlot(count int) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	if count <= 0 {
		fmt.Println("Invalid count for free slots:", count)
		return -1
	}

	if len(s.freeSlots) < count {
		return s.expandSlots()
	}

	sortedSlots := make([]int, 0, len(s.freeSlots))
	for slot := range s.freeSlots {
		sortedSlots = append(sortedSlots, slot)
	}
	sort.Ints(sortedSlots)

	startIndex := -1
	consecutiveCount := 1
	for i := 0; i < len(sortedSlots)-1; i++ {
		if sortedSlots[i+1] == sortedSlots[i]+1 {
			if consecutiveCount == 1 {
				startIndex = sortedSlots[i]
			}
			consecutiveCount++
			if consecutiveCount == count {
				return startIndex
			}
		} else {
			consecutiveCount = 1
			startIndex = -1
		}
	}
	// fmt.Println("No contiguous free slots found for count:", count)
	return s.expandSlots()
}

func (s *SlotManager) allocateContiguousSlots(startSlot, count int) []int {
	s.lock.Lock()
	defer s.lock.Unlock()

	for i := 0; i < count; i++ {
		slotIndex := startSlot + i

		if slotIndex >= s.slotNum {
			s.expandSlots()
		}

		if _, used := s.usedSlots[slotIndex]; used {
			fmt.Println("Slot", slotIndex, "is already used")
			return nil
		}

		if _, free := s.freeSlots[slotIndex]; !free {
			fmt.Println("Slot", slotIndex, "is not in free list")
			return nil
		}
	}

	allocated := make([]int, count)
	for i := 0; i < count; i++ {
		slotIndex := startSlot + i
		delete(s.freeSlots, slotIndex)
		s.usedSlots[slotIndex] = struct{}{}
		allocated[i] = slotIndex
	}

	return allocated
}

func (s *SlotManager) releaseContiguousSlots(startSlot, count int, file *os.File) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// offset := startSlot * s.slotSize
	// emptyData := make([]byte, count*s.slotSize)
	// _, err := file.WriteAt(emptyData, int64(offset))
	// if err != nil {
	// 	panic("failed to clear contiguous slots content: " + err.Error())
	// }

	for i := 0; i < count; i++ {
		slotIndex := startSlot + i
		if slotIndex >= s.slotNum {
			continue
		}
		delete(s.usedSlots, slotIndex)
		s.freeSlots[slotIndex] = struct{}{}
	}
	// sort.Ints(s.freeSlots) // Keep free slots sorted for easier management
}
