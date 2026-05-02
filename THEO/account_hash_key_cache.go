package theo

import "sync"

const defaultAccountHashToKeyCacheCapacity = 10_000

// accountHashToKeyCache is an in-memory fixed-entry-size LRU cache.
//
// - Key: fixed 32 bytes (accountHash)
// - Value: max 64 bytes (accountKey) stored in a fixed [64]byte with an explicit length
// - Capacity: fixed number of entries (default 10K)
//
// NOTE: The returned data is copied out by callers; cache entries themselves are fixed-size.
// The internal index map is not fixed-size, but is bounded by capacity.
type accountHashToKeyCache struct {
	mu   sync.Mutex
	cap  int32
	size int32

	head int32
	tail int32

	index   map[[32]byte]int32
	entries []accountHashToKeyCacheEntry
}

type accountHashToKeyCacheEntry struct {
	hash   [32]byte
	key    [64]byte
	keyLen uint8

	prev  int32
	next  int32
	inUse bool
}

func newAccountHashToKeyCache(capacity int) *accountHashToKeyCache {
	if capacity <= 0 {
		capacity = defaultAccountHashToKeyCacheCapacity
	}
	c := &accountHashToKeyCache{
		cap:     int32(capacity),
		head:    -1,
		tail:    -1,
		index:   make(map[[32]byte]int32, capacity),
		entries: make([]accountHashToKeyCacheEntry, capacity),
	}
	for i := range c.entries {
		c.entries[i].prev = -1
		c.entries[i].next = -1
	}
	return c
}

func (c *accountHashToKeyCache) Get(accountHash []byte, dst *[64]byte) (n uint8, ok bool) {
	if c == nil || dst == nil {
		return 0, false
	}
	if len(accountHash) != 32 {
		return 0, false
	}
	var k [32]byte
	copy(k[:], accountHash)

	c.mu.Lock()
	defer c.mu.Unlock()

	idx, ok := c.index[k]
	if !ok {
		return 0, false
	}
	e := &c.entries[idx]
	if !e.inUse {
		delete(c.index, k)
		return 0, false
	}

	if e.keyLen > 0 {
		copy(dst[:], e.key[:e.keyLen])
	}
	n = e.keyLen
	c.moveToFront(idx)
	return n, true
}

func (c *accountHashToKeyCache) Put(accountHash []byte, accountKey []byte) {
	if c == nil {
		return
	}
	if len(accountHash) != 32 {
		return
	}
	if len(accountKey) == 0 || len(accountKey) > 64 {
		// Out of spec; keep correctness by not caching.
		return
	}

	var k [32]byte
	copy(k[:], accountHash)

	c.mu.Lock()
	defer c.mu.Unlock()

	if idx, ok := c.index[k]; ok {
		e := &c.entries[idx]
		copy(e.key[:], accountKey)
		e.keyLen = uint8(len(accountKey))
		copy(e.hash[:], accountHash)
		e.inUse = true
		c.moveToFront(idx)
		return
	}

	idx := c.allocIndexLocked()
	e := &c.entries[idx]

	// If reusing an entry, remove its old mapping.
	if e.inUse {
		delete(c.index, e.hash)
		c.detach(idx)
	}

	copy(e.hash[:], accountHash)
	copy(e.key[:], accountKey)
	e.keyLen = uint8(len(accountKey))
	e.inUse = true

	c.attachFront(idx)
	c.index[k] = idx
}

func (c *accountHashToKeyCache) allocIndexLocked() int32 {
	if c.size < c.cap {
		idx := c.size
		c.size++
		return idx
	}
	// Evict LRU (tail)
	if c.tail >= 0 {
		return c.tail
	}
	// Should be unreachable if cap>0, but keep safe.
	return 0
}

func (c *accountHashToKeyCache) moveToFront(idx int32) {
	if idx == c.head {
		return
	}
	c.detach(idx)
	c.attachFront(idx)
}

func (c *accountHashToKeyCache) detach(idx int32) {
	e := &c.entries[idx]
	p := e.prev
	n := e.next

	if p >= 0 {
		c.entries[p].next = n
	} else {
		c.head = n
	}
	if n >= 0 {
		c.entries[n].prev = p
	} else {
		c.tail = p
	}
	e.prev = -1
	e.next = -1
}

func (c *accountHashToKeyCache) attachFront(idx int32) {
	e := &c.entries[idx]
	e.prev = -1
	e.next = c.head
	if c.head >= 0 {
		c.entries[c.head].prev = idx
	} else {
		c.tail = idx
	}
	c.head = idx
}
