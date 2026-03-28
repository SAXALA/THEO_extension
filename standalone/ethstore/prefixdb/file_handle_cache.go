package prefixdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	lru "github.com/hashicorp/golang-lru"
)

const (
	minFileHandleCacheSize = 128
	maxFileHandleCacheSize = 1048576
)

type fileHandleCacheKey struct {
	path string
	flag int
}

type fileHandleCache struct {
	cache       *lru.Cache
	closeCh     chan *os.File
	closeMu     sync.Mutex
	closeCond   *sync.Cond
	closePending int
}

var (
	globalFileHandleCache     *fileHandleCache
	globalFileHandleCacheOnce sync.Once
	globalHandleCacheHits     uint64
	globalHandleCacheMisses   uint64
)

func resolveFileHandleCacheCapacity(capacity int) int {
	if capacity <= 0 {
		return defaultFileHandleCacheCapacity()
	}
	if capacity < minFileHandleCacheSize {
		capacity = minFileHandleCacheSize
	}
	if capacity > maxFileHandleCacheSize {
		capacity = maxFileHandleCacheSize
	}
	fmt.Println("The file handle cache capacity is set to", capacity)
	return capacity
}

func defaultFileHandleCacheCapacity() int {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil || lim.Cur == 0 {
		return 2048
	}
	capacity := int(lim.Cur / 2)
	if capacity < minFileHandleCacheSize {
		capacity = minFileHandleCacheSize
	}
	if capacity > maxFileHandleCacheSize {
		capacity = maxFileHandleCacheSize
	}
	fmt.Println("The file handle cache capacity is set to", capacity)
	return capacity
}

func getGlobalFileHandleCache(capacity int) *fileHandleCache {
	globalFileHandleCacheOnce.Do(func() {
		cache, err := newFileHandleCache(resolveFileHandleCacheCapacity(capacity))
		if err != nil {
			panic(fmt.Sprintf("failed to init global file handle cache: %v", err))
		}
		globalFileHandleCache = cache
	})
	return globalFileHandleCache
}

func newFileHandleCache(capacity int) (*fileHandleCache, error) {
	if capacity < minFileHandleCacheSize {
		capacity = minFileHandleCacheSize
	}
	if capacity > maxFileHandleCacheSize {
		capacity = maxFileHandleCacheSize
	}
	fhc := &fileHandleCache{
		closeCh: make(chan *os.File, 1024),
	}
	fhc.closeCond = sync.NewCond(&fhc.closeMu)
	go fhc.closeWorker()
	cache, err := lru.NewWithEvict(capacity, func(key interface{}, value interface{}) {
		if f, ok := value.(*os.File); ok && f != nil {
			fhc.enqueueClose(f)
		}
	})
	if err != nil {
		return nil, err
	}
	fhc.cache = cache
	return fhc, nil
}

func (c *fileHandleCache) closeWorker() {
	for f := range c.closeCh {
		if f != nil {
			_ = f.Close()
		}
		c.closeMu.Lock()
		c.closePending--
		if c.closePending == 0 {
			c.closeCond.Broadcast()
		}
		c.closeMu.Unlock()
	}
}

func (c *fileHandleCache) enqueueClose(f *os.File) {
	if c == nil || f == nil {
		return
	}
	c.closeMu.Lock()
	c.closePending++
	c.closeMu.Unlock()
	select {
	case c.closeCh <- f:
	default:
		// Keep eviction path non-blocking even under close bursts.
		go func(file *os.File) {
			c.closeCh <- file
		}(f)
	}
}

func (c *fileHandleCache) waitForPendingCloses() {
	if c == nil {
		return
	}
	c.closeMu.Lock()
	for c.closePending > 0 {
		c.closeCond.Wait()
	}
	c.closeMu.Unlock()
}

func normalizeOpenPath(path string) string {
	if path == "" {
		return ""
	}
	if abs, err := filepath.Abs(path); err == nil {
		return abs
	}
	return path
}

func cacheableOpenFlag(flag int) bool {
	// Keep cache semantics simple and safe: cache stable read/read-write handles.
	mask := os.O_APPEND | os.O_CREATE | os.O_EXCL | os.O_SYNC | os.O_TRUNC | os.O_WRONLY
	return flag&mask == 0
}

func (c *fileHandleCache) Open(path string, flag int) (*os.File, error) {
	if c == nil || !cacheableOpenFlag(flag) {
		return os.OpenFile(path, flag, 0644)
	}
	key := fileHandleCacheKey{path: normalizeOpenPath(path), flag: flag}
	if v, ok := c.cache.Get(key); ok {
		atomic.AddUint64(&globalHandleCacheHits, 1)
		return v.(*os.File), nil
	}

	f, err := os.OpenFile(key.path, flag, 0644)
	if err != nil {
		return nil, err
	}

	if prev, ok, _ := c.cache.PeekOrAdd(key, f); ok {
		// Another goroutine won the race; avoid leaking the extra file.
		_ = f.Close()
		atomic.AddUint64(&globalHandleCacheHits, 1)
		return prev.(*os.File), nil
	}
	atomic.AddUint64(&globalHandleCacheMisses, 1)
	return f, nil
}

func (c *fileHandleCache) InvalidatePath(path string) {
	if c == nil {
		return
	}
	normalized := normalizeOpenPath(path)
	keys := c.cache.Keys()
	for _, kAny := range keys {
		k, ok := kAny.(fileHandleCacheKey)
		if !ok {
			continue
		}
		if k.path == normalized {
			// Remove triggers on-evict callback and schedules async close.
			c.cache.Remove(k)
		}
	}
}

func (c *fileHandleCache) Purge() {
	if c == nil {
		return
	}
	// Purge triggers on-evict callback and schedules async close for all files.
	c.cache.Purge()
	// Preserve purge semantics: when Purge returns, queued closes are finished.
	c.waitForPendingCloses()
}
