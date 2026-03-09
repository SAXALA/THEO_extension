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
	maxFileHandleCacheSize = 65536
)

type fileHandleCacheKey struct {
	path string
	flag int
}

type fileHandleCache struct {
	mu      sync.Mutex
	cache   *lru.Cache
	entries map[fileHandleCacheKey]*os.File
}

var (
	globalFileHandleCache     *fileHandleCache
	globalFileHandleCacheOnce sync.Once
	globalHandleCacheHits     uint64
	globalHandleCacheMisses   uint64
)

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

func getGlobalFileHandleCache() *fileHandleCache {
	globalFileHandleCacheOnce.Do(func() {
		cache, err := newFileHandleCache(defaultFileHandleCacheCapacity())
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
	fhc := &fileHandleCache{entries: make(map[fileHandleCacheKey]*os.File)}
	cache, err := lru.NewWithEvict(capacity, func(key interface{}, value interface{}) {
		if f, ok := value.(*os.File); ok && f != nil {
			_ = f.Close()
		}
	})
	if err != nil {
		return nil, err
	}
	fhc.cache = cache
	return fhc, nil
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

	c.mu.Lock()
	if f, ok := c.entries[key]; ok {
		c.cache.Get(key)
		c.mu.Unlock()
		atomic.AddUint64(&globalHandleCacheHits, 1)
		return f, nil
	}
	c.mu.Unlock()

	f, err := os.OpenFile(key.path, flag, 0644)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if existing, ok := c.entries[key]; ok {
		c.cache.Get(key)
		c.mu.Unlock()
		_ = f.Close()
		atomic.AddUint64(&globalHandleCacheHits, 1)
		return existing, nil
	}
	c.entries[key] = f
	c.cache.Add(key, f)
	c.mu.Unlock()
	atomic.AddUint64(&globalHandleCacheMisses, 1)
	return f, nil
}

func (c *fileHandleCache) InvalidatePath(path string) {
	if c == nil {
		return
	}
	normalized := normalizeOpenPath(path)
	c.mu.Lock()
	keys := make([]fileHandleCacheKey, 0, 4)
	for k := range c.entries {
		if k.path == normalized {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		delete(c.entries, k)
		c.cache.Remove(k)
	}
	c.mu.Unlock()
}

func (c *fileHandleCache) Purge() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries = make(map[fileHandleCacheKey]*os.File)
	c.cache.Purge()
	c.mu.Unlock()
}
