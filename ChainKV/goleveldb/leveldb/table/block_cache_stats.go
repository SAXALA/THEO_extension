package table

import "sync/atomic"

type BlockCacheClass uint8

const (
	BlockCacheData BlockCacheClass = iota
	BlockCacheIndex
	BlockCacheFilter
)

type BlockCacheSnapshot struct {
	DataBlocks   int64
	DataBytes    int64
	IndexBlocks  int64
	IndexBytes   int64
	FilterBlocks int64
	FilterBytes  int64
}

type BlockCacheStats struct {
	dataBlocks   int64
	dataBytes    int64
	indexBlocks  int64
	indexBytes   int64
	filterBlocks int64
	filterBytes  int64
}

func (s *BlockCacheStats) add(class BlockCacheClass, size int) {
	if s == nil {
		return
	}
	switch class {
	case BlockCacheData:
		atomic.AddInt64(&s.dataBlocks, 1)
		atomic.AddInt64(&s.dataBytes, int64(size))
	case BlockCacheIndex:
		atomic.AddInt64(&s.indexBlocks, 1)
		atomic.AddInt64(&s.indexBytes, int64(size))
	case BlockCacheFilter:
		atomic.AddInt64(&s.filterBlocks, 1)
		atomic.AddInt64(&s.filterBytes, int64(size))
	}
}

func (s *BlockCacheStats) remove(class BlockCacheClass, size int) {
	if s == nil {
		return
	}
	switch class {
	case BlockCacheData:
		atomic.AddInt64(&s.dataBlocks, -1)
		atomic.AddInt64(&s.dataBytes, -int64(size))
	case BlockCacheIndex:
		atomic.AddInt64(&s.indexBlocks, -1)
		atomic.AddInt64(&s.indexBytes, -int64(size))
	case BlockCacheFilter:
		atomic.AddInt64(&s.filterBlocks, -1)
		atomic.AddInt64(&s.filterBytes, -int64(size))
	}
}

func (s *BlockCacheStats) Snapshot() BlockCacheSnapshot {
	if s == nil {
		return BlockCacheSnapshot{}
	}
	return BlockCacheSnapshot{
		DataBlocks:   atomic.LoadInt64(&s.dataBlocks),
		DataBytes:    atomic.LoadInt64(&s.dataBytes),
		IndexBlocks:  atomic.LoadInt64(&s.indexBlocks),
		IndexBytes:   atomic.LoadInt64(&s.indexBytes),
		FilterBlocks: atomic.LoadInt64(&s.filterBlocks),
		FilterBytes:  atomic.LoadInt64(&s.filterBytes),
	}
}
