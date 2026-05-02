package table

import (
	"bytes"
	"testing"

	"theo.local/ChainKV/goleveldb/leveldb/cache"
	"theo.local/ChainKV/goleveldb/leveldb/filter"
	"theo.local/ChainKV/goleveldb/leveldb/opt"
	"theo.local/ChainKV/goleveldb/leveldb/storage"
	"theo.local/ChainKV/goleveldb/leveldb/util"
)

func TestReaderBlockCacheStatsTracksCachedBlockKinds(t *testing.T) {
	o := &opt.Options{
		BlockSize:   128,
		Compression: opt.NoCompression,
		Filter:      filter.NewBloomFilter(10),
	}
	buf := &bytes.Buffer{}
	tw := NewWriter(buf, o)
	for i := 0; i < 32; i++ {
		key := []byte{byte('k'), byte('0' + i/10), byte('0' + i%10)}
		value := bytes.Repeat([]byte{'v'}, 32)
		if err := tw.Append(key, value); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	blockCache := cache.NewCache(cache.NewLRU(1 << 20))
	stats := &BlockCacheStats{}
	tr, err := NewReader(
		bytes.NewReader(buf.Bytes()),
		int64(buf.Len()),
		storage.FileDesc{},
		&cache.NamespaceGetter{Cache: blockCache, NS: 1},
		util.NewBufferPool(o.GetBlockSize()+5),
		o,
		stats,
	)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}

	if _, _, err := tr.Find([]byte("k15"), true, nil); err != nil {
		t.Fatalf("find with filter: %v", err)
	}

	snap := stats.Snapshot()
	if snap.IndexBlocks == 0 || snap.IndexBytes == 0 {
		t.Fatalf("expected index block to be cached, got %+v", snap)
	}
	if snap.FilterBlocks == 0 || snap.FilterBytes == 0 {
		t.Fatalf("expected filter block to be cached, got %+v", snap)
	}
	if snap.DataBlocks == 0 || snap.DataBytes == 0 {
		t.Fatalf("expected data block to be cached, got %+v", snap)
	}

	tr.Release()
	if err := blockCache.Close(); err != nil {
		t.Fatalf("close block cache: %v", err)
	}

	snap = stats.Snapshot()
	if snap != (BlockCacheSnapshot{}) {
		t.Fatalf("expected block cache stats to be released, got %+v", snap)
	}
}
