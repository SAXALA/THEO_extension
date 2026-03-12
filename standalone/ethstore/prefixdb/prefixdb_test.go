package prefixdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestSortStrategyThreshold(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const maxEntries = 1 << 14
	const runsPerSample = 16
	threshold := 0
	for n := 8; n <= maxEntries; n <<= 1 {
		pairs := makeRandomKVPairs(n, rng)
		std := measureSortDuration(pairs, runsPerSample, sortSliceKVPairs)
		merge := measureSortDuration(pairs, runsPerSample, sortKVPairs)
		fmt.Printf("entries=%d std=%s merge=%s", n, std, merge)
		if merge < std {
			threshold = n
			break
		}
	}
	if threshold == 0 {
		t.Logf("merge sort did not outperform std sort up to %d entries", maxEntries)
	} else {
		t.Logf("merge sort becomes faster at %d entries", threshold)
	}
}

func measureSortDuration(entries []kvPair, runs int, sorter func([]kvPair)) time.Duration {
	if len(entries) == 0 {
		return 0
	}
	buf := make([]kvPair, len(entries))
	start := time.Now()
	for i := 0; i < runs; i++ {
		copy(buf, entries)
		sorter(buf)
	}
	return time.Since(start)
}

func makeRandomKVPairs(n int, rng *rand.Rand) []kvPair {
	pairs := make([]kvPair, n)
	for i := range pairs {
		keyLen := rng.Intn(32) + 1
		valLen := rng.Intn(64) + 1
		key := make([]byte, keyLen)
		val := make([]byte, valLen)
		_, _ = rng.Read(key)
		_, _ = rng.Read(val)
		pairs[i] = kvPair{key: key, val: val}
	}
	return pairs
}

func sortSliceKVPairs(entries []kvPair) {
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
}

func TestSegmentedChunkSizePolicy(t *testing.T) {
	db := &PrefixDB{
		storageChunkSize:        16 * 1024,
		segmentedChunkHardLimit: 32 * 1024,
	}

	if got := db.segmentedChunkTargetSize(); got != 16*1024 {
		t.Fatalf("target size mismatch: got %d, want %d", got, 16*1024)
	}
	if got := db.segmentedChunkTriggerSize(); got != 32*1024 {
		t.Fatalf("trigger size mismatch: got %d, want %d", got, 32*1024)
	}
}

func TestSegmentedChunkSizePolicyFallback(t *testing.T) {
	db := &PrefixDB{}

	if got := db.segmentedChunkTargetSize(); got <= 0 {
		t.Fatalf("target size should be positive, got %d", got)
	}
	if got := db.segmentedChunkTriggerSize(); got <= 0 {
		t.Fatalf("trigger size should be positive, got %d", got)
	}
}

func TestSegmentIndexCacheRespectsByteBudget(t *testing.T) {
	cache := newSegmentIndexCache(1)
	if cache == nil {
		t.Fatal("expected non-nil cache")
	}

	metas1 := []segmentChunkMeta{{
		FileName: strings.Repeat("a", 256*1024),
		KeyStart: bytes.Repeat([]byte{0x01}, 128*1024),
		KeyEnd:   bytes.Repeat([]byte{0x02}, 128*1024),
	}}
	metas2 := []segmentChunkMeta{{
		FileName: strings.Repeat("b", 256*1024),
		KeyStart: bytes.Repeat([]byte{0x03}, 128*1024),
		KeyEnd:   bytes.Repeat([]byte{0x04}, 128*1024),
	}}

	cache.Add(1, metas1)
	if _, ok := cache.Get(1); !ok {
		t.Fatal("expected first cache entry to exist")
	}

	cache.Add(2, metas2)
	if _, ok := cache.Get(2); !ok {
		t.Fatal("expected second cache entry to exist")
	}
	if _, ok := cache.Get(1); ok {
		t.Fatal("expected first cache entry to be evicted after exceeding byte budget")
	}
	if cache.usedBytes > cache.capacityBytes {
		t.Fatalf("cache exceeds byte budget: used=%d capacity=%d", cache.usedBytes, cache.capacityBytes)
	}
}

func TestSharedCacheEvictsAcrossCacheTypes(t *testing.T) {
	shared := newSharedByteCache(1024)
	nodeCache := newSharedNodeCache(shared)
	storageCache := newSharedStorageValueCache(shared)
	segmentCache := newSharedSegmentIndexCache(shared)

	nodeCache.Put(NodeCacheEntry{
		Key:   "node",
		Value: bytes.Repeat([]byte{0x01}, 360),
	})
	if _, ok := nodeCache.Get("node"); !ok {
		t.Fatal("expected node cache entry to exist")
	}

	storageCache.Add("storage", bytes.Repeat([]byte{0x02}, 360))
	if _, ok := storageCache.Get("storage"); !ok {
		t.Fatal("expected storage cache entry to exist")
	}

	segmentCache.Add(7, []segmentChunkMeta{{
		FileName: strings.Repeat("c", 220),
		KeyStart: bytes.Repeat([]byte{0x03}, 120),
		KeyEnd:   bytes.Repeat([]byte{0x04}, 120),
	}})
	if _, ok := segmentCache.Get(7); !ok {
		t.Fatal("expected segment index cache entry to exist")
	}

	if _, ok := nodeCache.Get("node"); ok {
		t.Fatal("expected oldest node-cache entry to be evicted by shared budget")
	}
	if _, ok := storageCache.Get("storage"); !ok {
		t.Fatal("expected newer storage cache entry to remain resident")
	}
	if _, ok := segmentCache.Get(7); !ok {
		t.Fatal("expected newest segment index cache entry to remain resident")
	}

	usedTotal := uint64(0)
	for _, namespace := range []sharedCacheNamespace{
		sharedCacheNamespaceNode,
		sharedCacheNamespaceStorage,
		sharedCacheNamespaceSegmentIndex,
	} {
		used, capacity := shared.NamespaceStats(namespace)
		usedTotal += used
		if capacity != 1024 {
			t.Fatalf("unexpected shared capacity for namespace %d: got %d want %d", namespace, capacity, 1024)
		}
	}
	if usedTotal > 1024 {
		t.Fatalf("shared cache exceeds total budget: used=%d capacity=%d", usedTotal, 1024)
	}
}

func TestFileNodeCacheUsesSharedBudget(t *testing.T) {
	shared := newSharedByteCache(1024)
	pt := &PrefixTree{sharedCache: shared}

	pt.setFileNodeCache("filenode", bytes.Repeat([]byte{0x05}, 32), bytes.Repeat([]byte{0x06}, 220))
	entry, ok := pt.getFileNodeCache("filenode")
	if !ok {
		t.Fatal("expected file node cache entry to exist")
	}
	entry.Release()

	used, capacity := shared.NamespaceStats(sharedCacheNamespaceFileNode)
	if used == 0 {
		t.Fatal("expected file node cache to consume shared budget")
	}
	if capacity != 1024 {
		t.Fatalf("unexpected shared capacity for file node namespace: got %d want %d", capacity, 1024)
	}

	shared.Remove(sharedCacheNamespaceFileNode, "filenode")
	used, _ = shared.NamespaceStats(sharedCacheNamespaceFileNode)
	if used != 0 {
		t.Fatalf("expected file node cache budget to be released, got %d", used)
	}
}

func writeAccountRecordForTest(t *testing.T, file *os.File, key []byte, value []byte) int64 {
	t.Helper()
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	offset := info.Size()
	buf := make([]byte, 4+len(key)+len(value))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(key)))
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(value)))
	copy(buf[4:4+len(key)], key)
	copy(buf[4+len(key):], value)
	if _, err := file.WriteAt(buf, offset); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	return offset
}

func TestGlobalNodeKeysBypassNodeCache(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 16*1024, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	shortKey := []byte("A1234")
	shortValue := []byte("short-value")
	shortOffset := writeAccountRecordForTest(t, db.accountFile, shortKey, shortValue)
	if err := db.storeNode(shortKey, &TrieNode{offset: shortOffset}); err != nil {
		t.Fatalf("storeNode shortKey failed: %v", err)
	}
	value, found, err := db.Get(shortKey, nil)
	if err != nil {
		t.Fatalf("Get shortKey failed: %v", err)
	}
	if !found || !bytes.Equal(value, shortValue) {
		t.Fatalf("unexpected shortKey result: found=%t value=%q", found, value)
	}
	if _, ok := db.nodeCache.Get(string(shortKey)); ok {
		t.Fatal("expected global.node-backed key to bypass nodeCache")
	}

	longKey := []byte("A12345")
	longValue := []byte("long-value")
	longOffset := writeAccountRecordForTest(t, db.accountFile, longKey, longValue)
	if err := db.storeNode(longKey, &TrieNode{offset: longOffset}); err != nil {
		t.Fatalf("storeNode longKey failed: %v", err)
	}
	value, found, err = db.Get(longKey, nil)
	if err != nil {
		t.Fatalf("Get longKey failed: %v", err)
	}
	if !found || !bytes.Equal(value, longValue) {
		t.Fatalf("unexpected longKey result: found=%t value=%q", found, value)
	}
	if _, ok := db.nodeCache.Get(string(longKey)); !ok {
		t.Fatal("expected bucket-backed key to keep using nodeCache")
	}
}

func TestCloneSegmentChunkMetasCopiesBackingData(t *testing.T) {
	original := []segmentChunkMeta{{
		FileName: strings.Repeat("chunk", 8),
		KeyStart: []byte{0x01, 0x02, 0x03},
		KeyEnd:   []byte{0x04, 0x05, 0x06},
		KVCount:  7,
	}}

	cloned := cloneSegmentChunkMetas(original)
	if len(cloned) != 1 {
		t.Fatalf("unexpected clone length: %d", len(cloned))
	}

	original[0].KeyStart[0] = 0xff
	original[0].KeyEnd[0] = 0xee
	original[0].FileName = "mutated"

	if cloned[0].KeyStart[0] != 0x01 {
		t.Fatalf("expected cloned KeyStart to remain unchanged, got %x", cloned[0].KeyStart[0])
	}
	if cloned[0].KeyEnd[0] != 0x04 {
		t.Fatalf("expected cloned KeyEnd to remain unchanged, got %x", cloned[0].KeyEnd[0])
	}
	if cloned[0].FileName == "mutated" {
		t.Fatal("expected cloned FileName to remain independent from source")
	}
}

func encodeLegacySegmentChunkMetasForTest(t *testing.T, metas []segmentChunkMeta) []byte {
	t.Helper()
	buf := make([]byte, 0, 4+len(metas)*32)
	var tmp32 [4]byte
	var tmp64 [8]byte
	writeUint32BE(tmp32[:], uint32(len(metas)))
	buf = append(buf, tmp32[:]...)
	for _, meta := range metas {
		var err error
		if buf, err = appendVarBytes(buf, []byte(meta.FileName)); err != nil {
			t.Fatalf("append FileName failed: %v", err)
		}
		if buf, err = appendVarBytes(buf, meta.KeyStart); err != nil {
			t.Fatalf("append KeyStart failed: %v", err)
		}
		if buf, err = appendVarBytes(buf, meta.KeyEnd); err != nil {
			t.Fatalf("append KeyEnd failed: %v", err)
		}
		writeUint32BE(tmp32[:], meta.KVCount)
		buf = append(buf, tmp32[:]...)
		writeUint64BE(tmp64[:], meta.ChunkSize)
		buf = append(buf, tmp64[:]...)
	}
	return buf
}

func TestEncodeSegmentChunkMetasUsesCompactFormat(t *testing.T) {
	metas := []segmentChunkMeta{{
		FileName:  "chunk_0012.dat",
		KeyStart:  []byte{0x01, 0x02},
		KeyEnd:    []byte{0x03, 0x04},
		KVCount:   7,
		ChunkSize: 1024,
	}}

	buf, err := encodeSegmentChunkMetas(metas)
	if err != nil {
		t.Fatalf("encodeSegmentChunkMetas failed: %v", err)
	}
	if got := binary.BigEndian.Uint32(buf[:4]); got != segmentIndexFlatMagic {
		t.Fatalf("expected compact flat magic, got 0x%x", got)
	}
	if len(buf) != estimateSegmentIndexSize(metas) {
		t.Fatalf("encoded size mismatch: got %d want %d", len(buf), estimateSegmentIndexSize(metas))
	}

	var decoded []segmentChunkMeta
	var arena []byte
	if err := decodeSegmentIndexBuffer(buf, &decoded, &arena, false, ""); err != nil {
		t.Fatalf("decodeSegmentIndexBuffer failed: %v", err)
	}
	if !segmentChunkMetasEqual(decoded, metas) {
		t.Fatalf("decoded metas mismatch: got %+v want %+v", decoded, metas)
	}
	if len(buf) >= len(encodeLegacySegmentChunkMetasForTest(t, metas)) {
		t.Fatalf("expected compact encoding to be smaller than legacy encoding, got compact=%d legacy=%d", len(buf), len(encodeLegacySegmentChunkMetasForTest(t, metas)))
	}
}

func TestDecodeSegmentIndexBufferSupportsLegacyFormat(t *testing.T) {
	metas := []segmentChunkMeta{{
		FileName:  "chunk_0042.dat",
		KeyStart:  []byte{0x0a},
		KeyEnd:    []byte{0x0f},
		KVCount:   3,
		ChunkSize: 4096,
	}}

	buf := encodeLegacySegmentChunkMetasForTest(t, metas)
	var decoded []segmentChunkMeta
	var arena []byte
	if err := decodeLegacySegmentIndexBuffer(buf, &decoded, &arena, false, ""); err != nil {
		t.Fatalf("decode legacy segment index failed: %v", err)
	}
	if !segmentChunkMetasEqual(decoded, metas) {
		t.Fatalf("decoded metas mismatch: got %+v want %+v", decoded, metas)
	}
}

func TestDecodeSegmentIndexBufferRejectsLegacyFormat(t *testing.T) {
	buf := encodeLegacySegmentChunkMetasForTest(t, []segmentChunkMeta{{
		FileName:  "chunk_0042.dat",
		KeyStart:  []byte{0x0a},
		KeyEnd:    []byte{0x0f},
		KVCount:   3,
		ChunkSize: 4096,
	}})
	var decoded []segmentChunkMeta
	var arena []byte
	if err := decodeSegmentIndexBuffer(buf, &decoded, &arena, false, ""); err == nil {
		t.Fatal("expected normal decode path to reject legacy segment index format")
	}
}

func TestEncodeSegmentChunkMetasRejectsUnsafeCompactEncoding(t *testing.T) {
	t.Run("non ordinal file name", func(t *testing.T) {
		metas := []segmentChunkMeta{{
			FileName:  "legacy-name.dat",
			KeyStart:  []byte{0x01},
			KeyEnd:    []byte{0x02},
			KVCount:   1,
			ChunkSize: 128,
		}}

		buf, err := encodeSegmentChunkMetas(metas)
		if err == nil {
			t.Fatalf("expected compact encoding rejection, got buffer len %d", len(buf))
		}
	})

	t.Run("chunk size overflow", func(t *testing.T) {
		metas := []segmentChunkMeta{{
			FileName:  "chunk_0007.dat",
			KeyStart:  []byte{0x03},
			KeyEnd:    []byte{0x04},
			KVCount:   2,
			ChunkSize: uint64(math.MaxUint32) + 1,
		}}

		buf, err := encodeSegmentChunkMetas(metas)
		if err == nil {
			t.Fatalf("expected compact encoding rejection, got buffer len %d", len(buf))
		}
	})
}

func TestMigrateLegacySegmentIndexFormatsMigratesFlatIndex(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 16*1024, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	folderPath := db.segmentedFolderPath(1)
	if err := os.MkdirAll(folderPath, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	metas := []segmentChunkMeta{
		{FileName: "chunk_0001.dat", KeyStart: []byte{0x01}, KeyEnd: []byte{0x02}, KVCount: 1, ChunkSize: 128},
		{FileName: "chunk_0002.dat", KeyStart: []byte{0x03}, KeyEnd: []byte{0x04}, KVCount: 2, ChunkSize: 256},
	}
	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	if err := os.WriteFile(indexPath, encodeLegacySegmentChunkMetasForTest(t, metas), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := db.MigrateLegacySegmentIndexFormats(); err != nil {
		t.Fatalf("MigrateLegacySegmentIndexFormats failed: %v", err)
	}
	buf, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if got := binary.BigEndian.Uint32(buf[:4]); got != segmentIndexFlatMagic {
		t.Fatalf("expected migrated flat magic, got 0x%x", got)
	}
	decoded, err := db.readSegmentIndexNoCache(1)
	if err != nil {
		t.Fatalf("readSegmentIndexNoCache failed after migration: %v", err)
	}
	if !segmentChunkMetasEqual(decoded, metas) {
		t.Fatalf("decoded metas mismatch after migration: got %+v want %+v", decoded, metas)
	}
}

func TestMigrateLegacySegmentIndexFormatsMigratesLegacyLevel2Files(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 16*1024, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	folderPath := db.segmentedFolderPath(2)
	if err := os.MkdirAll(folderPath, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	group1 := []segmentChunkMeta{{
		FileName:  "chunk_0001.dat",
		KeyStart:  bytes.Repeat([]byte{0x01}, 5000),
		KeyEnd:    bytes.Repeat([]byte{0x02}, 5000),
		KVCount:   1,
		ChunkSize: 128,
	}}
	group2 := []segmentChunkMeta{{
		FileName:  "chunk_0002.dat",
		KeyStart:  bytes.Repeat([]byte{0x03}, 5000),
		KeyEnd:    bytes.Repeat([]byte{0x04}, 5000),
		KVCount:   2,
		ChunkSize: 256,
	}}
	layout := segmentIndexLayout{
		mode:       indexLayoutMultiLevel,
		nextMetaID: 3,
		entries: []segmentIndexL1Entry{
			{MetaID: 1, KeyStart: cloneBytes(group1[0].KeyStart), KeyEnd: cloneBytes(group1[0].KeyEnd), ChunkCount: 1},
			{MetaID: 2, KeyStart: cloneBytes(group2[0].KeyStart), KeyEnd: cloneBytes(group2[0].KeyEnd), ChunkCount: 1},
		},
	}
	topBuf, err := encodeTopLevelIndex(layout)
	if err != nil {
		t.Fatalf("encodeTopLevelIndex failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(folderPath, segmentIndexFileName), topBuf, 0644); err != nil {
		t.Fatalf("WriteFile top-level failed: %v", err)
	}
	if err := os.WriteFile(level2IndexFilePath(folderPath, 1), encodeLegacySegmentChunkMetasForTest(t, group1), 0644); err != nil {
		t.Fatalf("WriteFile level2 #1 failed: %v", err)
	}
	buf2, err := encodeSegmentChunkMetas(group2)
	if err != nil {
		t.Fatalf("encodeSegmentChunkMetas for group2 failed: %v", err)
	}
	if err := os.WriteFile(level2IndexFilePath(folderPath, 2), buf2, 0644); err != nil {
		t.Fatalf("WriteFile level2 #2 failed: %v", err)
	}

	if err := db.MigrateLegacySegmentIndexFormats(); err != nil {
		t.Fatalf("MigrateLegacySegmentIndexFormats failed: %v", err)
	}
	buf, err := os.ReadFile(level2IndexFilePath(folderPath, 1))
	if err != nil {
		t.Fatalf("ReadFile migrated level2 failed: %v", err)
	}
	if got := binary.BigEndian.Uint32(buf[:4]); got != segmentIndexFlatMagic {
		t.Fatalf("expected migrated level2 flat magic, got 0x%x", got)
	}
	decoded, err := db.readSegmentIndexNoCache(2)
	if err != nil {
		t.Fatalf("readSegmentIndexNoCache failed after level2 migration: %v", err)
	}
	if !segmentChunkMetasEqual(decoded, append(group1, group2...)) {
		t.Fatalf("decoded metas mismatch after level2 migration: got %+v", decoded)
	}
}

func TestUpgradeSegmentIndexFilesRebuildsUsingCurrentLayoutConstants(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 16*1024, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	folderPath := db.segmentedFolderPath(3)
	if err := os.MkdirAll(folderPath, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	group1 := []segmentChunkMeta{{FileName: "chunk_0001.dat", KeyStart: []byte{0x01}, KeyEnd: []byte{0x02}, KVCount: 1, ChunkSize: 128}}
	group2 := []segmentChunkMeta{{FileName: "chunk_0002.dat", KeyStart: []byte{0x03}, KeyEnd: []byte{0x04}, KVCount: 2, ChunkSize: 256}}
	layout := segmentIndexLayout{
		mode:       indexLayoutMultiLevel,
		nextMetaID: 3,
		entries: []segmentIndexL1Entry{
			{MetaID: 1, KeyStart: cloneBytes(group1[0].KeyStart), KeyEnd: cloneBytes(group1[0].KeyEnd), ChunkCount: 1},
			{MetaID: 2, KeyStart: cloneBytes(group2[0].KeyStart), KeyEnd: cloneBytes(group2[0].KeyEnd), ChunkCount: 1},
		},
	}
	topBuf, err := encodeTopLevelIndex(layout)
	if err != nil {
		t.Fatalf("encodeTopLevelIndex failed: %v", err)
	}
	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	if err := os.WriteFile(indexPath, topBuf, 0644); err != nil {
		t.Fatalf("WriteFile top-level failed: %v", err)
	}
	l2Buf1, err := encodeSegmentChunkMetas(group1)
	if err != nil {
		t.Fatalf("encodeSegmentChunkMetas group1 failed: %v", err)
	}
	if err := os.WriteFile(level2IndexFilePath(folderPath, 1), l2Buf1, 0644); err != nil {
		t.Fatalf("WriteFile level2 #1 failed: %v", err)
	}
	l2Buf2, err := encodeSegmentChunkMetas(group2)
	if err != nil {
		t.Fatalf("encodeSegmentChunkMetas group2 failed: %v", err)
	}
	if err := os.WriteFile(level2IndexFilePath(folderPath, 2), l2Buf2, 0644); err != nil {
		t.Fatalf("WriteFile level2 #2 failed: %v", err)
	}
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("Stat before upgrade failed: %v", err)
	}
	l2Info1, err := os.Stat(level2IndexFilePath(folderPath, 1))
	if err != nil {
		t.Fatalf("Stat level2 #1 before upgrade failed: %v", err)
	}
	l2Info2, err := os.Stat(level2IndexFilePath(folderPath, 2))
	if err != nil {
		t.Fatalf("Stat level2 #2 before upgrade failed: %v", err)
	}
	beforeTotalSize := beforeInfo.Size() + l2Info1.Size() + l2Info2.Size()

	if err := db.UpgradeSegmentIndexFiles(); err != nil {
		t.Fatalf("UpgradeSegmentIndexFiles failed: %v", err)
	}
	afterBuf, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("ReadFile upgraded index failed: %v", err)
	}
	if got := binary.BigEndian.Uint32(afterBuf[:4]); got != segmentIndexFlatMagic {
		t.Fatalf("expected rebuilt flat index magic, got 0x%x", got)
	}
	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("Stat after upgrade failed: %v", err)
	}
	afterTotalSize := afterInfo.Size()
	if afterTotalSize >= beforeTotalSize {
		t.Fatalf("expected rebuilt index footprint to shrink, before=%d after=%d", beforeTotalSize, afterTotalSize)
	}
	if _, err := os.Stat(level2IndexFilePath(folderPath, 1)); !os.IsNotExist(err) {
		t.Fatalf("expected level2 file #1 to be removed, err=%v", err)
	}
	if _, err := os.Stat(level2IndexFilePath(folderPath, 2)); !os.IsNotExist(err) {
		t.Fatalf("expected level2 file #2 to be removed, err=%v", err)
	}
	decoded, err := db.readSegmentIndexNoCache(3)
	if err != nil {
		t.Fatalf("readSegmentIndexNoCache failed after upgrade: %v", err)
	}
	if !segmentChunkMetasEqual(decoded, append(group1, group2...)) {
		t.Fatalf("decoded metas mismatch after rebuild: got %+v", decoded)
	}
}

func TestRefreshSegmentIndexCacheUpdatesEntries(t *testing.T) {
	shared := newSharedByteCache(4096)
	db := &PrefixDB{
		storageDir:           t.TempDir(),
		storageIndexCache:    newSharedSegmentIndexCache(shared),
		storageIndexFolderId: 7,
		storageIndexMetas:    []segmentChunkMeta{{FileName: "old.dat"}},
	}
	metas := []segmentChunkMeta{{
		FileName:  "chunk_0001.dat",
		KeyStart:  []byte{0x01},
		KeyEnd:    []byte{0x02},
		KVCount:   1,
		ChunkSize: 128,
	}}

	db.refreshSegmentIndexCache(7, metas)

	if got, ok := db.storageIndexCache.Get(7); !ok || len(got) != 1 || got[0].FileName != "chunk_0001.dat" {
		t.Fatalf("segment index cache not refreshed correctly: ok=%t metas=%v", ok, got)
	}
	if len(db.storageIndexMetas) != 1 || db.storageIndexMetas[0].FileName != "chunk_0001.dat" {
		t.Fatalf("in-memory segment index snapshot not refreshed: %+v", db.storageIndexMetas)
	}
}

func TestCompactFileFromStateRefreshesFileNodeCache(t *testing.T) {
	shared := newSharedByteCache(4096)
	pt := &PrefixTree{
		sharedCache: shared,
		fileNodeDir: t.TempDir(),
	}
	state := &gcState{
		header: FileNodeHeader{
			Magic:              FileNodeMagic,
			Version:            2,
			SortedEntryCount:   1,
			UnsortedEntryCount: 1,
		},
		sorted:   append([]byte(nil), encodeNodeEntry(NodeInfo{key: []byte{0x01}, accountOffset: 1, storageFileID: 1, storageOffset: 11, storageSize: 22})...),
		unsorted: append([]byte(nil), encodeNodeEntry(NodeInfo{key: []byte{0x01}, accountOffset: 2, storageFileID: 3, storageOffset: 33, storageSize: 44})...),
	}

	if err := pt.compactFileFromState("bucket.node", state); err != nil {
		t.Fatalf("compactFileFromState failed: %v", err)
	}
	entry, ok := pt.getFileNodeCache("bucket.node")
	if !ok {
		t.Fatal("expected compacted file to refresh file node cache")
	}
	entry.Release()
	used, _ := shared.NamespaceStats(sharedCacheNamespaceFileNode)
	if used == 0 {
		t.Fatal("expected refreshed file node cache to consume shared budget")
	}
}

func TestNewPrefixTreeLoadsGlobalNodeIntoSkipList(t *testing.T) {
	baseDir := t.TempDir()
	fileNodeDir := filepath.Join(baseDir, "prefixdb", "filenodes")
	if err := os.MkdirAll(fileNodeDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	filePath := filepath.Join(fileNodeDir, globalFileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	header := FileNodeHeader{Magic: FileNodeMagic, Version: 2, SortedEntryCount: 1, UnsortedEntryCount: 1}
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		_ = file.Close()
		t.Fatalf("write header failed: %v", err)
	}
	key := []byte{0x01, 0x02, 0x03}
	if _, err := file.Write(encodeNodeEntry(NodeInfo{key: key, accountOffset: 10, storageFileID: 1, storageOffset: 11, storageSize: 12})); err != nil {
		_ = file.Close()
		t.Fatalf("write sorted entry failed: %v", err)
	}
	if _, err := file.Write(encodeNodeEntry(NodeInfo{key: key, accountOffset: 20, storageFileID: 2, storageOffset: 21, storageSize: 22})); err != nil {
		_ = file.Close()
		t.Fatalf("write unsorted entry failed: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file failed: %v", err)
	}

	pt, err := NewPrefixTree(&PrefixDB{}, baseDir)
	if err != nil {
		t.Fatalf("NewPrefixTree failed: %v", err)
	}
	defer pt.Close()

	if pt.globalNodeIndex == nil || pt.globalNodeIndex.Len() != 1 {
		t.Fatalf("unexpected global skiplist state: %+v", pt.globalNodeIndex)
	}
	node, found, err := pt.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("expected global node entry to be loaded")
	}
	if node.accountOffset != 20 || node.storageFileID != 2 || node.storageOffset != 21 || node.storageSize != 22 {
		t.Fatalf("unexpected loaded node info: %+v", node)
	}
	if _, ok := pt.getFileNodeCache(globalFileName); ok {
		t.Fatal("global.node should not use the shared file node cache")
	}
}

func TestGlobalNodePutAppendsAndUpdatesSkipList(t *testing.T) {
	baseDir := t.TempDir()
	pt, err := NewPrefixTree(&PrefixDB{}, baseDir)
	if err != nil {
		t.Fatalf("NewPrefixTree failed: %v", err)
	}
	defer pt.Close()

	key := []byte{0x0a, 0x0b, 0x0c}
	headerSize := int64(binary.Size(FileNodeHeader{}))
	if err := pt.Put(key, 11, 1, 101, 1001); err != nil {
		t.Fatalf("first Put failed: %v", err)
	}
	if pt.globalHeader.UnsortedEntryCount != 1 {
		t.Fatalf("unexpected unsorted count after first append: %d", pt.globalHeader.UnsortedEntryCount)
	}
	if err := pt.Put(key, 22, 2, 202, 2002); err != nil {
		t.Fatalf("second Put failed: %v", err)
	}
	if pt.globalHeader.UnsortedEntryCount != 2 {
		t.Fatalf("unexpected unsorted count after second append: %d", pt.globalHeader.UnsortedEntryCount)
	}
	if pt.globalNodeIndex.Len() != 1 {
		t.Fatalf("expected one deduplicated key in skiplist, got %d", pt.globalNodeIndex.Len())
	}
	node, found, err := pt.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("expected key in global skiplist")
	}
	if node.accountOffset != 22 || node.storageFileID != 2 || node.storageOffset != 202 || node.storageSize != 2002 {
		t.Fatalf("unexpected node after append updates: %+v", node)
	}
	stat, err := os.Stat(filepath.Join(pt.fileNodeDir, globalFileName))
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if stat.Size() != headerSize+2*NodeEntrySize {
		t.Fatalf("unexpected global.node size: got %d want %d", stat.Size(), headerSize+2*NodeEntrySize)
	}
	if _, ok := pt.getFileNodeCache(globalFileName); ok {
		t.Fatal("global.node should bypass the shared file node cache")
	}
}

func TestGlobalNodeDeleteRewritesDedicatedFile(t *testing.T) {
	baseDir := t.TempDir()
	pt, err := NewPrefixTree(&PrefixDB{}, baseDir)
	if err != nil {
		t.Fatalf("NewPrefixTree failed: %v", err)
	}
	defer pt.Close()

	key1 := []byte{0x01}
	key2 := []byte{0x02}
	if err := pt.Put(key1, 11, 1, 101, 1001); err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}
	if err := pt.Put(key2, 22, 2, 202, 2002); err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}
	deleted, err := pt.Delete(key1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !deleted {
		t.Fatal("expected global node delete to remove the key")
	}
	if pt.globalHeader.SortedEntryCount != 1 || pt.globalHeader.UnsortedEntryCount != 0 {
		t.Fatalf("unexpected global header after rewrite: %+v", pt.globalHeader)
	}
	if pt.globalNodeIndex.Len() != 1 {
		t.Fatalf("unexpected skiplist size after delete: %d", pt.globalNodeIndex.Len())
	}
	if _, found, err := pt.Get(key1); err != nil || found {
		t.Fatalf("expected deleted key to disappear, found=%t err=%v", found, err)
	}
	node, found, err := pt.Get(key2)
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if !found || node.accountOffset != 22 {
		t.Fatalf("expected surviving key to remain readable, found=%t node=%+v", found, node)
	}
}

func TestGlobalNodeCommitFlushesOnceAtEnd(t *testing.T) {
	baseDir := t.TempDir()
	pt, err := NewPrefixTree(&PrefixDB{}, baseDir)
	if err != nil {
		t.Fatalf("NewPrefixTree failed: %v", err)
	}
	defer pt.Close()

	headerSize := int64(binary.Size(FileNodeHeader{}))
	key1 := []byte{0x01}
	key2 := []byte{0x02}

	pt.beginGlobalCommit()
	if err := pt.Put(key1, 11, 1, 101, 1001); err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}
	if err := pt.Put(key2, 22, 2, 202, 2002); err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}
	stat, err := os.Stat(filepath.Join(pt.fileNodeDir, globalFileName))
	if err != nil {
		t.Fatalf("Stat during commit failed: %v", err)
	}
	if stat.Size() != headerSize {
		t.Fatalf("expected no global.node payload before commit flush, got %d", stat.Size())
	}
	if pt.globalHeader.SortedEntryCount != 0 || pt.globalHeader.UnsortedEntryCount != 0 {
		t.Fatalf("unexpected header before deferred flush: %+v", pt.globalHeader)
	}
	if err := pt.endGlobalCommit(); err != nil {
		t.Fatalf("endGlobalCommit failed: %v", err)
	}
	stat, err = os.Stat(filepath.Join(pt.fileNodeDir, globalFileName))
	if err != nil {
		t.Fatalf("Stat after commit failed: %v", err)
	}
	if stat.Size() != headerSize+2*NodeEntrySize {
		t.Fatalf("unexpected global.node size after single flush: got %d want %d", stat.Size(), headerSize+2*NodeEntrySize)
	}
	if pt.globalHeader.SortedEntryCount != 0 || pt.globalHeader.UnsortedEntryCount != 2 {
		t.Fatalf("unexpected header after deferred append flush: %+v", pt.globalHeader)
	}
	if node, found, err := pt.Get(key1); err != nil || !found || node.accountOffset != 11 {
		t.Fatalf("expected key1 after deferred flush, found=%t node=%+v err=%v", found, node, err)
	}
	if node, found, err := pt.Get(key2); err != nil || !found || node.accountOffset != 22 {
		t.Fatalf("expected key2 after deferred flush, found=%t node=%+v err=%v", found, node, err)
	}
}

func TestGlobalNodeNestedCommitFlushesOnOuterEnd(t *testing.T) {
	baseDir := t.TempDir()
	pt, err := NewPrefixTree(&PrefixDB{}, baseDir)
	if err != nil {
		t.Fatalf("NewPrefixTree failed: %v", err)
	}
	defer pt.Close()

	headerSize := int64(binary.Size(FileNodeHeader{}))
	key := []byte{0x03}

	pt.beginGlobalCommit()
	pt.beginGlobalCommit()
	if err := pt.Put(key, 33, 3, 303, 3003); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := pt.endGlobalCommit(); err != nil {
		t.Fatalf("inner endGlobalCommit failed: %v", err)
	}
	stat, err := os.Stat(filepath.Join(pt.fileNodeDir, globalFileName))
	if err != nil {
		t.Fatalf("Stat after inner commit failed: %v", err)
	}
	if stat.Size() != headerSize {
		t.Fatalf("expected nested commit to defer flush until outer end, got %d", stat.Size())
	}
	if err := pt.endGlobalCommit(); err != nil {
		t.Fatalf("outer endGlobalCommit failed: %v", err)
	}
	stat, err = os.Stat(filepath.Join(pt.fileNodeDir, globalFileName))
	if err != nil {
		t.Fatalf("Stat after outer commit failed: %v", err)
	}
	if stat.Size() != headerSize+NodeEntrySize {
		t.Fatalf("unexpected global.node size after outer flush: got %d want %d", stat.Size(), headerSize+NodeEntrySize)
	}
	if pt.globalHeader.SortedEntryCount != 0 || pt.globalHeader.UnsortedEntryCount != 1 {
		t.Fatalf("unexpected header after nested append flush: %+v", pt.globalHeader)
	}
}

func TestGlobalNodeCloseCompactsDeferredUpdates(t *testing.T) {
	baseDir := t.TempDir()
	pt, err := NewPrefixTree(&PrefixDB{}, baseDir)
	if err != nil {
		t.Fatalf("NewPrefixTree failed: %v", err)
	}

	headerSize := int64(binary.Size(FileNodeHeader{}))
	key1 := []byte{0x04}
	key2 := []byte{0x05}
	pt.beginGlobalCommit()
	if err := pt.Put(key1, 44, 4, 404, 4004); err != nil {
		t.Fatalf("Put key1 failed: %v", err)
	}
	if err := pt.Put(key2, 55, 5, 505, 5005); err != nil {
		t.Fatalf("Put key2 failed: %v", err)
	}
	if err := pt.endGlobalCommit(); err != nil {
		t.Fatalf("endGlobalCommit failed: %v", err)
	}
	if pt.globalHeader.SortedEntryCount != 0 || pt.globalHeader.UnsortedEntryCount != 2 {
		t.Fatalf("expected deferred append state before close, got %+v", pt.globalHeader)
	}
	if err := pt.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	stat, err := os.Stat(filepath.Join(baseDir, "prefixdb", "filenodes", globalFileName))
	if err != nil {
		t.Fatalf("Stat after close failed: %v", err)
	}
	if stat.Size() != headerSize+2*NodeEntrySize {
		t.Fatalf("unexpected global.node size after close compact: got %d want %d", stat.Size(), headerSize+2*NodeEntrySize)
	}
	pt2, err := NewPrefixTree(&PrefixDB{}, baseDir)
	if err != nil {
		t.Fatalf("reopen NewPrefixTree failed: %v", err)
	}
	defer pt2.Close()
	if pt2.globalHeader.SortedEntryCount != 2 || pt2.globalHeader.UnsortedEntryCount != 0 {
		t.Fatalf("expected compacted header after reopen, got %+v", pt2.globalHeader)
	}
}

func TestPrefixTreeShouldScheduleGCUsesRatioThreshold(t *testing.T) {
	pt := &PrefixTree{gcRatioThreshold: 1.5}
	if pt.shouldScheduleGC(10, 14) {
		t.Fatal("expected GC to stay idle below configured ratio")
	}
	if !pt.shouldScheduleGC(10, 15) {
		t.Fatal("expected GC when unsorted/sorted reaches configured ratio")
	}
	if !pt.shouldScheduleGC(0, 1) {
		t.Fatal("expected GC when only unsorted entries exist")
	}
	if pt.shouldScheduleGC(10, 0) {
		t.Fatal("did not expect GC without unsorted entries")
	}
}

func TestPrefixTreeGCWorkerConcurrency(t *testing.T) {
	pt := &PrefixTree{gcWorkerCount: 3}
	if got := pt.gcWorkerConcurrency(); got != 3 {
		t.Fatalf("worker count mismatch: got %d want %d", got, 3)
	}
	pt.gcWorkerCount = 0
	expected := runtime.NumCPU() / 2
	if expected < 1 {
		expected = 1
	}
	if expected > maxPrefixTreeGCWorkers {
		expected = maxPrefixTreeGCWorkers
	}
	if got := pt.gcWorkerConcurrency(); got != expected {
		t.Fatalf("unexpected automatic worker count: got %d want %d", got, expected)
	}
}

func TestStorageGCQueueCapacity(t *testing.T) {
	if got := storageGCQueueCapacity(4); got != 32 {
		t.Fatalf("queue capacity mismatch: got %d want %d", got, 32)
	}

	autoWorkers := sanitizePrefixTreeGCWorkerCount(0)
	if got := storageGCQueueCapacity(0); got != autoWorkers*storageGCQueueMultiplier {
		t.Fatalf("unexpected automatic queue capacity: got %d want %d", got, autoWorkers*storageGCQueueMultiplier)
	}
}

func TestComputeSegmentedChunkHardLimit(t *testing.T) {
	if got := computeSegmentedChunkHardLimit(1024, 1.5); got != 1536 {
		t.Fatalf("hard limit mismatch: got %d want %d", got, 1536)
	}
	if got := computeSegmentedChunkHardLimit(1024, 0); got != 2048 {
		t.Fatalf("default hard limit mismatch: got %d want %d", got, 2048)
	}
}

func TestPrefixTreeGetDuringGCInFlightUsesSnapshot(t *testing.T) {
	baseDir := t.TempDir()
	db := &PrefixDB{}
	pt, err := NewPrefixTree(db, baseDir)
	if err != nil {
		t.Fatalf("NewPrefixTree failed: %v", err)
	}
	defer pt.Close()

	key := bytes.Repeat([]byte{0x01}, 32)
	fileID := pt.fileIDForKey(key)
	if err := pt.putIntoFileNode(fileID, key, 123, 7, 11, 13); err != nil {
		t.Fatalf("putIntoFileNode failed: %v", err)
	}

	state, err := pt.buildGCStateFromFile(fileID)
	if err != nil {
		t.Fatalf("buildGCStateFromFile failed: %v", err)
	}
	if state == nil {
		t.Fatal("expected non-nil GC state")
	}

	pt.gcMu.Lock()
	pt.gcInFlight[fileID] = state
	pt.gcMu.Unlock()
	defer pt.finishGC(fileID)

	node, found, err := pt.getFromFileNode(fileID, key)
	if err != nil {
		t.Fatalf("getFromFileNode during GC returned error: %v", err)
	}
	if !found {
		t.Fatal("expected key to remain readable during GC")
	}
	if node.accountOffset != 123 || node.storageFileID != 7 || node.storageOffset != 11 || node.storageSize != 13 {
		t.Fatalf("unexpected node info during GC: %+v", node)
	}

	filePath := filepath.Join(pt.fileNodeDir, fileID)
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("failed to remove file node: %v", err)
	}
	node, found, err = pt.getFromFileNode(fileID, key)
	if err != nil {
		t.Fatalf("getFromFileNode after file removal during GC returned error: %v", err)
	}
	if !found || node.accountOffset != 123 {
		t.Fatalf("expected GC snapshot to keep data readable after file removal, found=%t node=%+v", found, node)
	}
}
