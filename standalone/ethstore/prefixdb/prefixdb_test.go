package prefixdb

import (
	"bytes"
	"fmt"
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
