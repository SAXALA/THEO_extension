package prefixdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	datatypepkg "github.com/tinoryj/EthStore/standalone/ethstore/datatype"
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

func TestSharedCacheHybridPolicyKeepsFrequentOldEntry(t *testing.T) {
	shared := newSharedByteCache(3)
	shared.Add(sharedCacheNamespaceStorage, "a", []byte{0x01}, 1)
	shared.Add(sharedCacheNamespaceStorage, "b", []byte{0x02}, 1)
	shared.Add(sharedCacheNamespaceStorage, "c", []byte{0x03}, 1)

	for i := 0; i < 5; i++ {
		if _, ok := shared.Get(sharedCacheNamespaceStorage, "a"); !ok {
			t.Fatal("expected hot entry a to remain present during warmup")
		}
	}
	if _, ok := shared.Get(sharedCacheNamespaceStorage, "c"); !ok {
		t.Fatal("expected c to be present")
	}
	if _, ok := shared.Get(sharedCacheNamespaceStorage, "b"); !ok {
		t.Fatal("expected b to be present")
	}

	shared.Add(sharedCacheNamespaceStorage, "d", []byte{0x04}, 1)

	if _, ok := shared.Get(sharedCacheNamespaceStorage, "a"); !ok {
		t.Fatal("expected frequent entry a to survive hybrid eviction")
	}
	if _, ok := shared.Get(sharedCacheNamespaceStorage, "d"); !ok {
		t.Fatal("expected newest entry d to be cached")
	}
	if _, ok := shared.Get(sharedCacheNamespaceStorage, "c"); ok {
		t.Fatal("expected lower-frequency older entry c to be evicted")
	}
	if _, ok := shared.Get(sharedCacheNamespaceStorage, "b"); !ok {
		t.Fatal("expected b to remain cached")
	}
	if shared.usedBytes > shared.capacityBytes {
		t.Fatalf("shared cache exceeds total budget: used=%d capacity=%d", shared.usedBytes, shared.capacityBytes)
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

func TestReadSegmentIndexForKeyUsesPartialLevel2Cache(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 16*1024, 16, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	folderID := uint32(123)
	folderPath := db.segmentedFolderPath(folderID)
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	metas := make([]segmentChunkMeta, 1000)
	for i := range metas {
		start := []byte(fmt.Sprintf("k%08d", i*2))
		end := []byte(fmt.Sprintf("k%08d", i*2+1))
		metas[i] = segmentChunkMeta{
			FileName:  fmt.Sprintf("chunk_%04d.dat", i),
			KeyStart:  start,
			KeyEnd:    end,
			KVCount:   1,
			ChunkSize: 1,
		}
	}
	if err := db.writeSegmentIndex(folderPath, metas); err != nil {
		t.Fatalf("writeSegmentIndex failed: %v", err)
	}
	db.invalidateSegmentIndexCache(folderID)

	targetKey := metas[333].KeyStart
	before := atomic.LoadUint64(&db.diskIOStats[diskIOUsageStorageSegmentIndex].readOps)
	first, err := db.readSegmentIndexForKey(folderID, targetKey)
	if err != nil {
		t.Fatalf("first readSegmentIndexForKey failed: %v", err)
	}
	if len(first) == 0 {
		t.Fatal("expected non-empty metas from first read")
	}
	afterFirst := atomic.LoadUint64(&db.diskIOStats[diskIOUsageStorageSegmentIndex].readOps)
	second, err := db.readSegmentIndexForKey(folderID, targetKey)
	if err != nil {
		t.Fatalf("second readSegmentIndexForKey failed: %v", err)
	}
	if len(second) != len(first) {
		t.Fatalf("cached metas size mismatch: got %d want %d", len(second), len(first))
	}
	afterSecond := atomic.LoadUint64(&db.diskIOStats[diskIOUsageStorageSegmentIndex].readOps)

	firstDelta := afterFirst - before
	secondDelta := afterSecond - afterFirst
	if firstDelta < 1 {
		t.Fatalf("expected first read to hit index storage, got delta=%d", firstDelta)
	}
	if secondDelta >= firstDelta {
		t.Fatalf("expected second read to use partial L2 cache; firstDelta=%d secondDelta=%d", firstDelta, secondDelta)
	}
}

func writeAccountRecordForTest(t *testing.T, file *os.File, key []byte, value []byte) int64 {
	t.Helper()
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	offset := info.Size()
	if offset == 0 {
		offset = 1
	}
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

func makeTestAccountKey(seed byte) []byte {
	key := make([]byte, 33)
	key[0] = 'A'
	for i := 1; i < len(key); i++ {
		key[i] = seed + byte(i)
	}
	return key
}

func makeTestStorageRawKey(accountKey []byte, suffix ...byte) []byte {
	raw := make([]byte, 1+32+len(suffix))
	raw[0] = 'O'
	copy(raw[1:33], accountKey[1:33])
	copy(raw[33:], suffix)
	return raw
}

func makeTestStorageRawKeyWithSuffix(suffix ...byte) []byte {
	raw := make([]byte, 1+32+len(suffix))
	raw[0] = 'O'
	copy(raw[33:], suffix)
	return raw
}

func TestCommitStorageForAccountUsesAccountNamedSegmentedFolder(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x10)
	kvs := []kvPair{
		{key: []byte{0x01}, val: bytes.Repeat([]byte("a"), 40)},
		{key: []byte{0x02}, val: bytes.Repeat([]byte("b"), 40)},
		{key: []byte{0x03}, val: bytes.Repeat([]byte("c"), 40)},
	}
	if err := db.commitStorageForAccount(string(accountKey), kvs); err != nil {
		t.Fatalf("commitStorageForAccount failed: %v", err)
	}

	node, err := db.getNode(accountKey)
	if err != nil {
		t.Fatalf("getNode failed: %v", err)
	}
	if node == nil {
		t.Fatal("expected account node to exist")
	}
	if node.storageFileID != segmentedStorageFlag || node.storageOffset != 0 || node.storageSize != 0 {
		t.Fatalf("expected account-named segmented sentinel pointer, got fileID=%d offset=%d size=%d", node.storageFileID, node.storageOffset, node.storageSize)
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	if _, err := os.Stat(filepath.Join(folderPath, segmentIndexFileName)); err != nil {
		t.Fatalf("expected account-named segment index to exist: %v", err)
	}
	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, 0x02), accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if !found || !bytes.Equal(value, bytes.Repeat([]byte("b"), 40)) {
		t.Fatalf("unexpected storage lookup result: found=%t value=%q", found, value)
	}
}

func TestShortAccountKeyFolderManagedStorageSurvivesReopen(t *testing.T) {
	baseDir := t.TempDir()
	accountKey := []byte{0x41, 0x03, 0x07, 0x0d, 0x06, 0x05, 0x0e, 0x0a}
	storageSuffix := []byte{0x07, 0x0c, 0x01}

	db, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	kvs := []kvPair{
		{key: append([]byte(nil), storageSuffix...), val: bytes.Repeat([]byte("a"), 40)},
		{key: []byte{0x07, 0x0c, 0x02}, val: bytes.Repeat([]byte("b"), 40)},
		{key: []byte{0x07, 0x0c, 0x03}, val: bytes.Repeat([]byte("c"), 40)},
	}
	if err := db.commitStorageForAccount(string(accountKey), kvs); err != nil {
		_ = db.Close()
		t.Fatalf("commitStorageForAccount failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("reopen NewPrefixDB failed: %v", err)
	}
	defer reopened.Close()

	if !reopened.isAccountStorageFolderManaged(accountKey) {
		t.Fatal("expected short account key folder marker to be restored after reopen")
	}
	value, found, err := reopened.Get(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKeyWithSuffix(storageSuffix...), accountKey)
	if err != nil {
		t.Fatalf("Get storage after reopen failed: %v", err)
	}
	if !found || !bytes.Equal(value, bytes.Repeat([]byte("a"), 40)) {
		t.Fatalf("unexpected storage lookup after reopen: found=%t value=%q", found, value)
	}
}

func TestSmallStorageStillUsesAccountEntryPointer(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x11)
	kvs := []kvPair{{key: []byte{0x01}, val: []byte("small-value")}}
	if err := db.commitStorageForAccount(string(accountKey), kvs); err != nil {
		t.Fatalf("commitStorageForAccount failed: %v", err)
	}

	node, err := db.getNode(accountKey)
	if err != nil {
		t.Fatalf("getNode failed: %v", err)
	}
	if node == nil {
		t.Fatal("expected account node to exist")
	}
	if node.storageFileID == 0 || isSegmentedStorage(node.storageFileID) {
		t.Fatalf("expected inline storage pointer, got fileID=%d", node.storageFileID)
	}
	if db.isAccountStorageFolderManaged(accountKey) {
		t.Fatal("small storage should not be marked as folder-managed")
	}

	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, 0x01), accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if !found || !bytes.Equal(value, []byte("small-value")) {
		t.Fatalf("unexpected storage result: found=%t value=%q", found, value)
	}
}

func TestFolderManagedPutSkipsAccountEntryPointerRewrite(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x12)
	first := []kvPair{
		{key: []byte{0x01}, val: bytes.Repeat([]byte("a"), 40)},
		{key: []byte{0x02}, val: bytes.Repeat([]byte("b"), 40)},
	}
	if err := db.commitStorageForAccount(string(accountKey), first); err != nil {
		t.Fatalf("first commitStorageForAccount failed: %v", err)
	}
	if !db.isAccountStorageFolderManaged(accountKey) {
		t.Fatal("expected account to be marked as folder-managed after first large commit")
	}
	nodeMutationWriteOpsBefore := atomic.LoadUint64(&db.diskIOStats[diskIOUsageNodeFileMutation].writeOps)

	second := []kvPair{
		{key: []byte{0x01}, val: bytes.Repeat([]byte("x"), 48)},
		{key: []byte{0x03}, val: bytes.Repeat([]byte("y"), 48)},
	}
	if err := db.commitStorageForAccount(string(accountKey), second); err != nil {
		t.Fatalf("second commitStorageForAccount failed: %v", err)
	}
	nodeMutationWriteOpsAfter := atomic.LoadUint64(&db.diskIOStats[diskIOUsageNodeFileMutation].writeOps)
	if nodeMutationWriteOpsAfter != nodeMutationWriteOpsBefore {
		t.Fatalf("expected folder-managed put to skip account-entry pointer rewrite, nodefile writes before=%d after=%d", nodeMutationWriteOpsBefore, nodeMutationWriteOpsAfter)
	}
}

func TestBatchCommitPlansInlineStoragePointerBeforeAccountNodeWrite(t *testing.T) {
	accountOnlyDB, err := NewPrefixDB(filepath.Join(t.TempDir(), "account-only"), 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer accountOnlyDB.Close()

	combinedDB, err := NewPrefixDB(filepath.Join(t.TempDir(), "combined"), 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer combinedDB.Close()

	accountOnlyKey := makeTestAccountKey(0x13)
	accountOnlyWritesBefore := atomic.LoadUint64(&accountOnlyDB.diskIOStats[diskIOUsageNodeFileMutation].writeOps)
	if err := accountOnlyDB.BatchPut(datatypepkg.TrieNodeAccountDataType, accountOnlyKey, []byte("account-value"), nil); err != nil {
		t.Fatalf("BatchPut account-only failed: %v", err)
	}
	if err := accountOnlyDB.BatchCommit(); err != nil {
		t.Fatalf("BatchCommit account-only failed: %v", err)
	}
	accountOnlyDelta := atomic.LoadUint64(&accountOnlyDB.diskIOStats[diskIOUsageNodeFileMutation].writeOps) - accountOnlyWritesBefore

	accountKey := makeTestAccountKey(0x14)
	nodeMutationWriteOpsBefore := atomic.LoadUint64(&combinedDB.diskIOStats[diskIOUsageNodeFileMutation].writeOps)

	if err := combinedDB.BatchPut(datatypepkg.TrieNodeAccountDataType, accountKey, []byte("account-value"), nil); err != nil {
		t.Fatalf("BatchPut account failed: %v", err)
	}
	if err := combinedDB.BatchPut(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, 0x01), []byte("small-value"), accountKey); err != nil {
		t.Fatalf("BatchPut storage failed: %v", err)
	}
	if err := combinedDB.BatchCommit(); err != nil {
		t.Fatalf("BatchCommit failed: %v", err)
	}

	nodeMutationWriteOpsAfter := atomic.LoadUint64(&combinedDB.diskIOStats[diskIOUsageNodeFileMutation].writeOps)
	combinedDelta := nodeMutationWriteOpsAfter - nodeMutationWriteOpsBefore
	if combinedDelta != accountOnlyDelta {
		t.Fatalf("expected combined account/storage batch to reuse the same node write count as account-only commit, accountOnly=%d combined=%d", accountOnlyDelta, combinedDelta)
	}

	node, err := combinedDB.getNode(accountKey)
	if err != nil {
		t.Fatalf("getNode failed: %v", err)
	}
	if node == nil {
		t.Fatal("expected account node to exist")
	}
	if node.storageFileID == 0 || isSegmentedStorage(node.storageFileID) {
		t.Fatalf("expected inline storage pointer after BatchCommit, got fileID=%d", node.storageFileID)
	}
}

func TestBatchCommitPlansSegmentedStoragePointerBeforeAccountNodeWrite(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x15)
	inlineStorageKey := makeTestStorageRawKey(accountKey, 0x01)
	if err := db.BatchPut(datatypepkg.TrieNodeAccountDataType, accountKey, []byte("account-v1"), nil); err != nil {
		t.Fatalf("BatchPut initial account failed: %v", err)
	}
	if err := db.BatchPut(datatypepkg.TrieNodeStorageDataType, inlineStorageKey, []byte("small-value"), accountKey); err != nil {
		t.Fatalf("BatchPut initial storage failed: %v", err)
	}
	if err := db.BatchCommit(); err != nil {
		t.Fatalf("initial BatchCommit failed: %v", err)
	}

	before, err := db.getNode(accountKey)
	if err != nil {
		t.Fatalf("getNode before migration failed: %v", err)
	}
	if before == nil {
		t.Fatal("expected account node before migration")
	}
	if before.storageFileID == 0 || isSegmentedStorage(before.storageFileID) {
		t.Fatalf("expected initial inline storage pointer, got fileID=%d", before.storageFileID)
	}

	if err := db.BatchPut(datatypepkg.TrieNodeAccountDataType, accountKey, []byte("account-v2"), nil); err != nil {
		t.Fatalf("BatchPut migrated account failed: %v", err)
	}
	largeValues := [][]byte{
		bytes.Repeat([]byte("a"), 40),
		bytes.Repeat([]byte("b"), 40),
		bytes.Repeat([]byte("c"), 40),
	}
	for idx, value := range largeValues {
		storageKey := makeTestStorageRawKey(accountKey, byte(idx+1))
		if err := db.BatchPut(datatypepkg.TrieNodeStorageDataType, storageKey, value, accountKey); err != nil {
			t.Fatalf("BatchPut large storage %d failed: %v", idx, err)
		}
	}
	if err := db.BatchCommit(); err != nil {
		t.Fatalf("migration BatchCommit failed: %v", err)
	}

	after, err := db.getNode(accountKey)
	if err != nil {
		t.Fatalf("getNode after migration failed: %v", err)
	}
	if after == nil {
		t.Fatal("expected account node after migration")
	}
	if after.storageFileID != segmentedStorageFlag || after.storageOffset != 0 || after.storageSize != 0 {
		t.Fatalf("expected account-named segmented sentinel pointer after mixed batch migration, got fileID=%d offset=%d size=%d", after.storageFileID, after.storageOffset, after.storageSize)
	}
	if !db.isAccountStorageFolderManaged(accountKey) {
		t.Fatal("expected account to be marked as folder-managed after mixed batch migration")
	}
	if value, found, err := db.Get(datatypepkg.TrieNodeAccountDataType, accountKey, nil); err != nil {
		t.Fatalf("Get account failed: %v", err)
	} else if !found || !bytes.Equal(value, []byte("account-v2")) {
		t.Fatalf("unexpected migrated account value: found=%t value=%q", found, value)
	}
	for idx, expected := range largeValues {
		storageKey := makeTestStorageRawKey(accountKey, byte(idx+1))
		value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, storageKey, accountKey)
		if err != nil {
			t.Fatalf("Get migrated storage %d failed: %v", idx, err)
		}
		if !found || !bytes.Equal(value, expected) {
			t.Fatalf("unexpected migrated storage %d: found=%t value=%q", idx, found, value)
		}
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	if _, err := os.Stat(filepath.Join(folderPath, segmentIndexFileName)); err != nil {
		t.Fatalf("expected segment index after mixed batch migration: %v", err)
	}
}

func TestGCPrefixTreePreservesInlineStoragePointer(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x14)
	if err := db.BatchPut(datatypepkg.TrieNodeAccountDataType, accountKey, []byte("account-value"), nil); err != nil {
		t.Fatalf("BatchPut account failed: %v", err)
	}
	if err := db.BatchPut(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, 0x01), []byte("small-value"), accountKey); err != nil {
		t.Fatalf("BatchPut storage failed: %v", err)
	}
	if err := db.BatchCommit(); err != nil {
		t.Fatalf("BatchCommit failed: %v", err)
	}

	before, err := db.getNode(accountKey)
	if err != nil {
		t.Fatalf("getNode before GC failed: %v", err)
	}
	if before == nil {
		t.Fatal("expected account node before GC")
	}
	if before.storageFileID == 0 || isSegmentedStorage(before.storageFileID) {
		t.Fatalf("expected inline storage pointer before GC, got fileID=%d", before.storageFileID)
	}

	if err := db.GCPrefixTree(); err != nil {
		t.Fatalf("GCPrefixTree failed: %v", err)
	}

	after, err := db.getNode(accountKey)
	if err != nil {
		t.Fatalf("getNode after GC failed: %v", err)
	}
	if after == nil {
		t.Fatal("expected account node after GC")
	}
	if after.storageFileID != before.storageFileID || after.storageOffset != before.storageOffset || after.storageSize != before.storageSize {
		t.Fatalf("expected inline storage pointer to survive GC, before=%+v after=%+v", before, after)
	}
}

func TestGetStorageLogsInvalidLargeLogPointer(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x16)
	storageKey := makeTestStorageRawKey(accountKey, 0x01)
	db.nodeCache.StoreMetadata(string(accountKey), 1, StorageInfo{storageFileID: 1, storageOffset: 0, storageSize: 0})
	node, err := db.getNode(accountKey)
	if err != nil {
		t.Fatalf("getNode failed: %v", err)
	}
	if node == nil || node.storageFileID != 1 || node.storageSize != 0 {
		t.Fatalf("expected invalid inline pointer setup to persist, node=%+v", node)
	}

	var logBuf bytes.Buffer
	oldLogWriter := prefixdbLogWriter
	prefixdbLogWriter = &logBuf
	defer func() {
		prefixdbLogWriter = oldLogWriter
	}()

	{
		value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, storageKey, accountKey)
		if err != nil {
			t.Fatalf("Get storage failed: %v", err)
		}
		if found || value != nil {
			t.Fatalf("expected invalid large-log pointer read to fail without value, found=%t value=%q", found, value)
		}
	}

	output := logBuf.String()

	if !strings.Contains(output, "prefixdb ERROR: failed to read large log via account entry") {
		t.Fatalf("expected large-log read error log, got %q", output)
	}
	if !strings.Contains(output, "reason=invalid-account-entry-pointer") {
		t.Fatalf("expected invalid pointer reason in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("fileID=%d", uint32(1))) {
		t.Fatalf("expected fileID in log, got %q", output)
	}
}

func TestGetAccountLogsMissingAccountKV(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x19)
	var logBuf bytes.Buffer
	oldLogWriter := prefixdbLogWriter
	prefixdbLogWriter = &logBuf
	defer func() {
		prefixdbLogWriter = oldLogWriter
	}()

	value, found, err := db.Get(datatypepkg.TrieNodeAccountDataType, accountKey, nil)
	if err != nil {
		t.Fatalf("Get account failed: %v", err)
	}
	if found || value != nil {
		t.Fatalf("expected missing account read to fail without value, found=%t value=%q", found, value)
	}

	output := logBuf.String()
	if !strings.Contains(output, "prefixdb ERROR: account kv read failed") {
		t.Fatalf("expected account read failure log, got %q", output)
	}
	if !strings.Contains(output, "reason=account-not-found") {
		t.Fatalf("expected account-not-found reason in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("key=%x", accountKey)) {
		t.Fatalf("expected account key in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("dir=%s", filepath.Dir(db.accountFile.Name()))) {
		t.Fatalf("expected account dir in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("file=%s", filepath.Base(db.accountFile.Name()))) {
		t.Fatalf("expected account file in log, got %q", output)
	}
	if !strings.Contains(output, "offset=0") || !strings.Contains(output, "size=0") {
		t.Fatalf("expected account offset/size in log, got %q", output)
	}
}

func TestGetStorageLogsMissingStorageKV(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x1a)
	storageKey := makeTestStorageRawKey(accountKey, 0x01)
	var logBuf bytes.Buffer
	oldLogWriter := prefixdbLogWriter
	prefixdbLogWriter = &logBuf
	defer func() {
		prefixdbLogWriter = oldLogWriter
	}()

	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, storageKey, accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if found || value != nil {
		t.Fatalf("expected missing storage read to fail without value, found=%t value=%q", found, value)
	}

	output := logBuf.String()
	if !strings.Contains(output, "prefixdb ERROR: storage kv read failed") {
		t.Fatalf("expected storage read failure log, got %q", output)
	}
	if !strings.Contains(output, "reason=storage-not-found") {
		t.Fatalf("expected storage-not-found reason in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("account=%x", accountKey)) {
		t.Fatalf("expected account key in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("storage=%x", storageKey[33:])) {
		t.Fatalf("expected normalized storage key in log, got %q", output)
	}
}

func TestGetStorageLogsFolderManagedChunkMiss(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x33)
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	metas, err := db.writeSegmentedChunksToFolder(folderPath, []kvPair{{key: []byte{0x0a}, val: []byte("value-a")}})
	if err != nil {
		t.Fatalf("writeSegmentedChunksToFolder failed: %v", err)
	}
	if err := db.writeSegmentIndex(folderPath, metas); err != nil {
		t.Fatalf("writeSegmentIndex failed: %v", err)
	}
	db.markAccountStorageFolder(accountKey)

	storageKey := makeTestStorageRawKey(accountKey, 0x0b)
	var logBuf bytes.Buffer
	oldLogWriter := prefixdbLogWriter
	prefixdbLogWriter = &logBuf
	defer func() {
		prefixdbLogWriter = oldLogWriter
	}()

	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, storageKey, accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if found || value != nil {
		t.Fatalf("expected missing folder-managed storage to fail without value, found=%t value=%q", found, value)
	}

	output := logBuf.String()
	if !strings.Contains(output, "mode=folder") {
		t.Fatalf("expected folder mode in log, got %q", output)
	}
	if !strings.Contains(output, "index=index.meta") {
		t.Fatalf("expected index file in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("chunk=%s", metas[0].FileName)) {
		t.Fatalf("expected selected chunk file in log, got %q", output)
	}
	if !strings.Contains(output, "reason=segment-chunk-key-not-found") {
		t.Fatalf("expected exact folder miss reason in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("file=%s", filepath.Base(folderPath))) {
		t.Fatalf("expected folder basename in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("storage=%x", storageKey[33:])) {
		t.Fatalf("expected normalized storage key in log, got %q", output)
	}
}

func TestGetStorageLogsMissingLargeLogFile(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x17)
	storageKey := makeTestStorageRawKey(accountKey, 0x01)
	db.nodeCache.StoreMetadata(string(accountKey), 1, StorageInfo{storageFileID: 7, storageOffset: 0, storageSize: 16})

	var logBuf bytes.Buffer
	oldLogWriter := prefixdbLogWriter
	prefixdbLogWriter = &logBuf
	defer func() {
		prefixdbLogWriter = oldLogWriter
	}()

	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, storageKey, accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if found || value != nil {
		t.Fatalf("expected missing large-log file read to fail without value, found=%t value=%q", found, value)
	}

	output := logBuf.String()
	if !strings.Contains(output, "prefixdb ERROR: failed to read large log via account entry") {
		t.Fatalf("expected missing-file error log, got %q", output)
	}
	if !strings.Contains(output, "reason=open-storage-file") {
		t.Fatalf("expected open-storage-file reason in log, got %q", output)
	}
	if !strings.Contains(output, "fileID=7") {
		t.Fatalf("expected fileID in log, got %q", output)
	}
	storagePath, _ := db.storagePathByFileID(7)
	if !strings.Contains(output, fmt.Sprintf("dir=%s", filepath.Dir(storagePath))) {
		t.Fatalf("expected storage dir in log, got %q", output)
	}
	if !strings.Contains(output, fmt.Sprintf("file=%s", filepath.Base(storagePath))) {
		t.Fatalf("expected storage file in log, got %q", output)
	}
	if !strings.Contains(output, "offset=0") || !strings.Contains(output, "size=16") {
		t.Fatalf("expected storage offset/size in log, got %q", output)
	}
}

func TestGetStorageLogsCorruptedLargeLogContent(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x18)
	storageKey := makeTestStorageRawKey(accountKey, 0x01)
	storagePath, _ := db.storagePathByFileID(8)
	corrupted := make([]byte, 10)
	binary.BigEndian.PutUint32(corrupted[:4], 1)
	copy(corrupted[4:], []byte{0x00, 0x04, 0x00, 0x00, 0x00, 0x08})
	if err := os.WriteFile(storagePath, corrupted, 0o644); err != nil {
		t.Fatalf("WriteFile corrupted storage failed: %v", err)
	}
	db.nodeCache.StoreMetadata(string(accountKey), 1, StorageInfo{storageFileID: 8, storageOffset: 0, storageSize: uint64(len(corrupted))})

	var logBuf bytes.Buffer
	oldLogWriter := prefixdbLogWriter
	prefixdbLogWriter = &logBuf
	defer func() {
		prefixdbLogWriter = oldLogWriter
	}()

	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, storageKey, accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if found || value != nil {
		t.Fatalf("expected corrupted large-log read to fail without value, found=%t value=%q", found, value)
	}

	output := logBuf.String()
	if !strings.Contains(output, "prefixdb ERROR: failed to read large log via account entry") {
		t.Fatalf("expected corrupted-content error log, got %q", output)
	}
	if !strings.Contains(output, "reason=corrupted-storage-segment") {
		t.Fatalf("expected corrupted-storage-segment reason in log, got %q", output)
	}
	if !strings.Contains(output, "fileID=8") {
		t.Fatalf("expected fileID in log, got %q", output)
	}
}

func TestGetStorageBypassesAccountEntryForAccountNamedSegmentedFolder(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x20)
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	kvs := []kvPair{
		{key: []byte{0x0a}, val: []byte("value-a")},
		{key: []byte{0x0b}, val: []byte("value-b")},
	}
	metas, err := db.writeSegmentedChunksToFolder(folderPath, kvs)
	if err != nil {
		t.Fatalf("writeSegmentedChunksToFolder failed: %v", err)
	}
	if err := db.writeSegmentIndex(folderPath, metas); err != nil {
		t.Fatalf("writeSegmentIndex failed: %v", err)
	}
	db.markAccountStorageFolder(accountKey)

	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, 0x0b), accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if !found || !bytes.Equal(value, []byte("value-b")) {
		t.Fatalf("expected direct folder read without account entry, found=%t value=%q", found, value)
	}
	count, totalSize, err := db.GetStorageCount(accountKey)
	if err != nil {
		t.Fatalf("GetStorageCount failed: %v", err)
	}
	if count != 2 || totalSize == 0 {
		t.Fatalf("unexpected account-named storage count result: count=%d totalSize=%d", count, totalSize)
	}
}

func TestMissingFolderReadFallsBackToAccountEntry(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 128, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x21)
	if err := db.commitStorageForAccount(string(accountKey), []kvPair{{key: []byte{0x01}, val: []byte("fallback-value")}}); err != nil {
		t.Fatalf("commitStorageForAccount failed: %v", err)
	}
	db.markAccountStorageFolder(accountKey)

	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, 0x01), accountKey)
	if err != nil {
		t.Fatalf("Get storage failed: %v", err)
	}
	if !found || !bytes.Equal(value, []byte("fallback-value")) {
		t.Fatalf("unexpected fallback result: found=%t value=%q", found, value)
	}
	if db.isAccountStorageFolderManaged(accountKey) {
		t.Fatal("expected missing folder fallback to clear folder-managed marker")
	}
}

func TestUpgradeAndGCHandleAccountNamedSegmentedFolders(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x30)
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	kvs := []kvPair{
		{key: []byte{0x01}, val: bytes.Repeat([]byte("x"), 48)},
		{key: []byte{0x02}, val: bytes.Repeat([]byte("y"), 48)},
		{key: []byte{0x03}, val: bytes.Repeat([]byte("z"), 48)},
	}
	metas, err := db.writeSegmentedChunksToFolder(folderPath, kvs)
	if err != nil {
		t.Fatalf("writeSegmentedChunksToFolder failed: %v", err)
	}
	if err := db.writeSegmentIndex(folderPath, metas); err != nil {
		t.Fatalf("writeSegmentIndex failed: %v", err)
	}
	db.markAccountStorageFolder(accountKey)

	if err := db.UpgradeSegmentIndexFiles(); err != nil {
		t.Fatalf("UpgradeSegmentIndexFiles failed: %v", err)
	}
	if err := db.GCAllStorageChunkFiles(); err != nil {
		t.Fatalf("GCAllStorageChunkFiles failed: %v", err)
	}
	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, 0x03), accountKey)
	if err != nil {
		t.Fatalf("Get storage failed after maintenance: %v", err)
	}
	if !found || !bytes.Equal(value, bytes.Repeat([]byte("z"), 48)) {
		t.Fatalf("unexpected storage lookup after maintenance: found=%t value=%q", found, value)
	}
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
	value, found, err := db.Get(datatypepkg.TrieNodeAccountDataType, shortKey, nil)
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
	value, found, err = db.Get(datatypepkg.TrieNodeAccountDataType, longKey, nil)
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

}

func TestMigrateLegacySegmentIndexFormatsNotSupported(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 16*1024, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	if err := db.MigrateLegacySegmentIndexFormats(); err == nil {
		t.Fatal("expected legacy migration to be unsupported")
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

func TestSegmentIndexSmallPayloadStaysUncompressed(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDBWithRuntimeOptions(baseDir, 16*1024, 8, 16, 0, 0, 0, false, true, 0)
	if err != nil {
		t.Fatalf("NewPrefixDBWithRuntimeOptions failed: %v", err)
	}
	defer db.Close()

	folderID := uint32(9)
	folderPath := db.segmentedFolderPath(folderID)
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	metas := []segmentChunkMeta{
		{FileName: "chunk_0001.dat", KeyStart: []byte{0x01}, KeyEnd: []byte{0x02}, KVCount: 1, ChunkSize: 128},
		{FileName: "chunk_0002.dat", KeyStart: []byte{0x03}, KeyEnd: []byte{0x04}, KVCount: 2, ChunkSize: 256},
	}
	if err := db.writeSegmentIndex(folderPath, metas); err != nil {
		t.Fatalf("writeSegmentIndex failed: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(folderPath, segmentIndexFileName))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if got := binary.BigEndian.Uint32(raw[:4]); got == compressedMetadataMagic {
		t.Fatalf("expected small segment index payload to remain uncompressed")
	}
	decoded, err := db.readSegmentIndexNoCache(folderID)
	if err != nil {
		t.Fatalf("readSegmentIndexNoCache failed: %v", err)
	}
	if !segmentChunkMetasEqual(decoded, metas) {
		t.Fatalf("decoded metas mismatch: got %+v want %+v", decoded, metas)
	}
	if err := db.UpgradeSegmentIndexFiles(); err != nil {
		t.Fatalf("UpgradeSegmentIndexFiles failed: %v", err)
	}
	decoded, err = db.readSegmentIndexNoCache(folderID)
	if err != nil {
		t.Fatalf("readSegmentIndexNoCache after upgrade failed: %v", err)
	}
	if !segmentChunkMetasEqual(decoded, metas) {
		t.Fatalf("decoded metas mismatch after upgrade: got %+v want %+v", decoded, metas)
	}
}

func TestSegmentIndexLargePayloadUsesCompression(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDBWithRuntimeOptions(baseDir, 16*1024, 8, 16, 0, 0, 0, false, true, 0)
	if err != nil {
		t.Fatalf("NewPrefixDBWithRuntimeOptions failed: %v", err)
	}
	defer db.Close()

	folderID := uint32(10)
	folderPath := db.segmentedFolderPath(folderID)
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	metas := []segmentChunkMeta{{
		FileName:  "chunk_0001.dat",
		KeyStart:  bytes.Repeat([]byte{0x01}, segmentIndexCompressionMinSize+32),
		KeyEnd:    []byte{0x02},
		KVCount:   1,
		ChunkSize: 128,
	}}
	if got := estimateSegmentIndexSize(metas); got <= segmentIndexCompressionMinSize {
		t.Fatalf("test fixture must exceed compression threshold: got %d", got)
	}
	if err := db.writeSegmentIndex(folderPath, metas); err != nil {
		t.Fatalf("writeSegmentIndex failed: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(folderPath, segmentIndexFileName))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if got := binary.BigEndian.Uint32(raw[:4]); got != compressedMetadataMagic {
		t.Fatalf("expected compressed metadata wrapper magic, got 0x%x", got)
	}
	decoded, err := db.readSegmentIndexNoCache(folderID)
	if err != nil {
		t.Fatalf("readSegmentIndexNoCache failed: %v", err)
	}
	if !segmentChunkMetasEqual(decoded, metas) {
		t.Fatalf("decoded metas mismatch: got %+v want %+v", decoded, metas)
	}
}

func TestRefreshSegmentIndexCacheUpdatesEntries(t *testing.T) {
	shared := newSharedByteCache(4096)
	storageDir := t.TempDir()
	db := &PrefixDB{
		storageDir:             storageDir,
		storageIndexCache:      newSharedSegmentIndexCache(shared),
		storageIndexFolderPath: filepath.Join(storageDir, segmentedDirNamePrefix+"00000007"),
		storageIndexMetas:      []segmentChunkMeta{{FileName: "old.dat"}},
	}
	metas := []segmentChunkMeta{{
		FileName:  "chunk_0001.dat",
		KeyStart:  []byte{0x01},
		KeyEnd:    []byte{0x02},
		KVCount:   1,
		ChunkSize: 128,
	}}
	folderPath := filepath.Join(storageDir, segmentedDirNamePrefix+"00000007")

	db.refreshSegmentIndexCache(7, metas)

	if got, ok := db.storageIndexCache.GetByPath(folderPath); !ok || len(got) != 1 || got[0].FileName != "chunk_0001.dat" {
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

func TestNewPrefixTreeLoadsCompressedGlobalNodeIntoSkipList(t *testing.T) {
	baseDir := t.TempDir()
	fileNodeDir := filepath.Join(baseDir, "prefixdb", "filenodes")
	if err := os.MkdirAll(fileNodeDir, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	filePath := filepath.Join(fileNodeDir, globalFileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	header := FileNodeHeader{Magic: FileNodeMagic, Version: fileNodeVersionBase, SortedEntryCount: 1, UnsortedEntryCount: 1}
	sortedData := encodeNodeEntries([]NodeInfo{{key: []byte{0x01, 0x02, 0x03}, accountOffset: 10, storageFileID: 1, storageOffset: 11, storageSize: 12}})
	unsortedData := encodeNodeEntries([]NodeInfo{{key: []byte{0x01, 0x02, 0x03}, accountOffset: 20, storageFileID: 2, storageOffset: 21, storageSize: 22}})
	payload, err := encodeNodeFilePayload(&header, sortedData, unsortedData, true)
	if err != nil {
		_ = file.Close()
		t.Fatalf("encodeNodeFilePayload failed: %v", err)
	}
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		_ = file.Close()
		t.Fatalf("write header failed: %v", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		t.Fatalf("write payload failed: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file failed: %v", err)
	}

	pt, err := NewPrefixTree(&PrefixDB{}, baseDir)
	if err != nil {
		t.Fatalf("NewPrefixTree failed: %v", err)
	}
	defer pt.Close()

	node, found, err := pt.Get([]byte{0x01, 0x02, 0x03})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("expected compressed global node entry to be loaded")
	}
	if node.accountOffset != 20 || node.storageFileID != 2 || node.storageOffset != 21 || node.storageSize != 22 {
		t.Fatalf("unexpected loaded node info: %+v", node)
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

func TestPutIntoCompressedNodeFileAppendsAfterCompressedSortedPayload(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDBWithRuntimeOptions(baseDir, 16*1024, 8, 16, 0, 0, 0, true, false, 0)
	if err != nil {
		t.Fatalf("NewPrefixDBWithRuntimeOptions failed: %v", err)
	}
	defer db.Close()

	pt := db.prefixTree
	fileID := pt.getBucketID([]byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee})
	filePath := filepath.Join(pt.fileNodeDir, fileID)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	header := FileNodeHeader{Magic: FileNodeMagic, Version: fileNodeVersionBase, SortedEntryCount: 1}
	sortedEntry := NodeInfo{key: []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee}, accountOffset: 10, storageFileID: 1, storageOffset: 11, storageSize: 12}
	payload, err := encodeNodeFilePayload(&header, encodeNodeEntries([]NodeInfo{sortedEntry}), nil, true)
	if err != nil {
		t.Fatalf("encodeNodeFilePayload failed: %v", err)
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		_ = file.Close()
		t.Fatalf("write header failed: %v", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		t.Fatalf("write payload failed: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file failed: %v", err)
	}

	updated := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee}
	if err := pt.putIntoFileNode(fileID, updated, 20, 2, 21, 22); err != nil {
		t.Fatalf("putIntoFileNode failed: %v", err)
	}
	node, found, err := pt.getFromFileNode(fileID, updated)
	if err != nil {
		t.Fatalf("getFromFileNode failed: %v", err)
	}
	if !found {
		t.Fatal("expected appended node to be found")
	}
	if node.accountOffset != 20 || node.storageFileID != 2 || node.storageOffset != 21 || node.storageSize != 22 {
		t.Fatalf("unexpected appended node info: %+v", node)
	}
}

func TestGetFromFileNodeFindsSortedEntryInSortedPart(t *testing.T) {
	baseDir := t.TempDir()
	db, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	pt := db.prefixTree
	key := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0x20}
	fileID := pt.getBucketID(key)
	filePath := filepath.Join(pt.fileNodeDir, fileID)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	entries := make([]NodeInfo, 0, 64)
	for i := 0; i < 64; i++ {
		entryKey := []byte{0xaa, 0xbb, 0xcc, 0xdd, byte(i)}
		entries = append(entries, NodeInfo{key: entryKey, accountOffset: int64(100 + i), storageFileID: uint32(i + 1), storageOffset: int64(200 + i), storageSize: uint64(300 + i)})
	}
	header := FileNodeHeader{Magic: FileNodeMagic, Version: fileNodeVersionBase, SortedEntryCount: uint32(len(entries))}
	payload, err := encodeNodeFilePayload(&header, encodeNodeEntries(entries), nil, true)
	if err != nil {
		t.Fatalf("encodeNodeFilePayload failed: %v", err)
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	if err := binary.Write(file, binary.BigEndian, &header); err != nil {
		_ = file.Close()
		t.Fatalf("write header failed: %v", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		t.Fatalf("write payload failed: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close file failed: %v", err)
	}

	node, found, err := pt.getFromFileNode(fileID, key)
	if err != nil {
		t.Fatalf("getFromFileNode failed: %v", err)
	}
	if !found {
		t.Fatal("expected sorted entry to be found")
	}
	if node.accountOffset != 132 || node.storageFileID != 33 || node.storageOffset != 232 || node.storageSize != 332 {
		t.Fatalf("unexpected sorted lookup result: %+v", node)
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

func TestRunPostLoadGCCompactsAllNodeFilesIgnoringRatio(t *testing.T) {
	db, err := NewPrefixDBWithRuntimeOptions(t.TempDir(), 16*1024, 8, 16, 1e9, 0, 1e9, true, false, 0)
	if err != nil {
		t.Fatalf("NewPrefixDBWithRuntimeOptions failed: %v", err)
	}
	defer db.Close()

	globalKey := []byte{0x01, 0x02, 0x03}
	bucketKey := bytes.Repeat([]byte{0xaa}, 32)
	if err := db.prefixTree.Put(globalKey, 11, 1, 101, 1001); err != nil {
		t.Fatalf("put global key failed: %v", err)
	}
	if err := db.prefixTree.Put(bucketKey, 22, 2, 202, 2002); err != nil {
		t.Fatalf("put bucket key failed: %v", err)
	}

	globalPath := filepath.Join(db.prefixTree.fileNodeDir, globalFileName)
	bucketID := db.prefixTree.fileIDForKey(bucketKey)
	bucketPath := filepath.Join(db.prefixTree.fileNodeDir, bucketID)

	for _, path := range []string{globalPath, bucketPath} {
		file, err := os.Open(path)
		if err != nil {
			t.Fatalf("open node file before GC failed: %v", err)
		}
		var header FileNodeHeader
		if err := binary.Read(file, binary.BigEndian, &header); err != nil {
			_ = file.Close()
			t.Fatalf("read header before GC failed: %v", err)
		}
		_ = file.Close()
		if header.UnsortedEntryCount == 0 {
			t.Fatalf("expected unsorted entries before post-load GC for %s", path)
		}
	}

	if err := db.RunPostLoadGC(); err != nil {
		t.Fatalf("RunPostLoadGC failed: %v", err)
	}

	for _, tc := range []struct {
		name string
		path string
		key  []byte
	}{
		{name: "global", path: globalPath, key: globalKey},
		{name: "bucket", path: bucketPath, key: bucketKey},
	} {
		file, err := os.Open(tc.path)
		if err != nil {
			t.Fatalf("open %s node file after GC failed: %v", tc.name, err)
		}
		var header FileNodeHeader
		if err := binary.Read(file, binary.BigEndian, &header); err != nil {
			_ = file.Close()
			t.Fatalf("read %s header after GC failed: %v", tc.name, err)
		}
		_ = file.Close()
		if header.UnsortedEntryCount != 0 {
			t.Fatalf("expected %s node file to be fully compacted, unsorted=%d", tc.name, header.UnsortedEntryCount)
		}
		if header.SortedEntryCount == 0 {
			t.Fatalf("expected %s node file to retain sorted entries", tc.name)
		}
		if !header.sortedCompressed() {
			t.Fatalf("expected %s node file sorted part to be compressed", tc.name)
		}
		node, found, err := db.prefixTree.Get(tc.key)
		if err != nil {
			t.Fatalf("Get %s key after GC failed: %v", tc.name, err)
		}
		if !found {
			t.Fatalf("expected %s key to remain after GC", tc.name)
		}
		if node.accountOffset == 0 {
			t.Fatalf("expected %s node info to remain populated after GC", tc.name)
		}
	}
}

func TestRunPostLoadGCFullyRewritesStorageSegmentsIgnoringThreshold(t *testing.T) {
	db, err := NewPrefixDBWithRuntimeOptions(t.TempDir(), 64, 8, 16, 1e9, 0, 1e9, true, true, 0)
	if err != nil {
		t.Fatalf("NewPrefixDBWithRuntimeOptions failed: %v", err)
	}
	defer db.Close()

	folderID := uint32(7)
	folderPath := db.segmentedFolderPath(folderID)
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	const totalChunks = 96
	metas := make([]segmentChunkMeta, 0, totalChunks)
	for i := 0; i < totalChunks; i++ {
		key := []byte(fmt.Sprintf("storage-key-%03d-%s", i, strings.Repeat("k", 64)))
		value := []byte(fmt.Sprintf("value-%03d", i))
		if i == totalChunks-1 {
			key = []byte(fmt.Sprintf("storage-key-%03d-%s", 0, strings.Repeat("k", 64)))
			value = []byte("value-latest")
		}
		entries := []kvPair{{key: key, val: value}}
		fileName := chunkFileNameForOrdinal(uint32(i))
		chunkSize, err := db.writeChunkFile(folderPath, fileName, entries)
		if err != nil {
			t.Fatalf("writeChunkFile %s failed: %v", fileName, err)
		}
		metas = append(metas, segmentChunkMeta{
			FileName:  fileName,
			KeyStart:  append([]byte(nil), key...),
			KeyEnd:    append([]byte(nil), key...),
			KVCount:   1,
			ChunkSize: uint64(chunkSize),
		})
	}
	if err := db.writeSegmentIndex(folderPath, metas); err != nil {
		t.Fatalf("writeSegmentIndex failed: %v", err)
	}

	if err := db.RunPostLoadGC(); err != nil {
		t.Fatalf("RunPostLoadGC failed: %v", err)
	}

	rawIndex, err := os.ReadFile(filepath.Join(folderPath, segmentIndexFileName))
	if err != nil {
		t.Fatalf("ReadFile index.meta failed: %v", err)
	}
	if len(rawIndex) < 4 || binary.BigEndian.Uint32(rawIndex[:4]) != compressedMetadataMagic {
		t.Fatalf("expected compressed segment index after post-load GC")
	}

	updatedMetas, err := db.readSegmentIndexNoCache(folderID)
	if err != nil {
		t.Fatalf("readSegmentIndexNoCache failed: %v", err)
	}
	if len(updatedMetas) == 0 {
		t.Fatal("expected rewritten storage chunk metadata after post-load GC")
	}

	allEntries := make([]kvPair, 0)
	for _, meta := range updatedMetas {
		entries, backing, err := db.readSegmentChunkFile(folderID, meta.FileName)
		if err != nil {
			t.Fatalf("readSegmentChunkFile %s failed: %v", meta.FileName, err)
		}
		for _, entry := range entries {
			keyCopy := append([]byte(nil), entry.key...)
			var valCopy []byte
			if entry.val != nil {
				valCopy = append([]byte(nil), entry.val...)
			}
			allEntries = append(allEntries, kvPair{key: keyCopy, val: valCopy})
		}
		if backing != nil {
			backing.Release()
		}
	}
	if len(allEntries) != totalChunks-1 {
		t.Fatalf("expected deduplicated storage entries after full GC, got %d want %d", len(allEntries), totalChunks-1)
	}
	entriesByKey := make(map[string][]byte, len(allEntries))
	for _, entry := range allEntries {
		entriesByKey[string(entry.key)] = entry.val
	}
	if len(entriesByKey) != len(allEntries) {
		t.Fatalf("expected post-load GC to leave only unique storage keys, got %d unique out of %d", len(entriesByKey), len(allEntries))
	}
}

func TestSegmentedChunkFileUsesPayloadWithoutLeadingKVCount(t *testing.T) {
	db, err := NewPrefixDB(t.TempDir(), 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	folderPath := db.segmentedFolderPath(11)
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	entries := []kvPair{{key: []byte{0x0a}, val: []byte("value-a")}}
	if _, err := db.writeChunkFile(folderPath, "chunk_0000.dat", entries); err != nil {
		t.Fatalf("writeChunkFile failed: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(folderPath, "chunk_0000.dat"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	wantPrefix := []byte{0x00, 0x01, 0x00, 0x07}
	if len(raw) < len(wantPrefix) || !bytes.Equal(raw[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("expected chunk to start with first kv header, got %x", raw)
	}
	if got, want := len(raw), len(wantPrefix)+1+len("value-a"); got != want {
		t.Fatalf("unexpected chunk size: got %d want %d", got, want)
	}
	if len(raw) >= 4 && binary.BigEndian.Uint32(raw[:4]) == uint32(len(entries)) {
		t.Fatalf("unexpected leading kvCount metadata in chunk: %x", raw[:4])
	}
}

func TestAppendChunkFileReadsBackWithoutLeadingKVCount(t *testing.T) {
	db, err := NewPrefixDB(t.TempDir(), 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	folderID := uint32(12)
	folderPath := db.segmentedFolderPath(folderID)
	if err := os.MkdirAll(folderPath, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	initial := []kvPair{{key: []byte{0x0a}, val: []byte("value-a")}}
	chunkSize, err := db.writeChunkFile(folderPath, "chunk_0000.dat", initial)
	if err != nil {
		t.Fatalf("writeChunkFile failed: %v", err)
	}
	if err := db.appendChunkFile(filepath.Join(folderPath, "chunk_0000.dat"), []kvPair{{key: []byte{0x0b}, val: []byte("value-b")}}, int64(chunkSize)); err != nil {
		t.Fatalf("appendChunkFile failed: %v", err)
	}

	entries, backing, err := db.readSegmentChunkFile(folderID, "chunk_0000.dat")
	if backing != nil {
		defer backing.Release()
	}
	if err != nil {
		t.Fatalf("readSegmentChunkFile failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries after append, got %d", len(entries))
	}
	if !bytes.Equal(entries[0].key, []byte{0x0a}) || !bytes.Equal(entries[0].val, []byte("value-a")) {
		t.Fatalf("unexpected first entry after append: %+v", entries[0])
	}
	if !bytes.Equal(entries[1].key, []byte{0x0b}) || !bytes.Equal(entries[1].val, []byte("value-b")) {
		t.Fatalf("unexpected second entry after append: %+v", entries[1])
	}
}

func TestCommonStorageSegmentUsesPayloadWithoutLeadingKVCount(t *testing.T) {
	db, err := NewPrefixDB(t.TempDir(), 256, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	kvs := []kvPair{
		{key: []byte{0x01}, val: []byte("value-a")},
		{key: []byte{0x02}, val: []byte("value-b")},
	}
	fileID, offset, size, err := db.appendStorageSegment(kvs)
	if err != nil {
		t.Fatalf("appendStorageSegment failed: %v", err)
	}

	path, _ := db.storagePathByFileID(fileID)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	segment := raw[offset : offset+int64(size)]
	wantPrefix := []byte{0x00, 0x01, 0x00, 0x07}
	if len(segment) < len(wantPrefix) || !bytes.Equal(segment[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("expected common storage segment to start with first kv header, got %x", segment)
	}
	if len(segment) >= 4 && binary.BigEndian.Uint32(segment[:4]) == uint32(len(kvs)) {
		t.Fatalf("unexpected leading kvCount metadata in common storage segment: %x", segment[:4])
	}
	entries, backing, err := db.readStorageSegmentPairs(fileID, offset, size)
	if err != nil {
		t.Fatalf("readStorageSegmentPairs failed: %v", err)
	}
	if backing != nil {
		defer backing.Release()
	}
	if len(entries) != len(kvs) {
		t.Fatalf("unexpected common storage entry count: got %d want %d", len(entries), len(kvs))
	}
	for i := range kvs {
		if !bytes.Equal(entries[i].key, kvs[i].key) || !bytes.Equal(entries[i].val, kvs[i].val) {
			t.Fatalf("unexpected common storage entry %d: got=%+v want=%+v", i, entries[i], kvs[i])
		}
	}
}

func TestAccountNamedSegmentedAppendDoesNotRewriteIndex(t *testing.T) {
	db, err := NewPrefixDB(t.TempDir(), 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	defer db.Close()

	accountKey := makeTestAccountKey(0x44)
	initial := []kvPair{
		{key: []byte{0x01}, val: bytes.Repeat([]byte("a"), 40)},
		{key: []byte{0x02}, val: bytes.Repeat([]byte("b"), 40)},
		{key: []byte{0x03}, val: bytes.Repeat([]byte("c"), 40)},
	}
	if err := db.commitStorageForAccount(string(accountKey), initial); err != nil {
		t.Fatalf("first commitStorageForAccount failed: %v", err)
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	indexBefore, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("ReadFile index before append failed: %v", err)
	}

	appendOnly := []kvPair{{key: []byte{0x04}, val: bytes.Repeat([]byte("d"), 40)}}
	if err := db.commitStorageForAccount(string(accountKey), appendOnly); err != nil {
		t.Fatalf("second commitStorageForAccount failed: %v", err)
	}
	indexAfter, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("ReadFile index after append failed: %v", err)
	}
	if !bytes.Equal(indexBefore, indexAfter) {
		t.Fatalf("expected append-only segmented update to keep index.meta unchanged\nbefore=%x\nafter=%x", indexBefore, indexAfter)
	}

	count, _, err := db.GetStorageCount(accountKey)
	if err != nil {
		t.Fatalf("GetStorageCount failed: %v", err)
	}
	if count != 4 {
		t.Fatalf("expected append-only update to preserve old entries and add the new one, got %d", count)
	}
	value, found, err := db.Get(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, 0x04), accountKey)
	if err != nil {
		t.Fatalf("Get appended storage failed: %v", err)
	}
	if !found || !bytes.Equal(value, bytes.Repeat([]byte("d"), 40)) {
		t.Fatalf("unexpected appended storage value: found=%t value=%q", found, value)
	}
	value, found, err = db.Get(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, 0x01), accountKey)
	if err != nil {
		t.Fatalf("Get preserved storage failed: %v", err)
	}
	if !found || !bytes.Equal(value, bytes.Repeat([]byte("a"), 40)) {
		t.Fatalf("unexpected preserved storage value: found=%t value=%q", found, value)
	}
}

func TestAccountNamedSegmentedAppendSurvivesReopenWithoutIndexRewrite(t *testing.T) {
	baseDir := t.TempDir()
	accountKey := makeTestAccountKey(0x45)

	db, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("NewPrefixDB failed: %v", err)
	}
	initial := []kvPair{
		{key: []byte{0x01}, val: bytes.Repeat([]byte("a"), 40)},
		{key: []byte{0x02}, val: bytes.Repeat([]byte("b"), 40)},
		{key: []byte{0x03}, val: bytes.Repeat([]byte("c"), 40)},
	}
	if err := db.commitStorageForAccount(string(accountKey), initial); err != nil {
		_ = db.Close()
		t.Fatalf("first commitStorageForAccount failed: %v", err)
	}
	folderPath := db.segmentedFolderPathForAccount(accountKey)
	indexPath := filepath.Join(folderPath, segmentIndexFileName)
	indexBefore, err := os.ReadFile(indexPath)
	if err != nil {
		_ = db.Close()
		t.Fatalf("ReadFile index before append failed: %v", err)
	}
	if err := db.commitStorageForAccount(string(accountKey), []kvPair{{key: []byte{0x04}, val: bytes.Repeat([]byte("d"), 40)}}); err != nil {
		_ = db.Close()
		t.Fatalf("second commitStorageForAccount failed: %v", err)
	}
	indexAfter, err := os.ReadFile(indexPath)
	if err != nil {
		_ = db.Close()
		t.Fatalf("ReadFile index after append failed: %v", err)
	}
	if !bytes.Equal(indexBefore, indexAfter) {
		_ = db.Close()
		t.Fatalf("expected append-only segmented update to keep index.meta unchanged across reopen setup")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := NewPrefixDB(baseDir, 64, 8, 16)
	if err != nil {
		t.Fatalf("reopen NewPrefixDB failed: %v", err)
	}
	defer reopened.Close()

	if !reopened.isAccountStorageFolderManaged(accountKey) {
		t.Fatal("expected folder-managed marker to survive reopen after append-only update")
	}
	for suffix, expected := range map[byte][]byte{
		0x01: bytes.Repeat([]byte("a"), 40),
		0x02: bytes.Repeat([]byte("b"), 40),
		0x03: bytes.Repeat([]byte("c"), 40),
		0x04: bytes.Repeat([]byte("d"), 40),
	} {
		value, found, err := reopened.Get(datatypepkg.TrieNodeStorageDataType, makeTestStorageRawKey(accountKey, suffix), accountKey)
		if err != nil {
			t.Fatalf("Get storage after reopen failed for suffix %x: %v", suffix, err)
		}
		if !found || !bytes.Equal(value, expected) {
			t.Fatalf("unexpected storage after reopen for suffix %x: found=%t value=%q", suffix, found, value)
		}
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
