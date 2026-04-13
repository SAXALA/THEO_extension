package prefixdb

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type storageCommitTask struct {
	index      int
	accountKey string
}

type storageCommitResult struct {
	index int
	plan  storageCommitPlan
	err   error
}

type storageCommitPlan struct {
	accountKey    string
	accountOffset uint64
	accountSize   uint32
	existingInfo  StorageInfo
	storageInfo   StorageInfo
	skipNodeWrite bool
	cacheEntries  []kvPair
	inlineSegment []byte
}

type preparedAccountCommit struct {
	entries   map[string][]byte
	order     []string
	totalSize int
}

type storageBatcher struct {
	mu      sync.Mutex
	pending map[string]map[string][]byte
	// unresolved stores original full keys (string) -> value for entries
	// where the accountKey was not provided at BatchPut time.
	unresolved map[string][]byte
}

func newStorageBatcher() *storageBatcher {
	return &storageBatcher{
		pending:    make(map[string]map[string][]byte),
		unresolved: make(map[string][]byte),
	}
}

func (sb *storageBatcher) reset() {
	sb.mu.Lock()
	sb.pending = make(map[string]map[string][]byte)
	sb.unresolved = make(map[string][]byte)
	sb.mu.Unlock()
}

func (sb *storageBatcher) put(accountKey string, storageKey, value []byte) {
	if accountKey == "" {
		// Should not drop unresolved here; caller should use putUnresolved when accountKey is unknown.
		fmt.Printf("storageBatcher.put: dropped storage entry with empty accountKey - storageKey=%x valueLen=%d\n",
			storageKey, len(value))
		return
	}
	sb.mu.Lock()
	perAccount := sb.pending[accountKey]
	if perAccount == nil {
		perAccount = make(map[string][]byte)
		sb.pending[accountKey] = perAccount
	}
	keyStr := string(storageKey)
	if value == nil {
		perAccount[keyStr] = nil
		sb.mu.Unlock()
		return
	}
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	perAccount[keyStr] = valCopy
	sb.mu.Unlock()
}

func (sb *storageBatcher) putUnresolved(originalKey string, value []byte) {
	if originalKey == "" {
		fmt.Printf("storageBatcher.putUnresolved: dropped storage entry with empty originalKey - valueLen=%d\n",
			len(value))
		return
	}
	sb.mu.Lock()
	if value == nil {
		sb.unresolved[originalKey] = nil
		sb.mu.Unlock()
		return
	}
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	sb.unresolved[originalKey] = valCopy
	sb.mu.Unlock()
}

func (sb *storageBatcher) get(accountKey string, storageKey []byte) ([]byte, bool) {
	if accountKey == "" {
		return nil, false
	}
	keyStr := string(storageKey)
	sb.mu.Lock()
	perAccount := sb.pending[accountKey]
	if perAccount == nil {
		sb.mu.Unlock()
		return nil, false
	}
	value, ok := perAccount[keyStr]
	sb.mu.Unlock()
	return value, ok
}

// drain transfers ownership of all pending storage kvs to the caller.
// drain transfers ownership of all pending storage kvs to the caller.
// Returns both the per-account pending map and the unresolved original-key map.
func (sb *storageBatcher) drain() (map[string]map[string][]byte, map[string][]byte) {
	sb.mu.Lock()
	emptyPending := len(sb.pending) == 0
	emptyUnresolved := len(sb.unresolved) == 0
	if emptyPending && emptyUnresolved {
		sb.mu.Unlock()
		return nil, nil
	}
	batch := sb.pending
	unresolved := sb.unresolved
	sb.pending = make(map[string]map[string][]byte)
	sb.unresolved = make(map[string][]byte)
	sb.mu.Unlock()
	return batch, unresolved
}

func (db *PrefixDB) initStorageBatcher() {
	if db.storageBatch == nil {
		db.storageBatch = newStorageBatcher()
	}
}

func (db *PrefixDB) stopStorageBatcher() {
	if db.storageBatch == nil {
		return
	}
	db.storageBatch.reset()
	db.storageBatch = nil
}

// StorageBatchPut stages storage kvs in memory until BatchCommit.
func (db *PrefixDB) StorageBatchPut(key, value, accountKey []byte) error {
	if db.storageBatch == nil {
		return errors.New("storage batcher not initialized")
	}
	storageKey, err := db.normalizeStorageKey(key)
	if err != nil {
		return err
	}
	if len(accountKey) == 0 {
		// Defer resolution of the parent account key until BatchCommit.
		db.storageBatch.putUnresolved(string(key), value)
		return nil
	}
	// Note: We don't need to invalidate storageCache here because:
	// 1. batchGetOverlayNormalized is checked before storageCache in Get()
	// 2. syncStorageCacheEntries in BatchCommit will update the cache correctly
	db.storageBatch.put(string(accountKey), storageKey, value)
	return nil
}

// StorageBatchCommit persists all staged storage kvs and waits for storage GC completion.
func (db *PrefixDB) StorageBatchCommit() (err error) {
	if db.storageBatch == nil {
		return nil
	}
	if db.prefixTree != nil {
		db.prefixTree.beginGlobalCommit()
		defer func() {
			if endErr := db.prefixTree.endGlobalCommit(); err == nil {
				err = endErr
			}
		}()
	}
	batch, unresolved := db.storageBatch.drain()
	if len(batch) == 0 && len(unresolved) == 0 {
		return nil
	}
	if batch == nil {
		batch = make(map[string]map[string][]byte)
	}

	// Hold the write lock across the commit to serialize with regular Put/Delete.
	shouldWaitForStorageGC := false
	var plans []storageCommitPlan
	db.writeMutex.Lock()
	err = func() error {
		plans, err = db.prepareStorageCommitPlans(batch, unresolved, nil)
		if err != nil {
			return err
		}
		if err := db.appendPreparedInlineStorageSegments(plans); err != nil {
			return err
		}
		if len(plans) > 0 {
			shouldWaitForStorageGC = true
		}
		if err := db.applyStorageCommitPlans(plans, nil, true); err != nil {
			return err
		}
		return nil
	}()
	db.writeMutex.Unlock()
	if err != nil {
		return err
	}
	for _, plan := range plans {
		db.syncStorageCacheEntries([]byte(plan.accountKey), plan.cacheEntries)
	}
	if shouldWaitForStorageGC {
		return db.waitForStorageGCIdle()
	}
	return nil
}

func (db *PrefixDB) prepareAccountCommit(accountOps map[string]WriteOperation) (*preparedAccountCommit, error) {
	prepared := &preparedAccountCommit{
		entries: make(map[string][]byte, len(accountOps)),
	}
	if len(accountOps) == 0 {
		return prepared, nil
	}
	prepared.order = make([]string, 0, len(accountOps))
	for key, op := range accountOps {
		if op.modifiedType == None {
			continue
		}
		prepared.order = append(prepared.order, key)
	}
	sort.Strings(prepared.order)
	for _, key := range prepared.order {
		op := accountOps[key]
		if op.value == nil {
			continue
		}
		entry, err := db.ConvertKV([]byte(key), op.value)
		if err != nil {
			return nil, err
		}
		prepared.entries[key] = entry
		prepared.totalSize += len(entry)
	}
	return prepared, nil
}

func (db *PrefixDB) prepareStorageCommitPlans(batch map[string]map[string][]byte, unresolved map[string][]byte, accountOps map[string]WriteOperation) ([]storageCommitPlan, error) {
	if db.storageBatch == nil && len(batch) == 0 && len(unresolved) == 0 {
		return nil, nil
	}
	if batch == nil {
		batch = make(map[string]map[string][]byte)
	}
	if err := db.resolveUnresolvedStorageBatch(batch, unresolved); err != nil {
		return nil, err
	}
	if len(batch) == 0 {
		return nil, nil
	}

	accountKeys := make([]string, 0, len(batch))
	for accountKey := range batch {
		// if op, ok := accountOps[accountKey]; ok && op.value == nil {
		// 	continue
		// }
		accountKeys = append(accountKeys, accountKey)
	}
	if len(accountKeys) == 0 {
		return nil, nil
	}
	sort.Strings(accountKeys)

	plans := make([]storageCommitPlan, len(accountKeys))
	workerCount := db.storageCommitWorkerCount(len(accountKeys))
	if workerCount <= 1 {
		for idx, accountKey := range accountKeys {
			perAccount := batch[accountKey]
			prefixdbDebugf("BatchCommit: storage plan %d/%d account=%x keys=%d",
				idx+1, len(accountKeys), []byte(accountKey), len(perAccount))
			start := time.Now()
			plan, err := db.buildStorageCommitPlan(accountKey, perAccount)
			if err != nil {
				prefixdbDebugf("prepareStorageCommitPlans: buildStorageCommitPlan failed - accountKey=%x error=%v",
					[]byte(accountKey), err)
				return nil, err
			}
			prefixdbDebugf("BatchCommit: storage plan %d/%d done account=%x elapsed=%s",
				idx+1, len(accountKeys), []byte(accountKey), time.Since(start))
			plans[idx] = plan
		}
		return plans, nil
	}

	tasks := make(chan storageCommitTask, len(accountKeys))
	results := make(chan storageCommitResult, len(accountKeys))
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				perAccount := batch[task.accountKey]
				prefixdbDebugf("BatchCommit: storage plan %d/%d account=%x keys=%d",
					task.index+1, len(accountKeys), []byte(task.accountKey), len(perAccount))
				start := time.Now()
				plan, err := db.buildStorageCommitPlan(task.accountKey, perAccount)
				if err != nil {
					prefixdbDebugf("prepareStorageCommitPlans: buildStorageCommitPlan failed - accountKey=%x error=%v",
						[]byte(task.accountKey), err)
					results <- storageCommitResult{index: task.index, err: err}
					continue
				}
				prefixdbDebugf("BatchCommit: storage plan %d/%d done account=%x elapsed=%s",
					task.index+1, len(accountKeys), []byte(task.accountKey), time.Since(start))
				results <- storageCommitResult{index: task.index, plan: plan}
			}
		}()
	}
	for idx, accountKey := range accountKeys {
		tasks <- storageCommitTask{index: idx, accountKey: accountKey}
	}
	close(tasks)
	go func() {
		wg.Wait()
		close(results)
	}()
	var firstErr error
	for result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		plans[result.index] = result.plan
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return plans, nil
}

func (db *PrefixDB) storageCommitWorkerCount(taskCount int) int {
	workers := sanitizePrefixTreeGCWorkerCount(db.gcWorkers)
	if taskCount > 0 && workers > taskCount {
		workers = taskCount
	}
	if workers < 1 {
		workers = 1
	}
	return workers
}

func (db *PrefixDB) resolveUnresolvedStorageBatch(batch map[string]map[string][]byte, unresolved map[string][]byte) error {
	if len(unresolved) == 0 {
		return nil
	}
	unresolvedCount := 0
	unresolvedSamples := make([]string, 0, 3)
	for origKeyStr, v := range unresolved {
		origKeyBytes := []byte(origKeyStr)
		var accountKey []byte
		if db.ParentKeyResolver != nil {
			accountKey = db.ParentKeyResolver(origKeyBytes)
		}
		if accountKey == nil {
			unresolvedCount++
			if len(unresolvedSamples) < cap(unresolvedSamples) {
				unresolvedSamples = append(unresolvedSamples, fmt.Sprintf("%x", origKeyBytes))
			}
			prefixdbDebugf("prepareStorageCommitPlans: unresolved storage entry will be dropped - storageKey=%x valueLen=%d reason=ParentKeyResolver returned nil\n",
				origKeyBytes, len(v))
			continue
		}
		storageKey, err := db.normalizeStorageKey(origKeyBytes)
		if err != nil {
			return err
		}
		accStr := string(accountKey)
		perAcc := batch[accStr]
		if perAcc == nil {
			perAcc = make(map[string][]byte)
			batch[accStr] = perAcc
		}
		perAcc[string(storageKey)] = v
	}
	if unresolvedCount > 0 {
		return fmt.Errorf("prepareStorageCommitPlans: unresolved storage entries cannot be resolved: unresolved=%d total=%d sampleStorageKeys=%v (check ParentKeyResolver/account-hash index readiness)",
			unresolvedCount, len(unresolved), unresolvedSamples)
	}
	return nil
}

func (db *PrefixDB) buildStorageCommitPlan(accountKey string, perAccount map[string][]byte) (storageCommitPlan, error) {
	plan := storageCommitPlan{accountKey: accountKey}
	if db.testBuildStoragePlanHook != nil {
		db.testBuildStoragePlanHook(accountKey)
	}
	accountKeyBytes := []byte(accountKey)
	node, err := db.getNode(accountKeyBytes)
	if err != nil {
		return plan, err
	}
	var (
		existingFileID uint32
		existingOffset uint64
		existingSize   uint64
	)
	if node != nil {
		plan.accountOffset = node.accountOffset
		plan.accountSize = node.accountSize
		existingFileID = node.storageFileID
		existingOffset = node.storageOffset
		existingSize = node.storageSize
		plan.existingInfo = StorageInfo{
			storageFileID: existingFileID,
			storageOffset: existingOffset,
			storageSize:   existingSize,
		}
	}
	if len(perAccount) == 0 {
		fmt.Printf("buildStorageCommitPlan: no storage entries to write for account - accountKey=%s\n",
			accountKey)
		return plan, nil
	}
	kvs := make([]kvPair, 0, len(perAccount))
	for key, value := range perAccount {
		kvs = append(kvs, kvPair{key: []byte(key), val: value})
	}
	sortKVPairs(kvs)
	plan.cacheEntries = kvs
	info, inlineSegment, err := db.prepareStorageEntriesForCommit(accountKeyBytes, kvs, existingFileID, existingOffset, existingSize)
	if err != nil {
		return plan, err
	}
	plan.storageInfo = info
	plan.inlineSegment = inlineSegment
	if len(inlineSegment) == 0 {
		plan.skipNodeWrite = shouldSkipAccountEntryPointerUpdate(existingFileID, info.storageFileID, info.storageOffset, info.storageSize)
	}
	return plan, nil
}

func (db *PrefixDB) applyStorageCommitPlans(plans []storageCommitPlan, accountOps map[string]WriteOperation, updateAccountBatch bool) error {
	nodeEntries := make([]NodeInfo, 0, len(plans))
	cacheUpdates := make([]pendingNodeCacheUpdate, 0, len(plans))
	for _, plan := range plans {
		if accountOps != nil {
			if op, ok := accountOps[plan.accountKey]; ok {
				if op.value == nil {
					continue
				}
				continue
			}
		}
		if plan.skipNodeWrite {
			continue
		}
		nodeEntries = append(nodeEntries, NodeInfo{
			key:           []byte(plan.accountKey),
			accountOffset: plan.accountOffset,
			accountSize:   plan.accountSize,
			storageFileID: plan.storageInfo.storageFileID,
			storageOffset: plan.storageInfo.storageOffset,
			storageSize:   plan.storageInfo.storageSize,
		})
		cacheUpdates = append(cacheUpdates, pendingNodeCacheUpdate{
			key:           plan.accountKey,
			accountOffset: plan.accountOffset,
			accountSize:   plan.accountSize,
			storageInfo:   plan.storageInfo,
		})
		if updateAccountBatch && db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(plan.accountKey, plan.storageInfo)
		}
	}
	return db.applyNodeBatch(nodeEntries, cacheUpdates)
}

func (db *PrefixDB) commitStorageForAccount(accountKey string, kvs []kvPair) error {
	var (
		accOff         uint64
		accSize        uint32
		existingFileID uint32
		existingOffset uint64
		existingSize   uint64
	)

	accountKeyBytes := []byte(accountKey)
	node, err := db.getNode(accountKeyBytes)
	if err != nil {
		return err
	}
	if node != nil {
		accOff = node.accountOffset
		accSize = node.accountSize
		existingFileID = node.storageFileID
		existingOffset = node.storageOffset
		existingSize = node.storageSize
	}
	if len(kvs) == 0 {
		fmt.Printf("commitStorageForAccount: no storage kvs to write for account - accountKey=%s\n",
			accountKey)
		if err := db.prefixTree.Put(accountKeyBytes, accOff, accSize, 0, 0, 0); err != nil {
			return err
		}
		db.nodeCache.StoreMetadata(accountKey, accOff, accSize, StorageInfo{})
		if db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(accountKey, StorageInfo{})
		}
		return nil
	}

	fileID, off, sz, err := db.persistStorageEntries(accountKeyBytes, kvs, existingFileID, existingOffset, existingSize)
	if err != nil {
		return err
	}
	info := StorageInfo{
		storageFileID: fileID,
		storageOffset: off,
		storageSize:   sz,
	}
	skipAccountPointerUpdate := shouldSkipAccountEntryPointerUpdate(existingFileID, fileID, off, sz)
	if !skipAccountPointerUpdate {
		if err := db.prefixTree.Put(accountKeyBytes, accOff, accSize, fileID, off, sz); err != nil {
			return err
		}
		db.nodeCache.StoreMetadata(accountKey, accOff, accSize, info)
		if db.accountBatch != nil {
			_ = db.accountBatch.updateStoragePointer(accountKey, info)
		}
	}

	// cacheKeyHex := hex.EncodeToString([]byte(accountKey))
	// fmt.Println("store nodeCache:" + cacheKeyHex + ", fileID:" + fmt.Sprintf("%d", info.storageFileID) + ", offset:" + fmt.Sprintf("%d", info.storageOffset) + ", size:" + fmt.Sprintf("%d", info.storageSize))
	db.syncStorageCacheEntries(accountKeyBytes, kvs)
	return nil
}

// batchGetOverlay returns staged values for read-your-writes semantics.
func (db *PrefixDB) batchGetOverlayNormalized(storageKey, accountKey []byte) ([]byte, bool) {
	if db.storageBatch == nil || len(accountKey) == 0 {
		return nil, false
	}
	return db.storageBatch.get(string(accountKey), storageKey)
}

func (db *PrefixDB) waitForStorageGCIdle() error {
	for {
		if db.isStorageGCIdle() {
			return nil
		}
		time.Sleep(100 * time.Microsecond)
	}
}
