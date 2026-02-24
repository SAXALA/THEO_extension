package prefixdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// storageBatcher implements a storage-only, deferred commit pipeline:
//  1) BatchPut stores storage kvs in memory.
//  2) A background goroutine preloads segment indexes and prepares per-file append plans.
//  3) BatchCommit executes the prepared plan with parallel file writes.
//
// Notes:
// - This batcher is only for TrieStorage/TSSnapshot keys.
// - It is designed for the workload pattern described by the user (BatchPut -> background plan -> BatchCommit).

type storageBatcher struct {
	db *PrefixDB

	mu sync.Mutex
	// pending[accountKeyString][storageKeyString] = value (nil means delete)
	pending      map[string]map[string][]byte
	pendingCount int
	epoch        uint64
	// committing holds the batch currently being committed so Get/Has can still
	// observe staged values while pending has been swapped for new writes.
	committing      map[string]map[string][]byte
	committingEpoch uint64
	freePending     map[string]map[string][]byte
	freeKVMaps      []map[string][]byte

	planMu sync.Mutex
	plan   *storageBatchPlan

	oldAccountKey string // best-effort optimization to reduce planner wakeups for same-account bursts

	wakeCh chan struct{}
	stopCh chan struct{}
	wg     sync.WaitGroup
}

type storageBatchPlan struct {
	epoch uint64
	// per account plans (accountKey string -> plan)
	accounts map[string]*storageAccountPlan
}

type storageAccountPlan struct {
	accountKey string
	// sorted+deduped staged mutations for this account, used by fallback commit paths
	additions []kvPair

	// node pointer before commit
	existingFileID uint32
	existingOffset int64
	existingSize   uint64

	// planned segmented updates (existing segmented storage only)
	segmented *segmentedUpdatePlan

	// planned full rewrite (non-segmented to new segment/segmented)
	fullRewrite *fullRewritePlan
}

type fullRewriteKind uint8

const (
	fullRewriteEmpty fullRewriteKind = iota
	fullRewriteAppendSegment
	fullRewriteNewSegmented
)

type fullRewritePlan struct {
	kind fullRewriteKind

	// merged sorted kvs after applying additions (and tombstones)
	merged []kvPair

	// for fullRewriteAppendSegment
	segmentBytes []byte

	// for fullRewriteNewSegmented
	chunks []plannedChunk
	metas  []segmentChunkMeta
}

type plannedChunk struct {
	fileName string
	data     []byte
}

type segmentedUpdatePlan struct {
	folderID   uint32
	folderPath string
	// updated chunk metas after append operations
	updatedMetas []segmentChunkMeta
	// operations to apply to chunk files
	chunkOps []chunkAppendOp
}

type chunkAppendOp struct {
	path        string
	newKVCount  uint32
	appendAt    int64
	appendBytes []byte // serialized segment payload (without the 4-byte count header)
}

var errStaleSegmentPlan = errors.New("stale segmented append plan")

const maxStorageBatchKVMapPool = 256

func (db *PrefixDB) initStorageBatcher() {
	if db.storageBatch != nil {
		return
	}
	b := &storageBatcher{
		db:      db,
		pending: make(map[string]map[string][]byte),
		wakeCh:  make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}
	db.storageBatch = b
	b.wg.Add(1)
	go b.plannerLoop()
}

func (db *PrefixDB) stopStorageBatcher() {
	if db.storageBatch == nil {
		return
	}
	close(db.storageBatch.stopCh)
	db.storageBatch.wg.Wait()
	db.storageBatch = nil
}

func (b *storageBatcher) notifyPlanner() {
	select {
	case b.wakeCh <- struct{}{}:
	default:
	}
}

func (b *storageBatcher) acquirePendingMapLocked() map[string]map[string][]byte {
	if b.freePending != nil {
		m := b.freePending
		b.freePending = nil
		return m
	}
	return make(map[string]map[string][]byte)
}

func (b *storageBatcher) acquireKVMapLocked() map[string][]byte {
	n := len(b.freeKVMaps)
	if n == 0 {
		return make(map[string][]byte)
	}
	m := b.freeKVMaps[n-1]
	b.freeKVMaps = b.freeKVMaps[:n-1]
	return m
}

func (b *storageBatcher) recycleCommittedMapsLocked(committed map[string]map[string][]byte) {
	for acct, kvmap := range committed {
		delete(committed, acct)
		if len(b.freeKVMaps) >= maxStorageBatchKVMapPool {
			continue
		}
		for k := range kvmap {
			delete(kvmap, k)
		}
		b.freeKVMaps = append(b.freeKVMaps, kvmap)
	}
	if b.freePending == nil {
		b.freePending = committed
	}
}

func (b *storageBatcher) snapshotPending() (uint64, map[string][]kvPair) {
	b.mu.Lock()
	epoch := b.epoch
	if b.pendingCount == 0 {
		b.mu.Unlock()
		return epoch, nil
	}
	snapshot := make(map[string][]kvPair, len(b.pending))
	for acct, kvmap := range b.pending {
		pairs := make([]kvPair, 0, len(kvmap))
		for k, v := range kvmap {
			pairs = append(pairs, kvPair{key: stringToBytes(k), val: v})
		}
		snapshot[acct] = pairs
	}
	b.mu.Unlock()
	return epoch, snapshot
}

func (b *storageBatcher) plannerLoop() {
	defer b.wg.Done()
	for {
		select {
		case <-b.wakeCh:
			// Coalesce bursts.
			drain := true
			for drain {
				select {
				case <-b.wakeCh:
				default:
					drain = false
				}
			}
			for !b.db.isStorageGCIdle() {
				select {
				case <-b.stopCh:
					return
				default:
					time.Sleep(1 * time.Millisecond)
				}
			}
			epoch, snap := b.snapshotPending()
			if snap == nil {
				b.planMu.Lock()
				b.plan = nil
				b.planMu.Unlock()
				continue
			}
			plan := b.db.buildStorageBatchPlan(epoch, snap)
			// Publish only if epoch unchanged.
			b.mu.Lock()
			same := b.epoch == epoch
			b.mu.Unlock()
			if same {
				b.planMu.Lock()
				b.plan = plan
				b.planMu.Unlock()
			}
		case <-b.stopCh:
			return
		}
	}
}

func (db *PrefixDB) buildStorageBatchPlan(epoch uint64, snap map[string][]kvPair) *storageBatchPlan {
	plan := &storageBatchPlan{epoch: epoch, accounts: make(map[string]*storageAccountPlan, len(snap))}
	for acct, pairs := range snap {
		if len(pairs) == 0 {
			continue
		}
		sortKVPairs(pairs)
		pairs = dedupSortedKVPairs(pairs)
		accPlan := &storageAccountPlan{accountKey: acct, additions: pairs}

		node, err := db.getNode([]byte(acct))
		if err != nil {
			// leave empty; BatchCommit will retry with slow path
			accPlan.fullRewrite = &fullRewritePlan{merged: pairs}
			plan.accounts[acct] = accPlan
			continue
		}
		if node != nil {
			accPlan.existingFileID = node.storageFileID
			accPlan.existingOffset = node.storageOffset
			accPlan.existingSize = node.storageSize
		}

		if isSegmentedStorage(accPlan.existingFileID) {
			folderID := accPlan.existingFileID & ^segmentedStorageFlag
			folderPath := db.segmentedFolderPath(folderID)
			segPlan, err := db.planSegmentedAppend(folderID, folderPath, pairs)
			if err == nil {
				accPlan.segmented = segPlan
			} else {
				// Fallback: plan full rewrite (will execute via persistStorageEntries).
				accPlan.fullRewrite = &fullRewritePlan{kind: fullRewriteAppendSegment, merged: pairs}
			}
			plan.accounts[acct] = accPlan
			continue
		}
		rw, err := db.planFullRewriteNonSegmented(accPlan.existingFileID, accPlan.existingOffset, accPlan.existingSize, pairs)
		if err != nil {
			// Fallback: still commit using persistStorageEntries.
			accPlan.fullRewrite = &fullRewritePlan{kind: fullRewriteAppendSegment, merged: pairs}
		} else {
			accPlan.fullRewrite = rw
		}
		plan.accounts[acct] = accPlan
	}
	return plan
}

func (db *PrefixDB) planFullRewriteNonSegmented(existingFileID uint32, existingOffset int64, existingSize uint64, additions []kvPair) (*fullRewritePlan, error) {
	merged := additions
	if existingFileID != 0 && existingSize > 0 {
		existingEntries, backing, err := db.readStorageSegmentPairs(existingFileID, existingOffset, existingSize)
		if err != nil {
			return nil, err
		}
		// Deep copy because existingEntries may reference a pooled buffer.
		copied := make([]kvPair, len(existingEntries))
		for i, kv := range existingEntries {
			k := make([]byte, len(kv.key))
			copy(k, kv.key)
			v := make([]byte, len(kv.val))
			copy(v, kv.val)
			copied[i] = kvPair{key: k, val: v}
		}
		if backing != nil {
			backing.Release()
		}
		if len(copied) > 1 {
			sort.SliceStable(copied, func(i, j int) bool {
				return bytes.Compare(copied[i].key, copied[j].key) < 0
			})
		}
		merged = mergeAndDedupPairs(copied, additions)
	}
	if len(merged) == 0 {
		return &fullRewritePlan{kind: fullRewriteEmpty, merged: nil}, nil
	}
	sz := estimateSegmentSize(merged)
	if sz <= db.storageChunkSize {
		seg, release, _, err := db.serializeStorageSegment(merged)
		if err != nil {
			return nil, err
		}
		out := make([]byte, len(seg))
		copy(out, seg)
		release()
		return &fullRewritePlan{kind: fullRewriteAppendSegment, merged: merged, segmentBytes: out}, nil
	}
	// Plan new segmented folder contents (folderID allocated at commit time).
	chunks := make([]plannedChunk, 0)
	metas := make([]segmentChunkMeta, 0)
	chunk := make([]kvPair, 0)
	chunkSize := 4
	chunkIdx := 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		seg, release, _, err := db.serializeStorageSegment(chunk)
		if err != nil {
			return err
		}
		data := make([]byte, len(seg))
		copy(data, seg)
		release()
		name := fmt.Sprintf("chunk_%04d.dat", chunkIdx)
		metas = append(metas, segmentChunkMeta{
			FileName:  name,
			KeyStart:  cloneBytes(chunk[0].key),
			KeyEnd:    cloneBytes(chunk[len(chunk)-1].key),
			KVCount:   uint32(len(chunk)),
			ChunkSize: uint64(len(data)),
		})
		chunks = append(chunks, plannedChunk{fileName: name, data: data})
		chunk = make([]kvPair, 0)
		chunkSize = 4
		chunkIdx++
		return nil
	}
	for _, kv := range merged {
		s := 6 + len(kv.key) + len(kv.val)
		if chunkSize+s > db.storageChunkSize && len(chunk) > 0 {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		chunk = append(chunk, kv)
		chunkSize += s
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, errors.New("failed to build segmented storage chunks")
	}
	return &fullRewritePlan{kind: fullRewriteNewSegmented, merged: merged, chunks: chunks, metas: metas}, nil
}

func (db *PrefixDB) planSegmentedAppend(folderID uint32, folderPath string, additions []kvPair) (*segmentedUpdatePlan, error) {
	// Protect against concurrent GC/index rewrites while we read index + compute ops.
	db.segmentedMu.RLock()
	metas, err := db.readSegmentIndexNoCache(folderID)
	db.segmentedMu.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, fmt.Errorf("segment index missing for folder %d", folderID)
	}
	buckets := partitionEntriesByChunks(metas, additions)
	updated := make([]segmentChunkMeta, 0, len(metas)+len(additions)/64+1)
	ops := make([]chunkAppendOp, 0)
	for idx, meta := range metas {
		adds := buckets[idx]
		if len(adds) == 0 {
			updated = append(updated, meta)
			continue
		}
		appendBytes := payloadSize(adds)
		if appendBytes == 0 {
			updated = append(updated, meta)
			continue
		}
		seg, release, _, err := db.serializeStorageSegment(adds)
		if err != nil {
			return nil, err
		}
		payload := make([]byte, len(seg)-4)
		copy(payload, seg[4:])
		release()

		chunkPath := filepath.Join(folderPath, meta.FileName)
		metaCopy := meta
		metaCopy.KVCount += uint32(len(adds))
		metaCopy.ChunkSize += uint64(appendBytes)
		adjustMetaRange(&metaCopy, adds)
		updated = append(updated, metaCopy)

		ops = append(ops, chunkAppendOp{
			path:        chunkPath,
			newKVCount:  metaCopy.KVCount,
			appendAt:    int64(meta.ChunkSize),
			appendBytes: payload,
		})
	}
	return &segmentedUpdatePlan{
		folderID:     folderID,
		folderPath:   folderPath,
		updatedMetas: updated,
		chunkOps:     ops,
	}, nil
}

// BatchPut stages a storage kv in memory. Only TrieStorage/TSSnapshot keys are accepted.
// accountKey must be the parent account key (same as Put) for StateDB.
func (db *PrefixDB) BatchPut(key, value, accountKey []byte) error {
	if db.storageBatch == nil {
		return errors.New("storage batcher not initialized")
	}
	keyType, err := db.getKeyType(key)
	if err != nil {
		return err
	}
	if keyType != TrieStorage && keyType != TSSnapshot {
		return fmt.Errorf("BatchPut only supports storage keys, got %v", keyType)
	}
	storageKey, err := db.normalizeStorageKey(key, keyType)
	if err != nil {
		return err
	}

	// Normalize accountKey for snapshot DB, consistent with Put/Get.
	var acctStr string
	switch db.databaseType {
	case StateDB:
		if accountKey == nil {
			return errors.New("parent accountKey is required for StateDB storage BatchPut")
		}
		acctStr = string(accountKey)
	case SnapshotDB:
		var acc [32]byte
		copy(acc[:], key[1:33])
		acctStr = string(acc[:])
	default:
		return errors.New("unknown database type")
	}

	keyStr := string(storageKey)
	var valCopy []byte
	if value != nil {
		valCopy = make([]byte, len(value))
		copy(valCopy, value)
	}

	b := db.storageBatch
	b.mu.Lock()
	kvmap := b.pending[acctStr]
	if kvmap == nil {
		kvmap = b.acquireKVMapLocked()
		b.pending[acctStr] = kvmap
	}
	if _, exists := kvmap[keyStr]; !exists {
		b.pendingCount++
	}
	kvmap[keyStr] = valCopy
	b.epoch++
	b.mu.Unlock()

	// Avoid stale reads from storageCache (including cached misses).
	if db.storageCache != nil {
		db.storageCache.Remove(keyStr)
	}

	b.oldAccountKey = acctStr
	b.notifyPlanner()
	return nil
}

// BatchCommit persists all staged storage kvs.
func (db *PrefixDB) BatchCommit() error {
	if db.storageBatch == nil {
		return nil
	}
	b := db.storageBatch

	// Swap pending under lock so concurrent BatchPut won't be lost.
	b.mu.Lock()
	if b.pendingCount == 0 {
		b.mu.Unlock()
		return nil
	}
	commitEpoch := b.epoch
	pendingToCommit := b.pending
	nextPending := b.acquirePendingMapLocked()
	// Reset for new writes while we commit.
	b.pending = nextPending
	b.pendingCount = 0
	b.committing = pendingToCommit
	b.committingEpoch = commitEpoch
	b.epoch++
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		if b.committingEpoch == commitEpoch {
			b.committing = nil
			b.committingEpoch = 0
		}
		b.recycleCommittedMapsLocked(pendingToCommit)
		b.mu.Unlock()
	}()

	// Remove staged keys from cache to avoid stale hits/misses.
	if db.storageCache != nil {
		for _, kvmap := range pendingToCommit {
			for k := range kvmap {
				db.storageCache.Remove(k)
			}
		}
	}

	// Ensure we have a plan for commitEpoch; if background hasn't produced one yet, wait briefly.
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		b.planMu.Lock()
		p := b.plan
		b.planMu.Unlock()
		if p != nil && p.epoch == commitEpoch {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Build or reuse plan.
	var plan *storageBatchPlan
	b.planMu.Lock()
	if b.plan != nil && b.plan.epoch == commitEpoch {
		plan = b.plan
	}
	b.planMu.Unlock()
	if plan == nil {
		snap := make(map[string][]kvPair, len(pendingToCommit))
		for acct, kvmap := range pendingToCommit {
			pairs := make([]kvPair, 0, len(kvmap))
			for k, v := range kvmap {
				pairs = append(pairs, kvPair{key: stringToBytes(k), val: v})
			}
			snap[acct] = pairs
		}
		plan = db.buildStorageBatchPlan(commitEpoch, snap)
	}

	// Block other writers and flush any outstanding single-account buffer to keep ordering sane.
	db.writeMutex.Lock()
	defer db.writeMutex.Unlock()
	if err := db.flushStorageBuffer(); err != nil {
		return err
	}

	// Apply segmented plans with parallel chunk appends.
	needsSegmented := false
	for _, ap := range plan.accounts {
		if ap.segmented != nil {
			needsSegmented = true
			break
		}
		if ap.fullRewrite != nil && ap.fullRewrite.kind == fullRewriteNewSegmented {
			needsSegmented = true
			break
		}
	}
	if needsSegmented {
		db.segmentedMu.Lock()
		defer db.segmentedMu.Unlock()
	}

	for _, ap := range plan.accounts {
		if ap.segmented != nil {
			if err := db.applySegmentedUpdatePlan(ap); err != nil {
				if !errors.Is(err, errStaleSegmentPlan) {
					return err
				}
				// Plan became stale (typically GC/rewrites changed chunk file sizes).
				// Fallback to the canonical merge+persist path using staged additions.
				node, nerr := db.getNode([]byte(ap.accountKey))
				if nerr != nil {
					return nerr
				}
				var existingFileID uint32
				var existingOffset int64
				var existingSize uint64
				var accOff int64
				if node != nil {
					accOff = node.offset
					existingFileID = node.storageFileID
					existingOffset = node.storageOffset
					existingSize = node.storageSize
				}
				var (
					fileID uint32
					off    int64
					sz     uint64
					perr   error
				)
				if isSegmentedStorage(existingFileID) {
					pairs := dedupSortedKVPairs(ap.additions)
					fileID, off, sz, perr = db.updateSegmentedStorageWithLockHeld(existingFileID, pairs)
				} else {
					fileID, off, sz, perr = db.persistStorageEntries(ap.additions, existingFileID, existingOffset, existingSize)
				}
				if perr != nil {
					return perr
				}
				if perr = db.prefixTree.Put([]byte(ap.accountKey), accOff, fileID, off, sz); perr != nil {
					return perr
				}
				info := StorageInfo{storageFileID: fileID, storageOffset: off, storageSize: sz}
				db.nodeCache.UpdateStoragePointer(ap.accountKey, info)
				if db.batch != nil {
					_ = db.batch.updateStoragePointer(stringToBytes(ap.accountKey), info)
				}
			}
			db.invalidateStorageBuffer(ap.accountKey)
			continue
		}
		if ap.fullRewrite != nil {
			// Need existing pointer again in case it changed.
			node, err := db.getNode([]byte(ap.accountKey))
			if err != nil {
				return err
			}
			var existingFileID uint32
			var existingOffset int64
			var existingSize uint64
			var accOff int64
			if node != nil {
				accOff = node.offset
				existingFileID = node.storageFileID
				existingOffset = node.storageOffset
				existingSize = node.storageSize
			}

			switch ap.fullRewrite.kind {
			case fullRewriteEmpty:
				if err := db.prefixTree.Put([]byte(ap.accountKey), accOff, 0, 0, 0); err != nil {
					return err
				}
				db.nodeCache.UpdateStoragePointer(ap.accountKey, StorageInfo{})
				if db.batch != nil {
					_ = db.batch.updateStoragePointer(stringToBytes(ap.accountKey), StorageInfo{})
				}
				db.invalidateStorageBuffer(ap.accountKey)
			case fullRewriteAppendSegment:
				if ap.fullRewrite.segmentBytes != nil {
					need := int64(len(ap.fullRewrite.segmentBytes))
					if err := db.ensureStorageCapacity(need); err != nil {
						return err
					}
					offset := db.storageCurSize
					if _, err := db.storageCurFile.WriteAt(ap.fullRewrite.segmentBytes, offset); err != nil {
						return err
					}
					db.storageCurSize += need
					fileID := db.storageCurFileID
					if err := db.prefixTree.Put([]byte(ap.accountKey), accOff, fileID, offset, uint64(need)); err != nil {
						return err
					}
					info := StorageInfo{storageFileID: fileID, storageOffset: offset, storageSize: uint64(need)}
					db.nodeCache.UpdateStoragePointer(ap.accountKey, info)
					if db.batch != nil {
						_ = db.batch.updateStoragePointer(stringToBytes(ap.accountKey), info)
					}
					db.invalidateStorageBuffer(ap.accountKey)
				} else {
					// Fallback slow-path.
					fileID, off, sz, err := db.persistStorageEntries(ap.fullRewrite.merged, existingFileID, existingOffset, existingSize)
					if err != nil {
						return err
					}
					if err := db.prefixTree.Put([]byte(ap.accountKey), accOff, fileID, off, sz); err != nil {
						return err
					}
					info := StorageInfo{storageFileID: fileID, storageOffset: off, storageSize: sz}
					db.nodeCache.UpdateStoragePointer(ap.accountKey, info)
					if db.batch != nil {
						_ = db.batch.updateStoragePointer(stringToBytes(ap.accountKey), info)
					}
					db.invalidateStorageBuffer(ap.accountKey)
				}
			case fullRewriteNewSegmented:
				folderID := db.nextSegmentedDirID()
				folderPath := db.segmentedFolderPath(folderID)
				if err := os.MkdirAll(folderPath, 0755); err != nil {
					return err
				}
				writeAtomic := func(fullPath string, data []byte) error {
					tmpPath := fullPath + ".tmp"
					f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
					if err != nil {
						return err
					}
					if _, err := f.Write(data); err != nil {
						_ = f.Close()
						_ = os.Remove(tmpPath)
						return err
					}
					if err := f.Close(); err != nil {
						_ = os.Remove(tmpPath)
						return err
					}
					if err := os.Rename(tmpPath, fullPath); err != nil {
						_ = os.Remove(tmpPath)
						return err
					}
					return nil
				}

				workers := runtime.GOMAXPROCS(0)
				if workers < 2 {
					workers = 2
				}
				if workers > 16 {
					workers = 16
				}
				tasks := make(chan plannedChunk)
				var wg sync.WaitGroup
				var firstErr atomic.Value
				for i := 0; i < workers; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for c := range tasks {
							if firstErr.Load() != nil {
								continue
							}
							fullPath := filepath.Join(folderPath, c.fileName)
							if err := writeAtomic(fullPath, c.data); err != nil {
								firstErr.Store(err)
								continue
							}
						}
					}()
				}
				for _, c := range ap.fullRewrite.chunks {
					tasks <- c
				}
				close(tasks)
				wg.Wait()
				if err := firstErr.Load(); err != nil {
					return err.(error)
				}
				if err := db.writeSegmentIndex(folderPath, ap.fullRewrite.metas); err != nil {
					return err
				}
				db.invalidateSegmentIndexCache(folderID)
				fileID := segmentedStorageFlag | folderID
				if err := db.prefixTree.Put([]byte(ap.accountKey), accOff, fileID, 0, uint64(len(ap.fullRewrite.metas))); err != nil {
					return err
				}
				info := StorageInfo{storageFileID: fileID, storageOffset: 0, storageSize: uint64(len(ap.fullRewrite.metas))}
				db.nodeCache.UpdateStoragePointer(ap.accountKey, info)
				if db.batch != nil {
					_ = db.batch.updateStoragePointer(stringToBytes(ap.accountKey), info)
				}
				db.invalidateStorageBuffer(ap.accountKey)
			default:
				// Fallback slow-path.
				fileID, off, sz, err := db.persistStorageEntries(ap.fullRewrite.merged, existingFileID, existingOffset, existingSize)
				if err != nil {
					return err
				}
				if err := db.prefixTree.Put([]byte(ap.accountKey), accOff, fileID, off, sz); err != nil {
					return err
				}
				info := StorageInfo{storageFileID: fileID, storageOffset: off, storageSize: sz}
				db.nodeCache.UpdateStoragePointer(ap.accountKey, info)
				if db.batch != nil {
					_ = db.batch.updateStoragePointer(stringToBytes(ap.accountKey), info)
				}
				db.invalidateStorageBuffer(ap.accountKey)
			}
		}
	}

	// Clear only the committed plan epoch.
	b.planMu.Lock()
	if b.plan != nil && b.plan.epoch == commitEpoch {
		b.plan = nil
	}
	b.planMu.Unlock()

	if db.storageCache != nil {
		for _, kvmap := range pendingToCommit {
			for k, v := range kvmap {
				if v == nil {
					db.storageCache.Add(k, nil)
					continue
				}
				db.storageCache.Add(k, v)
			}
		}
	}
	return nil
}

func (db *PrefixDB) applySegmentedUpdatePlan(ap *storageAccountPlan) error {
	sp := ap.segmented
	if sp == nil {
		return nil
	}
	// Parallelize independent chunk file appends.
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		workers = 2
	}
	if workers > 16 {
		workers = 16
	}

	tasks := make(chan chunkAppendOp)
	var wg sync.WaitGroup
	var firstErr atomic.Value
	worker := func() {
		defer wg.Done()
		var header [4]byte
		for op := range tasks {
			if firstErr.Load() != nil {
				continue
			}
			st, statErr := os.Stat(op.path)
			if statErr != nil {
				firstErr.Store(statErr)
				continue
			}
			if st.Size() != op.appendAt {
				firstErr.Store(fmt.Errorf("%w: chunk size changed before append (%s): want=%d got=%d", errStaleSegmentPlan, op.path, op.appendAt, st.Size()))
				continue
			}
			f, err := os.OpenFile(op.path, os.O_RDWR, 0644)
			if err != nil {
				firstErr.Store(err)
				continue
			}
			writeUint32BE(header[:], op.newKVCount)
			if _, err := f.WriteAt(header[:], 0); err != nil {
				_ = f.Close()
				firstErr.Store(err)
				continue
			}
			if _, err := f.WriteAt(op.appendBytes, op.appendAt); err != nil {
				_ = f.Close()
				firstErr.Store(err)
				continue
			}
			_ = f.Close()
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}
	for _, op := range sp.chunkOps {
		// If chunk file is missing, fall back to slow path for this account.
		if _, err := os.Stat(op.path); err != nil {
			close(tasks)
			wg.Wait()
			return fmt.Errorf("%w: chunk missing during batch commit (%s): %v", errStaleSegmentPlan, op.path, err)
		}
		tasks <- op
	}
	close(tasks)
	wg.Wait()
	if err := firstErr.Load(); err != nil {
		return err.(error)
	}

	// Persist updated index (single write).
	if err := db.writeSegmentIndex(sp.folderPath, sp.updatedMetas); err != nil {
		return err
	}
	db.invalidateSegmentIndexCache(sp.folderID)

	// Account pointer for segmented storage remains the same fileID; size is chunk count.
	node, err := db.getNode([]byte(ap.accountKey))
	if err != nil {
		return err
	}
	var accOff int64
	if node != nil {
		accOff = node.offset
	}
	if err := db.prefixTree.Put([]byte(ap.accountKey), accOff, segmentedStorageFlag|sp.folderID, 0, uint64(len(sp.updatedMetas))); err != nil {
		return err
	}
	info := StorageInfo{storageFileID: segmentedStorageFlag | sp.folderID, storageOffset: 0, storageSize: uint64(len(sp.updatedMetas))}
	db.nodeCache.UpdateStoragePointer(ap.accountKey, info)
	if db.batch != nil {
		_ = db.batch.updateStoragePointer(stringToBytes(ap.accountKey), info)
	}
	db.invalidateStorageBuffer(ap.accountKey)
	return nil
}

// Optional: a best-effort read overlay for staged storage keys.
func (db *PrefixDB) batchGetOverlay(key []byte, accountKey []byte) ([]byte, bool) {
	if db.storageBatch == nil {
		return nil, false
	}
	keyType, err := db.getKeyType(key)
	if err != nil {
		return nil, false
	}
	if keyType != TrieStorage && keyType != TSSnapshot {
		return nil, false
	}
	storageKey, err := db.normalizeStorageKey(key, keyType)
	if err != nil {
		return nil, false
	}
	var acctStr string
	switch db.databaseType {
	case StateDB:
		if accountKey == nil {
			return nil, false
		}
		acctStr = bytesToString(accountKey)
	case SnapshotDB:
		var acc [32]byte
		copy(acc[:], key[1:33])
		acctStr = bytesToString(acc[:])
	}
	keyStr := bytesToString(storageKey)
	b := db.storageBatch
	b.mu.Lock()
	kvmap := b.pending[acctStr]
	if kvmap != nil {
		if val, ok := kvmap[keyStr]; ok {
			b.mu.Unlock()
			if val == nil {
				return nil, true
			}
			copyVal := make([]byte, len(val))
			copy(copyVal, val)
			return copyVal, true
		}
	}
	commitMap := b.committing[acctStr]
	if commitMap == nil {
		b.mu.Unlock()
		return nil, false
	}
	val, ok := commitMap[keyStr]
	b.mu.Unlock()
	if !ok {
		return nil, false
	}
	if val == nil {
		return nil, true
	}
	copyVal := make([]byte, len(val))
	copy(copyVal, val)
	return copyVal, true
}
