# EthStore File Handle Optimization Log

Date: 2026-03-09
Scope: `standalone/ethstore` (`blockstore` + `prefixdb`)

## 1. File I/O Hotspot Audit

### blockstore (`datablockStore.go`)

Identified high-frequency open/close paths before optimization:
- `findBlockIndexEntryOnDisk`: opened `blockindex.map` on every lookup (`os.OpenFile` + `defer Close`).
- `flushIndexBufferWithBlockID`: opened `blockindex.map` on every flush batch (`os.OpenFile` + `defer Close`).
- `releaseBootstrapResources`: closed `indexMapFile` after bootstrap, forcing subsequent disk lookups/flushes to re-open file handles.

### prefixdb (`prefixdb.go`, `cache.go`)

Identified high-frequency open/close paths before optimization:
- `readSegmentFileBuffer`: opened segment chunk files (`chunk_*.dat`) for each read.
- `readStorageSegmentFile`: opened storage data files for each read.
- `readStorageSegmentPayload`: opened storage data files for each payload read.
- `GetStorageCount`: opened storage data files per query.

## 2. Block Store Lifetime Handle Policy

### Goal
Keep `index` and `data` files opened after startup and close only on process/database shutdown.

### Implemented Changes
- File: `standalone/ethstore/datablockStore.go`
- `releaseBootstrapResources`:
  - Removed bootstrap-time close of `indexMapFile`.
- `findBlockIndexEntryOnDisk`:
  - Switched from per-call `os.OpenFile` to reusing persistent `baol.indexMapFile`.
- `flushIndexBufferWithBlockID`:
  - Switched from per-flush `os.OpenFile` to persistent `baol.indexMapFile` + `WriteAt` + `Sync`.
- Shutdown behavior remains in `Close()`:
  - `dataFile` and `indexMapFile` are `Sync` + `Close` only during close.

Result: index/data file handles stay open for lifecycle, removing repeated open/close overhead on hot paths.

## 3. PrefixDB Global File Handle Cache

### Goal
Introduce a process-global file handle cache. Cached files stay open until LRU eviction. Default cache size = half of system file-descriptor limit.

### Implemented Changes
- New file: `standalone/ethstore/prefixdb/file_handle_cache.go`
- Added global cache with these properties:
  - Global singleton (`sync.Once`) shared by PrefixDB instances.
  - LRU policy (`hashicorp/golang-lru`).
  - Cached handle eviction closes file descriptors.
  - Default capacity:
    - Read `RLIMIT_NOFILE` via `syscall.Getrlimit`.
    - Use `limit/2`.
    - Apply safety clamp to `[128, 32768]` handles.

### PrefixDB Integration
- File: `standalone/ethstore/prefixdb/prefixdb.go`
- Added field: `fileHandleCache *fileHandleCache` to `PrefixDB`.
- Initialize in constructor (`NewPrefixDBWithCacheSettings`) via global cache.
- Added helper: `openCachedReadOnlyFile(path string)`.
- Rewired read hot paths to use cached handles:
  - `readSegmentFileBuffer`
  - `readStorageSegmentFile`
  - `readStorageSegmentPayload`
  - `GetStorageCount`

Result: repeated reads of segment/storage files now reuse open handles and avoid frequent `open/close` syscalls.

## 4. PrefixDB Memory Layout (CPU Cache Friendliness)

### Goal
Reduce pointer chasing and improve spatial locality for frequently accessed storage chunk metadata.

### Implemented Changes
- File: `standalone/ethstore/prefixdb/cache.go`
- `storageChunkBuffer.chunks`:
  - Changed from `[]*storageChunkEntry` to contiguous `[]storageChunkEntry`.
- Updated related logic to use indexed address-of access (`&b.chunks[i]`) where mutation is required:
  - `reset`
  - `adopt`
  - `evictIfNeeded`
  - `lookup`

Why this helps:
- Fewer heap allocations for chunk metadata entries.
- Better cache locality while scanning chunks (`lookup`, eviction path).
- Reduced pointer indirection on hot read paths.

## 5. Validation

Executed:
- `cd standalone/ethstore && go test ./...`

Observed:
- `prefixdb` tests pass.
- `pebblestore` tests pass.
- Top-level `ethstore` package currently fails to build due to pre-existing API signature mismatch in tests (`Database.Get` expected args mismatch), unrelated to this optimization.

## 6. Changed Files

- `standalone/ethstore/datablockStore.go`
- `standalone/ethstore/prefixdb/file_handle_cache.go` (new)
- `standalone/ethstore/prefixdb/prefixdb.go`
- `standalone/ethstore/prefixdb/cache.go`

## 7. Notes

- Workspace already contains unrelated local changes in:
  - `standalone/ethstore/workload/multiple_replay.sh`
  - `standalone/ethstore/workload/replay.sh`
- These unrelated changes were not modified by this optimization task.
