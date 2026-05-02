# THEO: A Protocol-Transparent Hybrid Storage System for Ethereum Nodes

## Overview

Ethereum execution clients (Geth) persist all blockchain data as key-value (KV) pairs in PebbleDB, a monolithic LSM-tree-based store. However, different KV classes have fundamentally different access patterns:

- **World-state trie classes** (`TrieNodeAccount`, `TrieNodeStorage`) are dominated by *point lookups* with intra-account spatial locality — they are never scanned globally.
- **Block-data classes** (`BlockHeader`, `BlockBody`, `BlockReceipts`) are *append-only* in writes and exhibit tiered temporal read locality — multi-level compaction is pure overhead.

Forcing these heterogeneous KV classes into the same LSM-tree structure causes unnecessary write amplification, read amplification, and compaction overhead, especially as node resources tighten.

**THEO** is a protocol-**t**ransparent **h**ybrid storage system for **E**thereum n**o**des that addresses this KV-class heterogeneity by routing each class to a structurally appropriate backend while preserving Geth's KV interface, authenticated data structures (ADSs), and block-level crash consistency.

### Key Results

| Workload | vs. PebbleDB | vs. ChainKV |
| --- | --- | --- |
| CacheTrace (resource-rich) | +22.6% throughput | +23.8% throughput |
| NocacheTrace (resource-constrained) | +42.1% throughput | +2.1% throughput |
| BareTrace (extremely constrained) | +55.5% throughput | +29.3% throughput |

Under the extremely resource-constrained setting, THEO is only **16.6% slower** than PebbleDB under the default resource-rich setting while using **14.2% less on-disk storage**.

## Requirements

### Software

- Ubuntu 24.04 LTS, Linux kernel 6.17+
- **Go 1.24.2+**
- Python 3.8+ (for analysis scripts)
- `rsync`, `fstrim` (for experiment automation)
- Optional: `hioadm` (Huawei SSDs) or `nvme-cli` (Intel DC SSDs) for NAND-level I/O stats

### Ethereum block synchronization traces

Traces are captured as in [ren25]: `CacheTrace`, `NocacheTrace`, and `BareTrace` starting at block 20,500,000.

[ren25]: Ren Y, Zhao J, Li J, et al. An Analysis of Ethereum Workloads from a Key-Value Storage Perspective. 2025 IEEE International Symposium on Workload Characterization (IISWC). IEEE, 2025: 394-406.

---

## Quick Start

### 1. Clone and set up the workspace

```bash
git clone <repo-url> THEO
cd THEO
go work sync
```

### 2. Build the replay binary

```bash
cd replayWorkload
go build -trimpath -ldflags="-s -w" -o ./bin/replayWorkload ./replayWorkload.go
```

### 3. Configure paths

Edit `replayWorkload/replay_config.json` to point to your trace files and target directories:

```json
{
  "loadDataDir":     "/path/to/20500000_key_value_pairs.txt",
  "aolDataFile":     "/path/to/aol/print_all_output.txt",
  "traceFile":       "/path/to/geth-trace-withcache-...",
  "traceFileNocache": "/path/to/geth-trace-without-cache-...",
  "traceFileNoCacheWithSnapshot": "/path/to/geth-trace-no-cache-enable-snapshot",
  "theoDir":         "/path/to/running/theo",
  "pebbleDir":       "/path/to/running/pebble",
  "chainKVDir":      "/path/to/running/chainkv",
  "loadedTheoDir":   "/path/to/loaded/theo",
  "loadedPebbleDir": "/path/to/loaded/pebble",
  "loadedChainKVDir": "/path/to/loaded/chainkv"
}
```

### 4. Required before running scripts: configure sudo password

Before running evaluation scripts, make sure `SUDO_PASSWD` is configured for your environment. Or manually modified in the two scripts: `replayWorkload/replay.sh` or `replayWorkload/monitor.sh`

```bash
SUDO_PASSWD="${SUDO_PASSWD:-admin}"
```

### 5. Load data (one-time setup)

```bash
cd replayWorkload

# Load THEO block store
./exps/loadBlockStore.sh

# Load THEO state store (account phase, then storage phase)
./exps/loadStateStore.sh

# Load PebbleDB and ChainKV baselines (with snapshot)
./exps/loadLSMWithSnapshot.sh all

# Load PebbleDB and ChainKV baselines (without snapshot, for BareTrace)
./exps/loadLSMWithoutSnapshot.sh all
```

### 6. Run a single experiment

```bash
cd replayWorkload

# Replay with THEO on CacheTrace
TRACE_FILE=cache ./replay.sh replay theo

# Replay with PebbleDB
TRACE_FILE=cache ./replay.sh replay pebble

# Replay with ChainKV
TRACE_FILE=cache ./replay.sh replay chainkv
```

### 7. Manually recover from a crash

```bash
cd replayWorkload
# Recover THEO
./exps/recoverTheo.sh
```

---

## Running the Full Evaluation (Paper Experiments)

All paper experiments (Exp#1–#9) can be reproduced with the scripts in `replayWorkload/exps/`.

### Exp#1–#8: Main evaluation (all backends × all traces, 50K blocks)

```bash
cd replayWorkload
TEST_RUN_ROUNDS=5 ./exps/all_all_50K.sh
```

This runs 5 rounds of each (THEO / PebbleDB / ChainKV) × (CacheTrace / NocacheTrace / BareTrace) combination.

```bash
cd replayWorkload
./exps/recoverTheo.sh
```

This tests the crash recovery procedure on THEO.

### Sub-backend ablation (Appendix)

```bash
# Block-store contribution
TEST_RUN_ROUNDS=5 ./exps/all_block_10K.sh

# State-store contribution
TEST_RUN_ROUNDS=5 ./exps/all_state_10K.sh
```

### Parameter sensitivity (Appendix)

```bash
# State cache size sensitivity
TEST_RUN_ROUNDS=3 ./exps/theo_state_cache_size_10K.sh

# Contract log size sensitivity
TEST_RUN_ROUNDS=3 ./exps/theo_state_log_size_10K.sh
```

### Analyzing results

```bash
cd replayWorkload/toolScript

# Parse replay logs into throughput/latency tables
python3 replay_metrics.py ../replayLog/

# Extract Go GC stats
python3 go_gc_stats.py ../replayLog/

# Sub-backend breakdown
python3 breakdown_metrics.py ../replayLog/
```

---

## Troubleshooting

### `replay.sh` exits with "Cannot derive mount point from RUNNING_ROOT"

- Ensure `RUNNING_ROOT` points to an existing, mounted filesystem. Use `findmnt` or `df` to verify.

### `Cannot resolve block device from SSD_TARGET`

- Pass an explicit block device: `SSD_TARGET=/dev/nvme0n1p1 ./replay.sh ...`
- Or set `IDLE_OBSERVE_ENABLED=false` to skip the idle observation step.

### Build fails with Go module errors

- Run `go work sync` from the repository root to resolve workspace dependencies.
- Check `GOPROXY`: the scripts default to `https://goproxy.cn,direct`; change to `https://proxy.golang.org,direct` if outside China.

### Experiment results vary significantly across rounds

- Increase `TEST_RUN_ROUNDS` (e.g., `TEST_RUN_ROUNDS=10`).
- Ensure `IDLE_OBSERVE_ENABLED=true` so disk I/O settles between runs.
- Verify that no other processes are competing for SSD bandwidth.

### NAND-level I/O stats show "unavailable"

- NAND counters are only supported on Huawei SSDs (`hioadm`) and Intel DC SSDs (`nvme intel smart-log-add`). On other hardware, filesystem-level I/O stats are still collected from `/proc/[pid]/io`.

---

## License

See [LICENSE](LICENSE).
