#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir" || exit 1

set -Eeuo pipefail

# 可执行动作: load(加载数据) | load-account(prefixdb 仅加载 account) | load-storage(prefixdb 仅加载 storage) | restore(恢复数据库目录) | replay(回放trace) | gc(手动触发ethstore state db GC) | upgrade-index(升级 segment index 文件)
ACTIONS="load load-account load-storage restore replay gc upgrade-index"
# 后端类型: ethstore | chainkv | pebble | prefixdb | all(依次执行前三者)
BACKENDS="ethstore chainkv pebble prefixdb all"

# 位置参数1: ACTION，可选值见 ACTIONS，默认 replay
ACTION="${1:-replay}"
# 位置参数2: BACKEND，可选值见 BACKENDS，默认 ethstore
BACKEND="${2:-${WORKLOAD_BACKEND:-ethstore}}"
if [ "$ACTION" = "load-account" ] || [ "$ACTION" = "load-storage" ]; then
    BACKEND="${2:-prefixdb}"
fi

# 回放最大操作数；0 代表不限制
WORKLOAD_MAX_OPS="${WORKLOAD_MAX_OPS:-0}"
# 回放 block 窗口；0 代表不限制（起点从头/终点不限）
START_BLOCK_ID="${START_BLOCK_ID:-20500000}"
END_BLOCK_ID="${END_BLOCK_ID:-20550000}"
# 每累计处理多少个 block 才 commit 一次；默认每 1 个 block commit 一次
COMMIT_BLOCK_INTERVAL="${COMMIT_BLOCK_INTERVAL:-1}"
# trace 文件类型，可选值: cache | nocache | nocache_snap
TRACE_FILE="${TRACE_FILE:-nocache_snap}"
# 仅对 ethstore/pebble 回放生效；可选值: all | aol | prefixdb | pebble
DB_TYPE="${DB_TYPE:-all}"
# ethstore 回放参数，storage chunk cache 数量
CACHE_COUNT="${CACHE_COUNT:-32}"
# PrefixTree node file GC 阈值：当 unsorted/sorted 达到该比例时触发 GC
NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD="${NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD:-0.2}"
# segmented storage GC 阈值：当 CHUNK_FILE_SIZE_BYTES >= target_chunk_size * threshold 时触发 GC
STORAGE_GC_THRESHOLD="${STORAGE_GC_THRESHOLD:-2}"
# node file sorted part 是否启用 zstd 压缩；默认开启
NODE_FILE_SORTED_COMPRESSION="${NODE_FILE_SORTED_COMPRESSION:-false}"
# segment index 是否启用 zstd 压缩；默认开启
SEGMENT_INDEX_COMPRESSION="${SEGMENT_INDEX_COMPRESSION:-false}"
# 统一 GC worker 数；默认使用系统 CPU 数量的一半，最少 1
DEFAULT_GC_WORKERS=$(($(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 1) / 2))
if [ "$DEFAULT_GC_WORKERS" -lt 1 ]; then
    DEFAULT_GC_WORKERS=1
fi
GC_WORKERS="${GC_WORKERS:-$DEFAULT_GC_WORKERS}"

# ethstore load 参数: chunk 文件大小（KiB），如 4096/8192/16384
CHUNK_FILE_SIZE_BYTES="${CHUNK_FILE_SIZE_BYTES:-8192}"
# ethstore 参数: PrefixDB 总缓存大小（MiB），所有 cache 共用
TOTAL_CACHE_SIZE_MIB="${TOTAL_CACHE_SIZE_MIB:-512}"

# Keep byte conversions for logging only; Go now receives MiB and converts internally.
TOTAL_CACHE_SIZE_BYTES=$((TOTAL_CACHE_SIZE_MIB * 1024 * 1024))

# chainkv 参数: cache 大小（MB）
CHAINKV_CACHE_MB="${CHAINKV_CACHE_MB:-16}"
# pebble 参数: cache 大小（MB）
PEBBLE_CACHE_MB="${PEBBLE_CACHE_MB:-16}"
# pebble 参数: handles 数量
PEBBLE_HANDLES="${PEBBLE_HANDLES:-32768}"
# prefixdb 参数: file handle cache 数量
PREFIXDB_HANDLES="${PREFIXDB_HANDLES:-32768}"
# chainkv 参数: leveldb handles 数量
CHAINKV_HANDLES="${CHAINKV_HANDLES:-32768}"
# chainkv 参数: true/false，是否启用 state 特化路径（Put_s/Get_s）
CHAINKV_STATE="${CHAINKV_STATE:-true}"
# chainkv load 限制条数；0 代表不限制
CHAINKV_LOAD_LIMIT="${CHAINKV_LOAD_LIMIT:-0}"
# 多轮回放参数（由 multiple_replay.sh 注入）
RUN_ROUND="${RUN_ROUND:-0}"
RUN_ROUNDS="${RUN_ROUNDS:-0}"

resolve_mount_point_from_path() {
    local path="$1"
    local candidate="$path"
    local mount_point=""

    if [ -z "$candidate" ]; then
        return 1
    fi

    while [ ! -e "$candidate" ] && [ "$candidate" != "/" ]; do
        candidate=$(dirname "$candidate")
    done

    mount_point=$(findmnt -n -o TARGET --target "$candidate" 2>/dev/null || true)
    if [ -z "$mount_point" ]; then
        mount_point=$(df --output=target "$candidate" 2>/dev/null | tail -n 1 | awk '{$1=$1; print}')
    fi

    if [ -z "$mount_point" ]; then
        return 1
    fi

    printf "%s" "$mount_point"
}

# 已加载数据根目录（source）与回放运行目录（target）。
# 默认把 loaded 放到另一块 SSD，给 /data 上的 running 目录腾出更多可用空间。
LOADED_ROOT="${LOADED_ROOT:-/mnt/ssd2/loaded}"
RUNNING_ROOT="${RUNNING_ROOT:-/data/running}"
DISK_MOUNT_POINT=$(resolve_mount_point_from_path "$RUNNING_ROOT")

# ethstore statedb 目录名，可选: database_statedb4KB | database_statedb8KB | database_statedb16KB | database_statedb64KB | database_statedb256KB
calculate_default_ethstore_statedb_dirname() {
    case "$CHUNK_FILE_SIZE_BYTES" in
        4096) echo "database_statedb4KB" ;;
        8192) echo "database_statedb8KB" ;; # database_statedb8KB_0326_compressed
        16384) echo "database_statedb16KB" ;;
        65536) echo "database_statedb64KB" ;;
        262144) echo "database_statedb256KB" ;;
        *) echo "Invalid CHUNK_FILE_SIZE_BYTES=${CHUNK_FILE_SIZE_BYTES}. Supported values: 4096, 8192, 16384, 65536, 262144" >&2; exit 1 ;;
    esac
}
ETHSTORE_STATEDB_DIRNAME="${ETHSTORE_STATEDB_DIRNAME:-$(calculate_default_ethstore_statedb_dirname)}"

# 手动 GC 目录：直接在该 statedb 目录执行，不进行复制
GC_STATE_DIR="${GC_STATE_DIR:-${LOADED_ROOT}/ethstore/${ETHSTORE_STATEDB_DIRNAME}}"
# segment index 升级目录：直接在该 statedb 目录执行，不进行复制
UPGRADE_STATE_DIR="${UPGRADE_STATE_DIR:-${GC_STATE_DIR}}"
# prefixdb storage 阶段要求给出已经 load 完 account 的 statedb 目录
PREFIXDB_ACCOUNT_STATE_DIR="${PREFIXDB_ACCOUNT_STATE_DIR:-}"
# ethstore prefixdb replay 可选专用 Pebble 源目录；仅在特定实验需要隔离 accountHash->accountKey 索引时使用
ETHSTORE_PREFIXDB_PEBBLE_SOURCE_DIR="${ETHSTORE_PREFIXDB_PEBBLE_SOURCE_DIR:-}"

# ethstore prefixdb 目录（用于权限预检查）
ETHSTORE_PREFIXDB_DIR="${ETHSTORE_PREFIXDB_DIR:-${RUNNING_ROOT}/ethstore_state/prefixdb}"

log_date=$(date +%m-%d-%H-%M-%S)
log_dir="./replayLog"
mkdir -p "$log_dir"

# Track running child processes so signal handlers can stop them cleanly.
CURRENT_REPLAY_PID=""
CURRENT_MONITOR_PID=""

# sudo 密码；留空则使用交互式 sudo
SUDO_PASSWD="${SUDO_PASSWD:-qwe123}"
# rsync 3.2.x 默认可能受 1GB max-alloc 限制影响；0 表示不额外限制。
RSYNC_MAX_ALLOC="${RSYNC_MAX_ALLOC:-0}"
# replay 前清理后的空闲观察配置。
IDLE_OBSERVE_ENABLED="${IDLE_OBSERVE_ENABLED:-true}"
IDLE_OBSERVE_INTERVAL_SECONDS="${IDLE_OBSERVE_INTERVAL_SECONDS:-5}"
IDLE_OBSERVE_MAX_SECONDS="${IDLE_OBSERVE_MAX_SECONDS:-120}"
IDLE_OBSERVE_STABLE_WINDOWS="${IDLE_OBSERVE_STABLE_WINDOWS:-3}"
IDLE_OBSERVE_MAX_DIRTY_KB="${IDLE_OBSERVE_MAX_DIRTY_KB:-1024}"

# replay 进程可选 cgroup v2 IO 限速；默认关闭。
# https://www.samsung.com.cn/memory-storage/nvme-ssd/870-evo-4tb-sata-3-2-5-ssd-mz-77e4t0bw/
REPLAY_CGROUP_IO_LIMIT_ENABLED="${REPLAY_CGROUP_IO_LIMIT_ENABLED:-false}"
REPLAY_CGROUP_NAME_PREFIX="${REPLAY_CGROUP_NAME_PREFIX:-theo-replay}"
REPLAY_CGROUP_READ_IOPS_LIMIT="${REPLAY_CGROUP_READ_IOPS_LIMIT:-98000}" # NVME 850K, SATA 98K
REPLAY_CGROUP_WRITE_IOPS_LIMIT="${REPLAY_CGROUP_WRITE_IOPS_LIMIT:-88000}" # NVME 110K, SATA 88K
REPLAY_CGROUP_READ_MBPS_LIMIT="${REPLAY_CGROUP_READ_MBPS_LIMIT:-560}" # NVME 3700MB/s, SATA 560MB/s
REPLAY_CGROUP_WRITE_MBPS_LIMIT="${REPLAY_CGROUP_WRITE_MBPS_LIMIT:-530}" # NVME 2400MB/s, SATA 530MB/s
REPLAY_CGROUP_READ_BPS_LIMIT="${REPLAY_CGROUP_READ_BPS_LIMIT:-$((REPLAY_CGROUP_READ_MBPS_LIMIT * 1000 * 1000))}"
REPLAY_CGROUP_WRITE_BPS_LIMIT="${REPLAY_CGROUP_WRITE_BPS_LIMIT:-$((REPLAY_CGROUP_WRITE_MBPS_LIMIT * 1000 * 1000))}"

sudo_run() {
    if [ -n "${SUDO_PASSWD}" ]; then
        echo "${SUDO_PASSWD}" | sudo -S "$@"
    else
        sudo "$@"
    fi
}

report_error() {
    local exit_code="$1"
    local line_no="$2"
    local cmd="$3"
    echo "replay.sh failed at line ${line_no}: ${cmd} (exit=${exit_code})" >&2
}

trap 'report_error "$?" "$LINENO" "$BASH_COMMAND"' ERR

ACTIVE_REPLAY_CGROUP_PATH=""
ACTIVE_REPLAY_CGROUP_MAJMIN=""

sudo_rsync_run() {
    local args=()
    local has_max_alloc=0
    local arg
    for arg in "$@"; do
        if [[ "$arg" == --max-alloc=* ]]; then
            has_max_alloc=1
            break
        fi
    done
    if [ "$has_max_alloc" -eq 0 ] && [ -n "$RSYNC_MAX_ALLOC" ]; then
        args+=("--max-alloc=${RSYNC_MAX_ALLOC}")
    fi
    args+=("$@")

    set +e
    sudo_run rsync "${args[@]}"
    local rc=$?
    set -e
    if [ "$rc" -eq 24 ]; then
        echo "rsync exited with code 24 (source files vanished during transfer); continuing" >&2
        return 0
    fi
    return "$rc"
}

terminate_pid_tree() {
    local pid="$1"
    if [ -z "$pid" ]; then
        return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
        return 0
    fi

    # Recursively stop descendants first, then stop the process itself.
    local children
    children=$(pgrep -P "$pid" 2>/dev/null || true)
    if [ -n "$children" ]; then
        local child
        for child in $children; do
            terminate_pid_tree "$child"
        done
    fi

    kill -TERM "$pid" 2>/dev/null || true
    sleep 0.2
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null || true
    fi
}

cleanup_running_processes() {
    local monitor_pid="$CURRENT_MONITOR_PID"
    local replay_pid="$CURRENT_REPLAY_PID"

    terminate_pid_tree "$monitor_pid"
    terminate_pid_tree "$replay_pid"

    if [ -n "$monitor_pid" ]; then
        wait "$monitor_pid" 2>/dev/null || true
    fi
    if [ -n "$replay_pid" ]; then
        wait "$replay_pid" 2>/dev/null || true
    fi

    CURRENT_MONITOR_PID=""
    CURRENT_REPLAY_PID=""

    cleanup_replay_cgroup
}

handle_interrupt() {
    echo "Interrupted, stopping running replay/monitor processes..."
    cleanup_running_processes
    exit 130
}

trap 'handle_interrupt' INT TERM
trap 'cleanup_running_processes' EXIT

export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export GOSUMDB="${GOSUMDB:-sum.golang.google.cn}"

usage() {
    cat <<EOF
Usage: $0 [load|load-account|load-storage|restore|replay|gc|upgrade-index] [ethstore|chainkv|pebble|prefixdb|all]

Current values:
  action=${ACTION}
  backend=${BACKEND}

Common env vars:
    WORKLOAD_MAX_OPS(0=unlimited)
    START_BLOCK_ID(0=from beginning)
    END_BLOCK_ID(0=no block-id stop)
    TRACE_FILE(cache|nocache|nocache_snap)
    DB_TYPE(all|aol|prefixdb|pebble)
    CACHE_COUNT
    NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD GC_WORKERS STORAGE_GC_THRESHOLD
    NODE_FILE_SORTED_COMPRESSION SEGMENT_INDEX_COMPRESSION
    CHUNK_FILE_SIZE_BYTES(bytes), TOTAL_CACHE_SIZE_MIB(MiB)
    CHAINKV_CACHE_MB, PEBBLE_CACHE_MB, CHAINKV_HANDLES, PEBBLE_HANDLES, PREFIXDB_HANDLES
    CHAINKV_STATE(true|false), CHAINKV_LOAD_LIMIT(0=unlimited)
    LOADED_ROOT RUNNING_ROOT ETHSTORE_STATEDB_DIRNAME
    GC_STATE_DIR
    UPGRADE_STATE_DIR
    ETHSTORE_PREFIXDB_DIR SUDO_PASSWD
    IDLE_OBSERVE_ENABLED IDLE_OBSERVE_INTERVAL_SECONDS IDLE_OBSERVE_MAX_SECONDS
    IDLE_OBSERVE_STABLE_WINDOWS IDLE_OBSERVE_MAX_DIRTY_KB
    REPLAY_CGROUP_IO_LIMIT_ENABLED(true|false)
    REPLAY_CGROUP_NAME_PREFIX
    REPLAY_CGROUP_READ_IOPS_LIMIT REPLAY_CGROUP_WRITE_IOPS_LIMIT
    REPLAY_CGROUP_READ_MBPS_LIMIT REPLAY_CGROUP_WRITE_MBPS_LIMIT
    REPLAY_CGROUP_READ_BPS_LIMIT REPLAY_CGROUP_WRITE_BPS_LIMIT

Required by action/backend:
    restore: uses LOADED_ROOT as source and RUNNING_ROOT as target
    load/replay ethstore: ETHSTORE_PREFIXDB_DIR (default: RUNNING_ROOT/ethstore_state)
    load-account prefixdb: uses replay_config.json 中 loadedEthStoreDir/loadDataDir
    load-storage prefixdb: uses replay_config.json 中 loadDataDir/accountHashKeyPebbleDir，并要求 PREFIXDB_ACCOUNT_STATE_DIR
    gc: backend must be ethstore, and GC_STATE_DIR must be provided
    upgrade-index: backend must be ethstore, and UPGRADE_STATE_DIR must be provided
EOF
}

validate_inputs() {
    if [[ " ${ACTIONS} " != *" ${ACTION} "* ]]; then
        echo "Invalid action: ${ACTION}"
        usage
        exit 1
    fi
    if [[ " ${BACKENDS} " != *" ${BACKEND} "* ]]; then
        echo "Invalid backend: ${BACKEND}"
        usage
        exit 1
    fi
}

validate_runtime_requirements() {
    local needs_ethstore_prefixdb="false"

    if [ -z "$DISK_MOUNT_POINT" ]; then
        echo "无法根据 RUNNING_ROOT 推导挂载点: ${RUNNING_ROOT}"
        exit 1
    fi

    if [ "$ACTION" = "load" ] || [ "$ACTION" = "replay" ]; then
        if [ "$BACKEND" = "ethstore" ] || [ "$BACKEND" = "all" ]; then
            needs_ethstore_prefixdb="true"
        fi
    fi

    if [ "$needs_ethstore_prefixdb" = "true" ]; then
        if [ -z "$ETHSTORE_PREFIXDB_DIR" ]; then
            echo "ethstore 的 load/replay 需要配置 ETHSTORE_PREFIXDB_DIR，当前为空。"
            usage
            exit 1
        fi
    fi

    if [ "$ACTION" = "replay" ]; then
        if [ ! -d "$LOADED_ROOT" ]; then
            echo "${ACTION} 需要已加载数据目录 LOADED_ROOT，当前不存在: ${LOADED_ROOT}"
            exit 1
        fi
        if [ ! -d "$RUNNING_ROOT" ]; then
            echo "RUNNING_ROOT 不存在，自动创建: ${RUNNING_ROOT}"
            sudo_run mkdir -p "$RUNNING_ROOT"
        fi
    fi

    if [ "$BACKEND" = "prefixdb" ] && [ "$ACTION" = "load" ]; then
        echo "prefixdb backend 已拆分为 load-account / load-storage，请使用新的 action"
        exit 1
    fi

    if { [ "$ACTION" = "load-account" ] || [ "$ACTION" = "load-storage" ]; } && [ "$BACKEND" != "prefixdb" ]; then
        echo "${ACTION} 仅支持 prefixdb backend（当前 BACKEND=${BACKEND}）"
        exit 1
    fi

    if [ "$ACTION" = "load-storage" ]; then
        if [ -z "$PREFIXDB_ACCOUNT_STATE_DIR" ]; then
            echo "load-storage 需要 PREFIXDB_ACCOUNT_STATE_DIR（已完成 account load 的 statedb 目录）"
            exit 1
        fi
        if [ ! -d "$PREFIXDB_ACCOUNT_STATE_DIR" ]; then
            echo "PREFIXDB_ACCOUNT_STATE_DIR 不存在: ${PREFIXDB_ACCOUNT_STATE_DIR}"
            exit 1
        fi
    fi

    if { [ "$ACTION" = "gc" ] || [ "$ACTION" = "upgrade-index" ]; } && [ "$BACKEND" != "ethstore" ]; then
        echo "${ACTION} 仅支持 ethstore backend（当前 BACKEND=${BACKEND}）"
        exit 1
    fi
    if [ "$ACTION" = "gc" ]; then
        if [ -z "$GC_STATE_DIR" ]; then
            echo "gc 需要 GC_STATE_DIR（直接指定 statedb 目录）"
            exit 1
        fi
        if [ ! -d "$GC_STATE_DIR" ]; then
            echo "GC_STATE_DIR 不存在: ${GC_STATE_DIR}"
            exit 1
        fi
    fi

    if [ "$ACTION" = "upgrade-index" ]; then
        if [ -z "$UPGRADE_STATE_DIR" ]; then
            echo "upgrade-index 需要 UPGRADE_STATE_DIR（直接指定 statedb 目录）"
            exit 1
        fi
        if [ ! -d "$UPGRADE_STATE_DIR" ]; then
            echo "UPGRADE_STATE_DIR 不存在: ${UPGRADE_STATE_DIR}"
            exit 1
        fi
    fi

    if [ "$REPLAY_CGROUP_IO_LIMIT_ENABLED" = "true" ]; then
        local numeric_value numeric_name
        for numeric_name in \
            REPLAY_CGROUP_READ_IOPS_LIMIT \
            REPLAY_CGROUP_WRITE_IOPS_LIMIT \
            REPLAY_CGROUP_READ_BPS_LIMIT \
            REPLAY_CGROUP_WRITE_BPS_LIMIT; do
            numeric_value="${!numeric_name}"
            if ! [[ "$numeric_value" =~ ^[0-9]+$ ]] || [ "$numeric_value" -le 0 ]; then
                echo "${numeric_name} 必须是正整数，当前值=${numeric_value}"
                exit 1
            fi
        done
    fi
}

build_replay_binary() {
    if ! go mod download; then
        echo "依赖下载失败，请检查网络或 GOPROXY 配置（当前 GOPROXY=$GOPROXY）"
        exit 1
    fi
    mkdir -p ./bin
    # if ! GOAMD64=v4 go build -tags theo_no_analysis_stats -trimpath -ldflags="-s -w" -o ./bin/replayWorkload ./replayWorkload.go; then
    if ! GOAMD64=v4 go build -trimpath -ldflags="-s -w" -o ./bin/replayWorkload ./replayWorkload.go; then
        echo "构建 replayWorkload 失败，退出。"
        exit 1
    fi
}

drop_caches() {
    # Trim the target SSD to minimize the impact of leftover data on performance.
    echo "Trimming target disk to drop caches..."
    sync
    sudo_run fstrim -v "$DISK_MOUNT_POINT"
    sleep 5
    echo "Drop caches"
    sudo_run sh -c 'echo 1 > /proc/sys/vm/drop_caches'
    sudo_run sh -c 'echo 2 > /proc/sys/vm/drop_caches'
    sudo_run sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    # Check if caches are dropped successfully.
    local cached_bytes
    cached_bytes=$(sudo_run cat /proc/meminfo | grep -E '^(Cached|Buffers|SReclaimable):' | awk '{sum += $2} END {print sum}')
    if [ "$cached_bytes" -gt 1048576 ]; then
        echo "Warning: Cached memory is still ${cached_bytes} KiB after dropping caches"
    else
        echo "Cached memory after dropping caches: ${cached_bytes} KiB"
    fi
}

resolve_mount_device() {
    local mount_device
    mount_device=$(findmnt -n -o SOURCE --target "$DISK_MOUNT_POINT" 2>/dev/null || true)
    if [ -z "$mount_device" ]; then
        mount_device=$(df --output=source "$DISK_MOUNT_POINT" 2>/dev/null | tail -n 1 | tr -d ' ')
    fi
    printf "%s" "$mount_device"
}

resolve_mount_majmin() {
    local majmin
    majmin=$(findmnt -n -o MAJ:MIN --target "$DISK_MOUNT_POINT" 2>/dev/null || true)
    if [ -n "$majmin" ]; then
        printf "%s" "$majmin"
        return 0
    fi

    local mount_device sys_stat_path
    mount_device=$(resolve_mount_device)
    if [ -z "$mount_device" ]; then
        return 1
    fi
    sys_stat_path="/sys/class/block/$(basename "$mount_device")/dev"
    if [ -r "$sys_stat_path" ]; then
        tr -d '[:space:]' < "$sys_stat_path"
        return 0
    fi
    return 1
}

cleanup_replay_cgroup() {
    if [ -z "$ACTIVE_REPLAY_CGROUP_PATH" ]; then
        return 0
    fi

    sudo_run sh -c "rmdir '$ACTIVE_REPLAY_CGROUP_PATH' 2>/dev/null || true"
    ACTIVE_REPLAY_CGROUP_PATH=""
    ACTIVE_REPLAY_CGROUP_MAJMIN=""
}

setup_replay_cgroup() {
    if [ "$ACTION" != "replay" ] || [ "$REPLAY_CGROUP_IO_LIMIT_ENABLED" != "true" ]; then
        return 1
    fi
    if [ ! -f /sys/fs/cgroup/cgroup.controllers ]; then
        echo "Skip replay cgroup IO limit: host is not using cgroup v2" >&2
        return 1
    fi

    local majmin cgroup_name cgroup_path uid gid
    majmin=$(resolve_mount_majmin)
    if [ -z "$majmin" ]; then
        echo "Skip replay cgroup IO limit: cannot resolve MAJ:MIN for ${DISK_MOUNT_POINT}" >&2
        return 1
    fi

    uid=$(id -u)
    gid=$(id -g)
    cgroup_name="${REPLAY_CGROUP_NAME_PREFIX}_$(date +%s)_$$"
    cgroup_path="/sys/fs/cgroup/${cgroup_name}"

    sudo_run mkdir "$cgroup_path"
    sudo_run sh -c "printf '%s riops=%s wiops=%s rbps=%s wbps=%s\n' '$majmin' '$REPLAY_CGROUP_READ_IOPS_LIMIT' '$REPLAY_CGROUP_WRITE_IOPS_LIMIT' '$REPLAY_CGROUP_READ_BPS_LIMIT' '$REPLAY_CGROUP_WRITE_BPS_LIMIT' > '$cgroup_path/io.max'"
    sudo_run sh -c "chown '$uid:$gid' '$cgroup_path' '$cgroup_path/cgroup.procs' '$cgroup_path/cgroup.threads' 2>/dev/null || true"

    ACTIVE_REPLAY_CGROUP_PATH="$cgroup_path"
    ACTIVE_REPLAY_CGROUP_MAJMIN="$majmin"
    return 0
}

launch_replay_process() {
    local log_file="$1"
    shift

    if setup_replay_cgroup; then
        echo "Replay cgroup IO limit enabled: path=${ACTIVE_REPLAY_CGROUP_PATH} device=${ACTIVE_REPLAY_CGROUP_MAJMIN} riops=${REPLAY_CGROUP_READ_IOPS_LIMIT} wiops=${REPLAY_CGROUP_WRITE_IOPS_LIMIT} rbps=${REPLAY_CGROUP_READ_BPS_LIMIT} wbps=${REPLAY_CGROUP_WRITE_BPS_LIMIT}"
        ./bin/replayWorkload "$@" >> "$log_file" 2>&1 &
        CURRENT_REPLAY_PID=$!

        if ! sudo_run sh -c "echo '${CURRENT_REPLAY_PID}' > '${ACTIVE_REPLAY_CGROUP_PATH}/cgroup.procs'"; then
            echo "Failed to move replay PID ${CURRENT_REPLAY_PID} into cgroup: ${ACTIVE_REPLAY_CGROUP_PATH}" >&2
            terminate_pid_tree "$CURRENT_REPLAY_PID"
            wait "$CURRENT_REPLAY_PID" 2>/dev/null || true
            CURRENT_REPLAY_PID=""
            cleanup_replay_cgroup
            return 1
        fi
        return 0
    fi

    ./bin/replayWorkload "$@" >> "$log_file" 2>&1 &
    CURRENT_REPLAY_PID=$!
}

read_diskstats_snapshot() {
    local device_name="$1"
    awk -v dev="$device_name" '$3 == dev {printf "%s %s %s %s %s %s", $4, $6, $8, $10, $12, $13; exit}' /proc/diskstats
}

read_dirty_writeback_kb() {
    awk '
        /^Dirty:/ {dirty = $2}
        /^Writeback:/ {writeback = $2}
        END {printf "%s", dirty + writeback}
    ' /proc/meminfo
}

observe_disk_idle() {
    if [ "$IDLE_OBSERVE_ENABLED" != "true" ]; then
        echo "Skip idle observation because IDLE_OBSERVE_ENABLED=${IDLE_OBSERVE_ENABLED}"
        return 0
    fi

    if ! [[ "$IDLE_OBSERVE_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || [ "$IDLE_OBSERVE_INTERVAL_SECONDS" -le 0 ]; then
        echo "Invalid IDLE_OBSERVE_INTERVAL_SECONDS=${IDLE_OBSERVE_INTERVAL_SECONDS}, skip idle observation" >&2
        return 0
    fi
    if ! [[ "$IDLE_OBSERVE_MAX_SECONDS" =~ ^[0-9]+$ ]] || [ "$IDLE_OBSERVE_MAX_SECONDS" -le 0 ]; then
        echo "Invalid IDLE_OBSERVE_MAX_SECONDS=${IDLE_OBSERVE_MAX_SECONDS}, skip idle observation" >&2
        return 0
    fi
    if ! [[ "$IDLE_OBSERVE_STABLE_WINDOWS" =~ ^[0-9]+$ ]] || [ "$IDLE_OBSERVE_STABLE_WINDOWS" -le 0 ]; then
        echo "Invalid IDLE_OBSERVE_STABLE_WINDOWS=${IDLE_OBSERVE_STABLE_WINDOWS}, skip idle observation" >&2
        return 0
    fi
    if ! [[ "$IDLE_OBSERVE_MAX_DIRTY_KB" =~ ^[0-9]+$ ]] || [ "$IDLE_OBSERVE_MAX_DIRTY_KB" -lt 0 ]; then
        echo "Invalid IDLE_OBSERVE_MAX_DIRTY_KB=${IDLE_OBSERVE_MAX_DIRTY_KB}, skip idle observation" >&2
        return 0
    fi

    local mount_device device_name baseline_snapshot max_windows stable_windows observed_windows max_observed_windows
    mount_device=$(resolve_mount_device)
    if [ -z "$mount_device" ]; then
        echo "Cannot resolve block device for $DISK_MOUNT_POINT, skip idle observation" >&2
        return 0
    fi
    device_name=$(basename "$mount_device")
    baseline_snapshot=$(read_diskstats_snapshot "$device_name")
    if [ -z "$baseline_snapshot" ]; then
        echo "Cannot read /proc/diskstats for device ${device_name}, skip idle observation" >&2
        return 0
    fi

    max_windows=$((IDLE_OBSERVE_MAX_SECONDS / IDLE_OBSERVE_INTERVAL_SECONDS))
    if [ "$max_windows" -lt 1 ]; then
        max_windows=1
    fi

    stable_windows=0
    observed_windows=0
    max_observed_windows=$max_windows

    echo "Observe idle on ${mount_device}: interval=${IDLE_OBSERVE_INTERVAL_SECONDS}s max=${IDLE_OBSERVE_MAX_SECONDS}s stable_windows=${IDLE_OBSERVE_STABLE_WINDOWS} dirty_limit=${IDLE_OBSERVE_MAX_DIRTY_KB}KiB"

    while [ "$observed_windows" -lt "$max_observed_windows" ]; do
        sleep "$IDLE_OBSERVE_INTERVAL_SECONDS"
        observed_windows=$((observed_windows + 1))

        local current_snapshot prev_reads prev_read_sectors prev_writes prev_write_sectors prev_inflight prev_io_ms
        local cur_reads cur_read_sectors cur_writes cur_write_sectors cur_inflight cur_io_ms
        local delta_reads delta_read_sectors delta_writes delta_write_sectors delta_io_ms dirty_kb
        current_snapshot=$(read_diskstats_snapshot "$device_name")
        if [ -z "$current_snapshot" ]; then
            echo "Lost /proc/diskstats entry for ${device_name}, stop idle observation" >&2
            return 0
        fi

        read -r prev_reads prev_read_sectors prev_writes prev_write_sectors prev_inflight prev_io_ms <<< "$baseline_snapshot"
        read -r cur_reads cur_read_sectors cur_writes cur_write_sectors cur_inflight cur_io_ms <<< "$current_snapshot"

        delta_reads=$((cur_reads - prev_reads))
        delta_read_sectors=$((cur_read_sectors - prev_read_sectors))
        delta_writes=$((cur_writes - prev_writes))
        delta_write_sectors=$((cur_write_sectors - prev_write_sectors))
        delta_io_ms=$((cur_io_ms - prev_io_ms))
        dirty_kb=$(read_dirty_writeback_kb)

        echo "Idle observe [${observed_windows}/${max_observed_windows}] ${device_name}: readOpsDelta=${delta_reads} writeOpsDelta=${delta_writes} readBytesDelta=$((delta_read_sectors * 512)) writeBytesDelta=$((delta_write_sectors * 512)) ioBusyMsDelta=${delta_io_ms} inflight=${cur_inflight} dirtyWritebackKB=${dirty_kb}"

        if [ "$delta_reads" -eq 0 ] && [ "$delta_writes" -eq 0 ] && [ "$delta_io_ms" -eq 0 ] && [ "$cur_inflight" -eq 0 ] && [ "$dirty_kb" -le "$IDLE_OBSERVE_MAX_DIRTY_KB" ]; then
            stable_windows=$((stable_windows + 1))
            if [ "$stable_windows" -ge "$IDLE_OBSERVE_STABLE_WINDOWS" ]; then
                echo "Idle observation converged after $((observed_windows * IDLE_OBSERVE_INTERVAL_SECONDS))s on ${mount_device}"
                return 0
            fi
        else
            stable_windows=0
        fi

        baseline_snapshot="$current_snapshot"
    done

    echo "Idle observation reached max wait ${IDLE_OBSERVE_MAX_SECONDS}s; continue with latest device state" >&2
}

restore_ethstore_db() {
    echo "Restore ethstore database..."
    local src_root="${LOADED_ROOT}/ethstore"
    local dst_prefix="${RUNNING_ROOT}/ethstore"
    if [ "$DB_TYPE" = "all" ] || [ "$DB_TYPE" = "aol" ]; then
        sudo_rsync_run -avP --delete "${src_root}/database_aol/" "${dst_prefix}_aol/"
        sudo_run chmod -R 777 "${dst_prefix}_aol/"
    fi
    if [ "$DB_TYPE" = "all" ] || [ "$DB_TYPE" = "pebble" ]; then
        sudo_rsync_run -avP --delete "${src_root}/database_pebble/" "${dst_prefix}_pebble/"
        sudo_run chmod -R 777 "${dst_prefix}_pebble/"
    fi
    if [ "$DB_TYPE" = "all" ] || [ "$DB_TYPE" = "prefixdb" ]; then
        sudo_rsync_run -avP --delete "${src_root}/${ETHSTORE_STATEDB_DIRNAME}/" "${dst_prefix}_state/"
        sudo_run chmod -R 777 "${dst_prefix}_state/"
    fi
}

restore_chainkv_db() {
    echo "Restore chainkv baseline database..."
    sudo_rsync_run -avP --delete "${LOADED_ROOT}/chainkv/" "${RUNNING_ROOT}/chainkv/"
    sudo_run chmod -R 777 "${RUNNING_ROOT}/chainkv/"
}

restore_pebble_db() {
    echo "Restore pebble baseline database..."
    sudo_rsync_run -avP --delete "${LOADED_ROOT}/pebble/" "${RUNNING_ROOT}/pebble/"
    sudo_run chmod -R 777 "${RUNNING_ROOT}/pebble/"
}

ensure_ethstore_permissions() {
    echo "Fix ethstore permissions (prefixdb)..."
    # Ensure the lock directory exists and is writable to avoid LOCK permission denied.
    sudo_run chmod -R 777 "${ETHSTORE_PREFIXDB_DIR}"
}

ensure_ethstore_statedb_dirname() {
    local src_state="${LOADED_ROOT}/ethstore/${ETHSTORE_STATEDB_DIRNAME}"
    if [ ! -d "${src_state}" ]; then
        echo "Invalid ETHSTORE_STATEDB_DIRNAME=${ETHSTORE_STATEDB_DIRNAME}. Directory not found: ${src_state}"
        echo "Available state dirs under ${LOADED_ROOT}/ethstore:"
        ls -1d "${LOADED_ROOT}/ethstore"/database_statedb* 2>/dev/null || true
        exit 1
    fi
}

normalize_trace_selector() {
    printf "%s" "$TRACE_FILE" | tr '[:upper:]' '[:lower:]'
}

resolve_chainkv_loaded_dir() {
    case "$(normalize_trace_selector)" in
        cache|nocache_snap)
            printf "%s" "${LOADED_ROOT}/chainkv"
            ;;
        nocache)
            printf "%s" "${LOADED_ROOT}/chainkv_without"
            ;;
        *)
            echo "Unsupported TRACE_FILE for chainkv replay sync: ${TRACE_FILE}" >&2
            exit 1
            ;;
    esac
}

resolve_pebble_loaded_dir() {
    case "$(normalize_trace_selector)" in
        cache|nocache_snap)
            printf "%s" "${LOADED_ROOT}/pebble"
            ;;
        nocache)
            printf "%s" "${LOADED_ROOT}/pebble_without"
            ;;
        *)
            echo "Unsupported TRACE_FILE for pebble replay sync: ${TRACE_FILE}" >&2
            exit 1
            ;;
    esac
}

resolve_ethstore_pebble_loaded_dir() {
    if [ "$DB_TYPE" = "prefixdb" ] && [ -n "$ETHSTORE_PREFIXDB_PEBBLE_SOURCE_DIR" ]; then
        printf "%s" "$ETHSTORE_PREFIXDB_PEBBLE_SOURCE_DIR"
        return 0
    fi
    case "$(normalize_trace_selector)" in
        cache|nocache_snap)
            printf "%s" "${LOADED_ROOT}/ethstore/database_pebble"
            ;;
        nocache)
            printf "%s" "${LOADED_ROOT}/ethstore/database_pebble_without"
            ;;
        *)
            echo "Unsupported TRACE_FILE for ethstore pebble replay sync: ${TRACE_FILE}" >&2
            exit 1
            ;;
    esac
}

sync_ethstore_loaded_to_running() {
    ensure_ethstore_statedb_dirname
    local src_root="${LOADED_ROOT}/ethstore"
    local dst_prefix="${RUNNING_ROOT}/ethstore"

    local src_aol="${src_root}/database_aol"
    local src_pebble
    src_pebble="$(resolve_ethstore_pebble_loaded_dir)"
    local src_state="${src_root}/${ETHSTORE_STATEDB_DIRNAME}"

    if [ "$DB_TYPE" = "all" ] || [ "$DB_TYPE" = "aol" ]; then
        if [ ! -d "${src_aol}" ]; then
            echo "ethstore source directory missing under ${src_root}: database_aol"
            exit 1
        fi
    fi
    if [ "$DB_TYPE" = "all" ] || [ "$DB_TYPE" = "pebble" ]; then
        if [ ! -d "${src_pebble}" ]; then
            echo "ethstore source directory missing under ${src_root}: database_pebble"
            exit 1
        fi
    fi
    if [ "$DB_TYPE" = "all" ] || [ "$DB_TYPE" = "prefixdb" ]; then
        if [ ! -d "${src_state}" ]; then
            echo "ethstore source directory missing under ${src_root}: ${ETHSTORE_STATEDB_DIRNAME}"
            exit 1
        fi
        if [ ! -d "${src_pebble}" ]; then
            echo "ethstore source directory missing under ${src_root}: $(basename "${src_pebble}")"
            exit 1
        fi
    fi

    case "$DB_TYPE" in
        prefixdb)
            echo "Sync ethstore state+pebble data: ${src_state} -> ${dst_prefix}_state, ${src_pebble} -> ${dst_prefix}_pebble"
            sudo_run mkdir -p "${dst_prefix}_state" "${dst_prefix}_pebble"
            sudo_rsync_run -avP --delete "${src_state}/" "${dst_prefix}_state/"
            sudo_rsync_run -avP --delete "${src_pebble}/" "${dst_prefix}_pebble/"
            sudo_run chmod -R 777 "${dst_prefix}_state" "${dst_prefix}_pebble"
            ;;
        aol)
            echo "Sync ethstore block data: ${src_aol} -> ${dst_prefix}_aol"
            sudo_run mkdir -p "${dst_prefix}_aol"
            sudo_rsync_run -avP --delete "${src_aol}/" "${dst_prefix}_aol/"
            sudo_run chmod -R 777 "${dst_prefix}_aol"
            ;;
        pebble)
            echo "Sync ethstore pebble data: ${src_pebble} -> ${dst_prefix}_pebble"
            sudo_run mkdir -p "${dst_prefix}_pebble"
            sudo_rsync_run -avP --delete "${src_pebble}/" "${dst_prefix}_pebble/"
            sudo_run chmod -R 777 "${dst_prefix}_pebble"
            ;;
        *)
            echo "Sync ethstore data: ${src_root} -> ${dst_prefix}{_aol,_pebble,_state}"
            sudo_run mkdir -p "${dst_prefix}_aol" "${dst_prefix}_pebble" "${dst_prefix}_state"
            sudo_rsync_run -avP --delete "${src_aol}/" "${dst_prefix}_aol/"
            sudo_rsync_run -avP --delete "${src_pebble}/" "${dst_prefix}_pebble/"
            sudo_rsync_run -avP --delete "${src_state}/" "${dst_prefix}_state/"
            sudo_run chmod -R 777 "${dst_prefix}_aol" "${dst_prefix}_pebble" "${dst_prefix}_state"
            ;;
    esac
}

sync_chainkv_loaded_to_running() {
    local src
    src="$(resolve_chainkv_loaded_dir)"
    local dst="${RUNNING_ROOT}/chainkv"
    if [ ! -d "${src}" ]; then
        echo "chainkv source directory missing: ${src}"
        exit 1
    fi
    echo "Sync chainkv data: ${src} -> ${dst}"
    sudo_run mkdir -p "${dst}"
    sudo_rsync_run -avP --delete "${src}/" "${dst}/"
    sudo_run chmod -R 777 "${dst}"
}

sync_pebble_loaded_to_running() {
    local src
    src="$(resolve_pebble_loaded_dir)"
    local dst="${RUNNING_ROOT}/pebble"
    if [ ! -d "${src}" ]; then
        echo "pebble source directory missing: ${src}"
        exit 1
    fi
    echo "Sync pebble data: ${src} -> ${dst}"
    sudo_run mkdir -p "${dst}"
    sudo_rsync_run -avP --delete "${src}/" "${dst}/"
    sudo_run chmod -R 777 "${dst}"
}

sync_loaded_to_running_for_backend() {
    local backend="$1"
    case "$backend" in
        ethstore)
            sync_ethstore_loaded_to_running
            ;;
        chainkv)
            sync_chainkv_loaded_to_running
            ;;
        pebble)
            sync_pebble_loaded_to_running
            ;;
    esac
}

sanitize_tag_value() {
    local value="$1"
    value="${value,,}"
    value=$(printf "%s" "$value" | tr -cs 'a-z0-9._-' '_')
    value="${value##_}"
    value="${value%%_}"
    if [ -z "$value" ]; then
        value="empty"
    fi
    # Keep filenames readable and bounded.
    if [ "${#value}" -gt 48 ]; then
        local hash
        hash=$(printf "%s" "$value" | cksum | awk '{print $1}')
        value="${value:0:32}_h${hash}"
    fi
    printf "%s" "$value"
}

build_run_tag() {
    local action="$1"
    local backend="$2"

    local trace_tag dbtype_tag loaded_root_tag running_root_tag
    trace_tag=$(sanitize_tag_value "$TRACE_FILE")
    dbtype_tag=$(sanitize_tag_value "$DB_TYPE")
    loaded_root_tag=$(sanitize_tag_value "$LOADED_ROOT")
    running_root_tag=$(sanitize_tag_value "$RUNNING_ROOT")

    local base_tag
    base_tag="act_${action}_be_${backend}_max_${WORKLOAD_MAX_OPS}_trace_${trace_tag}_db_${dbtype_tag}_block_${START_BLOCK_ID}-${END_BLOCK_ID}_cbi_${COMMIT_BLOCK_INTERVAL}"

    local round_tag=""
    if [[ "$RUN_ROUND" =~ ^[0-9]+$ ]] && [ "$RUN_ROUND" -gt 0 ]; then
        round_tag="_r_${RUN_ROUND}"
    fi

    local cgroup_tag=""
    if [ "$action" = "replay" ]; then
        if [ "$REPLAY_CGROUP_IO_LIMIT_ENABLED" = "true" ]; then
            cgroup_tag="_cg_1"
        else
            cgroup_tag="_cg_0"
        fi
    fi

    if [ "$backend" = "chainkv" ]; then
        local ckv_state_tag
        ckv_state_tag=$(sanitize_tag_value "$CHAINKV_STATE")
        printf "%s" "${base_tag}_ckvc_${CHAINKV_CACHE_MB}_ckvh_${CHAINKV_HANDLES}_ckvs_${ckv_state_tag}_ckvl_${CHAINKV_LOAD_LIMIT}${round_tag}${cgroup_tag}"
    elif [ "$backend" = "pebble" ]; then
        printf "%s" "${base_tag}_pbc_${PEBBLE_CACHE_MB}_pbh_${PEBBLE_HANDLES}${round_tag}${cgroup_tag}"
    elif [ "$backend" = "prefixdb" ]; then
        printf "%s" "${base_tag}_cfs_${CHUNK_FILE_SIZE_BYTES}_tcs_${TOTAL_CACHE_SIZE_MIB}_pfh_${PREFIXDB_HANDLES}_ngcr_${NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD}_gcw_${GC_WORKERS}_sgct_${STORAGE_GC_THRESHOLD}_nfsc_${NODE_FILE_SORTED_COMPRESSION}_sic_${SEGMENT_INDEX_COMPRESSION}${round_tag}${cgroup_tag}"
    elif [ "$backend" = "ethstore" ]; then
        printf "%s" "${base_tag}_cfs_${CHUNK_FILE_SIZE_BYTES}_tcs_${TOTAL_CACHE_SIZE_MIB}_pfh_${PREFIXDB_HANDLES}_pbc_${PEBBLE_CACHE_MB}_pbh_${PEBBLE_HANDLES}_cc_${CACHE_COUNT}_ngcr_${NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD}_gcw_${GC_WORKERS}_sgct_${STORAGE_GC_THRESHOLD}_nfsc_${NODE_FILE_SORTED_COMPRESSION}_sic_${SEGMENT_INDEX_COMPRESSION}${round_tag}${cgroup_tag}"
    else
        printf "%s" "${base_tag}${round_tag}${cgroup_tag}"
    fi
}

print_param_snapshot() {
    local snapshot_action="$1"
    local snapshot_backend="$2"
    printf '%s\n' "==== Runtime Parameters ===="
    printf 'ACTION=%s\n' "$snapshot_action"
    printf 'BACKEND=%s\n' "$snapshot_backend"
    printf 'WORKLOAD_MAX_OPS=%s\n' "$WORKLOAD_MAX_OPS"
    printf 'START_BLOCK_ID=%s\n' "$START_BLOCK_ID"
    printf 'END_BLOCK_ID=%s\n' "$END_BLOCK_ID"
    printf 'COMMIT_BLOCK_INTERVAL=%s\n' "$COMMIT_BLOCK_INTERVAL"
    printf 'TRACE_FILE=%s\n' "$TRACE_FILE"
    printf 'DB_TYPE=%s\n' "$DB_TYPE"
    printf 'RUN_ROUND=%s\n' "$RUN_ROUND"
    printf 'RUN_ROUNDS=%s\n' "$RUN_ROUNDS"
    printf 'ETHSTORE_PREFIXDB_DIR=%s\n' "$ETHSTORE_PREFIXDB_DIR"
    printf 'LOADED_ROOT=%s\n' "$LOADED_ROOT"
    printf 'RUNNING_ROOT=%s\n' "$RUNNING_ROOT"
    printf 'DISK_MOUNT_POINT=%s\n' "$DISK_MOUNT_POINT"
    printf 'ETHSTORE_STATEDB_DIRNAME=%s\n' "$ETHSTORE_STATEDB_DIRNAME"
    printf 'GC_STATE_DIR=%s\n' "$GC_STATE_DIR"
    printf 'UPGRADE_STATE_DIR=%s\n' "$UPGRADE_STATE_DIR"
    printf 'PREFIXDB_ACCOUNT_STATE_DIR=%s\n' "$PREFIXDB_ACCOUNT_STATE_DIR"
    printf 'ETHSTORE_PREFIXDB_PEBBLE_SOURCE_DIR=%s\n' "$ETHSTORE_PREFIXDB_PEBBLE_SOURCE_DIR"
    printf 'IDLE_OBSERVE_ENABLED=%s\n' "$IDLE_OBSERVE_ENABLED"
    printf 'IDLE_OBSERVE_INTERVAL_SECONDS=%s\n' "$IDLE_OBSERVE_INTERVAL_SECONDS"
    printf 'IDLE_OBSERVE_MAX_SECONDS=%s\n' "$IDLE_OBSERVE_MAX_SECONDS"
    printf 'IDLE_OBSERVE_STABLE_WINDOWS=%s\n' "$IDLE_OBSERVE_STABLE_WINDOWS"
    printf 'IDLE_OBSERVE_MAX_DIRTY_KB=%s\n' "$IDLE_OBSERVE_MAX_DIRTY_KB"
    printf 'REPLAY_CGROUP_IO_LIMIT_ENABLED=%s\n' "$REPLAY_CGROUP_IO_LIMIT_ENABLED"
    printf 'REPLAY_CGROUP_NAME_PREFIX=%s\n' "$REPLAY_CGROUP_NAME_PREFIX"
    printf 'REPLAY_CGROUP_READ_IOPS_LIMIT=%s\n' "$REPLAY_CGROUP_READ_IOPS_LIMIT"
    printf 'REPLAY_CGROUP_WRITE_IOPS_LIMIT=%s\n' "$REPLAY_CGROUP_WRITE_IOPS_LIMIT"
    printf 'REPLAY_CGROUP_READ_MBPS_LIMIT=%s\n' "$REPLAY_CGROUP_READ_MBPS_LIMIT"
    printf 'REPLAY_CGROUP_WRITE_MBPS_LIMIT=%s\n' "$REPLAY_CGROUP_WRITE_MBPS_LIMIT"
    printf 'REPLAY_CGROUP_READ_BPS_LIMIT=%s\n' "$REPLAY_CGROUP_READ_BPS_LIMIT"
    printf 'REPLAY_CGROUP_WRITE_BPS_LIMIT=%s\n' "$REPLAY_CGROUP_WRITE_BPS_LIMIT"

    if [ "$snapshot_backend" = "ethstore" ]; then
        printf 'CACHE_COUNT=%s\n' "$CACHE_COUNT"
        printf 'NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD=%s\n' "$NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD"
        printf 'GC_WORKERS=%s\n' "$GC_WORKERS"
        printf 'STORAGE_GC_THRESHOLD=%s\n' "$STORAGE_GC_THRESHOLD"
        printf 'NODE_FILE_SORTED_COMPRESSION=%s\n' "$NODE_FILE_SORTED_COMPRESSION"
        printf 'SEGMENT_INDEX_COMPRESSION=%s\n' "$SEGMENT_INDEX_COMPRESSION"
        printf 'CHUNK_FILE_SIZE_BYTES=%s\n' "$CHUNK_FILE_SIZE_BYTES"
        printf 'TOTAL_CACHE_SIZE_MIB=%s MiB (%s bytes)\n' "$TOTAL_CACHE_SIZE_MIB" "$TOTAL_CACHE_SIZE_BYTES"
        printf 'PREFIXDB_HANDLES=%s\n' "$PREFIXDB_HANDLES"
        printf 'PEBBLE_CACHE_MB=%s\n' "$PEBBLE_CACHE_MB"
        printf 'PEBBLE_HANDLES=%s\n' "$PEBBLE_HANDLES"
    elif [ "$snapshot_backend" = "prefixdb" ]; then
        printf 'CHUNK_FILE_SIZE_BYTES=%s (bytes)\n' "$CHUNK_FILE_SIZE_BYTES"
        printf 'TOTAL_CACHE_SIZE_MIB=%s MiB (%s bytes)\n' "$TOTAL_CACHE_SIZE_MIB" "$TOTAL_CACHE_SIZE_BYTES"
        printf 'PREFIXDB_HANDLES=%s\n' "$PREFIXDB_HANDLES"
        printf 'NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD=%s\n' "$NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD"
        printf 'GC_WORKERS=%s\n' "$GC_WORKERS"
        printf 'STORAGE_GC_THRESHOLD=%s\n' "$STORAGE_GC_THRESHOLD"
        printf 'NODE_FILE_SORTED_COMPRESSION=%s\n' "$NODE_FILE_SORTED_COMPRESSION"
        printf 'SEGMENT_INDEX_COMPRESSION=%s\n' "$SEGMENT_INDEX_COMPRESSION"
    elif [ "$snapshot_backend" = "chainkv" ]; then
        printf 'CHAINKV_CACHE_MB=%s\n' "$CHAINKV_CACHE_MB"
        printf 'CHAINKV_HANDLES=%s\n' "$CHAINKV_HANDLES"
        printf 'CHAINKV_STATE=%s\n' "$CHAINKV_STATE"
        printf 'CHAINKV_LOAD_LIMIT=%s\n' "$CHAINKV_LOAD_LIMIT"
    elif [ "$snapshot_backend" = "pebble" ]; then
        printf 'PEBBLE_CACHE_MB=%s\n' "$PEBBLE_CACHE_MB"
        printf 'PEBBLE_HANDLES=%s\n' "$PEBBLE_HANDLES"
    fi

    printf '%s\n' "============================"
}

run_and_monitor() {
    local backend="$1"
    local log_file="$2"
    local io_file="$3"
    shift 3

    if [ -e "$log_file" ]; then
        local uniq
        uniq=$(date +%s%N)
        log_file="${log_file%.log}_${uniq}.log"
    fi
    if [ -e "$io_file" ]; then
        local uniq_io
        uniq_io=$(date +%s%N)
        io_file="${io_file%.log}_${uniq_io}.log"
    fi

    echo "Log file: $log_file"
    echo "IO  file: $io_file"

    {
        print_param_snapshot "${ACTION}" "${backend}"
        printf "\n"
    } >> "$log_file"

    launch_replay_process "$log_file" "$@"
    echo "${backend} monitor target PID: ${CURRENT_REPLAY_PID}"
    (
        trap - ERR INT TERM EXIT
        set +e
        sudo_run ./monitor.sh "$CURRENT_REPLAY_PID" 1 "$io_file" "$DISK_MOUNT_POINT"
    ) &
    CURRENT_MONITOR_PID=$!

    local replay_rc
    set +e
    wait "$CURRENT_REPLAY_PID"
    replay_rc=$?
    set -e
    CURRENT_REPLAY_PID=""

    # Let monitor.sh observe process exit and flush SSD stats to *.stat.
    if [ -n "$CURRENT_MONITOR_PID" ]; then
        wait "$CURRENT_MONITOR_PID" 2>/dev/null || true
        CURRENT_MONITOR_PID=""
    fi

    cleanup_replay_cgroup
    cleanup_running_processes

    return "$replay_rc"
}

run_load() {
    local backend="$1"
    local run_tag
    run_tag=$(build_run_tag "load" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"
    local io_file="./replayLog/${run_tag}_io_${log_date}.log"
    case "$backend" in
        ethstore)
            ensure_ethstore_permissions
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode ld -backend ethstore -contract-chunk-file-size-bytes "$CHUNK_FILE_SIZE_BYTES" -total-cache-size-mib "$TOTAL_CACHE_SIZE_MIB" -prefixdb-handles "$PREFIXDB_HANDLES" -pebble-cache "$PEBBLE_CACHE_MB" -pebble-handles "$PEBBLE_HANDLES" \
                -node-file-gc-unsorted-ratio-threshold "$NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD" -gc-workers "$GC_WORKERS" -storage-gc-threshold "$STORAGE_GC_THRESHOLD" \
                -node-file-sorted-compression="$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression="$SEGMENT_INDEX_COMPRESSION"
            ;;
        prefixdb)
            echo "prefixdb backend 已拆分为 load-account / load-storage，请使用新的 action"
            exit 1
            ;;
        chainkv)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode ld -backend chainkv -ckv-cache "$CHAINKV_CACHE_MB" -ckv-handles "$CHAINKV_HANDLES" -ckv-state "$CHAINKV_STATE" -ckv-limit "$CHAINKV_LOAD_LIMIT"
            ;;
        pebble)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode ld -backend pebble -pebble-cache "$PEBBLE_CACHE_MB" -pebble-handles "$PEBBLE_HANDLES"
            ;;
    esac
}

run_load_account() {
    local backend="$1"
    local run_tag
    run_tag=$(build_run_tag "load-account" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"
    local io_file="./replayLog/${run_tag}_io_${log_date}.log"

    case "$backend" in
        prefixdb)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode ld -backend prefixdb -prefixdb-load-stage account \
                -contract-chunk-file-size-bytes "$CHUNK_FILE_SIZE_BYTES" -total-cache-size-mib "$TOTAL_CACHE_SIZE_MIB" -prefixdb-handles "$PREFIXDB_HANDLES" \
                -node-file-gc-unsorted-ratio-threshold "$NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD" -gc-workers "$GC_WORKERS" -storage-gc-threshold "$STORAGE_GC_THRESHOLD" \
                -node-file-sorted-compression="$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression="$SEGMENT_INDEX_COMPRESSION"
            ;;
        *)
            echo "load-account 仅支持 prefixdb backend"
            exit 1
            ;;
    esac
}

run_load_storage() {
    local backend="$1"
    local run_tag
    run_tag=$(build_run_tag "load-storage" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"
    local io_file="./replayLog/${run_tag}_io_${log_date}.log"

    case "$backend" in
        prefixdb)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode ld -backend prefixdb -prefixdb-load-stage storage -prefixdb-state-dir "$PREFIXDB_ACCOUNT_STATE_DIR" \
                -contract-chunk-file-size-bytes "$CHUNK_FILE_SIZE_BYTES" -total-cache-size-mib "$TOTAL_CACHE_SIZE_MIB" -prefixdb-handles "$PREFIXDB_HANDLES" \
                -node-file-gc-unsorted-ratio-threshold "$NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD" -gc-workers "$GC_WORKERS" -storage-gc-threshold "$STORAGE_GC_THRESHOLD" \
                -node-file-sorted-compression="$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression="$SEGMENT_INDEX_COMPRESSION"
            ;;
        *)
            echo "load-storage 仅支持 prefixdb backend"
            exit 1
            ;;
    esac
}

run_restore() {
    local backend="$1"
    local run_tag
    run_tag=$(build_run_tag "restore" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"

    echo "Restore log file: $log_file"
    {
        print_param_snapshot "${ACTION}" "${backend}"
        printf "\n"

    case "$backend" in
        ethstore)
            restore_ethstore_db
            ;;
        chainkv)
            restore_chainkv_db
            ;;
        pebble)
            restore_pebble_db
            ;;
    esac
    } > "$log_file" 2>&1
}

run_replay() {
    local backend="$1"
    # 每次回放前，把已加载数据同步到 running/system 目录
    sync_loaded_to_running_for_backend "$backend"
    # Ensure cache drop happens after loaded DB is synced.
    drop_caches
    observe_disk_idle

    local run_tag
    run_tag=$(build_run_tag "replay" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"
    local io_file="./replayLog/${run_tag}_io_${log_date}.log"
    case "$backend" in
        ethstore)
            ensure_ethstore_permissions
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode re -backend ethstore -max-ops "$WORKLOAD_MAX_OPS" -db-type "$DB_TYPE" -trace-file "$TRACE_FILE" -cache-count "$CACHE_COUNT" \
                -start-block-id "$START_BLOCK_ID" -end-block-id "$END_BLOCK_ID" -commit-block-interval "$COMMIT_BLOCK_INTERVAL" \
                -contract-chunk-file-size-bytes "$CHUNK_FILE_SIZE_BYTES" -total-cache-size-mib "$TOTAL_CACHE_SIZE_MIB" -prefixdb-handles "$PREFIXDB_HANDLES" -pebble-cache "$PEBBLE_CACHE_MB" -pebble-handles "$PEBBLE_HANDLES" \
                -node-file-gc-unsorted-ratio-threshold "$NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD" -gc-workers "$GC_WORKERS" -storage-gc-threshold "$STORAGE_GC_THRESHOLD" \
                -node-file-sorted-compression="$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression="$SEGMENT_INDEX_COMPRESSION"
            ;;
        chainkv)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode re -backend chainkv -max-ops "$WORKLOAD_MAX_OPS" -db-type "$DB_TYPE" -trace-file "$TRACE_FILE" -start-block-id "$START_BLOCK_ID" -end-block-id "$END_BLOCK_ID" -commit-block-interval "$COMMIT_BLOCK_INTERVAL" -ckv-cache "$CHAINKV_CACHE_MB" -ckv-handles "$CHAINKV_HANDLES" -ckv-state "$CHAINKV_STATE"
            ;;
        pebble)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode re -backend pebble -max-ops "$WORKLOAD_MAX_OPS" -db-type "$DB_TYPE" -trace-file "$TRACE_FILE" -start-block-id "$START_BLOCK_ID" -end-block-id "$END_BLOCK_ID" -commit-block-interval "$COMMIT_BLOCK_INTERVAL" -pebble-cache "$PEBBLE_CACHE_MB" -pebble-handles "$PEBBLE_HANDLES"
            ;;
    esac
}

run_gc() {
    local backend="$1"
    local run_tag
    run_tag=$(build_run_tag "gc" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"
    local io_file="./replayLog/${run_tag}_io_${log_date}.log"

    case "$backend" in
        ethstore)
            ensure_ethstore_permissions
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode gc -backend ethstore -cache-count "$CACHE_COUNT" \
                -gc-state-dir "$GC_STATE_DIR" -contract-chunk-file-size-bytes "$CHUNK_FILE_SIZE_BYTES" -total-cache-size-mib "$TOTAL_CACHE_SIZE_MIB" -prefixdb-handles "$PREFIXDB_HANDLES" \
                -node-file-gc-unsorted-ratio-threshold "$NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD" -gc-workers "$GC_WORKERS" -storage-gc-threshold "$STORAGE_GC_THRESHOLD" \
                -node-file-sorted-compression="$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression="$SEGMENT_INDEX_COMPRESSION"
            ;;
        *)
            echo "gc 仅支持 ethstore backend"
            exit 1
            ;;
    esac
}

run_upgrade_index() {
    local backend="$1"
    local run_tag
    run_tag=$(build_run_tag "upgrade-index" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"
    local io_file="./replayLog/${run_tag}_io_${log_date}.log"

    case "$backend" in
        ethstore)
            ensure_ethstore_permissions
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode upgrade-index -backend ethstore -upgrade-state-dir "$UPGRADE_STATE_DIR" \
                -cache-count "$CACHE_COUNT" -contract-chunk-file-size-bytes "$CHUNK_FILE_SIZE_BYTES" -total-cache-size-mib "$TOTAL_CACHE_SIZE_MIB" -prefixdb-handles "$PREFIXDB_HANDLES" \
                -node-file-gc-unsorted-ratio-threshold "$NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD" -gc-workers "$GC_WORKERS" -storage-gc-threshold "$STORAGE_GC_THRESHOLD" \
                -node-file-sorted-compression="$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression="$SEGMENT_INDEX_COMPRESSION"
            ;;
        *)
            echo "upgrade-index 仅支持 ethstore backend"
            exit 1
            ;;
    esac
}

run_action() {
    local backend="$1"
    case "$ACTION" in
        load)
            run_load "$backend"
            ;;
        load-account)
            run_load_account "$backend"
            ;;
        load-storage)
            run_load_storage "$backend"
            ;;
        restore)
            run_restore "$backend"
            ;;
        replay)
            run_replay "$backend"
            ;;
        gc)
            run_gc "$backend"
            ;;
        upgrade-index)
            run_upgrade_index "$backend"
            ;;
    esac
}

main() {
    if [ "${ACTION}" = "-h" ] || [ "${ACTION}" = "--help" ]; then
        usage
        exit 0
    fi

    validate_inputs
    validate_runtime_requirements
    build_replay_binary

    if [ "$BACKEND" = "all" ]; then
        for b in ethstore chainkv pebble; do
            echo "==== ${ACTION} ${b} ===="
            run_action "$b"
        done
    else
        echo "==== ${ACTION} ${BACKEND} ===="
        run_action "$BACKEND"
    fi
}

main
