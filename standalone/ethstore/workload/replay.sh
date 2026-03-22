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
END_BLOCK_ID="${END_BLOCK_ID:-20500200}"
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
NODE_FILE_SORTED_COMPRESSION="${NODE_FILE_SORTED_COMPRESSION:-true}"
# segment index 是否启用 zstd 压缩；默认开启
SEGMENT_INDEX_COMPRESSION="${SEGMENT_INDEX_COMPRESSION:-true}"
# 统一 GC worker 数；默认使用系统 CPU 数量的一半，最少 1
DEFAULT_GC_WORKERS=$(($(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 1)))
if [ "$DEFAULT_GC_WORKERS" -lt 1 ]; then
    DEFAULT_GC_WORKERS=1
fi
GC_WORKERS="${GC_WORKERS:-${NODE_FILE_GC_WORKERS:-$DEFAULT_GC_WORKERS}}"

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
# chainkv 参数: 逗号分隔 key 前缀列表；空字符串表示不过滤
CHAINKV_STATE_KEY_PREFIXES="${CHAINKV_STATE_KEY_PREFIXES:-}"
# chainkv load 限制条数；0 代表不限制
CHAINKV_LOAD_LIMIT="${CHAINKV_LOAD_LIMIT:-0}"
# 多轮回放参数（由 multiple_replay.sh 注入）
RUN_ROUND="${RUN_ROUND:-0}"
RUN_ROUNDS="${RUN_ROUNDS:-0}"

# 已加载数据根目录（source）与回放运行目录（target）
LOADED_ROOT="${LOADED_ROOT:-/mnt/ssd2/loaded}"
RUNNING_ROOT="${RUNNING_ROOT:-/mnt/ssd2/running}"
DISK_MOUNT_POINT="/mnt/ssd2"

# ethstore statedb 目录名，可选: database_statedb4KB | database_statedb8KB | database_statedb16KB | database_statedb64KB | database_statedb256KB
calculate_default_ethstore_statedb_dirname() {
    case "$CHUNK_FILE_SIZE_BYTES" in
        4096) echo "database_statedb4KB" ;;
        8192) echo "database_statedb8KB" ;;
        16384) echo "database_statedb16KB" ;;
        65536) echo "database_statedb64KB" ;;
        262144) echo "database_statedb256KB" ;;
        *) echo "Invalid CHUNK_FILE_SIZE_BYTES=${CHUNK_FILE_SIZE_BYTES}. Supported values: 4096, 8192, 16384, 65536, 262144" >&2; exit 1 ;;
    esac
}
ETHSTORE_STATEDB_DIRNAME="${ETHSTORE_STATEDB_DIRNAME:-$(calculate_default_ethstore_statedb_dirname)}"

# 手动 GC 目录：直接在该 statedb 目录执行，不进行复制
GC_STATE_DIR="${GC_STATE_DIR:-/mnt/ssd2/loaded/ethstore/${ETHSTORE_STATEDB_DIRNAME}}"
# segment index 升级目录：直接在该 statedb 目录执行，不进行复制
UPGRADE_STATE_DIR="${UPGRADE_STATE_DIR:-${GC_STATE_DIR}}"
# prefixdb storage 阶段要求给出已经 load 完 account 的 statedb 目录
PREFIXDB_ACCOUNT_STATE_DIR="${PREFIXDB_ACCOUNT_STATE_DIR:-}"

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
    CHAINKV_STATE(true|false), CHAINKV_STATE_KEY_PREFIXES(csv), CHAINKV_LOAD_LIMIT(0=unlimited)
    LOADED_ROOT RUNNING_ROOT ETHSTORE_STATEDB_DIRNAME
    GC_STATE_DIR
    UPGRADE_STATE_DIR
    ETHSTORE_PREFIXDB_DIR SUDO_PASSWD

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
}

build_replay_binary() {
    if ! go mod download; then
        echo "依赖下载失败，请检查网络或 GOPROXY 配置（当前 GOPROXY=$GOPROXY）"
        exit 1
    fi
    mkdir -p ./bin
    if ! GOAMD64=v4 go build -trimpath -ldflags="-s -w" -o ./bin/replayWorkload ./replayWorkload.go; then
        echo "构建 replayWorkload 失败，退出。"
        exit 1
    fi
}

drop_caches() {
    echo "Drop caches"
    sudo_run sh -c 'echo 1 > /proc/sys/vm/drop_caches'
    sudo_run sh -c 'echo 2 > /proc/sys/vm/drop_caches'
    sudo_run sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    # Trim the target SSD to minimize the impact of leftover data on performance.
    sudo_run fstrim -v "$DISK_MOUNT_POINT"
}

restore_ethstore_db() {
    echo "Restore ethstore database..."
    local src_root="${LOADED_ROOT}/ethstore"
    local dst_prefix="${RUNNING_ROOT}/ethstore"
    sudo_rsync_run -avP --delete "${src_root}/database_aol/" "${dst_prefix}_aol/"
    sudo_run chmod -R 777 "${dst_prefix}_aol/"
    sudo_rsync_run -avP --delete "${src_root}/database_pebble/" "${dst_prefix}_pebble/"
    sudo_run chmod -R 777 "${dst_prefix}_pebble/"
    sudo_rsync_run -avP --delete "${src_root}/${ETHSTORE_STATEDB_DIRNAME}/" "${dst_prefix}_state/"
    sudo_run chmod -R 777 "${dst_prefix}_state/"
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

sync_ethstore_loaded_to_running() {
    ensure_ethstore_statedb_dirname
    local src_root="${LOADED_ROOT}/ethstore"
    local dst_prefix="${RUNNING_ROOT}/ethstore"

    local src_aol="${src_root}/database_aol"
    local src_pebble="${src_root}/database_pebble"
    local src_state="${src_root}/${ETHSTORE_STATEDB_DIRNAME}"

    if [ ! -d "${src_aol}" ] || [ ! -d "${src_pebble}" ] || [ ! -d "${src_state}" ]; then
        echo "ethstore source directories missing under ${src_root}. Required: database_aol, database_pebble, ${ETHSTORE_STATEDB_DIRNAME}"
        exit 1
    fi

    echo "Sync ethstore data: ${src_root} -> ${dst_prefix}{_aol,_pebble,_state}"
    sudo_run mkdir -p "${dst_prefix}_aol" "${dst_prefix}_pebble" "${dst_prefix}_state"
    sudo_rsync_run -avP --delete "${src_aol}/" "${dst_prefix}_aol/"
    sudo_rsync_run -avP --delete "${src_pebble}/" "${dst_prefix}_pebble/"
    sudo_rsync_run -avP --delete "${src_state}/" "${dst_prefix}_state/"
    sudo_run chmod -R 777 "${dst_prefix}_aol" "${dst_prefix}_pebble" "${dst_prefix}_state"
}

sync_chainkv_loaded_to_running() {
    local src="${LOADED_ROOT}/chainkv"
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
    local src="${LOADED_ROOT}/pebble"
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
    base_tag="act_${action}_be_${backend}_max_${WORKLOAD_MAX_OPS}_trace_${trace_tag}_db_${dbtype_tag}_block_${START_BLOCK_ID}-${END_BLOCK_ID}"

    local round_tag=""
    if [[ "$RUN_ROUND" =~ ^[0-9]+$ ]] && [ "$RUN_ROUND" -gt 0 ]; then
        round_tag="_r_${RUN_ROUND}"
    fi

    if [ "$backend" = "chainkv" ]; then
        local ckv_state_tag ckv_prefix_tag
        ckv_state_tag=$(sanitize_tag_value "$CHAINKV_STATE")
        ckv_prefix_tag=$(sanitize_tag_value "$CHAINKV_STATE_KEY_PREFIXES")
        printf "%s" "${base_tag}_ckvc_${CHAINKV_CACHE_MB}_ckvh_${CHAINKV_HANDLES}_ckvs_${ckv_state_tag}_ckvp_${ckv_prefix_tag}_ckvl_${CHAINKV_LOAD_LIMIT}${round_tag}"
    elif [ "$backend" = "pebble" ]; then
        printf "%s" "${base_tag}_pbc_${PEBBLE_CACHE_MB}_pbh_${PEBBLE_HANDLES}${round_tag}"
    elif [ "$backend" = "prefixdb" ]; then
        printf "%s" "${base_tag}_cfs_${CHUNK_FILE_SIZE_BYTES}_tcs_${TOTAL_CACHE_SIZE_MIB}_pfh_${PREFIXDB_HANDLES}_ngcr_${NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD}_gcw_${GC_WORKERS}_sgct_${STORAGE_GC_THRESHOLD}_nfsc_${NODE_FILE_SORTED_COMPRESSION}_sic_${SEGMENT_INDEX_COMPRESSION}${round_tag}"
    elif [ "$backend" = "ethstore" ]; then
        printf "%s" "${base_tag}_cfs_${CHUNK_FILE_SIZE_BYTES}_tcs_${TOTAL_CACHE_SIZE_MIB}_pfh_${PREFIXDB_HANDLES}_pbc_${PEBBLE_CACHE_MB}_pbh_${PEBBLE_HANDLES}_cc_${CACHE_COUNT}_ngcr_${NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD}_gcw_${GC_WORKERS}_sgct_${STORAGE_GC_THRESHOLD}_nfsc_${NODE_FILE_SORTED_COMPRESSION}_sic_${SEGMENT_INDEX_COMPRESSION}${round_tag}"
    else
        printf "%s" "${base_tag}${round_tag}"
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
    printf 'TRACE_FILE=%s\n' "$TRACE_FILE"
    printf 'DB_TYPE=%s\n' "$DB_TYPE"
    printf 'RUN_ROUND=%s\n' "$RUN_ROUND"
    printf 'RUN_ROUNDS=%s\n' "$RUN_ROUNDS"
    printf 'ETHSTORE_PREFIXDB_DIR=%s\n' "$ETHSTORE_PREFIXDB_DIR"
    printf 'LOADED_ROOT=%s\n' "$LOADED_ROOT"
    printf 'RUNNING_ROOT=%s\n' "$RUNNING_ROOT"
    printf 'ETHSTORE_STATEDB_DIRNAME=%s\n' "$ETHSTORE_STATEDB_DIRNAME"
    printf 'GC_STATE_DIR=%s\n' "$GC_STATE_DIR"
    printf 'UPGRADE_STATE_DIR=%s\n' "$UPGRADE_STATE_DIR"
    printf 'PREFIXDB_ACCOUNT_STATE_DIR=%s\n' "$PREFIXDB_ACCOUNT_STATE_DIR"

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
        printf 'CHAINKV_STATE_KEY_PREFIXES=%s\n' "$CHAINKV_STATE_KEY_PREFIXES"
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

    ./bin/replayWorkload "$@" >> "$log_file" 2>&1 &
    CURRENT_REPLAY_PID=$!
    echo "${backend} monitor target PID: ${CURRENT_REPLAY_PID}"
    (
        trap - ERR INT TERM EXIT
        set +e
        sudo_run ./monitor.sh "$CURRENT_REPLAY_PID" 1 "$io_file"
    ) &
    CURRENT_MONITOR_PID=$!

    wait "$CURRENT_REPLAY_PID"
    cleanup_running_processes
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
                -node-file-sorted-compression "$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression "$SEGMENT_INDEX_COMPRESSION"
            ;;
        prefixdb)
            echo "prefixdb backend 已拆分为 load-account / load-storage，请使用新的 action"
            exit 1
            ;;
        chainkv)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode ld -backend chainkv -ckv-cache "$CHAINKV_CACHE_MB" -ckv-handles "$CHAINKV_HANDLES" -ckv-state "$CHAINKV_STATE" -ckv-state-key-prefixes "$CHAINKV_STATE_KEY_PREFIXES" -ckv-limit "$CHAINKV_LOAD_LIMIT"
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
                -node-file-sorted-compression "$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression "$SEGMENT_INDEX_COMPRESSION"
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
                -node-file-sorted-compression "$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression "$SEGMENT_INDEX_COMPRESSION"
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

    local run_tag
    run_tag=$(build_run_tag "replay" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"
    local io_file="./replayLog/${run_tag}_io_${log_date}.log"
    case "$backend" in
        ethstore)
            ensure_ethstore_permissions
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode re -backend ethstore -max-ops "$WORKLOAD_MAX_OPS" -db-type "$DB_TYPE" -trace-file "$TRACE_FILE" -cache-count "$CACHE_COUNT" \
                -start-block-id "$START_BLOCK_ID" -end-block-id "$END_BLOCK_ID" \
                -contract-chunk-file-size-bytes "$CHUNK_FILE_SIZE_BYTES" -total-cache-size-mib "$TOTAL_CACHE_SIZE_MIB" -prefixdb-handles "$PREFIXDB_HANDLES" -pebble-cache "$PEBBLE_CACHE_MB" -pebble-handles "$PEBBLE_HANDLES" \
                -node-file-gc-unsorted-ratio-threshold "$NODE_FILE_GC_UNSORTED_RATIO_THRESHOLD" -gc-workers "$GC_WORKERS" -storage-gc-threshold "$STORAGE_GC_THRESHOLD" \
                -node-file-sorted-compression "$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression "$SEGMENT_INDEX_COMPRESSION"
            ;;
        chainkv)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode re -backend chainkv -max-ops "$WORKLOAD_MAX_OPS" -db-type "$DB_TYPE" -trace-file "$TRACE_FILE" -start-block-id "$START_BLOCK_ID" -end-block-id "$END_BLOCK_ID" -ckv-cache "$CHAINKV_CACHE_MB" -ckv-handles "$CHAINKV_HANDLES" -ckv-state "$CHAINKV_STATE" -ckv-state-key-prefixes "$CHAINKV_STATE_KEY_PREFIXES"
            ;;
        pebble)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode re -backend pebble -max-ops "$WORKLOAD_MAX_OPS" -db-type "$DB_TYPE" -trace-file "$TRACE_FILE" -start-block-id "$START_BLOCK_ID" -end-block-id "$END_BLOCK_ID" -pebble-cache "$PEBBLE_CACHE_MB" -pebble-handles "$PEBBLE_HANDLES"
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
                -node-file-sorted-compression "$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression "$SEGMENT_INDEX_COMPRESSION"
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
                -node-file-sorted-compression "$NODE_FILE_SORTED_COMPRESSION" -segment-index-compression "$SEGMENT_INDEX_COMPRESSION"
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
    if [ "$ACTION" != "restore" ]; then
        build_replay_binary
    fi

    if [ "$BACKEND" = "all" ]; then
        for b in ethstore chainkv pebble; do
            echo "==== ${ACTION} ${b} ===="
            if [ "$ACTION" = "load" ] || [ "$ACTION" = "load-account" ] || [ "$ACTION" = "load-storage" ]; then
                drop_caches
            fi
            run_action "$b"
        done
    else
        echo "==== ${ACTION} ${BACKEND} ===="
        if [ "$ACTION" = "load" ] || [ "$ACTION" = "load-account" ] || [ "$ACTION" = "load-storage" ]; then
            drop_caches
        fi
        run_action "$BACKEND"
    fi
}

main
