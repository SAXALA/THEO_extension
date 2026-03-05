#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir" || exit 1

set -euo pipefail

# 可执行动作: load(加载数据) | restore(恢复数据库目录) | replay(回放trace)
ACTIONS="load restore replay"
# 后端类型: ethstore | chainkv | pebble | all(依次执行前三者)
BACKENDS="ethstore chainkv pebble all"

# 位置参数1: ACTION，可选值见 ACTIONS，默认 replay
ACTION="${1:-replay}"
# 位置参数2: BACKEND，可选值见 BACKENDS，默认 ethstore
BACKEND="${2:-${WORKLOAD_BACKEND:-ethstore}}"

# 回放最大操作数；0 代表不限制
WORKLOAD_MAX_OPS="${WORKLOAD_MAX_OPS:-100000000}"
# trace 文件类型，可选值: cache | nocache | nocache_snap
TRACE_FILE="${TRACE_FILE:-nocache_snap}"
# 仅对 ethstore/pebble 回放生效；可选值: all | aol | prefixdb | pebble
DB_TYPE="${DB_TYPE:-all}"
# ethstore 回放参数，storage chunk cache 数量
CACHE_COUNT="${CACHE_COUNT:-16}"

# ethstore load 参数: chunk 文件大小（字节），如 4096/16384/65536/262144
LD_CHUNK_FILE_SIZE="${LD_CHUNK_FILE_SIZE:-65536}"
# ethstore load 参数: prefixdb cache 大小（字节）
LD_CACHE_SIZE="${LD_CACHE_SIZE:-536870912}"

# chainkv 参数: cache 大小（MB）
CHAINKV_CACHE_MB="${CHAINKV_CACHE_MB:-16}"
# chainkv 参数: leveldb handles 数量
CHAINKV_HANDLES="${CHAINKV_HANDLES:-128}"
# chainkv 参数: true/false，是否启用 state 特化路径（Put_s/Get_s）
CHAINKV_STATE="${CHAINKV_STATE:-true}"
# chainkv 参数: 逗号分隔 key 前缀列表；空字符串表示不过滤
CHAINKV_STATE_KEY_PREFIXES="${CHAINKV_STATE_KEY_PREFIXES:-}"
# chainkv load 限制条数；0 代表不限制
CHAINKV_LOAD_LIMIT="${CHAINKV_LOAD_LIMIT:-0}"

# restore 路径: 实际数据根目录
RESTORE_ROOT="${RESTORE_ROOT:-}"
# restore 路径: 备份根目录
RESTORE_BAK_ROOT="${RESTORE_BAK_ROOT:-}"
# ethstore prefixdb 目录（用于权限预检查）
ETHSTORE_PREFIXDB_DIR="${ETHSTORE_PREFIXDB_DIR:-}"

log_date=$(date +%m-%d-%H-%M-%S)
log_dir="./replayLog"
mkdir -p "$log_dir"
mkdir -p "${log_dir}/IO"

# Track running child processes so signal handlers can stop them cleanly.
CURRENT_REPLAY_PID=""
CURRENT_MONITOR_PID=""

# sudo 密码；留空则使用交互式 sudo
SUDO_PASSWD="${SUDO_PASSWD:-}"

sudo_run() {
    if [ -n "${SUDO_PASSWD}" ]; then
        echo "${SUDO_PASSWD}" | sudo -S "$@"
    else
        sudo "$@"
    fi
}

terminate_pid_tree() {
    local pid="$1"
    if [ -z "$pid" ]; then
        return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
        return 0
    fi

    # Best effort: stop direct children first, then parent process.
    local children
    children=$(pgrep -P "$pid" 2>/dev/null || true)
    if [ -n "$children" ]; then
        # shellcheck disable=SC2086
        kill -TERM $children 2>/dev/null || true
    fi
    kill -TERM "$pid" 2>/dev/null || true
}

cleanup_running_processes() {
    terminate_pid_tree "$CURRENT_MONITOR_PID"
    terminate_pid_tree "$CURRENT_REPLAY_PID"
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
Usage: $0 [load|restore|replay] [ethstore|chainkv|pebble|all]

Current values:
  action=${ACTION}
  backend=${BACKEND}

Common env vars:
    WORKLOAD_MAX_OPS(0=unlimited)
    TRACE_FILE(cache|nocache|nocache_snap)
    DB_TYPE(all|aol|prefixdb|pebble)
    CACHE_COUNT
    LD_CHUNK_FILE_SIZE(bytes), LD_CACHE_SIZE(bytes)
    CHAINKV_CACHE_MB, CHAINKV_HANDLES
    CHAINKV_STATE(true|false), CHAINKV_STATE_KEY_PREFIXES(csv), CHAINKV_LOAD_LIMIT(0=unlimited)
    RESTORE_ROOT RESTORE_BAK_ROOT ETHSTORE_PREFIXDB_DIR SUDO_PASSWD

Required by action/backend:
    restore: RESTORE_ROOT + RESTORE_BAK_ROOT
    load/replay ethstore: ETHSTORE_PREFIXDB_DIR
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
    local needs_restore_paths="false"
    local needs_ethstore_prefixdb="false"

    if [ "$ACTION" = "restore" ]; then
        needs_restore_paths="true"
    fi

    if [ "$ACTION" = "load" ] || [ "$ACTION" = "replay" ]; then
        if [ "$BACKEND" = "ethstore" ] || [ "$BACKEND" = "all" ]; then
            needs_ethstore_prefixdb="true"
        fi
    fi

    if [ "$needs_restore_paths" = "true" ]; then
        if [ -z "$RESTORE_ROOT" ] || [ -z "$RESTORE_BAK_ROOT" ]; then
            echo "restore 需要配置 RESTORE_ROOT 和 RESTORE_BAK_ROOT，当前为空。"
            usage
            exit 1
        fi
    fi

    if [ "$needs_ethstore_prefixdb" = "true" ]; then
        if [ -z "$ETHSTORE_PREFIXDB_DIR" ]; then
            echo "ethstore 的 load/replay 需要配置 ETHSTORE_PREFIXDB_DIR，当前为空。"
            usage
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
}

restore_ethstore_db() {
    echo "Restore ethstore database..."
    sudo_run rsync -avP --delete "${RESTORE_BAK_ROOT}/database_state/prefixdb/" "${RESTORE_ROOT}/database_state/prefixdb/"
    sudo_run chmod -R 777 "${RESTORE_ROOT}/database_state/prefixdb/"
    sudo_run rsync -avP --delete "${RESTORE_BAK_ROOT}/database_aol/" "${RESTORE_ROOT}/database_aol/"
    sudo_run chmod -R 777 "${RESTORE_ROOT}/database_aol/"
    sudo_run rsync -avP --delete "${RESTORE_BAK_ROOT}/database_pebble/" "${RESTORE_ROOT}/database_pebble/"
    sudo_run chmod -R 777 "${RESTORE_ROOT}/database_pebble/"
}

restore_chainkv_db() {
    echo "Restore chainkv baseline database..."
    sudo_run rsync -avP --delete "${RESTORE_BAK_ROOT}/baseline/chainkv/" "${RESTORE_ROOT}/baseline/chainkv/"
    sudo_run chmod -R 777 "${RESTORE_ROOT}/baseline/chainkv/"
}

restore_pebble_db() {
    echo "Restore pebble baseline database..."
    sudo_run rsync -avP --delete "${RESTORE_BAK_ROOT}/baseline/pebble/" "${RESTORE_ROOT}/baseline/pebble/"
    sudo_run chmod -R 777 "${RESTORE_ROOT}/baseline/pebble/"
}

ensure_ethstore_permissions() {
    echo "Fix ethstore permissions (prefixdb + accountHash_key_pebble)..."
    # Ensure the lock directory exists and is writable to avoid LOCK permission denied.
    sudo_run mkdir -p "${ETHSTORE_PREFIXDB_DIR}/accountHash_key_pebble"
    sudo_run chmod -R 777 "${ETHSTORE_PREFIXDB_DIR}"
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

    local trace_tag dbtype_tag restore_root_tag restore_bak_tag
    trace_tag=$(sanitize_tag_value "$TRACE_FILE")
    dbtype_tag=$(sanitize_tag_value "$DB_TYPE")
    restore_root_tag=$(sanitize_tag_value "$RESTORE_ROOT")
    restore_bak_tag=$(sanitize_tag_value "$RESTORE_BAK_ROOT")

    local base_tag
    base_tag="act_${action}_be_${backend}_max_${WORKLOAD_MAX_OPS}_trace_${trace_tag}_db_${dbtype_tag}_cc_${CACHE_COUNT}_ldc_${LD_CHUNK_FILE_SIZE}_ldm_${LD_CACHE_SIZE}_rr_${restore_root_tag}_rb_${restore_bak_tag}"

    if [ "$backend" = "chainkv" ]; then
        local ckv_state_tag ckv_prefix_tag
        ckv_state_tag=$(sanitize_tag_value "$CHAINKV_STATE")
        ckv_prefix_tag=$(sanitize_tag_value "$CHAINKV_STATE_KEY_PREFIXES")
        printf "%s" "${base_tag}_ckvc_${CHAINKV_CACHE_MB}_ckvh_${CHAINKV_HANDLES}_ckvs_${ckv_state_tag}_ckvp_${ckv_prefix_tag}_ckvl_${CHAINKV_LOAD_LIMIT}"
    else
        printf "%s" "$base_tag"
    fi
}

print_param_snapshot() {
    local snapshot_action="$1"
    local snapshot_backend="$2"
    cat <<EOF
==== Runtime Parameters ====
ACTION=${snapshot_action}
BACKEND=${snapshot_backend}
WORKLOAD_MAX_OPS=${WORKLOAD_MAX_OPS}
TRACE_FILE=${TRACE_FILE}
DB_TYPE=${DB_TYPE}
CACHE_COUNT=${CACHE_COUNT}
LD_CHUNK_FILE_SIZE=${LD_CHUNK_FILE_SIZE}
LD_CACHE_SIZE=${LD_CACHE_SIZE}
CHAINKV_CACHE_MB=${CHAINKV_CACHE_MB}
CHAINKV_HANDLES=${CHAINKV_HANDLES}
CHAINKV_STATE=${CHAINKV_STATE}
CHAINKV_STATE_KEY_PREFIXES=${CHAINKV_STATE_KEY_PREFIXES}
CHAINKV_LOAD_LIMIT=${CHAINKV_LOAD_LIMIT}
RESTORE_ROOT=${RESTORE_ROOT}
RESTORE_BAK_ROOT=${RESTORE_BAK_ROOT}
ETHSTORE_PREFIXDB_DIR=${ETHSTORE_PREFIXDB_DIR}
============================
EOF
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
    sudo_run ./monitor.sh "$CURRENT_REPLAY_PID" 1 "$io_file" &
    CURRENT_MONITOR_PID=$!

    wait "$CURRENT_REPLAY_PID"
    cleanup_running_processes
}

run_load() {
    local backend="$1"
    local run_tag
    run_tag=$(build_run_tag "load" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"
    local io_file="./replayLog/IO/${run_tag}_io_${log_date}.log"
    case "$backend" in
        ethstore)
            ensure_ethstore_permissions
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode ld -backend ethstore -ld-chunk-file-size "$LD_CHUNK_FILE_SIZE" -ld-cache-size "$LD_CACHE_SIZE"
            ;;
        chainkv)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode ld -backend chainkv -ckv-cache "$CHAINKV_CACHE_MB" -ckv-handles "$CHAINKV_HANDLES" -ckv-state "$CHAINKV_STATE" -ckv-state-key-prefixes "$CHAINKV_STATE_KEY_PREFIXES" -ckv-limit "$CHAINKV_LOAD_LIMIT"
            ;;
        pebble)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode ld -backend pebble
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
    local run_tag
    run_tag=$(build_run_tag "replay" "$backend")
    local log_file="./replayLog/${run_tag}_${log_date}.log"
    local io_file="./replayLog/IO/${run_tag}_io_${log_date}.log"
    case "$backend" in
        ethstore)
            ensure_ethstore_permissions
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode re -backend ethstore -max-ops "$WORKLOAD_MAX_OPS" -db-type "$DB_TYPE" -trace-file "$TRACE_FILE" -cache-count "$CACHE_COUNT"
            ;;
        chainkv)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode re -backend chainkv -max-ops "$WORKLOAD_MAX_OPS" -trace-file "$TRACE_FILE" -ckv-cache "$CHAINKV_CACHE_MB" -ckv-handles "$CHAINKV_HANDLES" -ckv-state "$CHAINKV_STATE" -ckv-state-key-prefixes "$CHAINKV_STATE_KEY_PREFIXES"
            ;;
        pebble)
            run_and_monitor "$backend" "$log_file" "$io_file" \
                -mode rb -max-ops "$WORKLOAD_MAX_OPS" -db-type "$DB_TYPE" -trace-file "$TRACE_FILE"
            ;;
    esac
}

run_action() {
    local backend="$1"
    case "$ACTION" in
        load)
            run_load "$backend"
            ;;
        restore)
            run_restore "$backend"
            ;;
        replay)
            run_replay "$backend"
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
            if [ "$ACTION" = "replay" ] || [ "$ACTION" = "load" ]; then
                drop_caches
            fi
            run_action "$b"
        done
    else
        echo "==== ${ACTION} ${BACKEND} ===="
        if [ "$ACTION" = "replay" ] || [ "$ACTION" = "load" ]; then
            drop_caches
        fi
        run_action "$BACKEND"
    fi
}

main
