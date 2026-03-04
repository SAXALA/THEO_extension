#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

# Always run relative to this script's directory so that ./replayLog paths
# are stable no matter where the script is invoked from.
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir" || exit 1

set -euo pipefail

date +%Y-%m-%d-%H-%M-%S
chunk_file_size_4KB=4096
chunk_file_size_16KB=16384
chunk_file_size_64KB=65536
chunk_file_size_256KB=262144
WORKLOAD_BACKEND="${WORKLOAD_BACKEND:-ethstore}" # ethstore | chainkv | pebble
WORKLOAD_MAX_OPS="${WORKLOAD_MAX_OPS:-100000000}"
CHAINKV_CACHE_MB="${CHAINKV_CACHE_MB:-16}"
CHAINKV_HANDLES="${CHAINKV_HANDLES:-128}"
CHAINKV_STATE="${CHAINKV_STATE:-true}"
CHAINKV_STATE_KEY_PREFIXES="${CHAINKV_STATE_KEY_PREFIXES:-}"
CHAINKV_LOAD_LIMIT="${CHAINKV_LOAD_LIMIT:-0}"
log_date=$(date +%m-%d-%H-%M-%S)  # for log file name
log_dir="./replayLog"
if [ ! -d "$log_dir" ]; then
    mkdir -p "$log_dir"
fi
mkdir -p "${log_dir}/IO"

# Optional: if you want fully non-interactive sudo, export SUDO_PASSWD.
# Leaving it empty will fall back to normal sudo (may prompt).
SUDO_PASSWD="${SUDO_PASSWD:-}"

sudo_run() {
    if [ -n "${SUDO_PASSWD}" ]; then
        echo "${SUDO_PASSWD}" | sudo -S "$@"
    else
        sudo "$@"
    fi
}

# Ensure Go module download works under sudo/root env
export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export GOSUMDB="${GOSUMDB:-sum.golang.google.cn}"


drop_caches() {
    # clean system caches
    echo "Drop caches"
    sudo_run sh -c 'echo 1 > /proc/sys/vm/drop_caches'
    sudo_run sh -c 'echo 2 > /proc/sys/vm/drop_caches'
    sudo_run sh -c 'echo 3 > /proc/sys/vm/drop_caches'
}

# Pre-download dependencies to fail fast with clear error logs
if ! go mod download; then
    echo "依赖下载失败，请检查网络或 GOPROXY 配置（当前 GOPROXY=$GOPROXY）"
    exit 1
fi

mkdir -p ./bin
if ! GOAMD64=v4 go build -trimpath -ldflags="-s -w" -o ./bin/replayWorkload ./replayWorkload.go; then
    echo "构建 replayWorkload 失败，退出。"
    exit 1
fi

# # baseline replay 
# # 1. recover from baseline data
# sudo rsync -avz --progress --delete /mnt/ssd2/ethstore/DBbak/baseline/ /mnt/ssd2/ethstore/baseline/
# drop_caches
# # 2. replay trace
# ./bin/replayWorkload -mode rb > ./replayLog/baseline_replay_${log_date}.log 2>&1 &
# replay_pid=$!
# echo "monitor target PID: $replay_pid"
# # 3. record resource usage
# sudo ./monitor.sh "$replay_pid" 1 ./replay/IO/baselineIO_${log_date}.log &
# wait  # wait for the replay to finish

# load account
# 4KB
# echo "Start loading accounts with 4KB chunk file size and 512MB cache size..."
# go run replayWorkload.go -mode ld -ld-db-type state -ld-chunk-file-size $chunk_file_size_4KB -ld-cache-size $cache_size_512MB > ./replayLog/loadAccount_stateDB_4KB_512MB_${log_date}.log 2>&1 
# go run replayWorkload.go -mode ld -ld-db-type snapshot -ld-chunk-file-size $chunk_file_size_4KB -ld-cache-size $cache_size_512MB > ./replayLog/loadAccount_snapshot_4KB_512MB_${log_date}.log 2>&1 
# 16KB
# echo "Start loading accounts with 16KB chunk file size and 512MB cache size..."
# ./bin/replayWorkload -mode ld  -ld-chunk-file-size $chunk_file_size_16KB -ld-cache-size $cache_size_512MB > ./replayLog/loadAccount_stateDB_16KB_512MB_${log_date}.log 2>&1
# go run replayWorkload.go -mode ld -ld-db-type snapshot -ld-chunk-file-size $chunk_file_size_16KB -ld-cache-size $cache_size_512MB > ./replayLog/loadAccount_snapshot_16KB_512MB_${log_date}.log 2>&1 
# # # 64KB
# echo "Start loading accounts with 64KB chunk file size and 512MB cache size..."
# # go run replayWorkload.go -mode ld -ld-db-type state -ld-chunk-file-size $chunk_file_size_64KB -ld-cache-size $cache_size_512MB > ./replayLog/loadAccount_stateDB_64KB_512MB_${log_date}.log 2>&1
# go run replayWorkload.go -mode ld -ld-db-type snapshot -ld-chunk-file-size $chunk_file_size_64KB -ld-cache-size $cache_size_512MB > ./replayLog/loadAccount_snapshot_64KB_512MB_${log_date}.log 2>&1 
# # 256KB
# echo "Start loading accounts with 256KB chunk file size and 512MB cache size..."
# go run replayWorkload.go -mode ld -ld-db-type state -ld-chunk-file-size $chunk_file_size_256KB -ld-cache-size $cache_size_512MB > ./replayLog/loadAccount_stateDB_256KB_512MB_${log_date}.log 2>&1
# go run replayWorkload.go -mode ld -ld-db-type snapshot -ld-chunk-file-size $chunk_file_size_256KB -ld-cache-size $cache_size_512MB > ./replayLog/loadAccount_snapshot_256KB_512MB_${log_date}.log 2>&1 

restore_ethstore_db() {
    echo "Start rsync database files from DBbak to current database directory..."
    sudo_run rsync -avP --delete /mnt/ssd2/ethstore/DBbak/database_state/prefixdb/ /mnt/ssd2/ethstore/database_state/prefixdb/
    sudo_run chmod -R 777 /mnt/ssd2/ethstore/database_state/prefixdb/
    sudo_run rsync -avP --delete /mnt/ssd2/ethstore/DBbak/database_aol/ /mnt/ssd2/ethstore/database_aol/
    sudo_run chmod -R 777 /mnt/ssd2/ethstore/database_aol/
    sudo_run rsync -avP --delete /mnt/ssd2/ethstore/DBbak/database_pebble/ /mnt/ssd2/ethstore/database_pebble/
    sudo_run chmod -R 777 /mnt/ssd2/ethstore/database_pebble/
}

restore_baseline_db() {
    sudo_run rsync -avz --progress --delete /mnt/ssd2/ethstore/DBbak/baseline/ /mnt/ssd2/ethstore/baseline/
}

run_and_monitor() {
    local mode="$1"
    local db_type="$2"
    local trace_file="$3"
    local cache_count="$4"
    local log_file="$5"
    local io_file="$6"

    # Avoid overwriting logs when the computed filenames collide (e.g., trace_file is the same).
    # If a target path already exists, append a unique suffix.
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

    if [ "$mode" = "re" ]; then
        ./bin/replayWorkload -mode "$mode" -max-ops "$WORKLOAD_MAX_OPS" -db-type "$db_type" -trace-file "$trace_file" -cache-count "$cache_count" > "$log_file" 2>&1 &
    else
        ./bin/replayWorkload -mode "$mode" -max-ops "$WORKLOAD_MAX_OPS" -db-type "$db_type" -trace-file "$trace_file" -cache-count "$cache_count" > "$log_file" 2>&1 &
    fi
    replay_pid=$!
    echo "monitor target PID: $replay_pid"
    sudo_run ./monitor.sh "$replay_pid" 1 "$io_file" &
    monitor_pid=$!
    wait "$replay_pid"
    wait "$monitor_pid" 2>/dev/null || true
}

# # replay trace ethstore
replay_db_types=(all aol prefixdb pebble)
replay_trace_files=(nocache_snap cache cache cache)
# for i in "${!replay_db_types[@]}"; do
#     restore_ethstore_db
#     drop_caches
#     echo "Start replaying trace with re mode... db-type=${replay_db_types[$i]} trace=${replay_trace_files[$i]}"
#     run_and_monitor \
#         re \
#         "${replay_db_types[$i]}" \
#         "${replay_trace_files[$i]}" \
#         16 \
#         "./replayLog/ethstoreLog_${log_date}_${replay_db_types[$i]}_${replay_trace_files[$i]}.log" \
#         "./replayLog/IO/ethstoreIO_${log_date}_${replay_db_types[$i]}_${replay_trace_files[$i]}.log"
# done

# # baseline replay
# for baseline_db_type in prefixdb aol pebble; do
#     restore_baseline_db
#     drop_caches
#     echo "Start baseline replay with rb mode... db-type=${baseline_db_type}"
#     run_and_monitor \
#         rb \
#         "$baseline_db_type" \
#         cache \
#         16 \
#         "./replayLog/baseline_replay_${log_date}_${baseline_db_type}.log" \
#         "./replayLog/IO/baselineIO_${log_date}_${baseline_db_type}.log"
# done

# restore_ethstore_db
# drop_caches
# echo "Start replaying trace with re mode... db-type=${replay_db_types[0]} trace=${replay_trace_files[1]}"
#     run_and_monitor \
#         re \
#         "${replay_db_types[0]}" \
#         "${replay_trace_files[1]}" \
#         "./replayLog/ethstoreLog_${log_date}_${replay_db_types[0]}_${replay_trace_files[1]}.log" \
#         "./replayLog/IO/ethstoreIO_${log_date}_${replay_db_types[0]}_${replay_trace_files[1]}.log"

# restore_ethstore_db
# drop_caches
# echo "Start replaying trace with re mode... db-type=${replay_db_types[0]} trace=${replay_trace_files[0]}"
#     run_and_monitor \
#         re \
#         "${replay_db_types[0]}" \
#         "${replay_trace_files[0]}" \
#         "./replayLog/ethstoreLog_${log_date}_${replay_db_types[0]}_${replay_trace_files[0]}.log" \
#         "./replayLog/IO/ethstoreIO_${log_date}_${replay_db_types[0]}_${replay_trace_files[0]}.log"

# restore_baseline_db
# drop_caches
# echo "Start baseline replay with rb mode... db-type=all"
# run_and_monitor \
#     rb \
#     all \
#     cache \
#     16 \
#     "./replayLog/baseline_replay_${log_date}_all_cache.log" \
#     "./replayLog/IO/baselineIO_${log_date}_all_cache.log"


restore_ethstore_db
drop_caches
echo "Start replaying trace with re mode... db-type=${replay_db_types[0]} trace=${replay_trace_files[0]}"
    run_and_monitor \
        re \
        "${replay_db_types[0]}" \
        "${replay_trace_files[0]}" \
        16 \
        "./replayLog/ethstoreLog_${log_date}_${replay_db_types[0]}_${replay_trace_files[0]}.log" \
        "./replayLog/IO/ethstoreIO_${log_date}_${replay_db_types[0]}_${replay_trace_files[0]}.log"

restore_ethstore_db
drop_caches
echo "Start replaying trace with re mode... db-type=${replay_db_types[0]} trace=${replay_trace_files[0]}"
    run_and_monitor \
        re \
        "${replay_db_types[0]}" \
        "${replay_trace_files[0]}" \
        64 \
        "./replayLog/ethstoreLog_${log_date}_${replay_db_types[0]}_${replay_trace_files[0]}.log" \
        "./replayLog/IO/ethstoreIO_${log_date}_${replay_db_types[0]}_${replay_trace_files[0]}.log"

restore_ethstore_db
drop_caches
echo "Start replaying trace with re mode... db-type=${replay_db_types[0]} trace=${replay_trace_files[0]}"
    run_and_monitor \
        re \
        "${replay_db_types[0]}" \
        "${replay_trace_files[0]}" \
        4096 \
        "./replayLog/ethstoreLog_${log_date}_${replay_db_types[0]}_${replay_trace_files[0]}.log" \
        "./replayLog/IO/ethstoreIO_${log_date}_${replay_db_types[0]}_${replay_trace_files[0]}.log"
