#!/bin/bash
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

# Ensure Go module download works under sudo/root env
export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export GOSUMDB="${GOSUMDB:-sum.golang.google.cn}"

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
# sudo rsync -avz --progress /mnt/ssd2/ethstore/DBbak/baseline/ /mnt/ssd2/ethstore/baseline/
# # 2. replay trace
# ./bin/replayWorkload -mode rb > ./replayLog/baseline_replay_${log_date}.log 2>&1 &
# replay_pid=$!
# echo "monitor target PID: $replay_pid"
# # 3. record resource usage
# sudo ./monitor.sh "$replay_pid" 1 baselineIO_${log_date}.log &
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

case "$WORKLOAD_BACKEND" in
    ethstore)
        echo "Selected backend: ethstore"
        echo "Start rsync database files from DBbak to current database directory..."
        sudo rsync -avP /mnt/ssd2/ethstore/database/database_statedb16KB/prefixdb/ /mnt/ssd2/ethstore/database_state/prefixdb/
        sudo chmod -R 777 /mnt/ssd2/ethstore/database_state/prefixdb/
        sudo rsync -avP /mnt/ssd2/ethstore/DBbak/database_aol /mnt/ssd2/ethstore/database_aol/
        sudo chmod -R 777 /mnt/ssd2/ethstore/database_aol/
        sudo rsync -avP /mnt/ssd2/ethstore/DBbak/database_pebble/ /mnt/ssd2/ethstore/database_pebble/
        sudo chmod -R 777 /mnt/ssd2/ethstore/database_pebble/
        replay_cmd=(./bin/replayWorkload -mode re -backend ethstore -max-ops "$WORKLOAD_MAX_OPS")
        ;;
    chainkv)
        echo "Selected backend: chainkv"
        sudo rsync -avz --progress /mnt/ssd2/ethstore/DBbak/chainkv/ /mnt/ssd2/ethstore/chainkv/
        replay_cmd=(./bin/replayWorkload -mode re -backend chainkv -max-ops "$WORKLOAD_MAX_OPS" -ckv-cache "$CHAINKV_CACHE_MB" -ckv-handles "$CHAINKV_HANDLES" -ckv-state "$CHAINKV_STATE" -ckv-state-key-prefixes "$CHAINKV_STATE_KEY_PREFIXES" -ckv-limit "$CHAINKV_LOAD_LIMIT")
        ;;
    pebble)
        echo "Selected backend: pebble (baseline)"
        sudo rsync -avz --progress /mnt/ssd2/ethstore/DBbak/baseline/ /mnt/ssd2/ethstore/baseline/
        replay_cmd=(./bin/replayWorkload -mode rb -max-ops "$WORKLOAD_MAX_OPS")
        ;;
    *)
        echo "不支持的 WORKLOAD_BACKEND=$WORKLOAD_BACKEND，支持: ethstore | chainkv | pebble"
        exit 1
        ;;
esac

echo "Start replaying trace... backend=$WORKLOAD_BACKEND"
"${replay_cmd[@]}" > ./replayLog/${WORKLOAD_BACKEND}Log_${log_date}.log 2>&1 &
replay_pid=$!
echo "monitor target PID: $replay_pid"
sudo ./monitor.sh "$replay_pid" 1 ${WORKLOAD_BACKEND}IO_${log_date}.log &
wait