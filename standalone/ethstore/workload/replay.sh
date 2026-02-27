date +%Y-%m-%d-%H-%M-%S
chunk_file_size_4KB=4096
chunk_file_size_16KB=16384
chunk_file_size_64KB=65536
chunk_file_size_256KB=262144
cache_size_1MB=1048576
cache_size_8MB=8388608
cache_size_64MB=67108864
cache_size_512MB=536870912
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

# replay trace ethstore
# 1. recover from bak data
echo "Start rsync database files from DBbak to current database directory..."
sudo rsync -avP /mnt/ssd2/ethstore/database/database_statedb16KB/prefixdb/ /mnt/ssd2/ethstore/database_state/prefixdb/
sudo chmod -R 777 /mnt/ssd2/ethstore/database_state/prefixdb/
sudo rsync -avP /mnt/ssd2/ethstore/DBbak/database/aol/ /mnt/ssd2/ethstore/database/aol/
sudo chmod -R 777 /mnt/ssd2/ethstore/database/aol/
sudo rsync -avP /mnt/ssd2/ethstore/DBbak/database_pebble/ /mnt/ssd2/ethstore/database_pebble/
sudo chmod -R 777 /mnt/ssd2/ethstore/database_pebble/
# # 2. replay trace
echo "Start replaying trace with re mode..."


./bin/replayWorkload -mode re -max-ops 100000000> ./replayLog/ethstoreLog_${log_date}.log 2>&1 &
replay_pid=$!
echo "monitor target PID: $replay_pid"
# 3. record resource usage
sudo ./monitor.sh "$replay_pid" 1 ethstoreIO_${log_date}.log &
wait  # wait for the replay to finish