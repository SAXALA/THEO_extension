#!/usr/bin/env bash

# CHUNK_FILE_SIZE_BYTES=8192 TOTAL_CACHE_SIZE_MIB=512 ./replay.sh load-account prefixdb

# sleep 10

echo "Backup loaded account data"
cp -r /mnt/ssd2/loaded/ethstore/database_statedb8KB /mnt/ssd2/loaded/ethstore/database_statedb8KB_bak

sleep 10

CHUNK_FILE_SIZE_BYTES=8192 TOTAL_CACHE_SIZE_MIB=512 PREFIXDB_ACCOUNT_STATE_DIR=/mnt/ssd2/loaded/ethstore/database_statedb8KB ./replay.sh load-storage prefixdb
