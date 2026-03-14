#!/bin/#!/bin/bash

CHUNK_FILE_SIZE=8192 TOTAL_CACHE_SIZE_MIB=512 ./replay.sh load prefixdb

sleep 10

./multiple_replay.sh