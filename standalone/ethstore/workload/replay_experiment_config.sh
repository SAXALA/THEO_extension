#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# replay_experiment_config.sh – default experiment configuration
#
# Copy this file and edit the arrays to define a new experiment sequence.
# Then run:  ./multiple_replay.sh [action] [backend] [trace] <your_config.sh>
#
# All variables here are sourced by multiple_replay.sh before the run loop.
# ---------------------------------------------------------------------------

# Fill these arrays with candidate values (MiB / count).
CACHE_SIZE_CANDIDATES=(16)      # e.g. 64 256
CACHE_COUNT_CANDIDATES=(0)      # e.g. 64
COMMIT_BLOCK_INTERVAL_CANDIDATES=(1)
BACKEND_CANDIDATES=(pebble ethstore)   # pebble ethstore
TRACE_FILE_CANDIDATES=(nocache)        # cache nocache_snap nocache
REPLAY_CGROUP_CASE_CANDIDATES=(false)

# Chunk file size in bytes (used by ethstore/prefixdb).
CHUNK_FILE_SIZE_BYTES=8192
