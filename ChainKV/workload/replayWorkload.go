package main

import (
	_ "net/http/pprof"

	chainkvdb "github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/ethdb"
	// Please replace "ethstore_module" with the actual module path defined in your ethstore/go.mod file
)

// chainKVLDB wraps ChainKV's LDBDatabase to satisfy kvStore.
type chainKVLDB struct {
	db       *chainkvdb.LDBDatabase
	useState bool // if true, use Put_s/Get_s for state data; else use Put/Get
}
