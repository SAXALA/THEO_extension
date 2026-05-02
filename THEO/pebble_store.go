package ethstore

import "theo.local/THEO/pebblestore"

// PebbleStore is the PebbleDB-backed key-value store used both as an internal
// component of ethstore and as a standalone baseline for benchmarks.
// It is a type alias for pebblestore.PebbleStore so the two types are identical.
type PebbleStore = pebblestore.PebbleStore

// NewPebbleStore creates (or opens) a PebbleStore at the given path.
var NewPebbleStore = pebblestore.NewPebbleStore
