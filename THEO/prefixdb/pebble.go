package prefixdb

import "theo.local/THEO/pebblestore"

// PebbleStore is a type alias for pebblestore.PebbleStore.
// prefixdb uses it internally for the account-hash-key index.
type PebbleStore = pebblestore.PebbleStore

// NewPebbleStore creates (or opens) a PebbleStore at the given path.
var NewPebbleStore = pebblestore.NewPebbleStore
