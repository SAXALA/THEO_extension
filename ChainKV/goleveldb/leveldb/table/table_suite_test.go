package table

import (
	"testing"

	"theo.local/ChainKV/goleveldb/leveldb/testutil"
)

func TestTable(t *testing.T) {
	testutil.RunSuite(t, "Table Suite")
}
