package leveldb

import (
	"testing"

	"theo.local/ChainKV/goleveldb/leveldb/testutil"
)

func TestLevelDB(t *testing.T) {
	testutil.RunSuite(t, "LevelDB Suite")
}
