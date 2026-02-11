package leveldb

import (
	"testing"

	"github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/testutil"
)

func TestLevelDB(t *testing.T) {
	testutil.RunSuite(t, "LevelDB Suite")
}
