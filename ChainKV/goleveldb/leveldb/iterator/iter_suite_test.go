package iterator_test

import (
	"testing"

	"theo.local/ChainKV/goleveldb/leveldb/testutil"
)

func TestIterator(t *testing.T) {
	testutil.RunSuite(t, "Iterator Suite")
}
