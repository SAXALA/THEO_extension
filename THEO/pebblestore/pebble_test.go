package pebblestore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helpers

func openStore(t *testing.T) (*PebbleStore, string) {
	t.Helper()
	dir := t.TempDir()
	ps, err := NewPebbleStore(dir, 0, 0, "", false)
	require.NoError(t, err)
	require.NotNil(t, ps)
	return ps, dir
}

type batchOverlay interface {
	BatchGet(key []byte) ([]byte, bool)
}

// NewPebbleStore

func TestNewPebbleStore_DefaultParams(t *testing.T) {
	ps, dir := openStore(t)
	defer ps.Close()
	_, err := os.Stat(dir)
	require.NoError(t, err)
}

func TestNewPebbleStore_ExplicitParams(t *testing.T) {
	dir := t.TempDir()
	ps, err := NewPebbleStore(dir, 32, 64, "test-ns/", false)
	require.NoError(t, err)
	require.NotNil(t, ps)
	require.NoError(t, ps.Close())
}

func TestNewPebbleStore_InvalidPath(t *testing.T) {
	f, err := os.CreateTemp("", "pebblestore_conflict_*")
	require.NoError(t, err)
	f.Close()
	defer os.Remove(f.Name())
	_, err = NewPebbleStore(f.Name(), 0, 0, "", false)
	assert.Error(t, err)
}

func TestNewPebbleStore_ReadOnly(t *testing.T) {
	dir := t.TempDir()
	ps, err := NewPebbleStore(dir, 0, 0, "", false)
	require.NoError(t, err)
	require.NoError(t, ps.Put([]byte("ro-key"), []byte("ro-val")))
	require.NoError(t, ps.Close())
	ro, err := NewPebbleStore(dir, 0, 0, "", true)
	require.NoError(t, err)
	defer ro.Close()
	v, err := ro.Get([]byte("ro-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("ro-val"), v)
}

// Close

func TestPebbleStore_Close_Idempotent(t *testing.T) {
	ps, _ := openStore(t)
	require.NoError(t, ps.Close())
	require.NoError(t, ps.Close())
}

func TestPebbleStore_Close_OpsReturnErrClosed(t *testing.T) {
	ps, _ := openStore(t)
	require.NoError(t, ps.Close())
	_, _, err := ps.Has([]byte("k"))
	assert.True(t, errors.Is(err, ErrClosed))
	_, err = ps.Get([]byte("k"))
	assert.True(t, errors.Is(err, ErrClosed))
	assert.True(t, errors.Is(ps.Put([]byte("k"), []byte("v")), ErrClosed))
	assert.True(t, errors.Is(ps.Delete([]byte("k")), ErrClosed))
	assert.True(t, errors.Is(ps.Flush(), ErrClosed))
}

// Has

func TestPebbleStore_Has(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	key, val := []byte("has-key"), []byte("has-val")
	require.NoError(t, ps.Put(key, val))
	n, ok, err := ps.Has(key)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, len(val), n)
	_, ok, err = ps.Has([]byte("missing"))
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestPebbleStore_Has_AfterDelete(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	key := []byte("gone-key")
	require.NoError(t, ps.Put(key, []byte("v")))
	require.NoError(t, ps.Delete(key))
	_, ok, err := ps.Has(key)
	require.NoError(t, err)
	assert.False(t, ok)
}

// Get / Put / Delete

func TestPebbleStore_PutGetDelete(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	key, val := []byte("k"), []byte("v")
	require.NoError(t, ps.Put(key, val))
	got, err := ps.Get(key)
	require.NoError(t, err)
	assert.Equal(t, val, got)
	require.NoError(t, ps.Delete(key))
	_, err = ps.Get(key)
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestPebbleStore_Get_NonExistent(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	_, err := ps.Get([]byte("no-such-key"))
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestPebbleStore_Put_Overwrite(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	key := []byte("ow")
	require.NoError(t, ps.Put(key, []byte("first")))
	require.NoError(t, ps.Put(key, []byte("second")))
	v, err := ps.Get(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("second"), v)
}

func TestPebbleStore_Get_ReturnsCopy(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	key, val := []byte("copy-key"), []byte("copy-val")
	require.NoError(t, ps.Put(key, val))
	v, err := ps.Get(key)
	require.NoError(t, err)
	v[0] = 'X'
	v2, err := ps.Get(key)
	require.NoError(t, err)
	assert.Equal(t, val, v2)
}

// DeleteRange

func TestPebbleStore_DeleteRange(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, ps.Put([]byte(k), []byte(k)))
	}
	require.NoError(t, ps.DeleteRange([]byte("b"), []byte("d")))
	for k, shouldExist := range map[string]bool{
		"a": true, "b": false, "c": false, "d": true, "e": true,
	} {
		_, ok, err := ps.Has([]byte(k))
		require.NoError(t, err)
		assert.Equal(t, shouldExist, ok, "key %q", k)
	}
}

// Flush

func TestPebbleStore_Flush(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	require.NoError(t, ps.Put([]byte("flush-key"), []byte("flush-val")))
	require.NoError(t, ps.Flush())
	v, err := ps.Get([]byte("flush-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("flush-val"), v)
}

// Iterators

func TestPebbleStore_NewIterator_All(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	data := [][2]string{{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}}
	for _, kv := range data {
		require.NoError(t, ps.Put([]byte(kv[0]), []byte(kv[1])))
	}
	it := ps.NewIterator(nil, nil)
	defer it.Release()
	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.NoError(t, it.Error())
	assert.Equal(t, []string{"k1", "k2", "k3"}, keys)
}

func TestPebbleStore_NewIterator_WithPrefix(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	pairs := map[string]string{"abc1": "v1", "abc2": "v2", "xyz1": "v3"}
	for k, v := range pairs {
		require.NoError(t, ps.Put([]byte(k), []byte(v)))
	}
	it := ps.NewIterator([]byte("abc"), nil)
	defer it.Release()
	var keys []string
	for it.Next() {
		assert.True(t, bytes.HasPrefix(it.Key(), []byte("abc")))
		keys = append(keys, string(it.Key()))
	}
	require.NoError(t, it.Error())
	assert.ElementsMatch(t, []string{"abc1", "abc2"}, keys)
}

func TestPebbleStore_NewIterator_WithStart(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	for _, k := range []string{"k1", "k2", "k3", "k4"} {
		require.NoError(t, ps.Put([]byte(k), []byte("v")))
	}
	it := ps.NewIterator(nil, []byte("k3"))
	defer it.Release()
	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.NoError(t, it.Error())
	assert.Equal(t, []string{"k3", "k4"}, keys)
}

func TestPebbleStore_NewIterator_PrefixPlusStart(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	for _, k := range []string{"p:1", "p:2", "p:3", "q:1"} {
		require.NoError(t, ps.Put([]byte(k), []byte("v")))
	}
	it := ps.NewIterator([]byte("p:"), []byte("2"))
	defer it.Release()
	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	require.NoError(t, it.Error())
	assert.Equal(t, []string{"p:2", "p:3"}, keys)
}

func TestPebbleStore_NewIterator_Empty(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	it := ps.NewIterator(nil, nil)
	defer it.Release()
	assert.False(t, it.Next())
	assert.Nil(t, it.Key())
	assert.Nil(t, it.Value())
}

func TestPebbleStore_Iterator_KeyValueAreCopies(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	require.NoError(t, ps.Put([]byte("ck"), []byte("cv")))
	it := ps.NewIterator(nil, nil)
	defer it.Release()
	require.True(t, it.Next())
	k := it.Key()
	v := it.Value()
	it.Next()
	assert.Equal(t, []byte("ck"), k)
	assert.Equal(t, []byte("cv"), v)
}

func TestPebbleStore_GetIterator(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	require.NoError(t, ps.Put([]byte("gi-key"), []byte("gi-val")))
	raw, err := ps.GetIterator()
	require.NoError(t, err)
	defer raw.Close()
	require.True(t, raw.First())
	assert.Equal(t, []byte("gi-key"), raw.Key())
}

// Reopen / persistence

func TestPebbleStore_Reopen(t *testing.T) {
	dir := t.TempDir()
	ps1, err := NewPebbleStore(dir, 0, 0, "", false)
	require.NoError(t, err)
	require.NoError(t, ps1.Put([]byte("persist"), []byte("data")))
	require.NoError(t, ps1.Close())
	ps2, err := NewPebbleStore(dir, 0, 0, "", false)
	require.NoError(t, err)
	defer ps2.Close()
	v, err := ps2.Get([]byte("persist"))
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), v)
}

// Batch -- Put / Delete / ValueSize / NewBatchWithSize

func TestPebbleBatch_PutAndWrite(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	batch := ps.NewBatch()
	require.NoError(t, batch.Put([]byte("bk1"), []byte("bv1")))
	require.NoError(t, batch.Put([]byte("bk2"), []byte("bv2")))
	_, err := ps.Get([]byte("bk1"))
	assert.True(t, errors.Is(err, ErrNotFound))
	require.NoError(t, batch.Write())
	v, err := ps.Get([]byte("bk1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("bv1"), v)
	v, err = ps.Get([]byte("bk2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("bv2"), v)
}

func TestPebbleBatch_DeleteAndWrite(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	require.NoError(t, ps.Put([]byte("dk"), []byte("dv")))
	batch := ps.NewBatch()
	require.NoError(t, batch.Delete([]byte("dk")))
	_, ok, err := ps.Has([]byte("dk"))
	require.NoError(t, err)
	assert.True(t, ok)
	require.NoError(t, batch.Write())
	_, ok, err = ps.Has([]byte("dk"))
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestPebbleBatch_ValueSize(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	batch := ps.NewBatch()
	assert.Equal(t, 0, batch.ValueSize())
	k, v := []byte("sz-key"), []byte("sz-val")
	require.NoError(t, batch.Put(k, v))
	assert.Equal(t, len(k)+len(v), batch.ValueSize())
	require.NoError(t, batch.Delete(k))
	assert.Equal(t, len(k)+len(v)+len(k), batch.ValueSize())
}

func TestPebbleBatch_NewBatchWithSize(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	batch := ps.NewBatchWithSize(1024)
	require.NoError(t, batch.Put([]byte("wsz"), []byte("wsz-val")))
	require.NoError(t, batch.Write())
	v, err := ps.Get([]byte("wsz"))
	require.NoError(t, err)
	assert.Equal(t, []byte("wsz-val"), v)
}

// Batch -- DeleteRange

func TestPebbleBatch_DeleteRange_Basic(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	for _, k := range []string{"1", "2", "3", "4", "5"} {
		require.NoError(t, ps.Put([]byte(k), []byte(k)))
	}
	batch := ps.NewBatch()
	rb, ok := batch.(interface{ DeleteRange(start, end []byte) error })
	require.True(t, ok)
	require.NoError(t, rb.DeleteRange([]byte("2"), []byte("4")))
	require.NoError(t, batch.Write())
	for k, want := range map[string]bool{
		"1": true, "2": false, "3": false, "4": true, "5": true,
	} {
		_, ok, err := ps.Has([]byte(k))
		require.NoError(t, err)
		assert.Equal(t, want, ok, "key %q", k)
	}
}

func TestPebbleBatch_DeleteRange_NilEnd(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, ps.Put([]byte(k), []byte(k)))
	}
	batch := ps.NewBatch()
	rb, ok := batch.(interface{ DeleteRange(start, end []byte) error })
	require.True(t, ok)
	require.NoError(t, rb.DeleteRange([]byte("b"), nil))
	require.NoError(t, batch.Write())
	_, ok, err := ps.Has([]byte("a"))
	require.NoError(t, err)
	assert.True(t, ok)
	for _, k := range []string{"b", "c"} {
		_, ok, err = ps.Has([]byte(k))
		require.NoError(t, err)
		assert.False(t, ok, "key %q should be deleted", k)
	}
}

// Batch -- Reset

func TestPebbleBatch_Reset(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	batch := ps.NewBatch()
	require.NoError(t, batch.Put([]byte("rst"), []byte("rst-val")))
	assert.Greater(t, batch.ValueSize(), 0)
	batch.Reset()
	assert.Equal(t, 0, batch.ValueSize())
	if ov, ok := batch.(batchOverlay); ok {
		_, present := ov.BatchGet([]byte("rst"))
		assert.False(t, present)
	}
	require.NoError(t, batch.Write())
	_, err := ps.Get([]byte("rst"))
	assert.True(t, errors.Is(err, ErrNotFound))
}

// Batch -- Replay

func TestPebbleBatch_Replay_IntoBatch(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	b1 := ps.NewBatch()
	for _, k := range []string{"r1", "r2", "r3", "r4"} {
		require.NoError(t, b1.Put([]byte(k), []byte("v")))
	}
	b2 := ps.NewBatch()
	require.NoError(t, b1.Replay(b2))
	require.NoError(t, b2.Write())
	for _, k := range []string{"r1", "r2", "r3", "r4"} {
		_, ok, err := ps.Has([]byte(k))
		require.NoError(t, err)
		assert.True(t, ok, "key %q", k)
	}
}

func TestPebbleBatch_Replay_IntoStore(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	batch := ps.NewBatch()
	require.NoError(t, batch.Put([]byte("rs1"), []byte("rv1")))
	require.NoError(t, batch.Delete([]byte("rs-missing")))
	require.NoError(t, batch.Replay(ps))
	v, err := ps.Get([]byte("rs1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("rv1"), v)
}

func TestPebbleBatch_Replay_WithDeleteRange(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	for i := 1; i <= 10; i++ {
		require.NoError(t, ps.Put([]byte(fmt.Sprintf("%d", i)), []byte("v")))
	}
	b1 := ps.NewBatch()
	require.NoError(t, b1.Put([]byte("new"), []byte("new-val")))
	rb, ok := b1.(interface{ DeleteRange(start, end []byte) error })
	require.True(t, ok)
	require.NoError(t, rb.DeleteRange([]byte("3"), []byte("7")))
	require.NoError(t, b1.Delete([]byte("8")))
	b2 := ps.NewBatch()
	require.NoError(t, b1.Replay(b2))
	require.NoError(t, b2.Write())
	for k, want := range map[string]bool{
		"1": true, "2": true, "3": false, "4": false,
		"5": false, "6": false, "7": true, "8": false,
		"9": true, "10": true, "new": true,
	} {
		_, ok, err := ps.Has([]byte(k))
		require.NoError(t, err)
		assert.Equal(t, want, ok, "key %q", k)
	}
}

// BatchGet overlay

func TestPebbleBatch_BatchGet_Overlay(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	key := []byte("ov-key")
	require.NoError(t, ps.Put(key, []byte("db-val")))
	batch := ps.NewBatch()
	ov, ok := batch.(batchOverlay)
	require.True(t, ok)
	v, present := ov.BatchGet(key)
	assert.False(t, present)
	assert.Nil(t, v)
	require.NoError(t, batch.Put(key, []byte("staged")))
	v, present = ov.BatchGet(key)
	require.True(t, present)
	assert.Equal(t, []byte("staged"), v)
	v[0] = 'Z'
	v2, _ := ov.BatchGet(key)
	assert.Equal(t, []byte("staged"), v2)
	dbv, err := ps.Get(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("db-val"), dbv)
	require.NoError(t, batch.Delete(key))
	v, present = ov.BatchGet(key)
	assert.True(t, present)
	assert.Nil(t, v)
	require.NoError(t, batch.Write())
	_, err = ps.Get(key)
	assert.True(t, errors.Is(err, ErrNotFound))
}

// Batch.Write after store closed

func TestPebbleBatch_Write_StoreClosed(t *testing.T) {
	ps, _ := openStore(t)
	batch := ps.NewBatch()
	require.NoError(t, batch.Put([]byte("closed-key"), []byte("v")))
	require.NoError(t, ps.Close())
	err := batch.Write()
	assert.Error(t, err)
}

// Large data set

func TestPebbleStore_LargeDataSet(t *testing.T) {
	ps, _ := openStore(t)
	defer ps.Close()
	const n = 500
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("large-key-%04d", i))
		v := []byte(fmt.Sprintf("large-val-%04d", i))
		require.NoError(t, ps.Put(k, v))
	}
	it := ps.NewIterator(nil, nil)
	defer it.Release()
	count := 0
	for it.Next() {
		count++
	}
	require.NoError(t, it.Error())
	assert.Equal(t, n, count)
}
