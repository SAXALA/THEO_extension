// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

import (
	"path/filepath"
	"testing"

	ldberrors "github.com/tinoryj/EthStore/ChainKV/goleveldb/leveldb/errors"
)

func TestLDBDatabaseDeleteS(t *testing.T) {
	db, err := NewLDBDatabase(filepath.Join(t.TempDir(), "ldb"), 16, 16)
	if err != nil {
		t.Fatalf("NewLDBDatabase failed: %v", err)
	}
	defer db.Close()

	key := []byte("state-key")
	if err := db.Put_s(key, []byte("value")); err != nil {
		t.Fatalf("Put_s failed: %v", err)
	}
	got, err := db.Get_s(key)
	if err != nil {
		t.Fatalf("Get_s after Put_s failed: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("unexpected value after Put_s: %q", got)
	}

	if err := db.Delete_s(key); err != nil {
		t.Fatalf("Delete_s failed: %v", err)
	}
	_, err = db.Get_s(key)
	if err == nil {
		t.Fatal("expected not found after Delete_s")
	}
	if err != ldberrors.ErrNotFound {
		t.Fatalf("expected ErrNotFound after Delete_s, got: %v", err)
	}
}

func TestTableDeleteS(t *testing.T) {
	db, err := NewLDBDatabase(filepath.Join(t.TempDir(), "ldb"), 16, 16)
	if err != nil {
		t.Fatalf("NewLDBDatabase failed: %v", err)
	}
	defer db.Close()

	table := NewTable(db, "prefix-")
	key := []byte("state-key")
	if err := table.Put_s(key, []byte("value")); err != nil {
		t.Fatalf("table Put_s failed: %v", err)
	}
	got, err := table.Get_s(key)
	if err != nil {
		t.Fatalf("table Get_s after Put_s failed: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("unexpected table value after Put_s: %q", got)
	}

	if err := table.Delete_s(key); err != nil {
		t.Fatalf("table Delete_s failed: %v", err)
	}
	_, err = table.Get_s(key)
	if err == nil {
		t.Fatal("expected not found after table Delete_s")
	}
	if err != ldberrors.ErrNotFound {
		t.Fatalf("expected ErrNotFound after table Delete_s, got: %v", err)
	}
}
