package ssPrefixdb

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestPrefixDBAccount(t *testing.T) {
	pd, err := NewSSPrefixDB(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()
	Key1 := []byte("41")
	Value1 := []byte("f90211a03b2efef87d199d537512f015b61843519da328438cca3030f7bfe3da06feb15ba0a525e2359dbcd31a325f4cfbb5f431f0819567deace3bc3dd3fbb6cb2dca9d6aa046a48a46e0f27a8d1971d613bc1915fa42d2b6296dadf241976f5811b6f73c3ea06a822377f74dbdbf49f5a70abded3f6f3764f3432edd2c36b69b7b45ba1faf72a05cc4e53a3eb3e05e468e5a4f9b6419346d7d0960e68ea2804f2cae3212bfe4c9a08a8ee284e0375c0fd9736ec55bd05b6b836e1f9af12a7b42533f79f5aca3f03aa02ee353e46550dd6f9130d64bd04c96b38cf8a3c7100f1cf7d139dc6cae649b94a0b62f0b610a86597644215bf2489ccc2d8b92b5e79a81e9a36606c19fb95ff8d9a086e6897d5c4be7f8e03d2e3835a83a01b8f9f69609566e96ca8f8f1e5bb90a87a05a2c50af6f90ed669350915cefeea73ab0ee983b9a80f0bd589ff6fdb9d0c282a01b196b46f61a4dd2fe8fe705f1a1d435340da908c803335344e161e7586a0212a0c38a12351e1acbe47091377223796bc0db05efbbf055b2910a24f07e7e970cb9a01a449a64c485eff5c8d51095c7094f886b0bf5997fe45d4964a1bb9cb43369fba0f807e4e04dbbdba01d87e419ef90c4aa827dfed61758c78f86aae03e6347fe2fa05b4f7cabd46d1b92f78042ccb8cda71bfa836ef910c548a96749b5722a261482a08feed97f2d3bc9bad9aaf6d68019fdd656dafb33f5e4877fa6409dc662bb4c4480")
	// Test basic operations
	keystr1, _ := hex.DecodeString(string(Key1))
	err = pd.Put(keystr1, Value1)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	value, got, err := pd.Get(keystr1)
	if err != nil || !got {
		t.Fatalf("Failed to get key1: %v", err)
	}

	got, err = pd.Has(keystr1)
	if err != nil || !got {
		t.Fatalf("Expected key1 to exist, but got: %v", err)
	}
	if !bytes.Equal(value, Value1) {
		t.Fatalf("Expected value1, got %x", value)
	}

	if err != nil {
		t.Fatalf("Failed to commit batch: %v", err)
	}

	value, got, err = pd.Get(keystr1)
	if err != nil || !got {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if !bytes.Equal(value, Value1) {
		t.Fatalf("Expected %x, got %x", Value1, value)
	}

	err = pd.Delete(keystr1)
	if err != nil {
		t.Fatalf("Failed to delete key1: %v", err)
	}
	value, got, err = pd.Get(keystr1)
	if err == nil || got {
		t.Fatalf("Expected key1 to be deleted, but got: %v", err)
	}
	if value != nil {
		t.Fatalf("Expected nil value for deleted key1, got %s", value)
	}
}

func TestStorage(t *testing.T) {
	pd, err := NewSSPrefixDB(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()
	Key1 := []byte("61000001b1d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d031")
	Value1 := []byte("f90211a03b2efef87d199d537512f015b61843519da328438cca3030f7bfe3da06feb15ba0a525e2359dbcd31a325f4cfbb5f431f0819567deace3bc3dd3fbb6cb2dca9d6aa046a48a46e0f27a8d1971d613bc1915fa42d2b6296dadf241976f5811b6f73c3ea06a822377f74dbdbf49f5a70abded3f6f3764f3432edd2c36b69b7b45ba1faf72a05cc4e53a3eb3e05e468e5a4f9b6419346d7d0960e68ea2804f2cae3212bfe4c9a08a8ee284e0375c0fd9736ec55bd05b6b836e1f9af12a7b42533f79f5aca3f03aa02ee353e46550dd6f9130d64bd04c96b38cf8a3c7100f1cf7d139dc6cae649b94a0b62f0b610a86597644215bf2489ccc2d8b92b5e79a81e9a36606c19fb95ff8d9a086e6897d5c4be7f8e03d2e3835a83a01b8f9f69609566e96ca8f8f1e5bb90a87a05a2c50af6f90ed669350915cefeea73ab0ee983b9a80f0bd589ff6fdb9d0c282a01b196b46f61a4dd2fe8fe705f1a1d435340da908c803335344e161e7586a0212a0c38a12351e1acbe47091377223796bc0db05efbbf055b2910a24f07e7e970cb9a01a449a64c485eff5c8d51095c7094f886b0bf5997fe45d4964a1bb9cb43369fba0f807e4e04dbbdba01d87e419ef90c4aa827dfed61758c78f86aae03e6347fe2fa05b4f7cabd46d1b92f78042ccb8cda71bfa836ef910c548a96749b5722a261482a08feed97f2d3bc9bad9aaf6d68019fdd656dafb33f5e4877fa6409dc662bb4c4480")
	Key2 := []byte("61000001b1d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d032")
	Value2 := []byte("f8669d31d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d031b846f8440180a02e90fa6e0dd972de88c3d7365b293f8fb67afadb98ba5c58cac1e1ee8ce47d12a0bafa57ebfbfd24de79a762fec12871b565cd7da7206993a55cae3f2a3476aae3")
	// Test basic operations
	keystr1, _ := hex.DecodeString(string(Key1))
	keystr2, _ := hex.DecodeString(string(Key2))

	SK_1 := []byte("6f000001b1d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d03101")
	SV_1 := []byte("SV_1_value")
	SK_2 := []byte("6f000001b1d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d03102")
	SV_2 := []byte("SV_2_value")
	SK_1, _ = hex.DecodeString(string(SK_1))
	SK_2, _ = hex.DecodeString(string(SK_2))

	err = pd.Put(keystr1, Value1)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	err = pd.Put(keystr2, Value2)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	err = pd.Put(SK_1, SV_1)
	if err != nil {
		t.Fatalf("Failed to put SK_1: %v", err)
	}
	err = pd.Put(SK_2, SV_2)
	if err != nil {
		t.Fatalf("Failed to put SK_2: %v", err)
	}
	value, got, err := pd.Get(SK_2)
	if err != nil || !got || !bytes.Equal(value, SV_2) {
		t.Fatalf("Failed to get SK_2: %v", err)
	}

	pd.batch.SetThreshold(1)
	pd.slotCache.Delete(0)

	value, _, err = pd.Get(SK_1)
	if err != nil || !bytes.Equal(value, SV_1) {
		t.Fatalf("Failed to put SK_1: %v", err)
	}
	value, got, err = pd.Get(SK_2)
	if err != nil || !got || !bytes.Equal(value, SV_2) {
		t.Fatalf("Failed to get SK_2: %v", err)
	}

	value, got, err = pd.Get(keystr1)
	if err != nil || !got {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if !bytes.Equal(value, Value1) {
		t.Fatalf("Expected value1, got %x", value)
	}
	value, got, err = pd.Get(keystr2)
	if err != nil || !got {
		t.Fatalf("Failed to get key2: %v", err)
	}
	if !bytes.Equal(value, Value2) {
		t.Fatalf("Expected value2, got %x", value)
	}

	pd.Delete(SK_1)
	pd.Delete(SK_2)
	pd.slotCache.Delete(0)
	pd.batch.CommitBatch()
	value, got, err = pd.Get(SK_1)
	if err == nil || got {
		t.Fatalf("Expected SK_1 to be deleted, but got: %v", err)
	}
	if value != nil {
		t.Fatalf("Expected nil value for deleted SK_1, got %s", value)
	}

	value, got, err = pd.Get(SK_2)
	if err == nil || got {
		t.Fatalf("Expected SK_2 to be deleted, but got: %v", err)
	}
	if value != nil {
		t.Fatalf("Expected nil value for deleted SK_2, got %s", value)
	}
	err = pd.Close()
	if err != nil {
		t.Fatalf("Failed to close PrefixDB: %v", err)
	}
	pd, err = NewSSPrefixDB(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create PrefixDB: %v", err)
	}
	value, got, err = pd.Get(keystr1)
	if err != nil || !got {
		t.Fatalf("Failed to get key1 after reopen: %v", err)
	}
	if !bytes.Equal(value, Value1) {
		t.Fatalf("Expected value1 after reopen, got %x", value)
	}
}

func TestGetParentKey(t *testing.T) {
	dirpath := "/mnt/ssd/ethstore/database"
	pd, err := NewSSPrefixDB(dirpath)
	if err != nil {
		t.Fatalf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()
	SK_1 := []byte("610000019759ea326fa019a55bda5dff44477be6e1d9c48db950e3fe07a0ba671e")
	SV_1 := []byte("f8440180a0665081a76be9ad792eec7ba0b7819e48a97cd6ab5210cae849c1ea4777ba9b6aa029164acf9a06c22bbe9da20100d94116c6ef93f44a5b58ebd6e1954c3bf436df")
	SK_1, err = hex.DecodeString(string(SK_1))
	SV_1, err = hex.DecodeString(string(SV_1))

	pd.Put(SK_1, SV_1)

	Key1 := []byte("6f0000019759ea326fa019a55bda5dff44477be6e1d9c48db950e3fe07a0ba671e290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563")
	Value1 := []byte("f91111111")

	parentKey1 := pd.GetParentAccountKey(Key1)

	Key1, err = hex.DecodeString(string(Key1))
	Value1, err = hex.DecodeString(string(Value1))
	pd.Put(Key1, Value1)
	if err != nil {
		t.Fatalf("Failed to get parent key for Key1: %v", err)
	}
	if !bytes.Equal(parentKey1, Key1[:len(Key1)-2]) {
		t.Fatalf("Expected parent key for Key1 to be %x, got %x", Key1[:len(Key1)-2], parentKey1)
	}
}
