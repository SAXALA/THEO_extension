package prefixdb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestPrefixDBAccount(t *testing.T) {
	pd, err := NewPrefixDB(t.TempDir())
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
	filepath := "/mnt/ssd/ethstore/testDB"
	pd, err := NewPrefixDB(filepath)
	if err != nil {
		t.Fatalf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()
	Key1 := []byte("41")
	Value1 := []byte("f90211a03b2efef87d199d537512f015b61843519da328438cca3030f7bfe3da06feb15ba0a525e2359dbcd31a325f4cfbb5f431f0819567deace3bc3dd3fbb6cb2dca9d6aa046a48a46e0f27a8d1971d613bc1915fa42d2b6296dadf241976f5811b6f73c3ea06a822377f74dbdbf49f5a70abded3f6f3764f3432edd2c36b69b7b45ba1faf72a05cc4e53a3eb3e05e468e5a4f9b6419346d7d0960e68ea2804f2cae3212bfe4c9a08a8ee284e0375c0fd9736ec55bd05b6b836e1f9af12a7b42533f79f5aca3f03aa02ee353e46550dd6f9130d64bd04c96b38cf8a3c7100f1cf7d139dc6cae649b94a0b62f0b610a86597644215bf2489ccc2d8b92b5e79a81e9a36606c19fb95ff8d9a086e6897d5c4be7f8e03d2e3835a83a01b8f9f69609566e96ca8f8f1e5bb90a87a05a2c50af6f90ed669350915cefeea73ab0ee983b9a80f0bd589ff6fdb9d0c282a01b196b46f61a4dd2fe8fe705f1a1d435340da908c803335344e161e7586a0212a0c38a12351e1acbe47091377223796bc0db05efbbf055b2910a24f07e7e970cb9a01a449a64c485eff5c8d51095c7094f886b0bf5997fe45d4964a1bb9cb43369fba0f807e4e04dbbdba01d87e419ef90c4aa827dfed61758c78f86aae03e6347fe2fa05b4f7cabd46d1b92f78042ccb8cda71bfa836ef910c548a96749b5722a261482a08feed97f2d3bc9bad9aaf6d68019fdd656dafb33f5e4877fa6409dc662bb4c4480")
	Key2 := []byte("410000000000010b")
	Value2 := []byte("f8669d31d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d031b846f8440180a02e90fa6e0dd972de88c3d7365b293f8fb67afadb98ba5c58cac1e1ee8ce47d12a0bafa57ebfbfd24de79a762fec12871b565cd7da7206993a55cae3f2a3476aae3")
	// Test basic operations
	keystr1, _ := hex.DecodeString(string(Key1))
	keystr2, _ := hex.DecodeString(string(Key2))
	Value1, _ = hex.DecodeString(string(Value1))
	Value2, _ = hex.DecodeString(string(Value2))

	SK_1 := []byte("4f000001b1d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d03101")
	SV_1 := []byte("SV_1_value")
	SK_2 := []byte("4f000001b1d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d03102")
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
	//pd.nodeCache.FlushModifiedNodes()
	pd.nodeCache.Evict(string(keystr1))
	pd.nodeCache.Evict(string(keystr2))
	// pd.slotCache.FlushModifiedSlots()
	pd.batch.CommitBatch()

	// pd.storeNode(keystr1, &TrieNode{
	// 	startSlotindex: 0,
	// 	slotNum:        0,
	// 	offset:         0,
	// })

	node, err := pd.getNode(keystr1)
	if err != nil {
		t.Fatalf("Failed to get node for key1: %v", err)
	}
	if node == nil {
		t.Fatalf("Expected node for key1, got nil")
	}

	value, _, err = pd.Get(SK_1)
	if err != nil || !bytes.Equal(value, SV_1) {
		t.Fatalf("Failed to get SK_1: %v", err)
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
	// pd.slotCache.Delete(1023)
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
	pd, err = NewPrefixDB(t.TempDir())
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

func TestPrefixDBAccountHash(t *testing.T) {
	pd, err := NewPrefixDB(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()

	AK_1 := []byte("410000000000010907")
	AK_1, _ = hex.DecodeString(string(AK_1))
	AV_1 := []byte("f8669d31d1daa0ba2662877f4fff747d528318c1b343a7575d4429170f40d031b846f8440180a02e90fa6e0dd972de88c3d7365b293f8fb67afadb98ba5c58cac1e1ee8ce47d12a0bafa57ebfbfd24de79a762fec12871b565cd7da7206993a55cae3f2a3476aae3")
	AV_1, _ = hex.DecodeString(string(AV_1))
	err = pd.Put(AK_1, AV_1)

	SK_1 := []byte("4f0000019759ea326fa019a55bda5dff44477be6e1d9c48db950e3fe07a0ba671e01")
	SK_1, _ = hex.DecodeString(string(SK_1))
	SV_1 := []byte("SV_1_value")

	pd.Put(SK_1, SV_1)

	key := pd.getParentAccountKey(SK_1)
	if key == nil {
		t.Fatalf("Failed to get parent account key for %x", SK_1)
	}
	fmt.Printf("Parent account key for %x: %x\n", SK_1, key)
}

func TestMemCache(t *testing.T) {
	dirPath := "/mnt/ssd/ethstore/testDB"
	pd, err := NewPrefixDB(dirPath)
	if err != nil {
		t.Fatalf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()
	SK_1 := []byte("4f0000019759ea326fa019a55bda5dff44477be6e1d9c48db950e3fe07a0ba671e01")
	SK_1, _ = hex.DecodeString(string(SK_1))
	// SV_1 := []byte("SV_1_value")

	pd.getParentAccountKey(SK_1)
	value, got, err := pd.Get(SK_1)
	if err != nil || !got {
		t.Fatalf("Failed to get SK_1: %v", err)
	}
	if value == nil {
		t.Fatalf("Expected value for SK_1, got nil")
	}
	fmt.Printf("Value for SK_1: %x\n", value)
}

func TestReadFile(t *testing.T) {
	dirPath := "/mnt/ssd/ethstore/testDB"
	pd, err := NewPrefixDB(dirPath)
	if err != nil {
		t.Fatalf("Failed to create PrefixDB: %v", err)
	}
	defer pd.Close()

	value, _ := pd.readFromFile(117*8, TrieAccount)
	decodedValue := hex.EncodeToString(value)
	fmt.Printf("Read value from file: %x\n", decodedValue)
}
