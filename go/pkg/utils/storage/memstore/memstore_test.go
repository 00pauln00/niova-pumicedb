package memstore

import (
	"sync"
	"testing"

	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
)

func TestMemStore_BasicCRUD(t *testing.T) {
	store := NewMemStore()
	db := "testdb"

	// Write & Read
	if err := store.Write("k1", "v1", db); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if val, _ := store.Read("k1", db); string(val) != "v1" {
		t.Errorf("Expected 'v1', got %q", val)
	}

	// Overwrite
	store.Write("k1", "v2", db)
	if val, _ := store.Read("k1", db); string(val) != "v2" {
		t.Errorf("Expected overwritten 'v2', got %q", val)
	}

	// Read Missing Key/Selector
	if _, err := store.Read("missing", db); err == nil {
		t.Error("Expected error for missing key")
	}
	if _, err := store.Read("k1", "missing-db"); err == nil {
		t.Error("Expected error for missing selector")
	}

	// Delete & Verify
	store.Delete("k1", db)
	if _, err := store.Read("k1", db); err == nil {
		t.Error("Expected error after deletion")
	}

	// Delete Non-existent (Should not panic/error)
	if err := store.Delete("ghost", db); err != nil {
		t.Errorf("Delete non-existent returned err: %v", err)
	}
}

func TestMemStore_Isolation(t *testing.T) {
	store := NewMemStore()
	store.Write("shared-key", "valA", "dbA")
	store.Write("shared-key", "valB", "dbB")

	vA, _ := store.Read("shared-key", "dbA")
	vB, _ := store.Read("shared-key", "dbB")

	if string(vA) != "valA" || string(vB) != "valB" {
		t.Error("Data from different selectors must be isolated")
	}
}

func TestMemStore_RangeRead(t *testing.T) {
	store := NewMemStore()
	db := "logs"
	store.Write("log:1", "a", db)
	store.Write("log:2", "b", db)
	store.Write("metric:1", "c", db)

	// Test Prefix
	res, err := store.RangeRead(storageiface.RangeReadArgs{Selector: db, Prefix: "log:"})
	if err != nil || len(res.ResultMap) != 2 {
		t.Errorf("Expected 2 prefix results, got %d", len(res.ResultMap))
	}

	// Test Start Key
	res, _ = store.RangeRead(storageiface.RangeReadArgs{Selector: db, Key: "log:2"})
	if len(res.ResultMap) != 2 { // Should get log:2 and metric:1
		t.Errorf("Expected 2 results starting from key, got %d", len(res.ResultMap))
	}
}

func TestMemStore_Concurrency(t *testing.T) {
	store := NewMemStore()
	var wg sync.WaitGroup

	// Spin up 50 pairs of readers/writers to trigger race detector if missing locks
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			store.Write("k1", "v", "concurrent")
		}()
		go func() {
			defer wg.Done()
			store.Read("k1", "concurrent")
			store.RangeRead(storageiface.RangeReadArgs{Selector: "concurrent", Prefix: "k"})
		}()
	}

	wg.Wait()
}

func TestMemIterator_BasicIteration(t *testing.T) {
	ms := NewMemStore()
	ms.Write("n_cfg/aaa/d", "dev-0", "")
	ms.Write("n_cfg/aaa/pp", "8000", "")
	ms.Write("n_cfg/bbb/d", "dev-1", "")
	ms.Write("n_cfg/bbb/pp", "8001", "")
	ms.Write("v/xxx/cfg/sz", "1024", "")

	itr, err := ms.NewRangeIterator(storageiface.RangeReadArgs{
		Key:    "n_cfg/",
		Prefix: "n_cfg/",
		SeqNum: 42,
	})
	if err != nil {
		t.Fatalf("NewRangeIterator failed: %v", err)
	}
	defer itr.Close()

	var keys []string
	for itr.Valid() {
		k, _ := itr.GetKV()
		keys = append(keys, k)
		itr.Next()
	}

	// Should get exactly the 4 n_cfg keys, sorted
	if len(keys) != 4 {
		t.Fatalf("expected 4 keys, got %d: %v", len(keys), keys)
	}
	expected := []string{
		"n_cfg/aaa/d",
		"n_cfg/aaa/pp",
		"n_cfg/bbb/d",
		"n_cfg/bbb/pp",
	}
	for i, k := range keys {
		if k != expected[i] {
			t.Errorf("key[%d] = %q, want %q", i, k, expected[i])
		}
	}

	if itr.GetSeqNum() != 42 {
		t.Errorf("GetSeqNum() = %d, want 42", itr.GetSeqNum())
	}
}

func TestMemIterator_SeekToKey(t *testing.T) {
	ms := NewMemStore()
	ms.Write("n_cfg/aaa/d", "dev-0", "")
	ms.Write("n_cfg/bbb/d", "dev-1", "")
	ms.Write("n_cfg/ccc/d", "dev-2", "")

	// Seek to bbb (simulates pagination resume)
	itr, err := ms.NewRangeIterator(storageiface.RangeReadArgs{
		Key:    "n_cfg/bbb/d",
		Prefix: "n_cfg/",
	})
	if err != nil {
		t.Fatalf("NewRangeIterator failed: %v", err)
	}
	defer itr.Close()

	var keys []string
	for itr.Valid() {
		keys = append(keys, itr.Key())
		itr.Next()
	}

	if len(keys) != 2 {
		t.Fatalf("expected 2 keys from seek, got %d: %v", len(keys), keys)
	}
	if keys[0] != "n_cfg/bbb/d" || keys[1] != "n_cfg/ccc/d" {
		t.Errorf("unexpected keys: %v", keys)
	}
}

func TestMemIterator_PrefixBoundary(t *testing.T) {
	ms := NewMemStore()
	ms.Write("v/id1/cfg/sz", "1024", "")
	ms.Write("v/id1/c/0/R.0", "nisd-1", "")
	ms.Write("v/id2/cfg/sz", "2048", "")

	// Iterator with prefix "v/id1/" should NOT return v/id2 keys
	itr, err := ms.NewRangeIterator(storageiface.RangeReadArgs{
		Key:    "v/id1/",
		Prefix: "v/id1/",
	})
	if err != nil {
		t.Fatalf("NewRangeIterator failed: %v", err)
	}
	defer itr.Close()

	var keys []string
	for itr.Valid() {
		keys = append(keys, itr.Key())
		itr.Next()
	}

	if len(keys) != 2 {
		t.Fatalf("expected 2 keys for prefix v/id1/, got %d: %v", len(keys), keys)
	}
}

func TestMemIterator_EmptyStore(t *testing.T) {
	ms := NewMemStore()

	itr, err := ms.NewRangeIterator(storageiface.RangeReadArgs{
		Key:    "anything",
		Prefix: "anything",
	})
	if err != nil {
		t.Fatalf("NewRangeIterator failed: %v", err)
	}
	defer itr.Close()

	if itr.Valid() {
		t.Error("expected iterator to be invalid on empty store")
	}
}

func TestMemIterator_ValueLookup(t *testing.T) {
	ms := NewMemStore()
	ms.Write("k1", "val1", "")
	ms.Write("k2", "val2", "")

	itr, err := ms.NewRangeIterator(storageiface.RangeReadArgs{
		Key:    "k1",
		Prefix: "k",
	})
	if err != nil {
		t.Fatalf("NewRangeIterator failed: %v", err)
	}
	defer itr.Close()

	if !itr.Valid() {
		t.Fatal("expected valid iterator")
	}

	if string(itr.Value()) != "val1" {
		t.Errorf("Value() = %q, want %q", string(itr.Value()), "val1")
	}

	k, v := itr.GetKV()
	if k != "k1" || v != "val1" {
		t.Errorf("GetKV() = (%q, %q), want (k1, val1)", k, v)
	}
}
