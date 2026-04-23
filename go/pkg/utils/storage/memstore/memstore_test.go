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

func TestMemStore_ImplementsInterface(t *testing.T) {
	var _ storageiface.DataStore = NewMemStore()
}
