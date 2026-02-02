package memstore

import "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"

// MemStore is an in-memory key-value store that implements the DataStore interface.
type MemStore struct {
	data map[string]string
}

// NewMemStore creates a new MemStore.
func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[string]string),
	}
}

// Read reads a value for a given key.
func (s *MemStore) Read(key, selector string) ([]byte, error) {
	value, ok := s.data[string(key)]
	if !ok {
		return nil, nil // Or an error indicating not found
	}
	return []byte(value), nil
}

// RangeRead reads a range of key-value pairs.
// For an in-memory map, this is a simplified implementation.
func (s *MemStore) RangeRead(args storageiface.RangeReadArgs) (*storageiface.RangeReadResult, error) {
	//Do prefix matching based lookup

	var result storageiface.RangeReadResult
	return &result, nil
}

// Write writes a key-value pair.
func (s *MemStore) Write(key, value, selector string) error {
	s.data[string(key)] = value
	return nil
}

// Delete deletes a key.
func (s *MemStore) Delete(key, selector string) error {
	delete(s.data, string(key))
	return nil
}

// Ensure MemStore implements the DataStore interface.
var _ storageiface.DataStore = &MemStore{}
