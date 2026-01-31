package pumicestore

import (
	"sync"
)

// MemStore is an in-memory key-value store that implements the DataStore interface.
type MemStore struct {
	data map[string][]byte
	mu   sync.RWMutex
}

// NewMemStore creates a new MemStore.
func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[string][]byte),
	}
}

// Read reads a value for a given key.
func (s *MemStore) Read(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[string(key)]
	if !ok {
		return nil, nil // Or an error indicating not found
	}
	return value, nil
}

// RangeRead reads a range of key-value pairs.
// For an in-memory map, this is a simplified implementation.
func (s *MemStore) RangeRead(startKey, endKey []byte) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	results := make(map[string]string)
	for k, v := range s.data {
		if k >= string(startKey) && k < string(endKey) {
			results[k] = string(v)
		}
	}
	return results, nil
}

// Write writes a key-value pair.
func (s *MemStore) Write(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[string(key)] = value
	return nil
}

// Delete deletes a key.
func (s *MemStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, string(key))
	return nil
}

// Ensure MemStore implements the DataStore interface.
var _ DataStore = &MemStore{}
