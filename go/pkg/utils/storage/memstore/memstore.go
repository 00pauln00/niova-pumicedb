package memstore

import (
	"fmt"
	"strings"

	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
)

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
		return nil, fmt.Errorf("key %s not found", key) // Or an error indicating not found
	}
	return []byte(value), nil
}

// RangeRead reads a range of key-value pairs.
// For an in-memory map, this is a simplified implementation.
func (s *MemStore) RangeRead(args storageiface.RangeReadArgs) (*storageiface.RangeReadResult, error) {

	result := &storageiface.RangeReadResult{
		ResultMap: make(map[string][]byte),
	}

	for k, v := range s.data {
		if strings.HasPrefix(k, args.Prefix) {
			result.ResultMap[k] = []byte(v)
		}
	}

	result.SeqNum = args.SeqNum
	result.LastKey = ""

	return result, nil
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

// NewRangeIterator creates a new iterator for the MemStore.
func (s *MemStore) NewRangeIterator(args storageiface.RangeReadArgs) (storageiface.Iterator, error) {
	return &MemIterator{
		data: s.data,
		// Simplified implementation, not actually doing range/prefix yet for MemStore
	}, nil
}

type MemIterator struct {
	data map[string]string
}

func (i *MemIterator) Valid() bool            { return false }
func (i *MemIterator) Next()                  {}
func (i *MemIterator) Key() string            { return "" }
func (i *MemIterator) Value() []byte          { return nil }
func (i *MemIterator) GetKV() (string, string) { return "", "" }
func (i *MemIterator) SeqNum() uint64         { return 0 }
func (i *MemIterator) Close()                 {}

// Ensure MemStore implements the DataStore interface.
var _ storageiface.DataStore = &MemStore{}
