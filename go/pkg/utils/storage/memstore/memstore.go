package memstore

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
)

// MemStore is an in-memory key-value store that implements the DataStore interface.
type MemStore struct {
	mu   sync.RWMutex
	data map[string]map[string]string
}

// NewMemStore creates a new MemStore.
func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[string]map[string]string),
	}
}

// Read reads a value for a given key.
func (s *MemStore) Read(key, selector string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db, ok := s.data[selector]
	if !ok {
		return nil, fmt.Errorf("db selector not found: %s", selector)
	}
	val, ok := db[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return []byte(val), nil
}

// RangeRead reads a range of key-value pairs.
// For an in-memory map, this is a simplified implementation.
func (s *MemStore) RangeRead(args storageiface.RangeReadArgs) (*storageiface.RangeReadResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := &storageiface.RangeReadResult{
		ResultMap: make(map[string][]byte),
	}

	db, ok := s.data[args.Selector]
	if !ok || len(db) == 0 {
		return result, nil
	}

	sortedKeys := make([]string, 0, len(db))
	for k := range db {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	// Binary search for starting point
	startIdx := 0
	if args.Key != "" {
		startIdx = sort.Search(len(sortedKeys), func(i int) bool {
			return sortedKeys[i] >= args.Key
		})
	} else if args.Prefix != "" {
		startIdx = sort.Search(len(sortedKeys), func(i int) bool {
			return sortedKeys[i] >= args.Prefix
		})
	}

	var currentSize int64
	for i := startIdx; i < len(sortedKeys); i++ {
		k := sortedKeys[i]

		if !strings.HasPrefix(k, args.Prefix) {
			break
		}

		entrySize := int64(len(k) + len(db[k]))
		if args.BufSize > 0 && currentSize+entrySize > args.BufSize {
			if len(result.ResultMap) == 0 {
				result.ResultMap[k] = []byte(db[k])
				result.LastKey = k + "\x00"
			} else {
				result.LastKey = k
			}
			return result, nil
		}

		result.ResultMap[k] = []byte(db[k])
		currentSize += entrySize
	}

	return result, nil
}

// Write writes a key-value pair.
func (s *MemStore) Write(key, value, selector string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data[selector] == nil {
		s.data[selector] = make(map[string]string)
	}
	s.data[selector][key] = value
	return nil
}

// Delete deletes a key.
func (s *MemStore) Delete(key, selector string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if db, ok := s.data[selector]; ok {
		delete(db, key)

		if len(db) == 0 {
			delete(s.data, selector)
		}
	}
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
