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
// It collects all keys from the map, sorts them lexicographically,
// and seeks to args.Key — mirroring the PumiceIterator (RocksDB) behavior.
func (s *MemStore) NewRangeIterator(args storageiface.RangeReadArgs) (storageiface.Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	db, ok := s.data[args.Selector]
	if !ok {
		// Return an empty iterator if the selector is not found
		return &MemIterator{
			keys:   nil,
			data:   nil,
			pos:    0,
			prefix: args.Prefix,
			seqNum: args.SeqNum,
		}, nil
	}

	// Collect all keys from the map (full snapshot).
	allKeys := make([]string, 0, len(db))
	for k := range db {
		allKeys = append(allKeys, k)
	}
	sort.Strings(allKeys)

	// Seek: find the first key >= args.Key using binary search.
	startPos := sort.SearchStrings(allKeys, args.Key)

	return &MemIterator{
		keys:   allKeys,
		data:   db,
		pos:    startPos,
		prefix: args.Prefix,
		seqNum: args.SeqNum,
	}, nil
}

// ------------------------------------------------------------
// MemIterator — sorted-key cursor iterator over MemStore
// ------------------------------------------------------------

// MemIterator provides a sorted, prefix-filtered, seek-capable iterator
// over a MemStore's data, matching the PumiceIterator interface.
type MemIterator struct {
	keys   []string          // lexicographically sorted key snapshot
	data   map[string]string // reference to MemStore data for value lookups (specific column family)
	pos    int               // current cursor position in keys
	prefix string            // prefix filter (from RangeReadArgs.Prefix)
	seqNum uint64            // passthrough sequence number
}

// Valid reports whether the iterator points to a valid entry.
// Returns false when the cursor is past the end or the current key
// no longer matches the prefix (same semantics as PumiceIterator.Valid).
func (i *MemIterator) Valid() bool {
	if i.pos >= len(i.keys) {
		return false
	}
	if i.prefix != "" {
		if !strings.HasPrefix(i.keys[i.pos], i.prefix) {
			return false
		}
	}
	return true
}

// Next advances the iterator to the next key.
func (i *MemIterator) Next() {
	i.pos++
}

// Key returns the key at the current cursor position.
func (i *MemIterator) Key() string {
	return i.keys[i.pos]
}

// Value returns the value at the current cursor position.
func (i *MemIterator) Value() []byte {
	return []byte(i.data[i.keys[i.pos]])
}

// GetKV returns the key and value at the current cursor position.
func (i *MemIterator) GetKV() (string, string) {
	return i.Key(), string(i.Value())
}

// GetSeqNum returns the sequence number passed in via RangeReadArgs.
func (i *MemIterator) GetSeqNum() uint64 {
	return i.seqNum
}

// Close is a no-op for MemIterator (no external resources to release).
func (i *MemIterator) Close() {}

// Ensure MemStore implements the DataStore interface.
var _ storageiface.DataStore = &MemStore{}
