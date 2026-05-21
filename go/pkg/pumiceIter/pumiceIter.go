package pumiceiter

/*
#cgo LDFLAGS: -lrocksdb -lniova
#include <rocksdb/c.h>
#include <pumice/pumice_db.h>
*/
import "C"

import (
	"strings"
	"unsafe"

	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
)

// ------------------------------------------------------------
// Exported C type aliases (for cross-package unsafe.Pointer casts)
// ------------------------------------------------------------

type cRocksdbSnapshotT = C.rocksdb_snapshot_t
type cRocksdbIteratorT = C.rocksdb_iterator_t
type cRocksdbReadoptionsT = C.rocksdb_readoptions_t

// ------------------------------------------------------------
// Iterator Implementation
// ------------------------------------------------------------

// PumiceIterator implements the storageiface.Iterator interface using RocksDB.
// It supports prefix-based iteration and consistent reads via snapshots.
type PumiceIterator struct {
	itr       *cRocksdbIteratorT
	ropts     *cRocksdbReadoptionsT
	snapshot  *cRocksdbSnapshotT
	seqNum 	  uint64
	prefix    string
}

// ------------------------------------------------------------
// Iterator Methods
// ------------------------------------------------------------

// Valid returns true if the iterator is positioned at a valid key-value pair
// and the current key satisfies the prefix constraint (if any).
func (p *PumiceIterator) Valid() bool {
	valid := C.rocksdb_iter_valid(p.itr) != 0
	if !valid {
		return false
	}
	if p.prefix != "" {
		if !strings.HasPrefix(p.Key(), p.prefix) {
			return false
		}
	}
	return true
}

// Next advances the iterator to the next key-value pair.
func (p *PumiceIterator) Next() {
	C.rocksdb_iter_next(p.itr)
}

// Key returns the key at the current iterator position as a string.
func (p *PumiceIterator) Key() string {
	var klen C.size_t
	k := C.rocksdb_iter_key(p.itr, &klen)

	return C.GoStringN((*C.char)(k), C.int(klen))
}

// Value returns the value at the current iterator position as a byte slice.
func (p *PumiceIterator) Value() []byte {
	var vlen C.size_t
	v := C.rocksdb_iter_value(p.itr, &vlen)

	return C.GoBytes(unsafe.Pointer(v), C.int(vlen))
}

// GetKV returns the key and value at the current iterator position as strings.
func (p *PumiceIterator) GetKV() (string, string) {
	return p.Key(), string(p.Value())
}

// GetSeqNum returns the sequence number associated with this iterator.
func (p *PumiceIterator) GetSeqNum() uint64 {
	return p.seqNum
}

// Close releases the underlying RocksDB iterator, read options, and snapshot.
func (p *PumiceIterator) Close() {

	if p.snapshot != nil {
		C.rocksdb_release_snapshot(C.PmdbGetRocksDB(), p.snapshot)
	}

	if p.ropts != nil {
		if p.seqNum > 0 && p.snapshot == nil {
			C.PmdbPutRoptionsWithSnapshot(C.ulong(p.seqNum))
		} else {
			C.rocksdb_readoptions_destroy(p.ropts)
		}
	}

	if p.itr != nil {
		C.rocksdb_iter_destroy(p.itr)
	}
}

// NewRangeIterator creates a new PumiceIterator for the given RangeReadArgs.
// It handles consistent read options and positions the iterator at the start key.
func NewRangeIterator(args storageiface.RangeReadArgs) (*PumiceIterator, error) {

	var snapshot *C.rocksdb_snapshot_t
	var ropts *C.rocksdb_readoptions_t
	var retSeqNum C.ulong
	var seqNum = args.SeqNum

	if args.Consistent {
		ropts = C.PmdbGetRoptionsWithSnapshot(C.ulong(seqNum), &retSeqNum)
		seqNum = uint64(retSeqNum)
	} else {
		ropts = C.rocksdb_readoptions_create()
	}

	cf := goToCString(args.Selector)
	cfHandle := C.PmdbCfHandleLookup(cf)

	itr := C.rocksdb_create_iterator_cf(
		C.PmdbGetRocksDB(),
		ropts,
		cfHandle,
	)

	seekTo(args.Key, int64(len(args.Key)), itr)

	freeCMem(cf)

	return &PumiceIterator{
		itr:       itr,
		ropts:     ropts,
		snapshot:  snapshot,
		seqNum: seqNum,
		prefix:    args.Prefix,
	}, nil
}

// seekTo positions the RocksDB iterator at the specified key.
func seekTo(key string, keyLen int64, itr *C.rocksdb_iterator_t) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	C.rocksdb_iter_seek(itr, cKey, C.size_t(keyLen))
}

// goToCString converts a Go string to a C string.
func goToCString(s string) *C.char {
	return C.CString(s)
}

// freeCMem releases memory allocated for a C string.
func freeCMem(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}
