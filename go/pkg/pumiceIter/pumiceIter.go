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

type CRocksdbSnapshotT = C.rocksdb_snapshot_t
type CRocksdbIteratorT = C.rocksdb_iterator_t
type CRocksdbReadoptionsT = C.rocksdb_readoptions_t

// ------------------------------------------------------------
// Iterator Implementation
// ------------------------------------------------------------

type PumiceIterator struct {
	Itr       *CRocksdbIteratorT
	Ropts     *CRocksdbReadoptionsT
	Snapshot  *CRocksdbSnapshotT
	SeqNumVal uint64
	Prefix    string
}

// ------------------------------------------------------------
// Iterator Methods
// ------------------------------------------------------------

func (p *PumiceIterator) Valid() bool {
	valid := C.rocksdb_iter_valid(p.Itr) != 0
	if !valid {
		return false
	}
	if p.Prefix != "" {
		if !strings.HasPrefix(p.Key(), p.Prefix) {
			return false
		}
	}
	return true
}

func (p *PumiceIterator) Next() {
	C.rocksdb_iter_next(p.Itr)
}

func (p *PumiceIterator) Key() string {
	var klen C.size_t
	k := C.rocksdb_iter_key(p.Itr, &klen)

	return C.GoStringN((*C.char)(k), C.int(klen))
}

func (p *PumiceIterator) Value() []byte {
	var vlen C.size_t
	v := C.rocksdb_iter_value(p.Itr, &vlen)

	return C.GoBytes(unsafe.Pointer(v), C.int(vlen))
}

func (p *PumiceIterator) GetKV() (string, string) {
	return p.Key(), string(p.Value())
}

func (p *PumiceIterator) SeqNum() uint64 {
	return p.SeqNumVal
}

func (p *PumiceIterator) Close() {

	if p.Snapshot != nil {
		C.rocksdb_release_snapshot(C.PmdbGetRocksDB(), p.Snapshot)
	}

	if p.Ropts != nil {
		if p.SeqNumVal > 0 && p.Snapshot == nil {
			C.PmdbPutRoptionsWithSnapshot(C.ulong(p.SeqNumVal))
		} else {
			C.rocksdb_readoptions_destroy(p.Ropts)
		}
	}

	if p.Itr != nil {
		C.rocksdb_iter_destroy(p.Itr)
	}
}

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

	cf := GoToCString(args.Selector)
	cfHandle := C.PmdbCfHandleLookup(cf)

	itr := C.rocksdb_create_iterator_cf(
		C.PmdbGetRocksDB(),
		ropts,
		cfHandle,
	)

	seekTo(args.Key, int64(len(args.Key)), itr)

	FreeCMem(cf)

	return &PumiceIterator{
		Itr:       itr,
		Ropts:     ropts,
		Snapshot:  snapshot,
		SeqNumVal: seqNum,
		Prefix:    args.Prefix,
	}, nil
}

func seekTo(key string, keyLen int64, itr *C.rocksdb_iterator_t) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	C.rocksdb_iter_seek(itr, cKey, C.size_t(keyLen))
}

func GoToCString(s string) *C.char {
	return C.CString(s)
}

func FreeCMem(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}
