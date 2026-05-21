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

func (p *PumiceIterator) Next() {
	C.rocksdb_iter_next(p.itr)
}

func (p *PumiceIterator) Key() string {
	var klen C.size_t
	k := C.rocksdb_iter_key(p.itr, &klen)

	return C.GoStringN((*C.char)(k), C.int(klen))
}

func (p *PumiceIterator) Value() []byte {
	var vlen C.size_t
	v := C.rocksdb_iter_value(p.itr, &vlen)

	return C.GoBytes(unsafe.Pointer(v), C.int(vlen))
}

func (p *PumiceIterator) GetKV() (string, string) {
	return p.Key(), string(p.Value())
}

func (p *PumiceIterator) GetSeqNum() uint64 {
	return p.seqNum
}

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

func seekTo(key string, keyLen int64, itr *C.rocksdb_iterator_t) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	C.rocksdb_iter_seek(itr, cKey, C.size_t(keyLen))
}

func goToCString(s string) *C.char {
	return C.CString(s)
}

func freeCMem(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}
