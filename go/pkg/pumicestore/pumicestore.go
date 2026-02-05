package pumicestore

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceerr"
	storageiface "github.com/00pauln00/niova-pumicedb/go/pkg/utils/storage/interface"
	log "github.com/sirupsen/logrus"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft -lniova_pumice -lrocksdb
#include <pumice/pumice_db.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
#include <pumice/pumice_db_client.h>
*/
import "C"

// PumiceStore is a client for the PumiceDB key-value store.
type PumiceStore struct {
	// Add any necessary fields here, like a connection handle.
	Rncui     unsafe.Pointer
	WSHandler unsafe.Pointer
}

// The encoding overhead for a single key-val entry is 2 bytes
var encodingOverhead int = 2

// Read reads a value for a given key.
func (s *PumiceStore) Read(key, selector string) ([]byte, error) {
	ccf := GoToCString(selector)
	defer FreeCMem(ccf)

	ck := GoToCString(key)
	defer FreeCMem(ck)
	ckl := GoToCSize_t(int64(len(key)))

	//Get the specified rocksdb coloum family struct
	cfh := C.PmdbCfHandleLookup(ccf)

	var cerr *C.char
	var cvl C.size_t

	ropts := C.rocksdb_readoptions_create()
	/*
		rockdb_get_cf returns the value buffer(cv) of size(cvl) for the key(ckl)
		from the coloumn family(cfh), if the operation errors, the error is filled in cerr
	*/
	cv := C.rocksdb_get_cf(C.PmdbGetRocksDB(), ropts, cfh, ck, ckl, &cvl, &cerr)
	C.rocksdb_readoptions_destroy(ropts)

	if cerr != nil {
		log.Error("PmdbReadKV: rocksdb_get_cf failed with error: ", C.GoString(cerr))
		C.rocksdb_free(unsafe.Pointer(cerr))
		return nil, pumiceerr.ErrReadFromStorage
	}

	var result []byte
	if cv != nil {
		result = C.GoBytes(unsafe.Pointer(cv), C.int(cvl))
		C.rocksdb_free(unsafe.Pointer(cv))
	}

	return result, nil
}

// RangeRead reads a range of key-value pairs.
func (s *PumiceStore) RangeRead(args storageiface.RangeReadArgs) (*storageiface.RangeReadResult, error) {
	res := &storageiface.RangeReadResult{
		ResultMap: make(map[string][]byte),
		SeqNum:    args.SeqNum,
	}
	var mapSize int
	var itr *C.rocksdb_iterator_t
	var ropts *C.rocksdb_readoptions_t
	var endReached bool

	//Create ropts based on consistency and seqNum
	ropts, res.SnapMiss = createRopts(args.Consistent, &res.SeqNum)

	// create iterator
	cf := GoToCString(args.Selector)
	cf_handle := C.PmdbCfHandleLookup(cf)
	itr = C.rocksdb_create_iterator_cf(C.PmdbGetRocksDB(), ropts, cf_handle)

	//Seek to the provided key
	seekTo(args.Key, int64(len(args.Key)), itr)

	// Iterate over keys store them in map if prefix
	for C.rocksdb_iter_valid(itr) != 0 {
		k, v := getKeyVal(itr)

		// check if passed key is prefix of fetched key or exit
		if (args.Prefix != "") && (!strings.HasPrefix(k, args.Prefix)) {
			endReached = true
			break
		}

		// check if the key-val can be stored in the buffer
		entrySize := len([]byte(k)) + len([]byte(v)) + encodingOverhead
		if (int64(mapSize) + int64(entrySize)) > args.BufSize {
			res.LastKey = k
			break
		}
		mapSize = mapSize + entrySize
		res.ResultMap[k] = v

		C.rocksdb_iter_next(itr)
	}

	//Destroy ropts for consistent mode only when reached the end of the range query
	//Wheras, destroy ropts in every iteration if the range query is not consistent
	if C.rocksdb_iter_valid(itr) == 0 || endReached == true || !args.Consistent {
		destroyRopts(res.SeqNum, ropts, args.Consistent)
	}

	//Free the iterator and memory
	C.rocksdb_iter_destroy(itr)
	FreeCMem(cf)

	if len(res.ResultMap) == 0 {
		return nil, errors.New("Failed to lookup for key")
	}

	return res, nil
}

// Write writes a key-value pair.
func (s *PumiceStore) Write(key, value, selector string) error {
	ck := GoToCString(key)
	defer FreeCMem(ck)
	ckl := GoToCSize_t(int64(len(key)))

	cv := GoToCString(value)
	defer FreeCMem(cv)

	cvl := GoToCSize_t(int64(len(value)))

	capp_id := (*C.struct_raft_net_client_user_id)(s.Rncui)

	ccf := GoToCString(selector)
	defer FreeCMem(ccf)
	cfh := C.PmdbCfHandleLookup(ccf)

	//Calling pmdb library function to write Key-Value.
	rc := C.PmdbWriteKV(capp_id, s.WSHandler, ck, ckl, cv, cvl, nil, unsafe.Pointer(cfh))
	if int(rc) != 0 {
		return pumiceerr.TranslatePumiceServerOpErrCode(int(rc))
	}

	return nil
}

// Delete deletes a key.
func (s *PumiceStore) Delete(key, selector string) error {
	// TODO: Implement the logic to delete from PumiceDB.
	ck := GoToCString(key)
	defer FreeCMem(ck)
	ckl := GoToCSize_t(int64(len(key)))

	capp_id := (*C.struct_raft_net_client_user_id)(s.Rncui)

	ccf := GoToCString(selector)
	defer FreeCMem(ccf)
	cfh := C.PmdbCfHandleLookup(ccf)

	//Calling pmdb library function to write Key-Value.
	rc := C.PmdbDeleteKV(capp_id, s.WSHandler, ck, ckl, nil, unsafe.Pointer(cfh))
	if int(rc) != 0 {
		return pumiceerr.TranslatePumiceServerOpErrCode(int(rc))
	}

	return nil
}

func createRopts(consistent bool, seqNum *uint64) (*C.rocksdb_readoptions_t, bool) {
	var snapMiss bool
	var ropts *C.rocksdb_readoptions_t
	var retSeqNum C.ulong

	//Create ropts based on consistency requirement
	if consistent {
		ropts = C.PmdbGetRoptionsWithSnapshot(C.ulong(*seqNum), &retSeqNum)
		if *seqNum != CToGoUint64(retSeqNum) {
			if *seqNum != math.MaxUint64 {
				snapMiss = true
			}
			*seqNum = CToGoUint64(retSeqNum)
		}
	} else {
		ropts = C.rocksdb_readoptions_create()
	}

	return ropts, snapMiss
}

func destroyRopts(seqNum uint64, ropts *C.rocksdb_readoptions_t, consistent bool) {
	if consistent {
		C.PmdbPutRoptionsWithSnapshot(C.ulong(seqNum))
	} else {
		C.rocksdb_readoptions_destroy(ropts)
	}
}

/* Typecast Go string to C String */
func GoToCString(gstring string) *C.char {
	return C.CString(gstring)
}

/* Free the C memory */
func FreeCMem(cstring *C.char) {
	C.free(unsafe.Pointer(cstring))
}

/* Typecast Go Int to string */
func GoIntToString(value int) string {
	return strconv.Itoa(value)
}

/* Get length of the Go string */
func GoStringLen(str string) int {
	return len(str)
}

/* Type cast Go int64 to C size_t */
func GoToCSize_t(glen int64) C.size_t {
	return C.size_t(glen)
}

/* Typecast C size_t to Go int64 */
func CToGoInt64(cvalue C.size_t) int64 {
	return int64(cvalue)
}

/* Typecast C uint64_t to Go uint64*/
func CToGoUint64(cvalue C.ulong) uint64 {
	return uint64(cvalue)
}

/* Type cast C char * to Go string */
func CToGoBytes(C_value *C.char, C_value_len C.int) []byte {
	return C.GoBytes(unsafe.Pointer(C_value), C_value_len)
}

func getKeyVal(itr *C.rocksdb_iterator_t) (string, []byte) {
	var cKeyLen C.size_t
	var cValLen C.size_t

	C_key := C.rocksdb_iter_key(itr, &cKeyLen)
	C_value := C.rocksdb_iter_value(itr, &cValLen)

	keyBytes := CToGoBytes(C_key, C.int(cKeyLen))
	valueBytes := CToGoBytes(C_value, C.int(cValLen))

	return string(keyBytes), valueBytes
}

func seekTo(key string, key_len int64, itr *C.rocksdb_iterator_t) {
	var cKey *C.char
	var cLen C.size_t

	if key == "" {
		C.rocksdb_iter_seek_to_first(itr)
	} else {
		cKey = GoToCString(key)
		cLen = GoToCSize_t(key_len)
		C.rocksdb_iter_seek(itr, cKey, cLen)
		FreeCMem(cKey)
	}
}

// Ensure PumiceStore implements the DataStore interface.
var _ storageiface.DataStore = &PumiceStore{}
