package pumiceserver

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	PumiceDBCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumiceerr"
	gopointer "github.com/mattn/go-pointer"

	log "github.com/sirupsen/logrus"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft -lniova_pumice -lrocksdb
#include <pumice/pumice_db.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
#include <pumice/pumice_db_client.h>
extern ssize_t writePrepCgo(struct pumicedb_cb_cargs *args, int *);
extern ssize_t applyCgo(struct pumicedb_cb_cargs *args, void *);
extern ssize_t readCgo(struct pumicedb_cb_cargs *args);
extern void initCgo(struct pumicedb_cb_cargs *args);
extern ssize_t fillReplyCgo(struct pumicedb_cb_cargs *args,void *);
*/
import "C"

// The encoding overhead for a single key-val entry is 2 bytes
var encodingOverhead int = 2

type PmdbCbArgs struct {
	// App level request payload
	Payload []byte

	// Fields for reply buffer.
	ReplyBuf  unsafe.Pointer
	ReplySize int64

	// Fields for application specific data set in WritePrep stage.
	// Read in Apply stage.
	AppData     unsafe.Pointer
	AppDataSize int64

	// Continue int ptr for WritePrep stage to notify
	// whether to continue with write to apply stage or not.
	// By default write continues to apply stage.
	// Call DiscontinueWrite to stop the write.
	continueWR unsafe.Pointer

	// Contains the raft state change enum (RAFT_STATE_CHANGE).
	// Provided for Init handler.
	InitState int

	// Rncui and write supplement handler
	rncui     unsafe.Pointer
	wsHandler unsafe.Pointer

	// Pumice server handler pointer
	pumiceHandler unsafe.Pointer
}

type PmdbServerAPI interface {
	WritePrep(goCbArgs *PmdbCbArgs) int64
	Apply(goCbArgs *PmdbCbArgs) int64
	Read(goCbArgs *PmdbCbArgs) int64
	Init(goCbArgs *PmdbCbArgs)
	FillReply(goCbArgs *PmdbCbArgs) int64
}

type LeaseServerAPI interface {
	WritePrep(goCbArgs *PmdbCbArgs) int64
	Apply(goCbArgs *PmdbCbArgs) int64
	Read(goCbArgs *PmdbCbArgs) int64
	Init(goCbArgs *PmdbCbArgs)
}

type FuncServerAPI interface {
	WritePrep(goCbArgs *PmdbCbArgs) int64
	Apply(goCbArgs *PmdbCbArgs) int64
	Read(goCbArgs *PmdbCbArgs) int64
	Init(goCbArgs *PmdbCbArgs)
}

type PmdbServerObject struct {
	PmdbAPI        PmdbServerAPI
	LeaseAPI       LeaseServerAPI
	FuncAPI        FuncServerAPI
	RaftUuid       string
	PeerUuid       string
	SyncWrites     bool
	CoalescedWrite bool
	LeaseEnabled   bool
	ColumnFamilies []string
}

type PmdbLeaderTS struct {
	Term int64
	Time int64
}

type RangeReadArgs struct {
	Key        string
	Prefix     string
	SeqNum     uint64
	Consistent bool
	BufSize    int64
	ColFamily  string
}

type RangeReadResult struct {
	ResultMap map[string][]byte
	LastKey   string
	SeqNum    uint64
	SnapMiss  bool
}

// RAFT_STATE_CHANGE enum values
const (
	INIT_TYPE_NONE                int = 0
	INIT_BOOTUP_STATE                 = 1
	INIT_BECOMING_LEADER_STATE        = 2
	INIT_BECOMING_CANDIDATE_STATE     = 3
	INIT_SHUTDOWN_STATE               = 4
	INIT_TYPE_ANY                     = 5
)

type charsSlice []*C.char

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

func pmdbCbArgsInit(cargs *C.struct_pumicedb_cb_cargs,
	goCbArgs *PmdbCbArgs) int {
	goCbArgs.rncui = unsafe.Pointer(cargs.pcb_userid)
	ReqBuf := unsafe.Pointer(cargs.pcb_req_buf)
	ReqSize := CToGoInt64(cargs.pcb_req_bufsz)
	goCbArgs.ReplyBuf = unsafe.Pointer(cargs.pcb_reply_buf)
	goCbArgs.ReplySize = CToGoInt64(cargs.pcb_reply_bufsz)
	goCbArgs.InitState = int(cargs.pcb_init)
	goCbArgs.continueWR = unsafe.Pointer(cargs.pcb_continue_wr)
	goCbArgs.wsHandler = unsafe.Pointer(cargs.pcb_pmdb_handler)
	goCbArgs.pumiceHandler = unsafe.Pointer(cargs.pcb_user_data)
	goCbArgs.AppData = unsafe.Pointer(cargs.pcb_app_data)
	goCbArgs.AppDataSize = CToGoInt64(cargs.pcb_app_data_sz)
	//Decode Pumice level request
	request := &PumiceDBCommon.PumiceRequest{}
	err := PumiceDBCommon.Decode(ReqBuf, request, ReqSize)
	if err != nil {
		log.Error(err)
		return -1
	}
	goCbArgs.Payload = request.ReqPayload
	return request.ReqType
}

/*
 The following goWritePrep, goApply and goRead functions are the exported
 functions which is needed for calling the golang function
 pointers from C.
*/

//export goWritePrep
func goWritePrep(args *C.struct_pumicedb_cb_cargs) int64 {

	var wrPrepArgs PmdbCbArgs
	reqType := pmdbCbArgsInit(args, &wrPrepArgs)

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(wrPrepArgs.pumiceHandler).(*PmdbServerObject)

	var ret int64
	if reqType == PumiceDBCommon.APP_REQ {
		//Calling the golang Application's WritePrep function.
		ret = gcb.PmdbAPI.WritePrep(&wrPrepArgs)
	} else if reqType == PumiceDBCommon.LEASE_REQ {
		//Calling leaseAPP WritePrep
		ret = gcb.LeaseAPI.WritePrep(&wrPrepArgs)
	} else if reqType == PumiceDBCommon.FUNC_REQ {
		//Calling the golang Function's WritePrep function.
		ret = gcb.FuncAPI.WritePrep(&wrPrepArgs)
	} else {
		return -1
	}
	return ret
}

//export goApply
func goApply(args *C.struct_pumicedb_cb_cargs,
	pmdb_handle unsafe.Pointer) int64 {

	var applyArgs PmdbCbArgs
	reqType := pmdbCbArgsInit(args, &applyArgs)

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(applyArgs.pumiceHandler).(*PmdbServerObject)

	var ret int64
	if reqType == PumiceDBCommon.APP_REQ {
		//Calling the golang Application's Apply function.
		ret = gcb.PmdbAPI.Apply(&applyArgs)
	} else if reqType == PumiceDBCommon.LEASE_REQ {
		//Calling leaseAPP Apply
		ret = gcb.LeaseAPI.Apply(&applyArgs)
	} else if reqType == PumiceDBCommon.FUNC_REQ {
		//Calling the golang Function's Apply function.
		ret = gcb.FuncAPI.Apply(&applyArgs)
	}
	return ret
}

//export goRead
func goRead(args *C.struct_pumicedb_cb_cargs) int64 {

	var readArgs PmdbCbArgs
	reqType := pmdbCbArgsInit(args, &readArgs)

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(readArgs.pumiceHandler).(*PmdbServerObject)

	var ret int64
	if reqType == PumiceDBCommon.APP_REQ {
		//Calling the golang Application's Read function.
		ret = gcb.PmdbAPI.Read(&readArgs)
	} else if reqType == PumiceDBCommon.LEASE_REQ {
		//Calling leaseAPP Read
		ret = gcb.LeaseAPI.Read(&readArgs)
	} else if reqType == PumiceDBCommon.FUNC_REQ {
		//Calling the golang Function's Read function.
		ret = gcb.FuncAPI.Read(&readArgs)
	}
	return ret
}

//export goInit
func goInit(args *C.struct_pumicedb_cb_cargs) {

	var initArgs PmdbCbArgs
	pmdbCbArgsInit(args, &initArgs)

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(initArgs.pumiceHandler).(*PmdbServerObject)

	gcb.PmdbAPI.Init(&initArgs)
	if gcb.LeaseEnabled {
		gcb.LeaseAPI.Init(&initArgs)
	}
}

//export goFillReply
func goFillReply(args *C.struct_pumicedb_cb_cargs) int64 {
	var replyArgs PmdbCbArgs
	reqType := pmdbCbArgsInit(args, &replyArgs)
	gcb := gopointer.Restore(replyArgs.pumiceHandler).(*PmdbServerObject)

	var ret int64
	if reqType == PumiceDBCommon.APP_REQ {
		ret = gcb.PmdbAPI.FillReply(&replyArgs)
	}
	return ret
}

/**
 * Start the pmdb server.
 * @raft_uuid: Raft UUID.
 * @peer_uuid: Peer UUID.
 * @cf: Column Family
 * @cb: PmdbAPI callback funcs.
 */
func PmdbStartServer(pso *PmdbServerObject) error {

	if pso == nil {
		return errors.New("Null server object parameter")
	}
	if pso.RaftUuid == "" || pso.PeerUuid == "" {
		return errors.New("Raft and/or peer UUIDs were not specified")
	}

	/*
	 * Convert the raft_uuid and peer_uuid go strings into C strings
	 * so that we can pass these to C function.
	 */

	raft_uuid_c := GoToCString(pso.RaftUuid)
	defer FreeCMem(raft_uuid_c)

	peer_uuid_c := GoToCString(pso.PeerUuid)
	defer FreeCMem(peer_uuid_c)

	cCallbacks := C.struct_PmdbAPI{}

	//Assign the callback functions
	cCallbacks.pmdb_apply = C.pmdb_apply_sm_handler_t(C.applyCgo)
	cCallbacks.pmdb_read = C.pmdb_read_sm_handler_t(C.readCgo)
	cCallbacks.pmdb_write_prep = C.pmdb_write_prep_sm_handler_t(C.writePrepCgo)
	cCallbacks.pmdb_init = C.pmdb_init_sm_handler_t(C.initCgo)
	cCallbacks.pmdb_fill_reply = C.pmdb_fill_reply_sm_handler_t(C.fillReplyCgo)

	/*
	 * Store the column family name into char * array.
	 * Store gostring to byte array.
	 * Don't forget to append the null terminating character.
	 */

	var i int
	cf_name := make(charsSlice, len(pso.ColumnFamilies))
	for _, cFamily := range pso.ColumnFamilies {
		cf_byte_arr := []byte(cFamily + "\000")
		cf_name[i] = (*C.char)(C.CBytes(cf_byte_arr))
		i = i + 1
	}

	//Convert Byte array to char **
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&cf_name))
	cf_array := (**C.char)(unsafe.Pointer(sH.Data))

	// Create an opaque C pointer for cbs to pass to PmdbStartServer.
	opa_ptr := gopointer.Save(pso)
	defer gopointer.Unref(opa_ptr)

	// Starting the pmdb server.
	rc := C.PmdbExec(raft_uuid_c, peer_uuid_c, &cCallbacks, cf_array,
		C.int(len(pso.ColumnFamilies)), (C.bool)(pso.SyncWrites),
		(C.bool)(pso.CoalescedWrite), opa_ptr)

	if rc != 0 {
		return fmt.Errorf("PmdbExec() returned %d", rc)
	}

	return nil
}

// Method version of PmdbStartServer()
// writes metadata to 'path' while exiting
func (pso *PmdbServerObject) Run() error {
	go PumiceDBCommon.HandleKillSignal()
	return PmdbStartServer(pso)
}

func (pca *PmdbCbArgs) DiscontinueWrite() {
	if pca.continueWR != nil {
		*(*C.int)(pca.continueWR) = C.int(0)
	}
}

// search a key in RocksDB
func (pca *PmdbCbArgs) PmdbReadKV(cf string, key string) ([]byte, error) {
	ccf := GoToCString(cf)
	defer FreeCMem(ccf)

	ck := GoToCString(key)
	defer FreeCMem(ck)
	ckl := GoToCSize_t(int64(len(key)))

	cfh := C.PmdbCfHandleLookup(ccf)

	ropts := C.rocksdb_readoptions_create()

	var cerr *C.char
	var cvl C.size_t

	cv := C.rocksdb_get_cf(C.PmdbGetRocksDB(), ropts, cfh, ck, ckl, &cvl, &cerr)

	C.rocksdb_readoptions_destroy(ropts)

	var result []byte
	var err error

	if err != nil {
		log.Error("PmdbReadKV: rocksdb_get_cf failed with error: ", C.GoString(cerr))
		err = errors.New("rocksdb_get_cf failed")
		C.rocksdb_free(unsafe.Pointer(cerr))
	}

	if cv != nil {
		result = C.GoBytes(unsafe.Pointer(cv), C.int(cvl))
		err = nil
		C.rocksdb_free(unsafe.Pointer(cv))
	}

	log.Trace("Result is :", result)
	return result, err
}

func (pca *PmdbCbArgs) PmdbDeleteKV(cf, key string) error {

	ck := GoToCString(key)
	defer FreeCMem(ck)
	ckl := GoToCSize_t(int64(len(key)))

	capp_id := (*C.struct_raft_net_client_user_id)(pca.rncui)

	ccf := GoToCString(cf)
	defer FreeCMem(ccf)
	cfh := C.PmdbCfHandleLookup(ccf)

	//Calling pmdb library function to write Key-Value.
	rc := C.PmdbDeleteKV(capp_id, pca.wsHandler, ck, ckl, nil, unsafe.Pointer(cfh))

	return pumiceerr.TranslatePumiceServerOpErrCode(int(rc))
}

func (pca *PmdbCbArgs) PmdbWriteKV(cf, key, val string) error {

	ck := GoToCString(key)
	defer FreeCMem(ck)
	ckl := GoToCSize_t(int64(len(key)))

	cv := GoToCString(val)
	defer FreeCMem(cv)

	cvl := GoToCSize_t(int64(len(val)))

	capp_id := (*C.struct_raft_net_client_user_id)(pca.rncui)

	ccf := GoToCString(cf)
	defer FreeCMem(ccf)
	cfh := C.PmdbCfHandleLookup(ccf)

	//Calling pmdb library function to write Key-Value.
	rc := C.PmdbWriteKV(capp_id, pca.wsHandler, ck, ckl, cv, cvl, nil, unsafe.Pointer(cfh))

	return pumiceerr.TranslatePumiceServerOpErrCode(int(rc))
}

// Wrapper for rocksdb_iter_seek -
// Seeks the passed iterator to the passed key or first key
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

// Wrapper for rocksdb_iter_key/val -
// Returns the key and value from the where
// the iterator is present
func getKeyVal(itr *C.rocksdb_iterator_t) (string, []byte) {
	var cKeyLen C.size_t
	var cValLen C.size_t

	C_key := C.rocksdb_iter_key(itr, &cKeyLen)
	C_value := C.rocksdb_iter_value(itr, &cValLen)

	keyBytes := CToGoBytes(C_key, C.int(cKeyLen))
	valueBytes := CToGoBytes(C_value, C.int(cValLen))

	return string(keyBytes), valueBytes
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

func (pca *PmdbCbArgs) PmdbRangeRead(args RangeReadArgs) (*RangeReadResult, error) {
	var lookup_err error
	res := &RangeReadResult{
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
	cf := GoToCString(args.ColFamily)
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
		mapSize = mapSize + entrySize + encodingOverhead
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
		lookup_err = errors.New("Failed to lookup for key")
	} else {
		lookup_err = nil
	}
	return res, lookup_err
}

func PmdbCopyBytesToBuffer(ed []byte,
	buffer unsafe.Pointer) (int64, error) {
	if ed == nil || len(ed) == 0 {
		return 0, errors.New("No data to copy")
	}
	if buffer == nil {
		return 0, errors.New("Buffer is nil")
	}
	// Copy the byte slice into the buffer
	C.memcpy(buffer, unsafe.Pointer(&ed[0]), C.size_t(len(ed)))
	return int64(len(ed)), nil
}

// Copy data from the user's application into the pmdb reply buffer
func PmdbCopyDataToBuffer(ed interface{}, buffer unsafe.Pointer) (int64, error) {
	var key_len int64
	//Encode the structure into void pointer.
	encoded_key, err := PumiceDBCommon.Encode(ed, &key_len)
	if err != nil {
		log.Print("Failed to encode data during copy data: ", err)
		return -1, err
	}

	//Copy the encoded structed into buffer
	C.memcpy(buffer, encoded_key, C.size_t(key_len))

	return key_len, nil
}

func (*PmdbServerObject) GetCurrentHybridTime() float64 {
	//TODO: Update this code
	return 0.0
}

// Get the leader timestamp.
func PmdbGetLeaderTimeStamp(ts *PmdbLeaderTS) error {

	var ts_c C.struct_raft_leader_ts
	err := errors.New("Not a leader")

	rc := C.PmdbGetLeaderTimeStamp(&ts_c)

	rc_go := int(rc)
	if rc_go == 0 {
		ts.Term = int64(ts_c.rlts_term)
		ts.Time = int64(ts_c.rlts_time)
		err = nil
	}

	return err
}

func PmdbIsLeader() bool {
	rc := C.PmdbIsLeader()
	return bool(rc)
}

func pmdbInitRPCMsg(rcm *C.struct_raft_client_rpc_msg, dataSize uint32) {
	rcm.rcrm_type = C.RAFT_CLIENT_RPC_MSG_TYPE_WRITE
	rcm.rcrm_version = 0
	rcm.rcrm_data_size = C.uint32_t(dataSize)
}

func PmdbEnqueuePutRequest(areq []byte, rtype int, rncui string, wsn int64) int {
	var req PumiceDBCommon.PumiceRequest
	req.Rncui = rncui
	req.ReqType = rtype
	req.ReqPayload = areq

	//Encode the PumiceRequest
	var reqBuf bytes.Buffer
	pumiceEnc := gob.NewEncoder(&reqBuf)
	err := pumiceEnc.Encode(req)
	if err != nil {
		log.Error(err)
		return -1
	}

	data := reqBuf.Bytes()

	//Prepare rncui c string
	rncuiStrC := GoToCString(req.Rncui)
	defer FreeCMem(rncuiStrC)

	//Convert it to unsafe pointer (void * for C function)
	kvdata := unsafe.Pointer(&data[0])

	dsize := int64(len(data))
	rmsize := C.sizeof_struct_raft_client_rpc_msg
	pmdSize := C.sizeof_struct_pmdb_msg + C.int64_t(dsize)

	//total size of the request buffer
	totalSize := int64(rmsize) + int64(pmdSize)
	buf := C.malloc(C.size_t(totalSize))
	defer C.free(buf)

	//Populate raft_client_rpc_msg structure
	rcm := (*C.struct_raft_client_rpc_msg)(buf)
	pmdbInitRPCMsg(rcm, uint32(pmdSize))

	// Get the pointer for rcrm_data
	rptr := unsafe.Pointer(uintptr(buf) + C.sizeof_struct_raft_client_rpc_msg)

	//Prepare pmdb_obj_id
	var rncui_id C.struct_raft_net_client_user_id
	var obj_id *C.pmdb_obj_id_t
	C.raft_net_client_user_id_parse(rncuiStrC, &rncui_id, 0)
	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	//Populate pmdb_msg structure
	pmdb_msg := (*C.struct_pmdb_msg)(rptr)
	C.pmdb_direct_msg_init(pmdb_msg, obj_id, C.uint32_t(dsize), C.pmdb_op_write, C.int64_t(wsn))

	//Get the pointer to pmdbrm_data and store the PumiceRequest
	dataPtr := unsafe.Pointer(uintptr(rptr) + C.sizeof_struct_pmdb_msg)
	C.memcpy(dataPtr, kvdata, C.size_t(dsize))

	//Enqueue the direct request
	ret := C.raft_server_enq_direct_raft_req_from_leader((*C.char)(buf), C.int64_t(totalSize))

	return int(ret)
}
