package pumiceclient

import (
	"errors"
	"fmt"
	PumiceDBCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	"strconv"
	"syscall"
	"unsafe"
	gopointer "github.com/mattn/go-pointer"

	"github.com/google/uuid"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft_client -lniova_pumice_client
#include <pumice/pumice_db_client.h>
#include <pumice/pumice_db_net.h>
extern void PmdbAsyncReqCompletionCB(void *args, ssize_t size);
static void CPmdbAsyncCompletionCB(void *args, ssize_t size) {
	PmdbAsyncReqCompletionCB(args, size);
}
static inline pmdb_user_cb_t getAsyncReqCompCB() {
    return &CPmdbAsyncCompletionCB;
}
*/
import "C"

type PmdbReq struct {
	Rncui       string
	ReqType		int
	Request  	[]byte
	Reply    	*[]byte
	GetReply 	int
	ZeroCopyObj *RDZeroCopyObj
}

type PmdbClientObj struct {
	pmdb        C.pmdb_t
	raftUUID    *C.char
	clientUUID  *C.char
	initialized bool
	//Deprecated fields
	AppUUID     string
	WriteSeqNo  uint64
}

type RDZeroCopyObj struct {
	buffer     unsafe.Pointer
	buffer_len int64
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

/* Type cast Go int64 to C size_t */
func GoToCSize_t(glen int64) C.size_t {
	return C.size_t(glen)
}

/* Typecast C size_t to Go int64 */
func CToGoInt64(cvalue C.size_t) int64 {
	return int64(cvalue)
}

/* Type cast C char * to Go string */
func CToGoString(cstring *C.char) string {
	return C.GoString(cstring)
}

//Get PumiceRequest in common format
func pmdbreqwrap(ra *PmdbReq) (*C.char, int64) {
	// get bytes for requestResponse.Request and
	// convert PumiceDBCommon.PumiceRequest
	var req PumiceDBCommon.PumiceRequest

	req.ReqType = ra.ReqType
	req.ReqPayload = ra.Request

	var reqLen int64
	reqPtr, err := PumiceDBCommon.Encode(req, &reqLen)
	if err != nil {
		return nil, 0
	}

	return (*C.char)(reqPtr), reqLen
}

func (pco *PmdbClientObj) Put(ra *PmdbReq) error {
	rsb := (C.int)(ra.GetReply)
	rp, rl := pmdbreqwrap(ra)
	if rp == nil || rl == 0 {
		return errors.New("Error encoding the request")
	}

	var rs int64
	rb, err := pco.put(ra.Rncui, rp, rl, rsb, &rs)
	if err != nil || rb == nil {
		return err
	}

	defer C.free(unsafe.Pointer(rb))
	*ra.Reply = C.GoBytes(unsafe.Pointer(rb), C.int(rs))

	return nil
}

/*
Get allows client to pass the encoded KV struct for reading
*/
func (pco *PmdbClientObj) Get(ra *PmdbReq) error {
	rp, rl := pmdbreqwrap(ra)
	if rp == nil || rl == 0 {
		return errors.New("Error encoding the request")
	}

	var rs int64
	var rb unsafe.Pointer
	var err error
	if len(ra.Rncui) == 0 {
		rb, err = pco.get_any(rp, rl, &rs)
	} else {
		rb, err = pco.get(ra.Rncui, rp, rl, &rs)
	}
	if err != nil || rb == nil {
		return err
	}

	defer C.free(unsafe.Pointer(rb))
	*ra.Reply = C.GoBytes(unsafe.Pointer(rb), C.int(rs))

	return nil
}

//Read the value of key on the client the application passed buffer
func (pco *PmdbClientObj) GetZeroCopy(ra *PmdbReq) error {

	var len int64
	//Encode the input buffer passed by client.
	ed_key, err := PumiceDBCommon.Encode(ra.Request, &len)
	if err != nil {
		return err
	}

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	//Read the value of the key in application buffer
	return pco.get_zero_copy(ra.Rncui, encoded_key,
		len, ra.ZeroCopyObj)
}

//Get the Leader UUID.
func (pco *PmdbClientObj) PmdbGetLeader() (uuid.UUID, error) {

	var leader_info C.raft_client_leader_info_t
	Cpmdb := (C.pmdb_t)(pco.pmdb)

	rc := C.PmdbGetLeaderInfo(Cpmdb, &leader_info)
	if rc != 0 {
		return uuid.Nil, fmt.Errorf("Failed to get leader info (%d)", rc)
	}

	//C uuid to Go bytes
	return uuid.FromBytes(C.GoBytes(unsafe.Pointer(&leader_info.rcli_leader_uuid),
		C.int(unsafe.Sizeof(leader_info.rcli_leader_uuid))))
}

//export PmdbAsyncReqCompletionCB
func PmdbAsyncReqCompletionCB(args unsafe.Pointer, size C.ssize_t) {
	ch := gopointer.Restore(args).(*chan int)
	*ch <- int(size)
}

// Call the pmdb C library function to write the application data.
// If application expects response on write operation,
// get_response should be 1
func (pco *PmdbClientObj) put(rncui string, obj *C.char, len int64,
	get_response C.int, replySize *int64) (unsafe.Pointer, error) {

	
	var rncui_id C.struct_raft_net_client_user_id
	rncuiStrC := GoToCString(rncui)
	defer FreeCMem(rncuiStrC)
	C.raft_net_client_user_id_parse(rncuiStrC, &rncui_id, 0)

	//To respect CGO memory invarients of 2nd level pointers
	stat := (*C.pmdb_obj_stat_t) (C.malloc(C.size_t(unsafe.Sizeof(C.pmdb_obj_stat_t{}))))
	defer C.free(unsafe.Pointer(stat))

	//Get the completion callback function pointer
	reqComplCh := make(chan int, 1)
	defer close(reqComplCh)
	args := gopointer.Save(&reqComplCh)
	defer gopointer.Unref(args)
	cb := C.getAsyncReqCompCB()

	var pmdb_req_opt C.pmdb_request_opts_t
	C.pmdb_request_options_init(&pmdb_req_opt, 1, 1, get_response, stat, cb, args, 
								nil, 0, 0);

	rc := C.PmdbObjPutX(pco.pmdb, (*C.pmdb_obj_id_t)(&rncui_id.rncui_key),
		obj, GoToCSize_t(len), &pmdb_req_opt)

	if rc != 0 {
		return nil, fmt.Errorf("PmdbObjPut(): %d", rc)
	}

	//Await for the request response!
	err := <-reqComplCh
	if err != 0 {
		return nil, fmt.Errorf("PmdbObjPut(): %d", err)
	}

	get_response_go := int(get_response)
	if get_response_go == 1 {
		reply_buf := stat.reply_buffer
		*replySize = int64(stat.reply_size)
		return reply_buf, nil
	}

	return nil, nil
}

//Call the pmdb C library function to read the value for the key.
func (pco *PmdbClientObj) get(rncui string, obj *C.char, len int64,
	replySize *int64) (unsafe.Pointer, error) {

	rncuiStrC := GoToCString(rncui)
	defer FreeCMem(rncuiStrC)
	var rncui_id C.struct_raft_net_client_user_id
	C.raft_net_client_user_id_parse(rncuiStrC, &rncui_id, 0)

	//To respect CGO memory invarients of 2nd level pointers
	stat := (*C.pmdb_obj_stat_t) (C.malloc(C.size_t(unsafe.Sizeof(C.pmdb_obj_stat_t{}))))
	defer C.free(unsafe.Pointer(stat))

	//Get the completion callback function pointer
	reqComplCh := make(chan int, 1)
	defer close(reqComplCh)
	args := gopointer.Save(&reqComplCh)
	defer gopointer.Unref(args)
	cb := C.getAsyncReqCompCB()

	var pmdb_req_opt C.pmdb_request_opts_t
	C.pmdb_request_options_init(&pmdb_req_opt, 1, 1, 1, stat, cb, args, 
								nil, 0, 0);

	rc := C.PmdbObjGetX(pco.pmdb, (*C.pmdb_obj_id_t)(&rncui_id.rncui_key),
			obj, GoToCSize_t(len), &pmdb_req_opt)
	if rc != 0 {
		return nil, fmt.Errorf("PmdbObjGetX(): %d", rc)
	}

	err := <-reqComplCh
	if err != 0 {
		return nil, fmt.Errorf("PmdbObjGetX(): %d", err)
	}

	vsize := stat.reply_size
	reply_buf := stat.reply_buffer

	if reply_buf == nil {
		*replySize = 0
		err := errors.New("Key not found")
		return nil, err
	}

	*replySize = int64(vsize)

	return reply_buf, nil
}

//Call the pmdb C library function to read the value for the key.
func (pco *PmdbClientObj) get_any(obj *C.char, len int64,
	replySize *int64) (unsafe.Pointer, error) {

	//To respect CGO memory invarients of 2nd level pointers
	stat := (*C.pmdb_obj_stat_t) (C.malloc(C.size_t(unsafe.Sizeof(C.pmdb_obj_stat_t{}))))
	defer C.free(unsafe.Pointer(stat))

	//Get the completion callback function pointer
	reqComplCh := make(chan int, 1)
	defer close(reqComplCh)
	args := gopointer.Save(&reqComplCh)
	defer gopointer.Unref(args)
	cb := C.getAsyncReqCompCB()

	var pmdb_req_opt C.pmdb_request_opts_t
	C.pmdb_request_options_init(&pmdb_req_opt, 1, 1, 1, stat, cb, args, 
								nil, 0, 0);

	rc := C.PmdbObjGetAnyX(pco.pmdb, obj, GoToCSize_t(len), &pmdb_req_opt)
	if rc != 0 {
		return nil, fmt.Errorf("PmdbObjGetAnyX(): %d", rc)
	}

	err := <-reqComplCh
	if err != 0 {
		return nil, fmt.Errorf("PmdbObjGetAnyX(): %d", err)
	}

	vsize := stat.reply_size
	reply_buf := stat.reply_buffer

	if reply_buf == nil {
		*replySize = 0
		err := errors.New("Key not found")
		return nil, err
	}

	*replySize = int64(vsize)

	return reply_buf, nil
}

//Allocate memory in C heap
func (pco *RDZeroCopyObj) AllocateCMem(size int64) unsafe.Pointer {
	return C.malloc(C.size_t(size))
}

//Relase the C memory allocated for reading the value
func (pco *RDZeroCopyObj) ReleaseCMem() {
	C.free(pco.buffer)
}

/*
 * Note the data is not decoded in this method. Application should
 * take care of decoding the buffer data.
 */
func (pco *PmdbClientObj) get_zero_copy(rncui string, key *C.char,
	len int64, zco *RDZeroCopyObj) error {

	rncuiStrC := GoToCString(rncui)
	defer FreeCMem(rncuiStrC)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(rncuiStrC, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	var stat C.pmdb_obj_stat_t
	var pmdb_req_opt C.pmdb_request_opts_t

	C.pmdb_request_options_init(&pmdb_req_opt, 1, 0, 0, &stat, nil, nil,
		zco.buffer, C.size_t(zco.buffer_len), 0)

	rc := C.PmdbObjGetX(pco.pmdb, obj_id, key, GoToCSize_t(len),
		&pmdb_req_opt)

	if rc != 0 {
		return fmt.Errorf("PmdbObjGetX(): return code: %d", rc)
	}

	return nil
}

// Return the decode / encode size of the provided object
func (pco *PmdbClientObj) GetSize(ed interface{}) int64 {
	return PumiceDBCommon.GetStructSize(ed)
}

// Decode in the input buffer into the output object
// XXX note this function *should* return an error
func (pco *PmdbClientObj) Decode(input unsafe.Pointer, output interface{},
	len int64) error {
	return PumiceDBCommon.Decode(input, output, len)
}

// Stop the Pmdb client instance
func (pco *PmdbClientObj) Stop() error {
	if !pco.initialized {
		return errors.New("Client object is not initialized")
	}

	rc := C.PmdbClientDestroy((C.pmdb_t)(pco.pmdb))
	if rc != 0 {
		return fmt.Errorf("PmdbClientDestroy() returned %d", rc)
	}

	C.free(unsafe.Pointer(pco.raftUUID))
	C.free(unsafe.Pointer(pco.clientUUID))

	pco.initialized = false

	return nil
}

//Start the Pmdb client instance
func (pco *PmdbClientObj) Start() error {
	if pco.initialized == true {
		return errors.New("Client object is already initialized")
	}

	//Start the client
	pco.pmdb = C.PmdbClientStart(pco.raftUUID, pco.clientUUID)
	if pco.pmdb == nil {
		var errno syscall.Errno
		return fmt.Errorf("PmdbClientStart(): %d", errno)
	}

	pco.initialized = true

	return nil
}

func PmdbClientNew(raftUUID string, clientUUID string) (*PmdbClientObj, error) {
	var pco PmdbClientObj

	pco.initialized = false

	pco.raftUUID = C.CString(raftUUID)
	if pco.raftUUID == nil {
		return nil, errors.New("Memory allocation error")
	}

	pco.clientUUID = C.CString(clientUUID)
	if pco.clientUUID == nil {
		C.free(unsafe.Pointer(pco.raftUUID))
		return nil, errors.New("Memory allocation error")
	}

	return &pco, nil
}