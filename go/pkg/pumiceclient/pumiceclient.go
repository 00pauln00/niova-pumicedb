package pumiceclient

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	PumiceDBCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	"strconv"
	"syscall"
	"unsafe"

	"github.com/google/uuid"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft_client -lniova_pumice_client
#include <pumice/pumice_db_client.h>
#include <pumice/pumice_db_net.h>
*/
import "C"

type PmdbReqArgs struct {
	Rncui       string
	ReqED       interface{}
	ResponseED  interface{}
	ReqByteArr  []byte
	Response    *[]byte
	ReplySize   *int64
	GetResponse int
	ZeroCopyObj *RDZeroCopyObj
}

type PmdbClientObj struct {
	initialized bool
	pmdb        C.pmdb_t
	raftUuid    string
	myUuid      string
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
func getPmdbReq(ra *PmdbReqArgs) (unsafe.Pointer, int64) {
	// get bytes for requestResponse.Request and
	// convert PumiceDBCommon.PumiceRequest
	var req PumiceDBCommon.PumiceRequest

	req.ReqType = PumiceDBCommon.APP_REQ
	req.ReqPayload = ra.ReqByteArr

	var reqLen int64
	reqPtr, err := PumiceDBCommon.Encode(req, &reqLen)
	if err != nil {
		return nil, 0
	}

	return reqPtr, reqLen
}

func (pco *PmdbClientObj) Put(ra *PmdbReqArgs) (unsafe.Pointer, error) {

	var rBytes bytes.Buffer
	var err error
	var wr_err error
	var replyB unsafe.Pointer
	var replySize int64

	enc := gob.NewEncoder(&rBytes)
	err = enc.Encode(ra.ReqED)
	if err != nil {
		return nil, err
	}

	ra.ReqByteArr = rBytes.Bytes()

	//Convert to unsafe pointer (void * for C function)
	eData, len := getPmdbReq(ra)

	//Typecast the encoded key to char*
	ekey := (*C.char)(eData)
	getResC := (C.int)(ra.GetResponse)

	//Perform the put
	return pco.put(ra.Rncui, ekey, len, getResC, ra.ReplySize)
}

/*
* PutEncoded allows client to pass an already encoded object for writing
 */
func (pco *PmdbClientObj) PutEncoded(ra *PmdbReqArgs) (unsafe.Pointer, error) {
	//Convert it to unsafe pointer (void * for C function)
	eData := unsafe.Pointer(&ra.ReqByteArr[0])
	len := int64(len(ra.ReqByteArr))
	eReq := (*C.char)(eData)
	getResC := (C.int)(ra.GetResponse)

	return pco.put(ra.Rncui, eReq, len, getResC, ra.ReplySize)
}

func (pco *PmdbClientObj) PutEncodedAndGetResponse(ra *PmdbReqArgs) error {
	var replySize int64
	var wr_err error
	var replyB unsafe.Pointer

	replyB, wr_err = pco.PutEncoded(ra)
	if wr_err != nil {
		return wr_err
	}

	if replyB != nil {
		bytes_data := C.GoBytes(unsafe.Pointer(replyB), C.int(replySize))
		buffer := bytes.NewBuffer(bytes_data)
		*ra.Response = buffer.Bytes()
	}
	// Free the buffer allocated by the C library
	C.free(replyB)
	return nil
}

func (pco *PmdbClientObj) Get(ra *PmdbReqArgs) error {

	var replySize int64
	var rd_err error
	var replyB unsafe.Pointer
	var requestBytes bytes.Buffer
	var err error

	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(ra.ReqED)
	if err != nil {
		return err
	}

	ra.ReqByteArr = requestBytes.Bytes()

	//Convert to unsafe pointer (void * for C function)
	eData, reqLen := getPmdbReq(ra)

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(eData)

	if len(ra.Rncui) == 0 {
		replyB, rd_err = pco.get_any(encoded_key,
			reqLen, &replySize)
	} else {
		replyB, rd_err = pco.get(ra.Rncui, encoded_key,
			reqLen, &replySize)
	}

	if rd_err != nil {
		return rd_err
	}

	if replyB != nil {
		err = PumiceDBCommon.Decode(unsafe.Pointer(replyB),
			ra.ResponseED,
			replySize)
	}
	//Free the buffer allocated by C library.
	C.free(replyB)
	return err
}

/*
GetEncoded allows client to pass the encoded KV struct for reading
*/
func (pco *PmdbClientObj) GetEncoded(ra *PmdbReqArgs) error {
	var replySize int64
	var rd_err error
	var replyB unsafe.Pointer

	//Convert it to unsafe pointer (void * for C function)
	eData := unsafe.Pointer(&ra.ReqByteArr[0])
	reqLen := int64(len(ra.ReqByteArr))
	eReq := (*C.char)(eData)

	if len(ra.Rncui) == 0 {
		replyB, rd_err = pco.get_any(eReq,
			reqLen, &replySize)
	} else {
		replyB, rd_err = pco.get(ra.Rncui, eReq,
			reqLen, &replySize)
	}

	if rd_err != nil {
		return rd_err
	}

	if replyB != nil {
		bytes_data := C.GoBytes(unsafe.Pointer(replyB), C.int(replySize))
		buffer := bytes.NewBuffer(bytes_data)
		*ra.Response = buffer.Bytes()
	}
	//Free the buffer allocated by C library.
	C.free(replyB)
	return nil
}

//Read the value of key on the client the application passed buffer
func (pco *PmdbClientObj) GetZeroCopy(ra *PmdbReqArgs) error {

	var len int64
	//Encode the input buffer passed by client.
	ed_key, err := PumiceDBCommon.Encode(ra.ReqED, &len)
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

// Call the pmdb C library function to write the application data.
// If application expects response on write operation,
// get_response should be 1
func (pco *PmdbClientObj) put(rncui string, obj *C.char, len int64,
	get_response C.int, replySize *int64) (unsafe.Pointer, error) {

	var stat C.pmdb_obj_stat_t

	rncuiStrC := GoToCString(rncui)
	defer FreeCMem(rncuiStrC)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(rncuiStrC, &rncui_id, 0)

	rc := C.PmdbObjPut(pco.pmdb, (*C.pmdb_obj_id_t)(&rncui_id.rncui_key),
		obj, GoToCSize_t(len), get_response, &stat)

	if rc != 0 {
		return nil, fmt.Errorf("PmdbObjPut(): %d", rc)
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

	var vsize C.size_t

	reply_buf :=
		C.PmdbObjGet(pco.pmdb, (*C.pmdb_obj_id_t)(&rncui_id.rncui_key),
			obj, GoToCSize_t(len), &vsize)

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

	var vsize C.size_t

	reply_buf := C.PmdbObjGetAny(pco.pmdb, obj, GoToCSize_t(len), &vsize)

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
	if pco.initialized == true {
		return errors.New("Client object is not initialized")
	}

	rc := C.PmdbClientDestroy((C.pmdb_t)(pco.pmdb))
	if rc != 0 {
		return fmt.Errorf("PmdbClientDestroy() returned %d", rc)
	}
	return nil
}

//Start the Pmdb client instance
func (pco *PmdbClientObj) Start() error {
	if pco.initialized == true {
		return errors.New("Client object is already initialized")
	}

	raftUuid := GoToCString(pco.raftUuid)
	if raftUuid == nil {
		return errors.New("Memory allocation error")
	}
	defer FreeCMem(raftUuid)

	clientUuid := GoToCString(pco.myUuid)
	if clientUuid == nil {
		return errors.New("Memory allocation error")
	}
	defer FreeCMem(clientUuid)

	//Start the client
	pco.pmdb = C.PmdbClientStart(raftUuid, clientUuid)
	if pco.pmdb == nil {
		var errno syscall.Errno
		return fmt.Errorf("PmdbClientStart(): %d", errno)
	}

	return nil
}

func PmdbClientNew(Graft_uuid string, Gclient_uuid string) *PmdbClientObj {
	var client PmdbClientObj

	client.initialized = false
	client.raftUuid = Graft_uuid
	client.myUuid = Gclient_uuid

	return &client
}
