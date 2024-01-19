package PumiceDBClient

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"strconv"
	"syscall"
	"unsafe"

	"github.com/google/uuid"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft_client -lniova_pumice_client
#include <pumice_db_client.h>
#include <pumice_db_net.h>
*/
import "C"

type PmdbReqArgs struct {
	Rncui         string
	IRequest      interface{}     // Decode source which fills 'request'
	IResponse     interface{}     // Decode destination filled by 'response'
	request       []byte
	response      *[]byte
	responseLen   int64
	GetResponse   int
	ZeroCopyObj   *RDZeroCopyObj
	AllowEmptyRncui bool
	PmdbClientObj *PmdbClientObj
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

// Getter method for request, response, and responseLen
func (p *PmdbReqArgs) GetPmdbData() ([]byte, *[]byte, int64) {
    return p.request, p.response, p.responseLen
}

// Setter method for request, response, and responseLen
func (p *PmdbReqArgs) SetPmdbData(req []byte, resp *[]byte, len int64) {
    p.request = req
    p.response = resp
    p.responseLen = len
}

func (r *PmdbReqArgs) Write() error {
	return r.writeInternal() // Perform the write
}

// Encode the user's portion of the request
func (r *PmdbReqArgs) encodeRequestPayload() error {
	// If 'request' is present then the contents are already here
	if r.request != nil { // Already encoded
		return nil
	}

	if r.IRequest == nil {
		return fmt.Errorf("IRequest is not present")
	}

	var b bytes.Buffer

	err := gob.NewEncoder(&b).Encode(r.IRequest)
	if err == nil {
		r.request = b.Bytes()
	}

	return err
}

// Encode the user's data along w/ the pmdb request
func (r *PmdbReqArgs) encodeRequest() (unsafe.Pointer, int64, error) {
	if r.request == nil {
		return nil, 0, fmt.Errorf("r.request is nil")
	}

	pmdbReq := &PumiceDBCommon.PumiceRequest {
		ReqType: PumiceDBCommon.APP_REQ,
		ReqPayload: r.request,
	}

	var len int64
	ptr, err := PumiceDBCommon.Encode(pmdbReq, &len)

	return ptr, len, err
}

func (r *PmdbReqArgs) getObjectID(o *C.pmdb_obj_id_t)  error {
	rncuiStrC := GoToCString(r.Rncui)
	defer FreeCMem(rncuiStrC)

	var err error
	var rncui_id C.struct_raft_net_client_user_id

	ret := C.raft_net_client_user_id_parse(rncuiStrC, &rncui_id, 0)

	if ret != 0 {
		err = fmt.Errorf("C.raft_net_client_user_id_parse(): %d", ret)
	} else {
		*o = rncui_id.rncui_key
	}

	return err
}

// Read performs the read operation for the given request arguments
func (r *PmdbReqArgs) Read() error {
        if r.PmdbClientObj == nil {
                return fmt.Errorf("Missing pmdbClientObj")
        }

        // Make available r.request, encode user payload if not already
        err := r.encodeRequestPayload()
        if err != nil {
                return err
        }

        // Encode the entire request
        pmdbReqPtr, pmdbReqSz, pmdbErr := r.encodeRequest()
        if pmdbErr != nil {
                return pmdbErr
        }

        if pmdbReqPtr == nil {
                return fmt.Errorf("encodeRequest() returned nil req pointer")
        }

        var rd_err error
        var replySize int64

        if len(r.Rncui) == 0 {
                rd_err = r.readInternalAny(pmdbReqPtr, pmdbReqSz, &replySize)
        } else {
                rd_err = r.readInternal(r.Rncui, pmdbReqPtr, pmdbReqSz, &replySize)
        }

        return rd_err
}

//ReadEncoded
/*
ReadEncoded allows client to pass the encoded KV struct for reading
*/
/*
func (obj *PmdbClientObj) ReadEncoded(reqArgs *PmdbReqArgs) error {
	var replySize int64
	var rd_err error
	var replyB unsafe.Pointer

	//Convert it to unsafe pointer (void * for C function)
	eData := unsafe.Pointer(&reqArgs.request[0])
	reqLen := int64(len(reqArgs.request))
	eReq := (*C.char)(eData)

	if len(reqArgs.Rncui) == 0 {
		replyB, rd_err = obj.readKVAny(eReq,
			reqLen, &replySize)
	} else {
		replyB, rd_err = obj.readKV(reqArgs.Rncui, eReq,
			reqLen, &replySize)
	}

	if rd_err != nil {
		return rd_err
	}

	if replyB != nil {
		bytes_data := C.GoBytes(unsafe.Pointer(replyB), C.int(replySize))
		buffer := bytes.NewBuffer(bytes_data)
		*reqArgs.response = buffer.Bytes()
	}
	//Free the buffer allocated by C library.
	C.free(replyB)
	return nil
}
*/

// readKV performs the read operation for the given request arguments with Rncui
func (r *PmdbReqArgs) readInternal(rncui string, key unsafe.Pointer, keyLen int64, replySize *int64) error {
        var vsize C.size_t

	var obj_id C.pmdb_obj_id_t

	err := r.getObjectID(&obj_id)
        if err != nil {
                return err
        }

        replyB := C.PmdbObjGet(r.PmdbClientObj.pmdb, &obj_id, (*C.char)(key), GoToCSize_t(keyLen), &vsize)
        if replyB == nil {
                *replySize = 0
                return errors.New("Key not found")
        }
        *replySize = int64(vsize)
        // Decode the response
    if err := PumiceDBCommon.Decode(unsafe.Pointer(replyB), r.IResponse, int64(vsize)); err != nil {
        return fmt.Errorf("Failed to decode response: %v", err)
    }
    	C.free(replyB)
	return nil
}

// readKVAny performs the read operation for the given request arguments without Rncui

func (r *PmdbReqArgs) readInternalAny(key unsafe.Pointer, keyLen int64, replySize *int64) error {
    var vsize C.size_t

    replyB := C.PmdbObjGetAny(r.PmdbClientObj.pmdb, (*C.char)(key), GoToCSize_t(keyLen), &vsize)

    if replyB == nil {
        *replySize = 0
        return errors.New("Key not found")
    }

    *replySize = int64(vsize)

    // Use r.ResponseED to decode the response if needed
    // Example: err := PumiceDBCommon.Decode(unsafe.Pointer(replyB), r.ResponseED, int64(vsize))
    // Check for any errors during decoding and handle accordingly
    // Decode the response
    if err := PumiceDBCommon.Decode(unsafe.Pointer(replyB), r.IResponse, int64(vsize)); err != nil {
        return fmt.Errorf("Failed to decode response: %v", err)
    }
    // Free the buffer allocated by C library.
    C.free(replyB)

    return nil
}

//Read the value of key on the client the application passed buffer
func (obj *PmdbClientObj) ReadZeroCopy(reqArgs *PmdbReqArgs) error {

	var keyLen int64
	//Encode the input buffer passed by client.
	ed_key, err := PumiceDBCommon.Encode(reqArgs.IRequest, &keyLen)
	if err != nil {
		return err
	}

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	//Read the value of the key in application buffer
	return obj.readKVZeroCopy(reqArgs.Rncui, encoded_key,
		keyLen, reqArgs.ZeroCopyObj)
}

//Get the Leader UUID.
func (pmdb_client *PmdbClientObj) PmdbGetLeader() (uuid.UUID, error) {
	var leader_info C.raft_client_leader_info_t
	Cpmdb := (C.pmdb_t)(pmdb_client.pmdb)

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
func (r *PmdbReqArgs) writeInternal() error {
	if r.PmdbClientObj == nil {
		return fmt.Errorf("Missing pmdbClientObj")
	}

	if r.Rncui == "" {
		return fmt.Errorf("Missing rncui")
	}

	// Make available r.request, encode user payload it not already
	err := r.encodeRequestPayload()
	if err != nil {
		return err
	}

	// Encode the entire request
	pmdbReqPtr, pmdbReqSz, pmdbErr := r.encodeRequest()
	if pmdbErr != nil {
		return pmdbErr
	}

	if pmdbReqPtr == nil {
		return fmt.Errorf("encodeRequest() returned nil req pointer")
	}

	// Obtain the pmdb object id
	var oid C.pmdb_obj_id_t

	err = r.getObjectID(&oid)
	if err != nil {
		return err
	}

	var ostat C.pmdb_obj_stat_t
	rc := C.PmdbObjPut(r.PmdbClientObj.pmdb, &oid, (*C.char)(pmdbReqPtr),
		C.size_t(pmdbReqSz), C.int(r.GetResponse), &ostat)

	if rc != 0 {
		return fmt.Errorf("PmdbObjPut(): %d", rc)
	}

	if r.GetResponse == 1 {
		// Convert the unsafe.Pointer to a []byte
		reply := C.GoBytes(ostat.reply_buffer, C.int(ostat.reply_size))

		*r.response = bytes.NewBuffer(reply).Bytes()
	}

	return nil
}

//Allocate memory in C heap
func (obj *RDZeroCopyObj) AllocateCMem(size int64) unsafe.Pointer {
	return C.malloc(C.size_t(size))
}

//Relase the C memory allocated for reading the value
func (obj *RDZeroCopyObj) ReleaseCMem() {
	C.free(obj.buffer)
}

/*
 * Note the data is not decoded in this method. Application should
 * take care of decoding the buffer data.
 */
func (obj *PmdbClientObj) readKVZeroCopy(rncui string, key *C.char,
	keyLen int64,
	zeroCopyObj *RDZeroCopyObj) error {

	rncuiStrC := GoToCString(rncui)
	defer FreeCMem(rncuiStrC)

	keyLenC := GoToCSize_t(keyLen)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(rncuiStrC, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	var stat C.pmdb_obj_stat_t
	var pmdb_req_opt C.pmdb_request_opts_t

	C.pmdb_request_options_init(&pmdb_req_opt, 1, 0, 0, &stat, nil, nil,
		zeroCopyObj.buffer, C.size_t(zeroCopyObj.buffer_len), 0)

	rc := C.PmdbObjGetX(obj.pmdb, obj_id, key, keyLenC,
		&pmdb_req_opt)

	if rc != 0 {
		return fmt.Errorf("PmdbObjGetX(): return code: %d", rc)
	}

	return nil
}

// Return the decode / encode size of the provided object
func (obj *PmdbClientObj) GetSize(ed interface{}) int64 {
	return PumiceDBCommon.GetStructSize(ed)
}

// Decode in the input buffer into the output object
// XXX note this function *should* return an error
func (obj *PmdbClientObj) Decode(input unsafe.Pointer, output interface{},
	len int64) error {
	return PumiceDBCommon.Decode(input, output, len)
}

// Stop the Pmdb client instance
func (obj *PmdbClientObj) Stop() error {
	if obj.initialized == true {
		return errors.New("Client object is not initialized")
	}

	rc := C.PmdbClientDestroy((C.pmdb_t)(obj.pmdb))
	if rc != 0 {
		return fmt.Errorf("PmdbClientDestroy() returned %d", rc)
	}
	return nil
}

//Start the Pmdb client instance
func (obj *PmdbClientObj) Start() error {
	if obj.initialized == true {
		return errors.New("Client object is already initialized")
	}

	raftUuid := GoToCString(obj.raftUuid)
	if raftUuid == nil {
		return errors.New("Memory allocation error")
	}
	defer FreeCMem(raftUuid)

	clientUuid := GoToCString(obj.myUuid)
	if clientUuid == nil {
		return errors.New("Memory allocation error")
	}
	defer FreeCMem(clientUuid)

	//Start the client
	obj.pmdb = C.PmdbClientStart(raftUuid, clientUuid)
	if obj.pmdb == nil {
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
