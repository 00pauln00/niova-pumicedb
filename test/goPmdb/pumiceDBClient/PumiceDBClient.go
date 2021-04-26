package PumiceDBClient
import (
	"fmt"
	"unsafe"
	"strconv"
	"gopmdblib/goPmdbCommon"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft_client -lniova_pumice_client
#include <raft/pumice_db_client.h>
*/
import "C"

type PmdbClientObj struct {
	Pmdb unsafe.Pointer
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

//Write KV from client.
func (pmdb_client *PmdbClientObj) PmdbClientWrite(ed interface{}, rncui string) {

	var key_len int64
	//Encode the structure into void pointer.
	ed_key := PumiceDBCommon.Encode(ed, &key_len)

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	//Perform the write
	PmdbClientWriteKV(pmdb_client.Pmdb, rncui, encoded_key, key_len)
}

//Read the value of key on the client
func (pmdb_client *PmdbClientObj) PmdbClientRead(ed interface{},
												  rncui string,
												  value unsafe.Pointer,
												  value_len int64,
												  reply_size *int64) int {
	//Byte array
	fmt.Println("Client: Read Value for the given Key")

	var key_len int64
	//Encode the input buffer passed by client.
	ed_key := PumiceDBCommon.Encode(ed, &key_len)

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	value_ptr := (*C.char)(value)

	return PmdbClientReadKV(pmdb_client.Pmdb, rncui, encoded_key,
							key_len, value_ptr, value_len, reply_size)
}

func PmdbStartClient(Graft_uuid string, Gclient_uuid string) unsafe.Pointer {

	raft_uuid := GoToCString(Graft_uuid)

	client_uuid := GoToCString(Gclient_uuid)

	//Start the client.
	Cpmdb := C.PmdbClientStart(raft_uuid, client_uuid)

	//Free C memory
	FreeCMem(raft_uuid)
	FreeCMem(client_uuid)
	return unsafe.Pointer(Cpmdb)
}

func PmdbClientWriteKV(pmdb unsafe.Pointer, rncui string, key *C.char,
					 key_len int64) {

	var obj_stat C.pmdb_obj_stat_t

	crncui_str := GoToCString(rncui)

	c_key_len := GoToCSize_t(key_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	Cpmdb := (C.pmdb_t)(pmdb)

	C.PmdbObjPut(Cpmdb, obj_id, key, c_key_len, &obj_stat)

	//Free C memory
	FreeCMem(crncui_str)
}

func PmdbClientReadKV(pmdb unsafe.Pointer, rncui string, key *C.char,
					  key_len int64, value *C.char, value_len int64,
					  reply_size *int64) int {
	var obj_stat C.pmdb_obj_stat_t

	crncui_str := GoToCString(rncui)

	c_key_len := GoToCSize_t(key_len)
	c_value_len := GoToCSize_t(value_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	Cpmdb := (C.pmdb_t)(pmdb)
	rc := C.PmdbObjGetX(Cpmdb, obj_id, key, c_key_len, value, c_value_len,
			&obj_stat)

	*reply_size = int64(obj_stat.reply_size)

	fmt.Println("Reply size is: ", obj_stat.reply_size)
	fmt.Println("Reply size is int(): ", int64(obj_stat.reply_size))
	//Free C memory
	FreeCMem(crncui_str)

	return int(rc)
}