package PumiceDBServer
import (
	"fmt"
	"unsafe"
	"reflect"
	"strconv"
	"gopmdblib/goPmdbCommon"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft -lniova_pumice -lrocksdb
#include <raft/pumice_db.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
#include <raft/pumice_db_client.h>
extern void applyCgo(const struct raft_net_client_user_id *, const void *,
                     size_t, void *, void *);
extern size_t readCgo(const struct raft_net_client_user_id *, const void *,
                    size_t, void *, size_t, void *);
*/
import "C"

import gopointer "github.com/mattn/go-pointer"

type PmdbApplyCallback func(unsafe.Pointer, unsafe.Pointer, int64,
                          unsafe.Pointer)
type PmdbReadCallback func(unsafe.Pointer, unsafe.Pointer, int64,
                         unsafe.Pointer, int64) int64

//Callback function for pmdb apply and read
type PmdbCallbacks struct {
	ApplyCb PmdbApplyCallback
	ReadCb PmdbReadCallback
}

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

/* Type cast C char * to Go string */
func CToGoString(cstring *C.char) string {
	return C.GoString(cstring)
}

/*
 The following goApply and goRead functions are the exported
 functions which is needed for calling the golang function
 pointers from C.
*/

//export goApply
func goApply(app_id *C.struct_raft_net_client_user_id, input_buf unsafe.Pointer,
			input_buf_sz C.size_t, pmdb_handle unsafe.Pointer,
            user_data unsafe.Pointer) {

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(user_data).(*PmdbCallbacks)

	//Convert buffer size from c data type size_t to golang int64.
	input_buf_sz_go := CToGoInt64(input_buf_sz)

	//Calling the golang Application's Apply function.
	gcb.ApplyCb(unsafe.Pointer(app_id), input_buf, input_buf_sz_go, pmdb_handle)
}

//export goRead
func goRead(app_id *C.struct_raft_net_client_user_id, request_buf unsafe.Pointer,
            request_bufsz C.size_t, reply_buf unsafe.Pointer, reply_bufsz C.size_t,
            user_data unsafe.Pointer) int64 {

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(user_data).(*PmdbCallbacks)

	//Convert buffer size from c data type size_t to golang int64.
	request_bufsz_go := CToGoInt64(request_bufsz)
	reply_bufsz_go := CToGoInt64(reply_bufsz)

	//Calling the golang Application's Read function.
	return gcb.ReadCb(unsafe.Pointer(app_id), request_buf, request_bufsz_go, reply_buf, reply_bufsz_go)
}

/**
 * Start the pmdb server.
 * @raft_uuid: Raft UUID.
 * @peer_uuid: Peer UUID.
 * @cf: Column Family
 * @cb: PmdbAPI callback funcs.
 */
func PmdbStartServer(raft_uuid string, peer_uuid string, cf string,
					 cb *PmdbCallbacks) {

	/*
	 * Convert the raft_uuid and peer_uuid go strings into C strings
	 * so that we can pass these to C function.
	 */

	raft_uuid_c := GoToCString(raft_uuid)

	peer_uuid_c := GoToCString(peer_uuid)

	cCallbacks := C.struct_PmdbAPI{}

	//Assign the callback functions for apply and read
	cCallbacks.pmdb_apply = C.pmdb_apply_sm_handler_t(C.applyCgo)
	cCallbacks.pmdb_read = C.pmdb_read_sm_handler_t(C.readCgo)

	/*
	 * Store the column family name into char * array.
	 * Store gostring to byte array.
	 * Don't forget to append the null terminating character.
	 */
	cf_byte_arr := []byte(cf + "\000")

	cf_name := make(charsSlice, len(cf_byte_arr))
	cf_name[0] = (*C.char)(C.CBytes(cf_byte_arr))

	//Convert Byte array to char **
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&cf_name))
	cf_array := (**C.char)(unsafe.Pointer(sH.Data))

	// Create an opaque C pointer for cbs to pass to PmdbStartServer.
	opa_ptr:= gopointer.Save(cb)
	defer gopointer.Unref(opa_ptr)

	// Starting the pmdb server.
	C.PmdbExec(raft_uuid_c, peer_uuid_c, &cCallbacks, cf_array, 1, true, opa_ptr)

	//Free the C memory
	FreeCMem(raft_uuid_c)
	FreeCMem(peer_uuid_c)

}

func PmdbLookupKey(key string, key_len int64, value string,
				   go_cf string) string {

	var goerr string
	var C_value_len C.size_t

	err := GoToCString(goerr)

	cf := GoToCString(go_cf)

	//Convert go string to C char *
	C_key := GoToCString(key)

	C_key_len := GoToCSize_t(key_len)

	//Get the column family handle
	fmt.Println("Lookup for key :", key)
	fmt.Println("key length:", key_len)
	cf_handle := C.PmdbCfHandleLookup(cf)

	ropts := C.rocksdb_readoptions_create()

	C_value := C.rocksdb_get_cf(C.PmdbGetRocksDB(), ropts, cf_handle, C_key,
							  C_key_len, &C_value_len, &err)

	C.rocksdb_readoptions_destroy(ropts)

	go_value := CToGoString(C_value)
	fmt.Println("Value returned by rocksdb_get_cf is: ", go_value)

	go_value_len := CToGoInt64(C_value_len)
	fmt.Println("Value len is", go_value_len)

	result := go_value
	if result != "" {
		result = go_value[0:go_value_len]
	}
	fmt.Println("Result of the lookup is: ", result)

	//Free C memory
	FreeCMem(err)
	FreeCMem(cf)
	FreeCMem(C_key)
	FreeCMem(C_value)

	return result
}

func PmdbWriteKV(app_id unsafe.Pointer, pmdb_handle unsafe.Pointer, key string,
			   key_len int64, value string, value_len int64, gocolfamily string) {

	//typecast go string to C char *
	cf := GoToCString(gocolfamily)

	C_key := GoToCString(key)

	C_key_len := GoToCSize_t(key_len)

	C_value := GoToCString(value)

	C_value_len := GoToCSize_t(value_len)

	capp_id := (*C.struct_raft_net_client_user_id)(app_id)

	cf_handle := C.PmdbCfHandleLookup(cf)
	fmt.Println("key", key)
	fmt.Println("value", value)

	//Calling pmdb library function to write Key-Value.
	C.PmdbWriteKV(capp_id, pmdb_handle, C_key, C_key_len, C_value, C_value_len, nil, unsafe.Pointer(cf_handle))

	//Free C memory
	FreeCMem(cf)
	FreeCMem(C_key)
	FreeCMem(C_value)
}

func PmdbReadKV(app_id unsafe.Pointer, key string,
			    key_len int64, gocolfamily string) string {

	var value string
	//Convert the golang string to C char*

	fmt.Println("Read request for key: ", key)
	go_value := PmdbLookupKey(key, key_len, value, gocolfamily)

	//Get the result
	fmt.Println("Value is: ", go_value)
	return go_value
}

func PmdbCopyDataToBuffer(ed interface{}, buffer unsafe.Pointer) int64 {
	var key_len int64
	//Encode the structure into void pointer.
	encoded_key := PumiceDBCommon.Encode(ed, &key_len)

	//Copy the encoded structed into buffer
	C.memcpy(buffer, encoded_key, C.size_t(key_len))

	return key_len
}