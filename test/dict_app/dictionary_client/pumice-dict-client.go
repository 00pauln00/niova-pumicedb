package main
import (
	"fmt"
	"os"
	"bufio"
	"strings"
	"flag"
	"gopmdblib/goPmdbClient"
	"gopmdblib/goPmdbCommon"
	"dictapplib/dict_libs"
)

/*
#include <stdlib.h>
*/
import "C"

var raft_uuid_go string
var peer_uuid_go string

/*
 Start the pmdb client.
 Read the request from console and process it.
 User will pass the request in following format.
 app_uuid.Text.write => For write opertion.
 app_uuid.Word.write  => For read operation.
*/
func pmdbDictClient() {

	//Start the client.
	pmdb := PumiceDBClient.PmdbStartClient(raft_uuid_go, peer_uuid_go)

	client_obj := PumiceDBClient.PmdbClientObj{
		Pmdb: pmdb,
	}


	for {
		//Read the input from console
		words := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("Enter request in the format ")
			fmt.Print("app_uuid.text.write/read: ")
			text, _ := words.ReadString('\n')

			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			input := strings.Split(text, ".")
			rncui := input[0]
			input_text := input[1]
			ops := input[2]

			//Format is: AppUUID.text.write or AppUUID.text.read
			// Prepare the dictionary structure from values passed by user.
			req_dict := DictAppLib.Dict_app{
				Dict_text: input_text,
				Dict_wcount: 0,
			}


			fmt.Println("rncui: ", rncui)
			fmt.Println("Operation: ", ops)
			fmt.Println("Input string: ", req_dict.Dict_text)

			if ops == "write" {
				//write operation
				client_obj.PmdbClientWrite(req_dict, rncui)
			} else {
				/*
				 * Get the size of the structure
				 */
				data_length := PumiceDBCommon.GetStructSize(req_dict)
				fmt.Println("Length of the structure: ", data_length)
				rc := -1
				/* Retry the read on failure */
				for ok := true; ok; ok = (rc < 0) {

					// Allocate C memory to store the value of the result.
					fmt.Println("Allocating buffer of size: ", data_length)
					value_buf := C.malloc(C.size_t(data_length))

					var reply_size int64
					//read operation
					rc = client_obj.PmdbClientRead(req_dict, rncui, value_buf,
												   int64(data_length), &reply_size)

					if rc < 0 {
						fmt.Println("Read request failed, error: ", rc)
						//if rc == os.E2BIG {
						if reply_size > data_length {
							fmt.Println("Allocate bigger buffer and retry read operation: ", data_length)
							data_length = reply_size
						}
					} else {
						result_dict := &DictAppLib.Dict_app{}
						PumiceDBCommon.Decode(value_buf, result_dict, reply_size)

						fmt.Println("Result of the read request is:")
						fmt.Println("Word: ", input_text)
						fmt.Println("Frequecy of the word: ", result_dict.Dict_wcount)
					}

					C.free(value_buf)
				}
			}
		}
	}
}

func pmdb_dict_app_getopts() {

	flag.StringVar(&raft_uuid_go, "raft", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid_go, "peer", "NULL", "peer uuid")

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Peer UUID: ", peer_uuid_go)
}

func main() {
	//Parse the cmdline parameter
	pmdb_dict_app_getopts()

	//Start pmdbDictionary client.
	pmdbDictClient()
}