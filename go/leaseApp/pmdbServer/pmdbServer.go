package main

import (
	"bytes"
	"common/requestResponseLib"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	uuid "github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft -lniova_pumice -lrocksdb
#include <stdlib.h>
#include <string.h>
#include <raft/pumice_db.h>
#include <raft/raft_net.h>
#include <raft/pumice_db_client.h>
*/
import "C"

var seqno = 0
var ttlDefault = 60

// Use the default column family
var colmfamily = "PMDBTS_CF"

const (
	INPROGRESS int = 0
	GRANTED        = 1
	EXPIRED        = 2
)

type leaseServer struct {
	raftUUID string
	peerUUID string
	logDir   string
	logLevel string
	leaseMap map[uuid.UUID]*requestResponseLib.LeaseStruct
	pso      *PumiceDBServer.PmdbServerObject
}

func main() {
	lso, pErr := parseArgs()
	if pErr != nil {
		log.Println(pErr)
		return
	}

	switch lso.logLevel {
	case "Info":
		log.SetLevel(log.InfoLevel)
	case "Trace":
		log.SetLevel(log.TraceLevel)
	}

	//Create log file
	err := PumiceDBCommon.InitLogger(lso.logDir)
	if err != nil {
		log.Error("Error while initating logger ", err)
		os.Exit(1)
	}

	if err != nil {
		log.Fatal("Error while initializing serf agent ", err)
	}

	log.Info("Raft and Peer UUID: ", lso.raftUUID, " ", lso.peerUUID)
	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	lso.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: colmfamily,
		RaftUuid:       lso.raftUUID,
		PeerUuid:       lso.peerUUID,
		PmdbAPI:        lso.pso.PmdbAPI,
		SyncWrites:     false,
		CoalescedWrite: true,
	}

	// Start the pmdb server
	err = lso.pso.Run()

	if err != nil {
		log.Error(err)
	}
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func parseArgs() (*leaseServer, error) {

	var err error
	lso := &leaseServer{}

	flag.StringVar(&lso.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&lso.peerUUID, "u", "NULL", "peer uuid")

	/* If log path is not provided, it will use Default log path.
	   default log path: /tmp/<peer-uuid>.log
	*/
	defaultLog := "/" + "tmp" + "/" + lso.peerUUID + ".log"
	flag.StringVar(&lso.logDir, "l", defaultLog, "log dir")
	flag.StringVar(&lso.logLevel, "ll", "Info", "Log level")
	flag.Parse()

	if lso == nil {
		err = errors.New("Not able to parse the arguments")
	} else {
		err = nil
	}

	return lso, err
}

func addMinorToHybrid(current_time float64, add_minor int) float64 {
	//TODO: Validate this func
	stringTime := fmt.Sprintf("%f", current_time)
	minorComponent, _ := strconv.Atoi(strings.Split(stringTime, ".")[1])
	newMinor := minorComponent + add_minor
	updatedTime := strings.Split(stringTime, ".")[0] + "." + string(newMinor)
	f, _ := strconv.ParseFloat(updatedTime, 64)
	return f
}

func isPermitted(entry *requestResponseLib.LeaseStruct, clientUUID uuid.UUID, currentTime float64, operation int) bool {
	if entry.Status == INPROGRESS {
		return false
	}

	//Check if existing lease is valid
	stillValid := entry.LeaseExpiryTS >= currentTime
	//If valid, Check if client uuid is same and operation is refresh
	if stillValid {
		if (operation == requestResponseLib.REFRESH) && (clientUUID == entry.Client) {
			return true
		} else {
			return false
		}
	}
	return true
}

func (lso *leaseServer) WritePrep(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, continue_wr *int, replyBuf unsafe.Pointer, replySize int64, pmdbHande unsafe.Pointer) int {

	var copyErr error

	log.Trace("Lease server : Write prep request")
	fmt.Println("JJJ - WRITEPREP")

	Request := &requestResponseLib.LeaseReq{}

	decodeErr := lso.pso.Decode(inputBuf, Request, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}
	//Get current hybrid time
	//TODO: GetCurrentHybridTime() def this in pumiceDBServer.go
	currentTime := lso.pso.GetCurrentHybridTime()

	//Check if its a refresh request
	if Request.Operation == requestResponseLib.REFRESH {
		*continue_wr = 0
		vdev_lease_info, isPresent := lso.leaseMap[Request.Resource]
		if isPresent {
			if isPermitted(vdev_lease_info, Request.Client, currentTime, Request.Operation) {
				//Refresh the lease
				lso.leaseMap[Request.Resource].LeaseGrantedTS = currentTime
				lso.leaseMap[Request.Resource].LeaseExpiryTS = addMinorToHybrid(currentTime, ttlDefault)
				//Copy the encoded result in replyBuffer
				replySize, copyErr = lso.pso.CopyDataToBuffer(*lso.leaseMap[Request.Resource], replyBuf)
				if copyErr != nil {
					log.Error("Failed to Copy result in the buffer: %s", copyErr)
					return -1
				}
				return 0
			}
		}
		return -1
	}

	//Check if get lease
	if Request.Operation == requestResponseLib.GET {
		vdev_lease_info, isPresent := lso.leaseMap[Request.Resource]
		if isPresent {
			if !isPermitted(vdev_lease_info, Request.Client, currentTime, Request.Operation) {
				//Dont provide lease
				*continue_wr = 0
				return -1
			}
		}

		//Insert or update into MAP
		lso.leaseMap[Request.Resource] = &requestResponseLib.LeaseStruct{
			Resource: Request.Resource,
			Client:   Request.Client,
			Status:   INPROGRESS,
		}
	}

	*continue_wr = 1
	return 0
}

func (lso *leaseServer) Apply(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, replyBuf unsafe.Pointer, replySize int64, pmdbHandle unsafe.Pointer) int {
	log.Trace("Lease server: Apply request received")
	var valueBytes bytes.Buffer
	var copyErr error

	// Decode the input buffer into structure format
	applyLeaseReq := &requestResponseLib.LeaseReq{}

	decodeErr := lso.pso.Decode(inputBuf, applyLeaseReq, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	log.Trace("Key passed by client: ", applyLeaseReq.Client.String())

	// length of key.
	keyLength := len(applyLeaseReq.Client.String())

	leaseObj := lso.leaseMap[applyLeaseReq.Resource]
	//TODO Use actual values set TTL to 60s
	leaseObj.LeaseGrantedTS = lso.pso.GetCurrentHybridTime()
	leaseObj.LeaseExpiryTS = addMinorToHybrid(leaseObj.LeaseGrantedTS, ttlDefault)

	enc := gob.NewEncoder(&valueBytes)
	err := enc.Encode(&leaseObj)
	if err != nil {
		log.Error(err)
	}

	byteToStr := string(valueBytes.Bytes())
	log.Trace("Value passed by client: ", byteToStr)

	// Length of value.
	valLen := len(byteToStr)

	rc := lso.pso.WriteKV(appId, pmdbHandle, applyLeaseReq.Client.String(),
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)

	//Copy the encoded result in replyBuffer
	replySize, copyErr = lso.pso.CopyDataToBuffer(leaseObj, replyBuf)
	if copyErr != nil {
		log.Error("Failed to Copy result in the buffer: %s", copyErr)
		return -1
	}
	return rc
}

func (lso *leaseServer) Read(appId unsafe.Pointer, requestBuf unsafe.Pointer,
	requestBufSize int64, replyBuf unsafe.Pointer, replyBufSize int64) int64 {

	log.Trace("NiovaCtlPlane server: Read request received")

	//Decode the request structure sent by client.
	reqStruct := &requestResponseLib.LeaseReq{}
	decodeErr := lso.pso.Decode(requestBuf, reqStruct, requestBufSize)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Trace("Key passed by client: ", reqStruct.Client)

	keyLen := len(reqStruct.Client.String())

	//Pass the work as key to PmdbReadKV and get the value from pumicedb
	readResult, readErr := lso.pso.ReadKV(appId, reqStruct.Client.String(),
		int64(keyLen), colmfamily)
	var valType []byte
	var replySize int64
	var copyErr error

	if readErr == nil {
		valType = readResult
		inputVal, _ := uuid.Parse(string(valType))

		leaseObj := requestResponseLib.LeaseStruct{}
		leaseObj.Status = 1
		dec := gob.NewDecoder(bytes.NewBuffer(readResult))
		err := dec.Decode(&leaseObj)
		if err != nil {
			log.Error(err)
		}
		fmt.Println("YYYY - ", leaseObj)
		log.Trace("Input value after read request:", inputVal)

		//Copy the encoded result in replyBuffer
		replySize, copyErr = lso.pso.CopyDataToBuffer(leaseObj, replyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		log.Error(readErr)
	}

	return replySize
}
