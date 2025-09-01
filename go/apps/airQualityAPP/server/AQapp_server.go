package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	AQappLib "github.com/00pauln00/niova-pumicedb/go/apps/airQualityAPP/lib"
	PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"

	log "github.com/sirupsen/logrus"
)



/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var seqno = 0
var raftUuid string
var peerUuid string
var logDir string

// Use the default column family
var colmfamily = "PMDBTS_CF"

type AirQServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}

func main() {

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "-h" {
		fmt.Println("Positional Arguments: \n		'-r' - RAFT UUID \n		'-u' - PEER UUID")
		fmt.Println("Optional Arguments: \n		'-l' - Log Dir Path \n		-h, -help")
		fmt.Println("covid_app_server -r <RAFT UUID> -u <PEER UUID> -l <log directory>")
		os.Exit(0)
	}

	aq := parseArgs()

	//Create log directory if not Exist.
	makeDirectoryIfNotExists()

	//Create log file.
	initLogger(aq)

	log.Info("Raft UUID: %s", aq.raftUuid)
	log.Info("Peer UUID: %s", aq.peerUuid)

	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	aq.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: []string{colmfamily},
		RaftUuid:       aq.raftUuid,
		PeerUuid:       aq.peerUuid,
		PmdbAPI:        aq.pso.PmdbAPI, //aq
	}

	// Start the pmdb server
	err := aq.pso.Run()

	if err != nil {
		log.Error(err)
	}
}



func parseArgs() *AirQServer {
	aq := &AirQServer{}

	flag.StringVar(&aq.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&aq.peerUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&logDir, "l", "/tmp/AirQualityAPPLog", "log dir")

	flag.Parse()

	return aq
}

/*If log directory is not exist it creates directory.
  and if dir path is not passed then it will create
  log file in "/tmp/covidAppLog" path.
*/
func makeDirectoryIfNotExists() error {

	if _, err := os.Stat(logDir); os.IsNotExist(err) {

		return os.Mkdir(logDir, os.ModeDir|0755)
	}

	return nil
}

//Create logfile for each peer.
func initLogger(aq *AirQServer) {

	var filename string = logDir + "/" + aq.peerUuid + ".log"

	fmt.Println("logfile:", filename)

	//Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Timestamp format for logfile.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		fmt.Println(err)
	} else {
		log.SetOutput(f)
	}
}

func (aq *AirQServer) Init(initArgs *PumiceDBServer.PmdbCbArgs) {
	return
}

func (aq *AirQServer) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {
	return 0
}


func (aq *AirQServer) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Info("AirQuality_Data app server: Apply request received")
	/* Decode the input buffer into structure format */
	applyAQ := &AQappLib.AirInfo{}

	decodeErr := aq.pso.DecodeApplicationReq(applyArgs.Payload, applyAQ)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	log.Info("Key passed by client: ", applyAQ.Location)

	//length of key.
	keyLength := len(applyAQ.Location)

	//Lookup the key first
	prevResult, err := aq.pso.LookupKey(applyAQ.Location,
		int64(keyLength), colmfamily)

	log.Info("Previous values of the AirQualityData: ", prevResult)

	if err == nil{
		
	}
}

