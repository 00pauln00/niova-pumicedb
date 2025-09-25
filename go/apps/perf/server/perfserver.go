package main

import (
	"flag"
	//"fmt"
	//"os"

	//perflib "github.com/00pauln00/niova-pumicedb/go/apps/perf/lib"
	pumiceserver "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"

	log "github.com/sirupsen/logrus"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var raftUuid string
var peerUuid string
var logDir string
// Use the default column family
var colmfamily = "PMDBTS_CF"

type PerfServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *pumiceserver.PmdbServerObject
}

func parseArgs() *PerfServer {
	pfo := &PerfServer{}

	flag.StringVar(&pfo.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&pfo.peerUuid, "u", "NULL", "peer uuid")
	flag.Parse()

	return pfo
}

func main() {

	pfo := parseArgs()

	log.Info("Raft UUID: %s", pfo.raftUuid)
	log.Info("Peer UUID: %s", pfo.peerUuid)

	pfo.pso = &pumiceserver.PmdbServerObject{
		ColumnFamilies: []string{colmfamily},
		RaftUuid:       pfo.raftUuid,
		PeerUuid:       pfo.peerUuid,
		PmdbAPI:        pfo,
		CoalescedWrite: false,
	}

	// Start the pmdb server
	err := pfo.pso.Run()

	if err != nil {
		log.Error(err)
	}
}

func (pfo *PerfServer) Init(initArgs *pumiceserver.PmdbCbArgs) {
	return
}

func (pfo *PerfServer) WritePrep(wrPrepArgs *pumiceserver.PmdbCbArgs) int64 {
	return 0
}

func (pfo *PerfServer) Apply(applyArgs *pumiceserver.PmdbCbArgs) int64 {
	return 0
}

func (pfo *PerfServer) Read(readArgs *pumiceserver.PmdbCbArgs) int64 {
	return 0
}

func (pfo *PerfServer) FillReply(applyArgs *pumiceserver.PmdbCbArgs) int64 {
	return 0
}
