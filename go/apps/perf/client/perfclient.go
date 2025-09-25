package main

import (
	"bytes"
	//"encoding/csv"
	"encoding/gob"
	//"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	//log "github.com/sirupsen/logrus"
	"strconv"
	"time"
	"math/rand"
	"os"
	"sync"

	pumiceclient "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceclient"
	perflib "github.com/00pauln00/niova-pumicedb/go/apps/perf/lib"

	//PumiceDBCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
)

type metric struct {
	startTime time.Time
	submissionTime time.Time
	completionTime time.Time
	appDataSize int
	requestType	int
}

type PerfClient struct {
	raftUUID      string
	//workload	  int
	queueDepth	  int
	kvsize		  int
	testTime	  int
	writeCount    uint64
	metricCh	  chan metric
	pco			  pumiceclient.PmdbClientObj
}

func parseKVSize(s string) int {
	if s == "" {
		return 0
	}

	multiplier := 1
	last := s[len(s)-1]

	// Check for K or M suffix
	if last == 'K' || last == 'k' {
		multiplier = 1024
		s = s[:len(s)-1]
	} else if last == 'M' || last == 'm' {
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	}

	// Convert the numeric part
	value, err := strconv.Atoi(s)
	if err != nil {
		fmt.Println("invalid kvsize: %v", err)
		os.Exit(-1)
	}

	return value * multiplier
}

//Positional Arguments.
func parseArgs() PerfClient {
	var pfo PerfClient
	var kvsize string
	flag.StringVar(&pfo.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&kvsize, "s", "10", "KV size per operation(bytes)")
	flag.IntVar(&pfo.testTime, "t", 5, "Test duration in seconds")
	flag.IntVar(&pfo.queueDepth, "q", 1, "Queue depth (concurrent ops)")
	flag.Parse()
	pfo.kvsize = parseKVSize(kvsize)


	fmt.Printf("RaftUUID   : %s\n", pfo.raftUUID)
	fmt.Printf("KVSize     : %d Bytes\n", pfo.kvsize)
	fmt.Printf("TestTime   : %d sec\n", pfo.testTime)
	fmt.Printf("QueueDepth : %d\n", pfo.queueDepth)
	
	return pfo
}

func main() {
	//Parse the cmdline parameter
	pfo := parseArgs()

	//Create new client object with raftUUID and Client uuid
	clientUUID := uuid.New().String()
	fmt.Println("Raft uuid : ", pfo.raftUUID)
	fmt.Println("Client UUID : ", clientUUID)
	clientObj := pumiceclient.PmdbClientNew(pfo.raftUUID, clientUUID)
	if clientObj == nil {
		return
	}

	//Start the pumice client
	clientObj.Start()
	defer clientObj.Stop()


	//Start the experiment
	pfo.setup()
	pfo.run()
}


func (pfo *PerfClient) metricsHandler() {
	var totalData int
	var totalOp int
	var totalLatency time.Duration
	var firstSubmission, lastCompletion time.Time

	for data := range pfo.metricCh {
		
		latency := data.completionTime.Sub(data.submissionTime)
		totalOp++
		totalData += data.appDataSize
		totalLatency += latency

		if totalOp == 1 {
			firstSubmission = data.submissionTime
		}
		lastCompletion = data.completionTime
	}

	if totalOp == 0 {
		fmt.Println("No operations recorded")
		return
	}

	// Averages
	avgLatency := totalLatency / time.Duration(totalOp)

	// Avg IOPS = total operations / total elapsed time (seconds)
	totalDuration := lastCompletion.Sub(firstSubmission).Seconds()
	avgIOPS := float64(totalOp) / totalDuration

	// Avg bandwidth = total data / total elapsed time (bytes/sec)
	avgBW := float64(totalData) / totalDuration

	fmt.Printf("Avg latency: %v\n", avgLatency)
	fmt.Printf("Avg IOPS   : %.2f ops/sec\n", avgIOPS)
	fmt.Printf("Avg BW     : %.2f bytes/sec\n", avgBW)
}

func (pfo *PerfClient) setup() {
	//Initialize everything
	pfo.metricCh = make(chan metric, 1024)
	go pfo.metricsHandler()
}



func (pfo *PerfClient) runner(rncui string) {
	st := time.Now()

	//Create a request
	b := make([]byte, pfo.kvsize)
	rand.Read(b) 

	ar := perflib.AppReq {
		Key: b[:pfo.kvsize/2],
		Value: b[pfo.kvsize/2:],
	}

	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	enc.Encode(ar)

	var replySize int64
	var response []byte
	pr := &pumiceclient.PmdbReqArgs{
		Rncui:       rncui,
		ReqED:       data.Bytes(),
		GetResponse: 1,
		ReplySize:   &replySize,
		Response:    &response,
	}

	//Send and wait
	rt := time.Now()
	pfo.pco.Put(pr)
	ct := time.Now()

	//Put stats to the metric handler
	m := metric{
		startTime : st,
		submissionTime: rt,
		completionTime: ct,
		appDataSize: pfo.kvsize,
	}
	pfo.metricCh <- m
}

func (pfo *PerfClient) run() {
	qdlimiter := make(chan int, pfo.queueDepth)
	rncui := uuid.New().String() + ":0:0:0:"
	duration := time.Duration(pfo.testTime) * time.Second
	start := time.Now()

	var wg sync.WaitGroup

	for time.Since(start) < duration {
		wg.Add(1)
		qdlimiter <- 1
		go func(id string) {
			defer wg.Done()
			defer func() { <-qdlimiter }()
			pfo.runner(id)
		}(rncui + strconv.FormatUint(pfo.writeCount, 10))
		pfo.writeCount++
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Now it's safe to close the channel
	close(pfo.metricCh)
}
