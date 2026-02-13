package main

import (
	//"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"strconv"
	"path/filepath"
	"time"
	"math/rand"
	"strings"
	"math"
	"sort"
	"sync"
	"os"

	pumiceclient "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceclient"
	perflib "github.com/00pauln00/niova-pumicedb/go/apps/perf/lib"

	//PumiceDBCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
)

const (
	KVREAD = iota
	KVWRITE
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
	workload	  int
	queueDepth	  int
	kvsize		  int
	testTime	  int
	expname		  string
	writeCount    uint64
	initLeader	  string
	initTerm	  string
	peers		  []string
	metricCh	  chan metric
	pco			  *pumiceclient.PmdbClientObj
	metricWG	  sync.WaitGroup
	configdir	  string
	ctldir		  string
	clientUUID	  string
}

func parseWorkload(w string) int {
	switch w {
	case "w":
		return KVWRITE
	case "r":
		return KVREAD
	}
	log.Fatal("Provide acception options for worload (Read - r/ Write - w)")
	return 0
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
		log.Fatalf("invalid kvsize: %v", err)
	}

	return value * multiplier
}

//Positional Arguments.
func parseArgs() PerfClient {
	var pfo PerfClient
	var kvsize string
	var workload string
	flag.StringVar(&pfo.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&kvsize, "s", "10", "KV size per operation(bytes)")
	flag.IntVar(&pfo.testTime, "t", 5, "Test duration in seconds")
	flag.IntVar(&pfo.queueDepth, "q", 1, "Queue depth (concurrent ops)")
	flag.StringVar(&workload, "w", "r", "Workload type (read/write)")
	flag.StringVar(&pfo.expname, "n", "job", "Experiment name")
	flag.StringVar(&pfo.clientUUID, "u", "NULL", "Client UUID")
	flag.Parse()
	pfo.kvsize = parseKVSize(kvsize)
	pfo.workload = parseWorkload(workload)

	fmt.Printf("Exp name : %s\n", pfo.expname)
	fmt.Printf("RaftUUID   : %s\n", pfo.raftUUID)
	fmt.Printf("KVSize     : %d Bytes\n", pfo.kvsize)
	fmt.Printf("TestTime   : %d sec\n", pfo.testTime)
	fmt.Printf("QueueDepth : %d\n", pfo.queueDepth)
	fmt.Printf("Workload : %s\n", workload)
	
	return pfo
}

func main() {
	//Parse the cmdline parameter
	pfo := parseArgs()

	fmt.Println("Raft uuid : ", pfo.raftUUID)
	fmt.Println("Client UUID : ", pfo.clientUUID)
	pfo.pco = pumiceclient.PmdbClientNew(pfo.raftUUID, pfo.clientUUID)
	if pfo.pco == nil {
		return
	}

	//Start the pumice client
	err := pfo.pco.Start()
	if err != nil {
		log.Fatal("Client err : ", err)
	}

	defer pfo.pco.Stop()


	//Start the experiment
	pfo.setup()
	pfo.run()
	pfo.metricWG.Wait()
}


func (pfo *PerfClient) metricsHandler() {
	defer pfo.metricWG.Done()
	var (
		totalData      int
		totalOp        int
		totalLatency   time.Duration
		firstSubmission, lastCompletion time.Time
		latencies      []time.Duration
	)

	for data := range pfo.metricCh {
		latency := data.completionTime.Sub(data.submissionTime)

		totalOp++
		totalData += data.appDataSize
		totalLatency += latency
		latencies = append(latencies, latency)

		if totalOp == 1 {
			firstSubmission = data.submissionTime
		}
		lastCompletion = data.completionTime
	}

	if totalOp == 0 {
		fmt.Println("No operations recorded")
		return
	}

	// Sort latencies
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// 99th percentile
	p99Idx := int(math.Ceil(0.99*float64(totalOp))) - 1
	p90Idx := int(math.Ceil(0.90*float64(totalOp))) - 1
	p50Idx := int(math.Ceil(0.50*float64(totalOp))) - 1
	if p99Idx < 0 {
		p99Idx = 0
		p90Idx = 0
		p50Idx = 0
	}
	if p99Idx >= len(latencies) {
		p99Idx = len(latencies) - 1
	}
	if p90Idx >= len(latencies) {
		p90Idx = len(latencies) - 1
	}
	p99Latency := latencies[p99Idx]
	p90Latency := latencies[p90Idx]
	p50Latency := latencies[p50Idx]
	// Averages
	//avgLatency := totalLatency / time.Duration(totalOp)

	// Avg IOPS
	totalDuration := lastCompletion.Sub(firstSubmission).Seconds()
	avgIOPS := float64(totalOp) / totalDuration

	// Avg bandwidth
	avgBW := float64(totalData) / totalDuration


	// Do ctl query of leader and term
	q := []string{"leader-uuid", "term"}
	lt := pfo.doctl(q,pfo.peers[0],"server")
	fmt.Println("Leader and term", lt)
	q = []string{"commit-latency-msec"}
	cmtlat := pfo.doctl(q, lt[0],"server")
	//clientlat := pfo.doctl(q,pfo.clientUUID,"client")

	// Struct for JSON (latencies in microseconds)
	results := struct {
		TotalOps    int     `json:"total_ops"`
		TotalData   int     `json:"total_data_bytes"`
		P50Latency  int64   `json:"p50_latency_ms"`
		P99Latency  int64   `json:"p99_latency_ms"`
		P90Latency  int64	`json:"p90_latency_ms"`
		AvgIOPS     float64 `json:"avg_iops"`
		AvgBW       float64 `json:"avg_bandwidth_bytes_per_sec"`
		FinalLeader string	`json:"final_leader"`
		FinalTerm	string	`json:"final_term"`
		InitLeader	string	`json:"init_leader"`
		InitTerm	string	`json:"init_term"`
		SCommitLat	string	`json:"commit_lat_ms"`
		CReqLat		string 	`json:"req_lat_ms"`
	}{
		TotalOps:   totalOp,
		TotalData:  totalData,
		P50Latency: p50Latency.Milliseconds(),
		P99Latency: p99Latency.Milliseconds(),
		P90Latency: p90Latency.Milliseconds(),
		AvgIOPS:    avgIOPS,
		AvgBW:      avgBW,
		FinalLeader: lt[0],
		FinalTerm: lt[1],
		InitLeader: pfo.initLeader,
		InitTerm: pfo.initTerm,
		SCommitLat: cmtlat[0],
		//CReqLat: clientlat[0],
	}

	// Encode to JSON
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Fatalf("failed to marshal metrics: %v", err)
	}

	file, err := os.Create(pfo.expname+".json")
	if err != nil {
		log.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		log.Fatalf("failed to write file: %v", err)
	}
}


// Struct for decoding JSON response
type raftResponse struct {
	RaftRootEntry []map[string]interface{} `json:"raft_root_entry"`
}

func (pfo *PerfClient) doctl(queries []string, peer string, node string) []string {
	// Ensure randomness for filenames
	qu := "raft_root_entry"
	if node == "client" {
		qu = "raft_client_root_entry"
	}
	rand.Seed(time.Now().UnixNano())
	randomName := fmt.Sprintf("out_%d", rand.Int63())
	var op []string

	// Construct input/output paths
	inputDir := filepath.Join(pfo.ctldir, peer, "input")
	outputDir := filepath.Join(pfo.ctldir, peer, "output")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		fmt.Printf("failed to create input dir: %v\n", err)
		return op
	}

	// Write input request file
	inFilePath := filepath.Join(inputDir, randomName)
	content := fmt.Sprintf("GET /%s/.*/.*\nOUTFILE /%s\n", qu, randomName)
	if err := os.WriteFile(inFilePath, []byte(content), 0644); err != nil {
		fmt.Printf("failed to write ctl file: %v\n", err)
		return op
	}

	// Path to wait for
	outFilePath := filepath.Join(outputDir, randomName)

	// Wait for output file to appear (with timeout)
	var data []byte
	timeout := time.After(5 * time.Second) // configurable
	for {
		select {
		case <-timeout:
			fmt.Printf("timeout waiting for output file %s\n", outFilePath)
			return op
		default:
			if _, err := os.Stat(outFilePath); err == nil {
				// File exists, read it
				var readErr error
				data, readErr = os.ReadFile(outFilePath)
				if readErr != nil {
					fmt.Printf("failed to read output file: %v\n", readErr)
					return op
				}
				goto PARSE
			}
			time.Sleep(100 * time.Millisecond) // polling interval
		}
	}

PARSE:
	// Parse JSON
	var resp raftResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		fmt.Printf("failed to parse JSON: %v\n", err)
		return op
	}

	for _, q := range queries {
		if len(resp.RaftRootEntry) > 0 {
			if val, ok := resp.RaftRootEntry[0][q]; ok {
				// Convert any type to string safely
				op = append(op, fmt.Sprintf("%v", val))
			}
		}
	}

	return op
}


func (pfo *PerfClient) setup() {
	//Initialize everything
	pfo.configdir = os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
	pfo.ctldir = os.Getenv("NIOVA_INOTIFY_BASE_PATH")
	if pfo.configdir == "" || pfo.ctldir == ""{
		log.Fatal("Config/CTL dir not set")
	}
	// Walk the directory and collect UUIDs from *.peer files
	err := filepath.Walk(pfo.configdir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(info.Name(), ".peer") {
			// Strip ".peer" suffix to get the UUID
			uuid := strings.TrimSuffix(info.Name(), ".peer")
			pfo.peers = append(pfo.peers, uuid)
		}
		return nil
	})

	if err != nil {
		log.Fatal("Unable to fetch peer uuids")
	}

	// Do a ctl interface request to get leader and term value
	q := []string{"leader-uuid", "term"}
	lt := pfo.doctl(q,pfo.peers[0],"server")
	fmt.Println("Leader, term", lt)
	pfo.initLeader = lt[0]
	pfo.initTerm = lt[1]

	pfo.metricCh = make(chan metric, 1024)
	pfo.metricWG.Add(1)
	go pfo.metricsHandler()
}



func (pfo *PerfClient) runner() {
	st := time.Now()

	//Create a request
	rncui := uuid.New().String() + ":0:0:0:0"
	b := make([]byte, pfo.kvsize)
	rand.Read(b) 

	ar := perflib.AppReq {
		Key: b[:pfo.kvsize/2],
		Value: b[pfo.kvsize/2:],
	}

	var replySize int64
	var response []byte
	pr := &pumiceclient.PmdbReqArgs{
		Rncui:       rncui,
		ReqED:       ar,
		GetResponse: 1,
		ReplySize:   &replySize,
		Response:    &response,
		ResponseED: &ar,
	}

	//Send and wait
	//var err error
	rt := time.Now()
	var err error
	switch pfo.workload {
	case KVWRITE:
		_, err = pfo.pco.Put(pr)
	case KVREAD:
		err = pfo.pco.Get(pr)
	}
	ct := time.Now()

	if err != nil {
		return
	}
	
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
	duration := time.Duration(pfo.testTime) * time.Second
	start := time.Now()

	var wg sync.WaitGroup

	for time.Since(start) < duration {
		wg.Add(1)
		qdlimiter <- 1
		go func() {
			defer wg.Done()
			defer func() { <-qdlimiter }()
			pfo.runner()
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Now it's safe to close the channel
	close(pfo.metricCh)
}
