package lookout // niova control interface

import (
	//	"math/rand"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// #include <unistd.h>
// //#include <errno.h>
// //int usleep(useconds_t usec);
import "C"

const (
	maxPendingCmdsEP  = 32
	maxOutFileSize    = 4 * 1024 * 1024
	outFileTimeoutSec = 2
	outFilePollMsec   = 1
	EPtimeoutSec      = 60.0
)

type Time struct {
	WrappedTime time.Time `json:"time"`
}

type SystemInfo struct {
	CurrentTime             Time      `json:"current_time"`
	StartTime               Time      `json:"start_time"`
	Pid                     int       `json:"pid"`
	UUID                    uuid.UUID `json:"uuid"`
	CtlInterfacePath        string    `json:"ctl_interface_path"`
	CommandLine             string    `json:"command_line"`
	UtsNodename             string    `json:"uts.nodename"`
	UtsSysname              string    `json:"uts.sysname"`
	UtsRelease              string    `json:"uts.release"`
	UtsVersion              string    `json:"uts.version"`
	UtsMachine              string    `json:"uts.machine"`
	RusageUserCPUTimeUsed   float64   `json:"rusage.user_cpu_time_used" type:"gauge" metric:"SYSINFO_user_cpu_time"`
	RusageSystemCPUTimeUsed float64   `json:"rusage.system_cpu_time_used" type:"gauge" metric:"SYSINFO_sys_cpu_time"`
	RusageMaxRss            int       `json:"rusage.max_rss" type:"counter" metric:"SYSINFO_max_rss"`
	RusageMinFault          int       `json:"rusage.min_fault" type:"counter" metric:"SYSINFO_min_fault"`
	RusageMajFault          int       `json:"rusage.maj_fault" type:"counter" metric:"SYSINFO_maj_fault"`
	RusageInBlock           int       `json:"rusage.in_block" type:"counter" metric:"SYSINFO_in_block_usage"`
	RusageOutBlock          int       `json:"rusage.out_block" type:"counter" metric:"SYSINFO_out_block_usage"`
	RusageVolCtsw           int       `json:"rusage.vol_ctsw" type:"gauge" metric:"SYSINFO_vol_ctsw"`
	RusageInvolCtsw         int       `json:"rusage.invol_ctsw" type:"gauge" metric:"SYSINFO_in_vol_ctsw"`
}

type NISDInfo struct {
	ReadBytes          int       `json:"dev-bytes-read" type:"counter" metric:"nisd_dev_read_bytes"`
	WriteBytes         int       `json:"dev-bytes-write" type:"counter" metric:"nisd_dev_write_bytes"`
	NetRecvBytes       int       `json:"net-bytes-recv" type:"counter" metric:"nisd_net_bytes_recv"`
	NetSendBytes       int       `json:"net-bytes-send" type:"counter" metric:"nisd_net_bytes_send"`
	DevRdLatencyUsec   Histogram `json:"dev-rd-latency-usec" type:"histogram" metric:"nisd_dev_rd_latency_usec"`
	DevWrLatencyUsec   Histogram `json:"dev-wr-latency-usec" type:"histogram" metric:"nisd_dev_wr_latency_usec"`
	DevReadSize        Histogram `json:"dev-rd-size" type:"histogram" metric:"nisd_dev_rd_size"`
	DevWriteSize       Histogram `json:"dev-wr-size" type:"histogram" metric:"nisd_dev_wr_size"`
	NetRecvSize        Histogram `json:"net-recv-size" type:"histogram" metric:"nisd_net_recv_size"`
	NetSendSize        Histogram `json:"net-send-size" type:"histogram" metric:"nisd_net_send_size"`
	NetRecvLatencyUsec Histogram `json:"net-recv-latency-usec" type:"histogram" metric:"nisd_net_recv_latency_usec"`
	NetSendLatencyUsec Histogram `json:"net-send-latency-usec" type:"histogram" metric:"nisd_net_send_latency_usec"`
}

type NISDRoot struct {
	VBlockWritten         int    `json:"vblks-written" type:"counter" metric:"nisd_vblk_write"`
	VBlockRead            int    `json:"vblks-read" type:"counter" metric:"nisd_vblk_read"`
	VBlockHoleRead        int    `json:"vblks-hole-read" type:"gauge" metric:"nisd_vblk_hole_read"`
	VBlockReplicationSent int    `json:"vblks-replication-sent" type:"gauge" metric:"nisd_vblk_replication_sent"`
	VBlockReplicationRecv int    `json:"vblks-replication-recv" type:"gauge" metric:"nisd_vblk_replication_recv"`
	MetablockWritten      int    `json:"metablock-sectors-written" type:"counter" metric:"nisd_metablock_wriitten"`
	MetablockRead         int    `json:"metablcock-sectors-read" type:"counter" metric:"nisd_metablock_read"`
	MetablockCacheHit     int    `json:"metablock-cache-hits" type:"counter" metric:"nisd_metablock_cache_hits"`
	MetablockCacheMiss    int    `json:"metablock-cache-misses" type:"counter" metric:"nisd_metablock_cache_misses"`
	NumPblks              int    `json:"num-pblks" type:"counter" metric:"nisd_num_pblk"`
	NumPblksUsed          int    `json:"num-pblks-used" type:"counter" metric:"nisd_num_pblk_used"`
	NumReservedPblks      int    `json:"num-reserved-pblks" type:"counter" metric:"nisd_num_reserved_pblks"`
	NumReservedPblksUsed  int    `json:"num-reserved-pblks-used" type:"counter" metric:"nisd_num_reserved_pblks_used"`
	Status                string `json:"status"`
	AltName               string `json:"alt-name"`
}

type Histogram struct {
	Num1       int `json:"1,omitempty"`
	Num2       int `json:"2,omitempty"`
	Num4       int `json:"4,omitempty"`
	Num8       int `json:"8,omitempty"`
	Num16      int `json:"16,omitempty"`
	Num32      int `json:"32,omitempty"`
	Num64      int `json:"64,omitempty"`
	Num128     int `json:"128,omitempty"`
	Num256     int `json:"256,omitempty"`
	Num512     int `json:"512,omitempty"`
	Num1024    int `json:"1024,omitempty"`
	Num2048    int `json:"2048,omitempty"`
	Num4096    int `json:"4096,omitempty"`
	Num8192    int `json:"8192,omitempty"`
	Num16384   int `json:"16384,omitempty"`
	Num32768   int `json:"32768,omitempty"`
	Num65536   int `json:"65536,omitempty"`
	Num131072  int `json:"131072,omitempty"`
	Num262144  int `json:"262144,omitempty"`
	Num524288  int `json:"524288,omitempty"`
	Num1048576 int `json:"1048576,omitempty"`
}

type RaftInfo struct {
	RaftUUID                 string    `json:"raft-uuid"`
	PeerUUID                 string    `json:"peer-uuid"`
	VotedForUUID             string    `json:"voted-for-uuid"`
	LeaderUUID               string    `json:"leader-uuid"`
	State                    string    `json:"state"`
	FollowerReason           string    `json:"follower-reason"`
	ClientRequests           string    `json:"client-requests"`
	Term                     int       `json:"term" type:"gauge" metric:"PMDB_term"`
	CommitIdx                int       `json:"commit-idx" type:"gauge" metric:"PMDB_commitIdx"`
	LastApplied              int       `json:"last-applied" type:"gauge" metric:"PMDB_last_applied"`
	LastAppliedCumulativeCrc int64     `json:"last-applied-cumulative-crc" type:"gauge" metric:"PMDB_last_applied_cumulative_crc"`
	NewestEntryIdx           int       `json:"newest-entry-idx" type:"gauge" metric:"PMDB_newest_entry_idx"`
	NewestEntryTerm          int       `json:"newest-entry-term" type:"gauge" metric:"PMDB_newest_entry_term"`
	NewestEntryDataSize      int       `json:"newest-entry-data-size" type:"gauge" metric:"PMDB_newest_entry_data_size"`
	NewestEntryCrc           int64     `json:"newest-entry-crc" type:"gauge" metric:"PMDB_newest_entry_crc"`
	DevReadLatencyUsec       Histogram `json:"dev-read-latency-usec" type:"histogram" metric:"dev_read_latency_usec"`
	DevWriteLatencyUsec      Histogram `json:"dev-write-latency-usec" type:"histogram" metric:"dev_write_latency_usec"`
	FollowerStats            []struct {
		PeerUUID    string `json:"peer-uuid"`
		LastAckMs   int    `json:"ms-since-last-ack"`
		LastAck     Time   `json:"last-ack"`
		NextIdx     int    `json:"next-idx"`
		PrevIdxTerm int    `json:"prev-idx-term"`
	} `json:"follower-stats,omitempty"`
	CommitLatencyMsec Histogram `json:"commit-latency-msec"`
	ReadLatencyMsec   Histogram `json:"read-latency-msec"`
}

type CtlIfOut struct {
	SysInfo         SystemInfo `json:"system_info,omitempty"`
	RaftRootEntry   []RaftInfo `json:"raft_root_entry,omitempty"`
	NISDInformation []NISDInfo `json:"niorq_mgr_root_entry,omitempty"`
	NISDRootEntry   []NISDRoot `json:"nisd_root_entry,omitempty"`
}

type NcsiEP struct {
	Uuid         uuid.UUID             `json:"-"`
	Path         string                `json:"-"`
	Name         string                `json:"name"`
	NiovaSvcType string                `json:"type"`
	Port         int                   `json:"port"`
	LastReport   time.Time             `json:"-"`
	LastClear    time.Time             `json:"-"`
	Alive        bool                  `json:"responsive"`
	EPInfo       CtlIfOut              `json:"ep_info"`
	pendingCmds  map[string]*epCommand `json:"-"`
	Mutex        sync.Mutex            `json:"-"`
}

type EPcmdType uint32

const (
	RaftInfoOp   EPcmdType = 1
	SystemInfoOp EPcmdType = 2
	NISDInfoOp   EPcmdType = 3
	Custom       EPcmdType = 4
)

type epCommand struct {
	ep      *NcsiEP
	cmd     string
	fn      string
	outJSON []byte
	err     error
	op      EPcmdType
}

// XXX this can be replaced with: func Trim(s string, cutset string) string
func chompQuotes(data []byte) []byte {
	s := string(data)

	// Check for quotes
	if len(s) > 0 {
		if s[0] == '"' {
			s = s[1:]
		}
		if s[len(s)-1] == '"' {
			s = s[:len(s)-1]
		}
	}

	return []byte(s)
}

// custom UnmarshalJSON method used for handling various timestamp formats.
func (t *Time) UnmarshalJSON(data []byte) error {
	var err error

	data = chompQuotes(data)

	if err = json.Unmarshal(data, t.WrappedTime); err == nil {
		return nil
	}
	const layout = "Mon Jan 02 15:04:05 MST 2006"

	t.WrappedTime, err = time.Parse(layout, string(data))

	return err
}

func (cmd *epCommand) getOutFnam() string {
	return cmd.ep.epRoot() + "/output/" + cmd.fn
}

func (cmd *epCommand) getInFnam() string {
	return cmd.ep.epRoot() + "/input/" + cmd.fn
}

func (cmd *epCommand) getCmdBuf() []byte {
	return []byte(cmd.cmd)
}

func (cmd *epCommand) getOutJSON() []byte {
	return []byte(cmd.outJSON)
}

func msleep() {
	C.usleep(1000)
}

func (cmd *epCommand) checkOutFile() error {
	var tmp_stb syscall.Stat_t
	if err := syscall.Stat(cmd.getOutFnam(), &tmp_stb); err != nil {
		return err
	}

	if tmp_stb.Size > maxOutFileSize {
		return syscall.E2BIG
	}

	return nil
}

func (cmd *epCommand) loadOutfile() {
	if cmd.err = cmd.checkOutFile(); cmd.err != nil {
		return
	}

	// Try to read the file
	cmd.outJSON, cmd.err = ioutil.ReadFile(cmd.getOutFnam())

	return
}

// Makes a 'unique' filename for the command and adds it to the map
func (cmd *epCommand) prep() {
	if cmd.fn == "" {
		cmd.fn = "lookout_ncsiep_" + strconv.FormatInt(int64(os.Getpid()), 10) +
			"_" + strconv.FormatInt(int64(time.Now().Nanosecond()), 10)
	}
	cmd.cmd = cmd.cmd + "\nOUTFILE /" + cmd.fn + "\n"

	// Add the cmd into the endpoint's pending cmd map
	cmd.ep.addCmd(cmd)
}

func (cmd *epCommand) write() {
	cmd.err = ioutil.WriteFile(cmd.getInFnam(), cmd.getCmdBuf(), 0644)
	if cmd.err != nil {
		log.Printf("ioutil.WriteFile(): %s", cmd.err)
		return
	}
}

func (cmd *epCommand) submit() {
	if err := cmd.ep.mayQueueCmd(); err == false {
		return
	}
	cmd.prep()
	cmd.write()
}

func (ep *NcsiEP) mayQueueCmd() bool {
	if len(ep.pendingCmds) < maxPendingCmdsEP {
		return true
	}
	return false
}

func (ep *NcsiEP) addCmd(cmd *epCommand) error {
	// Add the cmd into the endpoint's pending cmd map
	cmd.ep.Mutex.Lock()
	_, exists := cmd.ep.pendingCmds[cmd.fn]
	if exists == false {
		cmd.ep.pendingCmds[cmd.fn] = cmd
	}
	cmd.ep.Mutex.Unlock()

	if exists == true {
		return syscall.EEXIST
	}

	return nil
}

func (ep *NcsiEP) removeCmd(cmdName string) *epCommand {
	ep.Mutex.Lock()
	cmd, ok := ep.pendingCmds[cmdName]
	if ok {
		delete(ep.pendingCmds, cmdName)
	}
	ep.Mutex.Unlock()

	return cmd
}

func (ep *NcsiEP) epRoot() string {
	return ep.Path
}

func (ep *NcsiEP) getRaftinfo() error {
	cmd := epCommand{ep: ep, cmd: "GET /raft_root_entry/.*/.*",
		op: RaftInfoOp}
	cmd.submit()

	return cmd.err
}

func (ep *NcsiEP) getSysinfo() error {
	cmd := epCommand{ep: ep, cmd: "GET /system_info/.*", op: SystemInfoOp}
	cmd.submit()
	return cmd.err
}

func (ep *NcsiEP) getNISDinfo() error {
	cmd := epCommand{ep: ep, cmd: "GET /.*/.*/.*", op: NISDInfoOp}
	cmd.submit()
	return cmd.err
}

func (ep *NcsiEP) CtlCustomQuery(customCMD string, ID string) error {
	cmd := epCommand{ep: ep, cmd: customCMD, op: Custom, fn: ID}
	cmd.submit()
	return cmd.err
}

func (ep *NcsiEP) update(ctlData *CtlIfOut, op EPcmdType) {
	switch op {
	case RaftInfoOp:
		ep.EPInfo.RaftRootEntry = ctlData.RaftRootEntry
		//		log.Printf("update-raft %+v \n", ctlData.RaftRootEntry)
	case SystemInfoOp:
		ep.EPInfo.SysInfo = ctlData.SysInfo
		//ep.LastReport = ep.EPInfo.SysInfo.CurrentTime.WrappedTime
		//		log.Printf("update-sys %+v \n", ctlData.SysInfo)
	case NISDInfoOp:
		//update
		ep.EPInfo.NISDInformation = ctlData.NISDInformation
		ep.EPInfo.NISDRootEntry = ctlData.NISDRootEntry
		ep.EPInfo.SysInfo = ctlData.SysInfo

	default:
		log.Printf("invalid op=%d \n", op)
	}
	ep.LastReport = time.Now()
}

func (ep *NcsiEP) Complete(cmdName string, output *[]byte) error {
	cmd := ep.removeCmd(cmdName)
	if cmd == nil {
		return syscall.ENOENT
	}

	cmd.loadOutfile()
	if cmd.err != nil {
		return cmd.err
	}

	//Add here to break for custom command
	if cmd.op == Custom {
		*output = cmd.getOutJSON()
		return nil
	}

	var err error
	var ctlifout CtlIfOut
	if err = json.Unmarshal(cmd.getOutJSON(), &ctlifout); err != nil {
		if ute, ok := err.(*json.UnmarshalTypeError); ok {
			log.Printf("UnmarshalTypeError %v - %v - %v\n",
				ute.Value, ute.Type, ute.Offset)
		} else {
			log.Printf("Other error: %s\n", err)
			log.Printf("Contents: %s\n", string(cmd.getOutJSON()))
		}
		return err
	}
	ep.update(&ctlifout, cmd.op)

	return nil
}

func (ep *NcsiEP) removeFiles(folder string) {
	files, err := ioutil.ReadDir(folder)
	if err != nil {
		return
	}

	for _, file := range files {
		if strings.Contains(file.Name(), "lookout") {
			checkTime := file.ModTime().Local().Add(time.Hour)
			if time.Now().After(checkTime) {
				os.Remove(folder + file.Name())
			}
		}
	}
}

func (ep *NcsiEP) Remove() {
	//Remove stale ctl files
	input_path := ep.Path + "/input/"
	ep.removeFiles(input_path)
	//output files
	output_path := ep.Path + "/output/"
	ep.removeFiles(output_path)
}

func (ep *NcsiEP) Detect(appType string) error {
	if ep.Alive {
		var err error
		switch appType {
		case "NISD":
			ep.getNISDinfo()
		case "PMDB":
			err = ep.getSysinfo()
			if err == nil {
				err = ep.getRaftinfo()
			}

		}

		if time.Since(ep.LastReport) > time.Second*EPtimeoutSec {
			ep.Alive = false
		}
		return err
	}
	return nil
}

func (ep *NcsiEP) Check() error {
	return nil
}
