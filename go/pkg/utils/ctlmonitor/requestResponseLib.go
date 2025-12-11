package lookout

import (
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
)

type KVRequest struct {
	Operation string
	Key       string
	Value     []byte
	CheckSum  [16]byte
}

type KVResponse struct {
	Status int
	Value  []byte
}

type PMDBKVResponse struct {
	Status       int
	Key          string
	Result       []pumicecommon.Data // Changed from map to slice to preserve RocksDB ordering
	ContinueRead bool
	Prefix       string
	SeqNum       uint64
	SnapMiss     bool
}

type LookoutRequest struct {
	UUID [16]byte
	Cmd  string
}
