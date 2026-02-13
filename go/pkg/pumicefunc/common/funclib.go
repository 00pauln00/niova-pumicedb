package funclib

// Struct to forward from the pmdb client to the pmdb server
type FuncReq struct {
	Name string
	Args any
}

// OpType defines the type of operation to perform in a commit change
type OpType int

const (
	// OpApply sets or updates a key-value pair in the store
	OpApply OpType = iota

	// OpDelete removes a key from the store
	OpDelete
)

// Struct to contain KV changes
type CommitChg struct {
	Key   []byte
	Value []byte
	Op    OpType
}

// Struct for sending the KV changes and Response to client from the WritePrep to Apply
type FuncIntrm struct {
	Changes  []CommitChg
	Response []byte
}
