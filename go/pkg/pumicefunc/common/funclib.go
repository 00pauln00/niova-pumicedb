package funclib

// Struct to forward from the pmdb client to the pmdb server
type FuncReq struct {
	Name string
	Args []byte
}

// Struct to contain KV changes
type CommitChg struct {
	Key   []byte
	Value []byte
}

// Struct for sending the KV changes and Response to client from the WritePrep to Apply
type FuncIntrm struct {
	Changes  []CommitChg
	Response []byte
}
