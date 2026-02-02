package storageiface

type RangeReadArgs struct {
	Key        string
	Prefix     string
	SeqNum     uint64
	Consistent bool
	BufSize    int64
	Selector   string
}

type RangeReadResult struct {
	ResultMap map[string][]byte
	LastKey   string
	SeqNum    uint64
	SnapMiss  bool
}

// DataStore is the interface for a key-value store.
type DataStore interface {
	Read(key, selector string) ([]byte, error)
	RangeRead(args RangeReadArgs) (*RangeReadResult, error)
	Write(key, value, selector string) error
	Delete(key, selector string) error
}
