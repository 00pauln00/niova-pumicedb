package funcserver

import (
    "fmt"
    PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
    "encoding/gob"
	log "github.com/sirupsen/logrus"
    "bytes"
    funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
    "unsafe"
    "C"
)

// FuncServer is a struct that represents a function server.
type FuncServer struct {
    WritePrepFuncs map[string]func(args ...interface{}) (interface{}, error)
    ApplyFuncs map[string]func(args ...interface{}) (interface{}, error)
    ReadFuncs map[string]func(args ...interface{}) (interface{}, error)
}


func NewFuncServer() *FuncServer {
    return &FuncServer{
        WritePrepFuncs: make(map[string]func(args ...interface{}) (interface{}, error)),
        ApplyFuncs: make(map[string]func(args ...interface{}) (interface{}, error)),
        ReadFuncs: make(map[string]func(args ...interface{}) (interface{}, error)),
    }
}


// RegisterWriteFunc registers a write function with the server.
func (fs *FuncServer) RegisterWritePrepFunc(name string, fn func(args ...interface{}) (interface{}, error)) {
    if _, exists := fs.WritePrepFuncs[name]; exists {
        panic(fmt.Sprintf("Write function %s already registered", name))
    }
    fs.WritePrepFuncs[name] = fn
}


// RegisterApplyFunc registers an apply function with the server.
func (fs *FuncServer) RegisterApplyFunc(name string, fn func(args ...interface{}) (interface{}, error)) {
    if _, exists := fs.ApplyFuncs[name]; exists {
        panic(fmt.Sprintf("Apply function %s already registered", name))
    }
    fs.ApplyFuncs[name] = fn
}

// RegisterReadFunc registers a read function with the server.
func (fs *FuncServer) RegisterReadFunc(name string, fn func(args ...interface{}) (interface{}, error)) {
    if _, exists := fs.ReadFuncs[name]; exists {
        panic(fmt.Sprintf("Read function %s already registered", name))
    }
    fs.ReadFuncs[name] = fn
}

func decode(payload []byte) (funclib.FuncReq, error) {
	r := &funclib.FuncReq{}
	dec := gob.NewDecoder(bytes.NewBuffer(payload))
	err := dec.Decode(r)
	return *r, err
}

func copyResultToBuffer(r interface{}, buf unsafe.Pointer, bufsz int) int64 {
    if _, ok := r.([]byte); !ok {
        log.Error("Func result is in unsupported format, expected []byte")
        return -1
    }

    if len(r.([]byte)) > bufsz {
        log.Error("Func result size is more than pumicedb buffer size (4MB)")
        return -1
    }

    size, err := PumiceDBServer.PmdbCopyBytesToBuffer(r.([]byte), buf)
    if err != nil {
        log.Error("Failed to copy data to buffer: ", err)
        return -1
    }

    return size
}

func (fs *FuncServer) WritePrep(wpa *PumiceDBServer.PmdbCbArgs) int64 {
    r, err := decode(wpa.Payload)
    if err != nil {
        log.Error("Failed to decode write prep request: ", err)
        return -1
    }
    
    cw := (*int)(wpa.ContinueWr)
    if fn, exists := fs.WritePrepFuncs[r.Name]; exists {
        res, err := fn(r.Args)
        if err != nil {
            log.Errorf("Write prep function %s failed: %v", r.Name, err)
            return -1
        }

        size := copyResultToBuffer(res, wpa.AppData, int(wpa.AppDataSize))
        //Continue write if the function executed successfully
        if size < 0 {
            goto error       
        } 

        return size
    }

error:
    *cw = 0
    return -1
}

func (fs *FuncServer) Apply(apar *PumiceDBServer.PmdbCbArgs) int64 {
    log.Info("Apply request received in FuncServer")

    //Get the function to be executed
    r, err := decode(apar.Payload)
    if err != nil {
        log.Error("Failed to decode apply request: ", err)
        return -1
    }

    var res interface{}
    if fn, exists := fs.ApplyFuncs[r.Name]; exists {
        res, err = fn(apar)
        if err != nil {
            log.Error("Apply function %s failed: %v", r.Name, err)
            return -1
        }
        goto out

    } else {
        //Use wildcard function if exist
        fn := fs.ApplyFuncs["*"]
        if fn == nil {
            log.Error("Wildcard apply function not found")
            return -1
        }
        res, err = fn(apar)
        if err != nil {
            log.Error("Default apply function failed: %v", err)
            return -1
        }
    }

out:
    size := copyResultToBuffer(res, apar.ReplyBuf, int(apar.ReplySize))
    return size
}

func (fs *FuncServer) Read(rda *PumiceDBServer.PmdbCbArgs) int64 {
    // Implement the read logic here
    r, err := decode(rda.Payload)
    if err != nil {
        log.Error("Failed to decode read request: ", err)
        return -1
    }
    if fn, exists := fs.ReadFuncs[r.Name]; exists {
        res, err := fn(rda, r.Args)
        if err != nil {
            log.Errorf("Read function %s failed: %v", r.Name, err)
            return -1
        }

        size := copyResultToBuffer(res, rda.ReplyBuf, int(rda.ReplySize))
        return size  
    }

    log.Errorf("Read function %s not found", r.Name)
    return -1
}   

func (nso *FuncServer) Init(ipa *PumiceDBServer.PmdbCbArgs) {
	return
}
