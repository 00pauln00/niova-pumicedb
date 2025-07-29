package funcserver

import (
    "fmt"
    PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
    "encoding/gob"
	log "github.com/sirupsen/logrus"
    "bytes"
    funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"

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

func (fs *FuncServer) WritePrep(wpa *PumiceDBServer.PmdbCbArgs) int64 {
    r, err := decode(wpa.Payload)
    if err != nil {
        log.Error("Failed to decode write prep request: ", err)
        return -1
    }
    
    cw := (*int)(wpa.ContinueWr)
    if fn, exists := fs.WritePrepFuncs[r.Name]; exists {
        result, err := fn(r.Args)
        if err != nil {
            log.Errorf("Write prep function %s failed: %v", r.Name, err)
            return -1
        }
        log.Infof("Write prep function %s executed successfully with result: %v", r.Name, result)
        size, err := PumiceDBServer.PmdbCopyBytesToBuffer(result.([]byte), wpa.AppData)
        if err != nil {
            log.Error("Failed to copy data to buffer: ", err)
            goto error
        }
        //Continue write if the function executed successfully
        *cw = 1
        log.Info("Data copied to buffer successfully, size: ", size)
        
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


    if fn, exists := fs.ApplyFuncs[r.Name]; exists {
        _, err = fn(apar)
        if err != nil {
            log.Error("Apply function %s failed: %v", r.Name, err)
            return -1
        }
        log.Info("Apply function %s executed successfully with result: %v", r.Name)
        return 0
    }
    
    fn := fs.ApplyFuncs["*"]
    if fn == nil {
        log.Error("Apply function not found")
        return -1
    }

    ret, err := fn(apar)
    if err != nil {
        log.Error("Default apply function failed: %v", err)
        return -1
    }

    return ret.(int64)
}

func (fs *FuncServer) Read(rda *PumiceDBServer.PmdbCbArgs) int64 {
    // Implement the read logic here
    r, err := decode(rda.Payload)
    if err != nil {
        log.Error("Failed to decode read request: ", err)
        return -1
    }
    if fn, exists := fs.ReadFuncs[r.Name]; exists {
        result, err := fn(rda, r.Args)
        if err != nil {
            log.Errorf("Read function %s failed: %v", r.Name, err)
            return -1
        }

        if result != nil {
            return result.(int64)
        } else {
            return int64(0)
        }
    }
    log.Errorf("Read function %s not found", r.Name)
    return -1
}   

func (nso *FuncServer) Init(ipa *PumiceDBServer.PmdbCbArgs) {
	return
}
