package funcserver

import (
    "fmt"
    PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
    "encoding/gob"
	log "github.com/sirupsen/logrus"
    "bytes"
    funclib "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/common"
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

func (fs *FuncServer) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {
    r, err := decode(wrPrepArgs.Payload)
    if err != nil {
        log.Error("Failed to decode write prep request: ", err)
        return -1
    }
    
    cw := (*int)(wrPrepArgs.ContinueWr)
    if fn, exists := fs.WritePrepFuncs[r.Name]; exists {
        result, err := fn(r.Args...)
        if err != nil {
            log.Error("Write prep function %s failed: %v", r.Name, err)
            return -1
        }
        log.Info("Write prep function %s executed successfully with result: %v", r.Name, result)
        size, err := PumiceDBServer.PmdbCopyDataToBuffer(result, wrPrepArgs.AppData)
        if err != nil {
            log.Error("Failed to copy data to buffer: ", err)
            goto error
        }
        //Continue write if the function executed successfully
        *cw = 1
        log.Info("Data copied to buffer successfully, size: %d", size)
        return size
    }

error:
    *cw = 0
    return -1
    
    
}

func (fs *FuncServer) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {
    log.Info("Apply request received in FuncServer")
    r, err := decode(applyArgs.Payload)
    if err != nil {
        log.Error("Failed to decode apply request: ", err)
        return -1
    }
    if fn, exists := fs.ApplyFuncs[r.Name]; exists {
        result, err := fn(applyArgs.AppData, applyArgs.AppDataSize)
        if err != nil {
            log.Error("Apply function %s failed: %v", r.Name, err)
            return -1
        }
        //TODO: Fill the response using the result
        log.Info("Apply function %s executed successfully with result: %v", r.Name, result)
        return 0
    }
    return -1
}

func (fs *FuncServer) Read(readArgs *PumiceDBServer.PmdbCbArgs) int64 {
    // Implement the read logic here
    r, err := decode(readArgs.Payload)
    if err != nil {
        log.Error("Failed to decode read request: ", err)
        return -1
    }
    if fn, exists := fs.ReadFuncs[r.Name]; exists {
        result, err := fn(r.Args...)
        if err != nil {
            log.Error("Read function %s failed: %v", r.Name, err)
            return -1
        }
        //TODO: Fill the response using the result
        log.Info("Read function %s executed successfully with result: %v", r.Name, result)
        return 0
    }
    log.Error("Read function %s not found", r.Name)
    return -1
}   

func (nso *FuncServer) Init(initPeerArgs *PumiceDBServer.PmdbCbArgs) {
	return
}