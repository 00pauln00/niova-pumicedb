package httpserver

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type HTTPServerHandler struct {
	//Exported
	Addr                net.IP
	Port                uint16
	GETHandler          func([]byte, *[]byte) error
	PUTHandler          func([]byte, *[]byte) error
	ReadHandler         func(string, []byte, *[]byte, *http.Request) error
	WriteHandler        func(string, string, []byte, *[]byte, *http.Request) error
	HTTPConnectionLimit int
	PMDBServerConfig    map[string][]byte
	PortRange           []uint16
	RecvdPort           *int
	AppType             string
	//Non-exported
	HTTPServer        http.Server
	rncui             string
	connectionLimiter chan int
	//For stats
	StatsRequired bool
	Stat          HTTPServerStat
	statLock      sync.Mutex
}

type HTTPServerStat struct {
	GetCount        int64
	PutCount        int64
	GetSuccessCount int64
	PutSuccessCount int64
	Queued          int64
	ReceivedCount   int64
	FinishedCount   int64
	syncRequest     int64
	StatusMap       map[int64]*RequestStatus
}

type RequestStatus struct {
	RequestHash string
	Status      string
}

func (handler *HTTPServerHandler) configHandler(writer http.ResponseWriter, reader *http.Request) {

	uuid, err := ioutil.ReadAll(reader.Body)
	if err != nil {
		fmt.Fprintf(writer, "Unable to parse UUID")
	}
	configData, present := handler.PMDBServerConfig[string(uuid)]
	if present {
		fmt.Fprintf(writer, "%s", configData)
	} else {
		fmt.Fprintf(writer, "UUID not present")
	}
}

func (handler *HTTPServerHandler) statHandler(writer http.ResponseWriter, reader *http.Request) {
	log.Trace(handler.Stat)
	handler.statLock.Lock()
	stat, err := json.MarshalIndent(handler.Stat, "", " ")
	handler.statLock.Unlock()
	if err != nil {
		log.Error("(HTTP Server) Writing to http response writer failed :", err)
	}
	_, err = fmt.Fprintf(writer, "%s", string(stat))
	if err != nil {
		log.Error("(HTTP Server) Writing to http response writer failed :", err)
	}
	return
}

func (handler *HTTPServerHandler) updateStat(id int64, success bool, read bool) {
	handler.statLock.Lock()
	defer handler.statLock.Unlock()
	delete(handler.Stat.StatusMap, id)
	handler.Stat.FinishedCount += int64(1)
	if read {
		handler.Stat.GetCount += int64(1)
		if success {
			handler.Stat.GetSuccessCount += int64(1)
		}
	} else {
		handler.Stat.PutCount += int64(1)
		if success {
			handler.Stat.PutSuccessCount += int64(1)
		}
	}
}

func (handler *HTTPServerHandler) createStat(requestStatHandler *RequestStatus) int64 {
	handler.statLock.Lock()
	defer handler.statLock.Unlock()

	handler.Stat.ReceivedCount += 1
	id := handler.Stat.ReceivedCount
	handler.Stat.Queued += 1
	requestStatHandler = &RequestStatus{
		Status: "Queued",
	}
	handler.Stat.StatusMap[id] = requestStatHandler
	return id
}

func (handler *HTTPServerHandler) kvRequestHandler(writer http.ResponseWriter, reader *http.Request) {
	var thisRequestStat RequestStatus
	var id int64

	//Create stat for the request
	if handler.StatsRequired {
		id = handler.createStat(&thisRequestStat)
	}

	//HTTP connections limiter
	handler.connectionLimiter <- 1
	defer func() {
		<-handler.connectionLimiter
	}()

	var success bool
	var read bool
	var result []byte
	var err error

	//Handle the KV request
	requestBytes, err := ioutil.ReadAll(reader.Body)
	switch reader.Method {
	case "GET":
		if handler.StatsRequired {
			thisRequestStat.Status = "Processing"
		}
		err = handler.GETHandler(requestBytes, &result)
		read = true
		fallthrough

	case "PUT":
		if !read {
			if handler.StatsRequired {
				thisRequestStat.Status = "Processing"
			}
			err = handler.PUTHandler(requestBytes, &result)
		}
		if err == nil {
			success = true
		}

		//Write the output to HTTP response buffer
		writer.Write(result)

	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}

	//Update status
	if handler.StatsRequired {
		handler.updateStat(id, success, read)
	}
}

func (handler *HTTPServerHandler) HTTPFuncHandler(writer http.ResponseWriter, reader *http.Request) {
	body, err := io.ReadAll(reader.Body)
	if err != nil {
		log.Error("Error reading request body: ", err)
		http.Error(writer, "Bad Request", http.StatusBadRequest)
		return
	}
	defer reader.Body.Close()

	name := reader.URL.Query().Get("name")
	rncui := reader.URL.Query().Get("rncui")
	var response []byte

	switch reader.Method {
	case "GET":
		err = handler.ReadHandler(name, body, &response, reader)
	case "PUT":
		err = handler.WriteHandler(name, rncui, body, &response, reader)
	}
	if err != nil {
		log.Error("Error in FuncHandler: ", err)
		http.Error(writer, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	if response != nil {
		// Write the response back to the client
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, err = writer.Write(response)
		if err != nil {
			log.Error("Error writing response: ", err)
			http.Error(writer, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	} else {
		http.Error(writer, "No response from function", http.StatusNotFound)
	}
}

// HTTP server handler called when request is received
func (handler *HTTPServerHandler) ServeHTTP(writer http.ResponseWriter, reader *http.Request) {
	if reader.URL.Path == "/config" {
		handler.configHandler(writer, reader)
	} else if (reader.URL.Path == "/stat") && (handler.StatsRequired) {
		handler.statHandler(writer, reader)
	} else if reader.URL.Path == "/check" {
		writer.Write([]byte("HTTP server in operation"))
	} else if reader.URL.Path == "/func" {
		handler.HTTPFuncHandler(writer, reader)
	} else {
		handler.kvRequestHandler(writer, reader)
	}
}

func (handler *HTTPServerHandler) TryConnect(addr string) (net.Listener, bool) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, false
	} else {
		return listener, true
	}
}

func (handler *HTTPServerHandler) Start_HTTPListener() (net.Listener, error) {
	if handler.AppType == "PMDB" {
		for i := handler.PortRange[0]; i < handler.PortRange[len(handler.PortRange)-1]; i++ {
			handler.HTTPServer.Addr = handler.Addr.String() + ":" + strconv.Itoa(int(i))
			listener, ok := handler.TryConnect(handler.HTTPServer.Addr)
			if ok {
				*handler.RecvdPort = int(i)
				return listener, nil
			} else {
				continue
			}
		}
	} else {
		for i := handler.PortRange[len(handler.PortRange)-1]; i > handler.PortRange[0]; i-- {
			handler.HTTPServer.Addr = handler.Addr.String() + ":" + strconv.Itoa(int(i))
			listener, ok := handler.TryConnect(handler.HTTPServer.Addr)
			if ok {
				*handler.RecvdPort = int(i)
				return listener, nil
			} else {
				continue
			}
		}
	}
	return nil, nil
}

// Start server
func (handler *HTTPServerHandler) Start_HTTPServer() error {
	handler.connectionLimiter = make(chan int, handler.HTTPConnectionLimit)
	handler.HTTPServer = http.Server{}
	handler.HTTPServer.Addr = handler.Addr.String() + ":" + strconv.Itoa(int(handler.Port))

	//Update the timeout using little's fourmula
	handler.HTTPServer.Handler = http.TimeoutHandler(handler, 150*time.Second, "Server Timeout")
	handler.Stat.StatusMap = make(map[int64]*RequestStatus)

	//Start listener
	listener, err := handler.Start_HTTPListener()
	if err != nil {
		return err
	}
	//Start server
	err = handler.HTTPServer.Serve(listener)
	//err := handler.HTTPServer.ListenAndServe()
	return err
}

// Close server
func (h HTTPServerHandler) Stop_HTTPServer() error {
	err := h.HTTPServer.Close()
	return err
}
