package leaseclient

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/00pauln00/niova-pumicedb/go/pkg/pumicelease/common"

	pmdbClient "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceclient"
	PumiceDBCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	serviceDiscovery "github.com/00pauln00/niova-pumicedb/go/pkg/utils/servicediscovery"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

type LeaseClient struct {
	RaftUUID            uuid.UUID
	PmdbClientObj       *pmdbClient.PmdbClientObj
	ServiceDiscoveryObj *serviceDiscovery.ServiceDiscoveryHandler
}

type LeaseClientReqHandler struct {
	LeaseClientObj *LeaseClient
	LeaseReq       leaseLib.LeaseReq
	LeaseRes       leaseLib.LeaseRes
	ReqBuff        bytes.Buffer
	Rncui		   string
	WSN            int64
	Err            error
}

func (lh *LeaseClientReqHandler) prepareReq() error {
	enc := gob.NewEncoder(&lh.ReqBuff)
	return enc.Encode(lh.LeaseReq)
}

/*
Structure : LeaseHandler
Method	  : write()
Arguments : LeaseReq, rncui, *LeaseResp
Return(s) : error
Description : Wrapper function for WriteEncoded() function
*/
func (co LeaseClient) write(obj *[]byte, rncui string, seq int64,
	response *[]byte) error {

	reqArgs := &pmdbClient.PmdbReq{
		Rncui:       rncui,
		Request:  	 *obj,
		GetReply: 	 1,
		Reply:    	 response,
		ReqType:	 PumiceDBCommon.LEASE_REQ,
		WriteSeqNum: seq,
	}

	return co.PmdbClientObj.Put(reqArgs)
}

/*
Structure : LeaseHandler
Method	  : read()
Arguments : LeaseReq, rncui, *response
Return(s) : error
Description : Wrapper function for GetEncoded() function
*/
func (co LeaseClient) read(obj *[]byte, rncui string,
	response *[]byte) error {

	reqArgs := &pmdbClient.PmdbReq{
		Rncui:      rncui,
		Request: 	*obj,
		Reply:      response,
		ReqType:	PumiceDBCommon.LEASE_REQ,
	}

	return co.PmdbClientObj.Get(reqArgs)
}

/*
Structure : LeaseHandler
Method	  : InitLeaseReq()
Arguments :
Return(s) : error
Description : Initialize the handler's leaseReq struct
*/
func (lh *LeaseClientReqHandler) InitLeaseReq(client, resource string,
	operation int) error {

	rUUID, err := uuid.FromString(resource)
	if err != nil {
		log.Error(err)
		return err
	}
	cUUID, err := uuid.FromString(client)
	if err != nil {
		log.Error(err)
		return err
	}

	if operation == leaseLib.GET_VALIDATE {
		lh.LeaseReq.Operation = leaseLib.GET
	} else {
		lh.LeaseReq.Operation = operation
	}
	lh.LeaseReq.Resource = rUUID
	lh.LeaseReq.Client = cUUID

	return err
}

/*
Structure : LeaseHandler
Method	  : LeaseOperation()
Arguments : leaseLib.LeaseReq
Return(s) : error
Description : Handler function for all lease operations
*/
func (lh *LeaseClientReqHandler) LeaseOperation() error {
	var err error
	var b []byte

	// Prepare reqBytes for pumiceReq type
	err = lh.prepareReq()
	if err != nil {
		return err
	}

	// send req
	rqb := lh.ReqBuff.Bytes()
	switch lh.LeaseReq.Operation {
	case leaseLib.GET, leaseLib.GET_VALIDATE:
		fallthrough
	case leaseLib.REFRESH:
		err = lh.LeaseClientObj.write(&rqb, lh.Rncui, lh.WSN, &b)
	case leaseLib.LOOKUP, leaseLib.LOOKUP_VALIDATE:
		err = lh.LeaseClientObj.read(&rqb, "", &b)
	}
	if err != nil {
		return err
	}

	// decode req response
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	err = dec.Decode(&lh.LeaseRes)
	if err != nil {
		return err
	}

	log.Info("Lease request status - ", lh.LeaseRes.Status)

	return err
}

/*
Structure : LeaseHandler
Method	  : LeaseOperationOverHttp()
Arguments :
Return(s) : error
Description :
*/
func (lh *LeaseClientReqHandler) LeaseOperationOverHTTP() error {
	var err error
	var isWrite bool = false
	var b []byte

	// Prepare reqBytes for pumiceReq type
	err = lh.prepareReq()
	if err != nil {
		return err
	}

	var uri string
	if lh.LeaseReq.Operation != leaseLib.LOOKUP {
		isWrite = true
		uri = fmt.Sprintf("?rncui=%s&wsn=%d", lh.Rncui, lh.WSN)
	}
	// send req
	b, err = lh.LeaseClientObj.ServiceDiscoveryObj.Request(lh.ReqBuff.Bytes(), fmt.Sprintf("/lease%s", uri), isWrite)
	if err != nil {
		return err
	}

	// decode the response if response is not blank
	if len(b) == 0 {
		lh.LeaseRes.Status = leaseLib.FAILURE
		lh.Err = errors.New("Key not found")
	} else {
		lh.LeaseRes.Status = leaseLib.SUCCESS
		dec := gob.NewDecoder(bytes.NewBuffer(b))
		err = dec.Decode(&lh.LeaseRes)
	}
	log.Info("Lease request status - ", lh.LeaseRes.Status)

	return err
}
