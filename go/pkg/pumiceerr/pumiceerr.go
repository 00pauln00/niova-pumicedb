package pumiceerr

import (
	"errors"
	"fmt"
	"math"
)

// PumiceError represents a custom error type for PumiceDB
var (
	ErrRNCUISequenceUsed        = errors.New("Write sequence number already used for the RNCUI")
	ErrRNCUISequenceInProgress  = errors.New("Write sequence in progress of apply for the RNCUI")
	ErrRNCUISequenceTooFarAhead = errors.New("Write sequence way ahead")
	ErrRNCUIClientMismatch      = errors.New("RNCUI used by wrong pumice client")
	ErrRNCUIAlreadyQueued       = errors.New("RNCUI already queued in the pumice client")

	ErrInvalidInitialization = errors.New("Invalid initialization")
	ErrOutOfMemory           = errors.New("No memory in pumice client/server")
	ErrMessageTooLarge       = errors.New("The application message is too big, expects 41,93,704 bytes max (4MB - 88 bytes - 512 bytes)")
	ErrNoLeader              = errors.New("No pumice leader identified yet")
	ErrClientOutOfSpace      = errors.New("Pumice client heap is full")
	ErrRequestAlreadyQueued  = errors.New("Request already queued for the same RNCUI in the pumice client")
	ErrTimeout               = errors.New("Operation timed out at the pumice client")
	ErrTryAgain              = errors.New("Temporary failure in the raft side, try again")
	ErrNoSpaceOnServer       = errors.New("No space left on write supplement buffer on the pumice server")
)

// TranslatePumiceReqErrCode translates PumiceDB C error codes to Go errors
func TranslatePumiceReqErrCode(code int) error {
	switch math.Abs(float64(code)) {
	case 17:
		//-EALREADY (If in case Req RNCUI is less than to the last RNCUI)
		return ErrRNCUISequenceUsed
	case 115:
		//-EINPROGRESS (If same RNCUI is in progress of commit)
		return ErrRNCUISequenceInProgress
	case 52:
		//-EBADE (If the req RNCUI way ahead)
		return ErrRNCUISequenceTooFarAhead
	case 1:
		//-EPERM (If two different client uses same RNCUI)
		return ErrRNCUIClientMismatch
	case 22:
		return ErrInvalidInitialization
	case 12:
		//ENOMEM
		return ErrOutOfMemory
	case 27:
		//EFBIG
		return ErrMessageTooLarge
	case 107:
		//ENOTCONN
		return ErrNoLeader
	case 28:
		//ENOSPC
		return ErrClientOutOfSpace
	case 114:
		//EBUSY
		return ErrRNCUIAlreadyQueued
	case 110:
		//ETIMEDOUT
		return ErrTimeout
	case 11:
		//EAGAIN
		return ErrTryAgain
	default:
		return fmt.Errorf("unknown error code for : %d", code)
	}

	return nil
}

func TranslatePumiceServerOpErrCode(code int) error {
	switch math.Abs(float64(code)) {
	case 22:
		return ErrInvalidInitialization
	case 12:
		//ENOMEM
		return ErrOutOfMemory
	case 28:
		//ENOSPC
		return ErrNoSpaceOnServer
	default:
		return fmt.Errorf("unknown error code: %d", code)
	}
	return nil
}
