package pumiceerr

import (
	"errors"
	"fmt"
	"math"
)

// PumiceError represents a custom error type for PumiceDB
var (
	ErrRNCUIAlready       = errors.New("Write sequence number already used for the RNCUI")
	ErrRNCUIINProgress    = errors.New("Write sequence in progress of apply for the RNCUI")
	ErrRNCUIWayAhead      = errors.New("Write sequence way ahead")
	ErrRNCUIWrongClient   = errors.New("RNCUI used by wrong pumice client")
	ErrRNCUIAlreadyQueued = errors.New("RNCUI already queued in the pumice client")

	ErrInvalid         = errors.New("Invalid initialization")
	ErrNoMem           = errors.New("No memory in pumice client/server")
	ErrTooBIG          = errors.New("The application message is too big, expects 41,93,704 bytes max (4MB - 88 bytes - 512 bytes)")
	ErrNoLeader        = errors.New("No pumice leader identified yet")
	ErrNoSpace         = errors.New("Pumice client heap is full")
	ErrAlreadyQ        = errors.New("Request already queued for the same RNCUI in the pumice client")
	ErrTimeout         = errors.New("Operation timed out at the pumice client")
	ErrTryAgain        = errors.New("Temporary failure in the raft side, try again")
	ErrNoSpaceOnServer = errors.New("No space left on write supplement buffer on the pumice server")
)

// TranslatePumiceReqErrCode translates PumiceDB C error codes to Go errors
func TranslatePumiceReqErrCode(code int) error {
	switch math.Abs(float64(code)) {
	case 17:
		//-EALREADY (If in case Req RNCUI is less than to the last RNCUI)
		return ErrRNCUIAlready
	case 115:
		//-EINPROGRESS (If same RNCUI is in progress of commit)
		return ErrRNCUIINProgress
	case 52:
		//-EBADE (If the req RNCUI way ahead)
		return ErrRNCUIWayAhead
	case 1:
		//-EPERM (If two different client uses same RNCUI)
		return ErrRNCUIWrongClient
	case 22:
		return ErrInvalid
	case 12:
		//ENOMEM
		return ErrNoMem
	case 27:
		//EFBIG
		return ErrTooBIG
	case 107:
		//ENOTCONN
		return ErrNoLeader
	case 28:
		//ENOSPC
		return ErrNoSpace
	case 114:
		//EBUSY
		return ErrAlreadyQ
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
		return ErrInvalid
	case 12:
		//ENOMEM
		return ErrNoMem
	case 28:
		//ENOSPC
		return ErrNoSpaceOnServer
	default:
		return fmt.Errorf("unknown error code: %d", code)
	}
	return nil
}
