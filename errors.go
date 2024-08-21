package lfsb

import (
	"errors"
	"fmt"

	"github.com/superfly/ltx"
)

type ErrorType string

const (
	// ErrorTypeAuth          ErrorType = "auth"
	ErrorTypeConflict      ErrorType = "conflict"
	ErrorTypeNotFound      ErrorType = "notfound"
	ErrorTypeValidation    ErrorType = "validation"
	ErrorTypeUnprocessable ErrorType = "unprocessable"

	ErrorTypeUnknown = "unknown"
)

type Error struct {
	Type              ErrorType
	Code              string
	Message           string
	TXID              ltx.TXID
	PostApplyChecksum ltx.Pos
}

// Error implements Error interace
func (e *Error) Error() string {
	return fmt.Sprintf("%s (%s): %s", e.Type, e.Code, e.Message)
}

// Errorf is a helper function for returning Error values.
func Errorf(typ ErrorType, code, format string, a ...any) *Error {
	return &Error{
		Type:    typ,
		Code:    code,
		Message: fmt.Sprintf(format, a...),
	}
}

func (e *Error) Is(err error) bool {
	if re, ok := err.(*Error); ok {
		return e.Type == re.Type && e.Code == re.Code && e.Message == re.Message
	}

	return false
}

// ErrorCode returns the error code from an error. Returns blank if err is nil.
// Returns EINTERNAL if no lfsc.Error is found.
func ErrorCode(err error) string {
	if err == nil {
		return ""
	}

	for {
		switch x := err.(type) {
		case *Error:
			return x.Code
		case interface{ Unwrap() error }:
			err = x.Unwrap()
		default:
			return "EINTERNAL"
		}
	}
}

// Common error codes. Used for compile-time checks.
const (
	EPOSMISMATCH       = "EPOSMISMATCH"
	ENOCLUSTER         = "ENOCLUSTER"
	ENOCOMPACTION      = "ENOCOMPACTION"
	EPARTIALCOMPACTION = "EPARTIALCOMPACTION"
)

var (
	ErrInvalidLTXFilename = Errorf(ErrorTypeValidation, "EBADPATH", "invalid ltx filename")
	// ErrStoragePathInvalid = Errorf(ErrorTypeValidation, "EBADPATH", "invalid storage path")

	// ErrRegionRequired = Errorf(ErrorTypeValidation, "EBADREGION", "region required")

	ErrClusterRequired = Errorf(ErrorTypeValidation, "EBADCLUSTER", "cluster required")
	ErrClusterInvalid  = Errorf(ErrorTypeValidation, "EBADCLUSTER", "cluster invalid")
	// ErrClusterExists   = Errorf(ErrorTypeConflict, "ECLUSTEREXIST", "cluster already exists")
	// ErrClusterDeleted  = Errorf(ErrorTypeUnprocessable, "ECLUSTERDELETED", "cluster deleted")

	// ErrClusterIDRequired = Errorf(ErrorTypeValidation, "EBADCLUSTERID", "cluster id required")
	// ErrClusterIDInvalid  = Errorf(ErrorTypeUnprocessable, "EBADCLUSTERID", "cluster id invalid")
	// ErrClusterIDNotFound = Errorf(ErrorTypeNotFound, "ENOCLUSTERID", "cluster id not found")

	ErrDatabaseRequired = Errorf(ErrorTypeValidation, "EBADDB", "database required")
	ErrDatabaseNotFound = Errorf(ErrorTypeNotFound, "ENODB", "database not found")

	// ErrDatabaseEmpty    = Errorf(ErrorTypeNotFound, "EDBEMPTY", "database empty")

	// ErrBucketNotFound = Errorf(ErrorTypeNotFound, "ENOBUCKET", "bucket not found")
	// ErrRegionNotFound = Errorf(ErrorTypeNotFound, "ENOREGION", "region not found")

	ErrMinTXIDRequired = Errorf(ErrorTypeValidation, "EBADTXID", "minimum transaction id required")
	ErrMaxTXIDRequired = Errorf(ErrorTypeValidation, "EBADTXID", "maximum transaction id required")
	// ErrInvalidPos      = Errorf(ErrorTypeValidation, "EBADPOS", "DB position is invalid")

	ErrCannotCompactToLevelZero = Errorf(ErrorTypeValidation, "EBADLEVEL", "cannot compact to level zero")
	ErrCompactionLevelTooHigh   = Errorf(ErrorTypeValidation, "EBADLEVEL", "compaction level too high")

	ErrTxNotAvailable = Errorf(ErrorTypeNotFound, "ENOTXID", "tx not available")
	// ErrTxExists        = Errorf(ErrorTypeConflict, "ETXEXISTS", "tx already exists")
	// ErrPgnoOutOfBounds = Errorf(ErrorTypeNotFound, "EPGNOOOB", "page number out of bounds")
	ErrPageNotFound = Errorf(ErrorTypeNotFound, "ENOPAGE", "page not found")
	// ErrInvalidPgno     = Errorf(ErrorTypeValidation, "EINVALIDPGNO", "invalid page number")

	ErrTimestampNotAvailable = Errorf(ErrorTypeNotFound, "ENOTIMESTAMP", "timestamp not available")

	// ErrClusterLimitReached = Errorf(ErrorTypeUnprocessable, "ECLUSTERLIMIT", "clusters limit reached")

	ErrPageSizeMismatch = Errorf(ErrorTypeUnprocessable, "EBADHEADER", "page size mismatch")

// ErrLeaseExists   = Errorf(ErrorTypeConflict, "ELEASEEXISTS", "lease already exists")
// ErrLeaseNotFound = Errorf(ErrorTypeNotFound, "ENOLEASE", "lease not found")
// ErrLeaseMismatch = Errorf(ErrorTypeUnprocessable, "ELEASEMISMATCH", "lease mismatch")
)

// IsApplicationError returns true if err is an lfsc.Error or ltx.PositionMismatchError.
func IsApplicationError(err error) bool {
	if err == nil {
		return false
	}

	var lfscError *Error
	if errors.As(err, &lfscError) {
		return true
	}

	var posMismatchError *ltx.PosMismatchError
	return errors.As(err, &posMismatchError)
}
