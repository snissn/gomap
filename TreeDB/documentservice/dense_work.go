package documentservice

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
)

// DenseSearchWork is an owned, versioned observation of one selected typed
// service call. Absence means unavailable. It never samples process counters.
type DenseSearchWork struct {
	Version   uint64                           `json:"version"`
	Completed bool                             `json:"completed"`
	Graph     collections.ColumnGraphQueryWork `json:"graph"`
	Output    DenseSearchOutputWork            `json:"output"`
}

// DenseSearchOutputWork counts the actual fetch prefix, including errors.
// OutputBytes are materialized document bytes, not encoded frame/HTTP bytes.
type DenseSearchOutputWork struct {
	Attempted              bool   `json:"attempted"`
	Completed              bool   `json:"completed"`
	Requested              uint64 `json:"requested"`
	Fetched                uint64 `json:"fetched"`
	Missing                uint64 `json:"missing"`
	OutputBytes            uint64 `json:"output_bytes"`
	RetainedPayloadFetches uint64 `json:"retained_payload_fetches"`
	JSONReconstructionRows uint64 `json:"json_reconstruction_rows"`
	TypedColumnRows        uint64 `json:"typed_column_rows"`
}

func withDenseSearchWork(err error, work *DenseSearchWork) error {
	if err == nil || work == nil {
		return err
	}
	owned := *work
	owned.Completed = false
	out := &Error{Code: ErrorCodeOf(err), Message: err.Error(), Err: err, DenseWork: &owned}
	var original *Error
	if errors.As(err, &original) {
		out.Message = original.Message
	}
	return out
}
