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
	return withDenseSearchWorkAndScorePlane(err, work, nil)
}

// withDenseSearchWorkAndScorePlane preserves the selected score-plane proof
// when an error happens after the raw search has completed (for example while
// decoding one returned document). Both proofs are copied so the error never
// retains response buffers or an owner-local value.
func withDenseSearchWorkAndScorePlane(err error, work *DenseSearchWork, scorePlane *collections.ColumnGraphScorePlaneWork) error {
	if err == nil || (work == nil && scorePlane == nil) {
		return err
	}
	var ownedWork *DenseSearchWork
	if work != nil {
		copy := *work
		copy.Completed = false
		ownedWork = &copy
	}
	out := &Error{Code: ErrorCodeOf(err), Message: err.Error(), Err: err, DenseWork: ownedWork}
	if ownedWork != nil && (ownedWork.Graph.ScorePlane.Available || ownedWork.Graph.ScorePlane.Version != 0) {
		plane := ownedWork.Graph.ScorePlane
		out.ScorePlane = &plane
	}
	if scorePlane != nil {
		plane := *scorePlane
		out.ScorePlane = &plane
	}
	var original *Error
	if errors.As(err, &original) {
		out.Message = original.Message
		if out.DenseWork == nil && original.DenseWork != nil {
			workCopy := *original.DenseWork
			out.DenseWork = &workCopy
		}
		if out.ScorePlane == nil && original.ScorePlane != nil {
			plane := *original.ScorePlane
			out.ScorePlane = &plane
		}
	}
	return out
}
