package iterator

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/page"
)

var ErrOrdinalScanStale = errors.New("treedb: maintenance source changed")
var ErrOrdinalScanIncomplete = errors.New("treedb: maintenance backend/winner traversal incomplete")

// OrdinalScanCursor contains scalars only. It owns no source or snapshot lease.
// A changed publication epoch invalidates it before another source is touched.
type OrdinalScanFrame struct {
	Ref  page.ChildRef
	Slot uint16
}

type OrdinalScanCursor struct {
	PublicationGeneration        uint64
	Epoch                        uint64
	MutableBytes                 int64
	Source, Ordinal              uint64
	Initialized                  bool
	SourceEnd                    uint64
	SourceCaptured               bool
	CacheDone                    bool
	BackendCaptured, BackendDone bool
	BackendRoot, BackendCommit   uint64
	BackendDepth                 uint8
	BackendStack                 [50]OrdinalScanFrame
}

// FrameBoundsCursor retains only scalar directory validation progress. It owns
// no frame buffer, physical reader, source or root across a yield.
type FrameBoundsCursor struct {
	NextRID, NextOffset  uint16
	Previous, Start, End uint32
}

// OrdinalRecordCursor is borrowed from the current maintenance owner. The
// immutable record/header identity is rechecked under current source guards
// before directory progress is consumed again.
type OrdinalRecordCursor struct {
	Pointer page.ValuePtr
	Header  [20]byte
	Frame   [12]byte
	Active  bool
	// CRCProved certifies the complete directory of this physical frame.
	// Retarget and SelectedOffsets resume only its two current bounds.
	CRCProved       bool
	Retarget        bool
	SelectedOffsets uint8
	Directory       FrameBoundsCursor
}

// OrdinalLeafPage owns one decoded page while its separately admitted page
// checksum or entry tail awaits a guarded cut. It contains no physical reader or root lease.
type OrdinalLeafPage struct {
	Data    *[page.PageSize]byte
	Pointer page.ValuePtr
	Header  [20]byte
}

// OrdinalScanWork is local to a single call. All source and entry work must
// reserve before copying or consuming data. Cleanup is reserved by the caller up front.
type OrdinalScanWork struct {
	Record     *OrdinalRecordCursor
	LeafRecord *OrdinalRecordCursor
	// LeafPage borrows a serialized maintenance owner's one-page scratch slot.
	LeafPage               **OrdinalLeafPage
	RecordLimit, ByteLimit uint64
	Records, Bytes         uint64
	// Current admitted observation: smaller source priority wins across tables,
	// later insertion ordinals win within one table.
	SourcePriority, EntryOrdinal uint64
	EntryRef                     page.ChildRef
}

func (w *OrdinalScanWork) Reserve(records, bytes uint64) bool {
	if w.Records > w.RecordLimit || w.Bytes > w.ByteLimit || records > w.RecordLimit-w.Records || bytes > w.ByteLimit-w.Bytes {
		return false
	}
	w.Records += records
	w.Bytes += bytes
	return true
}

// OrdinalUnitTooLarge describes a unit that cannot fit even in a fresh quantum.
type OrdinalUnitTooLarge struct{ Records, Bytes uint64 }

func (e *OrdinalUnitTooLarge) Error() string {
	return fmt.Sprintf("treedb: maintenance unit requires records=%d bytes=%d", e.Records, e.Bytes)
}
