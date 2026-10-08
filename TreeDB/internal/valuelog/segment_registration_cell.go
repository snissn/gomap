package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"sync/atomic"
	"unsafe"
)

// A constructor-created File and every ordinary Set reference use this one
// actual counter. Selected registration holds retain only this scalar cell,
// never an interior pointer into the callback-bearing File allocation.
type segmentRegistrationCell struct {
	refs           atomic.Int64
	closed         atomic.Bool
	terminalClosed atomic.Bool // unlink+namespace persistence+actual parent-close completion
	id             uint32
	generation     uint64
	classBytes     uint64
	incarnation    *managerRegistrationIncarnation
}

// The nonzero-size tag is an actually retained incarnation, not an address key
// that can be reused after a Manager is collected. It contains no Manager edge.
type managerRegistrationIncarnation struct {
	closed     atomic.Bool
	classBytes uint64
}

func newSegmentRegistrationCell() *segmentRegistrationCell {
	n, _ := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(segmentRegistrationCell{})), true)
	return &segmentRegistrationCell{classBytes: n}
}
func newManagerRegistrationIncarnation() *managerRegistrationIncarnation {
	n, _ := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(managerRegistrationIncarnation{})), false)
	return &managerRegistrationIncarnation{classBytes: n}
}

// File literals remain valid ordinary fixtures. Their inline count is never
// migrated after publication and cannot certify a selected finite retention.
type fileReferenceCount struct {
	cell     *segmentRegistrationCell
	ordinary atomic.Int64
}

func (r *fileReferenceCount) Load() int64 {
	if r.cell != nil {
		return r.cell.refs.Load()
	}
	return r.ordinary.Load()
}
func (r *fileReferenceCount) Add(delta int64) int64 {
	if r.cell != nil {
		return r.cell.refs.Add(delta)
	}
	return r.ordinary.Add(delta)
}
func (r *fileReferenceCount) Store(value int64) {
	if r.cell != nil {
		r.cell.refs.Store(value)
		return
	}
	r.ordinary.Store(value)
}
