package node

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

var ErrProjectionPending = errors.New("node: projection operation pending")
var ErrProjectionGeometry = errors.New("node: reconstructed projection key exceeds admitted scratch; integration must extend phased geometry")

// ProjectionComparison borrows immutable operands and retains the exact byte
// offset across quanta. It has no counters separate from OrdinalScanWork.
type ProjectionComparison struct {
	a, b           []byte
	offset, result int
	active         bool
}

func (c *ProjectionComparison) Begin(a, b []byte, w *iterator.OrdinalScanWork) bool {
	if c.active || w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*c))) {
		return false
	}
	*c = ProjectionComparison{a: a, b: b, active: true}
	return true
}
func (c *ProjectionComparison) Step(w *iterator.OrdinalScanWork) (int, bool) {
	if !c.active {
		return c.result, true
	}
	if w == nil {
		return 0, false
	}
	for c.offset < min(len(c.a), len(c.b)) {
		if !w.Reserve(0, 2) {
			return 0, false
		}
		a, b := c.a[c.offset], c.b[c.offset]
		c.offset++
		if a < b {
			c.result = -1
			c.active = false
			return -1, true
		}
		if a > b {
			c.result = 1
			c.active = false
			return 1, true
		}
	}
	if len(c.a) < len(c.b) {
		c.result = -1
	} else if len(c.a) > len(c.b) {
		c.result = 1
	}
	c.active = false
	return c.result, true
}
func (c *ProjectionComparison) Cancel(w *iterator.OrdinalScanWork) bool {
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*c))) {
		return false
	}
	*c = ProjectionComparison{}
	return true
}
