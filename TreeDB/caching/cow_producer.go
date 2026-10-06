package caching

import (
	"slices"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type cowProducedFrame struct {
	header valuelog.FrameHeader
	first  page.ValuePtr
	count  int
}

// cowFrameCapture is one pre-admitted finite producer vector. It never retains
// a RID registry, encoded frame, writer pointer slice, or borrowed value bytes.
type cowFrameCapture struct {
	frames   []cowProducedFrame
	observer valuelog.ProducedFrameObserver
	lease    *memtable.COWExternalLease
	err      error
	sorted   bool
}

func newCOWFrameCapture(budget *memtable.COWBudget, points int) (*cowFrameCapture, error) {
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowFrameCapture{}))) +
		memtable.COWAllocationCharge(uint64(points)*uint64(unsafe.Sizeof(cowProducedFrame{}))) +
		memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0)))) +
		// The write attempt installs one unlock environment containing code,
		// batch, previous unlock function and this capture. Its lifetime ends only
		// after unlocked cleanup closes this same capture admission.
		memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(uintptr(0))))
	lease, err := budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	c := &cowFrameCapture{frames: make([]cowProducedFrame, 0, points), lease: lease}
	c.observer = c.observe
	return c, nil
}

func (c *cowFrameCapture) observe(header valuelog.FrameHeader, first page.ValuePtr, count int) {
	if c.err != nil {
		return
	}
	if count < 1 || count > valuelog.MaxFrameK || header.K != uint8(count) || len(c.frames) == cap(c.frames) {
		c.err = memtable.ErrCOWCapacity
		return
	}
	c.frames = append(c.frames, cowProducedFrame{header: header, first: first, count: count})
	c.sorted = false
}

func (c *cowFrameCapture) frame(ptr page.ValuePtr) (valuelog.FrameHeader, bool) {
	if !c.sorted {
		// Placement may reorder dictionary classes. Sort the admitted backing
		// once, then use logarithmic association for each canonical point.
		slices.SortFunc(c.frames, compareCOWProducedFrames)
		c.sorted = true
	}
	lo, hi := 0, len(c.frames)
	for lo < hi {
		mid := lo + (hi-lo)/2
		first := c.frames[mid].first
		if first.FileID < ptr.FileID || first.FileID == ptr.FileID && first.Offset < ptr.Offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(c.frames) {
		frame := c.frames[lo]
		if ptr.FileID == frame.first.FileID && ptr.Offset == frame.first.Offset && int(page.ValuePtrSubIndex(ptr)) < frame.count {
			return frame.header, true
		}
	}
	return valuelog.FrameHeader{}, false
}

func compareCOWProducedFrames(a, b cowProducedFrame) int {
	if a.first.FileID < b.first.FileID || a.first.FileID == b.first.FileID && a.first.Offset < b.first.Offset {
		return -1
	}
	if a.first.FileID == b.first.FileID && a.first.Offset == b.first.Offset {
		return 0
	}
	return 1
}

func (b *Batch) cowProducedFrameObserver() valuelog.ProducedFrameObserver {
	if b.cowState == nil || b.cowState.producer == nil {
		return nil
	}
	return b.cowState.producer.observer
}

func (c *cowFrameCapture) close() {
	if c == nil {
		return
	}
	c.frames = nil
	c.observer = nil
	c.lease.Close()
}

type cowObservedValueWriter interface {
	SwapProducedFrameObserver(valuelog.ProducedFrameObserver) valuelog.ProducedFrameObserver
}

func installCOWProducerObserver(w valueWriter, observer valuelog.ProducedFrameObserver) (valuelog.ProducedFrameObserver, error) {
	if observer == nil {
		return nil, nil
	}
	owner, ok := w.(cowObservedValueWriter)
	if !ok {
		return nil, ErrCOWUnsupported
	}
	return owner.SwapProducedFrameObserver(observer), nil
}

func restoreCOWProducerObserver(w valueWriter, observer, old valuelog.ProducedFrameObserver) {
	if observer != nil {
		w.(cowObservedValueWriter).SwapProducedFrameObserver(old)
	}
}
