package valuelog

import (
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// COWBasisRefusalObserver is a diagnostic-only, fixture-owned scalar receipt.
// It retains only a resource-free owner identity, never a DB, snapshot, file,
// allocator, error, key or body. It is single-owner and must never be reset or
// reused for another owner. Each scalar is safe to read even after failed Close.
// Cross-counter snapshots are deliberately not claimed to be atomic cuts.
type COWBasisRefusalObserver struct {
	owner     atomic.Pointer[transactionowner.Owner]
	calls     atomic.Uint64
	completed atomic.Uint64
	first     atomic.Uint64
	overflow  atomic.Bool
	missing   atomic.Bool
	entered   [COWBasisStageCount]atomic.Uint64
	finished  [COWBasisStageCount]atomic.Uint64
}

type COWBasisStage uint8

const (
	COWBasisRefresh COWBasisStage = iota
	COWBasisDescriptor
	COWBasisPin
	COWBasisInspect
	COWBasisDecoderSizes
	COWBasisAdmission
	COWBasisDecoder
	COWBasisLeafRead
	COWBasisStageCount
)

type COWBasisDescriptorKind uint8

const (
	COWBasisNoDescriptor COWBasisDescriptorKind = iota
	COWBasisNextIncarnation
	COWBasisMetadata
	COWBasisIncarnation
	COWBasisOverlay
	COWBasisRoot
)

type COWBasisRefusal uint8

const (
	COWBasisOtherError COWBasisRefusal = iota + 1
	COWBasisUnsupported
	COWBasisCapacity
	COWBasisCorrupt
	COWBasisSourceContract
	COWBasisFrameContract
	COWBasisPayloadContract
	COWBasisDictionary
)

// COWBasisCall is a synchronous, explicit invocation label. Copies contain
// only scalar labels and the fixture receipt, never borrowed storage authority.
// The cache's original writer gate serializes Begin calls for the bound owner;
// no other readers discover or look up a current call.
type COWBasisCall struct {
	observer   *COWBasisRefusalObserver
	ordinal    uint32
	frozen     bool
	descriptor COWBasisDescriptorKind
	after      bool
	leafRead   bool
}

// COWBasisRefusalSnapshot.First is one indivisible first-refusal tuple:
// [0:23] ordinal, [24] frozen, [25:27] descriptor, [28] after,
// [29:32] stage, [33:36] refusal, [37] shape-known, [38] compressed,
// [39] dictionary-present, [40:47] codec. Zero means NO OBSERVED REFUSAL,
// not success. Completed/counts are independent diagnostics, not a join proof.
type COWBasisRefusalSnapshot struct {
	Calls, Completed, First uint64
	Overflow, Missing       bool
	Entered, Finished       [COWBasisStageCount]uint64
}

func (o *COWBasisRefusalObserver) Bind(owner *transactionowner.Owner) bool {
	return o != nil && owner != nil && o.owner.CompareAndSwap(nil, owner)
}

func (o *COWBasisRefusalObserver) Begin(owner *transactionowner.Owner, frozen bool) COWBasisCall {
	if o == nil || owner == nil || o.owner.Load() != owner || o.overflow.Load() {
		return COWBasisCall{}
	}
	n := o.calls.Add(1)
	if n > 0xffffff {
		o.overflow.Store(true)
		return COWBasisCall{}
	}
	c := COWBasisCall{observer: o, ordinal: uint32(n), frozen: frozen}
	c.Enter(COWBasisRefresh)
	return c
}

func (c COWBasisCall) Enabled() bool { return c.observer != nil && c.ordinal != 0 }

func (c COWBasisCall) Descriptor(kind COWBasisDescriptorKind) COWBasisCall {
	c.descriptor = kind
	return c
}

func (c COWBasisCall) Side(after bool) COWBasisCall {
	c.after = after
	return c
}

// LeafRead distinguishes revalidation during the original final read from
// the earlier inspector; it does not imply that a stage was entered.
func (c COWBasisCall) LeafRead() COWBasisCall {
	c.leafRead = true
	return c
}

func (c COWBasisCall) Enter(stage COWBasisStage) {
	if c.Enabled() && stage < COWBasisStageCount {
		c.observer.entered[stage].Add(1)
	}
}

func (c COWBasisCall) Finish(stage COWBasisStage) {
	if c.Enabled() && stage < COWBasisStageCount {
		c.observer.finished[stage].Add(1)
		if stage == COWBasisRefresh {
			c.observer.completed.Add(1)
		}
	}
}

func (c COWBasisCall) Missing() {
	if c.Enabled() {
		c.observer.missing.Store(true)
	}
}

func (c COWBasisCall) Refuse(stage COWBasisStage, why COWBasisRefusal, shape COWRecordShape, known bool) {
	if !c.Enabled() || stage >= COWBasisStageCount || why == 0 || why > 15 {
		return
	}
	if c.leafRead && stage == COWBasisInspect {
		stage = COWBasisLeafRead
	}
	word := uint64(c.ordinal) | uint64(c.descriptor)<<25 | uint64(stage)<<29 | uint64(why)<<33
	if c.frozen {
		word |= 1 << 24
	}
	if c.after {
		word |= 1 << 28
	}
	if known {
		word |= 1 << 37
		if shape.Compressed {
			word |= 1 << 38
		}
		if shape.DictID != 0 {
			word |= 1 << 39
		}
		word |= uint64(uint8(shape.Codec)) << 40
	}
	c.observer.first.CompareAndSwap(0, word)
}

// Direct comparisons never invoke an arbitrary error's Is/Unwrap/Error methods.
func (c COWBasisCall) Error(stage COWBasisStage, err error, shape COWRecordShape, known bool) {
	if !c.Enabled() || err == nil {
		return
	}
	why := COWBasisOtherError
	switch err {
	case ErrCOWReadUnsupported:
		why = COWBasisUnsupported
	case ErrCOWReadCapacity:
		why = COWBasisCapacity
	case ErrCorrupt:
		why = COWBasisCorrupt
	}
	c.Refuse(stage, why, shape, known)
}

func (o *COWBasisRefusalObserver) Snapshot() COWBasisRefusalSnapshot {
	if o == nil {
		return COWBasisRefusalSnapshot{}
	}
	s := COWBasisRefusalSnapshot{Calls: o.calls.Load(), Completed: o.completed.Load(), First: o.first.Load(), Overflow: o.overflow.Load(), Missing: o.missing.Load()}
	for i := range s.Entered {
		s.Entered[i] = o.entered[i].Load()
		s.Finished[i] = o.finished[i].Load()
	}
	return s
}
