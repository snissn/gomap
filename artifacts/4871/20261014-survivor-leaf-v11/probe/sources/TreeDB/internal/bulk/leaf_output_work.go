package bulk

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"unsafe"
)

var ErrLeafOutputState = errors.New("bulk: leaf output state")

// LeafOutput retains the SAME levelBuilder/node encoder and exactly one page.
// It owns no allocator, publication or root authority. Its caller must keep the
// original input cut and containing producer alive through checked completion.
type LeafOutput struct {
	level    levelBuilder
	encoder  node.Builder
	image    [page.PageSize]byte
	owner    *retainedalloc.Owner
	charge   uint64
	finished bool
	append   freelist.AppendWork
	stored   bool
}

func NewLeafOutput(owner *retainedalloc.Owner, opts BuildOptions, w *iterator.OrdinalScanWork) (*LeafOutput, bool, error) {
	if owner == nil {
		return nil, false, ErrLeafOutputState
	}
	if opts.LeafColumnar || opts.LeafPageLog != nil {
		return nil, false, node.ErrBuilderWorkFormat
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(LeafOutput{})))
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(LeafOutput{}))) {
		return nil, false, nil
	}
	if err := owner.AddPending(charge); err != nil {
		return nil, false, err
	}
	o := &LeafOutput{owner: owner, charge: charge}
	o.encoder.ResetWithOptions(o.image[:], page.PageTypeLeaf, leafBuilderOptions(opts))
	o.level.builder = &o.encoder
	return o, true, nil
}
func (o *LeafOutput) Add(record node.LeafEntry, w *iterator.OrdinalScanWork) (bool, error) {
	if o == nil || o.owner == nil || o.finished {
		return false, ErrLeafOutputState
	}
	return o.level.builder.AddLeafEntryWithRevisionWithWork(record.Key, record.Value, record.Flags, record.ValuePtr, record.Revision, w)
}
func (o *LeafOutput) Finish(pageID uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if o == nil || o.owner == nil {
		return false, ErrLeafOutputState
	}
	if o.finished {
		return true, nil
	}
	if w != nil && !w.Reserve(1, 8) {
		return false, nil
	}
	o.encoder.SetPageID(pageID)
	ready, err := o.level.builder.FinishNoNodeWithWork(w)
	if ready && err == nil {
		o.finished = true
	}
	return ready, err
}

// Store advances original append allocation/growth, final encoding and pager
// write. No ordinary allocator or write fallback is called on work refusal.
func (o *LeafOutput) Store(a *freelist.Allocator, p *pager.Pager, w *iterator.OrdinalScanWork) (uint64, bool, error) {
	if o == nil || o.owner == nil || o.finished {
		return 0, false, ErrLeafOutputState
	}
	id, ready, err := o.append.Step(a, w)
	if !ready || err != nil {
		return id, false, err
	}
	if !o.stored {
		if w != nil && !w.Reserve(1, 8) {
			return id, false, nil
		}
		o.encoder.SetPageID(id)
		ready, err = o.encoder.FinishNoNodeWithWork(w)
		if !ready || err != nil {
			return id, false, err
		}
		ready, err = p.WriteWithWork(id, o.image[:], w)
		if !ready || err != nil {
			return id, false, err
		}
		o.stored = true
		o.finished = true
	}
	return id, true, nil
}

// Image is borrowed until Close. It cannot be used as an installed root or
// publication proof; storage must write the actual page under its allocator.
func (o *LeafOutput) Image() []byte {
	if o == nil || !o.finished {
		return nil
	}
	return o.image[:]
}

// Close detaches all original encoder/page aliases before returning by-value
// accounting. Caller clears its last *LeafOutput alias before RemovePending.
func (o *LeafOutput) Close(w *iterator.OrdinalScanWork) (*retainedalloc.Owner, uint64, bool, error) {
	if o == nil || o.owner == nil {
		return nil, 0, true, nil
	}
	if ready, err := o.append.Cancel(w); !ready || err != nil {
		return nil, 0, false, err
	}
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(*o))) {
		return nil, 0, false, nil
	}
	owner, charge := o.owner, o.charge
	*o = LeafOutput{}
	return owner, charge, true, nil
}
