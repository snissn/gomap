package collections

import (
	"strings"
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeDomainState is the original owner of one immutable root-local source.
// baseRoot and pending are resolved under the enclosing complete engine cut's
// exact disk snapshot. No read follows a mutable write-domain table or catalog.
// catalogRoot authenticates the separately persisted catalog descriptor. A full
// replacement keeps that provenance while setting baseRoot to zero, so physical
// maintenance cannot turn the superseded graph back into a logical read source.
// Ordinary changes preserve interpretation; DDL replaces it explicitly.
// sourceGeneration records the primary generation covered by a named saved
// image; physical rollover/handoff and ordinary domain deltas preserve it.
type nativeDomainState struct {
	refs             atomic.Int64
	name             string
	baseRoot         uint64
	catalogRoot      uint64 // authenticated physical catalog provenance; not a read source
	interpretation   uint64
	sourceGeneration uint64
	storagePolicy    backenddb.OrderedRootStoragePolicy
	pending          *memtable.COWRoot
	typed            *nativeTypedDelta
	captured         *nativePendingSources
	privateSource    *nativePendingSource // partial rollover edge; never published
	refund           *cowbtree.OwnedMapRefund
	finished         bool
}

// newNativeDomainState consumes pending only on success. Its descriptor/name
// and original independent refund receipt are admitted before allocation. The
// returned reference belongs to the original sparse plan until transferred.
func newNativeDomainState(name string, baseRoot, interpretation uint64, policy backenddb.OrderedRootStoragePolicy, pending *memtable.COWRoot, admit cowbtree.OwnedMapAdmit) (*nativeDomainState, error) {
	if name == "" || interpretation == 0 || admit == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	charge := nativeDomainStateCharge(name)
	lease, err := admit(charge)
	if err != nil {
		return nil, err
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	d := &nativeDomainState{name: strings.Clone(name), baseRoot: baseRoot, catalogRoot: baseRoot, interpretation: interpretation, storagePolicy: policy, pending: pending, refund: refund}
	d.refs.Store(1)
	return d, nil
}

// nativeDomainLifetime attaches actual descriptor edges from structurally
// shared directory nodes. Physical cleanup runs only from the original native
// worker. Returning receipts lets the caller detach its value slot first.
type nativeDomainLifetime struct{}

func (nativeDomainLifetime) Retain(d *nativeDomainState) {
	if d == nil || d.refs.Add(1) <= 1 {
		panic("collections: retain released native domain")
	}
}
func (nativeDomainLifetime) Release(d *nativeDomainState) (cowbtree.OwnedMapCompletion, error) {
	if d == nil {
		return cowbtree.OwnedMapCompletion{}, nil
	}
	refs := d.refs.Add(-1)
	if refs < 0 {
		panic("collections: native domain released twice")
	}
	if refs != 0 {
		return cowbtree.OwnedMapCompletion{}, nil
	}
	if d.finished {
		panic("collections: native domain cleanup replayed")
	}
	d.finished = true
	var retirement memtable.COWRetirement
	if d.pending != nil {
		retirement = d.pending.Release()
		d.pending = nil
	}
	// The original COW source alias is detached before its generation worker
	// refunds source backing. No source ownership is reconstructed from names.
	retirement.Drain()
	captured := d.captured
	d.captured = nil
	done := captured.release()
	typed := d.typed
	d.typed = nil
	done.Append(typed.release())
	typed = nil
	done.Append(d.privateSource.release())
	d.privateSource = nil
	captured = nil
	refund := d.refund
	d.refund = nil
	d.name = ""
	done.Append(refund.Completion())
	return done, nil
}

func nativeDomainStateCharge(name string) uint64 {
	metadata := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainState{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	text := memtable.COWAllocationCharge(uint64(len(name)))
	if text > ^uint64(0)-metadata {
		return ^uint64(0) // original admission rejects before allocating anything
	}
	return metadata + text
}
