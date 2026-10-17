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
// Ordinary changes preserve interpretation; DDL replaces it explicitly.
type nativeDomainState struct {
	refs           atomic.Int64
	name           string
	baseRoot       uint64
	interpretation uint64
	storagePolicy  backenddb.OrderedRootStoragePolicy
	pending        *memtable.COWRoot
	captured       *nativePendingSources
	refund         *cowbtree.OwnedMapRefund
	finished       bool
}

// newNativeDomainState consumes pending only on success. Its descriptor/name
// and original independent refund receipt are admitted before allocation. The
// returned reference belongs to the original sparse plan until transferred.
func newNativeDomainState(name string, baseRoot, interpretation uint64, policy backenddb.OrderedRootStoragePolicy, pending *memtable.COWRoot, admit cowbtree.OwnedMapAdmit) (*nativeDomainState, error) {
	if name == "" || interpretation == 0 || admit == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeDomainState{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{}))) +
		memtable.COWAllocationCharge(uint64(len(name)))
	lease, err := admit(charge)
	if err != nil {
		return nil, err
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	d := &nativeDomainState{name: strings.Clone(name), baseRoot: baseRoot, interpretation: interpretation, storagePolicy: policy, pending: pending, refund: refund}
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
	captured = nil
	refund := d.refund
	d.refund = nil
	d.name = ""
	done.Append(refund.Completion())
	return done, nil
}
