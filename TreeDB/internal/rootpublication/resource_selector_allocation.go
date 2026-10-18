package rootpublication

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Only selected callers construct this original, admitted operation ledger.
// Each partial is adopted before further fallibility. A physical cleanup
// refusal returns this ledger's own preborn receipt, preserving the SAME
// operands; it adds no worker, retry, registry or replacement failure owner.
type selectedResourceSelectorOperation struct {
	allocation *resourceAllocation
	borrow     resourceKindBorrow
	token      *StableResourceToken
	entry      *resourceEntryBacking
	view       stableResourceKindView
	kinds      *resourceKindBacking
	result     *StableResourceSet
}

type selectedResourceSelectorWork struct {
	LogicalIndexNodes, EntriesCloned uint64
}

// The exact immutable kind-root edge is held while lookup and construction run
// outside source.mu. No resource/obligation catalog walk or generic backing is
// needed. The caller keeps its independently enrolled allocation owner alive.
func cloneSelectedStableResourceForSelector(source *StableResourceSet, selector StableResourceSelector, work *selectedResourceSelectorWork) (*StableResourceSet, error) {
	owner := source.MetadataOwner()
	if owner == nil {
		return nil, ErrResourceOwnership
	}
	allocation, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(selectedResourceSelectorOperation{}))))
	if err != nil {
		return nil, err
	}
	op := &selectedResourceSelectorOperation{allocation: allocation}
	op.borrow, err = source.borrowSelectedKindViews()
	if err != nil {
		return op.complete(err)
	}
	err = op.selectResource(selector, work)
	return op.complete(err)
}

func (op *selectedResourceSelectorOperation) selectResource(selector StableResourceSelector, work *selectedResourceSelectorWork) error {
	view, ok := op.borrow.backing.find(selector.Kind)
	if !ok {
		return ErrUnresolvedResource
	}
	key := stableLogicalResourceKey{kind: selector.Kind, lane: selector.LogicalLane, resourceID: selector.ResourceID, generation: selector.PhysicalGeneration}
	var entry *stableResourceEntry
	for root := view.logical; root != nil; {
		if work != nil {
			work.LogicalIndexNodes++
		}
		if key == root.key {
			entry = root.entry
			break
		}
		if stableLogicalResourceKeyLess(key, root.key) {
			root = root.left
		} else {
			root = root.right
		}
	}
	if entry == nil {
		return ErrUnresolvedResource
	}
	value, found, err := entry.logicalObligations.lookup(selector.Obligation, nil)
	if err != nil {
		return err
	}
	if !found {
		return ErrUnresolvedResource
	}
	if value != selector.Obligation {
		return ErrResourceConflict
	}
	token := activeEntryToken(*entry)
	if token == nil || token.metadata == nil || token.metadata.owner != op.allocation.owner {
		return ErrResourceOwnership
	}
	values := [1]StableLogicalObligation{value}
	op.token, err = token.cloneOwnedPinnedDirectory(entry.logicalLane, entry.resourceID, entry.diagnosticPath, entry.frontier, value.Reachability, values[:], nil, nil)
	if err != nil {
		return err
	}
	if err = op.prepareSingleEntry(); err != nil {
		return err
	}
	if work != nil {
		work.EntriesCloned++
	}
	return nil
}

// All metadata and output admission precedes physical ownership transfer. The
// private rope initially owns only its entry backing, with entries detached;
// cancellation can therefore dispose it without claiming the caller-owned
// token. This is the selected singleton construction, not a change to generic
// builder ownership. No published node or source header is mutated here.
func (op *selectedResourceSelectorOperation) prepareSingleEntry() error {
	owner := op.allocation.owner
	var err error
	op.entry, err = newOwnedResourceEntry(owner, op.token)
	if err != nil {
		return err
	}
	entry := &op.entry.entries[0]
	op.view.logical, err = insertOwnedResourceLogical(nil, entry, owner)
	if err != nil {
		return err
	}
	op.view.physical, err = insertOwnedResourcePhysical(nil, entry, owner)
	if err != nil {
		return err
	}
	op.view.fields, err = mergeResourceFieldBackings(owner, nil, entry.reachability)
	if err != nil {
		return err
	}
	_, added, err := mergeOwnedResourceMembership(&op.view.logicalMembership, entry.outgoing.index, owner)
	if err != nil {
		return err
	}
	op.view.logicalMembershipCount = added
	op.view.count, op.view.logicalObligationCount = 1, entry.logicalObligations.count
	op.view.directory = entry.outgoing.directory
	op.view.root, err = newOwnedStableResourceEntryLeaf(op.entry, 0, 1, owner)
	if err != nil {
		return err
	}
	op.view.root.entries = nil // Physical token remains owned by op until commit.
	cells := [1]resourceKindCell{{kind: op.token.kind, view: op.view}}
	op.kinds, err = newResourceKindBacking(owner, cells[:])
	if err != nil {
		return err
	}
	op.view = stableResourceKindView{} // Consumed by the SAME private kind root.
	op.result, err = newStableResourceSetFromKindViews(op.kinds)
	if err != nil {
		return err
	}
	op.kinds = nil // Consumed by the SAME private output wrapper.
	// The cloned token has never escaped this operation. Metadata constructors
	// borrow its descriptor; no callback can acquire or transfer its Token edge.
	// Every fallible allocation is complete, so this transfer cannot fail.
	if err := op.token.claim(ResourceOwnerShared); err != nil {
		panic("rootpublication: private selected token ownership changed")
	}
	op.result.kindViews.cells[0].view.root.entries = op.entry.entries[:1:1]
	op.token = nil
	return nil
}

// complete runs once, off-gate. Successful temporary destruction and alias
// detachment precede the operation's containing refund. On failure all pending
// operands, including a complete but untransferred result, stay original.
func (op *selectedResourceSelectorOperation) complete(cause error) (*StableResourceSet, error) {
	failed := func(cleanupErr error) (*StableResourceSet, error) {
		err := op.allocation.cleanupFailed(op, cleanupErr)
		if cause != nil {
			op.allocation.failure.TryLinkPriorCleanupError(cause)
		}
		return nil, err
	}
	if cause != nil && op.result != nil {
		if err := op.result.ReleaseWithError(); err != nil {
			return failed(err)
		}
		op.result = nil
	}
	if op.kinds != nil {
		if err := op.kinds.release(); err != nil {
			return failed(err)
		}
		op.kinds = nil
	}
	if err := op.releasePartialView(); err != nil {
		return failed(err)
	}
	op.view = stableResourceKindView{}
	if op.entry != nil {
		entry := op.entry
		op.entry = nil
		if err := entry.release(); err != nil {
			return failed(err)
		}
	}
	if op.token != nil {
		if err := op.token.ReleaseWithError(); err != nil {
			return failed(err)
		}
		op.token = nil
	}
	if err := op.borrow.release(); err != nil {
		return failed(err)
	}
	result, allocation := op.result, op.allocation
	op.result, op.allocation = nil, nil
	op = nil
	if allocation.drop() {
		allocation.refund()
	}
	return result, cause
}

// Detach each original ownership edge before its graph finalizer can refund
// that child. A failed consumed child returns its own admitted original receipt;
// complete links that receipt while preserving all remaining operation fields.
func (op *selectedResourceSelectorOperation) releasePartialView() error {
	logical := op.view.logical
	op.view.logical = nil
	if err := releaseOwnedResourceLogical(logical); err != nil {
		return err
	}
	physical := op.view.physical
	op.view.physical = nil
	if err := releaseOwnedResourcePhysical(physical); err != nil {
		return err
	}
	membership := op.view.logicalMembership
	op.view.logicalMembership = nil
	releaseOwnedResourceObligationIndex(membership)
	fields := op.view.fields
	op.view.fields = nil
	fields.release()
	root := op.view.root
	op.view.root, op.view.directory = nil, nil
	return root.release()
}
