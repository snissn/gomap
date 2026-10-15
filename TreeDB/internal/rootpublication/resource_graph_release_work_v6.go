package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// resourceReleaseFrameV6 lives in the original admitted allocation. Only its
// actual last edge may install this intrusive traversal state. The root frame
// retains the current child and the child links its original ancestors, so a
// yield or first error retains the original graph without a disposal registry.
type resourceReleaseFrameV6 struct {
	value           any
	parent, current *resourceReleaseFrameV6
	allocation      *resourceAllocation
	owner           *retainedalloc.Owner
	charge          uint64
	err             error
	phase           uint8
	index, offset   int
	claimed         bool
	deferToParent   bool
	// running is guarded by the originating Set mutex. It claims the
	// existing release actor while external providers run without that mutex.
	running         bool
	directory       *DependencyDirectoryReleaseWork
	directoryActive bool
	scratchOwner    *retainedalloc.Owner
	scratchCharge   uint64
	refund          retainedalloc.RemovalWork
	refundOwner     *retainedalloc.Owner
	refundCharge    uint64
	failureCursor   *resourceReleaseFrameV6
}

type resourceGraphReleaseCompletionV6 struct {
	owner  *retainedalloc.Owner
	charge uint64
}

func (c *resourceGraphReleaseCompletionV6) complete() {
	owner, charge := c.owner, c.charge
	c.owner, c.charge = nil, 0
	if owner != nil {
		owner.RemovePending(charge)
	}
}

func resourceGraphReserveV6(w *iterator.OrdinalScanWork, r, b uint64) bool {
	return w == nil || w.Reserve(r, b)
}

// startResourceGraphReleaseV6 consumes exactly one existing allocation edge.
// A nonlast edge finishes immediately. The root frame itself is the retained
// continuation for a last edge; callers must keep that original operand.
func startResourceGraphReleaseV6(a *resourceAllocation, value any, w *iterator.OrdinalScanWork) (*resourceReleaseFrameV6, bool, error) {
	if a == nil {
		return nil, true, nil
	}
	if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*a))) {
		return nil, false, nil
	}
	if err := a.cleanupResult(); err != nil {
		return nil, false, err
	}
	if !a.drop() {
		return nil, true, nil
	}
	f := &a.release
	*f = resourceReleaseFrameV6{value: value, allocation: a}
	f.current = f
	return f, true, nil
}

// child transfers this existing child edge into the same original last-owner
// traversal. The parent clears its child alias only after this admission. A
// last child remains reachable through root.current until all its stages end.
func (root *resourceReleaseFrameV6) child(a *resourceAllocation, value any, w *iterator.OrdinalScanWork) (bool, error) {
	if a == nil {
		return true, nil
	}
	f, ready, err := startResourceGraphReleaseV6(a, value, w)
	if !ready || err != nil {
		return false, err
	}
	if f != nil {
		f.parent = root.current
		root.current = f
	}
	return true, nil
}

func (root *resourceReleaseFrameV6) continueWork(w *iterator.OrdinalScanWork) (resourceGraphReleaseCompletionV6, bool, error) {
	var empty resourceGraphReleaseCompletionV6
	if !resourceGraphReserveV6(w, 1, uint64(unsafe.Sizeof(*root))) {
		return empty, false, nil
	}
	fail := func(err error) (resourceGraphReleaseCompletionV6, bool, error) {
		if root.err == nil {
			root.err, root.failureCursor = err, root.current
		}
		return root.propagateFailure(w)
	}
	if root.err != nil {
		return root.propagateFailure(w)
	}
	for root.current != nil {
		if w != nil && root.directoryActive {
			consumed, ready, err := root.directory.continueWork(w)
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
			if !consumed {
				return fail(ErrResourceOwnership)
			}
			ready, err = root.directory.resetCompletedWork(w)
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
			root.directoryActive = false
		}
		f := root.current
		if w != nil {
			if ready, err := f.refund.CompletePendingWork(&f.refundOwner, &f.refundCharge, w); !ready || err != nil {
				return empty, false, err
			}
		}
		if f.allocation != nil {
			if err := f.allocation.cleanupResult(); err != nil {
				return fail(err)
			}
		}
		// A previous call can finish the physical stage but refuse its final
		// frame-detachment admission. Resume that detachment, not a nil value.
		if f.value != nil {
			ready, err := root.stepMetadata(f, w)
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
		}
		// A child was entered, or this frame still has another original stage.
		if root.current != f || f.value != nil {
			continue
		}
		a := f.allocation
		parent := f.parent
		if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*f))) {
			return empty, false, nil
		}
		owner, charge := f.owner, f.charge
		if a != nil {
			owner, charge = a.owner, a.charge
		}
		// Validate the exact receiving owner before detaching any source.
		if parent == nil && root.scratchOwner != nil && root.scratchOwner != owner {
			return fail(ErrResourceOwnership)
		}
		if parent != nil && f.deferToParent && owner != nil && parent.owner != owner {
			return fail(ErrResourceOwnership)
		}
		deferToParent := f.deferToParent
		if a != nil {
			owner, charge = a.owner, a.charge
			a.owner, a.charge = nil, 0
		}
		f.owner, f.charge = nil, 0
		f.parent, f.allocation = nil, nil
		root.current = parent
		if parent == nil {
			if root.scratchOwner != nil {
				scratchOwner, scratchCharge := root.scratchOwner, root.scratchCharge
				root.directory, root.directoryActive = nil, false
				root.scratchOwner, root.scratchCharge = nil, 0
				if scratchOwner != owner {
					return fail(ErrResourceOwnership)
				}
				charge += scratchCharge
			}
			return resourceGraphReleaseCompletionV6{owner, charge}, true, nil
		}
		// The parent alias was detached when this child was entered. Only the
		// original intrusive frame retained it, and that alias has now ended too.
		if owner != nil {
			if deferToParent {
				if parent.owner != owner {
					return fail(ErrResourceOwnership)
				}
				parent.charge += charge
			} else {
				if w == nil {
					owner.RemovePending(charge)
				} else {
					parent.refundOwner, parent.refundCharge = owner, charge
					f, a = nil, nil
					if ready, err := parent.refund.CompletePendingWork(&parent.refundOwner, &parent.refundCharge, w); !ready || err != nil {
						return empty, false, err
					}
				}
			}
		}
	}
	return empty, true, nil
}

// A failure retains the original ancestor links. Each actual ancestor and
// first-error write is admitted before inspection; no unbounded error walk is
// hidden in a return from the physical provider.
func (root *resourceReleaseFrameV6) propagateFailure(w *iterator.OrdinalScanWork) (resourceGraphReleaseCompletionV6, bool, error) {
	for root.failureCursor != nil {
		if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(resourceReleaseFrameV6{}))+2*uint64(unsafe.Sizeof(resourceAllocation{}))+2*uint64(unsafe.Sizeof(retainedalloc.Owner{}))+2*uint64(unsafe.Sizeof(StableResourceSet{}))+2*uint64(unsafe.Sizeof(StableResourceToken{}))+2*uint64(unsafe.Sizeof(resourcePinnedBacking{}))+2*uint64(unsafe.Sizeof(IdentityPinRegistry{}))) {
			return resourceGraphReleaseCompletionV6{}, false, nil
		}
		f, err := root.failureCursor, root.err
		if f.allocation != nil {
			f.allocation.cleanupFailed(f.value, err)
		}
		if f.owner != nil {
			f.owner.CleanupFailed()
		}
		switch n := f.value.(type) {
		case *StableResourceSet:
			n.cleanupFailure.original, n.cleanupFailure.causes[0] = n, err
			n.cleanupDone.Store(true)
		case *stableResourceEntryNode:
			n.cleanupFailure.original, n.cleanupFailure.causes[0] = n, err
			n.cleanupDone.Store(true)
		case *StableResourceToken:
			n.cleanupFailure.original, n.cleanupFailure.causes[0] = n, err
			n.cleanupDone.Store(true)
		case *resourcePinnedBacking:
			n.failure = err
			if n.registry != nil {
				n.registry.retainFailedPinnedBacking(n)
			}
		}
		root.failureCursor = f.parent
	}
	return resourceGraphReleaseCompletionV6{}, false, root.err
}

func (root *resourceReleaseFrameV6) stepMetadata(f *resourceReleaseFrameV6, w *iterator.OrdinalScanWork) (bool, error) {
	if f.value == nil {
		return true, nil
	}
	// Each stage admits its frame/parent writes independently of the child
	// decrement. A child refusal leaves the stage and original alias unchanged.
	if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*f))) {
		return false, nil
	}
	switch n := f.value.(type) {
	case *StableResourceSet:
		return root.stepSet(f, n, w)
	case *stableResourceLogicalIndexNode:
		switch f.phase {
		case 0:
			if n.left != nil {
				if ok, e := root.child(n.left.allocation, n.left, w); !ok || e != nil {
					return false, e
				}
				n.left = nil
			}
			f.phase = 1
		case 1:
			if n.right != nil {
				if ok, e := root.child(n.right.allocation, n.right, w); !ok || e != nil {
					return false, e
				}
				n.right = nil
			}
			f.phase = 2
		case 2:
			if n.entry != nil && n.entry.backing != nil {
				if ok, e := root.child(n.entry.backing.allocation, n.entry.backing, w); !ok || e != nil {
					return false, e
				}
			}
			n.entry = nil
			f.phase = 3
		default:
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*n))) {
				return false, nil
			}
			n.key = stableLogicalResourceKey{}
			f.value = nil
		}
	case *stableResourcePhysicalIndexNode:
		switch f.phase {
		case 0:
			if f.index < len(n.entries) {
				e := n.entries[f.index]
				if e != nil && e.backing != nil {
					if ok, err := root.child(e.backing.allocation, e.backing, w); !ok || err != nil {
						return false, err
					}
				}
				n.entries[f.index] = nil
				f.index++
			} else {
				n.entries = nil
				f.phase = 1
			}
		case 1:
			if n.left != nil {
				if ok, e := root.child(n.left.allocation, n.left, w); !ok || e != nil {
					return false, e
				}
				n.left = nil
			}
			f.phase = 2
		case 2:
			if n.right != nil {
				if ok, e := root.child(n.right.allocation, n.right, w); !ok || e != nil {
					return false, e
				}
				n.right = nil
			}
			f.phase = 3
		default:
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*n))) {
				return false, nil
			}
			n.key = stablePhysicalIdentityKey{}
			f.value = nil
		}
	case *stableLogicalObligationIndexNode:
		switch f.phase {
		case 0:
			if n.left != nil {
				if ok, e := root.child(n.left.allocation, n.left, w); !ok || e != nil {
					return false, e
				}
				n.left = nil
			}
			f.phase = 1
		case 1:
			if n.right != nil {
				if ok, e := root.child(n.right.allocation, n.right, w); !ok || e != nil {
					return false, e
				}
				n.right = nil
			}
			f.phase = 2
		case 2:
			if n.backing != nil {
				if ok, e := root.child(n.backing.allocation, n.backing, w); !ok || e != nil {
					return false, e
				}
				n.backing = nil
			}
			f.phase = 3
		default:
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*n))) {
				return false, nil
			}
			n.key = stableLogicalObligationIndex{}
			n.obligation = StableLogicalObligation{}
			f.value = nil
		}
	case *resourceKindBacking:
		if f.index == len(n.cells) {
			n.cells = nil
			f.value = nil
			break
		}
		cell := &n.cells[f.index]
		switch f.phase {
		case 0:
			if cell.view.logical != nil {
				if ok, e := root.child(cell.view.logical.allocation, cell.view.logical, w); !ok || e != nil {
					return false, e
				}
				cell.view.logical = nil
			}
			f.phase = 1
		case 1:
			if cell.view.physical != nil {
				if ok, e := root.child(cell.view.physical.allocation, cell.view.physical, w); !ok || e != nil {
					return false, e
				}
				cell.view.physical = nil
			}
			f.phase = 2
		case 2:
			if cell.view.logicalMembership != nil {
				if ok, e := root.child(cell.view.logicalMembership.allocation, cell.view.logicalMembership, w); !ok || e != nil {
					return false, e
				}
				cell.view.logicalMembership = nil
			}
			f.phase = 3
		case 3:
			if cell.view.fields != nil {
				if ok, e := root.child(cell.view.fields.allocation, cell.view.fields, w); !ok || e != nil {
					return false, e
				}
				cell.view.fields = nil
			}
			f.phase = 4
		case 4:
			if cell.view.root != nil {
				if ok, e := root.rope(cell.view.root, w); !ok || e != nil {
					return false, e
				}
				cell.view.root = nil
			}
			f.phase = 5
		case 5:
			if cell.directoryOwned {
				if ok, e := root.releaseDirectory(cell.view.directory, w); !ok || e != nil {
					return false, e
				}
				cell.directoryOwned = false
				cell.view.directory = nil
			}
			f.phase = 6
		default:
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*cell))) {
				return false, nil
			}
			*cell = resourceKindCell{}
			f.index++
			f.phase = 0
		}
	case *stableResourceEntryNode:
		switch f.phase {
		case 0:
			if n.left != nil {
				if ok, e := root.rope(n.left, w); !ok || e != nil {
					return false, e
				}
				n.left = nil
			}
			f.phase = 1
		case 1:
			if n.right != nil {
				if ok, e := root.rope(n.right, w); !ok || e != nil {
					return false, e
				}
				n.right = nil
			}
			f.phase = 2
		case 2:
			if f.index < len(n.entries) {
				if ok, e := root.token(n.entries[f.index].token, ResourceOwnerShared, w); !ok || e != nil {
					return false, e
				}
				f.index++
			} else {
				f.phase = 3
			}
		case 3:
			if n.metadata.backing != nil {
				backing := n.metadata.backing
				if ok, e := root.child(backing.allocation, backing, w); !ok || e != nil {
					return false, e
				}
				n.metadata.backing = nil
				n.entries = nil
			}
			f.phase = 4
		default:
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*n))+2*uint64(unsafe.Sizeof(*n.metadata))) {
				return false, nil
			}
			n.left, n.right, n.entries = nil, nil, nil
			n.metadata.owner, n.metadata.charge = nil, 0
			n.metadata = nil
			n.cleanupDone.Store(true)
			f.value = nil
		}
	case *resourcePinnedBacking:
		switch f.phase {
		case 0:
			if n.sourceRoot != nil {
				if ok, e := root.rope(n.sourceRoot, w); !ok || e != nil {
					return false, e
				}
				n.sourceRoot = nil
				n.file = nil
			}
			f.phase = 1
		default:
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*n))+128) {
				return false, nil
			}
			if n.file != nil {
				if e := n.file.Close(); e != nil {
					return false, e
				}
			}
			n.file, n.owner, n.registry, n.charge = nil, nil, nil, 0
			f.value = nil
		}
	case *StableResourceToken:
		return root.stepToken(f, n, w)
	case *resourceTokenMetadataReleaseV6:
		t := n.token
		switch f.phase {
		case 0:
			if t.ownedOperations != nil {
				if w == nil {
					consumed, e := t.ownedOperations.Release()
					if consumed {
						t.ownedOperations = nil
					}
					if e != nil {
						return false, e
					}
					if !consumed {
						return false, ErrResourceOwnership
					}
				} else {
					ops, ok := t.ownedOperations.(StableResourceOwnedOperationsWithWorkV6)
					if !ok {
						return false, retainedalloc.ErrWorkRequired
					}
					owner, charge, ready, e := ops.ReleaseWithWorkV6(&t.releaseWork.operationsConsumed, w)
					if !ready || e != nil {
						return false, e
					}
					t.ownedOperations = nil
					if owner != nil {
						f.refundOwner, f.refundCharge = owner, charge
					}
				}
			}
			f.phase = 1
		case 1:
			if t.obligationBacking != nil {
				b := t.obligationBacking
				if ok, e := root.child(b.allocation, b, w); !ok || e != nil {
					return false, e
				}
				t.obligationBacking = nil
			}
			f.phase = 2
		default:
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*t))) {
				return false, nil
			}
			t.importedSource, t.importedFields = nil, nil
			t.kind, t.logicalLane, t.resourceID, t.diagnosticPath, t.reachability = "", "", "", "", ""
			t.identity = StableIdentity{}
			t.frontier, t.syncedFrontier = DurableFrontier{}, DurableFrontier{}
			t.logicalObligations = nil
			t.flush, t.sync, t.onRelease, t.identityPin, t.releaseObservation = nil, nil, nil, nil, nil
			t.metadata = nil
			n.token = nil
			f.value = nil
		}
	case *resourceEntryBacking:
		if f.index == len(n.entries) {
			n.entries = nil
			f.value = nil
			break
		}
		entry := &n.entries[f.index]
		switch f.phase {
		case 0:
			if entry.outgoing != nil {
				if ok, e := root.child(entry.outgoing.allocation, entry.outgoing, w); !ok || e != nil {
					return false, e
				}
				entry.outgoing = nil
			}
			f.phase = 1
		case 1:
			if entry.tokenMetadataOwned {
				if ok, e := root.tokenMetadata(entry.token, w); !ok || e != nil {
					return false, e
				}
				entry.tokenMetadataOwned = false
				entry.token = nil
			}
			f.phase = 2
		default:
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*entry))) {
				return false, nil
			}
			*entry = stableResourceEntry{}
			f.index++
			f.phase = 0
		}
	case *resourceEntryOutgoing:
		switch f.phase {
		case 0:
			if n.frontier != nil {
				if !f.claimed {
					if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*n.frontier))) {
						return false, nil
					}
					if !n.frontier.drop() {
						n.frontier = nil
						n.frontierValue = DurableFrontier{}
						f.phase = 1
						break
					}
					f.claimed = true
				}
				if r := n.frontierValue.exactRIDs; r != nil && f.offset < cap(r.values) {
					if !resourceGraphReserveV6(w, 1, 16) {
						return false, nil
					}
					r.values[:cap(r.values)][f.offset] = 0
					f.offset++
					break
				}
				a := n.frontier
				n.frontier = nil
				n.frontierValue = DurableFrontier{}
				if w == nil {
					a.refund()
				} else {
					f.refundOwner, f.refundCharge = a.owner, a.charge
					a.owner, a.charge = nil, 0
				}
			}
			f.claimed = false
			f.offset = 0
			f.phase = 1
		case 1:
			if n.index != nil {
				if ok, e := root.child(n.index.allocation, n.index, w); !ok || e != nil {
					return false, e
				}
				n.index = nil
			}
			f.phase = 2
		case 2:
			if n.obligations != nil {
				if ok, e := root.child(n.obligations.allocation, n.obligations, w); !ok || e != nil {
					return false, e
				}
				n.obligations = nil
			}
			f.phase = 3
		case 3:
			if n.directory != nil {
				if ok, e := root.releaseDirectory(n.directory, w); !ok || e != nil {
					return false, e
				}
				n.directory = nil
			}
			f.phase = 4
		case 4:
			if n.fields != nil {
				if ok, e := root.child(n.fields.allocation, n.fields, w); !ok || e != nil {
					return false, e
				}
				n.fields = nil
			}
			f.phase = 5
		case 5:
			n.cache.mu.Lock()
			value := n.cache.value
			if value != nil {
				ok, e := root.child(value.allocation, value, w)
				if !ok || e != nil {
					n.cache.mu.Unlock()
					return false, e
				}
				n.cache.value = nil
			}
			n.cache.mu.Unlock()
			f.phase = 6
		default:
			f.value = nil
		}
	case *DependencyManifestV1:
		switch f.phase {
		case 0:
			if f.index < len(n.entries) {
				entry := n.entries[f.index]
				if entry != nil && entry.allocation != nil {
					if ok, err := root.child(entry.allocation, entry, w); !ok || err != nil {
						return false, err
					}
				}
				n.entries[f.index] = nil
				f.index++
			} else {
				f.index = 0
				f.phase = 1
			}
		case 1:
			if f.index < cap(n.entries) {
				if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof((*dependencyManifestEncodedEntryV1)(nil)))) {
					return false, nil
				}
				n.entries[:cap(n.entries)][f.index] = nil
				f.index++
			} else {
				n.entries = nil
				f.index = 0
				f.phase = 2
			}
		case 2:
			if f.index < cap(n.loadedEntries) {
				if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(dependencyManifestEncodedEntryV1{}))) {
					return false, nil
				}
				n.loadedEntries[:cap(n.loadedEntries)][f.index] = dependencyManifestEncodedEntryV1{}
				f.index++
			} else {
				n.loadedEntries = nil
				f.offset = 0
				f.phase = 3
			}
		case 3:
			if f.offset < cap(n.payload) {
				end := min(cap(n.payload), f.offset+4096)
				if !resourceGraphReserveV6(w, 1, uint64(end-f.offset)) {
					return false, nil
				}
				clear(n.payload[:cap(n.payload)][f.offset:end])
				f.offset = end
			} else {
				n.payload = nil
				n.byteLength = 0
				n.digest = [32]byte{}
				f.value = nil
			}
		}
	case *dependencyManifestEncodedEntryV1:
		switch f.phase {
		case 0:
			if f.offset < len(n.encoded) {
				end := min(len(n.encoded), f.offset+4096)
				if !resourceGraphReserveV6(w, 1, uint64(end-f.offset)) {
					return false, nil
				}
				clear(n.encoded[f.offset:end])
				f.offset = end
			} else {
				n.encoded = nil
				f.offset = 0
				f.phase = 1
			}
		case 1:
			if f.offset < len(n.entry.Reachability) {
				if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(ReachabilityField("")))) {
					return false, nil
				}
				n.entry.Reachability[f.offset] = ""
				f.offset++
			} else {
				n.entry.Reachability = nil
				f.offset = 0
				f.phase = 2
			}
		case 2:
			if f.offset < len(n.entry.LogicalObligations) {
				if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(StableLogicalObligation{}))) {
					return false, nil
				}
				n.entry.LogicalObligations[f.offset] = StableLogicalObligation{}
				f.offset++
			} else {
				n.entry.LogicalObligations = nil
				f.offset = 0
				f.phase = 3
			}
		case 3:
			if r := n.entry.Frontier.exactRIDs; r != nil && f.offset < len(r.values) {
				if !resourceGraphReserveV6(w, 1, 16) {
					return false, nil
				}
				r.values[f.offset] = 0
				f.offset++
			} else {
				n.entry = DependencyManifestEntryV1{}
				n.cacheCandidate = nil
				f.value = nil
			}
		}
	case *resourceObligationBacking:
		values := n.values[:cap(n.values)]
		if f.index < len(values) {
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(StableLogicalObligation{}))) {
				return false, nil
			}
			values[f.index] = StableLogicalObligation{}
			f.index++
		} else {
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*n))) {
				return false, nil
			}
			n.values = nil
			f.value = nil
		}
	case *resourceFieldBacking:
		cells := n.cells[:cap(n.cells)]
		if f.index < len(cells) {
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(resourceFieldCell{}))) {
				return false, nil
			}
			cells[f.index] = resourceFieldCell{}
			f.index++
		} else {
			if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*n))) {
				return false, nil
			}
			n.cells = nil
			f.value = nil
		}
	default:
		return false, retainedalloc.ErrWorkRequired
	}
	return true, nil
}

// Ordinary synchronous producers drain precisely the same metadata stages.
// They retain their historical error-return API; bounded parents keep the
// original frame between calls and detach their containing alias before refund.
func releaseResourceMetadataV6(a *resourceAllocation, value any) error {
	root, ready, err := startResourceGraphReleaseV6(a, value, nil)
	if err != nil || !ready {
		return err
	}
	if root == nil {
		return nil
	}
	completion, _, err := root.continueWork(nil)
	if err != nil {
		return err
	}
	completion.complete()
	return nil
}

// releaseDirectory uses the exact existing provider/last-edge protocol. One
// admitted scratch continuation is reused by this serial original traversal.
func (root *resourceReleaseFrameV6) releaseDirectory(d *DependencyDirectoryV2, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		consumed, err := d.ReleaseOutcome()
		if !consumed && err == nil {
			err = ErrResourceOwnership
		}
		return consumed, err
	}
	if root.directory == nil {
		owner := root.owner
		if root.allocation != nil {
			owner = root.allocation.owner
		}
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(DependencyDirectoryReleaseWork{})))
		if !w.Reserve(1, charge) {
			return false, nil
		}
		if ready, err := retainedalloc.AddPendingWithOrdinalWork(owner, charge, w); !ready || err != nil {
			return false, err
		}
		root.directory = &DependencyDirectoryReleaseWork{}
		root.scratchOwner, root.scratchCharge = owner, charge
	}
	if root.directoryActive {
		return false, ErrResourceOwnership
	}
	ready, err := root.directory.bind(d, w)
	if !ready || err != nil {
		return false, err
	}
	// The caller now clears the original outgoing directory alias. Drive its
	// original receipt only after that caller returned to the graph loop.
	root.directoryActive = true
	return true, nil
}

type resourceTokenMetadataReleaseV6 struct{ token *StableResourceToken }

func (root *resourceReleaseFrameV6) tokenMetadata(t *StableResourceToken, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || t.metadata == nil {
		return true, nil
	}
	if w != nil && t.ownedOperations != nil {
		ops, ok := t.ownedOperations.(StableResourceOwnedOperationsWithWorkV6)
		if !ok {
			return false, retainedalloc.ErrWorkRequired
		}
		ready, err := ops.SupportsReleaseWithWorkV6(w)
		if !ready || err != nil {
			return false, err
		}
	}
	f, ready, err := startResourceGraphReleaseV6(t.metadata, &t.releaseWork.metadata, w)
	if !ready || err != nil {
		return false, err
	}
	if f != nil {
		t.releaseWork.metadata.token = t
		f.parent = root.current
		if root.current != nil {
			if parent, ok := root.current.value.(*StableResourceToken); ok && parent == t {
				f.deferToParent = true
			}
		}
		root.current = f
	}
	return true, nil
}
