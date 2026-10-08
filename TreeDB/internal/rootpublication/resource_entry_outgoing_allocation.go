package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// This backing owns the actual entry callback/cache cell and outgoing immutable
// summaries, independently of the physical token and rope. Its lifetime ends
// at the entry array's last metadata alias, never at physical Release.
type resourceEntryOutgoing struct {
	allocation    *resourceAllocation
	frontier      *resourceAllocation
	frontierValue DurableFrontier
	fields        *resourceFieldBacking
	obligations   *resourceObligationBacking
	index         *stableLogicalObligationIndexNode
	directory     *DependencyDirectoryV2
	cache         dependencyManifestEntryCacheV1
}

func (out *resourceEntryOutgoing) release() {
	if out == nil || !out.allocation.drop() {
		return
	}
	if out.frontier != nil {
		if out.frontierValue.exactRIDs != nil {
			clear(out.frontierValue.exactRIDs.values[:cap(out.frontierValue.exactRIDs.values)])
			out.frontierValue.exactRIDs.values = nil
		}
		out.frontierValue = DurableFrontier{}
		out.frontier.drop()
		out.frontier.refund()
		out.frontier = nil
	}
	releaseOwnedResourceObligationIndex(out.index)
	out.index = nil
	out.obligations.release()
	out.obligations = nil
	if out.directory != nil {
		out.directory.Release()
		out.directory = nil
	}
	out.fields.release()
	out.fields = nil
	out.cache.mu.Lock()
	value := out.cache.value
	out.cache.value = nil
	out.cache.mu.Unlock()
	value.releaseOwned()
	out.allocation.refund()
}

// Only a constructor-owned token from the same governing owner is accepted.
// The token descriptor and all outgoing summaries gain genuine independent
// allocation edges. Refusal leaves the token and its physical owner unchanged.
func newOwnedResourceEntry(owner *retainedalloc.Owner, token *StableResourceToken) (*resourceEntryBacking, error) {
	if owner == nil || token == nil || token.metadata == nil || token.metadata.owner != owner {
		return nil, ErrResourceOwnership
	}
	backing, err := newResourceEntryBacking(owner, 1)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*resourceEntryBacking, error) { backing.release(); return nil, e }
	entry := &backing.entries[0]
	if err = entry.bindOwnedToken(token); err != nil {
		return fail(err)
	}
	allocation, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceEntryOutgoing{}))))
	if err != nil {
		return fail(err)
	}
	out := &resourceEntryOutgoing{allocation: allocation}
	entry.outgoing = out
	if token.directory != nil {
		if token.directory.metadata != owner {
			return fail(ErrResourceOwnership)
		}
		if err = token.directory.Retain(); err != nil {
			return fail(err)
		}
		out.directory = token.directory
	}
	out.fields, err = newImportedEntryFieldBacking(owner, token)
	if err != nil {
		return fail(err)
	}
	if !token.obligationBacking.retain() {
		return fail(ErrResourceOwnership)
	}
	out.obligations = token.obligationBacking
	if out.obligations != nil {
		for i := range out.obligations.values {
			next, e := insertOwnedResourceObligationIndex(out.index, out.obligations, i, owner)
			if e != nil {
				return fail(e)
			}
			if next != out.index {
				releaseOwnedResourceObligationIndex(out.index)
				out.index = next
			}
		}
	}
	entry.logicalLane, entry.resourceID, entry.diagnosticPath = token.logicalLane, token.resourceID, token.diagnosticPath
	entry.reachability = out.fields
	entry.logicalObligations = stableLogicalObligationView{fieldSummary: out.fields, ownedValues: out.obligations, index: out.index, count: len(token.logicalObligations), directory: token.directory}
	entry.dependencyManifestV1 = &out.cache
	return backing, nil
}
