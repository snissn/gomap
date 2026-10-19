package rootpublication

import (
	"bytes"
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
	directoryKey  []byte
	cache         dependencyManifestEntryCacheV1
}

func (out *resourceEntryOutgoing) release() {
	if out == nil {
		return
	}
	_ = releaseResourceMetadataV6(out.allocation, out)
}

// Only a constructor-owned token from the same governing owner is accepted.
// The token descriptor and all outgoing summaries gain genuine independent
// allocation edges. Refusal leaves the token and its physical owner unchanged.
func newOwnedResourceEntry(owner *retainedalloc.Owner, token *StableResourceToken) (*resourceEntryBacking, error) {
	return newOwnedResourceDirectoryEntry(owner, token, nil, nil, nil, 0)
}

// Directory summaries are private until the original builder claims the token.
// A bound entry retains the original immutable field cells and logical count;
// recovery constructs private cells before scanning its persisted obligations.
func newOwnedResourceDirectoryEntry(owner *retainedalloc.Owner, token *StableResourceToken, key []byte, fields []ReachabilityField, summary *resourceFieldBacking, count int) (*resourceEntryBacking, error) {
	if owner == nil || token == nil || token.metadata == nil || token.metadata.owner != owner || count < 0 || (summary != nil && (summary.allocation == nil || summary.allocation.owner != owner)) {
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
	allocation, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceEntryOutgoing{})))+retainedalloc.AllocationCharge(uint64(len(key))))
	if err != nil {
		return fail(err)
	}
	out := &resourceEntryOutgoing{allocation: allocation, directoryKey: bytes.Clone(key)}
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
	if summary != nil {
		if !summary.retain() {
			return fail(ErrResourceOwnership)
		}
		out.fields = summary
	} else if key != nil {
		out.fields, err = newDirectoryEntryFieldBacking(owner, fields)
	} else {
		out.fields, err = newImportedEntryFieldBacking(owner, token)
	}
	if err != nil {
		return fail(err)
	}
	if summary != nil {
		entry.logicalLane, entry.resourceID, entry.diagnosticPath = token.logicalLane, token.resourceID, token.diagnosticPath
		entry.reachability = out.fields
		entry.logicalObligations = stableLogicalObligationView{fieldSummary: out.fields, count: count, directory: out.directory, owner: out.directoryKey}
		entry.dependencyManifestV1 = &out.cache
		return backing, nil
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
	entry.logicalObligations = stableLogicalObligationView{fieldSummary: out.fields, ownedValues: out.obligations, index: out.index, count: len(token.logicalObligations), directory: token.directory, owner: out.directoryKey}
	entry.dependencyManifestV1 = &out.cache
	return backing, nil
}
