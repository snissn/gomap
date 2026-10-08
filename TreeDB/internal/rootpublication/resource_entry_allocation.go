package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// resourceEntryBacking owns one actual immutable entry-array allocation. The
// existing rope and each retained index edge independently retain this backing;
// releasing a physical token cannot authorize refund of an index alias.
// Outgoing entry metadata is governed separately by its constructor allocation.
type resourceEntryBacking struct {
	allocation *resourceAllocation
	entries    []stableResourceEntry
}

func newResourceEntryBacking(owner *retainedalloc.Owner, count int) (*resourceEntryBacking, error) {
	if owner == nil || count <= 0 {
		return nil, ErrResourceOwnership
	}
	width := uint64(unsafe.Sizeof(stableResourceEntry{}))
	if uint64(count) > ^uint64(0)/width {
		return nil, retainedalloc.ErrCapacity
	}
	wrapper := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceEntryBacking{})))
	array := retainedalloc.AllocationCharge(uint64(count) * width)
	if array > ^uint64(0)-wrapper {
		return nil, retainedalloc.ErrCapacity
	}
	allocation, err := newResourceAllocation(owner, wrapper+array)
	if err != nil {
		return nil, err
	}
	backing := &resourceEntryBacking{allocation: allocation, entries: make([]stableResourceEntry, count)}
	for i := range backing.entries {
		backing.entries[i].backing = backing
	}
	return backing, nil
}

func (entry *stableResourceEntry) retainBacking() bool {
	return entry == nil || entry.backing == nil || entry.backing.allocation.retain()
}
func (entry *stableResourceEntry) releaseBacking() {
	if entry != nil && entry.backing != nil {
		entry.backing.release()
	}
}
func (backing *resourceEntryBacking) release() {
	if backing == nil || !backing.allocation.drop() {
		return
	}
	// No remaining rope or metadata index may read these immutable entries.
	for i := range backing.entries {
		entry := &backing.entries[i]
		if entry.outgoing != nil {
			entry.outgoing.release()
			entry.outgoing = nil
		}
		if entry.tokenMetadataOwned {
			entry.token.releaseMetadata()
		}
	}
	clear(backing.entries)
	backing.entries = nil
	backing.allocation.refund()
}

// Bind keeps the token's immutable descriptor allocation independently of its
// physical rope pin. A refusal leaves both input operands with their callers.
func (entry *stableResourceEntry) bindOwnedToken(token *StableResourceToken) error {
	if entry == nil || entry.backing == nil || entry.backing.allocation == nil || token == nil || token.metadata == nil || token.metadata.owner != entry.backing.allocation.owner || entry.token != nil {
		return ErrResourceOwnership
	}
	if !token.retainMetadata() {
		return ErrResourceOwnership
	}
	entry.token = token
	entry.tokenMetadataOwned = true
	entry.frontier = token.frontier
	return nil
}
