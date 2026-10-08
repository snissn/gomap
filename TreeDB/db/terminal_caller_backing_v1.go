package db

import (
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// terminalCallerBackingCensusV1 enumerates the distinct retained control
// classes and exact scratch capacities introduced by checked caller integration.
// This is a numeric inventory, never admission, a reserve or ownership proof.
// Ordinary constructors currently allocate these controls. Finite construction
// remains closed until its actual caller prepays each instance and all closures.
func terminalCallerBackingCensusV1(members, seals, candidates, roles, tokens uint64) (rootpublication.BackingCensus, error) {
	var census rootpublication.BackingCensus
	kinds := [5]struct{ count, size uint64 }{
		{members, uint64(unsafe.Sizeof(rootPublicationVisibleMemberV1{}))},
		{seals, uint64(unsafe.Sizeof(rootPublicationSealV1{}))},
		{candidates, uint64(unsafe.Sizeof(durableRootPublishCandidateV1{}))},
		{roles, uint64(unsafe.Sizeof(rootpublication.StableTerminalOwnedSet{}))},
		{tokens, uint64(unsafe.Sizeof((*rootpublication.StableResourceToken)(nil)))},
	}
	for i, item := range kinds {
		if item.count == 0 {
			continue
		}
		if item.size > ^uint64(0)/item.count {
			return census, rootpublication.ErrStableMetadataShapeUnsupported
		}
		raw := item.size
		births := item.count
		if i >= 3 {
			raw *= item.count
			births = 1
		}
		class, err := rootpublication.StableBackingClassBytes(raw, true)
		if err != nil || class > ^uint64(0)/births || class*births > ^uint64(0)-census.AllocatedClassBytes {
			return rootpublication.BackingCensus{}, rootpublication.ErrStableMetadataShapeUnsupported
		}
		census.AllocatedClassBytes += class * births
		census.Allocations += births
	}
	census.LiveClassBytes = census.AllocatedClassBytes
	census.LiveAllocations = census.Allocations
	return census, nil
}
