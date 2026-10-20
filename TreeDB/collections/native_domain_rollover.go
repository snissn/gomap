package collections

import (
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
)

// prepareNativeDomainRollover returns an owned immutable physical successor.
// It preserves every logical effect and identity. A prepared old-basis writer
// may finish privately, but the whole-engine bridge rejects that physical basis
// before assignment. No shared generation is frozen in place.
func prepareNativeDomainRollover(base *nativeDomainState, admit cowbtree.OwnedMapAdmit) (*nativeDomainState, error) {
	if base.pending == nil || base.pending.Len() == 0 {
		(nativeDomainLifetime{}).Retain(base)
		return base, nil
	}
	domain, err := newNativeDomainState(base.name, base.baseRoot, base.interpretation, base.storagePolicy, nil, admit)
	if err != nil {
		return nil, err
	}
	base.typed.retain()
	domain.typed = base.typed
	// This preborn descriptor owns every partial edge before the next fallible
	// stage. On error the caller transfers it into its original successor Work;
	// only the existing off-gate worker may release it.
	domain.privateSource, err = newNativePendingSource(base.pending)
	if err != nil {
		return domain, err
	}
	domain.captured, err = newNativePendingSources(domain.privateSource, nil, admit)
	if err != nil {
		return domain, err
	}
	domain.privateSource = nil // original physical edge adopted by list position
	base.captured.retain()
	domain.captured.older = base.captured
	return domain, nil
}
