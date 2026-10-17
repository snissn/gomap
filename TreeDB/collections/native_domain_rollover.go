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
	source, err := newNativePendingSource(base.pending)
	if err != nil {
		return nil, err
	}
	list, err := newNativePendingSources(source, nil, admit)
	if err != nil {
		done := source.release()
		source = nil
		done.Complete()
		return nil, err
	}
	source = nil // original physical edge adopted by position
	base.captured.retain()
	list.older = base.captured
	domain, err := newNativeDomainState(base.name, base.baseRoot, base.interpretation, base.storagePolicy, nil, admit)
	if err != nil {
		done := list.release()
		list = nil
		done.Complete()
		return nil, err
	}
	domain.captured = list
	return domain, nil
}
