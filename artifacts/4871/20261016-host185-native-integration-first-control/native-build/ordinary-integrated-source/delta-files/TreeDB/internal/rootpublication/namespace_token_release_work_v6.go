package rootpublication

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// ReleaseWithWorkV6 uses the original scoped namespace descriptor and caller's
// admitted phase storage. Its caller clears the namespace alias before refund.
func (ns *StableNamespaceToken) ReleaseWithWorkV6(phase *uint8, w *iterator.OrdinalScanWork) (StableResourceTokenReleaseCompletionV6, bool, error) {
	var empty StableResourceTokenReleaseCompletionV6
	if ns == nil {
		return empty, true, nil
	}
	if phase == nil || w == nil {
		return empty, false, retainedalloc.ErrWorkRequired
	}
	ns.mu.Lock()
	defer ns.mu.Unlock()
	fail := func(err error) (StableResourceTokenReleaseCompletionV6, bool, error) {
		ns.cleanupFailure.add(err)
		ns.cleanupErr = &ns.cleanupFailure
		if ns.metadata != nil {
			ns.metadata.CleanupFailed()
		}
		if ns.cleanupRegistry != nil {
			ns.cleanupRegistry.retainFailedNamespace(ns)
		}
		return empty, false, err
	}
	if *phase == 0 {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*ns))) {
			return empty, false, nil
		}
		if ns.metadata == nil || ns.batch != nil || ns.originalCleanup != nil || ns.released.Load() || ns.refs.Load() <= 0 {
			return empty, false, retainedalloc.ErrWorkRequired
		}
		if ns.refs.Add(-1) > 0 {
			*phase = 3
			return empty, true, nil
		}
		ns.released.Store(true)
		*phase = 1
	}
	if *phase == 1 {
		if ns.persistence != nil && ns.persistence != ns.parent {
			if !w.Reserve(1, 128) {
				return empty, false, nil
			}
			if err := ns.persistence.Close(); err != nil {
				return fail(err)
			}
			ns.persistence = nil
		}
		*phase = 2
	}
	if *phase == 2 {
		if ns.parent != nil {
			if !w.Reserve(1, 128) {
				return empty, false, nil
			}
			parent := ns.parent
			if err := parent.Close(); err != nil {
				return fail(err)
			}
			ns.parent = nil
			if ns.persistence == parent {
				ns.persistence = nil
			}
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*ns))) {
			return empty, false, nil
		}
		completion := StableResourceTokenReleaseCompletionV6{owner: ns.metadata, charge: ns.metadataCharge}
		ns.metadata, ns.metadataCharge = nil, 0
		ns.oldName, ns.newName, ns.diagnosticPath = "", "", ""
		ns.adapter, ns.cleanupRegistry = nil, nil
		*phase = 3
		return completion, true, nil
	}
	return empty, true, nil
}
