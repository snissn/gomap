package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// validateNativePostActivationV6 is called before the original visibility
// transition. It excludes opaque/scanned/delta/vacuum/maintenance callbacks;
// ordinary completeOrderedPostActivation/finalizeCommitPostWork are unchanged.
func (install *rootPublicationVisibleInstallV1) validateNativePostActivationV6(w *iterator.OrdinalScanWork) (bool, error) {
	if install == nil || w == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if !w.Reserve(2, uint64(unsafe.Sizeof(*install))+uint64(unsafe.Sizeof(finalizeCommitPost{}))+uint64(unsafe.Sizeof(pruneWorker{}))) {
		return false, nil
	}
	db := install.db
	p := &install.post
	if db == nil || install.idx == nil || install.vlogRefCounts != nil || p.vlogRefDelta != nil || install.recordVacuumMutation != nil || install.commandWALPublish ||
		p.persistLeafGenerationManifest || p.persistLeafGenerationIndexesOnly || p.drainLeafGenerationPending || len(p.clearLeafGenerationPendingFileIDs) != 0 || len(p.persistLeafGenerationRawFileIDs) != 0 || p.persistLeafGenerationManifestView != nil || p.persistLeafGenerationStateView != nil || p.debug || db.adaptive != nil || db.pruner.Enabled() {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	empty, ready := install.idx.graveyard.EmptyWithWorkV6(w)
	if !ready {
		return false, nil
	}
	if !empty {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	return true, nil
}

// completeNativeOrderedPostActivationV6 must run in the SAME final no-yield
// builder invocation, after enqueue and outside coordinator.mu, before another
// writer can pass durablePublishMu. The exact fixed original operations are
// priced before they execute; no custom callback is admitted by a scalar.
func (install *rootPublicationVisibleInstallV1) completeNativeOrderedPostActivationV6(w *iterator.OrdinalScanWork) (bool, error) {
	if ready, err := install.validateNativePostActivationV6(w); !ready || err != nil {
		return false, err
	}
	// Admit the actual empty extraction before any ordered-post mutation.
	empty, ready := install.idx.graveyard.EmptyWithWorkV6(w)
	if !ready {
		return false, nil
	}
	if !empty {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(7, uint64(unsafe.Sizeof(*install))+uint64(unsafe.Sizeof(valueLogRefTracker{}))+uint64(unsafe.Sizeof(finalizeCommitPost{}))+2*uint64(unsafe.Sizeof(uint64(0)))+uint64(unsafe.Sizeof(pruneWorker{}))+2*uint64(unsafe.Sizeof(install.post.doPrune))+2*uint64(unsafe.Sizeof(install.post.kickPrune))) {
		return false, nil
	}
	// Original fixed branch: completion flag; optional tracker invalidate under
	// its mutex; advanced flag; nil reference evidence; disabled pruner flags;
	// conditional root marker; completion of the already empty prune branch.
	if err := install.completeOrderedPostActivation(); err != nil {
		return false, err
	}
	install.post.doPrune, install.post.kickPrune = false, false
	return true, nil
}
