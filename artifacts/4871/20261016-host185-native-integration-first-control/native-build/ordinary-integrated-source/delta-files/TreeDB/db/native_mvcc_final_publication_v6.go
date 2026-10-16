package db

import (
	"context"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// FenceNativeMVCCPreparedV6 is the single original nonyielding publication
// invocation. A true return means fence+Consume+turn release, NEVER physical
// Finish or a prune ACK. Every pre-effect false return retains private custody;
// genuine I/O failure keeps the original linked recovery-required receipt.
// The enclosing cursor still owns all callback frames through real Finish.
func (c *NativeMVCCCurrentDeleteV6) FenceNativeMVCCPreparedV6(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	if !w.Reserve(6, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(nativeSealInstallationV6{}))+2*uint64(unsafe.Sizeof(mvccadmission.Cut{}))+2*uint64(unsafe.Sizeof(rootpublication.NativePublicationCompletionPlanV6{}))+2*uint64(unsafe.Sizeof(rootpublication.NativeBuilderReleasePlanV6{}))+2*uint64(unsafe.Sizeof(rootpublication.PublishResult{}))+64) {
		return false, nil
	}
	if c.phase >= 7 && c.phase <= 11 {
		return true, nil
	}
	if c.phase != 4 || c.db == nil || !c.publicationBound || c.publicationCoordinator == nil || c.transaction == nil || c.candidatePreparation.candidate == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if err, ready := c.NativeMVCCPublicationRecoveryErrorV6(w); !ready || err != nil {
		return false, err
	}
	db, coordinator := c.db, c.publicationCoordinator
	db.writeMu.Lock()
	db.durablePublishMu.Lock()
	db.rootReuseMu.Lock()
	buildLocked := true
	unlockBuild := func() {
		if buildLocked {
			buildLocked = false
			db.rootReuseMu.Unlock()
			db.durablePublishMu.Unlock()
			db.writeMu.Unlock()
		}
	}
	defer unlockBuild()
	installation, ready, err := c.prepareNativeSealInstallationV6(w)
	if !ready || err != nil {
		return false, err
	}
	var cut mvccadmission.Cut
	defer cut.Abort()
	if ready, err := c.observeNativePublicationCutV6(&cut, w); !ready || err != nil {
		return false, err
	}
	remainder, ready, err := c.nativePublicationRemainderWithWorkV6(w)
	if !ready || err != nil {
		return false, err
	}
	candidate := c.candidatePreparation.candidate
	completion, ready, err := coordinator.AdmitNativePublicationCompletionV6(&c.publication, candidate, w)
	if !ready || err != nil {
		return false, err
	}
	builderRelease, ready, err := coordinator.AdmitNativeBuilderReleaseV6(&c.publication, w)
	if !ready || err != nil {
		return false, err
	}
	floor, ready := remainder.withhold(w)
	if !ready {
		return false, nil
	}
	defer floor.restore()
	if ready, err := coordinator.AcquireNativeBuilderWithWorkV6(&c.publication, w); !ready || err != nil {
		return false, err
	}
	// The SAME stack Cut reaches the accepted nonfallible visibility hook.
	// Any failed enqueue ends this borrow before releasing the original builder.
	c.candidatePreparation.member.install.conditionalMutation.nativeCut = &cut
	ready, err = coordinator.EnqueueNativeBuiltWithWorkV6(&c.publication, candidate, w)
	floor.restore()
	if !ready || err != nil {
		c.candidatePreparation.member.install.conditionalMutation.nativeCut = nil
		cut.Abort()
		if released, releaseErr := builderRelease.Release(); !released || releaseErr != nil {
			return false, releaseErr
		}
		return false, err
	}
	// Enqueue atomically activated, consumed its builder and started the SAME
	// attempt. No private Abort/Cancel is legal from here onward. The floor
	// covers all following actual work on the original invocation grant.
	installation.commit()
	if ready, err := c.candidatePreparation.member.install.completeNativeOrderedPostActivationV6(w); !ready || err != nil {
		return false, err
	}
	unlockBuild() // no DB build/coordination lock survives dependency/index I/O
	result := db.rootPublication.publishNativeAuthorityWithWorkV6(context.Background(), candidate, &c.authorityScope, w)
	hardIO := c.authorityScope.hardIO
	c.authorityScope.hardIO = false
	if result.Outcome != rootpublication.PublishSucceeded {
		return completion.Complete(result, hardIO)
	}
	if ready, err := coordinator.ConsumeNativePublicationResourcesV6(&c.publication, result, nil, w); !ready || err != nil {
		return false, err
	}
	if ready, err := coordinator.ConsumeNativePublicationTransactionV6(&c.publication, result, w); !ready || err != nil {
		return false, err
	}
	if ready, err := completion.Complete(result, false); !ready || err != nil {
		return false, err
	}
	c.phase = 7
	return true, nil
}
