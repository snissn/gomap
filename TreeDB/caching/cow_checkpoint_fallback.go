package caching

import (
	"context"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// RefreshCOWCommandWALCheckpointFallback converges recovery slots through the
// same complete-cut physical build and exact Snapshot handoff as a checkpoint.
// It never grants the ordinary backend publisher cache-owner authority.
func (db *DB) RefreshCOWCommandWALCheckpointFallback() error {
	if db == nil || db.cow == nil {
		return ErrCOWUnsupported
	}
	return db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{
		skipCheckpoint: true, cowCheckpointFallback: true,
	}, nil)
}

// refreshCOWAutomaticCheckpointFallback runs under the original checkpoint or
// physical maintenance's flushMu, with post-frontier writers still admitted.
// Only the original applied basis's captured T may
// be republished: mutable suffixes remain outside frozen coverage and are
// preserved by the same accepted Snapshot handoff as the first publication.
// If a newer suffix already rolled over, leave it for the next checkpoint.
func (db *DB) refreshCOWAutomaticCheckpointFallback(ctx context.Context, through rootpublication.SealedPrefix) (retErr error) {
	c := db.cow
	cut := c.retainCut()
	defer func() { retErr = joinCOWCleanupError(retErr, c.releaseCut(cut)) }()
	if cut == nil || c.prefix == nil {
		return backenddb.ErrClosed
	}
	basis, live := cut.basis.snapshot.StateToken()
	if !live {
		return backenddb.ErrClosed
	}
	if through.CanonicalLSN == 0 || len(cut.frozen) != 0 ||
		cut.basis.logicalSeq != through.LogicalSequence || basis.AppliedCommandLSN != through.CanonicalLSN ||
		cut.frozenLogicalSeq != through.LogicalSequence || cut.frozenCommandLSN != through.CanonicalLSN {
		return nil
	}
	backend, ok := db.backend.(interface {
		CommandWALCheckpointFallbackNeedsRefresh(backenddb.StateToken) (bool, error)
	})
	if !ok {
		return ErrCOWUnsupported
	}
	needed, err := backend.CommandWALCheckpointFallbackNeedsRefresh(basis)
	if err != nil || !needed {
		return err
	}
	// Physical completion may precede the original accepted custody's release.
	// Join that same T before capturing an empty accepted cut for convergence.
	if err = c.prefix.WaitAccepted(ctx, through); err != nil {
		return err
	}
	if c.prefix.LogicalFrontiers().S != through {
		return nil
	}
	// WaitAccepted can return immediately for an already accepted prefix even
	// after this optional slice expired. Do not start another physical build in
	// that case. Once begun, native I/O and accepted custody must finish under
	// the original owner; this remains a cooperative, not a syscall deadline.
	if err = ctx.Err(); err != nil {
		return err
	}
	err = db.flushCOWFrozenCutForFallback(true, nil, cut, true)
	cut = nil // the existing flush owner consumed this exact retained cut
	if err != nil {
		return err
	}
	// Writers can replace the current cut throughout this pass. Retain the
	// installed basis before inspecting it, just as for the opening predicate.
	cut = c.retainCut()
	if cut == nil {
		return backenddb.ErrClosed
	}
	basis, live = cut.basis.snapshot.StateToken()
	if !live {
		return backenddb.ErrClosed
	}
	needed, err = backend.CommandWALCheckpointFallbackNeedsRefresh(basis)
	if err != nil {
		return err
	}
	if needed || basis.AppliedCommandLSN != through.CanonicalLSN || cut.basis.logicalSeq != through.LogicalSequence {
		return backenddb.ErrRootPublicationBasisMismatch
	}
	return nil
}

// Called only under maintenance admission and flushMu, after admitted writers
// and the previous accepted handoff drain. No generic post-maintenance capture
// follows: the build itself preadmits and binds its exact successor Snapshot.
func (db *DB) refreshCOWCheckpointFallback() (retErr error) {
	c := db.cow
	backend, ok := db.backend.(interface {
		CommandWALCheckpointFallbackNeedsRefresh(backenddb.StateToken) (bool, error)
	})
	if !ok {
		return ErrCOWUnsupported
	}
	cut := c.retainCut()
	defer func() { retErr = joinCOWCleanupError(retErr, c.releaseCut(cut)) }()
	if cut == nil || c.prefix == nil {
		return backenddb.ErrClosed
	}
	basis, live := cut.basis.snapshot.StateToken()
	if !live {
		return backenddb.ErrClosed
	}
	needed, err := backend.CommandWALCheckpointFallbackNeedsRefresh(basis)
	if err != nil {
		return err
	}
	// An automatic pass may install this exact basis after the preceding
	// explicit checkpoint joined its own endpoint. The backend predicate has
	// completed physical sealing and released its publication gates; join the
	// original accepted custody before comparing the two completion frontiers.
	// flushMu keeps direct Close from detaching the original prefix meanwhile.
	through := rootpublication.SealedPrefix{LogicalSequence: cut.basis.logicalSeq, CanonicalLSN: basis.AppliedCommandLSN}
	if err = c.prefix.WaitAccepted(context.Background(), through); err != nil {
		return err
	}
	frontiers := c.prefix.LogicalFrontiers()
	if cut.logicalSeq == frontiers.V && cut.basis.logicalSeq == frontiers.S.LogicalSequence &&
		cut.visibleCommandLSN >= frontiers.S.CanonicalLSN && cut.visibleCommandLSN < frontiers.C {
		// A delayed durable group may close a root-neutral suffix after the
		// checkpoint captured its finite endpoint. Maintenance has drained all
		// admitted writers; prove that exact suffix under the original writer
		// exclusion rather than rewriting this immutable cut's command identity.
		c.writerMu.Lock()
		_, proofErr := c.prefix.MarkAcceptedRootNeutralBoundary(rootpublication.SealedPrefix{LogicalSequence: cut.logicalSeq, CanonicalLSN: cut.visibleCommandLSN})
		c.writerMu.Unlock()
		if proofErr != nil {
			return proofErr
		}
		// Optional fallback convergence cannot seal this newer suffix. The next
		// checkpoint (or ordinary recovery) carries its real barrier identity.
		return nil
	}
	if cut.logicalSeq != frontiers.V || cut.visibleCommandLSN != frontiers.C || cut.basis.logicalSeq != frontiers.S.LogicalSequence || cut.logicalSeq < frontiers.S.LogicalSequence || cut.visibleCommandLSN < frontiers.S.CanonicalLSN {
		return fmt.Errorf("%w: fallback cut logical=%d canonical=%d basis=%d frontiers=%+v", backenddb.ErrRootPublicationBasisMismatch, cut.logicalSeq, cut.visibleCommandLSN, cut.basis.logicalSeq, frontiers)
	}
	if !needed {
		return nil
	}
	// A writer admitted after Checkpoint may already own a newer suffix.
	// Optional idle convergence must neither fail that completed boundary nor
	// seal the suffix. Its next explicit checkpoint will carry the coverage.
	if cut.logicalSeq > frontiers.S.LogicalSequence || cut.visibleCommandLSN > frontiers.S.CanonicalLSN {
		return nil
	}
	if len(cut.frozen) != 0 {
		return backenddb.ErrRootPublicationBasisMismatch
	}
	err = db.flushCOWFrozenCutForFallback(true, nil, cut, true)
	cut = nil // the same flush owner consumed the original retained cut
	if err != nil {
		return err
	}
	basis, live = c.cut.basis.snapshot.StateToken()
	if !live {
		return backenddb.ErrClosed
	}
	needed, err = backend.CommandWALCheckpointFallbackNeedsRefresh(basis)
	if err != nil {
		return err
	}
	if needed {
		return backenddb.ErrRootPublicationBasisMismatch
	}
	return nil
}
