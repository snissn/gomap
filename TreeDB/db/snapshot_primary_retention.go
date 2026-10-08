package db

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// AdoptPrimaryMetadataEnrollment transfers a successful capture's governing
// edge to the existing Snapshot finalizer. No public reader or iterator may
// have escaped before this transfer. Concurrent Close refuses adoption.
func (s *Snapshot) AdoptPrimaryMetadataEnrollment(e *retainedalloc.Enrollment) error {
	if e == nil {
		return nil
	}
	s.iteratorMu.Lock()
	defer s.iteratorMu.Unlock()
	if s.closed.Load() || s.finalized.Load() || s.primaryMetadata != nil || s.primaryRoot == nil || !e.BelongsToPair(s.primaryRoot.arena.MetadataOwner(), s.db.valueLogIdentityPins.MetadataOwner()) {
		return ErrSnapshotCapacity
	}
	s.primaryMetadata = e
	s.primaryRoot.arena.InitializeReleaseQueue(&s.primaryRelease)
	return nil
}

// ActivatePrimaryMetadataProducer is for the actual COW producer after all
// startup admission succeeds and before it accepts writes. Temporary captures
// only adopt a borrow and never invoke this producer transition.
func (s *Snapshot) ActivatePrimaryMetadataProducer() error {
	s.iteratorMu.Lock()
	defer s.iteratorMu.Unlock()
	if s.closed.Load() || s.finalized.Load() {
		return ErrClosed
	}
	return s.primaryMetadata.ActivateProducer()
}
