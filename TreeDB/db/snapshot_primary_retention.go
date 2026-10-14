package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// A bank reference protects its allocation from reuse; the independent physical
// owner protects the companion mapping through DB and DATA-generation teardown.
// Both edges belong to the original Snapshot finalizer.
func (s *Snapshot) acquirePrimaryRoot(idx *indexGen, root *primaryStateRootV5) error {
	if idx == nil || idx.primaryOwner == nil || idx.primary != root.arena {
		return rootpublication.ErrResourceOwnership
	}
	owner := idx.primaryOwner
	retained, err := owner.retain(nil)
	if !retained || err != nil {
		return errors.Join(err, rootpublication.ErrResourceOwnership)
	}
	acquired, err := root.arena.Acquire(root.ref, nil)
	if !acquired || err != nil {
		_, releaseErr := owner.release(nil)
		return errors.Join(err, releaseErr, rootpublication.ErrResourceOwnership)
	}
	s.primaryRoot, s.primaryOwner = root, owner
	return nil
}

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

// SnapshotCleanupCompletion belongs to a private admitted consumer of this SAME
// Snapshot. Completion follows actual last-read cleanup and exported alias scrub;
// Close acceptance alone never releases the consumer's retained allocation.
type SnapshotCleanupCompletion interface {
	SnapshotCleanupCompleted(error)
}

// AdoptSnapshotCleanupCompletion transfers one preborn consumer before publication.
// A private capture refused after Close still joins the original running finalizer
// or observes its exact completed outcome; it never substitutes a scalar Close.
func (s *Snapshot) AdoptSnapshotCleanupCompletion(completion SnapshotCleanupCompletion) error {
	if s == nil || completion == nil {
		return rootpublication.ErrResourceOwnership
	}
	s.iteratorMu.Lock()
	if s.cleanupCompletionAdopted {
		s.iteratorMu.Unlock()
		return rootpublication.ErrResourceOwnership
	}
	s.cleanupCompletionAdopted = true
	if s.cleanupFinished {
		err := s.cleanupErr
		s.iteratorMu.Unlock()
		completion.SnapshotCleanupCompleted(err)
		return nil
	}
	s.cleanupCompletion = completion
	s.iteratorMu.Unlock()
	return nil
}
