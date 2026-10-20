package db

import "github.com/snissn/gomap/TreeDB/internal/rootpublication"

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
