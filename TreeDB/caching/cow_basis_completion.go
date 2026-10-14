package caching

import (
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Admission creates the SAME basis and its original failure edge before the
// backend capture acquires any physical references. No replacement debt owner.
func admitCOWBackendBasis(budget *memtable.COWBudget, s backenddb.SnapshotAllocationSizes) (*cowBackendBasis, error) {
	lease, metadata, err := admitCOWSnapshotRetention(budget, cowBasisCharge(s), s)
	if err != nil {
		return nil, err
	}
	if s.IdentityRegistry == nil || s.RegistryMetadata != s.IdentityRegistry.MetadataOwner() {
		metadata.Close()
		lease.Close()
		return nil, rootpublication.ErrResourceOwnership
	}
	if s.PrimaryMetadata == nil {
		metadata, err = s.RegistryMetadata.Enroll(budget)
		if err != nil {
			lease.Close()
			return nil, err
		}
	}
	custody, err := rootpublication.NewOriginalCleanupCustody(s.IdentityRegistry, s.RegistryMetadata)
	if err != nil {
		metadata.Close()
		lease.Close()
		return nil, err
	}
	b := &cowBackendBasis{primaryMetadata: s.PrimaryMetadata != nil, lease: lease, metadata: metadata, originalCustody: custody, pins: make([]*rootpublication.IdentityPin, s.ValueLog.FileCount)}
	b.refs.Store(1)
	return b, nil
}

func (b *cowBackendBasis) adoptSnapshot(s *backenddb.Snapshot) error {
	if b == nil || s == nil {
		return rootpublication.ErrResourceOwnership
	}
	b.completionMu.Lock()
	b.snapshot = s
	b.completionMu.Unlock()
	if err := s.AdoptSnapshotCleanupCompletion(b); err != nil {
		// Refused ownership cannot detach this admitted original payload.
		// Retain its first refusal without claiming physical completion or
		// refunding the enrollment; the existing Snapshot remains sole finalizer.
		b.completionMu.Lock()
		b.cleanupErr = err
		b.originalCustody.Complete(b, err)
		b.completionMu.Unlock()
		return err
	}
	b.completionMu.Lock()
	if b.completed {
		err := b.cleanupErr
		b.completionMu.Unlock()
		if err != nil {
			return err
		}
		return backenddb.ErrClosed
	}
	if b.primaryMetadata {
		if err := s.AdoptPrimaryMetadataEnrollment(b.metadata); err != nil {
			b.completionMu.Unlock()
			return err
		}
		b.metadata = nil // actual Snapshot last-read finalizer owns this SAME enrollment
	}
	pins := b.pins
	b.completionMu.Unlock()
	return s.PinValueLogReadFiles(pins)
}

func (b *cowBackendBasis) SnapshotCleanupCompleted(err error) {
	b.completionMu.Lock()
	defer b.completionMu.Unlock()
	if b.completed {
		return
	}
	b.completed = true
	b.cleanupErr = err
	if err != nil {
		b.originalCustody.Complete(b, err)
		b.metadata.CloseAfterCleanup(err)
		b.metadata = nil
		return
	}
	for _, pin := range b.pins {
		pin.Release()
	}
	b.pins = nil
	b.snapshot = nil
	b.originalCustody.Complete(nil, nil)
	b.originalCustody = nil
	b.metadata.Close()
	b.metadata = nil
	b.lease.Close()
}

// Preserve the original refusal identity when actual cleanup succeeds.
func joinCOWCleanupError(err, cleanupErr error) error {
	if cleanupErr == nil {
		return err
	}
	if err == nil {
		return cleanupErr
	}
	return errors.Join(err, cleanupErr)
}
