package db

import (
	"os"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// AcquireStableSnapshotWithAllocationAdmission adds the existing namespace
// maintenance fence to the bounded snapshot constructor. It performs no
// checkpoint, namespace stabilization, or durable resource capture.
func (db *DB) AcquireStableSnapshotWithAllocationAdmission(admit func(SnapshotAllocationSizes) error) (*Snapshot, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	snapshot, err := db.AcquireSnapshotWithAllocationAdmission(admit)
	if err != nil {
		return nil, err
	}
	db.stableIndexCaptures.Add(1)
	snapshot.stableIndexCapture = true
	snapshot.stableIndexCaptureCounter = &db.stableIndexCaptures
	return snapshot, nil
}

// PinnedValueLogFile exposes the exact registered handle in this snapshot's
// retained Set. The caller must retain the snapshot throughout all handle use;
// the result grants no mutation, durability, or namespace authority.
func (s *Snapshot) PinnedValueLogFile(fileID uint32) (*os.File, rootpublication.StableIdentity, error) {
	if s == nil {
		return nil, rootpublication.StableIdentity{}, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return nil, rootpublication.StableIdentity{}, err
	}
	defer s.endRead()
	if s.state == nil || s.state.ValueLogSet == nil {
		return nil, rootpublication.StableIdentity{}, rootpublication.ErrUnresolvedResource
	}
	f := s.state.ValueLogSet.Files[fileID]
	if f == nil || f.File == nil {
		return nil, rootpublication.StableIdentity{}, rootpublication.ErrUnresolvedResource
	}
	identity, ok := f.RegisteredStableIdentity()
	if !ok {
		return nil, rootpublication.StableIdentity{}, rootpublication.ErrUnresolvedResource
	}
	return f.File, identity, nil
}

// PinValueLogReadFiles fills caller-admitted storage without discovering a new
// Set or allocating a second registry. Pin's concrete wrapper size belongs to
// each reservation. The exact registered File identity gates cross-manager
// deletion; a deletion conflict refuses capture before publication.
func (s *Snapshot) PinValueLogReadFiles(dst []*rootpublication.IdentityPin) error {
	if s == nil {
		return ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	if s.state == nil || s.state.ValueLogSet == nil {
		if len(dst) == 0 {
			return nil
		}
		return rootpublication.ErrUnresolvedResource
	}
	set := s.state.ValueLogSet
	if len(dst) != len(set.Files) || s.db == nil || s.vlogManager == nil {
		return rootpublication.ErrResourceConflict
	}
	registry := s.db.ValueLogIdentityPinRegistry()
	if registry == nil || registry != s.vlogManager.StableResourcePinRegistry() {
		return rootpublication.ErrResourceConflict
	}
	i := 0
	for _, file := range set.Files {
		identity, ok := file.RegisteredStableIdentity()
		if !ok {
			releaseReadPins(dst)
			return rootpublication.ErrUnresolvedResource
		}
		pin, err := registry.Pin(identity)
		if err != nil {
			releaseReadPins(dst)
			return err
		}
		dst[i] = pin
		i++
	}
	return nil
}

func releaseReadPins(pins []*rootpublication.IdentityPin) {
	for i, pin := range pins {
		pin.Release()
		pins[i] = nil
	}
}

// OwnedPointerProjectionIterator borrows this snapshot's exact tree owner.
// Its caller must reserve tree.OwnedPointerProjectionAllocationSize before
// calling, retain the snapshot until iterator Close, and exclude DB teardown.
// No snapshot iterator registry is allocated or independently populated.
func (s *Snapshot) OwnedPointerProjectionIterator(start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (iterator.UnsafeIterator, error) {
	if s == nil {
		return nil, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	it := s.tree.OwnedPointerProjectionIterator(start, end, readLeaf)
	if err := it.Error(); err != nil {
		_ = it.Close()
		return nil, err
	}
	return it, nil
}

// GetEntryExactWithFixedScratch reads a persisted entry with caller-admitted
// fixed-page scratch. The returned slices remain borrowed from this snapshot
// and the scratch until the next operation using either.
func (s *Snapshot) GetEntryExactWithFixedScratch(key, keyScratch, leafScratch []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, error) {
	if s == nil {
		return node.LeafEntry{}, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return node.LeafEntry{}, err
	}
	defer s.endRead()
	return s.tree.GetEntryWithFixedScratch(key, keyScratch, leafScratch, readLeaf)
}

// OwnedUserRootEmpty checks the exact tree pinned by this snapshot. It does
// not capture a current backend root or construct an iterator.
func (s *Snapshot) OwnedUserRootEmpty() (bool, error) {
	if s == nil {
		return false, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return false, err
	}
	defer s.endRead()
	return s.tree.OwnedUserRootEmpty()
}
