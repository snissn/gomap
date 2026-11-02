package db

import (
	"github.com/snissn/gomap/TreeDB/tree"
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

// CopyVerifiedCachedLeafPage copies a hit from this captured snapshot's existing
// decoded cache into caller-admitted page storage. The caller must first inspect
// the record through PinnedValueLogFile and apply its shape/codec/limit policy.
// The exact inspected handle and registered identity must still belong to this
// snapshot. Misses, unverified records, disabled caches and insufficient scratch
// return no hit; this operation never allocates or admits a cache entry. The
// original tree caller remains responsible for page validation after copying.
func (s *Snapshot) CopyVerifiedCachedLeafPage(ptr page.LeafLogPtr, inspected *os.File, identity rootpublication.StableIdentity, dst []byte) ([]byte, bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, false, err
	}
	defer s.endRead()
	if err := s.checkInspectedLeafFile(ptr, inspected, identity); err != nil {
		return nil, false, err
	}
	if cap(dst) < page.PageSize {
		return nil, false, nil
	}
	data, _, _, hit := s.reader.leafPageCache.getToWithRecordPolicy(ptr, dst[:0:page.PageSize], true)
	return data, hit, nil
}

// CopyDictionaryVerifiedCachedLeafPage requires the current decoder's independently
// owned definition digest in addition to the normal pinned-file authority.
func (s *Snapshot) CopyDictionaryVerifiedCachedLeafPage(ptr page.LeafLogPtr, inspected *os.File, identity rootpublication.StableIdentity, dst []byte, digest [32]byte) ([]byte, bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, false, err
	}
	defer s.endRead()
	if err := s.checkInspectedLeafFile(ptr, inspected, identity); err != nil {
		return nil, false, err
	}
	data, hit := s.reader.leafPageCache.dictionaryVerifiedPage(ptr, dst, digest)
	return data, hit, nil
}

// ObserveDictionaryCacheEpoch captures eligibility before the physical decode.
// A disabled cache or concurrent maintenance cannot produce a usable token.
func (s *Snapshot) ObserveDictionaryCacheEpoch() (uint64, bool, error) {
	if err := s.beginRead(); err != nil {
		return 0, false, err
	}
	defer s.endRead()
	c := s.reader.leafPageCache
	if c == nil || c.disabled.Load() {
		return 0, false, nil
	}
	epoch := c.dictionaryEpoch.Load()
	if c.disabled.Load() || c.dictionaryEpoch.Load() != epoch {
		return 0, false, nil
	}
	return epoch, true, nil
}

// MarkDictionaryVerifiedCachedLeafPage follows successful actual full decode.
// A concurrent eviction or non-identical page declines the upgrade.
func (s *Snapshot) MarkDictionaryVerifiedCachedLeafPage(ptr page.LeafLogPtr, inspected *os.File, identity rootpublication.StableIdentity, data []byte, digest [32]byte, epoch uint64) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	if err := s.checkInspectedLeafFile(ptr, inspected, identity); err != nil {
		return err
	}
	s.reader.leafPageCache.markDictionaryVerified(ptr, data, digest, epoch)
	return nil
}

// MarkCachedLeafRecordVerified follows a successful full CRC-verified decode
// through this snapshot's exact inspected file. It can upgrade only an existing
// byte-identical decoded page; misses never allocate or populate the cache.
func (s *Snapshot) MarkCachedLeafRecordVerified(ptr page.LeafLogPtr, inspected *os.File, identity rootpublication.StableIdentity, verifiedPage []byte) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	if err := s.checkInspectedLeafFile(ptr, inspected, identity); err != nil {
		return err
	}
	s.reader.leafPageCache.markRecordChecksumVerified(ptr, verifiedPage)
	return nil
}

// StoreVerifiedCachedLeafPage follows a successful full CRC-verified decode
// through this snapshot's exact inspected file. It upgrades an existing
// byte-identical entry or applies the original repeated-miss admission policy
// using only existing slot backing. Cold slots and contended locks decline
// admission; this method never allocates page backing or retains a new owner.
// Record verification does not establish the decoded page's own checksum.
func (s *Snapshot) StoreVerifiedCachedLeafPage(ptr page.LeafLogPtr, inspected *os.File, identity rootpublication.StableIdentity, verifiedPage []byte) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	if err := s.checkInspectedLeafFile(ptr, inspected, identity); err != nil {
		return err
	}
	cache := s.reader.leafPageCache
	if !cache.markRecordChecksumVerifiedWithLockPolicy(ptr, verifiedPage, true) {
		cache.storeReadMissNoAlloc(ptr, verifiedPage, true)
	}
	return nil
}

// AdmitVerifiedCachedLeafPage follows a successful full CRC-verified decode
// through this snapshot's exact inspected file. Unlike StoreVerifiedCachedLeafPage,
// it permits the existing cache owner to allocate one page of backing for an
// unallocated fixed slot. Backing is shared by readers and bounded by configured
// cache capacity, independently of the caller's admitted decode workspace.
// Admission keeps repeated-miss probation and nonblocking bucket/slot locks;
// disabled caches and contention decline without failing the decoded read.
// The cache copies the page and never retains the caller's scratch or file.
// Record verification does not establish the decoded page's own checksum.
func (s *Snapshot) AdmitVerifiedCachedLeafPage(ptr page.LeafLogPtr, inspected *os.File, identity rootpublication.StableIdentity, verifiedPage []byte) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	if err := s.checkInspectedLeafFile(ptr, inspected, identity); err != nil {
		return err
	}
	cache := s.reader.leafPageCache
	if !cache.markRecordChecksumVerifiedWithLockPolicy(ptr, verifiedPage, true) {
		cache.storeReadMiss(ptr, verifiedPage, true)
	}
	return nil
}

// The caller holds beginRead, so the captured Set and reader cannot detach.
func (s *Snapshot) checkInspectedLeafFile(ptr page.LeafLogPtr, inspected *os.File, identity rootpublication.StableIdentity) error {
	if s.state == nil || s.state.ValueLogSet == nil {
		return rootpublication.ErrUnresolvedResource
	}
	f := s.state.ValueLogSet.Files[ptr.ValueLogFileID()]
	if f == nil || f.File == nil {
		return rootpublication.ErrUnresolvedResource
	}
	registered, ok := f.RegisteredStableIdentity()
	if !ok || f.File != inspected || registered != identity {
		return rootpublication.ErrResourceConflict
	}
	return nil
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

// OwnedReversePointerProjectionIterator borrows the same exact tree and admitted
// fixed projection storage, with reverse movement and the original domain.
func (s *Snapshot) OwnedReversePointerProjectionIterator(start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (iterator.UnsafeIterator, error) {
	if s == nil {
		return nil, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	it := s.tree.OwnedReversePointerProjectionIterator(start, end, readLeaf)
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

// SeekGEEntryWithFixedScratch reads metadata through this already captured tree.
// Returned bytes borrow caller-admitted scratch and this snapshot until reuse.
func (s *Snapshot) SeekGEEntryWithFixedScratch(start, end, keyScratch, leafScratch []byte, scratch *tree.OwnedSuccessorScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, bool, error) {
	if s == nil {
		return node.LeafEntry{}, false, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return node.LeafEntry{}, false, err
	}
	defer s.endRead()
	return s.tree.SeekGEWithFixedScratch(start, end, keyScratch, leafScratch, scratch, readLeaf)
}
