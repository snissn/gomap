package db

import (
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
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
	f, identity, err := s.pinnedValueLogSegment(fileID)
	if err != nil {
		return nil, rootpublication.StableIdentity{}, err
	}
	return f.File, identity, nil
}

// InspectPinnedCOWLeafRecord inspects the same retained segment as
// PinnedValueLogFile and can use its managed mapping. The caller must retain
// the snapshot throughout any use of the returned handle.
func (s *Snapshot) InspectPinnedCOWLeafRecord(ptr page.ValuePtr, limits valuelog.COWReadLimits) (*os.File, rootpublication.StableIdentity, valuelog.COWRecordShape, error) {
	if s == nil {
		return nil, rootpublication.StableIdentity{}, valuelog.COWRecordShape{}, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return nil, rootpublication.StableIdentity{}, valuelog.COWRecordShape{}, err
	}
	defer s.endRead()
	f, identity, err := s.pinnedValueLogSegment(ptr.FileID)
	if err != nil {
		return nil, rootpublication.StableIdentity{}, valuelog.COWRecordShape{}, err
	}
	shape, err := f.InspectCOWLeafRecord(ptr, limits)
	return f.File, identity, shape, err
}

// PinnedCOWLeafRead borrows the exact inspected segment and its decoded cache
// only during WithPinnedCOWLeafRecord's callback. It must not be retained.
type PinnedCOWLeafRead struct {
	file   *valuelog.File
	cache  *leafPageReadCache
	ptr    page.LeafLogPtr
	limits valuelog.COWReadLimits
	shape  valuelog.COWRecordShape
}

// WithPinnedCOWLeafRecord holds one snapshot read pin from live metadata
// inspection through the caller's shape policy, cache lookup and, on a miss,
// verified page materialization. The registered handle and stable identity
// come from that same captured Set. The Set cannot detach within this scope.
func (s *Snapshot) WithPinnedCOWLeafRecord(ptr page.LeafLogPtr, limits valuelog.COWReadLimits, read func(PinnedCOWLeafRead) ([]byte, error)) ([]byte, error) {
	if err := s.beginRead(); err != nil {
		return nil, fmt.Errorf("COW physical leaf pinned-file ptr=%+v: %w", ptr.ValuePtr(), err)
	}
	defer s.endRead()
	f, identity, err := s.pinnedValueLogSegment(ptr.ValueLogFileID())
	if err != nil {
		return nil, fmt.Errorf("COW physical leaf pinned-file ptr=%+v: %w", ptr.ValuePtr(), err)
	}
	shape, err := f.InspectCOWLeafRecord(ptr.ValuePtr(), limits)
	if err != nil {
		return nil, fmt.Errorf("COW physical leaf inspect ptr=%+v file=%q identity=%+v: %w", ptr.ValuePtr(), f.File.Name(), identity, err)
	}
	return read(PinnedCOWLeafRead{file: f, cache: s.reader.leafPageCache, ptr: ptr, limits: limits, shape: shape})
}

func (r *PinnedCOWLeafRead) FileName() string { return r.file.File.Name() }

// Shape returns the inspected metadata for caller policy and scratch admission.
func (r *PinnedCOWLeafRead) Shape() valuelog.COWRecordShape { return r.shape }

func (r *PinnedCOWLeafRead) ReadPage(input, raw, dst []byte, decode valuelog.COWDecodeFunc) ([]byte, error) {
	return r.file.ReadCOWLeafPage(r.ptr.ValuePtr(), r.shape, input, raw, dst, decode)
}

// CopyVerifiedPage requires the callback to finish all current caller-policy
// checks first. Dictionary leaves require a dominated decode credit.
func (r *PinnedCOWLeafRead) CopyVerifiedPage(dst []byte) ([]byte, bool, error) {
	return copyVerifiedCachedLeafPage(r.cache, r.ptr, dst, r.shape.DictID != 0, r.limits)
}

// AdmitVerifiedPage is valid only after ReadPage completed successfully on
// this borrow. The cache retains neither the borrow nor the page scratch.
func (r *PinnedCOWLeafRead) AdmitVerifiedPage(data []byte) error {
	return admitVerifiedCachedLeafPage(r.cache, r.ptr, data, r.shape.DictID != 0, r.limits)
}

// The caller holds a snapshot read.
func (s *Snapshot) pinnedValueLogSegment(fileID uint32) (*valuelog.File, rootpublication.StableIdentity, error) {
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
	return f, identity, nil
}

// CopyVerifiedCachedLeafPage copies a hit from this captured snapshot's existing
// decoded cache into caller-admitted page storage. The caller must first inspect
// the pinned record and apply its shape/codec/limit policy.
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
	return copyVerifiedCachedLeafPage(s.reader.leafPageCache, ptr, dst, false, valuelog.COWReadLimits{})
}

// CopyVerifiedCachedLeafPageWithCOWPolicy additionally requires dictionary
// decode credit whose complete limits are dominated by this caller. The caller
// must inspect under its original limits and check its dictionary capability
// before calling. An uncredited page is a miss, even for canonical limits.
func (s *Snapshot) CopyVerifiedCachedLeafPageWithCOWPolicy(ptr page.LeafLogPtr, inspected *os.File, identity rootpublication.StableIdentity, dst []byte, limits valuelog.COWReadLimits) ([]byte, bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, false, err
	}
	defer s.endRead()
	if err := s.checkInspectedLeafFile(ptr, inspected, identity); err != nil {
		return nil, false, err
	}
	return copyVerifiedCachedLeafPage(s.reader.leafPageCache, ptr, dst, true, limits)
}

func copyVerifiedCachedLeafPage(cache *leafPageReadCache, ptr page.LeafLogPtr, dst []byte, dictionary bool, limits valuelog.COWReadLimits) ([]byte, bool, error) {
	if dictionary && !cowReadPolicyAllows(limits, limits) {
		return nil, false, valuelog.ErrCOWReadUnsupported
	}
	if cap(dst) < page.PageSize {
		return nil, false, nil
	}
	if dictionary {
		data, _, _, hit := cache.getToWithCOWPolicy(ptr, dst[:0:page.PageSize], true, limits)
		return data, hit, nil
	}
	data, _, _, hit := cache.getToWithRecordPolicy(ptr, dst[:0:page.PageSize], true)
	return data, hit, nil
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
	return admitVerifiedCachedLeafPage(s.reader.leafPageCache, ptr, verifiedPage, false, valuelog.COWReadLimits{})
}

// AdmitVerifiedCachedLeafPageWithCOWPolicy follows a successful full CRC-
// verified dictionary decode under limits, including definition preparation.
// Existing fixed slots own the copied page and policy; no dictionary owner,
// decoder, snapshot or file is retained. Miss probation and lock contention may
// decline either admission or credit without failing the completed read.
func (s *Snapshot) AdmitVerifiedCachedLeafPageWithCOWPolicy(ptr page.LeafLogPtr, inspected *os.File, identity rootpublication.StableIdentity, verifiedPage []byte, limits valuelog.COWReadLimits) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	if err := s.checkInspectedLeafFile(ptr, inspected, identity); err != nil {
		return err
	}
	return admitVerifiedCachedLeafPage(s.reader.leafPageCache, ptr, verifiedPage, true, limits)
}

func admitVerifiedCachedLeafPage(cache *leafPageReadCache, ptr page.LeafLogPtr, verifiedPage []byte, dictionary bool, limits valuelog.COWReadLimits) error {
	if dictionary && !cowReadPolicyAllows(limits, limits) {
		return valuelog.ErrCOWReadUnsupported
	}
	if !cache.markRecordChecksumVerifiedWithLockPolicy(ptr, verifiedPage, true) {
		cache.storeReadMiss(ptr, verifiedPage, true)
	}
	if dictionary {
		cache.markCOWReadPolicy(ptr, verifiedPage, limits)
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
