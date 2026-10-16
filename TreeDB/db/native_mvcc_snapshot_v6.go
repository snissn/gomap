package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/lifecycle"
)

var ErrNativeMVCCSnapshotWork = errors.New("native MVCC snapshot requires original work-bearing capture/cleanup")

// State lives in the actual Snapshot containing allocation. Its original
// in-flight capture epoch protects DATA reuse until the actual fixed registry
// registration completes. All physical operands remain the original objects.
type nativeMVCCSnapshotStateV6 struct {
	view                *snapshotView
	registration        lifecycle.NativeMVCCRegistration
	acquireShard        int
	epochLive           bool
	capturePhase        uint8
	finalizePhase       uint8
	workOwned           bool
	leafRelease         leafGenerationPinReleaseWork
	managerRelease      valuelog.ManagerWrapperReleaseWork
	enrollmentRelease   retainedalloc.EnrollmentReleaseWork
	enrollmentBound     bool
	primaryReleaseWork  primaryArenaReleaseWork
	primaryReleaseBound bool
}

// BeginNativeMVCCSnapshotV6 admits the new Snapshot and shared producer pair
// before allocation, epoch mutation or reference effects. A returned Snapshot
// owns every accepted effect even if error is nonnil; it must be physically
// cancelled through the native finalizer. It is unreadable until Step finishes.
func (db *DB) BeginNativeMVCCSnapshotV6(admit func(SnapshotAllocationSizes) error, w *iterator.OrdinalScanWork) (*Snapshot, bool, error) {
	if db == nil || admit == nil {
		return nil, false, ErrClosed
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(snapshotView{}))+128) {
		return nil, false, nil
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	db.valueLogPublicationMu.RLock()
	defer db.valueLogPublicationMu.RUnlock()
	db.rootReuseMu.RLock()
	defer db.rootReuseMu.RUnlock()
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closing.Load() || db.publicationPoisoned.Load() {
		return nil, false, ErrClosed
	}
	view := db.snapshotViewRO.Load()
	if view == nil || view.idx == nil || view.state == nil || view.idx.registry == nil {
		return nil, false, ErrClosed
	}
	state, idx := view.state, view.idx
	// Native capture uses the maintained current cut, rather than an older
	// lock-free view that could already have a stale leaf holder. The original
	// publication exclusion remains held until every physical borrow is owned.
	if state != db.state.Load() || idx != db.idx.Load() {
		return nil, false, nil
	}
	var leafPins *leafGenerationPinSet
	if state.LeafGenerations != nil {
		leafPins = state.LeafGenerations.PinSet
		if leafPins != nil {
			leafPins.mu.Lock()
			current := !leafPins.stale
			leafPins.mu.Unlock()
			if !current {
				return nil, false, ErrSnapshotCapacity
			}
		}
	}
	if !idx.pager.SupportsBoundedPhysicalClose() {
		return nil, false, ErrNativeMVCCSnapshotWork
	}
	sizes := SnapshotAllocationSizes{Wrapper: uint64(unsafe.Sizeof(Snapshot{})), RegistryMetadata: db.valueLogIdentityPins.MetadataOwner(), IdentityRegistry: db.valueLogIdentityPins}
	if root := state.primaryRoot; root != nil {
		if idx.primaryOwner == nil || idx.primary != root.arena {
			return nil, false, ErrSnapshotCapacity
		}
		sizes.PrimaryRoot, sizes.PrimaryMetadata = uint64(unsafe.Sizeof(primaryStateRootV5{})), root.arena.MetadataOwner()
	}
	if generations := state.LeafGenerations; generations != nil {
		if len(generations.GenerationOrder) != 0 && generations.PinSet == nil {
			return nil, false, ErrSnapshotCapacity
		}
		if pins := generations.PinSet; pins != nil {
			sizes.PinSet = uint64(unsafe.Sizeof(*pins))
			sizes.Refs = uint64(cap(pins.refs)) * uint64(unsafe.Sizeof((*leafGenerationPinRef)(nil)))
			sizes.IDs = uint64(cap(generations.GenerationOrder)) * 8
			sizes.PinRefCount, sizes.PinRef = len(pins.refs), uint64(unsafe.Sizeof(leafGenerationPinRef{}))
		}
	}
	var known bool
	sizes.ValueLog, known = state.ValueLogSet.RetentionSizes()
	if sizes.ValueLog.FileCount > 17 {
		return nil, false, ErrSnapshotCapacity
	}
	if !known || state.ValueLogSet != nil && view.vlogManager == nil {
		return nil, false, ErrSnapshotCapacity
	}
	// All fixed constructor/reference effects below are individually represented
	// here; registry probes and any stale pin-set loop use their own continuations.
	refBytes := 4*sizes.Wrapper + 256
	refRecords := uint64(6)
	if view.vlogManager != nil {
		refRecords++
		refBytes += 2*uint64(unsafe.Sizeof(valuelog.Manager{})) + 64
	}
	if leafPins != nil {
		refRecords++
		refBytes += 2 * uint64(unsafe.Sizeof(*leafPins))
	}
	if state.ValueLogSet != nil {
		refRecords++
	}
	if state.primaryRoot != nil {
		refRecords += 2
		refBytes += primaryarena.RefAcquireBytesV6() + 2*uint64(unsafe.Sizeof(primaryArenaOwnerV5{}))
	}
	if !w.Reserve(refRecords, refBytes) {
		return nil, false, nil
	}
	if err := admit(sizes); err != nil {
		return nil, false, err
	}
	s := db.snapPool.Get()
	s.readState.Store(snapshotReadClosedBit)
	s.db, s.idx, s.state, s.vlogManager = db, idx, state, view.vlogManager
	s.nativeMVCC = nativeMVCCSnapshotStateV6{view: view, acquireShard: snapshotAcquireShard(), epochLive: true, capturePhase: 2, workOwned: true}
	db.snapshotAcquireRO[s.nativeMVCC.acquireShard].Add(1)
	db.snapshotAcquireEpoch.Add(1)
	db.stableIndexCaptures.Add(1)
	s.stableIndexCapture, s.stableIndexCaptureCounter = true, &db.stableIndexCaptures
	if view.vlogManager != nil {
		if !view.vlogManager.RetainSnapshotWrapper() {
			return s, false, ErrClosed
		}
		s.vlogWrapperBorrowed = true
	}
	idx.retainWrapperBorrow()
	s.indexWrapperBorrowed = true
	if root := state.primaryRoot; root != nil {
		owner := idx.primaryOwner
		ready, err := owner.retain(nil)
		if !ready || err != nil {
			return s, false, errors.Join(err, ErrSnapshotCapacity)
		}
		s.primaryOwner = owner
		ready, err = root.arena.Acquire(root.ref, nil)
		if !ready || err != nil {
			return s, false, errors.Join(err, ErrSnapshotCapacity)
		}
		s.primaryRoot = root
		root.arena.InitializeReleaseQueue(&s.primaryRelease)
	}
	if set := state.ValueLogSet; set != nil {
		view.vlogManager.Acquire(set)
		s.vlogPinned = true
	}
	if leafPins != nil {
		// The current producer never needs a stale refs walk here. This exact
		// holder edge is acquired before the original publication locks release;
		// later producer retirement observes it through the SAME PinSet.
		leafPins.mu.Lock()
		if leafPins.stale {
			leafPins.mu.Unlock()
			return s, false, ErrSnapshotCapacity
		}
		leafPins.holders++
		leafPins.mu.Unlock()
		s.leafGenerationIDs = state.LeafGenerations.GenerationOrder
		s.leafGenerationPinSet = leafPins
	}
	return s, true, nil
}

func (s *Snapshot) StepNativeMVCCSnapshotCaptureV6(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || !s.nativeMVCC.workOwned || s.nativeMVCC.finalizePhase != 0 {
		return false, ErrNativeMVCCSnapshotWork
	}
	if s.nativeMVCC.capturePhase == 4 {
		return true, nil
	}
	if w == nil || w.Records >= w.RecordLimit || w.Bytes >= w.ByteLimit {
		return false, nil
	}
	if s.nativeMVCC.capturePhase == 2 {
		ready, err := s.idx.registry.BeginNativeMVCCRegistration(&s.nativeMVCC.registration, s.state.primaryRetentionCommitV5(), s.registryShardHint, w)
		if !ready || err != nil {
			return false, err
		}
		s.nativeMVCC.capturePhase = 3
	}
	if s.nativeMVCC.capturePhase == 3 {
		id, hint, ready, err := s.idx.registry.StepNativeMVCCRegistration(&s.nativeMVCC.registration, w)
		if !ready || err != nil {
			return false, err
		}
		if id == 0 {
			return false, ErrSnapshotCapacity
		}
		// The registration owns id until finalizer consumption; adoption merely
		// gives the original Snapshot its original scalar handle.
		if w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(*s))+64) {
			return false, nil
		}
		s.registryID, s.registryShardHint = id, hint
		s.systemRootPublishEpoch = s.nativeMVCC.view.systemRootPublishEpoch
		s.reader.reconfigure(s.state.ValueLogSet, s.db.leafPageReadCache)
		s.tree.Reset(s.idx.pager, &s.reader, s.state.RootPageID)
		s.treePager, s.treeRoot = s.idx.pager, s.state.RootPageID
		s.tree.SetNegativeFilter(s.nativeMVCC.view.negativeFilter)
		s.iteratorMu.Lock()
		s.closed.Store(false)
		s.readState.Store(0)
		s.nativeMVCC.capturePhase = 4
		s.nativeMVCC.view = nil
		s.finishNativeMVCCCaptureEpochV6()
		s.iteratorMu.Unlock()
		return true, nil
	}
	return false, nil
}

func (s *Snapshot) finishNativeMVCCCaptureEpochV6() {
	if s.nativeMVCC.epochLive {
		s.db.snapshotAcquireEpoch.Add(1)
		s.db.snapshotAcquireRO[s.nativeMVCC.acquireShard].Add(-1)
		s.nativeMVCC.epochLive = false
	}
}

// AdoptNativeMVCCMetadataEnrollmentV6 installs exact producer enrollment while
// this SAME Snapshot is still unreadable. DATA-only captures enroll the original
// registry as the single producer; PRIMARY captures require the exact pair.
func (s *Snapshot) AdoptNativeMVCCMetadataEnrollmentV6(e *retainedalloc.Enrollment) error {
	if s == nil || e == nil {
		return ErrSnapshotCapacity
	}
	s.iteratorMu.Lock()
	defer s.iteratorMu.Unlock()
	if !s.nativeMVCC.workOwned || s.nativeMVCC.capturePhase != 2 || s.nativeMVCC.finalizePhase != 0 || s.primaryMetadata != nil || s.db == nil {
		return ErrSnapshotCapacity
	}
	first, second := s.db.valueLogIdentityPins.MetadataOwner(), (*retainedalloc.Owner)(nil)
	if s.primaryRoot != nil {
		first, second = s.primaryRoot.arena.MetadataOwner(), first
	}
	if !e.BelongsToPair(first, second) {
		return ErrSnapshotCapacity
	}
	s.primaryMetadata = e
	return nil
}
