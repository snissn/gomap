package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// capturePrivatePrimarySnapshotV6 attempts one actual reader cohort. A refused
// cell returns the next hint, without any retained partial pin. The same registered
// custody stays readable during immutable preparation; terminal Close takes its
// finalization gate before releasing any edge. No public Snapshot is adopted.
func (db *DB) capturePrivatePrimarySnapshotV6(hint int, w *iterator.OrdinalScanWork) (*primarySnapshotCustodyV5, int, bool, error) {
	if db == nil || w == nil {
		return nil, hint, false, ErrClosed
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(DB{}))) {
		return nil, hint, false, nil
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	db.valueLogPublicationMu.RLock()
	defer db.valueLogPublicationMu.RUnlock()
	db.rootReuseMu.RLock()
	defer db.rootReuseMu.RUnlock()
	db.primaryCustodyMu.Lock()
	defer db.primaryCustodyMu.Unlock()
	if db.closing.Load() || db.publicationPoisoned.Load() {
		return nil, hint, false, ErrClosed
	}
	if !w.Reserve(2, 32) {
		return nil, hint, false, nil
	}
	shard := snapshotAcquireShard()
	db.snapshotAcquireRO[shard].Add(1)
	db.snapshotAcquireEpoch.Add(1)
	defer func() { db.snapshotAcquireEpoch.Add(1); db.snapshotAcquireRO[shard].Add(-1) }()

	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(snapshotView{}))+2*uint64(unsafe.Sizeof(DBState{}))) {
		return nil, hint, false, nil
	}
	view := db.snapshotViewRO.Load()
	if view == nil || view.idx == nil || view.idx.registry == nil || view.state == nil || view.state.primaryRoot == nil {
		return nil, hint, false, ErrSnapshotCapacity
	}
	state, idx, vm := view.state, view.idx, view.vlogManager
	var pins *leafGenerationPinSet
	if state.LeafGenerations != nil && len(state.LeafGenerations.GenerationOrder) != 0 {
		pins = state.LeafGenerations.PinSet
		if pins == nil {
			return nil, hint, false, ErrSnapshotCapacity
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(leafGenerationPinSet{}))) {
			return nil, hint, false, nil
		}
		pins.mu.Lock()
		defer pins.mu.Unlock()
		// Repinning every ref is a different finite preparation phase, never an
		// ordinary retain fallback hidden inside this fixed capture.
		if pins.stale && !pins.pinned {
			return nil, hint, false, ErrSnapshotCapacity
		}
	}
	needsVlog := state.ValueLogSet != nil && len(state.ValueLogSet.Files) != 0
	if needsVlog && vm == nil {
		return nil, hint, false, ErrClosed
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(valuelog.Set{}))) {
		return nil, hint, false, nil
	}
	if _, known := state.ValueLogSet.RetentionSizes(); !known {
		return nil, hint, false, ErrSnapshotCapacity
	}
	if hint < 0 {
		hint = int(state.primaryRetentionCommitV5() & lifecycle.FastReaderShardMask)
	}
	hint &= lifecycle.FastReaderShardMask
	// Admit the exact remaining fixed operands before registration. Acquisition
	// and error unwind still execute their real charged primitives below. No
	// state or validation is supplied by this size descriptor.
	wrapperBytes := 2*uint64(unsafe.Sizeof(Snapshot{})) + 2*uint64(unsafe.Sizeof(primaryBaseReadLeaseV5{})) + 2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))
	linkBytes := 2*uint64(unsafe.Sizeof(DB{})) + 2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{})) + 16
	vlogBytes := 2*uint64(unsafe.Sizeof(valuelog.Set{})) + 2*uint64(unsafe.Sizeof(valuelog.Manager{}))
	records := uint64(5) // cohort, arena, wrapper, link, exact registry error unwind
	bytes := uint64(128) + primaryarena.RefAcquireBytesV6() + wrapperBytes + linkBytes + 128
	if needsVlog {
		records++
		bytes += vlogBytes
	}
	if pins != nil {
		records++
		bytes += 2 * uint64(unsafe.Sizeof(leafGenerationPinSet{}))
	}
	if w.Records > w.RecordLimit || w.Bytes > w.ByteLimit || records > w.RecordLimit-w.Records || bytes > w.ByteLimit-w.Bytes {
		return nil, hint, false, nil
	}
	id, attempted, e := idx.registry.RegisterOneFastWithWorkV6(state.primaryRetentionCommitV5(), hint, w)
	if e != nil || !attempted {
		return nil, hint, false, e
	}
	if id == 0 {
		return nil, (hint + 1) & lifecycle.FastReaderShardMask, true, nil
	}
	ready, e := state.primaryRoot.arena.Acquire(state.primaryRoot.ref, w)
	if e != nil || !ready {
		if released, re := idx.registry.UnregisterWithWorkV5(id, w); !released || re != nil {
			panic("owned primary capture lost admitted registry unwind")
		}
		return nil, hint, true, e
	}
	if needsVlog {
		w.Reserve(1, vlogBytes)
		vm.Acquire(state.ValueLogSet)
	}
	if pins != nil {
		w.Reserve(1, 2*uint64(unsafe.Sizeof(leafGenerationPinSet{})))
		pins.holders++
	}
	w.Reserve(1, wrapperBytes)
	s := &Snapshot{privatePrimaryOwnerV5: true, primaryRoot: state.primaryRoot, db: db, idx: idx, state: state, vlogManager: vm, vlogPinned: needsVlog, registryID: id, registryShardHint: hint, systemRootPublishEpoch: view.systemRootPublishEpoch, leafGenerationPinSet: pins}
	s.generation.Store(1)
	s.readState.Store(1) // the sole private owner's retained read edge
	if pins != nil {
		s.leafGenerationIDs = state.LeafGenerations.GenerationOrder
		if pins.pinned {
			s.leafGenerationPinnedIDs = s.leafGenerationIDs
		}
	}
	s.reader.reconfigure(state.ValueLogSet, db.leafPageReadCache)
	s.tree.Reset(idx.pager, &s.reader, state.RootPageID)
	s.tree.SetNegativeFilter(view.negativeFilter)
	s.treePager, s.treeRoot = idx.pager, state.RootPageID
	lease := &primaryBaseReadLeaseV5{snapshot: s, pinned: true, wrapperGeneration: 1, root: s.primaryRoot}
	w.Reserve(1, linkBytes)
	db.stableIndexCaptures.Add(1)
	s.stableIndexCapture = true
	s.stableIndexCaptureCounter = &db.stableIndexCaptures
	c := &primarySnapshotCustodyV5{snapshot: s, lease: lease, ownerDB: db, next: db.primaryCustodyHead}
	if c.next != nil {
		c.next.prev = c
	}
	db.primaryCustodyHead = c
	return c, hint, true, nil
}

// Preparation uses this owner's existing read pin. It neither adds a second
// public lifetime nor claims finalization before an immutable proof can finish.
func (c *primarySnapshotCustodyV5) prepareBaseKeyProofV6(key []byte, revision page.EntryRevision, w *iterator.OrdinalScanWork) (*primaryarena.BaseKeyProofV5, bool, error) {
	if c == nil || w == nil {
		return nil, false, primaryarena.ErrStale
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))) {
		return nil, false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal || c.retirementClaimed || c.done || c.ownerDB == nil {
		return nil, false, primaryarena.ErrStale
	}
	return c.ownerDB.preparePrimaryBaseKeyProofWithReadLeaseV6(c.lease, key, revision, w)
}
