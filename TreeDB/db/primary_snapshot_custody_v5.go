package db

import (
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
)

// primarySnapshotCustodyV5 transfers one private proof owner's whole Snapshot.
// The retained read pin prevents public Close/endRead from completing retirement
// during transfer. snapshot remains real registry/DATA/vlog/primary custody,
// not a completed Finish receipt. The caller exclusively owns the proof lease;
// exported iterator/foreground-read lifetimes cannot be transferred here.
type primarySnapshotCustodyV5 struct {
	mu                                             sync.Mutex
	ownerDB                                        *DB
	next, prev                                     *primarySnapshotCustodyV5
	terminal                                       bool
	done                                           bool
	primaryQueue                                   *primaryarena.ReleaseQueue
	primaryDropped                                 bool
	vlogCursor                                     *valuelog.TerminalSetRetirementV5
	vlogDone, registryDone, captureDone, leaseDone bool
	snapshot                                       *Snapshot
	lease                                          *primaryBaseReadLeaseV5
	pinReleased                                    bool
	retirementClaimed                              bool
	leafStarted                                    bool
	leafDone                                       bool
	leafRefs                                       []*leafGenerationPinRef
	leafIDs                                        []uint64
	leafAt                                         int
	leafLookupAt                                   int
	leafClearAt                                    int
}

func claimPrimarySnapshotCustodyV5(lease *primaryBaseReadLeaseV5, w *iterator.OrdinalScanWork) (*primarySnapshotCustodyV5, bool, error) {
	if w == nil {
		return nil, false, primaryarena.ErrStale
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(primaryBaseReadLeaseV5{}))+2*uint64(unsafe.Sizeof(Snapshot{}))+uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))) {
		return nil, false, nil
	}
	if lease == nil {
		return nil, false, primaryarena.ErrStale
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	s := lease.snapshot
	if !lease.pinned || s == nil || s.generation.Load() != lease.wrapperGeneration {
		return nil, false, primaryarena.ErrStale
	}
	if s.db == nil {
		return nil, false, primaryarena.ErrStale
	}
	db := s.db
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(DB{}))+16) {
		return nil, false, nil
	}
	db.primaryCustodyMu.Lock()
	defer db.primaryCustodyMu.Unlock()
	if db.closing.Load() {
		return nil, false, primaryarena.ErrStale
	}
	s.iteratorMu.Lock()
	defer s.iteratorMu.Unlock()
	if !s.privatePrimaryOwnerV5 || len(s.iterators) != 0 || s.foregroundReadMarked || s.foregroundReadEnd != nil || s.stableIndexCaptureTransferred || s.finalized.Load() {
		return nil, false, primaryarena.ErrStale
	}
	// Claim the ordinary finalization gate before detaching the read edge.
	// A transient endRead cannot become a last-retirement helper.
	s.closed.Store(true)
	s.readState.Or(snapshotReadClosedBit)
	if !s.finalized.CompareAndSwap(false, true) {
		return nil, false, primaryarena.ErrStale
	}
	c := &primarySnapshotCustodyV5{snapshot: s, lease: lease, ownerDB: db, next: db.primaryCustodyHead, retirementClaimed: true}
	if c.next != nil {
		c.next.prev = c
	}
	db.primaryCustodyHead = c
	return c, true, nil
}

func (c *primarySnapshotCustodyV5) ReleaseReadPinV5(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil {
		return false, primaryarena.ErrStale
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal || !c.retirementClaimed {
		return false, primaryarena.ErrStale
	}
	return c.releaseReadPinV5(w)
}
func (c *primarySnapshotCustodyV5) releaseReadPinV5(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, primaryarena.ErrStale
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))) {
		return false, nil
	}
	if c == nil || c.snapshot == nil || c.lease == nil {
		return false, primaryarena.ErrStale
	}
	if c.pinReleased {
		return true, nil
	}
	ready, debt := c.lease.ReleasePinV5(w)
	if !ready {
		return false, nil
	}
	if debt != c.snapshot {
		return false, primaryarena.ErrStale
	}
	c.pinReleased = true
	return true, nil
}

// ReleaseLeafPinsV5 detaches exactly one PinSet holder, then drops every actual
// ref with a separately admitted CAS attempt. New ordinary holders may repin
// the immutable refs while this old holder's decrements are pending. IDs-only
// snapshots perform one charged tracker lookup per source ID first. This ends
// only leaf custody; all other Snapshot resources remain held.
func (c *primarySnapshotCustodyV5) ReleaseLeafPinsV5(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if c == nil {
		return false, false, primaryarena.ErrStale
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal {
		return false, false, primaryarena.ErrStale
	}
	return c.releaseLeafPinsV5(w)
}
func (c *primarySnapshotCustodyV5) releaseLeafPinsV5(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if w == nil {
		return false, false, primaryarena.ErrStale
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))) {
		return false, false, nil
	}
	if c == nil || c.snapshot == nil || !c.pinReleased {
		return false, false, primaryarena.ErrStale
	}
	if c.leafDone {
		return true, false, nil
	}
	s := c.snapshot
	progress := false
	if !c.leafStarted {
		bytes := 2*uint64(unsafe.Sizeof(Snapshot{})) + 2*uint64(unsafe.Sizeof(leafGenerationPinSet{}))
		if s.leafGenerationPinSet == nil && len(s.leafGenerationRefs) == 0 {
			bytes += uint64(len(s.leafGenerationIDs)) * uint64(unsafe.Sizeof((*leafGenerationPinRef)(nil)))
		}
		if !w.Reserve(1, bytes) {
			return false, false, nil
		}
		switch {
		case s.db == nil:
		case s.leafGenerationPinSet != nil:
			set := s.leafGenerationPinSet
			set.mu.Lock()
			if set.holders == 0 {
				set.mu.Unlock()
				return false, false, primaryarena.ErrStale
			}
			set.holders--
			if set.holders == 0 && set.pinned {
				c.leafRefs = set.refs
				set.pinned = false
			}
			set.mu.Unlock()
		case len(s.leafGenerationRefs) != 0:
			c.leafRefs = s.leafGenerationRefs
		case len(s.leafGenerationIDs) != 0:
			c.leafIDs = s.leafGenerationIDs
			c.leafRefs = make([]*leafGenerationPinRef, len(c.leafIDs))
		}
		c.leafStarted = true
		progress = true
	}
	for c.leafLookupAt < len(c.leafIDs) {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(leafGenerationPinTracker{}))+2*uint64(unsafe.Sizeof(leafGenerationPinRef{}))+16) {
			return false, progress, nil
		}
		id := c.leafIDs[c.leafLookupAt]
		t := &s.db.leafGenerationPins
		t.mu.RLock()
		c.leafRefs[c.leafLookupAt] = t.refs[id]
		t.mu.RUnlock()
		c.leafLookupAt++
		progress = true
	}
	for c.leafAt < len(c.leafRefs) {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(leafGenerationPinRef{}))+16) {
			return false, progress, nil
		}
		ref := c.leafRefs[c.leafAt]
		if ref == nil {
			c.leafAt++
			progress = true
			continue
		}
		count := ref.count.Load()
		if count <= 0 || ref.count.CompareAndSwap(count, count-1) {
			c.leafAt++
		}
		// A failed CAS is charged and retried only through this same cursor.
		progress = true
	}
	for c.leafClearAt < len(s.leafGenerationRefs) {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof((*leafGenerationPinRef)(nil)))) {
			return false, progress, nil
		}
		s.leafGenerationRefs[c.leafClearAt] = nil
		c.leafClearAt++
		progress = true
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Snapshot{}))+2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))) {
		return false, progress, nil
	}
	s.leafGenerationIDs = nil
	s.leafGenerationPinnedIDs = nil
	s.leafGenerationPinSet = nil
	s.leafGenerationRefs = s.leafGenerationRefs[:0]
	c.leafIDs, c.leafRefs = nil, nil
	c.leafDone = true
	return true, true, nil
}

// terminalStepV5 takes this exact registered Snapshot cursor. DB retains all DATA
// generations and the manager until every registered cursor completes. Registry
// removal therefore transfers physical generation retirement to DB's terminal
// owner, rather than calling ordinary maybeReleaseRetiredIndex as hidden work.
func (c *primarySnapshotCustodyV5) terminalStepV5(db *DB, w *iterator.OrdinalScanWork) (bool, bool, error) {
	return c.stepV6(db, w, true)
}

// liveStepV6 releases the same private owner while DATA is still owned by DB.
// Its stable capture excludes index namespace cutover. Ordinary materialization
// inside that physical DATA file remains legal. Terminal Close can take this
// exact partially consumed cursor, including live Set physical debt.
func (c *primarySnapshotCustodyV5) liveStepV6(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if c == nil {
		return false, false, primaryarena.ErrStale
	}
	return c.stepV6(nil, w, false)
}

func (c *primarySnapshotCustodyV5) stepV6(db *DB, w *iterator.OrdinalScanWork, terminal bool) (bool, bool, error) {
	if c == nil || w == nil {
		return false, false, primaryarena.ErrStale
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))+2*uint64(unsafe.Sizeof(DB{}))) {
		return false, false, nil
	}
	if c.done {
		return true, false, nil
	}
	if db == nil {
		db = c.ownerDB
	}
	if db == nil {
		return false, false, primaryarena.ErrStale
	}
	if c.ownerDB != db || db.closing.Load() != terminal || (!terminal && c.terminal) {
		return false, false, primaryarena.ErrStale
	}
	if terminal {
		c.terminal = true
	}
	progress := false
	if !terminal && !c.captureDone {
		// The DB still owns this exact DATA file. No ordinary retired-pager
		// finalizer or ghost queue can run behind this cursor.
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(indexGen{}))+2*uint64(unsafe.Sizeof(Snapshot{}))+16) {
			return false, false, nil
		}
		db.idxMu.Lock()
		current := db.idx.Load() == c.snapshot.idx && c.snapshot.stableIndexCapture && c.snapshot.stableIndexCaptureCounter == &db.stableIndexCaptures
		db.idxMu.Unlock()
		if !current {
			return false, false, primaryarena.ErrStale
		}
		snap := c.snapshot
		if c.vlogCursor == nil && !c.vlogDone && snap.vlogPinned && snap.state != nil && snap.state.ValueLogSet != nil && snap.vlogManager != nil {
			cursor, ready, e := snap.vlogManager.PrepareLiveSetRetirementV6(snap.state.ValueLogSet, w)
			if e != nil || !ready {
				return false, false, e
			}
			c.vlogCursor = cursor
			progress = true
		}
	}
	if !c.retirementClaimed {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Snapshot{}))+2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))) {
			return false, progress, nil
		}
		s := c.snapshot
		s.iteratorMu.Lock()
		if !s.privatePrimaryOwnerV5 || len(s.iterators) != 0 || s.foregroundReadMarked || s.foregroundReadEnd != nil || s.stableIndexCaptureTransferred || s.finalized.Load() {
			s.iteratorMu.Unlock()
			return false, progress, primaryarena.ErrStale
		}
		s.closed.Store(true)
		s.readState.Or(snapshotReadClosedBit)
		if !s.finalized.CompareAndSwap(false, true) {
			s.iteratorMu.Unlock()
			return false, progress, primaryarena.ErrStale
		}
		s.iteratorMu.Unlock()
		c.retirementClaimed = true
		progress = true
	}
	if !c.pinReleased {
		ready, e := c.releaseReadPinV5(w)
		if e != nil || !ready {
			return false, false, e
		}
		progress = true
	}
	if !c.leafDone {
		ready, p, e := c.releaseLeafPinsV5(w)
		progress = progress || p
		if e != nil || !ready {
			return false, progress, e
		}
	}

	s := c.snapshot
	if s.primaryRoot != nil {
		root := s.primaryRoot
		if c.primaryQueue == nil {
			queue, ready, e := root.arena.NewReleaseQueue(w)
			if e != nil || !ready {
				return false, progress, e
			}
			c.primaryQueue = queue
			progress = true
		}
		if !c.primaryDropped {
			ready, e := root.arena.DropToReleaseQueue(root.ref, c.primaryQueue, w)
			if e != nil || !ready {
				return false, progress, e
			}
			c.primaryDropped = true
			progress = true
		}
		for {
			ready, p, e := root.arena.ReleaseQueueStep(c.primaryQueue, w)
			progress = progress || p
			if e != nil || !ready {
				return false, progress, e
			}
			// ready certifies only this step. A charged empty observation
			// certifies this owner's entire closure is retired.
			if !p {
				break
			}
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Snapshot{}))) {
			return false, progress, nil
		}
		s.primaryRoot = nil
		progress = true
	}
	if !c.vlogDone {
		if terminal && db.terminalSnapshotHookV5 != nil {
			if e := db.terminalSnapshotHookV5("vlog"); e != nil {
				return false, progress, e
			}
		}
		if s.vlogPinned && s.state != nil && s.state.ValueLogSet != nil && s.vlogManager != nil {
			if c.vlogCursor == nil {
				var cursor *valuelog.TerminalSetRetirementV5
				var ready bool
				var e error
				if terminal {
					cursor, ready, e = s.vlogManager.PrepareTerminalSetRetirementV5(s.state.ValueLogSet, w)
				} else {
					cursor, ready, e = s.vlogManager.PrepareLiveSetRetirementV6(s.state.ValueLogSet, w)
				}
				if e != nil || !ready {
					return false, progress, e
				}
				c.vlogCursor = cursor
				progress = true
			}
			ready, p, e := c.vlogCursor.Step(w)
			progress = progress || p
			if e != nil || !ready {
				return false, progress, e
			}
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Snapshot{}))) {
			return false, progress, nil
		}
		s.vlogPinned = false
		c.vlogDone = true
		progress = true
	}
	if !c.registryDone {
		if s.idx != nil && s.registryID != 0 {
			ready, e := s.idx.registry.UnregisterWithWorkV5(s.registryID, w)
			if e != nil || !ready {
				return false, progress, e
			}
			// Persist completion immediately: later wrapper accounting may yield.
			s.registryID = 0
		}
		c.registryDone = true
		progress = true
	}
	if !c.captureDone {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Snapshot{}))+16) {
			return false, progress, nil
		}
		if !terminal {
			db.idxMu.Lock()
		}
		if s.stableIndexCapture && s.stableIndexCaptureCounter != nil && !s.stableIndexCaptureTransferred {
			s.stableIndexCaptureCounter.Add(-1)
		}
		if !terminal {
			db.idxMu.Unlock()
		}
		s.stableIndexCapture = false
		s.stableIndexCaptureCounter = nil
		c.captureDone = true
		progress = true
	}
	if !c.leaseDone {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(primaryBaseReadLeaseV5{}))) {
			return false, progress, nil
		}
		c.lease.mu.Lock()
		c.lease.snapshot, c.lease.root, c.lease.generation = nil, nil, nil
		c.lease.mu.Unlock()
		c.leaseDone = true
		progress = true
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Snapshot{}))+2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))) {
		return false, progress, nil
	}
	if !terminal {
		db.primaryCustodyMu.Lock()
		records := uint64(1)
		bytes := 2*uint64(unsafe.Sizeof(DB{})) + 2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))
		if c.prev != nil {
			records++
			bytes += 2 * uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))
		}
		if c.next != nil {
			records++
			bytes += 2 * uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))
		}
		if !w.Reserve(records, bytes) {
			db.primaryCustodyMu.Unlock()
			return false, progress, nil
		}
		if c.prev != nil {
			c.prev.next = c.next
		} else if db.primaryCustodyHead == c {
			db.primaryCustodyHead = c.next
		} else {
			db.primaryCustodyMu.Unlock()
			return false, progress, primaryarena.ErrStale
		}
		if c.next != nil {
			c.next.prev = c.prev
		}
		c.prev, c.next = nil, nil
		db.primaryCustodyMu.Unlock()
	}
	// Private eligibility forbids exported iterators and foreground callbacks.
	// All retained slices are detached by their finite phases; no pool Put scan.
	s.privatePrimaryOwnerV5 = false
	s.tree.Reset(nil, nil, 0)
	s.rootTreesMu.Lock()
	s.rootTrees = nil
	s.rootTreesMu.Unlock()
	s.db = nil
	s.idx = nil
	s.state = nil
	s.vlogManager = nil
	s.reader = valueReader{}
	s.leafGenerationRefs = nil
	s.leafGenerationIDs = nil
	s.leafGenerationPinnedIDs = nil
	s.treePager = nil
	s.treeRoot = 0
	c.primaryQueue = nil
	c.vlogCursor = nil
	c.done = true
	if !terminal {
		c.ownerDB = nil
	}
	return true, true, nil
}
