package db

import (
	"sync"
	"sync/atomic"
)

type leafGenerationView struct {
	CurrentGenerationID uint64
	GenerationOrder     []uint64
	PinRefs             []*leafGenerationPinRef
	PinSet              *leafGenerationPinSet
	Generations         map[uint64]leafGenerationViewGeneration
	FileToGeneration    map[uint32]uint64
}

type leafGenerationViewGeneration struct {
	State   string
	FileIDs []uint32
}

func newLeafGenerationView(manifest *leafGenerationManifest) *leafGenerationView {
	if manifest == nil {
		return nil
	}
	view := &leafGenerationView{
		CurrentGenerationID: manifest.CurrentGenerationID,
		GenerationOrder:     make([]uint64, 0, len(manifest.Generations)),
		Generations:         make(map[uint64]leafGenerationViewGeneration, len(manifest.Generations)),
		FileToGeneration:    make(map[uint32]uint64),
	}
	for i := range manifest.Generations {
		gen := manifest.Generations[i]
		if gen.State == leafGenerationStateDeleted || gen.State == leafGenerationStateRetiring {
			continue
		}
		view.GenerationOrder = append(view.GenerationOrder, gen.GenerationID)
		files := append([]uint32(nil), gen.FileIDs...)
		view.Generations[gen.GenerationID] = leafGenerationViewGeneration{
			State:   gen.State,
			FileIDs: files,
		}
		for _, fileID := range files {
			view.FileToGeneration[fileID] = gen.GenerationID
		}
	}
	return view
}

func (db *DB) currentLeafGenerationView() *leafGenerationView {
	if db == nil || db.leafGenerationManifest == nil {
		return nil
	}
	view := newLeafGenerationView(db.leafGenerationManifest)
	if view != nil && len(view.GenerationOrder) > 0 {
		view.PinRefs = db.leafGenerationPins.refsForGenerationIDs(view.GenerationOrder)
		view.PinSet = newLeafGenerationPinSet(view.PinRefs)
	}
	return view
}

type leafGenerationPinRef struct {
	id    uint64
	count atomic.Int64
}

type leafGenerationPinSet struct {
	refs    []*leafGenerationPinRef
	holders atomic.Int64
	stale   atomic.Bool
	pinned  atomic.Bool
}

func newLeafGenerationPinSet(refs []*leafGenerationPinRef) *leafGenerationPinSet {
	if len(refs) == 0 {
		return nil
	}
	return &leafGenerationPinSet{refs: refs}
}

func (s *leafGenerationPinSet) retain(tracker *leafGenerationPinTracker) bool {
	if s == nil || tracker == nil {
		return false
	}
	if s.holders.Add(1) == 1 && s.stale.Load() {
		return s.pinIfNeeded(tracker)
	}
	return false
}

func (s *leafGenerationPinSet) release(tracker *leafGenerationPinTracker) {
	if s == nil || tracker == nil {
		return
	}
	if s.holders.Add(-1) == 0 {
		s.unpinIfNeeded(tracker)
	}
}

func (s *leafGenerationPinSet) markStale(tracker *leafGenerationPinTracker) {
	if s == nil || tracker == nil {
		return
	}
	if !s.stale.CompareAndSwap(false, true) {
		return
	}
	if s.holders.Load() > 0 {
		s.pinIfNeeded(tracker)
	}
}

func (s *leafGenerationPinSet) pinIfNeeded(tracker *leafGenerationPinTracker) bool {
	if s == nil || tracker == nil {
		return false
	}
	if s.pinned.CompareAndSwap(false, true) {
		tracker.pinRefs(s.refs)
		return true
	}
	return false
}

func (s *leafGenerationPinSet) unpinIfNeeded(tracker *leafGenerationPinTracker) {
	if s == nil || tracker == nil {
		return
	}
	if s.pinned.CompareAndSwap(true, false) {
		tracker.unpinRefs(s.refs)
	}
}

type leafGenerationPinTracker struct {
	mu   sync.RWMutex
	refs map[uint64]*leafGenerationPinRef
}

func (t *leafGenerationPinTracker) refsForGenerationIDs(ids []uint64) []*leafGenerationPinRef {
	if len(ids) == 0 {
		return nil
	}
	t.mu.RLock()
	allPresent := t.refs != nil
	if allPresent {
		for _, id := range ids {
			if id == 0 {
				continue
			}
			if t.refs[id] == nil {
				allPresent = false
				break
			}
		}
	}
	if allPresent {
		refs := make([]*leafGenerationPinRef, 0, len(ids))
		for _, id := range ids {
			if id == 0 {
				continue
			}
			if ref := t.refs[id]; ref != nil {
				refs = append(refs, ref)
			}
		}
		t.mu.RUnlock()
		return refs
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.refs == nil {
		t.refs = make(map[uint64]*leafGenerationPinRef, len(ids))
	}
	refs := make([]*leafGenerationPinRef, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		ref := t.refs[id]
		if ref == nil {
			ref = &leafGenerationPinRef{id: id}
			t.refs[id] = ref
		}
		refs = append(refs, ref)
	}
	return refs
}

func (t *leafGenerationPinTracker) lookupRefs(ids []uint64) []*leafGenerationPinRef {
	if len(ids) == 0 {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.refs) == 0 {
		return nil
	}
	refs := make([]*leafGenerationPinRef, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if ref := t.refs[id]; ref != nil {
			refs = append(refs, ref)
		}
	}
	return refs
}

func (t *leafGenerationPinTracker) pin(ids []uint64) {
	t.pinRefs(t.refsForGenerationIDs(ids))
}

func (t *leafGenerationPinTracker) pinRefs(refs []*leafGenerationPinRef) {
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		ref.count.Add(1)
	}
}

func (t *leafGenerationPinTracker) unpin(ids []uint64) {
	t.unpinRefs(t.lookupRefs(ids))
}

func (t *leafGenerationPinTracker) unpinRefs(refs []*leafGenerationPinRef) {
	for _, ref := range refs {
		t.unpinRef(ref)
	}
}

func (t *leafGenerationPinTracker) unpinRef(ref *leafGenerationPinRef) {
	if ref == nil {
		return
	}
	for {
		current := ref.count.Load()
		if current <= 0 {
			return
		}
		next := current - 1
		if !ref.count.CompareAndSwap(current, next) {
			continue
		}
		return
	}
}

func (t *leafGenerationPinTracker) pruneInactiveGenerationIDs(active []uint64) {
	activeSet := make(map[uint64]struct{}, len(active))
	for _, id := range active {
		if id != 0 {
			activeSet[id] = struct{}{}
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, ref := range t.refs {
		if ref == nil {
			delete(t.refs, id)
			continue
		}
		if _, ok := activeSet[id]; ok {
			continue
		}
		if ref.count.Load() == 0 {
			delete(t.refs, id)
		}
	}
}

func (t *leafGenerationPinTracker) count(id uint64) uint64 {
	if id == 0 {
		return 0
	}
	t.mu.RLock()
	ref := t.refs[id]
	t.mu.RUnlock()
	if ref == nil {
		return 0
	}
	count := ref.count.Load()
	if count <= 0 {
		return 0
	}
	return uint64(count)
}

func (db *DB) pinLeafGenerationIDs(ids []uint64) {
	if db == nil {
		return
	}
	db.leafGenerationPins.pin(ids)
}

func (db *DB) unpinLeafGenerationIDs(ids []uint64) {
	if db == nil {
		return
	}
	db.leafGenerationPins.unpin(ids)
}

func (db *DB) pinLeafGenerationRefs(refs []*leafGenerationPinRef) {
	if db == nil {
		return
	}
	db.leafGenerationPins.pinRefs(refs)
}

func (db *DB) unpinLeafGenerationRefs(refs []*leafGenerationPinRef) {
	if db == nil {
		return
	}
	db.leafGenerationPins.unpinRefs(refs)
}

func (db *DB) retainLeafGenerationPinSet(pinSet *leafGenerationPinSet) bool {
	if db == nil || pinSet == nil {
		return false
	}
	return pinSet.retain(&db.leafGenerationPins)
}

func (db *DB) releaseLeafGenerationPinSet(pinSet *leafGenerationPinSet) {
	if db == nil || pinSet == nil {
		return
	}
	pinSet.release(&db.leafGenerationPins)
}

func (db *DB) markLeafGenerationPinSetStale(pinSet *leafGenerationPinSet) {
	if db == nil || pinSet == nil {
		return
	}
	pinSet.markStale(&db.leafGenerationPins)
}

func (db *DB) leafGenerationPinCountForTesting(id uint64) uint64 {
	if db == nil {
		return 0
	}
	return db.leafGenerationPins.count(id)
}
