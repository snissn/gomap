package db

import (
	"sync"
	"sync/atomic"
)

type leafGenerationView struct {
	CurrentGenerationID uint64
	sourceManifest      *leafGenerationManifest
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
	activeGenerations := 0
	fileIDs := 0
	for i := range manifest.Generations {
		gen := manifest.Generations[i]
		if gen.State == leafGenerationStateDeleted || gen.State == leafGenerationStateRetiring {
			continue
		}
		activeGenerations++
		fileIDs += len(gen.FileIDs)
	}
	view := &leafGenerationView{
		CurrentGenerationID: manifest.CurrentGenerationID,
		sourceManifest:      manifest,
		GenerationOrder:     make([]uint64, 0, activeGenerations),
		Generations:         make(map[uint64]leafGenerationViewGeneration, activeGenerations),
		FileToGeneration:    make(map[uint32]uint64, fileIDs),
	}
	var fileBacking []uint32
	if fileIDs > 0 {
		fileBacking = make([]uint32, fileIDs)
	}
	fileCursor := 0
	for i := range manifest.Generations {
		gen := manifest.Generations[i]
		if gen.State == leafGenerationStateDeleted || gen.State == leafGenerationStateRetiring {
			continue
		}
		view.GenerationOrder = append(view.GenerationOrder, gen.GenerationID)
		files := fileBacking[fileCursor : fileCursor+len(gen.FileIDs)]
		copy(files, gen.FileIDs)
		fileCursor += len(gen.FileIDs)
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
	return db.leafGenerationViewForManifest(db.leafGenerationManifest)
}

func (db *DB) leafGenerationViewForManifest(manifest *leafGenerationManifest) *leafGenerationView {
	if db == nil || manifest == nil {
		return nil
	}
	if state := db.state.Load(); state != nil && state.LeafGenerations != nil && state.LeafGenerations.sourceManifest == manifest {
		return state.LeafGenerations
	}
	view := newLeafGenerationView(manifest)
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
	refs []*leafGenerationPinRef

	mu      sync.Mutex
	holders int64
	stale   bool
	pinned  bool
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holders++
	if s.stale && !s.pinned {
		tracker.pinRefs(s.refs)
		s.pinned = true
		return true
	}
	return false
}

func (s *leafGenerationPinSet) release(tracker *leafGenerationPinTracker) {
	if s == nil || tracker == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.holders == 0 {
		return
	}
	s.holders--
	if s.holders == 0 && s.pinned {
		tracker.unpinRefs(s.refs)
		s.pinned = false
	}
}

func (s *leafGenerationPinSet) markStale(tracker *leafGenerationPinTracker) {
	if s == nil || tracker == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stale {
		return
	}
	s.stale = true
	if s.holders > 0 && !s.pinned {
		tracker.pinRefs(s.refs)
		s.pinned = true
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
