package db

import "sync"

type leafGenerationView struct {
	CurrentGenerationID uint64
	GenerationOrder     []uint64
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
		if gen.State == leafGenerationStateDeleted {
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
	return newLeafGenerationView(db.leafGenerationManifest)
}

type leafGenerationPinTracker struct {
	mu     sync.Mutex
	counts map[uint64]uint64
}

func (t *leafGenerationPinTracker) pin(ids []uint64) {
	if len(ids) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.counts == nil {
		t.counts = make(map[uint64]uint64, len(ids))
	}
	for _, id := range ids {
		if id == 0 {
			continue
		}
		t.counts[id]++
	}
}

func (t *leafGenerationPinTracker) unpin(ids []uint64) {
	if len(ids) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.counts) == 0 {
		return
	}
	for _, id := range ids {
		if id == 0 {
			continue
		}
		count := t.counts[id]
		if count <= 1 {
			delete(t.counts, id)
			continue
		}
		t.counts[id] = count - 1
	}
}

func (t *leafGenerationPinTracker) count(id uint64) uint64 {
	if id == 0 {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.counts[id]
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

func (db *DB) leafGenerationPinCountForTesting(id uint64) uint64 {
	if db == nil {
		return 0
	}
	return db.leafGenerationPins.count(id)
}
