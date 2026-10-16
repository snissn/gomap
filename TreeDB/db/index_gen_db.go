package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
)

func (db *DB) nextIndexID() uint64 {
	db.idxMu.Lock()
	defer db.idxMu.Unlock()
	if db.idxNext == 0 {
		db.idxNext = 1
	}
	id := db.idxNext
	db.idxNext++
	return id
}

func (db *DB) trackIndex(gen *indexGen) {
	if gen == nil {
		return
	}
	db.idxMu.Lock()
	defer db.idxMu.Unlock()
	if db.idxAll == nil {
		db.idxAll = make(map[uint64]*indexGen, 2)
	}
	db.idxAll[gen.id] = gen
	if gen.id >= db.idxNext {
		db.idxNext = gen.id + 1
	}
}

func (db *DB) releaseIndex(gen *indexGen) {
	if gen == nil {
		return
	}
	if gen.release() != 0 {
		return
	}

	db.idxMu.Lock()
	current := db.idx.Load()
	if current == gen {
		// DB should hold a ref for the current generation. Re-acquire and keep.
		gen.acquire()
		db.idxMu.Unlock()
		return
	}
	if db.idxAll != nil {
		// Keep retired generations tracked while snapshot readers remain pinned
		// so Snapshot.Close can release them once the registry drains.
		if gen.registry == nil || gen.registry.MinPinnedSeq() == math.MaxUint64 {
			delete(db.idxAll, gen.id)
		} else {
			db.idxMu.Unlock()
			return
		}
	}
	db.idxMu.Unlock()

	// Ghost instead of immediate close
	var metadata *retainedalloc.Owner
	var charge uint64
	var err error
	if db.ghostManager != nil {
		metadata, charge, err = db.ghostManager.add(gen)
	} else {
		metadata, charge, err = gen.closeWithCompletionV6()
	}
	if err != nil {
		db.retainOriginalCleanupError(gen.originalCloseFailure(err))
		db.quiesceFailedRetiredGeneration(gen)
	}
	gen = nil
	if charge != 0 {
		metadata.RemovePending(charge)
	}
}

// maybeReleaseRetiredIndex releases a retired index generation once all reader
// pins have drained and no explicit refs remain.
func (db *DB) maybeReleaseRetiredIndex(gen *indexGen) {
	if gen == nil {
		return
	}
	db.idxMu.Lock()
	current := db.idx.Load()
	if current == gen {
		db.idxMu.Unlock()
		return
	}
	if gen.refs.Load() != 0 {
		db.idxMu.Unlock()
		return
	}
	if gen.registry != nil && gen.registry.MinPinnedSeq() != math.MaxUint64 {
		db.idxMu.Unlock()
		return
	}
	if db.idxAll == nil {
		db.idxMu.Unlock()
		return
	}
	if _, ok := db.idxAll[gen.id]; !ok {
		db.idxMu.Unlock()
		return
	}
	delete(db.idxAll, gen.id)
	db.idxMu.Unlock()

	var metadata *retainedalloc.Owner
	var charge uint64
	var err error
	if db.ghostManager != nil {
		metadata, charge, err = db.ghostManager.add(gen)
	} else {
		metadata, charge, err = gen.closeWithCompletionV6()
	}
	if err != nil {
		db.retainOriginalCleanupError(gen.originalCloseFailure(err))
		db.quiesceFailedRetiredGeneration(gen)
	}
	gen = nil
	if charge != 0 {
		metadata.RemovePending(charge)
	}
}

// Quiescence is separate from physical disposal. A retired failed generation
// cannot regain write admission; a PRIMARY still shared by the actual current
// generation remains eligible and is quiesced only at terminal DB withdrawal.
func (db *DB) quiesceFailedRetiredGeneration(gen *indexGen) {
	if gen == nil || gen.primary == nil {
		return
	}
	db.idxMu.Lock()
	current := db.idx.Load()
	eligible := current == gen || (current != nil && current.primary == gen.primary)
	physicalPager := gen.primary.Pager()
	db.idxMu.Unlock()
	if !eligible {
		physicalPager.QuiesceBackgroundGrowth()
	}
}

func (db *DB) closeAllIndexes() error {
	db.idxMu.Lock()
	var gens []*indexGen
	for _, g := range db.idxAll {
		gens = append(gens, g)
	}
	db.idxAll = nil
	db.idx.Store(nil)
	db.idxMu.Unlock()

	var errs []error
	for i, g := range gens {
		// Current publication is already nil and writers have drained. Stop
		// growth without consuming any physical owner or failed debt.
		db.quiesceFailedRetiredGeneration(g)
		metadata, charge, err := g.closeWithCompletionV6()
		if err != nil {
			db.retainOriginalCleanupError(g.originalCloseFailure(err))
			errs = append(errs, err)
		}
		gens[i], g = nil, nil
		if charge != 0 {
			metadata.RemovePending(charge)
		}
	}
	return errors.Join(errs...)
}

// Prune borrows original wrapper aliases before terminal discovery withdrawal.
// Physical refs retain their existing retirement meaning.
func (db *DB) acquireIndexWithWrapperBorrow() *indexGen {
	db.idxMu.Lock()
	defer db.idxMu.Unlock()
	idx := db.idx.Load()
	if idx != nil {
		idx.retainWrapperBorrow()
		idx.acquire()
	}
	return idx
}
