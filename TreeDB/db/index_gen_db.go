package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
)

// DrainRetiredIndexGenerations reuses the original ghost queue before a bounded
// maintenance constructor needs its capacity. The admitted caller supplies its
// teardown lease and holds no writer, root-reuse or publication-build lock.
// Live reader floors still exclude retirement; no age delay is needed for an
// already unreachable generation when making explicit maintenance progress.
func (db *DB) DrainRetiredIndexGenerations(lease *StableResourceCaptureLease) error {
	if err := lease.ValidateDBV1(db); err != nil {
		return err
	}
	if err := db.publicationPoisonedError(); err != nil {
		return err
	}
	if db.ghostManager != nil {
		db.ghostManager.scavenge(0)
	}
	// The original scavenge route retains actual disposal failures in bgErr.
	// Observe that exact graph before allowing another publication.
	if err := db.backgroundError(); err != nil {
		db.publicationPoisoned.Store(true)
		return errors.Join(err, ErrRecoveryRequired)
	}
	return nil
}

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
	}
	gen = nil
	if charge != 0 {
		metadata.RemovePending(charge)
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
