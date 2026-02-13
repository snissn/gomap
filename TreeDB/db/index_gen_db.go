package db

import (
	"errors"
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
	if db.ghostManager != nil {
		db.ghostManager.add(gen)
	} else {
		_ = gen.close()
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

	if db.ghostManager != nil {
		db.ghostManager.add(gen)
	} else {
		_ = gen.close()
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
	for _, g := range gens {
		if err := g.close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
