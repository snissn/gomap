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
		if err := gen.close(); err != nil {
			db.trackIndex(gen)
		}
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
		if err := gen.close(); err != nil {
			db.trackIndex(gen)
		}
	}
}

func (db *DB) closeAllIndexes() ([]*indexGen, error) {
	db.idxMu.Lock()
	var gens []*indexGen
	for _, g := range db.idxAll {
		gens = append(gens, g)
	}
	// Keep actual failed generations on idxAll through retry. Admission is
	// closed by DB; this table is still the real terminal owner.
	db.idx.Store(nil)
	db.idxMu.Unlock()

	var errs []error
	for _, g := range gens {
		if err := g.closeForShutdownV1(); err != nil {
			errs = append(errs, err)
			continue
		}
		// Retain the real generation until the caller completes allocator/runtime
		// terminal, including retry after otherwise successful physical Close.
	}
	return gens, errors.Join(errs...)
}

// releaseClosedRetiredIndexMetadataV1 is the selected no-IO terminal seam.
// The exact handle-close proof precedes reader discharge; this only removes
// an already-closed, unreferenced generation from the existing DB table.
func (db *DB) releaseClosedRetiredIndexMetadataV1(gen *indexGen) {
	if gen == nil || !gen.handlesClosed.Load() {
		return
	}
	gen.closeMu.Lock()
	defer gen.closeMu.Unlock()
	if gen.creator != nil {
		return
	} // actual allocator terminal still owns this entry
	db.idxMu.Lock()
	defer db.idxMu.Unlock()
	if db.idx.Load() == gen || gen.refs.Load() != 0 || !gen.handlesClosed.Load() {
		return
	}
	if gen.registry != nil && gen.registry.MinPinnedSeq() != math.MaxUint64 {
		return
	}
	delete(db.idxAll, gen.id)
}
