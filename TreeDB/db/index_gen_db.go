package db

import (
	"errors"
	"time"
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
		delete(db.idxAll, gen.id)
	}
	db.idxMu.Unlock()

	// Ghost instead of immediate close
	if db.ghostManager != nil {
		db.ghostManager.add(gen)
		// Opportunistically scavenge old ghosts
		db.ghostManager.scavenge(5 * time.Second)
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

	if db.ghostManager != nil {
		db.ghostManager.closeAll()
	}

	var errs []error
	for _, g := range gens {
		if err := g.close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
