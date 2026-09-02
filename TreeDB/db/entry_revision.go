package db

import (
	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

func (db *DB) seedEntryRevisionFloor() {
	if db == nil {
		return
	}
	floor := db.meta.MaxEntryRevision
	if db.meta.CommitSeq > floor {
		floor = db.meta.CommitSeq
	}
	db.entryRevisionFloor.Store(floor)
}

func (db *DB) nextEntryRevision() page.EntryRevision {
	if db == nil {
		return page.LegacyEntryRevision
	}
	return page.EntryRevision(db.entryRevisionFloor.Add(1))
}

func (db *DB) advanceEntryRevisionFloor(revision page.EntryRevision) {
	if db == nil || revision == page.LegacyEntryRevision {
		return
	}
	target := uint64(revision)
	for {
		cur := db.entryRevisionFloor.Load()
		if cur >= target {
			return
		}
		if db.entryRevisionFloor.CompareAndSwap(cur, target) {
			return
		}
	}
}

func (db *DB) assignBatchEntryRevisions(batch *batchpkg.Batch) page.EntryRevision {
	if db == nil || batch == nil || batch.Len() == 0 {
		return page.LegacyEntryRevision
	}
	maxRevision, hasLegacy := batch.PointRevisionStats()
	db.advanceEntryRevisionFloor(maxRevision)
	if hasLegacy {
		return batch.AssignLegacyPointRevisions(db.nextEntryRevision())
	}
	return maxRevision
}
