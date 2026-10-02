package db

import (
	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/tree"
)

// negativeRootCoverage carries the existing publication coordinates, not a
// second generation protocol. An uncovered base cannot acquire delta coverage.
type negativeRootCoverage struct {
	bootstrap                            bool
	idx                                  *indexGen
	baseSeq, baseRoot, nextSeq, nextRoot uint64
	filter                               *tree.NegativeFilter
}

func (c *negativeRootCoverage) matches(old *snapshotView, idx *indexGen, next *DBState) bool {
	return c != nil && c.filter != nil && (c.bootstrap || old.negativeFilter == c.filter) && c.idx == idx && old.idx == idx &&
		old.state.CommitSeq == c.baseSeq && old.state.RootPageID == c.baseRoot &&
		next.CommitSeq == c.nextSeq && next.RootPageID == c.nextRoot
}

func (db *DB) prepareNegativeCoverage(idx *indexGen, seq, root, nextRoot uint64, entries []batchpkg.Entry) *negativeRootCoverage {
	view := db.snapshotViewRO.Load()
	if view == nil || view.negativeFilter == nil || view.idx != idx || view.state == nil ||
		view.state.CommitSeq != seq || view.state.RootPageID != root {
		return nil
	}
	for i := range entries {
		view.negativeFilter.Add(entries[i].Key)
	}
	return &negativeRootCoverage{idx: idx, baseSeq: seq, baseRoot: root, nextSeq: seq + 1, nextRoot: nextRoot, filter: view.negativeFilter}
}

// Bootstrap runs once after replay/finalizers, before this handle is returned.
// A partial/error/budget-exhausted scan never authorizes a negative result.
func (db *DB) bootstrapNegativeFilter(budget int) {
	if budget < 8 || budget > 64<<20 {
		return
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	view := db.snapshotViewRO.Load()
	if view == nil || view.idx == nil || view.state == nil {
		return
	}
	f := tree.NewNegativeFilter(budget)
	snap := db.AcquireSnapshot()
	if snap == nil {
		return
	}
	defer snap.Close()
	it := snap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModeKeysOnly, IncludeTombstones: true})
	defer it.Close()
	entriesLeft := uint64(budget) * 8 / 10
	bytesLeft := uint64(budget) * 64
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if entriesLeft == 0 || uint64(len(key)) > bytesLeft {
			return
		}
		entriesLeft--
		bytesLeft -= uint64(len(key))
		f.Add(key)
	}
	if it.Error() != nil {
		return
	}
	c := &negativeRootCoverage{bootstrap: true, idx: view.idx, baseSeq: view.state.CommitSeq, baseRoot: view.state.RootPageID, nextSeq: view.state.CommitSeq, nextRoot: view.state.RootPageID, filter: f}
	db.publishSnapshotView(view.idx, view.state, view.vlogManager, c)
}
