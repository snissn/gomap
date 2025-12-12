package db

import (
	"fmt"

	"github.com/snissn/gomap-gemini/TreeDB/internal/merging"
	"github.com/snissn/gomap-gemini/TreeDB/tree"
)

// ...

// Iterator returns an iterator.
func (db *DB) Iterator(start, end []byte) (merging.Iterator, error) {
	snap := db.AcquireSnapshot()
	it := snap.tree.Iterator(start, end)
	return &DBIterator{snap: snap, iter: it}, nil
}

// ReverseIterator returns a reverse iterator.
func (db *DB) ReverseIterator(start, end []byte) (merging.Iterator, error) {
	snap := db.AcquireSnapshot()
	it := snap.tree.ReverseIterator(start, end)
	return &DBIterator{snap: snap, iter: it}, nil
}

// Stats returns database statistics.
func (db *DB) Stats() map[string]string {
	stats := make(map[string]string)
	stats["cosmos.db.type"] = "treedb"
	
	state := db.state.Load()
	stats["treedb.commit_seq"] = fmt.Sprintf("%d", state.CommitSeq)
	stats["treedb.root_page"] = fmt.Sprintf("%d", state.RootPageID)
	
	db.pager.PageCount() // Accessing pager safely (read-only mostly)
	stats["treedb.pages.total"] = fmt.Sprintf("%d", db.pager.PageCount())
	
	stats["treedb.slabs.active_id"] = fmt.Sprintf("%d", db.slabManager.ActiveSlabID())
	stats["treedb.slabs.zombies"] = fmt.Sprintf("%d", db.slabManager.ZombieCount())
	
	return stats
}

// Print debugs the tree (simple dump).
func (db *DB) Print() error {
	// Not implemented fully
	return nil
}
