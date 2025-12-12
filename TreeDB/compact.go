package treedb

import (
	"fmt"

	"treedb/internal/compaction"
)

// Compact runs a full blocking slab compaction cycle.
func (db *DB) Compact() error {
	if db == nil {
		return fmt.Errorf("treedb: nil db")
	}
	if db.closed.Load() {
		return fmt.Errorf("treedb: db closed")
	}
	c := compaction.New(db.pager, db.slabs, db.state, db.grave, db.pruner, &db.writerMu)
	return c.CompactAll()
}

