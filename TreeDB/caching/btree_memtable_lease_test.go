package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestBTreeMemLeaseKeepCoversDefaultShards(t *testing.T) {
	if got, want := btreeMemLeaseKeepForShardCount(0), defaultMemtableShards(); got != want {
		t.Fatalf("default BTree lease keep=%d want default shards %d", got, want)
	}
	if got, want := btreeMemLeaseKeepForShardCount(maxBTreeMemLeases+4), maxBTreeMemLeases; got != want {
		t.Fatalf("capped BTree lease keep=%d want %d", got, want)
	}
}

func TestTrimBTreeMemLeasesUsesShardBudget(t *testing.T) {
	db := &DB{
		mutableShards:  make([]memShard, 4),
		btreeMemLeases: make([]*memtable.BTree, maxBTreeMemLeases),
	}
	for i := range db.btreeMemLeases {
		db.btreeMemLeases[i] = memtable.NewBTree()
	}

	db.trimBTreeMemLeases(db.btreeMemLeaseKeep())
	if got, want := len(db.btreeMemLeases), len(db.mutableShards); got != want {
		t.Fatalf("retained BTree leases=%d want %d", got, want)
	}
	for i, mt := range db.btreeMemLeases {
		if mt == nil {
			t.Fatalf("retained BTree lease %d is nil", i)
		}
	}
}
