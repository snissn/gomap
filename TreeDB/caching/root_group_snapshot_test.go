package caching

import (
	"encoding/binary"
	"testing"
)

func rootGroupTestShardKey(t *testing.T, db *DB, shard int) []byte {
	t.Helper()
	if db == nil {
		t.Fatal("nil db")
	}
	var key [8]byte
	for i := uint64(0); ; i++ {
		binary.BigEndian.PutUint64(key[:], i)
		if db.shardIndex(key[:]) == shard {
			return append([]byte(nil), key[:]...)
		}
	}
}

func TestRootDomainSnapshotFromCachedSnapshot_UsesPerRootPublishedView(t *testing.T) {
	db := &DB{
		mutableShards:    make([]memShard, 2),
		mutableShardMask: 1,
	}

	key0 := rootGroupTestShardKey(t, db, 0)
	key1 := rootGroupTestShardKey(t, db, 1)
	pub0 := newRootDomainTestTable(t, rootDomainTestOp{key: string(key0), value: "published-0"})
	pub1 := newRootDomainTestTable(t, rootDomainTestOp{key: string(key1), value: "published-1"})

	snap := &Snapshot{
		db: db,
		publishedRoots: &publishedRootSet{
			generation: 44,
			pointShards: []publishedRootRef{
				{lookup: pub0, rootID: 101},
				{lookup: pub1, rootID: 202},
			},
		},
	}

	root0 := rootDomainSnapshotFromCachedSnapshot(snap, key0)
	if root0.publishedRootID != 101 {
		t.Fatalf("root0 published id=%d want 101", root0.publishedRootID)
	}
	assertRootDomainVisibleValue(t, root0, string(key0), "published-0")

	root1 := rootDomainSnapshotFromCachedSnapshot(snap, key1)
	if root1.publishedRootID != 202 {
		t.Fatalf("root1 published id=%d want 202", root1.publishedRootID)
	}
	assertRootDomainVisibleValue(t, root1, string(key1), "published-1")
}

func TestRootDomainIteratorSnapshotFromCachedSnapshot_UsesPinnedPublishedSet(t *testing.T) {
	snap := &Snapshot{
		publishedRoots: &publishedRootSet{
			generation: 77,
			iterator: publishedRootRef{
				lookup: newRootDomainTestTable(t, rootDomainTestOp{key: "iter/a", value: "published-a"}),
				rootID: 303,
			},
		},
	}

	iterSnap := rootDomainIteratorSnapshotFromCachedSnapshot(snap)
	if iterSnap.publishedRootID != 303 {
		t.Fatalf("iterator published id=%d want 303", iterSnap.publishedRootID)
	}
	assertRootDomainVisibleValue(t, iterSnap, "iter/a", "published-a")
}
