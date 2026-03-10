package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

type rootDomainTestOp struct {
	key       string
	value     string
	tombstone bool
}

func newRootDomainTestTable(t *testing.T, ops ...rootDomainTestOp) memtable.Table {
	t.Helper()

	mt, err := newRootDomainTable(ops...)
	if err != nil {
		t.Fatalf("new memtable: %v", err)
	}
	return mt
}

func newRootDomainTable(ops ...rootDomainTestOp) (memtable.Table, error) {
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		return nil, err
	}
	for _, op := range ops {
		if op.tombstone {
			mt.Delete([]byte(op.key))
			continue
		}
		mt.Set([]byte(op.key), []byte(op.value))
	}
	mt.Freeze()
	return mt, nil
}

func TestRootDomainSnapshot_GetEntryNewestWinsAcrossRuns(t *testing.T) {
	t.Parallel()

	snap := rootDomainSnapshot{
		publishedRootID: 11,
		published: newRootDomainTestTable(t,
			rootDomainTestOp{key: "a", value: "published-a"},
			rootDomainTestOp{key: "b", value: "published-b"},
			rootDomainTestOp{key: "c", value: "published-c"},
		),
		immutables: []memtable.Table{
			newRootDomainTestTable(t,
				rootDomainTestOp{key: "b", value: "immutable-b"},
				rootDomainTestOp{key: "c", tombstone: true},
			),
		},
		mutable: newRootDomainTestTable(t,
			rootDomainTestOp{key: "c", value: "mutable-c"},
			rootDomainTestOp{key: "d", value: "mutable-d"},
		),
	}

	assertRootDomainVisibleValue(t, snap, "a", "published-a")
	assertRootDomainVisibleValue(t, snap, "b", "immutable-b")
	assertRootDomainVisibleValue(t, snap, "c", "mutable-c")
	assertRootDomainVisibleValue(t, snap, "d", "mutable-d")
}

func TestRootDomainSnapshot_TombstoneHidesOlderRuns(t *testing.T) {
	t.Parallel()

	snap := rootDomainSnapshot{
		publishedRootID: 22,
		published: newRootDomainTestTable(t,
			rootDomainTestOp{key: "x", value: "published-x"},
		),
		immutables: []memtable.Table{
			newRootDomainTestTable(t,
				rootDomainTestOp{key: "x", value: "immutable-x"},
			),
		},
		mutable: newRootDomainTestTable(t,
			rootDomainTestOp{key: "x", tombstone: true},
		),
	}

	if _, _, flags, found := snap.getEntry([]byte("x")); !found {
		t.Fatal("expected tombstoned key to resolve as found")
	} else if flags&node.FlagTombstone == 0 {
		t.Fatalf("flags=%08b want tombstone", flags)
	}
	if snap.hasVisibleKey([]byte("x")) {
		t.Fatal("expected tombstoned key to be invisible")
	}
}

func TestRootDomainState_SealMutablePreservesVisibility(t *testing.T) {
	t.Parallel()

	state := &rootDomainState{
		publishedRootID: 33,
		published: newRootDomainTestTable(t,
			rootDomainTestOp{key: "a", value: "published-a"},
		),
		mutable: newRootDomainTestTable(t,
			rootDomainTestOp{key: "a", value: "mutable-a"},
			rootDomainTestOp{key: "b", value: "mutable-b"},
		),
	}

	before := state.snapshot()
	assertRootDomainVisibleValue(t, before, "a", "mutable-a")
	assertRootDomainVisibleValue(t, before, "b", "mutable-b")

	nextMutable, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("new mutable: %v", err)
	}
	state.sealMutable(nextMutable)
	after := state.snapshot()

	assertRootDomainVisibleValue(t, after, "a", "mutable-a")
	assertRootDomainVisibleValue(t, after, "b", "mutable-b")
	if got, want := len(after.immutables), 1; got != want {
		t.Fatalf("immutable runs=%d want %d", got, want)
	}
	if after.mutable == nil {
		t.Fatal("expected new mutable run after seal")
	}
}

func TestRootDomainSnapshot_SameHandleVisibilityIncludesMutableRun(t *testing.T) {
	t.Parallel()

	state := &rootDomainState{publishedRootID: 44}
	mutable, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("new mutable: %v", err)
	}
	mutable.Set([]byte("k"), []byte("v"))
	state.mutable = mutable

	snap := state.snapshot()
	assertRootDomainVisibleValue(t, snap, "k", "v")
}

func BenchmarkRootDomainSnapshotVisibleValue(b *testing.B) {
	published, err := newRootDomainTable(rootDomainTestOp{key: "a", value: "published-a"})
	if err != nil {
		b.Fatalf("new published table: %v", err)
	}
	immutable, err := newRootDomainTable(rootDomainTestOp{key: "b", value: "immutable-b"})
	if err != nil {
		b.Fatalf("new immutable table: %v", err)
	}
	mutable, err := newRootDomainTable(rootDomainTestOp{key: "c", value: "mutable-c"})
	if err != nil {
		b.Fatalf("new mutable table: %v", err)
	}
	snap := rootDomainSnapshot{
		publishedRootID: 55,
		published:       published,
		immutables: []memtable.Table{
			immutable,
		},
		mutable: mutable,
	}
	key := []byte("c")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if got, ok := snap.visibleValue(key); !ok || string(got) != "mutable-c" {
			b.Fatalf("visibleValue mismatch: ok=%v got=%q", ok, string(got))
		}
	}
}

func BenchmarkRootDomainStateSealMutable(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mutable, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
		if err != nil {
			b.Fatalf("new mutable: %v", err)
		}
		mutable.Set([]byte("a"), []byte("1"))
		state := &rootDomainState{mutable: mutable}
		next, err := memtable.NewWithCapacityMode(0, memtable.ModeAppendOnly)
		if err != nil {
			b.Fatalf("new next mutable: %v", err)
		}
		state.sealMutable(next)
		if len(state.immutables) != 1 {
			b.Fatalf("immutables=%d want 1", len(state.immutables))
		}
	}
}

func assertRootDomainVisibleValue(t *testing.T, snap rootDomainSnapshot, key, want string) {
	t.Helper()

	got, ok := snap.visibleValue([]byte(key))
	if !ok {
		t.Fatalf("key %q not visible", key)
	}
	if string(got) != want {
		t.Fatalf("key %q: got=%q want=%q", key, string(got), want)
	}
}
