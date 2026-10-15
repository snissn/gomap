//go:build treedb_test

package mvcc

import (
	"errors"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
)

func TestNativePrivateOutputFreshAllocatorMembershipV6(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.MemtableShards = 4
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	d, e := treedb.Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if e := d.Close(); e != nil {
			t.Error("checked DB.Close", e)
		}
	}()
	store := New(d)
	commit := func(ts uint64, key string) {
		t.Helper()
		if e := store.CommitAt(ts, []Mutation{{Key: []byte(key), Value: []byte("value")}}, CommitDurable); e != nil {
			t.Fatal(e)
		}
	}
	commit(1, "old")
	if e = d.Checkpoint(); e != nil {
		t.Fatal(e)
	}
	old := d.NativeMembershipSnapshotForTest()
	if old == nil {
		t.Fatal("missing retained original generation")
	}
	defer func() {
		if e := old.Close(); e != nil {
			t.Error("checked snapshot.Close", e)
		}
	}()
	id, e := old.AllocateMembershipLeafForTest()
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if e := old.RetireMembershipLeafForTest(id); e != nil {
			t.Error(e)
		}
	}()
	// A real public commit occurs while private output is owned by the original
	// allocator transaction; there is no prepared allocator candidate here.
	commit(100, "future")
	g, records, e := old.BindMembershipLeafForTest(id, false, false)
	if e != nil {
		t.Fatal("fresh producer incorporation / PreparedPrimary", e)
	}
	t.Logf("privatePage=%d freshGeneration=%d actualProofRecords=%d", id, g, records)
	if _, _, e = old.BindMembershipLeafForTest(id, true, false); !errors.Is(e, rootpublication.ErrInvalidCandidate) {
		t.Fatalf("stale generation: %v", e)
	}
	if _, _, e = old.BindMembershipLeafForTest(id, false, true); !errors.Is(e, rootpublication.ErrDurableRootRecordDigest) {
		t.Fatalf("changed physical DATA: %v", e)
	}
	commit(101, "after-abort")
	if got, e := store.GetAt([]byte("future"), 100); e != nil || string(got.Value) != "value" {
		t.Fatalf("intervening public writer lost: %q %v", got.Value, e)
	}
}
