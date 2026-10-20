package treedb

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Exercise the complete default-budget operation, independently of the opt-in
// cost runner. The shared public trace retains all 2048 old rows, stages eight
// updates plus raw Set, commits once, checkpoints, reopens and checks final zero.
func TestNativeExistingIndexedJSONOrderedDefaultAdmission(t *testing.T) {
	runNativeIndexedOrderedRows(t, 2048, "ascending")
}

// A parser-scratch growth refusal after an earlier successful stage must not
// publish that earlier stage, invoke either callback again, or advance a cut.
func TestNativeExistingIndexedJSONScratchRefusalAtomic(t *testing.T) {
	database, err := Open(indexedSliceOptions(ProfileNoWALFast, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	t.Cleanup(func() {
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users", Indexes: []collections.IndexDefinition{
		{Name: "city", Field: "city", ValueType: collections.IndexValueString},
		{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
	}}); err != nil {
		t.Fatal(err)
	}
	users, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	ids := [][]byte{[]byte("a"), []byte("b")}
	seed := [][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`), []byte(`{"email":"b@example.com","city":"hnl"}`)}
	if _, err := users.InsertBatch(ids, seed); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	native, err := collections.NewNativeJSONManager(handle)
	if err != nil {
		t.Fatal(err)
	}
	// Valid JSON under the canonical frame's byte limit. The original decoder
	// allowance alone exceeds producer headroom; do not tune limits to admit it.
	large := []byte(`{"email":"b@example.com","city":"` + strings.Repeat("x", 256<<10) + `"}`)
	before := database.Stats()["treedb.cache.cow.txn.logical_seq"]
	lsn := handle.CommandWALNextLSN()
	tx, err := native.Begin("users")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if tx != nil {
			if err := tx.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	calls := [2]int{}
	if _, err := tx.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: ids[0], Update: func([]byte) ([]byte, bool, error) {
		calls[0]++
		return []byte(`{"email":"a@example.com","city":"sea"}`), true, nil
	}}}); err != nil {
		t.Fatal(err)
	}
	_, refusal := tx.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: ids[1], Update: func([]byte) ([]byte, bool, error) {
		calls[1]++
		return large, true, nil
	}}})
	if !errors.Is(refusal, memtable.ErrCOWCapacity) {
		t.Fatalf("scratch growth refusal: %v", refusal)
	}
	if err := tx.Commit(); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("poisoned commit: %v", err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	tx = nil
	if calls != [2]int{1, 1} {
		t.Fatalf("callbacks %v", calls)
	}
	if database.Stats()["treedb.cache.cow.txn.logical_seq"] != before || handle.CommandWALNextLSN() != lsn {
		t.Fatal("refused transaction advanced publication")
	}
	for i, id := range ids {
		if got, err := users.Get(id); err != nil || !bytes.Equal(got, seed[i]) {
			t.Fatalf("current%d=%s: %v", i, got, err)
		}
	}
	if owners, err := users.FindByIndexValue("city", "sea"); err != nil || len(owners) != 0 {
		t.Fatalf("refused index leaked %q: %v", owners, err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	manager = nil
	cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	requirePublicCOWFinalZero(t, cache)
	if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
		t.Fatalf("final pins %+v", s)
	}
}
