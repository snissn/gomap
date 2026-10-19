package treedb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const nativePreparedRequestLimit = 2 << 30

// Preparation retains the original logical opening across a sibling publication
// and physical basis refresh. Commit must complete its own physical publication,
// even though background checkpoints are disabled by the public fixture.
func TestNativePreparedInsertCheckpointBeforeCommitAndReopen(t *testing.T) {
	for _, family := range []string{"int64", "string"} {
		t.Run(family, func(t *testing.T) {
			opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			t.Cleanup(func() { _ = manager.Close() })
			fixture := nativeColumnPendingCaseFor(t, family)
			if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); err != nil {
				t.Fatal(err)
			}
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "sibling"}); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection("events")
			if err != nil {
				t.Fatal(err)
			}
			sibling, err := manager.OpenCollection("sibling")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := col.InsertBatch([][]byte{[]byte("seed")}, [][]byte{fixture.docs[0]}); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			held, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = held.Close() })
			next, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
			physical := indexedCRUDAppliedLSN(t, handle)
			ids := [][]byte{[]byte("two"), []byte("one")}
			docs := [][]byte{bytes.Clone(fixture.docs[2]), bytes.Clone(fixture.docs[1])}
			prepared, err := col.PrepareInsertBatchOwned(ids, docs, nativePreparedRequestLimit)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(prepared.Abandon)
			if prepared.OwnedBytes() <= 0 || prepared.ReservedBytes() <= prepared.OwnedBytes() || prepared.ReservedBytes() > nativePreparedRequestLimit {
				t.Fatalf("request bytes owned=%d reserved=%d", prepared.OwnedBytes(), prepared.ReservedBytes())
			}
			if handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence || indexedCRUDAppliedLSN(t, handle) != physical {
				t.Fatal("preparation published or assigned an identity")
			}
			// Public native preparation has already copied both the canonical
			// bytes and parsed projection; neither may borrow caller buffers.
			for i := range ids {
				ids[i][0], docs[i][0] = '!', '!'
			}
			if got, err := col.Get([]byte("one")); err != nil || got != nil {
				t.Fatalf("prepared row visible: %s %v", got, err)
			}
			if _, err := sibling.InsertBatch([][]byte{[]byte("sibling")}, [][]byte{[]byte(`{"n":42}`)}); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			next, sequence = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
			result, err := prepared.Commit()
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != 2 || string(result[0]) != "two" || string(result[1]) != "one" {
				t.Fatalf("result request order=%q", result)
			}
			requireNativeTypedOpsAdvance(t, database, handle, next, sequence)
			if indexedCRUDAppliedLSN(t, handle) != next {
				t.Fatal("prepared Commit acknowledged without physical completion")
			}
			result[0][0] = '!'
			check := func() {
				t.Helper()
				for i, id := range []string{"seed", "one", "two"} {
					got, err := col.Get([]byte(id))
					if err != nil {
						t.Fatal(err)
					}
					requireNativeColumnJSON(t, got, fixture.docs[i])
				}
				if got, err := sibling.Get([]byte("sibling")); err != nil || string(got) != `{"n":42}` {
					t.Fatalf("sibling %s: %v", got, err)
				}
			}
			check()
			current, err := col.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			fresh, fetchErr := current.FetchDocumentsByID([][]byte{[]byte("one"), []byte("two")}, collections.DocumentFetchOptions{})
			closeErr := current.Close()
			if fetchErr != nil || closeErr != nil || len(fresh.Results) != 2 || !fresh.Results[0].Found || !fresh.Results[1].Found {
				t.Fatalf("current complete opening=%+v: %v %v", fresh, fetchErr, closeErr)
			}
			requireNativeColumnJSON(t, fresh.Results[0].Document, fixture.docs[1])
			requireNativeColumnJSON(t, fresh.Results[1].Document, fixture.docs[2])
			old, err := held.FetchDocumentsByID([][]byte{[]byte("seed"), []byte("one"), []byte("two")}, collections.DocumentFetchOptions{})
			if err != nil || len(old.Results) != 3 || !old.Results[0].Found || old.Results[1].Found || old.Results[2].Found {
				t.Fatalf("held original opening=%+v: %v", old, err)
			}
			requireNativeColumnJSON(t, old.Results[0].Document, fixture.docs[0])
			if _, err := prepared.Commit(); err == nil {
				t.Fatal("prepared batch committed twice")
			}
			prepared.Abandon()
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			cache, pins := database.cached, handle.ValueLogIdentityPinRegistry()
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			requirePublicCOWFinalZero(t, cache)
			if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("retained original resources: %+v", s)
			}
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(handle)
			col, err = manager.OpenCollection("events")
			if err != nil {
				t.Fatal(err)
			}
			sibling, err = manager.OpenCollection("sibling")
			if err != nil {
				t.Fatal(err)
			}
			check()
			cache, pins = database.cached, handle.ValueLogIdentityPinRegistry()
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			requirePublicCOWFinalZero(t, cache)
			if s := pins.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
				t.Fatalf("reopen resources: %+v", s)
			}
		})
	}
}

func TestNativePreparedInsertRefusalAndConsumption(t *testing.T) {
	database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(handle)
	t.Cleanup(func() { _ = manager.Close() })
	fixture := nativeColumnPendingCaseFor(t, "int64")
	if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	frontier := func() (uint64, string) {
		return handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
	}
	unchanged := func(next uint64, sequence string) {
		t.Helper()
		if n, s := frontier(); n != next || s != sequence {
			t.Fatal("refusal/abandon changed a publication frontier")
		}
	}
	prepare := func(id string) *collections.PreparedInsertBatch {
		t.Helper()
		p, err := col.PrepareInsertBatchOwned([][]byte{[]byte(id)}, [][]byte{fixture.docs[1]}, nativePreparedRequestLimit)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Abandon)
		return p
	}
	next, sequence := frontier()
	for _, test := range []struct {
		name      string
		ids, docs [][]byte
		limit     int64
	}{
		{"limit", [][]byte{[]byte("limit")}, [][]byte{fixture.docs[1]}, 1024},
		{"malformed", [][]byte{[]byte("bad")}, [][]byte{[]byte(`{"time_us":`)}, nativePreparedRequestLimit},
		{"duplicate", [][]byte{[]byte("dup"), []byte("dup")}, [][]byte{fixture.docs[1], fixture.docs[2]}, nativePreparedRequestLimit},
	} {
		t.Run(test.name, func(t *testing.T) {
			if p, err := col.PrepareInsertBatchOwned(test.ids, test.docs, test.limit); err == nil || p != nil {
				if p != nil {
					p.Abandon()
				}
				t.Fatalf("invalid preparation p=%v: %v", p, err)
			}
			unchanged(next, sequence)
		})
	}
	abandoned := prepare("abandoned")
	abandoned.Abandon()
	abandoned.Abandon()
	if _, err := abandoned.Commit(); err == nil {
		t.Fatal("abandoned batch committed")
	}
	unchanged(next, sequence)
	winner, loser := prepare("race"), prepare("race")
	if _, err := winner.Commit(); err != nil {
		t.Fatal(err)
	}
	next, sequence = frontier()
	if _, err := loser.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
		t.Fatalf("stale opening: %v", err)
	}
	if _, err := loser.Commit(); err == nil {
		t.Fatal("failed commit did not consume batch")
	}
	unchanged(next, sequence)
	pending := prepare("closed")
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.Commit(); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed manager: %v", err)
	}
	unchanged(next, sequence)
	cache := database.cached
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, cache)
}

func TestNativePreparedInsertRejectsNonDurableProfiles(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			// A persisted eligible typed collection remains readable in these
			// profiles; preparing a write must refuse before opening a txn.
			database, err := Open(indexedSliceOptions(ProfileCommandWALDurable, dir))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			t.Cleanup(func() { _ = manager.Close() })
			fixture := nativeColumnPendingCaseFor(t, "int64")
			if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); err != nil {
				t.Fatal(err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			cache := database.cached
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			requirePublicCOWFinalZero(t, cache)
			database, err = Open(indexedSliceOptions(profile, dir))
			if err != nil {
				t.Fatal(err)
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(handle)
			col, err := manager.OpenCollection("events")
			if err != nil {
				t.Fatal(err)
			}
			next, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
			before, pins := database.cached.COWMemoryStats(), handle.ValueLogIdentityPinRegistry().Stats()
			if p, err := col.PrepareInsertBatchOwned([][]byte{[]byte("refused")}, [][]byte{fixture.docs[1]}, nativePreparedRequestLimit); p != nil || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				if p != nil {
					p.Abandon()
				}
				t.Fatalf("unsupported profile preparation p=%v: %v", p, err)
			}
			if handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence || database.cached.COWMemoryStats() != before || handle.ValueLogIdentityPinRegistry().Stats() != pins {
				t.Fatal("unsupported profile changed frontiers or original admission")
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			cache = database.cached
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			requirePublicCOWFinalZero(t, cache)
		})
	}
}
