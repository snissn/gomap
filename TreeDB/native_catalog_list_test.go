package treedb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestNativeExistingCollectionCatalogListing(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := indexedSliceOptions(profile, t.TempDir())
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			var manager, peer *collections.CollectionManager
			t.Cleanup(func() {
				if peer != nil {
					_ = peer.Close()
				}
				if manager != nil {
					_ = manager.Close()
				}
				if database != nil {
					_ = database.Close()
				}
			})
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(handle)
			peer = collections.NewCollectionManager(handle) // predates every catalog mutation
			for _, m := range []*collections.CollectionManager{manager, peer} {
				metas, truncated, err := m.ListCollectionsBounded(1)
				if err != nil || len(metas) != 0 || truncated {
					t.Fatalf("empty list=%+v truncated=%v err=%v", metas, truncated, err)
				}
			}
			for _, name := range []string{"zeta", "alpha", "middle"} {
				if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: name}); err != nil {
					t.Fatal(err)
				}
			}
			check := func(indexed bool) {
				t.Helper()
				lsn, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
				for _, m := range []*collections.CollectionManager{manager, peer} {
					for _, limit := range []int{-1, 0, 1, 2, 3, 4} {
						metas, truncated, err := m.ListCollectionsBounded(limit)
						count := 3
						if limit > 0 && limit < count {
							count = limit
						}
						if err != nil || len(metas) != count || truncated != (count < 3) {
							t.Fatalf("limit=%d count=%d truncated=%v err=%v", limit, len(metas), truncated, err)
						}
						for i, name := range []string{"alpha", "middle", "zeta"}[:count] {
							if metas[i].Name != name {
								t.Fatalf("limit=%d meta[%d]=%q want=%q", limit, i, metas[i].Name, name)
							}
						}
						wantIndexes := 0
						if indexed {
							wantIndexes = 1
						}
						if len(metas[0].Indexes) != wantIndexes || (indexed && (metas[0].Indexes[0].Name != "city" || metas[0].Indexes[0].Field != "city")) {
							t.Fatalf("limit=%d current alpha indexes=%+v", limit, metas[0].Indexes)
						}
						// The caller can replace slice elements without mutating the
						// captured schema or another listing's returned backing.
						metas[0].Name = "caller-owned"
						if indexed {
							metas[0].Indexes[0].Name = "caller-index"
						}
					}
					metas, err := m.ListCollections()
					if err != nil || len(metas) != 3 || metas[0].Name != "alpha" || (indexed && (len(metas[0].Indexes) != 1 || metas[0].Indexes[0].Name != "city")) {
						t.Fatalf("unbounded owned list=%+v err=%v", metas, err)
					}
				}
				if handle.CommandWALNextLSN() != lsn || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence {
					t.Fatal("listing changed canonical or logical publication frontier")
				}
			}
			check(false) // all three creates are still pending
			alpha, err := manager.OpenCollection("alpha")
			if err != nil {
				t.Fatal(err)
			}
			first, second := []byte(`{"city":"hnl"}`), []byte(`{"city":"sea"}`)
			if _, err := alpha.InsertBatch([][]byte{[]byte("a")}, [][]byte{first}); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if _, err := alpha.InsertBatch([][]byte{[]byte("b")}, [][]byte{second}); err != nil {
				t.Fatal(err)
			}
			if _, err := alpha.CreateIndex(collections.IndexDefinition{Name: "city", Field: "city", ValueType: collections.IndexValueString}); err != nil {
				t.Fatal(err)
			}
			check(true) // DDL metadata is visible through both preexisting managers
			owned, err := peer.ListCollections()
			if err != nil {
				t.Fatal(err)
			}
			for _, query := range []struct{ value, id string }{{"hnl", "a"}, {"sea", "b"}} {
				ids, err := alpha.FindByIndexValue("city", query.value)
				if err != nil || len(ids) != 1 || string(ids[0]) != query.id {
					t.Fatalf("index %s IDs=%q err=%v", query.value, ids, err)
				}
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			check(true)
			if _, err := alpha.DropIndex("city"); err != nil {
				t.Fatal(err)
			}
			check(false)
			if len(owned[0].Indexes) != 1 || owned[0].Indexes[0].Name != "city" {
				t.Fatal("later DDL mutated an earlier caller-owned catalog result")
			}
			closeOriginal := func() {
				t.Helper()
				if err := peer.Close(); err != nil {
					t.Fatal(err)
				}
				if _, _, err := peer.ListCollectionsBounded(1); !errors.Is(err, backenddb.ErrClosed) {
					t.Fatalf("closed native manager listing=%v", err)
				}
				peer = nil
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				manager = nil
				cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database = nil
				requirePublicCOWFinalZero(t, cache)
				if s := registry.Stats(); s.ActivePins != 0 || s.ActiveIdentities != 0 {
					t.Fatalf("final registry=%+v", s)
				}
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			closeOriginal()
			if owned[0].Indexes[0].Name != "city" {
				t.Fatal("Close retired caller-owned catalog backing")
			}
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager, peer = collections.NewCollectionManager(handle), collections.NewCollectionManager(handle)
			check(false) // includes the original nil-state physical catalog import
			alpha, err = peer.OpenCollection("alpha")
			if err != nil {
				t.Fatal(err)
			}
			for id, expected := range map[string][]byte{"a": first, "b": second} {
				got, err := alpha.Get([]byte(id))
				if err != nil || !bytes.Equal(got, expected) {
					t.Fatalf("reopened %s=%s err=%v", id, got, err)
				}
			}
			closeOriginal()
		})
	}
}
