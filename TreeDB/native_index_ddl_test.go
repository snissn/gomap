package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestNativeExistingJSONIndexDDLRejectsClosedManager(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			database, err := Open(indexedSliceOptions(profile, t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			defer manager.Close()
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users", Indexes: []collections.IndexDefinition{
				{Name: "city", Field: "city", ValueType: collections.IndexValueString},
				{Name: "email", Field: "email", ValueType: collections.IndexValueString},
			}}); err != nil {
				t.Fatal(err)
			}
			users, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			next, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
			for _, operation := range []struct {
				name string
				run  func() (*collections.CollectionMeta, error)
			}{
				{"create", func() (*collections.CollectionMeta, error) {
					return users.CreateIndex(collections.IndexDefinition{Name: "after_close", Field: "city", ValueType: collections.IndexValueString})
				}},
				{"drop", func() (*collections.CollectionMeta, error) { return users.DropIndex("city") }},
				{"drop-many", func() (*collections.CollectionMeta, error) { return users.DropIndexes([]string{"email"}) }},
				{"drop-all", users.DropAllIndexes},
			} {
				if _, err := operation.run(); !errors.Is(err, backenddb.ErrClosed) {
					t.Errorf("%s after manager.Close: %v, want ErrClosed", operation.name, err)
				}
				if handle.CommandWALNextLSN() != next || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence {
					t.Errorf("%s after manager.Close advanced canonical identity or logical sequence", operation.name)
				}
			}
			// Closing one facade does not close the database or another manager.
			fresh := collections.NewCollectionManager(handle)
			defer fresh.Close()
			current, err := fresh.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			indexes := current.Meta().Indexes
			if len(indexes) != 2 || indexes[0].Name != "city" || indexes[1].Name != "email" {
				t.Fatalf("closed-manager DDL changed current schema: %+v", indexes)
			}
			if _, err := current.DropIndex("city"); err != nil {
				t.Fatalf("live manager cannot mutate indexes: %v", err)
			}
		})
	}
}

// Index DDL must consume the complete native cut, rather than just its persisted
// primary root. Catalog changes also conflict with every earlier schema binding,
// including read-only bindings of a transaction that writes another collection.
func TestNativeExistingJSONIndexDDLLifecycle(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := indexedSliceOptions(profile, t.TempDir())
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if database != nil {
					_ = database.Close()
				}
			})
			handle, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(handle)
			t.Cleanup(func() {
				if manager != nil {
					_ = manager.Close()
				}
			})
			for _, name := range []string{"users", "audit"} {
				if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: name}); err != nil {
					t.Fatal(err)
				}
			}
			users, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			peer, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			checkMetadata := func(expected ...string) {
				t.Helper()
				for _, collection := range []*collections.Collection{users, peer} {
					for _, meta := range []collections.CollectionMeta{collection.Meta(), collection.MetaView()} {
						names := make([]string, len(meta.Indexes))
						for i, index := range meta.Indexes {
							names[i] = index.Name
						}
						sort.Strings(names)
						if fmt.Sprint(names) != fmt.Sprint(expected) {
							t.Fatalf("current metadata indexes=%q want=%q", names, expected)
						}
						if len(meta.Indexes) != 0 {
							meta.Indexes[0].Name = "caller-mutated-copy"
						}
					}
				}
			}
			original := []byte(`{"email":"a@example.com","city":"hnl"}`)
			want := map[string][]byte{
				"u1": original,
				"u2": []byte(`{"email":"b@example.com","city":"sea"}`),
				"u3": []byte(`{"email":"c@example.com","city":"hnl"}`),
			}
			if _, err := users.InsertBatch([][]byte{[]byte("u1"), []byte("u2")}, [][]byte{want["u1"], want["u2"]}); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			old, err := users.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if old != nil {
					_ = old.Close()
				}
			})
			if _, err := users.InsertBatch([][]byte{[]byte("u3")}, [][]byte{want["u3"]}); err != nil {
				t.Fatal(err)
			}
			city := collections.IndexDefinition{Name: "city", Field: "city", ValueType: collections.IndexValueString}
			email := collections.IndexDefinition{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true}
			if _, err := users.CreateIndex(city); err != nil {
				t.Fatalf("activated public CreateIndex over persisted and pending rows: %v", err)
			}
			if _, err := users.CreateIndex(email); err != nil {
				t.Fatal(err)
			}
			checkMetadata("city", "email")
			checkIndex := func(index, value string, expected ...string) {
				t.Helper()
				ids, err := users.FindByIndexValue(index, value)
				if err != nil {
					t.Fatalf("index %s=%s: %v", index, value, err)
				}
				actual := make([]string, len(ids))
				for i := range ids {
					actual[i] = string(ids[i])
				}
				sort.Strings(actual)
				if fmt.Sprint(actual) != fmt.Sprint(expected) {
					t.Fatalf("index %s=%s IDs=%q want=%q", index, value, actual, expected)
				}
			}
			checkIndex("city", "hnl", "u1", "u3")
			checkIndex("email", "b@example.com", "u2")
			assertUnchanged := func(lsn uint64, sequence string) {
				t.Helper()
				if handle.CommandWALNextLSN() != lsn || database.Stats()["treedb.cache.cow.txn.logical_seq"] != sequence {
					t.Fatal("refused operation advanced canonical identity or logical sequence")
				}
			}
			lsn, sequence := handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
			if _, err := users.CreateIndex(collections.IndexDefinition{Name: "unique_city", Field: "city", ValueType: collections.IndexValueString, Unique: true}); !errors.Is(err, collections.ErrUniqueIndexConflict) {
				t.Fatalf("duplicate backfill must refuse atomically: %v", err)
			}
			assertUnchanged(lsn, sequence)
			checkMetadata("city", "email")
			if ids, err := users.FindByIndexValue("unique_city", "hnl"); err != nil || ids != nil {
				t.Fatalf("failed unique index lookup IDs=%q err=%v, want nil/nil", ids, err)
			}
			want["u1"] = []byte(`{"email":"a@example.com","city":"bos"}`)
			if _, err := users.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte("u1"), Update: func([]byte) ([]byte, bool, error) { return want["u1"], true, nil }}}); err != nil {
				t.Fatal(err)
			}
			want["u4"] = []byte(`{"email":"d@example.com","city":"hnl"}`)
			if _, err := users.InsertBatch([][]byte{[]byte("u4")}, [][]byte{want["u4"]}); err != nil {
				t.Fatal(err)
			}
			checkIndex("city", "hnl", "u3", "u4")
			checkIndex("city", "bos", "u1")
			lsn, sequence = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
			if _, err := users.InsertBatch([][]byte{[]byte("duplicate")}, [][]byte{[]byte(`{"email":"a@example.com","city":"other"}`)}); !errors.Is(err, collections.ErrUniqueIndexConflict) {
				t.Fatalf("unique ordinary write: %v", err)
			}
			assertUnchanged(lsn, sequence)
			indexed, err := users.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if indexed != nil {
					_ = indexed.Close()
				}
			})
			native, err := collections.NewNativeJSONManager(handle)
			if err != nil {
				t.Fatal(err)
			}
			stale, err := native.Begin("users")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = stale.Close() })
			if err := stale.InsertBatch([][]byte{[]byte("stale")}, [][]byte{[]byte(`{"email":"stale@example.com","city":"hnl"}`)}); err != nil {
				t.Fatal(err)
			}
			if _, err := users.DropIndex("city"); err != nil {
				t.Fatal(err)
			}
			checkMetadata("email")
			lsn, sequence = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
			if err := stale.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
				t.Fatalf("stale data plan committed across DDL: %v", err)
			}
			assertUnchanged(lsn, sequence)
			if ids, err := users.FindByIndexValue("city", "hnl"); err != nil || ids != nil {
				t.Fatalf("dropped current index IDs=%q err=%v, want nil/nil", ids, err)
			}
			checkIndex("email", "c@example.com", "u3")
			checkHeldIndex := func() {
				t.Helper()
				var heldIDs []string
				if err := indexed.VisitIndexValueIDs("city", "hnl", func(id []byte) error { heldIDs = append(heldIDs, string(id)); return nil }); err != nil {
					t.Fatal(err)
				}
				sort.Strings(heldIDs)
				if fmt.Sprint(heldIDs) != "[u3 u4]" {
					t.Fatalf("held dropped index IDs=%q", heldIDs)
				}
			}
			checkHeldIndex()
			derived, err := native.Begin("audit", "users")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = derived.Close() })
			document, err := derived.GetFrom("users", []byte("u1"))
			if err != nil || !bytes.Equal(document, want["u1"]) {
				t.Fatalf("read-derived input=%s err=%v", document, err)
			}
			if err := derived.InsertBatch([][]byte{[]byte("derived")}, [][]byte{document}); err != nil {
				t.Fatal(err)
			}
			// Restore exactly the same schema: metadata equality alone cannot detect
			// this catalog ABA, or the users binding with no staged users mutation.
			if _, err := users.CreateIndex(city); err != nil {
				t.Fatal(err)
			}
			if _, err := users.DropIndexes([]string{"city"}); err != nil {
				t.Fatal(err)
			}
			lsn, sequence = handle.CommandWALNextLSN(), database.Stats()["treedb.cache.cow.txn.logical_seq"]
			if err := derived.Commit(); !errors.Is(err, backenddb.ErrConcurrentModification) {
				t.Fatalf("read-derived writer committed across catalog ABA: %v", err)
			}
			assertUnchanged(lsn, sequence)
			if _, err := users.CreateIndex(city); err != nil {
				t.Fatal(err)
			}
			if _, err := users.DropAllIndexes(); err != nil {
				t.Fatal(err)
			}
			checkMetadata()
			for _, name := range []string{"city", "email"} {
				if ids, err := users.FindByIndexValue(name, "unused"); err != nil || ids != nil {
					t.Fatalf("DropAllIndexes %s IDs=%q err=%v, want nil/nil", name, ids, err)
				}
			}
			want["u5"] = []byte(`{"email":"e@example.com","city":"sea"}`)
			if _, err := users.InsertBatch([][]byte{[]byte("u5")}, [][]byte{want["u5"]}); err != nil {
				t.Fatal(err)
			}
			if _, err := users.CreateIndex(email); err != nil {
				t.Fatal(err)
			}
			checkMetadata("email")
			checkIndex("email", "e@example.com", "u5")
			checkRows := func() {
				t.Helper()
				for id, expected := range want {
					got, err := users.Get([]byte(id))
					if err != nil || !bytes.Equal(got, expected) {
						t.Fatalf("primary %s=%s want=%s err=%v", id, got, expected, err)
					}
				}
				for _, id := range []string{"stale", "duplicate"} {
					got, err := users.Get([]byte(id))
					if err != nil || got != nil {
						t.Fatalf("refused row %s=%s err=%v", id, got, err)
					}
				}
				audit, err := manager.OpenCollection("audit")
				if err != nil {
					t.Fatal(err)
				}
				if got, err := audit.Get([]byte("derived")); err != nil || got != nil {
					t.Fatalf("refused derived row=%s err=%v", got, err)
				}
			}
			checkRows()
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkRows()
			response, err := old.FetchDocumentsByID([][]byte{[]byte("u1"), []byte("u3")}, collections.DocumentFetchOptions{})
			if err != nil || len(response.Results) != 2 || !response.Results[0].Found || !bytes.Equal(response.Results[0].Document, original) || response.Results[1].Found {
				t.Fatalf("held pre-DDL rows=%+v err=%v", response.Results, err)
			}
			checkHeldIndex()
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			old = nil
			if err := indexed.Close(); err != nil {
				t.Fatal(err)
			}
			indexed = nil
			closeOriginal := func() {
				t.Helper()
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
			closeOriginal()
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(handle)
			users, err = manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			checkRows()
			for _, query := range []struct{ id, email string }{{"u1", "a@example.com"}, {"u2", "b@example.com"}, {"u3", "c@example.com"}, {"u4", "d@example.com"}, {"u5", "e@example.com"}} {
				checkIndex("email", query.email, query.id)
			}
			if ids, err := users.FindByIndexValue("city", "hnl"); err != nil || ids != nil {
				t.Fatalf("dropped schema reappeared after reopen IDs=%q err=%v, want nil/nil", ids, err)
			}
			closeOriginal()
		})
	}
}
