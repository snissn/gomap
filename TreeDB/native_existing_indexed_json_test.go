package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// This desired public lifecycle deliberately fails on the current indexed
// Create fence. It must not be made green through an unactivated legacy route.
func TestNativeExistingIndexedJSONPublicLifecycle(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := database.Close(); err != nil {
						t.Errorf("cleanup DB Close: %v", err)
					}
				}
			})
			backend, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			t.Cleanup(func() {
				if manager != nil {
					if err := manager.Close(); err != nil {
						t.Errorf("cleanup manager Close: %v", err)
					}
				}
			})
			if _, err := manager.CreateCollection(&collections.CollectionMeta{
				Name: "users",
				Indexes: []collections.IndexDefinition{
					{Name: "city", Field: "city", ValueType: collections.IndexValueString},
					{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
				},
			}); err != nil {
				t.Fatalf("activated existing public indexed Create: %v", err)
			}
			collection, err := manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			seed := [][]byte{[]byte(`{"email":"a@example.com","city":"hnl"}`), []byte(`{"email":"b@example.com","city":"sea"}`)}
			ids := [][]byte{[]byte("u1"), []byte("u2")}
			returned, err := collection.InsertBatch(ids, seed)
			if err != nil || len(returned) != 2 || !bytes.Equal(returned[0], ids[0]) || !bytes.Equal(returned[1], ids[1]) {
				t.Fatalf("public indexed InsertBatch IDs=%q error=%v", returned, err)
			}
			checkDocument := func(id string, want []byte) {
				t.Helper()
				got, err := collection.Get([]byte(id))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("primary %s=%s want=%s error=%v", id, got, want, err)
				}
			}
			checkIndex := func(index, value string, want ...string) {
				t.Helper()
				found, err := collection.FindByIndexValue(index, value)
				if err != nil {
					t.Fatalf("index %s=%s: %v", index, value, err)
				}
				got := make([]string, len(found))
				for i := range found {
					got[i] = string(found[i])
				}
				sort.Strings(got)
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("index %s=%s IDs=%q want=%q", index, value, got, want)
				}
			}
			checkDocument("u1", seed[0])
			checkDocument("u2", seed[1])
			checkIndex("email", "a@example.com", "u1")
			checkIndex("email", "b@example.com", "u2")
			checkIndex("city", "hnl", "u1")
			checkIndex("city", "sea", "u2")
			held, err := collection.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if held != nil {
					if err := held.Close(); err != nil {
						t.Errorf("cleanup held Close: %v", err)
					}
				}
			})
			checkHeld := func() {
				t.Helper()
				fetched, err := held.FetchDocumentsByID([][]byte{[]byte("u1"), []byte("u2"), []byte("u3")}, collections.DocumentFetchOptions{})
				if err != nil || len(fetched.Results) != 3 {
					t.Fatalf("held primary: %+v %v", fetched.Results, err)
				}
				for i := range seed {
					if !fetched.Results[i].Found || !bytes.Equal(fetched.Results[i].Document, seed[i]) {
						t.Fatalf("held row %d: %+v", i, fetched.Results[i])
					}
				}
				if fetched.Results[2].Found {
					t.Fatal("held view acquired later inserted row")
				}
				for _, query := range []struct{ index, value, id string }{{"email", "a@example.com", "u1"}, {"email", "b@example.com", "u2"}, {"city", "hnl", "u1"}, {"city", "sea", "u2"}} {
					var got []string
					if err := held.VisitIndexValueIDs(query.index, query.value, func(id []byte) error {
						got = append(got, string(id)) // copy borrowed ID before callback ends
						return nil
					}); err != nil || len(got) != 1 || got[0] != query.id {
						t.Fatalf("held index %+v IDs=%q error=%v", query, got, err)
					}
				}
			}
			checkHeld()
			updated := [][]byte{[]byte(`{"email":"c@example.com","city":"nyc"}`), []byte(`{"email":"a@example.com","city":"nyc"}`)}
			replace := func(want, next []byte) func([]byte) ([]byte, bool, error) {
				return func(current []byte) ([]byte, bool, error) {
					if !bytes.Equal(current, want) {
						return nil, false, fmt.Errorf("update read %s want %s", current, want)
					}
					return bytes.Clone(next), true, nil
				}
			}
			results, err := collection.UpdateBatch([]collections.UpdateBatchItem{
				{DocumentID: ids[0], Update: replace(seed[0], updated[0])},
				{DocumentID: ids[1], Update: replace(seed[1], updated[1])},
			})
			if err != nil || len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
				t.Fatalf("indexed batch change and unique handoff: %+v %v", results, err)
			}
			checkChanged := func() {
				checkDocument("u1", updated[0])
				checkDocument("u2", updated[1])
				checkIndex("email", "a@example.com", "u2")
				checkIndex("email", "b@example.com")
				checkIndex("email", "c@example.com", "u1")
				checkIndex("city", "hnl")
				checkIndex("city", "sea")
				checkIndex("city", "nyc", "u1", "u2")
			}
			checkChanged()
			beforeLSN := backend.CommandWALNextLSN()
			duplicate := []byte(`{"email":"duplicate@example.com","city":"bad"}`)
			_, err = collection.UpdateBatch([]collections.UpdateBatchItem{
				{DocumentID: ids[0], Update: replace(updated[0], duplicate)},
				{DocumentID: ids[1], Update: replace(updated[1], duplicate)},
			})
			if !errors.Is(err, collections.ErrUniqueIndexConflict) || backend.CommandWALNextLSN() != beforeLSN {
				t.Fatalf("duplicate final unique owners: error=%v LSN before=%d after=%d", err, beforeLSN, backend.CommandWALNextLSN())
			}
			checkChanged()
			checkIndex("email", "duplicate@example.com")
			checkIndex("city", "bad")
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkChanged()
			checkHeld()
			if n, err := collection.DeleteBatch([][]byte{ids[0]}); err != nil || n != 1 {
				t.Fatalf("indexed DeleteBatch count=%d error=%v", n, err)
			}
			if _, found, err := collection.GetInto(ids[0], nil); err != nil || found {
				t.Fatalf("deleted primary found=%t error=%v", found, err)
			}
			checkIndex("email", "c@example.com")
			checkIndex("city", "nyc", "u2")
			if got, err := collection.InsertBatch([][]byte{[]byte("u3")}, [][]byte{updated[0]}); err != nil || len(got) != 1 || string(got[0]) != "u3" {
				t.Fatalf("reuse released unique value IDs=%q error=%v", got, err)
			}
			checkDocument("u3", updated[0])
			checkIndex("email", "c@example.com", "u3")
			checkIndex("city", "nyc", "u2", "u3")
			checkHeld()
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkHeld()
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			manager = nil
			cache, registry := database.cached, backend.ValueLogIdentityPinRegistry()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			requirePublicCOWFinalZero(t, cache)
			if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
				t.Fatalf("original pins: %+v", stats)
			}
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			closed = false
			backend, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(backend)
			collection, err = manager.OpenCollection("users")
			if err != nil {
				t.Fatal(err)
			}
			if _, found, err := collection.GetInto(ids[0], nil); err != nil || found {
				t.Fatalf("reopened deleted primary found=%t error=%v", found, err)
			}
			checkDocument("u2", updated[1])
			checkDocument("u3", updated[0])
			checkIndex("email", "a@example.com", "u2")
			checkIndex("email", "b@example.com")
			checkIndex("email", "c@example.com", "u3")
			checkIndex("city", "hnl")
			checkIndex("city", "sea")
			checkIndex("city", "nyc", "u2", "u3")
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			manager = nil
			cache, registry = database.cached, backend.ValueLogIdentityPinRegistry()
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			requirePublicCOWFinalZero(t, cache)
			if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
				t.Fatalf("reopened pins: %+v", stats)
			}
		})
	}
}
