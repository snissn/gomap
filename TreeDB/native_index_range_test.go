package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

// Ordinary range APIs must pair secondary winners and primary documents from
// one native cut, including pending index tombstones over persisted entries.
func TestNativeExistingScalarIndexRangeLifecycle(t *testing.T) {
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
			index := collections.IndexDefinition{Name: "score", Field: "score", ValueType: collections.IndexValueInt64}
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "scores", Indexes: []collections.IndexDefinition{index}}); err != nil {
				t.Fatal(err)
			}
			collection, err := manager.OpenCollection("scores")
			if err != nil {
				t.Fatal(err)
			}
			original := []byte(`{"score":0,"name":"zero"}`)
			want := map[string][]byte{
				"u1": []byte(`{"score":-10,"name":"low"}`), "u2": original,
				"u3": []byte(`{"score":2,"name":"deleted"}`), "u4": []byte(`{"score":10,"name":"ten"}`),
			}
			if _, err := collection.InsertBatch([][]byte{[]byte("u1"), []byte("u2"), []byte("u3"), []byte("u4")}, [][]byte{want["u1"], want["u2"], want["u3"], want["u4"]}); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			old, err := collection.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if old != nil {
					_ = old.Close()
				}
			})
			want["u2"] = []byte(`{"score":7,"name":"changed"}`)
			if _, err := collection.UpdateBatch([]collections.UpdateBatchItem{{DocumentID: []byte("u2"), Update: func([]byte) ([]byte, bool, error) { return want["u2"], true, nil }}}); err != nil {
				t.Fatal(err)
			}
			if count, err := collection.DeleteBatch([][]byte{[]byte("u3")}); err != nil || count != 1 {
				t.Fatalf("delete count=%d err=%v", count, err)
			}
			delete(want, "u3")
			want["u5"] = []byte(`{"score":7,"name":"tie"}`)
			want["u6"] = []byte(`{"score":20,"name":"high"}`)
			if _, err := collection.InsertBatch([][]byte{[]byte("u5"), []byte("u6")}, [][]byte{want["u5"], want["u6"]}); err != nil {
				t.Fatal(err)
			}

			unbounded := collections.IndexRangeBound{Unbounded: true}
			bound := func(value int64, inclusive bool) collections.IndexRangeBound {
				return collections.IndexRangeBound{Value: value, Inclusive: inclusive}
			}
			queries := []struct {
				name      string
				opts      collections.IndexRangeOptions
				ids       []string
				truncated bool
			}{
				{"all", collections.IndexRangeOptions{Lower: unbounded, Upper: unbounded}, []string{"u1", "u2", "u5", "u4", "u6"}, false},
				{"half_open", collections.IndexRangeOptions{Lower: bound(7, true), Upper: bound(10, false)}, []string{"u2", "u5"}, false},
				{"exclusive_lower", collections.IndexRangeOptions{Lower: bound(7, false), Upper: bound(10, true)}, []string{"u4"}, false},
				{"upper_only", collections.IndexRangeOptions{Lower: unbounded, Upper: bound(7, false)}, []string{"u1"}, false},
				{"lower_only", collections.IndexRangeOptions{Lower: bound(10, true), Upper: unbounded}, []string{"u4", "u6"}, false},
				{"exact_tie", collections.IndexRangeOptions{Lower: bound(7, true), Upper: bound(7, true)}, []string{"u2", "u5"}, false},
				{"empty", collections.IndexRangeOptions{Lower: bound(7, true), Upper: bound(7, false)}, nil, false},
				{"limit_more", collections.IndexRangeOptions{Lower: bound(7, true), Upper: unbounded, Limit: 2}, []string{"u2", "u5"}, true},
				{"limit_exact", collections.IndexRangeOptions{Lower: bound(10, true), Upper: unbounded, Limit: 2}, []string{"u4", "u6"}, false},
			}
			checkIDs := func(label string, ids [][]byte, expected []string, truncated, expectedTruncated bool, err error) {
				t.Helper()
				var got []string
				for _, id := range ids {
					got = append(got, string(id))
				}
				if err != nil || !reflect.DeepEqual(got, expected) || truncated != expectedTruncated {
					t.Fatalf("%s IDs=%q truncated=%v err=%v want=%q/%v", label, got, truncated, err, expected, expectedTruncated)
				}
			}
			checkRecords := func(label string, records []collections.DocumentRecord, expected []string, truncated, expectedTruncated bool, err error) {
				t.Helper()
				ids := make([][]byte, len(records))
				for i, record := range records {
					ids[i] = record.ID
					if !bytes.Equal(record.Document, want[string(record.ID)]) {
						t.Fatalf("%s ID=%q document=%s want=%s", label, record.ID, record.Document, want[string(record.ID)])
					}
				}
				checkIDs(label, ids, expected, truncated, expectedTruncated, err)
			}
			checkRanges := func() {
				t.Helper()
				for _, query := range queries {
					ids, truncated, err := collection.FindByIndexRange("score", query.opts)
					checkIDs("find/"+query.name, ids, query.ids, truncated, query.truncated, err)
					// Returned IDs belong to the caller, independently of later scans.
					if len(ids) > 0 {
						ids[0][0] ^= 1
					}
					var scanned [][]byte
					truncated, err = collection.ScanIndexRange("score", query.opts, func(id []byte) (bool, error) { scanned = append(scanned, bytes.Clone(id)); return true, nil })
					checkIDs("scan/"+query.name, scanned, query.ids, truncated, query.truncated, err)
					documentOpts := query.opts
					if documentOpts.Limit == 0 {
						documentOpts.Limit = 16
					}
					records, truncated, err := collection.FindDocumentsByIndexRange("score", documentOpts)
					checkRecords("documents/"+query.name, records, query.ids, truncated, query.truncated, err)
					if len(records) > 0 {
						records[0].ID[0] ^= 1
						records[0].Document[0] ^= 1
					}
					var borrowed []collections.DocumentRecord
					truncated, err = collection.ScanBorrowedDocumentsByIndexRange("score", documentOpts, func(record collections.BorrowedDocumentRecord) (bool, error) {
						// The borrowed API forbids mutation, retention and reentry. Copy
						// both fields before allowing its next callback to reuse scratch.
						borrowed = append(borrowed, collections.DocumentRecord{ID: bytes.Clone(record.ID), Document: bytes.Clone(record.Document)})
						return true, nil
					})
					checkRecords("borrowed/"+query.name, borrowed, query.ids, truncated, query.truncated, err)
				}
			}
			checkRanges() // Expected baseline RED: activated ordinary range API.
			all := collections.IndexRangeOptions{Lower: unbounded, Upper: unbounded, Limit: 16}
			sentinel := errors.New("stop range callback")
			for _, borrowed := range []bool{false, true} {
				for _, callbackError := range []bool{false, true} {
					calls := 0
					visit := func(id []byte) (bool, error) {
						calls++
						if string(id) != "u1" {
							t.Fatalf("first callback ID=%q", id)
						}
						if callbackError {
							return false, sentinel
						}
						return false, nil
					}
					var truncated bool
					if borrowed {
						truncated, err = collection.ScanBorrowedDocumentsByIndexRange("score", all, func(record collections.BorrowedDocumentRecord) (bool, error) { return visit(record.ID) })
					} else {
						truncated, err = collection.ScanIndexRange("score", all, visit)
					}
					if calls != 1 || truncated || (callbackError && !errors.Is(err, sentinel)) || (!callbackError && err != nil) {
						t.Fatalf("callback borrowed=%v error=%v calls=%d truncated=%v err=%v", borrowed, callbackError, calls, truncated, err)
					}
				}
			}
			if _, err := collection.ScanIndexRange("score", all, nil); err == nil {
				t.Fatal("nil ID callback accepted")
			}
			if _, err := collection.ScanBorrowedDocumentsByIndexRange("score", all, nil); err == nil {
				t.Fatal("nil document callback accepted")
			}
			if _, _, err := collection.FindDocumentsByIndexRange("score", collections.IndexRangeOptions{Lower: unbounded, Upper: unbounded}); err == nil {
				t.Fatal("document range without positive limit accepted")
			}
			for _, invalid := range []collections.IndexRangeOptions{{Lower: unbounded, Upper: unbounded, Limit: -1}, {Lower: unbounded, Upper: unbounded, Desc: true, Limit: 1}, {Lower: collections.IndexRangeBound{Value: "bad", Inclusive: true}, Upper: unbounded, Limit: 1}} {
				if _, _, err := collection.FindByIndexRange("score", invalid); err == nil {
					t.Fatalf("invalid range accepted: %+v", invalid)
				}
				if _, _, err := collection.FindDocumentsByIndexRange("score", invalid); err == nil {
					t.Fatalf("invalid document range accepted: %+v", invalid)
				}
			}
			checkOld := func() {
				t.Helper()
				response, err := old.FetchDocumentsByID([][]byte{[]byte("u2"), []byte("u3"), []byte("u5")}, collections.DocumentFetchOptions{})
				if err != nil || len(response.Results) != 3 || !response.Results[0].Found || !bytes.Equal(response.Results[0].Document, original) || !response.Results[1].Found || response.Results[2].Found {
					t.Fatalf("held seed rows=%+v err=%v", response.Results, err)
				}
				var ids []string
				if err := old.VisitIndexValueIDs("score", int64(0), func(id []byte) error { ids = append(ids, string(id)); return nil }); err != nil || fmt.Sprint(ids) != "[u2]" {
					t.Fatalf("held equality IDs=%q err=%v", ids, err)
				}
			}
			checkOld()
			indexed, err := collection.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if indexed != nil {
					_ = indexed.Close()
				}
			})
			if _, err := collection.DropIndex("score"); err != nil {
				t.Fatal(err)
			}
			ids, truncated, err := collection.FindByIndexRange("score", all)
			if err != nil || ids != nil || truncated {
				t.Fatalf("dropped index IDs=%q truncated=%v err=%v", ids, truncated, err)
			}
			records, truncated, err := collection.FindDocumentsByIndexRange("score", all)
			if err != nil || records != nil || truncated {
				t.Fatalf("dropped documents=%+v truncated=%v err=%v", records, truncated, err)
			}
			calls := 0
			if truncated, err := collection.ScanIndexRange("score", all, func([]byte) (bool, error) { calls++; return true, nil }); err != nil || truncated || calls != 0 {
				t.Fatalf("missing ID scan calls=%d truncated=%v err=%v", calls, truncated, err)
			}
			if truncated, err := collection.ScanBorrowedDocumentsByIndexRange("score", all, func(collections.BorrowedDocumentRecord) (bool, error) { calls++; return true, nil }); err != nil || truncated || calls != 0 {
				t.Fatalf("missing document scan calls=%d truncated=%v err=%v", calls, truncated, err)
			}
			var heldIDs []string
			if err := indexed.VisitIndexValueIDs("score", int64(7), func(id []byte) error { heldIDs = append(heldIDs, string(id)); return nil }); err != nil || fmt.Sprint(heldIDs) != "[u2 u5]" {
				t.Fatalf("held dropped index IDs=%q err=%v", heldIDs, err)
			}
			if _, err := collection.CreateIndex(index); err != nil {
				t.Fatal(err)
			}
			checkRanges()
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkRanges()
			checkOld()
			if err := indexed.Close(); err != nil {
				t.Fatal(err)
			}
			indexed = nil
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			old = nil
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
				if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
					t.Fatalf("final registry=%+v", stats)
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
			collection, err = manager.OpenCollection("scores")
			if err != nil {
				t.Fatal(err)
			}
			checkRanges()
			if got, err := collection.Get([]byte("u3")); err != nil || got != nil {
				t.Fatalf("deleted row reappeared=%s err=%v", got, err)
			}
			closeOriginal()
		})
	}
}
