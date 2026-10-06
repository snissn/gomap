package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func r1ReadCollectionMeta() CollectionMeta {
	meta := typedMinimaCollectionMeta()
	meta.Options.ColumnStore.Columns = []ColumnStoreColumn{{Name: "content", Path: "content", ValueType: ColumnStoreValueString}, {Name: "user", Path: "user", ValueType: ColumnStoreValueString}}
	meta.Indexes = []IndexDefinition{{Name: "user", Field: "user", ValueType: IndexValueString}}
	meta.TextIndexes = nil
	meta.VectorIndexes = nil
	return meta
}

func r1ReadColumns(content, user []string) []TypedColumnBatch {
	return []TypedColumnBatch{{Name: "content", Strings: content}, {Name: "user", Strings: user}}
}

func r1RequireCompleteRow(t *testing.T, got, want []byte) {
	t.Helper()
	var gm, wm any
	if err := json.Unmarshal(got, &gm); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &wm); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gm, wm) {
		t.Fatalf("complete-row parity: got=%s want=%s", got, want)
	}
}

func TestR1TypedRowRangeCompleteParity(t *testing.T) {
	for _, state := range []string{"buffered", "flushed", "checkpointed", "reopened"} {
		t.Run(state, func(t *testing.T) {
			dir, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
			defer func() { _ = d.Close() }()
			ids := [][]byte{[]byte("a"), []byte("b")}
			retained := [][]byte{[]byte(`{"id":"a","score":7,"optional":null}`), []byte(`{"id":"b","score":8}`)}
			if _, _, err := col.InsertTypedBatchWithStats(ids, retained, r1ReadColumns([]string{"whole row", "second row"}, []string{"u1", "u1"})); err != nil {
				t.Fatal(err)
			}
			if state != "buffered" {
				if err := col.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if state == "checkpointed" || state == "reopened" {
				if err := d.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			if state == "reopened" {
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				d = openTypedMinimaDB(t, dir)
				var err error
				col, err = NewCollectionManager(d).OpenCollection("minima")
				if err != nil {
					t.Fatal(err)
				}
			}
			opts := IndexRangeOptions{Lower: IndexRangeBound{Value: "u1", Inclusive: true}, Upper: IndexRangeBound{Value: "u1", Inclusive: true}, Limit: 2}
			got, cut, err := col.FindDocumentsByIndexRange("user", opts)
			if err != nil || cut || len(got) != 2 {
				t.Fatalf("range err=%v cut=%t len=%d", err, cut, len(got))
			}
			want := [][]byte{[]byte(`{"content":"whole row","user":"u1","id":"a","score":7,"optional":null}`), []byte(`{"content":"second row","user":"u1","id":"b","score":8}`)}
			for i := range got {
				if !bytes.Equal(got[i].ID, ids[i]) {
					t.Fatalf("range ID=%q want=%q", got[i].ID, ids[i])
				}
				r1RequireCompleteRow(t, got[i].Document, want[i])
			}
			// Results own their ID/document bytes even after the materializer is closed.
			got[0].ID[0] = 'x'
			got[0].Document[0] = '['
			fresh, err := col.Get(ids[0])
			if err != nil {
				t.Fatal(err)
			}
			r1RequireCompleteRow(t, fresh, want[0])
			seen := 0
			cut, err = col.ScanBorrowedDocumentsByIndexRange("user", opts, func(record BorrowedDocumentRecord) (bool, error) {
				if !bytes.Equal(record.ID, ids[seen]) {
					t.Fatalf("borrowed ID=%q", record.ID)
				}
				r1RequireCompleteRow(t, record.Document, want[seen])
				seen++
				return true, nil
			})
			if err != nil || cut || seen != 2 {
				t.Fatalf("borrowed err=%v cut=%t seen=%d", err, cut, seen)
			}
			opts.Limit = 1
			limited, cut, err := col.FindDocumentsByIndexRange("user", opts)
			if err != nil || !cut || len(limited) != 1 {
				t.Fatalf("limit err=%v cut=%t len=%d", err, cut, len(limited))
			}
			callbackErr := errors.New("stop typed range")
			if _, err := col.ScanBorrowedDocumentsByIndexRange("user", opts, func(BorrowedDocumentRecord) (bool, error) { return false, callbackErr }); !errors.Is(err, callbackErr) {
				t.Fatalf("callback error=%v", err)
			}
		})
	}
}

func TestR1TypedRowRangeTracksMutationAndCapturedReadView(t *testing.T) {
	dir, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("a"), []byte("b")}
	if _, _, err := col.InsertTypedBatchWithStats(ids, [][]byte{[]byte(`{"id":"a","score":7,"optional":null}`), []byte(`{"id":"b","score":8}`)}, r1ReadColumns([]string{"old row", "gone row"}, []string{"u1", "u2"})); err != nil {
		t.Fatal(err)
	}
	old, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if _, err := old.FetchDocumentsByID(ids, DocumentFetchOptions{}); err != nil {
		t.Fatal(err)
	}
	if n, err := col.UpsertTypedBatch([][]byte{ids[0]}, [][]byte{[]byte(`{"id":"a","score":9}`)}, r1ReadColumns([]string{"new row"}, []string{"u3"})); err != nil || n != 1 {
		t.Fatalf("upsert n=%d err=%v", n, err)
	}
	if n, err := col.DeleteBatch([][]byte{ids[1]}); err != nil || n != 1 {
		t.Fatalf("delete n=%d err=%v", n, err)
	}
	check := func() {
		t.Helper()
		all := IndexRangeOptions{Lower: IndexRangeBound{Unbounded: true}, Upper: IndexRangeBound{Unbounded: true}, Limit: 10}
		got, cut, err := col.FindDocumentsByIndexRange("user", all)
		if err != nil || cut || len(got) != 1 || !bytes.Equal(got[0].ID, ids[0]) {
			t.Fatalf("mutation range=%+v cut=%t err=%v", got, cut, err)
		}
		r1RequireCompleteRow(t, got[0].Document, []byte(`{"content":"new row","user":"u3","id":"a","score":9}`))
	}
	check()
	captured, err := old.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !captured.Results[1].Found {
		t.Fatal("old view lost deleted row")
	}
	r1RequireCompleteRow(t, captured.Results[0].Document, []byte(`{"content":"old row","user":"u1","id":"a","score":7,"optional":null}`))
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := old.FetchDocumentsByID(ids, DocumentFetchOptions{}); err == nil {
		t.Fatal("closed view accepted fetch")
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = openTypedMinimaDB(t, dir)
	col, err = NewCollectionManager(d).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	check()
}

func TestR1TypedRowGetIntoUsesSharedMaterializer(t *testing.T) {
	_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
	defer d.Close()
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a","score":7}`)}, r1ReadColumns([]string{"whole row"}, []string{"u1"})); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	before := workstats.Read().Output.Materialization
	dst := make([]byte, 0, 256)
	got, found, err := col.GetInto([]byte("a"), dst)
	if err != nil || !found {
		t.Fatalf("GetInto found=%t err=%v", found, err)
	}
	if &got[0] != &dst[:cap(dst)][0] {
		t.Fatal("GetInto did not reuse caller buffer")
	}
	r1RequireCompleteRow(t, got, []byte(`{"content":"whole row","user":"u1","id":"a","score":7}`))
	after := workstats.Read().Output.Materialization
	if after.JSONReconstructionRows-before.JSONReconstructionRows != 1 || after.Fetched-before.Fetched != 1 {
		t.Fatalf("public GetInto did not select shared materializer: before=%+v after=%+v", before, after)
	}
	snap := d.AcquireSnapshot()
	defer snap.Close()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	view := newCollectionReadViewAtSnapshot(col, snap, catalog, false, "")
	defer view.Close()
	response, err := view.FetchDocumentsByID([][]byte{[]byte("a")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if response.Stats.VisibilityScans != 0 || response.Stats.PointRowDecodes != 1 || response.Stats.RowLocatorLookups != 1 {
		t.Fatalf("shared route did not point-decode: %+v", response.Stats)
	}
	// A one-document view must still reject malformed unrelated manifest
	// entries, even when its requested row is already in the decoded cache.
	view.orderedPointRowRefs = true
	original := *view.columnSnapshotView
	part := original.AssetRefs[0]
	for _, tc := range []struct {
		name string
		refs []columnManifestAssetRefForScan
		want string
	}{
		{name: "duplicate", refs: []columnManifestAssetRefForScan{part, part}, want: "duplicate document row ref"},
		{name: "unrelated kind", refs: []columnManifestAssetRefForScan{part, {Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Generation: part.Ref.Generation + 1}}}, want: "unsupported asset kind"},
		{name: "unordered", refs: []columnManifestAssetRefForScan{{Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Generation: part.Ref.Generation + 1}}, part}, want: "not ordered"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := original
			corrupt.AssetRefs = tc.refs
			view.columnSnapshotView = &corrupt
			if _, err := view.materializeRetainedTypedDocument([]byte("a"), []byte(`{"id":"a","score":7}`)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("corrupt manifest err=%v want %q", err, tc.want)
			}
		})
	}
	view.columnSnapshotView = &original
}

// A range must never join the index key from one publication with typed or
// retained fields from another while the same manager publishes replacements.
func TestR1TypedRowRangeConcurrentPublication(t *testing.T) {
	_, d, col := openTypedMinimaCollectionMeta(t, r1ReadCollectionMeta())
	defer d.Close()
	id := []byte("a")
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{id}, [][]byte{[]byte(`{"id":"a","revision":0}`)}, r1ReadColumns([]string{"row-0"}, []string{"u0"})); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for revision := 1; revision <= 40; revision++ {
			retained := []byte(fmt.Sprintf(`{"id":"a","revision":%d}`, revision))
			if _, err := col.UpsertTypedBatch([][]byte{id}, [][]byte{retained}, r1ReadColumns([]string{fmt.Sprintf("row-%d", revision)}, []string{fmt.Sprintf("u%d", revision%2)})); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	var readErr error
	for probe := 0; probe < 40 && readErr == nil; probe++ {
		for bucket := 0; bucket < 2; bucket++ {
			user := fmt.Sprintf("u%d", bucket)
			rows, truncated, err := col.FindDocumentsByIndexRange("user", IndexRangeOptions{Lower: IndexRangeBound{Value: user, Inclusive: true}, Upper: IndexRangeBound{Value: user, Inclusive: true}, Limit: 2})
			if err != nil || truncated || len(rows) > 1 {
				readErr = fmt.Errorf("concurrent range len=%d truncated=%t err=%v", len(rows), truncated, err)
				break
			}
			for _, row := range rows {
				var document struct {
					ID       string `json:"id"`
					Revision int    `json:"revision"`
					Content  string `json:"content"`
					User     string `json:"user"`
				}
				if err := json.Unmarshal(row.Document, &document); err != nil {
					readErr = err
					break
				}
				if document.ID != "a" || document.User != user || document.User != fmt.Sprintf("u%d", document.Revision%2) || document.Content != fmt.Sprintf("row-%d", document.Revision) {
					readErr = fmt.Errorf("mixed publication: index=%s row=%s", user, row.Document)
					break
				}
			}
		}
	}
	// Join before closing the DB even if a reader detected an invariant failure.
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
}

func TestR1TypedRowOrderedPartLookupHistory5065(t *testing.T) {
	col, ids, want, _ := r1RangeHistoryFixture5065(t)
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	view.orderedPointRowRefs = true
	response, err := view.FetchDocumentsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if view.pointRowRefs != nil || view.validatedPointRowRefs == nil || view.validatedPointRowRefs != view.columnSnapshotView {
		t.Fatal("ordered lookup allocated a part map or failed to retain snapshot validation")
	}
	if response.Stats.VisibilityScans != 0 || response.Stats.RowLocatorBuilds != 0 {
		t.Fatalf("ordered lookup scanned historical rows: %+v", response.Stats)
	}
	for i := range response.Results {
		if !response.Results[i].Found {
			t.Fatalf("missing row%d", i)
		}
		r1RequireCompleteRow(t, response.Results[i].Document, want[i])
	}
	// A distinct snapshot cache must be fully revalidated, even if its first
	// requested block is already cached and only an unrelated tail is corrupt.
	original := *view.columnSnapshotView
	for _, kind := range []string{"duplicate", "kind", "order"} {
		t.Run(kind, func(t *testing.T) {
			corrupt := original
			corrupt.AssetRefs = append([]columnManifestAssetRefForScan(nil), original.AssetRefs...)
			n := len(corrupt.AssetRefs)
			switch kind {
			case "duplicate":
				corrupt.AssetRefs[n-1] = corrupt.AssetRefs[n-2]
			case "kind":
				corrupt.AssetRefs[n-1].Ref.Kind = ColumnAssetKindTCS1TypedColumnPart
			case "order":
				corrupt.AssetRefs[n-1], corrupt.AssetRefs[n-2] = corrupt.AssetRefs[n-2], corrupt.AssetRefs[n-1]
			}
			view.columnSnapshotView = &corrupt
			if _, err := view.FetchDocumentsByID(ids[:1], DocumentFetchOptions{}); err == nil {
				t.Fatal("ordered lookup accepted unrelated malformed manifest tail")
			}
		})
	}
	view.clearDerivedRowFetchCaches()
	if view.validatedPointRowRefs != nil {
		t.Fatal("derived cache reset retained old manifest validation")
	}
}
