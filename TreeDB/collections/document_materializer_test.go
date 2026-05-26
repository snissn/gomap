package collections

import (
	"bytes"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionReadViewFetchDocumentsByIDColumnReconstructionParity(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("e1"), []byte("e2")}
	docs := [][]byte{
		[]byte(`{"row_id":1,"kind":"alpha","score":1.5,"payload":"retained-a"}`),
		[]byte(`{"row_id":2,"kind":"beta","score":2.5,"payload":"retained-b"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	wantE1, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get e1: %v", err)
	}
	wantE2, err := col.Get([]byte("e2"))
	if err != nil {
		t.Fatalf("Get e2: %v", err)
	}

	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	got, err := view.FetchDocumentsByID([][]byte{[]byte("e2"), []byte("missing"), []byte("e1"), []byte("e1")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	if len(got.Results) != 4 {
		t.Fatalf("results=%d want 4", len(got.Results))
	}
	if !got.Results[0].Found || !bytes.Equal(got.Results[0].Document, wantE2) {
		t.Fatalf("e2 found=%t doc=%s want %s", got.Results[0].Found, got.Results[0].Document, wantE2)
	}
	if got.Results[1].Found || got.Results[1].Document != nil {
		t.Fatalf("missing result=%+v want not found", got.Results[1])
	}
	if !got.Results[2].Found || !bytes.Equal(got.Results[2].Document, wantE1) || !got.Results[3].Found || !bytes.Equal(got.Results[3].Document, wantE1) {
		t.Fatalf("e1 duplicate docs=%s/%s want %s", got.Results[2].Document, got.Results[3].Document, wantE1)
	}
	if got.Stats.DocumentsRequested != 4 || got.Stats.DocumentsFetched != 3 || got.Stats.DocumentsMissing != 1 {
		t.Fatalf("stats=%+v want requested=4 fetched=3 missing=1", got.Stats)
	}
	if got.Stats.RetainedPayloadFetches != 3 || got.Stats.VisibilityScans != 1 || got.Stats.VisibilityRowsScanned != 2 || got.Stats.VisibilityRows != 2 || got.Stats.JSONReconstructionRows != 3 {
		t.Fatalf("stats=%+v want retained fetches, one visibility scan over two row-asset rows, three reconstructions", got.Stats)
	}
	if got.Stats.TypedColumnRows != 3 || got.Stats.TypedColumnPartLoads == 0 || got.Stats.TypedColumnPartDecodes == 0 {
		t.Fatalf("stats=%+v want typed_column_part reconstruction counters", got.Stats)
	}

	rowRefFetch, err := view.FetchDocumentsByRowRef([]DocumentRowRef{got.Results[0].RowRef}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByRowRef: %v", err)
	}
	if len(rowRefFetch.Results) != 1 || !rowRefFetch.Results[0].Found || !bytes.Equal(rowRefFetch.Results[0].Document, wantE2) {
		t.Fatalf("row ref fetch=%+v want e2", rowRefFetch.Results)
	}
	badRef := got.Results[0].RowRef
	badRef.RowIndex++
	if _, err := view.FetchDocumentsByRowRef([]DocumentRowRef{badRef}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "row_index") {
		t.Fatalf("bad row ref err=%v want row_index mismatch", err)
	}
}

func TestCollectionReadViewClosedAndNilFailClosed(t *testing.T) {
	var nilView *CollectionReadView
	if _, err := nilView.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "nil collection read view") {
		t.Fatalf("nil FetchDocumentsByID err=%v want fail closed", err)
	}
	if _, err := nilView.FetchDocumentsByRowRef([]DocumentRowRef{{DocumentID: []byte("e1"), RowIndex: 0}}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "nil collection read view") {
		t.Fatalf("nil FetchDocumentsByRowRef err=%v want fail closed", err)
	}

	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("closed FetchDocumentsByID err=%v want fail closed", err)
	}
}

func TestCollectionReadViewMissingAndDeletedMatchCollectionGet(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1"), []byte("e2")},
		[][]byte{
			[]byte(`{"row_id":1,"kind":"alpha","score":1,"payload":"live"}`),
			[]byte(`{"row_id":2,"kind":"beta","score":2,"payload":"deleted"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := col.Delete([]byte("e2")); err != nil {
		t.Fatalf("Delete e2: %v", err)
	}
	if got, err := col.Get([]byte("e2")); err != nil || got != nil {
		t.Fatalf("Get deleted=%s err=%v want missing", got, err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	got, err := view.FetchDocumentsByID([][]byte{[]byte("e1"), []byte("e2"), []byte("missing")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	if !got.Results[0].Found || got.Results[1].Found || got.Results[2].Found {
		t.Fatalf("results=%+v want only e1 found", got.Results)
	}
	if got.Stats.DocumentsFetched != 1 || got.Stats.DocumentsMissing != 2 {
		t.Fatalf("stats=%+v want one fetched and two missing", got.Stats)
	}
}

func TestCollectionReadViewBoundSnapshotIgnoresLaterWrites(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	oldDoc := []byte(`{"row_id":1,"kind":"old","score":1,"payload":"before"}`)
	if _, err := col.Insert([]byte("e1"), oldDoc); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	if err := col.Delete([]byte("e1")); err != nil {
		t.Fatalf("Delete after read-view open: %v", err)
	}
	if live, err := col.Get([]byte("e1")); err != nil || live != nil {
		t.Fatalf("live Get after delete=%s err=%v want missing", live, err)
	}
	got, err := view.FetchDocumentsByID([][]byte{[]byte("e1")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	if len(got.Results) != 1 || !got.Results[0].Found {
		t.Fatalf("results=%+v want old snapshot document", got.Results)
	}
	want, err := reconstructDocumentMaterializerFixtureDoc(oldDoc)
	if err != nil {
		t.Fatalf("reconstruct old fixture: %v", err)
	}
	if !bytes.Equal(got.Results[0].Document, want) {
		t.Fatalf("snapshot document=%s want %s", got.Results[0].Document, want)
	}
}

func TestCollectionReadViewResponseDocumentsAreOwned(t *testing.T) {
	d, col := newDocumentMaterializerTestCollection(t)
	defer func() { _ = d.Close() }()
	if _, err := col.InsertBatch(
		[][]byte{[]byte("e1"), []byte("e2")},
		[][]byte{
			[]byte(`{"row_id":1,"kind":"alpha","score":1,"payload":"first"}`),
			[]byte(`{"row_id":2,"kind":"beta","score":2,"payload":"second"}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	got, err := view.FetchDocumentsByID([][]byte{[]byte("e1"), []byte("e2")}, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	secondBefore := append([]byte(nil), got.Results[1].Document...)
	_ = append(got.Results[0].Document, '!')
	if !bytes.Equal(got.Results[1].Document, secondBefore) {
		t.Fatalf("second document changed after appending to first: got %s want %s", got.Results[1].Document, secondBefore)
	}
	got.Results[0].Document[0] = 'X'
	fresh, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get e1 after mutating response: %v", err)
	}
	if len(fresh) == 0 || fresh[0] == 'X' {
		t.Fatalf("mutating response document affected stored document: %s", fresh)
	}
}

func newDocumentMaterializerTestCollection(t testing.TB) (*backenddb.DB, *Collection) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: cfg}}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	return d, col
}

func reconstructDocumentMaterializerFixtureDoc(doc []byte) ([]byte, error) {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart},
	}
	cfg.RetainedPayload = ColumnRetainedPayloadNonColumn
	retained, err := columnRetainedPayloadFromJSONDocument(*cfg, doc)
	if err != nil {
		return nil, err
	}
	return reconstructColumnJSONDocument(*cfg, retained, []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Int64: 1, Present: true},
		{Type: ColumnStoreValueString, String: "old", Present: true},
		{Type: ColumnStoreValueDouble, Double: 1, Present: true},
	})
}
