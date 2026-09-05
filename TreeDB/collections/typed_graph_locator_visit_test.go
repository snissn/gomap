package collections

import (
	"bytes"
	"errors"
	"testing"
)

func TestTypedGraphLocatorVisitorOwnership(t *testing.T) {
	db, col := newDocumentMaterializerTestCollection(t)
	defer db.Close()
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{[]byte(`{"row_id":1,"kind":"a","score":1.5}`), []byte(`{"row_id":2,"kind":"b","score":2.5}`)}); err != nil {
		t.Fatal(err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	ids := [][]byte{[]byte("e1"), []byte("missing"), []byte("e2")}
	owned, err := view.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	visited := 0
	stats, err := view.visitDocumentRowRefsByID(ids, func(id []byte, ref DocumentRowRef, found bool) error {
		want := owned.Results[visited]
		if !bytes.Equal(id, want.ID) || found != want.Found {
			t.Fatalf("visitor row%d id=%s found=%t", visited, id, found)
		}
		if found && (ref.Generation != want.RowRef.Generation || ref.PartID != want.RowRef.PartID || ref.RowIndex != want.RowRef.RowIndex || ref.AppliedCommandLSN != want.RowRef.AppliedCommandLSN || &ref.DocumentID[0] != &id[0]) {
			t.Fatalf("visitor row%d must borrow ID and preserve coordinates: %+v", visited, ref)
		}
		visited++
		return nil
	})
	if err != nil || visited != 3 || stats.RowLocatorLookups != 3 || stats.RowLocatorMisses != 1 {
		t.Fatalf("visit count=%d stats=%+v err=%v", visited, stats, err)
	}
	ids[0][0] = 'x'
	if string(owned.Results[0].ID) != "e1" || string(owned.Results[0].RowRef.DocumentID) != "e1" {
		t.Fatal("public response borrowed caller ID")
	}
	owned.Results[0].ID[0] = 'z'
	if string(owned.Results[0].RowRef.DocumentID) != "e1" {
		t.Fatal("public result ID mutation changed independently owned row-ref ID")
	}
	injected := errors.New("stop visitor")
	visited = 0
	_, err = view.visitDocumentRowRefsByID(ids, func([]byte, DocumentRowRef, bool) error { visited++; return injected })
	if !errors.Is(err, injected) || visited != 1 {
		t.Fatalf("callback error not preserved: visited=%d err=%v", visited, err)
	}
	if _, err := view.visitDocumentRowRefsByID(nil, nil); err != nil {
		t.Fatalf("empty visitor: %v", err)
	}
	if _, err := view.visitDocumentRowRefsByID([][]byte{nil}, func([]byte, DocumentRowRef, bool) error { return nil }); err == nil {
		t.Fatal("empty ID accepted")
	}
	ref := DocumentRowRef{DocumentID: []byte("owned"), Generation: 3, PartID: 2, RowIndex: 4, AppliedCommandLSN: 9}
	encoded := encodeColumnPrimaryRowLocator(ref)
	borrowed, err := decodeColumnPrimaryRowLocatorBorrowedID(ref.DocumentID, encoded)
	if err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	if borrowed.Generation != 3 || borrowed.PartID != 2 || borrowed.RowIndex != 4 || borrowed.AppliedCommandLSN != 9 {
		t.Fatal("borrowed decoder retained encoded locator scratch")
	}
	for _, invalid := range [][]byte{nil, encoded, []byte("CRL1")} {
		if _, err := decodeColumnPrimaryRowLocatorBorrowedID(ref.DocumentID, invalid); err == nil {
			t.Fatal("invalid locator accepted")
		}
	}
}
