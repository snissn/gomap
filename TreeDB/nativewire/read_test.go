package nativewire

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func seedReadCollection(t *testing.T, mgr *collections.CollectionManager) *collections.Collection {
	t.Helper()
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name: "users",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
		Indexes: []collections.IndexDefinition{
			{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: collections.IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("u1"), []byte("u2")}, [][]byte{
		[]byte(`{"email":"ada@example.com","city":"hnl","name":"Ada"}`),
		[]byte(`{"email":"grace@example.com","city":"hnl","name":"Grace"}`),
	}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	return col
}

func TestReadCommandsParity(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	col := seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	docs, present, err := client.GetMany(ctx, "users", [][]byte{[]byte("u2"), []byte("missing"), []byte("u1")})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if got, want := present, []bool{true, false, true}; !boolSlicesEqual(got, want) {
		t.Fatalf("present=%v want %v", got, want)
	}
	directU2, err := col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("direct get u2: %v", err)
	}
	if !bytes.Equal(docs[0], directU2) || len(docs[1]) != 0 {
		t.Fatalf("docs mismatch docs=%q directU2=%q", docs, directU2)
	}
	handle, err := client.OpenCollection(ctx, "users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	handleDocs, handlePresent, err := client.GetManyHandle(ctx, handle, [][]byte{[]byte("u1"), []byte("missing")})
	if err != nil {
		t.Fatalf("GetManyHandle: %v", err)
	}
	if got, want := handlePresent, []bool{true, false}; !boolSlicesEqual(got, want) {
		t.Fatalf("handle present=%v want %v", got, want)
	}
	if !bytes.Contains(handleDocs[0], []byte(`"Ada"`)) || len(handleDocs[1]) != 0 {
		t.Fatalf("handle docs=%q", handleDocs)
	}

	ids, truncated, err := client.IndexLookup(ctx, "users", "email", "ada@example.com", CursorLimits{MaxItems: 10})
	if err != nil {
		t.Fatalf("IndexLookup: %v", err)
	}
	if truncated || len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("lookup ids=%q truncated=%v", ids, truncated)
	}
	directIDs, directTruncated, err := col.FindByIndexValueLimit("email", "ada@example.com", 10)
	if err != nil {
		t.Fatalf("direct FindByIndexValueLimit: %v", err)
	}
	if directTruncated != truncated || !byteMatrixEqual(ids, directIDs) {
		t.Fatalf("lookup ids=%q trunc=%v direct ids=%q trunc=%v", ids, truncated, directIDs, directTruncated)
	}

	rangeIDs, rangeTruncated, err := client.IndexRange(ctx, "users", "city", IndexRange{
		Lower:          Scalar{Value: "h"},
		LowerInclusive: true,
		Upper:          Scalar{Value: "z"},
		UpperInclusive: true,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("IndexRange: %v", err)
	}
	if rangeTruncated || len(rangeIDs) != 2 {
		t.Fatalf("range ids=%q truncated=%v", rangeIDs, rangeTruncated)
	}
}

func TestOpenScanCursorLifecycle(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if len(first.IDs) != 1 || first.Cursor.CursorID == 0 || !first.Cursor.HasMore {
		t.Fatalf("first=%+v", first)
	}
	second, err := client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 10})
	if err != nil {
		t.Fatalf("CursorNext: %v", err)
	}
	if len(second.IDs) != 1 || second.Cursor.HasMore {
		t.Fatalf("second=%+v", second)
	}
	_, err = client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1})
	if !isRemoteError(err, iwire.ErrCursorNotFound) {
		t.Fatalf("CursorNext exhausted error=%v want cursor_not_found", err)
	}
}

func TestCursorClose(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if err := client.CursorClose(ctx, first.Cursor.CursorID); err != nil {
		t.Fatalf("CursorClose: %v", err)
	}
	_, err = client.CursorNext(ctx, first.Cursor.CursorID, CursorLimits{MaxItems: 1})
	if !isRemoteError(err, iwire.ErrCursorNotFound) {
		t.Fatalf("CursorNext after close error=%v want cursor_not_found", err)
	}
}

func boolSlicesEqual(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func byteMatrixEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
