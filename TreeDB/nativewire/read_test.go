package nativewire

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
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
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

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

func TestIndexLookupWithoutLimitsReturnsAllMatches(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexLookup(ctx, "users", "city", "hnl", CursorLimits{})
	if err != nil {
		t.Fatalf("IndexLookup: %v", err)
	}
	if truncated || len(ids) != 2 {
		t.Fatalf("ids=%q truncated=%v want two untruncated matches", ids, truncated)
	}
}

func TestIndexLookupByteOnlyLimitTruncatesIDs(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexLookup(ctx, "users", "city", "hnl", CursorLimits{MaxBytes: 2})
	if err != nil {
		t.Fatalf("IndexLookup: %v", err)
	}
	if !truncated || len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("ids=%q truncated=%v want first ID with truncation", ids, truncated)
	}
}

func TestIndexLookupDefaultResultBoundUsesWireLimit(t *testing.T) {
	client, mgr, _ := serveCollectionPipeWithOptions(t, ServerOptions{
		Limits: iwire.Limits{MaxByteVectorItems: 1},
	})
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexLookup(ctx, "users", "city", "hnl", CursorLimits{})
	if err != nil {
		t.Fatalf("IndexLookup: %v", err)
	}
	if !truncated || len(ids) != 1 {
		t.Fatalf("ids=%q truncated=%v want one bounded match with truncation", ids, truncated)
	}
}

func TestIndexRangeByteOnlyLimitTruncatesIDs(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexRange(ctx, "users", "city", IndexRange{
		LowerUnbounded: true,
		UpperUnbounded: true,
		MaxBytes:       2,
	})
	if err != nil {
		t.Fatalf("IndexRange: %v", err)
	}
	if !truncated || len(ids) != 1 || string(ids[0]) != "u1" {
		t.Fatalf("ids=%q truncated=%v want first ID with truncation", ids, truncated)
	}
}

func TestIndexRangeByteOnlyLimitDoesNotBecomeQueryLimit(t *testing.T) {
	_, opts, limits, err := indexRangeRequest([]iwire.Section{
		{ID: iwire.SectionIndexName, Bytes: encodeIndexName("city")},
		{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(CursorLimits{MaxBytes: 1 << 20})},
	})
	if err != nil {
		t.Fatalf("indexRangeRequest: %v", err)
	}
	if opts.Limit != 0 {
		t.Fatalf("opts.Limit=%d want 0 for byte-only limits", opts.Limit)
	}
	if limits.MaxBytes != 1<<20 {
		t.Fatalf("limits.MaxBytes=%d want byte limit preserved", limits.MaxBytes)
	}

	_, opts, _, err = indexRangeRequest([]iwire.Section{
		{ID: iwire.SectionIndexName, Bytes: encodeIndexName("city")},
		{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(CursorLimits{MaxItems: 7, MaxBytes: 1 << 20})},
	})
	if err != nil {
		t.Fatalf("indexRangeRequest with item limit: %v", err)
	}
	if opts.Limit != 7 {
		t.Fatalf("opts.Limit=%d want explicit item limit", opts.Limit)
	}
}

func TestIndexRangeOmittedBoundsAreUnbounded(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	ids, truncated, err := client.IndexRange(ctx, "users", "city", IndexRange{
		LowerUnbounded: true,
		UpperUnbounded: true,
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("IndexRange: %v", err)
	}
	if truncated || len(ids) != 2 {
		t.Fatalf("ids=%q truncated=%v want full unbounded range", ids, truncated)
	}
}

func TestDecodeDocumentsResultRejectsMismatchedVectors(t *testing.T) {
	_, err := decodeDocumentsResult([]iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("a"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte("{}"), []byte("{}"))},
	}, iwire.DefaultLimits())
	if nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeDocumentsResult err=%v code=%d want malformed frame", err, nativeCodeOf(err))
	}
}

func TestDecodeIDsAndTruncatedRejectsTrailingTruncatedBytes(t *testing.T) {
	_, _, err := decodeIDsAndTruncated([]iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("a"))},
		{ID: iwire.SectionTruncated, Bytes: []byte{1, 0}},
	}, iwire.DefaultLimits())
	if nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeIDsAndTruncated err=%v code=%d want malformed frame", err, nativeCodeOf(err))
	}
}

func TestDecodeDocumentsResultRejectsTrailingTruncatedBytes(t *testing.T) {
	_, err := decodeDocumentsResult([]iwire.Section{
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("a"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte("{}"))},
		{ID: iwire.SectionTruncated, Bytes: []byte{1, 0}},
	}, iwire.DefaultLimits())
	if nativeCodeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeDocumentsResult err=%v code=%d want malformed frame", err, nativeCodeOf(err))
	}
}

func TestOpenScanReportsTruncatedRetainedWindow(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, MaxScanDocuments: 1})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 10})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if len(first.IDs) != 1 || first.Cursor.CursorID != 0 || first.Cursor.HasMore || !first.Truncated {
		t.Fatalf("first=%+v want one truncated terminal batch", first)
	}
}

func TestOpenScanReportsTruncatedWhenCursorRetentionExceeded(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, MaxCursorRetainedBytes: 1})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if len(first.IDs) != 1 || first.Cursor.CursorID != 0 || first.Cursor.HasMore || !first.Truncated {
		t.Fatalf("first=%+v want one truncated terminal batch", first)
	}
}

func TestOpenScanDefaultBatchHonorsFrameLimit(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name:    "users",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := []byte(`{"payload":"` + strings.Repeat("x", 512) + `"}`)
	if _, err := col.InsertBatch([][]byte{[]byte("u1"), []byte("u2")}, [][]byte{doc, doc}); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	server := NewServer(ServerOptions{
		Collections:            mgr,
		Backend:                db,
		DefaultCursorBatchSize: 10,
		Limits:                 iwire.Limits{MaxFrameSize: 700},
	})
	client, _ := servePipe(t, server)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	first, err := client.OpenScan(ctx, "users", CursorLimits{})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	if len(first.IDs) != 1 || first.Cursor.CursorID == 0 || !first.Cursor.HasMore {
		t.Fatalf("first=%+v want one frame-limited cursor batch", first)
	}
	if err := client.CursorClose(ctx, first.Cursor.CursorID); err != nil {
		t.Fatalf("CursorClose cleanup: %v", err)
	}
}

func TestOpenScanCursorLifecycle(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

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

func TestCursorNextRequiresCursorRef(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	first, err := client.OpenScan(ctx, "users", CursorLimits{MaxItems: 1})
	if err != nil {
		t.Fatalf("OpenScan: %v", err)
	}
	_, err = client.commandSections(ctx, iwire.CommandCursorNext, iwire.Section{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(CursorLimits{MaxItems: 1})})
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("CursorNext without cursor_ref err=%v want invalid command", err)
	}
	_, err = client.commandSectionsOnStream(ctx, first.Cursor.CursorID+1, iwire.CommandCursorNext,
		iwire.Section{ID: iwire.SectionCursorRef, Bytes: encodeCursorRef(first.Cursor.CursorID)},
		iwire.Section{ID: iwire.SectionCursorLimits, Bytes: encodeCursorLimits(CursorLimits{MaxItems: 1})},
	)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("CursorNext stream/cursor_ref mismatch err=%v want invalid command", err)
	}
	if err := client.CursorClose(ctx, first.Cursor.CursorID); err != nil {
		t.Fatalf("CursorClose cleanup: %v", err)
	}
}

func TestCursorClose(t *testing.T) {
	client, mgr, _ := serveCollectionPipe(t)
	seedReadCollection(t, mgr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
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
