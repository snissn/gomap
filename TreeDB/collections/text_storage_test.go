package collections

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestTextStorageCodecsRoundTripAndFailClosed(t *testing.T) {
	postingKey := encodeTextPostingKey("refund", []byte{'d', 0, '1'})
	term, docID, err := decodeTextPostingKey(postingKey)
	if err != nil {
		t.Fatalf("decode postings key: %v", err)
	}
	if term != "refund" || !bytes.Equal(docID, []byte{'d', 0, '1'}) {
		t.Fatalf("postings key term=%q docID=%q", term, docID)
	}
	if !bytes.HasPrefix(postingKey, encodeTextPostingTermPrefix("refund")) {
		t.Fatalf("postings key %v missing term prefix %v", postingKey, encodeTextPostingTermPrefix("refund"))
	}

	posting := textPostingValue{
		TermFrequency: 3,
		Fields: []textPostingFieldValue{{
			Field:     "body",
			Frequency: 3,
			Positions: []uint32{0, 2, 5},
			Offsets:   []textTokenOffset{{Start: 0, End: 6}, {Start: 14, End: 20}, {Start: 30, End: 36}},
		}},
	}
	encodedPosting := encodeTextPostingValue(posting)
	decodedPosting, err := decodeTextPostingValue(encodedPosting)
	if err != nil {
		t.Fatalf("decode postings value: %v", err)
	}
	if decodedPosting.TermFrequency != 3 || len(decodedPosting.Fields) != 1 || decodedPosting.Fields[0].Field != "body" || !slices.Equal(decodedPosting.Fields[0].Positions, []uint32{0, 2, 5}) {
		t.Fatalf("decoded postings=%+v", decodedPosting)
	}
	searchPosting, err := decodeTextPostingValueForSearch(encodedPosting, []string{"body"})
	if err != nil {
		t.Fatalf("decode search postings value: %v", err)
	}
	if searchPosting.TermFrequency != decodedPosting.TermFrequency || searchPosting.fieldCount() != len(decodedPosting.Fields) {
		t.Fatalf("search posting=%+v decoded=%+v", searchPosting, decodedPosting)
	}
	searchField := searchPosting.fieldAt(0)
	if searchField.Field != decodedPosting.Fields[0].Field || searchField.Frequency != decodedPosting.Fields[0].Frequency {
		t.Fatalf("search field=%+v decoded=%+v", searchField, decodedPosting.Fields[0])
	}

	state := textDocumentStateValue{Fields: []textDocumentFieldState{{
		Field:  "body",
		Length: 4,
		Terms: []textDocumentTermState{{
			Term:      "refund",
			Frequency: 2,
			Positions: []uint32{0, 3},
			Offsets:   []textTokenOffset{{Start: 0, End: 6}, {Start: 21, End: 27}},
		}},
	}}}
	encodedState := encodeTextDocumentStateValue(state)
	decodedState, err := decodeTextDocumentStateValue(encodedState)
	if err != nil {
		t.Fatalf("decode text-state value: %v", err)
	}
	if len(decodedState.Fields) != 1 || decodedState.Fields[0].Terms[0].Term != "refund" || decodedState.Fields[0].Terms[0].Frequency != 2 {
		t.Fatalf("decoded state=%+v", decodedState)
	}
	fieldLengths, err := decodeTextDocumentStateFieldLengths(encodedState, nil, []string{"body"})
	if err != nil {
		t.Fatalf("decode search text-state field lengths: %v", err)
	}
	if len(fieldLengths) != len(decodedState.Fields) || fieldLengths[0].Field != decodedState.Fields[0].Field || fieldLengths[0].Length != decodedState.Fields[0].Length {
		t.Fatalf("field lengths=%+v decoded=%+v", fieldLengths, decodedState.Fields)
	}

	malformedPosting := textPostingValue{TermFrequency: 1, Fields: []textPostingFieldValue{{
		Field:     "body",
		Frequency: 1,
		Positions: []uint32{1, 2},
		Offsets:   []textTokenOffset{{Start: 0, End: 1}},
	}}}
	if _, err := decodeTextPostingValueForSearch(encodeTextPostingValue(malformedPosting), []string{"body"}); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("decode malformed search posting err=%v want ErrTextIndexStorageCorrupt", err)
	}
	malformedState := textDocumentStateValue{Fields: []textDocumentFieldState{{
		Field:  "body",
		Length: 2,
		Terms: []textDocumentTermState{{
			Term:      "refund",
			Frequency: 1,
			Positions: []uint32{1, 2},
			Offsets:   []textTokenOffset{{Start: 0, End: 1}},
		}},
	}}}
	if _, err := decodeTextDocumentStateFieldLengths(encodeTextDocumentStateValue(malformedState), nil, []string{"body"}); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("decode malformed search state err=%v want ErrTextIndexStorageCorrupt", err)
	}

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "postings key version", err: func() error { _, _, err := decodeTextPostingKey([]byte{99}); return err }()},
		{name: "postings value version", err: func() error { _, err := decodeTextPostingValue([]byte{99}); return err }()},
		{name: "postings value truncated", err: func() error { _, err := decodeTextPostingValue([]byte{textPostingValueVersion, 1, 1}); return err }()},
		{name: "state key version", err: func() error { _, err := decodeTextStateKey([]byte{99, 'd'}); return err }()},
		{name: "state value version", err: func() error { _, err := decodeTextDocumentStateValue([]byte{99}); return err }()},
		{name: "stats key unknown kind", err: func() error { _, err := decodeTextStatsKey([]byte{textStatsKeyVersion, 99}); return err }()},
		{name: "stats value version", err: func() error { _, err := decodeTextStatsCorpusValue([]byte{99}); return err }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, ErrTextIndexStorageCorrupt) {
				t.Fatalf("err=%v want ErrTextIndexStorageCorrupt", tc.err)
			}
		})
	}
}

func TestTextIndexAnalysisJSONFastPathAndNestedFallbackM2589(t *testing.T) {
	rootDef := TextIndexDefinition{
		Name:           "lexical",
		Fields:         []TextIndexField{{Field: "title"}, {Field: "body"}, {Field: "tags"}},
		StorePositions: true,
		StoreOffsets:   true,
	}
	analysis, err := analyzeTextIndexDocument(rootDef, []byte(`{"title":"Refund\nPolicy","body":["HTTP_500","refund",17,null],"tags":["alpha",""],"nested":{"body":"ignored"}}`))
	if err != nil {
		t.Fatalf("analyze root fast path: %v", err)
	}
	if len(analysis.Fields) != 3 {
		t.Fatalf("root analysis fields=%+v want title/body/tags", analysis.Fields)
	}
	body := requireAnalyzedFieldM2589(t, analysis, "body")
	if body.Length != 2 {
		t.Fatalf("body length=%d want 2", body.Length)
	}
	http := requireAnalyzedTermM2589(t, body, "http_500")
	if http.Frequency != 1 || !slices.Equal(http.Positions, []uint32{0}) || len(http.Offsets) != 1 {
		t.Fatalf("http_500 term=%+v want one positioned term", http)
	}
	if got := requireAnalyzedTermM2589(t, requireAnalyzedFieldM2589(t, analysis, "title"), "policy").Frequency; got != 1 {
		t.Fatalf("title policy frequency=%d want 1", got)
	}
	if got := requireAnalyzedFieldM2589(t, analysis, "tags").Length; got != 1 {
		t.Fatalf("tags length=%d want 1", got)
	}

	escapedRoot, err := analyzeTextIndexDocument(rootDef, []byte(`{"bo\u0064y":"Escaped Refund"}`))
	if err != nil {
		t.Fatalf("analyze escaped root key: %v", err)
	}
	if got := requireAnalyzedTermM2589(t, requireAnalyzedFieldM2589(t, escapedRoot, "body"), "escaped").Frequency; got != 1 {
		t.Fatalf("escaped-key body frequency=%d want 1", got)
	}
	if _, err := analyzeTextIndexDocument(rootDef, []byte(`{"a\\q":"ignored","body":"Backslash Safe"}`)); err != nil {
		t.Fatalf("analyze root with unrelated literal-backslash key: %v", err)
	}
	backslashDef := TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "a\\q"}}}
	backslashRoot, err := analyzeTextIndexDocument(backslashDef, []byte(`{"a\\q":"Backslash Field"}`))
	if err != nil {
		t.Fatalf("analyze literal-backslash root key: %v", err)
	}
	if got := requireAnalyzedTermM2589(t, requireAnalyzedFieldM2589(t, backslashRoot, "a\\q"), "backslash").Frequency; got != 1 {
		t.Fatalf("literal-backslash key frequency=%d want 1", got)
	}

	nestedDef := TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "nested.body"}}, StorePositions: true}
	nested, err := analyzeTextIndexDocument(nestedDef, []byte(`{"nested":{"body":["Deep","Refund"]},"nested.body":"literal should not win"}`))
	if err != nil {
		t.Fatalf("analyze nested fallback: %v", err)
	}
	nestedField := requireAnalyzedFieldM2589(t, nested, "nested.body")
	if nestedField.Length != 2 {
		t.Fatalf("nested field length=%d want 2", nestedField.Length)
	}
	requireAnalyzedTermM2589(t, nestedField, "deep")
	requireAnalyzedTermM2589(t, nestedField, "refund")
	if _, ok := nestedField.Terms["literal"]; ok {
		t.Fatalf("nested fallback indexed literal dotted root field: %+v", nestedField.Terms)
	}

	if _, err := analyzeTextIndexDocument(rootDef, []byte(`{"body":"ok"}{}`)); err == nil {
		t.Fatal("root fast path accepted trailing JSON value")
	}
	if _, err := analyzeTextIndexDocument(rootDef, []byte(`[{"body":"ok"}]`)); err == nil {
		t.Fatal("root fast path accepted non-object JSON document")
	}
}

func requireAnalyzedFieldM2589(t *testing.T, analysis textAnalyzedDocument, field string) textAnalyzedField {
	t.Helper()
	for _, got := range analysis.Fields {
		if got.Field == field {
			return got
		}
	}
	t.Fatalf("missing analyzed field %q in %+v", field, analysis.Fields)
	return textAnalyzedField{}
}

func requireAnalyzedTermM2589(t *testing.T, field textAnalyzedField, term string) *textAnalyzedTerm {
	t.Helper()
	got := field.Terms[term]
	if got == nil {
		t.Fatalf("missing analyzed term %q in field %+v", term, field)
	}
	return got
}

func TestCollectionCreateTextIndexBackfillsReopensAndReportsStorage(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := [][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}
	docs := [][]byte{
		[]byte(`{"title":"Refund Policy","body":"refund failed refund","tags":["policy","HTTP_500"]}`),
		[]byte(`{"title":"Other","body":"shipping policy"}`),
		[]byte(`{"title":"Empty","body":"---"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	meta, backfill, err := col.CreateTextIndex(TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV1,
		Fields:         []TextIndexField{{Field: "body"}, {Field: "title", Weight: 2}, {Field: "tags"}},
		StorePositions: true,
		StoreOffsets:   true,
		StoragePolicy:  RootStorageFast,
	})
	if err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if len(meta.TextIndexes) != 1 || meta.TextIndexes[0].Name != "lexical" {
		t.Fatalf("text indexes after create=%+v", meta.TextIndexes)
	}
	if backfill.DocumentsScanned != 3 || backfill.StateEntries != 3 || backfill.PostingEntries != 8 || backfill.StatsEntries != 11 || backfill.EncodedBytes == 0 {
		t.Fatalf("backfill stats=%+v want docs=3 state=3 postings=8 stats=11 bytes>0", backfill)
	}

	storageStats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if storageStats.Documents != 3 || storageStats.StateEntries != 3 || storageStats.PostingEntries != 8 || storageStats.StatsEntries != 11 || storageStats.EncodedBytes == 0 {
		t.Fatalf("storage stats=%+v want docs=3 state=3 postings=8 stats=11 bytes>0", storageStats)
	}

	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		postingRaw := textRootValue(t, snap, catalog, collectionTextIndexRootName("docs", "lexical"), encodeTextPostingKey("refund", []byte("d1")))
		posting, err := decodeTextPostingValue(postingRaw)
		if err != nil {
			t.Fatalf("decode refund posting: %v", err)
		}
		if posting.TermFrequency != 3 || len(posting.Fields) != 2 {
			t.Fatalf("refund posting=%+v want tf=3 in body+title", posting)
		}
		stateRaw := textRootValue(t, snap, catalog, collectionTextStateRootName("docs", "lexical"), encodeTextStateKey([]byte("d1")))
		state, err := decodeTextDocumentStateValue(stateRaw)
		if err != nil {
			t.Fatalf("decode d1 state: %v", err)
		}
		if len(state.Fields) != 3 {
			t.Fatalf("d1 state fields=%+v want body/title/tags", state.Fields)
		}
		termStatsRaw := textRootValue(t, snap, catalog, collectionTextStatsRootName("docs", "lexical"), encodeTextStatsTermKey("policy"))
		termStats, err := decodeTextStatsTermValue(termStatsRaw)
		if err != nil {
			t.Fatalf("decode policy stats: %v", err)
		}
		if termStats.DocumentFrequency != 2 || termStats.TotalTermFrequency != 3 {
			t.Fatalf("policy stats=%+v want df=2 tf=3", termStats)
		}
	})

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedStats, err := reopenedCol.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("reopened TextIndexStorageStats: %v", err)
	}
	if reopenedStats != storageStats {
		t.Fatalf("reopened stats=%+v want %+v", reopenedStats, storageStats)
	}
}

func TestCollectionDropTextIndexClearsMetadataRootsAndWriteGuard(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1")}, [][]byte{[]byte(`{"body":"refund policy"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	staleMgr := NewCollectionManager(d)
	stale, err := staleMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection stale: %v", err)
	}
	if _, err := col.Insert([]byte("d2"), []byte(`{"body":"maintained before drop"}`)); err != nil {
		t.Fatalf("Insert with text index before drop: %v", err)
	}

	meta, err := col.DropTextIndex("lexical")
	if err != nil {
		t.Fatalf("DropTextIndex: %v", err)
	}
	if len(meta.TextIndexes) != 0 {
		t.Fatalf("text indexes after drop=%+v", meta.TextIndexes)
	}
	if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("TextIndexStorageStats after drop err=%v want ErrIndexNotFound", err)
	}
	assertTextRootCleared(t, d, collectionTextIndexRootName("docs", "lexical"))
	assertTextRootCleared(t, d, collectionTextStateRootName("docs", "lexical"))
	assertTextRootCleared(t, d, collectionTextStatsRootName("docs", "lexical"))
	if _, err := col.Insert([]byte("d3"), []byte(`{"body":"allowed after drop"}`)); err != nil {
		t.Fatalf("Insert after DropTextIndex: %v", err)
	}
	if _, err := stale.Insert([]byte("d4"), []byte(`{"body":"stale handle allowed after drop"}`)); err != nil {
		t.Fatalf("stale Insert after DropTextIndex: %v", err)
	}
}

func TestCreateTextIndexFlushesBufferedWritesFromOtherManagers(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	writerMgr := NewCollectionManager(d)
	if _, err := writerMgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	writer, err := writerMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection writer: %v", err)
	}
	creatorMgr := NewCollectionManager(d)
	creator, err := creatorMgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection creator: %v", err)
	}

	if _, err := writer.Insert([]byte("d-buffered"), []byte(`{"body":"buffered refund policy"}`)); err != nil {
		t.Fatalf("writer buffered Insert: %v", err)
	}
	if got, err := creator.Get([]byte("d-buffered")); err != nil {
		t.Fatalf("creator Get before CreateTextIndex: %v", err)
	} else if got != nil {
		t.Fatalf("setup write was already published; expected buffered write from separate manager, got %q", got)
	}

	_, backfill, err := creator.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}})
	if err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if backfill.DocumentsScanned != 1 || backfill.StateEntries != 1 || backfill.PostingEntries != 3 {
		t.Fatalf("backfill stats=%+v want one flushed buffered document", backfill)
	}
	stats, err := creator.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.Documents != 1 || stats.StateEntries != 1 || stats.PostingEntries != 3 {
		t.Fatalf("storage stats=%+v want one flushed buffered document", stats)
	}
	if got, err := creator.Get([]byte("d-buffered")); err != nil {
		t.Fatalf("creator Get after CreateTextIndex: %v", err)
	} else if !bytes.Contains(got, []byte("buffered refund")) {
		t.Fatalf("flushed document after CreateTextIndex=%q", got)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("writer Flush after CreateTextIndex: %v", err)
	}
	if _, err := writer.Insert([]byte("d-after"), []byte(`{"body":"maintained after create"}`)); err != nil {
		t.Fatalf("writer Insert after CreateTextIndex: %v", err)
	}
	stats, err = creator.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats after writer insert: %v", err)
	}
	if stats.Documents != 2 || stats.StateEntries != 2 || stats.PostingEntries != 6 {
		t.Fatalf("storage stats after writer insert=%+v want docs=2 state=2 postings=6", stats)
	}
}

func TestCreateTextIndexMaintainsWritesFromStaleHandles(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	stale, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection stale: %v", err)
	}
	fresh, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection fresh: %v", err)
	}
	if _, err := stale.Insert([]byte("d1"), []byte(`{"body":"before text"}`)); err != nil {
		t.Fatalf("stale setup Insert: %v", err)
	}
	if _, _, err := fresh.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, err := stale.Insert([]byte("d2"), []byte(`{"body":"maintained stale handle"}`)); err != nil {
		t.Fatalf("stale Insert after CreateTextIndex: %v", err)
	}
	stats, err := fresh.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.Documents != 2 || stats.StateEntries != 2 || stats.PostingEntries != 5 {
		t.Fatalf("stats after stale maintained insert=%+v want two maintained documents", stats)
	}
}

func TestTextIndexStorageStatsFailsClosedOnMalformedRoot(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1")}, [][]byte{[]byte(`{"body":"refund policy"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	corruptTextRootValue(t, d, "docs", collectionTextStatsRootName("docs", "lexical"), encodeTextStatsCorpusKey(), []byte{99})
	if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("TextIndexStorageStats corrupted err=%v want ErrTextIndexStorageCorrupt", err)
	}
}

func BenchmarkCreateTextIndexBackfill(b *testing.B) {
	const docsPerBackfill = 256
	var lastStats TextIndexBackfillStats
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		d := openTextBenchDB(b)
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
			b.Fatalf("CreateCollection: %v", err)
		}
		col, err := mgr.OpenCollection("docs")
		if err != nil {
			b.Fatalf("OpenCollection: %v", err)
		}
		ids := make([][]byte, docsPerBackfill)
		docs := make([][]byte, docsPerBackfill)
		for j := 0; j < docsPerBackfill; j++ {
			ids[j] = []byte(fmt.Sprintf("doc-%06d", j))
			docs[j] = []byte(fmt.Sprintf(`{"title":"Ticket %d refund policy","body":"HTTP_500 retry refund policy customer %d"}`, j, j%17))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			b.Fatalf("InsertBatch setup: %v", err)
		}
		b.StartTimer()
		_, stats, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "title"}, {Field: "body"}}, StorePositions: true})
		b.StopTimer()
		if err != nil {
			b.Fatalf("CreateTextIndex: %v", err)
		}
		lastStats = stats
		_ = d.Close()
	}
	b.ReportMetric(float64(docsPerBackfill), "docs/backfill")
	b.ReportMetric(float64(lastStats.PostingEntries), "postings/backfill")
	b.ReportMetric(float64(lastStats.EncodedBytes), "encoded_bytes/backfill")
}

func openTextTestDB(t *testing.T) *backenddb.DB {
	t.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return d
}

func openTextBenchDB(b *testing.B) *backenddb.DB {
	b.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	return d
}

func withTextCatalog(t *testing.T, d *backenddb.DB, collection string, fn func(*backenddb.Snapshot, *collectionCatalog)) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	if catalog == nil {
		t.Fatal("catalog nil")
	}
	fn(snap, catalog)
}

func textRootValue(t *testing.T, snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, key []byte) []byte {
	t.Helper()
	value, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, key, nil)
	if err != nil {
		t.Fatalf("get text root %q key %v: %v", rootName, key, err)
	}
	if !ok {
		t.Fatalf("text root %q missing key %v", rootName, key)
	}
	return value
}

func assertTextRootCleared(t *testing.T, d *backenddb.DB, rootName string) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	raw, ok, err := getSystemValue(snap, systemCollectionRootKey(rootName))
	if err != nil {
		t.Fatalf("get root descriptor %q: %v", rootName, err)
	}
	if !ok {
		return
	}
	rootID, err := decodeRootID(raw)
	if err != nil {
		t.Fatalf("decode root descriptor %q: %v", rootName, err)
	}
	if rootID != 0 {
		t.Fatalf("root descriptor %q=%d want cleared", rootName, rootID)
	}
}

func corruptTextRootValue(t *testing.T, d *backenddb.DB, collection, rootName string, key, value []byte) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	baseRoot := catalog.rootID(rootName)
	policy, err := collectionRootStoragePolicyForDB(d, catalog.meta, rootName)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("root policy: %v", err)
	}
	_ = snap.Close()
	table := newCollectionRunTable(1)
	table.SetSteal(bytes.Clone(key), bytes.Clone(value))
	table.Freeze()
	defer resetCollectionTables([]memtable.Table{table})
	iter := table.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	ordered := []backenddb.OrderedRootDeltaPublishInput{{BaseRoot: baseRoot, Iter: iter, StoragePolicy: policy}}
	_, rootIDs, err := d.PublishOrderedRootDeltaGroupWithSystemBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("unexpected root ids %d", len(rootIDs))
		}
		current := d.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		return buildSystemTargetIterator(current, map[string][]byte{systemCollectionRootKey(rootName): encodeRootID(rootIDs[0])})
	})
	if err != nil {
		t.Fatalf("publish corrupt root: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}
}
