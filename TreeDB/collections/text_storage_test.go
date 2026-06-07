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
	decodedPosting, err := decodeTextPostingValue(encodeTextPostingValue(posting))
	if err != nil {
		t.Fatalf("decode postings value: %v", err)
	}
	if decodedPosting.TermFrequency != 3 || len(decodedPosting.Fields) != 1 || decodedPosting.Fields[0].Field != "body" || !slices.Equal(decodedPosting.Fields[0].Positions, []uint32{0, 2, 5}) {
		t.Fatalf("decoded postings=%+v", decodedPosting)
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
	decodedState, err := decodeTextDocumentStateValue(encodeTextDocumentStateValue(state))
	if err != nil {
		t.Fatalf("decode text-state value: %v", err)
	}
	if len(decodedState.Fields) != 1 || decodedState.Fields[0].Terms[0].Term != "refund" || decodedState.Fields[0].Terms[0].Frequency != 2 {
		t.Fatalf("decoded state=%+v", decodedState)
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
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, err := col.Insert([]byte("d2"), []byte(`{"body":"blocked"}`)); !errors.Is(err, ErrTextIndexUnavailable) {
		t.Fatalf("Insert with text index err=%v want ErrTextIndexUnavailable", err)
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
	if _, err := col.Insert([]byte("d2"), []byte(`{"body":"allowed after drop"}`)); err != nil {
		t.Fatalf("Insert after DropTextIndex: %v", err)
	}
}

func TestCreateTextIndexRejectsWritesFromStaleHandles(t *testing.T) {
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
	if _, _, err := fresh.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, err := stale.Insert([]byte("d2"), []byte(`{"body":"must not bypass"}`)); !errors.Is(err, ErrTextIndexUnavailable) {
		t.Fatalf("stale Insert after CreateTextIndex err=%v want ErrTextIndexUnavailable", err)
	}
	stats, err := fresh.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.Documents != 1 || stats.StateEntries != 1 {
		t.Fatalf("stats after stale rejected insert=%+v want one backfilled document", stats)
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
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "body"}}}); err != nil {
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
		_, stats, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "title"}, {Field: "body"}}, StorePositions: true})
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
