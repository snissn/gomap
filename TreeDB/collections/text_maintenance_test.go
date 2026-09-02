package collections

import (
	"bytes"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTextIndexMaintenanceInsertMaintainsPostingsStateStats(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextMaintenanceCollection(t, d)

	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund refund policy"}`)); err != nil {
		t.Fatalf("Insert d1: %v", err)
	}
	if _, err := col.Insert([]byte("d2"), []byte(`{"body":"shipping policy"}`)); err != nil {
		t.Fatalf("Insert d2: %v", err)
	}
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.Documents != 2 || stats.StateEntries != 2 || stats.PostingEntries != 4 || stats.StatsEntries != 5 {
		t.Fatalf("stats=%+v want docs=2 state=2 postings=4 stats=5", stats)
	}
	posting := requireTextPosting(t, d, "docs", "lexical", "refund", []byte("d1"))
	if posting.TermFrequency != 2 || len(posting.Fields) != 1 || posting.Fields[0].Frequency != 2 || len(posting.Fields[0].Positions) != 2 {
		t.Fatalf("refund posting=%+v want tf=2 with positions", posting)
	}
	policyStats := requireTextTermStats(t, d, "docs", "lexical", "policy")
	if policyStats.DocumentFrequency != 2 || policyStats.TotalTermFrequency != 2 {
		t.Fatalf("policy stats=%+v want df=2 tf=2", policyStats)
	}
}

func TestTextIndexMaintenanceDeleteRemovesPostingsAfterFlushReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	col := createTextMaintenanceCollection(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2")}, [][]byte{[]byte(`{"body":"refund policy"}`), []byte(`{"body":"shipping policy"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if deleted, err := col.DeleteDocument([]byte("d1")); err != nil || !deleted {
		t.Fatalf("DeleteDocument deleted=%v err=%v", deleted, err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
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
	stats, err := reopenedCol.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats reopened: %v", err)
	}
	if stats.Documents != 1 || stats.StateEntries != 1 || stats.PostingEntries != 2 {
		t.Fatalf("reopened stats=%+v want one remaining document", stats)
	}
	assertTextPostingMissing(t, reopened, "docs", "lexical", "refund", []byte("d1"))
	assertTextStateMissing(t, reopened, "docs", "lexical", []byte("d1"))
	shipping := requireTextPosting(t, reopened, "docs", "lexical", "shipping", []byte("d2"))
	if shipping.TermFrequency != 1 {
		t.Fatalf("shipping posting=%+v want tf=1", shipping)
	}
	assertTextTermStatsMissing(t, reopened, "docs", "lexical", "refund")
}

func TestTextIndexMaintenanceUpdateRemovesOldAndAddsNewTerms(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextMaintenanceCollection(t, d)
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	matched, modified, err := col.Update([]byte("d1"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"body":"shipping shipping policy"}`), true, nil
	})
	if err != nil || !matched || !modified {
		t.Fatalf("Update matched=%v modified=%v err=%v", matched, modified, err)
	}
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.Documents != 1 || stats.StateEntries != 1 || stats.PostingEntries != 2 || stats.StatsEntries != 4 {
		t.Fatalf("stats after update=%+v want docs=1 state=1 postings=2 stats=4", stats)
	}
	assertTextPostingMissing(t, d, "docs", "lexical", "refund", []byte("d1"))
	assertTextTermStatsMissing(t, d, "docs", "lexical", "refund")
	shipping := requireTextPosting(t, d, "docs", "lexical", "shipping", []byte("d1"))
	if shipping.TermFrequency != 2 {
		t.Fatalf("shipping posting=%+v want tf=2", shipping)
	}
	shippingStats := requireTextTermStats(t, d, "docs", "lexical", "shipping")
	if shippingStats.DocumentFrequency != 1 || shippingStats.TotalTermFrequency != 2 {
		t.Fatalf("shipping stats=%+v want df=1 tf=2", shippingStats)
	}
}

func TestTextIndexMaintenanceBatchInsertUpdateDelete(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextMaintenanceCollection(t, d)
	ids := [][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}
	docs := [][]byte{[]byte(`{"body":"alpha keep"}`), []byte(`{"body":"beta keep"}`), []byte(`{"body":"gamma keep"}`)}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	updates := []UpdateBatchItem{
		{DocumentID: []byte("d1"), Update: func(current []byte) ([]byte, bool, error) { return []byte(`{"body":"alpha updated"}`), true, nil }},
		{DocumentID: []byte("d2"), Update: func(current []byte) ([]byte, bool, error) { return []byte(`{"body":"beta updated"}`), true, nil }},
	}
	results, err := col.UpdateBatch(updates)
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != 2 || !results[0].Modified || !results[1].Modified {
		t.Fatalf("UpdateBatch results=%+v want two modified", results)
	}
	deleted, err := col.DeleteBatch([][]byte{[]byte("d3")})
	if err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.Documents != 2 || stats.StateEntries != 2 || stats.PostingEntries != 4 {
		t.Fatalf("batch stats=%+v want two updated documents", stats)
	}
	assertTextPostingMissing(t, d, "docs", "lexical", "gamma", []byte("d3"))
	requireTextPosting(t, d, "docs", "lexical", "updated", []byte("d1"))
	requireTextPosting(t, d, "docs", "lexical", "updated", []byte("d2"))
}

func TestTextIndexMaintenanceBufferedCreateFlushCheckpointReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	writer, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection writer: %v", err)
	}
	if _, err := writer.Insert([]byte("buffered"), []byte(`{"body":"buffered before create"}`)); err != nil {
		t.Fatalf("buffered Insert before CreateTextIndex: %v", err)
	}
	creator, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection creator: %v", err)
	}
	if got, err := creator.Get([]byte("buffered")); err != nil {
		t.Fatalf("creator Get before CreateTextIndex: %v", err)
	} else if got != nil {
		t.Fatalf("expected setup insert to remain buffered before CreateTextIndex, got %q", got)
	}
	if _, _, err := creator.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}, StorePositions: true}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, err := writer.Insert([]byte("after"), []byte(`{"body":"after create durable"}`)); err != nil {
		t.Fatalf("Insert after CreateTextIndex: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("writer Flush: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	col, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats reopened: %v", err)
	}
	if stats.Documents != 2 || stats.StateEntries != 2 || stats.PostingEntries != 6 {
		t.Fatalf("reopened stats=%+v want two maintained documents", stats)
	}
	requireTextPosting(t, reopened, "docs", "lexical", "buffered", []byte("buffered"))
	requireTextPosting(t, reopened, "docs", "lexical", "after", []byte("after"))
}

func BenchmarkTextIndexMaintenanceBatchWritePaths(b *testing.B) {
	const docsPerBatch = 256
	b.Run("insert_batch_no_text", func(b *testing.B) {
		ids, docs := textMaintenanceBenchDocuments(docsPerBatch, "insert")
		b.ReportAllocs()
		b.ReportMetric(docsPerBatch, "docs/batch")
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
			batchIDs := cloneBenchIDsWithIteration(ids, i)
			b.StartTimer()
			_, err = col.InsertBatch(batchIDs, docs)
			b.StopTimer()
			if err != nil {
				b.Fatalf("InsertBatch: %v", err)
			}
			_ = d.Close()
		}
	})
	b.Run("insert_batch_text_indexed", func(b *testing.B) {
		ids, docs := textMaintenanceBenchDocuments(docsPerBatch, "insert")
		var lastStats TextIndexStorageStats
		b.ReportAllocs()
		b.ReportMetric(docsPerBatch, "docs/batch")
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			d := openTextBenchDB(b)
			col := createTextMaintenanceBenchCollection(b, d)
			batchIDs := cloneBenchIDsWithIteration(ids, i)
			b.StartTimer()
			_, err := col.InsertBatch(batchIDs, docs)
			b.StopTimer()
			if err != nil {
				b.Fatalf("InsertBatch text-indexed: %v", err)
			}
			lastStats, err = col.TextIndexStorageStats("lexical")
			if err != nil {
				b.Fatalf("TextIndexStorageStats: %v", err)
			}
			_ = d.Close()
		}
		b.ReportMetric(float64(lastStats.PostingEntries), "postings/batch")
		b.ReportMetric(float64(lastStats.EncodedBytes), "encoded_bytes/batch")
	})
	b.Run("update_batch_text_indexed", func(b *testing.B) {
		ids, docs := textMaintenanceBenchDocuments(docsPerBatch, "update-old")
		_, updatedDocs := textMaintenanceBenchDocuments(docsPerBatch, "update-new")
		updates := make([]UpdateBatchItem, docsPerBatch)
		for i := 0; i < docsPerBatch; i++ {
			docID := bytes.Clone(ids[i])
			replacement := bytes.Clone(updatedDocs[i])
			updates[i] = UpdateBatchItem{DocumentID: docID, Update: func(current []byte) ([]byte, bool, error) {
				return replacement, true, nil
			}}
		}
		b.ReportAllocs()
		b.ReportMetric(docsPerBatch, "docs/batch")
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			d := openTextBenchDB(b)
			col := createTextMaintenanceBenchCollection(b, d)
			batchIDs := cloneBenchIDsWithIteration(ids, i)
			batchUpdates := make([]UpdateBatchItem, docsPerBatch)
			for j := 0; j < docsPerBatch; j++ {
				batchUpdates[j] = updates[j]
				batchUpdates[j].DocumentID = batchIDs[j]
			}
			if _, err := col.InsertBatch(batchIDs, docs); err != nil {
				b.Fatalf("InsertBatch setup: %v", err)
			}
			b.StartTimer()
			results, err := col.UpdateBatch(batchUpdates)
			b.StopTimer()
			if err != nil {
				b.Fatalf("UpdateBatch text-indexed: %v", err)
			}
			if len(results) != docsPerBatch {
				b.Fatalf("UpdateBatch results=%d want %d", len(results), docsPerBatch)
			}
			_ = d.Close()
		}
	})
	b.Run("delete_batch_text_indexed", func(b *testing.B) {
		ids, docs := textMaintenanceBenchDocuments(docsPerBatch, "delete")
		b.ReportAllocs()
		b.ReportMetric(docsPerBatch, "docs/batch")
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			d := openTextBenchDB(b)
			col := createTextMaintenanceBenchCollection(b, d)
			batchIDs := cloneBenchIDsWithIteration(ids, i)
			if _, err := col.InsertBatch(batchIDs, docs); err != nil {
				b.Fatalf("InsertBatch setup: %v", err)
			}
			b.StartTimer()
			deleted, err := col.DeleteBatch(batchIDs)
			b.StopTimer()
			if err != nil {
				b.Fatalf("DeleteBatch text-indexed: %v", err)
			}
			if deleted != docsPerBatch {
				b.Fatalf("DeleteBatch deleted=%d want %d", deleted, docsPerBatch)
			}
			_ = d.Close()
		}
	})
}

func textMaintenanceBenchDocuments(count int, label string) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%s-%06d", label, i))
		docs[i] = []byte(fmt.Sprintf(`{"title":"Ticket %s %d refund policy","body":"HTTP_500 retry refund policy customer %d batch %s"}`, label, i, i%17, label))
	}
	return ids, docs
}

func cloneBenchIDsWithIteration(ids [][]byte, iteration int) [][]byte {
	out := make([][]byte, len(ids))
	for i := range ids {
		out[i] = []byte(fmt.Sprintf("%s-%06d", ids[i], iteration))
	}
	return out
}

func createTextMaintenanceBenchCollection(b *testing.B, d *backenddb.DB) *Collection {
	b.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		b.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		b.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "title"}, {Field: "body"}}, StorePositions: true}); err != nil {
		b.Fatalf("CreateTextIndex: %v", err)
	}
	return col
}

func createTextMaintenanceCollection(t *testing.T, d *backenddb.DB) *Collection {
	t.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}, StorePositions: true}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	return col
}

func requireTextPosting(t *testing.T, d *backenddb.DB, collection, indexName, term string, documentID []byte) textPostingValue {
	t.Helper()
	raw, ok := lookupTextRootValue(t, d, collection, collectionTextIndexRootName(collection, indexName), encodeTextPostingKey(term, documentID))
	if !ok {
		t.Fatalf("missing text posting term=%q document=%q", term, documentID)
	}
	posting, err := decodeTextPostingValue(raw)
	if err != nil {
		t.Fatalf("decode text posting term=%q document=%q: %v", term, documentID, err)
	}
	return posting
}

func assertTextPostingMissing(t *testing.T, d *backenddb.DB, collection, indexName, term string, documentID []byte) {
	t.Helper()
	if raw, ok := lookupTextRootValue(t, d, collection, collectionTextIndexRootName(collection, indexName), encodeTextPostingKey(term, documentID)); ok {
		t.Fatalf("unexpected text posting term=%q document=%q raw=%v", term, documentID, raw)
	}
}

func assertTextStateMissing(t *testing.T, d *backenddb.DB, collection, indexName string, documentID []byte) {
	t.Helper()
	if raw, ok := lookupTextRootValue(t, d, collection, collectionTextStateRootName(collection, indexName), encodeTextStateKey(documentID)); ok {
		t.Fatalf("unexpected text state document=%q raw=%v", documentID, raw)
	}
}

func requireTextTermStats(t *testing.T, d *backenddb.DB, collection, indexName, term string) textStatsTermValue {
	t.Helper()
	raw, ok := lookupTextRootValue(t, d, collection, collectionTextStatsRootName(collection, indexName), encodeTextStatsTermKey(term))
	if !ok {
		t.Fatalf("missing text term stats term=%q", term)
	}
	stats, err := decodeTextStatsTermValue(raw)
	if err != nil {
		t.Fatalf("decode text term stats term=%q: %v", term, err)
	}
	return stats
}

func assertTextTermStatsMissing(t *testing.T, d *backenddb.DB, collection, indexName, term string) {
	t.Helper()
	if raw, ok := lookupTextRootValue(t, d, collection, collectionTextStatsRootName(collection, indexName), encodeTextStatsTermKey(term)); ok {
		t.Fatalf("unexpected text term stats term=%q raw=%v", term, raw)
	}
}

func lookupTextRootValue(t *testing.T, d *backenddb.DB, collection, rootName string, key []byte) ([]byte, bool) {
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
	value, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, key, nil)
	if err != nil {
		t.Fatalf("lookup text root %q key=%v: %v", rootName, key, err)
	}
	return bytes.Clone(value), ok
}
