package collections

import (
	"bytes"
	"errors"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func TestCollectionUpdateBatchDirectBufferedBSONSkipsUnchangedSecondaryRoots(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "email", Value: "a@example.com"},
			{Key: "city", Value: "hnl"},
			{Key: "score", Value: int32(1)},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setBSONField("score", int32(2))},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatal("BSON non-indexed update was declined")
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one matched modified row", results)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("direct-buffered BSON update advanced commit seq by %d, want 0", after.CommitSeq-before.CommitSeq)
	}

	rootCounts, rootRunCount := bufferedRootRunCountsForTest(t, col,
		collectionPrimaryRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	)
	overlayCount := bufferedPrimaryOverlayCountForTest(t, col)
	if got, want := rootRunCount, 0; got != want {
		t.Fatalf("rootRunCount=%d want %d before flush for primary-only overlay update", got, want)
	}
	if got, want := rootCounts[collectionPrimaryRootName("users")], 0; got != want {
		t.Fatalf("primary runs=%d want %d before flush for primary-only overlay update", got, want)
	}
	if got, want := overlayCount, 1; got != want {
		t.Fatalf("primary overlay entries=%d want %d", got, want)
	}
	for _, rootName := range []string{
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if got := rootCounts[rootName]; got != 0 {
			t.Fatalf("secondary root %q runs=%d want 0 for unchanged indexed values", rootName, got)
		}
	}
	emailIDs, err := col.FindByIndex("email", "a@example.com")
	if err != nil {
		t.Fatalf("find email: %v", err)
	}
	if len(emailIDs) != 1 || !bytes.Equal(emailIDs[0], []byte("u1")) {
		t.Fatalf("email ids=%q want [u1]", emailIDs)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get buffered BSON document: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(got, &doc); err != nil {
		t.Fatalf("unmarshal buffered BSON document: %v", err)
	}
	if gotScore := doc["score"]; gotScore != int32(2) {
		t.Fatalf("buffered BSON score=%v want int32(2)", gotScore)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush primary-only overlay update: %v", err)
	}
	if got, want := bufferedPrimaryCacheCountForTest(t, col), 1; got != want {
		t.Fatalf("primary cache entries after flush=%d want %d", got, want)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get flushed BSON document: %v", err)
	}
	if gotScore := bson.Raw(got).Lookup("score").Int32(); gotScore != int32(2) {
		t.Fatalf("flushed BSON score=%v want int32(2)", gotScore)
	}
	results, batched, err = col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setBSONField("score", int32(3))},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges after cache-retained flush: %v", err)
	}
	if !batched {
		t.Fatal("cached BSON update was declined")
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("cached update results=%+v want one matched modified row", results)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get second buffered BSON document: %v", err)
	}
	if gotScore := bson.Raw(got).Lookup("score").Int32(); gotScore != int32(3) {
		t.Fatalf("second buffered BSON score=%v want int32(3)", gotScore)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush second primary-only overlay update: %v", err)
	}
	if got, want := bufferedPrimaryCacheCountForTest(t, col), 1; got != want {
		t.Fatalf("primary cache entries after second flush=%d want %d", got, want)
	}
	deleted, err := col.DeleteDocument([]byte("u1"))
	if err != nil {
		t.Fatalf("delete document: %v", err)
	}
	if !deleted {
		t.Fatal("delete document deleted=false want true")
	}
	if got := bufferedPrimaryCacheCountForTest(t, col); got != 0 {
		t.Fatalf("primary cache entries after delete=%d want 0", got)
	}
}

func TestCollectionUpdateBatchDirectBufferedBSONReadsDetachedAsyncPrimaryOverlay(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:            DocumentFormatBSON,
			BufferedIndexedWrites:     true,
			BufferedIndexedAsyncFlush: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: "u1"},
			{Key: "email", Value: "a@example.com"},
			{Key: "city", Value: "hnl"},
			{Key: "score", Value: int32(1)},
		})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	updateSet := func(field string, value bson.RawValue) BSONSetUpdateBatchItem {
		return BSONSetUpdateBatchItem{
			DocumentID: []byte("u1"),
			Fields: []BSONSetField{{
				Key:   field,
				Value: value,
			}},
		}
	}
	if results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		updateSet("score", bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 2)}),
	}); err != nil {
		t.Fatalf("first UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched || len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("first results=%+v batched=%v want one matched modified row", results, batched)
	}

	col.writeDomain.mu.Lock()
	if !rotateIndexedMutableToFlushUnitForAsyncLocked(col.writeDomain) {
		col.writeDomain.mu.Unlock()
		t.Fatal("rotate async flush unit returned false")
	}
	if col.writeDomain.primaryOverlay != nil {
		col.writeDomain.mu.Unlock()
		t.Fatal("primary overlay remained mutable after async rotation")
	}
	if got := len(col.writeDomain.indexedFlushUnits); got != 1 {
		col.writeDomain.mu.Unlock()
		t.Fatalf("indexed flush units=%d want 1", got)
	}
	if got := col.writeDomain.indexedFlushUnits[0].primaryOverlay.len(); got != 1 {
		col.writeDomain.mu.Unlock()
		t.Fatalf("detached primary overlay entries=%d want 1", got)
	}
	col.writeDomain.mu.Unlock()

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get detached overlay BSON document: %v", err)
	}
	if gotScore := bson.Raw(got).Lookup("score").Int32(); gotScore != int32(2) {
		t.Fatalf("detached overlay score=%d want 2", gotScore)
	}

	if results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		updateSet("rank", bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, 7)}),
	}); err != nil {
		t.Fatalf("second UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched || len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("second results=%+v batched=%v want one matched modified row", results, batched)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get merged detached/current overlay BSON document: %v", err)
	}
	if gotScore := bson.Raw(got).Lookup("score").Int32(); gotScore != int32(2) {
		t.Fatalf("merged buffered score=%d want 2", gotScore)
	}
	if gotRank := bson.Raw(got).Lookup("rank").Int32(); gotRank != int32(7) {
		t.Fatalf("merged buffered rank=%d want 7", gotRank)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush detached overlay update: %v", err)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get flushed detached overlay BSON document: %v", err)
	}
	if gotScore := bson.Raw(got).Lookup("score").Int32(); gotScore != int32(2) {
		t.Fatalf("flushed score=%d want 2", gotScore)
	}
	if gotRank := bson.Raw(got).Lookup("rank").Int32(); gotRank != int32(7) {
		t.Fatalf("flushed rank=%d want 7", gotRank)
	}
}

func TestCollectionUpdateBSONSetDirectBufferedUnaffectedIndexesSkipStateExtractionAndRetainKeys(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	mgr.SetUpdateBatchDetailedStatsEnabled(true)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	ids := [][]byte{[]byte("u1"), []byte("u2"), []byte("u3"), []byte("u4")}
	docs := make([][]byte, len(ids))
	for i, id := range ids {
		docs[i] = mustBSONCollectionDocument(t, bson.D{
			{Key: "_id", Value: string(id)},
			{Key: "email", Value: string(id) + "@example.com"},
			{Key: "city", Value: "hnl"},
			{Key: "score", Value: int32(i)},
		})
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}

	updateScore := func(docID string, score int32) BSONSetUpdateBatchItem {
		return BSONSetUpdateBatchItem{
			DocumentID: []byte(docID),
			Fields: []BSONSetField{{
				Key:   "score",
				Value: bson.RawValue{Type: bson.TypeInt32, Value: bsoncore.AppendInt32(nil, score)},
			}},
		}
	}
	if results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		updateScore("u1", 11),
		updateScore("u2", 12),
	}); err != nil {
		t.Fatalf("first UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched || len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
		t.Fatalf("first results=%+v batched=%v want two matched modified rows", results, batched)
	}
	if results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		updateScore("u3", 13),
		updateScore("u4", 14),
	}); err != nil {
		t.Fatalf("second UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	} else if !batched || len(results) != 2 || !results[0].Matched || !results[0].Modified || !results[1].Matched || !results[1].Modified {
		t.Fatalf("second results=%+v batched=%v want two matched modified rows", results, batched)
	}
	stats := col.LastUpdateStats()
	if got, want := stats.IndexStateExtraction, time.Duration(0); got != want {
		t.Fatalf("index state extraction=%s want %s for BSON $set fields disjoint from indexes", got, want)
	}
	if got, want := stats.IndexValueUnchanged, 4; got != want {
		t.Fatalf("index unchanged=%d want %d", got, want)
	}
	if got, want := stats.UniqueIndexCheckSkips, 2; got != want {
		t.Fatalf("unique skips=%d want %d", got, want)
	}
	for _, tc := range []struct {
		id    string
		score int32
	}{
		{id: "u1", score: 11},
		{id: "u2", score: 12},
		{id: "u3", score: 13},
		{id: "u4", score: 14},
	} {
		got, err := col.Get([]byte(tc.id))
		if err != nil {
			t.Fatalf("get %s after buffered BSON updates: %v", tc.id, err)
		}
		if gotScore := bson.Raw(got).Lookup("score").Int32(); gotScore != tc.score {
			t.Fatalf("%s score=%d want %d", tc.id, gotScore, tc.score)
		}
	}
}

func TestCollectionUpdateBatchDirectBufferedBSONRejectsIDMutationBeforeStaging(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatBSON,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	doc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "hnl"},
		{Key: "score", Value: int32(1)},
	})
	if _, err := col.InsertBatch([][]byte{[]byte("u1")}, [][]byte{doc}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	beforeState := d.State()
	beforeRoot := collectionPrimaryRootIDForTest(t, d, "users")
	replacement := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u2"},
		{Key: "city", Value: "sea"},
		{Key: "score", Value: int32(2)},
	})

	_, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{{
		DocumentID: []byte("u1"),
		Update: func([]byte) ([]byte, bool, error) {
			return replacement, true, nil
		},
	}})
	if !errors.Is(err, errBSONIDMutation) {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges err=%v want _id mutation error", err)
	}
	if !batched {
		t.Fatal("BSON _id mutation did not exercise the direct-buffered batch path")
	}
	afterState := d.State()
	if afterState.CommitSeq != beforeState.CommitSeq {
		t.Fatalf("rejected direct-buffered _id update advanced commit seq by %d", afterState.CommitSeq-beforeState.CommitSeq)
	}
	afterRoot := collectionPrimaryRootIDForTest(t, d, "users")
	if afterRoot != beforeRoot {
		t.Fatalf("primary root changed from %d to %d after rejected direct-buffered _id update", beforeRoot, afterRoot)
	}
	rootCounts, rootRunCount := bufferedRootRunCountsForTest(t, col,
		collectionPrimaryRootName("users"),
		collectionSecondaryRootName("users", "city"),
	)
	if rootRunCount != 0 || rootCounts[collectionPrimaryRootName("users")] != 0 || rootCounts[collectionSecondaryRootName("users", "city")] != 0 {
		t.Fatalf("buffered root runs after rejected direct-buffered _id update: count=%d roots=%v want none", rootRunCount, rootCounts)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get u1: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("u1 after rejected direct-buffered _id update=%x want original %x", got, doc)
	}
	got, err = col.Get([]byte("u2"))
	if err != nil {
		t.Fatalf("get u2: %v", err)
	}
	if got != nil {
		t.Fatalf("u2 after rejected direct-buffered _id update=%x want nil", got)
	}
}

func TestCollectionUpdateBatchDirectBufferedTemplateV1SeparatesTemplateAndSecondaryRoots(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat:        DocumentFormatTemplateV1,
			BufferedIndexedWrites: true,
		},
		Indexes: []IndexDefinition{
			{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
			{Name: "city", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("u1")},
		[][]byte{mustTemplateV1Document(t, []string{"email", "city"}, []any{"a@example.com", "hnl"})},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush insert buffer: %v", err)
	}
	before := d.State()

	results, batched, err := col.UpdateBatchIfNoSecondaryUniqueIndexChanges([]UpdateBatchItem{
		{DocumentID: []byte("u1"), Update: setTemplateV1JSON(t, `{"email":"a@example.com","city":"hnl","score":2}`)},
	})
	if err != nil {
		t.Fatalf("UpdateBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched {
		t.Fatal("template-v1 non-indexed update was declined")
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("results=%+v want one matched modified row", results)
	}
	after := d.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("direct-buffered template-v1 update advanced commit seq by %d, want 0", after.CommitSeq-before.CommitSeq)
	}

	rootCounts, rootRunCount := bufferedRootRunCountsForTest(t, col,
		collectionPrimaryRootName("users"),
		collectionTemplateRootName("users"),
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	)
	if got, want := rootRunCount, 2; got != want {
		t.Fatalf("rootRunCount=%d want %d primary+template buffered update", got, want)
	}
	for _, rootName := range []string{
		collectionPrimaryRootName("users"),
		collectionTemplateRootName("users"),
	} {
		if got, want := rootCounts[rootName], 1; got != want {
			t.Fatalf("root %q runs=%d want %d", rootName, got, want)
		}
	}
	for _, rootName := range []string{
		collectionSecondaryRootName("users", "email"),
		collectionSecondaryRootName("users", "city"),
	} {
		if got := rootCounts[rootName]; got != 0 {
			t.Fatalf("secondary root %q runs=%d want 0 for unchanged indexed values", rootName, got)
		}
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get buffered template-v1 document: %v", err)
	}
	gotJSON, err := col.StoredDocumentJSON(got)
	if err != nil {
		t.Fatalf("materialize buffered template-v1 document: %v", err)
	}
	for _, want := range [][]byte{[]byte(`"email":"a@example.com"`), []byte(`"city":"hnl"`), []byte(`"score":2`)} {
		if !bytes.Contains(gotJSON, want) {
			t.Fatalf("buffered document=%s missing %s", gotJSON, want)
		}
	}
}

func bufferedRootRunCountsForTest(t *testing.T, col *Collection, rootNames ...string) (map[string]int, int) {
	t.Helper()
	col.writeDomain.mu.RLock()
	defer col.writeDomain.mu.RUnlock()
	out := make(map[string]int, len(rootNames))
	for _, rootName := range rootNames {
		out[rootName] = len(col.writeDomain.rootRuns[rootName])
	}
	return out, col.writeDomain.rootRunCount
}

func bufferedPrimaryOverlayCountForTest(t *testing.T, col *Collection) int {
	t.Helper()
	col.writeDomain.mu.RLock()
	defer col.writeDomain.mu.RUnlock()
	if col.writeDomain.primaryOverlay == nil {
		return 0
	}
	return col.writeDomain.primaryOverlay.len()
}

func bufferedPrimaryCacheCountForTest(t *testing.T, col *Collection) int {
	t.Helper()
	col.writeDomain.mu.RLock()
	defer col.writeDomain.mu.RUnlock()
	if col.writeDomain.primaryCache == nil {
		return 0
	}
	return col.writeDomain.primaryCache.len()
}
