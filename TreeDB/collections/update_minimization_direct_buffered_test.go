package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionUpdateBatchDirectBufferedBSONSkipsUnchangedSecondaryRoots(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
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
	if got, want := rootRunCount, 1; got != want {
		t.Fatalf("rootRunCount=%d want %d primary-only buffered update", got, want)
	}
	if got, want := rootCounts[collectionPrimaryRootName("users")], 1; got != want {
		t.Fatalf("primary runs=%d want %d", got, want)
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
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed})
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
