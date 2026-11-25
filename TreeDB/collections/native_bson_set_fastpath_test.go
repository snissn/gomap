package collections

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func bsonSetFastPathComplexMeta(name string) *CollectionMeta {
	return &CollectionMeta{
		Name: name,
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
		Indexes: []IndexDefinition{
			{Name: "idx_email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true},
			{Name: "idx_age", Field: "age", ValueType: IndexValueBSONOrderedV2},
			{Name: "idx_profile_city", Field: "profile.city", ValueType: IndexValueBSONOrderedV2},
			{
				Name:      "idx_city_age",
				ValueType: IndexValueBSONOrderedV2,
				Components: []IndexComponent{
					{Field: "city", Direction: IndexDirectionAscending},
					{Field: "age", Direction: IndexDirectionAscending},
				},
			},
		},
	}
}

func bsonSetFastPathComplexDoc(tb testing.TB, id string, email string, age int32, city, profileCity string, score int32) []byte {
	tb.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "_id", Value: id},
		{Key: "email", Value: email},
		{Key: "age", Value: age},
		{Key: "city", Value: city},
		{Key: "profile", Value: bson.D{{Key: "city", Value: profileCity}, {Key: "zip", Value: "96813"}}},
		{Key: "score", Value: score},
	})
	if err != nil {
		tb.Fatalf("marshal complex bson doc: %v", err)
	}
	return doc
}

func bsonSetDocField(tb testing.TB, key string, val bson.D) BSONSetField {
	tb.Helper()
	typ, raw, err := bson.MarshalValue(val)
	if err != nil {
		tb.Fatalf("marshal doc value: %v", err)
	}
	return BSONSetField{Key: key, Value: bson.RawValue{Type: typ, Value: raw}}
}

func openFastPathStandaloneCOWDB(tb testing.TB, dir string) (*treedb.DB, *backenddb.DB, *CollectionManager) {
	tb.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(opts)
	if err != nil {
		tb.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		_ = owner.Close()
		tb.Fatal(err)
	}
	return owner, backend, NewCollectionManager(backend)
}

func captureNativeEngineAndCollection(tb testing.TB, backend *backenddb.DB, name string) (backenddb.NativeSemanticCapture, *nativeEngineVersion, *nativeCollectionState) {
	tb.Helper()
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		tb.Fatal(err)
	}
	engine, ok := capture.State().(*nativeEngineVersion)
	if !ok || engine == nil {
		_ = capture.Close()
		tb.Fatalf("expected *nativeEngineVersion, got %T", capture.State())
	}
	colState, ok := engine.names.Get(name)
	if !ok || colState == nil {
		_ = capture.Close()
		tb.Fatalf("missing collection %q in native engine", name)
	}
	return capture, engine, colState
}

func bsonRawValueForTest(tb testing.TB, v any) bson.RawValue {
	tb.Helper()
	typ, raw, err := bson.MarshalValue(v)
	if err != nil {
		tb.Fatalf("marshal bson value %v: %v", v, err)
	}
	return bson.RawValue{Type: typ, Value: raw}
}

func TestNativeBSONSetFastPathCorrectnessAndIndexes(t *testing.T) {
	dir := t.TempDir()
	owner, backend, manager := openFastPathStandaloneCOWDB(t, dir)
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		}
	})

	if _, err := manager.CreateCollection(bsonSetFastPathComplexMeta("users")); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}

	id0, id1 := []byte("u0"), []byte("u1")
	doc0 := bsonSetFastPathComplexDoc(t, "u0", "a@example.com", 30, "hnl", "kailua", 10)
	doc1 := bsonSetFastPathComplexDoc(t, "u1", "b@example.com", 40, "sea", "bellevue", 20)
	if _, err := col.InsertBatchValidatedBSON([][]byte{id0, id1}, [][]byte{doc0, doc1}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 1. Update non-indexed field "score" -> primary-only fast path.
	if matched, modified, err := col.UpdateBSONSet(id0, []BSONSetField{bsonSetInt32Field(t, "score", 99)}); err != nil || !matched || !modified {
		t.Fatalf("non-indexed update failed: matched=%t modified=%t err=%v", matched, modified, err)
	}

	// Verify document updated and all secondary indexes remain intact.
	gotDoc, err := col.Get(id0)
	if err != nil {
		t.Fatal(err)
	}
	if got := bson.Raw(gotDoc).Lookup("score").Int32(); got != 99 {
		t.Fatalf("score=%d, want 99", got)
	}
	assertIndexLookup := func(c *Collection, indexName string, val any, wantID []byte) {
		t.Helper()
		ids, err := c.FindByIndexValue(indexName, bsonRawValueForTest(t, val))
		if err != nil {
			t.Fatalf("FindByIndexValue(%q, %v): %v", indexName, val, err)
		}
		if len(ids) != 1 || !bytes.Equal(ids[0], wantID) {
			t.Fatalf("FindByIndexValue(%q, %v) = %q, want [%q]", indexName, val, ids, wantID)
		}
	}
	assertIndexLookup(col, "idx_email", "a@example.com", id0)
	assertIndexLookup(col, "idx_age", int32(30), id0)
	assertIndexLookup(col, "idx_profile_city", "kailua", id0)

	// 2. Updating indexed field "email" updates secondary index and enforces uniqueness.
	if _, _, err := col.UpdateBSONSet(id0, []BSONSetField{bsonSetStringField(t, "email", "b@example.com")}); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("expected ErrUniqueIndexConflict for duplicate email, got %v", err)
	}
	if matched, modified, err := col.UpdateBSONSet(id0, []BSONSetField{bsonSetStringField(t, "email", "a2@example.com")}); err != nil || !matched || !modified {
		t.Fatalf("indexed email update failed: matched=%t modified=%t err=%v", matched, modified, err)
	}
	if ids, err := col.FindByIndexValue("idx_email", bsonRawValueForTest(t, "a@example.com")); err != nil || len(ids) != 0 {
		t.Fatalf("old email still indexed: ids=%q err=%v", ids, err)
	}
	assertIndexLookup(col, "idx_email", "a2@example.com", id0)

	// 3. Updating non-unique indexed field "age".
	if matched, modified, err := col.UpdateBSONSet(id0, []BSONSetField{bsonSetInt32Field(t, "age", 31)}); err != nil || !matched || !modified {
		t.Fatalf("indexed age update failed: matched=%t modified=%t err=%v", matched, modified, err)
	}
	if ids, err := col.FindByIndexValue("idx_age", bsonRawValueForTest(t, int32(30))); err != nil || len(ids) != 0 {
		t.Fatalf("old age still indexed: ids=%q err=%v", ids, err)
	}
	assertIndexLookup(col, "idx_age", int32(31), id0)

	// 4. Updating parent of nested indexed field "profile" updates "profile.city".
	if matched, modified, err := col.UpdateBSONSet(id0, []BSONSetField{
		bsonSetDocField(t, "profile", bson.D{{Key: "city", Value: "wahiawa"}, {Key: "zip", Value: "96786"}}),
	}); err != nil || !matched || !modified {
		t.Fatalf("nested parent update failed: matched=%t modified=%t err=%v", matched, modified, err)
	}
	if ids, err := col.FindByIndexValue("idx_profile_city", bsonRawValueForTest(t, "kailua")); err != nil || len(ids) != 0 {
		t.Fatalf("old profile.city still indexed: ids=%q err=%v", ids, err)
	}
	assertIndexLookup(col, "idx_profile_city", "wahiawa", id0)

	// 5. Updating compound component "city" updates compound index "idx_city_age".
	if matched, modified, err := col.UpdateBSONSet(id0, []BSONSetField{bsonSetStringField(t, "city", "maui")}); err != nil || !matched || !modified {
		t.Fatalf("compound component update failed: matched=%t modified=%t err=%v", matched, modified, err)
	}
	if ids, _, err := col.FindByCompoundIndexRange("idx_city_age", CompoundIndexRangeOptions{
		Prefix: []bson.RawValue{bsonRawValueForTest(t, "hnl")},
		Limit:  10,
	}); err != nil || len(ids) != 0 {
		t.Fatalf("old compound city still indexed: ids=%q err=%v", ids, err)
	}
	if ids, _, err := col.FindByCompoundIndexRange("idx_city_age", CompoundIndexRangeOptions{
		Prefix: []bson.RawValue{bsonRawValueForTest(t, "maui"), bsonRawValueForTest(t, int32(31))},
		Limit:  10,
	}); err != nil || len(ids) != 1 || !bytes.Equal(ids[0], id0) {
		t.Fatalf("new compound (maui, 31) lookup = %q err=%v, want [u0]", ids, err)
	}

	// 6. Another non-indexed score update + close + reopen to verify WAL durability.
	if matched, modified, err := col.UpdateBSONSet(id0, []BSONSetField{bsonSetInt32Field(t, "score", 777)}); err != nil || !matched || !modified {
		t.Fatalf("final non-indexed update failed: matched=%t modified=%t err=%v", matched, modified, err)
	}

	// Capture budget before close so we can confirm clean shutdown.
	capBeforeClose, _, _ := captureNativeEngineAndCollection(t, backend, "users")
	budget := capBeforeClose.NativeAllocator().(*memtable.COWBudget)
	if err := capBeforeClose.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
		t.Fatalf("budget leaked after Close: %+v", s)
	}

	// Reopen and verify all durable state (primary + secondary indexes).
	owner2, _, manager2 := openFastPathStandaloneCOWDB(t, dir)
	defer func() {
		if err := owner2.Close(); err != nil {
			t.Error(err)
		}
	}()
	col2, err := manager2.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := col2.Get(id0)
	if err != nil {
		t.Fatal(err)
	}
	if got := bson.Raw(reopened).Lookup("score").Int32(); got != 777 {
		t.Fatalf("reopened score=%d, want 777", got)
	}
	assertIndexLookup(col2, "idx_email", "a2@example.com", id0)
	assertIndexLookup(col2, "idx_profile_city", "wahiawa", id0)
	if ids, _, err := col2.FindByCompoundIndexRange("idx_city_age", CompoundIndexRangeOptions{
		Prefix: []bson.RawValue{bsonRawValueForTest(t, "maui"), bsonRawValueForTest(t, int32(31))},
		Limit:  10,
	}); err != nil || len(ids) != 1 || !bytes.Equal(ids[0], id0) {
		t.Fatalf("reopened idx_city_age lookup = %q err=%v", ids, err)
	}
}

func TestNativeBSONSetFastPathPredicateAndExclusions(t *testing.T) {
	baseMeta := *bsonSetFastPathComplexMeta("users")
	typedStorageMeta := typedMinimaCollectionMeta()
	typedStorageMeta.Options.DocumentFormat = DocumentFormatBSON
	typedStorageMeta.Indexes = nil
	typedStorageMeta.TextIndexes = nil
	typedStorageMeta.VectorIndexes = nil
	cases := []struct {
		name  string
		meta  CollectionMeta
		items []BSONSetUpdateBatchItem
		want  bool
	}{
		{
			name:  "non_indexed_field_score",
			meta:  baseMeta,
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "score"}}}},
			want:  false,
		},
		{
			name:  "unique_indexed_field_email",
			meta:  baseMeta,
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "email"}}}},
			want:  true,
		},
		{
			name:  "non_unique_indexed_field_age",
			meta:  baseMeta,
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "age"}}}},
			want:  true,
		},
		{
			name:  "parent_of_nested_index_profile",
			meta:  baseMeta,
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "profile"}}}},
			want:  true,
		},
		{
			name:  "child_of_indexed_field_email_sub",
			meta:  baseMeta,
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "email.sub"}}}},
			want:  true,
		},
		{
			name:  "compound_component_city",
			meta:  baseMeta,
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "city"}}}},
			want:  true,
		},
		{
			name:  "empty_field_key_conservative",
			meta:  baseMeta,
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: ""}}}},
			want:  true,
		},
		{
			name: "text_index_excluded",
			meta: CollectionMeta{
				Name:        "docs",
				Options:     CollectionOptions{DocumentFormat: DocumentFormatBSON},
				TextIndexes: []TextIndexDefinition{{Name: "txt"}},
			},
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "score"}}}},
			want:  true,
		},
		{
			name: "vector_index_excluded",
			meta: CollectionMeta{
				Name:          "docs",
				Options:       CollectionOptions{DocumentFormat: DocumentFormatBSON},
				VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "emb"}},
			},
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "score"}}}},
			want:  true,
		},
		{
			name: "json_format_excluded",
			meta: CollectionMeta{
				Name:    "docs",
				Options: CollectionOptions{DocumentFormat: DocumentFormatJSON},
			},
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "score"}}}},
			want:  true,
		},
		{
			name: "template_v1_format_excluded",
			meta: CollectionMeta{
				Name:    "docs",
				Options: CollectionOptions{DocumentFormat: DocumentFormatTemplateV1},
			},
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "score"}}}},
			want:  true,
		},
		{
			name:  "typed_storage_excluded",
			meta:  typedStorageMeta,
			items: []BSONSetUpdateBatchItem{{DocumentID: []byte("u0"), Fields: []BSONSetField{{Key: "score"}}}},
			want:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := bsonSetFieldsMayTouchAnyIndex(tc.meta, tc.items); got != tc.want {
				t.Fatalf("bsonSetFieldsMayTouchAnyIndex() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestNativeBSONSetFastPathExcludedBySubsequentTxnMutation(t *testing.T) {
	owner, _, manager := openFastPathStandaloneCOWDB(t, t.TempDir())
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := manager.CreateCollection(bsonSetCustodyTestMeta("users", 1)); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	id0, id1 := []byte("u0"), []byte("u1")
	if _, err := col.InsertBatchValidatedBSON([][]byte{id0}, [][]byte{
		bsonSetCustodyTestDoc(t, "u0", 30, 1, "a@example.com", "hnl"),
	}); err != nil {
		t.Fatal(err)
	}

	// Stage a verified primary-only BSON $set, then insert another doc in the same txn:
	// primaryOnlyBSONSet MUST be invalidated so the inserted doc's secondary index is built!
	tx, err := col.nativeJSON.Begin("users")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Close() }()
	bsonItems := []BSONSetUpdateBatchItem{{DocumentID: id0, Fields: []BSONSetField{bsonSetInt32Field(t, "score", 42)}}}
	updates, err := tx.prepareNativeBSONSetItems("users", bsonItems)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.updateBatchInto("users", updates, true); err != nil {
		t.Fatal(err)
	}
	if !tx.binding("users").primaryOnlyBSONSet {
		t.Fatal("expected primaryOnlyBSONSet == true after verified non-indexed BSON $set")
	}
	if err := tx.InsertBatch([][]byte{id1}, [][]byte{bsonSetCustodyTestDoc(t, "u1", 31, 2, "b@example.com", "sea")}); err != nil {
		t.Fatal(err)
	}
	if tx.binding("users").primaryOnlyBSONSet {
		t.Fatal("expected primaryOnlyBSONSet == false after subsequent InsertBatch")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ids, err := col.FindByIndexValue("idx_email", bsonRawValueForTest(t, "b@example.com"))
	if err != nil || len(ids) != 1 || !bytes.Equal(ids[0], id1) {
		t.Fatalf("inserted doc missing from idx_email: ids=%q err=%v", ids, err)
	}
}

func TestNativeBSONSetFastPathAllocsLowerThanIndexedPath(t *testing.T) {
	owner, _, manager := openFastPathStandaloneCOWDB(t, t.TempDir())
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := manager.CreateCollection(bsonSetFastPathComplexMeta("users")); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	id := []byte("u0")
	if _, err := col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{
		bsonSetFastPathComplexDoc(t, "u0", "a@example.com", 30, "hnl", "kailua", 0),
	}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Warm up both paths.
	for i := int32(1); i <= 5; i++ {
		if _, _, err := col.UpdateBSONSet(id, []BSONSetField{bsonSetInt32Field(t, "score", i)}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := col.UpdateBSONSet(id, []BSONSetField{bsonSetInt32Field(t, "age", 30+i)}); err != nil {
			t.Fatal(err)
		}
	}

	var scoreCounter int32 = 100
	fastAllocs := testing.AllocsPerRun(50, func() {
		scoreCounter++
		if _, _, err := col.UpdateBSONSet(id, []BSONSetField{bsonSetInt32Field(t, "score", scoreCounter)}); err != nil {
			t.Fatal(err)
		}
	})
	var ageCounter int32 = 100
	indexedAllocs := testing.AllocsPerRun(50, func() {
		ageCounter++
		if _, _, err := col.UpdateBSONSet(id, []BSONSetField{bsonSetInt32Field(t, "age", ageCounter)}); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("fastAllocs=%.1f indexedAllocs=%.1f", fastAllocs, indexedAllocs)
	if fastAllocs >= indexedAllocs*0.75 {
		t.Fatalf("non-indexed fast path allocs (%.1f) not dramatically lower than indexed path (%.1f)", fastAllocs, indexedAllocs)
	}
}

func TestNativeBSONSetFastPathSharedOwnedMapsAndPostHandoff(t *testing.T) {
	t.Run("single_domain_post_handoff_shares_both", func(t *testing.T) {
		owner, backend, manager := openFastPathStandaloneCOWDB(t, t.TempDir())
		closed := false
		t.Cleanup(func() {
			if !closed {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}
		})
		if _, err := manager.CreateCollection(&CollectionMeta{
			Name:    "docs",
			Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
		}); err != nil {
			t.Fatal(err)
		}
		col, err := manager.OpenCollection("docs")
		if err != nil {
			t.Fatal(err)
		}
		id := []byte("d0")
		if _, err := col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{
			bsonSetCustodyTestDoc(t, "d0", 20, 1, "a@example.com", "hnl"),
		}); err != nil {
			t.Fatal(err)
		}
		// Pre-checkpoint: collections/pending and domains/pending should be shared.
		cap1, eng1, col1 := captureNativeEngineAndCollection(t, backend, "docs")
		budget := cap1.NativeAllocator().(*memtable.COWBudget)
		if eng1.pending != eng1.collections {
			t.Errorf("pre-checkpoint: expected eng.pending == eng.collections")
		}
		if col1.pending != col1.domains {
			t.Errorf("pre-checkpoint: expected col.pending == col.domains")
		}
		if err := cap1.Close(); err != nil {
			t.Fatal(err)
		}

		// Checkpoint clears pending -> pending != collections and col.pending != col.domains.
		if err := owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		cap2, eng2, col2 := captureNativeEngineAndCollection(t, backend, "docs")
		if eng2.pending.Len() != 0 || col2.pending.Len() != 0 {
			t.Fatalf("post-checkpoint expected empty pending maps, got eng.pending=%d col.pending=%d", eng2.pending.Len(), col2.pending.Len())
		}
		if err := cap2.Close(); err != nil {
			t.Fatal(err)
		}

		// First post-handoff update MUST trigger Rule 2 sharing immediately!
		if matched, modified, err := col.UpdateBSONSet(id, []BSONSetField{bsonSetInt32Field(t, "score", 2)}); err != nil || !matched || !modified {
			t.Fatalf("post-handoff update 1 failed: matched=%t modified=%t err=%v", matched, modified, err)
		}
		cap3, eng3, col3 := captureNativeEngineAndCollection(t, backend, "docs")
		if eng3.pending != eng3.collections {
			t.Errorf("post-handoff update 1: expected eng.pending == eng.collections")
		}
		if col3.pending != col3.domains {
			t.Errorf("post-handoff update 1: expected col.pending == col.domains")
		}
		if err := cap3.Close(); err != nil {
			t.Fatal(err)
		}

		// Second post-handoff update keeps pointer-equality sharing.
		if matched, modified, err := col.UpdateBSONSet(id, []BSONSetField{bsonSetInt32Field(t, "score", 3)}); err != nil || !matched || !modified {
			t.Fatalf("post-handoff update 2 failed: matched=%t modified=%t err=%v", matched, modified, err)
		}
		cap4, eng4, col4 := captureNativeEngineAndCollection(t, backend, "docs")
		if eng4.pending != eng4.collections || col4.pending != col4.domains {
			t.Errorf("post-handoff update 2 lost sharing")
		}
		if err := cap4.Close(); err != nil {
			t.Fatal(err)
		}

		// Checkpoint again to prove shared maps handoff cleanly with zero stale pending entries.
		if err := owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
		closed = true
		if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
			t.Fatalf("budget leaked after Close: %+v", s)
		}
	})

	t.Run("multi_domain_post_handoff_keeps_pending_domains_minimal", func(t *testing.T) {
		owner, backend, manager := openFastPathStandaloneCOWDB(t, t.TempDir())
		closed := false
		t.Cleanup(func() {
			if !closed {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}
		})
		if _, err := manager.CreateCollection(bsonSetCustodyTestMeta("users", 2)); err != nil {
			t.Fatal(err)
		}
		col, err := manager.OpenCollection("users")
		if err != nil {
			t.Fatal(err)
		}
		id := []byte("u0")
		if _, err := col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{
			bsonSetCustodyTestDoc(t, "u0", 25, 1, "u0@example.com", "hnl"),
		}); err != nil {
			t.Fatal(err)
		}
		if err := owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}

		// Post-checkpoint: primary-only update shares eng.pending==eng.collections,
		// while col.pending MUST hold only "primary" (Len==1 != col.domains.Len()==4).
		if matched, modified, err := col.UpdateBSONSet(id, []BSONSetField{bsonSetInt32Field(t, "score", 42)}); err != nil || !matched || !modified {
			t.Fatalf("update failed: matched=%t modified=%t err=%v", matched, modified, err)
		}
		cap1, eng1, col1 := captureNativeEngineAndCollection(t, backend, "users")
		budget := cap1.NativeAllocator().(*memtable.COWBudget)
		if eng1.pending != eng1.collections {
			t.Errorf("multi-domain post-handoff: expected eng.pending == eng.collections")
		}
		if col1.pending == col1.domains || col1.pending.Len() != 1 || col1.domains.Len() <= 1 {
			t.Errorf("multi-domain post-handoff: expected col.pending (len=%d) != col.domains (len=%d)", col1.pending.Len(), col1.domains.Len())
		}
		if _, ok := col1.pending.Get("primary"); !ok {
			t.Errorf("multi-domain post-handoff: missing primary in col.pending")
		}
		if err := cap1.Close(); err != nil {
			t.Fatal(err)
		}
		if err := owner.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
		closed = true
		if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
			t.Fatalf("budget leaked after Close: %+v", s)
		}
	})
}

func TestNativeBSONSetFastPathCOWCapacityEnforcementNoLeak(t *testing.T) {
	owner, backend, manager := openFastPathStandaloneCOWDB(t, t.TempDir())
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := manager.CreateCollection(bsonSetCustodyTestMeta("users", 2)); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	id := []byte("u0")
	if _, err := col.InsertBatchValidatedBSON([][]byte{id}, [][]byte{
		bsonSetCustodyTestDoc(t, "u0", 25, 1, "u0@example.com", "hnl"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Exhaust COW budget at multiple headroom steps (0 B up to 16 KiB) to exercise
	// ErrCOWCapacity failures across staging, finalization, and prepareNativeCollectionChanges.
	cap0, _, _ := captureNativeEngineAndCollection(t, backend, "users")
	budget := cap0.NativeAllocator().(*memtable.COWBudget)
	if err := cap0.Close(); err != nil {
		t.Fatal(err)
	}

	sawCapacityErr := false
	for _, headroom := range []uint64{0, 64, 512, 2048, 4096, 8192, 16384} {
		blocker, err := budget.AcquireExternal(0)
		if err != nil {
			t.Fatal(err)
		}
		avail := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
		if avail > headroom {
			if err := blocker.Resize(avail - headroom); err != nil {
				blocker.Close()
				t.Fatal(err)
			}
		}
		_, _, updErr := col.UpdateBSONSet(id, []BSONSetField{bsonSetInt32Field(t, "score", int32(100+headroom))})
		blocker.Close()
		if errors.Is(updErr, memtable.ErrCOWCapacity) {
			sawCapacityErr = true
		} else if updErr != nil {
			t.Fatalf("headroom=%d unexpected error: %v", headroom, updErr)
		}
	}
	if !sawCapacityErr {
		t.Fatal("expected at least one ErrCOWCapacity under exhausted budget")
	}

	// Verify updates still succeed cleanly after capacity is released.
	if matched, modified, err := col.UpdateBSONSet(id, []BSONSetField{bsonSetInt32Field(t, "score", 999)}); err != nil || !matched || !modified {
		t.Fatalf("post-capacity update failed: matched=%t modified=%t err=%v", matched, modified, err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
		t.Fatalf("COW budget leaked after ErrCOWCapacity + Close: %+v", s)
	}
}

func TestNativeBSONSetFastPathMixedExistingAndMissingBatch(t *testing.T) {
	owner, _, manager := openFastPathStandaloneCOWDB(t, t.TempDir())
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := manager.CreateCollection(bsonSetCustodyTestMeta("users", 2)); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	id0 := []byte("u0")
	if _, err := col.InsertBatchValidatedBSON([][]byte{id0}, [][]byte{
		bsonSetCustodyTestDoc(t, "u0", 25, 1, "u0@example.com", "hnl"),
	}); err != nil {
		t.Fatal(err)
	}
	results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{
		{DocumentID: id0, Fields: []BSONSetField{bsonSetInt32Field(t, "score", 55)}},
		{DocumentID: []byte("missing"), Fields: []BSONSetField{bsonSetInt32Field(t, "score", 66)}},
	})
	if err != nil || !batched || len(results) != 2 {
		t.Fatalf("mixed batch failed: batched=%t results=%+v err=%v", batched, results, err)
	}
	if !results[0].Matched || !results[0].Modified || results[1].Matched || results[1].Modified {
		t.Fatalf("unexpected mixed batch results: %+v", results)
	}
	doc, err := col.Get(id0)
	if err != nil {
		t.Fatal(err)
	}
	if got := bson.Raw(doc).Lookup("score").Int32(); got != 55 {
		t.Fatalf("score=%d, want 55", got)
	}
}

func BenchmarkUpdateBSONSetNonIndexedStandaloneCOW(b *testing.B) {
	owner, _, manager := openFastPathStandaloneCOWDB(b, b.TempDir())
	b.Cleanup(func() {
		if err := owner.Close(); err != nil {
			b.Error(err)
		}
	})
	meta := &CollectionMeta{
		Name:    "users",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
		Indexes: []IndexDefinition{
			{Name: "idx_email", Field: "email", ValueType: IndexValueBSONOrderedV2, Unique: true},
			{Name: "idx_age", Field: "age", ValueType: IndexValueBSONOrderedV2},
		},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		b.Fatal(err)
	}
	col, err := manager.OpenCollection("users")
	if err != nil {
		b.Fatal(err)
	}
	const seedCount = 256
	ids := make([][]byte, seedCount)
	docs := make([][]byte, seedCount)
	for i := 0; i < seedCount; i++ {
		id := fmt.Sprintf("user-%04d", i)
		ids[i] = []byte(id)
		docs[i] = bsonSetCustodyTestDoc(b, id, int32(20+i%50), 0, fmt.Sprintf("u%d@example.com", i), "hnl")
	}
	if _, err := col.InsertBatchValidatedBSON(ids, docs); err != nil {
		b.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		b.Fatal(err)
	}

	var rawInt32 [4]byte
	fields := []BSONSetField{{Key: "score", Value: bson.RawValue{Type: bson.TypeInt32, Value: rawInt32[:]}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := uint32(i + 1)
		rawInt32[0] = byte(v)
		rawInt32[1] = byte(v >> 8)
		rawInt32[2] = byte(v >> 16)
		rawInt32[3] = byte(v >> 24)
		matched, modified, err := col.UpdateBSONSet(ids[i%seedCount], fields)
		if err != nil || !matched || !modified {
			b.Fatalf("iter %d: matched=%t modified=%t err=%v", i, matched, modified, err)
		}
	}
}
