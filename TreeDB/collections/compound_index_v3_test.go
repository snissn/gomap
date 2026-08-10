package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCompoundBSONIndexMetadataAndMutationContract(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	meta := &CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{
		Name:       "tenant_created_id",
		Components: []IndexComponent{{Field: "tenant", Direction: IndexDirectionAscending}, {Field: "createdAt", Direction: IndexDirectionDescending}, {Field: "_id", Direction: IndexDirectionAscending}},
		ValueType:  IndexValueBSONOrderedV2,
		Unique:     true,
	}}}
	if _, err := mgr.CreateCollection(meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	got := col.Meta().Indexes[0]
	if len(got.Components) != 3 || got.Components[1].Field != "createdAt" || got.Components[1].Direction != IndexDirectionDescending {
		t.Fatalf("components=%+v want persisted ordered compound definition", got.Components)
	}
	doc := func(id, tenant string, created int32) []byte {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "tenant", Value: tenant}, {Key: "createdAt", Value: created}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{doc("a", "acme", 3)}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("b")}, [][]byte{doc("b", "acme", 3)}); err != nil {
		t.Fatalf("distinct compound key should insert: %v", err)
	}
	if _, err := col.FindByIndexValue("tenant_created_id", "acme"); err == nil {
		t.Fatal("legacy single-field lookup silently accepted compound index")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("c")}, [][]byte{doc("a", "acme", 3)}); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate compound key error=%v want unique conflict", err)
	}
}

func TestCompoundBSONIndexRejectsMultiKeyDefinitionBeforeCatalogMutation(t *testing.T) {
	for _, def := range []IndexDefinition{
		{Name: "tenant_created", Components: []IndexComponent{{Field: "tenant", Direction: IndexDirectionAscending}, {Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2, MultiKey: true},
		{Name: "created_desc", Components: []IndexComponent{{Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2, MultiKey: true},
	} {
		t.Run(def.Name, func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			mgr := NewCollectionManager(db)
			_, err = mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{def}})
			if err == nil || !strings.Contains(err.Error(), "do not support multikey") {
				t.Fatalf("CreateCollection ordered BSON v2 multikey err=%v want rejection", err)
			}
			if _, err := mgr.OpenCollection("events"); err == nil {
				t.Fatal("ordered BSON v2 multikey rejection published collection metadata")
			}
		})
	}
}

func TestCompoundBSONIndexComponentsRejectArraysBeforeMutation(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{
		Name: "tenant_created", Components: []IndexComponent{{Field: "tenant", Direction: 1}, {Field: "createdAt", Direction: -1}}, ValueType: IndexValueBSONOrderedV2,
	}}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	bad, err := bson.Marshal(bson.D{{Key: "_id", Value: "bad"}, {Key: "tenant", Value: bson.A{"acme"}}, {Key: "createdAt", Value: int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("bad")}, [][]byte{bad}); err == nil {
		t.Fatal("array component insert succeeded")
	}
	if got, err := col.Get([]byte("bad")); err != nil || got != nil {
		t.Fatalf("array component insert mutated primary collection: document=%q err=%v", got, err)
	}
	intermediateArray, err := bson.Marshal(bson.D{{Key: "_id", Value: "intermediate-array"}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(1)}, {Key: "profile", Value: bson.A{bson.D{{Key: "other", Value: "value"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "tenant_profile", Components: []IndexComponent{{Field: "tenant", Direction: 1}, {Field: "profile.name", Direction: 1}}, ValueType: IndexValueBSONOrderedV2}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("intermediate-array"), intermediateArray); err == nil {
		t.Fatal("nonmatching intermediate array silently encoded a missing compound component")
	}
	if got, err := col.Get([]byte("intermediate-array")); err != nil || got != nil {
		t.Fatalf("intermediate array insert mutated primary collection: document=%q err=%v", got, err)
	}
	oversized, err := bson.Marshal(bson.D{{Key: "_id", Value: "oversized"}, {Key: "tenant", Value: string(bytes.Repeat([]byte{'x'}, (1<<20)+1))}, {Key: "createdAt", Value: int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Insert([]byte("oversized"), oversized); err == nil {
		t.Fatal("oversized compound component insert succeeded")
	}
	if got, err := col.Get([]byte("oversized")); err != nil || got != nil {
		t.Fatalf("oversized component insert mutated primary collection: document=%q err=%v", got, err)
	}
}

func TestCompoundBSONIndexExtractsDottedMissingNullAndNumericComponents(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{
		Name: "kind_tenant", Components: []IndexComponent{{Field: "kind", Direction: IndexDirectionAscending}, {Field: "profile.tenant", Direction: IndexDirectionAscending}}, ValueType: IndexValueBSONOrderedV2,
	}}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	doc := func(id string, tenant any, present bool) []byte {
		fields := bson.D{{Key: "_id", Value: id}, {Key: "kind", Value: "event"}}
		if present {
			fields = append(fields, bson.E{Key: "profile", Value: bson.D{{Key: "tenant", Value: tenant}}})
		}
		raw, marshalErr := bson.Marshal(fields)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return raw
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}, [][]byte{doc("a", nil, false), doc("b", nil, true), doc("c", int32(2), true), doc("d", int64(2), true)}); err != nil {
		t.Fatal(err)
	}
	ids, truncated, err := col.FindByCompoundIndexRange("kind_tenant", CompoundIndexRangeOptions{Prefix: []bson.RawValue{{Type: bson.TypeString, Value: bsoncoreAppendString("event")}}, Limit: 4})
	if err != nil || truncated || fmt.Sprint(ids) != "[[97] [98] [99] [100]]" {
		t.Fatalf("dotted component order ids=%q truncated=%v err=%v", ids, truncated, err)
	}
}

func TestCompoundBSONUniqueIndexMaintainsReplaceUpdateBatchAndDelete(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{
		Name: "tenant_created", Components: []IndexComponent{{Field: "tenant", Direction: IndexDirectionAscending}, {Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2, Unique: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	doc := func(id, tenant string, created int32, note string) []byte {
		raw, marshalErr := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "tenant", Value: tenant}, {Key: "createdAt", Value: created}, {Key: "note", Value: note}})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return raw
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{doc("a", "acme", 1, "seed"), doc("b", "beta", 1, "seed")}); err != nil {
		t.Fatal(err)
	}
	if matched, err := col.Replace([]byte("a"), doc("a", "acme", 2, "replace")); err != nil || !matched {
		t.Fatalf("Replace matched=%v err=%v", matched, err)
	}
	if matched, modified, err := col.Update([]byte("b"), func([]byte) ([]byte, bool, error) {
		return doc("b", "beta", 2, "update"), true, nil
	}); err != nil || !matched || !modified {
		t.Fatalf("Update matched=%v modified=%v err=%v", matched, modified, err)
	}
	batchDoc := doc("b", "beta", 2, "batch")
	results, err := col.UpdateBatch([]UpdateBatchItem{{DocumentID: []byte("b"), Update: func([]byte) ([]byte, bool, error) { return batchDoc, true, nil }}})
	if err != nil || len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("UpdateBatch results=%+v err=%v", results, err)
	}
	if matched, err := col.Replace([]byte("b"), doc("b", "acme", 2, "conflict")); !errors.Is(err, ErrUniqueIndexConflict) || matched {
		t.Fatalf("unique Replace matched=%v err=%v", matched, err)
	}
	if got, err := col.Get([]byte("b")); err != nil || !bytes.Equal(got, batchDoc) {
		t.Fatalf("unique conflict changed b document=%q err=%v", got, err)
	}
	if deleted, err := col.DeleteDocument([]byte("a")); err != nil || !deleted {
		t.Fatalf("delete a deleted=%v err=%v", deleted, err)
	}
	if _, err := col.Insert([]byte("c"), doc("c", "acme", 2, "reused")); err != nil {
		t.Fatalf("reusing deleted unique compound key: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("b"), []byte("c")}); err != nil || deleted != 2 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	ids, truncated, err := col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{{Type: bson.TypeString, Value: bsoncoreAppendString("acme")}}, Limit: 1})
	if err != nil || truncated || len(ids) != 0 {
		t.Fatalf("deleted compound rows ids=%q truncated=%v err=%v", ids, truncated, err)
	}
}

func TestBSONCompoundEntryV2PreservesComponentBoundariesAndTieID(t *testing.T) {
	a, err := encodeBSONIndexKeyComponentV2(bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString("acme")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := encodeBSONIndexKeyComponentV2(bson.RawValue{Type: bson.TypeInt32, Value: []byte{3, 0, 0, 0}})
	if err != nil {
		t.Fatal(err)
	}
	b, err = descendingBSONIndexKeyComponentV2(b)
	if err != nil {
		t.Fatal(err)
	}
	key, err := bsonIndexEntryKeyV2(append(a, b...), []byte("doc-7"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := bsonIndexKeyDocumentIDV2(key); err != nil || !bytes.Equal(got, []byte("doc-7")) {
		t.Fatalf("document ID=(%q,%v)", got, err)
	}
}

func TestSingleDescendingBSONIndexMaintainsUniqueState(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{
		Name: "created_desc", Components: []IndexComponent{{Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2, Unique: true,
	}}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	doc := func(id string) []byte {
		raw, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "createdAt", Value: int32(3)}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{doc("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("b")}, [][]byte{doc("b")}); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("descending duplicate error=%v want unique conflict", err)
	}
	value := bson.Raw(doc("query")).Lookup("createdAt")
	if _, _, err := col.FindByIndexRange("created_desc", IndexRangeOptions{Lower: IndexRangeBound{Value: value, Inclusive: true}, Upper: IndexRangeBound{Value: value, Inclusive: true}}); err == nil {
		t.Fatal("legacy range silently accepted descending BSON v2 index")
	}
}

func TestFindByCompoundIndexRangeHonorsDirectionsBoundsAndTieIDs(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{
		Name: "tenant_created", Components: []IndexComponent{{Field: "tenant", Direction: IndexDirectionAscending}, {Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2,
	}}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		id, tenant string
		created    int32
	}
	rows := []row{{"a", "acme", 1}, {"b", "acme", 3}, {"c", "acme", 2}, {"d", "other", 4}, {"e", "acme", 3}}
	ids := make([][]byte, len(rows))
	docs := make([][]byte, len(rows))
	for i, row := range rows {
		ids[i] = []byte(row.id)
		docs[i], err = bson.Marshal(bson.D{{Key: "_id", Value: row.id}, {Key: "tenant", Value: row.tenant}, {Key: "createdAt", Value: row.created}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	stringRaw := func(value string) bson.RawValue {
		return bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString(value)}
	}
	intRaw := func(value int32) bson.RawValue {
		return bson.RawValue{Type: bson.TypeInt32, Value: []byte{byte(value), byte(value >> 8), byte(value >> 16), byte(value >> 24)}}
	}
	assertIDs := func(got [][]byte, want ...string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("ids=%q want %q", got, want)
		}
		for i := range want {
			if string(got[i]) != want[i] {
				t.Fatalf("ids=%q want %q", got, want)
			}
		}
	}
	got, truncated, err := col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw("acme")}, Limit: 4})
	if err != nil || truncated {
		t.Fatalf("prefix scan err=%v truncated=%v", err, truncated)
	}
	assertIDs(got, "b", "e", "c", "a")
	got, truncated, err = col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw("acme")}, Desc: true, Limit: 3})
	if err != nil || !truncated {
		t.Fatalf("reverse scan err=%v truncated=%v", err, truncated)
	}
	assertIDs(got, "a", "c", "e")
	// Legacy direct descending scans retain physical reverse-ID order. Clients
	// that need gateway-compatible ordering opt into atomic ascending-ID ties.
	got, truncated, err = col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw("acme")}, Desc: true, StableDocumentIDTies: true, Limit: 4})
	if err != nil || truncated {
		t.Fatalf("stable reverse scan err=%v truncated=%v", err, truncated)
	}
	assertIDs(got, "a", "c", "b", "e")
	got, truncated, err = col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw("acme")}, Desc: true, StableDocumentIDTies: true, Limit: 3})
	if err != nil || !truncated {
		t.Fatalf("stable reverse limited scan err=%v truncated=%v", err, truncated)
	}
	// The two-ID tie at createdAt=3 cannot be partially emitted after a,c.
	assertIDs(got, "a", "c")
	got, truncated, err = col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw("acme")}, Lower: IndexRangeBound{Value: intRaw(2), Inclusive: true}, Upper: IndexRangeBound{Value: intRaw(3), Inclusive: true}, Limit: 3})
	if err != nil || truncated {
		t.Fatalf("range scan err=%v truncated=%v", err, truncated)
	}
	assertIDs(got, "b", "e", "c")
}

func TestCompoundBSONIndexUpdateBSONSetTracksNonLeadingComponent(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON, BufferedIndexedWrites: true}, Indexes: []IndexDefinition{{
		Name: "tenant_created", Components: []IndexComponent{{Field: "tenant", Direction: IndexDirectionAscending}, {Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2, Unique: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	doc := func(id string, created int32) []byte {
		raw, marshalErr := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: created}})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return raw
	}
	if _, err := col.Insert([]byte("u1"), doc("u1", 1)); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{Key: "createdAt", Value: mustBSONRawValue(t, int32(2))}})
	if err != nil || !matched || !modified {
		t.Fatalf("UpdateBSONSet matched=%v modified=%v err=%v want true true nil", matched, modified, err)
	}
	stringRaw := bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString("acme")}
	intRaw := func(value int32) bson.RawValue {
		return bson.RawValue{Type: bson.TypeInt32, Value: []byte{byte(value), byte(value >> 8), byte(value >> 16), byte(value >> 24)}}
	}
	oldIDs, truncated, err := col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw}, Lower: IndexRangeBound{Value: intRaw(1), Inclusive: true}, Upper: IndexRangeBound{Value: intRaw(1), Inclusive: true}, Limit: 2})
	if err != nil || truncated || len(oldIDs) != 0 {
		t.Fatalf("old component scan ids=%q truncated=%v err=%v want empty,false,nil", oldIDs, truncated, err)
	}
	newIDs, truncated, err := col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw}, Lower: IndexRangeBound{Value: intRaw(2), Inclusive: true}, Upper: IndexRangeBound{Value: intRaw(2), Inclusive: true}, Limit: 2})
	if err != nil || truncated || len(newIDs) != 1 || !bytes.Equal(newIDs[0], []byte("u1")) {
		t.Fatalf("new component scan ids=%q truncated=%v err=%v want [u1],false,nil", newIDs, truncated, err)
	}
	if _, err := col.Insert([]byte("u2"), doc("u2", 2)); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("new compound key duplicate err=%v want unique conflict", err)
	}
}

func TestCompoundReverseScanPersistsOverlayTombstonesAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	mgr := NewCollectionManager(db)
	meta := &CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON, BufferedIndexedOverlayRoots: true, BufferedIndexedWriteMaxDocuments: 1, BufferedIndexedWriteMaxRootRuns: 1, DisableBufferedIndexedAsyncFlush: true}, Indexes: []IndexDefinition{{
		Name: "tenant_created", Components: []IndexComponent{{Field: "tenant", Direction: IndexDirectionAscending}, {Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2,
	}}}
	if _, err := mgr.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	insert := func(id string, created int32) {
		t.Helper()
		doc, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: created}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.Insert([]byte(id), doc); err != nil {
			t.Fatal(err)
		}
		if err := col.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	insert("a", 1)
	insert("b", 3)
	insert("c", 2)
	insert("e", 3)
	if _, err := col.CompactRootOverlays(context.Background()); err != nil {
		t.Fatalf("compact persisted insert overlays: %v", err)
	}
	if deleted, err := col.DeleteDocument([]byte("c")); err != nil || !deleted {
		t.Fatalf("delete c deleted=%v err=%v", deleted, err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	stringRaw := bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString("acme")}
	assertReverse := func(collection *Collection) {
		t.Helper()
		ids, truncated, err := collection.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw}, Desc: true, StableDocumentIDTies: true, Limit: 4})
		if err != nil || truncated {
			t.Fatalf("reverse scan ids=%q truncated=%v err=%v", ids, truncated, err)
		}
		want := []string{"a", "b", "e"}
		if len(ids) != len(want) {
			t.Fatalf("reverse ids=%q want %q", ids, want)
		}
		for i := range want {
			if string(ids[i]) != want[i] {
				t.Fatalf("reverse ids=%q want %q", ids, want)
			}
		}
	}
	assertReverse(col)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	col, err = NewCollectionManager(db).OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	assertReverse(col)
}

func TestCompoundIndexRangeCapsInspectedTombstoneEntries(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON, BufferedIndexedOverlayRoots: true, BufferedIndexedWriteMaxDocuments: 128, BufferedIndexedWriteMaxRootRuns: 1, DisableBufferedIndexedAsyncFlush: true}, Indexes: []IndexDefinition{{
		Name: "tenant_created", Components: []IndexComponent{{Field: "tenant", Direction: IndexDirectionAscending}, {Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2,
	}}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	const rows = compoundIndexRangeInspectedMultiplier + 1
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := range ids {
		id := fmt.Sprintf("%03d", i)
		ids[i] = []byte(id)
		docs[i], err = bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: int32(i)}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := col.CompactRootOverlays(context.Background()); err != nil {
		t.Fatal(err)
	}
	if deleted, err := col.DeleteBatch(ids); err != nil || deleted != rows {
		t.Fatalf("DeleteBatch deleted=%d err=%v want %d", deleted, err, rows)
	}
	prefix := []bson.RawValue{{Type: bson.TypeString, Value: bsoncoreAppendString("acme")}}
	for _, desc := range []bool{false, true} {
		got, truncated, err := col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: prefix, Limit: 1, Desc: desc})
		if err != nil || !truncated || len(got) != 0 {
			t.Fatalf("desc=%v ids=%q truncated=%v err=%v; want cap after %d inspected tombstones", desc, got, truncated, err, compoundIndexRangeInspectedMultiplier)
		}
	}
}

func TestCompoundBSONIndexPersistedOverlayConstructorWorkCapReturnsTruncated(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{{
		Name: "tenant_created", Components: []IndexComponent{{Field: "tenant", Direction: IndexDirectionAscending}, {Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2,
	}}}); err != nil {
		t.Fatalf("create: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		t.Fatal(err)
	}
	_ = snap.Close()
	// Constructor-time fanout rejects before opening roots, so synthetic IDs
	// exercise the public truncation contract without unbounded storage setup.
	injected := *catalog
	injected.rootOverlays = make(map[string][]uint64, len(catalog.rootOverlays)+1)
	for name, ids := range catalog.rootOverlays {
		injected.rootOverlays[name] = append([]uint64(nil), ids...)
	}
	rootName := collectionSecondaryRootName("events", "tenant_created")
	injected.rootOverlays[rootName] = make([]uint64, compoundIndexRangeInspectedCap(1)+1)
	for i := range injected.rootOverlays[rootName] {
		injected.rootOverlays[rootName][i] = uint64(i + 1)
	}
	current := db.AcquireSnapshot()
	if current == nil {
		t.Fatal("acquire current snapshot")
	}
	col.catalogMu.Lock()
	col.catalog = &injected
	col.catalogSystemRoot = snapshotSystemRoot(current)
	col.catalogCommitSeq = snapshotCommitSeq(current)
	col.catalogMu.Unlock()
	_ = current.Close()
	prefix := []bson.RawValue{{Type: bson.TypeString, Value: bsoncoreAppendString("acme")}}
	for _, desc := range []bool{false, true} {
		ids, truncated, err := col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: prefix, Limit: 1, Desc: desc})
		if err != nil || !truncated || len(ids) != 0 {
			t.Fatalf("desc=%v ids=%q truncated=%v err=%v want empty,true,nil", desc, ids, truncated, err)
		}
	}
}

func TestCompoundIndexPointerCheckpointReopenAndRecreate(t *testing.T) {
	dir := t.TempDir()
	opts := backenddb.Options{Dir: dir, ValueLog: backenddb.ValueLogOptions{PointerThreshold: 1, ForcePointers: true}}
	db, err := backenddb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	// Keep the directory unlockable even when a maintenance assertion fails.
	// This matters on Windows, where TempDir cleanup cannot remove an open DB.
	defer func() { _ = db.Close() }()
	mgr := NewCollectionManager(db)
	index := IndexDefinition{Name: "tenant_created", Components: []IndexComponent{{Field: "tenant", Direction: IndexDirectionAscending}, {Field: "createdAt", Direction: IndexDirectionDescending}}, ValueType: IndexValueBSONOrderedV2, Unique: true}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}, Indexes: []IndexDefinition{index}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id      string
		created int32
	}{{"a", 1}, {"b", 2}} {
		doc, err := bson.Marshal(bson.D{{Key: "_id", Value: row.id}, {Key: "tenant", Value: "acme"}, {Key: "createdAt", Value: row.created}, {Key: "payload", Value: string(bytes.Repeat([]byte(row.id), 2048))}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.Insert([]byte(row.id), doc); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// Exercise the persistent ValueLog lifecycle against compound-secondary
	// pointers before the reopen assertion below. The index values and BSON
	// documents are forced through pointers by the options above.
	if _, err := db.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{BatchSize: 16, SyncEachBatch: true}); err != nil {
		t.Fatalf("compound ValueLogRewriteOnline: %v", err)
	}
	if _, err := db.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatalf("compound ValueLogGC: %v", err)
	}
	if err := db.VacuumIndexOnline(context.Background()); err != nil && !errors.Is(err, backenddb.ErrVacuumUnsupported) {
		t.Fatalf("compound VacuumIndexOnline: %v", err)
	} else if errors.Is(err, backenddb.ErrVacuumUnsupported) {
		// Windows cannot atomically swap the mapped index file. Keep rewrite/GC
		// and reopen coverage there; supported platforms still exercise vacuum.
		t.Log("compound VacuumIndexOnline unsupported on this platform")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = backenddb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	col, err = NewCollectionManager(db).OpenCollection("events")
	if err != nil {
		t.Fatal(err)
	}
	assertIDs := func() {
		t.Helper()
		ids, truncated, err := col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{{Type: bson.TypeString, Value: bsoncoreAppendString("acme")}}, Limit: 2})
		if err != nil || truncated || len(ids) != 2 || string(ids[0]) != "b" || string(ids[1]) != "a" {
			t.Fatalf("compound pointer scan ids=%q truncated=%v err=%v", ids, truncated, err)
		}
	}
	assertIDs()
	if _, err := col.DropIndex("tenant_created"); err != nil {
		t.Fatal(err)
	}
	if _, err := col.CreateIndex(index); err != nil {
		t.Fatal(err)
	}
	assertIDs()
}

func TestBufferedRootRunsReverseIteratorSeekUsesReverseBounds(t *testing.T) {
	makeRun := func(keys ...string) memtable.Table {
		t.Helper()
		table := newCollectionRunTable(len(keys))
		for _, key := range keys {
			setCollectionRunValue(table, []byte(key), nil)
		}
		table.Freeze()
		return table
	}
	first := makeRun("b", "d")
	second := makeRun("c", "e")
	defer resetCollectionRunTable(first)
	defer resetCollectionRunTable(second)
	it := newBufferedRootRunsReverseIteratorWithDeleted([]memtable.Table{first, second}, []byte("b"), []byte("f"), true)
	defer func() { _ = it.Close() }()
	assertKey := func(want string) {
		t.Helper()
		if !it.Valid() || string(it.UnsafeKey()) != want {
			t.Fatalf("reverse iterator key=%q valid=%v want %q", it.UnsafeKey(), it.Valid(), want)
		}
	}
	assertKey("e")
	it.Seek([]byte("d"))
	assertKey("d")
	it.Seek([]byte("z"))
	assertKey("e")
	it.Seek([]byte("a"))
	if it.Valid() {
		t.Fatalf("reverse seek below start key=%q want invalid", it.UnsafeKey())
	}
}

func TestScanMergedCompoundIndexIDsReverseInterleavesBufferedAndPersisted(t *testing.T) {
	key := func(value, id string) []byte {
		t.Helper()
		component, err := encodeBSONIndexKeyComponentV2(bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString(value)})
		if err != nil {
			t.Fatal(err)
		}
		entry, err := bsonIndexEntryKeyV2(component, []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	makeTable := func(pairs ...[2]string) memtable.Table {
		t.Helper()
		table := newCollectionRunTable(len(pairs))
		for _, pair := range pairs {
			setCollectionRunValue(table, key(pair[0], pair[1]), nil)
		}
		table.Freeze()
		return table
	}
	// The first comparison is buffered "c" < persisted "d". Reverse merge
	// must choose persisted d, then alternate the two sources without treating
	// unequal keys as equal/shadowed.
	buffered := makeTable([2]string{"c", "c"}, [2]string{"a", "a"})
	persisted := makeTable([2]string{"d", "d"}, [2]string{"b", "b"})
	defer resetCollectionRunTable(buffered)
	defer resetCollectionRunTable(persisted)
	bufferedIt := buffered.NewReverseIterator(nil, nil)
	persistedIt := persisted.NewReverseIterator(nil, nil)
	defer func() { _ = bufferedIt.Close() }()
	defer func() { _ = persistedIt.Close() }()
	var got []string
	truncated, err := scanMergedCollectionIndexIDsReverse(bufferedIt, persistedIt, IndexValueBSONOrderedV2, 4, false, func(id []byte) (bool, error) {
		got = append(got, string(id))
		return true, nil
	})
	if err != nil || truncated || fmt.Sprint(got) != "[d c b a]" {
		t.Fatalf("reverse merged ids=%v truncated=%v err=%v", got, truncated, err)
	}
}

func TestScanMergedCompoundIndexIDsStableReverseDoesNotEmitPartialTieAtWorkCap(t *testing.T) {
	key := func(value, id string) []byte {
		t.Helper()
		component, err := encodeBSONIndexKeyComponentV2(bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString(value)})
		if err != nil {
			t.Fatal(err)
		}
		entry, err := bsonIndexEntryKeyV2(component, []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	table := newCollectionRunTable(3)
	defer resetCollectionRunTable(table)
	// Reverse physical order reaches b,a (one logical tie) before c. A cap at
	// two entries must not publish b,a without proving that their tie ended.
	setCollectionRunValue(table, key("a", "a"), nil)
	setCollectionRunValue(table, key("a", "b"), nil)
	setCollectionRunValue(table, key("b", "c"), nil)
	table.Freeze()
	it := table.NewReverseIterator(nil, nil)
	defer func() { _ = it.Close() }()
	var got []string
	truncated, err := scanMergedCollectionIndexIDsWithOptionsAndDirectionWorkCap(nil, it, IndexValueBSONOrderedV2, 4, true, 2, scanMergedCollectionIndexIDOptions{
		CloneDocumentID:      true,
		StableDocumentIDTies: true,
		LogicalIndexKey:      bsonIndexKeyValuePrefixV2,
	}, func(id []byte) (bool, error) {
		got = append(got, string(id))
		return true, nil
	})
	if err != nil || !truncated || fmt.Sprint(got) != "[c]" {
		t.Fatalf("stable reverse cap ids=%q truncated=%v err=%v want [c],true,nil", got, truncated, err)
	}
}

func TestBSONCompoundDescendingEntryFailsClosedWhenCorrupt(t *testing.T) {
	component, err := encodeBSONIndexKeyComponentV2(bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString("acme")})
	if err != nil {
		t.Fatal(err)
	}
	descending, err := descendingBSONIndexKeyComponentV2(component)
	if err != nil {
		t.Fatal(err)
	}
	key, err := bsonIndexEntryKeyV2(descending, []byte("doc-7"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bsonIndexKeyDocumentIDV2(key[:len(key)-1]); err == nil {
		t.Fatal("truncated descending entry parsed successfully")
	}
	corrupt := append([]byte(nil), key...)
	corrupt[1] ^= 0x7f
	if _, err := bsonIndexKeyDocumentIDV2(corrupt); err == nil {
		t.Fatal("corrupt descending entry parsed successfully")
	}
}

func bsoncoreAppendString(value string) []byte {
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(value)+1))
	return append(append(header[:], value...), 0)
}

func TestBSONCoreAppendStringUsesFullLength(t *testing.T) {
	want := strings.Repeat("x", 300)
	raw := bson.RawValue{Type: bson.TypeString, Value: bsoncoreAppendString(want)}
	got, ok := raw.StringValueOK()
	if !ok || got != want {
		t.Fatalf("long BSON string=%q ok=%v", got, ok)
	}
}
