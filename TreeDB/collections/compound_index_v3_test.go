package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	if _, err := col.InsertBatch([][]byte{[]byte("c")}, [][]byte{doc("a", "acme", 3)}); !errors.Is(err, ErrUniqueIndexConflict) {
		t.Fatalf("duplicate compound key error=%v want unique conflict", err)
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
	got, truncated, err = col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw("acme")}, Lower: IndexRangeBound{Value: intRaw(2), Inclusive: true}, Upper: IndexRangeBound{Value: intRaw(3), Inclusive: true}, Limit: 3})
	if err != nil || truncated {
		t.Fatalf("range scan err=%v truncated=%v", err, truncated)
	}
	assertIDs(got, "b", "e", "c")
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
		ids, truncated, err := collection.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw}, Desc: true, Limit: 4})
		if err != nil || truncated {
			t.Fatalf("reverse scan ids=%q truncated=%v err=%v", ids, truncated, err)
		}
		want := []string{"a", "b"}
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
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	prefix := []bson.RawValue{{Type: bson.TypeString, Value: bsoncoreAppendString("acme")}}
	for _, desc := range []bool{false, true} {
		got, truncated, err := col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: prefix, Limit: 1, Desc: desc})
		if err != nil || !truncated || len(got) != 0 {
			t.Fatalf("desc=%v ids=%q truncated=%v err=%v; want cap after %d inspected tombstones", desc, got, truncated, err, compoundIndexRangeInspectedMultiplier)
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = backenddb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
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
	return append(append([]byte{byte(len(value) + 1), 0, 0, 0}, value...), 0)
}
