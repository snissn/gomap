package collections

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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
	got, truncated, err := col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw("acme")}})
	if err != nil || truncated {
		t.Fatalf("prefix scan err=%v truncated=%v", err, truncated)
	}
	assertIDs(got, "b", "e", "c", "a")
	got, truncated, err = col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw("acme")}, Desc: true, Limit: 3})
	if err != nil || !truncated {
		t.Fatalf("reverse scan err=%v truncated=%v", err, truncated)
	}
	assertIDs(got, "a", "c", "e")
	got, truncated, err = col.FindByCompoundIndexRange("tenant_created", CompoundIndexRangeOptions{Prefix: []bson.RawValue{stringRaw("acme")}, Lower: IndexRangeBound{Value: intRaw(2), Inclusive: true}, Upper: IndexRangeBound{Value: intRaw(3), Inclusive: true}})
	if err != nil || truncated {
		t.Fatalf("range scan err=%v truncated=%v", err, truncated)
	}
	assertIDs(got, "b", "e", "c")
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
