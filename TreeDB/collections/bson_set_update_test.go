package collections

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func TestBSONSetUpdateBatchCommandWALPrepareAndIntent(t *testing.T) {
	baseDoc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "hnl"},
		{Key: "visits", Value: int32(1)},
	})
	intermediateDoc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "hnl"},
		{Key: "visits", Value: int32(2)},
	})
	updatedDoc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "sea"},
		{Key: "visits", Value: int32(1)},
	})
	dir := prepareCollectionCommandWALDir(t, CollectionMeta{
		Name: "users",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}, collectionCommandWALSetupInsert{
		ids:  [][]byte{[]byte("u1")},
		docs: [][]byte{baseDoc},
	})
	db := openCollectionCommandWALDB(t, dir)
	defer func() { _ = db.Close() }()
	col, err := NewCollectionManager(db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	items := []BSONSetUpdateBatchItem{{
		DocumentID: []byte("u1"),
		Fields: []BSONSetField{{
			Key:   "city",
			Value: mustBSONRawValue(t, "sea"),
		}},
	}}
	results, docs, err := col.PrepareBSONSetUpdateBatchCommandWAL(items)
	if err != nil {
		t.Fatalf("PrepareBSONSetUpdateBatchCommandWAL: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("prepare results=%+v, want one matched modified result", results)
	}
	if len(docs) != 1 || !bytes.Equal(docs[0].ID, []byte("u1")) || !bytes.Equal(docs[0].Document, updatedDoc) {
		t.Fatalf("prepared command WAL docs=%+v, want u1 updated replacement", docs)
	}
	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get after prepare: %v", err)
	}
	if gotCity := bson.Raw(got).Lookup("city").StringValue(); gotCity != "hnl" {
		t.Fatalf("prepare mutated city=%q, want hnl", gotCity)
	}

	_, modified, err := col.UpdateBSONSet([]byte("u1"), []BSONSetField{{
		Key:   "visits",
		Value: mustBSONRawValue(t, int32(2)),
	}})
	if err != nil {
		t.Fatalf("intervening UpdateBSONSet: %v", err)
	}
	if !modified {
		t.Fatal("intervening UpdateBSONSet modified=false, want true")
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush intervening update: %v", err)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get after intervening update: %v", err)
	}
	if !bytes.Equal(got, intermediateDoc) {
		t.Fatalf("intermediate doc=%x, want %x", got, intermediateDoc)
	}

	payload, err := commitlog.EncodeCollectionUpdateBatchByIDPayload("users", docs)
	if err != nil {
		t.Fatalf("EncodeCollectionUpdateBatchByIDPayload: %v", err)
	}
	frame, err := commandwalapply.CollectionUpdateBatchByIDFrame(payload)
	if err != nil {
		t.Fatalf("CollectionUpdateBatchByIDFrame: %v", err)
	}
	handle, _, err := commandwalapply.Append(db, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{})
	if err != nil {
		t.Fatalf("Append command WAL frame: %v", err)
	}
	results, err = col.UpdateBSONSetBatchWithCommandWALIntent(items, docs, handle.CommandWALIntent())
	if err != nil {
		t.Fatalf("UpdateBSONSetBatchWithCommandWALIntent: %v", err)
	}
	if len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("apply results=%+v, want one matched modified result", results)
	}
	if _, err := commandwalapply.Finalize(db, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{}); err != nil {
		t.Fatalf("Finalize command WAL frame: %v", err)
	}
	got, err = col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get after command WAL intent apply: %v", err)
	}
	if !bytes.Equal(got, updatedDoc) {
		t.Fatalf("updated doc=%x, want %x", got, updatedDoc)
	}
}

func TestBSONSetUpdateAppendReplacementUsesDestinationArena(t *testing.T) {
	doc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "email", Value: "a@example.com"},
		{Key: "city", Value: "hnl"},
		{Key: "active", Value: true},
	})
	update, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("new BSON set update: %v", err)
	}
	arena := make([]byte, 7, len(doc)+bsonSetReplacementSlackBytes+16)
	for i := range arena {
		arena[i] = 0x7f
	}
	out, replacement, changed, err := update.appendReplacement(arena, doc)
	if err != nil {
		t.Fatalf("append replacement: %v", err)
	}
	if !changed {
		t.Fatal("changed=false want true")
	}
	if len(out) <= len(arena) {
		t.Fatalf("out len=%d want > %d", len(out), len(arena))
	}
	if len(replacement) == 0 {
		t.Fatal("empty replacement")
	}
	if len(out) == 0 || &out[0] != &arena[0] {
		t.Fatal("output does not reuse destination arena backing array")
	}
	if &replacement[0] != &out[len(arena)] {
		t.Fatal("replacement does not reference appended destination arena")
	}
	if got, ok := bson.Raw(replacement).Lookup("city").StringValueOK(); !ok || got != "sea" {
		t.Fatalf("city=%q ok=%v want sea", got, ok)
	}
	for i, b := range out[:len(arena)] {
		if b != 0x7f {
			t.Fatalf("prefix byte %d=%x want 7f", i, b)
		}
	}
}

func TestBSONSetUpdateAppendReplacementUnchangedRestoresDestination(t *testing.T) {
	doc := mustBSONCollectionDocument(t, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "city", Value: "hnl"},
	})
	update, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "hnl"),
	}})
	if err != nil {
		t.Fatalf("new BSON set update: %v", err)
	}
	arena := make([]byte, 3, len(doc)+bsonSetReplacementSlackBytes+16)
	out, replacement, changed, err := update.appendReplacement(arena, doc)
	if err != nil {
		t.Fatalf("append replacement: %v", err)
	}
	if changed {
		t.Fatal("changed=true want false")
	}
	if len(out) != len(arena) {
		t.Fatalf("out len=%d want restored len %d", len(out), len(arena))
	}
	if len(replacement) != len(doc) || &replacement[0] != &doc[0] {
		t.Fatal("unchanged replacement should be original document")
	}
}

func TestBSONSetUpdateAppendReplacementErrorPreservesGrownDestination(t *testing.T) {
	doc := mustBSONCollectionDocument(t, bson.D{
		{Key: "city", Value: "hnl"},
		{Key: "email", Value: "a@example.com"},
	})
	_, rem, ok := bsoncore.ReadLength(doc)
	if !ok {
		t.Fatal("read BSON length")
	}
	elem, _, ok := bsoncore.ReadElement(rem)
	if !ok {
		t.Fatal("read first BSON element")
	}
	malformed := append([]byte(nil), doc[:4+len(elem)]...)
	malformed = append(malformed, 0x02, 'x', 0x00)
	malformed = append(malformed, 0x00)
	bsoncore.UpdateLength(malformed, 0, int32(len(malformed)))
	update, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("new BSON set update: %v", err)
	}
	arena := []byte("prefix")
	arena = arena[:len(arena):len(arena)]
	out, replacement, changed, err := update.appendReplacement(arena, malformed)
	if err == nil {
		t.Fatal("append replacement err=nil want malformed BSON error")
	}
	if changed {
		t.Fatal("changed=true want false on error")
	}
	if replacement != nil {
		t.Fatalf("replacement=%x want nil on error", replacement)
	}
	if len(out) != len(arena) {
		t.Fatalf("out len=%d want restored len %d", len(out), len(arena))
	}
	if !bytes.Equal(out, arena) {
		t.Fatalf("out prefix=%q want %q", out, arena)
	}
	if cap(out) <= cap(arena) {
		t.Fatalf("out cap=%d want preserved grown capacity > %d", cap(out), cap(arena))
	}
}

func TestBSONSetUpdateAppendReplacementRejectsInvalidCurrentBSON(t *testing.T) {
	update, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(t, "sea"),
	}})
	if err != nil {
		t.Fatalf("new BSON set update: %v", err)
	}
	arena := []byte("prefix")
	arena = arena[:len(arena):len(arena)]
	out, replacement, changed, err := update.appendReplacement(arena, []byte{0x05, 0x00, 0x00, 0x00})
	if err == nil {
		t.Fatal("append replacement err=nil want invalid current BSON")
	}
	if changed {
		t.Fatal("changed=true want false on invalid current BSON")
	}
	if replacement != nil {
		t.Fatalf("replacement=%x want nil", replacement)
	}
	if !bytes.Equal(out, arena) {
		t.Fatalf("out=%q want restored arena %q", out, arena)
	}
}

func BenchmarkBSONSetUpdateApplyCity(b *testing.B) {
	doc := mustBSONCollectionDocument(b, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "email", Value: "a@example.com"},
		{Key: "city", Value: "hnl"},
		{Key: "active", Value: true},
		{Key: "score", Value: int32(42)},
		{Key: "name", Value: "Ada Lovelace"},
	})
	update, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(b, "sea"),
	}})
	if err != nil {
		b.Fatalf("new BSON set update: %v", err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(doc)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacement, changed, err := update.apply(doc)
		if err != nil {
			b.Fatal(err)
		}
		if !changed || len(replacement) == 0 {
			b.Fatal("unexpected unchanged replacement")
		}
	}
}

func BenchmarkBSONSetUpdateAppendReplacementCity(b *testing.B) {
	doc := mustBSONCollectionDocument(b, bson.D{
		{Key: "_id", Value: "u1"},
		{Key: "email", Value: "a@example.com"},
		{Key: "city", Value: "hnl"},
		{Key: "active", Value: true},
		{Key: "score", Value: int32(42)},
		{Key: "name", Value: "Ada Lovelace"},
	})
	update, err := newBSONSetUpdate([]BSONSetField{{
		Key:   "city",
		Value: mustBSONRawValue(b, "sea"),
	}})
	if err != nil {
		b.Fatalf("new BSON set update: %v", err)
	}
	arena := make([]byte, 0, len(doc)+bsonSetReplacementSlackBytes)
	b.ReportAllocs()
	b.SetBytes(int64(len(doc)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var replacement []byte
		var changed bool
		arena, replacement, changed, err = update.appendReplacement(arena[:0], doc)
		if err != nil {
			b.Fatal(err)
		}
		if !changed || len(replacement) == 0 {
			b.Fatal("unexpected unchanged replacement")
		}
	}
}
