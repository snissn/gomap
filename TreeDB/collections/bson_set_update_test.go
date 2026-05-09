package collections

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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
	arena := make([]byte, 7, 256)
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
	arena := make([]byte, 3, 128)
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
	arena := make([]byte, 0, len(doc)+64)
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
