package caching

import (
	"bytes"
	"testing"
)

func requireCachingRawKVValue(t *testing.T, db *DB, key []byte, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q)=%q want %q", key, got, want)
	}
	if len(want) == 0 && got == nil {
		t.Fatalf("Get(%q) returned nil for present zero-length value", key)
	}
	has, err := db.Has(key)
	if err != nil {
		t.Fatalf("Has(%q): %v", key, err)
	}
	if !has {
		t.Fatalf("Has(%q)=false, want true", key)
	}
}

func TestRawKVParityKeyRangePreservesConcreteEmptyKey(t *testing.T) {
	var r keyRange
	r.add([]byte{})
	if !r.valid || r.min == nil || len(r.min) != 0 || r.max == nil || len(r.max) != 0 {
		t.Fatalf("range after empty add = valid=%v min=%#v max=%#v, want non-nil empty bounds", r.valid, r.min, r.max)
	}
	var propagated keyRange
	propagated.add(r.min)
	propagated.add(r.max)
	if !propagated.valid || propagated.min == nil || len(propagated.min) != 0 || propagated.max == nil || len(propagated.max) != 0 {
		t.Fatalf("propagated range = valid=%v min=%#v max=%#v, want non-nil empty bounds", propagated.valid, propagated.min, propagated.max)
	}
}

func TestRawKVParityCachedEmptyKeyNilValue(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set(nil, nil); err != nil {
		t.Fatalf("Set(nil,nil): %v", err)
	}
	requireCachingRawKVValue(t, db, []byte{}, []byte{})
	requireCachingRawKVValue(t, db, nil, []byte{})

	if err := db.Set([]byte{}, []byte("empty")); err != nil {
		t.Fatalf("Set(empty,value): %v", err)
	}
	requireCachingRawKVValue(t, db, nil, []byte("empty"))

	b := db.NewBatch()
	if err := b.Set(nil, nil); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set(nil,nil): %v", err)
	}
	if err := b.Delete(nil); err != nil {
		_ = b.Close()
		t.Fatalf("batch Delete(nil): %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	if has, err := db.Has([]byte{}); err != nil {
		t.Fatalf("Has(empty after batch delete): %v", err)
	} else if has {
		t.Fatal("empty key still present after batch Delete(nil)")
	}
}
