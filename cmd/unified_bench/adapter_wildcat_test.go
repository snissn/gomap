package main

import (
	"reflect"
	"testing"
)

func TestWildcatAdapterBasicOperations(t *testing.T) {
	dbi, err := NewWildcat(t.TempDir())
	if err != nil {
		t.Fatalf("NewWildcat: %v", err)
	}
	t.Cleanup(func() {
		if err := dbi.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	db := dbi.(*WildcatWrapper)
	if db.Name() != "Wildcat" {
		t.Fatalf("Name() = %q", db.Name())
	}

	if err := db.Set([]byte("a"), []byte("one")); err != nil {
		t.Fatalf("Set a: %v", err)
	}
	if err := db.Set([]byte("b"), []byte("two")); err != nil {
		t.Fatalf("Set b: %v", err)
	}
	if err := db.Set([]byte{0, 1, 'x'}, []byte("binary-key")); err != nil {
		t.Fatalf("Set binary key: %v", err)
	}
	got, err := db.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get a: %v", err)
	}
	if string(got) != "one" {
		t.Fatalf("Get a = %q, want one", got)
	}
	got, err = db.Get([]byte{0, 1, 'x'})
	if err != nil {
		t.Fatalf("Get binary key: %v", err)
	}
	if string(got) != "binary-key" {
		t.Fatalf("Get binary key = %q, want binary-key", got)
	}

	batch, err := db.NewBatch()
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if err := batch.Set([]byte("c"), []byte("three")); err != nil {
		t.Fatalf("batch Set c: %v", err)
	}
	if err := batch.Delete([]byte("b")); err != nil {
		t.Fatalf("batch Delete b: %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("batch Commit: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}

	got, err = db.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Get deleted b: %v", err)
	}
	if got != nil {
		t.Fatalf("Get deleted b = %q, want nil", got)
	}

	it, err := db.Iterator([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Iterator Close: %v", err)
	}
	if !reflect.DeepEqual(keys, []string{"a", "c"}) {
		t.Fatalf("forward keys = %v, want [a c]", keys)
	}

	rit, err := db.ReverseIterator([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("ReverseIterator: %v", err)
	}
	keys = keys[:0]
	for ; rit.Valid(); rit.Next() {
		keys = append(keys, string(rit.Key()))
	}
	if err := rit.Close(); err != nil {
		t.Fatalf("ReverseIterator Close: %v", err)
	}
	if !reflect.DeepEqual(keys, []string{"c", "a"}) {
		t.Fatalf("reverse keys = %v, want [c a]", keys)
	}
}

func TestParseWildcatSyncOption(t *testing.T) {
	for _, mode := range []string{"none", "partial", "full", "0", "1", "2"} {
		if _, err := parseWildcatSyncOption(mode); err != nil {
			t.Fatalf("parseWildcatSyncOption(%q): %v", mode, err)
		}
	}
	if _, err := parseWildcatSyncOption("bad"); err == nil {
		t.Fatalf("parseWildcatSyncOption(bad) succeeded")
	}
}
