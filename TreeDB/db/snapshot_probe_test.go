package db

import (
	"reflect"
	"testing"
)

func TestSnapshot_HasManyAndHasPrefixes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.SetSync([]byte("acct/alice/doc-1"), []byte("v1")); err != nil {
		t.Fatalf("set alice: %v", err)
	}
	if err := db.SetSync([]byte("acct/bob/doc-1"), []byte("v2")); err != nil {
		t.Fatalf("set bob: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	if err := db.DeleteSync([]byte("acct/alice/doc-1")); err != nil {
		t.Fatalf("delete alice: %v", err)
	}
	if err := db.SetSync([]byte("acct/carol/doc-1"), []byte("v3")); err != nil {
		t.Fatalf("set carol: %v", err)
	}

	hasMany, err := snap.HasMany([][]byte{
		[]byte("acct/alice/doc-1"),
		[]byte("acct/bob/doc-1"),
		[]byte("acct/carol/doc-1"),
	})
	if err != nil {
		t.Fatalf("HasMany: %v", err)
	}
	if want := []bool{true, true, false}; !reflect.DeepEqual(hasMany, want) {
		t.Fatalf("HasMany mismatch: got=%v want=%v", hasMany, want)
	}

	hasPrefixes, err := snap.HasPrefixes([][]byte{
		[]byte("acct/alice/"),
		[]byte("acct/bob/"),
		[]byte("acct/carol/"),
	})
	if err != nil {
		t.Fatalf("HasPrefixes: %v", err)
	}
	if want := []bool{true, true, false}; !reflect.DeepEqual(hasPrefixes, want) {
		t.Fatalf("HasPrefixes mismatch: got=%v want=%v", hasPrefixes, want)
	}
}
