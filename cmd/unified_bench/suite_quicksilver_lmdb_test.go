//go:build lmdb

package main

import (
	"testing"

	"github.com/snissn/gomap/kvstore"
)

func TestQuicksilverLMDB(t *testing.T) {
	c := quicksilverSmokeConfig()
	r, err := runQuicksilverEngine(BenchConfig{}, c, "lmdb", NewLMDB, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if r.VerifiedKeys != c.Keys {
		t.Fatalf("incomplete proof: %+v", r)
	}
}

func TestQuicksilverLMDBSnapshotOwned(t *testing.T) {
	db, err := NewLMDB(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	snap, err := db.(kvstore.ReadSnapshotter).AcquireReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	v, err := snap.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	v[0] = 'X'
	again, err := snap.Get([]byte("key"))
	if err != nil || string(again) != "value" {
		t.Fatalf("snapshot borrowed bytes: %q/%v", again, err)
	}
	if err = snap.Close(); err != nil {
		t.Fatal(err)
	}
	if string(again) != "value" {
		t.Fatal("owned value changed after close")
	}
}
