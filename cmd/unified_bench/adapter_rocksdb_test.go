//go:build rocksdb && cgo

package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/snissn/gomap/kvstore"
)

func TestRocksDBOwnedSnapshotsAndDurability(t *testing.T) {
	dir := t.TempDir()
	db, err := NewRocksDB(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	r := db.(*RocksDBWrapper)
	key, value := []byte{0, 255, 1}, []byte{255, 0, 2}
	want := bytes.Clone(value)
	b, err := r.NewBatch()
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Set(key, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 7
	if err = b.Set([]byte("empty"), []byte{}); err != nil {
		t.Fatal(err)
	}
	if err = b.Set(nil, []byte("empty-key")); err != nil {
		t.Fatal(err)
	}
	if err = b.CommitSync(); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := r.AcquireReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, read := range []func([]byte) ([]byte, error){r.Get, s.Get} {
		got, err := read(key)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("copied input: %x/%v", got, err)
		}
		got[0] = 8
		got, err = read(key)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("owned output: %x/%v", got, err)
		}
		got, err = read([]byte("missing"))
		if err != nil || got != nil {
			t.Fatalf("missing: %x/%v", got, err)
		}
		got, err = read([]byte("empty"))
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("present empty: %x/%v", got, err)
		}
	}
	got, err := s.GetAppend(key, []byte("prefix"))
	if err != nil || !bytes.Equal(got, append([]byte("prefix"), want...)) {
		t.Fatalf("append: %x/%v", got, err)
	}
	if err = r.SetSync(key, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err = r.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get(key)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("snapshot changed across write/flush: %x/%v", got, err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Get(key); err != kvstore.ErrUnsupported {
		t.Fatalf("closed snapshot: %v", err)
	}
	if err = r.DeleteSync([]byte("empty")); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = NewRocksDB(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err = db.Get(key)
	if err != nil || string(got) != "new" {
		t.Fatalf("reopen: %q/%v", got, err)
	}
	got, err = db.Get([]byte("empty"))
	if err != nil || got != nil {
		t.Fatalf("delete reopen: %q/%v", got, err)
	}
	got, err = db.Get(nil)
	if err != nil || string(got) != "empty-key" {
		t.Fatalf("empty key reopen: %q/%v", got, err)
	}
}

func TestQuicksilverRocksDB(t *testing.T) {
	c := quicksilverSmokeConfig()
	raw, err := runQuicksilverSuite(BenchConfig{DBsArg: "rocksdb"}, c, "")
	if err != nil {
		t.Fatal(err)
	}
	var reports []quicksilverResult
	if err = json.Unmarshal([]byte(raw), &reports); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].VerifiedKeys != c.Keys || reports[0].VerifiedMisses != c.Keys || reports[0].UpdatedKeys != c.Updates {
		t.Fatalf("incomplete proof: %+v", reports)
	}
	for _, p := range reports[0].Phases[:3] {
		if p.Ops != c.Reads {
			t.Fatalf("lost aggregate remainder: %+v", p)
		}
	}
}
