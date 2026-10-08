package treedb_test

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestOwnedLeafManifestCachedPublicPointerRowsReopen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.Options{Dir: dir, OwnedLeafManifests: true, IndexOuterLeavesInValueLog: true}
	opts.ValueLog.PointerThreshold = 1
	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	writeRows := func(suffix string) {
		t.Helper()
		batch := d.NewBatch()
		defer batch.Close()
		for i := 0; i < 32; i++ {
			if err := batch.Set([]byte(fmt.Sprintf("public/%02d", i)), bytes.Repeat([]byte(fmt.Sprintf("full payload %02d %s|", i, suffix)), 256)); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			t.Fatal(err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	writeRows("original")
	held := d.AcquireSnapshot()
	writeRows("replacement")
	for i := 0; i < 32; i++ {
		key := []byte(fmt.Sprintf("public/%02d", i))
		want := bytes.Repeat([]byte(fmt.Sprintf("full payload %02d original|", i)), 256)
		got, err := held.Get(key)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("held public pointer row %d: %v", i, err)
		}
	}
	held.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// Persisted required feature infers ownership through the normal public API.
	d, err = treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 32; i++ {
		want := bytes.Repeat([]byte(fmt.Sprintf("full payload %02d replacement|", i)), 256)
		got, err := d.Get([]byte(fmt.Sprintf("public/%02d", i)))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reopened public pointer row %d: %v", i, err)
		}
	}
}
