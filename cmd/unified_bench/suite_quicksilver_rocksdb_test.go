//go:build rocksdb

package main

import "testing"

func TestQuicksilverRealisticRocksDB(t *testing.T) {
	c := quicksilverRealisticSmokeConfig()
	r, err := runQuicksilverEngine(BenchConfig{}, c, "rocksdb", NewRocksDB, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if r.VerifiedKeys != c.Keys-r.Mutations.Deletes+r.Mutations.Inserts || r.Config.CommitMode != "ordinary" {
		t.Fatalf("incomplete native proof: %+v", r)
	}
}
