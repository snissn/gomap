//go:build lmdb

package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/kvstore"
)

func TestQuicksilverLMDB(t *testing.T) {
	c := quicksilverSmokeConfig()
	c.Workers = 126 // LMDB default reader limit; one live read transaction per worker.
	c.Reads = 253
	raw, err := runQuicksilverSuite(BenchConfig{DBsArg: "lmdb"}, c, "")
	if err != nil {
		t.Fatal(err)
	}
	var reports []quicksilverResult
	if err := json.Unmarshal([]byte(raw), &reports); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].VerifiedKeys != c.Keys {
		t.Fatalf("incomplete proof: %+v", reports)
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

func TestQuicksilverLMDBRejectsReaderOverflowBeforeOpen(t *testing.T) {
	original := dbFactories
	t.Cleanup(func() { dbFactories = original })
	calls := 0
	dbFactories = make(map[string]DBFactory, len(original))
	for name := range original {
		dbFactories[name] = func(string) (kvstore.DB, error) { calls++; return nil, errors.New("unexpected engine open") }
	}
	for _, workers := range []int{127, 1024} {
		for _, batch := range []int{1, 64} {
			for _, selection := range []string{"lmdb", "treedb,lmdb"} {
				c := quicksilverSmokeConfig()
				c.Workers, c.ReadBatch = workers, batch
				_, err := runQuicksilverSuite(BenchConfig{DBsArg: selection}, c, "")
				if err == nil || !strings.Contains(err.Error(), "LMDB supports at most 126 read workers") || calls != 0 {
					t.Fatalf("selection=%s workers=%d readBatch=%d opened=%d: %v", selection, workers, batch, calls, err)
				}
			}
		}
	}
}
