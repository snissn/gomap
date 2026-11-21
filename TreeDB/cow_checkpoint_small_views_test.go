package treedb_test

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestCOWCheckpointSmallViewLimitStreamsFrozenSources(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
			opts.MemtableMode = mode
			opts.MemtableShards = 4
			opts.FlushThreshold = 64 << 20
			opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
			opts.COWMemtableLimits.MaxViews = 1
			open := func() *treedb.DB {
				t.Helper()
				db, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				return db
			}
			db := open()
			defer db.Close()
			check := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			count := func(stats map[string]string, metric string) uint64 {
				t.Helper()
				n, err := strconv.ParseUint(stats[metric], 10, 64)
				check(err)
				return n
			}
			var keys [32][8]byte
			for i := range keys {
				binary.BigEndian.PutUint64(keys[i][:], uint64(i+1))
			}
			b := db.NewBatch()
			for _, k := range keys {
				check(b.Set(k[:], k[:]))
			}
			check(b.Write())
			check(b.Close())
			const batches = "treedb.cache.stats.backend_write_batches_total"
			const views = "treedb.cache.cow.views"
			before := db.Stats()
			if sources := count(before, "treedb.cache.cow.sources"); sources != 4 {
				t.Fatalf("dirty sources=%d, want 4", sources)
			}
			check(db.Checkpoint())
			after := db.Stats()
			if writes := count(after, batches) - count(before, batches); writes != 4 {
				t.Fatalf("checkpoint backend writes=%d, want one per source", writes)
			}
			if n := count(after, views); n != 0 {
				t.Fatalf("checkpoint left %d cursor views", n)
			}
			check(db.Close())
			db = open()
			defer func() { check(db.Close()) }()
			for _, k := range keys {
				got, err := db.Get(k[:])
				if err != nil || !bytes.Equal(got, k[:]) {
					t.Fatalf("reopened Get(%x)=(%x, %v)", k, got, err)
				}
			}
		})
	}
}
