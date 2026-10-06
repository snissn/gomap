package treedb

import (
	"bytes"
	"strconv"
	"testing"
)

// This public-path witness must keep the cache dirty: moving all data into the
// backend before capturing a snapshot would conceal a rotating implementation.
func TestCOWCacheDirtySnapshotPinsWithoutRotation(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.MemtableShards = 2
			opts.FlushThreshold = 1 << 30
			db, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			key, original := []byte("account/first"), []byte("original")
			if err := db.Set(key, original); err != nil {
				t.Fatal(err)
			}
			before := db.Stats()
			snap := db.AcquireSnapshot()
			if snap == nil {
				t.Fatal("dirty COW snapshot unavailable")
			}
			defer snap.Close()
			for _, counter := range []string{
				"treedb.cache.snapshot.rotated_shards_total",
				"treedb.cache.snapshot.enqueued_records_total",
				"treedb.cache.snapshot.enqueued_bytes_total",
			} {
				after := db.Stats()
				prior, err := strconv.ParseUint(before[counter], 10, 64)
				if err != nil {
					t.Fatalf("missing capture counter %s: %v", counter, err)
				}
				next, err := strconv.ParseUint(after[counter], 10, 64)
				if err != nil || next != prior {
					t.Fatalf("capture changed %s: before=%q after=%q err=%v", counter, before[counter], after[counter], err)
				}
			}
			// Callers can immediately reuse write buffers after the ACK.
			copy(original, "recycled")
			if err := db.Set(key, []byte("replacement")); err != nil {
				t.Fatal(err)
			}
			if err := db.Delete(key); err != nil {
				t.Fatal(err)
			}
			if err := db.Set([]byte("account/second"), []byte("new")); err != nil {
				t.Fatal(err)
			}
			got, err := snap.Get(key)
			if err != nil || !bytes.Equal(got, []byte("original")) {
				t.Fatalf("pinned cut changed after buffer reuse/overwrite/delete: value=%q err=%v", got, err)
			}
			if exists, err := snap.Has([]byte("account/second")); err != nil || exists {
				t.Fatalf("pinned cut includes later insert: exists=%v err=%v", exists, err)
			}
		})
	}
}
