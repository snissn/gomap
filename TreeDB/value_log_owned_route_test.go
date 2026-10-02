package treedb_test

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestOwnedGetValueLogMmapCacheRouteAfterReopen(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("read_only_%t", readOnly), func(t *testing.T) {
			opts := treedb.Options{Dir: t.TempDir(), ValueLog: treedb.ValueLogOptions{
				PointerThreshold: 1, Compression: treedb.ValueLogCompressionBlock, BlockCodec: treedb.ValueLogBlockSnappy,
			}}
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte("owned-value:"), 128)
			batch := db.NewBatch()
			for i := range 128 {
				if err := batch.Set([]byte(fmt.Sprintf("key-%04d", i)), want); err != nil {
					t.Fatal(err)
				}
			}
			if err := batch.WriteSync(); err != nil {
				t.Fatal(err)
			}
			_ = batch.Close()
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			opts.ReadOnly = readOnly
			db, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			counter := func(stats map[string]string, suffix string) uint64 {
				n, err := strconv.ParseUint(stats["treedb.vlog."+suffix], 10, 64)
				if err != nil {
					t.Fatalf("missing backend route counter %s: %v", suffix, err)
				}
				return n
			}
			before := db.Stats()
			for range 2 {
				for i := range 128 {
					got, err := db.Get([]byte(fmt.Sprintf("key-%04d", i)))
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("Get: %v", err)
					}
					got[0] ^= 0xff
				}
			}
			after := db.Stats()
			if counter(after, "mmap_read.hits") <= counter(before, "mmap_read.hits") || counter(after, "mmap_read.fallback_readat") != counter(before, "mmap_read.fallback_readat") {
				t.Fatalf("owned public reads did not select mmap: before=%v after=%v", before["treedb.vlog.mmap_read.hits"], after["treedb.vlog.mmap_read.hits"])
			}
			if counter(after, "grouped_frame_cache.hits") <= counter(before, "grouped_frame_cache.hits") {
				t.Fatal("owned public reads did not hit grouped-frame cache")
			}
		})
	}
}
