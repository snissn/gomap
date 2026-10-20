package treedb

import (
	"fmt"
	"strconv"
	"testing"
)

func TestPublicCOWRawPrefixDoesNotCheckpointByCommandCount(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.MemtableShards = 1
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := database.Close(); err != nil {
					t.Error(err)
				}
			}()
			stat := func(key string) uint64 {
				n, err := strconv.ParseUint(database.Stats()[key], 10, 64)
				if err != nil {
					t.Fatalf("%s: %v", key, err)
				}
				return n
			}
			before := stat("treedb.cache.checkpoint.runs")
			for i := 0; i < 4097; i++ {
				if err := database.Set([]byte("raw"), []byte(fmt.Sprintf("v%d", i))); err != nil {
					t.Fatalf("write%d: %v", i, err)
				}
			}
			if got := stat("treedb.cache.checkpoint.runs"); got != before {
				t.Fatalf("command count forced checkpoints: %d -> %d", before, got)
			}
			if got, err := database.Get([]byte("raw")); err != nil || string(got) != "v4096" {
				t.Fatalf("latest %q: %v", got, err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
