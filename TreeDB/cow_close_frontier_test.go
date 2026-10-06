package treedb

import (
	"bytes"
	"fmt"
	"testing"
)

// Close must retain the backend read barrier while a later private build chunk
// reads external leaf pages produced by an earlier chunk. Overwriting a populated
// tree exercises this dependency, even though all user values are inline.
func TestCOWCloseDrainsChunkedOverwritesBeforeLeafReaderTeardown(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := OptionsFor(profile, t.TempDir())
			opts.MemtableMode, opts.MemtableShards = "cow_btree", 2
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.BackgroundIndexVacuumInterval = -1
			opts.MaxWALBytes = -1
			opts.DisableBackgroundPrune = true
			opts.ValueLog.PointerThreshold = 1 << 20
			opts.ValueLog.ForcePointers = false
			// A cache hit must not conceal reads of newly buffered private leaves.
			opts.LeafPageReadCacheEntries = -1
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			const records = 2048
			seed := func(marker byte) {
				pending := database.NewBatchWithSize(records)
				for i := 0; i < records; i++ {
					if err := pending.Set([]byte(fmt.Sprintf("cost/%08d", i)), bytes.Repeat([]byte{marker}, 64)); err != nil {
						t.Fatal(err)
					}
				}
				if err := pending.Write(); err != nil {
					t.Fatal(err)
				}
				if err := pending.Close(); err != nil {
					t.Fatal(err)
				}
			}
			seed(0x41)
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			seed(0x5a)
			// Record the acknowledged final metadata before Close; reopening must
			// recover this dirty seed, rather than only the earlier checkpoint.
			revisions := make([]EntryRevision, records)
			for i := range revisions {
				_, revisions[i], err = database.GetVersioned([]byte(fmt.Sprintf("cost/%08d", i)))
				if err != nil || revisions[i] == 0 {
					t.Fatalf("final live revision %d: %d, %v", i, revisions[i], err)
				}
			}
			cache := database.cached
			if err := database.Close(); err != nil {
				t.Fatalf("final dirty Close: %v", err)
			}
			if s := cache.COWMemoryStats(); s.TotalBytes != 0 || s.Generations != 0 || s.ExternalLeases != 0 || s.PeakBytes == 0 {
				t.Fatalf("final owner drain: %+v", s)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for i, expectedRevision := range revisions {
				value, revision, err := reopened.GetVersioned([]byte(fmt.Sprintf("cost/%08d", i)))
				if err != nil || !bytes.Equal(value, bytes.Repeat([]byte{0x5a}, 64)) || revision != expectedRevision {
					t.Fatalf("reopened final seed %d: len=%d revision=%d want=%d err=%v", i, len(value), revision, expectedRevision, err)
				}
			}
		})
	}
}
