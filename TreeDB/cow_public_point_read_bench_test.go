package treedb

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
)

// BenchmarkCOWPublicPointRead measures one public GetAppend into caller scratch.
// Settled500k uses the unified raw-read shape: 500,000 sortable 8-byte raw keys,
// 128-byte zero values, seed 1, and command-WAL-relaxed acknowledgment. Explicit
// Checkpoint before timing defines backend settlement; this is a separate
// focused boundary from unified-bench's naturally mixed post-preload state.
// Dirty4096 is a cached-inline guardrail with the same key/value/profile shape.
// Pointer guardrails use 4,096 records with forced 4,096-byte pointer values.
// Setup, checkpoint, warmup, final Close and stats are excluded equally. Required
// cut capture, traversal, integrity checks and caller copy-out remain timed.
//
// For an old source that lacks this benchmark, apply this exact test-only file
// as a shared overlay; do not include candidate production files in the baseline.
func BenchmarkCOWPublicPointRead(b *testing.B) {
	for _, fixture := range []struct {
		name                string
		records, valueBytes int
		settled, pointers   bool
	}{
		{"Settled500k", 500000, 128, true, false},
		{"Dirty4096", 4096, 128, false, false},
		{"SettledPointer4096", 4096, 4096, true, true},
		{"DirtyPointer4096", 4096, 4096, false, true},
	} {
		b.Run(fixture.name, func(b *testing.B) {
			b.StopTimer()
			records := fixture.records
			opts := OptionsFor(ProfileCommandWALRelaxed, b.TempDir())
			opts.MemtableMode, opts.MemtableShards = "cow_ordered", 2
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.BackgroundIndexVacuumInterval = -1
			opts.DisableBackgroundPrune = true
			if fixture.pointers {
				opts.ValueLog.PointerThreshold = 1
				opts.ValueLog.ForcePointers = true
			}
			if !fixture.settled {
				opts.FlushThreshold = 1 << 30
			}
			database, err := Open(opts)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := database.Close(); err != nil {
					b.Error(err)
				}
			})
			stats := database.Stats()
			if stats["treedb.cache.memtable_mode"] != "cow_ordered" || database.ResolvedProfile() != ProfileCommandWALRelaxed {
				b.Fatalf("public mode/profile not active: mode=%q profile=%s", stats["treedb.cache.memtable_mode"], database.ResolvedProfile())
			}
			value := make([]byte, fixture.valueBytes)
			var key [8]byte
			for i := 0; i < records; i++ {
				binary.BigEndian.PutUint64(key[:], uint64(i))
				if err := database.Set(key[:], value); err != nil {
					b.Fatal(err)
				}
			}
			if fixture.settled {
				if err := database.Checkpoint(); err != nil {
					b.Fatal(err)
				}
			}
			// Warm actual caller copy-out and all existing admitted scratch once.
			dst := make([]byte, 0, len(value))
			if got, err := database.GetAppend(key[:], dst); err != nil || !bytes.Equal(got, value) {
				b.Fatalf("public warmup=%d/%v", len(got), err)
			}
			rng := rand.New(rand.NewSource(1))
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				binary.BigEndian.PutUint64(key[:], uint64(rng.Intn(records)))
				got, err := database.GetAppend(key[:], dst)
				if err != nil || len(got) != len(value) {
					b.Fatalf("public read=%d/%v", len(got), err)
				}
			}
			b.StopTimer()
		})
	}
}
