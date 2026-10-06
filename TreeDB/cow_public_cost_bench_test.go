package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/node"
)

// BenchmarkCOWPublicDirtyCost is a bounded integration diagnostic, not a
// sustained-workload qualification. Run with a fixed operation count, e.g.
// -benchtime=16x. All modes use the same public profile, two shards, inline
// 64-byte or forced-pointer 4096-byte values and disabled automatic maintenance.
// Pointer values use the profile's default compression policy. Only COW consumes the
// explicit finite admission bundle; production defaults remain unchanged.
//
// FreshCapture checkpoints and reseeds N dirty entries outside the timer before
// each capture. The other read cases warm one capture outside the timer and
// measure a fixed published source set. IncrementalWrite replaces one existing
// key per operation without a warm snapshot, preserving the populated mutable
// tree in the existing btree comparator. Every latency sample includes the same
// clock overhead. Fixture construction and final checkpoint/close are excluded.
func BenchmarkCOWPublicDirtyCost(b *testing.B) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed, ProfileNoWALFast} {
		for _, mode := range []string{"append_only", "btree", "cow_btree"} {
			for _, layout := range []string{"Inline64", "Pointer4096"} {
				for _, records := range []int{1024, 2048} {
					for _, operation := range []string{"FreshCapture", "CaptureReadRelease", "Forward16", "IncrementalWrite"} {
						b.Run(fmt.Sprintf("%s/%s/%s/N=%d/%s", profile, mode, layout, records, operation), func(b *testing.B) {
							cowPublicDirtyCost(b, profile, mode, layout, records, operation)
						})
					}
				}
			}
		}
	}
}

func cowPublicDirtyCost(b *testing.B, profile Profile, mode, layout string, records int, operation string) {
	b.StopTimer()
	opts := OptionsFor(profile, b.TempDir())
	opts.MemtableMode, opts.MemtableShards = mode, 2
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.DisableBackgroundPrune = true
	opts.ValueLog.PointerThreshold = 1 << 20
	opts.ValueLog.ForcePointers = false
	valueSize := 64
	if layout == "Pointer4096" {
		opts.ValueLog.PointerThreshold = 1
		opts.ValueLog.ForcePointers = true
		valueSize = 4096
	}
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxGenerationBytes = 256 << 20
	opts.COWMemtableLimits.MaxTotalBytes = 1 << 30
	opts.COWMemtableLimits.MaxRetiredBytes = 1 << 30
	database, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := database.Close(); err != nil {
			b.Errorf("final Close: %v", err)
		}
		if mode == "cow_btree" {
			s := database.cached.COWMemoryStats()
			b.Logf("COW_FINAL_CLOSE total=%d history=%d reserved=%d retired=%d controls=%d external=%d generations=%d sources=%d views=%d leases=%d peak=%d", s.TotalBytes, s.HistoryBytes, s.ReservedBytes, s.RetiredBytes, s.ControlBytes, s.ExternalBytes, s.Generations, s.Sources, s.Views, s.ExternalLeases, s.PeakBytes)
			if s.TotalBytes != 0 || s.Generations != 0 || s.Views != 0 || s.ExternalLeases != 0 {
				b.Error("COW final close did not drain")
			}
		}
	})
	keys := make([][]byte, records)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("cost/%08d", i))
	}
	value := bytes.Repeat([]byte{0x5a}, valueSize)
	seed := func() {
		pending := database.NewBatchWithSize(records)
		for _, key := range keys {
			if err := pending.Set(key, value); err != nil {
				_ = pending.Close()
				b.Fatal(err)
			}
		}
		writeErr, closeErr := pending.Write(), pending.Close()
		if writeErr != nil || closeErr != nil {
			b.Fatalf("seed write=%v close=%v", writeErr, closeErr)
		}
	}
	seed()
	seedRotations := cowPublicCostCounter(b, database, "treedb.cache.snapshot.rotations_total")
	if operation != "IncrementalWrite" {
		warm := database.AcquireSnapshot()
		if warm == nil {
			b.Fatal("warm capture unavailable")
		}
		cowPublicCostCheckLayout(b, warm, keys[0], layout)
		if err := warm.Close(); err != nil {
			b.Fatal(err)
		}
	}
	initialRotations := cowPublicCostCounter(b, database, "treedb.cache.snapshot.rotations_total")
	latencies := make([]int64, b.N)
	var rotations uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if operation == "FreshCapture" {
			if err := database.Checkpoint(); err != nil {
				b.Fatal(err)
			}
			seed()
		}
		before := uint64(0)
		if operation == "FreshCapture" {
			before = cowPublicCostCounter(b, database, "treedb.cache.snapshot.rotations_total")
		}
		b.StartTimer()
		start := time.Now()
		if operation == "IncrementalWrite" {
			binary.LittleEndian.PutUint64(value, uint64(i+1))
			if err := database.Set(keys[i%records], value); err != nil {
				b.Fatal(err)
			}
		} else {
			snapshot := database.AcquireSnapshot()
			if snapshot == nil {
				b.Fatal("capture unavailable")
			}
			switch operation {
			case "CaptureReadRelease":
				got, err := snapshot.Get(keys[i%records])
				if err != nil || !bytes.Equal(got, value) {
					b.Fatalf("owned read len=%d err=%v", len(got), err)
				}
			case "Forward16":
				iterator, err := snapshot.Iterator(keys[0], nil)
				if err != nil {
					b.Fatal(err)
				}
				for j := 0; j < 16; j++ {
					if !iterator.Valid() || !bytes.Equal(iterator.Key(), keys[j]) || !bytes.Equal(iterator.Value(), value) {
						b.Fatalf("forward entry %d incorrect", j)
					}
					iterator.Next()
				}
				err, closeErr := iterator.Error(), iterator.Close()
				if err != nil || closeErr != nil {
					b.Fatalf("iterator err=%v close=%v", err, closeErr)
				}
			}
			if err := snapshot.Close(); err != nil {
				b.Fatal(err)
			}
		}
		latencies[i] = time.Since(start).Nanoseconds()
		b.StopTimer()
		if operation == "FreshCapture" {
			rotations += cowPublicCostCounter(b, database, "treedb.cache.snapshot.rotations_total") - before
		}
	}
	if operation != "FreshCapture" {
		rotations = cowPublicCostCounter(b, database, "treedb.cache.snapshot.rotations_total") - initialRotations
	}
	if mode == "cow_btree" && (rotations != 0 || initialRotations != seedRotations) {
		b.Fatalf("COW capture rotated cache: warm=%d timed=%d", initialRotations-seedRotations, rotations)
	}
	if operation == "IncrementalWrite" {
		got, err := database.Get(keys[(b.N-1)%records])
		if err != nil || !bytes.Equal(got, value) {
			b.Fatalf("final write value=%x err=%v", got, err)
		}
		final := database.AcquireSnapshot()
		if final == nil {
			b.Fatal("final layout capture unavailable")
		}
		cowPublicCostCheckLayout(b, final, keys[(b.N-1)%records], layout)
		if err := final.Close(); err != nil {
			b.Fatal(err)
		}
	}
	slices.Sort(latencies)
	for _, percentile := range []int{50, 95, 99} {
		index := (len(latencies)*percentile + 99) / 100
		b.ReportMetric(float64(latencies[index-1]), fmt.Sprintf("p%d-ns", percentile))
	}
	b.ReportMetric(float64(rotations)/float64(b.N), "snapshot_rotations/op")
	b.ReportMetric(float64(initialRotations-seedRotations), "warm_snapshot_rotations")
	if mode == "cow_btree" {
		// These are fixture-total counters/charges outside the timer, including
		// seeding and FreshCapture reseeds. They are not per-operation deltas.
		for _, key := range []string{"total_bytes", "history_bytes", "reserved_bytes", "retired_bytes", "peak_bytes", "control_bytes", "deferred_bytes", "external_bytes", "views", "generations", "sources", "external_leases", "active_cuts", "capture_calls_total", "prepare_calls_total", "publications_total", "rollovers_total", "handoffs_total", "current_roots", "frozen_roots"} {
			b.ReportMetric(float64(cowPublicCostCounter(b, database, "treedb.cache.cow."+key)), "fixture_cow_"+key)
		}
	}
}

func cowPublicCostCheckLayout(b *testing.B, snapshot Snapshot, key []byte, layout string) {
	b.Helper()
	entry, err := snapshot.GetEntry(key)
	if err != nil {
		b.Fatal(err)
	}
	if pointer := entry.Flags&node.FlagPointer != 0; pointer != (layout == "Pointer4096") {
		b.Fatalf("layout %s: observed pointer=%v", layout, pointer)
	}
}

func cowPublicCostCounter(b *testing.B, database *DB, key string) uint64 {
	b.Helper()
	value, err := strconv.ParseUint(database.Stats()[key], 10, 64)
	if err != nil {
		b.Fatalf("counter %s: %v", key, err)
	}
	return value
}
