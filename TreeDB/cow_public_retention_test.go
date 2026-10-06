package treedb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// This deliberately exhausts generation slots retained by real public cuts.
// The byte limits are fixed before the workload; no admission limit is widened
// after refusal. Checkpoint creates and drains the actual frozen-source prefix.
func TestCOWPublicRetentionRefusalDrainAndResume(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			limits := DefaultCOWMemtableLimits()
			limits.MaxViews, limits.MaxGenerations = 8, 6
			limits.MaxSources, limits.MaxResources = 6, 32
			limits.MaxGenerationBytes = 1 << 20
			database, opts, blocked := cowPublicContractOpen(t, profile, func(opts *Options) {
				opts.COWMemtableLimits = limits
				opts.ValueLog.ForcePointers = false
				opts.ValueLog.PointerThreshold = 1 << 30
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				opts.BackgroundIndexVacuumInterval = -1
				opts.MaxWALBytes = -1
				opts.DisableBackgroundPrune = true
			})
			cache := database.cached
			keys := [][]byte{[]byte("a"), []byte("d")}
			value := func(n uint64) []byte {
				result := bytes.Repeat([]byte{0x4a}, 512)
				binary.LittleEndian.PutUint64(result, n)
				return result
			}
			current := [][]byte{value(0), value(0)}
			for _, key := range keys {
				if err := database.Set(key, value(0)); err != nil {
					t.Fatal(err)
				}
			}
			var pinned []Snapshot
			t.Cleanup(func() {
				for _, snapshot := range pinned {
					_ = snapshot.Close()
				}
			})
			for stage := 0; stage < 2; stage++ {
				snapshot := database.AcquireSnapshot()
				if snapshot == nil {
					t.Fatal("predecessor capture refused")
				}
				pinned = append(pinned, snapshot)
				for _, key := range keys {
					if got, err := snapshot.Get(key); err != nil || !bytes.Equal(got, value(uint64(stage))) {
						t.Fatalf("pinned stage%d key%q len%d err%v", stage, key, len(got), err)
					}
				}
				if err := cowPublicContractComplete(t, blocked, "retained-cut checkpoint", database.Checkpoint); err != nil {
					t.Fatal(err)
				}
				for i, key := range keys {
					current[i] = value(uint64(stage + 1))
					if err := database.Set(key, current[i]); err != nil {
						t.Fatal(err)
					}
				}
			}
			if got := cache.COWMemoryStats().Generations; got != 6 {
				t.Fatalf("two whole predecessor cuts plus current generations=%d, want6", got)
			}
			var refusedKey, refusedValue []byte
			var before map[string]string
			accepted := 0
			for i := 0; i < 4096; i++ {
				keyIndex := i % len(keys)
				candidate := value(uint64(i + 3))
				before = database.Stats()
				err := database.Set(keys[keyIndex], candidate)
				if errors.Is(err, memtable.ErrCOWCapacity) {
					refusedKey, refusedValue = keys[keyIndex], candidate
					break
				}
				if err != nil {
					t.Fatalf("update%d: %v", i, err)
				}
				accepted++
				current[keyIndex] = candidate
				cowPublicRetentionBound(t, cache.COWMemoryStats(), limits)
			}
			if refusedKey == nil || accepted == 0 {
				t.Fatalf("expected progress then finite rollover refusal; accepted%d", accepted)
			}
			plateau := cache.COWMemoryStats()
			cowPublicRetentionBound(t, plateau, limits)
			after := database.Stats()
			for _, key := range []string{"treedb.cache.cow.publications_total", "treedb.cache.cow.rollovers_total"} {
				if before[key] == "" || before[key] != after[key] {
					t.Fatalf("refusal changed %s: %q -> %q", key, before[key], after[key])
				}
			}
			if profile != ProfileNoWALFast {
				for _, key := range []string{"treedb.command_wal.max_lsn", "treedb.command_wal.bytes"} {
					if before[key] == "" || before[key] != after[key] {
						t.Fatalf("refusal appended WAL %s: %q -> %q", key, before[key], after[key])
					}
				}
			}
			for attempt := 0; attempt < 8; attempt++ {
				if err := database.Set(refusedKey, refusedValue); !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("plateau retry%d: %v", attempt, err)
				}
				got := cache.COWMemoryStats()
				cowPublicRetentionBound(t, got, limits)
				got.PeakBytes = plateau.PeakBytes
				if got != plateau {
					t.Fatalf("refusal retained new charges: before%+v after%+v", plateau, got)
				}
			}
			for i, key := range keys {
				if got, err := database.Get(key); err != nil || !bytes.Equal(got, current[i]) {
					t.Fatalf("refusal changed current key%q err%v", key, err)
				}
				for stage, snapshot := range pinned {
					if got, err := snapshot.Get(key); err != nil || !bytes.Equal(got, value(uint64(stage))) {
						t.Fatalf("pressure invalidated predecessor%d key%q err%v", stage, key, err)
					}
				}
			}
			for _, snapshot := range pinned {
				if err := snapshot.Close(); err != nil {
					t.Fatal(err)
				}
			}
			pinned = nil
			drained := cache.COWMemoryStats()
			if drained.Generations >= plateau.Generations || drained.TotalBytes >= plateau.TotalBytes {
				t.Fatalf("predecessor release did not drain: plateau%+v drained%+v", plateau, drained)
			}
			if err := cowPublicContractComplete(t, blocked, "post-drain checkpoint", database.Checkpoint); err != nil {
				t.Fatal(err)
			}
			if err := database.Set(refusedKey, refusedValue); err != nil {
				t.Fatalf("identical refused write did not resume: %v", err)
			}
			for i, key := range keys {
				if bytes.Equal(key, refusedKey) {
					current[i] = refusedValue
				}
			}
			resumed := cache.COWMemoryStats()
			cowPublicRetentionBound(t, resumed, limits)
			if err := cowPublicContractComplete(t, blocked, "public close after drain", database.Close); err != nil {
				t.Fatal(err)
			}
			closed := cache.COWMemoryStats()
			zero := closed
			zero.PeakBytes = 0
			if zero != (memtable.COWStats{}) {
				t.Fatalf("closed integration retained charges: %+v", closed)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for i, key := range keys {
				if got, err := reopened.Get(key); err != nil || !bytes.Equal(got, current[i]) {
					t.Fatalf("reopen key%q err%v", key, err)
				}
			}
			packet, err := json.Marshal(map[string]any{"accepted_updates": accepted, "plateau": plateau, "drained": drained, "resumed": resumed, "closed": closed, "limits": limits})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("COW_PUBLIC_RETENTION %s", packet)
		})
	}
}

func cowPublicRetentionBound(t *testing.T, stats memtable.COWStats, limits COWMemtableLimits) {
	t.Helper()
	if stats.TotalBytes > limits.MaxTotalBytes || stats.PeakBytes > limits.MaxTotalBytes ||
		stats.ReservedBytes > limits.MaxInFlightBytes || stats.RetiredBytes > limits.MaxRetiredBytes ||
		stats.Views > limits.MaxViews || stats.Generations > limits.MaxGenerations || stats.Sources > limits.MaxSources {
		t.Fatal(fmt.Sprintf("integration admission exceeded fixed limits: stats%+v limits%+v", stats, limits))
	}
}
