package treedb_test

import (
	"fmt"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// This distinct generation-dimension discriminator keeps one historical view
// alive at the drain assertion. Its source cap still has one unused slot.
func TestPublicCOWRetainedGenerationPressureAcceptedPrefixDrain(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode, opts.MemtableShards = "cow_btree", 1
			opts.FlushThreshold = 1 << 30
			opts.COWMemtableLimits = treedb.DefaultCOWMemtableLimits()
			opts.COWMemtableLimits.MaxSources = 2
			opts.COWMemtableLimits.MaxGenerations = 2
			opts.COWMemtableLimits.MaxViews = 4
			t.Logf("declared limits=%+v shards=%d flush_threshold=%d", opts.COWMemtableLimits, opts.MemtableShards, opts.FlushThreshold)
			database, err := treedb.Open(opts)
			if err != nil {
				t.Fatalf("fixture open: %v", err)
			}
			var held []treedb.Snapshot
			inventory := func(phase string) map[string]string {
				stats := database.Stats()
				keys := []string{
					"treedb.cache.cow.txn.logical_seq", "treedb.command_wal.next_lsn",
					"treedb.command_wal.live_accepted_max_lsn", "treedb.command_wal.applied_lsn",
					"treedb.command_wal.durable_wal_lsn", "treedb.durable_root.commit_seq",
					"treedb.root_page", "treedb.system_root_page",
					"treedb.cache.cow.sources", "treedb.cache.cow.views", "treedb.cache.cow.generations",
					"treedb.cache.cow.current_roots", "treedb.cache.cow.frozen_roots", "treedb.cache.cow.active_cuts",
					"treedb.cache.cow.publications_total", "treedb.cache.cow.rollovers_total", "treedb.cache.cow.handoffs_total",
					"treedb.cache.cow.total_bytes", "treedb.cache.cow.reserved_bytes", "treedb.cache.cow.retired_bytes",
					"treedb.cache.cow.maintenance_capacity_bytes", "treedb.cache.cow.maintenance_used_bytes",
				}
				for _, key := range keys {
					t.Logf("phase=%s %s=%s", phase, key, stats[key])
				}
				return stats
			}
			t.Cleanup(func() {
				t.Log("TEARDOWN ONLY: releasing original views; no drain progress is claimed after this point")
				for i, view := range held {
					if err := view.Close(); err != nil {
						t.Errorf("teardown view %d: %v", i+1, err)
					}
				}
				held = nil
				inventory("teardown_views_released")
				if err := database.Close(); err != nil {
					t.Errorf("teardown Close: %v", err)
				}
			})
			inventory("opened")
			for version := 1; version <= 1; version++ {
				value := fmt.Sprintf("v%d", version)
				if err := database.Set([]byte("key"), []byte(value)); err != nil {
					t.Fatalf("fixture write %s: %v", value, err)
				}
				inventory(value + "_accepted")
				view := database.AcquireSnapshot()
				if view == nil {
					t.Fatalf("fixture view %s refused", value)
				}
				held = append(held, view)
				inventory(value + "_view_held")
				if err := database.Checkpoint(); err != nil {
					t.Fatalf("fixture checkpoint %s: %v", value, err)
				}
				stats := inventory(value + "_sealed")
				if got := stats["treedb.cache.cow.sources"]; got != strconv.Itoa(version) {
					t.Fatalf("fixture generation pressure setup source count unexpected: got %s want %d", got, version)
				}
			}
			before := inventory("generation_cap_before_v2")
			if before["treedb.cache.cow.generations"] != "2" || before["treedb.cache.cow.sources"] != "1" || before["treedb.cache.cow.views"] != "1" {
				t.Fatal("fixture did not establish exactly two generations, one source and one view")
			}
			writeErr := database.Set([]byte("key"), []byte("v2"))
			afterWrite := inventory("v2_write_returned")
			t.Logf("v2 write result=%v", writeErr)
			if writeErr != nil {
				for _, key := range []string{"treedb.cache.cow.txn.logical_seq", "treedb.command_wal.next_lsn", "treedb.command_wal.live_accepted_max_lsn", "treedb.command_wal.applied_lsn", "treedb.cache.cow.publications_total"} {
					if before[key] != afterWrite[key] {
						t.Errorf("refused v2 changed %s: %s -> %s", key, before[key], afterWrite[key])
					}
				}
				t.Log("OUTCOME: v2 refused before logical/canonical advancement")
			} else {
				checkpointErr := database.Checkpoint()
				inventory("v2_checkpoint_returned_views_still_held")
				t.Logf("v2 checkpoint result=%v held_views=%d", checkpointErr, len(held))
				if checkpointErr != nil {
					t.Errorf("OUTCOME: accepted v2 cannot drain while the original view remains held: %v", checkpointErr)
				} else {
					t.Log("OUTCOME: accepted v2 physically drained with the original view held")
				}
			}
			for i, view := range held {
				want := fmt.Sprintf("v%d", i+1)
				if got, err := view.Get([]byte("key")); err != nil || string(got) != want {
					t.Errorf("original view %d value=%q error=%v want=%q", i+1, got, err, want)
				}
			}
			want := "v2"
			if writeErr != nil {
				want = "v1"
			}
			if got, err := database.Get([]byte("key")); err != nil || string(got) != want {
				t.Errorf("latest value=%q error=%v want=%q", got, err, want)
			}
		})
	}
}
