package caching

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

// Retain the former emitter as an independent schema/value oracle.
func appendCheckpointStageStatsReference(stats map[string]string, name string, s *checkpointStageStats) {
	if stats == nil || s == nil || name == "" {
		return
	}
	prefix := "treedb.cache.checkpoint.stage." + name
	stats[prefix+".samples"] = fmt.Sprintf("%d", s.samples.Load())
	stats[prefix+".last_ns"] = fmt.Sprintf("%d", s.lastNs.Load())
	stats[prefix+".total_ns"] = fmt.Sprintf("%d", s.totalNs.Load())
	stats[prefix+".max_ns"] = fmt.Sprintf("%d", s.maxNs.Load())
}

func TestCheckpointStageStatsKeysParityAndLiveOwnership(t *testing.T) {
	names := []string{
		"cutover", "wal_rotate", "value_log_flush", "command_wal_publish",
		"command_wal_cleanup", "command_wal_prefix_close", "command_wal_post_release_cleanup",
		"auto_critical_section", "flush_all", "leaf_value_log_sync", "reducer_publish",
		"backend_boundary", "wal_cleanup", "post_maintenance", "future.stage/未知", "",
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			var stage checkpointStageStats
			for _, value := range []uint64{0, 123456789, math.MaxUint64} {
				stage.samples.Store(value)
				stage.lastNs.Store(value / 2)
				stage.totalNs.Store(value / 3)
				stage.maxNs.Store(value / 4)
				got, want := map[string]string{"unrelated": "owned"}, map[string]string{"unrelated": "owned"}
				appendCheckpointStageStatsReference(want, name, &stage)
				appendCheckpointStageStats(got, name, &stage)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("value %d: got %v, want %v", value, got, want)
				}
				// A second emission must reload counters and must not share the
				// first caller's mutated map or remember its removed entries.
				for key := range got {
					delete(got, key)
				}
				got["caller-only"] = "sentinel"
				stage.samples.Store(value ^ 1)
				next, reference := map[string]string{}, map[string]string{}
				appendCheckpointStageStats(next, name, &stage)
				appendCheckpointStageStatsReference(reference, name, &stage)
				if !reflect.DeepEqual(next, reference) || got["caller-only"] != "sentinel" {
					t.Fatalf("live/caller ownership mismatch: got %v, want %v", next, reference)
				}
			}
		})
	}
	var stage checkpointStageStats
	appendCheckpointStageStats(nil, "cutover", &stage)
	unchanged := map[string]string{"sentinel": "present"}
	appendCheckpointStageStats(unchanged, "cutover", nil)
	if !reflect.DeepEqual(unchanged, map[string]string{"sentinel": "present"}) {
		t.Fatal("nil stage changed the output")
	}
}

func TestCheckpointStageStatsKeysConcurrentLiveCounters(t *testing.T) {
	var stage checkpointStageStats
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			stage.samples.Add(1)
			stage.lastNs.Store(uint64(i))
			stage.totalNs.Add(uint64(i))
			stage.maxNs.Store(uint64(i))
		}
	}()
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				out := map[string]string{"sentinel": "present"}
				appendCheckpointStageStats(out, "cutover", &stage)
				if len(out) != 5 || out["sentinel"] != "present" {
					t.Errorf("incomplete or shared output: %v", out)
					return
				}
				for _, field := range []string{"samples", "last_ns", "total_ns", "max_ns"} {
					if _, err := strconv.ParseUint(out["treedb.cache.checkpoint.stage.cutover."+field], 10, 64); err != nil {
						t.Errorf("invalid live %s: %v", field, err)
					}
				}
			}
		}()
	}
	wg.Wait()
	out := map[string]string{}
	appendCheckpointStageStats(out, "cutover", &stage)
	if out["treedb.cache.checkpoint.stage.cutover.samples"] != "1000" {
		t.Fatalf("final counter was not refreshed: %v", out)
	}
}
