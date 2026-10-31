package caching

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
)

// Keep the reference independent of the optimized key table, including spelling
// and ordering of every category and metric in the original emitter.
func referenceMemtableResidencyStats(prefix string, summary memtableResidencyBreakdown) map[string]string {
	out := make(map[string]string)
	if prefix == "" {
		return out
	}
	for _, category := range []struct {
		name    string
		summary memtableResidencySummary
	}{
		{"total", summary.total}, {"skiplist", summary.skiplist},
		{"hash_sorted", summary.hashSorted}, {"btree", summary.btree}, {"append_only", summary.appendOnly},
	} {
		s := category.summary
		for _, metric := range []struct {
			name  string
			value int64
		}{
			{"count", int64(s.count)}, {"entries", int64(s.len)}, {"size_bytes", s.size},
			{"entry_capacity", s.entryCapacity}, {"entry_backing_bytes", s.entryBackingBytes},
			{"value_arena_active_chunks", s.valueArenaActiveChunks}, {"value_arena_active_bytes", s.valueArenaActiveBytes},
			{"value_arena_retained_chunks", s.valueArenaRetainedChunks}, {"value_arena_retained_bytes", s.valueArenaRetainedBytes},
		} {
			out[prefix+"."+category.name+"."+metric.name] = fmt.Sprintf("%d", metric.value)
		}
	}
	return out
}

func TestMemtableResidencyStatsKeysParityAndOwnership(t *testing.T) {
	prefixes := []string{
		"treedb.cache.memtable_residency.mutable", "treedb.cache.memtable_residency.queue",
		"treedb.process.memtable_residency.mutable", "treedb.process.memtable_residency.queue",
		"custom", "custom.λ/\x00", "treedb.cache.memtable_residency.mutable.extra", "",
	}
	edge := memtableResidencySummary{count: -1, len: int(^uint(0) >> 1), size: math.MinInt64,
		entryCapacity: math.MaxInt64, entryBackingBytes: -2, valueArenaActiveChunks: 3,
		valueArenaActiveBytes: -4, valueArenaRetainedChunks: 5, valueArenaRetainedBytes: -6}
	summary := memtableResidencyBreakdown{total: edge, skiplist: memtableResidencySummary{count: 7},
		hashSorted: memtableResidencySummary{len: 8}, btree: memtableResidencySummary{size: 9},
		appendOnly: memtableResidencySummary{entryCapacity: 10}}
	for _, prefix := range prefixes {
		t.Run(prefix, func(t *testing.T) {
			addMemtableResidencyStats(nil, prefix, summary)
			for _, snapshot := range []memtableResidencyBreakdown{{}, summary, {}} {
				got := map[string]string{"unrelated": "sentinel"}
				addMemtableResidencyStats(got, prefix, summary)
				addMemtableResidencyStats(got, prefix, snapshot)
				if got["unrelated"] != "sentinel" {
					t.Fatal("emission changed unrelated caller entry")
				}
				delete(got, "unrelated")
				if want := referenceMemtableResidencyStats(prefix, snapshot); !reflect.DeepEqual(got, want) {
					t.Fatalf("stats mismatch: got %v, want %v", got, want)
				}
			}
			original := make(map[string]string)
			addMemtableResidencyStats(original, prefix, summary)
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got := make(map[string]string)
					addMemtableResidencyStats(got, prefix, summary)
					for k := range got {
						got[k] = "caller mutation"
						delete(got, k)
					}
				}()
			}
			wg.Wait()
			if want := referenceMemtableResidencyStats(prefix, summary); !reflect.DeepEqual(original, want) {
				t.Fatal("independent caller mutation changed prior snapshot")
			}
		})
	}
}

func TestMemtableResidencyStatsFixedPrefixesReuseKeys(t *testing.T) {
	for _, prefix := range []string{
		"treedb.cache.memtable_residency.mutable", "treedb.cache.memtable_residency.queue",
		"treedb.process.memtable_residency.mutable", "treedb.process.memtable_residency.queue",
	} {
		t.Run(prefix, func(t *testing.T) {
			// Measure key selection alone so the allocation contract does not depend
			// on numeric formatting or the runtime's map implementation.
			var keys [5]memtableResidencyKeys
			if allocs := testing.AllocsPerRun(20, func() {
				keys = memtableResidencyMetricKeys(prefix)
			}); allocs != 0 {
				t.Fatalf("fixed-prefix key selection allocated %.0f times; want zero", allocs)
			}
			if keys[0].count != prefix+".total.count" || keys[4].valueArenaRetainedBytes != prefix+".append_only.value_arena_retained_bytes" {
				t.Fatal("key selection returned unexpected boundary keys")
			}
		})
	}
}
