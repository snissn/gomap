package memtable

import (
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWOrderedMonotoneProbeRetainedHistoryOracle(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	t.Cleanup(func() { cowTestClose(writer); budget.Close() })
	mutation := func(ts uint64, value string, revision page.EntryRevision) COWMutation {
		m := cowOrderedLifecycleMutation(t, "key\x00suffix", ts, value)
		m.Revision = revision
		return m
	}
	erase := func(ts uint64) COWMutation {
		m := mutation(ts, "", 0)
		m.Remove = true
		return m
	}
	tombstone := mutation(75, "", 3)
	tombstone.Flags = node.FlagTombstone
	groups := [][]COWMutation{
		{mutation(50, "first", math.MaxUint64)},
		{mutation(100, "monotone", 1)},
		{mutation(75, "backfill", 90), mutation(100, "replace max", 2)},
		{erase(101), erase(100), mutation(60, "backfill below new max", 4)},
		{tombstone, mutation(math.MaxUint64, "full timestamp", 5)},
		{erase(math.MaxUint64), erase(75)},
		{erase(60)}, // collapse to the surviving singleton at 50
		{erase(50), erase(50), mutation(2, "reinsert after empty", 800), mutation(1, "older TS", 900)},
		{erase(2), mutation(2, "last wins", 6), mutation(3, "monotone after repromotion", 7)},
	}
	type snapshot struct {
		root *COWRoot
		want map[string]COWMutation
	}
	var snapshots []snapshot
	allKeys := make(map[string][]byte)
	current := make(map[string]COWMutation)
	for groupIndex, group := range groups {
		for _, m := range group {
			key := string(m.Key)
			allKeys[key] = m.Key
			if m.Remove {
				delete(current, key)
			} else {
				current[key] = m
			}
		}
		root := cowTestPublish(t, writer, group)
		t.Cleanup(func() { cowTestRelease(root) })
		want := make(map[string]COWMutation, len(current))
		for key, m := range current {
			want[key] = m
		}
		snapshots = append(snapshots, snapshot{root, want})
		// Recheck every retained root after each mutation group, including
		// newer misses that later become a maximum and then disappear again.
		for cutIndex, cut := range snapshots {
			for key, physical := range allKeys {
				got, found := cut.root.Get(physical)
				m, present := cut.want[key]
				if found != present || (found && (got.Value != string(m.Value) || got.Flags != m.Flags || got.Revision != m.Revision)) {
					t.Fatalf("group %d retained cut %d key %x: got %+v/%v want %+v/%v", groupIndex, cutIndex, key, got, found, m, present)
				}
			}
		}
	}
}

func TestCOWOrderedMonotoneProbeMaximumAfterErase(t *testing.T) {
	low := cowOrderedStorageMutation(t, "history", 10)
	high := cowOrderedStorageMutation(t, "history", 100)
	basis := cowOrderedStorageBuild(t, []COWMutation{low, high})
	prefix, _, _ := cowOrderedPrefix(high.Key)
	history, ok := cowOrderedPrimaryGet(basis.keys, prefix)
	if !ok || history.versions == nil {
		t.Fatal("fixture did not promote the history")
	}
	if _, ok := history.exact(math.MaxUint64); ok {
		t.Fatal("timestamp beyond the numeric history maximum was present")
	}
	erase := high
	erase.Remove = true
	private := basis.fork()
	private.applyFrom(basis, []COWMutation{erase})
	current := private.immutableCopy()
	remaining, ok := cowOrderedPrimaryGet(current.keys, prefix)
	if !ok || remaining.maximum != 10 || remaining.versions != nil {
		t.Fatal("maximum deletion did not collapse to the exact singleton")
	}
	if _, ok := remaining.exact(100); ok {
		t.Fatal("removed maximum remained present")
	}
	if _, ok := history.exact(100); !ok {
		t.Fatal("maximum deletion changed the retained numeric history")
	}
	// Reinsert above the reduced maximum and then replace it in one group:
	// PUT cardinality comes from set's replacement result, without a preprobe.
	replacement := high
	replacement.Value = []byte("replacement")
	private = current.fork()
	private.applyFrom(current, []COWMutation{high, replacement})
	repromoted := private.immutableCopy()
	if repromoted.len() != 2 {
		t.Fatalf("reinsert/replacement changed distinct-version count: %d", repromoted.len())
	}
	if got, ok := repromoted.get(high.Key); !ok || got.Value != "replacement" {
		t.Fatal("reinsert/replacement failed", got, ok)
	}
}
