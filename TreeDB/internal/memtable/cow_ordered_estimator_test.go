package memtable

import (
	"fmt"
	"runtime"
	"testing"
)

func TestCOWOrderedHistorySingleInsertFitsIncrementalBudget(t *testing.T) {
	for _, kind := range []string{"append", "backfill"} {
		t.Run(kind, func(t *testing.T) {
			limits := DefaultCOWLimits()
			limits.MaxGenerationBytes = 384 << 10
			budget, err := NewCOWBudget(limits)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := NewCOWOrderedWriter(budget)
			if err != nil {
				t.Fatal(err)
			}
			var seed []COWMutation
			for version := uint64(1); version <= 64; version++ {
				seed = append(seed, cowOrderedStorageMutation(t, "shared", 2*version))
			}
			old := cowTestPublish(t, writer, seed)
			defer func() {
				cowTestRelease(old)
				cowTestClose(writer)
				budget.Close()
			}()
			timestamp := uint64(129)
			if kind == "backfill" {
				timestamp = 1
			}
			mutation := cowOrderedStorageMutation(t, "shared", timestamp)
			// The opening history remains owned. A single update must fit
			// this incremental generation envelope without a rollover.
			prepared, err := writer.Prepare([]COWMutation{mutation}, COWPrepareOptions{})
			if err != nil {
				t.Fatalf("single history insert refused despite bounded incremental storage: %v", err)
			}
			fresh := prepared.Publish()
			defer cowTestRelease(fresh)
			if old.Len() != 64 || fresh.Len() != 65 {
				t.Fatalf("history sizes old=%d new=%d", old.Len(), fresh.Len())
			}
			if _, ok := old.Get(mutation.Key); ok {
				t.Fatal("private insertion changed the retained opening history")
			}
			if record, ok := fresh.Get(mutation.Key); !ok || record.Value != string(mutation.Value) {
				t.Fatal("incremental insert lost its native value")
			}
		})
	}
}

func TestCOWOrderedHistoryIncrementalChargeCoversActualAllocations(t *testing.T) {
	// Promotion, cardinality height-bound changes and actual tree-height
	// transitions must remain covered after taking the per-operation cap.
	// Both strict maxima and interior backfills exercise Load and Set on
	// independently retained old roots.
	for _, count := range []int{1, 31, 62, 63, 64, 2046, 2047, 2079, 2080} {
		for _, kind := range []string{"append", "backfill"} {
			t.Run(fmt.Sprintf("%d/%s", count, kind), func(t *testing.T) {
				cowTestIsolateAllocationWitness(t)
				budget, err := NewCOWBudget(DefaultCOWLimits())
				if err != nil {
					t.Fatal(err)
				}
				writer, err := NewCOWOrderedWriter(budget)
				if err != nil {
					t.Fatal(err)
				}
				seed := make([]COWMutation, count)
				for version := range seed {
					seed[version] = cowOrderedStorageMutation(t, "shared", 2*uint64(version+1))
				}
				old := cowTestPublish(t, writer, seed)
				defer func() {
					cowTestRelease(old)
					cowTestClose(writer)
					budget.Close()
				}()
				timestamp := 2*uint64(count) + 1
				if kind == "backfill" {
					timestamp = 1
				}
				mutations := []COWMutation{cowOrderedStorageMutation(t, "shared", timestamp)}
				runtime.GC()
				accounting := budget.Stats()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				prepared, err := writer.Prepare(mutations, COWPrepareOptions{})
				runtime.ReadMemStats(&after)
				if err != nil {
					t.Fatal(err)
				}
				if allocated := after.TotalAlloc - before.TotalAlloc; allocated > prepared.Charge().Total() {
					t.Errorf("incremental history allocated %d bytes above admitted charge %+v", allocated, prepared.Charge())
				}
				retirement := prepared.Cancel()
				retirement.Drain()
				if got := budget.Stats(); got != accounting {
					// Peak is a high-water observation rather than retained ownership.
					got.PeakBytes = accounting.PeakBytes
					if got != accounting {
						t.Errorf("cancelled incremental postimage changed retained charges: %+v want %+v", got, accounting)
					}
				}
			})
		}
	}
}

func TestCOWOrderedHistoryUnroutedGapsPreserveCardinalityPromise(t *testing.T) {
	basis := newCOWOrderedTree()
	var contiguous, interrupted []COWMutation
	for version := uint64(1); version <= 64; version++ {
		mutation := cowOrderedStorageMutation(t, "shared", version)
		contiguous = append(contiguous, mutation)
		interrupted = append(interrupted, mutation, COWMutation{Key: []byte(fmt.Sprintf("raw-%03d", version)), Value: []byte("raw")})
	}
	plain, err := basis.estimate(contiguous)
	if err != nil {
		t.Fatal(err)
	}
	mixed, err := basis.estimate(interrupted)
	if err != nil {
		t.Fatal(err)
	}
	// The ordered work is identical: raw barriers cannot turn a growing
	// 64-version history into 64 fresh singleton estimates. The fallback must
	// retain at least the isolated history's allocation promise.
	if mixed.Nodes < plain.Nodes {
		t.Fatalf("unrouted gaps reduced the same ordered history promise: mixed=%+v contiguous=%+v", mixed, plain)
	}
	private := basis.fork()
	private.applyFrom(basis, interrupted)
	if private.len() != len(contiguous) || private.maxHistory != len(contiguous) || private.keys.Len() != 1 {
		t.Fatalf("interrupted history cardinality=%d maximum=%d keys=%d", private.len(), private.maxHistory, private.keys.Len())
	}
	for _, mutation := range contiguous {
		if record, ok := private.get(mutation.Key); !ok || record.Value != string(mutation.Value) {
			t.Fatal("interrupted history lost a version")
		}
	}
}

func TestCOWOrderedHistoryMixedRunsFitIncrementalBudget(t *testing.T) {
	for _, descending := range []bool{false, true} {
		t.Run(fmt.Sprintf("descending=%t", descending), func(t *testing.T) {
			cowTestIsolateAllocationWitness(t)
			budget, writer := cowOrderedLifecycleWriter(t)
			defer func() {
				cowTestClose(writer)
				budget.Close()
			}()
			var mutations []COWMutation
			for position := 0; position < 12; position++ {
				key := position
				if descending {
					key = 11 - position
				}
				for version := uint64(1); version <= 3; version++ {
					mutations = append(mutations,
						cowOrderedStorageMutation(t, fmt.Sprintf("key-%02d", key), version),
						COWMutation{Key: []byte(fmt.Sprintf("raw-%02d-%d", key, version)), Value: []byte("raw")})
				}
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			prepared, err := writer.Prepare(mutations, COWPrepareOptions{})
			runtime.ReadMemStats(&after)
			if err != nil {
				t.Fatalf("small mixed history batch exhausted its generation: %v", err)
			}
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > prepared.Charge().Total() {
				t.Errorf("mixed history allocated %d bytes above admitted charge %+v", allocated, prepared.Charge())
			}
			root := prepared.Publish()
			defer cowTestRelease(root)
			if root.Len() != len(mutations) || root.ordered.len() != 36 || root.ordered.keys.Len() != 12 {
				t.Fatalf("mixed batch lost projected entries: total=%d histories=%d keys=%d", root.Len(), root.ordered.len(), root.ordered.keys.Len())
			}
			for _, mutation := range mutations {
				if record, ok := root.Get(mutation.Key); !ok || record.Value != string(mutation.Value) {
					t.Fatal("mixed batch lost its native or raw value")
				}
			}
		})
	}
}
