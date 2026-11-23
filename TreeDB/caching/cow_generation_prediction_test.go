package caching

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// These operands can change routing within the same group. Even an unqualified
// entry after an out-of-order prefix must exclude the entire group from sorting.
func TestCOWMutationOrderingPreservesOrderDependentGroups(t *testing.T) {
	base := []memtable.COWMutation{
		{Key: []byte("z"), Value: []byte("first"), MVCC: true, Revision: 9},
		{Key: []byte("a"), Value: []byte("second"), MVCC: true, Revision: 3},
		{Key: []byte("m"), Value: []byte("third"), MVCC: true, Revision: 5},
	}
	for _, test := range []struct {
		name    string
		ordered bool
		mutate  func([]memtable.COWMutation)
	}{
		{"physical_mode", false, func([]memtable.COWMutation) {}},
		{"mixed_raw_tail", true, func(ms []memtable.COWMutation) { ms[2].MVCC = false }},
		{"raw_group", true, func(ms []memtable.COWMutation) {
			for i := range ms {
				ms[i].MVCC = false
			}
		}},
		{"physical_remove_tail", true, func(ms []memtable.COWMutation) { ms[2].Remove = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			group := append([]memtable.COWMutation(nil), base...)
			test.mutate(group)
			original := append([]memtable.COWMutation(nil), group...)
			cache := cowCache{ordered: test.ordered}
			cache.orderQualifiedMutationGroups([][]memtable.COWMutation{group})
			if !reflect.DeepEqual(group, original) {
				t.Fatal("order-dependent group was changed")
			}
		})
	}
}

func TestCOWOrderedPredictionPositiveAndPressureFallback(t *testing.T) {
	db, _ := cowOrderedLifecycleFixture(t)
	b := db.NewBatchWithSize(1)
	defer b.Close()
	key := cowOrderedLifecycleKey(t, "prediction", 9)
	groups := make([][]memtable.COWMutation, len(db.cow.writers))
	i := db.shardIndex(key)
	groups[i] = []memtable.COWMutation{{Key: key, Value: []byte("value"), MVCC: true, Timestamp: 9, Flags: node.FlagInline}}
	before := db.cow.budget.Stats()
	db.cow.writerMu.Lock()
	defer db.cow.writerMu.Unlock()
	if !b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("ample-headroom qualified inline group did not activate")
	}
	if db.cow.budget.Stats() != before {
		t.Fatal("positive prediction reserved or changed memory")
	}
	limits := db.cow.budget.Limits()
	bound, ok := db.cow.writers[i].OrderedPredictionUpperBound(groups[i])
	if !ok {
		t.Fatal("missing per-writer bound")
	}
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	remaining := limits.MaxInFlightBytes - before.ReservedBytes
	if remaining <= header+bound.Total() {
		t.Fatal("fixture lacks headroom")
	}
	pressure, err := db.cow.budget.AcquireExternal(remaining - header - bound.Total()/2)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	if b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("saturated shared frontier accepted loose prediction")
	}
	pressure.Close()
	if !b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("released pressure failed to restore eligibility")
	}
	for _, mutate := range []func(*memtable.COWMutation){
		func(m *memtable.COWMutation) { m.MVCC = false },
		func(m *memtable.COWMutation) { m.Remove = true },
		func(m *memtable.COWMutation) { m.Flags = node.FlagPointer },
		func(m *memtable.COWMutation) { m.Timestamp++ },
	} {
		original := groups[i][0]
		mutate(&groups[i][0])
		if b.cowPredictionCanSkipEstimate(groups) {
			t.Fatal("unsupported group skipped exact prediction")
		}
		groups[i][0] = original
	}
	db.mutableBytes.Store(db.mutableFlushThreshold() + 1)
	if b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("mutable flush threshold skipped")
	}
	db.mutableBytes.Store(0)
}

func TestCOWRawBatchPredictionCanSkipEstimateAndFallback(t *testing.T) {
	db := cowPointerFixture(t)
	b := db.NewBatchWithSize(4)
	defer b.Close()

	groups := make([][]memtable.COWMutation, len(db.cow.writers))
	for i := 0; i < 4; i++ {
		key := []byte{byte('a' + i), byte(i)}
		s := db.shardIndex(key)
		m := memtable.COWMutation{Key: key, Value: []byte("val"), Flags: node.FlagInline}
		if i == 3 {
			m.Flags = node.FlagTombstone
			m.Value = nil
		}
		groups[s] = append(groups[s], m)
	}

	before := db.cow.budget.Stats()
	db.cow.writerMu.Lock()
	defer db.cow.writerMu.Unlock()
	if !b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("ample-headroom raw inline/tombstone batch did not skip Estimate")
	}
	if db.cow.budget.Stats() != before {
		t.Fatal("raw prediction changed budget stats")
	}

	var boundTotal uint64
	for i, group := range groups {
		if len(group) == 0 {
			continue
		}
		bound, ok := db.cow.writers[i].RawBatchPredictionUpperBound(group)
		if !ok {
			t.Fatalf("shard %d RawBatchPredictionUpperBound returned false", i)
		}
		boundTotal += bound.Total()
	}
	limits := db.cow.budget.Limits()
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	remaining := limits.MaxInFlightBytes - before.ReservedBytes
	if remaining <= header+boundTotal {
		t.Fatal("fixture lacks headroom")
	}
	pressure, err := db.cow.budget.AcquireExternal(remaining - header - boundTotal/2)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	if b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("saturated shared frontier accepted raw loose prediction")
	}
	pressure.Close()
	if !b.cowPredictionCanSkipEstimate(groups) {
		t.Fatal("released pressure failed to restore raw eligibility")
	}

	// Find a non-empty shard and verify unsupported flags/remove fall back.
	var shard int
	for i, g := range groups {
		if len(g) > 0 {
			shard = i
			break
		}
	}
	orig := groups[shard][0]
	for _, mutate := range []func(*memtable.COWMutation){
		func(m *memtable.COWMutation) { m.Remove = true },
		func(m *memtable.COWMutation) { m.Flags = node.FlagPointer },
	} {
		mutate(&groups[shard][0])
		if b.cowPredictionCanSkipEstimate(groups) {
			t.Fatal("unsupported raw mutation skipped exact prediction")
		}
		groups[shard][0] = orig
	}
}
