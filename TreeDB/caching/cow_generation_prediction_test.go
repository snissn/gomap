package caching

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

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
