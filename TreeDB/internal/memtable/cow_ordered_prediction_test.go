package memtable

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWOrderedPredictionBoundDominatesExact(t *testing.T) {
	for _, shape := range []struct{ keys, depth, raw int }{{0, 0, 0}, {1, 1, 0}, {1, 33, 0}, {1, 1025, 0}, {80, 2, 0}, {1, 33, 80}} {
		t.Run(fmt.Sprintf("keys%d_depth%d_raw%d", shape.keys, shape.depth, shape.raw), func(t *testing.T) {
			budget, err := NewCOWBudget(DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			writer, err := NewCOWOrderedWriter(budget)
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			defer cowTestClose(writer)
			var seed []COWMutation
			for i := 0; i < shape.raw; i++ {
				m := cowOrderedStorageMutation(t, fmt.Sprintf("raw%03d", i), 7)
				m.MVCC = false
				seed = append(seed, m)
			}
			if len(seed) > 0 {
				root := cowTestPublish(t, writer, seed)
				defer cowTestRelease(root)
			}
			seed = nil
			for k := 0; k < shape.keys; k++ {
				for ts := 1; ts <= shape.depth; ts++ {
					seed = append(seed, cowOrderedStorageMutation(t, fmt.Sprintf("key%03d", k), uint64(ts)))
				}
			}
			if len(seed) > 0 {
				root := cowTestPublish(t, writer, seed)
				defer cowTestRelease(root)
			}
			batches := [][]COWMutation{{cowOrderedStorageMutation(t, "key000", math.MaxUint64)}}
			var group []COWMutation
			for _, key := range []string{"key000", "key001", "raw000", "raw079", "z\x00escaped"} {
				for _, ts := range []uint64{7, 1, math.MaxUint64, 7} {
					m := cowOrderedStorageMutation(t, key, ts)
					if ts == 1 {
						m.Flags, m.Value = node.FlagTombstone, nil
					}
					group = append(group, m)
				}
			}
			batches = append(batches, group)
			reverse := append([]COWMutation(nil), group...)
			for i, j := 0, len(reverse)-1; i < j; i, j = i+1, j-1 {
				reverse[i], reverse[j] = reverse[j], reverse[i]
			}
			batches = append(batches, reverse)
			for _, entries := range batches {
				before := budget.Stats()
				raw, ordered, nodes := writer.private, writer.ordered, writer.private.AllocationCharge()
				bound, ok := writer.OrderedPredictionUpperBound(entries)
				if !ok {
					t.Fatal("valid monotone qualified group declined")
				}
				exact, err := writer.Estimate(entries, COWPrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				for _, field := range []string{"Nodes", "Payload", "Wrappers", "Resources", "Extra", "Temporary"} {
					got, want := reflect.ValueOf(bound).FieldByName(field).Uint(), reflect.ValueOf(exact).FieldByName(field).Uint()
					if got < want {
						t.Fatalf("%s bound=%d below exact=%d; bound=%+v exact=%+v", field, got, want, bound, exact)
					}
				}
				if bound.Height < exact.Height || writer.pending != nil || raw != writer.private || ordered != writer.ordered || nodes != writer.private.AllocationCharge() || budget.Stats() != before {
					t.Fatal("prediction changed basis, receipt, budget, or height bound")
				}
			}
		})
	}
}

func TestCOWOrderedPredictionDeclinesWithoutChangingExactErrors(t *testing.T) {
	budget, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCOWOrderedWriter(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	defer cowTestClose(writer)
	m := cowOrderedStorageMutation(t, "a", 9)
	badKey, badTS, badFlags, raw, remove, pointer := m, m, m, m, m, m
	badKey.Key = []byte{0}
	badTS.Timestamp++
	badFlags.Flags = node.FlagPointer | node.FlagTombstone
	raw.MVCC = false
	remove.Remove = true
	pointer.Flags = node.FlagPointer
	for _, entries := range [][]COWMutation{nil, {badKey}, {badTS}, {badFlags, badKey}, {badKey, badFlags}, {raw}, {remove}, {pointer}, {m, cowOrderedStorageMutation(t, "z", 1), m}} {
		before, beforeErr := writer.Estimate(entries, COWPrepareOptions{})
		stats := budget.Stats()
		if bound, ok := writer.OrderedPredictionUpperBound(entries); ok || bound != (COWCharge{}) {
			t.Fatalf("unsupported group accepted: %+v", entries)
		}
		after, afterErr := writer.Estimate(entries, COWPrepareOptions{})
		if before != after || fmt.Sprint(beforeErr) != fmt.Sprint(afterErr) || budget.Stats() != stats {
			t.Fatal("decline altered exact result/error or budget")
		}
	}
	old := writer.ordered.maxHistory
	writer.ordered.maxHistory = int(^uint(0) >> 1)
	if _, ok := writer.OrderedPredictionUpperBound([]COWMutation{m}); ok {
		t.Fatal("overflowing history accepted")
	}
	writer.ordered.maxHistory = old
	if got := testing.AllocsPerRun(100, func() {
		if _, ok := writer.OrderedPredictionUpperBound([]COWMutation{m}); !ok {
			panic("eligible prediction declined")
		}
	}); got != 0 {
		t.Fatalf("positive prediction allocated %g objects", got)
	}
	p, err := writer.Prepare([]COWMutation{m}, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := writer.OrderedPredictionUpperBound([]COWMutation{m}); ok {
		t.Fatal("pending writer accepted prediction")
	}
	retirement := p.Cancel()
	retirement.Drain()
	cowTestClose(writer)
	if _, ok := writer.OrderedPredictionUpperBound([]COWMutation{m}); ok {
		t.Fatal("closed writer accepted prediction")
	}
}

func TestCOWOrderedPredictionAfterSparseEraseAndRegrowth(t *testing.T) {
	budget, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewCOWOrderedWriter(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	defer cowTestClose(w)
	var seed []COWMutation
	for i := 0; i < 128; i++ {
		m := cowOrderedStorageMutation(t, fmt.Sprintf("raw%03d", i), 7)
		m.MVCC = false
		seed = append(seed, m)
	}
	for ts := uint64(1); ts <= 129; ts++ {
		seed = append(seed, cowOrderedStorageMutation(t, "history", ts))
	}
	old := cowTestPublish(t, w, seed)
	defer cowTestRelease(old)
	var erase []COWMutation
	for i := 0; i < 127; i++ {
		m := cowOrderedStorageMutation(t, fmt.Sprintf("raw%03d", i), 7)
		m.MVCC, m.Remove = false, true
		erase = append(erase, m)
	}
	for ts := uint64(2); ts <= 129; ts++ {
		m := cowOrderedStorageMutation(t, "history", ts)
		m.Remove = true
		erase = append(erase, m)
	}
	sparse := cowTestPublish(t, w, erase)
	defer cowTestRelease(sparse)
	if w.private.Len() != 1 || w.maxEntries < 128 || w.ordered.maxHistory < 129 {
		t.Fatal("fixture did not retain historical capacity maxima after erasure")
	}
	entries := []COWMutation{cowOrderedStorageMutation(t, "raw127", math.MaxUint64), cowOrderedStorageMutation(t, "history", math.MaxUint64), cowOrderedStorageMutation(t, "history", 2), cowOrderedStorageMutation(t, "history", 1)}
	// Prefixes descend and the timestamp suffix exercises replacement,
	// backfill, promotion, full-width timestamps, and raw migration.
	for round := 0; round < 2; round++ {
		bound, ok := w.OrderedPredictionUpperBound(entries)
		exact, err := w.Estimate(entries, COWPrepareOptions{})
		if !ok || err != nil || bound.Nodes < exact.Nodes || bound.Payload < exact.Payload || bound.Wrappers < exact.Wrappers || bound.Temporary < exact.Temporary || bound.Height < exact.Height {
			t.Fatalf("sparse/regrown bound failed: ok=%v err=%v bound=%+v exact=%+v", ok, err, bound, exact)
		}
		root := cowTestPublish(t, w, entries)
		defer cowTestRelease(root)
	}
	if old.Len() != len(seed) || sparse.Len() != 2 {
		t.Fatal("prediction/regrowth changed held pre-erasure roots")
	}
}
