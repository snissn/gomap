package memtable

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWLogicalDeltaPhysicalCensusAndCancellation(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordered_%t", ordered), func(t *testing.T) {
			budget, err := NewCOWBudget(DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			var writer *COWWriter
			if ordered {
				writer, err = NewCOWOrderedWriter(budget)
			} else {
				writer, err = NewCOWWriter(budget)
			}
			if err != nil {
				t.Fatal(err)
			}
			var roots []*COWRoot
			t.Cleanup(func() {
				for _, root := range roots {
					cowTestRelease(root)
				}
				cowTestClose(writer)
				budget.Close()
				if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 {
					t.Fatalf("retained preparation accounting: %+v", stats)
				}
			})
			census := func(root *COWRoot) (size int64) {
				seen := make(map[string]struct{})
				root.ScanOwned(func(record COWRecord) bool {
					if _, duplicate := seen[record.Key]; duplicate {
						t.Fatal("raw/ordered physical key overlap")
					}
					seen[record.Key] = struct{}{}
					size += int64(len(record.Key))
					switch {
					case record.Flags&node.FlagPointer != 0:
						size += page.ValuePtrSize + int64(len(record.Value))
					case record.Flags&node.FlagTombstone == 0:
						size += int64(len(record.Value))
					}
					return true
				})
				if len(seen) != root.Len() {
					t.Fatal("raw/ordered physical overlap hidden by the compatibility merge")
				}
				return size
			}
			rng := rand.New(rand.NewSource(639))
			var size int64
			for batch := 0; batch < 32; batch++ {
				group := make([]COWMutation, 12)
				for i := range group {
					m := cowOrderedStorageMutation(t, fmt.Sprintf("key-%d", rng.Intn(4)), uint64(rng.Intn(16)+1))
					m.Value = make([]byte, rng.Intn(128))
					m.Flags = []byte{node.FlagInline, node.FlagPointer, node.FlagTombstone}[rng.Intn(3)]
					m.Ptr = page.ValuePtr{FileID: 3, Offset: 17, Length: 43}
					m.Remove = rng.Intn(5) == 0
					if rng.Intn(3) == 0 {
						m.MVCC, m.Timestamp = false, 0
					}
					if i == 3 {
						// Malformed reserved-namespace/raw keys remain compatible.
						m.Key, m.MVCC, m.Timestamp = []byte{0, 1, 0xff, byte(batch % 3)}, false, 0
					}
					group[i] = m
				}
				if batch%4 == 0 {
					before := budget.Stats()
					charge, err := writer.Estimate(group, COWPrepareOptions{})
					if err != nil {
						t.Fatal(err)
					}
					limit := budget.limits.MaxInFlightBytes
					budget.limits.MaxInFlightBytes = charge.Total() - 1
					failed, err := writer.Prepare(group, COWPrepareOptions{})
					budget.limits.MaxInFlightBytes = limit
					if failed != nil || !errors.Is(err, ErrCOWCapacity) || budget.Stats() != before {
						t.Fatal("refused size preparation changed original ownership", failed, err)
					}
					cancelled, err := writer.Prepare(group, COWPrepareOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if _, valid := cancelled.LogicalSizeDelta(); !valid {
						t.Fatal("bounded delta overflow")
					}
					retired := cancelled.Cancel()
					retired.Drain()
					after := budget.Stats()
					after.PeakBytes = before.PeakBytes // a canceled allocation still raises the high-water mark
					if after != before {
						t.Fatal("cancelled size preparation retained admission")
					}
				}
				prepared, err := writer.Prepare(group, COWPrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				delta, valid := prepared.LogicalSizeDelta()
				root := prepared.Publish()
				roots = append(roots, root)
				if len(roots) > 1 && census(roots[len(roots)-2]) != size {
					t.Fatal("private preparation changed opening root")
				}
				want := census(root)
				if !valid || size+delta != want {
					t.Fatalf("batch=%d: old=%d delta=%d valid=%t postimage=%d", batch, size, delta, valid, want)
				}
				size = want
			}
		})
	}
}

func TestCOWLogicalDeltaCheckedArithmetic(t *testing.T) {
	for _, test := range []struct {
		flags byte
		want  int64
	}{
		{node.FlagInline, 10}, {node.FlagTombstone, 3},
		{node.FlagPointer, 10 + page.ValuePtrSize},
		{node.FlagPointer | node.FlagTombstone, 10 + page.ValuePtrSize},
	} {
		got, valid := cowLogicalSize(3, 7, test.flags)
		if !valid || got != test.want {
			t.Fatalf("flags=%x size=(%d,%t), want=%d", test.flags, got, valid, test.want)
		}
	}
	if _, valid := cowLogicalSize(-1, 0, 0); valid {
		t.Fatal("negative size accepted")
	}
	if uint64(^uint(0)>>1) == math.MaxInt64 {
		for _, flags := range []byte{node.FlagInline, node.FlagPointer} {
			if _, valid := cowLogicalSize(int(^uint(0)>>1), 1, flags); valid {
				t.Fatal("logical size overflow accepted")
			}
		}
	}
	value := cowEncodeValue(COWMutation{Value: []byte("x"), Flags: node.FlagInline})
	for _, test := range []struct {
		initial int64
		add     bool
	}{
		{math.MaxInt64, true}, {math.MinInt64, false},
	} {
		delta := cowLogicalDelta{bytes: test.initial}
		if test.add {
			delta.replace(0, value, "")
		} else {
			delta.replace(0, "", value)
		}
		if _, valid := (&COWPrepared{logicalBytes: delta.bytes, logicalOverflow: delta.overflow}).LogicalSizeDelta(); valid {
			t.Fatal("signed delta overflow accepted")
		}
	}
	// Replacing an equally sized row contributes zero without a transient
	// signed overflow, even at the accumulator's representable boundary.
	delta := cowLogicalDelta{bytes: math.MaxInt64}
	delta.replace(0, value, value)
	if delta.overflow || delta.bytes != math.MaxInt64 {
		t.Fatal("equal replacement failed to telescope")
	}
}
