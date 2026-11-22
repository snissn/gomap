package cowbtree

import (
	"runtime"
	"runtime/debug"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestMapAllocationChargeCountsActualSitesAndDiscardedGrowth(t *testing.T) {
	m := NewMap[int, int](32)
	if m.AllocationCharge() != 0 {
		t.Fatal("empty header charged as a node")
	}
	m.Set(0, 0)
	node := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(mapNode[int, int]{})))
	pair := uint64(unsafe.Sizeof(mapPair[int, int]{}))
	want := node + retainedalloc.AllocationCharge(pair)
	if got := m.AllocationCharge(); got != want {
		t.Fatalf("first node/backing charge=%d want=%d", got, want)
	}
	for i := 1; i < 31; i++ {
		m.Set(i, i)
	}
	for capacity := 2; capacity <= 32; capacity *= 2 {
		want += retainedalloc.AllocationCharge(uint64(capacity) * pair)
	}
	if got := m.AllocationCharge(); got != want {
		t.Fatalf("growth charge=%d want=%d", got, want)
	}
	liveBacking := node + retainedalloc.AllocationCharge(uint64(cap(m.root.items))*pair)
	if want <= liveBacking {
		t.Fatal("discarded growth arrays lost their original charge")
	}
	m.Clear()
	if m.AllocationCharge() != want {
		t.Fatal("Clear refunded original allocations")
	}
}

func TestMapSharedLeafInsertUsesOneBacking(t *testing.T) {
	m := NewMap[int, int](32)
	m.Set(10, 10)
	m.Set(30, 30)
	if !m.root.leaf() || len(m.root.items) != 2 || cap(m.root.items) != 2 {
		t.Fatal("fixture needs a non-full leaf with no spare backing")
	}
	fork := m.Copy()
	if _, replaced := fork.Set(20, 21); replaced {
		t.Fatal("fork replaced an existing key")
	}
	if _, found := m.Get(20); found || m.Len() != 2 || fork.Len() != 3 || fork.root.count != 3 {
		t.Fatal("shared leaf contents or counts changed unexpectedly")
	}
	want := []mapPair[int, int]{{key: 10, value: 10}, {key: 20, value: 21}, {key: 30, value: 30}}
	for i, item := range want {
		if got := fork.root.items[i]; got != item {
			t.Fatalf("item %d = %+v, want %+v", i, got, item)
		}
	}
	if got := cap(fork.root.items); got != 4 {
		t.Fatalf("item capacity = %d, want 4", got)
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(*fork.root))) +
		retainedalloc.AllocationCharge(4*uint64(unsafe.Sizeof(mapPair[int, int]{})))
	if got := fork.AllocationCharge(); got != charge {
		t.Fatalf("fork charge = %d, want %d for leaf and final backing", got, charge)
	}
}

func TestMapAllocationChargeForksAndCopiedInternalNodes(t *testing.T) {
	m := NewMap[int, int](2)
	for i := range 1024 {
		m.Set(i, i)
	}
	original := m.AllocationCharge()
	fork := m.Copy()
	if fork.AllocationCharge() != 0 || m.AllocationCharge() != original {
		t.Fatal("Copy inherited or erased the opening allocation receipt")
	}
	key := 511
	var expected uint64
	for n := m.root; n != nil; {
		if n.leaf() {
			expected += retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(*n)))
		} else {
			expected += retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(mapInternalNode[int, int]{})))
		}
		expected += retainedalloc.AllocationCharge(uint64(min(len(n.items)+1, cap(n.items))) * uint64(unsafe.Sizeof(mapPair[int, int]{})))
		i, found := m.search(n, key)
		if !n.leaf() {
			expected += retainedalloc.AllocationCharge(uint64(min(len(*n.children)+1, m.max+1)) * uint64(unsafe.Sizeof((*mapNode[int, int])(nil))))
		}
		if found || n.leaf() {
			break
		}
		n = (*n.children)[i]
	}
	fork.Set(key, -1)
	if got := fork.AllocationCharge(); got != expected {
		t.Fatalf("copied path charge=%d want=%d", got, expected)
	}
	if value, _ := m.Get(key); value != key {
		t.Fatal("fork changed opening nodes")
	}
	immutable := fork.ImmutableCopy()
	if immutable.AllocationCharge() != 0 || fork.AllocationCharge() != expected {
		t.Fatal("immutable header lost private allocation receipt")
	}
	private := immutable.ForkImmutable()
	if private.AllocationCharge() != 0 {
		t.Fatal("immutable fork inherited old allocation receipt")
	}
	private.Set(1025, 1025)
	if private.AllocationCharge() == 0 || immutable.AllocationCharge() != 0 {
		t.Fatal("private fork did not own its new allocation charge")
	}
}

func TestMapAllocationChargeSaturates(t *testing.T) {
	m := NewMap[int, int](2)
	m.allocationCharge = ^uint64(0) - 1
	m.recordAllocation(16)
	if m.AllocationCharge() != ^uint64(0) {
		t.Fatal("allocation charge overflow wrapped")
	}
	m.recordAllocation(0)
	if m.AllocationCharge() != ^uint64(0) {
		t.Fatal("zero allocation changed saturated charge")
	}
}

func TestMapAllocationChargeCoversTotalAlloc(t *testing.T) {
	previousProcs, previousGC := runtime.GOMAXPROCS(1), debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(previousGC); runtime.GOMAXPROCS(previousProcs) })
	for _, shape := range []string{"set", "load", "replace_fork", "erase_fork", "split_merge"} {
		t.Run(shape, func(t *testing.T) {
			m := NewMap[int, int](4)
			if shape == "replace_fork" || shape == "erase_fork" {
				for i := range 2048 {
					m.Set(i, i)
				}
				m = m.Copy()
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			switch shape {
			case "set":
				for i := range 2048 {
					m.Set(i, i)
				}
			case "load":
				for i := range 2048 {
					m.Load(i, i)
				}
			case "replace_fork":
				for i := range 2048 {
					m.Set(i, -i)
				}
			case "erase_fork":
				for i := range 2048 {
					m.Delete(i)
				}
			case "split_merge":
				for i := range 2048 {
					m.Load(i, i)
				}
				for i := 0; i < 2048; i += 2 {
					m.Delete(i)
				}
				for i := 2048; i < 4096; i++ {
					m.Load(i, i)
				}
			}
			runtime.ReadMemStats(&after)
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > m.AllocationCharge() {
				t.Fatalf("actual node/backing allocation=%d above recorded charge=%d", allocated, m.AllocationCharge())
			}
			checkMutationCounts(t, m)
		})
	}
}
