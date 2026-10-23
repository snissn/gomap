package cowbtree

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

var compositeAllocationSink *Map[int, int]

func TestMapInternalForkAllocationObjects(t *testing.T) {
	m := NewMap[int, int](32)
	for i := range 8192 {
		m.Set(i, i)
	}
	nodes, branches := 0, 0
	for n := m.root; ; n = (*n.children)[0] {
		nodes++
		if n.leaf() {
			break
		}
		branches++
	}
	// One map header, one node and item backing per path node, and one
	// children backing per branch. The branch header must need no extra object.
	want := float64(1 + 2*nodes + branches)
	got := testing.AllocsPerRun(20, func() {
		f := m.Copy()
		f.Set(0, -1)
		compositeAllocationSink = f
	})
	compositeAllocationSink = nil
	if got > want {
		t.Fatalf("retained update allocated %.0f objects, want at most %.0f", got, want)
	}
}

// A fork retaining a large sparse tree must not allocate all the unused source
// capacity again for one replacement. The immutable source remains readable.
func TestMapSparseForkCopyAllocation(t *testing.T) {
	m := NewMap[int, int](32)
	for i := range 8192 {
		m.Set(i, i)
	}
	var full uint64
	for n := m.root; ; {
		full += retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(*n)))
		full += retainedalloc.AllocationCharge(uint64(cap(n.items)) * uint64(unsafe.Sizeof(mapPair[int, int]{})))
		if n.leaf() {
			break
		}
		full += retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(*n.children)))
		full += retainedalloc.AllocationCharge(uint64(m.max+1) * uint64(unsafe.Sizeof((*mapNode[int, int])(nil))))
		n = (*n.children)[0]
	}
	fork := m.Copy()
	fork.Set(0, -1)
	if got := fork.AllocationCharge(); got >= full {
		t.Fatalf("sparse replacement allocated %d bytes; want below full-capacity copy %d", got, full)
	}
	if v, ok := m.Get(0); !ok || v != 0 {
		t.Fatal("replacement changed the retained tree")
	}
	checkMutationCounts(t, fork)
}
