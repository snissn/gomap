package collections

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeColumnGraphConstructionCapacityRejectsUnsupportedAndOverflow(t *testing.T) {
	def, ids, _ := nativeColumnGraphScratchFixture(t, 2, false)
	walk := func(visit func([]byte) error) error {
		for _, id := range ids {
			if err := visit(id); err != nil {
				return err
			}
		}
		return nil
	}
	for _, mutate := range []func(*VectorIndexDefinition){
		func(d *VectorIndexDefinition) { d.M = 65 },
		func(d *VectorIndexDefinition) { d.EfConstruction = 129 },
		func(d *VectorIndexDefinition) { d.Metric = VectorMetricL2 },
		func(d *VectorIndexDefinition) { d.Encoding = VectorIndexEncodingInt8 },
		func(d *VectorIndexDefinition) { d.Representation = VectorIndexRepresentationCosineNormalizedF32V1 },
		func(d *VectorIndexDefinition) {
			d.QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "derived"}}
		},
		func(d *VectorIndexDefinition) { d.Name = "" },
	} {
		d := def
		mutate(&d)
		if _, err := nativeColumnGraphConstructionCapacity(d, 2, walk); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			t.Fatalf("unsupported shape: %v", err)
		}
	}
	if _, err := nativeColumnGraphConstructionCapacity(def, ^uint64(0), walk); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("row overflow: %v", err)
	}
	if _, err := nativeColumnGraphConstructionCapacity(def, 1, walk); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("long source: %v", err)
	}
	if _, err := nativeColumnGraphConstructionCapacity(def, 3, walk); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("short source: %v", err)
	}
	d := def
	d.Dimensions = int(^uint(0) >> 1)
	if _, err := nativeColumnGraphConstructionCapacity(d, 2, walk); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("dimension overflow: %v", err)
	}
	if _, err := nativeColumnGraphConstructionCapacity(def, 2, nil); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("nil source: %v", err)
	}
	if _, err := nativeColumnGraphConstructionCapacity(def, 1, func(visit func([]byte) error) error { return visit(nil) }); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("empty ID: %v", err)
	}
	cause := errors.New("source error")
	if _, err := nativeColumnGraphConstructionCapacity(def, 2, func(func([]byte) error) error { return cause }); !errors.Is(err, cause) {
		t.Fatalf("source identity: %v", err)
	}
	var sum nativeColumnGraphCapacitySum
	sum.allocation(^uint64(0), 2)
	if !errors.Is(sum.err, memtable.ErrCOWCapacity) {
		t.Fatalf("allocation overflow: %v", sum.err)
	}
	sum = nativeColumnGraphCapacitySum{}
	sum.copies(^uint64(0), 1, 16)
	if !errors.Is(sum.err, memtable.ErrCOWCapacity) {
		t.Fatalf("cohort overflow: %v", sum.err)
	}
}

// A concrete maintained-builder witness, not a restatement of coefficients.
// Source mappings/Config/authority are intentionally absent from this private
// constructor fixture and remain an explicit caller responsibility.
func TestNativeColumnGraphConstructionCapacityConcreteBacking(t *testing.T) {
	for _, count := range []int{0, 1, 31, 32, 258} {
		def, ids, vectors := nativeColumnGraphScratchFixture(t, count, false)
		p, err := nativeColumnGraphConstructionCapacity(def, uint64(count), func(visit func([]byte) error) error {
			for _, id := range ids {
				if err := visit(id); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		idx := nativeColumnGraphScratchIndex(t, def, count)
		var arena [nativeVectorFrozenPrefixBatchWidth]vectorIndexSearchScratch
		if count >= nativeVectorFrozenPrefixBatchMinimum {
			if err := idx.insertVectorFrozenPrefixBatchesWithPlanningScratchLocked(context.Background(), ids, vectors, &arena); err != nil {
				t.Fatal(err)
			}
		} else if err := idx.insertVectorBatchWithContextLocked(context.Background(), ids, vectors); err != nil {
			t.Fatal(err)
		}
		var concrete uint64
		add := func(count int, size uintptr) { concrete += memtable.COWAllocationCharge(uint64(count) * uint64(size)) }
		add(cap(idx.nodes), unsafe.Sizeof(vectorIndexNode{}))
		add(cap(idx.vectorRows), 4)
		for _, node := range idx.nodes {
			add(cap(node.documentID), 1)
			// Matrix rows alias the separately counted plane.
			if idx.vectorRows == nil {
				add(cap(node.vector), 4)
			}
			add(cap(node.neighbors), unsafe.Sizeof([]vectorIndexNeighbor{}))
			for _, neighbors := range node.neighbors {
				add(cap(neighbors), unsafe.Sizeof(vectorIndexNeighbor{}))
			}
		}
		for i := -1; i < len(arena); i++ {
			scratch := &idx.insertScratch
			if i >= 0 {
				scratch = &arena[i]
			}
			add(cap(scratch.visitedEpochs), 4)
			for _, n := range []int{cap(scratch.queue), cap(scratch.best), cap(scratch.liveBest), cap(scratch.out), cap(scratch.resumeCandidates), cap(scratch.resumeDeferred)} {
				add(n, unsafe.Sizeof(vectorIndexCandidate{}))
			}
		}
		if concrete > p.PrivateBytes {
			t.Fatalf("rows%d concrete backing%d exceeds private promise%d", count, concrete, p.PrivateBytes)
		}
		if p.Rows != uint64(count) || p.TotalBytes < p.PrivateBytes || p.TotalBytes-p.PrivateBytes != p.CallerRowsBytes {
			t.Fatalf("rows%d owner split invalid: %+v", count, p)
		}
		var levels [33]uint64
		for _, node := range idx.nodes {
			levels[node.level]++
		}
		if levels != p.LevelRows {
			t.Fatalf("rows%d census differs from actual builder", count)
		}
	}
}

func TestNativeColumnGraphConstructionCapacityOriginalIDStream(t *testing.T) {
	def, _, _ := nativeColumnGraphScratchFixture(t, 0, false)
	p, err := nativeColumnGraphConstructionCapacity(def, 4096, func(visit func([]byte) error) error {
		var borrowed [7]byte
		for i := 0; i < 4096; i++ {
			copy(borrowed[:], fmt.Sprintf("d%06d", i))
			if err := visit(borrowed[:]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := [33]uint64{3831, 243, 22}
	if p.LevelRows != want || p.IDBytes != 4096*7 {
		t.Fatalf("original immutable ID census: %+v", p)
	}
	t.Logf("original component: %+v", p)
	// This is a component inventory, never an assertion that the whole recipe
	// or even this conservative private component fits the maintenance child.
	if p.InsertionBytes == 0 || p.RemapBytes == 0 || p.SearchScratchBytes <= p.SerialScratchBytes {
		t.Fatalf("missing actual parallel/fallback phases: %+v", p)
	}
}
