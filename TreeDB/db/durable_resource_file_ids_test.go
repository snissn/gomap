package db

import (
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"math"
	"slices"
	"testing"
)

func TestDurableResourceFileIDs(t *testing.T) {
	for _, n := range []int{0, 1, 7, 8, 9, 257} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			refs := make(map[uint32]struct{}, n)
			for i := 0; i < n; i++ {
				refs[math.MaxUint32-uint32(i)] = struct{}{}
			}
			var scratch [8]uint32
			got := sortedDurableResourceFileIDs(refs, scratch[:])
			if len(got) != n || !slices.IsSorted(got) {
				t.Fatalf("IDs=%v", got)
			}
			for i, id := range got {
				if id != math.MaxUint32-uint32(n-1-i) {
					t.Fatalf("IDs=%v", got)
				}
			}
			if len(refs) != n {
				t.Fatal("references mutated")
			}
			if n > 0 && n <= len(scratch) && &got[0] != &scratch[0] {
				t.Fatal("small capture did not use caller scratch")
			}
			if n > len(scratch) {
				for _, v := range scratch {
					if v != 0 {
						t.Fatal("spill modified caller scratch")
					}
				}
			}
		})
	}
}

func TestDurableResourceFileIDsAllocation(t *testing.T) {
	for _, n := range []int{0, 1, 8, 9} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			refs := make(map[uint32]struct{}, n)
			for i := 0; i < n; i++ {
				refs[uint32(i)] = struct{}{}
			}
			var scratch [8]uint32
			want := float64(0)
			if n > len(scratch) {
				want = 1
			}
			got := testing.AllocsPerRun(100, func() {
				ids := sortedDurableResourceFileIDs(refs, scratch[:])
				if len(ids) != n {
					panic("lost reference")
				}
			})
			if got != want {
				t.Fatalf("capture scratch allocations=%g, want %g", got, want)
			}
		})
	}
}

func BenchmarkDurableResourceFileIDs(b *testing.B) {
	for _, n := range []int{1, 8, 9, 257} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			refs := make(map[uint32]struct{}, n)
			for i := 0; i < n; i++ {
				refs[uint32(n-i)] = struct{}{}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var scratch [8]uint32
				ids := sortedDurableResourceFileIDs(refs, scratch[:])
				if len(ids) != n {
					b.Fatal("lost reference")
				}
			}
		})
	}
}

func TestDurableResourceFileIDsCapture(t *testing.T) {
	for _, n := range []int{0, 1, 8, 9} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			dir := t.TempDir()
			d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			refs := make(map[uint32]struct{}, n)
			for i := n; i > 0; i-- {
				id, _, path := writeUnregisteredValueLogRecord(t, dir, 0, uint32(i), []byte("retained"), uint64(i))
				if err := d.RegisterValueLogSegment(path, id); err != nil {
					t.Fatal(err)
				}
				refs[id] = struct{}{}
			}
			resources, err := d.captureRegisteredDurableValueLogResourcesV1(refs)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				if resources != nil {
					resources.Release()
					t.Fatal("empty capture owns resources")
				}
				return
			}
			defer resources.Release()
			diagnostics, err := resources.AcquirePhysicalDiagnostics()
			if err != nil {
				t.Fatal(err)
			}
			defer diagnostics.Close()
			seen := make(map[uint32]bool, n)
			for _, physical := range diagnostics.Physical() {
				if physical.Kind == rootpublication.ResourceValueLog {
					id := uint32(physical.Generation)
					if _, ok := refs[id]; !ok || seen[id] {
						t.Fatalf("unexpected/duplicate resource %d", id)
					}
					seen[id] = true
				}
			}
			if len(seen) != n {
				t.Fatalf("captured %d IDs, want %d", len(seen), n)
			}
			limits := &PreparedRootPublicationLimits{MaxRegisteredValueLogFiles: n - 1}
			if result, err := d.captureRegisteredDurableValueLogResourcesWithLimitsV1(refs, limits); err == nil || result != nil {
				if result != nil {
					result.Release()
				}
				t.Fatal("capture exceeded registered-file limit")
			}
			refs[math.MaxUint32] = struct{}{}
			if result, err := d.captureRegisteredDurableValueLogResourcesV1(refs); err == nil || result != nil {
				if result != nil {
					result.Release()
				}
				t.Fatal("capture accepted missing file")
			}
		})
	}
}
