package freelist

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

const (
	allocatorBenchmarkIDs        = 10_000
	allocatorBenchmarkRegion     = 8192
	allocatorBenchmarkRegionHint = 4 * allocatorBenchmarkRegion
)

var allocatorBenchmarkIDsSink []uint64

func BenchmarkAllocator_FreeMany(b *testing.B) {
	benchmarkAllocatorFree(b, true)
}

func BenchmarkAllocator_Free(b *testing.B) {
	benchmarkAllocatorFree(b, false)
}

func benchmarkAllocatorFree(b *testing.B, many bool) {
	b.Helper()
	ids := allocatorBenchmarkIDsSlice()
	root := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, p, path := newAllocatorBenchmarkFixture(b, root, i)
		b.StartTimer()
		freeAllocatorBenchmarkIDs(b, a, ids, many)
		b.StopTimer()
		closeAllocatorBenchmarkFixture(b, p, path)
	}

	benchmarkAllocatorMetrics(b, allocatorBenchmarkIDs)
}

func freeAllocatorBenchmarkIDs(b *testing.B, a *Allocator, ids []uint64, many bool) {
	b.Helper()
	if many {
		if err := a.FreeMany(ids); err != nil {
			b.Fatalf("FreeMany: %v", err)
		}
		return
	}
	for _, id := range ids {
		if err := a.Free(id); err != nil {
			b.Fatalf("Free(%d): %v", id, err)
		}
	}
}

func BenchmarkAllocator_AllocMany(b *testing.B) {
	benchmarkAllocatorRegion(b, allocatorBenchmarkIDs, true)
}

func BenchmarkAllocator_AllocMany2(b *testing.B) {
	benchmarkAllocatorRegion(b, 2, true)
}

func BenchmarkAllocator_AllocRepeated2(b *testing.B) {
	benchmarkAllocatorRegion(b, 2, false)
}

func benchmarkAllocatorRegion(b *testing.B, count int, many bool) {
	b.Helper()
	ids := allocatorBenchmarkIDsSlice()
	root := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, p, path := newAllocatorBenchmarkFixture(b, root, i)
		populateAllocatorBenchmarkFreelist(b, a, ids)
		a.SetFreelistRegion(allocatorBenchmarkRegion, 1)
		b.StartTimer()
		allocAllocatorBenchmarkIDs(b, a, count, many)
		b.StopTimer()
		closeAllocatorBenchmarkFixture(b, p, path)
	}

	benchmarkAllocatorMetrics(b, count)
}

func populateAllocatorBenchmarkFreelist(b *testing.B, a *Allocator, ids []uint64) {
	b.Helper()
	for _, id := range ids {
		if err := a.Free(id); err != nil {
			b.Fatalf("Free(%d) setup: %v", id, err)
		}
	}
}

func allocAllocatorBenchmarkIDs(b *testing.B, a *Allocator, count int, many bool) {
	b.Helper()
	if many {
		out, err := a.AllocMany(count, allocatorBenchmarkRegionHint)
		if err != nil {
			b.Fatalf("AllocMany: %v", err)
		}
		if len(out) != count {
			b.Fatalf("AllocMany returned %d IDs, want %d", len(out), count)
		}
		allocatorBenchmarkIDsSink = out
		return
	}

	hint := uint64(allocatorBenchmarkRegionHint)
	out := make([]uint64, 0, count)
	for i := 0; i < count; i++ {
		id, err := a.Alloc(hint)
		if err != nil {
			b.Fatalf("Alloc(%d): %v", hint, err)
		}
		out = append(out, id)
		hint = id
	}
	if len(out) != count {
		b.Fatalf("repeated Alloc returned %d IDs, want %d", len(out), count)
	}
	allocatorBenchmarkIDsSink = out
}

func allocatorBenchmarkIDsSlice() []uint64 {
	ids := make([]uint64, allocatorBenchmarkIDs)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	return ids
}

func newAllocatorBenchmarkFixture(b *testing.B, root string, iteration int) (*Allocator, *pager.Pager, string) {
	b.Helper()
	path := filepath.Join(root, fmt.Sprintf("%d.db", iteration))
	p, err := pager.Open(path, 64*1024)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	if _, err := p.Alloc(allocatorBenchmarkIDs + 1); err != nil {
		_ = p.Close()
		b.Fatalf("Alloc fixture: %v", err)
	}
	return New(p, 0), p, path
}

func closeAllocatorBenchmarkFixture(b *testing.B, p *pager.Pager, path string) {
	b.Helper()
	if err := p.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	if err := os.Remove(path); err != nil {
		b.Fatalf("Remove fixture: %v", err)
	}
}

func benchmarkAllocatorMetrics(b *testing.B, ids int) {
	b.ReportMetric(float64(ids*b.N)/b.Elapsed().Seconds(), "ids/sec")
}
