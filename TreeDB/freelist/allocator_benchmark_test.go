package freelist

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

const allocatorBenchmarkIDs = 10_000

func BenchmarkAllocator_FreeMany(b *testing.B) {
	benchmarkAllocatorFree(b, true)
}

func BenchmarkAllocator_Free(b *testing.B) {
	benchmarkAllocatorFree(b, false)
}

func benchmarkAllocatorFree(b *testing.B, many bool) {
	b.Helper()
	ids := allocatorBenchmarkIDsSlice()
	pagesTouched := (allocatorBenchmarkIDs + page.MaxFreeIDs) / (page.MaxFreeIDs + 1)
	checksumUpdates := pagesTouched
	if !many {
		pagesTouched = allocatorBenchmarkIDs
		checksumUpdates = allocatorBenchmarkIDs
	}

	root := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, p, path := newAllocatorBenchmarkFixture(b, root, i)
		b.StartTimer()
		if many {
			if err := a.FreeMany(ids); err != nil {
				b.Fatalf("FreeMany: %v", err)
			}
		} else {
			for _, id := range ids {
				if err := a.Free(id); err != nil {
					b.Fatalf("Free(%d): %v", id, err)
				}
			}
		}
		b.StopTimer()
		if err := p.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
		if err := os.Remove(path); err != nil {
			b.Fatalf("Remove fixture: %v", err)
		}
	}
	benchmarkAllocatorMetrics(b, allocatorBenchmarkIDs, pagesTouched, checksumUpdates)
}

func BenchmarkAllocator_AllocMany(b *testing.B) {
	ids := allocatorBenchmarkIDsSlice()
	pagesTouched := (allocatorBenchmarkIDs + page.MaxFreeIDs) / (page.MaxFreeIDs + 1)
	root := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a, p, path := newAllocatorBenchmarkFixture(b, root, i)
		if err := a.FreeMany(ids); err != nil {
			b.Fatalf("FreeMany setup: %v", err)
		}
		a.SetFreelistRegion(1024, 1)
		a.lastAlloc = 4 * 1024
		b.StartTimer()
		out, err := a.AllocMany(allocatorBenchmarkIDs, 0)
		if err != nil {
			b.Fatalf("AllocMany: %v", err)
		}
		if len(out) != allocatorBenchmarkIDs {
			b.Fatalf("AllocMany returned %d IDs, want %d", len(out), allocatorBenchmarkIDs)
		}
		b.StopTimer()
		if err := p.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
		if err := os.Remove(path); err != nil {
			b.Fatalf("Remove fixture: %v", err)
		}
	}
	benchmarkAllocatorMetrics(b, allocatorBenchmarkIDs, pagesTouched, pagesTouched)
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

func benchmarkAllocatorMetrics(b *testing.B, ids, pagesTouched, checksumUpdates int) {
	b.ReportMetric(float64(ids*b.N)/b.Elapsed().Seconds(), "ids/sec")
	b.ReportMetric(float64(pagesTouched), "pages_touched/op")
	b.ReportMetric(float64(checksumUpdates), "checksum_updates/op")
}
