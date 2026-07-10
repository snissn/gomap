package freelist

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

const (
	pl11CompareIDs        = 10_000
	pl11CompareRegion     = 8192
	pl11CompareRegionHint = 4 * pl11CompareRegion
)

var pl11CompareSink []uint64

func BenchmarkPL11Compare_AllocMany2(b *testing.B) {
	benchmarkPL11CompareRegion(b, 2, true)
}

func BenchmarkPL11Compare_AllocRepeated2(b *testing.B) {
	benchmarkPL11CompareRegion(b, 2, false)
}

func BenchmarkPL11Compare_AllocMany10000(b *testing.B) {
	benchmarkPL11CompareRegion(b, pl11CompareIDs, true)
}

func benchmarkPL11CompareRegion(b *testing.B, count int, many bool) {
	b.Helper()
	ids := make([]uint64, pl11CompareIDs)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	root := b.TempDir()
	path := filepath.Join(root, "fixture.db")
	p, err := pager.Open(path, 64*1024)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	if _, err := p.Alloc(pl11CompareIDs + 1); err != nil {
		_ = p.Close()
		b.Fatalf("Alloc fixture: %v", err)
	}
	a := New(p, 0)
	for _, id := range ids {
		if err := a.Free(id); err != nil {
			b.Fatalf("Free(%d) setup: %v", id, err)
		}
	}
	a.SetFreelistRegion(pl11CompareRegion, 1)
	initialHead := a.head
	initialLastAlloc := a.lastAlloc
	initialStats := a.stats
	snapshots := snapshotPL11CompareHeads(b, p, initialHead, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		restorePL11CompareHeads(b, p, snapshots)
		a.head = initialHead
		a.lastAlloc = initialLastAlloc
		a.stats = initialStats
		b.StartTimer()
		if many {
			out, err := a.AllocMany(count, pl11CompareRegionHint)
			if err != nil {
				b.Fatalf("AllocMany: %v", err)
			}
			if len(out) != count {
				b.Fatalf("AllocMany returned %d IDs, want %d", len(out), count)
			}
			pl11CompareSink = out
		} else {
			hint := uint64(pl11CompareRegionHint)
			out := make([]uint64, 0, count)
			for range count {
				id, err := a.Alloc(hint)
				if err != nil {
					b.Fatalf("Alloc(%d): %v", hint, err)
				}
				out = append(out, id)
				hint = id
			}
			pl11CompareSink = out
		}
	}
	b.StopTimer()
	if err := p.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
	if err := os.Remove(path); err != nil {
		b.Fatalf("Remove fixture: %v", err)
	}
	b.ReportMetric(float64(count*b.N)/b.Elapsed().Seconds(), "ids/sec")
}

type pl11ComparePageSnapshot struct {
	id   uint64
	data []byte
}

func snapshotPL11CompareHeads(b *testing.B, p *pager.Pager, head uint64, all bool) []pl11ComparePageSnapshot {
	b.Helper()
	var snapshots []pl11ComparePageSnapshot
	for head != 0 {
		data, err := p.Get(head)
		if err != nil {
			b.Fatalf("Get(%d): %v", head, err)
		}
		snapshots = append(snapshots, pl11ComparePageSnapshot{id: head, data: append([]byte(nil), data...)})
		if !all {
			break
		}
		head = freelistNextPageID(data)
	}
	return snapshots
}

func restorePL11CompareHeads(b *testing.B, p *pager.Pager, snapshots []pl11ComparePageSnapshot) {
	b.Helper()
	for _, snapshot := range snapshots {
		data, err := p.GetForWrite(snapshot.id)
		if err != nil {
			b.Fatalf("GetForWrite(%d): %v", snapshot.id, err)
		}
		copy(data, snapshot.data)
	}
}
