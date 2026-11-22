package db

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func BenchmarkValueLogRefDeltaOverwrite(b *testing.B) {
	const batchSize = 1024
	keys := make([][]byte, batchSize)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%06d", i))
	}

	for _, trackerEnabled := range []bool{true, false} {
		name := "tracker_disabled"
		if trackerEnabled {
			name = "tracker_enabled"
		}
		b.Run(name, func(b *testing.B) {
			d, err := Open(Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer func() { _ = d.Close() }()

			seed := d.NewBatch().(*Batch)
			for i, key := range keys {
				ptr := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: uint64(i + 1), Length: 64}
				if err := seed.SetPointer(key, ptr); err != nil {
					b.Fatalf("seed SetPointer: %v", err)
				}
			}
			if err := seed.Write(); err != nil {
				b.Fatalf("seed Write: %v", err)
			}
			_ = seed.Close()
			if !trackerEnabled {
				d.valueLogRefTracker = nil
			}

			b.ReportAllocs()
			b.ReportMetric(batchSize, "overwrites/op")
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				fileID := page.ValueLogFileID(uint32(2 + n%2))
				update := d.NewBatch().(*Batch)
				for i, key := range keys {
					ptr := page.ValuePtr{FileID: fileID, Offset: uint64(n*batchSize + i + 1), Length: 96}
					if err := update.SetPointer(key, ptr); err != nil {
						b.Fatalf("SetPointer: %v", err)
					}
				}
				if err := update.Write(); err != nil {
					b.Fatalf("Write: %v", err)
				}
				_ = update.Close()
			}
		})
	}
}

func BenchmarkValueLogGCLeafBacked(b *testing.B) {
	for _, name := range []string{"LeafOnly", "Mixed"} {
		b.Run(name, func(b *testing.B) {
			database, opts, _ := openValueLogGCLeafFixture(b, 8192)
			wantSegments := 0
			if name == "Mixed" {
				ptrs := appendPointersInNewSegment(b, opts.Dir, 100, 1, 1<<30, 1, func(int) []byte {
					return []byte("referenced external value")
				})
				batch := database.NewBatch().(*Batch)
				if err := batch.SetPointer([]byte("k008191"), ptrs[0]); err != nil {
					b.Fatalf("SetPointer: %v", err)
				}
				if err := errors.Join(batch.WriteSync(), batch.Close()); err != nil {
					b.Fatalf("publish mixed fixture: %v", err)
				}
				if err := database.Checkpoint(); err != nil {
					b.Fatalf("checkpoint mixed fixture: %v", err)
				}
				wantSegments = 1
			}

			ctx := context.Background()
			gcOpts := ValueLogGCOptions{}
			manager := database.valueLogManager
			checksBefore := manager.ReadStats().RecordCRCChecks
			refreshesBefore := manager.RefreshScanCount()
			b.ReportAllocs()
			for b.Loop() {
				stats, err := database.ValueLogGC(ctx, gcOpts)
				if err != nil {
					b.Fatalf("ValueLogGC: %v", err)
				}
				if stats.SegmentsTotal != wantSegments || stats.SegmentsReferenced != wantSegments || stats.SegmentsDeleted != 0 {
					b.Fatalf("unexpected GC stats: %+v", stats)
				}
			}
			b.ReportMetric(float64(manager.ReadStats().RecordCRCChecks-checksBefore)/float64(b.N), "leaf-record-crcs/op")
			b.ReportMetric(float64(manager.RefreshScanCount()-refreshesBefore)/float64(b.N), "inventory-refreshes/op")
		})
	}
}
