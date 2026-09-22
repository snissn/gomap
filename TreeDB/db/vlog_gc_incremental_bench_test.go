package db

import (
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
