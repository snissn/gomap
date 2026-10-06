package db

import (
	"context"
	"testing"
)

// Run the identical benchmark source on the base and repaired production.
// Outcomes may change; setup and measured scan boundaries remain identical.
func BenchmarkValueLogPhysicalAccounting(b *testing.B) {
	for _, chunk := range []bool{false, true} {
		name := "segment"
		if chunk {
			name = "chunk"
		}
		b.Run(name, func(b *testing.B) {
			db := openCompactStorageAuditBenchmarkFixture(b, 4096, 256)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if chunk {
					if _, err := db.estimateValueLogLiveBytesByChunk(context.Background(), 1<<20); err != nil {
						b.Fatal(err)
					}
				} else {
					b.StopTimer()
					clearRewritePlanLiveBytesCacheForTest(db)
					b.StartTimer()
					if _, err := db.estimateValueLogLiveBytesBySegment(context.Background()); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
