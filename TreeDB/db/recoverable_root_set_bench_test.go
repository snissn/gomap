package db

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkRecoverableRootSetCaptureRevalidate is the committed #3681 fixture
// for protected-set construction. Varying the logical key count while keeping
// publication debt fixed makes accidental tree-size-dependent capture work
// visible in both latency and allocations.
func BenchmarkRecoverableRootSetCaptureRevalidate(b *testing.B) {
	for _, keys := range []int{0, 16 * 1024} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			database, err := Open(Options{Dir: b.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			b.Cleanup(func() { _ = database.Close() })
			if keys != 0 {
				batch := database.NewBatchWithSize(keys)
				for i := 0; i < keys; i++ {
					key := []byte(fmt.Sprintf("recoverable-root-set-%08d", i))
					if err := batch.Set(key, []byte("fixture-value")); err != nil {
						_ = batch.Close()
						b.Fatalf("Set fixture key %d: %v", i, err)
					}
				}
				if err := batch.WriteSync(); err != nil {
					_ = batch.Close()
					b.Fatalf("WriteSync fixture: %v", err)
				}
				if err := batch.Close(); err != nil {
					b.Fatalf("Close fixture batch: %v", err)
				}
			}

			b.ReportAllocs()
			b.ReportMetric(float64(keys), "fixture-keys")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				set, err := database.CaptureRecoverableRootSet(context.Background())
				if err != nil {
					b.Fatalf("CaptureRecoverableRootSet: %v", err)
				}
				if err := set.Revalidate(); err != nil {
					set.Release()
					b.Fatalf("Revalidate: %v", err)
				}
				set.Release()
			}
		})
	}
}
