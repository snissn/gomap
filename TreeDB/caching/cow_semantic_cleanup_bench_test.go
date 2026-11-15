package caching

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Only the original synchronous drain is timed. Preborn work and its original
// admitted receipts are prepared outside the timer, then checked after return.
func BenchmarkCOWSemanticCleanupGuardDrain(b *testing.B) {
	for _, tc := range []struct {
		name string
		n    int
	}{{"one", 1}, {"many", 64}} {
		b.Run(tc.name, func(b *testing.B) {
			db, c := cowCutFixture(b)
			db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
			before := c.budget.Stats()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				states := make([]*testSemanticState, tc.n)
				refunds := make([]*observingSemanticRefund, tc.n)
				for i := range states {
					lease, err := c.budget.AcquireExternal(8192)
					if err != nil {
						b.Fatal(err)
					}
					refunds[i] = &observingSemanticRefund{lease: lease, observe: func() {}}
					states[i] = newTestSemanticState(nil)
					states[i].refund = refunds[i]
					c.semanticMu.Lock()
					c.enqueueSemanticLocked(states[i].Release())
					c.semanticMu.Unlock()
				}
				b.StartTimer()
				db.drainSemanticCleanup()
				b.StopTimer()
				for i, state := range states {
					if state.calls != 1 || state.cleanup.Work != nil || state.resource != nil || refunds[i].calls != 1 {
						b.Fatal("original cleanup/refund did not complete exactly once")
					}
				}
				if after := c.budget.Stats(); after.ExternalBytes != before.ExternalBytes || after.ReservedBytes != before.ReservedBytes {
					b.Fatal("original cleanup charge leaked")
				}
			}
			b.ReportMetric(float64(tc.n), "records/drain")
		})
	}
}
