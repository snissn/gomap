package db

import (
	"bytes"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// BenchmarkRootPublicationActivation is the committed safety-equivalent
// loss-debt harness for no_wal_fast root activation. Ordinary Set calls use the
// production accepted-candidate handoff; SetSync, Checkpoint, and Close then
// exercise the same coordinator's explicit durability boundaries. Fixed-delay
// rows validate the full 10-100ms adaptive window without replacing the real
// publisher or its dependency -> index -> alternate-meta ordering.
// Run with:
//
// go test ./TreeDB/db -run '^$' -bench '^BenchmarkRootPublicationActivation$' -benchmem -benchtime=200x -count=5
func BenchmarkRootPublicationActivation(b *testing.B) {
	rows := []struct {
		name  string
		delay time.Duration
	}{
		{name: "adaptive"},
		{name: "fixed=10ms", delay: 10 * time.Millisecond},
		{name: "fixed=25ms", delay: 25 * time.Millisecond},
		{name: "fixed=50ms", delay: 50 * time.Millisecond},
		{name: "fixed=100ms", delay: 100 * time.Millisecond},
	}
	for _, row := range rows {
		b.Run(row.name, func(b *testing.B) {
			value := bytes.Repeat([]byte("v"), 128)
			database, err := Open(Options{
				Dir:                       b.TempDir(),
				Durability:                DurabilityWALOffRelaxed,
				DisableBackgroundPrune:    true,
				rootPublicationFixedDelay: row.delay,
			})
			if err != nil {
				b.Fatal(err)
			}
			closed := false
			stable := newDurableRootStableCallAccumulator()
			stable.observeCaller(currentGoroutineID())
			restore := durabilitycut.Install(stable.observe)
			b.Cleanup(func() {
				restore()
				if !closed {
					_ = database.Close()
				}
			})

			coordinator := database.rootPublication.coordinator
			before := coordinator.Stats()
			ackLatency := make([]time.Duration, b.N)
			evidenceStarted := time.Now()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := strconv.AppendInt([]byte("activation/ordinary/"), int64(i), 10)
				started := time.Now()
				if err := database.Set(key, value); err != nil {
					b.Fatalf("ordinary Set(%d): %v", i, err)
				}
				ackLatency[i] = time.Since(started)
			}
			b.StopTimer()
			ordinaryElapsed := b.Elapsed()
			ordinary := coordinator.Stats()
			ordinaryCallerStableCalls := stable.callerStableCalls()
			if ordinaryCallerStableCalls != 0 {
				b.Fatalf("ordinary writer goroutine made %d stable calls", ordinaryCallerStableCalls)
			}

			syncStarted := time.Now()
			if err := database.SetSync([]byte("activation/explicit-sync"), value); err != nil {
				b.Fatalf("SetSync: %v", err)
			}
			syncLatency := time.Since(syncStarted)

			if err := database.Set([]byte("activation/checkpoint-debt"), value); err != nil {
				b.Fatalf("pre-Checkpoint Set: %v", err)
			}
			checkpointStarted := time.Now()
			if err := database.Checkpoint(); err != nil {
				b.Fatalf("Checkpoint: %v", err)
			}
			checkpointLatency := time.Since(checkpointStarted)

			if err := database.Set([]byte("activation/close-debt"), value); err != nil {
				b.Fatalf("pre-Close Set: %v", err)
			}
			closeStarted := time.Now()
			if err := database.Close(); err != nil {
				b.Fatalf("Close: %v", err)
			}
			closeLatency := time.Since(closeStarted)
			closed = true
			final := coordinator.Stats()
			evidenceElapsed := time.Since(evidenceStarted)

			operations := float64(b.N)
			if operations == 0 {
				operations = 1
			}
			publishCalls := final.PublishCalls - before.PublishCalls
			publishedCommits := final.DurableCommitSeq - before.DurableCommitSeq
			b.ReportMetric(operations/ordinaryElapsed.Seconds(), "throughput-ops/s")
			b.ReportMetric(float64(percentileDuration(ackLatency, 50).Nanoseconds()), "ack-p50-ns")
			b.ReportMetric(float64(percentileDuration(ackLatency, 99).Nanoseconds()), "ack-p99-ns")
			b.ReportMetric(float64(publishCalls)/evidenceElapsed.Seconds(), "publisher-calls/s")
			b.ReportMetric(float64(final.LastServiceDuration.Nanoseconds()), "publisher-service-ns")
			b.ReportMetric(float64(final.EWMAServiceDuration.Nanoseconds()), "publisher-ewma-ns")
			b.ReportMetric(float64(final.LastGroupSize), "publisher-last-group")
			if publishCalls != 0 {
				b.ReportMetric(float64(publishedCommits)/float64(publishCalls), "publisher-mean-group")
			}
			b.ReportMetric(float64(final.PublishDelay.Milliseconds()), "publisher-delay-ms")
			b.ReportMetric(float64(ordinary.PendingCommits), "debt-commits-at-ack")
			b.ReportMetric(float64(ordinary.PendingBytes), "debt-bytes-at-ack")
			b.ReportMetric(float64(ordinary.PendingAge.Nanoseconds()), "debt-age-ns-at-ack")
			b.ReportMetric(float64(final.AdmissionWaits-before.AdmissionWaits), "admission-waits")
			b.ReportMetric(float64(rootpublication.HardPendingCommits), "hard-debt-commits")
			b.ReportMetric(float64(rootpublication.HardPendingBytes), "hard-debt-bytes")
			b.ReportMetric(float64(final.TimerPublishes-before.TimerPublishes), "trigger-timer")
			b.ReportMetric(float64(final.WaiterPublishes-before.WaiterPublishes), "trigger-waiter")
			b.ReportMetric(float64(final.SoftBytesPublishes-before.SoftBytesPublishes), "trigger-soft-bytes")
			b.ReportMetric(float64(final.HardAdmissionPublishes-before.HardAdmissionPublishes), "trigger-hard-admission")
			b.ReportMetric(float64(final.RetryPublishes-before.RetryPublishes), "trigger-retry")
			b.ReportMetric(float64(final.DrainPublishes-before.DrainPublishes), "trigger-drain")
			b.ReportMetric(float64(syncLatency.Nanoseconds()), "set-sync-ns")
			b.ReportMetric(float64(checkpointLatency.Nanoseconds()), "checkpoint-ns")
			b.ReportMetric(float64(closeLatency.Nanoseconds()), "close-ns")
			b.ReportMetric(float64(ordinaryCallerStableCalls), "ordinary-caller-stable-calls")
			stable.report(b, operations)
		})
	}
}

func percentileDuration(values []time.Duration, percentile int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := ((len(values) - 1) * percentile) / 100
	return values[index]
}
