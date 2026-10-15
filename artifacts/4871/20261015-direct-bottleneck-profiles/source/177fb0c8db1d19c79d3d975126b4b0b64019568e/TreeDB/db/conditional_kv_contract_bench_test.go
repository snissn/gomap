package db

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

func BenchmarkGetVersioned(b *testing.B) {
	d, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}()

	const count = 10000
	val := make([]byte, 100)
	keys := benchmarkConditionalTxnKeys(count)
	seedConditionalTxnBenchmarkData(b, d, keys, val)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		dst := make([]byte, 0, len(val))
		for pb.Next() {
			out, revision, err := d.GetVersionedAppend(keys[r.Intn(count)], dst[:0])
			if err != nil {
				b.Fatalf("GetVersionedAppend: %v", err)
			}
			if len(out) != len(val) || revision == page.LegacyEntryRevision {
				b.Fatalf("GetVersionedAppend len=%d revision=%d, want len=%d non-legacy revision", len(out), revision, len(val))
			}
		}
	})
}

func BenchmarkConditionalTxnReadSet1(b *testing.B) {
	benchmarkConditionalTxnReadSet(b, 1)
}

func BenchmarkConditionalTxnReadSet10(b *testing.B) {
	benchmarkConditionalTxnReadSet(b, 10)
}

func BenchmarkConditionalTxnReadSet100(b *testing.B) {
	benchmarkConditionalTxnReadSet(b, 100)
}

func BenchmarkConditionalTxnReadSet10000(b *testing.B) {
	benchmarkConditionalTxnReadSet(b, 10000)
}

func BenchmarkConditionalTxnBaselineBatchWrite(b *testing.B) {
	d, err := Open(Options{Dir: b.TempDir(), rootPublicationFixedDelay: 100 * time.Millisecond})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}()

	const count = 10000
	seedVal := make([]byte, 100)
	keys := benchmarkConditionalTxnKeys(count)
	seedConditionalTxnBenchmarkData(b, d, keys, seedVal)

	key := []byte("conditional-baseline-write")
	val := []byte("value")
	coordinator := d.rootPublication.coordinator
	// Go's benchmark allocation accounting is process-wide, so allowing the
	// asynchronous root publisher to run during this row attributes its stable
	// publication work to the foreground Batch.Write path. Keep each bounded
	// group well below the fixed 100 ms production timer and the coordinator's
	// soft/hard debt triggers, then drain it outside the measured interval. The
	// gate pins this row to a fixed iteration count so base and head accumulate
	// the same amount of generation history.
	const foregroundGroup = 8
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for groupStart := 0; groupStart < b.N; groupStart += foregroundGroup {
		groupEnd := min(groupStart+foregroundGroup, b.N)
		before := coordinator.Stats()
		b.StartTimer()
		for i := groupStart; i < groupEnd; i++ {
			batch := d.NewBatch().(*Batch)
			batch.Reserve(1)
			if err := batch.Set(key, val); err != nil {
				b.Fatalf("Set: %v", err)
			}
			if err := batch.Write(); err != nil {
				b.Fatalf("Write: %v", err)
			}
			if err := batch.Close(); err != nil {
				b.Fatalf("Close batch: %v", err)
			}
		}
		b.StopTimer()
		after := coordinator.Stats()
		if after.PublishCalls != before.PublishCalls {
			b.Fatalf("root publisher ran during foreground group: before=%d after=%d", before.PublishCalls, after.PublishCalls)
		}
		if err := d.Checkpoint(); err != nil {
			b.Fatalf("Checkpoint: %v", err)
		}
		drained := coordinator.Stats()
		if drained.PendingCommits != 0 || drained.DurableCommitSeq != drained.VisibleCommitSeq {
			b.Fatalf("checkpoint left publication debt: pending=%d visible=%d durable=%d",
				drained.PendingCommits, drained.VisibleCommitSeq, drained.DurableCommitSeq)
		}
	}
}

func BenchmarkConditionalTxnBaselineGet1BatchWrite(b *testing.B) {
	benchmarkConditionalTxnBaselineGetBatchWrite(b, 1)
}

func BenchmarkConditionalTxnBaselineGet10BatchWrite(b *testing.B) {
	benchmarkConditionalTxnBaselineGetBatchWrite(b, 10)
}

func BenchmarkConditionalTxnBaselineGet100BatchWrite(b *testing.B) {
	benchmarkConditionalTxnBaselineGetBatchWrite(b, 100)
}

func BenchmarkConditionalTxnBaselineGet10000BatchWrite(b *testing.B) {
	benchmarkConditionalTxnBaselineGetBatchWrite(b, 10000)
}

func benchmarkConditionalTxnBaselineGetBatchWrite(b *testing.B, readSet int) {
	b.Helper()
	d, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}()

	count := 10000
	if readSet*2 > count {
		count = readSet * 2
	}
	val := make([]byte, 100)
	keys := benchmarkConditionalTxnKeys(count)
	seedConditionalTxnBenchmarkData(b, d, keys, val)

	writeKey := []byte("conditional-baseline-get-write")
	writeVal := []byte("value")
	readScratch := make([]byte, 0, len(val))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < readSet; j++ {
			out, revision, err := d.GetVersionedAppend(keys[(i+j)%count], readScratch[:0])
			if err != nil {
				b.Fatalf("GetVersionedAppend: %v", err)
			}
			if len(out) != len(val) || revision == page.LegacyEntryRevision {
				b.Fatalf("GetVersionedAppend len=%d revision=%d, want len=%d non-legacy revision", len(out), revision, len(val))
			}
		}
		batch := d.NewBatch().(*Batch)
		batch.Reserve(1)
		if err := batch.Set(writeKey, writeVal); err != nil {
			b.Fatalf("Set: %v", err)
		}
		if err := batch.Write(); err != nil {
			b.Fatalf("Write: %v", err)
		}
		if err := batch.Close(); err != nil {
			b.Fatalf("Close batch: %v", err)
		}
	}
}

func benchmarkConditionalTxnReadSet(b *testing.B, readSet int) {
	b.Helper()
	d, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}()

	count := 10000
	if readSet*2 > count {
		count = readSet * 2
	}
	val := make([]byte, 100)
	keys := benchmarkConditionalTxnKeys(count)
	seedConditionalTxnBenchmarkData(b, d, keys, val)

	writeKey := []byte("conditional-txn-write")
	writeVal := []byte("value")
	readScratch := make([]byte, 0, len(val))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := d.NewConditionalTxn()
		if err != nil {
			b.Fatalf("NewConditionalTxn: %v", err)
		}
		tx.ReserveReadSet(readSet)
		tx.ReserveWrites(1)
		for j := 0; j < readSet; j++ {
			out, revision, err := tx.GetVersionedAppend(keys[(i+j)%count], readScratch[:0])
			if err != nil {
				b.Fatalf("GetVersionedAppend: %v", err)
			}
			if len(out) != len(val) || revision == page.LegacyEntryRevision {
				b.Fatalf("GetVersionedAppend len=%d revision=%d, want len=%d non-legacy revision", len(out), revision, len(val))
			}
		}
		if err := tx.Set(writeKey, writeVal); err != nil {
			b.Fatalf("Set: %v", err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit: %v", err)
		}
	}
}

func benchmarkConditionalTxnKeys(count int) [][]byte {
	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}
	return keys
}

func seedConditionalTxnBenchmarkData(b *testing.B, d *DB, keys [][]byte, val []byte) {
	b.Helper()
	for i := 0; i < len(keys); i += 1000 {
		batch := d.NewBatch().(*Batch)
		for j := 0; j < 1000 && i+j < len(keys); j++ {
			revision := page.EntryRevision(i + j + 1)
			if err := batch.SetWithRevision(keys[i+j], val, revision); err != nil {
				b.Fatalf("SetWithRevision: %v", err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			b.Fatalf("WriteSync: %v", err)
		}
		if err := batch.Close(); err != nil {
			b.Fatalf("Close batch: %v", err)
		}
	}
}
