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
	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%09d", i))
	}
	for i := 0; i < count; i += 1000 {
		batch := d.NewBatch().(*Batch)
		for j := 0; j < 1000 && i+j < count; j++ {
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
	benchmarkConditionalTxnReadSetPlanned(b, 1)
}

func BenchmarkConditionalTxnReadSet10(b *testing.B) {
	benchmarkConditionalTxnReadSetPlanned(b, 10)
}

func BenchmarkConditionalTxnReadSet100(b *testing.B) {
	benchmarkConditionalTxnReadSetPlanned(b, 100)
}

func BenchmarkConditionalTxnReadSet10000(b *testing.B) {
	benchmarkConditionalTxnReadSetPlanned(b, 10000)
}

func benchmarkConditionalTxnReadSetPlanned(b *testing.B, readSet int) {
	b.Helper()
	b.ReportAllocs()
	b.Skipf("planned by #3424/#3425: replace with native conditional transaction benchmark for read set size %d", readSet)
}
