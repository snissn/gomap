package valuelog

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

// Resolve the production inspector once so the same test compiles on both revisions.
type managedLeafInspector interface {
	InspectCOWLeafRecord(page.ValuePtr, COWReadLimits) (COWRecordShape, error)
}

func BenchmarkCOWLeafInspector(b *testing.B) {
	benchmarkCOWLeafInspector(b, false)
}

func BenchmarkCOWInteriorLeafInspector(b *testing.B) {
	benchmarkCOWLeafInspector(b, true)
}

func benchmarkCOWLeafInspector(b *testing.B, interior bool) {
	for _, mode := range []string{"serial", "parallel"} {
		b.Run(mode, func(b *testing.B) {
			pageBytes := buildSparseLeafPageForPayloadBench(b)
			compact, ok, err := MaybeCompactLeafLogPayload(pageBytes)
			if err != nil || !ok {
				b.Fatalf("compact fixture: %t, %v", ok, err)
			}
			raw, ptr := cowTestLeafRecord(b, compact, BlockCodecSnappy)
			if interior {
				// Add a full leaf after the selected record so the stock inspector
				// can fill its entire metadata window in one positional read.
				writer, err := NewWriter(raw.Name(), ptr.FileID)
				if err != nil {
					b.Fatal(err)
				}
				_, writeErr := writer.Append(0, nil, 2, pageBytes)
				closeErr := writer.Close()
				if writeErr != nil || closeErr != nil {
					b.Fatalf("append %v, close %v", writeErr, closeErr)
				}
				st, err := raw.Stat()
				if err != nil || st.Size()-int64(ptr.Offset-4) < HeaderSize+cowMaxPrefix {
					b.Fatalf("insufficient bytes after record: %v, %v", st, err)
				}
			}
			f, err := openFile(raw.Name(), ptr.FileID, nil, nil, templ.DecodeOptions{}, nil)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := f.Close(); err != nil {
					b.Error(err)
				}
			})
			inspect := func(p page.ValuePtr, limits COWReadLimits) (COWRecordShape, error) {
				return InspectCOWLeafRecord(f.File, p, limits)
			}
			reader, managed := any(f).(managedLeafInspector)
			if managed {
				inspect = reader.InspectCOWLeafRecord
			}
			want, err := InspectCOWLeafRecord(f.File, ptr, cowTestLimits)
			if err != nil || !want.Compressed || want.ValueBytes != uint64(len(compact)) {
				b.Fatalf("unexpected source shape: %+v, %v", want, err)
			}
			got, err := inspect(ptr, cowTestLimits)
			if err != nil || got != want {
				b.Fatalf("warmup shape=%+v, err=%v", got, err)
			}
			hits := f.mmapReadHits.Load()
			fallbacks := f.mmapReadFallbackReadAt.Load()
			if managed && hits+fallbacks != 1 {
				b.Fatalf("warmup mmap hits=%d, ReadAt fallbacks=%d", hits, fallbacks)
			}
			check := func() {
				got, err := inspect(ptr, cowTestLimits)
				if err != nil || got.RecordBytes != want.RecordBytes {
					b.Errorf("inspection shape=%+v, err=%v", got, err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			if mode == "serial" {
				for range b.N {
					check()
				}
			} else {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						check()
					}
				})
			}
			b.StopTimer()
			deltaHits := f.mmapReadHits.Load() - hits
			deltaFallbacks := f.mmapReadFallbackReadAt.Load() - fallbacks
			if deltaHits != hits*uint64(b.N) || deltaFallbacks != fallbacks*uint64(b.N) {
				b.Fatalf("unstable inspection path: mmap hits=%d, ReadAt fallbacks=%d over %d inspections", deltaHits, deltaFallbacks, b.N)
			}
			b.ReportMetric(float64(deltaHits)/float64(b.N), "mmap-hits/op")
			b.ReportMetric(float64(deltaFallbacks)/float64(b.N), "readat-fallbacks/op")
			b.ReportMetric(float64(want.RecordBytes), "record-B")
		})
	}
}
