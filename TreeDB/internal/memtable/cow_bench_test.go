package memtable

import (
	"fmt"
	"testing"
)

// Fixed-count runs keep the measured foundation inside its finite generation.
// The public cached flush/rollover lifecycle is qualified by C2/C4.
func BenchmarkCOWPrepareReplace(b *testing.B) {
	for _, n := range []int{1024, 2048, 4096} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			budget, w := cowTestWriter(b, DefaultCOWLimits())
			root := cowTestPublish(b, w, cowTestEntries(n))
			entries := []COWMutation{{Key: []byte("00000000"), Value: []byte("replacement")}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p, e := w.Prepare(entries, COWPrepareOptions{})
				if e != nil {
					b.Fatal("use -benchtime=1000x or less for bounded-generation foundation witness:", e)
				}
				next := p.Publish()
				cowTestRelease(root)
				root = next
			}
			b.StopTimer()
			b.ReportMetric(float64(budget.Stats().HistoryBytes)/float64(b.N), "charged-B/op")
			b.ReportMetric(float64(budget.Stats().PeakBytes), "peak-charged-B")
			cowTestRelease(root)
			cowTestClose(w)
		})
	}
}

func BenchmarkCOWCapture(b *testing.B) {
	for _, n := range []int{1024, 2048, 4096} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			_, w := cowTestWriter(b, DefaultCOWLimits())
			root := cowTestPublish(b, w, cowTestEntries(n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v, e := root.Acquire(0)
				if e != nil {
					b.Fatal(e)
				}
				v.Close()
			}
			b.StopTimer()
			cowTestRelease(root)
			cowTestClose(w)
		})
	}
}

func BenchmarkCOWScan(b *testing.B) {
	for _, n := range []int{1024, 2048} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			_, w := cowTestWriter(b, DefaultCOWLimits())
			root := cowTestPublish(b, w, cowTestEntries(n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c, e := root.Cursor(nil, nil)
				if e != nil {
					b.Fatal(e)
				}
				visited := 0
				for {
					_, ok, e := c.Record()
					if e != nil {
						b.Fatal(e)
					}
					if !ok {
						break
					}
					visited++
					_ = c.Next()
				}
				c.Close()
				if visited != n {
					b.Fatalf("visited %d", visited)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(n), "records/op")
			cowTestRelease(root)
			cowTestClose(w)
		})
	}
}
