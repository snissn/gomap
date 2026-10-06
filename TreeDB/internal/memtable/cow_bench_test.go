package memtable

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"
)

// Use -benchtime=1000x or less to keep this fixed-count foundation witness
// inside its finite generation.
// The public cached flush/rollover lifecycle is qualified by C2/C4.
func BenchmarkCOWPrepareReplace(b *testing.B) {
	for _, n := range []int{1024, 2048, 4096} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			budget, w := cowTestWriter(b, DefaultCOWLimits())
			root := cowTestPublish(b, w, cowTestEntries(n))
			entries := []COWMutation{{Key: []byte("00000000"), Value: []byte("replacement")}}
			initialCharge := budget.Stats().HistoryBytes
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
			b.ReportMetric(float64(budget.Stats().HistoryBytes-initialCharge)/float64(b.N), "charged-B/op")
			b.ReportMetric(float64(budget.Stats().PeakBytes), "peak-charged-B")
			cowTestRelease(root)
			cowTestClose(w)
			budget.Close()
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

func BenchmarkCOWExternalLease(b *testing.B) {
	budget, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		b.Fatal(err)
	}
	baseline := budget.Stats().TotalBytes
	lease, err := budget.AcquireExternal(COWAllocationCharge(128))
	if err != nil {
		b.Fatal(err)
	}
	charge := budget.Stats().TotalBytes - baseline
	lease.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, err := budget.AcquireExternal(COWAllocationCharge(128))
		if err != nil {
			b.Fatal(err)
		}
		lease.Close()
	}
	b.StopTimer()
	b.ReportMetric(float64(charge), "charged-B/op")
	budget.Close()
}

// This fixture exceeds conversion elision's short-input buffer. Construction
// and the admitted cursor/lease live outside the measured lookup/refusal loops.
func BenchmarkCOWLargeKeyLookup(b *testing.B) {
	budget, w := cowTestWriter(b, DefaultCOWLimits())
	key := make([]byte, 1<<20)
	for i := range key {
		key[i] = 'k'
	}
	root := cowTestPublish(b, w, []COWMutation{{Key: key, Value: []byte("value")}})
	cursor, err := root.Cursor(key, nil)
	if err != nil {
		b.Fatal(err)
	}
	entries := []COWMutation{{Key: key, Value: []byte("new")}}
	defer func() { cursor.Close(); cowTestRelease(root); cowTestClose(w); budget.Close() }()
	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := root.Get(key); !ok {
				b.Fatal("missing")
			}
		}
	})
	b.Run("CursorSeek", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := cursor.Seek(key); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Estimate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := w.Estimate(entries, COWPrepareOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	lease, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{}))))
	if err != nil {
		b.Fatal(err)
	}
	defer lease.Close()
	b.Run("RefusedPrepare", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := w.Prepare(entries, COWPrepareOptions{}); !errors.Is(err, ErrCOWCapacity) {
				b.Fatalf("refusal=%v", err)
			}
		}
	})
}
