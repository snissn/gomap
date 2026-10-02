package tree

import (
	"fmt"
	"testing"
	"unsafe"
)

var negativeFilterBenchSink bool

func BenchmarkNegativeFilterMembership(b *testing.B) {
	for _, keys := range []int{8192, 81920} {
		b.Run(fmt.Sprintf("covered=%d", keys), func(b *testing.B) {
			f := NewNegativeFilter(10240)
			for i := 0; i < keys; i++ {
				f.Add([]byte(fmt.Sprintf("record/%08d/0", i)))
			}
			misses := make([][]byte, 10000)
			positive := 0
			for i := range misses {
				misses[i] = []byte(fmt.Sprintf("record/%08d/1", i))
				if !f.DefinitelyAbsent(misses[i]) {
					positive++
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				negativeFilterBenchSink = f.DefinitelyAbsent(misses[i%len(misses)])
			}
			b.StopTimer()
			b.ReportMetric(float64(positive)/float64(len(misses)), "false-positives/miss")
			b.ReportMetric(float64(f.Bytes())+float64(unsafe.Sizeof(*f)), "retained-bytes")
			b.ReportMetric(float64(f.Bytes())/float64(keys), "bytes/key")
		})
	}
}
