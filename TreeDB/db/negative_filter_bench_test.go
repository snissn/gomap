package db

import (
	"fmt"
	"testing"
	"unsafe"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/tree"
)

var negativeCoverageBenchSink *negativeRootCoverage

// This bounded causal probe isolates per-publication hash/token work; public
// WriteSync qualification supplies the full-path cost including storage I/O.
func BenchmarkNegativeCoveragePrepare(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		for _, count := range []int{1, 64, 256} {
			b.Run(fmt.Sprintf("enabled=%v/keys=%d", enabled, count), func(b *testing.B) {
				idx := &indexGen{}
				d := &DB{}
				view := &snapshotView{idx: idx, state: &DBState{CommitSeq: 1, RootPageID: 2}}
				if enabled {
					view.negativeFilter = tree.NewNegativeFilter(10240)
				}
				d.snapshotViewRO.Store(view)
				entries := make([]batchpkg.Entry, count)
				for i := range entries {
					entries[i].Key = []byte(fmt.Sprintf("record/%08d/0", i))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					negativeCoverageBenchSink = d.prepareNegativeCoverage(idx, 1, 2, 3, entries)
				}
				b.StopTimer()
				b.ReportMetric(float64(count), "keys/op")
				if enabled {
					b.ReportMetric(float64(unsafe.Sizeof(negativeRootCoverage{})), "token-bytes")
				}
			})
		}
	}
}
