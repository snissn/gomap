package collections

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"
	"time"
)

// New internal capability diagnostic, not a public rebuild comparison. Insert,
// initial graph/reconciliation, the eight post-T writes, scans and Close are
// excluded from B/op and ns/op. Fold checkpoint and reconciliation are included.
// Publication is the maintenance call interval (including lock acquisition),
// not an isolated writeMu hold measurement. Growth includes every DB file and
// the excluded post-T write, not retained live storage or reclaimed bytes.
func BenchmarkTypedGraphFold(b *testing.B) {
	for _, suffix := range []bool{false, true} {
		b.Run(fmt.Sprintf("suffix%t", suffix), func(b *testing.B) {
			if b.N > 10 {
				b.Skip("bounded diagnostic: use fixed 1x, at most10")
			}
			b.StopTimer()
			var build, publication, capture time.Duration
			var growth int64
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(b, 1024)
				if err := base.Close(); err != nil {
					b.Fatal(err)
				}
				cold := typedGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 4 << 20, AssetBytes: 128 << 20, DecodedTermBytes: 128 << 20}
				if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 4096, Tombstones: 4096, ValueSlots: 16384, OwnedBytes: 32 << 20, EncodedOutputBytes: 256 << 20}, cold); err != nil {
					b.Fatal(err)
				}
				if err := col.db.Checkpoint(); err != nil {
					b.Fatal(err)
				}
				directoryBytes := func() int64 {
					var total int64
					if err := filepath.WalkDir(col.db.Dir(), func(_ string, entry fs.DirEntry, err error) error {
						if err != nil || entry.IsDir() {
							return err
						}
						info, err := entry.Info()
						if err == nil {
							total += info.Size()
						}
						return err
					}); err != nil {
						b.Fatal(err)
					}
					return total
				}
				before := directoryBytes()
				var timing ColumnGraphBuildTiming
				b.StartTimer()
				err := col.foldTypedGraphTimed(context.Background(), cold, 4096, func() error {
					if !suffix {
						return nil
					}
					b.StopTimer()
					defer b.StartTimer()
					changed := []TypedColumnBatch{{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:8]}, {Name: "content", Strings: columns[1].Strings[:8]}, {Name: "user", Strings: columns[2].Strings[:8]}, {Name: "path", Strings: columns[3].Strings[:8]}}
					_, err := col.ReplaceTypedBatch(ids[:8], retained[:8], changed)
					return err
				}, &timing)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				capture += timing.Snapshot
				build += timing.RowExtraction + timing.AdjacencyBuild + timing.AssetPreparation
				publication += timing.Publication
				growth += directoryBytes() - before
				if err := col.db.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(capture.Nanoseconds())/float64(b.N), "capture-stage-ns/op")
			b.ReportMetric(float64(build.Nanoseconds())/float64(b.N), "off-admission-build-ns/op")
			b.ReportMetric(float64(publication.Nanoseconds())/float64(b.N), "publisher-call-ns/op")
			b.ReportMetric(float64(growth)/float64(b.N), "all-files-growth-B/op")
		})
	}
}

func BenchmarkTypedGraphFoldWarmQuery(b *testing.B) {
	col, base, _, _, columns, _ := openTypedGraphQualityFixture(b, 1024)
	if err := base.Close(); err != nil {
		b.Fatal(err)
	}
	limits := typedGraphOverlapLimits()
	if err := col.foldTypedGraph(context.Background(), limits.Cold, 4096, nil); err != nil {
		b.Fatal(err)
	}
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 128, Tombstones: 128, ValueSlots: 512, OwnedBytes: 8 << 20}, limits.Cold); err != nil {
		b.Fatal(err)
	}
	owner, err := col.openTypedGraphReadOwner(limits)
	if err != nil {
		b.Fatal(err)
	}
	defer owner.Close()
	var buffer VectorIndexSearchBuffer
	query := columns[0].Float32Vectors[0]
	if _, _, err := owner.overlay.search(query, 10, 128, 4096, &buffer); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := owner.overlay.search(query, 10, 128, 4096, &buffer); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
