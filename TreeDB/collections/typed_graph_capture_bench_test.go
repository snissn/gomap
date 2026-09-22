package collections

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"
	"time"
)

// Public rebuild boundary, 1024x8D/M16, command_wal_durable. Fixture insert,
// optional predecessor rebuild, checkpoints, directory scans and Close are
// excluded. Publication is the existing inclusive rebuild stage, not isolated
// lock hold time. File-size growth includes index/assets/WAL, not retained live
// bytes. Run fixed 1x/count3; neither case mutates an existing captured base.
func BenchmarkTypedGraphCapture(b *testing.B) {
	for _, recapture := range []bool{false, true} {
		b.Run(fmt.Sprintf("recapture%t", recapture), func(b *testing.B) {
			if b.N > 10 {
				b.Skip("bounded fixture diagnostic: use -benchtime=1x (maximum 10)")
			}
			b.StopTimer()
			var publication, seal time.Duration
			var growth int64
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				meta := typedMinimaCollectionMeta()
				meta.VectorIndexes[0].M = 16
				dir, db, col := openTypedMinimaCollectionMeta(b, meta)
				ids, retained := make([][]byte, 1024), make([][]byte, 1024)
				columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
				for i := range ids {
					ids[i] = []byte(fmt.Sprintf("row-%05d", i))
					retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, ids[i]))
					columns[0].Float32Vectors = append(columns[0].Float32Vectors, vectorBenchmarkEmbedding(i, 8))
					columns[1].Strings = append(columns[1].Strings, "content")
					columns[2].Strings = append(columns[2].Strings, fmt.Sprintf("%05d", (i*7919)%1024))
					columns[3].Strings = append(columns[3].Strings, "source")
				}
				if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
					b.Fatal(err)
				}
				if err := col.Flush(); err != nil {
					b.Fatal(err)
				}
				if recapture {
					if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
						b.Fatal(err)
					}
				}
				if err := db.Checkpoint(); err != nil {
					b.Fatal(err)
				}
				bytes := func() int64 {
					var total int64
					err := filepath.WalkDir(dir, func(_ string, entry fs.DirEntry, err error) error {
						if err != nil || entry.IsDir() {
							return err
						}
						info, err := entry.Info()
						if err == nil {
							total += info.Size()
						}
						return err
					})
					if err != nil {
						b.Fatal(err)
					}
					return total
				}
				before := bytes()
				b.StartTimer()
				status, err := col.RebuildVectorIndex("embedding_graph")
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				publication += status.ColumnGraphBuild.Publication
				start := time.Now()
				if err := db.Checkpoint(); err != nil {
					b.Fatal(err)
				}
				seal += time.Since(start)
				growth += bytes() - before
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(publication.Nanoseconds())/float64(b.N), "publication-stage-ns/op")
			b.ReportMetric(float64(seal.Nanoseconds())/float64(b.N), "post-ack-checkpoint-ns/op")
			b.ReportMetric(float64(growth)/float64(b.N), "all-files-growth-B/op")
		})
	}
}
