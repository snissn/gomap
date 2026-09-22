package collections

import (
	"fmt"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkPreparedInsertPublicPath(b *testing.B) {
	const batches, rowsPerBatch = 4, 16000
	for _, mode := range []string{"ordinary", "prepared_serial", "prepared_pipeline"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for run := 0; run < b.N; run++ {
				b.StopTimer()
				dir := b.TempDir()
				enableColumnRetainedPlacementCommandWAL(b, dir)
				d := openColumnRetainedPlacementDB(b, dir, backenddb.Options{})
				col := createColumnRetainedSemanticStreamCollection(b, d, "events")
				ids := make([][][]byte, batches)
				docs := make([][][]byte, batches)
				for batch := range ids {
					for row := 0; row < rowsPerBatch; row++ {
						ordinal := batch*rowsPerBatch + row
						ids[batch] = append(ids[batch], []byte(fmt.Sprintf("id-%08d", ordinal)))
						docs[batch] = append(docs[batch], []byte(fmt.Sprintf(`{"row_id":%d,"kind":"post","payload":"%s"}`, ordinal, strings.Repeat("content", 32))))
					}
				}
				b.StartTimer()
				switch mode {
				case "ordinary":
					for batch := range ids {
						if _, err := col.InsertBatch(ids[batch], docs[batch]); err != nil {
							b.Fatal(err)
						}
					}
				case "prepared_serial":
					for batch := range ids {
						prepared, err := col.PrepareInsertBatchOwned(ids[batch], docs[batch], 512<<20)
						if err != nil {
							b.Fatal(err)
						}
						if _, err := prepared.Commit(); err != nil {
							b.Fatal(err)
						}
					}
				case "prepared_pipeline":
					prepare := func(batch int) <-chan struct {
						value *PreparedInsertBatch
						err   error
					} {
						ready := make(chan struct {
							value *PreparedInsertBatch
							err   error
						}, 1)
						go func() {
							value, err := col.PrepareInsertBatchOwned(ids[batch], docs[batch], 512<<20)
							ready <- struct {
								value *PreparedInsertBatch
								err   error
							}{value, err}
						}()
						return ready
					}
					ready := prepare(0)
					for batch := range ids {
						result := <-ready
						if result.err != nil {
							b.Fatal(result.err)
						}
						if batch+1 < batches {
							ready = prepare(batch + 1)
						}
						if _, err := result.value.Commit(); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				if err := d.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
