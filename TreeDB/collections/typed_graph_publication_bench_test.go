package collections

import (
	"fmt"
	"testing"
)

// Identical durable typed-write fixtures; only internal derived admission is
// toggled. Ack-only excludes its final Flush; ack+Flush includes every Flush.
// At 10x, eight rows per batch grow the physical suffix from 0 to 80 rows.
func BenchmarkTypedGraphPublicationPublicWrite(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		for _, flush := range []bool{false, true} {
			b.Run(fmt.Sprintf("enabled%t/flush%t", enabled, flush), func(b *testing.B) {
				if b.N > 64 {
					b.Skip("bounded write diagnostic: use -benchtime=10x")
				}
				b.StopTimer()
				_, db, col := openTypedMinimaCollection(b)
				defer db.Close()
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					b.Fatal(err)
				}
				if enabled {
					snap := db.AcquireSnapshot()
					catalog, err := col.catalogForSnapshot(snap)
					if err != nil {
						b.Fatal(err)
					}
					if err := col.initializeTypedGraphPublication(catalog, typedGraphPublicationLimits{Rows: b.N * 8, Tombstones: b.N * 8, ValueSlots: b.N * 32, OwnedBytes: 1 << 20}); err != nil {
						b.Fatal(err)
					}
					_ = snap.Close()
				}
				ids, retained := make([][][]byte, b.N), make([][][]byte, b.N)
				columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
				for j := 0; j < 8; j++ {
					columns[0].Float32Vectors = append(columns[0].Float32Vectors, []float32{1, 0, 0, 0, 0, 0, 0, 0})
					for k := 1; k < 4; k++ {
						columns[k].Strings = append(columns[k].Strings, "fixture")
					}
				}
				for i := 0; i < b.N; i++ {
					for j := 0; j < 8; j++ {
						id := fmt.Sprintf("row%04d", i*8+j)
						ids[i] = append(ids[i], []byte(id))
						retained[i] = append(retained[i], []byte(fmt.Sprintf(`{"id":%q}`, id)))
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.StartTimer()
				for i := 0; i < b.N; i++ {
					if _, _, err := col.InsertTypedBatchWithStats(ids[i], retained[i], columns); err != nil {
						b.Fatal(err)
					}
					if flush {
						if err := col.Flush(); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				if err := col.Flush(); err != nil {
					b.Fatal(err)
				}
				if enabled {
					state := col.typedGraphPublicationSnapshot()
					if state.invalid || state.physicalRows != b.N*8 {
						b.Fatal("incomplete measured publication")
					}
				}
				b.ReportMetric(8, "rows/batch")
			})
		}
	}
}

// Measures internal prepare/reservation only: fixture, public durable seed,
// Flush, graph capture, and database Close are outside the timer. No publication
// latency, concurrent-writer tail, or public lifecycle-readiness claim.
func BenchmarkTypedGraphPublicationPreparation(b *testing.B) {
	for _, suffix := range []int{0, 32, 256} {
		for _, changed := range []int{1, 16} {
			b.Run(fmt.Sprintf("suffix%d/changed%d", suffix, changed), func(b *testing.B) {
				_, db, col := openTypedMinimaCollection(b)
				defer db.Close()
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					b.Fatal(err)
				}
				snap := db.AcquireSnapshot()
				catalog, err := col.catalogForSnapshot(snap)
				if err != nil {
					b.Fatal(err)
				}
				if err := col.initializeTypedGraphPublication(catalog, typedGraphPublicationLimits{Rows: suffix + changed + 1, Tombstones: suffix + changed + 1, ValueSlots: 4 * (suffix + changed + 1), OwnedBytes: 1 << 20}); err != nil {
					b.Fatal(err)
				}
				_ = snap.Close()
				ids, retained := make([][]byte, suffix), make([][]byte, suffix)
				columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
				for i := 0; i < suffix; i++ {
					ids[i] = []byte(fmt.Sprintf("old%04d", i))
					retained[i] = []byte(fmt.Sprintf(`{"id":"old%04d"}`, i))
					columns[0].Float32Vectors = append(columns[0].Float32Vectors, []float32{1, 0, 0, 0, 0, 0, 0, 0})
					for j := 1; j < len(columns); j++ {
						columns[j].Strings = append(columns[j].Strings, "fixture")
					}
				}
				if suffix != 0 {
					if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
						b.Fatal(err)
					}
					if err := col.Flush(); err != nil {
						b.Fatal(err)
					}
				}
				state := col.typedGraphPublicationSnapshot()
				documents := make([]columnWriteDocument, changed)
				for i := range documents {
					documents[i] = columnWriteDocument{ID: []byte(fmt.Sprintf("new%04d", i)), declaredValuesReady: true, declaredValues: []columnDeclaredValue{{Type: ColumnStoreValueFloat32Vector, Float32Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0}}, {Type: ColumnStoreValueString, String: "changed"}, {Type: ColumnStoreValueString, String: "tenant"}, {Type: ColumnStoreValueString, String: "path"}}}
				}
				input, err := prepareColumnWritePublishInputBeforeCommandWAL(columnWritePublishInput{meta: state.catalog.meta, catalog: state.catalog, operation: ColumnPublishOperationInsert, documents: documents, rows: changed})
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					candidate, err := col.prepareTypedGraphPublication(input)
					if err != nil {
						b.Fatal(err)
					}
					candidate.rejectBeforeAppend()
				}
				b.StopTimer()
				b.ReportMetric(float64(suffix), "existing-suffix-rows")
				b.ReportMetric(float64(changed), "changed-rows")
			})
		}
	}
}
