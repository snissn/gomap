package collections

import (
	"fmt"
	"testing"
)

// Cold invalidation is fixture-only and outside the timer. Each measured cold
// call reconciles the same persisted suffix; reuse verifies an unchanged frontier.
// Writes, graph capture, database setup/Close, and invalidation are excluded.
func BenchmarkTypedGraphReconciliation(b *testing.B) {
	for _, size := range []int{0, 32, 256} {
		for _, reuse := range []bool{false, true} {
			b.Run(fmt.Sprintf("suffix%d/reuse%t", size, reuse), func(b *testing.B) {
				if b.N > 64 {
					b.Skip("bounded cold diagnostic: use -benchtime=10x")
				}
				b.StopTimer()
				_, db, col := openTypedMinimaCollection(b)
				defer db.Close()
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					b.Fatal(err)
				}
				ids, retained := make([][]byte, size), make([][]byte, size)
				columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
				for i := range ids {
					ids[i] = []byte(fmt.Sprintf("row%04d", i))
					retained[i] = []byte(fmt.Sprintf(`{"id":"row%04d"}`, i))
					columns[0].Float32Vectors = append(columns[0].Float32Vectors, []float32{1, 0, 0, 0, 0, 0, 0, 0})
					for j := 1; j < len(columns); j++ {
						columns[j].Strings = append(columns[j].Strings, "fixture")
					}
				}
				if size != 0 {
					if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
						b.Fatal(err)
					}
					if err := col.Flush(); err != nil {
						b.Fatal(err)
					}
				}
				limits := typedGraphPublicationLimits{Rows: size + 1, Tombstones: size + 1, ValueSlots: 4 * (size + 1), OwnedBytes: 1 << 20}
				cold := typedGraphColdLimits{ManifestRecords: 1024, ManifestBytes: 1 << 20, AssetBytes: 8 << 20, DecodedTermBytes: 8 << 20}
				if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !reuse {
						coord := col.collectionSchemaCoordinator()
						before := coord.typedPublication.Load()
						invalid := *before
						invalid.invalid = true
						if !coord.typedPublication.CompareAndSwap(before, &invalid) {
							b.Fatal("fixture invalidation raced")
						}
					}
					b.StartTimer()
					err := col.reconcileTypedGraphPublication(limits, cold)
					b.StopTimer()
					if err != nil {
						b.Fatal(err)
					}
					if state := col.typedGraphPublicationSnapshot(); state.invalid || state.physicalRows != size || len(state.rows) != size {
						b.Fatal("incomplete measured reconciliation")
					}
				}
				b.ReportMetric(float64(size), "physical-suffix-rows")
			})
		}
	}
}
