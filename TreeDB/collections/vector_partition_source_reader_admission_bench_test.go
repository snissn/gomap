package collections

import (
	"fmt"
	"testing"
)

// Uses only the existing generic reader/fixture APIs, so the identical benchmark
// file can run on the prechange product to measure this shared allocation seam.
func BenchmarkVectorPartitionSourceReaderAdmission512x128V1(b *testing.B) {
	rows := make([]columnGraphRebuildInputRowV2A, 512)
	for i := range rows {
		vector := make([]float32, 128)
		vector[i%128] = 1
		rows[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("doc-%06d", i), vector: vector}
	}
	_, db, c, def := openColumnGraphTypedColumnVectorTestCollection1782(b, 128, 16, rows)
	defer db.Close()
	if _, err := c.RebuildVectorIndex(def.Name); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(512 * 128 * 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source, owned, err := c.ReadVectorPartitionRouterSourceRowsV1(def.Name)
		if err != nil || source.RowCount != 512 || len(owned) != 512 {
			b.Fatalf("source=%+v rows=%d err=%v", source, len(owned), err)
		}
	}
}
