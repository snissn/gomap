package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestPartitionSourceInsertHighDimensionReopen(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("command-WAL namespace persistence is unsupported")
	}
	for _, stride := range []int{1, 2} {
		t.Run(fmt.Sprintf("stride%d", stride), func(t *testing.T) {
			const rows = 8193
			vector := make([]float64, 768)
			for i := range vector {
				vector[i] = float64(float32(0.0012345678))
			}
			vectors := make([][]float64, rows*stride)
			for i := range vectors {
				vectors[i] = vector
			}
			dir := t.TempDir()
			if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
				t.Fatal(err)
			}
			db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			meta := partitionCollectionMeta("source", len(vector))
			// Exercise the same full-document command-WAL and typed columns,
			// without spending this ingestion regression on graph construction.
			meta.VectorIndexes = nil
			manager := collections.NewCollectionManager(db)
			if _, err := manager.CreateCollection(meta); err != nil {
				t.Fatal(err)
			}
			col, err := manager.OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			if stride == 1 {
				err = insertM3SourceRows(col, vectors)
			} else {
				err = insertPartitionRows(col, vectors, stride-1, stride)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := col.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			col, err = collections.NewCollectionManager(db).OpenCollection(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			truncated, err := col.ScanDocumentIDsFunc(rows+1, func(id []byte) (bool, error) {
				want := fmt.Sprintf("doc-%06d", count*stride+stride-1)
				if string(id) != want {
					t.Fatalf("id=%s want %s", id, want)
				}
				count++
				return true, nil
			})
			if err != nil || truncated || count != rows {
				t.Fatalf("count=%d truncated=%v err=%v", count, truncated, err)
			}
			for _, row := range []int{0, rows / 2, rows - 1} {
				ordinal := row*stride + stride - 1
				raw, err := col.Get([]byte(fmt.Sprintf("doc-%06d", ordinal)))
				if err != nil {
					t.Fatal(err)
				}
				var doc struct {
					TimeUS    int64     `json:"time_us"`
					Embedding []float32 `json:"embedding"`
				}
				if err := json.Unmarshal(raw, &doc); err != nil {
					t.Fatal(err)
				}
				if doc.TimeUS != int64(ordinal+1) || len(doc.Embedding) != len(vector) {
					t.Fatalf("row %d shape/time mismatch", ordinal)
				}
				for dimension, value := range doc.Embedding {
					if value != float32(vector[dimension]) {
						t.Fatalf("row %d dimension %d changed", ordinal, dimension)
					}
				}
			}
		})
	}
}

// Keep setup/close outside the timer; measure the public collection ingestion
// path, including JSON, command-WAL, typed-column publication and final flush.
func BenchmarkPartitionSourceInsert(b *testing.B) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		b.Skip("command-WAL namespace persistence is unsupported")
	}
	const rows = 8193
	for _, dims := range []int{128, 768} {
		b.Run(fmt.Sprintf("dims%d", dims), func(b *testing.B) {
			vector := make([]float64, dims)
			for i := range vector {
				vector[i] = float64(float32(0.0012345678))
			}
			vectors := make([][]float64, rows)
			for i := range vectors {
				vectors[i] = vector
			}
			b.ReportAllocs()
			b.SetBytes(rows * int64(dims) * 4)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
					b.Fatal(err)
				}
				db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
				if err != nil {
					b.Fatal(err)
				}
				meta := partitionCollectionMeta("source", dims)
				meta.VectorIndexes = nil
				manager := collections.NewCollectionManager(db)
				if _, err := manager.CreateCollection(meta); err != nil {
					_ = db.Close()
					b.Fatal(err)
				}
				col, err := manager.OpenCollection(meta.Name)
				if err != nil {
					_ = db.Close()
					b.Fatal(err)
				}
				b.StartTimer()
				err = insertM3SourceRows(col, vectors)
				if err == nil {
					err = col.Flush()
				}
				b.StopTimer()
				closeErr := db.Close()
				if err != nil {
					b.Fatal(err)
				}
				if closeErr != nil {
					b.Fatal(closeErr)
				}
				if err := os.RemoveAll(dir); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(rows*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}
