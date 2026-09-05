package collections

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// This diagnostic is deliberately separate from throughput: forced GC measures
// process-wide live heap deltas, not RSS or exact collection-owned heap.
func TestTypedMinimaMemoryLifecycle(t *testing.T) {
	for _, mode := range []string{"json", "typed"} {
		t.Run(mode, func(t *testing.T) {
			_, d, col := openTypedMinimaCollection(t)
			defer d.Close()
			const rows = 32
			ids, retained, documents := make([][]byte, rows+1), make([][]byte, rows+1), make([][]byte, rows+1)
			columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, rows+1)}, {Name: "content", Strings: make([]string, rows+1)}, {Name: "user", Strings: make([]string, rows+1)}, {Name: "path", Strings: make([]string, rows+1)}}
			for i := range ids {
				id := fmt.Sprintf("memory-%d", i)
				ids[i], retained[i] = []byte(id), []byte(fmt.Sprintf(`{"id":%q}`, id))
				columns[0].Float32Vectors[i] = []float32{1, 0, 0, 0, 0, 0, 0, 0}
				columns[1].Strings[i], columns[2].Strings[i], columns[3].Strings[i] = "minima alpha content searchable text", "user", "path"
				documents[i] = []byte(fmt.Sprintf(`{"id":%q,"embedding":[1,0,0,0,0,0,0,0],"content":%q,"meta":{"user_id":"user","fpath":"path"}}`, id, columns[1].Strings[i]))
			}
			if _, _, err := col.InsertBatchWithStats(ids[rows:], documents[rows:]); err != nil {
				t.Fatal(err)
			}
			if err := col.Flush(); err != nil {
				t.Fatal(err)
			}
			for i := range columns {
				if columns[i].Strings != nil {
					columns[i].Strings = columns[i].Strings[:rows]
				} else {
					columns[i].Float32Vectors = columns[i].Float32Vectors[:rows]
				}
			}
			runtime.GC()
			var before, pending, published runtime.MemStats
			runtime.ReadMemStats(&before)
			var err error
			if mode == "typed" {
				_, _, err = col.InsertTypedBatchWithStats(ids[:rows], retained[:rows], columns)
			} else {
				_, _, err = col.InsertBatchWithStats(ids[:rows], documents[:rows])
			}
			if err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			runtime.ReadMemStats(&pending)
			state := col.writeDomain.statsSnapshot()
			if err := col.Flush(); err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			runtime.ReadMemStats(&published)
			runtime.KeepAlive(ids)
			runtime.KeepAlive(retained)
			runtime.KeepAlive(documents)
			runtime.KeepAlive(columns)
			runtime.KeepAlive(col)
			t.Logf("rows=%d pending_live_heap_delta_B=%d post_flush_live_heap_delta_B=%d pending_full_document_rows=%d pending_reconstruction_rows=%d pending_raw_document_B=%d", rows, int64(pending.HeapAlloc)-int64(before.HeapAlloc), int64(published.HeapAlloc)-int64(before.HeapAlloc), state.PendingIndexedFullDocumentRows, state.PendingIndexedReconstructionRows, state.PendingIndexedRawDocumentBytes)
		})
	}
}

// Both modes publish the same logical rows into the same typed-storage layout.
// Schema/fixture construction and update/delete seeding are outside the timer;
// admission, durable WAL acknowledgement, and Flush publication are inside.
func BenchmarkTypedMinimaPublicBatch(b *testing.B) {
	for _, rows := range []int{1, 32, 128} {
		for features := 0; features <= 3; features++ {
			for _, mode := range []string{"json", "typed"} {
				b.Run(fmt.Sprintf("rows%d/indexes%d/%s", rows, features, mode), func(b *testing.B) {
					benchmarkTypedMinimaPublicBatch(b, rows, features, mode, "insert")
				})
			}
		}
	}
}

func BenchmarkTypedMinimaPublicMutation(b *testing.B) {
	for _, operation := range []string{"replace", "delete"} {
		for _, mode := range []string{"json", "typed"} {
			b.Run(operation+"/"+mode, func(b *testing.B) {
				benchmarkTypedMinimaPublicBatch(b, 32, 3, mode, operation)
			})
		}
	}
}

func benchmarkTypedMinimaPublicBatch(b *testing.B, rows, features int, mode, operation string) {
	b.StopTimer()
	meta := typedMinimaCollectionMeta()
	meta.Indexes = meta.Indexes[:min(features, 2)]
	if features < 3 {
		meta.TextIndexes = nil
	}
	dir, d, col := openTypedMinimaCollectionMeta(b, meta)
	defer d.Close()
	// Warm both admission paths and the shared publisher before measuring either.
	if _, _, err := col.InsertBatchWithStats([][]byte{[]byte("warm-json")}, [][]byte{[]byte(`{"id":"warm-json","embedding":[1,0,0,0,0,0,0,0],"content":"warm","meta":{"user_id":"warm","fpath":"warm"}}`)}); err != nil {
		b.Fatal(err)
	}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("warm-typed")}, [][]byte{[]byte(`{"id":"warm-typed"}`)}, []TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}}}, {Name: "content", Strings: []string{"warm"}}, {Name: "user", Strings: []string{"warm"}}, {Name: "path", Strings: []string{"warm"}}}); err != nil {
		b.Fatal(err)
	}
	if err := col.Flush(); err != nil {
		b.Fatal(err)
	}
	var publication time.Duration
	var extraction time.Duration
	var walGrowth, storageGrowth int64
	b.ReportAllocs()
	b.ResetTimer()
	for batch := 0; batch < b.N; batch++ {
		ids, retained, documents := make([][]byte, rows), make([][]byte, rows), make([][]byte, rows)
		columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, rows)}, {Name: "content", Strings: make([]string, rows)}, {Name: "user", Strings: make([]string, rows)}, {Name: "path", Strings: make([]string, rows)}}
		for row := range ids {
			id := fmt.Sprintf("minima-%d-%d", batch, row)
			ids[row] = []byte(id)
			retained[row] = []byte(fmt.Sprintf(`{"id":%q}`, id))
			columns[0].Float32Vectors[row] = []float32{1, 0, 0, 0, 0, 0, 0, 0}
			columns[1].Strings[row] = "minima alpha content searchable text"
			columns[2].Strings[row] = fmt.Sprintf("user-%d", row%4)
			columns[3].Strings[row] = fmt.Sprintf("path-%d", row)
			documents[row] = []byte(fmt.Sprintf(`{"id":%q,"embedding":[1,0,0,0,0,0,0,0],"content":%q,"meta":{"user_id":%q,"fpath":%q}}`, id, columns[1].Strings[row], columns[2].Strings[row], columns[3].Strings[row]))
		}
		if operation != "insert" {
			var err error
			if mode == "typed" {
				_, _, err = col.InsertTypedBatchWithStats(ids, retained, columns)
			} else {
				_, _, err = col.InsertBatchWithStats(ids, documents)
			}
			if err != nil {
				b.Fatal(err)
			}
			if err := col.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		items := make([]UpdateBatchItem, rows)
		if operation == "replace" {
			for row := range ids {
				columns[1].Strings[row] = "minima beta content searchable text"
				documents[row] = []byte(strings.ReplaceAll(string(documents[row]), "alpha", "beta"))
				doc := documents[row]
				items[row] = UpdateBatchItem{DocumentID: ids[row], Update: func([]byte) ([]byte, bool, error) { return doc, true, nil }}
			}
		}
		beforeWAL, beforeStorage := typedMinimaBenchmarkDiskBytes(b, dir)
		b.StartTimer()
		var err error
		switch operation {
		case "insert":
			var stats CollectionInsertStats
			if mode == "typed" {
				_, stats, err = col.InsertTypedBatchWithStats(ids, retained, columns)
			} else {
				_, stats, err = col.InsertBatchWithStats(ids, documents)
			}
			extraction += stats.ColumnPublishDocumentExtraction
		case "replace":
			if mode == "typed" {
				_, err = col.ReplaceTypedBatch(ids, retained, columns)
			} else {
				_, err = col.UpdateBatch(items)
			}
		case "delete":
			_, err = col.DeleteBatch(ids)
		}
		if err != nil {
			b.Fatal(err)
		}
		start := time.Now()
		if err := col.Flush(); err != nil {
			b.Fatal(err)
		}
		publication += time.Since(start)
		b.StopTimer()
		afterWAL, afterStorage := typedMinimaBenchmarkDiskBytes(b, dir)
		walGrowth += afterWAL - beforeWAL
		storageGrowth += afterStorage - beforeStorage
	}
	totalRows := float64(b.N * rows)
	b.ReportMetric(totalRows/b.Elapsed().Seconds(), "rows/s")
	b.ReportMetric(float64(publication.Nanoseconds())/totalRows, "final-flush-ns/row")
	if operation == "insert" {
		b.ReportMetric(float64(extraction.Nanoseconds())/totalRows, "JSON-extract-ns/row")
	}
	b.ReportMetric(float64(walGrowth)/totalRows, "WAL-B/row")
	b.ReportMetric(float64(storageGrowth)/totalRows, "storage-B/row")
}

func typedMinimaBenchmarkDiskBytes(b *testing.B, dir string) (wal, storage int64) {
	b.Helper()
	walDir := backenddb.WALDirPath(dir)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if strings.HasPrefix(path, walDir+string(filepath.Separator)) {
			wal += info.Size()
		} else {
			storage += info.Size()
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return wal, storage
}
