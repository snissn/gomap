package collections

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Opt-in phase diagnostic, not a latency assertion or a vector-recall benchmark.
// Input generation, empty graph capture, stats collection, and clean Close are
// outside the measured interval. Every timed request is a durable public typed
// upsert of previously absent IDs with two scalar indexes and one text index.
func TestTypedWrite768Diagnostic(t *testing.T) {
	if os.Getenv("GOMAP_WRITE_768_DIAGNOSTIC") != "1" {
		t.Skip("set GOMAP_WRITE_768_DIAGNOSTIC=1 to run the bounded durable-write sweep")
	}
	const calls = 16
	for _, rows := range []int{64, 256, 1024} {
		for _, writers := range []int{1, 4} {
			t.Run(fmt.Sprintf("rows%d/writers%d", rows, writers), func(t *testing.T) {
				meta := typedMinimaCollectionMeta()
				meta.Options.ColumnStore.Columns[0].VectorDims = 768
				meta.VectorIndexes[0].Dimensions = 768
				meta.VectorIndexes[0].M = 16
				dir, db, col := openTypedMinimaCollectionMeta(t, meta)
				defer db.Close()
				preload := 0
				if raw := os.Getenv("GOMAP_WRITE_768_PRELOAD"); raw != "" {
					var err error
					preload, err = strconv.Atoi(raw)
					if err != nil || preload < 0 || preload > 500000 {
						t.Fatal("GOMAP_WRITE_768_PRELOAD must be in [0,500000]")
					}
				}
				for offset := 0; offset < preload; offset += 1024 {
					ids, retained, columns := typedWrite768DiagnosticInput("seed", offset, min(1024, preload-offset))
					if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					t.Fatal(err)
				}
				if err := col.reconcileTypedGraphPublication(
					typedGraphPublicationLimits{Rows: calls * rows, Tombstones: calls * rows, ValueSlots: calls * rows * 4, OwnedBytes: 512 << 20, EncodedOutputBytes: 1 << 30},
					typedGraphColdLimits{ManifestRecords: 512, ManifestBytes: 1 << 20, AssetBytes: 512 << 20, DecodedTermBytes: 64 << 20},
				); err != nil {
					t.Fatal(err)
				}
				type input struct {
					ids, retained [][]byte
					columns       []TypedColumnBatch
				}
				inputs := make([]input, calls)
				for call := range inputs {
					in := &inputs[call]
					in.ids, in.retained, in.columns = typedWrite768DiagnosticInput("write", call*rows, rows)
				}
				type result struct {
					Call   int                   `json:"call"`
					WallNS int64                 `json:"wall_ns"`
					Stats  CollectionInsertStats `json:"stats"`
					err    error
				}
				results := make([]result, calls)
				before := db.Stats()
				start := make(chan struct{})
				var ready, done sync.WaitGroup
				ready.Add(writers)
				done.Add(writers)
				for writer := range writers {
					go func() {
						defer done.Done()
						ready.Done()
						<-start
						for call := writer; call < calls; call += writers {
							in := inputs[call]
							began := time.Now()
							updated, stats, err := col.UpsertTypedBatchWithStats(in.ids, in.retained, in.columns)
							results[call] = result{Call: call, WallNS: time.Since(began).Nanoseconds(), Stats: stats, err: err}
							if err == nil && updated != 0 {
								results[call].err = fmt.Errorf("updated=%d, expected inserts", updated)
							}
							if results[call].err != nil {
								return
							}
						}
					}()
				}
				ready.Wait()
				began := time.Now()
				close(start)
				done.Wait()
				elapsed := time.Since(began)
				after := db.Stats()
				for _, r := range results {
					if r.err != nil || r.WallNS == 0 {
						t.Fatalf("call %d: %v (wall=%d)", r.Call, r.err, r.WallNS)
					}
					encoded, err := json.Marshal(r)
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("write768_request=%s", encoded)
				}
				syncs := func(stats map[string]string) uint64 {
					n, err := strconv.ParseUint(stats["treedb.command_wal.file_sync.calls_total"], 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					return n
				}
				t.Logf("write768_summary preload=%d rows=%d writers=%d calls=%d wall_ns=%d rows_per_second=%.3f wal_file_syncs=%d", preload, rows, writers, calls, elapsed.Nanoseconds(), float64(rows*calls)/elapsed.Seconds(), syncs(after)-syncs(before))
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				// This checks clean-reopen correctness, not simulated power loss.
				reopened := openTypedMinimaDB(t, dir)
				defer reopened.Close()
				got, err := NewCollectionManager(reopened).OpenCollection("minima")
				if err != nil {
					t.Fatal(err)
				}
				for _, in := range inputs {
					for _, id := range [][]byte{in.ids[0], in.ids[len(in.ids)-1]} {
						if doc, err := got.Get(id); err != nil || len(doc) == 0 {
							t.Fatalf("reopen Get(%q): len=%d err=%v", id, len(doc), err)
						}
					}
				}
			})
		}
	}
}

func typedWrite768DiagnosticInput(prefix string, offset, rows int) (ids, retained [][]byte, columns []TypedColumnBatch) {
	ids, retained = make([][]byte, rows), make([][]byte, rows)
	columns = []TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, rows)}, {Name: "content", Strings: make([]string, rows)}, {Name: "user", Strings: make([]string, rows)}, {Name: "path", Strings: make([]string, rows)}}
	for row := range rows {
		id := fmt.Sprintf("%s768-%08d", prefix, offset+row)
		ids[row] = []byte(id)
		retained[row] = []byte(fmt.Sprintf(`{"id":%q}`, id))
		columns[0].Float32Vectors[row] = vectorBenchmarkEmbedding(offset+row, 768)
		columns[1].Strings[row] = "diagnostic content"
		columns[2].Strings[row] = fmt.Sprintf("user-%d", row%4)
		columns[3].Strings[row] = fmt.Sprintf("path-%d", row)
	}
	return
}
