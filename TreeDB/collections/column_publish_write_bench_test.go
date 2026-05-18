package collections

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func BenchmarkColumnStoreCommandWALRootPublicationM10B(b *testing.B) {
	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("insert/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			batches := makeColumnStoreCommandWALBenchBatches(b, 0, b.N, commandWALBenchBatchSize, false)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := collection.InsertBatch(batches[i].ids, batches[i].docs); err != nil {
					b.Fatalf("InsertBatch: %v", err)
				}
			}
			b.StopTimer()

			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, uint64(b.N), uint64(b.N)+1)
			reportColumnStoreCommandWALBenchMetrics(b, batches, true)
		})
	}

	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("update/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			batches := makeColumnStoreCommandWALBenchBatches(b, 0, b.N, commandWALBenchBatchSize, true)
			seedColumnStoreCommandWALBenchBatches(b, collection, batches)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				results, err := collection.UpdateBatch(batches[i].updates)
				if err != nil {
					b.Fatalf("UpdateBatch: %v", err)
				}
				if len(results) != len(batches[i].updates) {
					b.Fatalf("UpdateBatch results=%d, want %d", len(results), len(batches[i].updates))
				}
			}
			b.StopTimer()

			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, uint64(2*b.N), uint64(2*b.N)+1)
			reportColumnStoreCommandWALBenchMetrics(b, batches, true)
		})
	}

	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("delete/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			batches := makeColumnStoreCommandWALBenchBatches(b, 0, b.N, commandWALBenchBatchSize, false)
			seedColumnStoreCommandWALBenchBatches(b, collection, batches)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				deleted, err := collection.DeleteBatch(batches[i].ids)
				if err != nil {
					b.Fatalf("DeleteBatch: %v", err)
				}
				if deleted != len(batches[i].ids) {
					b.Fatalf("DeleteBatch deleted=%d, want %d", deleted, len(batches[i].ids))
				}
			}
			b.StopTimer()

			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, uint64(2*b.N), uint64(2*b.N)+1)
			reportColumnStoreCommandWALBenchMetrics(b, batches, false)
		})
	}
}

func BenchmarkColumnStoreCommandWALReplayM10C(b *testing.B) {
	const (
		frames    = 128
		batchSize = commandWALBenchBatchSize
	)
	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("insert/frames=%d/batch=%d/column_store=%t", frames, batchSize, columnStore), func(b *testing.B) {
			templateDir, docsPerReplay, payloadBytesPerReplay := prepareColumnStoreCommandWALReplayBenchmarkDirM10C(b, columnStore, frames, batchSize)
			wantAppliedLSN := uint64(frames + 1)

			b.ReportAllocs()
			b.SetBytes(int64(payloadBytesPerReplay))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := filepath.Join(b.TempDir(), fmt.Sprintf("replay-%06d", i))
				copyColumnStoreCommandWALReplayBenchmarkDirM10C(b, templateDir, dir)
				b.StartTimer()

				backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
				b.StopTimer()
				if err != nil {
					b.Fatalf("Open replay DB: %v", err)
				}
				if got := backend.State().AppliedCommandLSN; got != wantAppliedLSN {
					_ = backend.Close()
					b.Fatalf("AppliedCommandLSN=%d, want %d", got, wantAppliedLSN)
				}
				if columnStore {
					collection, err := NewCollectionManager(backend).OpenCollection("bench")
					if err != nil {
						_ = backend.Close()
						b.Fatalf("OpenCollection replayed: %v", err)
					}
					assertColumnManifestStateM10B(b, collection, uint64(frames), wantAppliedLSN)
				}
				if err := backend.Close(); err != nil {
					b.Fatalf("Close replay DB: %v", err)
				}
				b.StartTimer()
			}
			b.StopTimer()

			elapsed := b.Elapsed().Seconds()
			if elapsed > 0 {
				b.ReportMetric(float64(docsPerReplay*b.N)/elapsed, "replay_docs/s")
				b.ReportMetric(float64(frames*b.N)/elapsed, "replay_frames/s")
				b.ReportMetric((float64(payloadBytesPerReplay*b.N)/(1024*1024))/elapsed, "payload_MiB/s")
			}
		})
	}
}

type columnStoreCommandWALBenchBatch struct {
	ids      [][]byte
	docs     [][]byte
	updates  []UpdateBatchItem
	idBytes  int
	docBytes int
}

func openColumnStoreCommandWALRootPublicationBenchmark(b *testing.B, columnStore bool) (*backenddb.DB, *Collection) {
	b.Helper()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    b.TempDir(),
		CommandWAL:             true,
		Durability:             backenddb.DurabilityDurable,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		b.Fatalf("open benchmark DB: %v", err)
	}
	b.Cleanup(func() {
		if err := backend.Close(); err != nil {
			b.Errorf("close benchmark DB: %v", err)
		}
	})
	meta := CollectionMeta{
		Name: "bench",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
		},
	}
	if columnStore {
		meta.Options.ColumnStore = testColumnStoreConfig(nil)
	}
	manager := NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&meta); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("bench")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}
	return backend, collection
}

func makeColumnStoreCommandWALBenchBatches(b *testing.B, start, batches, batchSize int, includeUpdates bool) []columnStoreCommandWALBenchBatch {
	b.Helper()
	out := make([]columnStoreCommandWALBenchBatch, batches)
	for i := 0; i < batches; i++ {
		offset := start + i*batchSize
		ids, docs, idBytes, docBytes := columnStoreCommandWALBenchDocuments(offset, batchSize, 0)
		out[i] = columnStoreCommandWALBenchBatch{
			ids:      ids,
			docs:     docs,
			idBytes:  idBytes,
			docBytes: docBytes,
		}
		if includeUpdates {
			_, replacements, _, replacementBytes := columnStoreCommandWALBenchDocuments(offset, batchSize, 1_000_000)
			out[i].updates = make([]UpdateBatchItem, batchSize)
			out[i].docBytes = replacementBytes
			for j := 0; j < batchSize; j++ {
				replacement := replacements[j]
				out[i].updates[j] = UpdateBatchItem{
					DocumentID: ids[j],
					Update: func([]byte) ([]byte, bool, error) {
						return replacement, true, nil
					},
				}
			}
		}
	}
	return out
}

func columnStoreCommandWALBenchDocuments(start, count, timeOffset int) ([][]byte, [][]byte, int, int) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	idBytes := 0
	docBytes := 0
	for i := 0; i < count; i++ {
		n := start + i
		ids[i] = []byte(fmt.Sprintf("e%09d", n))
		docs[i] = []byte(fmt.Sprintf(`{"time_us":%d,"kind":"kind_%d","did":"d%06d"}`, n+timeOffset, n%8, n%1024))
		idBytes += len(ids[i])
		docBytes += len(docs[i])
	}
	return ids, docs, idBytes, docBytes
}

func seedColumnStoreCommandWALBenchBatches(b *testing.B, collection *Collection, batches []columnStoreCommandWALBenchBatch) {
	b.Helper()
	for i := range batches {
		if _, err := collection.InsertBatch(batches[i].ids, batches[i].docs); err != nil {
			b.Fatalf("seed InsertBatch: %v", err)
		}
	}
}

func assertColumnStoreCommandWALBenchState(b *testing.B, backend *backenddb.DB, collection *Collection, columnStore bool, wantGeneration, wantAppliedLSN uint64) {
	b.Helper()
	assertCommandWALBenchModeForMutations(b, backend, true, wantAppliedLSN)
	if columnStore {
		assertColumnManifestStateM10B(b, collection, wantGeneration, wantAppliedLSN)
	}
}

func reportColumnStoreCommandWALBenchMetrics(b *testing.B, batches []columnStoreCommandWALBenchBatch, includeDocs bool) {
	b.Helper()
	docs := 0
	bytes := 0
	for i := range batches {
		docs += len(batches[i].ids)
		bytes += batches[i].idBytes
		if includeDocs {
			bytes += batches[i].docBytes
		}
	}
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(docs)/elapsed, "docs/s")
	b.ReportMetric((float64(bytes)/(1024*1024))/elapsed, "payload_MiB/s")
}

func prepareColumnStoreCommandWALReplayBenchmarkDirM10C(b *testing.B, columnStore bool, frames, batchSize int) (string, int, int) {
	b.Helper()
	dir := b.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		Durability:             backenddb.DurabilityDurable,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		b.Fatalf("open setup DB: %v", err)
	}
	meta := CollectionMeta{
		Name: "bench",
		Options: CollectionOptions{
			DocumentFormat:               DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
		},
	}
	if columnStore {
		meta.Options.ColumnStore = testColumnStoreConfig(nil)
	}
	if _, err := NewCollectionManager(backend).CreateCollection(&meta); err != nil {
		_ = backend.Close()
		b.Fatalf("CreateCollection setup: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		_ = backend.Close()
		b.Fatalf("Checkpoint setup: %v", err)
	}
	if err := backend.Close(); err != nil {
		b.Fatalf("Close setup DB: %v", err)
	}

	totalDocs := 0
	totalPayloadBytes := 0
	for i := 0; i < frames; i++ {
		ids, documents, idBytes, docBytes := columnStoreCommandWALBenchDocuments(i*batchSize, batchSize, 0)
		docs, err := collectionDocumentsFromBatchInput(ids, documents)
		if err != nil {
			b.Fatalf("collectionDocumentsFromBatchInput: %v", err)
		}
		payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("bench", docs)
		if err != nil {
			b.Fatalf("EncodeCollectionInsertBatchByIDPayload: %v", err)
		}
		writeColumnStoreCommandWALReplayBenchmarkFrameM10C(b, dir, uint64(i+2), commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
		totalDocs += len(ids)
		totalPayloadBytes += idBytes + docBytes
	}
	return dir, totalDocs, totalPayloadBytes
}

func writeColumnStoreCommandWALReplayBenchmarkFrameM10C(tb testing.TB, dir string, lsn uint64, kind commitlog.CommandKind, format commitlog.PayloadFormat, payload []byte) {
	tb.Helper()
	walDir := backenddb.WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		tb.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		tb.Fatalf("NewWriter: %v", err)
	}
	if err := w.AppendCommand(commitlog.CommandEnvelope{
		LSN:           lsn,
		Kind:          kind,
		Scope:         commandWALScopeForKind(kind),
		PayloadFormat: format,
		Payload:       payload,
	}); err != nil {
		_ = w.Close()
		tb.Fatalf("AppendCommand: %v", err)
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("Close writer: %v", err)
	}
}

func copyColumnStoreCommandWALReplayBenchmarkDirM10C(tb testing.TB, src, dst string) {
	tb.Helper()
	if err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		return copyColumnStoreCommandWALReplayBenchmarkFileM10C(path, target, info.Mode())
	}); err != nil {
		tb.Fatalf("copy replay benchmark dir: %v", err)
	}
}

func copyColumnStoreCommandWALReplayBenchmarkFileM10C(src, dst string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
