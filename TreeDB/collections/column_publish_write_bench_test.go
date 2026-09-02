package collections

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func BenchmarkColumnStoreCommandWALRootPublicationM10B(b *testing.B) {
	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("insert/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			var totals columnStoreCommandWALBenchTotals

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				batch := makeColumnStoreCommandWALBenchBatch(b, i, commandWALBenchBatchSize, false)
				totals.add(batch, true)
				b.StartTimer()
				insertedIDs, err := collection.InsertBatch(batch.ids, batch.docs)
				if err != nil {
					b.Fatalf("InsertBatch: %v", err)
				}
				if len(insertedIDs) != len(batch.ids) {
					b.Fatalf("InsertBatch inserted=%d, want %d", len(insertedIDs), len(batch.ids))
				}
				totals.addPublishStats(collection.LastInsertStats())
			}
			b.StopTimer()

			iterations := uint64(b.N)
			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, iterations, iterations+1)
			reportColumnStoreCommandWALBenchMetrics(b, totals)
		})
	}

	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("update/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			seedColumnStoreCommandWALBenchBatches(b, collection, 0, b.N, commandWALBenchBatchSize)
			var totals columnStoreCommandWALBenchTotals

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				batch := makeColumnStoreCommandWALBenchBatch(b, i, commandWALBenchBatchSize, true)
				totals.add(batch, true)
				b.StartTimer()
				results, err := collection.UpdateBatch(batch.updates)
				if err != nil {
					b.Fatalf("UpdateBatch: %v", err)
				}
				if len(results) != len(batch.updates) {
					b.Fatalf("UpdateBatch results=%d, want %d", len(results), len(batch.updates))
				}
				totals.addPublishStats(collection.LastInsertStats())
			}
			b.StopTimer()

			iterations := uint64(b.N)
			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, 2*iterations, 2*iterations+1)
			reportColumnStoreCommandWALBenchMetrics(b, totals)
		})
	}

	for _, columnStore := range []bool{false, true} {
		b.Run(fmt.Sprintf("delete/column_store=%t", columnStore), func(b *testing.B) {
			backend, collection := openColumnStoreCommandWALRootPublicationBenchmark(b, columnStore)
			seedColumnStoreCommandWALBenchBatches(b, collection, 0, b.N, commandWALBenchBatchSize)
			var totals columnStoreCommandWALBenchTotals

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				batch := makeColumnStoreCommandWALBenchBatch(b, i, commandWALBenchBatchSize, false)
				totals.add(batch, false)
				b.StartTimer()
				deleted, err := collection.DeleteBatch(batch.ids)
				if err != nil {
					b.Fatalf("DeleteBatch: %v", err)
				}
				if deleted != len(batch.ids) {
					b.Fatalf("DeleteBatch deleted=%d, want %d", deleted, len(batch.ids))
				}
				totals.addPublishStats(collection.LastInsertStats())
			}
			b.StopTimer()

			iterations := uint64(b.N)
			assertColumnStoreCommandWALBenchState(b, backend, collection, columnStore, 2*iterations, 2*iterations+1)
			reportColumnStoreCommandWALBenchMetrics(b, totals)
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
			b.StopTimer()
			templateDir, docsPerReplay, encodedPayloadBytesPerReplay, wantAppliedLSN := prepareColumnStoreCommandWALReplayBenchmarkDirM10C(b, columnStore, frames, batchSize)
			workRoot := b.TempDir()

			b.ReportAllocs()
			b.SetBytes(int64(encodedPayloadBytesPerReplay))
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				workDir := filepath.Join(workRoot, fmt.Sprintf("replay-work-%06d", i))
				copyColumnStoreCommandWALReplayBenchmarkDirM10C(b, templateDir, workDir)
				b.StartTimer()
				backend, err := backenddb.Open(backenddb.Options{
					Dir:                    workDir,
					CommandWAL:             true,
					Durability:             backenddb.DurabilityDurable,
					DisableBackgroundPrune: true,
				})
				if err != nil {
					b.StopTimer()
					b.Fatalf("Open replay DB: %v", err)
				}
				if got := backend.State().AppliedCommandLSN; got != wantAppliedLSN {
					b.StopTimer()
					_ = backend.Close()
					b.Fatalf("AppliedCommandLSN=%d, want %d", got, wantAppliedLSN)
				}
				if columnStore {
					mgr := NewCollectionManager(backend)
					collection, err := mgr.OpenCollection("bench")
					if err != nil {
						b.StopTimer()
						_ = backend.Close()
						b.Fatalf("OpenCollection replayed: %v", err)
					}
					assertColumnManifestStateNoReopenM10C(b, collection, uint64(frames), wantAppliedLSN)
				}
				b.StopTimer()
				if err := backend.Close(); err != nil {
					b.Fatalf("Close replay DB: %v", err)
				}
				if err := os.RemoveAll(workDir); err != nil {
					b.Fatalf("Remove replay work dir: %v", err)
				}
			}

			elapsed := b.Elapsed().Seconds()
			if elapsed > 0 {
				iterations := float64(b.N)
				b.ReportMetric(float64(docsPerReplay)*iterations/elapsed, "replay_docs/s")
				b.ReportMetric(float64(frames)*iterations/elapsed, "replay_frames/s")
				b.ReportMetric((float64(encodedPayloadBytesPerReplay)*iterations/(1024*1024))/elapsed, "encoded_payload_MiB/s")
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

type columnStoreCommandWALBenchTotals struct {
	docs             int
	bytes            int
	commit           time.Duration
	orderedRootApply time.Duration
	finalize         time.Duration
	candidateBuild   time.Duration
	admissionWait    time.Duration
}

func (totals *columnStoreCommandWALBenchTotals) add(batch columnStoreCommandWALBenchBatch, includeDocs bool) {
	totals.docs += len(batch.ids)
	totals.bytes += batch.idBytes
	if includeDocs {
		totals.bytes += batch.docBytes
	}
}

func (totals *columnStoreCommandWALBenchTotals) addPublishStats(stats CollectionInsertStats) {
	totals.commit += stats.ColumnPublishCommit
	totals.orderedRootApply += stats.ColumnPublishOrderedRootApply
	totals.finalize += stats.ColumnPublishFinalize
	totals.candidateBuild += stats.ColumnPublishFinalizeCandidateBuild
	totals.admissionWait += stats.ColumnPublishFinalizeAdmissionWait
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

func makeColumnStoreCommandWALBenchBatch(b *testing.B, batchIndex, batchSize int, includeUpdates bool) columnStoreCommandWALBenchBatch {
	b.Helper()
	offset := batchIndex * batchSize
	ids, docs, idBytes, docBytes := columnStoreCommandWALBenchDocuments(offset, batchSize, 0)
	batch := columnStoreCommandWALBenchBatch{
		ids:      ids,
		docs:     docs,
		idBytes:  idBytes,
		docBytes: docBytes,
	}
	if includeUpdates {
		_, replacements, _, replacementBytes := columnStoreCommandWALBenchDocuments(offset, batchSize, 1_000_000)
		batch.updates = make([]UpdateBatchItem, batchSize)
		batch.docBytes = replacementBytes
		for j := 0; j < batchSize; j++ {
			replacement := replacements[j]
			batch.updates[j] = UpdateBatchItem{
				DocumentID: ids[j],
				Update: func([]byte) ([]byte, bool, error) {
					return replacement, true, nil
				},
			}
		}
	}
	return batch
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

func seedColumnStoreCommandWALBenchBatches(b *testing.B, collection *Collection, start, batches, batchSize int) {
	b.Helper()
	for i := 0; i < batches; i++ {
		batch := makeColumnStoreCommandWALBenchBatch(b, start+i, batchSize, false)
		if _, err := collection.InsertBatch(batch.ids, batch.docs); err != nil {
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

func reportColumnStoreCommandWALBenchMetrics(b *testing.B, totals columnStoreCommandWALBenchTotals) {
	b.Helper()
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(totals.docs)/elapsed, "docs/s")
	b.ReportMetric((float64(totals.bytes)/(1024*1024))/elapsed, "payload_MiB/s")
	if b.N > 0 {
		iterations := float64(b.N)
		b.ReportMetric(float64(totals.commit.Nanoseconds())/iterations, "publish_commit_ns/op")
		b.ReportMetric(float64(totals.orderedRootApply.Nanoseconds())/iterations, "ordered_root_apply_ns/op")
		b.ReportMetric(float64(totals.finalize.Nanoseconds())/iterations, "finalize_ns/op")
		b.ReportMetric(float64(totals.candidateBuild.Nanoseconds())/iterations, "candidate_build_ns/op")
		b.ReportMetric(float64(totals.admissionWait.Nanoseconds())/iterations, "admission_wait_ns/op")
	}
}

func prepareColumnStoreCommandWALReplayBenchmarkDirM10C(b *testing.B, columnStore bool, frames, batchSize int) (string, int, int, uint64) {
	b.Helper()
	dir := b.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		b.Fatalf("SaveFormatConfig setup: %v", err)
	}
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
	baseAppliedLSN := backend.State().AppliedCommandLSN
	if baseAppliedLSN == 0 {
		_ = backend.Close()
		b.Fatal("setup AppliedCommandLSN=0, want command WAL create LSN before replay frames")
	}
	if err := backend.Close(); err != nil {
		b.Fatalf("Close setup DB: %v", err)
	}

	walDir := backenddb.WALDirPath(dir)
	if err := os.RemoveAll(walDir); err != nil {
		b.Fatalf("RemoveAll wal: %v", err)
	}
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		b.Fatalf("MkdirAll wal: %v", err)
	}
	// The replay fixture starts from an empty command-WAL directory after a
	// checkpoint, so the first synthetic segment must use the commitlog's
	// initial segment name.
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
	totalDocs, totalEncodedPayloadBytes, err := writeColumnStoreCommandWALReplayFramesM10C(path, baseAppliedLSN, frames, batchSize)
	if err != nil {
		b.Fatalf("write command WAL replay frames: %v", err)
	}
	return dir, totalDocs, totalEncodedPayloadBytes, baseAppliedLSN + uint64(frames)
}

func writeColumnStoreCommandWALReplayFramesM10C(path string, baseAppliedLSN uint64, frames, batchSize int) (totalDocs int, totalEncodedPayloadBytes int, err error) {
	w, err := commitlog.NewWriter(path)
	if err != nil {
		return 0, 0, fmt.Errorf("new writer: %w", err)
	}
	defer func() {
		if closeErr := w.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close writer: %w", closeErr))
		}
	}()
	for i := 0; i < frames; i++ {
		ids, documents, _, _ := columnStoreCommandWALBenchDocuments(i*batchSize, batchSize, 0)
		docs, err := collectionDocumentsFromBatchInput(ids, documents)
		if err != nil {
			return 0, 0, fmt.Errorf("collection documents from batch input: %w", err)
		}
		payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("bench", docs)
		if err != nil {
			return 0, 0, fmt.Errorf("encode collection insert batch by ID payload: %w", err)
		}
		if err := w.AppendCommand(commitlog.CommandEnvelope{
			Version:         commitlog.CommandFrameVersionV2,
			LSN:             baseAppliedLSN + uint64(i) + 1,
			DurabilityClass: commitlog.CommandDurabilityDurable,
			Kind:            commitlog.CommandKindCollectionInsertBatchByID,
			Scope:           commitlog.CommandScopeCollection,
			PayloadFormat:   commitlog.PayloadFormatCollectionInsertBatchByIDV1,
			Payload:         payload,
		}); err != nil {
			return 0, 0, fmt.Errorf("append command: %w", err)
		}
		totalDocs += len(ids)
		totalEncodedPayloadBytes += len(payload)
	}
	return totalDocs, totalEncodedPayloadBytes, nil
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
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("symlink entries are not supported in replay benchmark fixtures: %s", path)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("non-regular entries are not supported in replay benchmark fixtures: %s", path)
		}
		return copyColumnStoreCommandWALReplayBenchmarkFileM10C(path, target, info.Mode().Perm())
	}); err != nil {
		tb.Fatalf("copy replay benchmark dir: %v", err)
	}
}

func copyColumnStoreCommandWALReplayBenchmarkFileM10C(src, dst string, mode fs.FileMode) (err error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := in.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := out.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	_, err = io.Copy(out, in)
	return err
}

func assertColumnManifestStateNoReopenM10C(tb testing.TB, col *Collection, generation, appliedLSN uint64) {
	tb.Helper()
	meta := col.Meta()
	cfg := meta.Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil {
		tb.Fatalf("missing active column manifest metadata: %+v", cfg)
	}
	if cfg.ActiveManifest.Generation != generation {
		tb.Fatalf("active generation=%d, want %d", cfg.ActiveManifest.Generation, generation)
	}
	if cfg.ActiveManifest.Format != columnManifestFormatTCS1 || cfg.ActiveManifest.Version != columnManifestIdentityVersion || cfg.ActiveManifest.Checksum == 0 {
		tb.Fatalf("invalid active manifest identity: %+v", cfg.ActiveManifest)
	}
	if cfg.RecoveryAuthoritativeManifest == nil || !columnManifestIdentityValueEqual(*cfg.RecoveryAuthoritativeManifest, *cfg.ActiveManifest) {
		tb.Fatalf("recovery-authoritative manifest mismatch: %+v active=%+v", cfg.RecoveryAuthoritativeManifest, cfg.ActiveManifest)
	}
	if cfg.RecoveryAuthoritativeAppliedCommandLSN != appliedLSN {
		tb.Fatalf("recovery AppliedCommandLSN=%d, want %d", cfg.RecoveryAuthoritativeAppliedCommandLSN, appliedLSN)
	}
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok {
		tb.Fatalf("ColumnStoreCacheIdentity ok=false")
	}
	if id.ManifestRoot == 0 {
		tb.Fatalf("ManifestRoot=0, want non-zero")
	}
	if id.ManifestGeneration != generation || id.RecoveryAuthoritativeGeneration != generation || id.RecoveryAuthoritativeAppliedCommandLSN != appliedLSN {
		tb.Fatalf("unexpected cache identity: %+v want generation=%d appliedLSN=%d", id, generation, appliedLSN)
	}
	snap := col.db.AcquireSnapshot()
	if snap == nil {
		tb.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	entry, err := snap.GetEntryAtRoot(id.ManifestRoot, []byte(columnManifestIdentityRecordKey))
	if err != nil {
		tb.Fatalf("GetEntryAtRoot manifest identity: %v", err)
	}
	record, err := decodeColumnManifestIdentityRecord(entry.Value)
	if err != nil {
		tb.Fatalf("decodeColumnManifestIdentityRecord: %v", err)
	}
	if record.Generation != generation || record.Version != columnManifestIdentityVersion || record.Checksum != cfg.ActiveManifest.Checksum {
		tb.Fatalf("manifest root record=%+v active=%+v", record, cfg.ActiveManifest)
	}
}
