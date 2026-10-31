package collections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/trace"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// Inspect actual active output after timing. Descriptor-derived compression
// accounting describes active output, not an all-history runtime event counter.
func report5345ActiveStorage(b *testing.B, col *Collection, rows int) {
	b.Helper()
	encoded, parts, err := activeStorage5345(col, rows)
	if err != nil {
		b.Fatalf("active physical accounting: %v", err)
	}
	b.Logf("5345_ACTIVE_STORAGE_CENSUS: %s", encoded)
	b.ReportMetric(float64(parts), "active-typed-parts/op")
}

// This is an independent, untimed test consumer, not native row reconstruction.
// Only the captured catalog/manifest view uses the engine's metadata receipt;
// asset reads, image inspection and JSON output are test-owned allocations and
// are not evidence of the engine's total memory bound. Return serialized bytes
// so no borrowed catalog/config/asset-ref strings outlive the captured receipt.
func activeStorage5345(col *Collection, rows int) (encoded []byte, parts int, resultErr error) {
	opts := ColumnStorePhysicalAccountingOptions{DetailedSections: true, ReadIntegrity: ColumnAssetReadIntegrityVerify}
	var accounting ColumnStorePhysicalAccounting
	var err error
	if col.nativeJSON == nil {
		accounting, err = col.ColumnStorePhysicalAccounting(nil, opts)
	} else {
		// Native collections have no legacy db handle. Use exactly one original
		// native capture, including its manifest root and resolved metadata.
		read, err := col.nativeJSON.Capture(col.name)
		if err != nil {
			return nil, 0, fmt.Errorf("native census capture: %w", err)
		}
		defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
		if !nativeTypedMeta(read.meta) || read.collection == nil {
			return nil, 0, backenddb.ErrNativeSemanticUnsupported
		}
		manifest, ok := read.collection.domains.Get("column/manifest")
		if !ok {
			return nil, 0, backenddb.ErrNativeSemanticUnsupported
		}
		metadata, err := nativeTypedReadManifestCapacity(read.capture.Snapshot(), manifest.baseRoot, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("native census manifest capacity: %w", err)
		}
		header, err := read.nativeTypedReadViewCharge()
		if err != nil {
			return nil, 0, err
		}
		if err = nativeTypedCheckedAdd(&metadata, header); err != nil {
			return nil, 0, err
		}
		lease, err := read.capture.NativeAllocator().AdmitNativeBytes(metadata)
		if err != nil {
			return nil, 0, fmt.Errorf("native census metadata admission bytes=%d: %w", metadata, err)
		}
		var wrapper *Collection
		var view columnPhysicalScanSnapshotView
		defer func() {
			accounting = ColumnStorePhysicalAccounting{}
			view = columnPhysicalScanSnapshotView{}
			wrapper = nil
			lease.Close()
		}()
		wrapper = read.nativeTypedReadCollection()
		view, err = wrapper.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(read.capture.Snapshot(), wrapper.catalog, read.meta.Name, manifest.baseRoot, *read.meta.Options.ColumnStore, true, columnManifestScanAllSidecars())
		if err != nil {
			return nil, 0, fmt.Errorf("native census captured view: %w", err)
		}
		accounting, err = physicalAccountingFromScanView(context.Background(), view, opts)
		if err != nil {
			return nil, 0, fmt.Errorf("native census asset inspection: %w", err)
		}
	}
	if err != nil {
		return nil, 0, err
	}
	if !accounting.Complete || accounting.TypedColumnPartRefs == 0 || accounting.TypedColumnPartRefs != len(accounting.TypedColumnParts) {
		return nil, 0, fmt.Errorf("incomplete active typed-column census: complete=%v refs=%d parts=%d", accounting.Complete, accounting.TypedColumnPartRefs, len(accounting.TypedColumnParts))
	}
	activeRows := 0
	for _, part := range accounting.TypedColumnParts {
		if part.Image.Rows != part.Asset.Rows || len(part.Image.CompressionDetail) == 0 || len(part.Image.SerializedSections) == 0 {
			return nil, 0, fmt.Errorf("incomplete active part accounting: %+v", part)
		}
		activeRows += part.Image.Rows
	}
	if activeRows != rows {
		return nil, 0, fmt.Errorf("active typed-column rows=%d want %d", activeRows, rows)
	}
	receipt := struct {
		Basis      string                        `json:"basis"`
		Accounting ColumnStorePhysicalAccounting `json:"accounting"`
	}{"active_persisted_descriptors_not_all_history_events", accounting}
	encoded, err = json.Marshal(receipt)
	receipt.Accounting = ColumnStorePhysicalAccounting{}
	return encoded, len(accounting.TypedColumnParts), err
}

func require5345NoCodecOverrides(b *testing.B) {
	b.Helper()
	for _, name := range []string{
		typedColumnBenchmarkCompressionEnv, typedColumnBenchmarkSectionCompressionEnv,
		typedColumnBenchmarkLocatorCompressionEnv, typedColumnBenchmarkDictionaryCompressionEnv,
		typedColumnBenchmarkPruningCompressionEnv, typedColumnBenchmarkInt64EncodingEnv,
		typedColumnBenchmarkRowsPerGranuleEnv, typedColumnBenchmarkAdaptiveEnabledEnv,
		typedColumnBenchmarkAdaptiveTargetBytesEnv, typedColumnBenchmarkAdaptiveMinRowsEnv,
		typedColumnBenchmarkAdaptiveMaxRowsEnv,
	} {
		if _, ok := os.LookupEnv(name); ok {
			b.Fatalf("benchmark requires absent codec override %s", name)
		}
	}
}

// BenchmarkOrdinaryGraphPublic5345 is a control for the collection-level
// operations underlying documentservice's deferred Optimize. It is not a
// documentservice or hybrid-search benchmark. In particular, the ordinary
// native route does not call typedColumnPublicationFP32EncodedBound, so this
// benchmark cannot establish activation or a performance benefit of that bound.
//
// Run with -benchtime=1x: each sample creates one fresh durable COW database.
// Fixture construction, opening, schema setup, and closing are untimed. The
// timed interval includes all 128-row public inserts, the same standalone sync
// used before Optimize, and the public graph rebuild. A failed operation is a
// failed sample, never a successful low-cost result. After timing, persisted
// active parts expose codec retention/raw fallback. This is not a census of
// every transient allocation, physical write or internal engine fallback.
func BenchmarkOrdinaryGraphPublic5345(b *testing.B) {
	policies := []struct {
		name  string
		codec ColumnStoreTypedColumnCompression
	}{
		{"none", ColumnStoreTypedColumnCompressionNone},
		{"lz4", ColumnStoreTypedColumnCompressionLZ4},
		{"snappy", ColumnStoreTypedColumnCompressionSnappy},
	}
	for _, policy := range policies {
		for _, rows := range []int{256, 257, 4096} {
			b.Run(fmt.Sprintf("%s/rows%d", policy.name, rows), func(b *testing.B) {
				if b.N != 1 {
					b.Fatal("one fresh database per sample: use -benchtime=1x")
				}
				b.StopTimer()
				require5345NoCodecOverrides(b)
				opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, b.TempDir())
				opts.MemtableMode = "cow_btree"
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				owner, err := treedb.Open(opts)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := owner.Close(); err != nil {
						b.Error(err)
					}
				})
				backend, err := owner.NativeSemanticBackend()
				if err != nil {
					b.Fatal(err)
				}
				manager := NewCollectionManager(backend)
				b.Cleanup(func() {
					if err := manager.Close(); err != nil {
						b.Error(err)
					}
				})
				meta := nativeOrdinaryPendingGraphMeta()
				meta.Options.ColumnStore.TypedColumnCompression = policy.codec
				meta.Options.ColumnStore.TypedColumnSectionCompression = policy.codec
				if _, err := manager.CreateCollection(&meta); err != nil {
					b.Fatal(err)
				}
				col, err := manager.OpenCollection("docs")
				if err != nil {
					b.Fatal(err)
				}
				if col.nativeJSON == nil {
					b.Fatal("native collection route unavailable")
				}
				type batch struct {
					ids, docs [][]byte
					vectors   [][]float32
				}
				var batches []batch
				for start := 0; start < rows; start += 128 {
					end := min(start+128, rows)
					var batch batch
					for i := start; i < end; i++ {
						batch.ids = append(batch.ids, []byte(fmt.Sprintf("doc-%04d", i)))
						batch.docs = append(batch.docs, []byte(fmt.Sprintf(`{"content":"content-%04d","embedding":[%d,1],"meta":{"tenant":"tenant-%02d"}}`, i, i+1, i%16)))
						batch.vectors = append(batch.vectors, []float32{float32(i + 1), 1})
					}
					batches = append(batches, batch)
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.StartTimer()
				for _, batch := range batches {
					ids, stats, err := col.InsertBatchWithStatsValidatedFloat32Projection(batch.ids, batch.docs, "embedding", VectorMetricCosine, batch.vectors)
					if err != nil || len(ids) != len(batch.ids) || stats.Documents != len(batch.ids) {
						b.StopTimer()
						b.Fatalf("public insert: returned=%d stats=%+v err=%v", len(ids), stats, err)
					}
				}
				if _, err := manager.SyncForStandaloneWriteConcern(); err != nil {
					b.StopTimer()
					b.Fatalf("pre-Optimize sync: %v", err)
				}
				status, err := col.RebuildVectorIndex("vec")
				b.StopTimer()
				if err != nil || !status.Loaded || status.State != VectorIndexStateColumnGraphLoaded || status.RebuildNeeded || status.Stats.RebuildNeeded || status.Stats.LiveDocs != rows || status.Stats.BytesDisk <= 0 {
					b.Fatalf("public rebuild: status=%+v err=%v", status, err)
				}
				b.ReportMetric(float64(rows), "rows/op")
				b.ReportMetric(float64(len(batches)), "accepted-insert-batches/op")
				b.ReportMetric(1, "loaded-graphs/op")
				report5345ActiveStorage(b, col, rows)
			})
		}
	}
}

// BenchmarkTypedGraphEncodedPublic5345 measures a different path from the
// ordinary native benchmark above: public typed inserts with the legacy
// typed-graph encoded-publication admission enabled. That path calls the
// changed FP32 image bound. Its existing Minima fixture has eight-dimensional
// vectors and four declared columns, so its timings are not comparable to the
// ordinary two-dimensional native workload. Initialization and reconciliation
// are untimed; one whole typed insert is timed. Existing finite typed-overlay
// limits are taken from BenchmarkTypedGraphFold, not the native COW limits.
//
// Unsupported pre-WAL errors are recorded as incomplete samples, never as
// cheap success. This error class does not by itself identify its cause. The
// encoded reservation delta and installed rows support path attribution; they
// are not direct per-call helper or general internal fallback counters.
// A Snappy bound refusal is identified separately by the same metadata-only
// bound returning the identical error, with no assignment, state or debt change.
func BenchmarkTypedGraphEncodedPublic5345(b *testing.B) {
	for _, policy := range []struct {
		name  string
		codec ColumnStoreTypedColumnCompression
	}{
		{"none", ColumnStoreTypedColumnCompressionNone},
		{"lz4", ColumnStoreTypedColumnCompressionLZ4},
		{"snappy", ColumnStoreTypedColumnCompressionSnappy},
	} {
		for _, rows := range []int{256, 257, 4096} {
			b.Run(fmt.Sprintf("%s/rows%d", policy.name, rows), func(b *testing.B) {
				if b.N != 1 {
					b.Fatal("one fresh database per sample: use -benchtime=1x")
				}
				b.StopTimer()
				require5345NoCodecOverrides(b)
				meta := typedMinimaCollectionMeta()
				meta.TextIndexes = nil // Use the existing immediate publication route.
				meta.Options.ColumnStore.TypedColumnCompression = policy.codec
				meta.Options.ColumnStore.TypedColumnSectionCompression = policy.codec
				_, db, col := openTypedMinimaCollectionMeta(b, meta)
				b.Cleanup(func() {
					if err := db.Close(); err != nil {
						b.Error(err)
					}
				})
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					b.Fatalf("initial rebuild: %v", err)
				}
				limits := typedGraphPublicationLimits{Rows: 4096, Tombstones: 4096, ValueSlots: 16384, OwnedBytes: 32 << 20, EncodedOutputBytes: 256 << 20}
				cold := typedGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 4 << 20, AssetBytes: 128 << 20, DecodedTermBytes: 128 << 20}
				if err := col.reconcileTypedGraphPublication(limits, cold); err != nil {
					b.Fatalf("encoded admission bootstrap: %v", err)
				}
				if !col.typedGraphEncodedAdmissionEnabled() {
					b.Fatal("encoded admission inactive")
				}
				beforeState := col.typedGraphPublicationSnapshot()
				if beforeState == nil || beforeState.invalid || beforeState.physicalRows != 0 {
					b.Fatalf("unexpected initial publication state: %+v", beforeState)
				}
				resolvedConfig := col.Meta().Options.ColumnStore
				if resolvedConfig == nil || resolvedConfig.AssetManager == nil || resolvedConfig.TypedColumnCompression != policy.codec || resolvedConfig.TypedColumnSectionCompression != policy.codec {
					b.Fatalf("missing or mismatched resolved column-store policy: %+v", resolvedConfig)
				}
				coord := col.collectionSchemaCoordinator()
				coord.typedPublicationDebtMu.Lock()
				beforeReserved := coord.typedPublicationEncodedBytes
				coord.typedPublicationDebtMu.Unlock()
				ids := make([][]byte, rows)
				retained := make([][]byte, rows)
				columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
				for i := range ids {
					id := fmt.Sprintf("row%04d", i)
					ids[i] = []byte(id)
					retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, id))
					columns[0].Float32Vectors = append(columns[0].Float32Vectors, []float32{1, 0, 0, 0, 0, 0, 0, 0})
					columns[1].Strings = append(columns[1].Strings, "fixture")
					columns[2].Strings = append(columns[2].Strings, "tenant")
					columns[3].Strings = append(columns[3].Strings, "path")
				}
				beforeLSN := db.CommandWALNextLSN()
				var traceFile *os.File
				if dir := os.Getenv("GOMAP_5345_INSERT_TRACE_DIR"); dir != "" {
					if !filepath.IsAbs(dir) {
						b.Fatal("trace directory must be absolute")
					}
					var err error
					traceFile, err = os.CreateTemp(dir, "typed-"+policy.name+"-*.trace")
					if err != nil {
						b.Fatalf("create trace: %v", err)
					}
					if err = trace.Start(traceFile); err != nil {
						if closeErr := traceFile.Close(); closeErr != nil {
							b.Errorf("close trace: %v", closeErr)
						}
						b.Fatalf("start trace: %v", err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.StartTimer()
				var region *trace.Region
				if traceFile != nil {
					region = trace.StartRegion(context.Background(), "5345-public-insert")
				}
				got, stats, err := col.InsertTypedBatchWithStats(ids, retained, columns)
				if region != nil {
					region.End()
				}
				b.StopTimer()
				if traceFile != nil {
					trace.Stop()
					if err := traceFile.Close(); err != nil {
						b.Fatalf("close trace: %v", err)
					}
				}
				if err != nil {
					if policy.codec == ColumnStoreTypedColumnCompressionSnappy {
						_, boundErr := typedColumnPublicationFP32EncodedBound(*resolvedConfig, rows)
						// The zero-row probe identifies the helper's private layout-error
						// singleton without exporting it or matching error text. The
						// adapter can return other errors before reaching that helper.
						_, layoutErr := typedcolumn.FP32ImageEncodedUpperBound(typedcolumn.Options{}, 0, typedcolumn.ColumnPartImageOptions{})
						if layoutErr != nil && errors.Is(boundErr, layoutErr) && errors.Is(err, layoutErr) && !backenddb.CommitPublicationAccepted(err) && db.CommandWALNextLSN() == beforeLSN {
							coord.typedPublicationDebtMu.Lock()
							reserved := coord.typedPublicationEncodedBytes
							coord.typedPublicationDebtMu.Unlock()
							if col.typedGraphPublicationSnapshot() != beforeState || reserved != beforeReserved || len(got) != 0 {
								b.Fatalf("bound refusal changed publication: reserved=%d->%d returned=%d", beforeReserved, reserved, len(got))
							}
							b.Skipf("EXPECTED_CAPABILITY_REFUSAL: Snappy FP32 image bound rejected before assignment without publication or debt change: %v", err)
						}
					}
					if errors.Is(err, ErrHybridSearchUnsupported) && !backenddb.CommitPublicationAccepted(err) && db.CommandWALNextLSN() == beforeLSN {
						b.Skipf("INCOMPLETE: public typed insert refused before command assignment: %v", err)
					}
					b.Fatalf("public typed insert: %v", err)
				}
				if len(got) != rows || stats.Documents != rows {
					b.Fatalf("incomplete typed insert: returned=%d stats=%+v", len(got), stats)
				}
				state := col.typedGraphPublicationSnapshot()
				coord.typedPublicationDebtMu.Lock()
				reserved := coord.typedPublicationEncodedBytes
				coord.typedPublicationDebtMu.Unlock()
				if state == nil || state.invalid || state.physicalRows != rows || reserved <= beforeReserved {
					b.Fatalf("encoded publication not installed: state=%+v reserved=%d->%d", state, beforeReserved, reserved)
				}
				b.ReportMetric(float64(rows), "rows/op")
				b.ReportMetric(1, "accepted-inserts/op")
				b.ReportMetric(float64(reserved-beforeReserved), "reserved-encoded-B/op")
				report5345ActiveStorage(b, col, rows)
			})
		}
	}
}
