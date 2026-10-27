package documentserviceowner

import (
	"context"
	"encoding/binary"
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
)

func TestOwnerModesAndLifetime(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
			t.Run(string(profile)+map[bool]string{true: "/legacy", false: "/default"}[legacy], func(t *testing.T) {
				dir := t.TempDir()
				opts := treedb.OptionsFor(profile, dir)
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				o, err := Open(opts, legacy)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := o.Close(); err != nil {
						t.Error(err)
					}
				})
				if got := o.Stats()["treedb.profile.resolved"]; got != string(profile) {
					t.Fatalf("profile=%s", got)
				}
				capture, err := o.Backend.CaptureNativeSemanticState()
				if legacy {
					if err == nil {
						_ = capture.Close()
						t.Fatal("legacy silently activated native owner")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if err := capture.Close(); err != nil {
						t.Fatal(err)
					}
					if got := o.Stats()["treedb.cache.memtable_mode"]; got != "cow_btree" {
						t.Fatalf("mode=%s", got)
					}
				}
				ctx := context.Background()
				req := documentservice.CreateIndexRequest{Name: "docs", Dimension: 2}
				if profile != treedb.ProfileCommandWALDurable {
					before := o.Backend.AcquireSnapshot()
					beforeToken, ok := before.StateToken()
					if !ok {
						t.Fatal("closed initial snapshot")
					}
					nextLSN, files := o.Backend.CommandWALNextLSN(), ownerAdvancedFiles(t, dir)
					var nativeBefore backenddb.NativeSemanticCapture
					if !legacy {
						nativeBefore, err = o.Backend.CaptureNativeSemanticState()
						if err != nil {
							t.Fatal(err)
						}
					}
					_, createErr := o.Service.CreateIndex(ctx, req)
					if createErr == nil || documentservice.ErrorCodeOf(createErr) != documentservice.CodeInternal {
						t.Fatalf("unsupported ordinary ColumnGraph profile accepted: %v", createErr)
					}
					if !legacy && !errors.Is(createErr, backenddb.ErrNativeSemanticUnsupported) {
						t.Fatalf("native profile refusal lost its cause: %v", createErr)
					}
					if legacy {
						var serviceErr *documentservice.Error
						if !errors.As(createErr, &serviceErr) || serviceErr.Err == nil || !strings.Contains(serviceErr.Err.Error(), "requires durable backend") {
							t.Fatalf("legacy profile refusal lost its cause: %v", createErr)
						}
					}
					after := o.Backend.AcquireSnapshot()
					afterToken, ok := after.StateToken()
					if !ok || afterToken != beforeToken || o.Backend.CommandWALNextLSN() != nextLSN {
						t.Fatalf("refusal changed roots/WAL: %+v/%+v next=%d/%d", beforeToken, afterToken, nextLSN, o.Backend.CommandWALNextLSN())
					}
					if !legacy {
						nativeAfter, err := o.Backend.CaptureNativeSemanticState()
						if err != nil {
							t.Fatal(err)
						}
						if nativeAfter.LogicalSequence() != nativeBefore.LogicalSequence() {
							t.Fatalf("refusal changed logical sequence: %d/%d", nativeBefore.LogicalSequence(), nativeAfter.LogicalSequence())
						}
						_ = nativeAfter.Close()
						_ = nativeBefore.Close()
					}
					_ = after.Close()
					_ = before.Close()
					if metas, err := o.Manager.ListCollections(); err != nil || len(metas) != 0 {
						t.Fatalf("refusal retained schema: %+v %v", metas, err)
					}
					if got := ownerAdvancedFiles(t, dir); !reflect.DeepEqual(got, files) {
						t.Fatalf("refusal retained filesystem effects: before=%v after=%v", files, got)
					}
					if _, err := o.Service.OpenIndex(ctx, req.Name); documentservice.ErrorCodeOf(err) != documentservice.CodeIndexNotFound {
						t.Fatalf("refused index visible: %v", err)
					}
				} else {
					first, err := o.Service.CreateIndex(ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					second, err := o.Service.CreateIndex(ctx, req)
					if err != nil || first.Generation != second.Generation || second.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
						t.Fatalf("ordinary create idempotence: %+v %+v %v", first, second, err)
					}
					col, err := o.Manager.OpenCollection(req.Name)
					if err != nil {
						t.Fatal(err)
					}
					wantPayload := collections.ColumnRetainedPayloadFull
					if legacy {
						wantPayload = collections.ColumnRetainedPayloadNonColumn
					}
					if got := col.Meta().Options.ColumnStore.RetainedPayload; got != wantPayload {
						t.Fatalf("owner retained layout=%s want=%s", got, wantPayload)
					}
					if _, err := o.Service.UpsertDocuments(ctx, "docs", documentservice.UpsertDocumentsRequest{Documents: []documentservice.Document{{ID: "a", Content: "refund", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
						t.Fatal(err)
					}
					if _, err := o.Service.OptimizeIndex(ctx, "docs", documentservice.OptimizeIndexRequest{}); err != nil {
						t.Fatal(err)
					}
				}
				if err := o.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := o.Manager.OpenCollection("docs"); err == nil {
					t.Fatal("manager survived owner Close")
				}
				if stats := o.Stats(); len(stats) != 0 {
					t.Fatal("public stats survived owner Close")
				}
			})
		}
	}
}

// This continuity proof uses the same explicit typed serving limits as the
// maintained documentservice typed-input public lifecycle. It exercises the
// command's actual owner composition; it does not expose new HTTP/wire routes.
func TestOwnerLegacyAdvancedAndCloseErrors(t *testing.T) {
	// Match the maintained serving platform requirements without importing a
	// TreeDB/internal package across its Go internal boundary.
	switch runtime.GOOS {
	case "darwin", "linux", "freebsd", "netbsd", "openbsd":
	default:
		t.Skip("typed serving requires exact relative namespace support")
	}
	var endian [2]byte
	binary.NativeEndian.PutUint16(endian[:], 1)
	if endian[0] != 1 {
		t.Skip("typed serving requires little-endian prepared views")
	}
	ctx := context.Background()
	req := documentservice.CreateIndexRequest{Name: "advanced", Dimension: 2, TypedInput: true,
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph},
		ScalarFields:       []documentservice.ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: documentservice.ScalarFieldString}},
	}
	dir := t.TempDir()
	o, err := Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := o.Close(); err != nil {
			t.Error(err)
		}
	})
	if o.Storage != "legacy" || o.Stats()["treedb.profile.resolved"] != string(treedb.ProfileCommandWALDurable) {
		t.Fatal("specialized owner/profile changed", o.Storage, o.Stats())
	}
	info, err := o.Service.CreateIndex(ctx, req)
	if err != nil || !info.TypedInput || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatalf("advanced declaration %+v err=%v", info, err)
	}
	docs := []documentservice.Document{
		{ID: "source#0", Content: "obsolete", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "old"}},
		{ID: "source#1", Content: "obsolete", Embedding: []float32{0, 1}, Meta: map[string]any{"user_id": "old"}},
		{ID: "source#2", Content: "obsolete", Embedding: []float32{1, 1}, Meta: map[string]any{"user_id": "old"}},
		{ID: "outside", Content: "fresh", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "other"}},
	}
	if out, err := o.Service.UpsertDocuments(ctx, req.Name, documentservice.UpsertDocumentsRequest{ExpectedGeneration: info.Generation, Documents: docs, DeferVectorIndexRebuild: true}); err != nil || out.Inserted != 4 {
		t.Fatalf("initial typed mutation %+v err=%v", out, err)
	}
	// These are the established specialized engine's resource limits, not the
	// shared-COW budget or an increased command/open limit.
	limits := ownerAdvancedServingOptions()
	if _, err := o.Service.OptimizeIndex(ctx, req.Name, documentservice.OptimizeIndexRequest{ExpectedGeneration: info.Generation, ColumnGraphAction: "build", ColumnGraphServing: &limits}); err != nil {
		t.Fatal(err)
	}
	old, err := o.Service.SearchDenseVector(ctx, req.Name, documentservice.DenseVectorSearchRequest{ExpectedGeneration: info.Generation, QueryEmbedding: []float32{1, 0}, TopK: 8, Route: documentservice.RouteAnn, Filter: &documentservice.Filter{Field: "meta.user_id", Operator: "==", Value: "old"}})
	if err != nil {
		t.Fatal(err)
	}
	ownerRequireDocumentIDs(t, old.Documents, "source#0", "source#1", "source#2")
	live := []documentservice.Document{
		{ID: "source#0", Content: "fresh", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "keep"}},
		{ID: "source#1", Content: "fresh", Embedding: []float32{0, 1}, Meta: map[string]any{"user_id": "keep"}},
	}
	replaced, err := o.Service.ReplaceSourceByID(ctx, req.Name, documentservice.ReplaceSourceByIDRequest{ExpectedGeneration: info.Generation, DeleteIDs: []string{"source#0", "source#1", "source#2"}, Documents: live})
	if err != nil || replaced.DeletedCount != 3 || replaced.InsertedCount != 2 || replaced.Index.Generation != info.Generation {
		t.Fatalf("source replacement %+v err=%v", replaced, err)
	}
	if _, err := o.Service.OptimizeIndex(ctx, req.Name, documentservice.OptimizeIndexRequest{ExpectedGeneration: info.Generation, ColumnGraphAction: "ensure", ColumnGraphServing: &limits}); err != nil {
		t.Fatal(err)
	}
	serve := func() {
		t.Helper()
		if count, err := o.Service.CountDocuments(ctx, req.Name, documentservice.CountDocumentsRequest{ExpectedGeneration: info.Generation}); err != nil || count.Count != 3 {
			t.Fatalf("replacement count %+v err=%v", count, err)
		}
		stale, err := o.Service.SearchKeyword(ctx, req.Name, documentservice.KeywordSearchRequest{ExpectedGeneration: info.Generation, Query: "obsolete", TopK: 8})
		if err != nil || len(stale.Documents) != 0 {
			t.Fatalf("stale source survived %+v err=%v", stale, err)
		}
		filter := &documentservice.Filter{Field: "meta.user_id", Operator: "==", Value: "keep"}
		dense, err := o.Service.SearchDenseVector(ctx, req.Name, documentservice.DenseVectorSearchRequest{ExpectedGeneration: info.Generation, QueryEmbedding: []float32{1, 0}, TopK: 8, Route: documentservice.RouteAnn, Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		ownerRequireDocumentIDs(t, dense.Documents, "source#0", "source#1")
		if dense.Route != documentservice.RouteAnn || dense.Exact || dense.NativeBasePlusLiveDelta || dense.ColumnGraphPreparedSearch != 1 || dense.DenseWork == nil || !dense.DenseWork.Completed {
			t.Fatalf("specialized Dense route lost %+v", dense)
		}
		hybrid, err := o.Service.SearchHybrid(ctx, req.Name, documentservice.HybridSearchRequest{ExpectedGeneration: info.Generation, Query: "fresh", QueryEmbedding: []float32{1, 0}, TopK: 8, TextCandidateLimit: 8, VectorCandidateLimit: 8, EfSearch: 8, Filter: filter})
		if err != nil {
			t.Fatal(err)
		}
		ownerRequireDocumentIDs(t, hybrid.Documents, "source#0", "source#1")
		if hybrid.TextIndex != info.TextIndexName || hybrid.VectorIndex != info.VectorIndexName || hybrid.Stats.TextCandidatesReturned == 0 || hybrid.Stats.VectorCandidatesReturned == 0 || hybrid.Stats.FullDocumentScanFallbacks != 0 {
			t.Fatalf("mixed Hybrid lost its declared sources %+v", hybrid)
		}
		for _, doc := range hybrid.Documents {
			if doc.Content != "fresh" || doc.Meta["user_id"] != "keep" {
				t.Fatalf("stale/reconstructed typed payload %+v", doc)
			}
		}
	}
	serve()
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir), true)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := o.Service.OpenIndex(ctx, req.Name)
	if err != nil || !reflect.DeepEqual(reopened, info) {
		t.Fatalf("fresh reopen changed persisted declaration before admission: %+v want=%+v err=%v", reopened, info, err)
	}
	// Omission preserves the persisted strategy. Serving re-admits the SAME
	// explicit specialized limits against the stored image, without reseeding.
	reopenReq := req
	reopenReq.VectorIndexOptions = nil
	reopenReq.ColumnGraphServing = &limits
	reopened, err = o.Service.CreateIndex(ctx, reopenReq)
	if err != nil || !reflect.DeepEqual(reopened, info) {
		t.Fatalf("stored strategy/ownership lost %+v err=%v", reopened, err)
	}
	serve()
	closeDB := o.closeDB
	sentinel := errors.New("owner close failure")
	o.closeDB = func() error { return errors.Join(closeDB(), sentinel) }
	closeErr := o.Close()
	o.closeDB = closeDB
	if !errors.Is(closeErr, sentinel) {
		t.Fatalf("Close suppressed error %v", closeErr)
	}
	if _, err := o.Manager.OpenCollection(req.Name); err == nil {
		t.Fatal("advanced manager survived owner Close")
	}
	if stats := o.Stats(); len(stats) != 0 {
		t.Fatal("advanced public stats survived owner Close")
	}

	t.Run("native_refuses_same_declaration_without_effects", func(t *testing.T) {
		nativeDir := t.TempDir()
		opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, nativeDir)
		opts.BackgroundCheckpointInterval = -1
		opts.BackgroundCheckpointIdleDuration = -1
		n, err := Open(opts, false)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := n.Close(); err != nil {
				t.Error(err)
			}
		})
		before, err := n.Backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		defer before.Close()
		beforeToken, ok := before.Snapshot().StateToken()
		if !ok {
			t.Fatal("closed initial capture")
		}
		sequence, nextLSN := before.LogicalSequence(), n.Backend.CommandWALNextLSN()
		files := ownerAdvancedFiles(t, nativeDir)
		if _, err := n.Service.CreateIndex(ctx, req); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
			t.Fatalf("native reinterpreted specialized declaration: %v", err)
		}
		after, err := n.Backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		defer after.Close()
		afterToken, ok := after.Snapshot().StateToken()
		if !ok || after.LogicalSequence() != sequence || afterToken != beforeToken || n.Backend.CommandWALNextLSN() != nextLSN {
			t.Fatalf("refusal published logical/physical/WAL effects: seq %d/%d token %+v/%+v next %d/%d", sequence, after.LogicalSequence(), beforeToken, afterToken, nextLSN, n.Backend.CommandWALNextLSN())
		}
		if metas, err := n.Manager.ListCollections(); err != nil || len(metas) != 0 {
			t.Fatalf("refused schema retained artifacts: %+v err=%v", metas, err)
		}
		if got := ownerAdvancedFiles(t, nativeDir); !reflect.DeepEqual(got, files) {
			t.Fatalf("refusal retained filesystem artifacts before=%v after=%v", files, got)
		}
		if _, err := n.Service.OpenIndex(ctx, req.Name); documentservice.ErrorCodeOf(err) != documentservice.CodeIndexNotFound {
			t.Fatalf("refused index visible: %v", err)
		}
		if err := after.Close(); err != nil {
			t.Fatal(err)
		}
		if err := before.Close(); err != nil {
			t.Fatal(err)
		}
		if err := n.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func ownerRequireDocumentIDs(t *testing.T, docs []documentservice.Document, want ...string) {
	t.Helper()
	remaining := make(map[string]bool, len(want))
	for _, id := range want {
		remaining[id] = true
	}
	if len(docs) != len(want) {
		t.Fatalf("document count=%d want=%v actual=%+v", len(docs), want, docs)
	}
	for _, doc := range docs {
		if !remaining[doc.ID] {
			t.Fatalf("duplicate/unexpected document %q actual=%+v", doc.ID, docs)
		}
		delete(remaining, doc.ID)
	}
}

func ownerAdvancedFiles(t *testing.T, dir string) map[string]int64 {
	t.Helper()
	files := make(map[string]int64)
	if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			files[relative] = -1
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		files[relative] = info.Size()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func ownerAdvancedServingOptions() collections.ColumnGraphServingOptions {
	// Exact public typedServiceTestOptions values from documentservice's maintained
	// typed-input tests. These remain explicit per-engine admission, not COW stats.
	return collections.ColumnGraphServingOptions{
		Publication:     collections.ColumnGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 4096, OwnedBytes: 16 << 20, EncodedOutputBytes: 16 << 20},
		Owners:          collections.ColumnGraphReadOwnerLimits{Owners: 8, States: 8, StateBytes: 128 << 20, AssetBytes: 128 << 20, Cold: collections.ColumnGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 8 << 20, AssetBytes: 64 << 20, DecodedTermBytes: 64 << 20}, Physical: collections.ColumnGraphPhysicalResourceLimits{Segments: 4096, Descriptors: 4096, MappedBytes: 1 << 30, FallbackBytes: 1 << 30, InventoryBytes: 64 << 20}},
		CandidateOutput: collections.ColumnGraphCandidateOutputLimits{Bytes: 1 << 30, AppenderAttempts: 4096},
		Maintenance:     collections.ColumnGraphMaintenanceLimits{NativeEntries: 4096, ColumnSegments: 4096, ManifestRecords: 4096, LifecycleEntries: 4096, NativeBytes: 128 << 20, ColumnBytes: 64 << 20, ManifestBytes: 8 << 20, RetainedBytes: 256 << 20, PagerPages: 32768},
		Filter:          collections.ColumnGraphFilterLimits{SourceIDs: 4096, SourceBytes: 4 << 20, RetainedBytes: 4 << 20, MappingWork: 100000, InspectedEntries: 4096}, FoldRows: 4096, SearchCandidates: 4096,
	}
}
