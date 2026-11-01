package documentservice

import (
	"context"
	"errors"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Maintenance requires a physical root seal before graph work. A journal-only
// fence would skip this real index-sync failure and incorrectly begin the work.
func TestNativeJournalMaintenanceRequiresPhysicalCheckpoint(t *testing.T) {
	for _, action := range []string{"ordinary", "build", "ensure", "fold", "renew"} {
		t.Run(action, func(t *testing.T) {
			// Match the existing ordinary/typed native service fixtures: native
			// ColumnGraph declarations require the durable profile.
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			svc := New(manager)
			failure := errors.New("original maintenance physical index sync refused")
			t.Cleanup(func() {
				if err := svc.Close(); err != nil {
					t.Error(err)
				}
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
				if err := owner.Close(); err != nil {
					if !errors.Is(err, failure) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
						t.Error(err)
					} else {
						t.Logf("checked original failed-checkpoint owner Close: %v", err)
					}
				}
			})
			if !manager.UsesNativeCommandWALApply() || svc.deferredVectorBuildMaintenance != nil {
				t.Fatal("fixture did not select original native owner without deferred maintenance")
			}
			ctx := context.Background()
			create := CreateIndexRequest{Name: "docs", Dimension: 8, TypedInput: action != "ordinary", VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2}}
			if _, err := svc.CreateIndex(ctx, create); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "checkpoint must precede graph work", Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
				t.Fatal(err)
			}
			col, info, err := svc.openIndex(ctx, "docs", 0)
			if err != nil {
				t.Fatal(err)
			}
			before, err := col.VectorIndexStatus(info.VectorIndexName)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == durabilitycut.BeforeIndexDataSync && event.Resource == durabilitycut.ResourceIndex {
					calls++
					return failure
				}
				return nil
			})
			req := OptimizeIndexRequest{}
			if action != "ordinary" {
				req.ColumnGraphAction = action
			}
			if action == "build" || action == "ensure" {
				serving := typedServiceTestOptions()
				req.ColumnGraphServing = &serving
			}
			_, err = svc.OptimizeIndex(ctx, "docs", req)
			restore()
			if calls != 1 || !errors.Is(err, failure) || ErrorCodeOf(err) != CodeInternal {
				t.Fatalf("physical checkpoint calls=%d code=%s err=%v", calls, ErrorCodeOf(err), err)
			}
			after, err := col.VectorIndexStatus(info.VectorIndexName)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("graph work followed failed checkpoint: before=%+v after=%+v err=%v", before, after, err)
			}
		})
	}
}

// The classic manager has actual buffered collection publication to drain.
// Refuse that publication's auxiliary asset sync, before either migrated
// no-deferred maintenance caller may checkpoint or begin graph work.
func TestNativeJournalMaintenanceClassicFlushFailureBeforeWork(t *testing.T) {
	for _, typed := range []bool{false, true} {
		name := "ordinary"
		if typed {
			name = "typed_build"
		}
		t.Run(name, func(t *testing.T) {
			backend, err := backenddb.Open(testBackendOptions(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			svc := New(manager)
			failure := errors.New("classic buffered maintenance asset sync refused")
			t.Cleanup(func() {
				for _, close := range []func() error{svc.Close, manager.Close, backend.Close} {
					if err := close(); err != nil {
						if !errors.Is(err, failure) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
							t.Error(err)
						} else {
							t.Logf("checked original failed-flush cleanup: %v", err)
						}
					}
				}
			})
			if manager.UsesNativeCommandWALApply() || svc.deferredVectorBuildMaintenance != nil {
				t.Fatal("fixture must retain classic buffered/no-deferred maintenance")
			}
			ctx := context.Background()
			if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 8, TypedInput: typed, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2}}); err != nil {
				t.Fatal(err)
			}
			// Reuse the ordinary service's existing buffered-insert fixture on
			// this SAME manager. Typed upsert itself publishes atomically, so
			// its maintenance caller must also drain the manager's other domain.
			if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "buffered", Dimension: 2, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2}}); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.UpsertDocuments(ctx, "buffered", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "retained classic buffered row", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
				t.Fatal(err)
			}
			col, info, err := svc.openIndex(ctx, "docs", 0)
			if err != nil {
				t.Fatal(err)
			}
			// Only inspect the unbuffered target. VectorIndexStatus on the
			// buffered domain would itself drain the publication under test.
			beforeGraph, err := col.VectorIndexStatus(info.VectorIndexName)
			if err != nil {
				t.Fatal(err)
			}
			before := manager.StatsSnapshot()
			if before.PendingRootRuns == 0 || before.IndexedFlushCalls != 0 {
				t.Fatalf("missing actual classic buffered publication: pending_root_runs=%d flush_calls=%d", before.PendingRootRuns, before.IndexedFlushCalls)
			}
			assetSyncs, rootSeals := 0, 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == durabilitycut.BeforeIndexDataSync || event.Point == durabilitycut.BeforePublicationSealWrite || event.Point == durabilitycut.BeforeMetaWrite {
					rootSeals++
				}
				if event.Resource == durabilitycut.ResourceAuxiliary && event.Point == durabilitycut.BeforeDependencyFileSync {
					assetSyncs++
					return failure
				}
				return nil
			})
			req := OptimizeIndexRequest{}
			if typed {
				serving := typedServiceTestOptions()
				req.ColumnGraphAction = "build"
				req.ColumnGraphServing = &serving
			}
			_, err = svc.OptimizeIndex(ctx, "docs", req)
			restore()
			if !errors.Is(err, failure) || ErrorCodeOf(err) != CodeInternal || assetSyncs != 1 || rootSeals != 0 {
				t.Fatalf("flush short circuit: asset_syncs=%d root_seals=%d code=%s err=%v", assetSyncs, rootSeals, ErrorCodeOf(err), err)
			}
			after := manager.StatsSnapshot()
			if after.IndexedFlushCalls-before.IndexedFlushCalls != 1 || after.IndexedFlushErrors-before.IndexedFlushErrors != 1 || after.PendingRootRuns == 0 {
				t.Fatalf("failed actual buffered drain: flush_calls=%d->%d flush_errors=%d->%d pending_root_runs=%d", before.IndexedFlushCalls, after.IndexedFlushCalls, before.IndexedFlushErrors, after.IndexedFlushErrors, after.PendingRootRuns)
			}
			afterGraph, err := col.VectorIndexStatus(info.VectorIndexName)
			if err != nil || !reflect.DeepEqual(beforeGraph, afterGraph) {
				t.Fatalf("graph work followed failed FlushAll: before=%+v after=%+v err=%v", beforeGraph, afterGraph, err)
			}
		})
	}
}
