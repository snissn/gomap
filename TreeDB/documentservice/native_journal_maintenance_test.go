package documentservice

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// Maintenance requires a physical root seal before graph work. The ordinary
// arm owns the original native public DB; typed service declarations use the
// existing eligible classic backend, without deferred maintenance in either arm.
func TestNativeJournalMaintenanceRequiresPhysicalCheckpoint(t *testing.T) {
	for _, action := range []string{"ordinary_native", "build", "ensure", "fold", "renew"} {
		t.Run(action, func(t *testing.T) {
			failure := errors.New("original maintenance physical index sync refused")
			var backend *backenddb.DB
			var manager *collections.CollectionManager
			var svc *Service
			var dirty func([]byte, []byte) error
			if action == "ordinary_native" {
				opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
				opts.MemtableMode = "cow_btree"
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				owner, err := treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { checkNativeJournalMaintenanceClose(t, owner.Close, failure) })
				backend, err = owner.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager = collections.NewCollectionManager(backend)
				svc = New(manager)
				t.Cleanup(func() {
					checkNativeJournalMaintenanceClose(t, svc.Close, failure)
					checkNativeJournalMaintenanceClose(t, manager.Close, failure)
				})
				if !manager.UsesNativeCommandWALApply() {
					t.Fatal("ordinary fixture lost its original native owner")
				}
				dirty = owner.Set
			} else {
				svc, backend, manager = newNativeJournalMaintenanceClassic(t, failure, nil)
				dirty = backend.Set
			}
			if svc.deferredVectorBuildMaintenance != nil {
				t.Fatal("fixture selected deferred maintenance")
			}
			col, info := prepareNativeJournalMaintenanceTarget(t, svc, action != "ordinary_native", action != "ensure")
			if err := manager.FlushAll(); err != nil {
				t.Fatal(err)
			}
			if err := manager.CheckpointStorage(); err != nil {
				t.Fatal(err)
			}
			beforeCache := nativeJournalMaintenanceCache(svc)
			if beforeCache == nil {
				t.Fatal("missing built target cache owner")
			}
			beforeServing, servingAvailable := nativeJournalMaintenanceServing(col)
			if (action == "ensure" && servingAvailable) || (action != "ordinary_native" && action != "ensure" && (!servingAvailable || !beforeServing.Ready)) {
				t.Fatalf("wrong serving admission for %s: available=%t identity=%+v", action, servingAvailable, beforeServing)
			}
			beforeDirty := backend.CommandWALNextLSN()
			if err := dirty([]byte("maintenance-physical-marker"), []byte("same original storage owner")); err != nil {
				t.Fatal(err)
			}
			nextLSN := backend.CommandWALNextLSN()
			if nextLSN <= beforeDirty {
				t.Fatalf("dirty original owner did not append: next_lsn=%d->%d", beforeDirty, nextLSN)
			}
			beforeWork := workstats.Read().Fold
			root, path := backend.Dir(), filepath.Join(backend.Dir(), "index.db")
			var mu sync.Mutex
			var events []string
			var optimizing atomic.Bool
			calls, stable := 0, false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if !optimizing.Load() || event.Root != root || event.Path != path {
					return nil
				}
				mu.Lock()
				defer mu.Unlock()
				events = append(events, fmt.Sprintf("optimize:%s:%s:lsn=%d", event.Point, event.Resource, event.LSN))
				if event.Point == durabilitycut.BeforeIndexDataSync && event.Resource == durabilitycut.ResourceIndex {
					calls++
					stable = event.StablePath != nil
					return failure
				}
				return nil
			})
			req := OptimizeIndexRequest{}
			if action != "ordinary_native" {
				req.ColumnGraphAction = action
				if action == "build" || action == "ensure" {
					options := typedServiceTestOptions()
					req.ColumnGraphServing = &options
				}
			}
			optimizing.Store(true)
			_, err := svc.OptimizeIndex(context.Background(), info.Name, req)
			optimizing.Store(false)
			restore()
			mu.Lock()
			t.Logf("original physical prerequisite: dirty_next_lsn=%d->%d calls=%d stable_handle=%t events=%v", beforeDirty, nextLSN, calls, stable, events)
			if calls != 1 || !stable || !errors.Is(err, failure) || ErrorCodeOf(err) != CodeInternal {
				t.Errorf("physical checkpoint calls=%d stable=%t code=%s err=%v", calls, stable, ErrorCodeOf(err), err)
			}
			mu.Unlock()
			if got := backend.CommandWALNextLSN(); got != nextLSN {
				t.Errorf("failed maintenance appended a command: next_lsn=%d->%d", nextLSN, got)
			}
			if nativeJournalMaintenanceCache(svc) != beforeCache {
				t.Error("graph work invalidated/replaced the target cache after failed checkpoint")
			}
			afterServing, afterAvailable := nativeJournalMaintenanceServing(col)
			if servingAvailable != afterAvailable || beforeServing != afterServing {
				t.Errorf("serving changed after failed checkpoint: before=%+v/%t after=%+v/%t", beforeServing, servingAvailable, afterServing, afterAvailable)
			}
			afterWork := workstats.Read().Fold
			if beforeWork.Public.Attempts != afterWork.Public.Attempts || beforeWork.Renew.Attempts != afterWork.Renew.Attempts || beforeWork.Build.Attempts != afterWork.Build.Attempts {
				t.Errorf("graph work entered after failed checkpoint: before=%+v after=%+v", beforeWork, afterWork)
			}
		})
	}
}

// Each Optimize invocation has a fresh matched direct-FlushAll control. An
// early raw-publish guard distinguishes explicit FlushAll from Checkpoint's
// implicit manager drain. It is registered before the manager's own barrier.
func TestNativeJournalMaintenanceClassicFlushFailureBeforeWork(t *testing.T) {
	for _, typed := range []bool{false, true} {
		name := "ordinary"
		if typed {
			name = "typed_build"
		}
		t.Run(name, func(t *testing.T) {
			var directPrefix []string
			for _, mode := range []string{"direct_flush", "optimize"} {
				t.Run(mode, func(t *testing.T) {
					failure := errors.New("classic buffered maintenance asset sync refused")
					guardFailure := errors.New("checkpoint entered before explicit buffered drain")
					var armed atomic.Bool
					var guardCalls atomic.Uint64
					svc, backend, manager := newNativeJournalMaintenanceClassic(t, failure, func(db *backenddb.DB) {
						unregister := db.RegisterCommandWALRawPublishBarrier(func() error {
							if armed.Load() {
								guardCalls.Add(1)
								return guardFailure
							}
							return nil
						})
						t.Cleanup(func() { armed.Store(false); unregister() })
					})
					col, info := prepareNativeJournalMaintenanceTarget(t, svc, typed, true)
					ctx := context.Background()
					if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "buffered", Dimension: 2, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2}}); err != nil {
						t.Fatal(err)
					}
					if err := manager.FlushAll(); err != nil {
						t.Fatal(err)
					}
					if err := manager.CheckpointStorage(); err != nil {
						t.Fatal(err)
					}
					// Typed upserts publish atomically. This separate ordinary
					// domain supplies real buffered debt on the SAME manager.
					if _, err := svc.UpsertDocuments(ctx, "buffered", UpsertDocumentsRequest{Documents: []Document{{ID: "pending", Content: "retained buffered row", Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
						t.Fatal(err)
					}
					before := manager.StatsSnapshot()
					if before.PendingRootRuns == 0 {
						t.Fatal("missing actual classic buffered publication")
					}
					beforeCache := nativeJournalMaintenanceCache(svc)
					beforeServing, beforeAvailable := nativeJournalMaintenanceServing(col)
					if beforeCache == nil || (typed && (!beforeAvailable || !beforeServing.Ready)) {
						t.Fatal("target graph/cache was not warmed before buffered failure")
					}
					beforeWork := workstats.Read().Fold
					nextLSN := backend.CommandWALNextLSN()
					var mu sync.Mutex
					var events, prefix []string
					assetSyncs, postFailurePhysical := 0, 0
					failed := false
					restore := durabilitycut.Install(func(event durabilitycut.Event) error {
						if !armed.Load() {
							return nil
						}
						physical := event.Point == durabilitycut.BeforeIndexDataSync || event.Point == durabilitycut.BeforePublicationSealWrite || event.Point == durabilitycut.BeforeMetaWrite || event.Point == durabilitycut.BeforeMetaSync
						auxiliary := event.Resource == durabilitycut.ResourceAuxiliary && event.Point == durabilitycut.BeforeDependencyFileSync
						if !physical && !auxiliary {
							return nil
						}
						mu.Lock()
						defer mu.Unlock()
						relRoot, _ := filepath.Rel(backend.Dir(), event.Root)
						relPath, _ := filepath.Rel(backend.Dir(), event.Path)
						events = append(events, fmt.Sprintf("%d:after_fault=%t:%s:%s:root=%s:path=%s:lsn=%d", len(events), failed, event.Point, event.Resource, relRoot, relPath, event.LSN))
						if failed && physical {
							postFailurePhysical++
						}
						if !failed {
							prefix = append(prefix, string(event.Point)+":"+string(event.Resource))
						}
						if auxiliary {
							assetSyncs++
							failed = true
							return failure
						}
						return nil
					})
					armed.Store(true)
					var err error
					if mode == "direct_flush" {
						err = manager.FlushAll()
					} else {
						req := OptimizeIndexRequest{}
						if typed {
							options := typedServiceTestOptions()
							req.ColumnGraphAction, req.ColumnGraphServing = "build", &options
						}
						_, err = svc.OptimizeIndex(ctx, info.Name, req)
					}
					armed.Store(false)
					restore()
					mu.Lock()
					t.Logf("%s flush transcript: %v", mode, events)
					if assetSyncs != 1 || postFailurePhysical != 0 || guardCalls.Load() != 0 || !errors.Is(err, failure) || errors.Is(err, guardFailure) || (mode == "optimize" && ErrorCodeOf(err) != CodeInternal) {
						t.Errorf("flush prerequisite: asset_syncs=%d post_fault_physical=%d checkpoint_guard=%d code=%s err=%v", assetSyncs, postFailurePhysical, guardCalls.Load(), ErrorCodeOf(err), err)
					}
					if mode == "direct_flush" {
						directPrefix = append([]string(nil), prefix...)
					} else if !reflect.DeepEqual(directPrefix, prefix) {
						t.Errorf("Optimize prerequisite differs from matched direct FlushAll: direct=%v optimize=%v (ordered paths above)", directPrefix, prefix)
					}
					mu.Unlock()
					after := manager.StatsSnapshot()
					if after.IndexedFlushCalls-before.IndexedFlushCalls != 1 || after.IndexedFlushErrors-before.IndexedFlushErrors != 1 || after.PendingRootRuns != before.PendingRootRuns {
						t.Errorf("failed actual drain: calls=%d->%d errors=%d->%d pending=%d->%d", before.IndexedFlushCalls, after.IndexedFlushCalls, before.IndexedFlushErrors, after.IndexedFlushErrors, before.PendingRootRuns, after.PendingRootRuns)
					}
					if got := backend.CommandWALNextLSN(); got != nextLSN {
						t.Errorf("failed assigned drain appended a command: next_lsn=%d->%d", nextLSN, got)
					}
					if nativeJournalMaintenanceCache(svc) != beforeCache {
						t.Error("graph work invalidated/replaced the target cache after failed FlushAll")
					}
					afterServing, afterAvailable := nativeJournalMaintenanceServing(col)
					if beforeAvailable != afterAvailable || beforeServing != afterServing {
						t.Errorf("serving changed after failed FlushAll: before=%+v/%t after=%+v/%t", beforeServing, beforeAvailable, afterServing, afterAvailable)
					}
					afterWork := workstats.Read().Fold
					if beforeWork.Public.Attempts != afterWork.Public.Attempts || beforeWork.Renew.Attempts != afterWork.Renew.Attempts || beforeWork.Build.Attempts != afterWork.Build.Attempts {
						t.Errorf("graph work entered after failed FlushAll: before=%+v after=%+v", beforeWork, afterWork)
					}
				})
			}
		})
	}
}

func newNativeJournalMaintenanceClassic(t *testing.T, failure error, beforeManager func(*backenddb.DB)) (*Service, *backenddb.DB, *collections.CollectionManager) {
	t.Helper()
	backend, err := backenddb.Open(testBackendOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { checkNativeJournalMaintenanceClose(t, backend.Close, failure) })
	if beforeManager != nil {
		beforeManager(backend)
	}
	manager := collections.NewCollectionManager(backend)
	svc := New(manager)
	t.Cleanup(func() {
		checkNativeJournalMaintenanceClose(t, svc.Close, failure)
		checkNativeJournalMaintenanceClose(t, manager.Close, failure)
	})
	if manager.UsesNativeCommandWALApply() || svc.deferredVectorBuildMaintenance != nil {
		t.Fatal("fixture must retain eligible classic/no-deferred maintenance")
	}
	return svc, backend, manager
}

func prepareNativeJournalMaintenanceTarget(t *testing.T, svc *Service, typed, admit bool) (*collections.Collection, IndexInfo) {
	t.Helper()
	ctx := context.Background()
	create := CreateIndexRequest{Name: "docs", Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}}
	documents := make([]Document, 8)
	for i := range documents {
		documents[i] = Document{ID: fmt.Sprintf("d%03d", i), Content: fmt.Sprintf("content %d", i), Embedding: []float32{1, float32(i) / 8}, Meta: map[string]any{"tenant": fmt.Sprintf("t%d", i%2)}}
	}
	if typed {
		create.Dimension, create.TypedInput = 8, true
		create.VectorIndexOptions = &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2}
		create.ScalarFields = []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}}
		documents = []Document{{ID: "a", Content: "alpha", Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "u"}}, {ID: "b", Content: "beta", Embedding: []float32{0, 1, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "v"}}}
	}
	if _, err := svc.CreateIndex(ctx, create); err != nil {
		t.Fatal(err)
	}
	if out, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: documents, DeferVectorIndexRebuild: true}); err != nil || out.Upserted != len(documents) {
		t.Fatalf("target upsert=%+v err=%v", out, err)
	}
	col, info, err := svc.openIndex(ctx, create.Name, 0)
	if err != nil {
		t.Fatal(err)
	}
	if typed && !admit {
		if status, err := col.RebuildVectorIndex(info.VectorIndexName); err != nil || !status.Loaded || status.RebuildNeeded {
			t.Fatalf("unadmitted built target=%+v err=%v", status, err)
		}
	} else {
		req := OptimizeIndexRequest{}
		if typed {
			options := typedServiceTestOptions()
			req.ColumnGraphServing = &options
		}
		if out, err := svc.OptimizeIndex(ctx, create.Name, req); err != nil || !out.Status.Loaded || out.Status.RebuildNeeded {
			for cause := err; cause != nil; cause = errors.Unwrap(cause) {
				t.Logf("initial target Optimize error chain: %T: %v", cause, cause)
			}
			t.Fatalf("initial successful target Optimize=%+v err=%v", out, err)
		}
	}
	col, info, err = svc.openIndex(ctx, create.Name, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The ordinary work closure invalidates this actual service-owned entry
	// before attempting its graph rebuild. Keep an entry even on profiles that
	// do not automatically warm the optional benchmark route after Optimize.
	if err := svc.primeBenchmarkSearchCache(info.Name, col, info); err != nil {
		t.Fatal(err)
	}
	return col, info
}

func nativeJournalMaintenanceCache(svc *Service) *serviceBenchmarkSearchCacheEntry {
	svc.benchmarkSearchCacheMu.RLock()
	defer svc.benchmarkSearchCacheMu.RUnlock()
	return svc.benchmarkSearchCache["docs"]
}

// Compare immutable publication/admission identities and charged work, never
// dynamic residency gauges or status APIs that can themselves flush writes.
type nativeJournalMaintenanceServingIdentity struct {
	PhysicalLimits                                               collections.ColumnGraphPhysicalResourceLimits
	Index                                                        string
	Ready, Invalid, Reconciling, BasePresent, PublicationPresent bool
	Base, Current                                                collections.ColumnManifestIdentity
	BaseCoverage, CurrentCoverage, WorkEpoch                     uint64
	Debt, Pending                                                collections.ColumnGraphPublicationDebtStats
	EncodedBytes, CandidateBytes, AppendCharges                  int64
}

func nativeJournalMaintenanceServing(col *collections.Collection) (nativeJournalMaintenanceServingIdentity, bool) {
	s, ok := col.ColumnGraphServingSnapshot()
	return nativeJournalMaintenanceServingIdentity{Index: s.Index, PublicationPresent: s.PublicationPresent, PhysicalLimits: s.Physical.Limits, Ready: s.ServingReady, Invalid: s.Invalid, Reconciling: s.Reconciling, BasePresent: s.BasePresent, Base: s.BaseManifest, Current: s.CurrentManifest, BaseCoverage: s.BaseCoverageLSN, CurrentCoverage: s.CurrentCoverageLSN, WorkEpoch: s.WorkEpoch, Debt: s.Debt, Pending: s.Pending, EncodedBytes: s.EncodedBytesCharged, CandidateBytes: s.CandidateBytesCharged, AppendCharges: s.AppenderAttemptsCharged}, ok
}

func checkNativeJournalMaintenanceClose(t *testing.T, close func() error, failure error) {
	t.Helper()
	if err := close(); err != nil {
		if !errors.Is(err, failure) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
			t.Error(err)
		} else {
			t.Logf("checked original failed-prerequisite cleanup: %v", err)
		}
	}
}
