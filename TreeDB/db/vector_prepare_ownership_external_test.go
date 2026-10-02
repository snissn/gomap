package db_test

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func vectorPrepareOwnershipDBV1(t *testing.T) *backenddb.DB {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

type vectorPrepareOwnershipFaultV1 func(raftapply.FaultPointV1, raftapply.ApplyFaultContextV1) error

func (f vectorPrepareOwnershipFaultV1) InjectApplyFault(point raftapply.FaultPointV1, ctx raftapply.ApplyFaultContextV1) error {
	return f(point, ctx)
}

// Actual committed-entry append is paused before rebuild. Maintenance then
// owns its real mutex and can reach checkpoint's raw wait. Resuming the rebuild
// must use its pre-admission generation pin, never reacquire maintenance.
func TestVectorPrepareActualAppendMaintenanceOrderV1(t *testing.T) {
	d := vectorPrepareOwnershipDBV1(t)
	completed := false
	defer func() {
		if completed {
			_ = d.Close()
		}
	}()
	manager := collections.NewCommandWALReplayCollectionManager(d)
	def := collections.VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 2, EfConstruction: 8, EfSearch: 8, Strategy: collections.VectorIndexStrategyColumnGraph}
	meta := collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}}, VectorIndexes: []collections.VectorIndexDefinition{def}}
	created, err := manager.CreateCollection(&meta)
	if err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ id, body string }{{"x", `{"embedding":[1,0]}`}, {"y", `{"embedding":[0,1]}`}, {"minus-x", `{"embedding":[-1,0]}`}} {
		if _, err := c.Insert([]byte(row.id), []byte(row.body)); err != nil {
			t.Fatal(err)
		}
	}
	def = created.VectorIndexes[0]
	v := commitlog.VectorPrepareV1{Version: 1, Operation: "rebuild", Collection: "docs", Index: def.Name, Group: "group-a", IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(def), Generation: 1, MaxSourceRows: 8}
	payload, err := commitlog.EncodeVectorPreparePayloadV1(v)
	if err != nil {
		t.Fatal(err)
	}
	sections := []nativewire.Section{{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandVectorPrepareV1, Version: 1})}, {ID: nativewire.SectionCollectionRef, Bytes: append([]byte{1}, []byte("docs")...)}, {ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 1)}, {ID: nativewire.SectionIdempotencyKey, Bytes: []byte("maintenance-witness")}, {ID: nativewire.SectionVectorPrepareV1, Bytes: payload}}
	validated, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := nativewire.AppendDeterministicEntry(nil, validated)
	if err != nil {
		t.Fatal(err)
	}
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		completed = true // This branch has no parked goroutines or assigned handle.
		before, ok := d.StateToken()
		if !ok {
			t.Fatal("unsupported prepare baseline has no state")
		}
		next := d.CommandWALNextLSN()
		progress, results := raftapply.NewMemoryApplyProgressStore(8, 8), raftapply.NewMemoryApplyResultStore(8)
		id := raftentry.ApplyEntryID{Term: 3, Index: 1}
		appended := false
		result, err := raftapply.ApplyCommittedEntryV1(d, raw, raftapply.ApplyMetadataV1{GroupID: "group-a", EntryID: id, LocalDurabilityBoundary: raftapply.LocalDurabilityCommandWALV1, SyncLocalCommandWAL: true, CurrentCatalogVersion: 1, HasCurrentCatalogVersion: true}, raftapply.Options{ProgressStore: progress, ResultStore: results, FaultInjector: vectorPrepareOwnershipFaultV1(func(point raftapply.FaultPointV1, _ raftapply.ApplyFaultContextV1) error {
			if point == raftapply.FaultAfterLocalWALAppendBeforeVisibleV1 {
				appended = true
			}
			return nil
		})})
		if !errors.Is(err, collections.ErrVectorPartitionNamespacePersistenceUnsupportedV1) || result.Status != raftentry.ApplyStatusRejectedConflict {
			t.Fatalf("unsupported prepare result=%+v err=%v", result, err)
		}
		if appended {
			t.Fatal("unsupported prepare reached actual Append")
		}
		after, ok := d.StateToken()
		if !ok || after != before || d.CommandWALNextLSN() != next {
			t.Fatalf("refused prepare changed roots/WAL: before=%+v/%d after=%+v/%d", before, next, after, d.CommandWALNextLSN())
		}
		if _, ok := progress.LastApplied(); ok {
			t.Fatal("refused prepare advanced apply progress")
		}
		if _, ok, err := results.LookupApplyResult(id); err != nil || ok {
			t.Fatalf("refused prepare published result: present=%v err=%v", ok, err)
		}
		if err := d.CheckCommandWALPublishReady(); err != nil {
			t.Fatal(err)
		}
		for _, row := range []struct{ id, body string }{{"x", `{"embedding":[1,0]}`}, {"y", `{"embedding":[0,1]}`}, {"minus-x", `{"embedding":[-1,0]}`}} {
			got, err := c.Get([]byte(row.id))
			if err != nil || string(got) != row.body {
				t.Fatalf("refused prepare source %s=%s err=%v", row.id, got, err)
			}
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	appended, resume, maintenance := make(chan struct{}), make(chan struct{}), make(chan struct{})
	restore := d.SetVectorPrepareMaintenanceWitnessForTestingV1(func(operation string) error {
		if operation == "compact-storage" {
			close(maintenance)
		}
		return nil
	})
	applyDone := make(chan error, 1)
	go func() {
		result, err := raftapply.ApplyCommittedEntryV1(d, raw, raftapply.ApplyMetadataV1{GroupID: "group-a", EntryID: raftentry.ApplyEntryID{Term: 3, Index: 1}, LocalDurabilityBoundary: raftapply.LocalDurabilityCommandWALV1, SyncLocalCommandWAL: true, CurrentCatalogVersion: 1, HasCurrentCatalogVersion: true}, raftapply.Options{ProgressStore: raftapply.NewMemoryApplyProgressStore(8, 8), ResultStore: raftapply.NewMemoryApplyResultStore(8), FaultInjector: vectorPrepareOwnershipFaultV1(func(point raftapply.FaultPointV1, _ raftapply.ApplyFaultContextV1) error {
			if point == raftapply.FaultAfterLocalWALAppendBeforeVisibleV1 {
				close(appended)
				select {
				case <-resume:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})})
		if err == nil && result.Status != raftentry.ApplyStatusApplied {
			err = errors.New("rebuild not applied")
		}
		applyDone <- err
	}()
	select {
	case <-appended:
	case err := <-applyDone:
		t.Fatalf("did not append: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	maintenanceDone := make(chan error, 1)
	go func() { _, err := d.CompactStorage(ctx, backenddb.CompactStorageOptions{}); maintenanceDone <- err }()
	select {
	case <-maintenance:
	case err := <-maintenanceDone:
		t.Fatalf("maintenance not admitted: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(resume)
	select {
	case err := <-applyDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("assigned rebuild deadlocked behind maintenance")
	}
	select {
	case err := <-maintenanceDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("maintenance did not finish after Finalize")
	}
	restore()
	completed = true
	source, err := c.VectorPartitionSourceIdentityV1(def.Name)
	if err != nil || source.RowCount != 3 || source.Generation == 0 {
		t.Fatalf("source=%+v err=%v", source, err)
	}
}

// A real Append retains the sole teardown reader. Queue actual Close first;
// then borrow without another RLock. Borrower Release must not release Append,
// while Finalize/Abort must invalidate every remaining borrower.
func TestVectorPrepareActualAppendCaptureBorrowCloseV1(t *testing.T) {
	for _, acquisition := range []string{"ordinary_barriers", "inherited_plain"} {
		for _, finish := range []string{"finalize", "abort"} {
			t.Run(acquisition+"/"+finish, func(t *testing.T) {
				d := vectorPrepareOwnershipDBV1(t)
				frame, err := commandwalapply.TestNoopFrame()
				if err != nil {
					t.Fatal(err)
				}
				appendOptions := commandwalapply.Options{Sync: true}
				if acquisition == "inherited_plain" {
					actual, err := d.LockCommandWALStagingGuardV1()
					if err != nil {
						t.Fatal(err)
					}
					defer actual.Release()
					staging, err := commandwalapply.NewStagingGuard(d, actual)
					if err != nil {
						t.Fatal(err)
					}
					defer staging.Release()
					appendOptions.Staging = staging
				}
				handle, _, err := commandwalapply.Append(d, frame, commandwalapply.ApplyMetadata{}, appendOptions)
				if err != nil {
					t.Fatal(err)
				}
				defer commandwalapply.Abort(d, handle)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				closeDone := make(chan error, 1)
				go func() { closeDone <- d.Close() }()
				if err := d.WaitVectorPrepareTeardownWriterForTestingV1(ctx); err != nil {
					t.Fatal(err)
				}
				borrowed := make(chan *backenddb.StableResourceCaptureLease, 1)
				borrowErr := make(chan error, 1)
				go func() {
					lease, err := handle.BorrowStableResourceCaptureLeaseV1()
					if err != nil {
						borrowErr <- err
						return
					}
					borrowed <- lease
				}()
				var lease *backenddb.StableResourceCaptureLease
				select {
				case lease = <-borrowed:
				case err := <-borrowErr:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal("capture reacquired teardown behind Close")
				}
				if err := lease.ValidateCommandWALStagingCaptureV1(d, handle.CommandWALIntent()); err != nil {
					t.Fatal(err)
				}
				other := vectorPrepareOwnershipDBV1(t)
				if err := lease.ValidateDBV1(other); err == nil {
					t.Fatal("capture accepted wrong DB")
				}
				foreign, err := other.NewCommandWALIntent(frame.Kind, frame.Scope, frame.PayloadFormat, frame.Payload)
				if err != nil {
					t.Fatal(err)
				}
				if err := lease.ValidateCommandWALStagingCaptureV1(d, foreign); err == nil {
					t.Fatal("capture accepted wrong intent")
				}
				if err := d.ValidateCommandWALReplayOperationV1(handle.CommandWALIntent()); err == nil {
					t.Fatal("staging intent accepted as replay")
				}
				if err := other.Close(); err != nil {
					t.Fatal(err)
				}
				lease.Release()
				if err := lease.ValidateDBV1(d); err == nil {
					t.Fatal("released borrower remained valid")
				}
				// The original staging ownership is still live after borrower Release.
				live, err := handle.BorrowStableResourceCaptureLeaseV1()
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-closeDone:
					t.Fatal("borrower released parent teardown")
				default:
				}
				finished := make(chan error, 1)
				go func() {
					if finish == "abort" {
						commandwalapply.Abort(d, handle)
						finished <- nil
						return
					}
					_, err := commandwalapply.Finalize(d, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: true})
					finished <- err
				}()
				select {
				case err := <-finished:
					if err != nil && !errors.Is(err, backenddb.ErrClosed) {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("Finalize/Abort deadlocked behind Close")
				}
				if err := live.ValidateDBV1(d); err == nil {
					t.Fatal("expired guard left borrower valid")
				}
				if _, err := handle.BorrowStableResourceCaptureLeaseV1(); err == nil {
					t.Fatal("expired Append guard admitted capture")
				}
				live.Release()
				select {
				case <-closeDone:
				case <-ctx.Done():
					t.Fatal("Close retained expired staging guard")
				}
			})
		}
	}
}
