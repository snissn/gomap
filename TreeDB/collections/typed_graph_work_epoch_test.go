package collections

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func typedGraphTestWorkEpochLimits() typedGraphWorkEpochLimits {
	return typedGraphWorkEpochLimits{NativeEntries: 4096, ColumnSegments: 4096, ManifestRecords: 4096, LifecycleEntries: 4096, NativeBytes: 128 << 20, ColumnBytes: 16 << 20, ManifestBytes: 4 << 20, RetainedBytes: 16 << 20, PagerPages: 32768}
}

func TestTypedGraphWorkEpochRecoverableManifestBudget(t *testing.T) {
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	roots, err := col.db.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer roots.Release()
	seq, system := dbCommitSeqAndSystemRoot(col.db)
	err = col.pinRecoverableColumnAssetSegments(context.Background(), roots, nil, ColumnAssetGCOptions{MaxManifestRecords: 1, MaxManifestBytes: 1 << 20})
	if !errors.Is(err, ErrColumnAssetReachabilityManifestLimit) {
		t.Fatalf("recoverable manifest budget=%v", err)
	}
	if afterSeq, afterSystem := dbCommitSeqAndSystemRoot(col.db); afterSeq != seq || afterSystem != system {
		t.Fatal("recoverable rejection changed authority")
	}
}

func TestTypedGraphWorkEpochRepeatedMaintenance(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(t, 32)
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	cold := typedGraphOverlapLimits().Cold
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 128, Tombstones: 128, ValueSlots: 512, OwnedBytes: 4 << 20, EncodedOutputBytes: 1 << 20}, cold); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphTestWorkEpochLimits()
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), limits); err != nil {
		t.Fatalf("configure: %v", err)
	}
	reuseBefore, err := strconv.ParseUint(col.db.Stats()["treedb.freelist.reuse_alloc_pages_total"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	var deleted int
	var columnBytesCeiling int64
	for cycle := 0; cycle < 8; cycle++ {
		// This small fixture disables background pruning. Exercise existing
		// caller-owned native maintenance; renewal itself never calls Prune.
		col.db.Prune()
		for row := range columns[1].Strings {
			columns[1].Strings[row] = fmt.Sprintf("cycle-%d-row-%d", cycle, row)
		}
		if _, err := col.ReplaceTypedBatch(ids, retained, columns); err != nil {
			t.Fatalf("cycle %d replace: %v", cycle, err)
		}
		if err := col.foldTypedGraph(context.Background(), cold, 128, typedGraphFoldTestAssetLimits(), nil); err != nil {
			t.Fatalf("cycle %d fold: %v", cycle, err)
		}
		coord := col.collectionSchemaCoordinator()
		if coord.typedPublicationEncodedBytes == 0 || coord.typedGraphCandidateBytes == 0 {
			t.Fatal("cycle produced no charged work")
		}
		seq, root := dbCommitSeqAndSystemRoot(col.db)
		stats, err := col.renewTypedGraphWorkEpoch(context.Background(), limits)
		if err != nil {
			t.Fatalf("cycle %d renew: %v", cycle, err)
		}
		if stats.Epoch != uint64(cycle+2) || coord.typedPublicationEncodedBytes != 0 || coord.typedGraphCandidateBytes != 0 || coord.typedGraphCandidateAttempts != 0 {
			t.Fatalf("renewal=%+v", stats)
		}
		if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(col.db); afterSeq != seq || afterRoot != root {
			t.Fatal("renewal changed logical authority")
		}
		deleted += stats.Columns.SegmentsDeleted
		if cycle == 1 {
			columnBytesCeiling = stats.Columns.BytesRetained
		} else if cycle > 1 && stats.Columns.BytesRetained > columnBytesCeiling {
			// Recovery-root retirement can release extra segments; shrinking is
			// valid, while later generations must not exceed the warm ceiling.
			t.Fatalf("equal-width generation column storage grew: %d exceeds %d", stats.Columns.BytesRetained, columnBytesCeiling)
		}
		t.Logf("cycle=%d native_bytes=%d entries=%d pager_pages=%d reusable=%d column_deleted=%d retained=%d", cycle, stats.Native.Bytes, stats.Native.Entries, stats.Pager.TotalPages, stats.Pager.FreelistReclaimable, stats.Columns.SegmentsDeleted, stats.Columns.BytesRetained)
		if cycle >= 3 {
			plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{})
			if err != nil || plan.Segments.BytesWholeReclaimable > 100000 {
				t.Fatalf("historical whole-segment replay retention: bytes=%d err=%v", plan.Segments.BytesWholeReclaimable, err)
			}
			if plan.RewriteDebtBytes > 20000 {
				t.Fatalf("cross-generation mixed row storage growth: bytes=%d", plan.RewriteDebtBytes)
			}
		}
	}
	if deleted == 0 {
		t.Fatal("repeated maintenance reclaimed no real segments")
	}
	reuseAfter, err := strconv.ParseUint(col.db.Stats()["treedb.freelist.reuse_alloc_pages_total"], 10, 64)
	t.Logf("native_reuse_alloc_pages_total=%d->%d", reuseBefore, reuseAfter)
	if err != nil || reuseAfter <= reuseBefore {
		t.Fatalf("native page reuse=%d->%d err=%v", reuseBefore, reuseAfter, err)
	}
	// Exact fallback roots remain readable after historical candidates were
	// reclaimed. Eligibility is not permission to remove an actual root ref.
	roots, err := col.db.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer roots.Release()
		var fallbackRefs int
		for _, root := range roots.Roots() {
			snapshot := roots.AcquireSnapshotForRoot(root)
			if snapshot == nil {
				t.Fatal("missing recovery snapshot")
			}
			catalog, err := loadCollectionCatalog(snapshot, "minima")
			if err != nil || catalog == nil {
				snapshot.Close()
				t.Fatalf("recovery catalog=%v err=%v", catalog, err)
			}
			view, err := col.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(snapshot, catalog, "minima", catalog.rootID(catalog.columnManifestRootName), *catalog.meta.Options.ColumnStore, true, columnManifestScanAllSidecars())
			if err == nil && catalog.typedGraphBase != nil {
				requirements, _, baseErr := catalog.typedGraphBase.requirementsAtSnapshot(snapshot)
				if baseErr != nil {
					snapshot.Close()
					t.Fatal(baseErr)
				}
				for _, obligation := range requirements.Obligations {
					ref := ColumnAssetRef{Kind: ColumnAssetKind(obligation.Kind), Namespace: obligation.Namespace, Generation: obligation.Generation, PartID: obligation.PartID, FileID: uint32(obligation.FileID), Offset: obligation.Offset, Length: obligation.Length, Checksum: obligation.Checksum}
					if _, err := readColumnPhysicalAssetFromManager(col.db.ColumnAssetRootDir(), ref); err != nil {
						snapshot.Close()
						t.Fatalf("captured fallback base ref=%+v err=%v", ref, err)
					}
					if root.Durable && !root.Visible {
						fallbackRefs++
					}
				}
			}
			snapshot.Close()
			if err != nil {
				t.Fatal(err)
			}
			for _, ref := range columnPhysicalScanSnapshotViewAssetRefs(view) {
				if _, err := readColumnPhysicalAssetFromManager(col.db.ColumnAssetRootDir(), ref); err != nil {
					t.Fatalf("recovery root=%+v ref=%+v err=%v", root, ref, err)
				}
				if root.Durable && !root.Visible {
					fallbackRefs++
				}
			}
		}
		if fallbackRefs == 0 {
			t.Fatal("fixture did not exercise fallback asset closure")
		}
		t.Logf("post-GC readable fallback refs=%d", fallbackRefs)
	}()
	// Reopen the post-GC native directory with a real unapplied typed command.
	// This exercises native replay, not reconstruction from retained JSON.
	if err := col.db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	replayLSN := col.db.State().AppliedCommandLSN
	replayDir := col.db.Dir()
	var payload commitlog.CollectionTypedBatchPayload
	for _, frame := range collectionCommandWALFrames(t, col.db.Dir()) {
		if frame.Kind == commitlog.CommandKindCollectionUpdateBatchByID && frame.PayloadFormat == commitlog.PayloadFormatCollectionTypedBatchByIDV1 {
			payload, err = commitlog.DecodeCollectionTypedBatchPayload(frame.Payload)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(payload.Documents) != len(ids) {
		t.Fatal("missing typed replacement payload")
	}
	for i, column := range payload.Columns {
		if column.Name == "content" {
			for j := range payload.Documents {
				payload.Documents[j].Values[i].String = "post-gc-replay"
			}
		}
	}
	encoded, err := commitlog.EncodeCollectionTypedBatchPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.db.Close(); err != nil {
		t.Fatal(err)
	}
	writeCollectionCommandWALFrame(t, replayDir, replayLSN+1, commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionTypedBatchByIDV1, encoded)
	reopened := openTypedMinimaDB(t, replayDir)
	defer reopened.Close()
	replayed, err := NewCollectionManager(reopened).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		got, err := replayed.Get(id)
		if err != nil || !strings.Contains(string(got), "post-gc-replay") {
			t.Fatalf("post-GC replay id=%s value=%s err=%v", id, got, err)
		}
	}
}

func TestTypedGraphWorkEpochRejectKeepsDebt(t *testing.T) {
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	defer base.Close()
	cold := typedGraphOverlapLimits().Cold
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 32, Tombstones: 32, ValueSlots: 128, OwnedBytes: 1 << 20, EncodedOutputBytes: 1 << 20}, cold); err != nil {
		t.Fatal(err)
	}
	if err := col.foldTypedGraph(context.Background(), cold, 128, typedGraphFoldTestAssetLimits(), nil); err != nil {
		t.Fatal(err)
	}
	coord := col.collectionSchemaCoordinator()
	beforeBytes, beforeAttempts := coord.typedGraphCandidateBytes, coord.typedGraphCandidateAttempts
	seq, root := dbCommitSeqAndSystemRoot(col.db)
	limits := typedGraphTestWorkEpochLimits()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := col.renewTypedGraphWorkEpoch(ctx, limits); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	limits.NativeEntries = 1
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), limits); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("native pressure=%v", err)
	}
	limits.NativeEntries++
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), limits); !errors.Is(err, ErrConcurrentMutation) {
		t.Fatalf("changed limits=%v", err)
	}
	if coord.typedGraphCandidateBytes != beforeBytes || coord.typedGraphCandidateAttempts != beforeAttempts || coord.typedGraphWorkEpoch != 0 || coord.typedGraphFoldActive.Load() {
		t.Fatal("rejected renewal lost debt or exclusion")
	}
	if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(col.db); afterSeq != seq || afterRoot != root {
		t.Fatal("rejection changed authority")
	}
}

func TestTypedGraphWorkEpochPinnedPressure(t *testing.T) {
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	cold := typedGraphOverlapLimits().Cold
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 32, Tombstones: 32, ValueSlots: 128, OwnedBytes: 1 << 20, EncodedOutputBytes: 1 << 20}, cold); err != nil {
		t.Fatal(err)
	}
	coord := col.collectionSchemaCoordinator()
	limits := typedGraphTestWorkEpochLimits()
	var baseline typedGraphWorkEpochStats
	coord.typedPublicationDebtMu.Lock()
	err := typedGraphWorkEpochRetainedAdmission(coord, limits, &baseline)
	coord.typedPublicationDebtMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	limits.RetainedBytes = baseline.RetainedBytes
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), limits); err != nil {
		t.Fatalf("configure: %v", err)
	}
	if err := col.foldTypedGraph(context.Background(), cold, 128, typedGraphFoldTestAssetLimits(), nil); err != nil {
		t.Fatal(err)
	}
	owner, err := col.openTypedGraphReadOwner(typedGraphOverlapLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	debt := coord.typedGraphCandidateBytes
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), limits); !errors.Is(err, errTypedGraphOwnerBudget) {
		t.Fatalf("pinned pressure=%v", err)
	}
	if coord.typedGraphCandidateBytes != debt || coord.typedGraphWorkEpoch != 1 {
		t.Fatal("pinned rejection renewed debt")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if stats, err := col.renewTypedGraphWorkEpoch(context.Background(), limits); !rootpublication.StableRelativeNamespaceSupported() {
		if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) || stats.Columns.SegmentsDeleted != 0 || coord.typedGraphCandidateBytes != debt || coord.typedGraphWorkEpoch != 1 {
			t.Fatalf("unsupported renewal changed debt/epoch or deleted assets: %+v err=%v", stats, err)
		}
	} else if err != nil {
		t.Fatalf("released owner renewal: %v", err)
	}
}

func TestTypedGraphWorkEpochCleanupFailure(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	cold := typedGraphOverlapLimits().Cold
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 32, Tombstones: 32, ValueSlots: 128, OwnedBytes: 1 << 20, EncodedOutputBytes: 1 << 20}, cold); err != nil {
		t.Fatal(err)
	}
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), typedGraphTestWorkEpochLimits()); err != nil {
		t.Fatalf("configure: %v", err)
	}
	if err := col.foldTypedGraph(context.Background(), cold, 128, typedGraphFoldTestAssetLimits(), nil); err != nil {
		t.Fatal(err)
	}
	// A recovery-selectable fallback root still owns the previous graph even
	// after all readers close. Cross that existing durable boundary before
	// expecting the injected physical deletion to be attempted.
	advanceColumnAssetDurableFallbackM15C(t, col.db)
	coord := col.collectionSchemaCoordinator()
	debt, attempts := coord.typedGraphCandidateBytes, coord.typedGraphCandidateAttempts
	injected := errors.New("renewal cleanup failure")
	restore := setColumnAssetGCTestHooks(func(string) error { return injected }, nil)
	failed, err := col.renewTypedGraphWorkEpoch(context.Background(), typedGraphTestWorkEpochLimits())
	restore()
	if !errors.Is(err, injected) {
		logTypedGraphWorkEpochRetention(t, col)
		t.Fatalf("cleanup error=%v stats=%+v", err, failed.Columns)
	}
	if coord.typedGraphCandidateBytes != debt || coord.typedGraphCandidateAttempts != attempts || coord.typedGraphWorkEpoch != 1 {
		t.Fatal("failed cleanup credited debt")
	}
	ctx, cancel := context.WithCancel(context.Background())
	restore = setColumnAssetStableDeleteAfterPlanTestHook(cancel)
	_, err = col.renewTypedGraphWorkEpoch(ctx, typedGraphTestWorkEpochLimits())
	restore()
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cleanup=%v", err)
	}
	if coord.typedGraphCandidateBytes != debt || coord.typedGraphCandidateAttempts != attempts || coord.typedGraphWorkEpoch != 1 {
		t.Fatal("canceled cleanup credited debt")
	}
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), typedGraphTestWorkEpochLimits()); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

func TestTypedGraphWorkEpochOldOwnerRetainsCandidates(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	col, base, _, _, _, _ := openTypedGraphQualityFixture(t, 8)
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	cold := typedGraphOverlapLimits().Cold
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 32, Tombstones: 32, ValueSlots: 128, OwnedBytes: 1 << 20, EncodedOutputBytes: 1 << 20}, cold); err != nil {
		t.Fatal(err)
	}
	limits := typedGraphTestWorkEpochLimits()
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), limits); err != nil {
		t.Fatal(err)
	}
	owner, err := col.openTypedGraphReadOwner(typedGraphOverlapLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := col.foldTypedGraph(context.Background(), cold, 128, typedGraphFoldTestAssetLimits(), nil); err != nil {
		t.Fatal(err)
	}
	pinned, err := col.renewTypedGraphWorkEpoch(context.Background(), limits)
	if err != nil {
		t.Fatal(err)
	}
	if pinned.Columns.SegmentsDeleted != 0 {
		t.Fatalf("old owner lost segments: %+v", pinned.Columns)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	advanceColumnAssetDurableFallbackM15C(t, col.db)
	released, err := col.renewTypedGraphWorkEpoch(context.Background(), limits)
	if err != nil {
		t.Fatal(err)
	}
	if released.Columns.SegmentsDeleted == 0 || released.Columns.BytesRetained >= pinned.Columns.BytesRetained {
		logTypedGraphWorkEpochRetention(t, col)
		t.Fatalf("closed old owner did not permit real candidate reclamation: pinned=%+v released=%+v", pinned.Columns, released.Columns)
	}
}

func logTypedGraphWorkEpochRetention(t *testing.T, col *Collection) {
	t.Helper()
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	t.Logf("retention plan=%+v err=%v", plan, err)
	roots, err := col.db.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Logf("recovery roots error=%v", err)
		return
	}
	defer roots.Release()
	t.Logf("recovery roots=%+v", roots.Roots())
}

func TestTypedGraphWorkEpochNativeInventory(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"unknown", "empty"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := inspectTypedGraphNativeResidual(context.Background(), dir, 1, 1024); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("empty unknown entries escaped cap: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "unknown"), []byte("abc"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectTypedGraphNativeResidual(context.Background(), dir, 2, 2); !errors.Is(err, errTypedGraphOverlayFoldNeeded) {
		t.Fatalf("byte cap: %v", err)
	}
	got, err := inspectTypedGraphNativeResidual(context.Background(), dir, 2, 3)
	if err != nil || got.Entries != 2 || got.Bytes != 3 {
		t.Fatalf("inventory=%+v err=%v", got, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := inspectTypedGraphNativeResidual(ctx, dir, 2, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if err := os.Symlink(dir, filepath.Join(dir, "cycle")); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectTypedGraphNativeResidual(context.Background(), dir, 10, 100); err == nil {
		t.Fatal("symlink accepted")
	}
}

func BenchmarkTypedGraphWorkEpochRenewal(b *testing.B) {
	col, base, ids, retained, columns, _ := openTypedGraphQualityFixture(b, 128)
	if err := base.Close(); err != nil {
		b.Fatal(err)
	}
	cold := typedGraphOverlapLimits().Cold
	if err := col.reconcileTypedGraphPublication(typedGraphPublicationLimits{Rows: 256, Tombstones: 256, ValueSlots: 1024, OwnedBytes: 4 << 20, EncodedOutputBytes: 4 << 20}, cold); err != nil {
		b.Fatal(err)
	}
	limits := typedGraphTestWorkEpochLimits()
	if _, err := col.renewTypedGraphWorkEpoch(context.Background(), limits); err != nil {
		b.Fatal(err)
	}
	var reclaimed int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		col.db.Prune()
		for row := range columns[1].Strings {
			columns[1].Strings[row] = fmt.Sprintf("cycle-%d-row-%d", i, row)
		}
		if _, err := col.ReplaceTypedBatch(ids, retained, columns); err != nil {
			b.Fatal(err)
		}
		if err := col.foldTypedGraph(context.Background(), cold, 256, typedGraphFoldTestAssetLimits(), nil); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		stats, err := col.renewTypedGraphWorkEpoch(context.Background(), limits)
		if err != nil {
			b.Fatal(err)
		}
		reclaimed += stats.Columns.BytesDeleted
	}
	b.ReportMetric(float64(reclaimed)/float64(b.N), "column-reclaimed-B/op")
}
