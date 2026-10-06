package collections

import (
	"context"
	"os"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// The diagnostic uses the fixture's same live direct backend. In particular,
// vacuum does not measure the public cached wrapper's checkpoint/reconcile work.
const r1LifecycleSchedule5060 = "supported-direct-fold-rewrite-vacuum-exhaustive-fallback-gc-v1"

type r1LifecycleFoldRecord5060 struct {
	Stats          ColumnStoreCompactStats `json:"stats"`
	PublishedRefs  int                     `json:"published_refs"`
	SupersededRefs int                     `json:"superseded_refs"`
}

type r1LifecycleReclaimRecord5060 struct {
	PlanNS        int64                    `json:"plan_ns"`
	PlanGC        ColumnAssetGCStats       `json:"plan_gc"`
	Decision      string                   `json:"decision"`
	ProbeNS       int64                    `json:"probe_ns"`
	Probe         *ColumnAssetRewriteStats `json:"probe"`
	RewriteNS     int64                    `json:"rewrite_ns"`
	Rewrite       *ColumnAssetRewriteStats `json:"rewrite"`
	CheckpointNS  int64                    `json:"checkpoint_ns"`
	GCNS          int64                    `json:"gc_ns"`
	TypedGC       ColumnAssetGCStats       `json:"typed_gc"`
	CandidateRefs int                      `json:"candidate_refs"`
}

func r1LifecycleTime5060(t testing.TB, operation func() error) int64 {
	t.Helper()
	started := time.Now()
	if err := operation(); err != nil {
		t.Fatal(err)
	}
	return time.Since(started).Nanoseconds()
}

func r1LifecycleAggregateRewrite5060(stats ColumnAssetRewriteStats) ColumnAssetRewriteStats {
	stats.Plan.Entries, stats.Plan.SegmentEntries = nil, nil
	stats.SupersededRefs, stats.RemappedRefs = nil, nil
	return stats
}

// Retire caller candidates only after completed GC reports deletions and the
// corresponding observed segment paths are absent. A stale missing candidate
// would correctly make the next reachability plan incomplete.
func r1LifecycleRetireCandidates5060(t testing.TB, gc ColumnAssetGCStats, candidates *[]ColumnAssetRef) {
	t.Helper()
	if gc.SegmentsDeleted == 0 {
		return
	}
	deleted := make(map[uint32]bool)
	for _, entry := range gc.Plan.SegmentEntries {
		_, err := os.Stat(entry.Path)
		if os.IsNotExist(err) {
			deleted[entry.FileID] = true
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if len(deleted) != gc.SegmentsDeleted {
		t.Fatal("GC deletion count does not match absent observed segment paths")
	}
	live := (*candidates)[:0]
	for _, ref := range *candidates {
		if !deleted[ref.FileID] {
			live = append(live, ref)
		}
	}
	*candidates = live
}

// Candidates come only from completed logical folds/remaps. They supplement
// discovery; active/recovery roots and mapped pins remain authoritative.
func r1LifecycleReclaim5060(t testing.TB, db *backenddb.DB, col *Collection, candidates *[]ColumnAssetRef) r1LifecycleReclaimRecord5060 {
	t.Helper()
	ctx := context.Background()
	var result r1LifecycleReclaimRecord5060
	var plan ColumnAssetGCStats
	result.PlanNS = r1LifecycleTime5060(t, func() (err error) {
		plan, err = col.ColumnAssetGC(ctx, ColumnAssetGCOptions{SegmentDetails: true, CandidateRefs: *candidates})
		return err
	})
	result.PlanGC = r1LifecycleAggregateGC5060(plan)
	r1LifecycleRetireCandidates5060(t, plan, candidates)
	if !plan.Plan.Complete {
		t.Fatal("incomplete maintenance reachability plan")
	}
	result.Decision = "no_debt"
	if plan.Plan.RewriteDebtBytes > 0 {
		var probe ColumnAssetRewriteStats
		result.ProbeNS = r1LifecycleTime5060(t, func() (err error) {
			probe, err = col.ColumnAssetRewrite(ctx, ColumnAssetRewriteOptions{DryRun: true, CandidateRefs: *candidates})
			return err
		})
		aggregate := r1LifecycleAggregateRewrite5060(probe)
		result.Probe = &aggregate
		if !probe.Plan.Complete {
			t.Fatal("incomplete rewrite eligibility plan")
		}
		result.Decision = "protected_or_ineligible"
		if probe.SegmentsEligible > 0 && probe.RefsEligible > 0 {
			result.Decision = "eligible"
			var rewrite ColumnAssetRewriteStats
			result.RewriteNS = r1LifecycleTime5060(t, func() (err error) {
				rewrite, err = col.ColumnAssetRewrite(ctx, ColumnAssetRewriteOptions{CandidateRefs: *candidates})
				return err
			})
			*candidates = append(*candidates, rewrite.SupersededRefs...)
			completed := r1LifecycleAggregateRewrite5060(rewrite)
			result.Rewrite = &completed
			result.CheckpointNS = r1LifecycleTime5060(t, db.Checkpoint)
		}
	}
	var gc ColumnAssetGCStats
	result.GCNS = r1LifecycleTime5060(t, func() (err error) {
		gc, err = col.ColumnAssetGC(ctx, ColumnAssetGCOptions{SegmentDetails: true, CandidateRefs: *candidates})
		return err
	})
	result.TypedGC = r1LifecycleAggregateGC5060(gc)
	r1LifecycleRetireCandidates5060(t, gc, candidates)
	if result.Probe != nil && (!result.Probe.DryRun || result.Probe.SegmentsRewritten != 0 || result.Probe.RefsRemapped != 0) {
		t.Fatal("rewrite probe attribution includes completed work")
	}
	if result.Rewrite != nil && result.Rewrite.DryRun {
		t.Fatal("completed rewrite attribution is a dry run")
	}
	result.CandidateRefs = len(*candidates)
	return result
}

// Observe performs full current/held-view oracles and census outside all API
// timers. The unmaintained churn phase is recorded by the caller before entry.
func r1LifecycleFold5060(t testing.TB, db *backenddb.DB, col *Collection, epoch int, candidates *[]ColumnAssetRef, observe func(string)) r1LifecycleMaintenanceRecord5060 {
	t.Helper()
	ctx := context.Background()
	result := r1LifecycleMaintenanceRecord5060{Epoch: epoch}
	result.FlushNS = r1LifecycleTime5060(t, col.Flush)
	result.CheckpointNS = r1LifecycleTime5060(t, db.Checkpoint)
	observe("checkpoint")
	var before ColumnAssetGCStats
	result.BeforeFoldGCNS = r1LifecycleTime5060(t, func() (err error) {
		before, err = col.ColumnAssetGC(ctx, ColumnAssetGCOptions{SegmentDetails: true, CandidateRefs: *candidates})
		return err
	})
	result.BeforeFoldGC = r1LifecycleAggregateGC5060(before)
	r1LifecycleRetireCandidates5060(t, before, candidates)
	var folded ColumnStoreCompactStats
	result.FoldNS = r1LifecycleTime5060(t, func() (err error) {
		folded, err = col.ColumnStoreCompact(ctx, ColumnStoreCompactOptions{})
		return err
	})
	*candidates = append(*candidates, folded.SupersededRefs...)
	result.Fold = r1LifecycleFoldRecord5060{Stats: folded, PublishedRefs: len(folded.PublishedRefs), SupersededRefs: len(folded.SupersededRefs)}
	result.Fold.Stats.PublishedRefs, result.Fold.Stats.SupersededRefs = nil, nil
	result.FoldCheckpointNS = r1LifecycleTime5060(t, db.Checkpoint)
	observe("folded")
	result.OverlayNS = r1LifecycleTime5060(t, func() (err error) {
		result.Overlay, err = col.CompactRootOverlays(ctx)
		return err
	})
	result.OverlayCheckpointNS = r1LifecycleTime5060(t, db.Checkpoint)
	result.Reclaim = r1LifecycleReclaim5060(t, db, col, candidates)
	result.VlogGCNS = r1LifecycleTime5060(t, func() (err error) {
		result.ValueLogGC, err = db.ValueLogGC(ctx, backenddb.ValueLogGCOptions{})
		return err
	})
	observe("before_vacuum")
	result.VacuumNS = r1LifecycleTime5060(t, func() (err error) {
		result.Vacuum, err = db.VacuumIndexOnlineWithStats(ctx)
		return err
	})
	observe("before_exhaustive")
	result.Full = r1LifecycleFull5060(t, db)
	observe("before_final_gc")
	result.Final = r1LifecycleFinal5060(t, db, col, candidates)
	observe("maintenance")
	// A sum of disjoint API timers, excluding observer/oracle/census overhead.
	result.MaintenanceNS = result.FlushNS + result.CheckpointNS + result.BeforeFoldGCNS + result.FoldNS + result.FoldCheckpointNS + result.OverlayNS + result.OverlayCheckpointNS + result.Reclaim.PlanNS + result.Reclaim.ProbeNS + result.Reclaim.RewriteNS + result.Reclaim.CheckpointNS + result.Reclaim.GCNS + result.VlogGCNS + result.VacuumNS + result.Full.PlanNS + result.Full.WorkNS + result.Final.RefreshNS + result.Final.TypedGCNS + result.Final.LeafGCNS
	return result
}

func TestR1LifecycleLogicalFoldAndVacuum5060(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	dir, db, col, cleanup := r1LifecycleNew5060(t, true)
	defer func() {
		if err := cleanup(); err != nil {
			t.Error(err)
		}
	}()
	want, known := r1LifecycleSeed5060(t, col)
	held, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	captured := make(map[string]map[string]any, len(want))
	for id, row := range want {
		captured[id] = r1MutationCopy5059(row)
	}
	r1LifecycleAssert5060(t, held, captured, known)
	var candidates []ColumnAssetRef
	var deletedSegments int
	for epoch := range 3 {
		// Multiple historical typed generations on the same live rows each time.
		for _, id := range []string{"row-005", "row-010", "row-015"} {
			row := r1MutationCopy5059(want[id])
			row["revision"] = float64(epoch + 1)
			row["bio"] = "folded history 雪"
			ids, retained, columns := r1MutationBatch5059(t, row)
			if _, err := col.ReplaceTypedBatch(ids, retained, columns); err != nil {
				t.Fatal(err)
			}
			want[id] = row
			r1MutationRemember5059(known, row)
		}
		observe := func(string) {
			r1LifecycleCurrent5060(t, col, want, known)
			if epoch == 0 {
				r1LifecycleAssert5060(t, held, captured, known)
			}
		}
		result := r1LifecycleFold5060(t, db, col, epoch, &candidates, observe)
		fold := result.Fold.Stats
		if !fold.Compacted || fold.RowsCompacted != 32 || fold.MutationPartsBefore == 0 || fold.MutationPartsAfter != 0 || fold.NewGeneration <= fold.PreviousGeneration || result.Fold.SupersededRefs == 0 {
			t.Fatalf("logical fold did not reset history: %+v", result.Fold)
		}
		if result.Reclaim.PlanGC.Plan.Sources.ActiveManifestRefs >= result.BeforeFoldGC.Plan.Sources.ActiveManifestRefs {
			t.Fatal("logical fold did not reduce active manifest refs")
		}
		if !result.Vacuum.WorkCompleted || result.Vacuum.Canceled {
			t.Fatalf("vacuum did not complete: %+v", result.Vacuum)
		}
		deleted := result.BeforeFoldGC.SegmentsDeleted + result.Reclaim.PlanGC.SegmentsDeleted + result.Reclaim.TypedGC.SegmentsDeleted
		deletedSegments += deleted
		t.Logf("epoch=%d active_refs=%d->%d fold_rows=%d rewrite=%s deleted=%d vacuum_completed=%t", epoch, result.BeforeFoldGC.Plan.Sources.ActiveManifestRefs, result.Reclaim.PlanGC.Plan.Sources.ActiveManifestRefs, fold.RowsCompacted, result.Reclaim.Decision, deleted, result.Vacuum.WorkCompleted)
		if epoch == 0 {
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			released := r1LifecycleReclaim5060(t, db, col, &candidates)
			deletedSegments += released.PlanGC.SegmentsDeleted + released.TypedGC.SegmentsDeleted
			t.Logf("released rewrite=%s deleted=%d retained=%d", released.Decision, released.TypedGC.SegmentsDeleted, released.TypedGC.BytesRetained)
			r1LifecycleCurrent5060(t, col, want, known)
		}
	}
	if deletedSegments == 0 {
		t.Fatal("following generations did not reclaim any unprotected typed segments")
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	db, cleanup, _ = r1LifecycleOpen5060(t, dir)
	col, err = NewCollectionManager(db).OpenCollection("r1")
	if err != nil {
		t.Fatal(err)
	}
	r1LifecycleCurrent5060(t, col, want, known)
}

type r1LifecycleFullRecord5060 struct {
	Owner   backenddb.CompactStorageLeafPageLogOwnerClassification `json:"owner"`
	Options map[string]any                                         `json:"options"`
	PlanNS  int64                                                  `json:"plan_ns"`
	Plan    backenddb.CompactStorageStats                          `json:"plan"`
	WorkNS  int64                                                  `json:"work_ns"`
	Work    backenddb.CompactStorageStats                          `json:"work"`
}

func r1LifecycleFull5060(t testing.TB, db *backenddb.DB) r1LifecycleFullRecord5060 {
	t.Helper()
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		t.Fatal(err)
	}
	opts := backenddb.CompactStorageOptions{Mode: backenddb.CompactStorageExhaustive, SyncEachPhase: true, ValueLogRewriteBatchSize: 32, LeafPackMaxPasses: 4, LeafPackMaxBytesToCopyPerPass: 1 << 20}
	result := r1LifecycleFullRecord5060{Owner: db.CompactStorageLeafPageLogOwnerClassification(backenddb.CompactStorageLifecycleQuiescedMaintenance), Options: map[string]any{"mode": opts.Mode, "sync_each_phase": opts.SyncEachPhase, "value_log_rewrite_batch_size": opts.ValueLogRewriteBatchSize, "leaf_pack_max_passes": opts.LeafPackMaxPasses, "leaf_pack_max_bytes_to_copy_per_pass": opts.LeafPackMaxBytesToCopyPerPass, "unsafe_value_log_reclaim_fenced_unreferenced": opts.UnsafeValueLogReclaimFencedUnreferenced}}
	if result.Owner.Status != backenddb.CompactStorageOwnerStatusSupportedTarget || result.Owner.OwnerClass != backenddb.CompactStorageLeafPageLogOwnerInternalHiddenByWrapper || !result.Owner.Replaceable {
		t.Fatalf("unsupported owned exhaustive handoff: %+v", result.Owner)
	}
	result.PlanNS = r1LifecycleTime5060(t, func() (err error) { result.Plan, err = db.CompactStoragePlan(context.Background(), opts); return err })
	result.WorkNS = r1LifecycleTime5060(t, func() (err error) { result.Work, err = db.CompactStorage(context.Background(), opts); return err })
	return result
}
