package colgranule

import (
	"reflect"
	"testing"
)

func TestColumnAssetReachabilityDeletesClosedSupersededSegment(t *testing.T) {
	activeRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	oldRef := lifecycleAssetRef(t, 2, 0, tcs1HeaderBytes+24)
	active := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, activeRef)
	old := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, oldRef)

	plan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:       &active,
		CleanupSafeManifests: []ColumnCollectionManifest{old},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.RetainedBytes != int(activeRef.Length) {
		t.Fatalf("retained bytes=%d want %d", plan.RetainedBytes, activeRef.Length)
	}
	if plan.CleanupSafeBytes != int(oldRef.Length) || plan.ReclaimableBytes != int(oldRef.Length) || plan.RewriteDebtBytes != 0 {
		t.Fatalf("cleanup-safe/reclaimable/rewrite=(%d,%d,%d) want (%d,%d,0)", plan.CleanupSafeBytes, plan.ReclaimableBytes, plan.RewriteDebtBytes, oldRef.Length, oldRef.Length)
	}
	if plan.Stats.DirectlyDeletableSegments != 1 || plan.Stats.MixedLiveDeadSegments != 0 {
		t.Fatalf("deletable/mixed segments=(%d,%d) want (1,0)", plan.Stats.DirectlyDeletableSegments, plan.Stats.MixedLiveDeadSegments)
	}
	oldEntry := lifecycleFindEntry(t, plan, oldRef)
	if oldEntry.State != ColumnAssetStateReclaimable || !oldEntry.DeleteEligible {
		t.Fatalf("old entry state/delete=(%s,%v) want reclaimable,true", oldEntry.State, oldEntry.DeleteEligible)
	}
	if plan.Stats.TCS1PayloadBytesDecoded != 0 || plan.Stats.RowsScanned != 0 || plan.Stats.ColumnPayloadBlocksRead != 0 {
		t.Fatalf("planner decoded payload/rows/blocks=(%d,%d,%d), want all zero", plan.Stats.TCS1PayloadBytesDecoded, plan.Stats.RowsScanned, plan.Stats.ColumnPayloadBlocksRead)
	}
}

func TestColumnAssetReachabilityTracksRewriteDebtForMixedSegments(t *testing.T) {
	oldRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	activeRef := lifecycleAssetRef(t, 1, oldRef.Length, tcs1HeaderBytes+32)
	active := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, activeRef)
	old := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, oldRef)

	plan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:       &active,
		CleanupSafeManifests: []ColumnCollectionManifest{old},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.ReclaimableBytes != 0 || plan.RewriteDebtBytes != int(oldRef.Length) {
		t.Fatalf("reclaimable/rewrite=(%d,%d) want (0,%d)", plan.ReclaimableBytes, plan.RewriteDebtBytes, oldRef.Length)
	}
	if plan.Stats.DirectlyDeletableSegments != 0 || plan.Stats.MixedLiveDeadSegments != 1 {
		t.Fatalf("deletable/mixed segments=(%d,%d) want (0,1)", plan.Stats.DirectlyDeletableSegments, plan.Stats.MixedLiveDeadSegments)
	}
	oldEntry := lifecycleFindEntry(t, plan, oldRef)
	if oldEntry.State != ColumnAssetStateCleanupSafe || oldEntry.DeleteEligible {
		t.Fatalf("old entry state/delete=(%s,%v) want cleanup_safe,false", oldEntry.State, oldEntry.DeleteEligible)
	}
}

func TestColumnAssetReachabilityProtectedSupersededBlocksSegmentDeletion(t *testing.T) {
	cleanupSafeRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	protectedRef := lifecycleAssetRef(t, 1, cleanupSafeRef.Length, tcs1HeaderBytes+24)
	cleanupSafe := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, cleanupSafeRef)
	protected := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, protectedRef)

	plan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		CleanupSafeManifests: []ColumnCollectionManifest{cleanupSafe},
		SupersededManifests:  []ColumnCollectionManifest{protected},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.ReclaimableBytes != 0 || plan.RewriteDebtBytes != int(cleanupSafeRef.Length) {
		t.Fatalf("reclaimable/rewrite=(%d,%d) want (0,%d)", plan.ReclaimableBytes, plan.RewriteDebtBytes, cleanupSafeRef.Length)
	}
	if plan.CleanupSafeBytes != int(cleanupSafeRef.Length) || plan.SupersededBytes != int(protectedRef.Length) {
		t.Fatalf("cleanup-safe/superseded=(%d,%d) want (%d,%d)", plan.CleanupSafeBytes, plan.SupersededBytes, cleanupSafeRef.Length, protectedRef.Length)
	}
	if plan.Stats.DirectlyDeletableSegments != 0 || plan.Stats.MixedLiveDeadSegments != 1 {
		t.Fatalf("deletable/mixed segments=(%d,%d) want (0,1)", plan.Stats.DirectlyDeletableSegments, plan.Stats.MixedLiveDeadSegments)
	}
	cleanupEntry := lifecycleFindEntry(t, plan, cleanupSafeRef)
	if cleanupEntry.State != ColumnAssetStateCleanupSafe || cleanupEntry.DeleteEligible {
		t.Fatalf("cleanup entry state/delete=(%s,%v) want cleanup_safe,false", cleanupEntry.State, cleanupEntry.DeleteEligible)
	}
	protectedEntry := lifecycleFindEntry(t, plan, protectedRef)
	if protectedEntry.State != ColumnAssetStateSuperseded || protectedEntry.DeleteEligible {
		t.Fatalf("protected entry state/delete=(%s,%v) want superseded,false", protectedEntry.State, protectedEntry.DeleteEligible)
	}

	cleanupView := mustColumnCollectionManifestView(t, cleanupSafe)
	protectedView := mustColumnCollectionManifestView(t, protected)
	viewInput := ColumnAssetReachabilityViewInput{
		CleanupSafeManifests: []ColumnCollectionManifestView{cleanupView},
		SupersededManifests:  []ColumnCollectionManifestView{protectedView},
	}
	fromViews, err := PlanColumnAssetReachabilityFromViews(viewInput)
	if err != nil {
		t.Fatalf("PlanColumnAssetReachabilityFromViews: %v", err)
	}
	if got, want := lifecycleSummaryFromPlan(fromViews), lifecycleSummaryFromPlan(plan); !reflect.DeepEqual(got, want) {
		t.Fatalf("view summary=%+v want %+v", got, want)
	}
	summary, err := PlanColumnAssetReachabilitySummaryFromViews(viewInput)
	if err != nil {
		t.Fatalf("PlanColumnAssetReachabilitySummaryFromViews: %v", err)
	}
	if want := lifecycleSummaryFromPlan(plan); !reflect.DeepEqual(summary, want) {
		t.Fatalf("summary=%+v want %+v", summary, want)
	}
}

func TestColumnAssetReachabilitySameRefCleanupSafeAndSupersededIsProtected(t *testing.T) {
	ref := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	cleanupSafe := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, ref)
	protected := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, ref)

	plan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		CleanupSafeManifests: []ColumnCollectionManifest{cleanupSafe},
		SupersededManifests:  []ColumnCollectionManifest{protected},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.CleanupSafeBytes != int(ref.Length) || plan.ReclaimableBytes != 0 || plan.RewriteDebtBytes != int(ref.Length) {
		t.Fatalf("cleanup-safe/reclaimable/rewrite=(%d,%d,%d) want (%d,0,%d)",
			plan.CleanupSafeBytes, plan.ReclaimableBytes, plan.RewriteDebtBytes, ref.Length, ref.Length)
	}
	if plan.Stats.DirectlyDeletableSegments != 0 || plan.Stats.MixedLiveDeadSegments != 1 {
		t.Fatalf("deletable/mixed segments=(%d,%d) want (0,1)", plan.Stats.DirectlyDeletableSegments, plan.Stats.MixedLiveDeadSegments)
	}
	entry := lifecycleFindEntry(t, plan, ref)
	if entry.State != ColumnAssetStateCleanupSafe || entry.DeleteEligible {
		t.Fatalf("entry state/delete=(%s,%v) want cleanup_safe,false", entry.State, entry.DeleteEligible)
	}

	cleanupView := mustColumnCollectionManifestView(t, cleanupSafe)
	protectedView := mustColumnCollectionManifestView(t, protected)
	viewInput := ColumnAssetReachabilityViewInput{
		CleanupSafeManifests: []ColumnCollectionManifestView{cleanupView},
		SupersededManifests:  []ColumnCollectionManifestView{protectedView},
	}
	fromViews, err := PlanColumnAssetReachabilityFromViews(viewInput)
	if err != nil {
		t.Fatalf("PlanColumnAssetReachabilityFromViews: %v", err)
	}
	if got, want := lifecycleSummaryFromPlan(fromViews), lifecycleSummaryFromPlan(plan); !reflect.DeepEqual(got, want) {
		t.Fatalf("view summary=%+v want %+v", got, want)
	}
	summary, err := PlanColumnAssetReachabilitySummaryFromViews(viewInput)
	if err != nil {
		t.Fatalf("PlanColumnAssetReachabilitySummaryFromViews: %v", err)
	}
	if want := lifecycleSummaryFromPlan(plan); !reflect.DeepEqual(summary, want) {
		t.Fatalf("summary=%+v want %+v", summary, want)
	}
}

func TestColumnAssetReachabilityProtectsSupersededUntilCleanupSafe(t *testing.T) {
	rootPublishedRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	oldRef := lifecycleAssetRef(t, 2, 0, tcs1HeaderBytes+24)
	rootPublished := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, rootPublishedRef)
	old := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, oldRef)

	plan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		RootPublishedManifests: []ColumnCollectionManifest{rootPublished},
		SupersededManifests:    []ColumnCollectionManifest{old},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.RecoveryPendingBytes != int(rootPublishedRef.Length) {
		t.Fatalf("recovery pending bytes=%d want %d", plan.RecoveryPendingBytes, rootPublishedRef.Length)
	}
	if plan.SupersededBytes != int(oldRef.Length) || plan.CleanupSafeBytes != 0 || plan.ReclaimableBytes != 0 || plan.RewriteDebtBytes != 0 {
		t.Fatalf("superseded/cleanup-safe/reclaimable/rewrite=(%d,%d,%d,%d) want (%d,0,0,0)",
			plan.SupersededBytes, plan.CleanupSafeBytes, plan.ReclaimableBytes, plan.RewriteDebtBytes, oldRef.Length)
	}
	if plan.Stats.DirectlyDeletableSegments != 0 || plan.Stats.MixedLiveDeadSegments != 0 {
		t.Fatalf("deletable/mixed segments=(%d,%d) want (0,0)", plan.Stats.DirectlyDeletableSegments, plan.Stats.MixedLiveDeadSegments)
	}
	oldEntry := lifecycleFindEntry(t, plan, oldRef)
	if oldEntry.State != ColumnAssetStateSuperseded || oldEntry.DeleteEligible {
		t.Fatalf("old entry state/delete=(%s,%v) want superseded,false", oldEntry.State, oldEntry.DeleteEligible)
	}
	rootEntry := lifecycleFindEntry(t, plan, rootPublishedRef)
	if rootEntry.State != ColumnAssetStateRootPublished || rootEntry.DeleteEligible {
		t.Fatalf("root entry state/delete=(%s,%v) want root_published,false", rootEntry.State, rootEntry.DeleteEligible)
	}
}

func TestColumnAssetReachabilityProtectsSnapshotPinnedManifest(t *testing.T) {
	activeRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	oldRef := lifecycleAssetRef(t, 2, 0, tcs1HeaderBytes+24)
	active := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, activeRef)
	old := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, oldRef)

	plan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:          &active,
		SnapshotPinnedManifests: []ColumnCollectionManifest{old},
		SupersededManifests:     []ColumnCollectionManifest{old},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.ReclaimableBytes != 0 || plan.RewriteDebtBytes != 0 {
		t.Fatalf("reclaimable/rewrite=(%d,%d) want (0,0)", plan.ReclaimableBytes, plan.RewriteDebtBytes)
	}
	if plan.Stats.DirectlyDeletableSegments != 0 || plan.Stats.MixedLiveDeadSegments != 0 {
		t.Fatalf("deletable/mixed segments=(%d,%d) want (0,0)", plan.Stats.DirectlyDeletableSegments, plan.Stats.MixedLiveDeadSegments)
	}
	if plan.SnapshotProtectedBytes != int(oldRef.Length) {
		t.Fatalf("snapshot protected bytes=%d want %d", plan.SnapshotProtectedBytes, oldRef.Length)
	}
	oldEntry := lifecycleFindEntry(t, plan, oldRef)
	if oldEntry.State != ColumnAssetStateSnapshotPinned || oldEntry.DeleteEligible {
		t.Fatalf("old entry state/delete=(%s,%v) want snapshot_pinned,false", oldEntry.State, oldEntry.DeleteEligible)
	}
}

func TestColumnAssetReachabilityProtectsPendingAndPreparedAssets(t *testing.T) {
	pendingRef := lifecycleAssetRef(t, 3, 0, tcs1HeaderBytes+16)
	preparedRef := lifecycleAssetRef(t, 4, 0, tcs1HeaderBytes+24)
	quarantinedRef := lifecycleAssetRef(t, 5, 0, tcs1HeaderBytes+32)
	pending := lifecycleManifest(t, "jsonbench", ColumnPartRoleDelta, 3, pendingRef)

	plan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		PendingManifests: []ColumnCollectionManifest{pending},
		PreparedAssets: []ColumnPreparedAsset{{
			Ref:          preparedRef,
			GenerationID: 3,
			Reason:       "publish staged",
		}},
		QuarantinedAssets: []ColumnPreparedAsset{{
			Ref:    quarantinedRef,
			Bytes:  int(quarantinedRef.Length),
			Reason: "checksum mismatch",
		}},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if plan.PendingBytes != int(pendingRef.Length) {
		t.Fatalf("pending bytes=%d want %d", plan.PendingBytes, pendingRef.Length)
	}
	if plan.PreparedBytes != int(preparedRef.Length) {
		t.Fatalf("prepared bytes=%d want %d", plan.PreparedBytes, preparedRef.Length)
	}
	if plan.QuarantinedBytes != int(quarantinedRef.Length) {
		t.Fatalf("quarantined bytes=%d want %d", plan.QuarantinedBytes, quarantinedRef.Length)
	}
	if plan.ReclaimableBytes != 0 || plan.RewriteDebtBytes != 0 {
		t.Fatalf("reclaimable/rewrite=(%d,%d) want (0,0)", plan.ReclaimableBytes, plan.RewriteDebtBytes)
	}
	if got := lifecycleFindEntry(t, plan, pendingRef); got.State != ColumnAssetStatePendingPublish {
		t.Fatalf("pending entry state=%s want pending_publish", got.State)
	}
	if got := lifecycleFindEntry(t, plan, preparedRef); got.State != ColumnAssetStatePrepared {
		t.Fatalf("prepared entry state=%s want prepared", got.State)
	}
	if got := lifecycleFindEntry(t, plan, quarantinedRef); got.State != ColumnAssetStateQuarantined {
		t.Fatalf("quarantined entry state=%s want quarantined", got.State)
	}
}

func TestColumnCollectionManifestAssetRefsEnumeratesManifestMetadata(t *testing.T) {
	baseRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	deltaRef := lifecycleAssetRef(t, 1, baseRef.Length, tcs1HeaderBytes+24)
	basePart := NewColumnManifestPartRef(ColumnPartRoleBase, 1, lifecyclePartManifest(11, baseRef))
	deltaPart := NewColumnManifestPartRef(ColumnPartRoleDelta, 2, lifecyclePartManifest(22, deltaRef))
	manifest, err := NewColumnCollectionManifest("jsonbench", partTestOptions([]SortKeyColumn{{Column: "id"}}), []ColumnManifestPartRef{basePart}, []ColumnManifestPartRef{deltaPart}, nil)
	if err != nil {
		t.Fatalf("NewColumnCollectionManifest: %v", err)
	}

	entries, err := ColumnCollectionManifestAssetRefs(manifest)
	if err != nil {
		t.Fatalf("ColumnCollectionManifestAssetRefs: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries=%d want 2", len(entries))
	}
	if entries[0].Ref != baseRef || entries[0].State != ColumnAssetStateActive || entries[0].PartID != 11 {
		t.Fatalf("base entry=%+v want ref=%+v state=active part=11", entries[0], baseRef)
	}
	if entries[1].Ref != deltaRef || entries[1].State != ColumnAssetStateActive || entries[1].PartID != 22 {
		t.Fatalf("delta entry=%+v want ref=%+v state=active part=22", entries[1], deltaRef)
	}
}

func TestColumnAssetRefDeltaAccountingUsesChangedRefsOnly(t *testing.T) {
	oldRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	newRef := lifecycleAssetRef(t, 2, 0, tcs1HeaderBytes+24)
	preparedRef := lifecycleAssetRef(t, 3, 0, tcs1HeaderBytes+32)
	oldPart := NewColumnManifestPartRef(ColumnPartRoleBase, 1, lifecyclePartManifest(11, oldRef))
	newPart := NewColumnManifestPartRef(ColumnPartRoleBase, 2, lifecyclePartManifest(22, newRef))

	plan, err := PlanColumnAssetRefDelta(ColumnAssetRefDeltaInput{
		PublishedParts:  []ColumnManifestPartRef{newPart},
		SupersededParts: []ColumnManifestPartRef{oldPart},
		PreparedAssets: []ColumnPreparedAsset{{
			Ref:          preparedRef,
			GenerationID: 2,
			Reason:       "publish staged",
		}},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetRefDelta: %v", err)
	}
	if plan.PublishedBytes != int(newRef.Length) || plan.SupersededBytes != int(oldRef.Length) || plan.PreparedBytes != int(preparedRef.Length) {
		t.Fatalf("delta bytes published/superseded/prepared=(%d,%d,%d)", plan.PublishedBytes, plan.SupersededBytes, plan.PreparedBytes)
	}
	if plan.Stats.AssetRefs != 3 || plan.Stats.RowsScanned != 0 || plan.Stats.TCS1PayloadBytesDecoded != 0 || plan.Stats.ColumnPayloadBlocksRead != 0 {
		t.Fatalf("delta stats=%+v want changed refs only without row/payload scans", plan.Stats)
	}
	if len(plan.Entries) != 3 {
		t.Fatalf("entries=%d want 3", len(plan.Entries))
	}
}

func TestColumnAssetReachabilityDeterministicChurnReclaimsAfterSnapshotDrain(t *testing.T) {
	var activeRefs []ColumnAssetRef
	var oldRefs []ColumnAssetRef
	var oldBytes int
	for i := 0; i < 8; i++ {
		activeRefs = append(activeRefs, lifecycleAssetRef(t, 1, int64(i*(tcs1HeaderBytes+64)), tcs1HeaderBytes+64))
		oldRef := lifecycleAssetRef(t, 2, int64(i*(tcs1HeaderBytes+96)), tcs1HeaderBytes+96)
		oldRefs = append(oldRefs, oldRef)
		oldBytes += int(oldRef.Length)
	}
	active := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 20, activeRefs...)
	old := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 10, oldRefs...)

	pinned, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:          &active,
		SnapshotPinnedManifests: []ColumnCollectionManifest{old},
		SupersededManifests:     []ColumnCollectionManifest{old},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability pinned: %v", err)
	}
	if pinned.ReclaimableBytes != 0 || pinned.SnapshotProtectedBytes != oldBytes {
		t.Fatalf("pinned reclaimable/snapshot=(%d,%d) want (0,%d)", pinned.ReclaimableBytes, pinned.SnapshotProtectedBytes, oldBytes)
	}

	drained, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:       &active,
		CleanupSafeManifests: []ColumnCollectionManifest{old},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability drained: %v", err)
	}
	if drained.ReclaimableBytes*100 < oldBytes*80 {
		t.Fatalf("reclaimable bytes=%d old bytes=%d want at least 80%%", drained.ReclaimableBytes, oldBytes)
	}
	if drained.RewriteDebtBytes != 0 || drained.Stats.DirectlyDeletableSegments != 1 || drained.Stats.MixedLiveDeadSegments != 0 {
		t.Fatalf("drained plan=%+v want closed superseded segment reclaimable", drained)
	}
	manager, err := NewColumnAssetManager(NewMemoryColumnAssetStore())
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	for _, entry := range drained.Entries {
		if entry.DeleteEligible {
			if err := manager.MarkZombie(entry.Ref, "snapshot drained"); err != nil {
				t.Fatalf("MarkZombie: %v", err)
			}
		}
	}
	reclamation := manager.PlanReclamation(drained)
	if reclamation.ReadyToDeleteBytes != drained.ReclaimableBytes {
		t.Fatalf("ready delete bytes=%d want reclaimable=%d plan=%+v", reclamation.ReadyToDeleteBytes, drained.ReclaimableBytes, reclamation)
	}
}

func TestColumnAssetReachabilityFromBinaryViewsMatchesMaterializedPlan(t *testing.T) {
	active := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2,
		lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16),
		lifecycleAssetRef(t, 1, int64(tcs1HeaderBytes+16), tcs1HeaderBytes+24),
	)
	superseded := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1,
		lifecycleAssetRef(t, 2, 0, tcs1HeaderBytes+32),
	)
	materialized, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:       &active,
		CleanupSafeManifests: []ColumnCollectionManifest{superseded},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	activeView := mustColumnCollectionManifestView(t, active)
	supersededView := mustColumnCollectionManifestView(t, superseded)
	fromViews, err := PlanColumnAssetReachabilityFromViews(ColumnAssetReachabilityViewInput{
		ActiveManifest:       &activeView,
		CleanupSafeManifests: []ColumnCollectionManifestView{supersededView},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachabilityFromViews: %v", err)
	}
	if !reflect.DeepEqual(fromViews, materialized) {
		t.Fatalf("view plan mismatch\nview=%+v\nmaterialized=%+v", fromViews, materialized)
	}
	summary, err := PlanColumnAssetReachabilitySummaryFromViews(ColumnAssetReachabilityViewInput{
		ActiveManifest:       &activeView,
		CleanupSafeManifests: []ColumnCollectionManifestView{supersededView},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachabilitySummaryFromViews: %v", err)
	}
	if want := lifecycleSummaryFromPlan(materialized); !reflect.DeepEqual(summary, want) {
		t.Fatalf("summary mismatch\nsummary=%+v\nmaterialized=%+v", summary, want)
	}
}

func TestColumnAssetReachabilitySummaryTracksDurabilityStates(t *testing.T) {
	recoveryRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	rootPublishedRef := lifecycleAssetRef(t, 2, 0, tcs1HeaderBytes+24)
	cleanupSafeRef := lifecycleAssetRef(t, 3, 0, tcs1HeaderBytes+32)
	recovery := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 3, recoveryRef)
	rootPublished := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, rootPublishedRef)
	cleanupSafe := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, cleanupSafeRef)

	materialized, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		RecoveryAuthoritativeManifests: []ColumnCollectionManifest{recovery},
		RootPublishedManifests:         []ColumnCollectionManifest{rootPublished},
		CleanupSafeManifests:           []ColumnCollectionManifest{cleanupSafe},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if materialized.RetainedBytes != int(recoveryRef.Length) || materialized.RecoveryPendingBytes != int(rootPublishedRef.Length) || materialized.CleanupSafeBytes != int(cleanupSafeRef.Length) {
		t.Fatalf("durability bytes retained/recovery-pending/cleanup-safe=(%d,%d,%d)",
			materialized.RetainedBytes, materialized.RecoveryPendingBytes, materialized.CleanupSafeBytes)
	}
	summary, err := PlanColumnAssetReachabilitySummaryFromViews(ColumnAssetReachabilityViewInput{
		RecoveryAuthoritativeManifests: []ColumnCollectionManifestView{mustColumnCollectionManifestView(t, recovery)},
		RootPublishedManifests:         []ColumnCollectionManifestView{mustColumnCollectionManifestView(t, rootPublished)},
		CleanupSafeManifests:           []ColumnCollectionManifestView{mustColumnCollectionManifestView(t, cleanupSafe)},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachabilitySummaryFromViews: %v", err)
	}
	if want := lifecycleSummaryFromPlan(materialized); !reflect.DeepEqual(summary, want) {
		t.Fatalf("summary mismatch\nsummary=%+v\nmaterialized=%+v", summary, want)
	}
}

func TestColumnAssetReachabilitySummaryScansPreparedRegistryView(t *testing.T) {
	activeRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	preparedRef := lifecycleAssetRef(t, 2, 0, tcs1HeaderBytes+24)
	quarantinedRef := lifecycleAssetRef(t, 3, 0, tcs1HeaderBytes+32)
	active := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, activeRef)
	prepared := ColumnPreparedAsset{
		Ref:          preparedRef,
		GenerationID: 3,
		Reason:       "publish staged",
	}
	quarantined := ColumnPreparedAsset{
		Ref:    quarantinedRef,
		Bytes:  int(quarantinedRef.Length),
		Reason: "checksum mismatch",
	}
	materialized, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:    &active,
		PreparedAssets:    []ColumnPreparedAsset{prepared},
		QuarantinedAssets: []ColumnPreparedAsset{quarantined},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	activeView := mustColumnCollectionManifestView(t, active)
	registryView := mustColumnPreparedAssetRegistryView(t, ColumnPreparedAssetRegistry{
		Magic:        columnWorkspacePreparedMagic,
		Version:      columnWorkspacePreparedVersion,
		Collection:   "jsonbench",
		PublishID:    3,
		GenerationID: 3,
		UpdatedUnix:  1,
		Assets:       []ColumnPreparedAsset{prepared},
	})
	summary, err := PlanColumnAssetReachabilitySummaryFromViews(ColumnAssetReachabilityViewInput{
		ActiveManifest:    &activeView,
		PreparedRegistry:  &registryView,
		QuarantinedAssets: []ColumnPreparedAsset{quarantined},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachabilitySummaryFromViews: %v", err)
	}
	if want := lifecycleSummaryFromPlan(materialized); !reflect.DeepEqual(summary, want) {
		t.Fatalf("summary mismatch\nsummary=%+v\nmaterialized=%+v", summary, want)
	}
}

func BenchmarkColumnAssetReachability10K(b *testing.B) {
	active := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 1, 1, 0)
	superseded := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 10_001, 2, 0)
	input := ColumnAssetReachabilityInput{
		ActiveManifest:       &active,
		CleanupSafeManifests: []ColumnCollectionManifest{superseded},
	}
	plan, err := PlanColumnAssetReachability(input)
	if err != nil {
		b.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	b.ReportMetric(float64(plan.Stats.Manifests), "manifests")
	b.ReportMetric(float64(plan.Stats.AssetRefs), "asset_refs")
	b.ReportMetric(float64(plan.Stats.SegmentRefs), "segments")
	b.ReportMetric(float64(plan.Stats.TCS1PayloadBytesDecoded), "payload_bytes_decoded")
	b.ReportMetric(float64(plan.Stats.RowsScanned), "rows_scanned")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan, err := PlanColumnAssetReachability(input)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(plan.RetainedBytes + plan.ReclaimableBytes + plan.RewriteDebtBytes)
	}
}

func BenchmarkColumnAssetReachabilityView10K(b *testing.B) {
	active := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 1, 1, 0)
	superseded := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 10_001, 2, 0)
	activeView := mustColumnCollectionManifestView(b, active)
	supersededView := mustColumnCollectionManifestView(b, superseded)
	input := ColumnAssetReachabilityViewInput{
		ActiveManifest:       &activeView,
		CleanupSafeManifests: []ColumnCollectionManifestView{supersededView},
	}
	plan, err := PlanColumnAssetReachabilityFromViews(input)
	if err != nil {
		b.Fatalf("PlanColumnAssetReachabilityFromViews: %v", err)
	}
	b.ReportMetric(float64(plan.Stats.Manifests), "manifests")
	b.ReportMetric(float64(plan.Stats.AssetRefs), "asset_refs")
	b.ReportMetric(float64(plan.Stats.SegmentRefs), "segments")
	b.ReportMetric(float64(plan.Stats.TCS1PayloadBytesDecoded), "payload_bytes_decoded")
	b.ReportMetric(float64(plan.Stats.RowsScanned), "rows_scanned")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan, err := PlanColumnAssetReachabilityFromViews(input)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(plan.RetainedBytes + plan.ReclaimableBytes + plan.RewriteDebtBytes)
	}
}

func BenchmarkColumnAssetReachabilitySummaryView10K(b *testing.B) {
	active := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 1, 1, 0)
	superseded := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 10_001, 2, 0)
	activeView := mustColumnCollectionManifestView(b, active)
	supersededView := mustColumnCollectionManifestView(b, superseded)
	input := ColumnAssetReachabilityViewInput{
		ActiveManifest:       &activeView,
		CleanupSafeManifests: []ColumnCollectionManifestView{supersededView},
	}
	summary, err := PlanColumnAssetReachabilitySummaryFromViews(input)
	if err != nil {
		b.Fatalf("PlanColumnAssetReachabilitySummaryFromViews: %v", err)
	}
	b.ReportMetric(float64(summary.Stats.Manifests), "manifests")
	b.ReportMetric(float64(summary.Stats.AssetRefs), "asset_refs")
	b.ReportMetric(float64(summary.Stats.SegmentRefs), "segments")
	b.ReportMetric(float64(summary.Stats.TCS1PayloadBytesDecoded), "payload_bytes_decoded")
	b.ReportMetric(float64(summary.Stats.RowsScanned), "rows_scanned")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		summary, err := PlanColumnAssetReachabilitySummaryFromViews(input)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(summary.RetainedBytes + summary.ReclaimableBytes + summary.RewriteDebtBytes)
	}
}

func BenchmarkColumnAssetReachabilitySummaryView10KReuse(b *testing.B) {
	active := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 1, 1, 0)
	superseded := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 10_001, 2, 0)
	activeView := mustColumnCollectionManifestView(b, active)
	supersededView := mustColumnCollectionManifestView(b, superseded)
	input := ColumnAssetReachabilityViewInput{
		ActiveManifest:       &activeView,
		CleanupSafeManifests: []ColumnCollectionManifestView{supersededView},
	}
	scratch := &ColumnAssetReachabilitySummaryScratch{}
	summary, err := PlanColumnAssetReachabilitySummaryFromViewsWithScratch(input, scratch)
	if err != nil {
		b.Fatalf("PlanColumnAssetReachabilitySummaryFromViewsWithScratch: %v", err)
	}
	b.ReportMetric(float64(summary.Stats.Manifests), "manifests")
	b.ReportMetric(float64(summary.Stats.AssetRefs), "asset_refs")
	b.ReportMetric(float64(summary.Stats.SegmentRefs), "segments")
	b.ReportMetric(float64(summary.Stats.TCS1PayloadBytesDecoded), "payload_bytes_decoded")
	b.ReportMetric(float64(summary.Stats.RowsScanned), "rows_scanned")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		summary, err := PlanColumnAssetReachabilitySummaryFromViewsWithScratch(input, scratch)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(summary.RetainedBytes + summary.ReclaimableBytes + summary.RewriteDebtBytes)
	}
}

func BenchmarkColumnAssetRefDelta128(b *testing.B) {
	published := lifecycleSyntheticPartRefsForBenchmark(64, 20_000, 3, 0)
	superseded := lifecycleSyntheticPartRefsForBenchmark(64, 10_000, 2, 0)
	input := ColumnAssetRefDeltaInput{
		PublishedParts:  published,
		SupersededParts: superseded,
	}
	plan, err := PlanColumnAssetRefDelta(input)
	if err != nil {
		b.Fatalf("PlanColumnAssetRefDelta: %v", err)
	}
	b.ReportMetric(float64(plan.Stats.AssetRefs), "changed_refs")
	b.ReportMetric(float64(plan.Stats.RowsScanned), "rows_scanned")
	b.ReportMetric(float64(plan.Stats.TCS1PayloadBytesDecoded), "payload_bytes_decoded")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan, err := PlanColumnAssetRefDelta(input)
		if err != nil {
			b.Fatal(err)
		}
		benchSink += int64(plan.PublishedBytes + plan.SupersededBytes)
	}
}

func BenchmarkColumnAssetManagerReclamation10K(b *testing.B) {
	active := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 1, 1, 0)
	superseded := lifecycleSyntheticManifestForBenchmark(b, "active", 10_000, 10_001, 2, 0)
	reachability, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:       &active,
		CleanupSafeManifests: []ColumnCollectionManifest{superseded},
	})
	if err != nil {
		b.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	manager, err := NewColumnAssetManager(NewMemoryColumnAssetStore())
	if err != nil {
		b.Fatalf("NewColumnAssetManager: %v", err)
	}
	for _, entry := range reachability.Entries {
		if entry.DeleteEligible {
			if err := manager.MarkZombie(entry.Ref, "benchmark superseded"); err != nil {
				b.Fatalf("MarkZombie: %v", err)
			}
		}
	}
	plan := manager.PlanReclamation(reachability)
	b.ReportMetric(float64(len(plan.Entries)), "asset_refs")
	b.ReportMetric(float64(plan.CandidateBytes), "candidate_bytes")
	b.ReportMetric(float64(plan.ReadyToDeleteBytes), "ready_delete_bytes")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan := manager.PlanReclamation(reachability)
		benchSink += int64(plan.ReadyToDeleteBytes + plan.RewriteDebtBytes)
	}
}

func lifecycleAssetRef(t *testing.T, fileID uint32, offset int64, length int) ColumnAssetRef {
	t.Helper()
	payload := make([]byte, length)
	for i := range payload {
		payload[i] = byte(int(fileID) + int(offset) + i)
	}
	ref, err := newColumnAssetRef(ColumnAssetKindTCS1PartImage, fileID, offset, length, payload)
	if err != nil {
		t.Fatalf("newColumnAssetRef: %v", err)
	}
	return ref
}

func lifecycleManifest(t *testing.T, collection string, role ColumnPartRole, generationID uint64, refs ...ColumnAssetRef) ColumnCollectionManifest {
	t.Helper()
	baseParts := make([]ColumnManifestPartRef, 0, len(refs))
	deltaParts := make([]ColumnManifestPartRef, 0, len(refs))
	for i, ref := range refs {
		partID := generationID*100 + uint64(i+1)
		partRef := NewColumnManifestPartRef(role, generationID, lifecyclePartManifest(partID, ref))
		switch role {
		case ColumnPartRoleBase:
			baseParts = append(baseParts, partRef)
		case ColumnPartRoleDelta:
			deltaParts = append(deltaParts, partRef)
		default:
			t.Fatalf("unsupported role %s", role)
		}
	}
	manifest, err := NewColumnCollectionManifest(collection, partTestOptions([]SortKeyColumn{{Column: "id"}}), baseParts, deltaParts, nil)
	if err != nil {
		t.Fatalf("NewColumnCollectionManifest: %v", err)
	}
	return manifest
}

func lifecycleSyntheticPartRefsForBenchmark(parts int, firstPartID uint64, fileID uint32, baseOffset int64) []ColumnManifestPartRef {
	partRefs := make([]ColumnManifestPartRef, parts)
	offset := baseOffset
	for i := range partRefs {
		ref := ColumnAssetRef{
			Kind:     ColumnAssetKindTCS1PartImage,
			FileID:   fileID,
			Offset:   offset,
			Length:   int64(tcs1HeaderBytes + 512),
			Checksum: uint32(i + 1),
		}
		partID := firstPartID + uint64(i)
		partRefs[i] = NewColumnManifestPartRef(ColumnPartRoleBase, uint64(i+1), lifecyclePartManifest(partID, ref))
		offset += ref.Length
	}
	return partRefs
}

func lifecyclePartManifest(partID uint64, ref ColumnAssetRef) ColumnWorkspacePartManifest {
	imageBytes := int(ref.Length) - tcs1HeaderBytes
	if imageBytes <= 0 {
		imageBytes = int(ref.Length)
	}
	return ColumnWorkspacePartManifest{
		PartID:        partID,
		Rows:          1,
		VisibleRows:   1,
		SchemaVersion: 1,
		SortKey:       []SortKeyColumn{{Column: "id"}},
		AssetRef:      ref,
		TCS1: TCS1PartRecord{
			Version:      tcs1Version,
			Kind:         tcs1PartImageKind,
			PartID:       partID,
			Rows:         1,
			ImageVersion: columnPartImageVersion,
			PayloadBytes: imageBytes,
			TotalBytes:   int(ref.Length),
			AssetRef:     ref,
		},
		ImageBytes:    imageBytes,
		ManifestBytes: 4,
		Sections:      1,
		AssetBytes:    int(ref.Length),
		PublishedUnix: int64(partID),
	}
}

func lifecycleFindEntry(t *testing.T, plan ColumnAssetReachabilityPlan, ref ColumnAssetRef) ColumnAssetReachabilityEntry {
	t.Helper()
	for _, entry := range plan.Entries {
		if entry.Ref == ref {
			return entry
		}
	}
	t.Fatalf("missing reachability entry for ref %+v in %+v", ref, plan.Entries)
	return ColumnAssetReachabilityEntry{}
}

func lifecycleSummaryFromPlan(plan ColumnAssetReachabilityPlan) ColumnAssetReachabilitySummary {
	return ColumnAssetReachabilitySummary{
		Stats:                  plan.Stats,
		RetainedBytes:          plan.RetainedBytes,
		PreparedBytes:          plan.PreparedBytes,
		ProcessVisibleBytes:    plan.ProcessVisibleBytes,
		QuarantinedBytes:       plan.QuarantinedBytes,
		SupersededBytes:        plan.SupersededBytes,
		PendingBytes:           plan.PendingBytes,
		RecoveryPendingBytes:   plan.RecoveryPendingBytes,
		CleanupSafeBytes:       plan.CleanupSafeBytes,
		SnapshotProtectedBytes: plan.SnapshotProtectedBytes,
		RewriteDebtBytes:       plan.RewriteDebtBytes,
		ReclaimableBytes:       plan.ReclaimableBytes,
	}
}

func lifecycleSyntheticManifestForBenchmark(b *testing.B, collection string, parts int, firstPartID uint64, fileID uint32, baseOffset int64) ColumnCollectionManifest {
	b.Helper()
	partRefs := lifecycleSyntheticPartRefsForBenchmark(parts, firstPartID, fileID, baseOffset)
	manifest, err := NewColumnCollectionManifest(collection, partTestOptions([]SortKeyColumn{{Column: "id"}}), partRefs, nil, nil)
	if err != nil {
		b.Fatalf("NewColumnCollectionManifest: %v", err)
	}
	return manifest
}

func mustColumnCollectionManifestView(t testing.TB, manifest ColumnCollectionManifest) ColumnCollectionManifestView {
	t.Helper()
	payload, err := EncodeColumnCollectionManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeColumnCollectionManifest: %v", err)
	}
	view, err := DecodeColumnCollectionManifestView(payload)
	if err != nil {
		t.Fatalf("DecodeColumnCollectionManifestView: %v", err)
	}
	return view
}

func mustColumnPreparedAssetRegistryView(t testing.TB, registry ColumnPreparedAssetRegistry) ColumnPreparedAssetRegistryView {
	t.Helper()
	payload, err := encodeColumnPreparedAssetRegistryEnvelope(registry)
	if err != nil {
		t.Fatalf("encodeColumnPreparedAssetRegistryEnvelope: %v", err)
	}
	view, err := DecodeColumnPreparedAssetRegistryView(payload)
	if err != nil {
		t.Fatalf("DecodeColumnPreparedAssetRegistryView: %v", err)
	}
	return view
}
