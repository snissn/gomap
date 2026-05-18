package colgranule

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

func TestColumnAssetManagerRequiresZombieAndPinDrainBeforeDelete(t *testing.T) {
	activeRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	oldRef := lifecycleAssetRef(t, 2, 0, tcs1HeaderBytes+24)
	active := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, activeRef)
	old := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, oldRef)
	reachability, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:       &active,
		CleanupSafeManifests: []ColumnCollectionManifest{old},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	manager, err := NewColumnAssetManager(NewMemoryColumnAssetStore())
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	reclaim := manager.PlanReclamation(reachability)
	if reclaim.CandidateBytes != int(oldRef.Length) || reclaim.ReadyToDeleteBytes != 0 {
		t.Fatalf("initial reclaim candidate/ready=(%d,%d) want (%d,0)", reclaim.CandidateBytes, reclaim.ReadyToDeleteBytes, oldRef.Length)
	}
	pin, err := manager.Pin(oldRef, "snapshot")
	if err != nil {
		t.Fatalf("Pin: %v", err)
	}
	if err := manager.MarkZombie(oldRef, "manifest superseded"); err != nil {
		t.Fatalf("MarkZombie: %v", err)
	}
	reclaim = manager.PlanReclamation(reachability)
	if reclaim.PinnedBytes != int(oldRef.Length) || reclaim.ZombieBytes != int(oldRef.Length) || reclaim.ReadyToDeleteBytes != 0 {
		t.Fatalf("pinned reclaim pinned/zombie/ready=(%d,%d,%d) want (%d,%d,0)", reclaim.PinnedBytes, reclaim.ZombieBytes, reclaim.ReadyToDeleteBytes, oldRef.Length, oldRef.Length)
	}
	if err := manager.Release(pin); err != nil {
		t.Fatalf("Release: %v", err)
	}
	reclaim = manager.PlanReclamation(reachability)
	if reclaim.ReadyToDeleteBytes != int(oldRef.Length) {
		t.Fatalf("ready bytes=%d want %d", reclaim.ReadyToDeleteBytes, oldRef.Length)
	}
}

func TestColumnAssetManagerReportsRewriteDebtForMixedSegment(t *testing.T) {
	oldRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	activeRef := lifecycleAssetRef(t, 1, oldRef.Length, tcs1HeaderBytes+32)
	active := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, activeRef)
	old := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, oldRef)
	reachability, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:       &active,
		CleanupSafeManifests: []ColumnCollectionManifest{old},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	manager, err := NewColumnAssetManager(NewMemoryColumnAssetStore())
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	reclaim := manager.PlanReclamation(reachability)
	if reclaim.CandidateBytes != 0 || reclaim.ReadyToDeleteBytes != 0 || reclaim.RewriteDebtBytes != int(oldRef.Length) {
		t.Fatalf("mixed reclaim candidate/ready/rewrite=(%d,%d,%d) want (0,0,%d)", reclaim.CandidateBytes, reclaim.ReadyToDeleteBytes, reclaim.RewriteDebtBytes, oldRef.Length)
	}
}

func TestColumnAssetManagerValidatesPreparedPublishClosure(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	if closure.RequiredAssets != 1 || closure.RequiredBytes != int(ref.Length) || !closure.FlushRequired {
		t.Fatalf("closure=%+v want one required flushed asset", closure)
	}

	missing := prepared
	missing.Ref.Offset += 4096
	_, err = manager.PreparePublishClosure([]ColumnPreparedAsset{missing})
	if err == nil {
		t.Fatalf("PreparePublishClosure missing ref succeeded")
	}
}

func TestColumnAssetManagerRejectsPublishClosureRequiredBytesOverflow(t *testing.T) {
	manager, err := NewColumnAssetManager(acceptingVerifierAssetStore{})
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	prepared := []ColumnPreparedAsset{
		{
			Ref:   ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, FileID: 1, Offset: 0, Length: 1, Checksum: 1},
			Bytes: maxColumnAssetInt,
		},
		{
			Ref:   ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, FileID: 1, Offset: 1, Length: 1, Checksum: 1},
			Bytes: 1,
		},
	}
	_, err = manager.PreparePublishClosure(prepared)
	if err == nil || !strings.Contains(err.Error(), "required bytes overflow") {
		t.Fatalf("PreparePublishClosure overflow err=%v, want required bytes overflow", err)
	}
}

type acceptingVerifierAssetStore struct{}

func (acceptingVerifierAssetStore) Put(ColumnAssetKind, []byte) (ColumnAssetRef, error) {
	return ColumnAssetRef{}, nil
}

func (acceptingVerifierAssetStore) Read(ColumnAssetRef) ([]byte, error) {
	return nil, nil
}

func (acceptingVerifierAssetStore) ReadTo(ColumnAssetRef, []byte) ([]byte, error) {
	return nil, nil
}

func (acceptingVerifierAssetStore) Verify(ref ColumnAssetRef) error {
	return validateColumnAssetRef(ref)
}

func TestColumnAssetManagerSyncPublishClosureDerivesSyncRequired(t *testing.T) {
	store := &syncProbeAssetStore{MemoryColumnAssetStore: NewMemoryColumnAssetStore()}
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	if !closure.SyncRequired {
		t.Fatalf("SyncRequired=false want true for sync-capable store")
	}
	synced, err := manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure required: %v", err)
	}
	if !synced.sealed || !synced.closure.SyncRequired {
		t.Fatalf("synced closure=%+v want sealed sync-required token", synced)
	}
	if got := store.syncCalls.Load(); got != 1 {
		t.Fatalf("sync calls=%d want 1", got)
	}
	closure.SyncRequired = false
	synced, err = manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure tampered sync flag: %v", err)
	}
	if !synced.closure.SyncRequired {
		t.Fatalf("synced closure SyncRequired=false want derived true")
	}
	if got := store.syncCalls.Load(); got != 2 {
		t.Fatalf("sync calls=%d want 2 after tampered sync flag", got)
	}

	empty, err := manager.PreparePublishClosure(nil)
	if err != nil {
		t.Fatalf("PreparePublishClosure empty: %v", err)
	}
	synced, err = manager.SyncPublishClosure(empty)
	if err != nil {
		t.Fatalf("SyncPublishClosure empty: %v", err)
	}
	if !synced.sealed || synced.closure.SyncRequired {
		t.Fatalf("empty synced closure=%+v want sealed no-sync token", synced)
	}
	if got := store.syncCalls.Load(); got != 2 {
		t.Fatalf("sync calls=%d want unchanged 2 after empty closure", got)
	}
}

func TestColumnAssetManagerSyncPublishClosureUsesPreparedIdentityWithoutReverify(t *testing.T) {
	store := &syncProbeAssetStore{MemoryColumnAssetStore: NewMemoryColumnAssetStore()}
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	closure.SyncRequired = false
	store.Reset()
	if _, err := store.Read(ref); err == nil {
		t.Fatal("Reset left prepared asset readable")
	}
	if _, err := manager.SyncPublishClosure(closure); err != nil {
		t.Fatalf("SyncPublishClosure reverified prepared asset after prepare: %v", err)
	}
	if got := store.syncCalls.Load(); got != 1 {
		t.Fatalf("sync calls=%d want 1 after prepared closure sync", got)
	}
}

func TestColumnAssetManagerSyncPublishClosureRejectsPreparedAssetMismatch(t *testing.T) {
	store := &syncProbeAssetStore{MemoryColumnAssetStore: NewMemoryColumnAssetStore()}
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	refA, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put A: %v", err)
	}
	refB, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+24))
	if err != nil {
		t.Fatalf("Put B: %v", err)
	}
	refC, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put C: %v", err)
	}
	prepared := []ColumnPreparedAsset{
		{Ref: refA, GenerationID: 7, PublishID: 11, Reason: "publish staged"},
		{Ref: refB, GenerationID: 7, PublishID: 11, Reason: "publish staged"},
	}
	closure, err := manager.PreparePublishClosure(prepared)
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	closure.PreparedAssets = closure.PreparedAssets[:1]
	if _, err := manager.SyncPublishClosure(closure); err == nil {
		t.Fatal("SyncPublishClosure accepted closure with missing prepared ref")
	}
	if got := store.syncCalls.Load(); got != 0 {
		t.Fatalf("sync calls=%d want 0 after rejected closure mismatch", got)
	}

	closure, err = manager.PreparePublishClosure(prepared)
	if err != nil {
		t.Fatalf("PreparePublishClosure replacement case: %v", err)
	}
	closure.PreparedAssets[0].Ref = refC
	if _, err := manager.SyncPublishClosure(closure); err == nil {
		t.Fatal("SyncPublishClosure accepted closure with substituted prepared ref")
	}
	if got := store.syncCalls.Load(); got != 0 {
		t.Fatalf("sync calls=%d want 0 after rejected closure substitution", got)
	}

	literal := ColumnAssetPublishClosure{
		PreparedAssets: []ColumnPreparedAsset{{Ref: refA, GenerationID: 7, PublishID: 11, Reason: "publish staged"}},
		RequiredAssets: 1,
		RequiredBytes:  int(refA.Length),
		FlushRequired:  true,
		SyncRequired:   true,
	}
	if _, err := manager.SyncPublishClosure(literal); err == nil || !strings.Contains(err.Error(), "not prepared by this manager") {
		t.Fatalf("SyncPublishClosure literal err=%v, want not prepared by this manager", err)
	}
	if got := store.syncCalls.Load(); got != 0 {
		t.Fatalf("sync calls=%d want 0 after rejected literal closure", got)
	}
}

func TestColumnAssetManagerSyncPublishClosureRejectsUnpreparedNoopLiteral(t *testing.T) {
	store := &syncProbeAssetStore{MemoryColumnAssetStore: NewMemoryColumnAssetStore()}
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	if _, err := manager.SyncPublishClosure(ColumnAssetPublishClosure{}); err == nil || !strings.Contains(err.Error(), "not prepared by this manager") {
		t.Fatalf("SyncPublishClosure zero literal err=%v want not prepared by this manager", err)
	}
	if got := store.syncCalls.Load(); got != 0 {
		t.Fatalf("sync calls=%d want 0 after rejected zero literal closure", got)
	}
}

func TestColumnAssetManagerPublishSucceededRequiresSyncedClosure(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed: %v", err)
	}
	if err := manager.MarkPublishSucceeded(ColumnAssetSyncedPublishClosure{}); err == nil {
		t.Fatal("MarkPublishSucceeded accepted an unsealed publish closure")
	}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	synced, err := manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure: %v", err)
	}
	otherManager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager other: %v", err)
	}
	if err := otherManager.MarkPublishSucceeded(synced); err == nil {
		t.Fatal("MarkPublishSucceeded accepted a synced closure from another manager")
	}
	if err := manager.MarkPublishSucceeded(synced); err != nil {
		t.Fatalf("MarkPublishSucceeded: %v", err)
	}
	state := manager.DebugState()
	if len(state.Quarantine) != 0 || len(state.PublishFailed) != 0 || len(state.RefFailedAt) != 0 {
		t.Fatalf("manager state quarantine=%d publishFailed=%d refFailedAt=%d want publish failure state cleared", len(state.Quarantine), len(state.PublishFailed), len(state.RefFailedAt))
	}
}

func TestColumnAssetManagerPublishSucceededConsumesSyncedClosure(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{Ref: ref, GenerationID: 7, PublishID: 11, Reason: "publish staged"}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed: %v", err)
	}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	synced, err := manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure: %v", err)
	}
	if err := manager.MarkPublishSucceeded(synced); err != nil {
		t.Fatalf("MarkPublishSucceeded: %v", err)
	}
	if err := manager.MarkPublishSucceeded(synced); err == nil || !strings.Contains(err.Error(), "already consumed") {
		t.Fatalf("second MarkPublishSucceeded err=%v, want already consumed", err)
	}
}

func TestColumnAssetManagerPublishFailureInvalidatesOlderSyncedClosure(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{Ref: ref, GenerationID: 7, PublishID: 11, Reason: "publish staged"}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	synced, err := manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure: %v", err)
	}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed: %v", err)
	}
	if err := manager.MarkPublishSucceeded(synced); err == nil || !strings.Contains(err.Error(), "already consumed") {
		t.Fatalf("MarkPublishSucceeded after failure err=%v, want consumed stale closure rejection", err)
	}
	if err := manager.MarkPublishSucceeded(synced); err == nil || !strings.Contains(err.Error(), "already consumed") {
		t.Fatalf("second MarkPublishSucceeded after failure err=%v, want consumed stale closure rejection", err)
	}
	state := manager.DebugState()
	gotReason, quarantined := state.Quarantine[ref]
	gotFailedReason, publishFailed := state.PublishFailed[ref]
	if !quarantined || gotReason != "root publish failed" || !publishFailed || gotFailedReason != "root publish failed" {
		t.Fatalf("manager state quarantine=(%q,%v) publishFailed=(%q,%v), want root publish failed retained", gotReason, quarantined, gotFailedReason, publishFailed)
	}
	if state.SyncedAttempts != 0 || state.SyncedRefs != 0 {
		t.Fatalf("synced attempts=%d refs=%d want failed publish to consume overlapping attempt", state.SyncedAttempts, state.SyncedRefs)
	}
}

func TestColumnAssetManagerPublishFailureDuringSyncInvalidatesLaterAttempt(t *testing.T) {
	store := &blockingSyncAssetStore{
		MemoryColumnAssetStore: NewMemoryColumnAssetStore(),
		entered:                make(chan struct{}),
		release:                make(chan struct{}),
	}
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{Ref: ref, GenerationID: 7, PublishID: 11, Reason: "publish staged"}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	result := make(chan struct {
		synced ColumnAssetSyncedPublishClosure
		err    error
	}, 1)
	go func() {
		synced, err := manager.SyncPublishClosure(closure)
		result <- struct {
			synced ColumnAssetSyncedPublishClosure
			err    error
		}{synced: synced, err: err}
	}()
	<-store.entered
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed: %v", err)
	}
	close(store.release)
	got := <-result
	if got.err != nil {
		t.Fatalf("SyncPublishClosure: %v", got.err)
	}
	if err := manager.MarkPublishSucceeded(got.synced); err == nil || !strings.Contains(err.Error(), "predates a later publish failure") {
		t.Fatalf("MarkPublishSucceeded after in-flight failure err=%v, want later publish failure rejection", err)
	}
}

func TestColumnAssetManagerEmptyPublishFailureDoesNotInvalidateSyncedClosure(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{Ref: ref, GenerationID: 7, PublishID: 11, Reason: "publish staged"}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	synced, err := manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure: %v", err)
	}
	if err := manager.MarkPublishFailed(nil, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed empty: %v", err)
	}
	if err := manager.MarkPublishSucceeded(synced); err != nil {
		t.Fatalf("MarkPublishSucceeded after empty publish failure: %v", err)
	}
	state := manager.DebugState()
	if state.PublishEpoch != 0 || len(state.RefFailedAt) != 0 {
		t.Fatalf("publishEpoch=%d refFailedAt=%d want unchanged after empty publish failure", state.PublishEpoch, len(state.RefFailedAt))
	}
}

func TestColumnAssetManagerDirectQuarantinePreservesFailureProvenanceUntilRetry(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{Ref: ref, GenerationID: 7, PublishID: 11, Reason: "publish staged"}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed: %v", err)
	}
	if err := manager.Quarantine(ref, "root publish failed"); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}
	state := manager.DebugState()
	gotFailedReason, publishFailed := state.PublishFailed[ref]
	_, operatorLocked := state.OperatorLocked[ref]
	if !publishFailed || gotFailedReason != "root publish failed" || !operatorLocked {
		t.Fatalf("publishFailed=(%q,%v) operatorLocked=%v, want retained root publish failed and operator lock", gotFailedReason, publishFailed, operatorLocked)
	}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	synced, err := manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure: %v", err)
	}
	if err := manager.MarkPublishSucceeded(synced); err != nil {
		t.Fatalf("MarkPublishSucceeded: %v", err)
	}
	state = manager.DebugState()
	gotQuarantine := state.Quarantine[ref]
	_, publishFailed = state.PublishFailed[ref]
	_, operatorLocked = state.OperatorLocked[ref]
	if gotQuarantine != "root publish failed" || publishFailed || !operatorLocked {
		t.Fatalf("quarantine=%q publishFailed=%v operatorLocked=%v, want operator quarantine retained and failure marker cleared", gotQuarantine, publishFailed, operatorLocked)
	}
}

func TestColumnAssetManagerPublishFailureDoesNotInvalidateDisjointSyncedClosure(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	refA, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put A: %v", err)
	}
	refB, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+24))
	if err != nil {
		t.Fatalf("Put B: %v", err)
	}
	preparedA := ColumnPreparedAsset{Ref: refA, GenerationID: 7, PublishID: 11, Reason: "publish staged A"}
	preparedB := ColumnPreparedAsset{Ref: refB, GenerationID: 8, PublishID: 12, Reason: "publish staged B"}
	closureA, err := manager.PreparePublishClosure([]ColumnPreparedAsset{preparedA})
	if err != nil {
		t.Fatalf("PreparePublishClosure A: %v", err)
	}
	syncedA, err := manager.SyncPublishClosure(closureA)
	if err != nil {
		t.Fatalf("SyncPublishClosure A: %v", err)
	}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{preparedB}, "root publish failed B"); err != nil {
		t.Fatalf("MarkPublishFailed B: %v", err)
	}
	if err := manager.MarkPublishSucceeded(syncedA); err != nil {
		t.Fatalf("MarkPublishSucceeded A after disjoint failure: %v", err)
	}
}

func TestColumnAssetManagerPublishSucceededPreservesUnrelatedQuarantine(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	if err := manager.Quarantine(ref, "checksum mismatch"); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	synced, err := manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure: %v", err)
	}
	if err := manager.MarkPublishSucceeded(synced); err != nil {
		t.Fatalf("MarkPublishSucceeded: %v", err)
	}
	state := manager.DebugState()
	if got := state.Quarantine[ref]; got != "checksum mismatch" {
		t.Fatalf("quarantine reason=%q want checksum mismatch", got)
	}
}

func TestColumnAssetManagerPublishFailurePreservesExistingQuarantine(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	if err := manager.Quarantine(ref, "checksum mismatch"); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed: %v", err)
	}
	state := manager.DebugState()
	got := state.Quarantine[ref]
	_, publishFailed := state.PublishFailed[ref]
	if got != "checksum mismatch" {
		t.Fatalf("quarantine reason=%q want checksum mismatch", got)
	}
	if publishFailed {
		t.Fatalf("publish-failed marker attached to an explicit quarantine")
	}
}

func TestColumnAssetManagerPublishSucceededPreservesQuarantineAfterPublishFailure(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed: %v", err)
	}
	if err := manager.Quarantine(ref, "checksum mismatch"); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	synced, err := manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure: %v", err)
	}
	if err := manager.MarkPublishSucceeded(synced); err != nil {
		t.Fatalf("MarkPublishSucceeded: %v", err)
	}
	state := manager.DebugState()
	if got := state.Quarantine[ref]; got != "checksum mismatch" {
		t.Fatalf("quarantine reason=%q want checksum mismatch", got)
	}
	if _, ok := state.PublishFailed[ref]; ok {
		t.Fatalf("publish-failed marker preserved after successful publish")
	}
}

func TestColumnAssetManagerPublishSucceededPreservesSameReasonQuarantineAfterPublishFailure(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed: %v", err)
	}
	if err := manager.Quarantine(ref, "root publish failed"); err != nil {
		t.Fatalf("Quarantine: %v", err)
	}
	closure, err := manager.PreparePublishClosure([]ColumnPreparedAsset{prepared})
	if err != nil {
		t.Fatalf("PreparePublishClosure: %v", err)
	}
	synced, err := manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure: %v", err)
	}
	if err := manager.MarkPublishSucceeded(synced); err != nil {
		t.Fatalf("MarkPublishSucceeded: %v", err)
	}
	state := manager.DebugState()
	if got := state.Quarantine[ref]; got != "root publish failed" {
		t.Fatalf("quarantine reason=%q want root publish failed", got)
	}
	if _, ok := state.PublishFailed[ref]; ok {
		t.Fatalf("publish-failed marker preserved after successful publish")
	}
}

func TestColumnAssetManagerPreparedPublishFailureQuarantinesAssets(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "root publish failed"); err != nil {
		t.Fatalf("MarkPublishFailed: %v", err)
	}
	state := manager.DebugState()
	gotReason, quarantined := state.Quarantine[ref]
	gotFailedReason, publishFailed := state.PublishFailed[ref]
	if !quarantined || gotReason != "root publish failed" {
		t.Fatalf("quarantine reason=%q present=%v want root publish failed,true", gotReason, quarantined)
	}
	if !publishFailed || gotFailedReason != "root publish failed" {
		t.Fatalf("publish-failed reason=%q present=%v want root publish failed,true", gotFailedReason, publishFailed)
	}
	plan, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		QuarantinedAssets: []ColumnPreparedAsset{prepared},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	reclaim := manager.PlanReclamation(plan)
	if len(reclaim.Entries) != 1 || !reclaim.Entries[0].Quarantined || reclaim.ReadyToDeleteBytes != 0 {
		t.Fatalf("reclaim=%+v want quarantined and not ready", reclaim)
	}
}

func TestColumnAssetManagerPublishFailurePreservesFirstReason(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{Ref: ref, GenerationID: 7, PublishID: 11, Reason: "publish staged"}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "first failure"); err != nil {
		t.Fatalf("MarkPublishFailed first: %v", err)
	}
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared}, "retry failure"); err != nil {
		t.Fatalf("MarkPublishFailed retry: %v", err)
	}
	state := manager.DebugState()
	gotReason := state.Quarantine[ref]
	gotFailedReason := state.PublishFailed[ref]
	if gotReason != "first failure" || gotFailedReason != "first failure" {
		t.Fatalf("quarantine/publish failure reasons=(%q,%q) want first failure preserved", gotReason, gotFailedReason)
	}
}

func TestColumnAssetManagerPreparedPublishFailureIsAtomic(t *testing.T) {
	store := NewMemoryColumnAssetStore()
	manager, err := NewColumnAssetManager(store)
	if err != nil {
		t.Fatalf("NewColumnAssetManager: %v", err)
	}
	ref, err := manager.Put(ColumnAssetKindTCS1PartImage, make([]byte, tcs1HeaderBytes+16))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	prepared := ColumnPreparedAsset{
		Ref:          ref,
		GenerationID: 7,
		PublishID:    11,
		Reason:       "publish staged",
	}
	invalid := prepared
	invalid.Ref.FileID = 0
	if err := manager.MarkPublishFailed([]ColumnPreparedAsset{prepared, invalid}, "root publish failed"); err == nil {
		t.Fatalf("MarkPublishFailed with invalid prepared asset succeeded")
	}
	state := manager.DebugState()
	if len(state.Quarantine) != 0 {
		t.Fatalf("quarantine entries=%d want 0 after failed publish-failure mark", len(state.Quarantine))
	}
}

func TestColumnAssetStoreFallbackVerificationReadsFullRef(t *testing.T) {
	ref := ColumnAssetRef{
		Kind:     ColumnAssetKindTCS1PartImage,
		FileID:   1,
		Offset:   0,
		Length:   int64(tcs1HeaderBytes),
		Checksum: 1,
	}
	store := &rangeProbeOnlyStore{}
	if err := verifyColumnAssetStoreRef(store, ref); err != nil {
		t.Fatalf("verifyColumnAssetStoreRef: %v", err)
	}
	if store.readToCalls != 1 || store.readRangeCalls != 0 {
		t.Fatalf("ReadTo/ReadRange calls=(%d,%d) want (1,0)", store.readToCalls, store.readRangeCalls)
	}
	badRef := ref
	badRef.Checksum = 2
	if err := verifyColumnAssetStoreRef(store, badRef); err == nil {
		t.Fatal("verifyColumnAssetStoreRef accepted checksum-mismatched ref")
	}
}

type rangeProbeOnlyStore struct {
	readToCalls    int
	readRangeCalls int
}

type syncProbeAssetStore struct {
	*MemoryColumnAssetStore
	syncCalls atomic.Int64
}

func (s *syncProbeAssetStore) Sync() error {
	s.syncCalls.Add(1)
	return nil
}

type blockingSyncAssetStore struct {
	*MemoryColumnAssetStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockingSyncAssetStore) Sync() error {
	close(s.entered)
	<-s.release
	return nil
}

func (s *rangeProbeOnlyStore) Put(ColumnAssetKind, []byte) (ColumnAssetRef, error) {
	return ColumnAssetRef{}, nil
}

func (s *rangeProbeOnlyStore) Read(ColumnAssetRef) ([]byte, error) {
	return nil, nil
}

func (s *rangeProbeOnlyStore) ReadTo(ref ColumnAssetRef, dst []byte) ([]byte, error) {
	s.readToCalls++
	if err := validateColumnAssetRef(ref); err != nil {
		return nil, err
	}
	if ref.Checksum != 1 {
		return nil, fmt.Errorf("checksum mismatch")
	}
	payload := make([]byte, ref.Length)
	return append(dst[:0], payload...), nil
}

func (s *rangeProbeOnlyStore) ReadRange(ref ColumnAssetRef, offset int64, length int) ([]byte, error) {
	s.readRangeCalls++
	if err := validateColumnAssetRef(ref); err != nil {
		return nil, err
	}
	if offset != 0 || length != 1 {
		return nil, fmt.Errorf("unexpected range probe offset=%d length=%d", offset, length)
	}
	if ref.Checksum != 1 {
		return nil, fmt.Errorf("checksum mismatch")
	}
	return []byte{0}, nil
}
