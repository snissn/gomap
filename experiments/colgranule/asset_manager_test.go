package colgranule

import "testing"

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
	if store.syncCalls != 1 {
		t.Fatalf("sync calls=%d want 1", store.syncCalls)
	}
	closure.SyncRequired = false
	synced, err = manager.SyncPublishClosure(closure)
	if err != nil {
		t.Fatalf("SyncPublishClosure tampered sync flag: %v", err)
	}
	if !synced.closure.SyncRequired {
		t.Fatalf("synced closure SyncRequired=false want derived true")
	}
	if store.syncCalls != 2 {
		t.Fatalf("sync calls=%d want 2 after tampered sync flag", store.syncCalls)
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
	if store.syncCalls != 2 {
		t.Fatalf("sync calls=%d want unchanged 2 after empty closure", store.syncCalls)
	}
}

func TestColumnAssetManagerSyncPublishClosureVerifiesNoopClosureAssets(t *testing.T) {
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
	if _, err := manager.SyncPublishClosure(closure); err == nil {
		t.Fatal("SyncPublishClosure succeeded for missing no-op closure asset")
	}
	if store.syncCalls != 0 {
		t.Fatalf("sync calls=%d want 0 after failed no-op closure verification", store.syncCalls)
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
	if err := manager.MarkPublishSucceeded(ColumnAssetSyncedPublishClosure{}, "root published"); err == nil {
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
	if err := manager.MarkPublishSucceeded(synced, "root published"); err != nil {
		t.Fatalf("MarkPublishSucceeded: %v", err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.quarantine) != 0 || len(manager.zombies) != 0 || len(manager.rewriteDebt) != 0 {
		t.Fatalf("manager state quarantine=%d zombies=%d rewrite=%d want all cleared", len(manager.quarantine), len(manager.zombies), len(manager.rewriteDebt))
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
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.quarantine) != 0 {
		t.Fatalf("quarantine entries=%d want 0 after failed publish-failure mark", len(manager.quarantine))
	}
}

func TestColumnAssetStoreRangeProbeReadsNonZeroBytes(t *testing.T) {
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
	if store.readRangeCalls != 1 || store.lastLength != 1 {
		t.Fatalf("ReadRange calls=%d length=%d want one non-zero byte probe", store.readRangeCalls, store.lastLength)
	}
}

type rangeProbeOnlyStore struct {
	readRangeCalls int
	lastLength     int
}

type syncProbeAssetStore struct {
	*MemoryColumnAssetStore
	syncCalls int
}

func (s *syncProbeAssetStore) Sync() error {
	s.syncCalls++
	return nil
}

func (s *rangeProbeOnlyStore) Put(ColumnAssetKind, []byte) (ColumnAssetRef, error) {
	return ColumnAssetRef{}, nil
}

func (s *rangeProbeOnlyStore) Read(ColumnAssetRef) ([]byte, error) {
	return nil, nil
}

func (s *rangeProbeOnlyStore) ReadTo(ColumnAssetRef, []byte) ([]byte, error) {
	return nil, nil
}

func (s *rangeProbeOnlyStore) ReadRange(ref ColumnAssetRef, offset int64, length int) ([]byte, error) {
	s.readRangeCalls++
	s.lastLength = length
	if err := validateColumnAssetRef(ref); err != nil {
		return nil, err
	}
	if offset != 0 || length != 1 {
		t := make([]byte, 0)
		return t, nil
	}
	return []byte{0}, nil
}
