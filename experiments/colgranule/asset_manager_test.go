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
