package colgranule

import "testing"

func TestColumnAssetManagerRequiresZombieAndPinDrainBeforeDelete(t *testing.T) {
	activeRef := lifecycleAssetRef(t, 1, 0, tcs1HeaderBytes+16)
	oldRef := lifecycleAssetRef(t, 2, 0, tcs1HeaderBytes+24)
	active := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 2, activeRef)
	old := lifecycleManifest(t, "jsonbench", ColumnPartRoleBase, 1, oldRef)
	reachability, err := PlanColumnAssetReachability(ColumnAssetReachabilityInput{
		ActiveManifest:      &active,
		SupersededManifests: []ColumnCollectionManifest{old},
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
		ActiveManifest:      &active,
		SupersededManifests: []ColumnCollectionManifest{old},
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
