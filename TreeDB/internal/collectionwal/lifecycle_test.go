package collectionwal

import "testing"

func TestSideRefLifecycleStateNames(t *testing.T) {
	tests := map[SideRefLifecycleState]string{
		SideRefStateAbsent:                  "S0 Absent",
		SideRefStatePrepared:                "S1 PreparedSideRefs",
		SideRefStateWALCommitted:            "S2 WALCommitted",
		SideRefStateMaterializedUnpublished: "S3 MaterializedUnpublished",
		SideRefStateApplied:                 "S4 Applied",
		SideRefStateCleanable:               "S5 Cleanable",
		SideRefStateCleaned:                 "S6 Cleaned",
		SideRefStateQuarantined:             "Q Quarantined",
	}
	for state, want := range tests {
		if got := state.String(); got != want {
			t.Fatalf("state %d String()=%q want %q", state, got, want)
		}
	}
}

func TestProtectionReasonNames(t *testing.T) {
	tests := map[ProtectionReason]string{
		ProtectionPrepared:           "prepared",
		ProtectionWALCommitted:       "wal_committed",
		ProtectionPendingPublish:     "pending_publish",
		ProtectionCollectionReadView: "collection_read_view",
		ProtectionBackupManifest:     "backup_manifest",
		ProtectionCleanupPending:     "cleanup_pending",
	}
	for reason, want := range tests {
		if got := reason.String(); got != want {
			t.Fatalf("reason %d String()=%q want %q", reason, got, want)
		}
	}
}

func TestBackupModeNames(t *testing.T) {
	if got := BackupModeCleanCheckpoint.String(); got != "CleanCheckpoint" {
		t.Fatalf("CleanCheckpoint String()=%q", got)
	}
	if got := BackupModeWALSnapshot.String(); got != "WALSnapshot" {
		t.Fatalf("WALSnapshot String()=%q", got)
	}
}

func TestMaintenanceBarrierSnapshotCanMutate(t *testing.T) {
	if (MaintenanceBarrierSnapshot{}).CanMutate() {
		t.Fatal("zero snapshot must not permit mutation before recovery complete")
	}
	if (MaintenanceBarrierSnapshot{RecoveryComplete: true, UnclassifiedPrepares: 1}).CanMutate() {
		t.Fatal("unclassified prepares must block mutation")
	}
	if !(MaintenanceBarrierSnapshot{RecoveryComplete: true}).CanMutate() {
		t.Fatal("recovered clean snapshot should permit mutation")
	}
}
