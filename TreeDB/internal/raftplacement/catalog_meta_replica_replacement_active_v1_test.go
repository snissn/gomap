package raftplacement

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func activeReplicaReplacementAuthorityV1(t *testing.T, activate bool) (*CatalogMetaAuthorityV1, ReplicaReplacementBeginV1, VectorPartitionLifecycleRecordV1) {
	t.Helper()
	a, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	identity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	identity.Immutable = VectorPartitionLifecycleImmutableAuthorityV1{
		ManifestDigest: strings.Repeat("a", 64), PlacementDigest: strings.Repeat("b", 64),
	}
	applied := uint64(1)
	building := catalogMetaLifecycleApplyV1(t, a, &applied, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-b"}, MutationEpoch: 9,
	})
	if !activate {
		return a, activeReplicaReplacementBeginV1(catalog), building
	}
	staged := catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(building, VectorPartitionLifecycleRecordGroupReadyV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.GroupReady = VectorPartitionLifecycleGroupReadyV1{GroupID: "group-b", AppliedIndex: applied, AssetSetDigest: strings.Repeat("c", 64)}
	}))
	digest, err := VectorPartitionLifecycleReadySetDigestV1(identity, staged.RequiredGroups, staged.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	prepared := catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(staged, VectorPartitionLifecyclePrepareV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.ReadySetDigest = digest
	}))
	active := catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(prepared, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 9
	}))
	return a, activeReplicaReplacementBeginV1(catalog), active
}

func activeReplicaReplacementBeginV1(catalog CatalogMetaRecordV1) ReplicaReplacementBeginV1 {
	return ReplicaReplacementBeginV1{
		OperationID: "replace-active-ann-owner", ConfigDigest: strings.Repeat("d", 64),
		ExpectedEpoch: catalog.Epoch, CatalogDigest: catalog.Digest, GroupID: "group-b",
		OldNodeID: "node-b", NewPeer: raftcluster.Peer{ID: "standby", Address: "127.0.0.1:19001"},
	}
}

func TestCatalogReplicaReplacementActiveImmutableRebindAndRestoreV1(t *testing.T) {
	a, begin, before := activeReplicaReplacementAuthorityV1(t, true)
	if before.State != VectorPartitionLifecycleActiveV1 {
		t.Fatalf("fixture state=%q", before.State)
	}
	raw, err := EncodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(raw, a.applied+1); err != nil {
		t.Fatalf("BEGIN while immutable generation is ACTIVE: %v", err)
	}
	pending, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	complete := completeReplicaReplacementForTestV1(t, a, begin)
	if complete.Catalog.Record.Epoch != begin.ExpectedEpoch+1 {
		t.Fatalf("completed epoch=%d", complete.Catalog.Record.Epoch)
	}
	rebound := before.Identity
	rebound.Index.CatalogEpoch = complete.Catalog.Record.Epoch
	rebound.Index.CatalogDigest = complete.Catalog.Record.Digest
	after, ok := a.VectorPartitionLifecycleRecordV1(rebound)
	if !ok || after.State != VectorPartitionLifecycleActiveV1 || after.Identity.Immutable != before.Identity.Immutable ||
		!reflect.DeepEqual(after.ReadyGroups, before.ReadyGroups) || !reflect.DeepEqual(after.RequiredGroups, before.RequiredGroups) {
		t.Fatalf("ACTIVE receipts were lost across completion: %+v available=%v", after, ok)
	}
	wantDigest, err := VectorPartitionLifecycleReadySetDigestV1(rebound, after.RequiredGroups, after.ReadyGroups)
	if err != nil || after.ReadySetDigest != wantDigest || after.ReadySetDigest == before.ReadySetDigest {
		t.Fatalf("rebound ready-set digest=%q want=%q err=%v", after.ReadySetDigest, wantDigest, err)
	}
	if _, old := a.VectorPartitionLifecycleRecordV1(before.Identity); old {
		t.Fatal("old catalog identity still admitted")
	}
	if err := catalogMetaLifecycleValidateSearchV1(a, rebound, after.ReadySetDigest); err != nil {
		t.Fatalf("rebound ACTIVE authority: %v", err)
	}
	final, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewCatalogMetaAuthorityV1()
	if err := restored.installCatalogMetaSnapshotBytesV1(pending); err != nil {
		t.Fatalf("restore pending: %v", err)
	}
	if err := restored.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("restore completion: %v", err)
	}
	if got, err := restored.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(got, final) {
		t.Fatalf("snapshot replay mismatch: %v", err)
	}
	if err := catalogMetaLifecycleValidateSearchV1(restored, rebound, after.ReadySetDigest); err != nil {
		t.Fatalf("restored ACTIVE authority: %v", err)
	}
	completeRaw, err := EncodeReplicaReplacementCompleteV1(complete)
	if err != nil {
		t.Fatal(err)
	}
	status, _ := restored.Status()
	if _, err := restored.applyCommittedCatalogMetaV1(completeRaw, status.AppliedIndex+1); err != nil {
		t.Fatalf("exact completion retry: %v", err)
	}
}

func TestCatalogReplicaReplacementRefusesPendingMutationAndBuildingV1(t *testing.T) {
	t.Run("pending-mutation", func(t *testing.T) {
		a, begin, active := activeReplicaReplacementAuthorityV1(t, true)
		key := vectorPartitionLifecycleServingKeyV1{Collection: active.Identity.Index.Collection, IndexName: active.Identity.Index.IndexName}
		a.mu.Lock()
		a.mutationFences[key] = vectorPartitionLifecycleMutationFenceStateV1{Epoch: 10, Pending: true}
		a.mu.Unlock()
		raw, err := EncodeReplicaReplacementBeginV1(begin)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.applyCommittedCatalogMetaV1(raw, a.applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
			t.Fatalf("pending mutation BEGIN err=%v", err)
		}
	})
	t.Run("building", func(t *testing.T) {
		a, begin, building := activeReplicaReplacementAuthorityV1(t, false)
		if building.State != VectorPartitionLifecycleBuildingV1 {
			t.Fatalf("fixture state=%q", building.State)
		}
		raw, err := EncodeReplicaReplacementBeginV1(begin)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.applyCommittedCatalogMetaV1(raw, a.applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
			t.Fatalf("BUILDING BEGIN err=%v", err)
		}
	})
}
