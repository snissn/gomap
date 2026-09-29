package raftplacement

import (
	"bytes"
	"encoding/json"
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
	// A terminal tombstone is retained alongside ACTIVE state. Completion must
	// rebind it too, or the exported snapshot cannot be restored.
	absent := cloneVectorPartitionLifecycleRecordV1(before)
	absent.Identity.Index.IndexName = "retired-embedding"
	absent.State = VectorPartitionLifecycleAbsentV1
	absent.CleanupComplete = true
	absent.RequiredGroups = nil
	absent.ReadyGroups = nil
	absent.ReadySetDigest = ""
	if _, err := EncodeVectorPartitionLifecycleRecordV1(absent); err != nil {
		t.Fatalf("terminal tombstone fixture: %v", err)
	}
	a.mu.Lock()
	a.lifecycle[absent.Identity] = absent
	lifecycleRaw, err := encodeVectorPartitionLifecycleSnapshotV1(a.lifecycle, a.mutationFences, a.collectionMutationBarriers)
	a.lifecycleBytes = uint64(len(lifecycleRaw))
	a.mu.Unlock()
	if err != nil {
		t.Fatalf("terminal tombstone snapshot: %v", err)
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
	reboundAbsent := absent.Identity
	reboundAbsent.Index.CatalogEpoch = complete.Catalog.Record.Epoch
	reboundAbsent.Index.CatalogDigest = complete.Catalog.Record.Digest
	if got, ok := a.VectorPartitionLifecycleRecordV1(reboundAbsent); !ok || got.State != VectorPartitionLifecycleAbsentV1 {
		t.Fatalf("terminal tombstone was not rebound: %+v available=%v", got, ok)
	}
	if err := catalogMetaLifecycleValidateSearchV1(a, rebound, after.ReadySetDigest); err != nil {
		t.Fatalf("rebound ACTIVE authority: %v", err)
	}
	final, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	fresh := NewCatalogMetaAuthorityV1()
	if err := fresh.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("fresh restore of completed roster and rebound ACTIVE: %v", err)
	}
	if err := catalogMetaLifecycleValidateSearchV1(fresh, rebound, after.ReadySetDigest); err != nil {
		t.Fatalf("freshly restored ACTIVE authority: %v", err)
	}
	restored := NewCatalogMetaAuthorityV1()
	if err := restored.installCatalogMetaSnapshotBytesV1(pending); err != nil {
		t.Fatalf("restore pending: %v", err)
	}
	var forbidden CatalogMetaSnapshotV1
	if err := json.Unmarshal(final, &forbidden); err != nil {
		t.Fatal(err)
	}
	forbiddenRecords, err := decodeReplicaReplacementSnapshotV1(forbidden.ReplicaReplacements, complete.Catalog.Record)
	if err != nil {
		t.Fatal(err)
	}
	forbiddenBegin := activeReplicaReplacementBeginV1(complete.Catalog.Record)
	forbiddenBegin.OperationID = "replace-active-source"
	forbiddenBegin.GroupID = "group-a"
	forbiddenBegin.OldNodeID = "node-a"
	forbiddenBegin.NewPeer = raftcluster.Peer{ID: "source-spare", Address: "127.0.0.1:19002"}
	forbiddenRecords[forbiddenBegin.GroupID], err = EncodeReplicaReplacementBeginV1(forbiddenBegin)
	if err != nil {
		t.Fatal(err)
	}
	forbidden.ReplicaReplacements, err = encodeReplicaReplacementSnapshotV1(forbiddenRecords)
	if err != nil {
		t.Fatal(err)
	}
	forbiddenRaw, err := json.Marshal(forbidden)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.installCatalogMetaSnapshotBytesV1(forbiddenRaw); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("completion snapshot introduced source-group BEGIN: %v", err)
	}
	if retained, err := restored.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, pending) {
		t.Fatalf("refused source-group BEGIN mutated authority: %v", err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(final, &forged); err != nil {
		t.Fatal(err)
	}
	var forgedLifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &forgedLifecycle); err != nil {
		t.Fatal(err)
	}
	forgedLifecycle.Records[0].ReadyGroups[0].AssetSetDigest = strings.Repeat("e", 64)
	forgedLifecycle.Records[0].ReadySetDigest, err = VectorPartitionLifecycleReadySetDigestV1(
		forgedLifecycle.Records[0].Identity, forgedLifecycle.Records[0].RequiredGroups, forgedLifecycle.Records[0].ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	forged.VectorPartitionLifecycle, err = json.Marshal(forgedLifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("replacement snapshot changed committed READY receipt: %v", err)
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

func TestCatalogReplicaReplacementActiveSnapshotRefusesSourceBeginV1(t *testing.T) {
	a, _, _ := activeReplicaReplacementAuthorityV1(t, true)
	before, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(before, &forged); err != nil {
		t.Fatal(err)
	}
	begin := activeReplicaReplacementBeginV1(a.record)
	begin.OperationID = "replace-active-source"
	begin.GroupID = "group-a"
	begin.OldNodeID = "node-a"
	begin.NewPeer = raftcluster.Peer{ID: "source-spare", Address: "127.0.0.1:19002"}
	beginRaw, err := EncodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	forged.ReplicaReplacements, err = encodeReplicaReplacementSnapshotV1(map[raftcluster.GroupID][]byte{begin.GroupID: beginRaw})
	if err != nil {
		t.Fatal(err)
	}
	forged.AppliedIndex++
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("same-epoch snapshot introduced source-group BEGIN: %v", err)
	}
	if retained, err := a.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused same-epoch source-group BEGIN mutated authority: %v", err)
	}
}

func TestCatalogReplicaReplacementActiveSnapshotCompactedRecoveryV1(t *testing.T) {
	t.Run("completion without local begin", func(t *testing.T) {
		leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
		before, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		completeReplicaReplacementForTestV1(t, leader, begin)
		final, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
			t.Fatalf("restore committed completion without local BEGIN: %v", err)
		}
		rebound := active.Identity
		rebound.Index.CatalogEpoch = leader.record.Epoch
		rebound.Index.CatalogDigest = leader.record.Digest
		got, ok := follower.VectorPartitionLifecycleRecordV1(rebound)
		if !ok || got.State != VectorPartitionLifecycleActiveV1 || !reflect.DeepEqual(got.ReadyGroups, active.ReadyGroups) {
			t.Fatalf("restored ACTIVE receipts=%+v available=%v", got, ok)
		}
	})

	t.Run("two completed epochs", func(t *testing.T) {
		leader, first, active := activeReplicaReplacementAuthorityV1(t, true)
		before, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		completeReplicaReplacementForTestV1(t, leader, first)
		second := first
		second.OperationID = "replace-active-ann-owner-again"
		second.ExpectedEpoch = leader.record.Epoch
		second.CatalogDigest = leader.record.Digest
		second.OldNodeID = first.NewPeer.ID
		second.NewPeer = raftcluster.Peer{ID: "standby-two", Address: "127.0.0.1:19003"}
		completeReplicaReplacementForTestV1(t, leader, second)
		final, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
			t.Fatalf("restore two committed replacement epochs: %v", err)
		}
		rebound := active.Identity
		rebound.Index.CatalogEpoch = leader.record.Epoch
		rebound.Index.CatalogDigest = leader.record.Digest
		got, ok := follower.VectorPartitionLifecycleRecordV1(rebound)
		if !ok || got.State != VectorPartitionLifecycleActiveV1 || !reflect.DeepEqual(got.ReadyGroups, active.ReadyGroups) {
			t.Fatalf("restored ACTIVE receipts=%+v available=%v", got, ok)
		}
	})

	t.Run("invalidation after completion", func(t *testing.T) {
		leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
		before, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		completeReplicaReplacementForTestV1(t, leader, begin)
		identity := active.Identity
		identity.Index.CatalogEpoch = leader.record.Epoch
		identity.Index.CatalogDigest = leader.record.Digest
		active, ok := leader.VectorPartitionLifecycleRecordV1(identity)
		if !ok {
			t.Fatal("completed authority lost ACTIVE record")
		}
		applied := leader.applied
		invalidated := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
			command.Reason = "relevant mutation"
			command.InvalidationEpoch = active.MutationEpoch + 1
		}))
		final, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
			t.Fatal(err)
		}
		var forged CatalogMetaSnapshotV1
		if err := json.Unmarshal(final, &forged); err != nil {
			t.Fatal(err)
		}
		var lifecycle vectorPartitionLifecycleSnapshotV1
		if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
			t.Fatal(err)
		}
		for i := range lifecycle.Records {
			if lifecycle.Records[i].State != VectorPartitionLifecycleInvalidatedV1 {
				continue
			}
			lifecycle.Records[i].ReadyGroups[0].AssetSetDigest = strings.Repeat("e", 64)
			lifecycle.Records[i].ReadySetDigest, err = VectorPartitionLifecycleReadySetDigestV1(
				lifecycle.Records[i].Identity, lifecycle.Records[i].RequiredGroups, lifecycle.Records[i].ReadyGroups)
			if err != nil {
				t.Fatal(err)
			}
		}
		forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
		if err != nil {
			t.Fatal(err)
		}
		forgedRaw, err := json.Marshal(forged)
		if err != nil {
			t.Fatal(err)
		}
		if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
			t.Fatalf("forged READY receipt snapshot was not self-consistent: %v", err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
			t.Fatalf("higher-revision invalidation changed committed READY receipt: %v", err)
		}
		if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
			t.Fatalf("refused higher-revision snapshot mutated authority: %v", err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
			t.Fatalf("restore committed post-completion invalidation: %v", err)
		}
		got, ok := follower.VectorPartitionLifecycleRecordV1(invalidated.Identity)
		if !ok || !reflect.DeepEqual(got, invalidated) {
			t.Fatalf("restored invalidation=%+v available=%v want %+v", got, ok, invalidated)
		}
	})

	t.Run("source roster changed", func(t *testing.T) {
		leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
		before, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		completeReplicaReplacementForTestV1(t, leader, begin)
		forged, err := leader.ExportCatalogMetaSnapshotV1()
		if err != nil {
			t.Fatal(err)
		}
		// Forge a self-consistent completed source roster from the committed
		// non-source evidence. It cannot be obtained through the live BEGIN guard.
		catalog := cloneCatalog(leader.record.Catalog)
		for i := range catalog.Groups {
			if catalog.Groups[i].ID == "group-a" {
				catalog.Groups[i].Members = []raftcluster.NodeID{"node-c", "source-spare"}
				catalog.Groups[i].LeaderHint = ""
			} else if catalog.Groups[i].ID == "group-b" {
				catalog.Groups[i].Members = []raftcluster.NodeID{"node-b", "node-c"}
				catalog.Groups[i].LeaderHint = "node-b"
			}
		}
		record, err := NewCatalogMetaRecordV1(leader.record.Epoch, catalog)
		if err != nil {
			t.Fatal(err)
		}
		forged.Record, err = encodeCatalogMetaRecordV1(record)
		if err != nil {
			t.Fatal(err)
		}
		forged.LastCommand, err = EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: record.Epoch - 1, Record: record})
		if err != nil {
			t.Fatal(err)
		}
		state, err := leader.ReplicaReplacementStateV1("group-b")
		if err != nil {
			t.Fatal(err)
		}
		state.Begin.GroupID = "group-a"
		state.Begin.OldNodeID = "node-a"
		state.Begin.NewPeer = raftcluster.Peer{ID: "source-spare", Address: "127.0.0.1:19002"}
		state.Seed.SourceNodeID = "node-a"
		state.Seed.Manifest.NodeID = "node-a"
		state.Seed.Manifest.GroupID = "group-a"
		state.Tail.GroupID = "group-a"
		state.Tail.LeaderID = "node-a"
		state.Peers = []raftcluster.Peer{{ID: "node-c", Address: "127.0.0.1:20001"}, state.Begin.NewPeer}
		state.Result.Digest = record.Digest
		stateRaw, err := EncodeReplicaReplacementStateV1(state)
		if err != nil {
			t.Fatal(err)
		}
		forged.ReplicaReplacements, err = encodeReplicaReplacementSnapshotV1(map[raftcluster.GroupID][]byte{"group-a": stateRaw})
		if err != nil {
			t.Fatal(err)
		}
		active.Identity.Index.CatalogEpoch = record.Epoch
		active.Identity.Index.CatalogDigest = record.Digest
		active.ReadySetDigest, err = VectorPartitionLifecycleReadySetDigestV1(active.Identity, active.RequiredGroups, active.ReadyGroups)
		if err != nil {
			t.Fatal(err)
		}
		forged.VectorPartitionLifecycle, err = encodeVectorPartitionLifecycleSnapshotV1(
			map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1{active.Identity: active}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		forgedRaw, err := json.Marshal(forged)
		if err != nil {
			t.Fatal(err)
		}
		if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
			t.Fatalf("forged source-roster snapshot was not self-consistent: %v", err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
			t.Fatalf("forward snapshot changed ACTIVE source roster: %v", err)
		}
		if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
			t.Fatalf("refused source-roster snapshot mutated authority: %v", err)
		}
	})
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
