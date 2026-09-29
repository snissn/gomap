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

func TestCatalogReplicaReplacementActiveSameEpochSnapshotPreservesEvidenceV1(t *testing.T) {
	for _, name := range []string{"ready receipt", "mutation fence", "completed mutation receipt"} {
		t.Run(name, func(t *testing.T) {
			a, _, active := activeReplicaReplacementAuthorityV1(t, true)
			if name == "mutation fence" {
				a.mu.Lock()
				if a.mutationFences == nil {
					a.mutationFences = make(map[vectorPartitionLifecycleServingKeyV1]vectorPartitionLifecycleMutationFenceStateV1)
				}
				a.mutationFences[vectorPartitionLifecycleServingKeyV1{
					Collection: active.Identity.Index.Collection, IndexName: active.Identity.Index.IndexName,
				}] = vectorPartitionLifecycleMutationFenceStateV1{Epoch: 11}
				a.mu.Unlock()
			}
			if name == "completed mutation receipt" {
				a.mu.Lock()
				if a.collectionMutationBarriers == nil {
					a.collectionMutationBarriers = make(map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1)
				}
				a.collectionMutationBarriers[active.Identity.Index.Collection] = vectorPartitionCollectionMutationBarrierStateV1{
					Epoch: 10, Pending: true, OperationDigest: strings.Repeat("e", 64),
					Completed: []vectorPartitionCollectionCompletedMutationV1{{Epoch: 9, OperationDigest: strings.Repeat("d", 64)}},
				}
				a.mu.Unlock()
			}
			before, err := a.ExportCatalogMetaSnapshotBytesV1()
			if err != nil {
				t.Fatal(err)
			}
			var forged CatalogMetaSnapshotV1
			if err := json.Unmarshal(before, &forged); err != nil {
				t.Fatal(err)
			}
			var lifecycle vectorPartitionLifecycleSnapshotV1
			if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "ready receipt":
				lifecycle.Records[0].ReadyGroups[0].AssetSetDigest = strings.Repeat("f", 64)
				lifecycle.Records[0].ReadySetDigest, err = VectorPartitionLifecycleReadySetDigestV1(
					lifecycle.Records[0].Identity, lifecycle.Records[0].RequiredGroups, lifecycle.Records[0].ReadyGroups)
				if err != nil {
					t.Fatal(err)
				}
			case "mutation fence":
				lifecycle.MutationFences[0].Epoch = 10
			case "completed mutation receipt":
				lifecycle.CollectionMutationBarriers[0].Completed[0].OperationDigest = strings.Repeat("f", 64)
			}
			forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
			if err != nil {
				t.Fatal(err)
			}
			forged.AppliedIndex++
			forgedRaw, err := json.Marshal(forged)
			if err != nil {
				t.Fatal(err)
			}
			if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
				t.Fatalf("forged snapshot is not self-consistent: %v", err)
			}
			if err := a.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
				t.Fatalf("same-epoch snapshot changed %s: %v", name, err)
			}
			if retained, err := a.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
				t.Fatalf("refused same-epoch %s mutated authority: %v", name, err)
			}
		})
	}
}

func TestCatalogReplicaReplacementInvalidatedSameEpochSnapshotCannotReviveActiveV1(t *testing.T) {
	a, _, active := activeReplicaReplacementAuthorityV1(t, true)
	activeRaw, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	applied := a.applied
	invalidated := catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "relevant mutation"
		command.InvalidationEpoch = active.MutationEpoch + 1
	}))
	_ = catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(invalidated, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = invalidated.InvalidationEpoch
	}))
	before, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	var forged, current CatalogMetaSnapshotV1
	if err := json.Unmarshal(activeRaw, &forged); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(before, &current); err != nil {
		t.Fatal(err)
	}
	var activeLifecycle, currentLifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &activeLifecycle); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(current.VectorPartitionLifecycle, &currentLifecycle); err != nil {
		t.Fatal(err)
	}
	activeLifecycle.MutationFences = currentLifecycle.MutationFences
	activeLifecycle.CollectionMutationBarriers = currentLifecycle.CollectionMutationBarriers
	forged.VectorPartitionLifecycle, err = json.Marshal(activeLifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forged.AppliedIndex = current.AppliedIndex + 1
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("revival snapshot is not self-consistent: %v", err)
	}
	if err := a.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("snapshot revived invalidated generation: %v", err)
	}
	if retained, err := a.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused revival mutated authority: %v", err)
	}
}

func TestCatalogReplicaReplacementInvalidatedSameEpochSnapshotAcceptsConfirmationV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	applied := leader.applied
	invalidated := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "relevant mutation"
		command.InvalidationEpoch = active.MutationEpoch + 1
	}))
	pending, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	confirmed := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(invalidated, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = invalidated.InvalidationEpoch
	}))
	final, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(pending); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("restore confirmed mutation after invalidation: %v", err)
	}
	got, ok := follower.VectorPartitionLifecycleRecordV1(confirmed.Identity)
	if !ok || !reflect.DeepEqual(got, confirmed) {
		t.Fatalf("restored confirmation=%+v available=%v want %+v", got, ok, confirmed)
	}
}

func TestCatalogReplicaReplacementCleanupSnapshotCannotReviveActiveV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	activeRaw, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	applied := leader.applied
	record := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "relevant mutation"
		command.InvalidationEpoch = active.MutationEpoch + 1
	}))
	record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = record.InvalidationEpoch
	}))
	confirmedRaw, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(confirmedRaw); err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		kind  VectorPartitionLifecycleCommandKindV1
		group raftcluster.GroupID
	}{
		{kind: VectorPartitionLifecycleRetireV1},
		{kind: VectorPartitionLifecycleMarkCleanableV1},
		{kind: VectorPartitionLifecycleRecordGroupCleanupV1, group: "group-b"},
		{kind: VectorPartitionLifecycleCompleteCleanupV1},
	}
	for _, step := range steps {
		record = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(record, step.kind, func(command *VectorPartitionLifecycleCommandV1) {
			command.GroupID = step.group
		}))
		currentRaw, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(currentRaw); err != nil {
			t.Fatalf("restore %s: %v", record.State, err)
		}
		if got, ok := follower.VectorPartitionLifecycleRecordV1(record.Identity); !ok || !reflect.DeepEqual(got, record) {
			t.Fatalf("restored %s=%+v available=%v want %+v", record.State, got, ok, record)
		}
		var forged, current CatalogMetaSnapshotV1
		if err := json.Unmarshal(activeRaw, &forged); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(currentRaw, &current); err != nil {
			t.Fatal(err)
		}
		var oldLifecycle, currentLifecycle vectorPartitionLifecycleSnapshotV1
		if err := json.Unmarshal(forged.VectorPartitionLifecycle, &oldLifecycle); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(current.VectorPartitionLifecycle, &currentLifecycle); err != nil {
			t.Fatal(err)
		}
		oldLifecycle.MutationFences = currentLifecycle.MutationFences
		oldLifecycle.CollectionMutationBarriers = currentLifecycle.CollectionMutationBarriers
		forged.VectorPartitionLifecycle, err = json.Marshal(oldLifecycle)
		if err != nil {
			t.Fatal(err)
		}
		forged.AppliedIndex = current.AppliedIndex + 1
		forgedRaw, err := json.Marshal(forged)
		if err != nil {
			t.Fatal(err)
		}
		if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
			t.Fatalf("forged %s revival snapshot is not self-consistent: %v", record.State, err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
			t.Fatalf("snapshot revived %s generation: %v", record.State, err)
		}
		if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, currentRaw) {
			t.Fatalf("refused %s revival mutated follower: %v", record.State, err)
		}
	}
}

func TestCatalogReplicaReplacementActiveSnapshotAllowsSkippedCollectionMutationEpochV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	collection := active.Identity.Index.Collection
	leader.mu.Lock()
	leader.collectionMutationBarriers = map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1{
		collection: {
			Epoch: 2, OperationDigest: strings.Repeat("d", 64),
			Completed: []vectorPartitionCollectionCompletedMutationV1{{Epoch: 2, OperationDigest: strings.Repeat("d", 64)}},
		},
	}
	leader.mu.Unlock()
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	command := vectorPartitionCollectionMutationCommandV1{
		Kind: vectorPartitionBeginCollectionMutationV1, Collection: collection,
		CatalogEpoch: leader.record.Epoch, CatalogDigest: leader.record.Digest,
		ExpectedMutationEpoch: active.MutationEpoch, MutationEpoch: active.MutationEpoch + 1,
		OperationDigest: strings.Repeat("e", 64),
	}
	for _, kind := range []vectorPartitionCollectionMutationCommandKindV1{
		vectorPartitionBeginCollectionMutationV1, vectorPartitionConfirmCollectionMutationV1,
	} {
		command.Kind = kind
		if kind == vectorPartitionConfirmCollectionMutationV1 {
			command.ExpectedMutationEpoch = command.MutationEpoch
		}
		raw, err := encodeVectorPartitionCollectionMutationCommandV1(command)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.applyCommittedVectorPartitionCollectionMutationV1(raw, leader.applied+1); err != nil {
			t.Fatalf("commit %s: %v", kind, err)
		}
	}
	final, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
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
	lifecycle.CollectionMutationBarriers[0].Completed = lifecycle.CollectionMutationBarriers[0].Completed[1:]
	forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forged.AppliedIndex++
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("forged dropped-receipt snapshot is not self-consistent: %v", err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("skipped-epoch snapshot dropped retained receipt: %v", err)
	}
	if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused snapshot changed follower: %v", err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("restore skipped collection mutation epoch: %v", err)
	}
}

func TestCatalogReplicaReplacementActiveSnapshotCompactedRecoveryV1(t *testing.T) {
	t.Run("completion then next begin", func(t *testing.T) {
		leader, first, active := activeReplicaReplacementAuthorityV1(t, true)
		before, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		completeReplicaReplacementForTestV1(t, leader, first)
		second := first
		second.OperationID = "replace-active-ann-owner-next"
		second.ExpectedEpoch = leader.record.Epoch
		second.CatalogDigest = leader.record.Digest
		second.OldNodeID = first.NewPeer.ID
		second.NewPeer = raftcluster.Peer{ID: "standby-two", Address: "127.0.0.1:19003"}
		raw, err := EncodeReplicaReplacementBeginV1(second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.applyCommittedCatalogMetaV1(raw, leader.applied+1); err != nil {
			t.Fatal(err)
		}
		final, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
			t.Fatalf("restore completion followed by next BEGIN: %v", err)
		}
		rebound := active.Identity
		rebound.Index.CatalogEpoch = leader.record.Epoch
		rebound.Index.CatalogDigest = leader.record.Digest
		got, ok := follower.VectorPartitionLifecycleRecordV1(rebound)
		if !ok || got.State != VectorPartitionLifecycleActiveV1 || !reflect.DeepEqual(got.ReadyGroups, active.ReadyGroups) {
			t.Fatalf("restored ACTIVE receipts=%+v available=%v", got, ok)
		}
	})

	t.Run("round trip roster", func(t *testing.T) {
		leader, first, active := activeReplicaReplacementAuthorityV1(t, true)
		before, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		completeReplicaReplacementForTestV1(t, leader, first)
		second := first
		second.OperationID = "replace-active-ann-owner-return"
		second.ExpectedEpoch = leader.record.Epoch
		second.CatalogDigest = leader.record.Digest
		second.OldNodeID = first.NewPeer.ID
		second.NewPeer = raftcluster.Peer{ID: first.OldNodeID, Address: "127.0.0.1:20000"}
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
			t.Fatalf("restore round-trip completed roster: %v", err)
		}
		rebound := active.Identity
		rebound.Index.CatalogEpoch = leader.record.Epoch
		rebound.Index.CatalogDigest = leader.record.Digest
		got, ok := follower.VectorPartitionLifecycleRecordV1(rebound)
		if !ok || got.State != VectorPartitionLifecycleActiveV1 || !reflect.DeepEqual(got.ReadyGroups, active.ReadyGroups) {
			t.Fatalf("restored ACTIVE receipts=%+v available=%v", got, ok)
		}
	})

	t.Run("round trip roster then next begin", func(t *testing.T) {
		leader, first, active := activeReplicaReplacementAuthorityV1(t, true)
		before, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		completeReplicaReplacementForTestV1(t, leader, first)
		second := first
		second.OperationID = "replace-active-ann-owner-return-before-next"
		second.ExpectedEpoch = leader.record.Epoch
		second.CatalogDigest = leader.record.Digest
		second.OldNodeID = first.NewPeer.ID
		second.NewPeer = raftcluster.Peer{ID: first.OldNodeID, Address: "127.0.0.1:20000"}
		completeReplicaReplacementForTestV1(t, leader, second)
		third := first
		third.OperationID = "replace-active-ann-owner-after-roundtrip"
		third.ExpectedEpoch = leader.record.Epoch
		third.CatalogDigest = leader.record.Digest
		third.NewPeer = raftcluster.Peer{ID: "standby-two", Address: "127.0.0.1:19003"}
		raw, err := EncodeReplicaReplacementBeginV1(third)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.applyCommittedCatalogMetaV1(raw, leader.applied+1); err != nil {
			t.Fatal(err)
		}
		final, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
			t.Fatalf("restore completed round trip followed by BEGIN: %v", err)
		}
		rebound := active.Identity
		rebound.Index.CatalogEpoch = leader.record.Epoch
		rebound.Index.CatalogDigest = leader.record.Digest
		got, ok := follower.VectorPartitionLifecycleRecordV1(rebound)
		if !ok || got.State != VectorPartitionLifecycleActiveV1 || !reflect.DeepEqual(got.ReadyGroups, active.ReadyGroups) {
			t.Fatalf("restored ACTIVE receipts=%+v available=%v", got, ok)
		}
	})

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

func TestCatalogReplicaReplacementSnapshotRejectsUnactivatedSupersessionV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	completeReplicaReplacementForTestV1(t, leader, begin)
	candidateIdentity := catalogMetaLifecycleTestIdentityV1(leader.record, active.Identity.Generation+1, 12)
	candidateIdentity.Immutable = active.Identity.Immutable
	applied := leader.applied
	candidate := catalogMetaLifecycleApplyV1(t, leader, &applied, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: candidateIdentity, RequiredGroups: []raftcluster.GroupID{"group-b"},
		PreviousActiveGeneration: active.Identity.Generation, MutationEpoch: active.MutationEpoch + 1,
	})
	if candidate.State != VectorPartitionLifecycleBuildingV1 {
		t.Fatalf("candidate state=%q", candidate.State)
	}
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	forged, err := leader.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	for i := range lifecycle.Records {
		if lifecycle.Records[i].Identity.Generation == active.Identity.Generation {
			lifecycle.Records[i].State = VectorPartitionLifecycleRetiredV1
			lifecycle.Records[i].SupersededByGeneration = candidateIdentity.Generation
			lifecycle.Records[i].Revision++
			lifecycle.Records[i].LastCommandDigest = strings.Repeat("f", 64)
		}
	}
	forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forged.AppliedIndex++
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("forged snapshot was not internally canonical: %v", err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("unactivated successor retired serving generation: %v", err)
	}
	if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused snapshot mutated authority: %v", err)
	}
	// An ABSENT candidate can be internally canonical without having ever
	// activated. Its terminal state alone cannot prove the old cutover.
	for i := range lifecycle.Records {
		if lifecycle.Records[i].Identity == candidateIdentity {
			lifecycle.Records[i].State = VectorPartitionLifecycleAbsentV1
			lifecycle.Records[i].Revision++
			lifecycle.Records[i].RequiredGroups = []raftcluster.GroupID{}
			lifecycle.Records[i].ReadyGroups = []VectorPartitionLifecycleGroupReadyV1{}
			lifecycle.Records[i].CleanedGroups = []raftcluster.GroupID{}
			lifecycle.Records[i].CleanupComplete = true
		}
	}
	forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	terminalRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(terminalRaw); err != nil {
		t.Fatalf("terminal forgery was not internally canonical: %v", err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(terminalRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("unactivated terminal successor retired serving generation: %v", err)
	}
	if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused terminal snapshot mutated authority: %v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotAcceptsCompactedCutoverCleanupV1(t *testing.T) {
	leader, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)
	oldIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	old := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, oldIdentity, 0, 9)
	old = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(old, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 9
	}))
	newIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 8, 12)
	candidate := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, newIdentity, oldIdentity.Generation, 10)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	candidate = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.PreviousActiveGeneration = oldIdentity.Generation
		command.PreviousActiveRevision = old.Revision
		command.MutationEpoch = 10
	}))
	cutover, err := leader.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cutover digest", "predecessor"} {
		t.Run(name, func(t *testing.T) {
			forged := cutover
			var lifecycle vectorPartitionLifecycleSnapshotV1
			if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
				t.Fatal(err)
			}
			for i := range lifecycle.Records {
				switch {
				case name == "cutover digest" && lifecycle.Records[i].Identity == oldIdentity:
					lifecycle.Records[i].LastCommandDigest = strings.Repeat("f", 64)
				case name == "predecessor" && lifecycle.Records[i].Identity == newIdentity:
					lifecycle.Records[i].PreviousActiveGeneration = 0
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
				t.Fatalf("forged %s snapshot was not internally canonical: %v", name, err)
			}
			follower := NewCatalogMetaAuthorityV1()
			if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
				t.Fatal(err)
			}
			if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
				t.Fatalf("snapshot changed %s: %v", name, err)
			}
			if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
				t.Fatalf("refused %s snapshot mutated authority: %v", name, err)
			}
		})
	}
	candidate = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "relevant mutation"
		command.InvalidationEpoch = 11
	}))
	candidate = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 11
	}))
	for _, kind := range []VectorPartitionLifecycleCommandKindV1{
		VectorPartitionLifecycleRetireV1, VectorPartitionLifecycleMarkCleanableV1,
		VectorPartitionLifecycleRecordGroupCleanupV1, VectorPartitionLifecycleCompleteCleanupV1,
	} {
		candidate = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, kind, func(command *VectorPartitionLifecycleCommandV1) {
			if kind == VectorPartitionLifecycleRecordGroupCleanupV1 {
				command.GroupID = "group-a"
			}
		}))
	}
	final, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("restore compacted cutover through successor cleanup: %v", err)
	}
	if got, ok := follower.VectorPartitionLifecycleRecordV1(newIdentity); !ok || !reflect.DeepEqual(got, candidate) || got.State != VectorPartitionLifecycleAbsentV1 {
		t.Fatalf("restored successor=%+v available=%v want %+v", got, ok, candidate)
	}
}

func TestCatalogReplicaReplacementSnapshotRejectsSkippedCleanupRevisionsV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	completeReplicaReplacementForTestV1(t, leader, begin)
	completed, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(completed, &forged); err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	found := false
	var rebound VectorPartitionLifecycleRecordV1
	for i := range lifecycle.Records {
		if lifecycle.Records[i].Identity.Generation != active.Identity.Generation {
			continue
		}
		record := &lifecycle.Records[i]
		rebound = *record
		record.State = VectorPartitionLifecycleAbsentV1
		record.Revision++ // Cleanup needs multiple committed reducer commands.
		record.InvalidationEpoch = record.MutationEpoch + 1
		record.InvalidationReason = "relevant mutation"
		record.MutationConfirmed = true
		record.CleanupComplete = true
		record.RequiredGroups = []raftcluster.GroupID{}
		record.ReadyGroups = []VectorPartitionLifecycleGroupReadyV1{}
		record.ReadySetDigest = ""
		record.CleanedGroups = []raftcluster.GroupID{}
		record.LastCommandDigest = strings.Repeat("e", 64)
		lifecycle.MutationFences = append(lifecycle.MutationFences, vectorPartitionLifecycleMutationFenceV1{
			Collection: record.Identity.Index.Collection, IndexName: record.Identity.Index.IndexName,
			Epoch: record.InvalidationEpoch,
		})
		found = true
	}
	if !found {
		t.Fatal("ACTIVE generation missing from completed catalog")
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
		t.Fatalf("forged cleanup snapshot was not internally canonical: %v", err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("unreachable ACTIVE-to-ABSENT revision jump restored: %v", err)
	}
	if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused cleanup jump mutated follower: %v", err)
	}
	// The actual reducer path may be compacted into one snapshot and must
	// remain restorable despite skipping all intermediate snapshot installs.
	applied := leader.applied
	real := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(rebound, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "relevant mutation"
		command.InvalidationEpoch = rebound.MutationEpoch + 1
	}))
	real = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(real, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = real.InvalidationEpoch
	}))
	for _, step := range []struct {
		kind  VectorPartitionLifecycleCommandKindV1
		group raftcluster.GroupID
	}{
		{kind: VectorPartitionLifecycleRetireV1},
		{kind: VectorPartitionLifecycleMarkCleanableV1},
		{kind: VectorPartitionLifecycleRecordGroupCleanupV1, group: "group-b"},
		{kind: VectorPartitionLifecycleCompleteCleanupV1},
	} {
		real = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(real, step.kind, func(command *VectorPartitionLifecycleCommandV1) {
			command.GroupID = step.group
		}))
	}
	final, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("restore genuine compacted cleanup: %v", err)
	}
	if got, ok := follower.VectorPartitionLifecycleRecordV1(real.Identity); !ok || !reflect.DeepEqual(got, real) {
		t.Fatalf("restored cleanup=%+v available=%v want %+v", got, ok, real)
	}
}

func TestCatalogReplicaReplacementSnapshotRejectsExcessInvalidationRevisionsV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	completeReplicaReplacementForTestV1(t, leader, begin)
	completed, err := leader.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(completed.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	found := false
	for i := range lifecycle.Records {
		if lifecycle.Records[i].Identity.Generation != active.Identity.Generation {
			continue
		}
		found = true
		record := &lifecycle.Records[i]
		record.State = VectorPartitionLifecycleInvalidatedV1
		record.Revision += 2 // One INVALIDATE cannot advance the reducer twice.
		record.InvalidationEpoch = record.MutationEpoch + 1
		record.InvalidationReason = "relevant mutation"
		record.LastCommandDigest = strings.Repeat("e", 64)
		lifecycle.MutationFences = append(lifecycle.MutationFences, vectorPartitionLifecycleMutationFenceV1{
			Collection: record.Identity.Index.Collection, IndexName: record.Identity.Index.IndexName,
			Epoch: record.InvalidationEpoch, Pending: true,
		})
	}
	if !found {
		t.Fatal("rebound ACTIVE record missing")
	}
	completed.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := json.Marshal(completed)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forged); err != nil {
		t.Fatalf("forged snapshot was not internally canonical: %v", err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(forged); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("excess invalidation revisions restored: %v", err)
	}
	if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused invalidation jump mutated follower: %v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotRejectsForgedNewActiveReadyV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	complete := completeReplicaReplacementForTestV1(t, leader, begin)
	rebound := active.Identity
	rebound.Index.CatalogEpoch = complete.Catalog.Record.Epoch
	rebound.Index.CatalogDigest = complete.Catalog.Record.Digest
	old, ok := leader.VectorPartitionLifecycleRecordV1(rebound)
	if !ok {
		t.Fatal("rebound active record missing")
	}
	applied := leader.applied
	identity := catalogMetaLifecycleTestIdentityV1(complete.Catalog.Record, active.Identity.Generation+1, 12)
	candidate := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, identity, old.Identity.Generation, old.MutationEpoch+1)
	catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.PreviousActiveGeneration = old.Identity.Generation
		command.PreviousActiveRevision = old.Revision
		command.MutationEpoch = candidate.MutationEpoch
	}))
	cutover, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(cutover); err != nil {
		t.Fatalf("genuine compacted cutover: %v", err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(cutover, &forged); err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	found := false
	for i := range lifecycle.Records {
		if lifecycle.Records[i].Identity != identity {
			continue
		}
		found = true
		record := &lifecycle.Records[i]
		if len(record.ReadyGroups) != 1 {
			t.Fatalf("new ACTIVE ready groups=%d", len(record.ReadyGroups))
		}
		record.ReadyGroups[0].AssetSetDigest = strings.Repeat("f", 64)
		record.ReadySetDigest, err = VectorPartitionLifecycleReadySetDigestV1(record.Identity, record.RequiredGroups, record.ReadyGroups)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !found {
		t.Fatal("new ACTIVE record missing")
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
		t.Fatalf("forged snapshot was not internally canonical: %v", err)
	}
	refusing := NewCatalogMetaAuthorityV1()
	if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("new ACTIVE with forged READY receipt restored: %v", err)
	}
	if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused new ACTIVE mutated follower: %v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotValidatesNewPreparationV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	completeReplicaReplacementForTestV1(t, leader, begin)
	identity := catalogMetaLifecycleTestIdentityV1(leader.record, active.Identity.Generation+1, 12)
	identity.Immutable = active.Identity.Immutable
	applied := leader.applied
	building := catalogMetaLifecycleApplyV1(t, leader, &applied, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-b"},
		PreviousActiveGeneration: active.Identity.Generation, MutationEpoch: active.MutationEpoch + 1,
	})
	buildingRaw, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	staged := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(building, VectorPartitionLifecycleRecordGroupReadyV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.GroupReady = VectorPartitionLifecycleGroupReadyV1{GroupID: "group-b", AppliedIndex: applied, AssetSetDigest: strings.Repeat("c", 64)}
	}))
	stagedRaw, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	readyDigest, err := VectorPartitionLifecycleReadySetDigestV1(identity, staged.RequiredGroups, staged.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	prepared := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(staged, VectorPartitionLifecyclePrepareV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.ReadySetDigest = readyDigest
	}))
	valid, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"building", buildingRaw},
		{"staged", stagedRaw},
		{"prepared", valid},
	} {
		t.Run("committed "+tc.name, func(t *testing.T) {
			follower := NewCatalogMetaAuthorityV1()
			if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
				t.Fatal(err)
			}
			if err := follower.installCatalogMetaSnapshotBytesV1(tc.raw); err != nil {
				t.Fatalf("committed %s successor did not restore: %v", tc.name, err)
			}
		})
	}
	impossibleBuilding := building
	impossibleBuilding.Revision++ // BEGIN creates revision 1; READY moves to STAGED.
	impossibleStaged := staged
	impossibleStaged.Revision++ // One READY after BEGIN creates revision 2.
	changedPrepared := cloneVectorPartitionLifecycleRecordV1(prepared)
	changedPrepared.ReadyGroups[0].AssetSetDigest = strings.Repeat("d", 64)
	changedPrepared.ReadySetDigest, err = VectorPartitionLifecycleReadySetDigestV1(identity, changedPrepared.RequiredGroups, changedPrepared.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	// The last committed PREPARE command still names the original ready digest.
	for _, tc := range []struct {
		name   string
		forged VectorPartitionLifecycleRecordV1
	}{
		{"building revision", impossibleBuilding},
		{"staged revision", impossibleStaged},
		{"prepared ready evidence", changedPrepared},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var forged CatalogMetaSnapshotV1
			if err := json.Unmarshal(valid, &forged); err != nil {
				t.Fatal(err)
			}
			var lifecycle vectorPartitionLifecycleSnapshotV1
			if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
				t.Fatal(err)
			}
			found := false
			for i := range lifecycle.Records {
				if lifecycle.Records[i].Identity == identity {
					lifecycle.Records[i] = tc.forged
					found = true
				}
			}
			if !found {
				t.Fatal("prepared candidate missing from snapshot")
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
				t.Fatalf("forged snapshot was not internally canonical: %v", err)
			}
			refusing := NewCatalogMetaAuthorityV1()
			if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
				t.Fatal(err)
			}
			if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
				t.Fatalf("impossible new %s restored: %v", tc.name, err)
			}
			if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
				t.Fatalf("refused snapshot mutated follower: %v", err)
			}
		})
	}
}
