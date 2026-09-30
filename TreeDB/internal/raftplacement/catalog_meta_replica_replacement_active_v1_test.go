package raftplacement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func activeReplicaReplacementAuthorityV1(t *testing.T, activate bool) (*CatalogMetaAuthorityV1, ReplicaReplacementBeginV1, VectorPartitionLifecycleRecordV1) {
	return activeReplicaReplacementAuthorityForOwnerModeV1(t, activate, false)
}

func activeReplicaReplacementAuthorityForOwnerModeV1(t *testing.T, activate, ownerOnly bool) (*CatalogMetaAuthorityV1, ReplicaReplacementBeginV1, VectorPartitionLifecycleRecordV1) {
	t.Helper()
	a, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	// group-b is the actual ANN owner; generic completion control replaces an
	// unrelated group rather than relying on absent TokenPartitions metadata.
	catalog.Catalog.Groups = append(catalog.Catalog.Groups, GroupV1{ID: "group-c", Members: []raftcluster.NodeID{"node-c"}})
	if ownerOnly {
		// Preparation is ANN-only: unrelated orders remain canonical on group-a.
		for i := range catalog.Catalog.Placements {
			if catalog.Catalog.Placements[i].GroupID == "group-b" {
				catalog.Catalog.Placements[i].GroupID = "group-a"
			}
		}
	}
	var err error
	catalog, err = NewCatalogMetaRecordV1(catalog.Epoch, catalog.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	a = NewCatalogMetaAuthorityV1()
	raw, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{Record: catalog})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(raw, 1); err != nil {
		t.Fatal(err)
	}
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
		ExpectedEpoch: catalog.Epoch, CatalogDigest: catalog.Digest, GroupID: "group-c",
		OldNodeID: "node-c", NewPeer: raftcluster.Peer{ID: "standby", Address: "127.0.0.1:19001"},
	}
}

func TestCatalogReplicaReplacementPendingFreezesLifecycleV1(t *testing.T) {
	a, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	beforeBegin, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	rawBegin, err := EncodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(rawBegin, a.applied+1); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	before, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	buildIdentity := catalogMetaLifecycleTestIdentityV1(a.record, active.Identity.Generation+1, 12)
	buildIdentity.Immutable = active.Identity.Immutable
	commands := []VectorPartitionLifecycleCommandV1{
		{
			Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
			Identity: buildIdentity, RequiredGroups: []raftcluster.GroupID{"group-b"},
			PreviousActiveGeneration: active.Identity.Generation, MutationEpoch: active.MutationEpoch + 1,
		},
		catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
			command.Reason = "relevant mutation"
			command.InvalidationEpoch = active.MutationEpoch + 1
		}),
	}
	for _, command := range commands {
		raw, err := EncodeVectorPartitionLifecycleCommandV1(command)
		if err != nil {
			t.Fatal(err)
		}
		baseline := NewCatalogMetaAuthorityV1()
		if err := baseline.installCatalogMetaSnapshotBytesV1(beforeBegin); err != nil {
			t.Fatal(err)
		}
		if _, err := baseline.applyCommittedVectorPartitionLifecycleV1(raw, baseline.applied+1); err != nil {
			t.Fatalf("%s baseline: %v", command.Kind, err)
		}
		if _, err := a.applyCommittedVectorPartitionLifecycleV1(raw, a.applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
			t.Fatalf("pending replacement accepted %s: %v", command.Kind, err)
		}
		after, err := a.ExportCatalogMetaSnapshotBytesV1()
		if err != nil || !bytes.Equal(after, before) {
			t.Fatalf("%s refusal changed authority: %v", command.Kind, err)
		}
	}
	activate := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleActivateV1, ExpectedRevision: active.Revision - 1,
		ExpectedState: VectorPartitionLifecyclePreparedV1, Identity: active.Identity,
		MutationEpoch: active.MutationEpoch, ReadySetDigest: active.ReadySetDigest,
	}
	rawActivate, err := EncodeVectorPartitionLifecycleCommandV1(activate)
	if err != nil {
		t.Fatal(err)
	}
	if sha256HexVectorPartitionLifecycleV1(rawActivate) != active.LastCommandDigest {
		t.Fatal("fixture ACTIVATE retry does not match committed digest")
	}
	if _, err := a.applyCommittedVectorPartitionLifecycleV1(rawActivate, a.applied+1); err != nil {
		t.Fatalf("exact lifecycle retry during pending replacement: %v", err)
	}
	after, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("exact retry changed authority: %v", err)
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
	validateServing := func(record VectorPartitionLifecycleRecordV1) {
		t.Helper()
		identity := record.Identity
		snapshot, err := a.VectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(
			context.Background(), a.applied, identity.Index.Collection, identity.Index.IndexName,
			identity.Generation, identity.Index.IndexDefinitionDigest, identity.Source.Generation,
			identity.Source.Checksum, identity.Source.SchemaHash, identity.Source.RowCount,
		)
		if err != nil {
			t.Fatalf("capture unchanged serving authority: %v", err)
		}
		if err := a.ValidateVectorPartitionServingAuthoritySnapshotAtAppliedIndexV1(context.Background(), a.applied, snapshot); err != nil {
			t.Fatalf("revalidate unchanged serving authority: %v", err)
		}
	}
	validateServing(before)
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
	validateServing(after)
	final, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	t.Run("one-step completion cannot introduce terminal history", func(t *testing.T) {
		var forged CatalogMetaSnapshotV1
		if err := json.Unmarshal(final, &forged); err != nil {
			t.Fatal(err)
		}
		records, _, _, fences, barriers, err := decodeVectorPartitionLifecycleSnapshotV1(forged.VectorPartitionLifecycle, complete.Catalog.Record)
		if err != nil {
			t.Fatal(err)
		}
		terminal := cloneVectorPartitionLifecycleRecordV1(records[reboundAbsent])
		terminal.Identity.Index.IndexName = "unseen-terminal"
		terminal.InvalidationEpoch = math.MaxUint64
		terminal.InvalidationReason = "forged-collection-mutation"
		terminal.MutationConfirmed = true
		records[terminal.Identity] = terminal
		fences[vectorPartitionLifecycleServingKeyV1{Collection: terminal.Identity.Index.Collection, IndexName: terminal.Identity.Index.IndexName}] =
			vectorPartitionLifecycleMutationFenceStateV1{Epoch: math.MaxUint64}
		forged.VectorPartitionLifecycle, err = encodeVectorPartitionLifecycleSnapshotV1(records, fences, barriers)
		if err != nil {
			t.Fatal(err)
		}
		forgedRaw, err := json.Marshal(forged)
		if err != nil {
			t.Fatal(err)
		}
		if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
			t.Fatalf("incoming terminal and fence are self-canonical: %v", err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(pending); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
			t.Fatalf("forward completion admitted unseen terminal and fence: %v", err)
		}
		if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, pending) {
			t.Fatalf("refusal mutated pending authority: %v", err)
		}
	})
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

func TestCatalogReplicaReplacementSnapshotRejectsUnwitnessedConfirmedFenceV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	beginRaw, err := EncodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.applyCommittedCatalogMetaV1(beginRaw, leader.applied+1); err != nil {
		t.Fatal(err)
	}
	pending, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	completeReplicaReplacementForTestV1(t, leader, begin)
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
	lifecycle.MutationFences = append(lifecycle.MutationFences, vectorPartitionLifecycleMutationFenceV1{
		Collection: active.Identity.Index.Collection, IndexName: active.Identity.Index.IndexName,
		Epoch: math.MaxUint64,
	})
	forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("fresh restore accepted unwitnessed confirmed fence: %v", err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(pending); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("forward restore accepted unwitnessed confirmed fence: %v", err)
	}
	if got, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(got, pending) {
		t.Fatalf("refusal changed follower state: %v", err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("genuine completion snapshot: %v", err)
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
				applied := a.applied
				invalidated := catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
					command.Reason = "relevant mutation"
					command.InvalidationEpoch = active.MutationEpoch + 1
				}))
				_ = catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(invalidated, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
					command.MutationEpoch = invalidated.InvalidationEpoch
				}))
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
				lifecycle.MutationFences[0].Epoch--
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
			freshErr := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw)
			if name == "mutation fence" {
				if !errors.Is(freshErr, ErrVectorPartitionLifecycleConflict) {
					t.Fatalf("fresh restore accepted unwitnessed fence: %v", freshErr)
				}
			} else if freshErr != nil {
				t.Fatalf("forged snapshot is not self-consistent: %v", freshErr)
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
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("fresh restore accepted ACTIVE with a later confirmed fence: %v", err)
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
		if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
			t.Fatalf("fresh restore accepted %s revival without confirmed-fence evidence: %v", record.State, err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
			t.Fatalf("snapshot revived %s generation: %v", record.State, err)
		}
		if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, currentRaw) {
			t.Fatalf("refused %s revival mutated follower: %v", record.State, err)
		}
	}
	final, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	lateFollower := NewCatalogMetaAuthorityV1()
	if err := lateFollower.installCatalogMetaSnapshotBytesV1(activeRaw); err != nil {
		t.Fatal(err)
	}
	if err := lateFollower.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("compacted confirmed fence and cleanup: %v", err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("fresh restore of confirmed fence and cleaned record: %v", err)
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

// A stateful ACTIVE follower only proves one replacement completion for a
// locally admitted BEGIN. Other compacted histories need independent durable
// provenance and remain fail closed; fresh restore still accepts a complete
// trusted Raft snapshot.
func TestCatalogReplicaReplacementActiveSnapshotCompactedRecoveryV1(t *testing.T) {
	t.Run("completion without locally admitted begin", func(t *testing.T) {
		leader, begin, _ := activeReplicaReplacementAuthorityV1(t, true)
		before, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		completeReplicaReplacementForTestV1(t, leader, begin)
		final, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(final); err != nil {
			t.Fatalf("fresh completed snapshot: %v", err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(final); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
			t.Fatalf("unanchored completion catch-up: %v", err)
		}
		if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
			t.Fatalf("refusal mutated follower: %v", err)
		}
	})
	t.Run("two completed epochs", func(t *testing.T) {
		leader, first, _ := activeReplicaReplacementAuthorityV1(t, true)
		firstRaw, err := EncodeReplicaReplacementBeginV1(first)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.applyCommittedCatalogMetaV1(firstRaw, leader.applied+1); err != nil {
			t.Fatal(err)
		}
		pending, err := leader.ExportCatalogMetaSnapshotBytesV1()
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
		if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(final); err != nil {
			t.Fatalf("fresh two-epoch snapshot: %v", err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(pending); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(final); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
			t.Fatalf("unanchored multi-epoch catch-up: %v", err)
		}
		if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, pending) {
			t.Fatalf("refusal mutated follower: %v", err)
		}
	})
	t.Run("post-completion lifecycle progress", func(t *testing.T) {
		leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
		firstRaw, err := EncodeReplicaReplacementBeginV1(begin)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.applyCommittedCatalogMetaV1(firstRaw, leader.applied+1); err != nil {
			t.Fatal(err)
		}
		pending, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		completeReplicaReplacementForTestV1(t, leader, begin)
		identity := active.Identity
		identity.Index.CatalogEpoch = leader.record.Epoch
		identity.Index.CatalogDigest = leader.record.Digest
		rebound, ok := leader.VectorPartitionLifecycleRecordV1(identity)
		if !ok {
			t.Fatal("completed authority lost ACTIVE record")
		}
		applied := leader.applied
		catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(rebound, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
			command.Reason = "later mutation"
			command.InvalidationEpoch = rebound.MutationEpoch + 1
		}))
		final, err := leader.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(pending); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(final); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
			t.Fatalf("unanchored post-rebind progress: %v", err)
		}
		if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, pending) {
			t.Fatalf("refusal mutated follower: %v", err)
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

func TestCatalogReplicaReplacementSnapshotAcceptsOrdinaryCompactedCutoverCleanupV1(t *testing.T) {
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
	fresh := NewCatalogMetaAuthorityV1()
	if err := fresh.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("fresh restore of compacted cleanup: %v", err)
	}
	if got, ok := fresh.VectorPartitionLifecycleRecordV1(newIdentity); !ok || !reflect.DeepEqual(got, candidate) || got.State != VectorPartitionLifecycleAbsentV1 {
		t.Fatalf("fresh restored successor=%+v available=%v want %+v", got, ok, candidate)
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
	found := false
	for i := range lifecycle.Records {
		if lifecycle.Records[i].Identity == newIdentity {
			lifecycle.Records[i].MutationEpoch--
			found = true
		}
	}
	if !found {
		t.Fatal("cleaned candidate missing")
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
		t.Fatalf("changed-source fixture is not canonical: %v", err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("changed source epoch restored after PREPARED: %v", err)
	}
	if retained, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("changed-source refusal mutated authority: %v", err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("restore ordinary compacted cleanup: %v", err)
	}
	if got, ok := follower.VectorPartitionLifecycleRecordV1(newIdentity); !ok || !reflect.DeepEqual(got, candidate) {
		t.Fatalf("restored successor=%+v available=%v want %+v", got, ok, candidate)
	}
}

func TestCatalogReplicaReplacementSnapshotAcceptsOrdinaryCleanedIntermediateCutoverV1(t *testing.T) {
	leader, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	applied := uint64(1)
	firstIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11)
	first := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, firstIdentity, 0, 9)
	first = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(first, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 9
	}))
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	secondIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 8, 12)
	second := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, secondIdentity, firstIdentity.Generation, 10)
	second = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(second, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.PreviousActiveGeneration = firstIdentity.Generation
		command.PreviousActiveRevision = first.Revision
		command.MutationEpoch = 10
	}))
	thirdIdentity := catalogMetaLifecycleTestIdentityV1(catalog, 9, 13)
	third := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, thirdIdentity, secondIdentity.Generation, 11)
	third = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(third, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.PreviousActiveGeneration = secondIdentity.Generation
		command.PreviousActiveRevision = second.Revision
		command.MutationEpoch = 11
	}))
	second, ok := leader.VectorPartitionLifecycleRecordV1(secondIdentity)
	if !ok || second.State != VectorPartitionLifecycleRetiredV1 {
		t.Fatalf("second cutover left candidate %+v, ok=%v", second, ok)
	}
	retiredSnapshot, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	var cleanableSnapshot []byte
	for _, kind := range []VectorPartitionLifecycleCommandKindV1{
		VectorPartitionLifecycleMarkCleanableV1, VectorPartitionLifecycleRecordGroupCleanupV1, VectorPartitionLifecycleCompleteCleanupV1,
	} {
		second = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(second, kind, func(command *VectorPartitionLifecycleCommandV1) {
			if kind == VectorPartitionLifecycleRecordGroupCleanupV1 {
				command.GroupID = "group-a"
			}
		}))
		if kind == VectorPartitionLifecycleRecordGroupCleanupV1 {
			cleanableSnapshot, err = leader.ExportCatalogMetaSnapshotBytesV1()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if second.State != VectorPartitionLifecycleAbsentV1 || second.SupersededByGeneration != thirdIdentity.Generation {
		t.Fatalf("second cleanup=%+v", second)
	}
	after, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	for name, snapshot := range map[string][]byte{
		"retired": retiredSnapshot, "cleanable": cleanableSnapshot, "absent": after,
	} {
		t.Run(name, func(t *testing.T) {
			fresh := NewCatalogMetaAuthorityV1()
			if err := fresh.installCatalogMetaSnapshotBytesV1(snapshot); err != nil {
				t.Fatalf("fresh restore of genuine intermediate cutover: %v", err)
			}
			if got, ok := fresh.VectorPartitionLifecycleRecordV1(thirdIdentity); !ok || !reflect.DeepEqual(got, third) {
				t.Fatalf("fresh restored third=%+v available=%v want %+v", got, ok, third)
			}
			follower := NewCatalogMetaAuthorityV1()
			if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
				t.Fatal(err)
			}
			if err := follower.installCatalogMetaSnapshotBytesV1(snapshot); err != nil {
				t.Fatalf("restore ordinary intermediate cutover: %v", err)
			}
			if got, ok := follower.VectorPartitionLifecycleRecordV1(thirdIdentity); !ok || !reflect.DeepEqual(got, third) {
				t.Fatalf("restored third=%+v available=%v want %+v", got, ok, third)
			}

		})
	}
}

func TestCatalogReplicaReplacementSnapshotRejectsSkippedCleanupRevisionsV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	completeReplicaReplacementForTestV1(t, leader, begin)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	completed, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(completed, &forged); err != nil {
		t.Fatal(err)
	}
	forged.AppliedIndex++
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
	completeReplicaReplacementForTestV1(t, leader, begin)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	completed, err := leader.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	completed.AppliedIndex++
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
	complete := completeReplicaReplacementForTestV1(t, leader, begin)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
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

func TestCatalogReplicaReplacementSnapshotRejectsNewActiveBeforeConfirmedFenceV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	complete := completeReplicaReplacementForTestV1(t, leader, begin)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	rebound := active.Identity
	rebound.Index.CatalogEpoch = complete.Catalog.Record.Epoch
	rebound.Index.CatalogDigest = complete.Catalog.Record.Digest
	old, ok := leader.VectorPartitionLifecycleRecordV1(rebound)
	if !ok {
		t.Fatal("rebound active record missing")
	}
	applied := leader.applied
	invalidated := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(old, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "relevant mutation"
		command.InvalidationEpoch = old.MutationEpoch + 1
	}))
	_ = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(invalidated, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = invalidated.InvalidationEpoch
	}))
	identity := catalogMetaLifecycleTestIdentityV1(complete.Catalog.Record, active.Identity.Generation+1, 12)
	candidate := catalogMetaLifecycleBuildPreparedV1(t, leader, &applied, identity, 0, invalidated.InvalidationEpoch)
	_ = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(candidate, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = candidate.MutationEpoch
	}))
	honest, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(honest); err != nil {
		t.Fatalf("genuine confirmed-fence successor: %v", err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(honest, &forged); err != nil {
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
		lifecycle.Records[i].MutationEpoch = invalidated.InvalidationEpoch - 1
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
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("fresh restore accepted new ACTIVE predating confirmed fence: %v", err)
	}
	refusing := NewCatalogMetaAuthorityV1()
	if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("new ACTIVE predating confirmed fence restored: %v", err)
	}
	if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused stale new ACTIVE mutated follower: %v", err)
	}
	// The incoming collection barrier is authoritative for a new ACTIVE
	// generation. A follower's older barrier must not admit stale source data.
	forged = CatalogMetaSnapshotV1{}
	if err := json.Unmarshal(honest, &forged); err != nil {
		t.Fatal(err)
	}
	lifecycle = vectorPartitionLifecycleSnapshotV1{}
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	lifecycle.CollectionMutationBarriers = []vectorPartitionCollectionMutationBarrierV1{{
		Collection: identity.Index.Collection, Epoch: candidate.MutationEpoch + 1,
		Pending: true, OperationDigest: strings.Repeat("f", 64),
	}}
	forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forgedRaw, err = json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("incoming-barrier snapshot was not internally canonical: %v", err)
	}
	refusing = NewCatalogMetaAuthorityV1()
	if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("new ACTIVE predating incoming collection barrier restored: %v", err)
	}
	if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused incoming-barrier snapshot mutated follower: %v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotAcceptsLegacyAdmittedBarrierConfirmationV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	raw, err := EncodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.applyCommittedCatalogMetaV1(raw, leader.applied+1); err != nil {
		t.Fatalf("admit replacement BEGIN: %v", err)
	}
	admitted, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	mutation := vectorPartitionCollectionMutationCommandV1{
		Kind:         vectorPartitionBeginCollectionMutationV1,
		Collection:   active.Identity.Index.Collection,
		CatalogEpoch: leader.record.Epoch, CatalogDigest: leader.record.Digest,
		ExpectedMutationEpoch: active.MutationEpoch, MutationEpoch: active.MutationEpoch + 1,
		OperationDigest: strings.Repeat("e", 64),
	}
	commitMutation := func() {
		t.Helper()
		raw, err := encodeVectorPartitionCollectionMutationCommandV1(mutation)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.applyCommittedVectorPartitionCollectionMutationV1(raw, leader.applied+1); err != nil {
			t.Fatalf("commit %s: %v", mutation.Kind, err)
		}
	}
	// Canonical state accepted by the old producer: BEGIN was committed after
	// replacement admission. Current admission refuses this sequence; no data
	// mutation or migration is asserted by this compatibility fixture. Retain
	// its owned CONFIRM and bounded snapshot catch-up behavior.
	leader.collectionMutationBarriers = map[CollectionRefV1]vectorPartitionCollectionMutationBarrierStateV1{
		mutation.Collection: {Epoch: mutation.MutationEpoch, Pending: true, OperationDigest: mutation.OperationDigest},
	}
	leader.applied++
	legacyRaw, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	leader = NewCatalogMetaAuthorityV1()
	if err := leader.installCatalogMetaSnapshotBytesV1(legacyRaw); err != nil {
		t.Fatal(err)
	}
	pending, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(pending); err != nil {
		t.Fatalf("install admitted BEGIN and pending mutation: %v", err)
	}
	commitMutation() // Exact retry of the legacy owned BEGIN must not create new debt.
	if retried, err := leader.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retried, pending) {
		t.Fatalf("legacy exact BEGIN retry changed authority: %v", err)
	}
	mutation.Kind = vectorPartitionConfirmCollectionMutationV1
	mutation.ExpectedMutationEpoch = mutation.MutationEpoch
	commitMutation()
	completeReplicaReplacementForTestV1(t, leader, begin)
	completed, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(completed); err != nil {
		t.Fatalf("completed snapshot is not internally canonical: %v", err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(completed); err != nil {
		t.Fatalf("already admitted BEGIN could not catch up after mutation confirmation: %v", err)
	}
	noBarrierFollower := NewCatalogMetaAuthorityV1()
	if err := noBarrierFollower.installCatalogMetaSnapshotBytesV1(admitted); err != nil {
		t.Fatal(err)
	}
	if err := noBarrierFollower.installCatalogMetaSnapshotBytesV1(completed); err != nil {
		t.Fatalf("admitted BEGIN could not catch up with a newly confirmed barrier: %v", err)
	}
	var admittedSnapshot, shortCompletion CatalogMetaSnapshotV1
	if err := json.Unmarshal(admitted, &admittedSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(completed, &shortCompletion); err != nil {
		t.Fatal(err)
	}
	shortCompletion.AppliedIndex = admittedSnapshot.AppliedIndex + 9 // Eight phases plus BEGIN and CONFIRM require ten entries.
	shortRaw, err := json.Marshal(shortCompletion)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(shortRaw); err != nil {
		t.Fatalf("short completion snapshot is not internally canonical: %v", err)
	}
	shortFollower := NewCatalogMetaAuthorityV1()
	if err := shortFollower.installCatalogMetaSnapshotBytesV1(admitted); err != nil {
		t.Fatal(err)
	}
	if err := shortFollower.installCatalogMetaSnapshotBytesV1(shortRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("completion reused a mutation command index: %v", err)
	}
	if got, err := shortFollower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(got, admitted) {
		t.Fatalf("short completion refusal mutated authority: %v", err)
	}
	if got, err := noBarrierFollower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(got, completed) {
		t.Fatalf("new-barrier catch-up differs from committed authority: %v", err)
	}
	if got, err := follower.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(got, completed) {
		t.Fatalf("catch-up snapshot differs from committed authority: %v", err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(completed, &forged); err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	if len(lifecycle.CollectionMutationBarriers) != 1 {
		t.Fatalf("completed snapshot has %d collection barriers", len(lifecycle.CollectionMutationBarriers))
	}
	lifecycle.CollectionMutationBarriers[0].Pending = true
	lifecycle.CollectionMutationBarriers[0].Completed = nil
	forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("forged missing-confirmation snapshot is not self-consistent: %v", err)
	}
	refusing := NewCatalogMetaAuthorityV1()
	if err := refusing.installCatalogMetaSnapshotBytesV1(pending); err != nil {
		t.Fatal(err)
	}
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("completion retained the same unconfirmed mutation: %v", err)
	}
	if got, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(got, pending) {
		t.Fatalf("refused missing-confirmation snapshot mutated authority: %v", err)
	}
	// A distinct mutation can begin after completion and legitimately remain
	// pending in the compacted snapshot seen by this same lagging follower.
	mutation.Kind = vectorPartitionBeginCollectionMutationV1
	mutation.CatalogEpoch, mutation.CatalogDigest = leader.record.Epoch, leader.record.Digest
	mutation.ExpectedMutationEpoch = mutation.MutationEpoch
	mutation.MutationEpoch++
	mutation.OperationDigest = strings.Repeat("d", 64)
	commitMutation()
	laterPending, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	catchup := NewCatalogMetaAuthorityV1()
	if err := catchup.installCatalogMetaSnapshotBytesV1(pending); err != nil {
		t.Fatal(err)
	}
	if err := catchup.installCatalogMetaSnapshotBytesV1(laterPending); err != nil {
		t.Fatalf("later distinct pending mutation could not catch up: %v", err)
	}
}

func TestCatalogReplicaReplacementSameEpochSnapshotRejectsUnwitnessedCollectionBarrierV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	mutation := vectorPartitionCollectionMutationCommandV1{
		Kind: vectorPartitionBeginCollectionMutationV1, Collection: active.Identity.Index.Collection,
		CatalogEpoch: leader.record.Epoch, CatalogDigest: leader.record.Digest,
		ExpectedMutationEpoch: active.MutationEpoch, MutationEpoch: active.MutationEpoch + 1,
		OperationDigest: strings.Repeat("e", 64),
	}
	for _, kind := range []vectorPartitionCollectionMutationCommandKindV1{
		vectorPartitionBeginCollectionMutationV1, vectorPartitionConfirmCollectionMutationV1,
	} {
		mutation.Kind = kind
		if kind == vectorPartitionConfirmCollectionMutationV1 {
			mutation.ExpectedMutationEpoch = mutation.MutationEpoch
		}
		raw, err := encodeVectorPartitionCollectionMutationCommandV1(mutation)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.applyCommittedVectorPartitionCollectionMutationV1(raw, leader.applied+1); err != nil {
			t.Fatalf("commit %s: %v", kind, err)
		}
	}
	completed, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(completed); err != nil {
		t.Fatalf("real new collection barrier could not catch up: %v", err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(completed, &forged); err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	if len(lifecycle.CollectionMutationBarriers) != 1 || len(lifecycle.CollectionMutationBarriers[0].Completed) != 1 {
		t.Fatalf("new barrier fixture=%+v", lifecycle.CollectionMutationBarriers)
	}
	lifecycle.CollectionMutationBarriers[0].Epoch = math.MaxUint64
	lifecycle.CollectionMutationBarriers[0].Completed[0].Epoch = math.MaxUint64
	forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("forged barrier snapshot is not internally canonical: %v", err)
	}
	refusing := NewCatalogMetaAuthorityV1()
	if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("incoming-only MaxUint64 barrier restored: %v", err)
	}
	if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused incoming-only barrier mutated authority: %v", err)
	}
}

func TestCatalogReplicaReplacementSameEpochSnapshotCountsBarrierFromEmptyAuthorityV1(t *testing.T) {
	leader, catalog := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	collection := catalogMetaLifecycleTestIdentityV1(catalog, 7, 11).Index.Collection
	mutation := vectorPartitionCollectionMutationCommandV1{
		Kind: vectorPartitionBeginCollectionMutationV1, Collection: collection,
		CatalogEpoch: catalog.Epoch, CatalogDigest: catalog.Digest,
		ExpectedMutationEpoch: 1, MutationEpoch: 2, OperationDigest: strings.Repeat("f", 64),
	}
	for _, kind := range []vectorPartitionCollectionMutationCommandKindV1{
		vectorPartitionBeginCollectionMutationV1, vectorPartitionConfirmCollectionMutationV1,
	} {
		mutation.Kind = kind
		if kind == vectorPartitionConfirmCollectionMutationV1 {
			mutation.ExpectedMutationEpoch = mutation.MutationEpoch
		}
		raw, err := encodeVectorPartitionCollectionMutationCommandV1(mutation)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.applyCommittedVectorPartitionCollectionMutationV1(raw, leader.applied+1); err != nil {
			t.Fatalf("commit %s: %v", kind, err)
		}
	}
	completed, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower, _ := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	if len(follower.lifecycle) != 0 || len(follower.mutationFences) != 0 || follower.lifecycle != nil || follower.mutationFences != nil {
		t.Fatalf("expected empty constructor maps, lifecycle=%v fences=%v", follower.lifecycle, follower.mutationFences)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(completed); err != nil {
		t.Fatalf("genuine barrier from empty authority: %v", err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(completed, &forged); err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	if len(lifecycle.CollectionMutationBarriers) != 1 || len(lifecycle.CollectionMutationBarriers[0].Completed) != 1 {
		t.Fatalf("completed barrier fixture=%+v", lifecycle.CollectionMutationBarriers)
	}
	lifecycle.CollectionMutationBarriers[0].Epoch = math.MaxUint64
	lifecycle.CollectionMutationBarriers[0].Completed[0].Epoch = math.MaxUint64
	forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("forged barrier snapshot is not internally canonical: %v", err)
	}
	refusing, _ := newCatalogMetaLifecycleTestAuthorityV1(t, true)
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("empty authority accepted forged MaxUint64 barrier: %v", err)
	}
	if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused barrier mutated empty authority: %v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotRejectsReceiptBeforeLocalEpochV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	mutation := vectorPartitionCollectionMutationCommandV1{
		Kind: vectorPartitionBeginCollectionMutationV1, Collection: active.Identity.Index.Collection,
		CatalogEpoch: leader.record.Epoch, CatalogDigest: leader.record.Digest,
		ExpectedMutationEpoch: active.MutationEpoch, MutationEpoch: active.MutationEpoch + 1,
		OperationDigest: strings.Repeat("e", 64),
	}
	for _, kind := range []vectorPartitionCollectionMutationCommandKindV1{
		vectorPartitionBeginCollectionMutationV1, vectorPartitionConfirmCollectionMutationV1,
	} {
		mutation.Kind = kind
		if kind == vectorPartitionConfirmCollectionMutationV1 {
			mutation.ExpectedMutationEpoch = mutation.MutationEpoch
		}
		raw, err := encodeVectorPartitionCollectionMutationCommandV1(mutation)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := leader.applyCommittedVectorPartitionCollectionMutationV1(raw, leader.applied+1); err != nil {
			t.Fatalf("commit %s: %v", kind, err)
		}
	}
	completed, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(completed); err != nil {
		t.Fatalf("real epoch %d mutation catch-up: %v", mutation.MutationEpoch, err)
	}
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(completed, &forged); err != nil {
		t.Fatal(err)
	}
	var lifecycle vectorPartitionLifecycleSnapshotV1
	if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
		t.Fatal(err)
	}
	if len(lifecycle.CollectionMutationBarriers) != 1 || len(lifecycle.CollectionMutationBarriers[0].Completed) != 1 {
		t.Fatalf("completed barrier fixture=%+v", lifecycle.CollectionMutationBarriers)
	}
	lifecycle.CollectionMutationBarriers[0].Completed = append([]vectorPartitionCollectionCompletedMutationV1{{
		Epoch: 2, OperationDigest: strings.Repeat("d", 64),
	}}, lifecycle.CollectionMutationBarriers[0].Completed...)
	forged.VectorPartitionLifecycle, err = json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	forged.AppliedIndex += 2 // Four entries could pay for two BEGIN/CONFIRM pairs, but epoch 2 is stale.
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("forged barrier snapshot is not internally canonical: %v", err)
	}
	refusing := NewCatalogMetaAuthorityV1()
	if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("receipt before local epoch restored: %v", err)
	}
	if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused stale receipt mutated authority: %v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotCountsRetainedCollectionBeginsV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 3; i++ {
		command := vectorPartitionCollectionMutationCommandV1{
			Kind: vectorPartitionBeginCollectionMutationV1, Collection: active.Identity.Index.Collection,
			CatalogEpoch: leader.record.Epoch, CatalogDigest: leader.record.Digest,
			ExpectedMutationEpoch: active.MutationEpoch + i, MutationEpoch: active.MutationEpoch + i + 1,
			OperationDigest: strings.Repeat(string(rune('a'+i)), 64),
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
				t.Fatalf("mutation %d %s: %v", i, kind, err)
			}
		}
	}
	completed, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(completed); err != nil {
		t.Fatalf("three committed mutations: %v", err)
	}

	// Three retained confirmations require three BEGINs as well.
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(completed, &forged); err != nil {
		t.Fatal(err)
	}
	var baseline CatalogMetaSnapshotV1
	if err := json.Unmarshal(before, &baseline); err != nil {
		t.Fatal(err)
	}
	forged.AppliedIndex = baseline.AppliedIndex + 5 // Three BEGINs and three CONFIRMs need six.
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("forged snapshot is not internally canonical: %v", err)
	}
	refusing := NewCatalogMetaAuthorityV1()
	if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("retained mutation BEGINs exceeded applied gap: %v", err)
	}
	if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
		t.Fatalf("refused short-gap snapshot mutated authority: %v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotPreservesMixedCompactedMutationHistoryV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < maxVectorPartitionCollectionCompletedMutationsV1+1; i++ {
		command := vectorPartitionCollectionMutationCommandV1{
			Kind: vectorPartitionBeginCollectionMutationV1, Collection: active.Identity.Index.Collection,
			CatalogEpoch: leader.record.Epoch, CatalogDigest: leader.record.Digest,
			ExpectedMutationEpoch: active.MutationEpoch + i, MutationEpoch: active.MutationEpoch + i + 1,
			OperationDigest: fmt.Sprintf("%064x", i+1),
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
				t.Fatalf("mutation %d %s: %v", i, kind, err)
			}
		}
	}
	applied := leader.applied
	invalidated := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "late invalidation"
		command.InvalidationEpoch = active.MutationEpoch + maxVectorPartitionCollectionCompletedMutationsV1
	}))
	_ = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(invalidated, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = invalidated.InvalidationEpoch
	}))
	final, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	follower := NewCatalogMetaAuthorityV1()
	if err := follower.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := follower.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("late lifecycle watermark after compacted mutation history: %v", err)
	}
}

func TestCatalogReplicaReplacementSnapshotValidatesNewPreparationV1(t *testing.T) {
	leader, begin, active := activeReplicaReplacementAuthorityV1(t, true)
	completeReplicaReplacementForTestV1(t, leader, begin)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
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
	t.Run("known staged candidate retains committed READY on prepare", func(t *testing.T) {
		follower := NewCatalogMetaAuthorityV1()
		if err := follower.installCatalogMetaSnapshotBytesV1(stagedRaw); err != nil {
			t.Fatal(err)
		}
		if err := follower.installCatalogMetaSnapshotBytesV1(valid); err != nil {
			t.Fatalf("committed STAGED to PREPARED: %v", err)
		}
		changedReady := catalogMetaLifecycleTestCommandV1(building, VectorPartitionLifecycleRecordGroupReadyV1, func(command *VectorPartitionLifecycleCommandV1) {
			command.GroupReady = VectorPartitionLifecycleGroupReadyV1{GroupID: "group-b", AppliedIndex: staged.ReadyGroups[0].AppliedIndex, AssetSetDigest: strings.Repeat("d", 64)}
		})
		changedStaged, err := ApplyVectorPartitionLifecycleCommandV1(building, changedReady)
		if err != nil {
			t.Fatal(err)
		}
		changedDigest, err := VectorPartitionLifecycleReadySetDigestV1(identity, changedStaged.RequiredGroups, changedStaged.ReadyGroups)
		if err != nil {
			t.Fatal(err)
		}
		changedPrepared, err := ApplyVectorPartitionLifecycleCommandV1(changedStaged,
			catalogMetaLifecycleTestCommandV1(changedStaged, VectorPartitionLifecyclePrepareV1, func(command *VectorPartitionLifecycleCommandV1) {
				command.ReadySetDigest = changedDigest
			}))
		if err != nil {
			t.Fatal(err)
		}
		var forged CatalogMetaSnapshotV1
		if err := json.Unmarshal(valid, &forged); err != nil {
			t.Fatal(err)
		}
		var lifecycle vectorPartitionLifecycleSnapshotV1
		if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
			t.Fatal(err)
		}
		for i := range lifecycle.Records {
			if lifecycle.Records[i].Identity == identity {
				lifecycle.Records[i] = changedPrepared
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
			t.Fatalf("forged PREPARED is self-canonical: %v", err)
		}
		refusing := NewCatalogMetaAuthorityV1()
		if err := refusing.installCatalogMetaSnapshotBytesV1(stagedRaw); err != nil {
			t.Fatal(err)
		}
		if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
			t.Fatalf("known STAGED READY was rewritten: %v", err)
		}
		if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, stagedRaw) {
			t.Fatalf("refusal mutated committed STAGED: %v", err)
		}
	})
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

func TestCatalogReplicaReplacementSameEpochSnapshotRejectsUnknownTerminalFenceV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	applied := leader.applied
	terminal := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(active, VectorPartitionLifecycleInvalidateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "relevant mutation"
		command.InvalidationEpoch = math.MaxUint64
	}))
	invalidated := cloneVectorPartitionLifecycleRecordV1(terminal)
	terminal = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(terminal, VectorPartitionLifecycleConfirmMutationV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = math.MaxUint64
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
		terminal = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(terminal, step.kind, func(command *VectorPartitionLifecycleCommandV1) {
			command.GroupID = step.group
		}))
	}
	if terminal.State != VectorPartitionLifecycleAbsentV1 || !terminal.MutationConfirmed || terminal.InvalidationEpoch != math.MaxUint64 {
		t.Fatalf("reducer did not retain the confirmed terminal witness: %+v", terminal)
	}
	final, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	known := NewCatalogMetaAuthorityV1()
	if err := known.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := known.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("genuine compacted MaxUint64 invalidation: %v", err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("fresh restore of genuine terminal witness: %v", err)
	}
	firstSeen := NewCatalogMetaAuthorityV1()
	firstSeenCatalog, err := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{Record: leader.record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstSeen.applyCommittedCatalogMetaV1(firstSeenCatalog, 1); err != nil {
		t.Fatal(err)
	}
	catalog := firstSeen.record
	if !reflect.DeepEqual(catalog, leader.record) {
		t.Fatal("first-seen fixture uses a different catalog")
	}
	firstSeenBefore, err := firstSeen.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	if err := firstSeen.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("ordinary first-seen compacted terminal history: %v", err)
	}
	if got, ok := firstSeen.VectorPartitionLifecycleRecordV1(active.Identity); !ok || got.State != VectorPartitionLifecycleAbsentV1 {
		t.Fatalf("first-seen terminal=%+v available=%v", got, ok)
	}
	if bytes.Equal(firstSeenBefore, final) {
		t.Fatal("terminal fixture did not advance")
	}
	knownActive, _, serving := activeReplicaReplacementAuthorityV1(t, true)
	knownBefore, err := knownActive.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	var sameKey CatalogMetaSnapshotV1
	if err := json.Unmarshal(knownBefore, &sameKey); err != nil {
		t.Fatal(err)
	}
	knownRecords, _, _, knownFences, knownBarriers, err := decodeVectorPartitionLifecycleSnapshotV1(sameKey.VectorPartitionLifecycle, knownActive.record)
	if err != nil {
		t.Fatal(err)
	}
	unrelated := cloneVectorPartitionLifecycleRecordV1(terminal)
	unrelated.Identity = serving.Identity
	unrelated.Identity.Generation++
	unrelated.MutationEpoch = serving.MutationEpoch - 1
	unrelated.InvalidationEpoch = serving.MutationEpoch
	knownRecords[unrelated.Identity] = unrelated
	knownFences[vectorPartitionLifecycleServingKeyV1{Collection: serving.Identity.Index.Collection, IndexName: serving.Identity.Index.IndexName}] =
		vectorPartitionLifecycleMutationFenceStateV1{Epoch: serving.MutationEpoch}
	sameKey.VectorPartitionLifecycle, err = encodeVectorPartitionLifecycleSnapshotV1(knownRecords, knownFences, knownBarriers)
	if err != nil {
		t.Fatal(err)
	}
	sameKey.AppliedIndex++
	sameKeyRaw, err := json.Marshal(sameKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(sameKeyRaw); err != nil {
		t.Fatalf("same-key terminal fixture is not canonical: %v", err)
	}
	if err := knownActive.installCatalogMetaSnapshotBytesV1(sameKeyRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("unrelated terminal witnessed known ACTIVE fence: %v", err)
	}
	if retained, err := knownActive.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, knownBefore) {
		t.Fatalf("same-key refusal changed local authority: %v", err)
	}
	pending, begin, _ := activeReplicaReplacementAuthorityV1(t, true)
	rawBegin, err := EncodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.applyCommittedCatalogMetaV1(rawBegin, pending.applied+1); err != nil {
		t.Fatalf("replacement BEGIN: %v", err)
	}
	replacementBefore, err := pending.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}

	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(replacementBefore, &forged); err != nil {
		t.Fatal(err)
	}
	records, _, _, fences, barriers, err := decodeVectorPartitionLifecycleSnapshotV1(forged.VectorPartitionLifecycle, pending.record)
	if err != nil {
		t.Fatal(err)
	}
	unknown := cloneVectorPartitionLifecycleRecordV1(terminal)
	unknown.Identity.Index.IndexName = "unknown-terminal"
	records[unknown.Identity] = unknown
	fences[vectorPartitionLifecycleServingKeyV1{Collection: unknown.Identity.Index.Collection, IndexName: unknown.Identity.Index.IndexName}] =
		vectorPartitionLifecycleMutationFenceStateV1{Epoch: math.MaxUint64}
	forged.VectorPartitionLifecycle, err = encodeVectorPartitionLifecycleSnapshotV1(records, fences, barriers)
	if err != nil {
		t.Fatal(err)
	}
	forged.AppliedIndex++
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedRaw); err != nil {
		t.Fatalf("forged terminal and fence are self-canonical: %v", err)
	}
	refusing := NewCatalogMetaAuthorityV1()
	if err := refusing.installCatalogMetaSnapshotBytesV1(replacementBefore); err != nil {
		t.Fatal(err)
	}
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("unknown same-epoch terminal and confirmed fence restored: %v", err)
	}
	if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, replacementBefore) {
		t.Fatalf("refusal changed local authority: %v", err)
	}
	var forgedPending CatalogMetaSnapshotV1
	if err := json.Unmarshal(replacementBefore, &forgedPending); err != nil {
		t.Fatal(err)
	}
	records, _, _, fences, barriers, err = decodeVectorPartitionLifecycleSnapshotV1(forgedPending.VectorPartitionLifecycle, pending.record)
	if err != nil {
		t.Fatal(err)
	}
	invalidated.Identity.Index.IndexName = "unknown-invalidated"
	invalidated.ReadySetDigest, err = VectorPartitionLifecycleReadySetDigestV1(invalidated.Identity, invalidated.RequiredGroups, invalidated.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	records[invalidated.Identity] = invalidated
	fences[vectorPartitionLifecycleServingKeyV1{Collection: invalidated.Identity.Index.Collection, IndexName: invalidated.Identity.Index.IndexName}] =
		vectorPartitionLifecycleMutationFenceStateV1{Epoch: math.MaxUint64, Pending: true}
	forgedPending.VectorPartitionLifecycle, err = encodeVectorPartitionLifecycleSnapshotV1(records, fences, barriers)
	if err != nil {
		t.Fatal(err)
	}
	forgedPending.AppliedIndex++
	forgedPendingRaw, err := json.Marshal(forgedPending)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(forgedPendingRaw); err != nil {
		t.Fatalf("unknown INVALIDATED fixture is not canonical: %v", err)
	}
	refusing = NewCatalogMetaAuthorityV1()
	if err := refusing.installCatalogMetaSnapshotBytesV1(replacementBefore); err != nil {
		t.Fatal(err)
	}
	if err := refusing.installCatalogMetaSnapshotBytesV1(forgedPendingRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("unknown same-epoch INVALIDATED and pending fence restored: %v", err)
	}
	if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, replacementBefore) {
		t.Fatalf("INVALIDATED refusal changed local authority: %v", err)
	}
}

func TestCatalogReplicaReplacementSameEpochSnapshotPreservesAbortedPreparationV1(t *testing.T) {
	leader, _, active := activeReplicaReplacementAuthorityV1(t, true)
	identity := catalogMetaLifecycleTestIdentityV1(leader.record, active.Identity.Generation+1, 12)
	identity.Immutable = active.Identity.Immutable
	applied := leader.applied
	building := catalogMetaLifecycleApplyV1(t, leader, &applied, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{"group-b"},
		PreviousActiveGeneration: active.Identity.Generation, MutationEpoch: active.MutationEpoch + 1,
	})
	staged := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(building, VectorPartitionLifecycleRecordGroupReadyV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.GroupReady = VectorPartitionLifecycleGroupReadyV1{GroupID: "group-b", AppliedIndex: applied, AssetSetDigest: strings.Repeat("c", 64)}
	}))
	before, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	retired := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(staged, VectorPartitionLifecycleAbortBuildV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.Reason = "cancelled build"
	}))
	final, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	known := NewCatalogMetaAuthorityV1()
	if err := known.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := known.installCatalogMetaSnapshotBytesV1(final); err != nil {
		t.Fatalf("committed STAGED to aborted RETIRED: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*VectorPartitionLifecycleRecordV1)
	}{
		{"READY asset", func(record *VectorPartitionLifecycleRecordV1) {
			record.ReadyGroups[0].AssetSetDigest = strings.Repeat("d", 64)
		}},
		{"source epoch", func(record *VectorPartitionLifecycleRecordV1) { record.MutationEpoch++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var forged CatalogMetaSnapshotV1
			if err := json.Unmarshal(final, &forged); err != nil {
				t.Fatal(err)
			}
			var lifecycle vectorPartitionLifecycleSnapshotV1
			if err := json.Unmarshal(forged.VectorPartitionLifecycle, &lifecycle); err != nil {
				t.Fatal(err)
			}
			found := false
			for i := range lifecycle.Records {
				if lifecycle.Records[i].Identity == identity {
					lifecycle.Records[i] = cloneVectorPartitionLifecycleRecordV1(retired)
					tc.mutate(&lifecycle.Records[i])
					found = true
				}
			}
			if !found {
				t.Fatal("aborted candidate missing")
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
				t.Fatalf("forged terminal is self-canonical: %v", err)
			}
			refusing := NewCatalogMetaAuthorityV1()
			if err := refusing.installCatalogMetaSnapshotBytesV1(before); err != nil {
				t.Fatal(err)
			}
			if err := refusing.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrVectorPartitionLifecycleConflict) {
				t.Fatalf("aborted candidate changed committed %s: %v", tc.name, err)
			}
			if retained, err := refusing.ExportCatalogMetaSnapshotBytesV1(); err != nil || !bytes.Equal(retained, before) {
				t.Fatalf("refusal changed committed STAGED candidate: %v", err)
			}
		})
	}
	cleanable := catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(retired, VectorPartitionLifecycleMarkCleanableV1, nil))
	cleanable = catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(cleanable, VectorPartitionLifecycleRecordGroupCleanupV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.GroupID = "group-b"
	}))
	catalogMetaLifecycleApplyV1(t, leader, &applied, catalogMetaLifecycleTestCommandV1(cleanable, VectorPartitionLifecycleCompleteCleanupV1, nil))
	cleaned, err := leader.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(cleaned); err != nil {
		t.Fatalf("fresh restore of cleaned candidate: %v", err)
	}
	lagging := NewCatalogMetaAuthorityV1()
	if err := lagging.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	if err := lagging.installCatalogMetaSnapshotBytesV1(cleaned); err != nil {
		t.Fatalf("restore ordinary compacted preparation cleanup: %v", err)
	}
}
