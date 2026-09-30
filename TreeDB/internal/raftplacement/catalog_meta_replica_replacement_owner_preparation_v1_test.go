package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"strings"
	"testing"
	"time"
)

func TestCatalogOwnerPreparationIdentityAndPhaseCapV1(t *testing.T) {
	for _, name := range []string{"unmarked-owner", "changed-identity", "source", "malformed-identity"} {
		t.Run(name, func(t *testing.T) {
			a, begin, active := activeReplicaReplacementAuthorityForOwnerModeV1(t, true, true)
			begin.GroupID, begin.OldNodeID = "group-b", "node-b"
			identity := active.Identity
			begin.OwnerPreparation = &identity
			switch name {
			case "unmarked-owner":
				begin.OwnerPreparation = nil
			case "changed-identity":
				identity.Source.Generation++
			case "source":
				begin.GroupID, begin.OldNodeID = "group-a", "node-a"
			case "malformed-identity":
				identity.Index.IndexDefinitionDigest = ""
			}
			before, err := a.ExportCatalogMetaSnapshotBytesV1()
			if err != nil {
				t.Fatal(err)
			}
			raw, err := EncodeReplicaReplacementBeginV1(begin)
			if name == "malformed-identity" {
				if err == nil {
					t.Fatal("incomplete identity encoded")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = a.applyCommittedCatalogMetaV1(raw, a.applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
				t.Fatalf("uncapped owner BEGIN=%v", err)
			}
			after, err := a.ExportCatalogMetaSnapshotBytesV1()
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("refusal changed authority: %v", err)
			}
		})
	}
	a, begin, active := activeReplicaReplacementAuthorityForOwnerModeV1(t, true, true)
	begin.GroupID, begin.OldNodeID = "group-b", "node-b"
	identity := active.Identity
	begin.OwnerPreparation = &identity
	raw, err := EncodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.applyCommittedCatalogMetaV1(raw, a.applied+1); err != nil {
		t.Fatal(err)
	}
	begin, err = DecodeReplicaReplacementBeginV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	seed := raftcluster.ReplacementSnapshotSeedV1{SourceNodeID: begin.OldNodeID, SnapshotID: "native-owner-seed", Version: hraft.SnapshotVersionMax, Term: 4, Index: 9, ConfigurationIndex: 1, ConfigurationSHA256: strings.Repeat("c", 64), ArchiveSHA256: strings.Repeat("d", 64), SizeBytes: 1024, Manifest: raftcluster.SnapshotManifestV1{Format: raftcluster.SnapshotManifestFormatV1, Version: 1, NodeID: begin.OldNodeID, GroupID: begin.GroupID, LastIncludedTerm: 4, LastIncludedIndex: 9, AppliedCommandLSN: 3, LogicalDigestV1: strings.Repeat("e", 64), Scope: raftcluster.SnapshotScopeIdentityV1{ScopeRule: "single-group-v1", DatabaseScope: "database/default", CatalogScope: "catalog/default"}, CreatedAt: time.Unix(1700000000, 0).UTC()}}
	state := ReplicaReplacementStateV1{Begin: begin, Seed: &seed}
	for _, phase := range []ReplicaReplacementPhaseV1{ReplicaReplacementSeededV1, ReplicaReplacementInstalledV1, ReplicaReplacementAddIntentV1} {
		state.Phase = phase
		encoded, err := EncodeReplicaReplacementStateV1(state)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = a.applyCommittedCatalogMetaV1(encoded, a.applied+1); err != nil {
			t.Fatalf("%s: %v", phase, err)
		}
		snapshot, err := a.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		cold := NewCatalogMetaAuthorityV1()
		if err = cold.installCatalogMetaSnapshotBytesV1(snapshot); err != nil {
			t.Fatalf("cold %s: %v", phase, err)
		}
		got, err := cold.ReplicaReplacementStateV1(begin.GroupID)
		if err != nil || got.Phase != phase || got.Begin.OwnerPreparation == nil || *got.Begin.OwnerPreparation != identity {
			t.Fatalf("cold cap=%+v %v", got, err)
		}
		// Accessors decode private canonical bytes; pointer mutation cannot rewrite BEGIN.
		got.Begin.OwnerPreparation.Source.Generation++
		again, err := cold.ReplicaReplacementStateV1(begin.GroupID)
		if err != nil || *again.Begin.OwnerPreparation != identity {
			t.Fatal("accessor alias changed durable identity")
		}
	}
	if _, err = a.applyCommittedCatalogMetaV1(raw, a.applied+1); err != nil {
		t.Fatalf("exact BEGIN restart: %v", err)
	}
	if _, err := a.ReplacementCompletionV1(begin.GroupID, nil); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("completion bypass=%v", err)
	}
	before, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	var stripped CatalogMetaSnapshotV1
	if err := json.Unmarshal(before, &stripped); err != nil {
		t.Fatal(err)
	}
	stripped.VectorPartitionLifecycle = nil
	stripped.AppliedIndex++
	unmarked := state
	unmarked.Begin.OwnerPreparation = nil
	unmarkedRaw, err := EncodeReplicaReplacementStateV1(unmarked)
	if err != nil {
		t.Fatal(err)
	}
	stripped.ReplicaReplacements, err = encodeReplicaReplacementSnapshotV1(map[raftcluster.GroupID][]byte{begin.GroupID: unmarkedRaw})
	if err != nil {
		t.Fatal(err)
	}
	strippedRaw, err := json.Marshal(stripped)
	if err != nil {
		t.Fatal(err)
	}
	for _, receiver := range []*CatalogMetaAuthorityV1{NewCatalogMetaAuthorityV1(), a} {
		if err := receiver.installCatalogMetaSnapshotBytesV1(strippedRaw); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
			t.Fatalf("both marker and ACTIVE evidence erased: %v", err)
		}
	}
	// No pending replacement grants no capability; ordinary empty cold state stays valid.
	stripped.ReplicaReplacements = nil
	emptyRaw, err := json.Marshal(stripped)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewCatalogMetaAuthorityV1().installCatalogMetaSnapshotBytesV1(emptyRaw); err != nil {
		t.Fatalf("empty snapshot without pending replacement: %v", err)
	}
	digest := raftentry.CommandDigestV1{1}
	state.Tail = &raftcluster.ReplacementTailV1{GroupID: begin.GroupID, LeaderID: begin.OldNodeID, LeaderTerm: 4, CommitIndex: 12, ConfigurationIndex: 10, Progress: raftcluster.ReplacementTailProgressV1{EntryID: raftentry.ApplyEntryID{Term: 4, Index: 11}, CommandDigest: digest, ProgressDigest: raftentry.CommandDigestV1{2}, Result: raftentry.ApplyResultV1{CommandDigest: digest, ResultDigest: raftentry.CommandDigestV1{3}}}}
	for _, phase := range []ReplicaReplacementPhaseV1{ReplicaReplacementPromoteIntentV1, ReplicaReplacementPromotedV1, ReplicaReplacementRemoveIntentV1, ReplicaReplacementRemovedV1} {
		state.Phase = phase
		if replacementPhaseOrdinalV1(phase) >= 6 {
			state.RemovalIndex = 13
		}
		if phase == ReplicaReplacementRemovedV1 {
			state.Result = &ReplicaReplacementResultV1{ConfigurationIndex: 14}
		}
		encoded, err := EncodeReplicaReplacementStateV1(state)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = a.applyCommittedCatalogMetaV1(encoded, a.applied+1); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
			t.Fatalf("direct %s: %v", phase, err)
		}
		for _, erase := range []bool{false, true} {
			forgedState := state
			if erase {
				forgedState.Begin.OwnerPreparation = nil
			}
			forgedRaw, err := EncodeReplicaReplacementStateV1(forgedState)
			if err != nil {
				t.Fatal(err)
			}
			var snapshot CatalogMetaSnapshotV1
			if err = json.Unmarshal(before, &snapshot); err != nil {
				t.Fatal(err)
			}
			snapshot.AppliedIndex++
			snapshot.ReplicaReplacements, err = encodeReplicaReplacementSnapshotV1(map[raftcluster.GroupID][]byte{begin.GroupID: forgedRaw})
			if err != nil {
				t.Fatal(err)
			}
			forged, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			for _, receiver := range []*CatalogMetaAuthorityV1{NewCatalogMetaAuthorityV1(), a} {
				if err = receiver.installCatalogMetaSnapshotBytesV1(forged); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
					t.Fatalf("snapshot %s erase=%t: %v", phase, erase, err)
				}
			}
		}
	}
	after, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("phase refusals changed authority: %v", err)
	}
	state.Phase = ReplicaReplacementCompletedV1
	if err := validateReplicaReplacementOwnerPreparationPhaseV1(a.record, a.lifecycle, state); !errors.Is(err, ErrVectorPartitionLifecycleGuard) {
		t.Fatalf("marked completed history bypassed permanent cap: %v", err)
	}
}

func TestCatalogCompletedReplacementHistoryAllowsLaterOwnerBindingSnapshotV1(t *testing.T) {
	// Lifecycle support is already enabled at BEGIN. Enabling it over older
	// replacement evidence is independently forbidden and is not this flow.
	a, begin, _ := activeReplicaReplacementAuthorityV1(t, true)
	completeReplicaReplacementForTestV1(t, a, begin)
	history, err := a.ReplicaReplacementStateV1(begin.GroupID)
	if err != nil || history.Phase != ReplicaReplacementCompletedV1 || history.Begin.OwnerPreparation != nil {
		t.Fatalf("ordinary completed history: %+v err=%v", history, err)
	}
	identity := catalogMetaLifecycleTestIdentityV1(a.record, 7, 11)
	identity.Index.IndexName = "replacement-embedding"
	identity.Immutable = VectorPartitionLifecycleImmutableAuthorityV1{
		ManifestDigest: strings.Repeat("a", 64), PlacementDigest: strings.Repeat("b", 64),
	}
	applied := a.applied
	building := catalogMetaLifecycleApplyV1(t, a, &applied, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleBeginBuildV1, ExpectedState: VectorPartitionLifecycleAbsentV1,
		Identity: identity, RequiredGroups: []raftcluster.GroupID{begin.GroupID}, MutationEpoch: 9,
	})
	staged := catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(building, VectorPartitionLifecycleRecordGroupReadyV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.GroupReady = VectorPartitionLifecycleGroupReadyV1{GroupID: begin.GroupID, AppliedIndex: applied, AssetSetDigest: strings.Repeat("c", 64)}
	}))
	digest, err := VectorPartitionLifecycleReadySetDigestV1(identity, staged.RequiredGroups, staged.ReadyGroups)
	if err != nil {
		t.Fatal(err)
	}
	prepared := catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(staged, VectorPartitionLifecyclePrepareV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.ReadySetDigest = digest
	}))
	catalogMetaLifecycleApplyV1(t, a, &applied, catalogMetaLifecycleTestCommandV1(prepared, VectorPartitionLifecycleActivateV1, func(command *VectorPartitionLifecycleCommandV1) {
		command.MutationEpoch = 9
	}))
	snapshot, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	for _, receiver := range []*CatalogMetaAuthorityV1{NewCatalogMetaAuthorityV1(), a} {
		if err := receiver.installCatalogMetaSnapshotBytesV1(snapshot); err != nil {
			t.Fatalf("completed history with later ACTIVE owner binding: %v", err)
		}
		restored, err := receiver.ExportCatalogMetaSnapshotBytesV1()
		if err != nil || !bytes.Equal(snapshot, restored) {
			t.Fatalf("snapshot authority changed on restore: %v", err)
		}
	}
}
