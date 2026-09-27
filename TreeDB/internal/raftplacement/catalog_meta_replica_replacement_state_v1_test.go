package raftplacement

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestCatalogReplicaReplacementSeedPhaseSnapshotV1(t *testing.T) {
	authority, begin := replicaReplacementAuthorityForTestV1(t)
	raw, _ := EncodeReplicaReplacementBeginV1(begin)
	if _, err := authority.applyCommittedCatalogMetaV1(raw, 2); err != nil {
		t.Fatal(err)
	}
	seed := raftcluster.ReplacementSnapshotSeedV1{SourceNodeID: begin.OldNodeID, SnapshotID: "native-seed", Version: hraft.SnapshotVersionMax, Term: 4, Index: 9, ConfigurationIndex: 1, ConfigurationSHA256: strings.Repeat("c", 64), ArchiveSHA256: strings.Repeat("d", 64), SizeBytes: 1024, Manifest: raftcluster.SnapshotManifestV1{Format: raftcluster.SnapshotManifestFormatV1, Version: 1, NodeID: begin.OldNodeID, GroupID: begin.GroupID, LastIncludedTerm: 4, LastIncludedIndex: 9, AppliedCommandLSN: 3, LogicalDigestV1: strings.Repeat("e", 64), Scope: raftcluster.SnapshotScopeIdentityV1{ScopeRule: "single-group-v1", DatabaseScope: "database/default", CatalogScope: "catalog/default"}, CreatedAt: time.Unix(1700000000, 0).UTC()}}
	state := ReplicaReplacementStateV1{Begin: begin, Phase: ReplicaReplacementInstalledV1, Seed: &seed}
	skipped, _ := EncodeReplicaReplacementStateV1(state)
	if _, err := authority.applyCommittedCatalogMetaV1(skipped, 3); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("skip seed=%v", err)
	}
	restored := NewCatalogMetaAuthorityV1()
	for i, phase := range []ReplicaReplacementPhaseV1{ReplicaReplacementSeededV1, ReplicaReplacementInstalledV1, ReplicaReplacementAddIntentV1, ReplicaReplacementPromoteIntentV1, ReplicaReplacementPromotedV1} {
		state.Phase = phase
		if phase == ReplicaReplacementPromoteIntentV1 {
			digest := raftentry.CommandDigestV1{1}
			state.Tail = &raftcluster.ReplacementTailV1{GroupID: begin.GroupID, LeaderID: begin.OldNodeID, LeaderTerm: 4, CommitIndex: 12, ConfigurationIndex: 10, Progress: raftcluster.ReplacementTailProgressV1{EntryID: raftentry.ApplyEntryID{Term: 4, Index: 11}, CommandDigest: digest, ProgressDigest: raftentry.CommandDigestV1{2}, Result: raftentry.ApplyResultV1{CommandDigest: digest, ResultDigest: raftentry.CommandDigestV1{3}}}}
		}

		encoded, err := EncodeReplicaReplacementStateV1(state)
		if err != nil {
			t.Fatal(err)
		}
		status, err := authority.applyCommittedCatalogMetaV1(encoded, uint64(3+i))
		if err != nil {
			t.Fatal(err)
		}
		retry, err := authority.applyCommittedCatalogMetaV1(encoded, uint64(10+i))
		if err != nil || retry.AppliedIndex != status.AppliedIndex {
			t.Fatalf("retry=%+v %v", retry, err)
		}
		snapshot, err := authority.ExportCatalogMetaSnapshotBytesV1()
		if err != nil {
			t.Fatal(err)
		}
		if err := restored.installCatalogMetaSnapshotBytesV1(snapshot); err != nil {
			t.Fatal(err)
		}
		got, err := restored.ReplicaReplacementStateV1(begin.GroupID)
		if err != nil || got.Phase != phase || !raftcluster.SameReplacementSnapshotSeedV1(*got.Seed, seed) {
			t.Fatalf("restored=%+v %v", got, err)
		}
		if state.Tail != nil {
			if got.Tail == nil || *got.Tail != *state.Tail {
				t.Fatal("snapshot lost promotion boundary")
			}
			changed := *state.Tail
			changed.Progress.ProgressDigest[0] ^= 1
			conflict := state
			conflict.Tail = &changed
			raw, err := EncodeReplicaReplacementStateV1(conflict)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := authority.applyCommittedCatalogMetaV1(raw, uint64(30+i)); !errors.Is(err, ErrCatalogMetaConflict) {
				t.Fatalf("changed durable tail accepted: %v", err)
			}
		}
		again, err := restored.ExportCatalogMetaSnapshotBytesV1()
		if err != nil || !bytes.Equal(snapshot, again) {
			t.Fatalf("snapshot parity=%v", err)
		}
	}
	// A retried BEGIN reads the same current operation without resetting it.
	if _, err := authority.applyCommittedCatalogMetaV1(raw, 20); err != nil {
		t.Fatal(err)
	}
	seed.ArchiveSHA256 = strings.Repeat("f", 64)
	changed, _ := EncodeReplicaReplacementStateV1(state)
	if _, err := authority.applyCommittedCatalogMetaV1(changed, 21); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("changed seed=%v", err)
	}
	values, err := authority.ReplicaReplacementBeginsV1()
	if err != nil || len(values) != 1 || values[0].OperationID != begin.OperationID {
		t.Fatalf("bounded current op=%v %v", values, err)
	}
}
