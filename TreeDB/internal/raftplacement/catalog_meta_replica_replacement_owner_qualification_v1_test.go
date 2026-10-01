package raftplacement

import (
	"bytes"
	"errors"
	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"strings"
	"testing"
	"time"
)

func TestCatalogOwnerQualificationReceiptIsHistoricalAndAtomicV1(t *testing.T) {
	authority, begin, active := activeReplicaReplacementAuthorityForOwnerModeV1(t, true, true)
	begin.GroupID, begin.OldNodeID = "group-b", "node-b"
	identity := active.Identity
	begin.OwnerPreparation = &identity
	beginRaw, err := EncodeReplicaReplacementBeginV1(begin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.applyCommittedCatalogMetaV1(beginRaw, authority.applied+1); err != nil {
		t.Fatal(err)
	}
	begin, err = DecodeReplicaReplacementBeginV1(beginRaw)
	if err != nil {
		t.Fatal(err)
	}
	seed := raftcluster.ReplacementSnapshotSeedV1{SourceNodeID: begin.OldNodeID, SnapshotID: "native-seed", Version: hraft.SnapshotVersionMax, Term: 4, Index: 9, ConfigurationIndex: 1, ConfigurationSHA256: strings.Repeat("c", 64), ArchiveSHA256: strings.Repeat("d", 64), SizeBytes: 1024, Manifest: raftcluster.SnapshotManifestV1{Format: raftcluster.SnapshotManifestFormatV1, Version: 1, NodeID: begin.OldNodeID, GroupID: begin.GroupID, LastIncludedTerm: 4, LastIncludedIndex: 9, AppliedCommandLSN: 3, LogicalDigestV1: strings.Repeat("e", 64), Scope: raftcluster.SnapshotScopeIdentityV1{ScopeRule: "single-group-v1", DatabaseScope: "database/default", CatalogScope: "catalog/default"}, CreatedAt: time.Unix(1700000000, 0).UTC()}}
	state := ReplicaReplacementStateV1{Begin: begin, Seed: &seed}
	for _, phase := range []ReplicaReplacementPhaseV1{ReplicaReplacementSeededV1, ReplicaReplacementInstalledV1, ReplicaReplacementAddIntentV1} {
		state.Phase = phase
		raw, err := EncodeReplicaReplacementStateV1(state)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := authority.applyCommittedCatalogMetaV1(raw, authority.applied+1); err != nil {
			t.Fatal(err)
		}
	}
	state, err = authority.ReplicaReplacementStateV1(begin.GroupID)
	if err != nil {
		t.Fatal(err)
	}
	noReceiptRaw, err := EncodeReplicaReplacementStateV1(state)
	if err != nil {
		t.Fatal(err)
	}
	digest := raftentry.CommandDigestV1{1}
	tail := raftcluster.ReplacementTailV1{GroupID: begin.GroupID, LeaderID: begin.OldNodeID, LeaderTerm: 4, CommitIndex: 12, ConfigurationIndex: 10, Progress: raftcluster.ReplacementTailProgressV1{EntryID: raftentry.ApplyEntryID{Term: 4, Index: 11}, CommandDigest: digest, ProgressDigest: raftentry.CommandDigestV1{2}, Result: raftentry.ApplyResultV1{CommandDigest: digest, ResultDigest: raftentry.CommandDigestV1{3}}}}
	state.OwnerQualification = &ReplicaReplacementOwnerQualificationReceiptV1{QueryDigest: strings.Repeat("a", 64), ResultDigest: strings.Repeat("b", 64), ReadySetDigest: active.ReadySetDigest, IssuerNode: begin.OldNodeID, ReadTerm: 4, ReadIndex: 12, TargetAppliedIndex: 12, Tail: tail}
	for _, name := range []string{"bad-digest", "zero-read", "behind-target", "empty-tail", "unmarked"} {
		t.Run(name, func(t *testing.T) {
			malformed := state
			receipt := *state.OwnerQualification
			malformed.OwnerQualification = &receipt
			switch name {
			case "bad-digest":
				receipt.ResultDigest = "not-a-digest"
			case "zero-read":
				receipt.ReadIndex = 0
			case "behind-target":
				receipt.TargetAppliedIndex = receipt.ReadIndex - 1
			case "empty-tail":
				receipt.Tail = raftcluster.ReplacementTailV1{}
			case "unmarked":
				malformed.Begin.OwnerPreparation = nil
			}
			if _, err := EncodeReplicaReplacementOwnerQualificationCommandV1(ReplicaReplacementOwnerQualificationCommandV1{State: malformed}); err == nil {
				t.Fatal("malformed receipt encoded")
			}
		})
	}
	raw, err := EncodeReplicaReplacementOwnerQualificationCommandV1(ReplicaReplacementOwnerQualificationCommandV1{State: state})
	if err != nil {
		t.Fatal(err)
	}
	generic, err := EncodeReplicaReplacementStateV1(state)
	if err != nil {
		t.Fatal(err)
	}
	before, err := authority.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.applyCommittedCatalogMetaV1(generic, authority.applied+1); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("generic receipt injection=%v", err)
	}
	after, err := authority.ExportCatalogMetaSnapshotBytesV1()
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("generic refusal changed bytes")
	}
	// Exercise entry-budget enforcement through the complete known-authority
	// snapshot install, using an independently restored pre-receipt authority.
	known := NewCatalogMetaAuthorityV1()
	if err := known.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	incoming, err := known.ExportCatalogMetaSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	incoming.ReplicaReplacements, err = encodeReplicaReplacementSnapshotV1(map[raftcluster.GroupID][]byte{begin.GroupID: generic})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := known.installCatalogMetaSnapshotV1(incoming); err == nil {
		t.Fatal("first receipt snapshot consumed no catalog entry")
	}
	refusedSnapshot, err := known.ExportCatalogMetaSnapshotBytesV1()
	if err != nil || !bytes.Equal(before, refusedSnapshot) {
		t.Fatal("entry-budget refusal changed known authority")
	}
	admitted := NewCatalogMetaAuthorityV1()
	if err := admitted.installCatalogMetaSnapshotBytesV1(before); err != nil {
		t.Fatal(err)
	}
	incoming.AppliedIndex++
	if _, err := admitted.installCatalogMetaSnapshotV1(incoming); err != nil {
		t.Fatalf("one-entry receipt snapshot refused: %v", err)
	}
	installed, err := admitted.ReplicaReplacementStateV1(begin.GroupID)
	if err != nil || installed.OwnerQualification == nil || *installed.OwnerQualification != *state.OwnerQualification {
		t.Fatalf("one-entry receipt not installed: %+v %v", installed, err)
	}
	status, err := authority.applyCommittedCatalogMetaV1(raw, authority.applied+1)
	if err != nil {
		t.Fatal(err)
	}
	owned, err := authority.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	retry, err := authority.applyCommittedCatalogMetaV1(raw, authority.applied+10)
	if err != nil || retry.AppliedIndex != status.AppliedIndex {
		t.Fatalf("historical retry=%+v %v", retry, err)
	}
	cold := NewCatalogMetaAuthorityV1()
	if err := cold.installCatalogMetaSnapshotBytesV1(owned); err != nil {
		t.Fatal(err)
	}
	restored, err := cold.ReplicaReplacementStateV1(begin.GroupID)
	if err != nil || restored.Phase != ReplicaReplacementAddIntentV1 || restored.OwnerQualification == nil || *restored.OwnerQualification != *state.OwnerQualification {
		t.Fatalf("cold receipt=%+v %v", restored, err)
	}
	// Accessor bytes are owned, including the optional receipt pointer.
	restored.OwnerQualification.QueryDigest = strings.Repeat("f", 64)
	again, err := cold.ReplicaReplacementStateV1(begin.GroupID)
	if err != nil || *again.OwnerQualification != *state.OwnerQualification {
		t.Fatal("accessor aliases receipt")
	}
	if _, err := DecodeCatalogMetaCommandV1(raw); err == nil {
		t.Fatal("external catalog-publish codec accepted receipt command")
	}
	for _, name := range []string{"changed-query", "changed-result", "changed-ready", "changed-issuer", "erased", "phase-cap"} {
		t.Run(name, func(t *testing.T) {
			next := state
			receipt := *state.OwnerQualification
			next.OwnerQualification = &receipt
			switch name {
			case "changed-query":
				receipt.QueryDigest = strings.Repeat("f", 64)
			case "changed-result":
				receipt.ResultDigest = strings.Repeat("f", 64)
			case "changed-ready":
				receipt.ReadySetDigest = strings.Repeat("f", 64)
			case "changed-issuer":
				receipt.IssuerNode = "unknown-node"
			case "erased":
				next.OwnerQualification = nil
			case "phase-cap":
				next.Phase, next.Tail = ReplicaReplacementPromoteIntentV1, &tail
			}
			encoded, err := EncodeReplicaReplacementStateV1(next)
			if err != nil {
				t.Fatal(err)
			}
			if name != "erased" && name != "phase-cap" {
				dedicated, err := EncodeReplicaReplacementOwnerQualificationCommandV1(ReplicaReplacementOwnerQualificationCommandV1{State: next})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := authority.applyCommittedCatalogMetaV1(dedicated, authority.applied+1); err == nil {
					t.Fatal("dedicated command rewrote historical receipt")
				}
			}
			if _, err := authority.applyCommittedCatalogMetaV1(encoded, authority.applied+1); err == nil {
				t.Fatal("generic alteration admitted")
			}
			snapshot, err := authority.ExportCatalogMetaSnapshotV1()
			if err != nil {
				t.Fatal(err)
			}
			snapshot.AppliedIndex++
			snapshot.ReplicaReplacements, err = encodeReplicaReplacementSnapshotV1(map[raftcluster.GroupID][]byte{begin.GroupID: encoded})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := authority.installCatalogMetaSnapshotV1(snapshot); err == nil {
				t.Fatal("known receipt alteration admitted")
			}
			current, err := authority.ExportCatalogMetaSnapshotBytesV1()
			if err != nil || !bytes.Equal(owned, current) {
				t.Fatal("refusal changed authority bytes")
			}
			if name == "changed-ready" || name == "changed-issuer" || name == "phase-cap" {
				incoming := NewCatalogMetaAuthorityV1()
				if _, err := incoming.installCatalogMetaSnapshotV1(snapshot); err == nil {
					t.Fatal("cold invalid receipt authority admitted")
				}
			}
		})
	}
	for _, budget := range []uint64{0, 1} {
		cost, err := replicaReplacementSnapshotEntryCostV1(map[raftcluster.GroupID][]byte{begin.GroupID: noReceiptRaw}, map[raftcluster.GroupID][]byte{begin.GroupID: generic}, budget)
		if budget == 0 && err == nil || budget == 1 && (err != nil || cost != 1) {
			t.Fatalf("receipt entry cost budget=%d cost=%d err=%v", budget, cost, err)
		}
	}
}
