package raftplacement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func completeReplicaReplacementForTestV1(t *testing.T, a *CatalogMetaAuthorityV1, begin ReplicaReplacementBeginV1) ReplicaReplacementCompleteV1 {
	t.Helper()
	raw, _ := EncodeReplicaReplacementBeginV1(begin)
	if _, err := a.applyCommittedCatalogMetaV1(raw, a.applied+1); err != nil {
		t.Fatal(err)
	}
	state, err := a.ReplicaReplacementStateV1(begin.GroupID)
	if err != nil {
		t.Fatal(err)
	}
	peers := slices.Clone(state.Peers)
	if len(peers) == 0 {
		for _, g := range a.record.Catalog.Groups {
			if g.ID == begin.GroupID {
				for i, id := range g.Members {
					peers = append(peers, raftcluster.Peer{ID: id, Address: fmt.Sprintf("127.0.0.1:%d", 20000+i)})
				}
			}
		}
	}
	for i, p := range peers {
		if p.ID == begin.OldNodeID {
			peers[i] = begin.NewPeer
		}
	}
	slices.SortFunc(peers, func(a, b raftcluster.Peer) int { return strings.Compare(string(a.ID), string(b.ID)) })
	seed := raftcluster.ReplacementSnapshotSeedV1{SourceNodeID: begin.OldNodeID, SnapshotID: "native-seed", Version: hraft.SnapshotVersionMax, Term: 4, Index: 9, ConfigurationIndex: 1, ConfigurationSHA256: strings.Repeat("c", 64), ArchiveSHA256: strings.Repeat("d", 64), SizeBytes: 1024, Manifest: raftcluster.SnapshotManifestV1{Format: raftcluster.SnapshotManifestFormatV1, Version: 1, NodeID: begin.OldNodeID, GroupID: begin.GroupID, LastIncludedTerm: 4, LastIncludedIndex: 9, AppliedCommandLSN: 3, LogicalDigestV1: strings.Repeat("e", 64), Scope: raftcluster.SnapshotScopeIdentityV1{ScopeRule: "single-group-v1", DatabaseScope: "database/default", CatalogScope: "catalog/default"}, CreatedAt: time.Unix(1700000000, 0).UTC()}}
	state.Seed = &seed
	for _, phase := range []ReplicaReplacementPhaseV1{ReplicaReplacementSeededV1, ReplicaReplacementInstalledV1, ReplicaReplacementAddIntentV1, ReplicaReplacementPromoteIntentV1, ReplicaReplacementPromotedV1, ReplicaReplacementRemoveIntentV1, ReplicaReplacementRemovedV1} {
		state.Phase = phase
		if phase == ReplicaReplacementPromoteIntentV1 {
			digest := raftentry.CommandDigestV1{1}
			state.Tail = &raftcluster.ReplacementTailV1{GroupID: begin.GroupID, LeaderID: begin.OldNodeID, LeaderTerm: 4, CommitIndex: 12, ConfigurationIndex: 10, Progress: raftcluster.ReplacementTailProgressV1{EntryID: raftentry.ApplyEntryID{Term: 4, Index: 11}, CommandDigest: digest, ProgressDigest: raftentry.CommandDigestV1{2}, Result: raftentry.ApplyResultV1{CommandDigest: digest, ResultDigest: raftentry.CommandDigestV1{3}}}}
		}
		if phase == ReplicaReplacementRemoveIntentV1 {
			state.RemovalIndex = 12
		}
		if phase == ReplicaReplacementRemovedV1 {
			state.Result = &ReplicaReplacementResultV1{ConfigurationIndex: 13}
		}
		raw, err := EncodeReplicaReplacementStateV1(state)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.applyCommittedCatalogMetaV1(raw, a.applied+1); err != nil {
			t.Fatal(err)
		}
	}
	complete, err := a.ReplacementCompletionV1(begin.GroupID, peers)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = EncodeReplicaReplacementCompleteV1(complete)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(raw, a.applied+1); err != nil {
		t.Fatal(err)
	}
	return complete
}

func TestCatalogReplicaReplacementSerialCompletionSnapshotAndNextV1(t *testing.T) {
	a, first := replicaReplacementAuthorityForTestV1(t)
	original := a.record.Catalog
	initialSnapshot, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := EncodeReplicaReplacementBeginV1(first)
	if _, err := a.applyCommittedCatalogMetaV1(raw, 2); err != nil {
		t.Fatal(err)
	}
	oldSnapshot, err := a.ExportCatalogMetaSnapshotBytesV1()
	if err != nil {
		t.Fatal(err)
	}
	// A second group may not begin while the first is unresolved.
	second := first
	second.OperationID = "other-group"
	for _, g := range a.record.Catalog.Groups {
		if g.ID != first.GroupID {
			second.GroupID = g.ID
			second.OldNodeID = g.Members[0]
			break
		}
	}
	if second.GroupID == first.GroupID {
		t.Fatal("fixture requires two groups")
	}
	secondRaw, _ := EncodeReplicaReplacementBeginV1(second)
	if _, err := a.applyCommittedCatalogMetaV1(secondRaw, 3); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("parallel begin: %v", err)
	}
	// Legacy parallel current-state snapshots are deliberately incompatible.
	records := map[raftcluster.GroupID][]byte{first.GroupID: raw, second.GroupID: secondRaw}
	parallel, _ := encodeReplicaReplacementSnapshotV1(records)
	if _, err := decodeReplicaReplacementSnapshotV1(parallel, a.record); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("parallel snapshot: %v", err)
	}
	terminal := completeReplicaReplacementForTestV1(t, a, first)
	if !reflect.DeepEqual(original.Placements, a.record.Catalog.Placements) {
		t.Fatal("completion moved ownership")
	}
	applied := a.applied
	completeRaw, _ := EncodeReplicaReplacementCompleteV1(terminal)
	if _, err := a.applyCommittedCatalogMetaV1(completeRaw, applied+1); err != nil || a.applied != applied {
		t.Fatalf("terminal retry: %v", err)
	}
	if _, err := a.applyCommittedCatalogMetaV1(raw, applied+1); err != nil || a.applied != applied {
		t.Fatalf("begin terminal retry: %v", err)
	}
	assertRestore := func(snapshot []byte) {
		t.Helper()
		restored := NewCatalogMetaAuthorityV1()
		if err := restored.installCatalogMetaSnapshotBytesV1(oldSnapshot); err != nil {
			t.Fatal(err)
		}
		if err := restored.installCatalogMetaSnapshotBytesV1(snapshot); err != nil {
			t.Fatalf("lagging old pending -> current: %v", err)
		}
		again, err := restored.ExportCatalogMetaSnapshotBytesV1()
		if err != nil || !bytes.Equal(snapshot, again) {
			t.Fatalf("snapshot parity: %v", err)
		}
	}
	snapshot, _ := a.ExportCatalogMetaSnapshotBytesV1()
	assertRestore(snapshot)
	// Replace the previous dynamic target, preserving other current addresses.
	next := first
	next.OperationID = "second"
	next.ExpectedEpoch = a.record.Epoch
	next.CatalogDigest = a.record.Digest
	next.OldNodeID = first.NewPeer.ID
	next.NewPeer = raftcluster.Peer{ID: "spare-two", Address: "127.0.0.1:19002"}
	nextRaw, _ := EncodeReplicaReplacementBeginV1(next)
	if _, err := a.applyCommittedCatalogMetaV1(nextRaw, a.applied+1); err != nil {
		t.Fatal(err)
	}
	pending, _ := a.ReplicaReplacementStateV1(next.GroupID)
	if !reflect.DeepEqual(pending.Peers, terminal.State.Peers) {
		t.Fatal("next begin lost current addresses")
	}
	snapshot, _ = a.ExportCatalogMetaSnapshotBytesV1()
	assertRestore(snapshot)
	if _, err := a.applyCommittedCatalogMetaV1(raw, a.applied+1); err == nil {
		t.Fatal("superseded terminal retry accepted")
	}
	completeReplicaReplacementForTestV1(t, a, next)
	second.ExpectedEpoch = a.record.Epoch
	second.CatalogDigest = a.record.Digest
	completeReplicaReplacementForTestV1(t, a, second)
	snapshot, _ = a.ExportCatalogMetaSnapshotBytesV1()
	assertRestore(snapshot)
	// A compatible ordinary update retains a self-consistent snapshot.
	record, err := NewCatalogMetaRecordV1(a.record.Epoch+1, a.record.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	compatible, _ := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: a.record.Epoch, Record: record})
	if _, err := a.applyCommittedCatalogMetaV1(compatible, a.applied+1); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = a.ExportCatalogMetaSnapshotBytesV1()
	assertRestore(snapshot)
	catalog := a.record.Catalog
	catalog.Features.Required = append(slices.Clone(catalog.Features.Required), raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.Version{Major: 1}})
	record, err = NewCatalogMetaRecordV1(a.record.Epoch+1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	unsupported, _ := EncodeCatalogMetaCommandV1(CatalogMetaCommandV1{ExpectedEpoch: a.record.Epoch, Record: record})
	if _, err := a.applyCommittedCatalogMetaV1(unsupported, a.applied+1); err == nil {
		t.Fatal("incompatible feature activation accepted")
	}
	// A lagging authority must refuse the same feature activation when it
	// arrives as a complete snapshot rather than a catalog command.
	var forged CatalogMetaSnapshotV1
	if err := json.Unmarshal(snapshot, &forged); err != nil {
		t.Fatal(err)
	}
	forged.Record, err = encodeCatalogMetaRecordV1(record)
	if err != nil {
		t.Fatal(err)
	}
	forged.LastCommand = unsupported
	forged.AppliedIndex++
	forgedRaw, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("incompatible feature activation snapshot: %v", err)
	}
	lagging := NewCatalogMetaAuthorityV1()
	if err := lagging.installCatalogMetaSnapshotBytesV1(initialSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := lagging.installCatalogMetaSnapshotBytesV1(forgedRaw); !errors.Is(err, ErrCatalogMetaConflict) {
		t.Fatalf("incoming completed replacement activated feature: %v", err)
	}
	laggingRaw, err := lagging.ExportCatalogMetaSnapshotBytesV1()
	if err != nil || !bytes.Equal(laggingRaw, initialSnapshot) {
		t.Fatalf("refused incoming replacement mutated authority: %v", err)
	}
	unchanged, _ := a.ExportCatalogMetaSnapshotBytesV1()
	if !bytes.Equal(snapshot, unchanged) {
		t.Fatal("refused activation mutated authority")
	}
}

func TestCatalogReplicaReplacementCompleteNestedLimitsV1(t *testing.T) {
	for _, tc := range []struct {
		name    string
		catalog []byte
	}{
		{"members", catalogMetaMembersShapeV1(MaxCatalogMetaMembersPerGroupV1 + 1)},
		{"groups", catalogMetaGroupsShapeV1(MaxCatalogMetaGroupsV1 + 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// State is deliberately invalid: the nested catalog count preflight must
			// reject before state validation or typed catalog allocation.
			raw, err := json.Marshal(struct {
				Format  uint16          `json:"format"`
				Kind    string          `json:"kind"`
				State   json.RawMessage `json:"state"`
				Catalog json.RawMessage `json:"catalog"`
			}{1, ReplicaReplacementCompleteKindV1, json.RawMessage(`{}`), tc.catalog})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeReplicaReplacementCompleteV1(raw); !errors.Is(err, ErrCatalogMetaLimit) {
				t.Fatalf("nested limit: %v", err)
			}
		})
	}
	for _, address := range []string{"host", "host:0", "host:65536", ":1234"} {
		if err := validateReplicaReplacementPeersV1([]raftcluster.Peer{{ID: "node", Address: address}}); err == nil {
			t.Fatalf("accepted address %q", address)
		}
	}
}

func TestCatalogReplicaReplacementCompletionBindingsV1(t *testing.T) {
	a, begin := replicaReplacementAuthorityForTestV1(t)
	complete := completeReplicaReplacementForTestV1(t, a, begin)
	removed := complete.State
	removed.Phase = ReplicaReplacementRemovedV1
	removed.Peers = slices.Clone(complete.State.Peers)
	for i, p := range removed.Peers {
		if p.ID == begin.NewPeer.ID {
			removed.Peers[i] = raftcluster.Peer{ID: begin.OldNodeID, Address: "127.0.0.1:20000"}
		}
	}
	slices.SortFunc(removed.Peers, func(a, b raftcluster.Peer) int { return strings.Compare(string(a.ID), string(b.ID)) })
	removed.Result = &ReplicaReplacementResultV1{ConfigurationIndex: complete.State.Result.ConfigurationIndex}
	oldRaw, err := EncodeReplicaReplacementStateV1(removed)
	if err != nil {
		t.Fatal(err)
	}
	good, err := EncodeReplicaReplacementStateV1(complete.State)
	if err != nil {
		t.Fatal(err)
	}
	if !replicaReplacementSnapshotSuccessorV1(oldRaw, good) {
		t.Fatal("valid terminal transition refused")
	}
	for _, name := range []string{"native index", "target address", "survivor address"} {
		t.Run(name, func(t *testing.T) {
			next := complete.State
			next.Peers = slices.Clone(next.Peers)
			result := *next.Result
			next.Result = &result
			switch name {
			case "native index":
				next.Result.ConfigurationIndex++
			case "target address":
				for i, p := range next.Peers {
					if p.ID == begin.NewPeer.ID {
						next.Peers[i].Address = "127.0.0.1:29001"
					}
				}
			case "survivor address":
				for i, p := range next.Peers {
					if p.ID != begin.NewPeer.ID {
						next.Peers[i].Address = "127.0.0.1:29002"
						break
					}
				}
			}
			raw, err := EncodeReplicaReplacementStateV1(next)
			if err != nil {
				t.Fatal(err)
			}
			if replicaReplacementSnapshotSuccessorV1(oldRaw, raw) {
				t.Fatal("mutated terminal binding accepted")
			}
		})
	}
}
