package raftcluster

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestThreeNodeHarnessTwoIndependentGroupsProveReadIndexAndApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	left, err := OpenThreeNodeHarness(ctx, "group-left")
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := OpenThreeNodeHarness(ctx, "group-right")
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	if left.GroupID() == right.GroupID() || left.LeaderID() == "" || right.LeaderID() == "" {
		t.Fatalf("independent topology is incomplete: left=%q/%q right=%q/%q", left.GroupID(), left.LeaderID(), right.GroupID(), right.LeaderID())
	}
	entry := threeNodeHarnessProofEntry(t)
	for _, harness := range []*ThreeNodeHarness{left, right} {
		commit, err := harness.CommitAndProve(ctx, entry)
		if err != nil || !commit.Evidence.ProvesProductionConsensus() || commit.Entry.Index == 0 {
			t.Fatalf("commit group=%q result=%+v err=%v", harness.GroupID(), commit, err)
		}
		proof, progress, err := harness.ReadCoordinator().CoordinateRoutedReadIndex(ctx, ReadIndexBarrier{NodeID: harness.LeaderID(), GroupID: harness.GroupID()})
		if err != nil || proof.EvidenceKind != ReadIndexEvidenceProduction || !proof.HasQuorum || progress.Index < proof.Index || progress.GroupID != harness.GroupID() {
			t.Fatalf("read proof group=%q proof=%+v progress=%+v err=%v", harness.GroupID(), proof, progress, err)
		}
	}
}

func threeNodeHarnessProofEntry(t *testing.T) []byte {
	t.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("three-node-harness-proof-v1")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 1)},
		{ID: iwire.SectionCollectionRef, Bytes: append([]byte{1}, "harness_proof"...)},
		{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(iwire.DocumentFormatJSON))},
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("proof"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"proof":true}`))},
	}
	command, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}
