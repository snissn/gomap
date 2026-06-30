package raftcluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestSingleGroupSubmitterLeaderCommitsAppliesAndReturnsRaftCommitted(t *testing.T) {
	entry := testClusterCommandEntry(t, 7)
	catalogVersion := uint64(7)
	applier := &recordingClusterApplier{
		result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1},
		hook: func(CommittedCommandEntryV1) {
			catalogVersion = 8
		},
	}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: NewSequencedCommitSource(SequencedCommitSourceOptions{
			GroupID:             "group-a",
			NodeID:              "node-a",
			LeaderID:            "node-a",
			Term:                3,
			EvidenceKind:        CommitEvidenceProductionConsensusV1,
			ProductionConsensus: true,
		}),
		Applier: applier,
		CatalogVersionProvider: CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) {
			return catalogVersion, true, nil
		}),
	})

	result, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{
		RequestID: 17,
		AckPolicy: iwire.AckRaftCommitted,
	})
	if err != nil {
		t.Fatalf("SubmitCommandEntryV1: %v", err)
	}
	if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable {
		t.Fatalf("ack/recoverable=%d/%v want raft_committed/true", result.ActualAck, result.CommittedRecoverable)
	}
	if result.Evidence.Kind != CommitEvidenceProductionConsensusV1 || !result.Evidence.ProvesProductionConsensus() {
		t.Fatalf("evidence=%+v does not prove production consensus", result.Evidence)
	}
	if result.CommittedEntry.Term != 3 || result.CommittedEntry.Index != 1 {
		t.Fatalf("committed entry id=%d/%d want 3/1", result.CommittedEntry.Term, result.CommittedEntry.Index)
	}
	if result.CatalogVersion != 8 || !result.HasCatalogVersion {
		t.Fatalf("post catalog=%d/%v want 8/true", result.CatalogVersion, result.HasCatalogVersion)
	}
	entries := applier.snapshot()
	if len(entries) != 1 {
		t.Fatalf("applied entries=%d want 1", len(entries))
	}
	applied := entries[0]
	if !bytes.Equal(applied.Bytes, entry) {
		t.Fatal("applied entry bytes differ from submitted entry")
	}
	if applied.CurrentCatalogVersion != 7 || !applied.HasCurrentCatalogVersion || !applied.SyncLocalCommandWAL {
		t.Fatalf("applied catalog/sync=%d/%v/%v want 7/true/true", applied.CurrentCatalogVersion, applied.HasCurrentCatalogVersion, applied.SyncLocalCommandWAL)
	}
	if applied.ExpectedTarget == nil || applied.ExpectedTarget.CommandID != iwire.CommandInsertBatch {
		t.Fatalf("applied expected target=%+v want insert_batch target", applied.ExpectedTarget)
	}
}

func TestSingleGroupSubmitterLowerAckDoesNotClaimRaftCommitted(t *testing.T) {
	entry := testClusterCommandEntry(t, 7)
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: NewSequencedCommitSource(SequencedCommitSourceOptions{
			GroupID:             "group-a",
			NodeID:              "node-a",
			EvidenceKind:        CommitEvidenceProductionConsensusV1,
			ProductionConsensus: true,
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})

	result, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckVisible})
	if err != nil {
		t.Fatalf("SubmitCommandEntryV1 visible: %v", err)
	}
	if result.ActualAck != iwire.AckVisible || result.CommittedRecoverable {
		t.Fatalf("ack/recoverable=%d/%v want visible/false", result.ActualAck, result.CommittedRecoverable)
	}
	if len(applier.snapshot()) != 1 {
		t.Fatal("visible submit did not apply through committed bridge")
	}
}

func TestSingleGroupSubmitterRejectsUnsupportedAckBeforeCommitApply(t *testing.T) {
	entry := testClusterCommandEntry(t, 7)
	commitCalls := 0
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			commitCalls++
			return CommitCommandEntryV1Result{}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})

	_, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckPolicy(99)})
	if !errors.Is(err, ErrUnsupportedSubmitAck) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want unsupported submit ack", err)
	}
	if commitCalls != 0 {
		t.Fatalf("commit calls=%d want 0", commitCalls)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("apply calls=%d want 0", got)
	}
}

func TestSingleGroupSubmitterRejectsUnsatisfiedLocalAckBeforeCommitApply(t *testing.T) {
	for _, ack := range []iwire.AckPolicy{iwire.AckFlushed, iwire.AckSynced} {
		t.Run(fmt.Sprintf("ack_%d", ack), func(t *testing.T) {
			entry := testClusterCommandEntry(t, 7)
			commitCalls := 0
			applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
			submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
				AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
				CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
					commitCalls++
					return CommitCommandEntryV1Result{}, nil
				}),
				Applier:                applier,
				CatalogVersionProvider: staticCatalogVersion(7),
			})

			_, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: ack})
			if !errors.Is(err, ErrLocalAckUnavailable) {
				t.Fatalf("SubmitCommandEntryV1 err=%v want local ack unavailable", err)
			}
			if commitCalls != 0 {
				t.Fatalf("commit calls=%d want 0", commitCalls)
			}
			if got := len(applier.snapshot()); got != 0 {
				t.Fatalf("apply calls=%d want 0", got)
			}
		})
	}
}

func TestSingleGroupSubmitterRejectsStaleCatalogGuardBeforeCommitApply(t *testing.T) {
	entry := testClusterCommandEntry(t, 6)
	commitCalls := 0
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			commitCalls++
			return CommitCommandEntryV1Result{}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})

	_, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
	if !errors.Is(err, ErrCatalogVersionMismatch) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want catalog-version-mismatch", err)
	}
	if commitCalls != 0 {
		t.Fatalf("commit calls=%d want 0", commitCalls)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("apply calls=%d want 0", got)
	}
}

func TestSingleGroupSubmitterAdmissionRejectsBeforeCommitApply(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  AdmissionStatus
		wantErr error
	}{
		{name: "follower", status: FollowerAdmission("node-b", "not leader"), wantErr: ErrNotLeader},
		{name: "unavailable", status: UnavailableAdmission("election pending"), wantErr: ErrAdmissionUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commitCalls := 0
			applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}}
			submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
				AdmissionProvider: StaticAdmissionProvider{Status: tc.status},
				CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
					commitCalls++
					return CommitCommandEntryV1Result{}, nil
				}),
				Applier:                applier,
				CatalogVersionProvider: staticCatalogVersion(7),
			})

			_, err := submitter.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("SubmitCommandEntryV1 err=%v want %v", err, tc.wantErr)
			}
			if commitCalls != 0 {
				t.Fatalf("commit calls=%d want 0", commitCalls)
			}
			if got := len(applier.snapshot()); got != 0 {
				t.Fatalf("apply calls=%d want 0", got)
			}
		})
	}
}

func TestSingleGroupSubmitterRejectsNonProductionEvidenceBeforeApply(t *testing.T) {
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: NewSequencedCommitSource(SequencedCommitSourceOptions{
			GroupID: "group-a",
			NodeID:  "node-a",
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})

	_, err := submitter.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
	if !errors.Is(err, ErrCommitNotProven) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want commit-not-proven", err)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("apply calls=%d want 0", got)
	}
}

func TestSingleGroupSubmitterRequiresLocalRecoverability(t *testing.T) {
	applier := &recordingClusterApplier{
		result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusDeterministicGuardFailure, DeterministicErrorCode: raftentry.ErrorUnsafeDurabilityModeV1},
		err:    fmt.Errorf("local coverage missing"),
	}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: NewSequencedCommitSource(SequencedCommitSourceOptions{
			GroupID:             "group-a",
			NodeID:              "node-a",
			EvidenceKind:        CommitEvidenceProductionConsensusV1,
			ProductionConsensus: true,
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})

	result, err := submitter.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
	if !errors.Is(err, ErrLocalApplyNotRecoverable) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want local-apply-not-recoverable", err)
	}
	if result.CommittedRecoverable {
		t.Fatal("failed local apply reported committed recoverable")
	}
	if got := len(applier.snapshot()); got != 1 {
		t.Fatalf("apply calls=%d want 1", got)
	}
}

func TestSingleGroupSubmitterRejectsMissingPostApplyCatalogVersion(t *testing.T) {
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	catalogCalls := 0
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: NewSequencedCommitSource(SequencedCommitSourceOptions{
			GroupID:             "group-a",
			NodeID:              "node-a",
			EvidenceKind:        CommitEvidenceProductionConsensusV1,
			ProductionConsensus: true,
		}),
		Applier: applier,
		CatalogVersionProvider: CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) {
			catalogCalls++
			if catalogCalls == 1 {
				return 7, true, nil
			}
			return 0, false, nil
		}),
	})

	result, err := submitter.SubmitCommandEntryV1(context.Background(), testClusterCommandEntry(t, 7), raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
	if !errors.Is(err, ErrLocalApplyNotRecoverable) || !errors.Is(err, ErrMissingCatalogVersion) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want local-apply-not-recoverable and missing-catalog-version", err)
	}
	if result.CommittedRecoverable {
		t.Fatal("missing post-apply catalog version reported committed recoverable")
	}
	if result.HasCatalogVersion {
		t.Fatalf("result HasCatalogVersion=%v want false", result.HasCatalogVersion)
	}
	if got := len(applier.snapshot()); got != 1 {
		t.Fatalf("apply calls=%d want 1", got)
	}
	if catalogCalls != 2 {
		t.Fatalf("catalog calls=%d want 2", catalogCalls)
	}
}

type recordingClusterApplier struct {
	entries []CommittedCommandEntryV1
	result  raftentry.ApplyResultV1
	err     error
	hook    func(CommittedCommandEntryV1)
}

func (a *recordingClusterApplier) ApplyCommittedCommandEntryV1(ctx context.Context, entry CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	clone := entry.Clone()
	a.entries = append(a.entries, clone)
	if a.hook != nil {
		a.hook(clone)
	}
	return a.result, a.err
}

func (a *recordingClusterApplier) snapshot() []CommittedCommandEntryV1 {
	out := make([]CommittedCommandEntryV1, len(a.entries))
	for i := range a.entries {
		out[i] = a.entries[i].Clone()
	}
	return out
}

func newTestSingleGroupSubmitter(tb testing.TB, opts SingleGroupSubmitterOptions) *SingleGroupSubmitter {
	tb.Helper()
	root := tb.TempDir()
	opts.Cluster = Config{
		Dir:     filepath.Join(root, "db"),
		NodeID:  "node-a",
		GroupID: "group-a",
		Peers: []Peer{
			{ID: "node-a", Address: "127.0.0.1:7000"},
			{ID: "node-b", Address: "127.0.0.1:7001"},
			{ID: "node-c", Address: "127.0.0.1:7002"},
		},
	}
	submitter, err := NewSingleGroupSubmitter(opts)
	if err != nil {
		tb.Fatalf("NewSingleGroupSubmitter: %v", err)
	}
	return submitter
}

func staticCatalogVersion(version uint64) CatalogVersionProvider {
	return CatalogVersionProviderFunc(func(context.Context) (uint64, bool, error) {
		return version, true, nil
	})
}

func testClusterCommandEntry(tb testing.TB, catalogVersion uint64) []byte {
	tb.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("raftcluster/insert/u1")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
		{ID: iwire.SectionCollectionRef, Bytes: append([]byte{1}, "users"...)},
		{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(iwire.DocumentFormatJSON))},
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("u1"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"name":"Ada"}`))},
	}
	cmd, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		tb.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := iwire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		tb.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}
