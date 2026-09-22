package raftcluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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
	if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable || !result.CommittedApplied {
		t.Fatalf("ack/recoverable/applied=%d/%v/%v want raft_committed/true/true", result.ActualAck, result.CommittedRecoverable, result.CommittedApplied)
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
	if result.ActualAck != iwire.AckVisible || result.CommittedRecoverable || !result.CommittedApplied {
		t.Fatalf("ack/recoverable/applied=%d/%v/%v want visible/false/true", result.ActualAck, result.CommittedRecoverable, result.CommittedApplied)
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

func TestSingleGroupSubmitterRejectsRaftCommittedWhenLocalCommandWALSyncDisabled(t *testing.T) {
	entry := testClusterCommandEntry(t, 7)
	preflightCalls := 0
	commitCalls := 0
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			commitCalls++
			return CommitCommandEntryV1Result{}, nil
		}),
		Preflight: CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			preflightCalls++
			return CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                    applier,
		CatalogVersionProvider:     staticCatalogVersion(7),
		DisableLocalCommandWALSync: true,
	})

	_, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
	if !errors.Is(err, ErrLocalAckUnavailable) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want local ack unavailable", err)
	}
	if preflightCalls != 0 {
		t.Fatalf("preflight calls=%d want 0", preflightCalls)
	}
	if commitCalls != 0 {
		t.Fatalf("commit calls=%d want 0", commitCalls)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("apply calls=%d want 0", got)
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

func TestSingleGroupSubmitterRejectsRouteGroupMismatchBeforePreflightCommitApply(t *testing.T) {
	entry := testClusterCommandEntry(t, 7)
	preflightCalls := 0
	commitCalls := 0
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			commitCalls++
			return CommitCommandEntryV1Result{}, nil
		}),
		Preflight: CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			preflightCalls++
			return CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})

	_, err := submitter.SubmitCommandEntryV1(context.Background(), entry, routeMetadata("group-b", iwire.AckRaftCommitted))
	if !errors.Is(err, ErrRouteGroupMismatch) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want route group mismatch", err)
	}
	if !strings.Contains(err.Error(), "leader_hint=node-a") {
		t.Fatalf("SubmitCommandEntryV1 err=%v want leader hint", err)
	}
	if preflightCalls != 0 {
		t.Fatalf("preflight calls=%d want 0", preflightCalls)
	}
	if commitCalls != 0 {
		t.Fatalf("commit calls=%d want 0", commitCalls)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("apply calls=%d want 0", got)
	}
}

func TestSingleGroupSubmitterMissingRouteGroupIDHasStableRouteClassBeforeSubmit(t *testing.T) {
	entry := testClusterCommandEntry(t, 7)
	preflightCalls := 0
	commitCalls := 0
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			commitCalls++
			return CommitCommandEntryV1Result{}, nil
		}),
		Preflight: CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			preflightCalls++
			return CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})

	_, err := submitter.SubmitCommandEntryV1(context.Background(), entry, routeMetadata("", iwire.AckRaftCommitted))
	if !errors.Is(err, ErrRouteGroupMismatch) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want route group mismatch", err)
	}
	route, ok := RouteErrorMetadataOf(err)
	if !ok {
		t.Fatalf("RouteErrorMetadataOf ok=false err=%v", err)
	}
	if route.Class != RouteErrorClassMissingOwner || route.LocalGroupID != "group-a" || route.GroupID != "" || route.Collection != "users" {
		t.Fatalf("route metadata=%+v want missing owner for local group-a/users", route)
	}
	if preflightCalls != 0 {
		t.Fatalf("preflight calls=%d want 0", preflightCalls)
	}
	if commitCalls != 0 {
		t.Fatalf("commit calls=%d want 0", commitCalls)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("apply calls=%d want 0", got)
	}
}

func TestSingleGroupSubmitterAllowsMatchingRouteGroup(t *testing.T) {
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

	result, err := submitter.SubmitCommandEntryV1(context.Background(), entry, routeMetadata("group-a", iwire.AckRaftCommitted))
	if err != nil {
		t.Fatalf("SubmitCommandEntryV1 matching route group: %v", err)
	}
	if result.ActualAck != iwire.AckRaftCommitted || !result.CommittedRecoverable {
		t.Fatalf("ack/recoverable=%d/%v want raft_committed/true", result.ActualAck, result.CommittedRecoverable)
	}
	if result.CommittedEntry.RequestMetadata.ClusterRouteGroupID != "group-a" {
		t.Fatalf("committed route group=%q want group-a", result.CommittedEntry.RequestMetadata.ClusterRouteGroupID)
	}
	if got := len(applier.snapshot()); got != 1 {
		t.Fatalf("apply calls=%d want 1", got)
	}
}

func TestSingleGroupSubmitterPreservesNoRouteMetadataBehavior(t *testing.T) {
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

	metadata := raftentry.RequestMetadataV1{
		AckPolicy:           iwire.AckRaftCommitted,
		ClusterRouteGroupID: "group-b",
	}
	if _, err := submitter.SubmitCommandEntryV1(context.Background(), entry, metadata); err != nil {
		t.Fatalf("SubmitCommandEntryV1 without known route metadata: %v", err)
	}
	if got := len(applier.snapshot()); got != 1 {
		t.Fatalf("apply calls=%d want 1", got)
	}
}

func TestSingleGroupSubmitterLetsIdempotentCreateRetryReachPreflightApply(t *testing.T) {
	entry := testClusterCreateCollectionEntry(t, 7)
	commitCalls := 0
	preflightCalls := 0
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusAlreadyApplied}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(ctx context.Context, req CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			commitCalls++
			return productionCommittedResult(req, 3, uint64(commitCalls)), nil
		}),
		Preflight: CommandEntryPreflightFunc(func(ctx context.Context, req CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			preflightCalls++
			if req.DecodedEntry.Target.CommandID != iwire.CommandCreateCollection {
				t.Fatalf("preflight command=%d want create_collection", req.DecodedEntry.Target.CommandID)
			}
			if req.CurrentCatalogVersion != 8 || !req.HasCurrentCatalogVersion {
				t.Fatalf("preflight catalog=%d/%v want 8/true", req.CurrentCatalogVersion, req.HasCurrentCatalogVersion)
			}
			return CommandEntryPreflightResultV1{KnownIdempotencyReplay: true}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(8),
	})

	result, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
	if err != nil {
		t.Fatalf("SubmitCommandEntryV1 idempotent create retry: %v", err)
	}
	if result.ApplyResult.Status != raftentry.ApplyStatusAlreadyApplied {
		t.Fatalf("apply status=%s want already-applied", result.ApplyResult.Status)
	}
	if preflightCalls != 1 || commitCalls != 1 {
		t.Fatalf("preflight/commit calls=%d/%d want 1/1", preflightCalls, commitCalls)
	}
	if got := len(applier.snapshot()); got != 1 {
		t.Fatalf("apply calls=%d want 1", got)
	}
}

func TestSingleGroupSubmitterRejectsStaleIdempotentCreateWhenPreflightRejects(t *testing.T) {
	entry := testClusterCreateCollectionEntry(t, 7)
	commitCalls := 0
	preflightCalls := 0
	preflightErr := errors.New("create preflight rejected")
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusAlreadyApplied}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			commitCalls++
			return CommitCommandEntryV1Result{}, nil
		}),
		Preflight: CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			preflightCalls++
			return CommandEntryPreflightResultV1{}, preflightErr
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(8),
	})

	_, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
	if !errors.Is(err, ErrCatalogVersionMismatch) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want catalog-version-mismatch", err)
	}
	if errors.Is(err, preflightErr) {
		t.Fatalf("SubmitCommandEntryV1 err=%v should preserve stale guard mismatch over preflight rejection", err)
	}
	if preflightCalls != 1 {
		t.Fatalf("preflight calls=%d want 1", preflightCalls)
	}
	if commitCalls != 0 {
		t.Fatalf("commit calls=%d want 0", commitCalls)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("apply calls=%d want 0", got)
	}
}

func TestSingleGroupSubmitterRejectsStaleIdempotentCreateWithoutKnownReplay(t *testing.T) {
	entry := testClusterCreateCollectionEntry(t, 7)
	commitCalls := 0
	preflightCalls := 0
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusAlreadyApplied}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			commitCalls++
			return CommitCommandEntryV1Result{}, nil
		}),
		Preflight: CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			preflightCalls++
			return CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(8),
	})

	_, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
	if !errors.Is(err, ErrCatalogVersionMismatch) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want catalog-version-mismatch", err)
	}
	if preflightCalls != 1 {
		t.Fatalf("preflight calls=%d want 1", preflightCalls)
	}
	if commitCalls != 0 {
		t.Fatalf("commit calls=%d want 0", commitCalls)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("apply calls=%d want 0", got)
	}
}

func TestAllowPreflightIdempotentCreateRetryRequiresCatalogMismatch(t *testing.T) {
	entry := raftentry.CommandEntryV1{
		Target: raftentry.TargetIdentityV1{
			CommandID:              iwire.CommandCreateCollection,
			ExpectedCatalogVersion: []byte{0x80},
		},
		Idempotency:    raftentry.IdempotencyRequiredV1,
		IdempotencyKey: []byte("raftcluster/create/users"),
	}
	guardErr := checkSubmitCatalogGuardV1(entry, 8)
	if guardErr == nil || errors.Is(guardErr, ErrCatalogVersionMismatch) {
		t.Fatalf("malformed guard err=%v, want non-mismatch guard error", guardErr)
	}
	if allowPreflightIdempotentCreateRetryV1(entry, guardErr) {
		t.Fatal("malformed create guard was allowed through stale-create retry path")
	}

	entry.Target.ExpectedCatalogVersion = nil
	guardErr = checkSubmitCatalogGuardV1(entry, 8)
	if guardErr == nil || errors.Is(guardErr, ErrCatalogVersionMismatch) {
		t.Fatalf("missing guard err=%v, want non-mismatch guard error", guardErr)
	}
	if allowPreflightIdempotentCreateRetryV1(entry, guardErr) {
		t.Fatal("missing create guard was allowed through stale-create retry path")
	}

	entry.Target.ExpectedCatalogVersion = binary.AppendUvarint(nil, 7)
	guardErr = checkSubmitCatalogGuardV1(entry, 8)
	if !errors.Is(guardErr, ErrCatalogVersionMismatch) {
		t.Fatalf("stale guard err=%v, want catalog-version mismatch", guardErr)
	}
	if !allowPreflightIdempotentCreateRetryV1(entry, guardErr) {
		t.Fatal("stale idempotent create guard was not allowed to reach preflight")
	}
}

func TestSingleGroupSubmitterSerializesLocalApplyByCommittedIndex(t *testing.T) {
	entry := testClusterCommandEntry(t, 7)
	applier := newOrderedBlockingApplier()
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
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstErr := make(chan error, 1)
	go func() {
		_, err := submitter.SubmitCommandEntryV1(ctx, entry, raftentry.RequestMetadataV1{RequestID: 1, AckPolicy: iwire.AckRaftCommitted})
		firstErr <- err
	}()
	select {
	case <-applier.firstEntered:
	case <-ctx.Done():
		t.Fatalf("first submit did not reach apply: %v", ctx.Err())
	}

	secondErr := make(chan error, 1)
	go func() {
		_, err := submitter.SubmitCommandEntryV1(ctx, entry, raftentry.RequestMetadataV1{RequestID: 2, AckPolicy: iwire.AckRaftCommitted})
		secondErr <- err
	}()
	select {
	case index := <-applier.outOfOrder:
		t.Fatalf("submitter let index %d reach local apply before index 1 completed", index)
	case <-time.After(100 * time.Millisecond):
	}
	close(applier.releaseFirst)

	for name, ch := range map[string]<-chan error{"first": firstErr, "second": secondErr} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s submit err=%v", name, err)
			}
		case <-ctx.Done():
			t.Fatalf("%s submit timed out: %v", name, ctx.Err())
		}
	}
	entries := applier.snapshot()
	if len(entries) != 2 {
		t.Fatalf("applied entries=%d want 2", len(entries))
	}
	if entries[0].Index != 1 || entries[1].Index != 2 {
		t.Fatalf("applied indexes=%d,%d want 1,2", entries[0].Index, entries[1].Index)
	}
}

func TestSingleGroupSubmitterPreflightsBeforeCommitApply(t *testing.T) {
	entry := testClusterCommandEntry(t, 7)
	commitCalls := 0
	preflightCalls := 0
	preflightErr := errors.New("missing collection preflight")
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}}
	submitter := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			commitCalls++
			return CommitCommandEntryV1Result{}, nil
		}),
		Preflight: CommandEntryPreflightFunc(func(ctx context.Context, req CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			preflightCalls++
			if req.GroupID != "group-a" || req.NodeID != "node-a" {
				t.Fatalf("preflight group/node=%q/%q want group-a/node-a", req.GroupID, req.NodeID)
			}
			if req.DecodedEntry.Target.CommandID != iwire.CommandInsertBatch {
				t.Fatalf("preflight command=%d want insert_batch", req.DecodedEntry.Target.CommandID)
			}
			if req.CurrentCatalogVersion != 7 || !req.HasCurrentCatalogVersion {
				t.Fatalf("preflight catalog=%d/%v want 7/true", req.CurrentCatalogVersion, req.HasCurrentCatalogVersion)
			}
			return CommandEntryPreflightResultV1{}, preflightErr
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})

	_, err := submitter.SubmitCommandEntryV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted})
	if !errors.Is(err, preflightErr) {
		t.Fatalf("SubmitCommandEntryV1 err=%v want preflight error", err)
	}
	if preflightCalls != 1 {
		t.Fatalf("preflight calls=%d want 1", preflightCalls)
	}
	if commitCalls != 0 {
		t.Fatalf("commit calls=%d want 0", commitCalls)
	}
	if got := len(applier.snapshot()); got != 0 {
		t.Fatalf("apply calls=%d want 0", got)
	}
}

func TestSingleGroupSubmitterSerializesPreCommitAfterPreflightBeforeCommitV1(t *testing.T) {
	entry := testClusterCommandEntry(t, 7)
	preflightErr := errors.New("idempotency key conflicts with a different command")
	var order []string
	rejected := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(context.Context, CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			order = append(order, "commit")
			return CommitCommandEntryV1Result{}, nil
		}),
		Preflight: CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			order = append(order, "preflight")
			return CommandEntryPreflightResultV1{}, preflightErr
		}),
		Applier:                &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}},
		CatalogVersionProvider: staticCatalogVersion(7),
	})
	_, err := rejected.SubmitCommandEntryWithPreCommitV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted}, func(context.Context) error {
		order = append(order, "lifecycle-admission")
		return nil
	})
	if !errors.Is(err, preflightErr) {
		t.Fatalf("rejected submit err=%v want preflight conflict", err)
	}
	if got, want := strings.Join(order, ","), "preflight"; got != want {
		t.Fatalf("rejected submit order=%q want %q", got, want)
	}

	order = nil
	applier := &recordingClusterApplier{result: raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied}}
	accepted := newTestSingleGroupSubmitter(t, SingleGroupSubmitterOptions{
		AdmissionProvider: StaticAdmissionProvider{Status: LeaderAdmission()},
		CommitSource: CommitSourceFunc(func(_ context.Context, req CommitCommandEntryV1Request) (CommitCommandEntryV1Result, error) {
			order = append(order, "commit")
			return CommitCommandEntryV1Result{
				Entry: CommittedCommandEntryV1{
					Term:                     1,
					Index:                    1,
					Bytes:                    bytes.Clone(req.EntryBytes),
					CurrentCatalogVersion:    req.CurrentCatalogVersion,
					HasCurrentCatalogVersion: req.HasCurrentCatalogVersion,
					SyncLocalCommandWAL:      req.SyncLocalCommandWAL,
					RequestMetadata:          cloneRequestMetadataV1(req.RequestMetadata),
					ExpectedTarget:           req.ExpectedTarget,
				},
				Evidence: CommitEvidenceV1{
					Kind: CommitEvidenceProductionConsensusV1, GroupID: "group-a", NodeID: "node-a", LeaderID: "node-a",
					Term: 1, Index: 1, Committed: true, ProductionConsensus: true,
				},
			}, nil
		}),
		Preflight: CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			order = append(order, "preflight")
			return CommandEntryPreflightResultV1{}, nil
		}),
		Applier:                applier,
		CatalogVersionProvider: staticCatalogVersion(7),
	})
	if _, err := accepted.SubmitCommandEntryWithPreCommitV1(context.Background(), entry, raftentry.RequestMetadataV1{AckPolicy: iwire.AckRaftCommitted}, func(context.Context) error {
		order = append(order, "lifecycle-admission")
		return nil
	}); err != nil {
		t.Fatalf("accepted submit: %v", err)
	}
	if got, want := strings.Join(order, ","), "preflight,lifecycle-admission,commit"; got != want {
		t.Fatalf("accepted submit order=%q want %q", got, want)
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
	if !result.CommittedApplied {
		t.Fatal("missing post-apply catalog version discarded committed-applied evidence")
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
	if opts.Preflight == nil {
		opts.Preflight = CommandEntryPreflightFunc(func(context.Context, CommandEntryPreflightRequestV1) (CommandEntryPreflightResultV1, error) {
			return CommandEntryPreflightResultV1{}, nil
		})
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

func routeMetadata(group string, ack iwire.AckPolicy) raftentry.RequestMetadataV1 {
	return raftentry.RequestMetadataV1{
		AckPolicy:                 ack,
		ClusterRouteKnown:         true,
		ClusterRouteDatabase:      "default",
		ClusterRouteCatalog:       "default",
		ClusterRouteCollection:    "users",
		ClusterRouteShape:         "collection",
		ClusterRouteGroupID:       group,
		ClusterRouteMembers:       []string{"node-a", "node-b"},
		ClusterRouteLeaderHint:    "node-a",
		ClusterRoutePlacementMode: "collection",
	}
}

func productionCommittedResult(req CommitCommandEntryV1Request, term, index uint64) CommitCommandEntryV1Result {
	entry := CommittedCommandEntryV1{
		Term:                     term,
		Index:                    index,
		Bytes:                    bytes.Clone(req.EntryBytes),
		CurrentCatalogVersion:    req.CurrentCatalogVersion,
		HasCurrentCatalogVersion: req.HasCurrentCatalogVersion,
		SyncLocalCommandWAL:      req.SyncLocalCommandWAL,
		RequestMetadata:          cloneRequestMetadataV1(req.RequestMetadata),
		ExpectedTarget:           cloneExpectedTargetV1(req.ExpectedTarget),
	}
	return CommitCommandEntryV1Result{
		Entry: entry,
		Evidence: CommitEvidenceV1{
			Kind:                CommitEvidenceProductionConsensusV1,
			GroupID:             req.GroupID,
			NodeID:              req.NodeID,
			LeaderID:            req.NodeID,
			Term:                term,
			Index:               index,
			Committed:           true,
			ProductionConsensus: true,
		},
	}
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

func testClusterCreateCollectionEntry(tb testing.TB, catalogVersion uint64) []byte {
	tb.Helper()
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("raftcluster/create/users")},
		{ID: iwire.SectionCollectionMeta, Bytes: testClusterCollectionMetaPayload("users")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, catalogVersion)},
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

func testClusterCollectionMetaPayload(collection string) []byte {
	dst := binary.AppendUvarint(nil, 1) // collection_meta version
	dst = appendTestString(dst, collection)
	dst = binary.AppendUvarint(dst, 0) // document_format
	dst = binary.AppendUvarint(dst, 0) // data_root_storage_policy
	dst = binary.AppendUvarint(dst, 0) // index_state_storage_policy
	dst = append(dst, 0)               // allow_array_values_in_index
	dst = append(dst, 0)               // disable_indexed_write_memtables
	dst = append(dst, 0)               // buffered_indexed_writes
	dst = binary.AppendVarint(dst, 0)  // buffered_indexed_write_max_documents
	dst = binary.AppendVarint(dst, 0)  // buffered_indexed_write_max_bytes
	dst = binary.AppendVarint(dst, 0)  // buffered_indexed_write_max_root_runs
	dst = append(dst, 0)               // buffered_indexed_async_flush
	dst = append(dst, 0)               // buffered_indexed_overlay_roots
	dst = binary.AppendVarint(dst, 0)  // buffered_indexed_async_flush_max_queued_units
	dst = binary.AppendUvarint(dst, 0) // index_count
	return dst
}

func appendTestString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

type orderedBlockingApplier struct {
	mu           sync.Mutex
	entries      []CommittedCommandEntryV1
	firstEntered chan struct{}
	releaseFirst chan struct{}
	outOfOrder   chan uint64
	firstDone    bool
}

func newOrderedBlockingApplier() *orderedBlockingApplier {
	return &orderedBlockingApplier{
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		outOfOrder:   make(chan uint64, 1),
	}
}

func (a *orderedBlockingApplier) ApplyCommittedCommandEntryV1(ctx context.Context, entry CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	clone := entry.Clone()
	a.mu.Lock()
	a.entries = append(a.entries, clone)
	if clone.Index == 1 && len(a.entries) == 1 {
		close(a.firstEntered)
		a.mu.Unlock()
		select {
		case <-a.releaseFirst:
		case <-ctx.Done():
			return raftentry.ApplyResultV1{}, ctx.Err()
		}
		a.mu.Lock()
		a.firstDone = true
		a.mu.Unlock()
		return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}, nil
	}
	if clone.Index != uint64(len(a.entries)) || !a.firstDone {
		select {
		case a.outOfOrder <- clone.Index:
		default:
		}
	}
	a.mu.Unlock()
	return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, AffectedCount: 1}, nil
}

func (a *orderedBlockingApplier) snapshot() []CommittedCommandEntryV1 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]CommittedCommandEntryV1, len(a.entries))
	for i := range a.entries {
		out[i] = a.entries[i].Clone()
	}
	return out
}
