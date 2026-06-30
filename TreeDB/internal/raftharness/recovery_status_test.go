package raftharness

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
)

func TestRecoveryStatusTransitionsAcrossSnapshotInstallTailReplayAndReopen(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := mixedUserEntries(t, 41)
	if _, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit mixed entries: %v", err)
	}

	status, err := h.RecoveryStatusV1(context.Background(), "node-b", RecoveryStatusOptionsV1{
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 before install: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessUnsafeNoSnapshotV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateNoneV1 ||
		status.TailState != raftcluster.RecoveryTailStateNoSnapshotV1 ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyAppliedIndexLaggingV1 ||
		status.TailTargetIndex != 4 ||
		status.SafeToServeReads {
		t.Fatalf("before-install status=%+v", status)
	}

	if _, err := h.ApplyCommittedEntriesToNode("node-a", entries[:2]...); err != nil {
		t.Fatalf("apply source prefix: %v", err)
	}
	source := mustNode(t, h, "node-a")
	manifest, err := source.fsm.ExportSnapshotManifestV1(raftfsm.SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712348000, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	install, err := h.InstallSnapshotPrefixToNodeV1("node-b", manifest)
	if err != nil {
		t.Fatalf("InstallSnapshotPrefixToNodeV1: %v evidence=%+v", err, install)
	}
	assertAppliedResults(t, "snapshot prefix install", install.Applied, []int64{1, 2})

	status, err = h.RecoveryStatusV1(context.Background(), "node-b", RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 after install: %v", err)
	}
	assertRecoveryTailPending(t, status, 4, 2)

	if _, err := h.ReopenNode("node-b"); err != nil {
		t.Fatalf("ReopenNode node-b: %v", err)
	}
	status, err = h.RecoveryStatusV1(context.Background(), "node-b", RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 after reopen: %v", err)
	}
	assertRecoveryTailPending(t, status, 4, 2)

	tail, err := h.ReplaySnapshotTailToNodeV1("node-b", manifest)
	if err != nil {
		t.Fatalf("ReplaySnapshotTailToNodeV1: %v results=%+v", err, tail)
	}
	assertAppliedResults(t, "snapshot tail replay", tail, []int64{1, 1})
	fullReplay, err := h.ApplyCommittedEntriesToNode("node-c", entries...)
	if err != nil {
		t.Fatalf("ApplyCommittedEntriesToNode full replay: %v results=%+v", err, fullReplay)
	}
	assertAppliedResults(t, "full replay", fullReplay, []int64{1, 2, 1, 1})
	if fullDigest, targetDigest := logicalDigest(t, h, "node-c"), logicalDigest(t, h, "node-b"); fullDigest != targetDigest {
		t.Fatalf("snapshot+tail digest mismatch full=%s target=%s", fullDigest.Hex(), targetDigest.Hex())
	}

	status, err = h.RecoveryStatusV1(context.Background(), "node-b", RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 after tail: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessReadyAppliedIndexV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestVerifiedV1 ||
		status.TailState != raftcluster.RecoveryTailStateCompleteV1 ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyAppliedIndexSatisfiedV1 ||
		status.TailLagEntries != 0 ||
		!status.SafeToServeReads {
		t.Fatalf("after-tail status=%+v", status)
	}
}

func TestRecoveryStatusReadSafetyAcceptsSparseCommittedPrefix(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := []raftfsm.CommittedEntryV1{
		committedCommand(44, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:recovery-status-sparse")),
		committedCommand(44, 3, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:recovery-status-sparse")),
	}
	if _, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit sparse recovery entries: %v", err)
	}
	if _, err := h.ApplyCommittedEntriesToNode("node-a", entries[:1]...); err != nil {
		t.Fatalf("apply sparse source prefix: %v", err)
	}
	manifest, err := mustNode(t, h, "node-a").fsm.ExportSnapshotManifestV1(raftfsm.SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712348003, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	if install, err := h.InstallSnapshotPrefixToNodeV1("node-b", manifest); err != nil {
		t.Fatalf("InstallSnapshotPrefixToNodeV1 sparse: %v evidence=%+v", err, install)
	}
	if tail, err := h.ReplaySnapshotTailToNodeV1("node-b", manifest); err != nil {
		t.Fatalf("ReplaySnapshotTailToNodeV1 sparse: %v results=%+v", err, tail)
	}

	status, err := h.RecoveryStatusV1(context.Background(), "node-b", RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 sparse: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessReadyAppliedIndexV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestVerifiedV1 ||
		status.TailState != raftcluster.RecoveryTailStateCompleteV1 ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyAppliedIndexSatisfiedV1 ||
		status.TailTargetIndex != 3 ||
		status.RequiredAppliedIndex != 3 ||
		!status.SafeToServeReads {
		t.Fatalf("sparse ready status=%+v", status)
	}
}

func TestRecoveryStatusDirtyTargetAndMismatchedManifestRemainUnsafe(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := mixedUserEntries(t, 42)
	if _, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit mixed entries: %v", err)
	}
	if _, err := h.ApplyCommittedEntriesToNode("node-a", entries[:2]...); err != nil {
		t.Fatalf("apply source prefix: %v", err)
	}
	manifest, err := mustNode(t, h, "node-a").fsm.ExportSnapshotManifestV1(raftfsm.SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712348001, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}

	target := mustNode(t, h, "node-b")
	if _, err := collections.NewCollectionManager(target.DB()).CreateCollection(&collections.CollectionMeta{Name: "localdirty"}); err != nil {
		t.Fatalf("dirty target CreateCollection: %v", err)
	}
	status, err := h.RecoveryStatusV1(context.Background(), "node-b", RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 dirty target: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessUnsafeManifestUnverifiedV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestRejectedV1 ||
		status.SafeToServeReads {
		t.Fatalf("dirty target status=%+v, want unsafe rejected manifest", status)
	}

	mismatched := manifest
	mismatched.LogicalDigestV1 = strings.Repeat("0", 64)
	status, err = h.RecoveryStatusV1(context.Background(), "node-a", RecoveryStatusOptionsV1{
		SnapshotManifest:  &mismatched,
		TailTargetIndex:   manifest.LastIncludedIndex,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 mismatched manifest: %v", err)
	}
	if status.Readiness != raftcluster.RecoveryReadinessUnsafeManifestUnverifiedV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestRejectedV1 ||
		len(status.Errors) == 0 ||
		status.SafeToServeReads {
		t.Fatalf("mismatched manifest status=%+v, want unsafe rejected manifest", status)
	}
}

func TestRecoveryStatusReadSafetyRejectsDivergentAppliedTail(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	committed := []raftfsm.CommittedEntryV1{
		committedCommand(43, 1, deterministicCreateCollectionEntry(t, "users", "harness:create:users:recovery-status-prefix")),
		committedCommand(43, 2, deterministicCreateCollectionEntry(t, "orders", "harness:create:orders:recovery-status-tail")),
	}
	if _, err := h.Commit(committed...); err != nil {
		t.Fatalf("Commit committed log: %v", err)
	}
	if _, err := h.ApplyCommittedEntriesToNode("node-a", committed[:1]...); err != nil {
		t.Fatalf("apply source prefix: %v", err)
	}
	manifest, err := mustNode(t, h, "node-a").fsm.ExportSnapshotManifestV1(raftfsm.SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712348002, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	if install, err := h.InstallSnapshotPrefixToNodeV1("node-b", manifest); err != nil {
		t.Fatalf("InstallSnapshotPrefixToNodeV1: %v evidence=%+v", err, install)
	}

	divergentTail := committedCommand(43, 2, deterministicCreateCollectionEntry(t, "admins", "harness:create:admins:recovery-status-divergent-tail"))
	seeded := applyEntriesDirectlyToNode(t, h, "node-b", divergentTail)
	assertAppliedResults(t, "node-b divergent recovery-status tail", seeded, []int64{1})

	status, err := h.RecoveryStatusV1(context.Background(), "node-b", RecoveryStatusOptionsV1{
		SnapshotManifest:  &manifest,
		RequireReadSafety: true,
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 divergent tail: %v", err)
	}
	if status.Readiness == raftcluster.RecoveryReadinessReadyAppliedIndexV1 ||
		status.SafeToServeReads ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyTargetMismatchV1 {
		t.Fatalf("divergent tail status=%+v, want unsafe read-safety mismatch", status)
	}
	if got := strings.Join(status.Errors, "\n"); !strings.Contains(got, "committed-prefix read safety validation failed") {
		t.Fatalf("divergent tail errors=%q, want committed-prefix validation error", got)
	}

	status, err = h.RecoveryStatusV1(context.Background(), "node-b", RecoveryStatusOptionsV1{
		SnapshotManifest: &manifest,
		ReadBarrier: raftcluster.AppliedIndexReadBarrier{
			MinAppliedIndex: 2,
		},
	})
	if err != nil {
		t.Fatalf("RecoveryStatusV1 explicit read barrier divergent tail: %v", err)
	}
	if status.Readiness == raftcluster.RecoveryReadinessReadyAppliedIndexV1 ||
		status.SafeToServeReads ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyTargetMismatchV1 {
		t.Fatalf("divergent tail explicit barrier status=%+v, want unsafe read-safety mismatch", status)
	}
}

func TestRecoveryStatusUnsupportedOperationsFailClosed(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	tests := []struct {
		name string
		run  func() (raftcluster.RecoveryStatusV1, error)
		want raftcluster.RecoveryUnsupportedOperationV1
	}{
		{
			name: "log truncation",
			run:  func() (raftcluster.RecoveryStatusV1, error) { return h.LogTruncationRecoveryStatusV1("node-b") },
			want: raftcluster.RecoveryUnsupportedLogTruncationV1,
		},
		{
			name: "production rejoin",
			run:  func() (raftcluster.RecoveryStatusV1, error) { return h.ProductionRejoinRecoveryStatusV1("node-b") },
			want: raftcluster.RecoveryUnsupportedProductionRejoinV1,
		},
		{
			name: "production snapshot transfer",
			run: func() (raftcluster.RecoveryStatusV1, error) {
				return h.ProductionSnapshotTransferRecoveryStatusV1("node-b")
			},
			want: raftcluster.RecoveryUnsupportedProductionSnapshotTransferV1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, err := tc.run()
			if !errors.Is(err, raftcluster.ErrRecoveryOperationUnsupported) {
				t.Fatalf("err=%v, want ErrRecoveryOperationUnsupported", err)
			}
			if status.Readiness != raftcluster.RecoveryReadinessUnsupportedV1 ||
				status.SafeToServeReads ||
				!reflect.DeepEqual(status.Unsupported, []raftcluster.RecoveryUnsupportedOperationV1{tc.want}) {
				t.Fatalf("status=%+v, want unsupported %s", status, tc.want)
			}
		})
	}
}

func assertRecoveryTailPending(t *testing.T, status raftcluster.RecoveryStatusV1, target, lag uint64) {
	t.Helper()
	if status.Readiness != raftcluster.RecoveryReadinessTailPendingV1 ||
		status.SnapshotState != raftcluster.RecoverySnapshotStateManifestVerifiedV1 ||
		status.TailState != raftcluster.RecoveryTailStatePendingV1 ||
		status.ReadSafetyState != raftcluster.RecoveryReadSafetyAppliedIndexLaggingV1 ||
		status.TailTargetIndex != target ||
		status.TailLagEntries != lag ||
		status.SafeToServeReads {
		t.Fatalf("tail-pending status=%+v", status)
	}
	if status.AppliedProgress.Index != target-lag ||
		status.AppliedCommandLSN == 0 ||
		status.SnapshotManifest == nil {
		t.Fatalf("tail-pending progress=%+v lsn=%d manifest=%+v", status.AppliedProgress, status.AppliedCommandLSN, status.SnapshotManifest)
	}
}
