package raftharness

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
)

func TestSnapshotInstallPrefixPlusTailReplayMatchesFullReplayDigest(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := mixedUserEntries(t, 31)
	if _, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit mixed entries: %v", err)
	}
	prefix := entries[:2]
	sourcePrefix, err := h.ApplyCommittedEntriesToNode("node-a", prefix...)
	if err != nil {
		t.Fatalf("apply source prefix: %v results=%+v", err, sourcePrefix)
	}
	assertAppliedResults(t, "source prefix", sourcePrefix, []int64{1, 2})
	source, ok := h.Node("node-a")
	if !ok {
		t.Fatal("node-a missing")
	}
	manifest, err := source.fsm.ExportSnapshotManifestV1(raftfsm.SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712346000, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	if manifest.LastIncludedIndex != 2 {
		t.Fatalf("manifest last index=%d, want 2", manifest.LastIncludedIndex)
	}

	install, err := h.InstallSnapshotPrefixToNodeV1("node-b", manifest)
	if err != nil {
		t.Fatalf("InstallSnapshotPrefixToNodeV1: %v evidence=%+v", err, install)
	}
	if install.Kind != EvidenceInjectedSnapshotInstallV1 || !install.Installed || install.ProvesProductionSnapshot() {
		t.Fatalf("install evidence=%+v, want installed injected snapshot evidence without production snapshot claim", install)
	}
	assertAppliedResults(t, "snapshot prefix install", install.Applied, []int64{1, 2})
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 31, Index: 2})

	targetTail, err := h.ReplaySnapshotTailToNodeV1("node-b", manifest)
	if err != nil {
		t.Fatalf("ReplaySnapshotTailToNodeV1 target tail: %v results=%+v", err, targetTail)
	}
	assertAppliedResults(t, "snapshot tail replay", targetTail, []int64{1, 1})
	fullReplay, err := h.ApplyCommittedEntriesToNode("node-c", entries...)
	if err != nil {
		t.Fatalf("ApplyCommittedEntriesToNode full replay: %v results=%+v", err, fullReplay)
	}
	assertAppliedResults(t, "full replay", fullReplay, []int64{1, 2, 1, 1})
	assertLastApplied(t, h, "node-c", raftentry.ApplyEntryID{Term: 31, Index: 4})
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 31, Index: 4})
	if fullDigest, targetDigest := logicalDigest(t, h, "node-c"), logicalDigest(t, h, "node-b"); fullDigest != targetDigest {
		t.Fatalf("snapshot+tail digest mismatch full=%s target=%s", fullDigest.Hex(), targetDigest.Hex())
	}
	assertDocumentCity(t, h, "node-b", "users", "u1", "sfo")
	assertDocumentMissing(t, h, "node-b", "users", "u2")
}

func TestSnapshotInstallReplaysSparseCommittedTail(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := mixedUserEntries(t, 35)
	entries[2].Index = 4
	entries[3].Index = 5
	if _, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit sparse mixed entries: %v", err)
	}
	prefix := entries[:2]
	if sourcePrefix, err := h.ApplyCommittedEntriesToNode("node-a", prefix...); err != nil {
		t.Fatalf("apply source sparse prefix: %v results=%+v", err, sourcePrefix)
	}
	source := mustNode(t, h, "node-a")
	manifest, err := source.fsm.ExportSnapshotManifestV1(raftfsm.SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712346004, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	if manifest.LastIncludedIndex != 2 {
		t.Fatalf("manifest last index=%d, want 2", manifest.LastIncludedIndex)
	}

	install, err := h.InstallSnapshotPrefixToNodeV1("node-b", manifest)
	if err != nil {
		t.Fatalf("InstallSnapshotPrefixToNodeV1 sparse: %v evidence=%+v", err, install)
	}
	assertAppliedResults(t, "sparse snapshot prefix install", install.Applied, []int64{1, 2})
	tail, err := h.ReplaySnapshotTailToNodeV1("node-b", manifest)
	if err != nil {
		t.Fatalf("ReplaySnapshotTailToNodeV1 sparse tail: %v results=%+v", err, tail)
	}
	assertAppliedResults(t, "sparse snapshot tail replay", tail, []int64{1, 1})
	fullReplay, err := h.ApplyCommittedEntriesToNode("node-c", entries...)
	if err != nil {
		t.Fatalf("ApplyCommittedEntriesToNode sparse full replay: %v results=%+v", err, fullReplay)
	}
	assertAppliedResults(t, "sparse full replay", fullReplay, []int64{1, 2, 1, 1})
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 35, Index: 5})
	assertLastApplied(t, h, "node-c", raftentry.ApplyEntryID{Term: 35, Index: 5})
	if fullDigest, targetDigest := logicalDigest(t, h, "node-c"), logicalDigest(t, h, "node-b"); fullDigest != targetDigest {
		t.Fatalf("sparse snapshot+tail digest mismatch full=%s target=%s", fullDigest.Hex(), targetDigest.Hex())
	}
}

func TestSnapshotInstallRejectsManifestMismatchBeforeTargetMutation(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := mixedUserEntries(t, 32)
	if _, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit mixed entries: %v", err)
	}
	if _, err := h.ApplyCommittedEntriesToNode("node-a", entries[:2]...); err != nil {
		t.Fatalf("apply source prefix: %v", err)
	}
	source, ok := h.Node("node-a")
	if !ok {
		t.Fatal("node-a missing")
	}
	manifest, err := source.fsm.ExportSnapshotManifestV1(raftfsm.SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712346001, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}
	tests := []struct {
		name string
		mut  func(*raftcluster.SnapshotManifestV1)
		want string
	}{
		{
			name: "scope",
			mut:  func(m *raftcluster.SnapshotManifestV1) { m.Scope.CatalogScope = "catalog/other" },
			want: "not valid for FSM scope",
		},
		{
			name: "group",
			mut:  func(m *raftcluster.SnapshotManifestV1) { m.GroupID = "other-group" },
			want: "does not match FSM group",
		},
		{
			name: "digest",
			mut: func(m *raftcluster.SnapshotManifestV1) {
				m.LogicalDigestV1 = strings.Repeat("0", 64)
			},
			want: "does not match FSM digest",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutated := manifest
			tc.mut(&mutated)
			install, err := h.InstallSnapshotPrefixToNodeV1("node-b", mutated)
			if err == nil || install.Installed {
				t.Fatalf("InstallSnapshotPrefixToNodeV1 err=%v evidence=%+v, want failed install", err, install)
			}
			if code, ok := raftfsm.ErrorCodeOf(err); !ok || code != raftentry.ErrorRejectedConflictV1 || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("InstallSnapshotPrefixToNodeV1 err=%v code=(%s,%t), want rejected conflict containing %q", err, code, ok, tc.want)
			}
			assertNoLastApplied(t, h, "node-b")
			assertCollectionMissing(t, h, "node-b", "users")
		})
	}
}

func TestSnapshotInstallRejectsDirtyTargetBeforeMutation(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := mixedUserEntries(t, 34)
	if _, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit mixed entries: %v", err)
	}
	if _, err := h.ApplyCommittedEntriesToNode("node-a", entries[:2]...); err != nil {
		t.Fatalf("apply source prefix: %v", err)
	}
	source := mustNode(t, h, "node-a")
	manifest, err := source.fsm.ExportSnapshotManifestV1(raftfsm.SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712346003, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}

	target := mustNode(t, h, "node-b")
	if _, err := collections.NewCollectionManager(target.DB()).CreateCollection(&collections.CollectionMeta{Name: "localdirty"}); err != nil {
		t.Fatalf("dirty target CreateCollection: %v", err)
	}
	beforeLSN := target.DB().State().AppliedCommandLSN
	if beforeLSN == 0 {
		t.Fatal("dirty target AppliedCommandLSN=0, want local coverage")
	}

	install, err := h.InstallSnapshotPrefixToNodeV1("node-b", manifest)
	if !errors.Is(err, ErrCommittedLogConflict) || install.Installed {
		t.Fatalf("InstallSnapshotPrefixToNodeV1 dirty target err=%v evidence=%+v, want conflict without install", err, install)
	}
	assertNoLastApplied(t, h, "node-b")
	if got := target.DB().State().AppliedCommandLSN; got != beforeLSN {
		t.Fatalf("dirty target AppliedCommandLSN after rejected install=%d, want unchanged %d", got, beforeLSN)
	}
	if _, openErr := collections.NewCollectionManager(target.DB()).OpenCollection("localdirty"); openErr != nil {
		t.Fatalf("localdirty collection missing after rejected install: %v", openErr)
	}
	assertCollectionMissing(t, h, "node-b", "users")
}

func TestSnapshotInstallRejectsUnsafeTargetAndTailConflict(t *testing.T) {
	h := openTestHarness(t)
	defer func() { _ = h.Close() }()

	entries := mixedUserEntries(t, 33)
	if _, err := h.Commit(entries...); err != nil {
		t.Fatalf("Commit mixed entries: %v", err)
	}
	if _, err := h.ApplyCommittedEntriesToNode("node-a", entries[:2]...); err != nil {
		t.Fatalf("apply source prefix: %v", err)
	}
	source, ok := h.Node("node-a")
	if !ok {
		t.Fatal("node-a missing")
	}
	manifest, err := source.fsm.ExportSnapshotManifestV1(raftfsm.SnapshotManifestExportOptionsV1{CreatedAt: time.Unix(1712346002, 0).UTC()})
	if err != nil {
		t.Fatalf("ExportSnapshotManifestV1: %v", err)
	}

	divergent := committedCommand(33, 1, deterministicCreateCollectionEntry(t, "admins", "harness:snapshot-install:admins"))
	seeded := applyEntriesDirectlyToNode(t, h, "node-c", divergent)
	assertAppliedResults(t, "node-c divergent seed", seeded, []int64{1})
	install, err := h.InstallSnapshotPrefixToNodeV1("node-c", manifest)
	if !errors.Is(err, ErrCommittedLogConflict) || install.Installed {
		t.Fatalf("InstallSnapshotPrefixToNodeV1 stale target err=%v evidence=%+v, want conflict without install", err, install)
	}
	assertLastApplied(t, h, "node-c", raftentry.ApplyEntryID{Term: 33, Index: 1})
	if _, openErr := collections.NewCollectionManager(mustNode(t, h, "node-c").DB()).OpenCollection("admins"); openErr != nil {
		t.Fatalf("node-c admins collection missing after rejected install: %v", openErr)
	}
	assertCollectionMissing(t, h, "node-c", "users")

	install, err = h.InstallSnapshotPrefixToNodeV1("node-b", manifest)
	if err != nil {
		t.Fatalf("InstallSnapshotPrefixToNodeV1 node-b: %v evidence=%+v", err, install)
	}
	h.mu.Lock()
	h.committed = h.committed[:1]
	h.mu.Unlock()
	tail, err := h.ReplaySnapshotTailToNodeV1("node-b", manifest)
	if !errors.Is(err, ErrCommittedLogGap) {
		t.Fatalf("ReplaySnapshotTailToNodeV1 after committed tail truncation err=%v results=%+v, want ErrCommittedLogGap", err, tail)
	}
	if len(tail) != 0 {
		t.Fatalf("ReplaySnapshotTailToNodeV1 after committed tail truncation results=%+v, want none", tail)
	}
	assertLastApplied(t, h, "node-b", raftentry.ApplyEntryID{Term: 33, Index: 2})
}

func mustNode(t testing.TB, h *Harness, nodeID raftcluster.NodeID) *Node {
	t.Helper()
	node, ok := h.Node(nodeID)
	if !ok {
		t.Fatalf("node %s missing", nodeID)
	}
	return node
}
