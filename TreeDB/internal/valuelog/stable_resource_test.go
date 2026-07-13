package valuelog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableValueLogRenameUsesCallerCapturedParent(t *testing.T) {
	root := t.TempDir()
	segmentDir := filepath.Join(root, "segments")
	if err := os.Mkdir(segmentDir, 0o700); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(segmentDir, "000001.vlog")
	writer, err := NewWriter(oldPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	parent, err := os.Open(segmentDir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	newName := "renamed.vlog"
	if err := os.Rename(oldPath, filepath.Join(segmentDir, newName)); err != nil {
		t.Fatal(err)
	}
	movedDir := filepath.Join(root, "segments-moved")
	if err := os.Rename(segmentDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(segmentDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(segmentDir, newName), []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	token, err := writer.StableResourceToken(StableResourceRegistration{
		LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/renamed.vlog",
		Reachability:    rootpublication.ReachabilityValueLogPointer,
		NamespaceParent: parent, ParentGeneration: 1, NamespaceOperation: rootpublication.NamespaceRename,
		OldName: "000001.vlog", NewName: newName,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	if token.Namespace() == nil || token.Namespace().Operation() != rootpublication.NamespaceRename {
		t.Fatalf("namespace=%v want exact rename token", token.Namespace())
	}
	replacementParent, err := os.Open(segmentDir)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementParent.Close()
	replacementNamespace, err := rootpublication.NewStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: replacementParent, ParentGeneration: 1, Operation: rootpublication.NamespaceCreate,
		NewName: "probe", DiagnosticPath: "segments",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replacementNamespace.Release()
	if token.Namespace().ParentIdentity() == replacementNamespace.ParentIdentity() {
		t.Fatal("rename token rebound to replacement parent path")
	}
}

func TestStableValueLogRotationCreatesThroughCapturedParent(t *testing.T) {
	root := t.TempDir()
	segmentDir := filepath.Join(root, "segments")
	if err := os.Mkdir(segmentDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(filepath.Join(segmentDir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	movedDir := filepath.Join(root, "segments-moved")
	originalOpenParent := openStableValueLogParent
	openStableValueLogParent = func(path string) (*os.File, error) {
		parent, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		if err := os.Rename(segmentDir, movedDir); err != nil {
			_ = parent.Close()
			return nil, err
		}
		if err := os.Mkdir(segmentDir, 0o700); err != nil {
			_ = parent.Close()
			return nil, err
		}
		return parent, nil
	}
	defer func() { openStableValueLogParent = originalOpenParent }()
	rotation, err := writer.RotateToWithStableResources(filepath.Join(segmentDir, "000002.vlog"), 2, false,
		StableResourceRegistration{LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog", Reachability: rootpublication.ReachabilityValueLogPointer},
		StableResourceRegistration{LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog", Reachability: rootpublication.ReachabilityValueLogPointer,
			ParentGeneration: 1, NamespaceOperation: rootpublication.NamespaceCreate})
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if _, err := os.Stat(filepath.Join(movedDir, "000002.vlog")); err != nil {
		t.Fatalf("captured-parent active segment: %v", err)
	}
	if _, err := os.Stat(filepath.Join(segmentDir, "000002.vlog")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement path unexpectedly received active segment: %v", err)
	}
	if stats := writer.DurabilityStats(); stats.FileSyncCalls != 0 {
		t.Fatalf("rotation with syncCurrent=false content syncs=%d want 0 before publication", stats.FileSyncCalls)
	}
}

func TestStableValueLogOrdinaryAppendDoesNotSyncNamespace(t *testing.T) {
	writer, err := NewWriter(filepath.Join(t.TempDir(), "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	before := writer.DurabilityStats().DirectorySyncCalls
	for rid := uint64(1); rid <= 32; rid++ {
		if _, err := writer.Append(0, nil, rid, []byte("record")); err != nil {
			t.Fatal(err)
		}
	}
	if after := writer.DurabilityStats().DirectorySyncCalls; after != before {
		t.Fatalf("ordinary appends added namespace syncs: before=%d after=%d", before, after)
	}
}

func TestRotateToWithStableResourcesRetainsClosedAndActiveIdentities(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, "000002.vlog"), 2, true,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability:     rootpublication.ReachabilityValueLogPointer,
			ParentGeneration: 1, NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if rotation.Closed == nil || rotation.Active == nil {
		t.Fatalf("rotation=%+v want closed and active tokens", rotation)
	}
	if rotation.Closed.Identity() == rotation.Active.Identity() {
		t.Fatal("rotation collapsed distinct file identities")
	}
	if rotation.Closed.Frontier().Bytes == 0 {
		t.Fatal("closed segment lost accepted byte frontier")
	}
	if rotation.Closed.ResourceID() != "1" || rotation.Active.ResourceID() != "2" {
		t.Fatalf("resource IDs closed=%s active=%s", rotation.Closed.ResourceID(), rotation.Active.ResourceID())
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
	if err := builder.Add(rotation.TakeClosed()); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(rotation.TakeActive()); err != nil {
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	if set.Len() != 2 {
		t.Fatalf("rotated set len=%d want 2", set.Len())
	}
	if err := set.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if err := set.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].PendingCount != 2 || stats[0].Flushes != 2 || stats[0].Syncs != 2 || stats[0].NamespaceSyncs != 1 {
		t.Fatalf("stable rotation operation counts=%+v", stats)
	}
}

func TestStableValueLogRotationNamespaceFailureKeepsOldWriterActive(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("before-failure")); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected namespace failure")
	originalFactory := newValueLogStableNamespaceToken
	newValueLogStableNamespaceToken = func(rootpublication.StableNamespaceSpec) (*rootpublication.StableNamespaceToken, error) {
		return nil, injected
	}
	defer func() { newValueLogStableNamespaceToken = originalFactory }()
	rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, "000002.vlog"), 2, true,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 1,
			NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want injected namespace failure", err)
	}
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if writer.FileID() != 1 || writer.f == nil || writer.f.Name() != firstPath {
		t.Fatalf("failed rotation changed active writer: id=%d file=%v", writer.FileID(), writer.f)
	}
	if _, err := writer.Append(0, nil, 2, []byte("after-failure")); err != nil {
		t.Fatalf("old writer append after failed rotation: %v", err)
	}
}

func TestStableValueLogTokenCarriesCanonicalExternalRIDFence(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000007.vlog"), 7)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 9, []byte("rid")); err != nil {
		t.Fatal(err)
	}
	token, err := writer.StableResourceToken(StableResourceRegistration{
		Kind:        rootpublication.ResourceCommandWALExternalRID,
		LogicalLane: "main", Generation: 7, DiagnosticPath: "maindb/value_vlog/000007.vlog",
		Reachability: rootpublication.ReachabilityCommandWALExternalRIDFence,
		ExternalRIDs: []uint64{9, 2, 9, 4}, Digest: sha256.Sum256([]byte("segment-header")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	frontier := token.Frontier()
	if frontier.RIDCount != 3 || frontier.RIDMin != 2 || frontier.RIDMax != 9 || frontier.RIDSetDigest == [32]byte{} {
		t.Fatalf("RID frontier=%+v", frontier)
	}
	if token.Kind() != rootpublication.ResourceCommandWALExternalRID {
		t.Fatalf("token kind=%q want command WAL external RID", token.Kind())
	}
}

func TestStableValueLogRegistrationSupportsOuterLeafProducerKinds(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "outer-leaf.log"), 9)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("outer-leaf")); err != nil {
		t.Fatal(err)
	}
	token, err := writer.StableResourceToken(StableResourceRegistration{
		Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "outer-leaf", Generation: 9,
		DiagnosticPath: "maindb/outer_leaf/raw/000009.log", Reachability: rootpublication.ReachabilityOuterLeafRawPointer,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()
	if token.Kind() != rootpublication.ResourceOuterLeafLog {
		t.Fatalf("token kind=%q", token.Kind())
	}
}

func TestStableValueLogRegistrationRejectsForeignProducerField(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000010.vlog"), 10)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("foreign-field")); err != nil {
		t.Fatal(err)
	}
	token, err := writer.StableResourceToken(StableResourceRegistration{
		Kind: rootpublication.ResourceColumnAsset, LogicalLane: "main", Generation: 10,
		DiagnosticPath: "column_assets/foreign/segments/000010.seg",
		Reachability:   rootpublication.ReachabilityColumnManifest,
	})
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("foreign producer field token=%v err=%v", token, err)
	}
}

func BenchmarkStableValueLogRotation(b *testing.B) {
	for _, syncCurrent := range []bool{false, true} {
		b.Run(fmt.Sprintf("sync_current=%t", syncCurrent), func(b *testing.B) {
			dir := b.TempDir()
			writer, err := NewWriter(filepath.Join(dir, "000001.vlog"), 1)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = writer.Close() })
			beforeContentSyncs := writer.DurabilityStats().FileSyncCalls
			var namespaceSyncs uint64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				if _, err := writer.Append(0, nil, uint64(i+1), []byte("rotation-benchmark-value")); err != nil {
					b.Fatal(err)
				}
				closedID := writer.FileID()
				activeID := closedID + 1
				activeName := fmt.Sprintf("%06d.vlog", activeID)
				b.StartTimer()
				rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, activeName), activeID, syncCurrent,
					StableResourceRegistration{
						LogicalLane: "main", Generation: uint64(closedID), DiagnosticPath: filepath.Join("maindb", "value_vlog", fmt.Sprintf("%06d.vlog", closedID)),
						Reachability: rootpublication.ReachabilityValueLogPointer,
					},
					StableResourceRegistration{
						LogicalLane: "main", Generation: uint64(activeID), DiagnosticPath: filepath.Join("maindb", "value_vlog", activeName),
						Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: uint64(activeID),
						NamespaceOperation: rootpublication.NamespaceCreate,
					})
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
				if err := builder.Add(rotation.TakeClosed()); err != nil {
					rotation.Release()
					b.Fatal(err)
				}
				if err := builder.Add(rotation.TakeActive()); err != nil {
					rotation.Release()
					b.Fatal(err)
				}
				set, err := builder.Freeze()
				if err != nil {
					rotation.Release()
					b.Fatal(err)
				}
				rotation.Release()
				stats := set.Stats(time.Now())
				if len(stats) != 1 || stats[0].PendingCount != 2 || stats[0].NamespaceSyncs != 1 {
					set.Release()
					b.Fatalf("stable rotation operation counts=%+v", stats)
				}
				namespaceSyncs += stats[0].NamespaceSyncs
				set.Release()
				b.StartTimer()
			}
			contentSyncs := writer.DurabilityStats().FileSyncCalls - beforeContentSyncs
			wantContentSyncs := uint64(0)
			if syncCurrent {
				wantContentSyncs = uint64(b.N)
			}
			if namespaceSyncs != uint64(b.N) || contentSyncs != wantContentSyncs {
				b.Fatalf("rotation counters namespace=%d content=%d want namespace=%d content=%d", namespaceSyncs, contentSyncs, b.N, wantContentSyncs)
			}
			b.ReportMetric(float64(namespaceSyncs)/float64(b.N), "stable-token-namespace-sync/op")
			b.ReportMetric(float64(contentSyncs)/float64(b.N), "producer-content-sync/op")
		})
	}
}
