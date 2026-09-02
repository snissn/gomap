package valuelog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestStableExistingPhysicalResourceTokenRejectsDiagnosticPathWithoutParentNamespace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.StableExistingPhysicalResourceToken(fileID, rootpublication.StableResourceSpec{
		DiagnosticPath: "value-l0-000001.log",
	}, func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
		t.Fatal("constructor called for a resource with no parent namespace")
		return nil, nil
	})
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("base-only diagnostic path error=%v want ErrUnresolvedResource", err)
	}
}

func TestOrdinaryOuterLeafCreationClassifiesFallbackDirectorySync(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "leaf_vlog")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "000001.vlog")
	var outerCreates, valueSyncs, outerBeforeSyncs, outerAfterSyncs int
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch {
		case event.Namespace == durabilitycut.NamespaceCreate && event.NewPath == path && event.Resource == durabilitycut.ResourceOuterLeaf:
			outerCreates++
		case event.Point == durabilitycut.BeforeNewFileDirectorySync && event.Resource == durabilitycut.ResourceOuterLeaf:
			outerBeforeSyncs++
		case event.Point == durabilitycut.AfterNewFileDirectorySync && event.Resource == durabilitycut.ResourceOuterLeaf:
			outerAfterSyncs++
		case (event.Point == durabilitycut.BeforeNewFileDirectorySync || event.Point == durabilitycut.AfterNewFileDirectorySync) && event.Resource == durabilitycut.ResourceValueLog:
			valueSyncs++
		}
		return nil
	})
	defer restore()

	writer, err := NewWriter(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if outerCreates != 1 || outerBeforeSyncs != 1 || outerAfterSyncs != 1 || valueSyncs != 0 {
		t.Fatalf("ordinary outer-leaf creation cuts create=%d directory(before/after)=%d/%d value-sync=%d want 1, 1/1, 0",
			outerCreates, outerBeforeSyncs, outerAfterSyncs, valueSyncs)
	}
}

func testStableValueLogRenameUsesCallerCapturedParent(t *testing.T) {
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

func testStableValueLogRotationCreatesThroughCapturedParent(t *testing.T) {
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
	if err := os.Rename(segmentDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(segmentDir, 0o700); err != nil {
		t.Fatal(err)
	}
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

func testStableValueLogRotationUsesParentRefreshedByOrdinaryRotation(t *testing.T) {
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

	movedDir := filepath.Join(root, "segments-moved")
	if err := os.Rename(segmentDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(segmentDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writer.RotateToWithSync(filepath.Join(segmentDir, "000002.vlog"), 2, false); err != nil {
		t.Fatal(err)
	}
	if writer.fileID != 2 || filepath.Base(writer.f.Name()) != "000002.vlog" {
		t.Fatalf("installed writer state fileID=%d path=%q", writer.fileID, writer.f.Name())
	}
	rotation, err := writer.RotateToWithStableResources(filepath.Join(segmentDir, "000003.vlog"), 3, false,
		StableResourceRegistration{LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog", Reachability: rootpublication.ReachabilityValueLogPointer},
		StableResourceRegistration{LogicalLane: "main", Generation: 3, DiagnosticPath: "maindb/value_vlog/000003.vlog", Reachability: rootpublication.ReachabilityValueLogPointer,
			ParentGeneration: 3, NamespaceOperation: rootpublication.NamespaceCreate})
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if _, err := os.Stat(filepath.Join(segmentDir, "000003.vlog")); err != nil {
		t.Fatalf("refreshed-parent active segment: %v", err)
	}
	if _, err := os.Stat(filepath.Join(movedDir, "000003.vlog")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale retained parent unexpectedly received active segment: %v", err)
	}
}

func testRotateToWithSyncMarksInstalledParentCloseFailure(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	oldParent := writer.stableParent
	cleanupErr := errors.New("test: close superseded parent")
	writer.closeRotateFn = func(file *os.File) error {
		if file == oldParent {
			return cleanupErr
		}
		return file.Close()
	}
	err = writer.RotateToWithSync(filepath.Join(dir, "000002.vlog"), 2, false)
	writer.closeRotateFn = nil
	defer oldParent.Close()
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("RotateToWithSync error=%v want cleanup error", err)
	}
	if !RotationInstalled(err) {
		t.Fatalf("RotationInstalled(%v)=false want true", err)
	}
	if writer.fileID != 2 || filepath.Base(writer.f.Name()) != "000002.vlog" {
		t.Fatalf("installed writer state fileID=%d path=%q", writer.fileID, writer.f.Name())
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

func TestStableRegistryRelaxedOrdinaryRotationDoesNotSyncOrClaimCreate(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	before := writer.DurabilityStats().DirectorySyncCalls
	if err := writer.RotateToWithSync(filepath.Join(dir, "000002.vlog"), 2, false); err != nil {
		t.Fatal(err)
	}
	if after := writer.DurabilityStats().DirectorySyncCalls; after != before {
		t.Fatalf("relaxed ordinary rotation namespace syncs: before=%d after=%d", before, after)
	}
	if writer.creationProof != nil || !writer.creationUncertified {
		t.Fatalf("relaxed successor proof=%v uncertified=%t, want nil/true", writer.creationProof, writer.creationUncertified)
	}
	token, err := writer.StableResourceToken(StableResourceRegistration{
		LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
		Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 2,
		NamespaceOperation: rootpublication.NamespaceCreate,
	})
	if token != nil {
		token.Release()
		t.Fatal("uncertified relaxed successor returned a create token")
	}
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("uncertified relaxed successor error=%v want ErrUnresolvedResource", err)
	}
	if after := writer.DurabilityStats().DirectorySyncCalls; after != before {
		t.Fatalf("failed late certification added namespace syncs: before=%d after=%d", before, after)
	}
	rotation, rotateErr := writer.RotateToWithStableResources(filepath.Join(dir, "000003.vlog"), 3, false,
		StableResourceRegistration{LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog", Reachability: rootpublication.ReachabilityValueLogPointer},
		StableResourceRegistration{LogicalLane: "main", Generation: 3, DiagnosticPath: "maindb/value_vlog/000003.vlog", Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 3, NamespaceOperation: rootpublication.NamespaceCreate})
	if rotation != nil {
		rotation.Release()
		t.Fatal("uncertified relaxed successor entered a stable rotation")
	}
	if !errors.Is(rotateErr, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("uncertified stable rotation error=%v want ErrUnresolvedResource", rotateErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "000003.vlog")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("uncertified stable rotation exposed successor: %v", statErr)
	}
	if after := writer.DurabilityStats().DirectorySyncCalls; after != before {
		t.Fatalf("failed stable rotation added namespace syncs: before=%d after=%d", before, after)
	}
}

func testRotateToWithStableResourcesRetainsClosedAndActiveIdentities(t *testing.T) {
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

func testRotateToWithStableResourcesOwnsRegistryObservations(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	beforeDirectorySyncs := writer.DurabilityStats().DirectorySyncCalls
	rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, "000002.vlog"), 2, true,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, PinRegistry: registry,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, PinRegistry: registry,
			ParentGeneration: 1, NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if got := registry.ActivePins(); got != 2 {
		t.Fatalf("rotation active pins=%d want 2", got)
	}
	if rotation.Closed.Namespace() == nil || rotation.Closed.Namespace().Operation() != rootpublication.NamespaceCreate {
		t.Fatal("freshly-created closed segment lost its retained creation proof")
	}
	if rotation.Active.Namespace() == nil || rotation.Active.Namespace().Operation() != rootpublication.NamespaceCreate {
		t.Fatal("freshly-created active segment lost its creation proof")
	}
	if got := writer.DurabilityStats().DirectorySyncCalls; got != beforeDirectorySyncs+1 {
		t.Fatalf("stable rotation directory syncs=%d want %d; binding the closed proof must not resync", got, beforeDirectorySyncs+1)
	}
	rotation.Release()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("rotation active pins after release=%d want 0", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("writer registry retained %d identities after close", got)
	}
}

func TestWriterRejectsLateRegistryInjection(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	_, err = writer.StableResourceToken(StableResourceRegistration{
		LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
		Reachability: rootpublication.ReachabilityValueLogPointer,
		PinRegistry:  rootpublication.NewIdentityPinRegistry(),
	})
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("late registry injection error=%v want ErrUnresolvedResource", err)
	}
}

func testStableValueLogRotationNamespaceFailureKeepsOldWriterActive(t *testing.T) {
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
	originalFactory := bindValueLogStableNamespaceCreationProof
	bindValueLogStableNamespaceCreationProof = func(*rootpublication.StableNamespaceCreationProof, *os.File, uint64, string, string) (*rootpublication.StableNamespaceToken, error) {
		return nil, injected
	}
	defer func() { bindValueLogStableNamespaceCreationProof = originalFactory }()
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

func testStableValueLogRollbackDoesNotUnlinkReplacement(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	failedPath := filepath.Join(dir, "000002.vlog")
	displacedPath := filepath.Join(dir, "000002-displaced.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	injected := errors.New("injected namespace failure after replacement")
	originalFactory := bindValueLogStableNamespaceCreationProof
	bindValueLogStableNamespaceCreationProof = func(*rootpublication.StableNamespaceCreationProof, *os.File, uint64, string, string) (*rootpublication.StableNamespaceToken, error) {
		if err := os.Rename(failedPath, displacedPath); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(failedPath, []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
		return nil, injected
	}
	defer func() { bindValueLogStableNamespaceCreationProof = originalFactory }()

	rotation, err := writer.RotateToWithStableResources(failedPath, 2, false,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 1,
			NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) || !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("rotation error=%v want injected failure and resource conflict", err)
	}
	if got, err := os.ReadFile(failedPath); err != nil || string(got) != "replacement" {
		t.Fatalf("replacement changed during rollback: data=%q err=%v", got, err)
	}
	if _, err := os.Stat(displacedPath); err != nil {
		t.Fatalf("prepared successor was not retained for diagnosis: %v", err)
	}
	if writer.FileID() != 1 || writer.f == nil || writer.f.Name() != firstPath {
		t.Fatalf("failed rotation changed active writer: id=%d file=%v", writer.FileID(), writer.f)
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

func TestCaptureStableExternalRIDFenceRequiresEveryManagerChild(t *testing.T) {
	dir := t.TempDir()
	segmentRIDs := [][]uint64{{9, 2}, {14, 4}}
	segmentPointers := make([][]page.ValuePtr, len(segmentRIDs))
	fileIDs := make([]uint32, 2)
	for i := range fileIDs {
		fileID, err := EncodeFileID(1, uint32(i+1))
		if err != nil {
			t.Fatal(err)
		}
		fileIDs[i] = fileID
		writer, err := NewWriter(SegmentPath(dir, fileID), fileID)
		if err != nil {
			t.Fatal(err)
		}
		for _, rid := range segmentRIDs[i] {
			ptr, err := writer.Append(0, nil, rid, []byte("external-rid-child"))
			if err != nil {
				_ = writer.Close()
				t.Fatal(err)
			}
			segmentPointers[i] = append(segmentPointers[i], ptr)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	registry := rootpublication.NewIdentityPinRegistry()
	manager, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	children := []StableExternalRIDSegment{
		{FileID: fileIDs[0], RIDs: []uint64{9, 2, 9}, Pointers: []page.ValuePtr{segmentPointers[0][0], segmentPointers[0][1], segmentPointers[0][0]}, Digest: sha256.Sum256([]byte("segment-1"))},
		{FileID: fileIDs[1], RIDs: []uint64{14, 4}, Pointers: segmentPointers[1], Digest: sha256.Sum256([]byte("segment-2"))},
	}
	fence, err := NewStableExternalRIDFence([]uint64{14, 9, 4, 2, 9})
	if err != nil {
		t.Fatal(err)
	}
	resources, err := manager.CaptureStableExternalRIDFence(fence, children)
	if err != nil {
		t.Fatal(err)
	}
	descriptors := resources.Descriptors()
	if len(descriptors) != len(children) {
		t.Fatalf("external-RID descriptors=%d want %d", len(descriptors), len(children))
	}
	for i, descriptor := range descriptors {
		if descriptor.Generation() != uint64(children[i].FileID) || descriptor.Kind() != rootpublication.ResourceCommandWALExternalRID {
			t.Fatalf("external-RID descriptor[%d]=%+v", i, descriptor)
		}
	}
	resources.Release()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("external-RID pins after release=%d want 0", got)
	}

	t.Run("current-writable barrier runs outside manager lock", func(t *testing.T) {
		if err := manager.PromoteCurrentWritable(fileIDs[0]); err != nil {
			t.Fatal(err)
		}
		manager.mu.RLock()
		managed := manager.files[fileIDs[0]]
		manager.mu.RUnlock()
		managed.verifiedFileSize.Store(0)
		barrierCalls := 0
		manager.SetCurrentWritableReadBarrier(func(uint32) error {
			if !manager.mu.TryLock() {
				return errors.New("manager lock held across current-writable barrier")
			}
			manager.mu.Unlock()
			barrierCalls++
			return nil
		})
		defer manager.SetCurrentWritableReadBarrier(nil)
		resources, err := manager.CaptureStableExternalRIDFence(fence, children)
		if err != nil {
			t.Fatal(err)
		}
		resources.Release()
		if barrierCalls == 0 {
			t.Fatal("current-writable membership validation did not invoke barrier")
		}
		if got := registry.ActivePins(); got != 0 {
			t.Fatalf("external-RID pins after current-writable capture=%d want 0", got)
		}
	})

	t.Run("RID assigned to wrong manager child", func(t *testing.T) {
		misassigned := append([]StableExternalRIDSegment(nil), children...)
		misassigned[0].RIDs = []uint64{9, 4, 9}
		misassigned[1].RIDs = []uint64{14, 2}
		resources, err := manager.CaptureStableExternalRIDFence(fence, misassigned)
		if resources != nil {
			resources.Release()
		}
		if !errors.Is(err, rootpublication.ErrResourceConflict) || !strings.Contains(err.Error(), "does not belong") {
			t.Fatalf("misassigned external RID resources=%v err=%v want ownership conflict", resources, err)
		}
		if got := registry.ActivePins(); got != 0 {
			t.Fatalf("external-RID pins after ownership conflict=%d want 0", got)
		}
	})

	for omitted := range children {
		t.Run(fmt.Sprintf("segment-%d", omitted), func(t *testing.T) {
			missing := append([]StableExternalRIDSegment(nil), children[:omitted]...)
			missing = append(missing, children[omitted+1:]...)
			if resources, err := manager.CaptureStableExternalRIDFence(fence, missing); !errors.Is(err, rootpublication.ErrUnresolvedResource) || resources != nil {
				if resources != nil {
					resources.Release()
				}
				t.Fatalf("omitted external-RID segment %d resources=%v err=%v want ErrUnresolvedResource", omitted, resources, err)
			}
			if got := registry.ActivePins(); got != 0 {
				t.Fatalf("external-RID pins after omitted segment %d=%d want 0", omitted, got)
			}
		})
	}
	for childIndex := range children {
		for ridIndex := range children[childIndex].RIDs {
			t.Run(fmt.Sprintf("segment-%d-rid-%d", childIndex, ridIndex), func(t *testing.T) {
				missing := append([]StableExternalRIDSegment(nil), children...)
				missing[childIndex].RIDs = append([]uint64(nil), children[childIndex].RIDs...)
				missing[childIndex].RIDs = append(missing[childIndex].RIDs[:ridIndex], missing[childIndex].RIDs[ridIndex+1:]...)
				missing[childIndex].Pointers = append([]page.ValuePtr(nil), children[childIndex].Pointers...)
				missing[childIndex].Pointers = append(missing[childIndex].Pointers[:ridIndex], missing[childIndex].Pointers[ridIndex+1:]...)
				resources, err := manager.CaptureStableExternalRIDFence(fence, missing)
				// A duplicate occurrence is not an omitted logical RID; the unique
				// fence remains complete and must still capture successfully.
				if children[childIndex].RIDs[ridIndex] == 9 && len(missing[childIndex].RIDs) == 2 {
					if err != nil || resources == nil {
						t.Fatalf("deduplicated RID occurrence resources=%v err=%v", resources, err)
					}
					resources.Release()
				} else if !errors.Is(err, rootpublication.ErrUnresolvedResource) || resources != nil {
					if resources != nil {
						resources.Release()
					}
					t.Fatalf("omitted external RID child=%d rid=%d resources=%v err=%v want ErrUnresolvedResource", childIndex, ridIndex, resources, err)
				}
				if got := registry.ActivePins(); got != 0 {
					t.Fatalf("external-RID pins after RID omission child=%d rid=%d: %d", childIndex, ridIndex, got)
				}
			})
		}
	}

	for omitted := range children {
		missing := append([]StableExternalRIDSegment(nil), children...)
		missingID, err := EncodeFileID(2, uint32(omitted+1))
		if err != nil {
			t.Fatal(err)
		}
		missing[omitted].FileID = missingID
		if resources, err := manager.CaptureStableExternalRIDFence(fence, missing); err == nil || resources != nil || !strings.Contains(err.Error(), fmt.Sprintf("child %d", missingID)) {
			if resources != nil {
				resources.Release()
			}
			t.Fatalf("omitted external-RID child %d resources=%v err=%v", omitted, resources, err)
		}
		if got := registry.ActivePins(); got != 0 {
			t.Fatalf("external-RID pins after omitted child %d=%d want 0", omitted, got)
		}
	}
}

func BenchmarkStableValueLogExternalRIDFenceClosure(b *testing.B) {
	dir := b.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	segmentRIDs := [][]uint64{{1, 101}, {2, 102}}
	segmentPointers := make([][]page.ValuePtr, len(segmentRIDs))
	fileIDs := make([]uint32, 2)
	for i := 0; i < 2; i++ {
		fileID, err := EncodeFileID(1, uint32(i+1))
		if err != nil {
			b.Fatal(err)
		}
		fileIDs[i] = fileID
		writer, err := NewWriter(SegmentPath(dir, fileID), fileID)
		if err != nil {
			b.Fatal(err)
		}
		for _, rid := range segmentRIDs[i] {
			ptr, err := writer.Append(0, nil, rid, []byte("external-rid-record"))
			if err != nil {
				_ = writer.Close()
				b.Fatal(err)
			}
			segmentPointers[i] = append(segmentPointers[i], ptr)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
	manager, err := NewManagerWithStableResourcePinRegistry(dir, registry)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = manager.Close() })
	children := []StableExternalRIDSegment{
		{FileID: fileIDs[0], RIDs: []uint64{1, 101, 1}, Pointers: []page.ValuePtr{segmentPointers[0][0], segmentPointers[0][1], segmentPointers[0][0]}, Digest: sha256.Sum256([]byte("external-rid-segment-1"))},
		{FileID: fileIDs[1], RIDs: []uint64{2, 102, 2}, Pointers: []page.ValuePtr{segmentPointers[1][0], segmentPointers[1][1], segmentPointers[1][0]}, Digest: sha256.Sum256([]byte("external-rid-segment-2"))},
	}
	fence, err := NewStableExternalRIDFence([]uint64{102, 1, 2, 101})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var descriptors, obligations, contentSyncs, namespaceSyncs uint64
	var pinHighWater uint64
	for i := 0; i < b.N; i++ {
		resources, err := manager.CaptureStableExternalRIDFence(fence, children)
		if err != nil {
			b.Fatal(err)
		}
		descriptors += uint64(len(resources.Descriptors()))
		for _, descriptor := range resources.Descriptors() {
			obligations += uint64(len(descriptor.LogicalObligations()))
		}
		for _, stats := range resources.Stats(time.Now()) {
			contentSyncs += stats.Syncs
			namespaceSyncs += stats.NamespaceSyncs
			if stats.PinHighWater > pinHighWater {
				pinHighWater = stats.PinHighWater
			}
		}
		resources.Release()
	}
	b.StopTimer()
	if got := registry.ActivePins(); got != 0 {
		b.Fatalf("active external-RID pins after release=%d want 0", got)
	}
	b.ReportMetric(float64(descriptors)/float64(b.N), "descriptors/op")
	b.ReportMetric(float64(obligations)/float64(b.N), "logical_obligations/op")
	b.ReportMetric(float64(pinHighWater), "pin_high_water")
	b.ReportMetric(float64(contentSyncs)/float64(b.N), "content_syncs/op")
	b.ReportMetric(float64(namespaceSyncs)/float64(b.N), "namespace_syncs/op")
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

func benchmarkStableValueLogRotation(b *testing.B) {
	for _, withRegistry := range []bool{false, true} {
		b.Run(fmt.Sprintf("with_registry=%t", withRegistry), func(b *testing.B) {
			for _, syncCurrent := range []bool{false, true} {
				b.Run(fmt.Sprintf("sync_current=%t", syncCurrent), func(b *testing.B) {
					dir := b.TempDir()
					var registry *rootpublication.IdentityPinRegistry
					var writer *Writer
					var err error
					if withRegistry {
						registry = rootpublication.NewIdentityPinRegistry()
						writer, err = NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
					} else {
						writer, err = NewWriter(filepath.Join(dir, "000001.vlog"), 1)
					}
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { _ = writer.Close() })
					beforeContentSyncs := writer.DurabilityStats().FileSyncCalls
					beforeDirectorySyncs := writer.DurabilityStats().DirectorySyncCalls
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
						// A writer opened without stable capture has no creation witness for
						// its initial segment. Every later closed segment was created by the
						// preceding stable rotation, so it retains that witness alongside the
						// newly active segment. Registry-backed construction captures the
						// initial segment as well.
						wantNamespaceSyncs := uint64(2)
						if !withRegistry && i == 0 {
							wantNamespaceSyncs = 1
						}
						if len(stats) != 1 || stats[0].PendingCount != 2 || stats[0].NamespaceSyncs != wantNamespaceSyncs {
							set.Release()
							b.Fatalf("stable rotation operation counts=%+v", stats)
						}
						namespaceSyncs += stats[0].NamespaceSyncs
						set.Release()
						b.StartTimer()
					}
					contentSyncs := writer.DurabilityStats().FileSyncCalls - beforeContentSyncs
					physicalNamespaceSyncs := writer.DurabilityStats().DirectorySyncCalls - beforeDirectorySyncs
					wantContentSyncs := uint64(0)
					if syncCurrent {
						wantContentSyncs = uint64(b.N)
					}
					wantNamespaceSyncs := uint64(2 * b.N)
					if !withRegistry {
						wantNamespaceSyncs--
					}
					if namespaceSyncs != wantNamespaceSyncs || physicalNamespaceSyncs != uint64(b.N) || contentSyncs != wantContentSyncs {
						b.Fatalf("rotation counters witnesses=%d physical_namespace=%d content=%d want witnesses=%d physical_namespace=%d content=%d", namespaceSyncs, physicalNamespaceSyncs, contentSyncs, wantNamespaceSyncs, b.N, wantContentSyncs)
					}
					if registry != nil && (registry.ActivePins() != 0 || registry.ActiveIdentities() != 1) {
						b.Fatalf("rotation registry pins=%d identities=%d, want 0 pins and one active writer", registry.ActivePins(), registry.ActiveIdentities())
					}
					b.ReportMetric(float64(namespaceSyncs)/float64(b.N), "stable-token-namespace-sync/op")
					b.ReportMetric(float64(physicalNamespaceSyncs)/float64(b.N), "producer-namespace-sync/op")
					b.ReportMetric(float64(contentSyncs)/float64(b.N), "producer-content-sync/op")
				})
			}
		})
	}
}
