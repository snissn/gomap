//go:build darwin || linux || freebsd || netbsd || openbsd

package valuelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCertifyStableCreationNamespaceRetainsProofAfterCompletedSyncCut(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err := writer.RotateToWithSync(filepath.Join(dir, "000002.vlog"), 2, false); err != nil {
		t.Fatal(err)
	}
	if !writer.creationUncertified || writer.creationProof != nil {
		t.Fatalf("relaxed rotation state proof=%v uncertified=%t", writer.creationProof, writer.creationUncertified)
	}
	before := writer.DurabilityStats().DirectorySyncCalls
	injected := errors.New("injected after namespace sync")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Point == durabilitycut.AfterNewFileDirectorySync {
			return injected
		}
		return nil
	})
	err = writer.CertifyStableCreationNamespace()
	restore()
	if !errors.Is(err, injected) {
		t.Fatalf("certify error=%v want injected cut", err)
	}
	if writer.creationProof == nil || writer.creationUncertified {
		t.Fatalf("completed sync proof=%v uncertified=%t", writer.creationProof, writer.creationUncertified)
	}
	after := writer.DurabilityStats().DirectorySyncCalls
	if after != before+1 {
		t.Fatalf("directory syncs before=%d after=%d want +1", before, after)
	}
	if err := writer.CertifyStableCreationNamespace(); err != nil {
		t.Fatalf("certify retry: %v", err)
	}
	if got := writer.DurabilityStats().DirectorySyncCalls; got != after {
		t.Fatalf("certify retry repeated sync: before=%d after=%d", after, got)
	}
}

func TestOrdinaryRotationRefreshesStableParentBeforeStableRotation(t *testing.T) {
	root := t.TempDir()
	firstDir := filepath.Join(root, "first")
	secondDir := filepath.Join(root, "second")
	if err := os.Mkdir(firstDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(secondDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(filepath.Join(firstDir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = writer.Close()
		}
	})
	firstParent := writer.stableParent
	if _, err := writer.Append(0, nil, 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := writer.RotateToWithSync(filepath.Join(secondDir, "000002.vlog"), 2, false); err != nil {
		t.Fatal(err)
	}
	secondParent := writer.stableParent
	if _, err := writer.Append(0, nil, 2, []byte("second")); err != nil {
		t.Fatal(err)
	}

	// Move the current writer's parent away and replace its path. The later
	// stable rotation must follow the exact parent captured by the ordinary
	// A-to-B rotation, never the construction-time A parent or replacement B.
	movedSecondDir := filepath.Join(root, "second-moved")
	if err := os.Rename(secondDir, movedSecondDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(secondDir, 0o700); err != nil {
		t.Fatal(err)
	}
	rotation, err := writer.RotateToWithStableResources(filepath.Join(secondDir, "000003.vlog"), 3, false,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 3, DiagnosticPath: "maindb/value_vlog/000003.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 3,
			NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if err != nil {
		t.Fatal(err)
	}
	rotation.Release()
	if _, err := os.Stat(filepath.Join(movedSecondDir, "000003.vlog")); err != nil {
		t.Fatalf("stable successor missing from exact ordinary-rotation parent: %v", err)
	}
	for _, wrong := range []string{
		filepath.Join(firstDir, "000003.vlog"),
		filepath.Join(secondDir, "000003.vlog"),
	} {
		if _, err := os.Stat(wrong); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stable successor escaped to %q: %v", wrong, err)
		}
	}
	if firstParent == nil || secondParent == nil || firstParent == secondParent {
		t.Fatalf("ordinary rotation did not replace retained parent: first=%p second=%p", firstParent, secondParent)
	}
	if _, err := firstParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("construction parent remains retained after ordinary rotation: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	if _, err := secondParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("ordinary-rotation parent remains retained after writer close: %v", err)
	}
}

func TestOrdinaryRotationCaptureFailureInvalidatesStableParent(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	oldParent := writer.stableParent
	failedParent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected ordinary-rotation parent capture failure")
	originalOpenParent := openStableValueLogParent
	openStableValueLogParent = func(string) (*os.File, error) { return failedParent, injected }
	defer func() { openStableValueLogParent = originalOpenParent }()

	if err := writer.RotateToWithSync(filepath.Join(dir, "000002.vlog"), 2, false); err != nil {
		t.Fatalf("ordinary rotation must remain usable after stable capture failure: %v", err)
	}
	if writer.stableParent != nil || !errors.Is(writer.stableParentErr, injected) {
		t.Fatalf("stable parent=%v err=%v want explicit invalidation", writer.stableParent, writer.stableParentErr)
	}
	if _, err := oldParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("superseded parent remains retained after capture failure: %v", err)
	}
	if _, err := failedParent.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("failed replacement parent was not released: %v", err)
	}
	if _, err := writer.Append(0, nil, 2, []byte("ordinary-writer-remains-live")); err != nil {
		t.Fatal(err)
	}
	rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, "000003.vlog"), 3, false,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 3, DiagnosticPath: "maindb/value_vlog/000003.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 3,
			NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if !errors.Is(err, injected) || rotation != nil {
		if rotation != nil {
			rotation.Release()
		}
		t.Fatalf("stable rotation=(%v, %v) want nil and retained capture error", rotation, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "000003.vlog")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed-closed stable rotation exposed successor: %v", err)
	}
}

func TestOrdinaryRotationRetainedParentPlateau(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	for id := uint32(2); id <= 321; id++ {
		oldParent := writer.stableParent
		path := filepath.Join(dir, fmt.Sprintf("%06d.vlog", id))
		if err := writer.RotateToWithSync(path, id, false); err != nil {
			t.Fatalf("rotation %d: %v", id, err)
		}
		if writer.stableParent == nil || writer.stableParent == oldParent {
			t.Fatalf("rotation %d did not transfer retained-parent ownership", id)
		}
		if _, err := oldParent.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("rotation %d retained superseded parent: %v", id, err)
		}
	}
}

func TestStableWriterCreationProofBindsRepeatedlyWithoutResync(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if pending, err := writer.StableCreationNamespacePending(); err != nil || !pending {
		t.Fatalf("new writer creation pending=%v err=%v want true, nil", pending, err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("proof")); err != nil {
		t.Fatal(err)
	}
	if got := writer.DurabilityStats().DirectorySyncCalls; got != 1 {
		t.Fatalf("initial exact-parent syncs=%d want 1", got)
	}
	for generation := uint64(1); generation <= 4; generation++ {
		token, err := writer.StableResourceToken(StableResourceRegistration{
			LogicalLane: "main", Generation: generation, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: generation,
			NamespaceOperation: rootpublication.NamespaceCreate,
		})
		if err != nil {
			t.Fatal(err)
		}
		builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityValueLogPointer)
		if err := builder.Add(token); err != nil {
			token.Release()
			t.Fatal(err)
		}
		set, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		stats := set.Stats(time.Now())
		set.Release()
		if len(stats) != 1 || stats[0].NamespaceSyncs != 1 {
			t.Fatalf("capture %d namespace stats=%+v want one creation sync", generation, stats)
		}
	}
	if got := writer.DurabilityStats().DirectorySyncCalls; got != 1 {
		t.Fatalf("repeated capture resynced exact parent: calls=%d", got)
	}
}

func TestStableWriterCreationProofCannotBeOmitted(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	token, err := writer.StableResourceToken(StableResourceRegistration{
		LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
		Reachability: rootpublication.ReachabilityValueLogPointer,
	})
	if token != nil {
		token.Release()
		t.Fatal("newly-created stable writer returned a token without namespace evidence")
	}
	if !errors.Is(err, rootpublication.ErrNamespaceUnstable) {
		t.Fatalf("omitted create evidence error=%v want ErrNamespaceUnstable", err)
	}
}

func TestStableWriterExistingOpenCannotClaimCreateProof(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.vlog")
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriterWithStableResourcePinRegistry(path, 1, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if pending, err := writer.StableCreationNamespacePending(); err != nil || pending {
		t.Fatalf("existing writer creation pending=%v err=%v want false, nil", pending, err)
	}
	token, err := writer.StableResourceToken(StableResourceRegistration{
		LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
		Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 1,
		NamespaceOperation: rootpublication.NamespaceCreate,
	})
	if token != nil {
		token.Release()
		t.Fatal("existing open returned create evidence")
	}
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("existing create capture error=%v want ErrUnresolvedResource", err)
	}
}

func TestStableRegistrySyncedOrdinaryRotationTransfersCreationProof(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	before := writer.DurabilityStats().DirectorySyncCalls
	if err := writer.RotateToWithSync(filepath.Join(dir, "000002.vlog"), 2, true); err != nil {
		t.Fatal(err)
	}
	if writer.creationProof == nil || writer.creationUncertified {
		t.Fatalf("synced ordinary rotation proof=%v uncertified=%t", writer.creationProof, writer.creationUncertified)
	}
	if got := writer.DurabilityStats().DirectorySyncCalls; got != before+1 {
		t.Fatalf("synced ordinary rotation namespace syncs=%d want %d", got, before+1)
	}
	token, err := writer.StableResourceToken(StableResourceRegistration{
		LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
		Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 2,
		NamespaceOperation: rootpublication.NamespaceCreate,
	})
	if err != nil {
		t.Fatal(err)
	}
	token.Release()
	if got := writer.DurabilityStats().DirectorySyncCalls; got != before+1 {
		t.Fatalf("capture after synced ordinary rotation resynced namespace: calls=%d", got)
	}
}

func TestStableRotationTransfersCreationProofAcrossFourSegments(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	baselineFDs, checkFDs := valueLogOpenDescriptorCount(t)
	for activeID := uint32(2); activeID <= 5; activeID++ {
		if _, err := writer.Append(0, nil, uint64(activeID), []byte("rotate")); err != nil {
			t.Fatal(err)
		}
		closedID := activeID - 1
		rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, fmt.Sprintf("%06d.vlog", activeID)), activeID, false,
			StableResourceRegistration{LogicalLane: "main", Generation: uint64(closedID), DiagnosticPath: fmt.Sprintf("maindb/value_vlog/%06d.vlog", closedID), Reachability: rootpublication.ReachabilityValueLogPointer},
			StableResourceRegistration{LogicalLane: "main", Generation: uint64(activeID), DiagnosticPath: fmt.Sprintf("maindb/value_vlog/%06d.vlog", activeID), Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: uint64(activeID), NamespaceOperation: rootpublication.NamespaceCreate})
		if err != nil {
			t.Fatalf("rotation %d: %v", activeID, err)
		}
		if writer.creationProof == nil || writer.creationUncertified {
			rotation.Release()
			t.Fatalf("rotation %d did not transfer active creation proof", activeID)
		}
		rotation.Release()
	}
	if got := writer.DurabilityStats().DirectorySyncCalls; got != 5 {
		t.Fatalf("initial plus four rotation namespace syncs=%d want 5", got)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("released rotations retain %d pins", got)
	}
	if got := registry.ActiveIdentities(); got != 1 {
		t.Fatalf("four rotations retain %d observed identities want 1", got)
	}
	if got, ok := valueLogOpenDescriptorCount(t); checkFDs && ok && got > baselineFDs+1 {
		t.Fatalf("four rotations grew retained descriptors: baseline=%d current=%d", baselineFDs, got)
	}
}

func TestStableValueLogRenameUsesCallerCapturedParent(t *testing.T) {
	testStableValueLogRenameUsesCallerCapturedParent(t)
}

func TestStableValueLogRotationCreatesThroughCapturedParent(t *testing.T) {
	testStableValueLogRotationCreatesThroughCapturedParent(t)
}

func TestStableValueLogRotationUsesParentRefreshedByOrdinaryRotation(t *testing.T) {
	testStableValueLogRotationUsesParentRefreshedByOrdinaryRotation(t)
}

func TestRotateToWithSyncMarksInstalledParentCloseFailure(t *testing.T) {
	testRotateToWithSyncMarksInstalledParentCloseFailure(t)
}

func TestRotateToWithStableResourcesRetainsClosedAndActiveIdentities(t *testing.T) {
	testRotateToWithStableResourcesRetainsClosedAndActiveIdentities(t)
}

func TestRotateToWithStableResourcesOwnsRegistryObservations(t *testing.T) {
	testRotateToWithStableResourcesOwnsRegistryObservations(t)
}

func TestStableRotationExistingClosedSegmentHasNoCreationWitness(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	if err := os.WriteFile(firstPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(firstPath, 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if pending, pendingErr := writer.StableCreationNamespacePending(); pendingErr != nil || pending {
		t.Fatalf("existing writer creation pending=%t err=%v want false, nil", pending, pendingErr)
	}
	rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, "000002.vlog"), 2, false,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 1,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 1,
			NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if rotation.Closed.Namespace() != nil {
		t.Fatal("existing closed segment fabricated a creation witness")
	}
	if rotation.Active.Namespace() == nil || rotation.Active.Namespace().Operation() != rootpublication.NamespaceCreate {
		t.Fatal("new active segment is missing its creation witness")
	}
}

func TestStableRotationRejectsRenameBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(firstPath, 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append(0, nil, 1, []byte("before rejected rename")); err != nil {
		t.Fatal(err)
	}
	beforeStats := writer.DurabilityStats()
	beforePins := registry.ActivePins()
	beforeIdentities := registry.ActiveIdentities()

	rotation, err := writer.RotateToWithStableResources(secondPath, 2, true,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 1,
			NamespaceOperation: rootpublication.NamespaceRename,
			OldName:            filepath.Base(firstPath),
			NewName:            filepath.Base(secondPath),
		})
	if rotation != nil {
		rotation.Release()
		t.Fatal("rejected rename returned owned resources")
	}
	if !errors.Is(err, rootpublication.ErrNamespaceUnstable) {
		t.Fatalf("stable rename rotation error=%v want ErrNamespaceUnstable", err)
	}
	if _, err := os.Stat(secondPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected rename exposed successor: %v", err)
	}
	if writer.FileID() != 1 || writer.f == nil || writer.f.Name() != firstPath {
		t.Fatalf("rejected rename changed active writer: id=%d file=%v", writer.FileID(), writer.f)
	}
	if got := writer.DurabilityStats(); got != beforeStats {
		t.Fatalf("rejected rename performed durability work: before=%+v after=%+v", beforeStats, got)
	}
	if got := registry.ActivePins(); got != beforePins {
		t.Fatalf("rejected rename changed active pins: before=%d after=%d", beforePins, got)
	}
	if got := registry.ActiveIdentities(); got != beforeIdentities {
		t.Fatalf("rejected rename changed observed identities: before=%d after=%d", beforeIdentities, got)
	}
	if _, err := writer.Append(0, nil, 2, []byte("old writer remains usable")); err != nil {
		t.Fatalf("append after rejected rename: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush after rejected rename: %v", err)
	}
}

func TestStableValueLogRotationNamespaceFailureKeepsOldWriterActive(t *testing.T) {
	testStableValueLogRotationNamespaceFailureKeepsOldWriterActive(t)
}

func TestStableValueLogRollbackDoesNotUnlinkReplacement(t *testing.T) {
	testStableValueLogRollbackDoesNotUnlinkReplacement(t)
}

func BenchmarkStableValueLogRotation(b *testing.B) {
	benchmarkStableValueLogRotation(b)
}
