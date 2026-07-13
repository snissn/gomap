//go:build darwin || linux || freebsd || netbsd || openbsd

package valuelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

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

func TestStableValueLogRotationNamespaceFailureKeepsOldWriterActive(t *testing.T) {
	testStableValueLogRotationNamespaceFailureKeepsOldWriterActive(t)
}

func TestStableValueLogRollbackDoesNotUnlinkReplacement(t *testing.T) {
	testStableValueLogRollbackDoesNotUnlinkReplacement(t)
}

func BenchmarkStableValueLogRotation(b *testing.B) {
	benchmarkStableValueLogRotation(b)
}
