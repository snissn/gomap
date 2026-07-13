//go:build darwin || linux || freebsd || netbsd || openbsd

package valuelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func stablePendingRegistrations(closedID, activeID uint32) (StableResourceRegistration, StableResourceRegistration) {
	return StableResourceRegistration{
			LogicalLane: "main", Generation: uint64(closedID),
			DiagnosticPath: fmt.Sprintf("maindb/value_vlog/%06d.vlog", closedID),
			Reachability:   rootpublication.ReachabilityValueLogPointer,
		}, StableResourceRegistration{
			LogicalLane: "main", Generation: uint64(activeID),
			DiagnosticPath:     fmt.Sprintf("maindb/value_vlog/%06d.vlog", activeID),
			Reachability:       rootpublication.ReachabilityValueLogPointer,
			ParentGeneration:   uint64(activeID),
			NamespaceOperation: rootpublication.NamespaceCreate,
		}
}

func appendStablePendingValue(t *testing.T, writer *Writer, rid uint64) {
	t.Helper()
	if _, err := writer.Append(0, nil, rid, []byte("stable-pending-value")); err != nil {
		t.Fatal(err)
	}
}

func valueLogOpenDescriptorCount(t *testing.T) (int, bool) {
	t.Helper()
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

func TestStableValueLogObserverFailureRetriesExactPendingSuccessor(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	oldFile := writer.f

	injected := errors.New("injected value-log create observer failure")
	createCalls := 0
	var createdInfo os.FileInfo
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceValueLog || event.Namespace != durabilitycut.NamespaceCreate {
			return nil
		}
		createCalls++
		if createCalls == 1 {
			var statErr error
			createdInfo, statErr = os.Stat(event.NewPath)
			if statErr != nil {
				t.Fatalf("stat created successor inside observer: %v", statErr)
			}
			return injected
		}
		return nil
	})
	defer restore()

	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want observer failure", err)
	}
	if writer.f != oldFile || writer.FileID() != 1 {
		t.Fatalf("observer failure changed old authority: file=%p/%p id=%d", writer.f, oldFile, writer.FileID())
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	defer rotation.Release()
	if createCalls != 1 {
		t.Fatalf("create observer calls=%d want 1", createCalls)
	}
	installedInfo, err := writer.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if createdInfo == nil || !os.SameFile(createdInfo, installedInfo) {
		t.Fatal("retry did not install the exact successor created before observer failure")
	}
}

func TestStableValueLogPendingSuccessorRejectsOrdinaryAndMismatchedRotation(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*Writer, string, StableResourceRegistration, StableResourceRegistration) error
	}{
		{name: "ordinary", run: func(writer *Writer, path string, _, _ StableResourceRegistration) error {
			return writer.RotateToWithSync(path, 2, false)
		}},
		{name: "mismatched-path", run: func(writer *Writer, _ string, closed, active StableResourceRegistration) error {
			rotation, err := writer.RotateToWithStableResources(filepath.Join(filepath.Dir(writer.f.Name()), "000003.vlog"), 3, false, closed, active)
			if rotation != nil {
				rotation.Release()
			}
			return err
		}},
		{name: "mismatched-active-registration", run: func(writer *Writer, path string, closed, active StableResourceRegistration) error {
			active.DiagnosticPath = "maindb/value_vlog/mismatched.vlog"
			rotation, err := writer.RotateToWithStableResources(path, 2, false, closed, active)
			if rotation != nil {
				rotation.Release()
			}
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			firstPath := filepath.Join(dir, "000001.vlog")
			secondPath := filepath.Join(dir, "000002.vlog")
			writer, err := NewWriter(firstPath, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			appendStablePendingValue(t, writer, 1)
			closed, active := stablePendingRegistrations(1, 2)
			oldFile := writer.f

			injected := errors.New("injected value-log namespace-token failure")
			originalFactory := newValueLogStableNamespaceToken
			newValueLogStableNamespaceToken = func(rootpublication.StableNamespaceSpec) (*rootpublication.StableNamespaceToken, error) {
				return nil, injected
			}
			rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
			newValueLogStableNamespaceToken = originalFactory
			if rotation != nil {
				rotation.Release()
				t.Fatal("failed rotation returned owned resources")
			}
			if !errors.Is(err, injected) {
				t.Fatalf("first rotation error=%v want token failure", err)
			}
			if err := tc.run(writer, secondPath, closed, active); !errors.Is(err, rootpublication.ErrResourceOwnership) {
				t.Fatalf("competing rotation error=%v want ErrResourceOwnership", err)
			}
			if writer.f != oldFile || writer.FileID() != 1 {
				t.Fatalf("competing rotation changed authority: file=%p/%p id=%d", writer.f, oldFile, writer.FileID())
			}
		})
	}
}

func TestStableValueLogTokenFailureRetriesExactPendingSuccessor(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)

	createCalls := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Namespace == durabilitycut.NamespaceCreate {
			createCalls++
		}
		return nil
	})
	defer restore()
	injected := errors.New("injected value-log namespace-token failure")
	originalFactory := newValueLogStableNamespaceToken
	factoryCalls := 0
	newValueLogStableNamespaceToken = func(spec rootpublication.StableNamespaceSpec) (*rootpublication.StableNamespaceToken, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return nil, injected
		}
		return originalFactory(spec)
	}
	defer func() { newValueLogStableNamespaceToken = originalFactory }()

	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want token failure", err)
	}
	createdInfo, err := os.Stat(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	defer rotation.Release()
	installedInfo, err := writer.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if createCalls != 1 || factoryCalls != 2 || !os.SameFile(createdInfo, installedInfo) {
		t.Fatalf("retry createCalls=%d factoryCalls=%d sameFile=%t, want 1/2/true",
			createCalls, factoryCalls, os.SameFile(createdInfo, installedInfo))
	}
}

func TestStableValueLogPendingSuccessorReplacementFailsClosedWithoutUnlink(t *testing.T) {
	beforeFDs, checkFDs := valueLogOpenDescriptorCount(t)
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	displacedPath := filepath.Join(dir, "000002-displaced.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	injected := errors.New("injected value-log observer failure before replacement")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Namespace == durabilitycut.NamespaceCreate {
			return injected
		}
		return nil
	})
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	restore()
	if rotation != nil {
		rotation.Release()
		t.Fatal("failed rotation returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("first rotation error=%v want observer failure", err)
	}
	if err := os.Rename(secondPath, displacedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Error("conflicting retry returned owned resources")
	}
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Errorf("conflicting retry error=%v want ErrResourceConflict", err)
	}
	if err := writer.Close(); err != nil {
		t.Errorf("close writer: %v", err)
	}
	if got, err := os.ReadFile(secondPath); err != nil || string(got) != "replacement" {
		t.Errorf("replacement changed: data=%q err=%v", got, err)
	}
	if _, err := os.Stat(displacedPath); err != nil {
		t.Errorf("displaced exact successor removed: %v", err)
	}
	if afterFDs, ok := valueLogOpenDescriptorCount(t); checkFDs && ok && afterFDs > beforeFDs+1 {
		t.Errorf("close retained descriptors: before=%d after=%d", beforeFDs, afterFDs)
	}
}

func TestStableValueLogOldCloseFailureIsFailStop(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "000001.vlog")
	secondPath := filepath.Join(dir, "000002.vlog")
	writer, err := NewWriter(firstPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	appendStablePendingValue(t, writer, 1)
	closed, active := stablePendingRegistrations(1, 2)
	injected := errors.New("injected value-log old close failure")
	writer.closeRotateFn = func(file *os.File) error {
		writer.closeRotateFn = nil
		return errors.Join(file.Close(), injected)
	}
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("old-close failure returned owned resources")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("rotation error=%v want old-close failure", err)
	}
	if err := writer.RotateTo(firstPath, 1); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("ordinary rotation after fail-stop error=%v want ErrResourceOwnership", err)
	}
	rotation, err = writer.RotateToWithStableResources(secondPath, 2, false, closed, active)
	if rotation != nil {
		rotation.Release()
		t.Fatal("stable retry after fail-stop returned owned resources")
	}
	if !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("stable retry after fail-stop error=%v want ErrResourceOwnership", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(secondPath); err != nil {
		t.Fatalf("fail-stop close removed created successor: %v", err)
	}
}

func TestStableValueLogRetryDescriptorPlateau(t *testing.T) {
	dir := t.TempDir()
	writer, err := NewWriter(filepath.Join(dir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	appendStablePendingValue(t, writer, 1)
	baseline, checkFDs := valueLogOpenDescriptorCount(t)
	injected := errors.New("injected value-log retry cycle")
	createCalls := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceValueLog && event.Namespace == durabilitycut.NamespaceCreate {
			createCalls++
			return injected
		}
		return nil
	})
	defer restore()

	for cycle := uint32(0); cycle < 64; cycle++ {
		closedID := writer.FileID()
		activeID := closedID + 1
		closed, active := stablePendingRegistrations(closedID, activeID)
		path := filepath.Join(dir, fmt.Sprintf("%06d.vlog", activeID))
		rotation, err := writer.RotateToWithStableResources(path, activeID, false, closed, active)
		if rotation != nil {
			rotation.Release()
			t.Fatalf("cycle %d first attempt returned resources", cycle)
		}
		if !errors.Is(err, injected) {
			t.Fatalf("cycle %d first attempt error=%v", cycle, err)
		}
		rotation, err = writer.RotateToWithStableResources(path, activeID, false, closed, active)
		if err != nil {
			t.Fatalf("cycle %d retry: %v", cycle, err)
		}
		rotation.Release()
		appendStablePendingValue(t, writer, uint64(activeID))
		if got, ok := valueLogOpenDescriptorCount(t); checkFDs && ok && got > baseline+1 {
			t.Fatalf("cycle %d descriptors=%d baseline=%d", cycle, got, baseline)
		}
	}
	if createCalls != 64 {
		t.Fatalf("create observer calls=%d want 64", createCalls)
	}
	if writer.FileID() != 65 {
		t.Fatalf("final file id=%d want 65", writer.FileID())
	}
}
