//go:build windows

package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"golang.org/x/sys/windows"
)

func TestStableValueLogRotationUsesCreateOnlyWindowsEvidence(t *testing.T) {
	for _, syncCurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "flush", true: "sync"}[syncCurrent], func(t *testing.T) {
			dir := t.TempDir()
			firstPath := filepath.Join(dir, "000001.vlog")
			secondPath := filepath.Join(dir, "000002.vlog")
			writer, err := NewWriter(firstPath, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			if _, err := writer.Append(0, nil, 1, []byte("before-stable-rotation")); err != nil {
				t.Fatal(err)
			}
			rotation, err := writer.RotateToWithStableResources(secondPath, 2, syncCurrent,
				StableResourceRegistration{
					LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
					Reachability: rootpublication.ReachabilityValueLogPointer,
				},
				StableResourceRegistration{
					LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
					Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 2,
					NamespaceOperation: rootpublication.NamespaceCreate,
				})
			if err != nil {
				t.Fatalf("stable rotation: %v", err)
			}
			if rotation == nil || rotation.Closed == nil || rotation.Active == nil {
				t.Fatalf("stable rotation=%+v want exact closed and active authority", rotation)
			}
			defer rotation.Release()
			if writer.FileID() != 2 || writer.f == nil || writer.f.Name() != secondPath {
				t.Fatalf("stable rotation active writer: id=%d file=%v", writer.FileID(), writer.f)
			}
			if _, err := os.Stat(secondPath); err != nil {
				t.Fatalf("stable rotation successor %q: %v", secondPath, err)
			}
			if _, err := writer.Append(0, nil, 2, []byte("after-stable-rotation")); err != nil {
				t.Fatalf("new writer append after stable rotation: %v", err)
			}
		})
	}
}

func TestStableValueLogCurrentCreationUsesRetainedParentAndFailsTyped(t *testing.T) {
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(t.TempDir(), "000001.vlog"), 1, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	token, err := writer.StableResourceToken(StableResourceRegistration{
		LogicalLane: "outer-leaf", Generation: 1, DiagnosticPath: "leaf_vlog/000001.vlog",
		Reachability: rootpublication.ReachabilityOuterLeafRawPointer, ParentGeneration: 1,
		NamespaceOperation: rootpublication.NamespaceCreate,
	})
	if token != nil {
		token.Release()
		t.Fatal("unsupported current-segment capture returned a token")
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("current-segment capture error=%v want ErrNamespacePersistenceUnsupported", err)
	}
}

func TestOrdinaryValueLogRotationRefreshesParentForStableWindowsRotation(t *testing.T) {
	dir := t.TempDir()
	secondPath := filepath.Join(dir, "000002.vlog")
	thirdPath := filepath.Join(dir, "000003.vlog")
	writer, err := NewWriter(filepath.Join(dir, "000001.vlog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err := writer.RotateToWithSync(secondPath, 2, false); err != nil {
		t.Fatalf("ordinary rotation: %v", err)
	}
	if _, err := writer.Append(0, nil, 2, []byte("ordinary-windows-append")); err != nil {
		t.Fatal(err)
	}
	rotation, err := writer.RotateToWithStableResources(thirdPath, 3, false,
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
		t.Fatalf("stable rotation after ordinary rotation: %v", err)
	}
	if rotation == nil || rotation.Closed == nil || rotation.Active == nil {
		t.Fatalf("stable rotation=%+v want exact closed and active authority", rotation)
	}
	defer rotation.Release()
	if writer.FileID() != 3 || writer.f == nil || writer.f.Name() != thirdPath {
		t.Fatalf("stable rotation active writer: id=%d file=%v", writer.FileID(), writer.f)
	}
	if _, err := os.Stat(thirdPath); err != nil {
		t.Fatalf("stable rotation successor: %v", err)
	}
}

func TestUnsupportedStableRotationDoesNotLeakRegistryOwnership(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	secondPath := filepath.Join(dir, "000002.vlog")
	rotation, err := writer.RotateToWithStableResources(secondPath, 2, false,
		StableResourceRegistration{
			LogicalLane: "main", Generation: 1, DiagnosticPath: "maindb/value_vlog/000001.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer,
		},
		StableResourceRegistration{
			LogicalLane: "main", Generation: 2, DiagnosticPath: "maindb/value_vlog/000002.vlog",
			Reachability: rootpublication.ReachabilityValueLogPointer, ParentGeneration: 2,
			NamespaceOperation: rootpublication.NamespaceCreate,
		})
	if rotation != nil {
		rotation.Release()
		t.Fatal("unsupported stable rotation returned owned resources")
	}
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("stable rotation error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("unsupported rotation retained %d pins", got)
	}
	if got := registry.ActiveIdentities(); got != 1 {
		t.Fatalf("unsupported rotation identities=%d want active writer only", got)
	}
	if _, statErr := os.Stat(secondPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("unsupported rotation exposed successor: %v", statErr)
	}
	if _, appendErr := writer.Append(0, nil, 1, []byte("writer-remains-usable")); appendErr != nil {
		t.Fatalf("append after unsupported stable rotation: %v", appendErr)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("closed writer retained %d identities", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestRetirementParentDeletedWindows(t *testing.T) {
	for _, mode := range []string{"live-directory", "nonempty-directory", "regular-file", "closed-parent", "deleted-directory", "unrelated-error", "nil-parent"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "nil-parent" {
				if deleted, err := retirementParentDeleted(nil, windows.ERROR_ACCESS_DENIED); deleted || !errors.Is(err, os.ErrInvalid) {
					t.Fatalf("nil-parent proof=%t %v", deleted, err)
				}
				return
			}
			dir := filepath.Join(t.TempDir(), "parent")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := dir
			if mode == "regular-file" {
				path = filepath.Join(dir, "file")
				if err := os.WriteFile(path, []byte("regular file is not a directory"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var parent *os.File
			var err error
			if mode == "regular-file" {
				parent, err = os.Open(path)
			} else {
				parent, err = rootpublication.OpenStableParent(path)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = parent.Close() })
			childErr := error(windows.ERROR_ACCESS_DENIED)
			switch mode {
			case "closed-parent", "unrelated-error":
				if err := parent.Close(); err != nil {
					t.Fatal(err)
				}
				if mode == "unrelated-error" {
					childErr = windows.ERROR_INVALID_NAME
				}
			case "deleted-directory":
				if err := os.Remove(dir); err != nil {
					t.Fatal(err)
				}
			case "nonempty-directory":
				id := writeTestSegment(t, dir, 0, 1, 1, []byte("physical child must remain"))
				if err := os.Remove(dir); err == nil {
					t.Fatal("nonempty directory deletion unexpectedly succeeded")
				}
				name := filepath.Base(segmentPath(dir, id))
				if absent, err := requireRetirementChildAbsent(parent, name); absent || !errors.Is(err, rootpublication.ErrResourceConflict) {
					t.Fatalf("nonempty parent proved child absence: %t %v", absent, err)
				}
			}
			deleted, proofErr := retirementParentDeleted(parent, childErr)
			if mode == "closed-parent" {
				if deleted || proofErr == nil {
					t.Fatalf("closed-parent proof=%t %v", deleted, proofErr)
				}
				return
			}
			if proofErr != nil || deleted != (mode == "deleted-directory") {
				t.Fatalf("%s proof=%t %v", mode, deleted, proofErr)
			}
			if mode == "deleted-directory" {
				if absent, err := requireRetirementChildAbsent(parent, "missing.vlog"); !absent || err != nil {
					t.Fatalf("exact deleted-parent absence=%t %v", absent, err)
				}
			}
		})
	}
}
