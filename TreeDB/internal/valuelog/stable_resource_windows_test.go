//go:build windows

package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableValueLogRotationFailsClosedWithoutSuccessorVisibility(t *testing.T) {
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
			if _, err := writer.Append(0, nil, 1, []byte("before-unsupported-rotation")); err != nil {
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
			if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
				if rotation != nil {
					rotation.Release()
				}
				t.Fatalf("stable rotation error=%v want ErrNamespacePersistenceUnsupported", err)
			}
			if rotation != nil {
				rotation.Release()
				t.Fatal("unsupported stable rotation returned owned resources")
			}
			if writer.FileID() != 1 || writer.f == nil || writer.f.Name() != firstPath {
				t.Fatalf("unsupported rotation changed active writer: id=%d file=%v", writer.FileID(), writer.f)
			}
			if _, err := os.Stat(secondPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsupported rotation exposed successor %q: %v", secondPath, err)
			}
			if _, err := writer.Append(0, nil, 2, []byte("after-unsupported-rotation")); err != nil {
				t.Fatalf("old writer append after unsupported rotation: %v", err)
			}
		})
	}
}

func TestOrdinaryValueLogRotationRemainsUsableAndStableRotationFailsClosed(t *testing.T) {
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
	if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) || rotation != nil {
		if rotation != nil {
			rotation.Release()
		}
		t.Fatalf("stable rotation=(%v, %v) want typed unsupported and no tokens", rotation, err)
	}
	if writer.FileID() != 2 || writer.f == nil || writer.f.Name() != secondPath {
		t.Fatalf("failed stable rotation changed ordinary active writer: id=%d file=%v", writer.FileID(), writer.f)
	}
	if _, err := os.Stat(thirdPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsupported stable rotation exposed successor: %v", err)
	}
}

func TestUnsupportedStableRotationDoesNotLeakRegistryOwnership(t *testing.T) {
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	rotation, err := writer.RotateToWithStableResources(filepath.Join(dir, "000002.vlog"), 2, false,
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
