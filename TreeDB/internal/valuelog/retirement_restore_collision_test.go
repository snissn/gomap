package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestRetirementRestoreNoReplace(t *testing.T) {
	dir := t.TempDir()
	_, path := writeIdentityPinTestSegment(t, dir)
	original := identityPinTestIdentity(t, path)
	qdir, qpath, err := stableDeleteQuarantinePaths(path, original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(qdir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, qpath); err != nil {
		t.Fatal(err)
	}
	successor := []byte("canonical successor must survive")
	if err := os.WriteFile(path, successor, 0600); err != nil {
		t.Fatal(err)
	}
	successorIdentity := identityPinTestIdentity(t, path)
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	parent, err := rootpublication.OpenStableParent(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	q, err := openRetirementQuarantine(root, filepath.Base(qdir))
	if err != nil {
		t.Fatal(err)
	}
	defer q.close()
	creates := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceCreate && event.NewPath == path {
			creates++
		}
		return nil
	})
	err = restoreRetirementQuarantine(q, parent, filepath.Base(path), dir, path, false)
	restore()
	if !errors.Is(err, rootpublication.ErrResourceConflict) || !errors.Is(err, os.ErrExist) {
		t.Fatalf("no-replace restoration lost conflict/collision: %v", err)
	}
	if creates != 0 {
		t.Fatal("failed no-replace restoration emitted create")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, successor) || !rootpublication.SamePhysicalIdentity(successorIdentity, identityPinTestIdentity(t, path)) {
		t.Fatalf("restoration replaced successor: %v", err)
	}
	if !rootpublication.SamePhysicalIdentity(original, identityPinTestIdentity(t, qpath)) {
		t.Fatal("restoration lost quarantine identity")
	}
}

func TestManagerRetirementRollbackPreservesSuccessor(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			manager, file := directRetirementManager(t, registry)
			original := identityPinTestIdentity(t, file.Path)
			want, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			qdir, qpath, err := stableDeleteQuarantinePaths(file.Path, original)
			if err != nil {
				t.Fatal(err)
			}
			successor := []byte("rollback successor must survive")
			wantUnlink := errors.New("injected rollback unlink failure")
			originalRemove := removeSegmentPath
			creates := 0
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Namespace == durabilitycut.NamespaceCreate && event.NewPath == file.Path {
					creates++
				}
				return nil
			})
			removeSegmentPath = func(string, func(string) error) error {
				if err := os.WriteFile(file.Path, successor, 0600); err != nil {
					return err
				}
				return wantUnlink
			}
			err = manager.RemoveSegment(file.ID)
			removeSegmentPath = originalRemove
			restore()
			if !errors.Is(err, rootpublication.ErrResourceConflict) || !errors.Is(err, wantUnlink) || !errors.Is(err, os.ErrExist) {
				t.Fatalf("rollback lost unlink/collision errors: %v", err)
			}
			if !file.closed.Load() || creates != 0 {
				t.Fatal("rollback collision violated close/create order")
			}
			directRetirementAssertState(t, manager, file, true)
			got, err := os.ReadFile(file.Path)
			if err != nil || !bytes.Equal(got, successor) {
				t.Fatalf("rollback replaced successor: %v", err)
			}
			got, err = os.ReadFile(qpath)
			if err != nil || !bytes.Equal(got, want) || !rootpublication.SamePhysicalIdentity(original, identityPinTestIdentity(t, qpath)) {
				t.Fatalf("rollback lost quarantined original: %v", err)
			}
			if err := os.Remove(file.Path); err != nil {
				t.Fatal(err)
			}
			if err := manager.RemoveSegment(file.ID); err != nil {
				t.Fatalf("rollback conflict retry: %v", err)
			}
			directRetirementAssertState(t, manager, file, false)
			if _, err := os.Stat(qdir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retry retained quarantine: %v", err)
			}
		})
	}
}
