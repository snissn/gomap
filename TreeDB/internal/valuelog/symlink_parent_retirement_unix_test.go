//go:build darwin || linux || freebsd || netbsd || openbsd

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

// Additive to 4cd37df: use only existing fixtures and real public removal calls.
func TestManagerSymlinkParentRetirementAliasDisappears(t *testing.T) {
	for _, mode := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		for _, pins := range []string{"nil", "registry"} {
			t.Run(mode+"/"+pins, func(t *testing.T) {
				manager, file, target := retirementSymlinkParentManager(t, false, pins == "registry")
				identity := retirementIdentityAtPath(t, file.Path)
				physicalPath := filepath.Join(target, filepath.Base(file.Path))
				want, err := os.ReadFile(physicalPath)
				if err != nil {
					t.Fatal(err)
				}
				alias := filepath.Dir(file.Path)
				resume, join := directRetirementPausedCall(t, file, func() error {
					return retirementIdentityRemove(manager, file, identity, mode)
				})
				// Close has set its CAS and is paused at cacheMu, after admission.
				if err := os.Remove(alias); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Symlink(target, alias) })
				resume()
				if err := join(); !errors.Is(err, rootpublication.ErrResourceConflict) {
					t.Fatalf("missing parent alias lost retirement owner: %v", err)
				}
				got, err := os.ReadFile(physicalPath)
				if err != nil || !bytes.Equal(got, want) || !rootpublication.SamePhysicalIdentity(identity, retirementIdentityAtPath(t, physicalPath)) {
					t.Fatalf("missing alias changed the physical target: %v", err)
				}
				directRetirementAssertState(t, manager, file, true)
				if err := manager.deleteZombieFile(file); !errors.Is(err, rootpublication.ErrResourceConflict) {
					t.Fatalf("missing alias retry lost retirement owner: %v", err)
				}
				if err := os.Symlink(target, alias); err != nil {
					t.Fatal(err)
				}
				if err := manager.Refresh(); err != nil {
					t.Fatal(err)
				}
				if err := retirementIdentityRemove(manager, file, identity, mode); err != nil {
					t.Fatal(err)
				}
				directRetirementAssertState(t, manager, file, false)
				if _, err := os.Stat(physicalPath); !os.IsNotExist(err) {
					t.Fatalf("restored alias retry did not delete original: %v", err)
				}
			})
		}
	}
}

func TestManagerSymlinkParentRetirementRetryAliasDisappears(t *testing.T) {
	for _, namespace := range []string{"primary", "additional"} {
		for _, pins := range []string{"nil", "registry"} {
			t.Run(namespace+"/"+pins, func(t *testing.T) {
				manager, file, target := retirementSymlinkParentManager(t, namespace == "additional", pins == "registry")
				retirementSymlinkParentCloseZombie(t, manager, file)
				alias := filepath.Dir(file.Path)
				if err := os.Remove(alias); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Symlink(target, alias) })
				if err := manager.deleteZombieFile(file); !errors.Is(err, rootpublication.ErrResourceConflict) {
					t.Fatalf("retired missing alias accepted: %v", err)
				}
				retirementIdentityAssertOwner(t, manager, file)
				if err := os.Symlink(target, alias); err != nil {
					t.Fatal(err)
				}
				if err := manager.Refresh(); err != nil {
					t.Fatal(err)
				}
				if err := manager.deleteZombieFile(file); err != nil {
					t.Fatal(err)
				}
				if manager.HasSegment(file.ID) {
					t.Fatal("valid restored-alias retry retained owner")
				}
			})
		}
	}
}

func TestManagerSymlinkParentRetirementPostRenameCut(t *testing.T) {
	manager, file, target := retirementSymlinkParentManager(t, false, false)
	alias := filepath.Dir(file.Path)
	wantErr := errors.New("injected alias disappearance after canonical unlink")
	called := false
	err := func() error {
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Namespace == durabilitycut.NamespaceUnlink && filepath.Clean(event.OldPath) == filepath.Clean(file.Path) {
				called = true
				if err := os.Remove(alias); err != nil {
					return err
				}
				return wantErr
			}
			return nil
		})
		defer restore()
		return manager.RemoveSegment(file.ID)
	}()
	if !called || !errors.Is(err, wantErr) || manager.HasSegment(file.ID) {
		t.Fatalf("canonical-unlink cut changed completion: called=%t error=%v", called, err)
	}
	if _, err := os.Stat(filepath.Join(target, filepath.Base(file.Path))); !os.IsNotExist(err) {
		t.Fatalf("canonical target survived real rename: %v", err)
	}
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	if err := requireNoStableDeleteQuarantines(alias); !errors.Is(err, ErrStableDeleteRecoveryRequired) {
		t.Fatalf("private intent was lost: %v", err)
	}
	if err := recoverStableDeleteQuarantines(alias); err != nil {
		t.Fatal(err)
	}
	if err := requireNoStableDeleteQuarantines(alias); err != nil {
		t.Fatal(err)
	}
}

func TestManagerSymlinkParentRetirementCaptureRebound(t *testing.T) {
	for _, pins := range []string{"nil", "registry"} {
		t.Run(pins, func(t *testing.T) {
			manager, file, target := retirementSymlinkParentManager(t, false, pins == "registry")
			identity := retirementIdentityAtPath(t, file.Path)
			alias := filepath.Dir(file.Path)
			foreign := t.TempDir()
			if err := os.Remove(alias); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(foreign, alias); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Remove(alias); _ = os.Symlink(target, alias) })
			if err := manager.MarkZombie(file.ID); !errors.Is(err, rootpublication.ErrResourceConflict) {
				t.Fatalf("empty rebound parent admitted unproven retirement: %v", err)
			}
			manager.mu.RLock()
			tracked := manager.files[file.ID]
			count := manager.retiredCount
			manager.mu.RUnlock()
			if tracked != file || file.IsZombie.Load() || file.closed.Load() || count != 0 {
				t.Fatal("capture refusal changed live ownership")
			}
			openIdentity, err := rootpublication.StableIdentityFromFile(file.File)
			if err != nil || !rootpublication.SamePhysicalIdentity(openIdentity, identity) {
				t.Fatalf("capture refusal closed or changed original: %v", err)
			}
			if manager.stableResourcePins != nil && manager.stableResourcePins.ActiveIdentities() != 1 {
				t.Fatal("capture refusal released registry observation")
			}
			entries, err := os.ReadDir(foreign)
			if err != nil || len(entries) != 0 {
				t.Fatalf("capture refusal changed foreign parent: %v", err)
			}
			if err := os.Remove(alias); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, alias); err != nil {
				t.Fatal(err)
			}
			if err := manager.MarkZombie(file.ID); err != nil {
				t.Fatal(err)
			}
			if err := manager.deleteZombieFile(file); err != nil {
				t.Fatal(err)
			}
		})
	}
}
