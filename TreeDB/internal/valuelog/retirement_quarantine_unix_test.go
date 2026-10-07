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

func TestManagerSymlinkParentRetirementQuarantineRedirect(t *testing.T) {
	for _, outcome := range []string{"remove", "rollback"} {
		for _, pins := range []string{"nil", "registry"} {
			t.Run(outcome+"/"+pins, func(t *testing.T) {
				manager, file := directRetirementManager(t, pins == "registry")
				identity := retirementIdentityAtPath(t, file.Path)
				originalBytes, err := os.ReadFile(file.Path)
				if err != nil {
					t.Fatal(err)
				}
				qdir, qpath, err := stableDeleteQuarantinePaths(file.Path, identity)
				if err != nil {
					t.Fatal(err)
				}
				parent := filepath.Dir(file.Path)
				foreignDir := filepath.Join(parent, "foreign-quarantine")
				if err := os.Mkdir(foreignDir, 0o700); err != nil {
					t.Fatal(err)
				}
				foreignPath := filepath.Join(foreignDir, filepath.Base(file.Path))
				foreignBytes := []byte("unrelated quarantine child must survive")
				if err := os.WriteFile(foreignPath, foreignBytes, 0o600); err != nil {
					t.Fatal(err)
				}
				foreignIdentity := retirementIdentityAtPath(t, foreignPath)
				saved := qdir + "-saved"
				redirected := false
				redirect := func() error {
					if err := os.Rename(qdir, saved); err != nil {
						return err
					}
					if err := os.Symlink(filepath.Base(foreignDir), qdir); err != nil {
						return err
					}
					redirected = true
					return nil
				}
				wantErr := errors.New("injected retained quarantine unlink failure")
				err = func() error {
					if outcome == "rollback" {
						old := removeSegmentPath
						defer func() { removeSegmentPath = old }()
						removeSegmentPath = func(string, func(string) error) error {
							if err := redirect(); err != nil {
								return err
							}
							return wantErr
						}
					} else {
						restore := durabilitycut.Install(func(event durabilitycut.Event) error {
							if event.Namespace == durabilitycut.NamespaceUnlink && event.OldPath == file.Path {
								return redirect()
							}
							return nil
						})
						defer restore()
					}
					return manager.RemoveSegment(file.ID)
				}()
				if !redirected {
					t.Fatalf("actual quarantine redirect did not run: %v", err)
				}
				if outcome == "remove" {
					if !errors.Is(err, rootpublication.ErrResourceConflict) {
						t.Fatalf("redirected cleanup did not refuse: %v", err)
					}
					directRetirementAssertState(t, manager, file, false)
					if _, err := os.Stat(file.Path); !os.IsNotExist(err) {
						t.Fatalf("completed original canonical unlink was lost: %v", err)
					}
				} else {
					if !errors.Is(err, wantErr) {
						t.Fatalf("rollback changed injected failure: %v", err)
					}
					directRetirementAssertState(t, manager, file, true)
					got, err := os.ReadFile(file.Path)
					if err != nil || !bytes.Equal(got, originalBytes) || !rootpublication.SamePhysicalIdentity(retirementIdentityAtPath(t, file.Path), identity) {
						t.Fatalf("rollback did not restore captured original: %v", err)
					}
				}
				got, err := os.ReadFile(foreignPath)
				if err != nil || !bytes.Equal(got, foreignBytes) || !rootpublication.SamePhysicalIdentity(retirementIdentityAtPath(t, foreignPath), foreignIdentity) {
					t.Fatalf("quarantine redirect changed foreign child: %v", err)
				}
				if _, err := os.Stat(filepath.Join(saved, filepath.Base(qpath))); !os.IsNotExist(err) {
					t.Fatalf("captured original quarantine child survived: %v", err)
				}
				if err := os.Remove(qdir); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(saved, qdir); err != nil {
					t.Fatal(err)
				}
				if outcome == "rollback" {
					if err := manager.RemoveSegment(file.ID); err != nil {
						t.Fatalf("restored original retry failed: %v", err)
					}
					directRetirementAssertState(t, manager, file, false)
				} else if err := recoverStableDeleteQuarantines(parent); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestManagerSymlinkParentRetirementQuarantineRecoveryRedirect(t *testing.T) {
	dir := t.TempDir()
	_, path := writeIdentityPinTestSegment(t, dir)
	identity := retirementIdentityAtPath(t, path)
	qdir, qpath, err := stableDeleteQuarantinePaths(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(qdir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Keep the original inode live until the replacement exists, excluding
	// inode reuse from this unexpected-quarantine recovery oracle.
	if err := os.WriteFile(qpath, []byte("unexpected recoverable original"), 0o600); err != nil {
		t.Fatal(err)
	}
	restoredIdentity := retirementIdentityAtPath(t, qpath)
	if rootpublication.SamePhysicalIdentity(identity, restoredIdentity) {
		t.Fatal("recovery fixture identities coincide")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	foreignDir := filepath.Join(dir, "foreign-recovery")
	if err := os.Mkdir(foreignDir, 0o700); err != nil {
		t.Fatal(err)
	}
	foreignPath := filepath.Join(foreignDir, filepath.Base(path))
	foreignBytes := []byte("foreign recovery child")
	if err := os.WriteFile(foreignPath, foreignBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	saved := qdir + "-saved"
	redirected := false
	err = func() error {
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Namespace != durabilitycut.NamespaceCreate || event.NewPath != path {
				return nil
			}
			if err := os.Rename(qdir, saved); err != nil {
				return err
			}
			if err := os.Symlink(filepath.Base(foreignDir), qdir); err != nil {
				return err
			}
			redirected = true
			return nil
		})
		defer restore()
		return recoverStableDeleteQuarantines(dir)
	}()
	if !redirected || !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("recovery cleanup redirect not refused: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "unexpected recoverable original" || !rootpublication.SamePhysicalIdentity(retirementIdentityAtPath(t, path), restoredIdentity) {
		t.Fatalf("recovery did not restore captured child: %v", err)
	}
	got, err = os.ReadFile(foreignPath)
	if err != nil || !bytes.Equal(got, foreignBytes) {
		t.Fatalf("recovery changed foreign child: %v", err)
	}
	if _, err := os.Stat(filepath.Join(saved, filepath.Base(path))); !os.IsNotExist(err) {
		t.Fatalf("recovery left original in captured quarantine: %v", err)
	}
}
