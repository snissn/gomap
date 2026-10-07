//go:build darwin || linux || freebsd || netbsd || openbsd

package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Keep this fixture additive to the pre-repair source for a causal RED run.
func retirementSymlinkParentManager(t *testing.T, additional, registry bool) (*Manager, *File, string) {
	t.Helper()
	target := t.TempDir()
	alias := filepath.Join(t.TempDir(), "segments")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	id := writeTestSegment(t, alias, 0, 1, 1, []byte("original symlink-parent owner"))
	dir := alias
	if additional {
		dir = t.TempDir()
	}
	var pins *rootpublication.IdentityPinRegistry
	if registry {
		pins = rootpublication.NewIdentityPinRegistry()
	}
	manager, err := NewManagerWithStableResourcePinRegistry(dir, pins)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if additional {
		if err := manager.AddScanDir(alias); err != nil {
			t.Fatal(err)
		}
	}
	manager.mu.RLock()
	file := manager.files[id]
	manager.mu.RUnlock()
	if file == nil || filepath.Dir(file.Path) != alias {
		t.Fatal("manager did not retain the configured symlink-parent path")
	}
	return manager, file, target
}

func retirementSymlinkParentCloseZombie(t *testing.T, manager *Manager, file *File) {
	t.Helper()
	set := manager.CurrentSetNoRefresh()
	if set.Files[file.ID] != file {
		t.Fatal("current set did not pin the original owner")
	}
	if err := manager.MarkZombie(file.ID); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("injected symlink-parent unlink failure")
	err := func() error {
		originalRemove := removeSegmentPath
		defer func() { removeSegmentPath = originalRemove }()
		removeSegmentPath = func(string, func(string) error) error { return wantErr }
		return manager.Release(set)
	}()
	if !errors.Is(err, wantErr) || !file.closed.Load() {
		t.Fatalf("supported symlink-parent retirement did not close then fail unlink: %v", err)
	}
	if err := manager.Refresh(); err != nil {
		t.Fatalf("unchanged symlink-parent retired identity refused: %v", err)
	}
	retirementIdentityAssertOwner(t, manager, file)
}

func TestManagerRetirementSymlinkParentSupported(t *testing.T) {
	for _, namespace := range []string{"primary", "additional"} {
		for _, mode := range []string{"nil", "registry"} {
			t.Run(namespace+"/"+mode, func(t *testing.T) {
				manager, file, _ := retirementSymlinkParentManager(t, namespace == "additional", mode == "registry")
				identity := retirementIdentityAtPath(t, file.Path)
				retirementSymlinkParentCloseZombie(t, manager, file)
				if !rootpublication.SamePhysicalIdentity(identity, retirementIdentityAtPath(t, file.Path)) {
					t.Fatal("failed unlink changed the original segment")
				}
				if err := manager.deleteZombieFile(file); err != nil {
					t.Fatalf("supported symlink-parent retirement retry: %v", err)
				}
				if _, err := os.Stat(file.Path); !os.IsNotExist(err) || manager.HasSegment(file.ID) {
					t.Fatalf("successful retirement retained the segment: %v", err)
				}
				if err := manager.Refresh(); err != nil {
					t.Fatalf("post-retirement refresh: %v", err)
				}
			})
		}
	}
}

func TestManagerRetirementSymlinkParentDirectRemoval(t *testing.T) {
	for _, mode := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		for _, pins := range []string{"nil", "registry"} {
			t.Run(mode+"/"+pins, func(t *testing.T) {
				manager, file, _ := retirementSymlinkParentManager(t, false, pins == "registry")
				identity := retirementIdentityAtPath(t, file.Path)
				if err := retirementIdentityRemove(manager, file, identity, mode); err != nil {
					t.Fatalf("supported symlink-parent %s: %v", mode, err)
				}
				if _, err := os.Stat(file.Path); !os.IsNotExist(err) || manager.HasSegment(file.ID) {
					t.Fatalf("direct removal retained the segment: %v", err)
				}
			})
		}
	}
}

func retirementSymlinkParentPromptRefusal(t *testing.T, pipe string, operation func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- operation() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("accepted a substituted symlink-parent resource")
		}
	case <-time.After(time.Second):
		// Join a regressed blocking child open before reporting its failure.
		if pipe != "" {
			fd, err := syscall.Open(pipe, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
			if err == nil {
				_ = syscall.Close(fd)
			}
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("substituted-resource operation did not join")
		}
		t.Fatal("substituted-resource operation blocked")
	}
}

func TestManagerRetirementSymlinkParentChildRefusal(t *testing.T) {
	for _, kind := range []string{"regular", "directory", "link-old-file", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			manager, file, _ := retirementSymlinkParentManager(t, false, false)
			retirementSymlinkParentCloseZombie(t, manager, file)
			oldPath := file.Path + ".original"
			if err := os.Rename(file.Path, oldPath); err != nil {
				t.Fatal(err)
			}
			pipe := ""
			switch kind {
			case "regular":
				writeTestSegment(t, filepath.Dir(file.Path), 0, 1, 2, []byte("replacement must survive"))
			case "directory":
				if err := os.Mkdir(file.Path, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(file.Path, "sentinel"), []byte("directory must survive"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "link-old-file":
				if err := os.Symlink(oldPath, file.Path); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				pipe = file.Path
				if err := syscall.Mkfifo(pipe, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			bytesPath := ""
			if kind == "regular" {
				bytesPath = file.Path
			} else if kind == "directory" {
				bytesPath = filepath.Join(file.Path, "sentinel")
			}
			var wantBytes []byte
			if bytesPath != "" {
				var err error
				wantBytes, err = os.ReadFile(bytesPath)
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			retirementSymlinkParentPromptRefusal(t, pipe, manager.Refresh)
			retirementSymlinkParentPromptRefusal(t, pipe, func() error { return manager.deleteZombieFile(file) })
			retirementIdentityAssertOwner(t, manager, file)
			after, err := os.Lstat(file.Path)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatalf("substituted child changed during refusal: %v", err)
			}
			if bytesPath != "" {
				got, err := os.ReadFile(bytesPath)
				if err != nil || !bytes.Equal(got, wantBytes) {
					t.Fatalf("substituted resource bytes changed: %v", err)
				}
			}
			if kind == "link-old-file" {
				if target, err := os.Readlink(file.Path); err != nil || target != oldPath {
					t.Fatalf("substituted child link changed: %q %v", target, err)
				}
			}
		})
	}
}

func TestManagerRetirementSymlinkParentAliasRebound(t *testing.T) {
	manager, file, originalDir := retirementSymlinkParentManager(t, false, true)
	retirementSymlinkParentCloseZombie(t, manager, file)
	originalPath := filepath.Join(originalDir, filepath.Base(file.Path))
	originalBytes, err := os.ReadFile(originalPath)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Dir(file.Path)
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	replacementDir := t.TempDir()
	if err := os.Symlink(replacementDir, alias); err != nil {
		t.Fatal(err)
	}
	writeTestSegment(t, alias, 0, 1, 2, []byte("rebound parent replacement must survive"))
	replacementBytes, err := os.ReadFile(file.Path)
	if err != nil {
		t.Fatal(err)
	}
	replacementIdentity := retirementIdentityAtPath(t, file.Path)
	for range 2 {
		if err := manager.Refresh(); !errors.Is(err, rootpublication.ErrResourceConflict) {
			t.Fatalf("Refresh accepted a conflicting rebound parent: %v", err)
		}
	}
	if err := manager.deleteZombieFile(file); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("retirement retry accepted a conflicting rebound parent: %v", err)
	}
	retirementIdentityAssertOwner(t, manager, file)
	got, err := os.ReadFile(file.Path)
	if err != nil || !bytes.Equal(got, replacementBytes) || !rootpublication.SamePhysicalIdentity(replacementIdentity, retirementIdentityAtPath(t, file.Path)) {
		t.Fatalf("rebound parent replacement changed: %v", err)
	}
	got, err = os.ReadFile(originalPath)
	if err != nil || !bytes.Equal(got, originalBytes) {
		t.Fatalf("original parent resource changed: %v", err)
	}
}

func TestManagerRetirementSymlinkParentQuarantineRecovery(t *testing.T) {
	target := t.TempDir()
	alias := filepath.Join(t.TempDir(), "segments")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	_, path := writeIdentityPinTestSegment(t, alias)
	identity := retirementIdentityAtPath(t, path)
	quarantineDir, quarantinePath, err := stableDeleteQuarantinePaths(path, identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, quarantinePath); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManagerWithStableResourcePinRegistry(alias, rootpublication.NewIdentityPinRegistry())
	if err != nil {
		t.Fatalf("supported symlink-parent quarantine recovery: %v", err)
	}
	defer manager.Close()
	if _, err := os.Stat(quarantineDir); !os.IsNotExist(err) {
		t.Fatalf("matching quarantine remains after recovery: %v", err)
	}
	set := manager.CurrentSetNoRefresh()
	if len(set.Files) != 0 {
		t.Fatal("recovered retirement was exposed as a live segment")
	}
	if err := manager.Release(set); err != nil {
		t.Fatal(err)
	}
}
