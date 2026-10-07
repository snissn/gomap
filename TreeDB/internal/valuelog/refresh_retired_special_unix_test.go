//go:build darwin || linux || freebsd || netbsd || openbsd

package valuelog

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestManagerRefreshRetiredSegmentRejectsSpecialPath(t *testing.T) {
	for _, kind := range []string{"fifo", "link-old-file", "link-fifo"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			id := writeTestSegment(t, dir, 0, 1, 1, []byte("retired"))
			manager, err := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Close()
			set := manager.CurrentSetNoRefresh()
			retired := set.Files[id]
			if err := manager.MarkZombie(id); err != nil {
				t.Fatal(err)
			}
			originalRemove := removeSegmentPath
			wantErr := errors.New("injected unlink failure")
			removeSegmentPath = func(string, func(string) error) error { return wantErr }
			t.Cleanup(func() { removeSegmentPath = originalRemove })
			err = manager.Release(set)
			removeSegmentPath = originalRemove
			if !errors.Is(err, wantErr) || !retired.closed.Load() {
				t.Fatalf("retired release did not fail after close: %v", err)
			}
			if err := manager.Refresh(); err != nil {
				t.Fatalf("unchanged retired identity refused: %v", err)
			}
			oldPath := retired.Path + ".retired"
			if err := os.Rename(retired.Path, oldPath); err != nil {
				t.Fatal(err)
			}
			pipe := retired.Path
			switch kind {
			case "fifo":
				if err := syscall.Mkfifo(pipe, 0o600); err != nil {
					t.Fatal(err)
				}
			case "link-old-file":
				if err := os.Symlink(oldPath, retired.Path); err != nil {
					t.Fatal(err)
				}
			case "link-fifo":
				pipe += ".fifo"
				if err := syscall.Mkfifo(pipe, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(pipe, retired.Path); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(retired.Path)
			if err != nil {
				t.Fatal(err)
			}
			assertPromptRefusal := func(label string, operation func() error) {
				t.Helper()
				done := make(chan error, 1)
				go func() { done <- operation() }()
				select {
				case err := <-done:
					if err == nil {
						t.Fatalf("%s accepted a special retired pathname", label)
					}
				case <-time.After(time.Second):
					// Unblock and join the old blocking open before reporting the
					// regression, so the failed test also releases manager.mu.
					fd, err := syscall.Open(pipe, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
					if err == nil {
						_ = syscall.Close(fd)
					}
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Fatal("special-path operation did not join after FIFO release")
					}
					t.Fatalf("%s blocked on a special retired pathname", label)
				}
			}
			assertPromptRefusal("Refresh", manager.Refresh)
			assertPromptRefusal("retirement retry", func() error { return manager.deleteZombieFile(retired) })
			manager.mu.RLock()
			tracked := manager.files[id]
			manager.mu.RUnlock()
			if tracked != retired || !retired.IsZombie.Load() {
				t.Fatal("special pathname changed retirement ownership")
			}
			current := manager.CurrentSetNoRefresh()
			if current.Files[id] != nil {
				t.Fatal("special pathname resurrected the retired segment")
			}
			if err := manager.Release(current); err != nil {
				t.Fatal(err)
			}
			after, err := os.Lstat(retired.Path)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatalf("special pathname changed during refusal: %v", err)
			}
			if kind != "fifo" {
				wantTarget := oldPath
				if kind == "link-fifo" {
					wantTarget = pipe
				}
				if target, err := os.Readlink(retired.Path); err != nil || target != wantTarget {
					t.Fatalf("replacement link changed: target=%q err=%v", target, err)
				}
			}
		})
	}
}
