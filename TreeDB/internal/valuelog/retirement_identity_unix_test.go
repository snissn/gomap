//go:build darwin || linux || freebsd || netbsd || openbsd

package valuelog

import (
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestManagerRetirementIdentityNilRegistrySpecialPath(t *testing.T) {
	for _, kind := range []string{"fifo", "link-old-file", "link-fifo"} {
		t.Run(kind, func(t *testing.T) {
			manager, file, _ := retirementIdentityClosedZombie(t, false)
			oldPath := file.Path + ".original"
			if err := os.Rename(file.Path, oldPath); err != nil {
				t.Fatal(err)
			}
			pipe := file.Path
			switch kind {
			case "fifo":
				if err := syscall.Mkfifo(pipe, 0o600); err != nil {
					t.Fatal(err)
				}
			case "link-old-file":
				if err := os.Symlink(oldPath, file.Path); err != nil {
					t.Fatal(err)
				}
			case "link-fifo":
				pipe += ".fifo"
				if err := syscall.Mkfifo(pipe, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(pipe, file.Path); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			assertPromptRefusal := func(label string, operation func() error) {
				t.Helper()
				done := make(chan error, 1)
				var joined sync.WaitGroup
				joined.Add(1)
				go func() {
					defer joined.Done()
					done <- operation()
				}()
				select {
				case err := <-done:
					joined.Wait()
					if err == nil {
						t.Fatalf("nil-registry %s accepted special retired path", label)
					}
				case <-time.After(time.Second):
					// Release a regressed blocking FIFO open and join before
					// reporting failure; the operation then releases manager.mu.
					fd, err := syscall.Open(pipe, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
					if err == nil {
						_ = syscall.Close(fd)
					}
					select {
					case <-done:
						joined.Wait()
					case <-time.After(time.Second):
						t.Fatal("special-path operation did not join after FIFO release")
					}
					t.Fatalf("nil-registry %s blocked on special retired path", label)
				}
			}
			assertPromptRefusal("Refresh", manager.Refresh)
			assertPromptRefusal("retry", func() error { return manager.deleteZombieFile(file) })
			retirementIdentityAssertOwner(t, manager, file)
			retirementIdentityAssertCount(t, manager, 1)
			after, err := os.Lstat(file.Path)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatalf("special replacement identity/type changed: %v", err)
			}
			if kind != "fifo" {
				wantTarget := oldPath
				if kind == "link-fifo" {
					wantTarget = pipe
				}
				if target, err := os.Readlink(file.Path); err != nil || target != wantTarget {
					t.Fatalf("special replacement link changed: %q error=%v", target, err)
				}
			}
			if err := os.Remove(file.Path); err != nil {
				t.Fatal(err)
			}
			if kind == "link-fifo" {
				if err := os.Remove(pipe); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Rename(oldPath, file.Path); err != nil {
				t.Fatal(err)
			}
			if err := manager.deleteZombieFile(file); err != nil {
				t.Fatalf("original identity did not retire after special refusal: %v", err)
			}
			retirementIdentityAssertCount(t, manager, 0)
			if _, err := os.Stat(file.Path); !os.IsNotExist(err) {
				t.Fatalf("completed retirement retained original pathname: %v", err)
			}
		})
	}
}
