//go:build darwin || linux || freebsd || netbsd || openbsd

package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This additive fixture compiles on the prior head. The existing Close cache
// lock pauses a real direct API after its identity preflight and closed CAS,
// before the handle close and quarantine rename; no production hook is added.
func TestManagerDirectRetirementPostPreflightReplacement(t *testing.T) {
	for _, mode := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		for _, pins := range []string{"nil", "registry"} {
			t.Run(mode+"/"+pins, func(t *testing.T) {
				manager, file := directRetirementManager(t, pins == "registry")
				identity := retirementIdentityAtPath(t, file.Path)
				file.cacheMu.Lock()
				locked, joined := true, false
				done := make(chan error, 1)
				go func() { done <- retirementIdentityRemove(manager, file, identity, mode) }()
				defer func() {
					if locked {
						file.cacheMu.Unlock()
					}
					if !joined {
						select {
						case <-done:
						case <-time.After(5 * time.Second):
							t.Error("direct retirement cleanup did not join the real API")
						}
					}
				}()
				timeout := time.NewTimer(5 * time.Second)
				defer timeout.Stop()
				for !file.closed.Load() {
					select {
					case err := <-done:
						joined = true
						t.Fatalf("direct API returned before the Close pause: %v", err)
					case <-timeout.C:
						t.Fatal("direct API never reached the Close pause")
					default:
						runtime.Gosched()
					}
				}
				manager.mu.RLock()
				captured := file.retirementIdentity
				manager.mu.RUnlock()
				if !rootpublication.SamePhysicalIdentity(identity, captured) {
					t.Fatal("Close pause preceded immutable identity admission")
				}
				oldPath := file.Path + ".original"
				if err := os.Rename(file.Path, oldPath); err != nil {
					t.Fatal(err)
				}
				writeTestSegment(t, filepath.Dir(file.Path), 0, 1, 2, []byte("post-preflight replacement must survive"))
				replacementIdentity := retirementIdentityAtPath(t, file.Path)
				wantBytes, err := os.ReadFile(file.Path)
				if err != nil || rootpublication.SamePhysicalIdentity(identity, replacementIdentity) {
					t.Fatalf("post-preflight fixture did not replace the inode: %v", err)
				}
				file.cacheMu.Unlock()
				locked = false
				select {
				case err = <-done:
					joined = true
				case <-time.After(5 * time.Second):
					t.Fatal("post-preflight direct API did not join")
				}
				if !errors.Is(err, rootpublication.ErrResourceConflict) {
					t.Fatalf("post-preflight direct API accepted substitution: %v", err)
				}
				directRetirementAssertState(t, manager, file, true)
				for range 2 {
					if err := manager.Refresh(); !errors.Is(err, rootpublication.ErrResourceConflict) {
						t.Fatalf("post-close direct owner allowed replacement Refresh: %v", err)
					}
				}
				if err := retirementIdentityRemove(manager, file, identity, mode); !errors.Is(err, rootpublication.ErrResourceConflict) {
					t.Fatalf("post-close direct retry accepted replacement: %v", err)
				}
				directRetirementAssertState(t, manager, file, true)
				gotBytes, err := os.ReadFile(file.Path)
				if err != nil || !bytes.Equal(gotBytes, wantBytes) || !rootpublication.SamePhysicalIdentity(replacementIdentity, retirementIdentityAtPath(t, file.Path)) {
					t.Fatalf("post-close refusal changed replacement: %v", err)
				}
				if err := os.Remove(file.Path); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(oldPath, file.Path); err != nil {
					t.Fatal(err)
				}
				if err := retirementIdentityRemove(manager, file, identity, mode); err != nil {
					t.Fatalf("restored direct retirement retry failed: %v", err)
				}
				directRetirementAssertState(t, manager, file, false)
			})
		}
	}
}
