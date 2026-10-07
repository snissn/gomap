package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func directRetirementManager(t *testing.T, registry bool) (*Manager, *File) {
	t.Helper()
	dir := t.TempDir()
	id := writeTestSegment(t, dir, 0, 1, 1, []byte("direct retirement owner"))
	var pins *rootpublication.IdentityPinRegistry
	if registry {
		pins = rootpublication.NewIdentityPinRegistry()
	}
	manager, err := NewManagerWithStableResourcePinRegistry(dir, pins)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	manager.mu.RLock()
	file := manager.files[id]
	manager.mu.RUnlock()
	if file == nil {
		t.Fatal("direct retirement fixture has no original owner")
	}
	return manager, file
}

func directRetirementAssertState(t *testing.T, manager *Manager, file *File, retained bool) {
	t.Helper()
	manager.mu.RLock()
	tracked := manager.files[file.ID]
	count := manager.retiredCount
	observed := file.stableObserved
	manager.mu.RUnlock()
	if retained {
		if tracked != file || !file.IsZombie.Load() || count != 1 {
			t.Fatal("post-close conflict dropped direct retirement owner")
		}
		if manager.stableResourcePins != nil && !observed {
			t.Fatal("failed direct deletion released registry observation")
		}
		retirementIdentityAssertOwner(t, manager, file)
	} else if tracked != nil || count != 0 || observed {
		t.Fatalf("successful direct deletion retained ownership: tracked=%v count=%d observed=%t", tracked != nil, count, observed)
	}
}

func TestManagerDirectRetirementFailedUnlink(t *testing.T) {
	for _, mode := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		for _, pins := range []string{"nil", "registry"} {
			t.Run(mode+"/"+pins, func(t *testing.T) {
				manager, file := directRetirementManager(t, pins == "registry")
				identity := retirementIdentityAtPath(t, file.Path)
				wantErr := errors.New("injected direct retirement unlink failure")
				err := func() error {
					originalRemove := removeSegmentPath
					defer func() { removeSegmentPath = originalRemove }()
					removeSegmentPath = func(string) error { return wantErr }
					if mode == "RemoveSegmentIfUnpinned" {
						admitted, err := manager.RemoveSegmentIfUnpinned(file.ID)
						if !admitted {
							t.Fatal("failed admitted removal changed IfUnpinned attempt result")
						}
						return err
					}
					return retirementIdentityRemove(manager, file, identity, mode)
				}()
				if !errors.Is(err, wantErr) || !file.closed.Load() {
					t.Fatalf("direct retirement did not close then fail unlink: %v", err)
				}
				directRetirementAssertState(t, manager, file, true)
				if err := manager.Refresh(); err != nil {
					t.Fatalf("unchanged closed direct retirement refused: %v", err)
				}
				if err := retirementIdentityRemove(manager, file, identity, mode); err != nil {
					t.Fatalf("direct retirement retry failed: %v", err)
				}
				directRetirementAssertState(t, manager, file, false)
				if _, err := os.Stat(file.Path); !os.IsNotExist(err) {
					t.Fatalf("successful direct retirement retry left a segment: %v", err)
				}
			})
		}
	}
}

func TestManagerDirectRetirementMissingPath(t *testing.T) {
	for _, mode := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		t.Run(mode, func(t *testing.T) {
			manager, file := directRetirementManager(t, false)
			identity := retirementIdentityAtPath(t, file.Path)
			wantErr := errors.New("injected direct missing-path setup failure")
			err := func() error {
				originalRemove := removeSegmentPath
				defer func() { removeSegmentPath = originalRemove }()
				removeSegmentPath = func(string) error { return wantErr }
				return retirementIdentityRemove(manager, file, identity, mode)
			}()
			if !errors.Is(err, wantErr) {
				t.Fatalf("failed unlink setup: %v", err)
			}
			directRetirementAssertState(t, manager, file, true)
			if err := os.Remove(file.Path); err != nil {
				t.Fatal(err)
			}
			if err := manager.Refresh(); err != nil {
				t.Fatalf("missing direct retirement path refused: %v", err)
			}
			if err := retirementIdentityRemove(manager, file, identity, mode); err != nil {
				t.Fatalf("missing direct retirement retry failed: %v", err)
			}
			directRetirementAssertState(t, manager, file, false)
		})
	}
}

func TestManagerDirectRetirementForceWithRefs(t *testing.T) {
	manager, file := directRetirementManager(t, false)
	set := manager.CurrentSetNoRefresh()
	if file.RefCount.Load() != 1 {
		t.Fatal("Force fixture has no logical reference")
	}
	if err := manager.RemoveSegmentForce(file.ID); err != nil {
		t.Fatalf("Force introduced a logical refcount gate: %v", err)
	}
	directRetirementAssertState(t, manager, file, false)
	if err := manager.Release(set); err != nil {
		t.Fatal(err)
	}
}

func TestManagerDirectRetirementSamePointerFinalization(t *testing.T) {
	manager, file := directRetirementManager(t, false)
	identity := retirementIdentityAtPath(t, file.Path)
	var replacement *File
	err := func() error {
		originalRemove := removeSegmentPath
		defer func() { removeSegmentPath = originalRemove }()
		removeSegmentPath = func(path string) error {
			// The old identity is already quarantined. Explicit eviction releases
			// this manager's ownership; explicit registration may install a new identity.
			if err := manager.EvictSegment(file.ID); err != nil {
				return err
			}
			writeTestSegment(t, filepath.Dir(file.Path), 0, 1, 2, []byte("new explicit owner"))
			if err := manager.RegisterSegment(file.Path, file.ID); err != nil {
				return err
			}
			manager.mu.RLock()
			replacement = manager.files[file.ID]
			manager.mu.RUnlock()
			return originalRemove(path)
		}
		return manager.RemoveSegmentExpectedIdentity(file.ID, identity)
	}()
	if err != nil || replacement == nil || replacement == file {
		t.Fatalf("same-pointer fixture failed: %v", err)
	}
	manager.mu.RLock()
	tracked, count := manager.files[file.ID], manager.retiredCount
	manager.mu.RUnlock()
	if tracked != replacement || count != 0 || !manager.HasSegment(file.ID) {
		t.Fatal("successful old deletion forgot a different current owner")
	}
}

// Pause an actual Close after its closed CAS, before handle cleanup. Cleanup
// always resumes and joins the real API, including a failed assertion path.
func directRetirementPausedCall(t *testing.T, file *File, call func() error) (func(), func() error) {
	t.Helper()
	file.cacheMu.Lock()
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { file.cacheMu.Unlock() }) }
	done := make(chan error, 1)
	go func() { done <- call() }()
	joined := false
	var result error
	join := func() error {
		t.Helper()
		if !joined {
			select {
			case result = <-done:
				joined = true
			case <-time.After(5 * time.Second):
				t.Fatal("paused direct retirement API did not join")
			}
		}
		return result
	}
	t.Cleanup(func() { resume(); _ = join() })
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for !file.closed.Load() {
		select {
		case result = <-done:
			joined = true
			t.Fatalf("paused retirement returned before closed CAS: %v", result)
		case <-timeout.C:
			t.Fatal("paused retirement never reached closed CAS")
		default:
			runtime.Gosched()
		}
	}
	return resume, join
}

func TestManagerDirectRetirementCloseJoinsAdmission(t *testing.T) {
	for _, mode := range []string{"direct", "Release"} {
		for _, pins := range []string{"nil", "registry"} {
			t.Run(mode+"/"+pins, func(t *testing.T) {
				manager, file := directRetirementManager(t, pins == "registry")
				call := func() error { return manager.RemoveSegment(file.ID) }
				if mode == "Release" {
					set := manager.CurrentSetNoRefresh()
					if err := manager.MarkZombie(file.ID); err != nil {
						t.Fatal(err)
					}
					call = func() error { return manager.Release(set) }
				}
				resume, join := directRetirementPausedCall(t, file, call)
				closeDone := make(chan error, 1)
				go func() { closeDone <- manager.Close() }()
				closeJoined := false
				t.Cleanup(func() {
					resume()
					_ = join()
					if !closeJoined {
						select {
						case <-closeDone:
						case <-time.After(5 * time.Second):
							t.Error("Manager.Close cleanup did not join")
						}
					}
				})
				timeout := time.NewTimer(5 * time.Second)
				defer timeout.Stop()
				for {
					manager.mu.RLock()
					closing := manager.closing
					manager.mu.RUnlock()
					if closing {
						break
					}
					select {
					case <-timeout.C:
						t.Fatal("Manager.Close never closed deletion admission")
					default:
						runtime.Gosched()
					}
				}
				// A bounded negative observation at a real blocked Close proves
				// shutdown joins ownership completion rather than the early CAS.
				select {
				case err := <-closeDone:
					closeJoined = true
					t.Fatalf("Manager.Close returned before admitted physical deletion: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
				if admitted, err := manager.RemoveSegmentIfUnpinned(file.ID); admitted || err != nil {
					t.Fatalf("closing admitted a new direct deletion: (%t,%v)", admitted, err)
				}
				resume()
				if err := join(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-closeDone:
					closeJoined = true
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Manager.Close did not join completed deletion")
				}
				directRetirementAssertState(t, manager, file, false)
			})
		}
	}
}

func TestManagerDirectRetirementJoinsExternalClose(t *testing.T) {
	for _, mode := range []string{"File.Close", "EvictSegment"} {
		t.Run(mode, func(t *testing.T) {
			manager, file := directRetirementManager(t, false)
			identity := retirementIdentityAtPath(t, file.Path)
			// Cache the immutable identity before an external closer marks the
			// handle closed. Hold manager.mu only for this capture, never a wait.
			manager.mu.Lock()
			_, err := manager.prepareRetirementIdentityLocked(file)
			manager.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			call := file.Close
			if mode == "EvictSegment" {
				call = func() error { return manager.EvictSegment(file.ID) }
			}
			resume, join := directRetirementPausedCall(t, file, call)
			deleteDone := make(chan error, 1)
			go func() {
				_, err := closeAndRemoveStableSegmentFileResult(file, identity)
				deleteDone <- err
			}()
			deleteJoined := false
			t.Cleanup(func() {
				resume()
				_ = join()
				if !deleteJoined {
					select {
					case <-deleteDone:
					case <-time.After(5 * time.Second):
						t.Error("external-close physical deletion did not join")
					}
				}
			})
			select {
			case err := <-deleteDone:
				deleteJoined = true
				t.Fatalf("physical deletion passed an incomplete external Close: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			if _, err := os.Stat(file.Path); err != nil {
				t.Fatalf("physical deletion ran before actual Close completed: %v", err)
			}
			resume()
			if err := join(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-deleteDone:
				deleteJoined = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("physical deletion did not join after external Close")
			}
		})
	}
}
