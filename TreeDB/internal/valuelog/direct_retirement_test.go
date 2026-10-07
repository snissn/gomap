package valuelog

import (
	"bytes"
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
					removeSegmentPath = func(string, func(string) error) error { return wantErr }
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
				removeSegmentPath = func(string, func(string) error) error { return wantErr }
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
	for _, registry := range []bool{false, true} {
		name := "nil-registry"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			manager, file := directRetirementManager(t, registry)
			identity := retirementIdentityAtPath(t, file.Path)
			var replacement *File
			var replacementBytes []byte
			var replacementIdentity rootpublication.StableIdentity
			err := func() error {
				originalRemove := removeSegmentPath
				defer func() { removeSegmentPath = originalRemove }()
				removeSegmentPath = func(path string, remove func(string) error) error {
					if err := manager.EvictSegment(file.ID); !errors.Is(err, ErrFilePinned) {
						return errors.New("public eviction did not refuse admitted deletion")
					}
					// The old identity is already quarantined. Public eviction cannot
					// replace an admitted owner; privately inject that otherwise
					// unreachable state to exercise the finalizer's exact-pointer
					// check. Balance the old zombie and observation, releasing its
					// parent outside mu; the real active borrow keeps it alive.
					manager.mu.Lock()
					if manager.files[file.ID] != file || file.deletionAdmissions != 1 {
						manager.mu.Unlock()
						return errors.New("same-pointer fixture lost admitted owner")
					}
					parent := manager.forgetSegmentLocked(file)
					unobserveErr := manager.unobserveStableFileLocked(file)
					manager.mu.Unlock()
					if err := errors.Join(unobserveErr, closeRetirementParent(parent)); err != nil {
						return err
					}
					writeTestSegment(t, filepath.Dir(file.Path), 0, 1, 2, []byte("new explicit owner"))
					if err := manager.RegisterSegment(file.Path, file.ID); err != nil {
						return err
					}
					manager.mu.RLock()
					replacement = manager.files[file.ID]
					manager.mu.RUnlock()
					replacementIdentity = retirementIdentityAtPath(t, file.Path)
					var readErr error
					replacementBytes, readErr = os.ReadFile(file.Path)
					if readErr != nil {
						return readErr
					}
					return originalRemove(path, remove)
				}
				return manager.RemoveSegmentExpectedIdentity(file.ID, identity)
			}()
			if err != nil || replacement == nil || replacement == file {
				t.Fatalf("same-pointer fixture failed: %v", err)
			}
			manager.mu.RLock()
			tracked, count := manager.files[file.ID], manager.retiredCount
			observed, oldAdmissions := replacement.stableObserved, file.deletionAdmissions
			manager.mu.RUnlock()
			if tracked != replacement || count != 0 || !manager.HasSegment(file.ID) {
				t.Fatal("successful old deletion forgot a different current owner")
			}
			if observed != registry || oldAdmissions != 0 {
				t.Fatalf("replacement observation=%t old admissions=%d", observed, oldAdmissions)
			}
			gotBytes, err := os.ReadFile(file.Path)
			if err != nil || !bytes.Equal(gotBytes, replacementBytes) || !rootpublication.SamePhysicalIdentity(replacementIdentity, retirementIdentityAtPath(t, file.Path)) {
				t.Fatalf("old finalization changed replacement bytes or physical identity: %v", err)
			}
		})
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

func TestManagerDirectRetirementParentLifetime(t *testing.T) {
	for _, mode := range []string{"delete", "EvictSegment", "Close", "borrowed-EvictSegment"} {
		t.Run(mode, func(t *testing.T) {
			manager, file := directRetirementManager(t, false)
			if err := manager.MarkZombie(file.ID); err != nil {
				t.Fatal(err)
			}
			retained := file.retirementParent
			if retained == nil {
				t.Fatal("retirement did not retain its physical parent")
			}
			parent := retained.file
			var release func() error
			if mode == "borrowed-EvictSegment" {
				borrowed, done, err := borrowRetirementParent(file)
				if err != nil || borrowed != parent {
					t.Fatalf("retirement parent borrow: %v", err)
				}
				release = done
				t.Cleanup(func() {
					if release != nil {
						_ = release()
					}
				})
			}
			var err error
			switch mode {
			case "delete":
				err = manager.deleteZombieFile(file)
			case "Close":
				err = manager.Close()
			default:
				err = manager.EvictSegment(file.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if release != nil {
				if _, err := parent.Stat(); err != nil {
					t.Fatalf("eviction closed an admitted borrow: %v", err)
				}
				err := release()
				release = nil
				if err != nil {
					t.Fatal(err)
				}
			}
			if parent.Fd() != ^uintptr(0) {
				t.Fatal("joined retirement parent still has a valid descriptor")
			}
			file.retirementParentMu.Lock()
			released := file.retirementParentReleased && file.retirementParent == nil
			file.retirementParentMu.Unlock()
			if !released {
				t.Fatal("released retirement parent remains owned or borrowed")
			}
			if mode == "Close" {
				if err := manager.MarkZombie(file.ID); err != nil {
					t.Fatal(err)
				}
				tracked, marked, err := manager.MarkZombieIfTracked(file.ID)
				if err != nil || tracked || marked {
					t.Fatalf("closed zombie admission: %t %t %v", tracked, marked, err)
				}
				if err := manager.RegisterSegment(file.Path, file.ID); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("closed registration admitted: %v", err)
				}
				if err := manager.EvictSegment(file.ID); err != nil {
					t.Fatal(err)
				}
				for _, api := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentForce"} {
					if err := retirementIdentityRemove(manager, file, file.retirementIdentity, api); err != nil {
						t.Fatalf("closed direct admission %s: %v", api, err)
					}
				}
				if admitted, err := manager.RemoveSegmentIfUnpinned(file.ID); err != nil || admitted {
					t.Fatalf("closed unpinned admission: %t %v", admitted, err)
				}
				if err := manager.Close(); err != nil {
					t.Fatalf("repeated Close completion: %v", err)
				}
			}
		})
	}
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
			_, parentToClose, err := manager.prepareRetirementIdentityLocked(file)
			manager.mu.Unlock()
			err = errors.Join(err, closeRetirementParent(parentToClose))
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
