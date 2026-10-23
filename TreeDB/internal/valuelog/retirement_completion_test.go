package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Completing one Manager's deletion does not grant a different Manager
// permission to reinterpret its original observation as a replacement file.
// Proved absence completes that observation automatically; a replacement
// requires refusal and an explicit non-destructive owner eviction.
func TestManagerRetirementCompletionOtherOwner(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		name := "absent"
		if replacement {
			name = "replacement"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			id, path := writeIdentityPinTestSegment(t, dir)
			registry := rootpublication.NewIdentityPinRegistry()
			first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := first.Close(); err != nil {
					t.Error(err)
				}
			})
			second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := second.Close(); err != nil {
					t.Error(err)
				}
			})
			identity := retirementIdentityAtPath(t, path)
			pin := stableManagerToken(t, second, id)
			removed, err := first.RemoveSegmentIfUnpinned(id)
			if err != nil || removed {
				pin.Release()
				t.Fatalf("other owner's retained physical pin failed to exclude deletion: removed=%t err=%v", removed, err)
			}
			if _, err := os.Stat(path); err != nil {
				pin.Release()
				t.Fatal(err)
			}
			pin.Release()
			for _, manager := range []*Manager{first, second} {
				if err := manager.MarkZombie(id); err != nil {
					t.Fatal(err)
				}
			}
			preparedDir := t.TempDir()
			writeTestSegment(t, preparedDir, 1, 1, 2, []byte("successor must remain intact"))
			preparedPath := filepath.Join(preparedDir, filepath.Base(path))
			preparedIdentity := retirementIdentityAtPath(t, preparedPath)
			if rootpublication.SamePhysicalIdentity(identity, preparedIdentity) {
				t.Fatal("replacement fixture reused the still-open original inode")
			}
			want, err := os.ReadFile(preparedPath)
			if err != nil {
				t.Fatal(err)
			}
			removed, err = first.RemoveSegmentIfUnpinned(id)
			if err != nil || !removed {
				t.Fatalf("first owner deletion: removed=%t err=%v", removed, err)
			}
			if replacement {
				if err := os.Rename(preparedPath, path); err != nil {
					t.Fatal(err)
				}
			}
			// A mismatched expected identity must not be reconciled even when
			// the original namespace has become absent.
			if err := second.RemoveSegmentExpectedIdentity(id, preparedIdentity); !errors.Is(err, rootpublication.ErrResourceConflict) || !second.IsTrackedZombie(id) {
				t.Fatalf("mismatched expected identity lost its refusal: %v", err)
			}
			removed, err = second.RemoveSegmentIfUnpinned(id)
			if replacement {
				if removed || !errors.Is(err, rootpublication.ErrResourceConflict) {
					t.Fatalf("stale other owner acquired deletion authority: removed=%t err=%v", removed, err)
				}
				if !second.IsTrackedZombie(id) || registry.ObserverCount(identity) != 1 {
					t.Fatal("refusal silently dropped the stale original observation")
				}
				if err := second.EvictSegment(id); err != nil {
					t.Fatalf("explicit non-destructive owner eviction: %v", err)
				}
			} else if removed || err != nil {
				t.Fatalf("proved-absent original cleanup: removed=%t err=%v", removed, err)
			}
			if second.IsTrackedZombie(id) || registry.ObserverCount(identity) != 0 {
				t.Fatal("original cleanup retained the old owner")
			}
			if replacement {
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, want) || !rootpublication.SamePhysicalIdentity(preparedIdentity, retirementIdentityAtPath(t, path)) {
					t.Fatalf("stale owner touched the canonical successor: %v", err)
				}
				if err := second.Refresh(); err != nil {
					t.Fatalf("new successor registration after explicit eviction: %v", err)
				}
				if !second.HasSegment(id) {
					t.Fatal("fresh registration omitted the successor")
				}
			} else if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("original cleanup recreated a deleted name: %v", err)
			}
		})
	}
}

func TestManagerRetirementCompletionOtherOwnerCallers(t *testing.T) {
	for _, mode := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentForce", "Release", "Retry"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			id, path := writeIdentityPinTestSegment(t, dir)
			registry := rootpublication.NewIdentityPinRegistry()
			first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := first.Close(); err != nil {
					t.Error(err)
				}
			})
			second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := second.Close(); err != nil {
					t.Error(err)
				}
			})
			identity := retirementIdentityAtPath(t, path)
			var set *Set
			if mode == "Release" {
				set = second.CurrentSetNoRefresh()
			}
			for _, manager := range []*Manager{first, second} {
				if err := manager.MarkZombie(id); err != nil {
					t.Fatal(err)
				}
			}
			if removed, err := first.RemoveSegmentIfUnpinned(id); err != nil || !removed {
				t.Fatalf("first owner deletion: removed=%t err=%v", removed, err)
			}
			switch mode {
			case "Release":
				err = second.Release(set)
			case "Retry":
				second.mu.RLock()
				file := second.files[id]
				second.mu.RUnlock()
				second.startZombieRetry(file)
				second.retryWorkers.Wait()
			case "RemoveSegmentExpectedIdentity":
				err = second.RemoveSegmentExpectedIdentity(id, identity)
			case "RemoveSegmentForce":
				err = second.RemoveSegmentForce(id)
			default:
				err = second.RemoveSegment(id)
			}
			if err != nil || second.IsTrackedZombie(id) || registry.ObserverCount(identity) != 0 {
				t.Fatalf("%s failed to complete proved-absent original: %v", mode, err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("deleted namespace: %v", err)
			}
		})
	}
}

func TestManagerRetirementCompletionUnpreparedOtherOwner(t *testing.T) {
	for _, mode := range []string{"RemoveSegmentIfUnpinned", "MarkZombie", "MarkZombieIfTracked", "RemoveSegmentExpectedIdentity", "RemoveSegmentForce"} {
		for _, namespace := range []string{"absent", "replacement", "parent-rebound"} {
			t.Run(mode+"/"+namespace, func(t *testing.T) {
				root := t.TempDir()
				dir := filepath.Join(root, "segments")
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				id, path := writeIdentityPinTestSegment(t, dir)
				registry := rootpublication.NewIdentityPinRegistry()
				first, err := NewManagerWithStableResourcePinRegistry(dir, registry)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := first.Close(); err != nil {
						t.Error(err)
					}
				})
				second, err := NewManagerWithStableResourcePinRegistry(dir, registry)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := second.Close(); err != nil {
						t.Error(err)
					}
				})
				second.mu.RLock()
				original := second.files[id]
				second.mu.RUnlock()
				if original == nil || original.retirementIdentity != (rootpublication.StableIdentity{}) {
					t.Fatal("fixture already prepared second owner's retirement")
				}
				identity := retirementIdentityAtPath(t, path)
				preparedDir := t.TempDir()
				writeTestSegment(t, preparedDir, 1, 1, 2, []byte("unprepared owner must not touch successor"))
				preparedPath := filepath.Join(preparedDir, filepath.Base(path))
				preparedIdentity := retirementIdentityAtPath(t, preparedPath)
				want, err := os.ReadFile(preparedPath)
				if err != nil {
					t.Fatal(err)
				}
				if rootpublication.SamePhysicalIdentity(identity, preparedIdentity) {
					t.Fatal("replacement reused still-live identity")
				}
				if removed, err := first.RemoveSegmentIfUnpinned(id); err != nil || !removed {
					t.Fatalf("first deletion: removed=%t err=%v", removed, err)
				}
				// A stale observation still keeps the terminal registry state,
				// but must never permit a new physical pin.
				if pin, err := registry.Pin(identity); pin != nil || !errors.Is(err, rootpublication.ErrResourceConflict) {
					if pin != nil {
						pin.Release()
					}
					t.Fatalf("late pin revived deleted identity: %v", err)
				}
				if namespace == "parent-rebound" {
					if err := os.Rename(dir, dir+"-original"); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(dir, 0700); err != nil {
						t.Fatal(err)
					}
				}
				if namespace == "replacement" {
					if err := os.Rename(preparedPath, path); err != nil {
						t.Fatal(err)
					}
				}
				call := func() error {
					switch mode {
					case "MarkZombie":
						if err := second.MarkZombie(id); err != nil {
							return err
						}
					case "MarkZombieIfTracked":
						tracked, marked, err := second.MarkZombieIfTracked(id)
						if err != nil {
							return err
						}
						if !tracked || !marked {
							t.Fatal("unprepared original was not marked")
						}
					case "RemoveSegmentExpectedIdentity":
						return second.RemoveSegmentExpectedIdentity(id, identity)
					case "RemoveSegmentForce":
						return second.RemoveSegmentForce(id)
					}
					removed, err := second.RemoveSegmentIfUnpinned(id)
					if removed {
						t.Fatal("stale owner reported a new physical unlink")
					}
					return err
				}
				err = call()
				second.mu.RLock()
				tracked := second.files[id]
				second.mu.RUnlock()
				if namespace == "absent" {
					if err != nil || tracked != nil || registry.ObserverCount(identity) != 0 {
						t.Fatalf("unprepared proved-absent cleanup refused or retained owner: %v", err)
					}
				} else {
					if !errors.Is(err, rootpublication.ErrResourceConflict) || tracked != original || original.closed.Load() || !original.stableObserved {
						t.Fatalf("namespace refusal lost original owner: %v", err)
					}
					if err := second.EvictSegment(id); err != nil {
						t.Fatal(err)
					}
				}
				if namespace == "replacement" {
					got, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(got, want) || !rootpublication.SamePhysicalIdentity(preparedIdentity, retirementIdentityAtPath(t, path)) {
						t.Fatalf("replacement changed: %v", err)
					}
				} else if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("absent child recreated: %v", err)
				}
			})
		}
	}
}
