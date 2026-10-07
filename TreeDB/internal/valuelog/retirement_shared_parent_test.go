package valuelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Additive: uses only APIs and observation seams present in f344e0ef.
func sharedParentFixture(t *testing.T, registry bool) (*Manager, []*File) {
	t.Helper()
	root := t.TempDir()
	dirs := []string{filepath.Join(root, "primary"), filepath.Join(root, "additional")}
	var ids []uint32
	for lane, dir := range dirs {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		for seq := 1; seq <= 8; seq++ {
			ids = append(ids, writeTestSegment(t, dir, uint32(lane), uint32(seq), 1, []byte("shared retirement parent")))
		}
	}
	var pins *rootpublication.IdentityPinRegistry
	if registry {
		pins = rootpublication.NewIdentityPinRegistry()
	}
	m, err := NewManagerWithStableResourcePinRegistry(dirs[0], pins)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if err := m.AddScanDir(dirs[1]); err != nil {
		t.Fatal(err)
	}
	var files []*File
	for _, id := range ids {
		if m.files[id] == nil {
			t.Fatalf("missing registered file %d", id)
		}
		files = append(files, m.files[id])
	}
	return m, files
}

func TestManagerSharedRetirementParentDescriptorBound(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			m, files := sharedParentFixture(t, registry)
			snapshot := m.CurrentSetNoRefresh()
			defer func() {
				if err := m.Release(snapshot); err != nil {
					t.Error(err)
				}
			}()
			for _, f := range files {
				if err := m.MarkZombie(f.ID); err != nil {
					t.Fatal(err)
				}
				if err := m.MarkZombie(f.ID); err != nil {
					t.Fatal(err)
				}
			}
			// Keep all operation borrows live simultaneously: Fd cannot be recycled.
			descriptors := map[uintptr]bool{}
			physical := map[rootpublication.StableIdentity]bool{}
			for _, f := range files {
				parent, release, err := borrowRetirementParent(f)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := release(); err != nil {
						t.Error(err)
					}
				}()
				if _, err := parent.Stat(); err != nil {
					t.Fatal(err)
				}
				identity, err := rootpublication.StableIdentityFromFile(parent)
				if err != nil {
					t.Fatal(err)
				}
				identity.Generation = 0
				descriptors[parent.Fd()] = true
				physical[identity] = true
			}
			if len(physical) != 2 {
				t.Fatalf("fixture has %d physical parents; want 2", len(physical))
			}
			if len(descriptors) != len(physical) {
				t.Fatalf("shared-parent FD bound violated: %d retired segments retain %d parent descriptors for %d physical directories", len(files), len(descriptors), len(physical))
			}
		})
	}
}

func sharedParentBorrow(t *testing.T, f *File) (*os.File, func() error) {
	t.Helper()
	parent, release, err := borrowRetirementParent(f)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	var releaseErr error
	joined := func() error { once.Do(func() { releaseErr = release() }); return releaseErr }
	t.Cleanup(func() {
		if err := joined(); err != nil {
			t.Error(err)
		}
	})
	return parent, joined
}
func sharedParentLive(t *testing.T, parent *os.File) {
	t.Helper()
	if _, err := parent.Stat(); err != nil {
		t.Fatalf("shared parent closed before final borrow: %v", err)
	}
}
func sharedParentClosed(t *testing.T, parent *os.File) {
	t.Helper()
	if parent.Fd() != ^uintptr(0) {
		t.Fatal("shared parent retained after final release")
	}
}

func TestManagerSharedRetirementParentSnapshotLifetime(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			m, files := sharedParentFixture(t, registry)
			full := m.CurrentSetNoRefresh()
			keep := m.CurrentSubsetNoRefresh(map[uint32]struct{}{files[0].ID: {}, files[8].ID: {}})
			for _, f := range files {
				if err := m.MarkZombie(f.ID); err != nil {
					t.Fatal(err)
				}
			}
			a, releaseA := sharedParentBorrow(t, files[0])
			b, releaseB := sharedParentBorrow(t, files[8])
			if a.Fd() == b.Fd() {
				t.Fatal("two physical parents share a descriptor")
			}
			if err := releaseA(); err != nil {
				t.Fatal(err)
			}
			if err := releaseB(); err != nil {
				t.Fatal(err)
			}
			if err := m.Release(full); err != nil {
				t.Fatal(err)
			}
			if m.retiredCount != 2 || len(m.files) != 2 {
				t.Fatal("partial snapshot release lost sibling owners")
			}
			sharedParentLive(t, a)
			sharedParentLive(t, b)
			if m.stableResourcePins != nil && m.stableResourcePins.ActiveIdentities() != 2 {
				t.Fatal("partial release lost registry observations")
			}
			a, releaseA = sharedParentBorrow(t, files[0])
			b, releaseB = sharedParentBorrow(t, files[8])
			if err := m.Release(keep); err != nil {
				t.Fatal(err)
			}
			if m.retiredCount != 0 || len(m.files) != 0 {
				t.Fatal("final snapshot release retained owners")
			}
			sharedParentLive(t, a)
			sharedParentLive(t, b)
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			sharedParentLive(t, a)
			sharedParentLive(t, b)
			if err := releaseA(); err != nil {
				t.Fatal(err)
			}
			sharedParentClosed(t, a)
			sharedParentLive(t, b)
			if err := releaseB(); err != nil {
				t.Fatal(err)
			}
			sharedParentClosed(t, b)
			if err := m.Close(); err != nil {
				t.Fatalf("repeated close: %v", err)
			}
		})
	}
}

func TestManagerSharedRetirementParentBorrowedEviction(t *testing.T) {
	for _, registry := range []bool{false, true} {
		for _, mode := range []string{"EvictSegment", "Close"} {
			name := "nil"
			if registry {
				name = "registry"
			}
			t.Run(name+"/"+mode, func(t *testing.T) {
				m, files := sharedParentFixture(t, registry)
				for _, f := range files {
					if err := m.MarkZombie(f.ID); err != nil {
						t.Fatal(err)
					}
				}
				parent, release := sharedParentBorrow(t, files[0])
				if mode == "Close" {
					if err := m.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					for _, f := range files {
						if err := m.EvictSegment(f.ID); err != nil {
							t.Fatal(err)
						}
						sharedParentLive(t, parent)
					}
				}
				sharedParentLive(t, parent)
				if err := release(); err != nil {
					t.Fatal(err)
				}
				sharedParentClosed(t, parent)
				if err := m.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestManagerSharedRetirementParentBorrowOnlyReuse(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			m, files := sharedParentFixture(t, registry)
			if err := m.MarkZombie(files[0].ID); err != nil {
				t.Fatal(err)
			}
			parent, release := sharedParentBorrow(t, files[0])
			if err := m.EvictSegment(files[0].ID); err != nil {
				t.Fatal(err)
			}
			sharedParentLive(t, parent)
			if err := m.MarkZombie(files[1].ID); err != nil {
				t.Fatal(err)
			}
			sibling, releaseSibling := sharedParentBorrow(t, files[1])
			if parent.Fd() != sibling.Fd() {
				t.Fatal("borrow-only indexed parent acquired a second descriptor")
			}
			if err := releaseSibling(); err != nil {
				t.Fatal(err)
			}
			if err := release(); err != nil {
				t.Fatal(err)
			}
			sharedParentLive(t, sibling)
			if err := m.EvictSegment(files[1].ID); err != nil {
				t.Fatal(err)
			}
			sharedParentClosed(t, sibling)
		})
	}
}

func TestManagerSharedRetirementParentPoolHitRebound(t *testing.T) {
	for _, registry := range []bool{false, true} {
		for _, mode := range []string{"MarkZombie", "RemoveSegment"} {
			name := "nil"
			if registry {
				name = "registry"
			}
			t.Run(name+"/"+mode, func(t *testing.T) {
				m, files := sharedParentFixture(t, registry)
				if err := m.MarkZombie(files[0].ID); err != nil {
					t.Fatal(err)
				}
				parent, release := sharedParentBorrow(t, files[0])
				defer func() {
					if err := release(); err != nil {
						t.Error(err)
					}
				}()
				dir := filepath.Dir(files[1].Path)
				saved := dir + "-original"
				if err := retirementTestRenameParent(dir, saved, files[:8]); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(filepath.Join(saved, filepath.Base(files[1].Path)), files[1].Path); err != nil {
					t.Fatal(err)
				}
				call := func() error {
					if mode == "MarkZombie" {
						return m.MarkZombie(files[1].ID)
					}
					return m.RemoveSegment(files[1].ID)
				}
				if err := call(); !errors.Is(err, rootpublication.ErrResourceConflict) {
					t.Fatalf("pool hit accepted rebound original parent: %v", err)
				}
				if files[1].closed.Load() || files[1].IsZombie.Load() || m.files[files[1].ID] != files[1] || m.retiredCount != 1 {
					t.Fatal("pool-hit refusal changed original owner")
				}
				sharedParentLive(t, parent)
				if err := os.Remove(files[1].Path); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(dir); err != nil {
					t.Fatal(err)
				}
				if err := retirementTestRenameParent(saved, dir, files[:8]); err != nil {
					t.Fatal(err)
				}
				if err := m.MarkZombie(files[1].ID); err != nil {
					t.Fatal(err)
				}
				sibling, done := sharedParentBorrow(t, files[1])
				if parent.Fd() != sibling.Fd() {
					t.Fatal("restored sibling did not share retained exact parent")
				}
				if err := done(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestManagerSharedRetirementParentConcurrentLastRelease(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			for attempt := 0; attempt < 8; attempt++ {
				m, files := sharedParentFixture(t, registry)
				if err := m.MarkZombie(files[0].ID); err != nil {
					t.Fatal(err)
				}
				_, release := sharedParentBorrow(t, files[0])
				if err := m.EvictSegment(files[0].ID); err != nil {
					t.Fatal(err)
				}
				start := make(chan struct{})
				joined := make(chan error, 2)
				go func() { <-start; joined <- release() }()
				go func() { <-start; joined <- m.MarkZombie(files[1].ID) }()
				close(start)
				first, second := <-joined, <-joined
				if first != nil || second != nil {
					t.Fatalf("concurrent last release/admission: %v %v", first, second)
				}
				parent, done := sharedParentBorrow(t, files[1])
				sharedParentLive(t, parent)
				if err := done(); err != nil {
					t.Fatal(err)
				}
				// Duplicate operation completion is idempotent and cannot underflow refs.
				if err := done(); err != nil {
					t.Fatal(err)
				}
				if err := release(); err != nil {
					t.Fatal(err)
				}
				if err := m.EvictSegment(files[1].ID); err != nil {
					t.Fatal(err)
				}
				sharedParentClosed(t, parent)
				if err := m.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestManagerSharedRetirementParentFailedSibling(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			m, files := sharedParentFixture(t, registry)
			for _, file := range files[:2] {
				if err := m.MarkZombie(file.ID); err != nil {
					t.Fatal(err)
				}
			}
			parent, done := sharedParentBorrow(t, files[0])
			if err := done(); err != nil {
				t.Fatal(err)
			}
			expected := errors.New("shared-parent injected unlink failure")
			failure := func() error {
				previous := removeSegmentPath
				defer func() { removeSegmentPath = previous }()
				removeSegmentPath = func(string, func(string) error) error { return expected }
				return m.RemoveSegment(files[0].ID)
			}()
			if !errors.Is(failure, expected) {
				t.Fatalf("failed unlink did not retain error: %v", failure)
			}
			if err := m.RemoveSegment(files[1].ID); err != nil {
				t.Fatal(err)
			}
			sharedParentLive(t, parent)
			if m.files[files[0].ID] != files[0] || m.files[files[1].ID] != nil {
				t.Fatal("sibling success changed failed retirement owner")
			}
			if err := m.RemoveSegment(files[0].ID); err != nil {
				t.Fatal(err)
			}
			sharedParentClosed(t, parent)
		})
	}
}

func TestManagerSharedRetirementParentMapShrink(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			var dirs []string
			var ids []uint32
			for seq := 1; seq <= 8; seq++ {
				dir := filepath.Join(root, fmt.Sprintf("parent-%d", seq))
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				dirs = append(dirs, dir)
				ids = append(ids, writeTestSegment(t, dir, 0, uint32(seq), 1, []byte("map shrink owner")))
			}
			var pins *rootpublication.IdentityPinRegistry
			if registry {
				pins = rootpublication.NewIdentityPinRegistry()
			}
			m, err := NewManagerWithStableResourcePinRegistry(dirs[0], pins)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			for _, dir := range dirs[1:] {
				if err := m.AddScanDir(dir); err != nil {
					t.Fatal(err)
				}
			}
			for _, id := range ids {
				if err := m.MarkZombie(id); err != nil {
					t.Fatal(err)
				}
			}
			if len(m.retirementParents.entries) != 8 {
				t.Fatal("distinct physical parents were coalesced")
			}
			for _, id := range ids {
				if err := m.EvictSegment(id); err != nil {
					t.Fatal(err)
				}
				live := len(m.retirementParents.entries)
				if m.retirementParents.highWater > 2*live {
					t.Fatal("pool map retained unbounded historical high-water")
				}
			}
			if m.retirementParents.entries != nil || m.retirementParents.highWater != 0 {
				t.Fatal("empty pool retained map capacity")
			}
		})
	}
}

func TestManagerSharedRetirementParentDistinctHardlinkParents(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			a := filepath.Join(root, "a")
			b := filepath.Join(root, "b")
			if err := os.Mkdir(a, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(b, 0700); err != nil {
				t.Fatal(err)
			}
			idA := writeTestSegment(t, a, 0, 1, 1, []byte("same child two parents"))
			pathA := filepath.Join(a, "value-l0-000001.log")
			pathB := filepath.Join(b, "value-l1-000001.log")
			if err := os.Link(pathA, pathB); err != nil {
				t.Fatal(err)
			}
			idB, err := EncodeFileID(1, 1)
			if err != nil {
				t.Fatal(err)
			}
			var pins *rootpublication.IdentityPinRegistry
			if registry {
				pins = rootpublication.NewIdentityPinRegistry()
			}
			m, err := NewManagerWithStableResourcePinRegistry(a, pins)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if err := m.AddScanDir(b); err != nil {
				t.Fatal(err)
			}
			for _, id := range []uint32{idA, idB} {
				if err := m.MarkZombie(id); err != nil {
					t.Fatal(err)
				}
			}
			first, releaseFirst := sharedParentBorrow(t, m.files[idA])
			second, releaseSecond := sharedParentBorrow(t, m.files[idB])
			if !rootpublication.SamePhysicalIdentity(identityPinTestIdentity(t, pathA), identityPinTestIdentity(t, pathB)) {
				t.Fatal("fixture does not share a physical child")
			}
			if first.Fd() == second.Fd() || len(m.retirementParents.entries) != 2 {
				t.Fatal("shared child incorrectly coalesced distinct physical parent handles")
			}
			if err := releaseFirst(); err != nil {
				t.Fatal(err)
			}
			if err := releaseSecond(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
