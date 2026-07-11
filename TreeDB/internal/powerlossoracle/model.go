// Package powerlossoracle provides deterministic test infrastructure for
// TreeDB power-loss tests. It is not imported by production packages.
package powerlossoracle

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CutPoint is a stable identifier emitted by the durability harness.
type CutPoint string

const (
	BeforeDependencyAppend     CutPoint = "before-dependency-append"
	AfterDependencyAppend      CutPoint = "after-dependency-append"
	AfterUserspaceFlush        CutPoint = "after-userspace-flush"
	AfterDependencyFileSync    CutPoint = "after-dependency-file-sync"
	AfterNewFileDirectorySync  CutPoint = "after-new-file-directory-sync"
	AfterIndexDataSync         CutPoint = "after-index-data-sync"
	BeforePublicationSealWrite CutPoint = "before-publication-seal-write"
	AfterPublicationSealWrite  CutPoint = "after-publication-seal-write"
	BeforeMetaWrite            CutPoint = "before-meta-write"
	AfterMetaWrite             CutPoint = "after-meta-write"
	AfterMetaSync              CutPoint = "after-meta-sync"
	BeforeAppliedLSNAdvance    CutPoint = "before-applied-lsn-advance"
	AfterAppliedLSNAdvance     CutPoint = "after-applied-lsn-advance"
	BeforeWALOrAssetUnlink     CutPoint = "before-wal-or-asset-unlink"
	AfterWALOrAssetUnlink      CutPoint = "after-wal-or-asset-unlink"
	AfterDeletionDirectorySync CutPoint = "after-deletion-directory-sync"
)

// CutPoints is the canonical deterministic enumeration order.
var CutPoints = []CutPoint{
	BeforeDependencyAppend,
	AfterDependencyAppend,
	AfterUserspaceFlush,
	AfterDependencyFileSync,
	AfterNewFileDirectorySync,
	AfterIndexDataSync,
	BeforePublicationSealWrite,
	AfterPublicationSealWrite,
	BeforeMetaWrite,
	AfterMetaWrite,
	AfterMetaSync,
	BeforeAppliedLSNAdvance,
	AfterAppliedLSNAdvance,
	BeforeWALOrAssetUnlink,
	AfterWALOrAssetUnlink,
	AfterDeletionDirectorySync,
}

type inode struct {
	volatile []byte
	stable   []byte
}

// Model separates the process-visible namespace and bytes from the bytes and
// names that survive power loss. Names reference inode identities so that file
// sync followed by rename and directory sync has realistic semantics.
type Model struct {
	nextID       uint64
	inodes       map[uint64]*inode
	volatile     map[string]uint64
	stable       map[string]uint64
	volatileDirs map[string]struct{}
	stableDirs   map[string]struct{}
	trace        []string
}

// Capture imports a real directory as an initially stable image.
func Capture(root string) (*Model, error) {
	m := newModel()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = clean(rel)
		if entry.IsDir() {
			m.volatileDirs[rel] = struct{}{}
			m.stableDirs[rel] = struct{}{}
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("powerlossoracle: unsupported entry %q (%s)", rel, entry.Type())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		id := m.allocate(data, data)
		m.volatile[rel] = id
		m.stable[rel] = id
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

func newModel() *Model {
	return &Model{
		inodes:       make(map[uint64]*inode),
		volatile:     make(map[string]uint64),
		stable:       make(map[string]uint64),
		volatileDirs: map[string]struct{}{".": {}},
		stableDirs:   map[string]struct{}{".": {}},
	}
}

func (m *Model) allocate(volatile, stable []byte) uint64 {
	m.nextID++
	m.inodes[m.nextID] = &inode{volatile: clone(volatile), stable: clone(stable)}
	return m.nextID
}

// Clone returns a deep copy suitable for evaluating another crash cut.
func (m *Model) Clone() *Model {
	out := newModel()
	out.nextID = m.nextID
	for id, node := range m.inodes {
		out.inodes[id] = &inode{volatile: clone(node.volatile), stable: clone(node.stable)}
	}
	for path, id := range m.volatile {
		out.volatile[path] = id
	}
	for path, id := range m.stable {
		out.stable[path] = id
	}
	for path := range m.volatileDirs {
		out.volatileDirs[path] = struct{}{}
	}
	for path := range m.stableDirs {
		out.stableDirs[path] = struct{}{}
	}
	out.trace = append([]string(nil), m.trace...)
	return out
}

// Overlay makes another real directory the volatile process-visible state.
// No stable bytes or names change until SyncFile or SyncDir is called.
func (m *Model) Overlay(root string) error {
	seenFiles := make(map[string]struct{})
	seenDirs := map[string]struct{}{".": {}}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = clean(rel)
		if entry.IsDir() {
			seenDirs[rel] = struct{}{}
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("powerlossoracle: unsupported entry %q (%s)", rel, entry.Type())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		seenFiles[rel] = struct{}{}
		if id, ok := m.volatile[rel]; ok {
			m.inodes[id].volatile = clone(data)
		} else {
			m.volatile[rel] = m.allocate(data, nil)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for path := range m.volatile {
		if _, ok := seenFiles[path]; !ok {
			delete(m.volatile, path)
		}
	}
	m.volatileDirs = seenDirs
	m.trace = append(m.trace, "overlay:"+root)
	return nil
}

// Write dirties process-visible file bytes without making them stable.
func (m *Model) Write(path string, data []byte) error {
	path = clean(path)
	id, ok := m.volatile[path]
	if !ok {
		return fmt.Errorf("powerlossoracle: write missing file %q", path)
	}
	m.inodes[id].volatile = clone(data)
	m.trace = append(m.trace, "write:"+path)
	return nil
}

// Create creates a volatile file and any volatile parent directories.
func (m *Model) Create(path string, data []byte) error {
	path = clean(path)
	if path == "." {
		return errors.New("powerlossoracle: cannot create root")
	}
	if _, ok := m.volatile[path]; ok {
		return fmt.Errorf("powerlossoracle: file already exists %q", path)
	}
	m.ensureVolatileParents(path)
	m.volatile[path] = m.allocate(data, nil)
	m.trace = append(m.trace, "create:"+path)
	return nil
}

// Rename changes only the volatile namespace.
func (m *Model) Rename(oldPath, newPath string) error {
	oldPath, newPath = clean(oldPath), clean(newPath)
	id, ok := m.volatile[oldPath]
	if !ok {
		return fmt.Errorf("powerlossoracle: rename missing file %q", oldPath)
	}
	m.ensureVolatileParents(newPath)
	delete(m.volatile, oldPath)
	m.volatile[newPath] = id
	m.trace = append(m.trace, "rename:"+oldPath+"->"+newPath)
	return nil
}

// Unlink changes only the volatile namespace.
func (m *Model) Unlink(path string) error {
	path = clean(path)
	if _, ok := m.volatile[path]; !ok {
		return fmt.Errorf("powerlossoracle: unlink missing file %q", path)
	}
	delete(m.volatile, path)
	m.trace = append(m.trace, "unlink:"+path)
	return nil
}

// Flush is deliberately stability-neutral: it represents userspace/kernel
// visibility, not an fsync boundary.
func (m *Model) Flush(path string) error {
	path = clean(path)
	if _, ok := m.volatile[path]; !ok {
		return fmt.Errorf("powerlossoracle: flush missing file %q", path)
	}
	m.trace = append(m.trace, "flush:"+path)
	return nil
}

// SyncFile promotes all process-visible bytes for one inode. It does not make a
// newly-created or renamed name stable; that requires SyncDir.
func (m *Model) SyncFile(path string) error {
	path = clean(path)
	id, ok := m.volatile[path]
	if !ok {
		return fmt.Errorf("powerlossoracle: sync missing file %q", path)
	}
	m.inodes[id].stable = clone(m.inodes[id].volatile)
	m.trace = append(m.trace, "sync-file:"+path)
	return nil
}

// SyncDir promotes immediate child names and removals for one directory.
func (m *Model) SyncDir(dir string) error {
	dir = clean(dir)
	if _, ok := m.volatileDirs[dir]; !ok {
		return fmt.Errorf("powerlossoracle: sync missing directory %q", dir)
	}
	for path := range m.stable {
		if clean(filepath.Dir(path)) == dir {
			delete(m.stable, path)
		}
	}
	for path, id := range m.volatile {
		if clean(filepath.Dir(path)) == dir {
			m.stable[path] = id
		}
	}
	for child := range m.stableDirs {
		if child != "." && clean(filepath.Dir(child)) == dir {
			delete(m.stableDirs, child)
		}
	}
	for child := range m.volatileDirs {
		if child != "." && clean(filepath.Dir(child)) == dir {
			m.stableDirs[child] = struct{}{}
		}
	}
	m.trace = append(m.trace, "sync-dir:"+dir)
	return nil
}

// Crash discards every volatile byte and namespace mutation.
func (m *Model) Crash() {
	m.volatile = make(map[string]uint64, len(m.stable))
	for path, id := range m.stable {
		m.volatile[path] = id
		m.inodes[id].volatile = clone(m.inodes[id].stable)
	}
	m.volatileDirs = make(map[string]struct{}, len(m.stableDirs))
	for dir := range m.stableDirs {
		m.volatileDirs[dir] = struct{}{}
	}
	m.trace = append(m.trace, "crash")
}

// MaterializeStable writes only stable bytes reachable through stable names.
// The result never contains process-volatile bytes.
func (m *Model) MaterializeStable(root string) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	dirs := keys(m.stableDirs)
	sort.Slice(dirs, func(i, j int) bool {
		di, dj := strings.Count(dirs[i], string(filepath.Separator)), strings.Count(dirs[j], string(filepath.Separator))
		if di == dj {
			return dirs[i] < dirs[j]
		}
		return di < dj
	})
	for _, dir := range dirs {
		if dir == "." {
			continue
		}
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			return err
		}
	}
	paths := make([]string, 0, len(m.stable))
	for path := range m.stable {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		parent := clean(filepath.Dir(path))
		if _, ok := m.stableDirs[parent]; !ok {
			return fmt.Errorf("powerlossoracle: stable file %q has unstable parent %q", path, parent)
		}
		if err := os.WriteFile(filepath.Join(root, path), m.inodes[m.stable[path]].stable, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// StablePaths returns stable regular-file paths in deterministic order.
func (m *Model) StablePaths() []string {
	paths := make([]string, 0, len(m.stable))
	for path := range m.stable {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// VolatilePaths returns process-visible regular-file paths in deterministic order.
func (m *Model) VolatilePaths() []string {
	paths := make([]string, 0, len(m.volatile))
	for path := range m.volatile {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// Trace returns the deterministic operation trace used in failure diagnostics.
func (m *Model) Trace() []string { return append([]string(nil), m.trace...) }

func (m *Model) ensureVolatileParents(path string) {
	for dir := clean(filepath.Dir(path)); ; dir = clean(filepath.Dir(dir)) {
		m.volatileDirs[dir] = struct{}{}
		if dir == "." {
			return
		}
	}
}

func clean(path string) string {
	path = filepath.Clean(path)
	path = strings.TrimPrefix(path, string(filepath.Separator))
	if path == "" {
		return "."
	}
	return path
}

func clone(in []byte) []byte { return append([]byte(nil), in...) }

func keys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	return out
}
