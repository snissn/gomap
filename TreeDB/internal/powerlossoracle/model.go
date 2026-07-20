// Package powerlossoracle provides deterministic test infrastructure for
// TreeDB power-loss tests. It is not imported by production packages.
package powerlossoracle

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// CutPoint is a stable identifier emitted by the durability harness.
type CutPoint = durabilitycut.Point

const (
	BeforeDependencyAppend      = durabilitycut.BeforeDependencyAppend
	AfterDependencyAppend       = durabilitycut.AfterDependencyAppend
	BeforeUserspaceFlush        = durabilitycut.BeforeUserspaceFlush
	AfterUserspaceFlush         = durabilitycut.AfterUserspaceFlush
	BeforeDependencyFileSync    = durabilitycut.BeforeDependencyFileSync
	AfterDependencyFileSync     = durabilitycut.AfterDependencyFileSync
	BeforeNewFileDirectorySync  = durabilitycut.BeforeNewFileDirectorySync
	AfterNewFileDirectorySync   = durabilitycut.AfterNewFileDirectorySync
	BeforeIndexDataSync         = durabilitycut.BeforeIndexDataSync
	AfterIndexDataSync          = durabilitycut.AfterIndexDataSync
	BeforePublicationSealWrite  = durabilitycut.BeforePublicationSealWrite
	AfterPublicationSealWrite   = durabilitycut.AfterPublicationSealWrite
	BeforeMetaWrite             = durabilitycut.BeforeMetaWrite
	AfterMetaWrite              = durabilitycut.AfterMetaWrite
	BeforeMetaSync              = durabilitycut.BeforeMetaSync
	AfterMetaSync               = durabilitycut.AfterMetaSync
	BeforeAppliedLSNAdvance     = durabilitycut.BeforeAppliedLSNAdvance
	AfterAppliedLSNAdvance      = durabilitycut.AfterAppliedLSNAdvance
	BeforeWALOrAssetUnlink      = durabilitycut.BeforeWALOrAssetUnlink
	AfterWALOrAssetUnlink       = durabilitycut.AfterWALOrAssetUnlink
	BeforeDeletionDirectorySync = durabilitycut.BeforeDeletionDirectorySync
	AfterDeletionDirectorySync  = durabilitycut.AfterDeletionDirectorySync
)

// CutPoints is the canonical deterministic enumeration order.
var CutPoints = []CutPoint{
	BeforeDependencyAppend,
	AfterDependencyAppend,
	BeforeUserspaceFlush,
	AfterUserspaceFlush,
	BeforeDependencyFileSync,
	AfterDependencyFileSync,
	BeforeNewFileDirectorySync,
	AfterNewFileDirectorySync,
	BeforeIndexDataSync,
	AfterIndexDataSync,
	BeforePublicationSealWrite,
	AfterPublicationSealWrite,
	BeforeMetaWrite,
	AfterMetaWrite,
	BeforeMetaSync,
	AfterMetaSync,
	BeforeAppliedLSNAdvance,
	AfterAppliedLSNAdvance,
	BeforeWALOrAssetUnlink,
	AfterWALOrAssetUnlink,
	BeforeDeletionDirectorySync,
	AfterDeletionDirectorySync,
}

type inode struct {
	volatile       []byte
	stable         []byte
	stableIdentity rootpublication.StableIdentity
}

type ByteRange struct{ Offset, Length int64 }

// Model separates the process-visible namespace and bytes from the bytes and
// names that survive power loss. Names reference inode identities so that file
// sync followed by rename and directory sync has realistic semantics.
type Model struct {
	nextID           uint64
	nextDirID        uint64
	inodes           map[uint64]*inode
	volatile         map[string]uint64
	stable           map[string]uint64
	volatileDirs     map[string]rootpublication.StableIdentity
	stableDirs       map[string]rootpublication.StableIdentity
	excludeLockFiles bool
	trace            []string
}

// Capture imports a real directory as an initially stable image. Process-local
// Lock files coordinate live handles rather than representing durable database
// state, and may be unreadable while held on Windows. Composite TreeDB layouts
// can contain independently-opened nested databases and command-WAL owners, so
// omit their process-local lock files at every depth.
func Capture(root string) (*Model, error) {
	return capture(root, true, nil)
}

func isProcessLocalLockFile(name string) bool {
	return name == "LOCK" || name == "command-wal-journal-owner.lock"
}

func captureExcluding(root string, excluded ...string) (*Model, error) {
	return capture(root, false, excluded)
}

func capture(root string, excludeLockFiles bool, excluded []string) (*Model, error) {
	excludedPaths := make(map[string]struct{}, len(excluded))
	for _, path := range excluded {
		rel, err := normalize(path)
		if err != nil {
			return nil, fmt.Errorf("powerlossoracle: exclude %q: %w", path, err)
		}
		excludedPaths[rel] = struct{}{}
	}
	m := newModel()
	m.excludeLockFiles = excludeLockFiles
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel, err = normalize(filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		if _, excluded := excludedPaths[rel]; excluded {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if excludeLockFiles && !entry.IsDir() && isProcessLocalLockFile(entry.Name()) {
			return nil
		}
		if entry.IsDir() {
			identity, err := captureStableIdentity(path)
			if err != nil {
				return err
			}
			m.volatileDirs[rel] = identity
			m.stableDirs[rel] = identity
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("powerlossoracle: unsupported entry %q (%s)", rel, entry.Type())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		identity, err := captureStableIdentity(path)
		if err != nil {
			return err
		}
		id := m.allocateWithIdentity(data, data, identity)
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
	rootIdentity := syntheticDirectoryIdentity(1)
	return &Model{
		nextDirID:    1,
		inodes:       make(map[uint64]*inode),
		volatile:     make(map[string]uint64),
		stable:       make(map[string]uint64),
		volatileDirs: map[string]rootpublication.StableIdentity{".": rootIdentity},
		stableDirs:   map[string]rootpublication.StableIdentity{".": rootIdentity},
	}
}

func (m *Model) allocate(volatile, stable []byte) uint64 {
	return m.allocateWithIdentity(volatile, stable, rootpublication.StableIdentity{})
}

func (m *Model) allocateWithIdentity(volatile, stable []byte, identity rootpublication.StableIdentity) uint64 {
	m.nextID++
	m.inodes[m.nextID] = &inode{
		volatile:       clone(volatile),
		stable:         clone(stable),
		stableIdentity: physicalStableIdentity(identity),
	}
	return m.nextID
}

// Clone returns a deep copy suitable for evaluating another crash cut.
func (m *Model) Clone() *Model {
	out := newModel()
	out.nextID = m.nextID
	out.nextDirID = m.nextDirID
	out.excludeLockFiles = m.excludeLockFiles
	for id, node := range m.inodes {
		out.inodes[id] = &inode{
			volatile:       clone(node.volatile),
			stable:         clone(node.stable),
			stableIdentity: node.stableIdentity,
		}
	}
	for path, id := range m.volatile {
		out.volatile[path] = id
	}
	for path, id := range m.stable {
		out.stable[path] = id
	}
	for path, identity := range m.volatileDirs {
		out.volatileDirs[path] = identity
	}
	for path, identity := range m.stableDirs {
		out.stableDirs[path] = identity
	}
	out.trace = append([]string(nil), m.trace...)
	return out
}

// PathStable reports whether path's process-visible name and complete bytes
// are present in the stable image under the same inode identity. Callers use
// this to derive resource-closure evidence from the model instead of assuming
// that a named sync cut made an entire generation complete.
func (m *Model) PathStable(root, path string) (bool, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false, err
	}
	rel, err = normalize(filepath.ToSlash(rel))
	if err != nil {
		return false, err
	}
	volatileID, volatileOK := m.volatile[rel]
	stableID, stableOK := m.stable[rel]
	if !volatileOK || !stableOK || volatileID != stableID {
		return false, nil
	}
	if !stableDirReachable(m.stableDirs, cleanInternal(pathpkg.Dir(rel))) {
		return false, nil
	}
	node := m.inodes[volatileID]
	return node != nil && bytes.Equal(node.volatile, node.stable), nil
}

// Overlay makes another real directory the volatile process-visible state.
// No stable bytes or names change until SyncFile or SyncDir is called.
func (m *Model) Overlay(root string) error {
	seenFiles := make(map[string]struct{})
	seenDirs := make(map[string]rootpublication.StableIdentity)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel, err = normalize(filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		if m.excludeLockFiles && !entry.IsDir() && isProcessLocalLockFile(entry.Name()) {
			return nil
		}
		if entry.IsDir() {
			identity, err := captureStableIdentity(path)
			if err != nil {
				return err
			}
			seenDirs[rel] = identity
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("powerlossoracle: unsupported entry %q (%s)", rel, entry.Type())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		identity, err := captureStableIdentity(path)
		if err != nil {
			return err
		}
		seenFiles[rel] = struct{}{}
		if id, ok := m.volatile[rel]; ok {
			node := m.inodes[id]
			switch {
			case !validStableIdentity(node.stableIdentity):
				node.stableIdentity = physicalStableIdentity(identity)
				node.volatile = clone(data)
			case validStableIdentity(identity) && !rootpublication.SamePhysicalIdentity(node.stableIdentity, identity):
				m.volatile[rel] = m.allocateWithIdentity(data, nil, identity)
			default:
				node.volatile = clone(data)
			}
		} else {
			m.volatile[rel] = m.allocateWithIdentity(data, nil, identity)
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

// Observe imports the process-visible bytes from an actual TreeDB cut event
// and promotes only the persistence boundary completed by that event.
func (m *Model) Observe(root string, event durabilitycut.Event) error {
	if event.Namespace != "" {
		if err := m.observeNamespace(root, event); err != nil {
			return err
		}
	}
	pathRequired := false
	switch event.Point {
	case durabilitycut.BeforeDependencyFileSync, durabilitycut.AfterDependencyFileSync,
		durabilitycut.BeforeNewFileDirectorySync, durabilitycut.AfterNewFileDirectorySync,
		durabilitycut.BeforeIndexDataSync, durabilitycut.AfterIndexDataSync,
		durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync,
		durabilitycut.BeforeDeletionDirectorySync, durabilitycut.AfterDeletionDirectorySync:
		pathRequired = true
	}
	var eventPaths []string
	if pathRequired {
		rawPaths := event.Paths
		if event.Path != "" {
			rawPaths = append(append([]string(nil), rawPaths...), event.Path)
		}
		if len(rawPaths) == 0 {
			return fmt.Errorf("powerlossoracle: cut %s requires an exact path", event.Point)
		}
		eventPaths = make([]string, 0, len(rawPaths))
		for _, path := range rawPaths {
			if path == "" {
				return fmt.Errorf("powerlossoracle: cut %s contains an empty exact path", event.Point)
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return fmt.Errorf("powerlossoracle: cut %s path %q: %w", event.Point, path, err)
			}
			normalized, err := normalize(filepath.ToSlash(rel))
			if err != nil {
				return fmt.Errorf("powerlossoracle: cut %s path %q is outside root %q: %w", event.Point, path, root, err)
			}
			eventPaths = append(eventPaths, normalized)
		}
	}
	createdDirectory := ""
	if event.CreatedDirectory != "" {
		if event.Point != durabilitycut.BeforeNewFileDirectorySync && event.Point != durabilitycut.AfterNewFileDirectorySync {
			return fmt.Errorf("powerlossoracle: cut %s carries a created-directory target", event.Point)
		}
		rel, err := filepath.Rel(root, event.CreatedDirectory)
		if err != nil {
			return fmt.Errorf("powerlossoracle: cut %s created directory %q: %w", event.Point, event.CreatedDirectory, err)
		}
		createdDirectory, err = normalize(filepath.ToSlash(rel))
		if err != nil {
			return fmt.Errorf("powerlossoracle: cut %s created directory %q is outside root %q: %w", event.Point, event.CreatedDirectory, root, err)
		}
	}
	if err := m.Overlay(root); err != nil {
		return err
	}
	switch event.Point {
	case durabilitycut.AfterDependencyFileSync:
		for _, path := range eventPaths {
			if err := m.SyncFile(path); err != nil {
				return err
			}
		}
	case durabilitycut.AfterNewFileDirectorySync:
		dir := eventPaths[0]
		if createdDirectory != "" {
			if _, ok := m.volatileDirs[createdDirectory]; !ok {
				return fmt.Errorf("powerlossoracle: created-directory sync missing directory %q", createdDirectory)
			}
			dir = cleanInternal(pathpkg.Dir(createdDirectory))
		}
		if err := m.SyncDir(dir); err != nil {
			return err
		}
	case durabilitycut.AfterIndexDataSync, durabilitycut.AfterMetaSync:
		if err := m.SyncFile(eventPaths[0]); err != nil {
			return err
		}
	case durabilitycut.AfterDeletionDirectorySync:
		if err := m.SyncDir(eventPaths[0]); err != nil {
			return err
		}
	}
	m.trace = append(m.trace, "cut:"+string(event.Point)+":"+string(event.Resource))
	return nil
}

// observeNamespace applies the explicit production mutation before Overlay.
// Doing this first is what preserves the source inode across rename: a plain
// post-operation directory scan would otherwise misclassify the destination
// as a newly-created inode and lose file-sync-versus-directory-sync semantics.
func (m *Model) observeNamespace(root string, event durabilitycut.Event) error {
	rel := func(path string) (string, error) {
		if path == "" {
			return "", errors.New("powerlossoracle: namespace event contains an empty path")
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return "", err
		}
		normalized, err := normalize(filepath.ToSlash(relative))
		if err != nil {
			return "", fmt.Errorf("powerlossoracle: namespace path %q is outside root %q: %w", path, root, err)
		}
		return normalized, nil
	}

	switch event.Namespace {
	case durabilitycut.NamespaceCreate:
		path, err := rel(event.NewPath)
		if err != nil {
			return err
		}
		info, err := os.Stat(event.NewPath)
		if err != nil {
			return fmt.Errorf("powerlossoracle: stat created path %q: %w", event.NewPath, err)
		}
		identity, err := captureStableIdentity(event.NewPath)
		if err != nil {
			return err
		}
		if info.IsDir() {
			m.ensureVolatileParents(path)
			m.volatileDirs[path] = identity
			m.trace = append(m.trace, "create-dir:"+path)
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("powerlossoracle: created path %q is unsupported type %s", event.NewPath, info.Mode())
		}
		data, err := os.ReadFile(event.NewPath)
		if err != nil {
			return fmt.Errorf("powerlossoracle: read created file %q: %w", event.NewPath, err)
		}
		if id, exists := m.volatile[path]; exists {
			if _, stable := m.stable[path]; stable {
				return fmt.Errorf("powerlossoracle: file already exists %q", path)
			}
			m.inodes[id].volatile = clone(data)
			m.inodes[id].stableIdentity = physicalStableIdentity(identity)
			m.trace = append(m.trace, "create-observed:"+path)
			return nil
		}
		return m.createWithIdentity(path, data, identity)
	case durabilitycut.NamespaceRename:
		oldPath, err := rel(event.OldPath)
		if err != nil {
			return err
		}
		newPath, err := rel(event.NewPath)
		if err != nil {
			return err
		}
		return m.Rename(oldPath, newPath)
	case durabilitycut.NamespaceUnlink:
		path, err := rel(event.OldPath)
		if err != nil {
			return err
		}
		return m.Unlink(path)
	default:
		return fmt.Errorf("powerlossoracle: unknown namespace operation %q", event.Namespace)
	}
}

// PromoteRange models a physically permitted partial dirty-byte writeback.
// The bytes must have been imported from the actual process-visible file first.
func (m *Model) PromoteRange(path string, offset, length int64) error {
	var err error
	path, err = normalize(path)
	if err != nil {
		return err
	}
	id, ok := m.volatile[path]
	if !ok {
		return fmt.Errorf("powerlossoracle: promote missing file %q", path)
	}
	n := m.inodes[id]
	if offset < 0 || length < 0 || offset > int64(len(n.volatile)) || offset+length > int64(len(n.volatile)) {
		return fmt.Errorf("powerlossoracle: promote range %s [%d,%d) outside %d", path, offset, offset+length, len(n.volatile))
	}
	end := offset + length
	if int64(len(n.stable)) < end {
		n.stable = append(n.stable, make([]byte, end-int64(len(n.stable)))...)
	}
	copy(n.stable[offset:end], n.volatile[offset:end])
	m.trace = append(m.trace, fmt.Sprintf("promote-range:%s:%d:%d", path, offset, length))
	return nil
}

// ChangedRanges returns actual process-visible byte runs that differ from the
// captured stable bytes. Adjacent changed bytes are coalesced deterministically.
func (m *Model) ChangedRanges(path string) ([]ByteRange, error) {
	var err error
	path, err = normalize(path)
	if err != nil {
		return nil, err
	}
	id, ok := m.volatile[path]
	if !ok {
		return nil, fmt.Errorf("powerlossoracle: diff missing file %q", path)
	}
	n := m.inodes[id]
	max := len(n.volatile)
	if len(n.stable) > max {
		max = len(n.stable)
	}
	var out []ByteRange
	for i := 0; i < max; {
		equal := i < len(n.volatile) && i < len(n.stable) && n.volatile[i] == n.stable[i]
		if equal {
			i++
			continue
		}
		start := i
		for i < max {
			eq := i < len(n.volatile) && i < len(n.stable) && n.volatile[i] == n.stable[i]
			if eq {
				break
			}
			i++
		}
		// Only volatile bytes can be promoted; truncation remains a namespace/file-size operation.
		if start < len(n.volatile) {
			end := i
			if end > len(n.volatile) {
				end = len(n.volatile)
			}
			out = append(out, ByteRange{Offset: int64(start), Length: int64(end - start)})
		}
	}
	return out, nil
}

// Write dirties process-visible file bytes without making them stable.
func (m *Model) Write(path string, data []byte) error {
	var err error
	path, err = normalize(path)
	if err != nil {
		return err
	}
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
	return m.createWithIdentity(path, data, rootpublication.StableIdentity{})
}

func (m *Model) createWithIdentity(path string, data []byte, identity rootpublication.StableIdentity) error {
	var err error
	path, err = normalize(path)
	if err != nil {
		return err
	}
	if path == "." {
		return errors.New("powerlossoracle: cannot create root")
	}
	if _, ok := m.volatile[path]; ok {
		return fmt.Errorf("powerlossoracle: file already exists %q", path)
	}
	m.ensureVolatileParents(path)
	m.volatile[path] = m.allocateWithIdentity(data, nil, identity)
	m.trace = append(m.trace, "create:"+path)
	return nil
}

// Rename changes only the volatile namespace.
func (m *Model) Rename(oldPath, newPath string) error {
	var err error
	oldPath, err = normalize(oldPath)
	if err != nil {
		return err
	}
	newPath, err = normalize(newPath)
	if err != nil {
		return err
	}
	if oldPath == "." || newPath == "." {
		return errors.New("powerlossoracle: cannot rename root")
	}
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
	var err error
	path, err = normalize(path)
	if err != nil {
		return err
	}
	if _, ok := m.volatile[path]; ok {
		delete(m.volatile, path)
		m.trace = append(m.trace, "unlink:"+path)
		return nil
	}
	if path == "." {
		return errors.New("powerlossoracle: cannot unlink root")
	}
	if _, ok := m.volatileDirs[path]; !ok {
		return fmt.Errorf("powerlossoracle: unlink missing file or directory %q", path)
	}
	prefix := path + "/"
	for file := range m.volatile {
		if strings.HasPrefix(file, prefix) {
			delete(m.volatile, file)
		}
	}
	for dir := range m.volatileDirs {
		if dir == path || strings.HasPrefix(dir, prefix) {
			delete(m.volatileDirs, dir)
		}
	}
	m.trace = append(m.trace, "unlink-tree:"+path)
	return nil
}

// Flush is deliberately stability-neutral: it represents userspace/kernel
// visibility, not an fsync boundary.
func (m *Model) Flush(path string) error {
	var err error
	path, err = normalize(path)
	if err != nil {
		return err
	}
	if _, ok := m.volatile[path]; !ok {
		return fmt.Errorf("powerlossoracle: flush missing file %q", path)
	}
	m.trace = append(m.trace, "flush:"+path)
	return nil
}

// SyncFile promotes all process-visible bytes for one inode. It does not make a
// newly-created or renamed name stable; that requires SyncDir.
func (m *Model) SyncFile(path string) error {
	var err error
	path, err = normalize(path)
	if err != nil {
		return err
	}
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
	var err error
	dir, err = normalize(dir)
	if err != nil {
		return err
	}
	if _, ok := m.volatileDirs[dir]; !ok {
		return fmt.Errorf("powerlossoracle: sync missing directory %q", dir)
	}
	for path := range m.stable {
		if cleanInternal(pathpkg.Dir(path)) == dir {
			delete(m.stable, path)
		}
	}
	for path, id := range m.volatile {
		if cleanInternal(pathpkg.Dir(path)) == dir {
			m.stable[path] = id
		}
	}
	removedTrees := make([]string, 0)
	for child, stableIdentity := range m.stableDirs {
		if child != "." && cleanInternal(pathpkg.Dir(child)) == dir {
			volatileIdentity, stillVisible := m.volatileDirs[child]
			replaced := stillVisible &&
				validStableIdentity(stableIdentity) &&
				validStableIdentity(volatileIdentity) &&
				!rootpublication.SamePhysicalIdentity(stableIdentity, volatileIdentity)
			if !stillVisible || replaced {
				removedTrees = append(removedTrees, child)
			}
			delete(m.stableDirs, child)
		}
	}
	// A durable removal of a directory entry makes the entire old subtree
	// unreachable. Its descendants do not each need an independent parent
	// directory sync: the synced removal of the top-level name is sufficient.
	for _, tree := range removedTrees {
		prefix := tree + "/"
		for path := range m.stable {
			if strings.HasPrefix(path, prefix) {
				delete(m.stable, path)
			}
		}
		for child := range m.stableDirs {
			if strings.HasPrefix(child, prefix) {
				delete(m.stableDirs, child)
			}
		}
	}
	for child, identity := range m.volatileDirs {
		if child != "." && cleanInternal(pathpkg.Dir(child)) == dir {
			m.stableDirs[child] = identity
		}
	}
	m.trace = append(m.trace, "sync-dir:"+dir)
	return nil
}

// Crash discards every volatile byte and namespace mutation.
func (m *Model) Crash() {
	reachableDirs, reachableFiles := reachableNamespace(m.stableDirs, m.stable)
	m.stableDirs = reachableDirs
	m.stable = reachableFiles
	m.volatile = make(map[string]uint64, len(reachableFiles))
	for path, id := range reachableFiles {
		m.volatile[path] = id
		m.inodes[id].volatile = clone(m.inodes[id].stable)
	}
	m.volatileDirs = make(map[string]rootpublication.StableIdentity, len(reachableDirs))
	for dir, identity := range reachableDirs {
		m.volatileDirs[dir] = identity
	}
	m.trace = append(m.trace, "crash")
}

// MaterializeStable writes only stable bytes reachable through stable names.
// The result never contains process-volatile bytes.
func (m *Model) MaterializeStable(root string) error {
	return m.materialize(root, m.stableDirs, m.stable, func(node *inode) []byte { return node.stable }, "stable")
}

// MaterializeVolatile writes the process-visible namespace and bytes at the
// modeled cut. It is evidence of the dirty pre-crash image and must never be
// used as the recovery input.
func (m *Model) MaterializeVolatile(root string) error {
	return m.materialize(root, m.volatileDirs, m.volatile, func(node *inode) []byte { return node.volatile }, "volatile")
}

func (m *Model) materialize(root string, dirs map[string]rootpublication.StableIdentity, files map[string]uint64, bytesFor func(*inode) []byte, label string) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	if label == "stable" {
		dirs, files = reachableNamespace(dirs, files)
	}
	dirPaths := keys(dirs)
	sort.Slice(dirPaths, func(i, j int) bool {
		di, dj := strings.Count(dirPaths[i], "/"), strings.Count(dirPaths[j], "/")
		if di == dj {
			return dirPaths[i] < dirPaths[j]
		}
		return di < dj
	})
	for _, dir := range dirPaths {
		if dir == "." {
			continue
		}
		parent := cleanInternal(pathpkg.Dir(dir))
		if _, ok := dirs[parent]; !ok {
			return fmt.Errorf("powerlossoracle: %s directory %q has missing parent %q", label, dir, parent)
		}
		if err := os.Mkdir(filepath.Join(root, filepath.FromSlash(dir)), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		parent := cleanInternal(pathpkg.Dir(path))
		if _, ok := dirs[parent]; !ok {
			return fmt.Errorf("powerlossoracle: %s file %q has missing parent %q", label, path, parent)
		}
		node := m.inodes[files[path]]
		if node == nil {
			return fmt.Errorf("powerlossoracle: %s file %q references missing inode", label, path)
		}
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(path)), bytesFor(node), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// InstallStableIdentityOverrides installs the model's original physical
// identities for a materialized stable image. A real power loss preserves
// those identities; recreating the same stable bytes in a temporary directory
// does not. Callers must release the returned scope after closing the image.
func (m *Model) InstallStableIdentityOverrides(root string) (func(), error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	reachableDirs, reachableFiles := reachableNamespace(m.stableDirs, m.stable)
	overrides := make(map[string]rootpublication.StableIdentity, len(reachableFiles)+len(reachableDirs))
	for dir, identity := range reachableDirs {
		if validStableIdentity(identity) {
			overrides[filepath.Join(root, filepath.FromSlash(dir))] = physicalStableIdentity(identity)
		}
	}
	for path, id := range reachableFiles {
		identity := m.inodes[id].stableIdentity
		if validStableIdentity(identity) {
			overrides[filepath.Join(root, filepath.FromSlash(path))] = physicalStableIdentity(identity)
		}
	}
	return rootpublication.InstallStableIdentityOverridesForTesting(overrides)
}

// StablePaths returns stable regular-file paths in deterministic order.
func (m *Model) StablePaths() []string {
	_, reachableFiles := reachableNamespace(m.stableDirs, m.stable)
	paths := make([]string, 0, len(reachableFiles))
	for path := range reachableFiles {
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

// reachableNamespace projects per-directory durable contents onto the names
// reachable from the materialized root. A child directory can be synced before
// its own parent entry; its durable contents become reachable only if a later
// ancestor sync persists the chain.
func reachableNamespace(dirs map[string]rootpublication.StableIdentity, files map[string]uint64) (map[string]rootpublication.StableIdentity, map[string]uint64) {
	reachableDirs := make(map[string]rootpublication.StableIdentity, len(dirs))
	if identity, ok := dirs["."]; ok {
		reachableDirs["."] = identity
	}
	dirPaths := keys(dirs)
	sort.Slice(dirPaths, func(i, j int) bool {
		di, dj := strings.Count(dirPaths[i], "/"), strings.Count(dirPaths[j], "/")
		if di == dj {
			return dirPaths[i] < dirPaths[j]
		}
		return di < dj
	})
	for _, dir := range dirPaths {
		if dir == "." {
			continue
		}
		if _, ok := reachableDirs[cleanInternal(pathpkg.Dir(dir))]; ok {
			reachableDirs[dir] = dirs[dir]
		}
	}
	reachableFiles := make(map[string]uint64, len(files))
	for path, id := range files {
		if _, ok := reachableDirs[cleanInternal(pathpkg.Dir(path))]; ok {
			reachableFiles[path] = id
		}
	}
	return reachableDirs, reachableFiles
}

func stableDirReachable(dirs map[string]rootpublication.StableIdentity, dir string) bool {
	for {
		if _, ok := dirs[dir]; !ok {
			return false
		}
		if dir == "." {
			return true
		}
		dir = cleanInternal(pathpkg.Dir(dir))
	}
}

// Trace returns the deterministic operation trace used in failure diagnostics.
func (m *Model) Trace() []string { return append([]string(nil), m.trace...) }

// UseObservedTrace binds an adversarial selective-writeback model to the
// operation trace captured from the corresponding real production cut. It
// intentionally changes no namespace or byte state: callers first derive the
// selective stable/dirty image from an older baseline, then attach the trace
// from the independently observed cut that produced those dirty bytes.
func (m *Model) UseObservedTrace(observed *Model) error {
	if m == nil || observed == nil {
		return errorsf("cannot bind an observed trace from a nil model")
	}
	m.trace = append(m.trace[:0], observed.trace...)
	return nil
}

// StableFingerprint identifies the exact stable namespace, inode identities,
// and bytes at a cut without materializing them.
func (m *Model) StableFingerprint() string {
	h := sha256.New()
	reachableDirs, reachableFiles := reachableNamespace(m.stableDirs, m.stable)
	dirs := keys(reachableDirs)
	sort.Strings(dirs)
	for _, dir := range dirs {
		identity := physicalStableIdentity(reachableDirs[dir])
		_, _ = fmt.Fprintf(h, "d:%s:%s:%d:%x\x00", dir, identity.Platform, identity.VolumeID, identity.ObjectID)
	}
	paths := keys(reachableFiles)
	sort.Strings(paths)
	for _, path := range paths {
		id := reachableFiles[path]
		_, _ = fmt.Fprintf(h, "f:%s:%d:", path, id)
		_, _ = h.Write(m.inodes[id].stable)
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (m *Model) ensureVolatileParents(path string) {
	for dir := cleanInternal(pathpkg.Dir(path)); ; dir = cleanInternal(pathpkg.Dir(dir)) {
		if _, exists := m.volatileDirs[dir]; !exists {
			m.nextDirID++
			m.volatileDirs[dir] = syntheticDirectoryIdentity(m.nextDirID)
		}
		if dir == "." {
			return
		}
	}
}

func syntheticDirectoryIdentity(id uint64) rootpublication.StableIdentity {
	var objectID [16]byte
	binary.BigEndian.PutUint64(objectID[8:], id)
	return rootpublication.StableIdentity{Platform: "powerlossoracle", ObjectID: objectID}
}

func captureStableIdentity(path string) (rootpublication.StableIdentity, error) {
	file, err := os.Open(path)
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	defer file.Close()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if errors.Is(err, rootpublication.ErrStableIdentityUnsupported) {
		return rootpublication.StableIdentity{}, nil
	}
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	return physicalStableIdentity(identity), nil
}

func validStableIdentity(identity rootpublication.StableIdentity) bool {
	return identity.Platform != "" && identity.ObjectID != [16]byte{}
}

func physicalStableIdentity(identity rootpublication.StableIdentity) rootpublication.StableIdentity {
	identity.Generation = 0
	return identity
}

func normalize(name string) (string, error) {
	// Model paths are logical root-relative names, not host filesystem paths.
	// Canonicalizing both separator spellings here keeps traces, fingerprints,
	// and path lists identical on every platform. It also prevents a Windows
	// rooted path (for example `\\absolute`) from reaching parent traversal.
	logical := strings.ReplaceAll(name, "\\", "/")
	if logical == "" || strings.HasPrefix(logical, "/") || hasWindowsVolume(logical) {
		return "", fmt.Errorf("powerlossoracle: path must be relative: %q", name)
	}
	cleaned := pathpkg.Clean(logical)
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("powerlossoracle: path escapes root: %q", name)
	}
	return cleaned, nil
}

func cleanInternal(name string) string {
	name = pathpkg.Clean(name)
	if name == "" {
		return "."
	}
	return name
}

func hasWindowsVolume(name string) bool {
	return len(name) >= 2 && ((name[0] >= 'A' && name[0] <= 'Z') || (name[0] >= 'a' && name[0] <= 'z')) && name[1] == ':'
}

func clone(in []byte) []byte { return append([]byte(nil), in...) }

func keys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	return out
}
