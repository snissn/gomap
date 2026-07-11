// Package durabilitycut exposes a package-internal observation seam for
// deterministic durability tests. With no installed observer, Emit is one
// atomic load and one nil branch at coarse commit/checkpoint boundaries.
package durabilitycut

import (
	"sync"
	"sync/atomic"
)

type Point string

const (
	BeforeDependencyAppend     Point = "before-dependency-append"
	AfterDependencyAppend      Point = "after-dependency-append"
	BeforeUserspaceFlush       Point = "before-userspace-flush"
	AfterUserspaceFlush        Point = "after-userspace-flush"
	BeforeDependencyFileSync   Point = "before-dependency-file-sync"
	AfterDependencyFileSync    Point = "after-dependency-file-sync"
	BeforeNewFileDirectorySync Point = "before-new-file-directory-sync"
	AfterNewFileDirectorySync  Point = "after-new-file-directory-sync"
	BeforeIndexDataSync        Point = "before-index-data-sync"
	AfterIndexDataSync         Point = "after-index-data-sync"
	// PublicationSealWrite is a logical publication-boundary placeholder. Issue
	// #3679 will retarget it to the future on-disk publication seal.
	BeforePublicationSealWrite  Point = "before-publication-seal-write"
	AfterPublicationSealWrite   Point = "after-publication-seal-write"
	BeforeMetaWrite             Point = "before-meta-write"
	AfterMetaWrite              Point = "after-meta-write"
	BeforeMetaSync              Point = "before-meta-sync"
	AfterMetaSync               Point = "after-meta-sync"
	BeforeAppliedLSNAdvance     Point = "before-applied-lsn-advance"
	AfterAppliedLSNAdvance      Point = "after-applied-lsn-advance"
	BeforeWALOrAssetUnlink      Point = "before-wal-or-asset-unlink"
	AfterWALOrAssetUnlink       Point = "after-wal-or-asset-unlink"
	BeforeDeletionDirectorySync Point = "before-deletion-directory-sync"
	AfterDeletionDirectorySync  Point = "after-deletion-directory-sync"
)

type Resource string

const (
	ResourceValueLog   Resource = "value-log"
	ResourceOuterLeaf  Resource = "outer-leaf"
	ResourceCommandWAL Resource = "command-wal"
	ResourceIndex      Resource = "index"
	ResourceSeal       Resource = "publication-seal"
	ResourceMeta       Resource = "meta"
	ResourceAuxiliary  Resource = "auxiliary"
)

// NamespaceOperation records a completed production namespace mutation. It is
// orthogonal to Point: named cut points describe persistence boundaries, while
// namespace operations let the stable/volatile model preserve inode identity
// across create, rename, and unlink instead of inferring names from a scan.
type NamespaceOperation string

const (
	NamespaceCreate NamespaceOperation = "create"
	NamespaceRename NamespaceOperation = "rename"
	NamespaceUnlink NamespaceOperation = "unlink"
)

type Event struct {
	Point    Point
	Resource Resource
	Root     string
	Path     string
	// Paths represents one atomic grouped persistence boundary. Path is used
	// when the production operation completes for one exact file or directory.
	Paths  []string
	LSN    uint64
	Offset int64
	Length int64
	// Namespace, OldPath, and NewPath describe one completed filesystem
	// namespace mutation. Create uses NewPath, unlink uses OldPath, and rename
	// uses both.
	Namespace NamespaceOperation
	OldPath   string
	NewPath   string
}

// EmitNamespace avoids constructing a namespace event on the
// production-disabled path.
func EmitNamespace(operation NamespaceOperation, resource Resource, root, oldPath, newPath string) error {
	h := installed.Load()
	if h == nil || h.observe == nil {
		return nil
	}
	return h.observe(Event{
		Resource:  resource,
		Root:      root,
		Namespace: operation,
		OldPath:   oldPath,
		NewPath:   newPath,
	})
}

type Observer func(Event) error

type holder struct{ observe Observer }

var (
	installed atomic.Pointer[holder]
	installMu sync.Mutex
)

func Emit(event Event) error {
	h := installed.Load()
	if h == nil || h.observe == nil {
		return nil
	}
	return h.observe(event)
}

// EmitBasic avoids constructing an Event on the production-disabled path.
func EmitBasic(point Point, resource Resource, root string) error {
	h := installed.Load()
	if h == nil || h.observe == nil {
		return nil
	}
	return h.observe(Event{Point: point, Resource: resource, Root: root})
}

// EmitPath avoids constructing a path event on the production-disabled path.
func EmitPath(point Point, resource Resource, root, path string) error {
	h := installed.Load()
	if h == nil || h.observe == nil {
		return nil
	}
	return h.observe(Event{Point: point, Resource: resource, Root: root, Path: path})
}

// EmitRange avoids constructing a byte-range event on the
// production-disabled path.
func EmitRange(point Point, resource Resource, root, path string, offset, length int64) error {
	h := installed.Load()
	if h == nil || h.observe == nil {
		return nil
	}
	return h.observe(Event{Point: point, Resource: resource, Root: root, Path: path, Offset: offset, Length: length})
}

// EmitLSN avoids constructing an LSN event on the production-disabled path.
func EmitLSN(point Point, resource Resource, root string, lsn uint64) error {
	h := installed.Load()
	if h == nil || h.observe == nil {
		return nil
	}
	return h.observe(Event{Point: point, Resource: resource, Root: root, LSN: lsn})
}

// Enabled lets coarse production boundaries avoid collecting diagnostic path
// metadata when no test observer is installed.
func Enabled() bool {
	h := installed.Load()
	return h != nil && h.observe != nil
}

// Install serializes process-global observers and returns a mandatory restore
// function. It is intended only for deterministic, non-parallel tests.
func Install(observer Observer) func() {
	installMu.Lock()
	installed.Store(&holder{observe: observer})
	return func() {
		installed.Store(nil)
		installMu.Unlock()
	}
}
