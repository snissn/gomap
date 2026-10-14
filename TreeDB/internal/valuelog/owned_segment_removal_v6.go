package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"unsafe"
)

// One retained namespace operation, driven only by the owning Set cursor.
// No retry loop, directory census, ordinary recovery, or background worker can
// consume its intent. Post-rename failures preserve the exact private handles.
type ownedSegmentRemovalV6 struct {
	file                       *File
	parent                     *os.File
	identity                   rootpublication.StableIdentity
	root                       *os.Root
	parentHandle               *os.File
	quarantine                 *retirementQuarantine
	name, base, quarantinePath string
	phase                      uint8
}

func (c *ownedSegmentRemovalV6) Step(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if c == nil || w == nil {
		return false, false, ErrTerminalRetirementV5
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(ownedSegmentRemovalV6{}))) {
		return false, false, nil
	}
	progress := false
	for {
		switch c.phase {
		case 0:
			if !w.Reserve(1, uint64(len(c.file.Path))*4+256) {
				return false, progress, nil
			}
			dir, path, e := stableDeleteQuarantinePaths(c.file.Path, c.identity)
			if e != nil {
				return false, progress, e
			}
			c.name, c.base, c.quarantinePath = filepath.Base(dir), filepath.Base(c.file.Path), path
			c.phase = 1
			progress = true
		case 1:
			// OpenRoot and its parent proof are retained independently across calls.
			// Each OS open/stat/close is admitted, including early failure unwind.
			if !w.Reserve(6, uint64(len(c.file.Path))*4+1024) {
				return false, progress, nil
			}
			root, e := os.OpenRoot(filepath.Dir(c.file.Path))
			if e != nil {
				return false, progress, e
			}
			parent, e := root.Open(".")
			if e != nil {
				return false, progress, errors.Join(e, root.Close())
			}
			got, e := rootpublication.StableIdentityFromFile(parent)
			want, pe := rootpublication.StableIdentityFromFile(c.parent)
			if e != nil || pe != nil || !rootpublication.SamePhysicalIdentity(got, want) {
				return false, progress, errors.Join(ErrLiveRetirementParentV6, e, pe, parent.Close(), root.Close())
			}
			c.root, c.parentHandle = root, parent
			c.phase = 2
			progress = true
		case 2:
			if !w.Reserve(1, uint64(len(c.name))*2+256) {
				return false, progress, nil
			}
			_, e := c.root.Lstat(c.name)
			if !os.IsNotExist(e) {
				return false, progress, errors.Join(rootpublication.ErrResourceConflict, e)
			}
			c.phase = 3
			progress = true
		case 3:
			if !w.Reserve(4, uint64(len(c.base))*2+512) {
				return false, progress, nil
			}
			linked, e := stableIdentityAtChild(c.parentHandle, c.base)
			if os.IsNotExist(e) {
				c.phase = 9
				progress = true
				continue
			}
			if e != nil || !rootpublication.SamePhysicalIdentity(linked, c.identity) {
				return false, progress, errors.Join(rootpublication.ErrResourceConflict, e)
			}
			c.phase = 4
			progress = true
		case 4:
			if !w.Reserve(1, uint64(len(c.name))*2+256) {
				return false, progress, nil
			}
			if e := c.root.Mkdir(c.name, 0700); e != nil {
				return false, progress, e
			}
			c.phase = 5
			progress = true
		case 5:
			if !w.Reserve(8, uint64(len(c.name))*2+1024) {
				return false, progress, nil
			}
			q, e := openRetirementQuarantine(c.root, c.name)
			if e != nil {
				return false, progress, e
			}
			c.quarantine = q
			c.phase = 6
			progress = true
		case 6:
			if !w.Reserve(1, uint64(len(c.base))*2+256) {
				return false, progress, nil
			}
			if e := retirementRenameChild(c.parentHandle, c.base, c.quarantine.parent, c.base); e != nil {
				return false, progress, e
			}
			c.phase = 7
			progress = true
		case 7:
			if !w.Reserve(1, uint64(len(c.file.Path))*4+256) {
				return false, progress, nil
			}
			// Persist the irreversible canonical unlink before injected-cut callbacks.
			c.phase = 8
			progress = true
			if e := durabilitycut.EmitNamespace(durabilitycut.NamespaceUnlink, segmentNamespaceResource(c.file.Path), filepath.Dir(c.file.Path), c.file.Path, ""); e != nil {
				return false, progress, e
			}
		case 8:
			if !w.Reserve(4, uint64(len(c.base))*2+512) {
				return false, progress, nil
			}
			got, e := stableIdentityAtChild(c.quarantine.parent, c.base)
			if e != nil || !rootpublication.SamePhysicalIdentity(got, c.identity) {
				return false, progress, errors.Join(rootpublication.ErrResourceConflict, e)
			}
			c.phase = 10
			progress = true
		case 10:
			if !w.Reserve(1, uint64(len(c.quarantinePath))*2+256) {
				return false, progress, nil
			}
			e := removeSegmentPath(c.quarantinePath, func(path string) error {
				if path != c.quarantinePath {
					return rootpublication.ErrResourceConflict
				}
				return c.quarantine.root.Remove(c.base)
			})
			if e != nil && !os.IsNotExist(e) {
				return false, progress, e
			}
			c.phase = 11
			progress = true
		case 11:
			if !w.Reserve(3, uint64(len(c.name))*2+512) {
				return false, progress, nil
			}
			if e := c.quarantine.removeDirectory(c.root, c.parentHandle, c.name); e != nil {
				return false, progress, e
			}
			c.phase = 12
			progress = true
		case 12:
			if !w.Reserve(2, 512) {
				return false, progress, nil
			}
			if e := c.quarantine.close(); e != nil {
				return false, progress, e
			}
			c.quarantine = nil
			c.phase = 9
			progress = true
		case 9:
			if c.parentHandle != nil {
				if !w.Reserve(1, 256) {
					return false, progress, nil
				}
				if e := c.parentHandle.Close(); e != nil {
					return false, progress, e
				}
				c.parentHandle = nil
				progress = true
			}
			if c.root != nil {
				if !w.Reserve(1, 256) {
					return false, progress, nil
				}
				if e := c.root.Close(); e != nil {
					return false, progress, e
				}
				c.root = nil
				progress = true
			}
			c.phase = 13
		case 13:
			return true, progress, nil
		}
	}
}
