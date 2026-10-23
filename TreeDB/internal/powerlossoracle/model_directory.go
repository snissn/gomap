package powerlossoracle

import (
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// renameDirectory changes only process-visible edges. Durable directory
// contents belong to identities, so no durable bytes or child entries move.
func (m *Model) renameDirectory(oldPath, newPath string, identity rootpublication.StableIdentity) error {
	if oldPath == newPath {
		return nil
	}
	if strings.HasPrefix(newPath, oldPath+"/") {
		return fmt.Errorf("powerlossoracle: cannot rename directory %q into itself %q", oldPath, newPath)
	}
	if _, ok := m.volatile[newPath]; ok {
		return fmt.Errorf("powerlossoracle: directory rename destination is a file %q", newPath)
	}
	visible, alreadyMoved := m.volatileDirs[newPath]
	alreadyMoved = alreadyMoved && rootpublication.SamePhysicalIdentity(identity, visible)
	if _, exists := m.volatileDirs[newPath]; exists && !alreadyMoved {
		prefix := newPath + "/"
		for path := range m.volatile {
			if strings.HasPrefix(path, prefix) {
				return fmt.Errorf("powerlossoracle: rename destination directory is not empty %q", newPath)
			}
		}
		for path := range m.volatileDirs {
			if strings.HasPrefix(path, prefix) {
				return fmt.Errorf("powerlossoracle: rename destination directory is not empty %q", newPath)
			}
		}
	}
	files := make(map[string]uint64)
	dirs := make(map[string]rootpublication.StableIdentity)
	prefix := oldPath + "/"
	_, sourceVisible := m.volatileDirs[oldPath]
	if sourceVisible {
		for path, id := range m.volatile {
			if strings.HasPrefix(path, prefix) {
				files[path] = id
			}
		}
		for path, child := range m.volatileDirs {
			if path == oldPath || strings.HasPrefix(path, prefix) {
				dirs[path] = child
			}
		}
	} else {
		// A scan may precede the synchronous mutation callback. Only reconcile
		// detached members that still exist at the destination with the same
		// identity: an earlier removed child must never be resurrected.
		if !alreadyMoved {
			return fmt.Errorf("powerlossoracle: detached directory rename has no matching destination %q", newPath)
		}
		for path, id := range m.overlayDetached {
			if strings.HasPrefix(path, prefix) {
				target := newPath + strings.TrimPrefix(path, oldPath)
				current, ok := m.volatile[target]
				if ok && rootpublication.SamePhysicalIdentity(m.inodes[id].stableIdentity, m.inodes[current].stableIdentity) {
					files[path] = id
				}
			}
		}
		for path, child := range m.overlayDetachedDirs {
			if path == oldPath || strings.HasPrefix(path, prefix) {
				target := newPath + strings.TrimPrefix(path, oldPath)
				current, ok := m.volatileDirs[target]
				if ok && rootpublication.SamePhysicalIdentity(child, current) {
					dirs[path] = child
				}
			}
		}
	}
	for path := range files {
		delete(m.volatile, path)
		delete(m.overlayDetached, path)
	}
	for path := range dirs {
		delete(m.volatileDirs, path)
		delete(m.overlayDetachedDirs, path)
	}
	for path, id := range files {
		m.volatile[newPath+strings.TrimPrefix(path, oldPath)] = id
	}
	for path, child := range dirs {
		m.volatileDirs[newPath+strings.TrimPrefix(path, oldPath)] = child
	}
	m.trace = append(m.trace, "rename-dir:"+oldPath+"->"+newPath)
	return nil
}
