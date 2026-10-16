package db

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func valueLogResourceForPath(path string) durabilitycut.Resource {
	if filepath.Base(filepath.Dir(filepath.Clean(path))) == leafVLogDirName {
		return durabilitycut.ResourceOuterLeaf
	}
	return durabilitycut.ResourceValueLog
}

// observeNamespaceMutation reports a completed filesystem namespace mutation.
// An observer error is necessarily post-mutation, so callers must fail closed
// and reopen before assuming which directory entry is durable.
func observeNamespaceMutation(operation durabilitycut.NamespaceOperation, resource durabilitycut.Resource, root, oldPath, newPath string) error {
	if err := durabilitycut.EmitNamespace(operation, resource, root, oldPath, newPath); err != nil {
		return errors.Join(err, ErrRecoveryRequired)
	}
	return nil
}

func observeStableNamespaceMutation(operation durabilitycut.NamespaceOperation, resource durabilitycut.Resource, root, oldPath, newPath string, parent, path *os.File, oldName, newName string) error {
	if err := durabilitycut.EmitStableNamespace(operation, resource, root, oldPath, newPath, parent, path, oldName, newName); err != nil {
		return errors.Join(err, ErrRecoveryRequired)
	}
	return nil
}

func removePersistentFile(root, path string, resource durabilitycut.Resource) (bool, error) {
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, observeNamespaceMutation(durabilitycut.NamespaceUnlink, resource, root, path, "")
}

// removePersistentFileBestEffort preserves cleanup paths that intentionally
// ignore operating-system removal failures while still propagating an injected
// post-removal durability cut.
func removePersistentFileBestEffort(root, path string, resource durabilitycut.Resource) error {
	_, err := removePersistentFileBestEffortResult(root, path, resource)
	return err
}

func removePersistentFileBestEffortResult(root, path string, resource durabilitycut.Resource) (bool, error) {
	if err := os.Remove(path); err != nil {
		return false, nil
	}
	return true, observeNamespaceMutation(durabilitycut.NamespaceUnlink, resource, root, path, "")
}

func removePersistentTree(root, path string, resource durabilitycut.Resource) error {
	_, statErr := os.Stat(path)
	existed := statErr == nil
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if !existed {
		return nil
	}
	return observeNamespaceMutation(durabilitycut.NamespaceUnlink, resource, root, path, "")
}

func renamePersistentFile(root, oldPath, newPath string, resource durabilitycut.Resource) (bool, error) {
	if err := os.Rename(oldPath, newPath); err != nil {
		return false, err
	}
	return true, observeNamespaceMutation(durabilitycut.NamespaceRename, resource, root, oldPath, newPath)
}

func writePersistentFile(root, path string, data []byte, perm os.FileMode, resource durabilitycut.Resource) error {
	_, statErr := os.Stat(path)
	created := os.IsNotExist(statErr)
	if err := os.WriteFile(path, data, perm); err != nil {
		return err
	}
	if !created {
		return nil
	}
	return observeNamespaceMutation(durabilitycut.NamespaceCreate, resource, root, "", path)
}

func observeCreatedPersistentFile(root, path string, resource durabilitycut.Resource, created bool) error {
	if !created {
		return nil
	}
	return observeNamespaceMutation(durabilitycut.NamespaceCreate, resource, root, "", path)
}

func syncNamespaceDirectory(dir string, resource durabilitycut.Resource, before, after durabilitycut.Point) error {
	if err := durabilitycut.EmitPath(before, resource, dir, dir); err != nil {
		return errors.Join(err, ErrRecoveryRequired)
	}
	if err := syncDirFn(dir); err != nil {
		return errors.Join(err, ErrRecoveryRequired)
	}
	if err := durabilitycut.EmitPath(after, resource, dir, dir); err != nil {
		return errors.Join(err, ErrRecoveryRequired)
	}
	return nil
}

func syncNewFileNamespaceDirectory(dir string, resource durabilitycut.Resource) error {
	return syncNamespaceDirectory(
		dir,
		resource,
		durabilitycut.BeforeNewFileDirectorySync,
		durabilitycut.AfterNewFileDirectorySync,
	)
}

func syncDeletionNamespaceDirectory(dir string, resource durabilitycut.Resource) error {
	return syncNamespaceDirectory(
		dir,
		resource,
		durabilitycut.BeforeDeletionDirectorySync,
		durabilitycut.AfterDeletionDirectorySync,
	)
}

func (db *DB) syncDeletionNamespaceDirectoryOrPoison(dir string, resource durabilitycut.Resource, operation string) error {
	err := syncDeletionNamespaceDirectory(dir, resource)
	if err == nil {
		return nil
	}
	err = fmt.Errorf("%s: %w", operation, err)
	if db != nil {
		db.publicationPoisoned.Store(true)
		db.reportError(err)
	}
	return err
}

// The owned producer fences the exact parent retained by the retirement lease,
// rather than reopening an alias. All normal and failure callbacks are admitted
// before the actual barrier; error custody poisons the DB before returning.
func (db *DB) syncOwnedDeletionParentV6(parent *os.File, path string, resource durabilitycut.Resource, w *iterator.OrdinalScanWork) (bool, error) {
	if db == nil || parent == nil || w == nil {
		return false, ErrRecoveryRequired
	}
	if !w.Reserve(4, 2*uint64(unsafe.Sizeof(DB{}))+uint64(len(path))*8+1024) {
		return false, nil
	}
	dir := filepath.Dir(path)
	e := durabilitycut.EmitPath(durabilitycut.BeforeDeletionDirectorySync, resource, dir, dir)
	if e == nil {
		e = parent.Sync()
	}
	if e == nil {
		e = durabilitycut.EmitPath(durabilitycut.AfterDeletionDirectorySync, resource, dir, dir)
	}
	if e != nil {
		e = errors.Join(ErrRecoveryRequired, e)
		db.publicationPoisoned.Store(true)
		db.reportError(e)
		return false, e
	}
	return true, nil
}
