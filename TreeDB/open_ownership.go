package treedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
)

// Ordinary and external initialization serialize before inspecting/persisting
// the recovery-policy marker. Keep the inode permanently: unlinking a lock file
// could let another process lock a different inode at the same pathname.
func acquireOrdinaryOpenOwnership(opts Options) (func(), error) {
	if opts.Dir == "" {
		return nil, errors.New("treedb: db dir required")
	}
	dir, err := resolveOpenOwnershipDirectory(opts.Dir)
	if err != nil {
		return nil, err
	}
	if err := refuseExternalRecoveryDirectory(dir); err != nil {
		return nil, err
	}
	if _, err := resolveOpenDir(dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "INIT.LOCK")
	var lock *lockfile.Lock
	if opts.ReadOnly {
		lock, err = lockfile.AcquireShared(path)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	} else {
		if err := backenddb.EnsureStorageLayoutDirsForOpen(0755, path, dir); err != nil {
			return nil, err
		}
		lock, err = lockfile.Acquire(path)
	}
	if err != nil {
		return nil, err
	}
	unlock := func() {
		if lock != nil {
			_ = lock.Close()
		}
	}
	if err := refuseExternalRecoveryDirectory(dir); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}

// openOwnershipRoot returns the canonical initialization lock directory.
func openOwnershipRoot(absoluteDir string) string {
	return absoluteDir
}

// Existing symlink aliases must select the same namespace before locking.
// Missing new directories retain their absolute spelling; the OS still resolves
// any existing ancestor to the same lock inode.
func resolveOpenOwnershipDirectory(dir string) (string, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return absolute, nil
	}
	return "", err
}

func refuseExternalRecoveryEnclosingLayout(dir, root string) error {
	if dir == root {
		return nil
	}
	if err := refuseExternalRecoveryDirectory(root); err != nil {
		return err
	}
	if _, err := os.Lstat(filepath.Join(root, "index.db")); err == nil {
		return fmt.Errorf("%w: enclosing directory already contains a database", ErrExternalRecovery)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func refuseExternalRecoveryDirectory(dir string) error {
	if _, err := os.Lstat(filepath.Join(dir, externalRecoveryMarker)); err == nil {
		return fmt.Errorf("%w: use OpenExternalRecovery for this directory", ErrExternalRecovery)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
