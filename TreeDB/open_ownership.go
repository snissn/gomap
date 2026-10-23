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
	dir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, err
	}
	root := openOwnershipRoot(dir)
	// Both <root> and <root>/maindb are accepted public spellings. Check
	// markers before layout validation, including an as-yet uninitialized
	// maindb path, so refusal itself cannot create a second namespace.
	refuse := func() error {
		for _, candidate := range [...]string{dir, root, filepath.Join(root, "maindb")} {
			if err := refuseExternalRecoveryDirectory(candidate); err != nil {
				return err
			}
		}
		return nil
	}
	if err := refuse(); err != nil {
		return nil, err
	}
	if _, err := resolveOpenDirLayout(dir, opts.DisableSideStores); err != nil {
		return nil, err
	}
	path := filepath.Join(root, "INIT.LOCK")
	var lock *lockfile.Lock
	if opts.ReadOnly {
		lock, err = lockfile.AcquireShared(path)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	} else {
		if err := backenddb.EnsureStorageLayoutDirsForOpen(0755, path, root); err != nil {
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
	if err := refuse(); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}

// Initialization uses one namespace lock even before index.db or side stores
// exist. Keep the maindb spelling canonical for flat/side-store-disabled opens
// too: it must not race an external owner selecting the enclosing root.
func openOwnershipRoot(absoluteDir string) string {
	if filepath.Base(absoluteDir) == "maindb" {
		return filepath.Dir(absoluteDir)
	}
	return absoluteDir
}

func refuseExternalRecoveryEnclosingLayout(dir, root string) error {
	if dir == root {
		return nil
	}
	if err := refuseExternalRecoveryDirectory(root); err != nil {
		return err
	}
	if _, err := os.Lstat(filepath.Join(root, "index.db")); err == nil {
		return fmt.Errorf("%w: enclosing directory already contains a flat database", ErrExternalRecovery)
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
