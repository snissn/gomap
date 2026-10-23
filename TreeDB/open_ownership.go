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
	if err := refuseExternalRecoveryDirectory(opts.Dir); err != nil {
		return nil, err
	}
	path := filepath.Join(opts.Dir, "INIT.LOCK")
	var lock *lockfile.Lock
	var err error
	if opts.ReadOnly {
		lock, err = lockfile.AcquireShared(path)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	} else {
		if err := backenddb.EnsureStorageLayoutDirsForOpen(0755, path, opts.Dir); err != nil {
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
	if err := refuseExternalRecoveryDirectory(opts.Dir); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}

func refuseExternalRecoveryDirectory(dir string) error {
	if _, err := os.Lstat(filepath.Join(dir, externalRecoveryMarker)); err == nil {
		return fmt.Errorf("%w: use OpenExternalRecovery for this directory", ErrExternalRecovery)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
