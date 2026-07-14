package collections

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type removeStableColumnAssetChildFunc func(parent *os.File, name, diagnosticPath string) error

var columnAssetStableDeleteTestHooks = struct {
	sync.RWMutex
	beforeSegmentLock func()
	beforeValidation  func()
}{}

func removeStableColumnAssetChild(parent *os.File, name, _ string) error {
	return rootpublication.RemoveStableChildFile(parent, name)
}

// deleteColumnAssetSegmentStable fences the exact opened identity before a
// handle-relative unlink. Internal producers and deleters serialize namespace
// mutation; the retained-link validation additionally fails closed if the name
// was rebound before that validation. Hardening against an uncooperative
// external actor rebinding the name after validation remains adjacent work.
func deleteColumnAssetSegmentStable(path string, registry *rootpublication.IdentityPinRegistry, remove removeStableColumnAssetChildFunc) (bool, error) {
	if registry == nil || remove == nil {
		return false, fmt.Errorf("%w: column asset stable delete requires registry and remover", rootpublication.ErrUnresolvedResource)
	}
	columnAssetStableDeleteTestHooks.RLock()
	beforeSegmentLock := columnAssetStableDeleteTestHooks.beforeSegmentLock
	columnAssetStableDeleteTestHooks.RUnlock()
	if beforeSegmentLock != nil {
		beforeSegmentLock()
	}
	// Match producer ordering: segment lock first, then identity registry. GC
	// and rewrite already hold the outer collection mutation lock when present.
	segmentLock := columnAssetSegmentWriteLock(path)
	segmentLock.Lock()
	defer segmentLock.Unlock()
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}
	defer parent.Close()
	name := filepath.Base(path)
	file, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}
	defer file.Close()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return false, err
	}
	lease, err := registry.BeginDeleteAt(identity, filepath.ToSlash(path))
	if err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			lease.Abort()
		}
	}()
	columnAssetStableDeleteTestHooks.RLock()
	beforeValidation := columnAssetStableDeleteTestHooks.beforeValidation
	columnAssetStableDeleteTestHooks.RUnlock()
	if beforeValidation != nil {
		beforeValidation()
	}
	if err := rootpublication.ValidateStableChildLink(parent, file, name); err != nil {
		return false, errors.Join(rootpublication.ErrResourceConflict, err)
	}
	if err := remove(parent, name, path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	lease.CommitDeleted()
	committed = true
	return true, nil
}

func setColumnAssetStableDeleteBeforeSegmentLockTestHook(hook func()) func() {
	columnAssetStableDeleteTestHooks.Lock()
	previous := columnAssetStableDeleteTestHooks.beforeSegmentLock
	columnAssetStableDeleteTestHooks.beforeSegmentLock = hook
	columnAssetStableDeleteTestHooks.Unlock()
	return func() {
		columnAssetStableDeleteTestHooks.Lock()
		columnAssetStableDeleteTestHooks.beforeSegmentLock = previous
		columnAssetStableDeleteTestHooks.Unlock()
	}
}

func setColumnAssetStableDeleteBeforeValidationTestHook(hook func()) func() {
	columnAssetStableDeleteTestHooks.Lock()
	previous := columnAssetStableDeleteTestHooks.beforeValidation
	columnAssetStableDeleteTestHooks.beforeValidation = hook
	columnAssetStableDeleteTestHooks.Unlock()
	return func() {
		columnAssetStableDeleteTestHooks.Lock()
		columnAssetStableDeleteTestHooks.beforeValidation = previous
		columnAssetStableDeleteTestHooks.Unlock()
	}
}
