package db

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

const (
	indexFileName      = "index.db"
	indexNewFileName   = "index.db.new"
	indexBakFileName   = "index.db.bak"
	indexReadyFileName = "index.db.new.ready"
)

var syncDirFn = func(dir string) (err error) {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// recoverIndexSwap is a best-effort recovery helper for crash-safe offline
// vacuum swaps.
//
// It prevents Open from accidentally creating an empty new DB when a previous
// vacuum crashed after moving index.db aside.
func recoverIndexSwap(dir string) error {
	if dir == "" {
		return fmt.Errorf("recover index swap: empty dir")
	}

	indexPath := filepath.Join(dir, indexFileName)
	newPath := filepath.Join(dir, indexNewFileName)
	bakPath := filepath.Join(dir, indexBakFileName)
	readyPath := filepath.Join(dir, indexReadyFileName)

	_, indexErr := os.Stat(indexPath)
	indexExists := indexErr == nil

	_, newErr := os.Stat(newPath)
	newExists := newErr == nil

	_, bakErr := os.Stat(bakPath)
	bakExists := bakErr == nil

	_, readyErr := os.Stat(readyPath)
	readyExists := readyErr == nil

	if indexExists {
		// If a prior vacuum crashed before the swap, clean up the temp artifacts so
		// future opens are unambiguous.
		namespaceChanged := false
		if newExists {
			removed, err := removePersistentFileBestEffortResult(dir, newPath, durabilitycut.ResourceIndex)
			if err != nil {
				return err
			}
			namespaceChanged = namespaceChanged || removed
		}
		if readyExists {
			removed, err := removePersistentFileBestEffortResult(dir, readyPath, durabilitycut.ResourceIndex)
			if err != nil {
				return err
			}
			namespaceChanged = namespaceChanged || removed
		}
		if namespaceChanged {
			if err := syncDeletionNamespaceDirectory(dir, durabilitycut.ResourceIndex); err != nil {
				return err
			}
		}
		// Keep bak around as a safety net; it will be removed by a successful
		// vacuum run.
		return nil
	}

	// If the primary index is missing, never create a fresh DB if there are any
	// signs of existing data.
	if !bakExists && !(newExists && readyExists) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, ent := range entries {
			name := ent.Name()
			if name == "LOCK" {
				continue
			}
			if name == indexNewFileName || name == indexBakFileName || name == indexReadyFileName {
				continue
			}
			if ent.IsDir() && name == "wal" {
				// WAL-only state is recoverable: Open will create a new index.db and
				// replay WAL records into it.
				continue
			}
		}
		// Directory looks empty (new DB); allow Open to create index.db.
		return nil
	}

	// Prefer the new index only if it was fully written and marked ready.
	if newExists && readyExists {
		if _, err := renamePersistentFile(dir, newPath, indexPath, durabilitycut.ResourceIndex); err != nil {
			return err
		}
		if err := removePersistentFileBestEffort(dir, readyPath, durabilitycut.ResourceIndex); err != nil {
			return err
		}
		if err := syncNewFileNamespaceDirectory(dir, durabilitycut.ResourceIndex); err != nil {
			return err
		}
		return nil
	}

	// Fall back to restoring the backup.
	if bakExists {
		if _, err := renamePersistentFile(dir, bakPath, indexPath, durabilitycut.ResourceIndex); err != nil {
			return err
		}
		if err := syncNewFileNamespaceDirectory(dir, durabilitycut.ResourceIndex); err != nil {
			return err
		}
		return nil
	}

	return fmt.Errorf("recover index swap: %s missing and no recoverable backup present", indexFileName)
}
