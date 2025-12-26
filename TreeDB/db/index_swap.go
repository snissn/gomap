package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	indexFileName      = "index.db"
	indexNewFileName   = "index.db.new"
	indexBakFileName   = "index.db.bak"
	indexReadyFileName = "index.db.new.ready"
)

var syncDirFn = func(dir string) (err error) {
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
		if newExists {
			_ = os.Remove(newPath)
		}
		if readyExists {
			_ = os.Remove(readyPath)
		}
		_ = syncDirFn(dir)
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
			if strings.HasPrefix(name, "data-") && strings.HasSuffix(name, ".slab") {
				return fmt.Errorf("recover index swap: %s missing but slab files exist", indexFileName)
			}
		}
		// Directory looks empty (new DB); allow Open to create index.db.
		return nil
	}

	// Prefer the new index only if it was fully written and marked ready.
	if newExists && readyExists {
		if err := os.Rename(newPath, indexPath); err != nil {
			return err
		}
		_ = os.Remove(readyPath)
		_ = syncDirFn(dir)
		return nil
	}

	// Fall back to restoring the backup.
	if bakExists {
		if err := os.Rename(bakPath, indexPath); err != nil {
			return err
		}
		_ = syncDirFn(dir)
		return nil
	}

	return fmt.Errorf("recover index swap: %s missing and no recoverable backup present", indexFileName)
}
