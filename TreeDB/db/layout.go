package db

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	walDirName       = "wal"
	valueVLogDirName = "value_vlog"
	leafVLogDirName  = "leaf_vlog"
)

type storageLayout struct {
	rootDir      string
	walDir       string
	valueVLogDir string
	leafVLogDir  string
}

func resolveStorageLayout(dir string) storageLayout {
	return storageLayout{
		rootDir:      dir,
		walDir:       filepath.Join(dir, walDirName),
		valueVLogDir: filepath.Join(dir, valueVLogDirName),
		leafVLogDir:  filepath.Join(dir, leafVLogDirName),
	}
}

func WALDirPath(dir string) string {
	return resolveStorageLayout(dir).walDir
}

func ValueLogDirPath(dir string) string {
	return resolveStorageLayout(dir).valueVLogDir
}

func LeafLogDirPath(dir string) string {
	return resolveStorageLayout(dir).leafVLogDir
}

func ensureStorageLayoutDirs(dir string) error {
	layout := resolveStorageLayout(dir)
	for _, path := range []string{layout.walDir, layout.valueVLogDir, layout.leafVLogDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
	}
	return nil
}

func hasLegacyMixedWALValueSegments(dir string) (bool, error) {
	layout := resolveStorageLayout(dir)
	entries, err := os.ReadDir(layout.walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matched, err := filepath.Match("value-*.log", entry.Name())
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func ensureNoLegacyMixedWALValueSegments(dir string) error {
	legacy, err := hasLegacyMixedWALValueSegments(dir)
	if err != nil {
		return err
	}
	if !legacy {
		return nil
	}
	return fmt.Errorf("treedb: legacy value-log segments found in %s; rebuild required for split wal/value_vlog layout", resolveStorageLayout(dir).walDir)
}
