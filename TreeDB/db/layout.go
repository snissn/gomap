package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
)

const (
	walDirName         = "wal"
	valueVLogDirName   = "value_vlog"
	leafVLogDirName    = "leaf_vlog"
	columnAssetDirName = "column_assets"
)

type storageLayout struct {
	rootDir        string
	walDir         string
	valueVLogDir   string
	leafVLogDir    string
	columnAssetDir string
}

func resolveStorageLayout(dir string) storageLayout {
	return storageLayout{
		rootDir:        dir,
		walDir:         filepath.Join(dir, walDirName),
		valueVLogDir:   filepath.Join(dir, valueVLogDirName),
		leafVLogDir:    filepath.Join(dir, leafVLogDirName),
		columnAssetDir: filepath.Join(dir, columnAssetDirName),
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

func ColumnAssetRootDirPath(dir string) string {
	return resolveStorageLayout(dir).columnAssetDir
}

func ensureStorageLayoutDirs(dir string) error {
	layout := resolveStorageLayout(dir)
	parentsToSync := make(map[string]struct{}, 5)
	for _, path := range []string{layout.rootDir, layout.walDir, layout.valueVLogDir, layout.leafVLogDir, layout.columnAssetDir} {
		created, err := ensureStorageLayoutDir(path)
		if err != nil {
			return err
		}
		if created {
			parent := filepath.Dir(path)
			if parent != "" && parent != path {
				parentsToSync[parent] = struct{}{}
			}
		}
	}
	if len(parentsToSync) == 0 {
		return nil
	}
	parents := make([]string, 0, len(parentsToSync))
	for parent := range parentsToSync {
		parents = append(parents, parent)
	}
	sort.Strings(parents)
	for _, parent := range parents {
		if err := syncStorageLayoutDir(parent); err != nil {
			return err
		}
	}
	return nil
}

func ensureStorageLayoutDir(path string) (bool, error) {
	if path == "" {
		return false, fmt.Errorf("treedb: empty storage layout path")
	}
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("treedb: storage layout path %q is not a directory", path)
		}
		return false, nil
	}
	if !os.IsNotExist(err) {
		return false, err
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return false, err
	}
	info, err = os.Stat(path)
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("treedb: storage layout path %q is not a directory", path)
	}
	return true, nil
}

var syncStorageLayoutDir = syncStorageLayoutDirBestEffort

func syncStorageLayoutDirBestEffort(dir string) error {
	if runtime.GOOS == "windows" || dir == "" {
		return nil
	}
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if err := file.Sync(); err != nil {
		lowerErr := strings.ToLower(err.Error())
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EPERM) || strings.Contains(lowerErr, "not supported") {
			return nil
		}
		return err
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
		for _, pattern := range []string{"value-*.log", "vlog-*.log"} {
			matched, err := filepath.Match(pattern, entry.Name())
			if err != nil {
				return false, err
			}
			if matched {
				return true, nil
			}
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
