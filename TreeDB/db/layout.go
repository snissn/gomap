package db

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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

// ErrNamespacePersistenceUnsupported reports that the active platform or
// filesystem cannot establish the directory namespace durability promised by
// a successful writable open.
var ErrNamespacePersistenceUnsupported = rootpublication.ErrNamespacePersistenceUnsupported

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
	return EnsureStorageLayoutDirs(
		0o700,
		layout.rootDir,
		layout.walDir,
		layout.valueVLogDir,
		layout.leafVLogDir,
		layout.columnAssetDir,
	)
}

// EnsureStorageLayoutDirs creates the requested directory paths in dependency
// order and persists exactly the namespace parents of directories created by
// this call. Parents are synchronized deepest-first so a newly created DB root
// is durable in its own parent only after its child layout is durable.
//
// This is exported for the public TreeDB wrapper, which owns the composite
// maindb/dictdb/templatedb layout above the backend package.
func EnsureStorageLayoutDirs(mode fs.FileMode, paths ...string) error {
	missingByPath := make([][]string, len(paths))
	needsCreation := false
	for i, path := range paths {
		missing, err := missingStorageLayoutDirs(path)
		if err != nil {
			return err
		}
		missingByPath[i] = missing
		needsCreation = needsCreation || len(missing) != 0
	}
	if !needsCreation {
		return nil
	}
	if !rootpublication.StableNamespaceCreationSupported() {
		return fmt.Errorf("%w: storage layout directory creation is unavailable on %s", ErrNamespacePersistenceUnsupported, runtime.GOOS)
	}
	if !rootpublication.StableRelativeNamespaceSupported() {
		return ensureStorageLayoutDirsCreateThroughChild(mode, paths)
	}
	parentsToSync := make(map[string]struct{}, len(paths)+1)
	for _, missing := range missingByPath {
		createdPaths, err := createMissingStorageLayoutDirs(missing, mode)
		if err != nil {
			return err
		}
		for _, created := range createdPaths {
			parent := filepath.Dir(created)
			if parent != "" && parent != created {
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
	sort.Slice(parents, func(i, j int) bool {
		leftDepth := storageLayoutPathDepth(parents[i])
		rightDepth := storageLayoutPathDepth(parents[j])
		if leftDepth != rightDepth {
			return leftDepth > rightDepth
		}
		return parents[i] < parents[j]
	})
	for _, parent := range parents {
		if err := syncStorageLayoutDir(parent); err != nil {
			return fmt.Errorf("treedb: persist storage layout namespace %q: %w", parent, err)
		}
	}
	return nil
}

func ensureStorageLayoutDirsCreateThroughChild(mode fs.FileMode, paths []string) error {
	for _, path := range paths {
		missing, err := missingStorageLayoutDirs(path)
		if err != nil {
			return err
		}
		if len(missing) == 0 {
			continue
		}
		parentPath := filepath.Dir(missing[len(missing)-1])
		parent, err := rootpublication.OpenStableParent(parentPath)
		if err != nil {
			return err
		}
		for i := len(missing) - 1; i >= 0; i-- {
			child, childErr := rootpublication.EnsureStableChildDirectory(parent, filepath.Base(missing[i]), mode, nil)
			closeErr := parent.Close()
			if childErr != nil {
				return childErr
			}
			if closeErr != nil {
				_ = child.Close()
				return closeErr
			}
			parent = child
		}
		if err := parent.Close(); err != nil {
			return err
		}
	}
	return nil
}

func createMissingStorageLayoutDirs(missing []string, mode fs.FileMode) ([]string, error) {
	created := make([]string, 0, len(missing))
	for i := len(missing) - 1; i >= 0; i-- {
		current := missing[i]
		if err := os.Mkdir(current, mode); err != nil {
			if !os.IsExist(err) {
				return nil, err
			}
			info, statErr := os.Stat(current)
			if statErr != nil {
				return nil, statErr
			}
			if !info.IsDir() {
				return nil, fmt.Errorf("treedb: storage layout path %q is not a directory", current)
			}
			continue
		}
		created = append(created, current)
	}
	return created, nil
}

func missingStorageLayoutDirs(path string) ([]string, error) {
	if path == "" {
		return nil, fmt.Errorf("treedb: empty storage layout path")
	}
	path = filepath.Clean(path)
	var missing []string
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return nil, fmt.Errorf("treedb: storage layout path %q is not a directory", current)
			}
			break
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == "" || parent == current {
			return nil, fmt.Errorf("treedb: storage layout has no existing ancestor for %q", path)
		}
	}

	return missing, nil
}

func storageLayoutPathDepth(path string) int {
	clean := filepath.Clean(path)
	depth := 0
	for {
		parent := filepath.Dir(clean)
		if parent == clean {
			return depth
		}
		depth++
		clean = parent
	}
}

var syncStorageLayoutDir = syncStorageLayoutDirRequired

func syncStorageLayoutDirRequired(dir string) error {
	if dir == "" {
		return fmt.Errorf("treedb: empty storage layout namespace")
	}
	if runtime.GOOS == "windows" {
		return fmt.Errorf("%w: generic parent-directory sync is unavailable on windows", ErrNamespacePersistenceUnsupported)
	}
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if err := file.Sync(); err != nil {
		lowerErr := strings.ToLower(err.Error())
		if errors.Is(err, syscall.EINVAL) ||
			errors.Is(err, syscall.EPERM) ||
			errors.Is(err, syscall.ENOTSUP) ||
			errors.Is(err, syscall.ENOSYS) ||
			strings.Contains(lowerErr, "not supported") {
			return fmt.Errorf("%w: sync directory %q: %v", ErrNamespacePersistenceUnsupported, dir, err)
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
