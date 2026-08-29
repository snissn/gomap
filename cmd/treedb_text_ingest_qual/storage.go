package main

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

// observeStorage classifies every regular file below dir. The categories are
// physical filesystem bytes, so the logical TextIndexStorageStats components
// must never be added to these values.
func observeStorage(dir string) (storage, error) {
	var result storage
	var other []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		bytes := info.Size()
		switch {
		case rel == "value_vlog" || strings.HasPrefix(rel, "value_vlog/") || rel == "leaf_vlog" || strings.HasPrefix(rel, "leaf_vlog/") || rel == "maindb/value_vlog" || strings.HasPrefix(rel, "maindb/value_vlog/") || rel == "maindb/leaf_vlog" || strings.HasPrefix(rel, "maindb/leaf_vlog/"):
			result.PhysicalValueLogBytes += bytes
		case rel == "wal" || strings.HasPrefix(rel, "wal/") || rel == "maindb/wal" || strings.HasPrefix(rel, "maindb/wal/"):
			result.PhysicalWALBytes += bytes
		case rel == "index.db" || strings.HasSuffix(rel, "/index.db"):
			result.PhysicalIndexPageBytes += bytes
		default:
			// This is an explicit, auditable bucket rather than silently dropping
			// a newly introduced DB file category.
			result.PhysicalOtherBytes += bytes
			other = append(other, rel)
		}
		return nil
	})
	if err != nil {
		return storage{}, fmt.Errorf("walk physical storage: %w", err)
	}
	sort.Strings(other)
	result.OtherPaths = other
	result.PhysicalTotalBytes = result.PhysicalIndexPageBytes + result.PhysicalValueLogBytes + result.PhysicalWALBytes + result.PhysicalOtherBytes
	result.PhysicalTotalWALExcludedBytes = result.PhysicalTotalBytes - result.PhysicalWALBytes
	return result, nil
}
