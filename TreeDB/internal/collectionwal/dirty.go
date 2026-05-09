package collectionwal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	segmentPrefix = "collection-l"
	segmentSuffix = ".log"
)

// IsSegmentName reports whether name is a canonical collection WAL segment
// name. The v1 cleanup scanner treats any such segment as dirty until the full
// segment classifier and cleanup-manifest reader are available.
func IsSegmentName(name string) bool {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(name, segmentPrefix), segmentSuffix)
	parts := strings.SplitN(rest, "-", 2)
	if len(parts) != 2 {
		return false
	}
	lane, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil || lane > uint64(^uint32(0)) {
		return false
	}
	_, err = strconv.ParseUint(parts[1], 10, 64)
	return err == nil
}

// DirtySegments returns collection WAL segments in dbDir/wal. It intentionally
// does not consult cleanup metadata yet; until that reader exists, every
// collection WAL segment is treated as requiring read-write recovery.
func DirtySegments(dbDir string) ([]string, error) {
	walDir := filepath.Join(dbDir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var dirty []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if IsSegmentName(name) {
			dirty = append(dirty, filepath.Join(walDir, name))
		}
	}
	sort.Strings(dirty)
	return dirty, nil
}

func RequireCleanForReadOnlyOpen(dbDir string) error {
	return requireNoDirtySegments(dbDir, "read-only open")
}

func RequireCleanForOfflineMaintenance(dbDir string) error {
	return requireNoDirtySegments(dbDir, "offline maintenance")
}

func requireNoDirtySegments(dbDir, operation string) error {
	dirty, err := DirtySegments(dbDir)
	if err != nil {
		return err
	}
	if len(dirty) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s found collection WAL segment %s", ErrCollectionWALRecoveryRequired, operation, filepath.Base(dirty[0]))
}
