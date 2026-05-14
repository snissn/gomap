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

type segmentFileInfo struct {
	path string
	name string
	lane uint32
	seq  uint64
}

func SegmentName(lane uint32, seq uint64) string {
	return fmt.Sprintf("%s%d-%06d%s", segmentPrefix, lane, seq, segmentSuffix)
}

func SegmentPath(dbDir string, lane uint32, seq uint64) string {
	return filepath.Join(dbDir, "wal", SegmentName(lane, seq))
}

// IsSegmentName reports whether name is a canonical collection WAL segment
// name. The v1 cleanup scanner treats any such segment as dirty until the full
// segment classifier and cleanup-manifest reader are available.
func IsSegmentName(name string) bool {
	_, _, ok := ParseSegmentName(name)
	return ok
}

func ParseSegmentName(name string) (lane uint32, seq uint64, ok bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, 0, false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(name, segmentPrefix), segmentSuffix)
	parts := strings.SplitN(rest, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	lane64, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil || lane64 > uint64(^uint32(0)) {
		return 0, 0, false
	}
	seq, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return uint32(lane64), seq, true
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
	var infos []segmentFileInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		lane, seq, ok := ParseSegmentName(name)
		if ok {
			infos = append(infos, segmentFileInfo{
				path: filepath.Join(walDir, name),
				name: name,
				lane: lane,
				seq:  seq,
			})
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].lane != infos[j].lane {
			return infos[i].lane < infos[j].lane
		}
		if infos[i].seq != infos[j].seq {
			return infos[i].seq < infos[j].seq
		}
		return infos[i].name < infos[j].name
	})
	dirty := segmentInfoPaths(infos)
	if err := validateSegmentSequenceContinuity(infos); err != nil {
		return dirty, err
	}
	return dirty, nil
}

func segmentInfoPaths(infos []segmentFileInfo) []string {
	if len(infos) == 0 {
		return nil
	}
	out := make([]string, 0, len(infos))
	for _, info := range infos {
		out = append(out, info.path)
	}
	return out
}

func validateSegmentSequenceContinuity(infos []segmentFileInfo) error {
	var (
		currentLane uint32
		wantSeq     uint64
		haveLane    bool
	)
	for _, info := range infos {
		if !haveLane || info.lane != currentLane {
			currentLane = info.lane
			wantSeq = 1
			haveLane = true
		}
		if info.seq != wantSeq {
			return fmt.Errorf("%w: collection WAL segment gap without cleanup lane=%d got_seq=%d want_seq=%d segment=%s", ErrCollectionWALCorruptMiddle, info.lane, info.seq, wantSeq, info.name)
		}
		if wantSeq != ^uint64(0) {
			wantSeq++
		}
	}
	return nil
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
