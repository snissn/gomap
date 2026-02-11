package db

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

const valueLogHealthFileName = "vlog_health.json"

type valueLogSegmentHealth struct {
	SegmentBytes        int64  `json:"segment_bytes"`
	LiveBytes           int64  `json:"live_bytes"`
	AgeSeconds          int64  `json:"age_seconds"`
	RewriteCount        uint64 `json:"rewrite_count"`
	ReadHotness         uint64 `json:"read_hotness"`
	LastUpdatedUnixNano int64  `json:"last_updated_unix_nano"`
}

type valueLogHealthFile struct {
	Segments map[string]valueLogSegmentHealth `json:"segments"`
}

func valueLogHealthPath(dir string) string {
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, valueLogHealthFileName)
}

func loadValueLogHealth(path string) (map[uint32]valueLogSegmentHealth, error) {
	out := make(map[uint32]valueLogSegmentHealth)
	if path == "" {
		return out, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	var raw valueLogHealthFile
	if len(data) > 0 {
		if err := json.Unmarshal(data, &raw); err != nil {
			// Health metadata is rebuildable; tolerate torn/corrupt JSON.
			return out, nil
		}
	}
	for k, v := range raw.Segments {
		id64, err := strconv.ParseUint(k, 10, 32)
		if err != nil {
			continue
		}
		out[uint32(id64)] = v
	}
	return out, nil
}

func saveValueLogHealth(path string, health map[uint32]valueLogSegmentHealth) error {
	if path == "" {
		return nil
	}
	raw := valueLogHealthFile{
		Segments: make(map[string]valueLogSegmentHealth, len(health)),
	}
	for id, h := range health {
		raw.Segments[strconv.FormatUint(uint64(id), 10)] = h
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o644)
}

func segmentAgeSeconds(path string, now time.Time) int64 {
	if path == "" {
		return 0
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	age := now.Sub(info.ModTime())
	if age < 0 {
		return 0
	}
	return int64(age / time.Second)
}

func updateValueLogHealthAfterGC(dbDir string, set *valuelog.Set, referenced map[uint32]struct{}) error {
	path := valueLogHealthPath(dbDir)
	health, err := loadValueLogHealth(path)
	if err != nil {
		return err
	}
	now := time.Now()

	present := make(map[uint32]struct{}, len(set.Files))
	for id, f := range set.Files {
		present[id] = struct{}{}
		h := health[id]
		size := fileSize(f)
		h.SegmentBytes = size
		if _, ok := referenced[id]; ok {
			h.LiveBytes = size
		} else {
			h.LiveBytes = 0
		}
		h.AgeSeconds = segmentAgeSeconds(f.Path, now)
		h.LastUpdatedUnixNano = now.UnixNano()
		health[id] = h
	}

	for id := range health {
		if _, ok := present[id]; !ok {
			delete(health, id)
		}
	}
	return saveValueLogHealth(path, health)
}

func updateValueLogHealthAfterRewrite(dbDir string, oldValueIDs map[uint32]struct{}) error {
	path := valueLogHealthPath(dbDir)
	health, err := loadValueLogHealth(path)
	if err != nil {
		return err
	}
	now := time.Now()
	nextRewriteCount := uint64(1)
	for id := range oldValueIDs {
		if h, ok := health[id]; ok && h.RewriteCount >= nextRewriteCount {
			nextRewriteCount = h.RewriteCount + 1
		}
	}

	segments, err := listWALSegments(dbDir)
	if err != nil {
		return err
	}
	out := make(map[uint32]valueLogSegmentHealth, len(segments))
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		id := seg.fileID
		h := health[id]
		if _, wasOld := oldValueIDs[id]; !wasOld {
			if h.RewriteCount < nextRewriteCount {
				h.RewriteCount = nextRewriteCount
			}
		}
		info, err := os.Stat(seg.path)
		if err == nil {
			h.SegmentBytes = info.Size()
			h.LiveBytes = info.Size()
		}
		h.AgeSeconds = segmentAgeSeconds(seg.path, now)
		h.LastUpdatedUnixNano = now.UnixNano()
		out[id] = h
	}
	return saveValueLogHealth(path, out)
}
