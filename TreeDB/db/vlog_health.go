package db

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

const valueLogHealthFileName = "vlog_health.json"

type valueLogSegmentHealth struct {
	SegmentBytes int64  `json:"segment_bytes"`
	LiveBytes    int64  `json:"live_bytes"`
	AgeSeconds   int64  `json:"age_seconds"`
	RewriteCount uint64 `json:"rewrite_count"`
	// ReadHotness is reserved for future per-segment read-frequency tracking.
	// It is persisted for schema stability but not actively updated yet.
	ReadHotness         uint64 `json:"read_hotness"`
	LastUpdatedUnixNano int64  `json:"last_updated_unix_nano"`
}

type valueLogHealthFile struct {
	Segments map[uint32]valueLogSegmentHealth `json:"segments"`
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
	for id, h := range raw.Segments {
		out[id] = h
	}
	return out, nil
}

func saveValueLogHealth(path string, health map[uint32]valueLogSegmentHealth) error {
	if path == "" {
		return nil
	}
	raw := valueLogHealthFile{
		Segments: health,
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o600)
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

func advanceSegmentAgeSeconds(h valueLogSegmentHealth, now time.Time) int64 {
	age := h.AgeSeconds
	if age < 0 {
		age = 0
	}
	if h.LastUpdatedUnixNano <= 0 {
		return age
	}
	prevSec := h.LastUpdatedUnixNano / int64(time.Second)
	nowSec := now.Unix()
	if nowSec <= prevSec {
		return age
	}
	delta := nowSec - prevSec
	if delta <= 0 {
		return age
	}
	// Clamp on overflow to preserve monotonic, bounded metadata.
	if age > math.MaxInt64-delta {
		return math.MaxInt64
	}
	return age + delta
}

func updateValueLogHealthAfterGC(dbDir string, set *valuelog.Set, referenced map[uint32]struct{}) error {
	path := valueLogHealthPath(dbDir)
	health, err := loadValueLogHealth(path)
	if err != nil {
		return err
	}
	now := time.Now()

	present := make(map[uint32]struct{}, len(health))
	if set != nil {
		for id, f := range set.Files {
			present[id] = struct{}{}
			h := health[id]
			size := fileSize(f)
			h.SegmentBytes = size
			// GC currently tracks referenced-vs-unreferenced membership, not exact
			// per-segment live bytes. Keep any previously-computed live byte estimate
			// for referenced segments, and clear only when a segment is unreferenced.
			if _, ok := referenced[id]; !ok {
				h.LiveBytes = 0
			} else if h.LiveBytes < 0 {
				h.LiveBytes = 0
			} else if size > 0 && h.LiveBytes > size {
				h.LiveBytes = size
			}
			// Avoid repeated per-segment stat calls on the GC fast path. For new
			// segments initialize from filesystem metadata once; thereafter preserve
			// age with monotonic last-update deltas.
			if h.LastUpdatedUnixNano == 0 {
				h.AgeSeconds = segmentAgeSeconds(f.Path, now)
			} else {
				h.AgeSeconds = advanceSegmentAgeSeconds(h, now)
			}
			h.LastUpdatedUnixNano = now.UnixNano()
			health[id] = h
		}
	}

	segments, err := listValueLogSegments(dbDir)
	if err != nil {
		return err
	}
	for _, seg := range segments {
		if !seg.valueLog {
			continue
		}
		id := seg.fileID
		if _, ok := present[id]; ok {
			continue
		}
		h := health[id]
		if info, err := os.Stat(seg.path); err == nil {
			h.SegmentBytes = info.Size()
		}
		if _, ok := referenced[id]; !ok {
			h.LiveBytes = 0
		} else if h.LiveBytes < 0 {
			h.LiveBytes = 0
		} else if h.SegmentBytes > 0 && h.LiveBytes > h.SegmentBytes {
			h.LiveBytes = h.SegmentBytes
		}
		h.AgeSeconds = segmentAgeSeconds(seg.path, now)
		h.LastUpdatedUnixNano = now.UnixNano()
		health[id] = h
		present[id] = struct{}{}
	}

	for id := range health {
		if _, ok := present[id]; !ok {
			delete(health, id)
		}
	}
	return saveValueLogHealth(path, health)
}

func updateValueLogHealthAfterRewrite(dbDir string, oldValueIDs map[uint32]struct{}, set *valuelog.Set) error {
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

	// Online rewrite callers can provide a current manager set and avoid an
	// expensive directory rescan on the hot path.
	if set != nil {
		out := make(map[uint32]valueLogSegmentHealth, len(set.Files))
		for id, f := range set.Files {
			if f == nil {
				continue
			}
			h := health[id]
			if _, wasOld := oldValueIDs[id]; !wasOld {
				if h.RewriteCount < nextRewriteCount {
					h.RewriteCount = nextRewriteCount
				}
			}
			size := fileSize(f)
			h.SegmentBytes = size
			h.LiveBytes = size
			// Online rewrite callers pass a manager set; keep age monotonic from
			// prior timestamps when available, but initialize from filesystem
			// metadata when health metadata is missing/corrupt.
			if h.LastUpdatedUnixNano == 0 {
				h.AgeSeconds = segmentAgeSeconds(f.Path, now)
			} else {
				h.AgeSeconds = advanceSegmentAgeSeconds(h, now)
			}
			h.LastUpdatedUnixNano = now.UnixNano()
			out[id] = h
		}
		return saveValueLogHealth(path, out)
	}

	segments, err := listValueLogSegments(dbDir)
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
