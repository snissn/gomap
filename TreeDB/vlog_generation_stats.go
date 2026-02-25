package treedb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
)

const (
	vlogGenerationHot  = "hot"
	vlogGenerationWarm = "warm"
	vlogGenerationCold = "cold"
)

type vlogHealthSegment struct {
	SegmentBytes int64  `json:"segment_bytes"`
	LiveBytes    int64  `json:"live_bytes"`
	RewriteCount uint64 `json:"rewrite_count"`
}

type vlogHealthFile struct {
	Segments map[string]vlogHealthSegment `json:"segments"`
}

func generationForRewriteCount(rewriteCount uint64) string {
	switch {
	case rewriteCount == 0:
		return vlogGenerationHot
	case rewriteCount == 1:
		return vlogGenerationWarm
	default:
		return vlogGenerationCold
	}
}

func addVlogGenerationStats(out map[string]string, rootDir string) {
	if out == nil || rootDir == "" {
		return
	}
	path := filepath.Join(rootDir, "maindb", "vlog_health.json")
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return
	}
	var f vlogHealthFile
	if err := json.Unmarshal(data, &f); err != nil || len(f.Segments) == 0 {
		return
	}

	type agg struct {
		segments int64
		totalB   int64
		liveB    int64
		staleB   int64
	}
	byGen := map[string]*agg{
		vlogGenerationHot:  {},
		vlogGenerationWarm: {},
		vlogGenerationCold: {},
	}

	for k, seg := range f.Segments {
		if _, err := strconv.ParseUint(k, 10, 32); err != nil {
			continue
		}
		if seg.SegmentBytes < 0 {
			seg.SegmentBytes = 0
		}
		if seg.LiveBytes < 0 {
			seg.LiveBytes = 0
		}
		if seg.LiveBytes > seg.SegmentBytes {
			seg.LiveBytes = seg.SegmentBytes
		}
		gen := generationForRewriteCount(seg.RewriteCount)
		a := byGen[gen]
		a.segments++
		a.totalB += seg.SegmentBytes
		a.liveB += seg.LiveBytes
		a.staleB += seg.SegmentBytes - seg.LiveBytes
	}

	totalSegments := byGen[vlogGenerationHot].segments + byGen[vlogGenerationWarm].segments + byGen[vlogGenerationCold].segments
	if totalSegments == 0 {
		return
	}

	for _, gen := range []string{vlogGenerationHot, vlogGenerationWarm, vlogGenerationCold} {
		a := byGen[gen]
		out["treedb.vlog.generation."+gen+".segments"] = strconv.FormatInt(a.segments, 10)
		out["treedb.vlog.generation."+gen+".bytes_total"] = strconv.FormatInt(a.totalB, 10)
		out["treedb.vlog.generation."+gen+".bytes_live"] = strconv.FormatInt(a.liveB, 10)
		out["treedb.vlog.generation."+gen+".bytes_stale"] = strconv.FormatInt(a.staleB, 10)
	}
}
