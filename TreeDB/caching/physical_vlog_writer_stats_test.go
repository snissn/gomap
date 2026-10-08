package caching

import (
	"fmt"
	"math"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestPhysicalValueLogWriterStatsKeepSharedLaneWritersDistinct(t *testing.T) {
	db := &DB{lanes: make([]lane, 1), valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold), valueLogHotLanes: []int{0}, valueLogMaxSegmentBytes: 256 << 20}
	db.initNativeRootValueLogAppendLanesWithWidth(4)
	for i, l := range db.allValueLogWriterLanesSnapshot() {
		recordLaneVlogPayloadSplitObservation(l, vlogPayloadSplitSingleValue, 100, 60, i+1)
		l.vlogRotateThresholdTotal.Store(uint64(i))
	}
	stats := make(map[string]string)
	db.appendCachePhysicalValueLogWriterStats(stats)
	if got := stats["treedb.cache.physical_vlog_writers.configured"]; got != "4" {
		t.Fatalf("physical writers=%s want4", got)
	}
	for i := range 4 {
		prefix := fmt.Sprintf("treedb.cache.physical_vlog_writers.writer.%d", i)
		if stats[prefix+".logical_lane"] != "0" || stats[prefix+".records_total"] != fmt.Sprint(i+1) || stats[prefix+".threshold_rotations_total"] != fmt.Sprint(i) {
			t.Fatalf("shared logical lane collapsed physical writer%d: %v", i, stats)
		}
	}
}

func TestPhysicalValueLogWriterStatsExposeInventoryAbove64(t *testing.T) {
	db := &DB{lanes: make([]lane, 66), valueLogMaxSegmentBytes: 256 << 20}
	recordLaneVlogPayloadSplitObservation(&db.lanes[65], vlogPayloadSplitOuterLeaf, 100, 50, 1)
	stats := make(map[string]string)
	db.appendCachePhysicalValueLogWriterStats(stats)
	if stats["treedb.cache.physical_vlog_writers.configured"] != "66" || stats["treedb.cache.physical_vlog_writers.writer.65.records_total"] != "1" {
		t.Fatal("physical inventory was truncated")
	}
}

func TestLeafLogStatsExposeWorker65(t *testing.T) {
	db := &DB{indexOuterLeavesInValueLog: true, leafLogAppendLanes: make([]*lane, 66)}
	for i := range db.leafLogAppendLanes {
		db.leafLogAppendLanes[i] = &lane{id: leafLogLaneID}
	}
	db.leafLogAppendLanes[65].leafLogAppendCalls.Store(1)
	db.leafLogAppendLanes[65].vlogRotateThresholdTotal.Store(2)
	stats := make(map[string]string)
	db.appendCacheLeafLogLaneStats(stats)
	if stats["treedb.cache.leaf_log_lanes.configured"] != "66" || stats["treedb.cache.leaf_log_lanes.lane.65.threshold_rotations_total"] != "2" {
		t.Fatal("leaf worker65 excluded from physical inventory")
	}
	t.Run("unused-index-holes", func(t *testing.T) {
		db.leafLogAppendLanes[0] = nil
		stats := make(map[string]string)
		db.appendCacheLeafLogLaneStats(stats)
		if stats["treedb.cache.leaf_log_lanes.lane.00.installed"] != "false" || stats["treedb.cache.leaf_log_lanes.lane.65.installed"] != "true" || stats["treedb.cache.leaf_log_lanes.configured"] != "66" {
			t.Fatal("configured leaf index hole was not represented")
		}
		if _, present := stats["treedb.cache.leaf_log_lanes.lane.00.append_calls_total"]; present {
			t.Fatal("unused hole presented as a writer")
		}
	})
}

func TestPhysicalValueLogWriterStatsRejectInvalidAndOverflowedIdentity(t *testing.T) {
	for _, kind := range []string{"records", "bytes", "sequence", "sequence-truncation", "lane-truncation", "lane-negative", "nil"} {
		t.Run(kind, func(t *testing.T) {
			db := &DB{lanes: make([]lane, 1), valueLogMaxSegmentBytes: 256 << 20}
			l := &db.lanes[0]
			switch kind {
			case "records":
				l.vlogPayloadSplitRecords[0].Store(math.MaxUint64)
				l.vlogPayloadSplitRecords[1].Store(1)
			case "bytes":
				l.vlogPayloadSplitRawBytes[0].Store(math.MaxUint64)
				l.vlogPayloadSplitRawBytes[1].Store(1)
			case "sequence":
				l.vlogSeq = -1
			case "sequence-truncation":
				l.vlogSeq, l.vlogPath = int(uint64(1)<<32)+1, "actual-installed-path"
			case "lane-truncation":
				l.id = int(uint64(1) << 32)
			case "lane-negative":
				l.id = -1
			case "nil":
				db.nativeRootValueLogAppendShared = true
				db.nativeRootValueLogAppendLanes = []*lane{l, nil}
			}
			stats := make(map[string]string)
			db.appendCachePhysicalValueLogWriterStats(stats)
			index := 0
			if kind == "nil" {
				index = 1
			}
			if stats[fmt.Sprintf("treedb.cache.physical_vlog_writers.writer.%d.identity_error", index)] == "" {
				t.Fatal("invalid physical observer presented as valid")
			}
		})
	}
}
