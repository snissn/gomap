package caching

import (
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestStats_ExportsVlogShapeKeys(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	cdb, err := Open(dir, backend, Options{
		AllowUnsafe:           true,
		DisableWAL:            true,
		ForceValueLogPointers: true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	backendOwnedByDB = true
	t.Cleanup(func() { _ = cdb.Close() })
	cdb.testSkipVlogCheckpointKick = true

	stats := cdb.Stats()
	if stats == nil {
		t.Fatalf("Stats() = nil")
	}

	needUint := func(k string) {
		t.Helper()
		v, ok := stats[k]
		if !ok {
			t.Fatalf("missing stats key %q", k)
		}
		if _, err := strconv.ParseUint(v, 10, 64); err != nil {
			t.Fatalf("stats[%q]=%q not uint: %v", k, v, err)
		}
	}
	needInt := func(k string) {
		t.Helper()
		v, ok := stats[k]
		if !ok {
			t.Fatalf("missing stats key %q", k)
		}
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			t.Fatalf("stats[%q]=%q not int: %v", k, v, err)
		}
	}

	needInt("treedb.cache.vlog_shape.bytes_total")
	needInt("treedb.cache.vlog_shape.l0.bytes_total")
	needUint("treedb.cache.vlog_shape.segments_total")
	needUint("treedb.cache.vlog_shape.l0.segments_total")

	needUint("treedb.cache.vlog_shape.lane.0.segments_total")
	needUint("treedb.cache.vlog_shape.lane.0.segments_closed")
	needUint("treedb.cache.vlog_shape.lane.0.segment_current")
	needInt("treedb.cache.vlog_shape.lane.0.bytes_total")
	needInt("treedb.cache.vlog_shape.lane.0.bytes_closed")
	needInt("treedb.cache.vlog_shape.lane.0.bytes_live")
	needUint("treedb.cache.vlog_shape.lane.0.rotations_total")
	needUint("treedb.cache.vlog_shape.lane.0.rotations_idle_total")
}

func TestStats_ExportsValueLogDictClassKeys(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	cdb, err := Open(dir, backend, Options{
		AllowUnsafe:           true,
		DisableWAL:            true,
		ForceValueLogPointers: true,
		ValueLogDictClassMode: uint8(vlogDictClassModeSplitOuterLeaf),
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	backendOwnedByDB = true
	t.Cleanup(func() { _ = cdb.Close() })

	cdb.valueLogDictLastAppliedDictIDByClass[vlogDictClassSingleValue].Store(11)
	cdb.valueLogDictLastPublishUnixNanoByClass[vlogDictClassSingleValue].Store(101)
	cdb.valueLogDictLastPublishFramesTotalByClass[vlogDictClassSingleValue].Store(29)
	cdb.valueLogDictPublishCountByClass[vlogDictClassSingleValue].Store(1)
	cdb.valueLogAutoChooseDictByClass[vlogDictClassSingleValue].Store(9)
	cdb.valueLogDictCurrentKByClass[vlogDictClassSingleValue].Store(7)
	cdb.dictCurrentCachedByClass[vlogDictClassSingleValue].Store(17)
	cdb.valueLogDictWriteSelectedByClass[vlogDictClassSingleValue].Store(3)
	cdb.valueLogDictWriteFinalByClass[vlogDictClassSingleValue].Store(2)
	cdb.valueLogDictLastAppliedDictIDByClass[vlogDictClassOuterLeaf].Store(22)
	cdb.valueLogDictLastPublishUnixNanoByClass[vlogDictClassOuterLeaf].Store(202)
	cdb.valueLogDictLastPublishFramesTotalByClass[vlogDictClassOuterLeaf].Store(57)
	cdb.valueLogDictPublishCountByClass[vlogDictClassOuterLeaf].Store(4)
	cdb.valueLogAutoChooseDictByClass[vlogDictClassOuterLeaf].Store(12)
	cdb.valueLogDictCurrentKByClass[vlogDictClassOuterLeaf].Store(13)
	cdb.dictCurrentCachedByClass[vlogDictClassOuterLeaf].Store(27)
	cdb.valueLogDictWriteSelectedByClass[vlogDictClassOuterLeaf].Store(11)
	cdb.valueLogDictWriteFinalByClass[vlogDictClassOuterLeaf].Store(5)
	cdb.valueLogDictWriteFallbackPauseByClass[vlogDictClassOuterLeaf].Store(2)
	cdb.valueLogDictWriteFallbackBypassByClass[vlogDictClassOuterLeaf].Store(3)
	cdb.valueLogDictWriteFallbackSizeFloorByClass[vlogDictClassOuterLeaf].Store(4)
	cdb.valueLogDictWriteFallbackDictLoadByClass[vlogDictClassOuterLeaf].Store(6)

	stats := cdb.Stats()
	if got := stats["treedb.cache.vlog_dict.last_applied_dict_id.single_value"]; got != "11" {
		t.Fatalf("single_value dict id=%q want 11", got)
	}
	if got := stats["treedb.cache.vlog_dict.current_k.single_value"]; got != "7" {
		t.Fatalf("single_value current k=%q want 7", got)
	}
	if got := stats["treedb.cache.vlog_dict.last_applied_dict_id.outer_leaf"]; got != "22" {
		t.Fatalf("outer_leaf dict id=%q want 22", got)
	}
	if got := stats["treedb.cache.vlog_dict.last_publish_unix_nano.single_value"]; got != "101" {
		t.Fatalf("single_value publish unix nano=%q want 101", got)
	}
	if got := stats["treedb.cache.vlog_dict.last_publish_frames_total.single_value"]; got != "29" {
		t.Fatalf("single_value publish frames total=%q want 29", got)
	}
	if got := stats["treedb.cache.vlog_dict.publish_count.single_value"]; got != "1" {
		t.Fatalf("single_value publish count=%q want 1", got)
	}
	if got := stats["treedb.cache.vlog_auto.choose_dict.single_value"]; got != "9" {
		t.Fatalf("single_value auto choose dict=%q want 9", got)
	}
	if got := stats["treedb.cache.vlog_dict.last_publish_unix_nano.outer_leaf"]; got != "202" {
		t.Fatalf("outer_leaf publish unix nano=%q want 202", got)
	}
	if got := stats["treedb.cache.vlog_dict.last_publish_frames_total.outer_leaf"]; got != "57" {
		t.Fatalf("outer_leaf publish frames total=%q want 57", got)
	}
	if got := stats["treedb.cache.vlog_dict.publish_count.outer_leaf"]; got != "4" {
		t.Fatalf("outer_leaf publish count=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_auto.choose_dict.outer_leaf"]; got != "12" {
		t.Fatalf("outer_leaf auto choose dict=%q want 12", got)
	}
	if got := stats["treedb.cache.vlog_dict.current_k.outer_leaf"]; got != "13" {
		t.Fatalf("outer_leaf current k=%q want 13", got)
	}
	if got := stats["treedb.cache.vlog_dict.current_cached_id.single_value"]; got != "17" {
		t.Fatalf("single_value current cached id=%q want 17", got)
	}
	if got := stats["treedb.cache.vlog_dict.current_cached_id.outer_leaf"]; got != "27" {
		t.Fatalf("outer_leaf current cached id=%q want 27", got)
	}
	if got := stats["treedb.cache.vlog_dict.write_selected.single_value"]; got != "3" {
		t.Fatalf("single_value write selected=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_dict.write_final.single_value"]; got != "2" {
		t.Fatalf("single_value write final=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_dict.write_selected.outer_leaf"]; got != "11" {
		t.Fatalf("outer_leaf write selected=%q want 11", got)
	}
	if got := stats["treedb.cache.vlog_dict.write_final.outer_leaf"]; got != "5" {
		t.Fatalf("outer_leaf write final=%q want 5", got)
	}
	if got := stats["treedb.cache.vlog_dict.write_fallback.pause.outer_leaf"]; got != "2" {
		t.Fatalf("outer_leaf fallback pause=%q want 2", got)
	}
	if got := stats["treedb.cache.vlog_dict.write_fallback.classifier_bypass.outer_leaf"]; got != "3" {
		t.Fatalf("outer_leaf fallback classifier bypass=%q want 3", got)
	}
	if got := stats["treedb.cache.vlog_dict.write_fallback.size_floor.outer_leaf"]; got != "4" {
		t.Fatalf("outer_leaf fallback size floor=%q want 4", got)
	}
	if got := stats["treedb.cache.vlog_dict.write_fallback.dict_load.outer_leaf"]; got != "6" {
		t.Fatalf("outer_leaf fallback dict load=%q want 6", got)
	}
}
