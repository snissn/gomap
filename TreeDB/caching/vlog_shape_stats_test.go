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
	cdb.valueLogDictCurrentKByClass[vlogDictClassSingleValue].Store(7)
	cdb.valueLogDictLastAppliedDictIDByClass[vlogDictClassOuterLeaf].Store(22)
	cdb.valueLogDictCurrentKByClass[vlogDictClassOuterLeaf].Store(13)

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
	if got := stats["treedb.cache.vlog_dict.current_k.outer_leaf"]; got != "13" {
		t.Fatalf("outer_leaf current k=%q want 13", got)
	}
}
