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

	needUint("treedb.cache.vlog_decode_scratch.small_pool.max_entries")
	needUint("treedb.cache.vlog_decode_scratch.small_pool.retained_bytes")
	needUint("treedb.cache.vlog_decode_scratch.large_pool.max_entries")
	needUint("treedb.cache.vlog_decode_scratch.large_pool.retained_bytes")
	needUint("treedb.cache.vlog_decode_scratch.file_stash.retain_max_bytes")
	needUint("treedb.cache.vlog_decode_scratch.file_stash.retained_bytes")
	needUint("treedb.cache.vlog_writer_append_buf.pool.max_entries")
	needUint("treedb.cache.vlog_writer_append_buf.pool.retained_bytes")
	needUint("treedb.cache.vlog_writer_append_buf.drops_total")
	needUint("treedb.cache.vlog_decode_buffer_grow.read_append_decoded_payload.calls_total")
	needUint("treedb.cache.vlog_decode_buffer_grow.read_append_decoded_payload.dst_fit_calls_total")
	needUint("treedb.cache.vlog_decode_buffer_grow.read_append_template_encoded_payload.calls_total")
}
