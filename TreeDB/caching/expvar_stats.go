package caching

import (
	"expvar"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const treedbExpvarEnabledEnvKey = "TREEDB_ENABLE_EXPVAR_STATS"

var (
	treedbExpvarEnabled     = parseBoolEnvDefault(treedbExpvarEnabledEnvKey, true)
	treedbExpvarPublishOnce sync.Once
	treedbExpvarCurrentDB   atomic.Pointer[DB]
	treedbExpvarDBsMu       sync.RWMutex
	treedbExpvarDBs         = make(map[*DB]struct{})
)

func parseBoolEnvDefault(key string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	switch strings.ToLower(raw) {
	case "1", "true", "on", "yes":
		return true
	case "0", "false", "off", "no":
		return false
	default:
		return def
	}
}

func publishTreeDBExpvarStats() {
	treedbExpvarPublishOnce.Do(func() {
		expvar.Publish("treedb", expvar.Func(func() any {
			return currentTreeDBExpvarStats()
		}))
	})
}

func registerTreeDBExpvarStatsDB(db *DB) {
	if db == nil || !treedbExpvarEnabled {
		return
	}
	publishTreeDBExpvarStats()
	treedbExpvarDBsMu.Lock()
	treedbExpvarDBs[db] = struct{}{}
	treedbExpvarDBsMu.Unlock()
	treedbExpvarCurrentDB.Store(db)
}

func unregisterTreeDBExpvarStatsDB(db *DB) {
	if db == nil || !treedbExpvarEnabled {
		return
	}
	treedbExpvarDBsMu.Lock()
	delete(treedbExpvarDBs, db)
	var replacement *DB
	for candidate := range treedbExpvarDBs {
		replacement = candidate
		break
	}
	treedbExpvarDBsMu.Unlock()
	if treedbExpvarCurrentDB.CompareAndSwap(db, replacement) {
		return
	}
	if replacement != nil {
		treedbExpvarCurrentDB.CompareAndSwap(nil, replacement)
	}
}

func currentTreeDBExpvarStats() map[string]any {
	current := treedbExpvarCurrentDB.Load()

	treedbExpvarDBsMu.RLock()
	dbs := make([]*DB, 0, len(treedbExpvarDBs))
	for db := range treedbExpvarDBs {
		dbs = append(dbs, db)
	}
	treedbExpvarDBsMu.RUnlock()

	if current == nil && len(dbs) == 0 {
		return map[string]any{}
	}

	out := map[string]any{}
	if current != nil {
		for k, v := range selectTreeDBExpvarStats(current.Stats()) {
			out[k] = v
		}
		out["treedb.expvar.current_wal_dir"] = current.dir
	}
	out["treedb.expvar.instances_count"] = len(dbs)

	instances := make(map[string]any, len(dbs))
	for _, db := range dbs {
		if db == nil {
			continue
		}
		key := treeDBExpvarInstanceKey(db)
		instanceStats := selectTreeDBExpvarStats(db.Stats())
		instanceStats["treedb.expvar.wal_dir"] = db.dir
		instanceStats["treedb.expvar.is_current"] = current == db
		instances[key] = instanceStats
	}
	out["instances"] = instances
	return out
}

func treeDBExpvarInstanceKey(db *DB) string {
	if db == nil {
		return ""
	}
	if db.dir == "" {
		return fmt.Sprintf("db_%p", db)
	}
	return fmt.Sprintf("%s#%p", db.dir, db)
}

func selectTreeDBExpvarStats(stats map[string]string) map[string]any {
	if len(stats) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any)
	for k, v := range stats {
		// Export only process-wide metric families under treedb.process.* and
		// select cache/backend families used for mmap/decode/batch-arena,
		// compression, vlog zombie accounting, and live maintenance tracking.
		if isProcessWideExpvarKey(k) ||
			strings.HasPrefix(k, "treedb.process.identity.") ||
			strings.HasPrefix(k, "treedb.command_wal.") ||
			strings.HasPrefix(k, "treedb.public.") ||
			strings.HasPrefix(k, "treedb.maintenance.") ||
			strings.HasPrefix(k, "treedb.bg_vacuum.") ||
			strings.HasPrefix(k, "treedb.flush_admission.") ||
			strings.HasPrefix(k, "treedb.flush_apply.") ||
			strings.HasPrefix(k, "treedb.raw.span_native.") ||
			strings.HasPrefix(k, "treedb.publish.ordered_root_delta_group.") ||
			strings.HasPrefix(k, "treedb.leaf_generation.") ||
			strings.HasPrefix(k, "treedb.vlog.mmap") ||
			strings.HasPrefix(k, "treedb.vlog.decode_buffer_grow.") ||
			strings.HasPrefix(k, "treedb.vlog.decode_scratch.") ||
			strings.HasPrefix(k, "treedb.vlog.writer_append_buf.") ||
			strings.HasPrefix(k, "treedb.cache.flush_apply.") ||
			strings.HasPrefix(k, "treedb.cache.flush_span_run.") ||
			strings.HasPrefix(k, "treedb.cache.flush_backlog_coalescing.") ||
			strings.HasPrefix(k, "treedb.cache.checkpoint.") ||
			strings.HasPrefix(k, "treedb.cache.auto_checkpoint.") ||
			strings.HasPrefix(k, "treedb.cache.command_wal.") ||
			strings.HasPrefix(k, "treedb.cache.leaf_log_lanes.") ||
			strings.HasPrefix(k, "treedb.cache.write.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_mmap.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_queue.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_shape.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_grouped_frame_cache.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_decode_buffer_grow.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_decode_scratch.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_writer_append_buf.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_write_mode.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_payload_split.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_auto.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_dict.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_generation.") ||
			k == "treedb.cache.vlog_retained_segments" ||
			k == "treedb.cache.vlog_retained_bytes_estimate" ||
			strings.HasPrefix(k, "treedb.cache.vlog_retained_prune.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_zombie.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_payload_kind.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_outer_leaf_codec.") ||
			strings.HasPrefix(k, "treedb.cache.append_only_direct_arena.") ||
			strings.HasPrefix(k, "treedb.cache.batch_arena.") {
			out[k] = coerceStatsValue(v)
		}
	}
	return out
}

func isProcessWideExpvarKey(k string) bool {
	processPrefixes := [...]string{
		"treedb.process.memory.",
		"treedb.process.batch.",
		"treedb.process.batch_arena.",
		"treedb.process.entry_slice.",
		"treedb.process.memtable_residency.",
		"treedb.process.read_path.",
		"treedb.process.flush_merge.",
		"treedb.process.append_only.",
		"treedb.process.append_only_direct_arena.",
		"treedb.process.batch_pool.",
		"treedb.process.vlog_mmap.",
	}
	for _, prefix := range processPrefixes {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

func coerceStatsValue(v string) any {
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i
	}
	if u, err := strconv.ParseUint(v, 10, 64); err == nil {
		return u
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	return v
}
