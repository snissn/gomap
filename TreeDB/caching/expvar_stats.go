package caching

import (
	"expvar"
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
	treedbExpvarCurrentDB.Store(db)
}

func unregisterTreeDBExpvarStatsDB(db *DB) {
	if db == nil || !treedbExpvarEnabled {
		return
	}
	treedbExpvarCurrentDB.CompareAndSwap(db, nil)
}

func currentTreeDBExpvarStats() map[string]any {
	db := treedbExpvarCurrentDB.Load()
	if db == nil {
		return map[string]any{}
	}
	return selectTreeDBExpvarStats(db.Stats())
}

func selectTreeDBExpvarStats(stats map[string]string) map[string]any {
	if len(stats) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any)
	for k, v := range stats {
		// Export only process-wide metric families under treedb.process.* and
		// select cache families used for mmap/decode/batch-arena tracking.
		if isProcessWideExpvarKey(k) ||
			strings.HasPrefix(k, "treedb.cache.vlog_mmap.") ||
			strings.HasPrefix(k, "treedb.cache.vlog_decode_buffer_grow.") ||
			strings.HasPrefix(k, "treedb.cache.batch_arena.") {
			out[k] = coerceStatsValue(v)
		}
	}
	return out
}

func isProcessWideExpvarKey(k string) bool {
	processPrefixes := [...]string{
		"treedb.process.memory.",
		"treedb.process.batch_arena.",
		"treedb.process.entry_slice.",
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
