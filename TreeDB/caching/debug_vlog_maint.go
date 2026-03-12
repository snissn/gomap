package caching

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	debugVlogMaintEnabled atomic.Bool
	debugVlogMaintBudget  atomic.Int64
)

func init() {
	if os.Getenv("TREEDB_DEBUG_VLOG_MAINT") == "" {
		return
	}
	debugVlogMaintEnabled.Store(true)

	budget := int64(5000)
	if s := os.Getenv("TREEDB_DEBUG_VLOG_MAINT_BUDGET"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v >= 0 {
			budget = v
		}
	}
	debugVlogMaintBudget.Store(budget)
}

func debugVlogMaintOn() bool {
	return debugVlogMaintEnabled.Load() && debugVlogMaintBudget.Load() > 0
}

func (db *DB) debugVlogMaintf(format string, args ...any) {
	if !debugVlogMaintOn() {
		return
	}
	if debugVlogMaintBudget.Add(-1) < 0 {
		return
	}
	walDir := ""
	if db != nil {
		walDir = db.dir
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	// Prefix with timestamp and a DB identity tag since multiple TreeDB instances
	// can interleave in a single process log.
	if walDir != "" {
		a := make([]any, 0, 3+len(args))
		a = append(a, ts, filepath.Base(filepath.Dir(walDir)), walDir)
		a = append(a, args...)
		fmt.Fprintf(os.Stderr, "ts=%s treedb debug vlog_maint db=%s wal_dir=%s "+format+"\n", a...)
		return
	}
	fmt.Fprintf(os.Stderr, "ts=%s treedb debug vlog_maint "+format+"\n", append([]any{ts}, args...)...)
}
