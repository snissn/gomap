package caching

import (
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
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
	fmt.Fprintf(os.Stderr, "treedb debug vlog_maint "+format+"\n", args...)
}
