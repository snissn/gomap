package caching

import (
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	debugVlogTimingsEnabled atomic.Bool
	debugVlogTimingsMinDur  atomic.Int64 // nanoseconds
	debugVlogTimingsBudget  atomic.Int64 // remaining log lines allowed
)

func init() {
	if os.Getenv("TREEDB_DEBUG_VLOG_TIMINGS") == "" {
		return
	}
	debugVlogTimingsEnabled.Store(true)

	minMs := int64(5)
	if s := os.Getenv("TREEDB_DEBUG_VLOG_TIMINGS_MIN_MS"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v >= 0 {
			minMs = v
		}
	}
	debugVlogTimingsMinDur.Store(int64(time.Duration(minMs) * time.Millisecond))

	budget := int64(200)
	if s := os.Getenv("TREEDB_DEBUG_VLOG_TIMINGS_BUDGET"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v >= 0 {
			budget = v
		}
	}
	debugVlogTimingsBudget.Store(budget)
}

func debugVlogTimingsOn() bool {
	return debugVlogTimingsEnabled.Load() && debugVlogTimingsBudget.Load() > 0
}

func (db *DB) debugVlogEvent(label string, laneID int, lock string) {
	if !debugVlogTimingsOn() {
		return
	}
	if debugVlogTimingsBudget.Add(-1) < 0 {
		return
	}
	fmt.Fprintf(os.Stderr,
		"treedb debug vlog_event label=%s lane=%d lock=%s disableJournal=%t relaxedSync=%t splitValueLog=%t\n",
		label,
		laneID,
		lock,
		db.disableJournal,
		db.relaxedSync,
		db.splitValueLogEnabled(),
	)
}

func (db *DB) debugVlogTiming(label string, laneID int, lock string, waited, dur time.Duration) {
	if !debugVlogTimingsOn() {
		return
	}
	min := time.Duration(debugVlogTimingsMinDur.Load())
	if waited < min && dur < min {
		return
	}
	if debugVlogTimingsBudget.Add(-1) < 0 {
		return
	}
	fmt.Fprintf(os.Stderr,
		"treedb debug vlog_timing label=%s lane=%d lock=%s waited_ms=%.3f dur_ms=%.3f disableJournal=%t relaxedSync=%t splitValueLog=%t\n",
		label,
		laneID,
		lock,
		float64(waited.Microseconds())/1000,
		float64(dur.Microseconds())/1000,
		db.disableJournal,
		db.relaxedSync,
		db.splitValueLogEnabled(),
	)
}
