package caching

import (
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	debugVlogShapeEnabled atomic.Bool
	debugVlogShapeEveryNs atomic.Int64
	debugVlogShapeBudget  atomic.Int64
)

func init() {
	if os.Getenv("TREEDB_DEBUG_VLOG_SHAPE") == "" {
		return
	}
	debugVlogShapeEnabled.Store(true)

	everyMs := int64(30000)
	if s := os.Getenv("TREEDB_DEBUG_VLOG_SHAPE_INTERVAL_MS"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
			everyMs = v
		}
	}
	debugVlogShapeEveryNs.Store(int64(time.Duration(everyMs) * time.Millisecond))

	budget := int64(200)
	if s := os.Getenv("TREEDB_DEBUG_VLOG_SHAPE_BUDGET"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v >= 0 {
			budget = v
		}
	}
	debugVlogShapeBudget.Store(budget)
}

func debugVlogShapeOn() bool {
	return debugVlogShapeEnabled.Load() && debugVlogShapeBudget.Load() > 0
}

func (db *DB) startVlogShapeLoop() {
	if db == nil || !debugVlogShapeOn() {
		return
	}
	db.wg.Add(1)
	go db.vlogShapeLoop()
}

func (db *DB) vlogShapeLoop() {
	defer db.wg.Done()

	interval := time.Duration(debugVlogShapeEveryNs.Load())
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			db.emitVlogShapeLog()
		}
	}
}

func (db *DB) emitVlogShapeLog() {
	if db == nil || !debugVlogShapeOn() {
		return
	}
	if debugVlogShapeBudget.Add(-1) < 0 {
		return
	}
	if !db.valueLogEnabled() || !db.splitValueLogEnabled() {
		fmt.Fprintf(os.Stderr, "treedb debug vlog_shape splitValueLog=%t valueLog=%t\n", db.splitValueLogEnabled(), db.valueLogEnabled())
		return
	}

	var (
		totalSegs  int64
		totalBytes int64
		l0Segs     int64
		l0Bytes    int64
		l0Rot      uint64
		l0RotIdle  uint64
	)

	for i := range db.lanes {
		l := &db.lanes[i]
		segs, bytes := snapshotVlogLaneShape(l)
		totalSegs += int64(segs)
		totalBytes += bytes
		if l.id == 0 {
			l0Segs = int64(segs)
			l0Bytes = bytes
			l0Rot = l.vlogRotateTotal.Load()
			l0RotIdle = l.vlogRotateIdleTotal.Load()
		}
	}

	fmt.Fprintf(os.Stderr,
		"treedb debug vlog_shape l0_segments=%d l0_bytes=%d l0_rotations=%d l0_rotations_idle=%d segments_total=%d bytes_total=%d\n",
		l0Segs,
		l0Bytes,
		l0Rot,
		l0RotIdle,
		totalSegs,
		totalBytes,
	)
}

func snapshotVlogLaneShape(l *lane) (segments int, bytes int64) {
	if l == nil {
		return 0, 0
	}
	closedBytes := l.vlogClosedBytes.Load()
	liveBytes := l.vlogLiveBytes.Load()
	closedSegs := 0
	hasCurrent := false

	l.vlogMu.Lock()
	if l.vlogClosedSizes != nil {
		closedSegs = len(l.vlogClosedSizes)
	}
	hasCurrent = l.vlogPath != ""
	l.vlogMu.Unlock()

	segments = closedSegs
	if hasCurrent {
		segments++
	}
	bytes = closedBytes + liveBytes
	if bytes < 0 {
		bytes = 0
	}
	return segments, bytes
}
