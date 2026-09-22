package caching

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	debugVlogShapeEnabled atomic.Bool
	debugVlogShapeEveryNs atomic.Int64
	debugVlogShapeBudget  atomic.Int64
	debugVlogShapeDisk    atomic.Bool
)

func init() {
	if os.Getenv("TREEDB_DEBUG_VLOG_SHAPE") == "" {
		return
	}
	debugVlogShapeEnabled.Store(true)

	if os.Getenv("TREEDB_DEBUG_VLOG_SHAPE_DISK") != "" {
		debugVlogShapeDisk.Store(true)
	}

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
		ts := time.Now().UTC().Format(time.RFC3339Nano)
		fmt.Fprintf(os.Stderr, "ts=%s treedb debug vlog_shape db=%s wal_dir=%s value_vlog_dir=%s splitValueLog=%t valueLog=%t\n",
			ts,
			filepath.Base(filepath.Dir(db.dir)),
			db.dir,
			db.valueLogDir,
			db.splitValueLogEnabled(),
			db.valueLogEnabled(),
		)
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

	var (
		diskL0Segs    int64
		diskL0Bytes   int64
		diskL255Segs  int64
		diskL255Bytes int64
		diskSegsTotal int64
		diskBytes     int64
		diskErr       error
	)
	if debugVlogShapeDisk.Load() && db.valueLogDir != "" {
		diskL0Segs, diskL0Bytes, diskL255Segs, diskL255Bytes, diskSegsTotal, diskBytes, diskErr = scanVlogSegmentsOnDisk(db.valueLogDir)
	}

	ts := time.Now().UTC().Format(time.RFC3339Nano)
	fmt.Fprintf(os.Stderr,
		"ts=%s treedb debug vlog_shape db=%s wal_dir=%s value_vlog_dir=%s l0_segments=%d l0_bytes=%d l0_rotations=%d l0_rotations_idle=%d segments_total=%d bytes_total=%d disk_l0_segments=%d disk_l0_bytes=%d disk_l255_segments=%d disk_l255_bytes=%d disk_segments_total=%d disk_bytes_total=%d disk_err=%v\n",
		ts,
		filepath.Base(filepath.Dir(db.dir)),
		db.dir,
		db.valueLogDir,
		l0Segs,
		l0Bytes,
		l0Rot,
		l0RotIdle,
		totalSegs,
		totalBytes,
		diskL0Segs,
		diskL0Bytes,
		diskL255Segs,
		diskL255Bytes,
		diskSegsTotal,
		diskBytes,
		diskErr,
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

var vlogSegmentNameRE = regexp.MustCompile(`^value-l([0-9]+)-[0-9]+\.log$`)

func scanVlogSegmentsOnDisk(walDir string) (l0Segs int64, l0Bytes int64, l255Segs int64, l255Bytes int64, totalSegs int64, totalBytes int64, err error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		m := vlogSegmentNameRE.FindStringSubmatch(ent.Name())
		if len(m) != 2 {
			continue
		}
		lane, parseErr := strconv.ParseInt(m[1], 10, 64)
		if parseErr != nil || lane < 0 {
			continue
		}
		info, statErr := ent.Info()
		if statErr != nil {
			err = statErr
			continue
		}
		sz := info.Size()
		if sz < 0 {
			sz = 0
		}
		totalSegs++
		totalBytes += sz
		switch lane {
		case 0:
			l0Segs++
			l0Bytes += sz
		case 255:
			l255Segs++
			l255Bytes += sz
		}
	}
	return l0Segs, l0Bytes, l255Segs, l255Bytes, totalSegs, totalBytes, err
}
