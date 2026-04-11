package db

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
)

const (
	valueLogDebtLedgerFileName = "vlog_debt_ledger.meta"
	valueLogDebtLedgerVersion  = 1
	envEnableVlogDebtLedger    = "TREEDB_VLOG_DEBT_LEDGER"
	envEnableVlogShadowCompare = "TREEDB_VLOG_SHADOW_COMPARE"
)

type valueLogDebtSegmentSummary struct {
	FileID               uint32 `json:"file_id"`
	TotalBytes           uint64 `json:"total_bytes"`
	LiveBytes            uint64 `json:"live_bytes"`
	StaleBytes           uint64 `json:"stale_bytes"`
	LastUpdatedCommitSeq uint64 `json:"last_updated_commit_seq"`
}

type valueLogDebtLedgerDisk struct {
	Version   uint32                       `json:"version"`
	CommitSeq uint64                       `json:"commit_seq"`
	Segments  []valueLogDebtSegmentSummary `json:"segments,omitempty"`
}

type valueLogDebtLedger struct {
	mu        sync.RWMutex
	commitSeq uint64
	segments  map[uint32]valueLogDebtSegmentSummary
	valid     bool
	dirty     bool
}

func newValueLogDebtLedger() *valueLogDebtLedger {
	return &valueLogDebtLedger{segments: make(map[uint32]valueLogDebtSegmentSummary)}
}

func envDebtLedgerBool(name string) bool {
	v, ok := os.LookupEnv(name)
	if !ok {
		return false
	}
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return true
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n != 0
	}
	return false
}

func valueLogDebtLedgerEnabled() bool {
	return envDebtLedgerBool(envEnableVlogDebtLedger)
}

func valueLogDebtLedgerShadowCompareEnabled() bool {
	return envDebtLedgerBool(envEnableVlogShadowCompare)
}

func (l *valueLogDebtLedger) replace(commitSeq uint64, segments map[uint32]valueLogDebtSegmentSummary, dirty bool) {
	if l == nil {
		return
	}
	next := make(map[uint32]valueLogDebtSegmentSummary, len(segments))
	for fileID, seg := range segments {
		if seg.FileID == 0 {
			seg.FileID = fileID
		}
		if seg.LiveBytes > seg.TotalBytes {
			seg.LiveBytes = seg.TotalBytes
		}
		if seg.StaleBytes > seg.TotalBytes {
			seg.StaleBytes = seg.TotalBytes
		}
		next[seg.FileID] = seg
	}
	l.mu.Lock()
	l.commitSeq = commitSeq
	l.segments = next
	l.valid = true
	l.dirty = dirty
	l.mu.Unlock()
}

func (l *valueLogDebtLedger) invalidate() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.valid = false
	l.mu.Unlock()
}

func (l *valueLogDebtLedger) liveBytesBySegment(commitSeq uint64) (map[uint32]int64, bool) {
	if l == nil {
		return nil, false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.valid || l.commitSeq != commitSeq {
		return nil, false
	}
	out := make(map[uint32]int64, len(l.segments))
	for fileID, seg := range l.segments {
		out[fileID] = int64(seg.LiveBytes)
	}
	return out, true
}

func (l *valueLogDebtLedger) dirtySnapshot() (uint64, []valueLogDebtSegmentSummary, bool) {
	if l == nil {
		return 0, nil, false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.valid || !l.dirty {
		return 0, nil, false
	}
	segments := make([]valueLogDebtSegmentSummary, 0, len(l.segments))
	for _, seg := range l.segments {
		segments = append(segments, seg)
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].FileID < segments[j].FileID })
	return l.commitSeq, segments, true
}

func (l *valueLogDebtLedger) markClean(commitSeq uint64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.valid && l.commitSeq == commitSeq {
		l.dirty = false
	}
	l.mu.Unlock()
}

func (db *DB) valueLogDebtLedgerPath() string {
	if db == nil || db.dir == "" {
		return ""
	}
	return filepath.Join(db.dir, valueLogDebtLedgerFileName)
}

func (db *DB) initValueLogDebtLedger() error {
	if db == nil || db.valueLogDebtLedger == nil || !valueLogDebtLedgerEnabled() {
		return nil
	}
	_, err := db.loadValueLogDebtLedger(db.currentCommitSeq())
	return err
}

func loadValueLogDebtLedgerFromPath(path string, commitSeq uint64) (valueLogDebtLedgerDisk, bool, error) {
	if path == "" {
		return valueLogDebtLedgerDisk{}, false, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return valueLogDebtLedgerDisk{}, false, nil
		}
		return valueLogDebtLedgerDisk{}, false, err
	}
	var disk valueLogDebtLedgerDisk
	if len(data) > 0 {
		if err := json.Unmarshal(data, &disk); err != nil {
			return valueLogDebtLedgerDisk{}, false, nil
		}
	}
	if disk.Version != valueLogDebtLedgerVersion || disk.CommitSeq != commitSeq {
		return valueLogDebtLedgerDisk{}, false, nil
	}
	return disk, true, nil
}

func (db *DB) loadValueLogDebtLedger(commitSeq uint64) (bool, error) {
	if db == nil || db.valueLogDebtLedger == nil || !valueLogDebtLedgerEnabled() {
		return false, nil
	}
	path := db.valueLogDebtLedgerPath()
	disk, ok, err := loadValueLogDebtLedgerFromPath(path, commitSeq)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	segments := make(map[uint32]valueLogDebtSegmentSummary, len(disk.Segments))
	for _, seg := range disk.Segments {
		segments[seg.FileID] = seg
	}
	db.valueLogDebtLedger.replace(disk.CommitSeq, segments, false)
	return true, nil
}

func (db *DB) persistValueLogDebtLedger() error {
	if db == nil || db.valueLogDebtLedger == nil || !valueLogDebtLedgerEnabled() {
		return nil
	}
	path := db.valueLogDebtLedgerPath()
	if path == "" {
		return nil
	}
	commitSeq, segments, ok := db.valueLogDebtLedger.dirtySnapshot()
	if !ok {
		return nil
	}
	blob, err := json.Marshal(valueLogDebtLedgerDisk{
		Version:   valueLogDebtLedgerVersion,
		CommitSeq: commitSeq,
		Segments:  segments,
	})
	if err != nil {
		return err
	}
	if err := atomicfile.Write(path, blob, 0o600); err != nil {
		return err
	}
	db.valueLogDebtLedger.markClean(commitSeq)
	return nil
}

func (db *DB) persistValueLogDebtLedgerBestEffort() {
	if err := db.persistValueLogDebtLedger(); err != nil {
		db.reportError(err)
	}
}

func (db *DB) storeValueLogDebtLedgerFromLiveBytes(liveByID map[uint32]int64) error {
	if db == nil || db.valueLogDebtLedger == nil || !valueLogDebtLedgerEnabled() {
		return nil
	}
	if err := db.valueLogManager.Refresh(); err != nil {
		return err
	}
	set := db.valueLogManager.CurrentSet()
	if set != nil {
		defer func() { _ = db.valueLogManager.Release(set) }()
	}
	segments := make(map[uint32]valueLogDebtSegmentSummary)
	if set != nil {
		for fileID, f := range set.Files {
			total := uint64(maxInt64Zero(fileSize(f)))
			live := uint64(maxInt64Zero(liveByID[fileID]))
			if live > total {
				live = total
			}
			segments[fileID] = valueLogDebtSegmentSummary{
				FileID:               fileID,
				TotalBytes:           total,
				LiveBytes:            live,
				StaleBytes:           total - live,
				LastUpdatedCommitSeq: db.currentCommitSeq(),
			}
		}
	}
	db.valueLogDebtLedger.replace(db.currentCommitSeq(), segments, true)
	return db.persistValueLogDebtLedger()
}

func (db *DB) rebuildValueLogDebtLedger(ctx context.Context) error {
	if db == nil || db.valueLogDebtLedger == nil || !valueLogDebtLedgerEnabled() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	liveByID, err := db.estimateValueLogLiveBytesBySegment(ctx)
	if err != nil {
		return err
	}
	return db.storeValueLogDebtLedgerFromLiveBytes(liveByID)
}

func maxInt64Zero(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

func (db *DB) rebuildValueLogDebtLedgerBestEffort() {
	if err := db.rebuildValueLogDebtLedger(context.Background()); err != nil {
		db.reportError(err)
	}
}

func sameValueLogLiveBytesBySegment(a, b map[uint32]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for fileID, live := range a {
		if b[fileID] != live {
			return false
		}
	}
	return true
}

func (db *DB) liveBytesBySegmentFromDebtLedger(ctx context.Context) (map[uint32]int64, bool, error) {
	if db == nil || db.valueLogDebtLedger == nil || !valueLogDebtLedgerEnabled() {
		return nil, false, nil
	}
	if liveByID, ok := db.valueLogDebtLedger.liveBytesBySegment(db.currentCommitSeq()); ok {
		return liveByID, true, nil
	}
	if err := db.rebuildValueLogDebtLedger(ctx); err != nil {
		return nil, false, err
	}
	liveByID, ok := db.valueLogDebtLedger.liveBytesBySegment(db.currentCommitSeq())
	return liveByID, ok, nil
}
