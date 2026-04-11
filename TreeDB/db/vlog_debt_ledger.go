package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
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

type valueLogDebtRecordKey struct {
	FileID uint32
	Start  uint64
}

type valueLogDebtRecordRef struct {
	Key       valueLogDebtRecordKey
	RecordLen uint32
	RefCount  uint32
}

type valueLogDebtDeltaChange struct {
	RecordLen uint32
	RefDelta  int32
}

type valueLogDebtDelta struct {
	changes map[valueLogDebtRecordKey]valueLogDebtDeltaChange
}

type valueLogOuterLeafChangeCollector struct {
	oldPtrs []page.ValuePtr
	newPtrs []page.ValuePtr
}

type valueLogDebtLedger struct {
	mu        sync.RWMutex
	commitSeq uint64
	segments  map[uint32]valueLogDebtSegmentSummary
	records   map[valueLogDebtRecordKey]valueLogDebtRecordRef
	valid     bool
	dirty     bool
}

func newValueLogDebtLedger() *valueLogDebtLedger {
	return &valueLogDebtLedger{
		segments: make(map[uint32]valueLogDebtSegmentSummary),
		records:  make(map[valueLogDebtRecordKey]valueLogDebtRecordRef),
	}
}

func newValueLogDebtDelta() *valueLogDebtDelta {
	return &valueLogDebtDelta{changes: make(map[valueLogDebtRecordKey]valueLogDebtDeltaChange)}
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

func valueLogDebtRecordKeyForPtr(ptr page.ValuePtr) (valueLogDebtRecordKey, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return valueLogDebtRecordKey{}, fmt.Errorf("treedb: non-vlog pointer for debt key: file=%d", ptr.FileID)
	}
	if ptr.Offset < 4 {
		return valueLogDebtRecordKey{}, fmt.Errorf("treedb: invalid value-log pointer offset %d", ptr.Offset)
	}
	return valueLogDebtRecordKey{FileID: ptr.FileID, Start: uint64(ptr.Offset - 4)}, nil
}

func (d *valueLogDebtDelta) addRecord(key valueLogDebtRecordKey, recordLen uint32, refDelta int32) {
	if d == nil || refDelta == 0 || key.FileID == 0 {
		return
	}
	if d.changes == nil {
		d.changes = make(map[valueLogDebtRecordKey]valueLogDebtDeltaChange)
	}
	change := d.changes[key]
	if change.RecordLen == 0 {
		change.RecordLen = recordLen
	}
	change.RefDelta += refDelta
	if change.RefDelta == 0 {
		delete(d.changes, key)
		return
	}
	d.changes[key] = change
}

func (d *valueLogDebtDelta) addPtr(ptr page.ValuePtr, recordLen uint32, refDelta int32) error {
	if d == nil || refDelta == 0 || !page.IsValueLogFileID(ptr.FileID) {
		return nil
	}
	key, err := valueLogDebtRecordKeyForPtr(ptr)
	if err != nil {
		return err
	}
	d.addRecord(key, recordLen, refDelta)
	return nil
}

func (d *valueLogDebtDelta) fileIDs() []uint32 {
	if d == nil || len(d.changes) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(d.changes))
	ids := make([]uint32, 0, len(d.changes))
	for key := range d.changes {
		if _, ok := seen[key.FileID]; ok {
			continue
		}
		seen[key.FileID] = struct{}{}
		ids = append(ids, key.FileID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (c *valueLogOuterLeafChangeCollector) Observe(oldPtrs, newPtrs []page.ValuePtr) {
	if c == nil {
		return
	}
	if len(oldPtrs) > 0 {
		c.oldPtrs = append(c.oldPtrs, oldPtrs...)
	}
	if len(newPtrs) > 0 {
		c.newPtrs = append(c.newPtrs, newPtrs...)
	}
}

func (c *valueLogOuterLeafChangeCollector) appendToDebtDelta(db *DB, delta *valueLogDebtDelta) error {
	if c == nil || delta == nil {
		return nil
	}
	for _, ptr := range c.oldPtrs {
		recordLen, err := db.valueLogRecordLengthForRewrite(ptr)
		if err != nil {
			return err
		}
		if err := delta.addPtr(ptr, recordLen, -1); err != nil {
			return err
		}
	}
	for _, ptr := range c.newPtrs {
		recordLen, err := db.valueLogRecordLengthForRewrite(ptr)
		if err != nil {
			return err
		}
		if err := delta.addPtr(ptr, recordLen, 1); err != nil {
			return err
		}
	}
	return nil
}

func (l *valueLogDebtLedger) replace(commitSeq uint64, segments map[uint32]valueLogDebtSegmentSummary, records map[valueLogDebtRecordKey]valueLogDebtRecordRef, dirty bool) {
	if l == nil {
		return
	}
	nextSegments := make(map[uint32]valueLogDebtSegmentSummary, len(segments))
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
		nextSegments[seg.FileID] = seg
	}
	nextRecords := make(map[valueLogDebtRecordKey]valueLogDebtRecordRef, len(records))
	for key, rec := range records {
		if rec.Key.FileID == 0 {
			rec.Key = key
		}
		if rec.Key.FileID == 0 || rec.RefCount == 0 || rec.RecordLen == 0 {
			continue
		}
		nextRecords[rec.Key] = rec
	}
	l.mu.Lock()
	l.commitSeq = commitSeq
	l.segments = nextSegments
	l.records = nextRecords
	l.valid = true
	l.dirty = dirty
	l.mu.Unlock()
}

func (l *valueLogDebtLedger) canTrack(baseSeq uint64) bool {
	if l == nil {
		return false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.valid && l.commitSeq == baseSeq && len(l.records) > 0
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
		if seg.LiveBytes == 0 {
			continue
		}
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

func (l *valueLogDebtLedger) applyDelta(nextCommitSeq uint64, delta *valueLogDebtDelta, totals map[uint32]uint64) error {
	if l == nil {
		return nil
	}
	if delta == nil {
		return errors.New("treedb: missing value-log debt delta")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.valid {
		return errors.New("treedb: value-log debt ledger invalid")
	}
	if nextCommitSeq != l.commitSeq+1 {
		return fmt.Errorf("treedb: value-log debt ledger sequence mismatch: have=%d next=%d", l.commitSeq, nextCommitSeq)
	}
	if len(l.records) == 0 {
		return errors.New("treedb: value-log debt ledger missing record refs")
	}

	touched := make(map[uint32]struct{}, len(delta.changes)+len(totals))
	for key, change := range delta.changes {
		if change.RefDelta == 0 {
			continue
		}
		rec := l.records[key]
		curRefCount := int64(rec.RefCount)
		nextRefCount := curRefCount + int64(change.RefDelta)
		if nextRefCount < 0 {
			return fmt.Errorf("treedb: value-log debt ledger refcount underflow: file=%d start=%d have=%d delta=%d", key.FileID, key.Start, rec.RefCount, change.RefDelta)
		}
		recordLen := rec.RecordLen
		if recordLen == 0 {
			recordLen = change.RecordLen
		}
		if nextRefCount > 0 && recordLen == 0 {
			return fmt.Errorf("treedb: missing record length for value-log debt delta: file=%d start=%d", key.FileID, key.Start)
		}
		seg := l.segments[key.FileID]
		seg.FileID = key.FileID
		switch {
		case curRefCount == 0 && nextRefCount > 0:
			seg.LiveBytes += uint64(recordLen)
		case curRefCount > 0 && nextRefCount == 0:
			if uint64(recordLen) > seg.LiveBytes {
				return fmt.Errorf("treedb: value-log debt live-byte underflow: file=%d have=%d dec=%d", key.FileID, seg.LiveBytes, recordLen)
			}
			seg.LiveBytes -= uint64(recordLen)
		}
		seg.LastUpdatedCommitSeq = nextCommitSeq
		l.segments[key.FileID] = seg
		touched[key.FileID] = struct{}{}

		if nextRefCount == 0 {
			delete(l.records, key)
			continue
		}
		rec.Key = key
		rec.RecordLen = recordLen
		rec.RefCount = uint32(nextRefCount)
		l.records[key] = rec
	}

	for fileID, total := range totals {
		seg := l.segments[fileID]
		seg.FileID = fileID
		seg.TotalBytes = total
		if seg.LiveBytes > seg.TotalBytes {
			seg.LiveBytes = seg.TotalBytes
		}
		seg.StaleBytes = seg.TotalBytes - seg.LiveBytes
		seg.LastUpdatedCommitSeq = nextCommitSeq
		l.segments[fileID] = seg
		touched[fileID] = struct{}{}
	}
	for fileID := range touched {
		seg := l.segments[fileID]
		if seg.LiveBytes > seg.TotalBytes {
			seg.LiveBytes = seg.TotalBytes
		}
		if seg.TotalBytes >= seg.LiveBytes {
			seg.StaleBytes = seg.TotalBytes - seg.LiveBytes
		} else {
			seg.StaleBytes = 0
		}
		seg.LastUpdatedCommitSeq = nextCommitSeq
		l.segments[fileID] = seg
	}

	l.commitSeq = nextCommitSeq
	l.dirty = true
	return nil
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
	db.valueLogDebtLedger.replace(disk.CommitSeq, segments, nil, false)
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
	// Segment-only snapshots remain useful for planning, but commit-path deltas
	// require a rebuilt physical-record catalog before the ledger becomes
	// authoritative again.
	db.valueLogDebtLedger.replace(db.currentCommitSeq(), segments, nil, true)
	return db.persistValueLogDebtLedger()
}

func observeValueLogDebtRecord(db *DB, ptr page.ValuePtr, records map[valueLogDebtRecordKey]valueLogDebtRecordRef, set *valuelog.Set) error {
	if db == nil || records == nil || !page.IsValueLogFileID(ptr.FileID) {
		return nil
	}
	key, err := valueLogDebtRecordKeyForPtr(ptr)
	if err != nil {
		return err
	}
	recordLen, err := db.valueLogRecordLengthForRewriteInSet(ptr, set)
	if err != nil {
		return err
	}
	rec := records[key]
	rec.Key = key
	rec.RecordLen = recordLen
	rec.RefCount++
	records[key] = rec
	return nil
}

func (db *DB) collectValueLogRecordRefs(ctx context.Context, it iterator.UnsafeIterator, records map[valueLogDebtRecordKey]valueLogDebtRecordRef, set *valuelog.Set) error {
	for it.Valid() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
			it.Next()
			continue
		}
		if err := observeValueLogDebtRecord(db, ptr, records, set); err != nil {
			return err
		}
		it.Next()
	}
	return it.Error()
}

func (db *DB) collectLeafRefValueLogRecordRefs(ctx context.Context, p *pager.Pager, rootID uint64, records map[valueLogDebtRecordKey]valueLogDebtRecordRef, set *valuelog.Set) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || rootID == 0 || records == nil {
		return nil
	}
	if ptr, ok := page.DecodeLeafRef(rootID); ok {
		return observeValueLogDebtRecord(db, ptr, records, set)
	}
	stack := make([]uint64, 0, 128)
	stack = append(stack, rootID)
	visited := make(map[uint64]struct{}, 1024)
	verifyAlways := p.VerifyOnRead()

	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		pageID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := visited[pageID]; ok {
			continue
		}
		visited[pageID] = struct{}{}

		data, err := p.Get(pageID)
		if err != nil {
			return err
		}
		n := node.NewNodeView(data)
		if verifyAlways || !p.IsVerified(pageID) {
			if !n.VerifyChecksum() {
				return fmt.Errorf("checksum mismatch on page %d", pageID)
			}
			if !verifyAlways {
				p.MarkVerified(pageID)
			}
		}

		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				childID, err := n.GetInternalChildID(i)
				if err != nil {
					return err
				}
				if ptr, ok := page.DecodeLeafRef(childID); ok {
					if err := observeValueLogDebtRecord(db, ptr, records, set); err != nil {
						return err
					}
					continue
				}
				stack = append(stack, childID)
			}
		case page.PageTypeLeaf:
			// Pager-backed leaves are possible during transitional states. They do
			// not contribute value-log debt directly.
		default:
			return fmt.Errorf("invalid page type %d on page %d", n.Type(), pageID)
		}
	}
	return nil
}

func buildValueLogDebtSegmentsFromRecords(set *valuelog.Set, records map[valueLogDebtRecordKey]valueLogDebtRecordRef, commitSeq uint64) map[uint32]valueLogDebtSegmentSummary {
	segments := make(map[uint32]valueLogDebtSegmentSummary)
	if set != nil {
		for fileID, f := range set.Files {
			total := uint64(maxInt64Zero(fileSize(f)))
			segments[fileID] = valueLogDebtSegmentSummary{
				FileID:               fileID,
				TotalBytes:           total,
				LastUpdatedCommitSeq: commitSeq,
			}
		}
	}
	for _, rec := range records {
		if rec.RefCount == 0 || rec.RecordLen == 0 {
			continue
		}
		seg := segments[rec.Key.FileID]
		seg.FileID = rec.Key.FileID
		seg.LiveBytes += uint64(rec.RecordLen)
		seg.LastUpdatedCommitSeq = commitSeq
		segments[rec.Key.FileID] = seg
	}
	for fileID, seg := range segments {
		if seg.LiveBytes > seg.TotalBytes {
			seg.LiveBytes = seg.TotalBytes
		}
		if seg.TotalBytes >= seg.LiveBytes {
			seg.StaleBytes = seg.TotalBytes - seg.LiveBytes
		} else {
			seg.StaleBytes = 0
		}
		segments[fileID] = seg
	}
	return segments
}

func (db *DB) scanValueLogDebtLedger(ctx context.Context) (map[uint32]valueLogDebtSegmentSummary, map[valueLogDebtRecordKey]valueLogDebtRecordRef, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil || snap.idx == nil {
		closeRewriteSnapshot(nil, snap)
		return nil, nil, 0, fmt.Errorf("missing snapshot state")
	}
	var closeErr error
	defer closeRewriteSnapshot(&closeErr, snap)

	records := make(map[valueLogDebtRecordKey]valueLogDebtRecordRef, 1024)

	userIter := snap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := db.collectValueLogRecordRefs(ctx, userIter, records, snap.state.ValueLogSet); err != nil {
		_ = userIter.Close()
		return nil, nil, 0, err
	}
	_ = userIter.Close()

	sysIter := tree.New(snap.idx.pager, newValueReader(snap.state.ValueLogSet), snap.state.SystemRootPageID).
		IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	if err := db.collectValueLogRecordRefs(ctx, sysIter, records, snap.state.ValueLogSet); err != nil {
		_ = sysIter.Close()
		return nil, nil, 0, err
	}
	_ = sysIter.Close()

	if err := db.collectLeafRefValueLogRecordRefs(ctx, snap.idx.pager, snap.state.RootPageID, records, snap.state.ValueLogSet); err != nil {
		return nil, nil, 0, err
	}
	if err := db.collectLeafRefValueLogRecordRefs(ctx, snap.idx.pager, snap.state.SystemRootPageID, records, snap.state.ValueLogSet); err != nil {
		return nil, nil, 0, err
	}

	segments := buildValueLogDebtSegmentsFromRecords(snap.state.ValueLogSet, records, snap.state.CommitSeq)
	return segments, records, snap.state.CommitSeq, nil
}

func (db *DB) rebuildValueLogDebtLedger(ctx context.Context) error {
	if db == nil || db.valueLogDebtLedger == nil || !valueLogDebtLedgerEnabled() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	scan := func() error {
		segments, records, commitSeq, err := db.scanValueLogDebtLedger(ctx)
		if err != nil {
			return err
		}
		db.valueLogDebtLedger.replace(commitSeq, segments, records, true)
		return db.persistValueLogDebtLedger()
	}
	if err := scan(); err != nil {
		if errors.Is(err, valuelog.ErrFileNotFound) {
			if refreshErr := db.RefreshValueLogSet(); refreshErr != nil {
				return refreshErr
			}
			return scan()
		}
		return err
	}
	return nil
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
	commitSeq := db.currentCommitSeq()
	if liveByID, ok := db.valueLogDebtLedger.liveBytesBySegment(commitSeq); ok {
		if db.valueLogDebtLedger.canTrack(commitSeq) {
			return liveByID, true, nil
		}
		// Persisted segment summaries remain useful for planning, but rebuild once
		// so subsequent commits can stay authoritative in-process.
		if err := db.rebuildValueLogDebtLedger(ctx); err == nil {
			if rebuilt, rebuiltOK := db.valueLogDebtLedger.liveBytesBySegment(commitSeq); rebuiltOK {
				return rebuilt, true, nil
			}
		} else {
			db.reportError(err)
		}
		return liveByID, true, nil
	}
	if err := db.rebuildValueLogDebtLedger(ctx); err != nil {
		return nil, false, err
	}
	liveByID, ok := db.valueLogDebtLedger.liveBytesBySegment(commitSeq)
	return liveByID, ok, nil
}

func lookupValueLogPtrAtKey(tr *tree.Tree, key []byte) (page.ValuePtr, bool, error) {
	if tr == nil {
		return page.ValuePtr{}, false, nil
	}
	entry, err := tr.GetEntry(key)
	if err != nil {
		if errors.Is(err, tree.ErrKeyNotFound) {
			return page.ValuePtr{}, false, nil
		}
		return page.ValuePtr{}, false, err
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		return page.ValuePtr{}, false, nil
	}
	return entry.ValuePtr, true, nil
}

func (db *DB) buildValueLogDebtDelta(p *pager.Pager, rootID uint64, baseSeq uint64, entries []batchpkg.Entry, outerLeafChanges *valueLogOuterLeafChangeCollector) (*valueLogDebtDelta, error) {
	if db == nil || db.valueLogDebtLedger == nil || !valueLogDebtLedgerEnabled() || !db.valueLogDebtLedger.canTrack(baseSeq) {
		return nil, nil
	}
	delta := newValueLogDebtDelta()
	if p != nil {
		var tr *tree.Tree
		if state := db.state.Load(); state != nil {
			reader := newValueReader(state.ValueLogSet)
			tr = tree.New(p, &reader, rootID)
		} else {
			tr = tree.New(p, nil, rootID)
		}
		for i := range entries {
			oldPtr, oldRef, err := lookupValueLogPtrAtKey(tr, entries[i].Key)
			if err != nil {
				return nil, err
			}
			if oldRef {
				recordLen, err := db.valueLogRecordLengthForRewrite(oldPtr)
				if err != nil {
					return nil, err
				}
				if err := delta.addPtr(oldPtr, recordLen, -1); err != nil {
					return nil, err
				}
			}
			if entries[i].Type == batchpkg.OpPut && entries[i].IsPtr && page.IsValueLogFileID(entries[i].ValuePtr.FileID) {
				recordLen, err := db.valueLogRecordLengthForRewrite(entries[i].ValuePtr)
				if err != nil {
					return nil, err
				}
				if err := delta.addPtr(entries[i].ValuePtr, recordLen, 1); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := outerLeafChanges.appendToDebtDelta(db, delta); err != nil {
		return nil, err
	}
	return delta, nil
}

func (db *DB) currentValueLogSegmentTotals(fileIDs []uint32) (map[uint32]uint64, error) {
	if db == nil || db.valueLogManager == nil || len(fileIDs) == 0 {
		return nil, nil
	}
	currentSet := db.valueLogManager.CurrentSetNoRefresh()
	if currentSet == nil {
		if err := db.valueLogManager.Refresh(); err != nil {
			return nil, err
		}
		currentSet = db.valueLogManager.CurrentSetNoRefresh()
	}
	if currentSet == nil {
		return nil, fmt.Errorf("treedb: value-log set unavailable")
	}
	defer func() { _ = db.valueLogManager.Release(currentSet) }()

	totals := make(map[uint32]uint64, len(fileIDs))
	for _, fileID := range fileIDs {
		if file, ok := currentSet.Files[fileID]; ok && file != nil {
			size := int64(-1)
			switch {
			case file.File != nil:
				if info, err := file.File.Stat(); err == nil {
					size = info.Size()
				}
			case file.Path != "":
				if info, err := os.Stat(file.Path); err == nil {
					size = info.Size()
				}
			}
			if size < 0 {
				size = fileSize(file)
			}
			totals[fileID] = uint64(maxInt64Zero(size))
		}
	}
	return totals, nil
}
