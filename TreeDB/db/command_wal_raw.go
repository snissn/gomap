package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

type commandWALBatchIntent struct {
	kind               commitlog.CommandKind
	scope              commitlog.CommandScope
	payloadFormat      commitlog.PayloadFormat
	payload            []byte
	rawKVEntries       []batchpkg.Entry
	rawKVScan          commitlog.RawKVBatchOperationScanner
	rawKVPlan          commitlog.RawKVBatchPayloadPlan
	rawKVRIDCache      *rawKVCommandWALRIDCache
	rawKVDirect        bool
	trustedPayload     bool
	externalRefs       bool
	externalRefFileIDs []uint32
	fromReplay         bool
	lsn                uint64
	maxEntryRevision   page.EntryRevision
	replayToken        uint64
	coveredRange       [1]CommandWALLSNRange
	syncOnPublish      bool
	staged             bool
}

const rawKVCommandWALRIDInlineCacheEntries = 4
const rawKVCommandWALRIDMaxPooledOverflowEntries = 4 * 1024

type rawKVCommandWALRIDCacheEntry struct {
	ptr page.ValuePtr
	rid uint64
}

type rawKVCommandWALRIDCache struct {
	entries       [rawKVCommandWALRIDInlineCacheEntries]rawKVCommandWALRIDCacheEntry
	overflow      map[page.ValuePtr]uint64
	mapHint       int
	count         int
	overflowCount int
}

var rawKVCommandWALRIDCacheMapPool sync.Pool

func (c *rawKVCommandWALRIDCache) lookup(ptr page.ValuePtr) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	for i := 0; i < c.count; i++ {
		entry := c.entries[i]
		if entry.ptr == ptr {
			return entry.rid, true
		}
	}
	if c.overflow != nil {
		rid, ok := c.overflow[ptr]
		return rid, ok
	}
	return 0, false
}

func (c *rawKVCommandWALRIDCache) store(ptr page.ValuePtr, rid uint64) {
	if c == nil || rid == 0 {
		return
	}
	for i := 0; i < c.count; i++ {
		if c.entries[i].ptr == ptr {
			c.entries[i].rid = rid
			return
		}
	}
	if c.count >= len(c.entries) {
		if c.overflow == nil {
			c.overflow = acquireRawKVCommandWALRIDCacheMap(c.mapHint)
		}
		c.overflowCount++
		c.overflow[ptr] = rid
		return
	}
	c.entries[c.count] = rawKVCommandWALRIDCacheEntry{ptr: ptr, rid: rid}
	c.count++
}

func (c *rawKVCommandWALRIDCache) release() {
	if c == nil {
		return
	}
	for i := 0; i < c.count; i++ {
		c.entries[i] = rawKVCommandWALRIDCacheEntry{}
	}
	c.count = 0
	if c.overflow != nil {
		if c.mapHint <= rawKVCommandWALRIDMaxPooledOverflowEntries && c.overflowCount <= rawKVCommandWALRIDMaxPooledOverflowEntries {
			clear(c.overflow)
			rawKVCommandWALRIDCacheMapPool.Put(c.overflow)
		}
		c.overflow = nil
	}
	c.overflowCount = 0
}

func acquireRawKVCommandWALRIDCacheMap(capHint int) map[page.ValuePtr]uint64 {
	if v := rawKVCommandWALRIDCacheMapPool.Get(); v != nil {
		return v.(map[page.ValuePtr]uint64)
	}
	if capHint <= 0 {
		return make(map[page.ValuePtr]uint64)
	}
	return make(map[page.ValuePtr]uint64, capHint)
}

func (db *DB) commandWALJournalUnavailableError() error {
	if db == nil || db.closing.Load() {
		return ErrClosed
	}
	return fmt.Errorf("treedb: command wal journal unavailable")
}

// CommandWALIntent is an opaque command-WAL append/finalize token used by
// higher-level deterministic command executors such as collections.
type CommandWALIntent struct {
	inner commandWALBatchIntent
}

var ErrCommandWALMissingValueLogRID = errors.New("treedb: command wal missing value-log rid")

// AssignedLSN returns the command LSN already assigned to this intent. Replay
// intents use this to finalize durable command coverage without appending a
// duplicate foreground command frame.
func (intent *CommandWALIntent) AssignedLSN() uint64 {
	if intent == nil {
		return 0
	}
	return intent.inner.lsn
}

// ReplayAssignedLSN returns the assigned non-zero LSN and true only for
// replay-originated intents. The boolean keeps callers from treating LSN 0 as a
// replay sentinel.
func (intent *CommandWALIntent) ReplayAssignedLSN() (uint64, bool) {
	if intent == nil || !intent.inner.fromReplay || intent.inner.lsn == 0 {
		return 0, false
	}
	return intent.inner.lsn, true
}

// StagedForPublish reports whether the intent was appended by
// AppendStagedCommandWALIntent and still expects its caller to hold the
// command-WAL staging lock through root publication.
func (intent *CommandWALIntent) StagedForPublish() bool {
	return intent.staged()
}

func (intent *CommandWALIntent) staged() bool {
	return intent != nil && intent.inner.staged
}

func (db *DB) CommandWALEnabled() bool {
	return db != nil && db.commandWAL
}

// CommandWALRequestTiming reports request-scoped command-journal phases for an
// opt-in diagnostic caller. Append and Flush are non-overlapping; Sync reports
// whether the flush phase used the durable sync path rather than a kernel flush.
type CommandWALRequestTiming struct {
	Append         time.Duration
	Flush          time.Duration
	AppendObserved bool
	FlushObserved  bool
	Sync           bool
}

// FlushCommandWAL flushes the command WAL writer. When sync is true, durable
// modes fsync the command WAL; DurabilityWALOnRelaxed intentionally downgrades
// this to a flush-to-kernel boundary to preserve relaxed-sync semantics.
func (db *DB) FlushCommandWAL(sync bool) error {
	return db.flushCommandWAL(sync, true)
}

// FlushCommandWALBarrier makes value-log bytes referenced by earlier command
// frames durable before flushing the command WAL itself. It serializes with
// command-WAL publishers so the two durability boundaries cannot be reordered.
func (db *DB) FlushCommandWALBarrier(sync bool) error {
	unlock, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return err
	}
	defer unlock()

	if appender := db.currentValueLogAppender(); appender != nil {
		if flusher, ok := appender.(ValueLogExternalRefFlusher); ok {
			if err := flusher.FlushValueLogExternalRefs(nil, sync); err != nil {
				return err
			}
		} else if sync {
			if err := appender.Sync(); err != nil {
				return err
			}
		} else if err := appender.Flush(); err != nil {
			return err
		}
	}
	return db.FlushCommandWAL(sync)
}

func (db *DB) flushCommandWAL(sync bool, observe bool) error {
	if db == nil || !db.commandWAL || db.commandJournal == nil {
		return nil
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	var err error
	actualSync := sync && db.durability != DurabilityWALOnRelaxed
	start := time.Time{}
	if observe {
		start = time.Now()
	}
	if actualSync {
		err = db.commandJournal.Sync()
	} else {
		err = db.commandJournal.Flush()
	}
	if err == nil && db.testFailCommandWALFlush.Load() {
		err = errTestCommandWALFlushFailpoint
	}
	if observe {
		db.observeCommandWALFlush(actualSync, time.Since(start))
	}
	if err != nil {
		db.commandWALFlushPoisoned.Store(true)
	}
	return err
}

func (db *DB) finishCommandWALAppendFlush(path commandWALAppendStatsPath, actualSync bool, lsn uint64, timing commitlog.CommandJournalAppendFlushTiming, err error) error {
	if lsn != 0 || err == nil {
		db.observeCommandWALAppend(path, timing.Append)
	}
	if lsn != 0 {
		if err == nil && db.testFailCommandWALFlush.Load() {
			err = errTestCommandWALFlushFailpoint
		}
		db.observeCommandWALFlush(actualSync, timing.Flush)
	}
	if err != nil {
		if lsn != 0 {
			db.commandWALFlushPoisoned.Store(true)
		}
		return err
	}
	db.observeCommandWALAccepted(lsn)
	return nil
}

func (db *DB) publicationPoisonedError() error {
	if db != nil && db.publicationPoisoned.Load() {
		return fmt.Errorf("%w: root publication outcome is ambiguous; reopen required", ErrRecoveryRequired)
	}
	return nil
}

func (db *DB) commandWALPoisonedError() error {
	if err := db.publicationPoisonedError(); err != nil {
		return err
	}
	if db != nil && db.commandWAL && db.commandWALFlushPoisoned.Load() {
		return fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
	}
	return nil
}

// CheckCommandWALPublishReady verifies that this open handle can publish
// command-WAL coverage without forcing another writer flush. Public cached
// command-WAL writes already flush their command frame before visibility; the
// checkpoint publish path uses this for relaxed AppliedCommandLSN publication.
func (db *DB) CheckCommandWALPublishReady() error {
	return db.commandWALPoisonedError()
}

// CommandWALActiveBytes reports the active command-WAL segment bytes accepted by
// the open writer, including command frames buffered for batch flush.
func (db *DB) CommandWALActiveBytes() int64 {
	_, active := db.commandWALActiveSegmentSnapshot()
	return active
}

func (db *DB) commandWALActiveSegmentSnapshot() (path string, bytes int64) {
	if db == nil || !db.commandWAL || db.commandJournal == nil {
		return "", 0
	}
	return db.commandJournal.ActiveSegmentSnapshot()
}

// CommandWALBytes reports command-WAL bytes currently present on disk, plus any
// active command frames buffered by the open writer. It intentionally counts
// covered non-active segments until cleanup removes them so byte-pressure
// checkpointing can bound total command-WAL growth rather than only the active
// file.
func (db *DB) CommandWALBytes() int64 {
	if db == nil || !db.commandWAL {
		return 0
	}
	_, active := db.commandWALActiveSegmentSnapshot()
	closed := db.commandWALClosedBytes.Load()
	if closed < 0 {
		closed = 0
	}
	return closed + active
}

func (db *DB) observeCommandWALSegmentRotated(closedBytes int64) {
	if db == nil || closedBytes <= 0 {
		return
	}
	db.commandWALClosedBytes.Add(closedBytes)
}

func (db *DB) refreshCommandWALClosedBytes() {
	if db == nil || !db.commandWAL {
		return
	}
	activePath, _ := db.commandWALActiveSegmentSnapshot()
	segments, err := listWALSegments(db.dir)
	if err != nil {
		return
	}
	var closed int64
	for _, seg := range segments {
		if seg.valueLog || !isCommandWALLaneSegment(seg) || seg.size <= 0 {
			continue
		}
		if activePath != "" && samePath(seg.path, activePath) {
			continue
		}
		closed += seg.size
	}
	db.commandWALClosedBytes.Store(closed)
}

func samePath(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	absA, errA := filepath.Abs(a)
	absB, errB := filepath.Abs(b)
	if errA == nil && errB == nil {
		return absA == absB
	}
	return a == b
}

// CommandWALNextLSN reports the next LSN this handle would reserve.
func (db *DB) CommandWALNextLSN() uint64 {
	if db == nil || !db.commandWAL || db.commandJournal == nil {
		return 0
	}
	return db.commandJournal.NextLSN()
}

// RotateCommandWALActiveSegment rotates the active command-WAL segment to a
// fresh file. Checkpoint cutovers use this to make covered frames non-active so
// cleanup can reclaim them.
func (db *DB) RotateCommandWALActiveSegment(sync bool) error {
	if db == nil || !db.commandWAL || db.commandJournal == nil {
		return nil
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	if sync && db.durability == DurabilityWALOnRelaxed {
		sync = false
	}
	return db.commandJournal.RotateActiveSegment(sync)
}

// CleanupCommandWALCoveredSegments removes command-WAL segments whose max LSN is
// covered by the current durable AppliedCommandLSN and that are not active.
func (db *DB) CleanupCommandWALCoveredSegments(sync bool) error {
	if db == nil || !db.commandWAL {
		return nil
	}
	if db.readOnly {
		return ErrReadOnly
	}
	state := db.state.Load()
	if state == nil || state.AppliedCommandLSN == 0 {
		return nil
	}
	scanStart := time.Now()
	decisions, err := scanCommandWALSegmentsCoveredByAppliedLSN(db.dir, state.AppliedCommandLSN, db.walMaxSegmentBytes)
	db.commandWALCleanupScans.Add(1)
	if scanNs := commandWALDurationNs(time.Since(scanStart)); scanNs > 0 {
		db.commandWALCleanupScanNs.Add(scanNs)
	}
	if err == nil {
		decisions, err = removeCoveredCommandWALSegments(decisions)
	}
	removed := uint64(0)
	removedBytes := uint64(0)
	scannedBytes := uint64(0)
	scannedFrames := uint64(0)
	for _, decision := range decisions {
		if decision.ScannedBytes > 0 {
			scannedBytes += uint64(decision.ScannedBytes)
		}
		scannedFrames += decision.Frames
		if !decision.Removed {
			continue
		}
		removed++
		if decision.Size > 0 {
			removedBytes += uint64(decision.Size)
		}
	}
	if scannedBytes > 0 {
		db.commandWALCleanupScanBytes.Add(scannedBytes)
	}
	if scannedFrames > 0 {
		db.commandWALCleanupScanFrames.Add(scannedFrames)
	}
	if removed > 0 {
		db.commandWALCleanupRemoved.Add(removed)
		db.commandWALCleanupBytes.Add(removedBytes)
		if removedBytes > 0 {
			db.commandWALClosedBytes.Add(-int64(removedBytes))
		}
		if sync && db.durability != DurabilityWALOnRelaxed {
			if syncErr := syncDirFn(WALDirPath(db.dir)); err == nil {
				err = syncErr
			}
		}
	}
	return err
}

func (db *DB) rejectUnloggedCommandWALRootPublish() error {
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	if db != nil && db.commandWAL {
		return fmt.Errorf("%w: command wal root publish requires a command frame", ErrCommandWALUnsupported)
	}
	return nil
}

func (db *DB) NewCommandWALIntent(kind commitlog.CommandKind, scope commitlog.CommandScope, payloadFormat commitlog.PayloadFormat, payload []byte) (*CommandWALIntent, error) {
	if db == nil || !db.commandWAL {
		return nil, nil
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if db.durability == DurabilityWALOffRelaxed {
		return nil, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	maxEntryRevision, err := maxEntryRevisionFromCommandWALPayload(kind, scope, payloadFormat, payload)
	if err != nil {
		return nil, err
	}
	return &CommandWALIntent{inner: commandWALBatchIntent{
		kind:             kind,
		scope:            scope,
		payloadFormat:    payloadFormat,
		payload:          payload,
		maxEntryRevision: maxEntryRevision,
	}}, nil
}

// NewTrustedCommandWALIntent creates a command-WAL intent for payload bytes
// constructed through a canonical commitlog encoder. The append path still
// validates command identity and size, but it skips payload decoding because
// the caller owns that construction boundary.
func (db *DB) NewTrustedCommandWALIntent(kind commitlog.CommandKind, scope commitlog.CommandScope, payloadFormat commitlog.PayloadFormat, payload []byte) (*CommandWALIntent, error) {
	intent, err := db.NewCommandWALIntent(kind, scope, payloadFormat, payload)
	if err != nil || intent == nil {
		return intent, err
	}
	intent.inner.trustedPayload = true
	return intent, nil
}

func newCommandWALReplayIntent(env commitlog.CommandEnvelope, replayToken uint64) *CommandWALIntent {
	return &CommandWALIntent{inner: commandWALBatchIntent{
		kind:          env.Kind,
		scope:         env.Scope,
		payloadFormat: env.PayloadFormat,
		payload:       env.Payload,
		fromReplay:    true,
		lsn:           env.LSN,
		replayToken:   replayToken,
		coveredRange:  [1]CommandWALLSNRange{{First: env.LSN, Last: env.LSN}},
		syncOnPublish: true,
	}}
}

func (db *DB) NewCommandWALReplayIntent(env commitlog.CommandEnvelope) (*CommandWALIntent, error) {
	if db == nil {
		return nil, ErrClosed
	}
	activeLSN := db.commandWALReplayLSN.Load()
	activeToken := db.commandWALReplayToken.Load()
	if err := checkCommandWALReplayFrameActive(env.LSN, activeLSN, activeToken); err != nil {
		return nil, err
	}
	return newCommandWALReplayIntent(env, activeToken), nil
}

func (db *DB) checkCommandWALReplayIntentActive(intent *commandWALBatchIntent) error {
	if intent == nil || !intent.fromReplay {
		return nil
	}
	active := uint64(0)
	activeToken := uint64(0)
	if db != nil {
		active = db.commandWALReplayLSN.Load()
		activeToken = db.commandWALReplayToken.Load()
	}
	if err := checkCommandWALReplayFrameActive(intent.lsn, active, activeToken); err != nil {
		return err
	}
	if intent.replayToken == 0 {
		return fmt.Errorf("%w: replay intent missing recovery token for lsn %d", ErrCommandWALRejected, intent.lsn)
	}
	if activeToken != intent.replayToken {
		return fmt.Errorf("%w: replay intent recovery token mismatch for lsn %d", ErrCommandWALRejected, intent.lsn)
	}
	return nil
}

func checkCommandWALReplayFrameActive(intentLSN, activeLSN, activeToken uint64) error {
	if intentLSN == 0 {
		return fmt.Errorf("%w: replay intent missing assigned lsn", ErrCommandWALRejected)
	}
	if activeLSN == 0 {
		return fmt.Errorf("%w: replay intent lsn %d has no active recovery frame", ErrCommandWALRejected, intentLSN)
	}
	if activeLSN != intentLSN {
		return fmt.Errorf("%w: replay intent lsn %d does not match active recovery frame lsn %d", ErrCommandWALRejected, intentLSN, activeLSN)
	}
	if activeToken == 0 {
		return fmt.Errorf("%w: replay intent lsn %d has no active recovery token", ErrCommandWALRejected, intentLSN)
	}
	return nil
}

// NewCommandWALCoverageIntent returns an intent for publishing roots that
// already reflect an appended contiguous command-WAL range. It does not append a
// new command frame when used with the ordered-root command-WAL publish APIs.
func NewCommandWALCoverageIntent(appliedLSN uint64, covered CommandWALLSNRange) (*CommandWALIntent, error) {
	if appliedLSN == 0 {
		return nil, fmt.Errorf("%w: applied lsn is zero", ErrCommandWALAppliedLSNNonContig)
	}
	if covered.First == 0 || covered.Last < covered.First || covered.Last != appliedLSN {
		return nil, fmt.Errorf("%w: invalid coverage range [%d,%d] for applied %d", ErrCommandWALAppliedLSNNonContig, covered.First, covered.Last, appliedLSN)
	}
	return &CommandWALIntent{inner: commandWALBatchIntent{
		lsn:          appliedLSN,
		coveredRange: [1]CommandWALLSNRange{covered},
	}}, nil
}

func (db *DB) prepareRawKVCommandWALIntent(b *Batch) (*commandWALBatchIntent, error) {
	if db == nil || !db.commandWAL {
		return nil, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return nil, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if b == nil || b.batch == nil {
		return nil, nil
	}
	entries := b.batch.OrderedEntries()
	if !b.batch.HasDeleteRanges() {
		// SortedEntries is idempotent after the first sort/compaction; point-only
		// batches keep the existing compacted command-WAL fast path.
		entries = b.batch.SortedEntries()
	}
	if len(entries) == 0 {
		return nil, nil
	}
	return db.newRawKVCommandWALIntentFromEntries(entries)
}

// NewRawKVCommandWALIntentFromOrderedEntries builds a public raw-KV command
// intent from entries that are already in the caller's required application
// order. Unlike prepareRawKVCommandWALIntent, this does not sort or compact
// point ops; public cached batches rely on replay order to preserve mixed
// set/delete/range-delete semantics.
func (db *DB) NewRawKVCommandWALIntentFromOrderedEntries(entries []batchpkg.Entry) (*CommandWALIntent, error) {
	if db == nil || !db.commandWAL {
		return nil, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return nil, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if len(entries) == 0 {
		return nil, nil
	}
	intent, err := db.newRawKVCommandWALPayloadIntentFromEntries(entries)
	if intent == nil || err != nil {
		return nil, err
	}
	return &CommandWALIntent{inner: *intent}, nil
}

// AppendRawKVCommandWALOrderedEntries appends a raw-KV command frame directly
// from entries that are already in the caller's required application order. The
// caller must keep entries and their key/value buffers immutable until this
// method returns.
func (db *DB) AppendRawKVCommandWALOrderedEntries(entries []batchpkg.Entry, sync bool) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if len(entries) == 0 {
		return 0, nil
	}
	intent, err := db.newRawKVCommandWALIntentFromEntries(entries)
	if intent == nil || err != nil {
		return 0, err
	}
	return db.AppendCommandWALIntent(&CommandWALIntent{inner: *intent}, sync)
}

// AppendRawKVCommandWALOrderedEntryScan appends a raw-KV command frame by
// replaying already-ordered entries directly into the command encoder. The
// replay source must be deterministic and replayable because planning and
// writing scan it separately. Callers must keep replayed entry buffers immutable
// until this method returns.
func (db *DB) AppendRawKVCommandWALOrderedEntryScan(scanEntries func(func(batchpkg.Entry) error) error, sync bool) (uint64, error) {
	return db.AppendRawKVCommandWALOrderedEntryScanWithHint(scanEntries, 0, sync)
}

// AppendRawKVCommandWALOrderedEntryScanWithHint appends a raw-KV command frame
// like AppendRawKVCommandWALOrderedEntryScan, using opHint only to pre-size
// transient planning caches.
func (db *DB) AppendRawKVCommandWALOrderedEntryScanWithHint(scanEntries func(func(batchpkg.Entry) error) error, opHint int, sync bool) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if scanEntries == nil {
		return 0, nil
	}
	intent, err := db.newRawKVCommandWALIntentFromEntryScanWithHint(scanEntries, opHint)
	if intent == nil || err != nil {
		return 0, err
	}
	return db.AppendCommandWALIntent(&CommandWALIntent{inner: *intent}, sync)
}

// AppendRawKVCommandWALOrderedEntryScanWithHintMeasured is the opt-in
// diagnostic variant of AppendRawKVCommandWALOrderedEntryScanWithHint. Planning
// and command-WAL serialization remain outside the returned append/flush
// subphases and are therefore attributable to the caller's command remainder.
func (db *DB) AppendRawKVCommandWALOrderedEntryScanWithHintMeasured(scanEntries func(func(batchpkg.Entry) error) error, opHint int, sync bool) (uint64, CommandWALRequestTiming, error) {
	var timing CommandWALRequestTiming
	if db == nil || !db.commandWAL {
		return 0, timing, nil
	}
	if db.readOnly {
		return 0, timing, ErrReadOnly
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, timing, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if scanEntries == nil {
		return 0, timing, nil
	}
	intent, err := db.newRawKVCommandWALIntentFromEntryScanWithHint(scanEntries, opHint)
	if intent == nil || err != nil {
		return 0, timing, err
	}
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return 0, timing, err
	}
	defer unlockCommandWALPublish()
	lsn, err := db.appendCommandWALIntentWithTiming(intent, sync, &timing)
	return lsn, timing, err
}

func (db *DB) newRawKVCommandWALIntentFromEntries(entries []batchpkg.Entry) (*commandWALBatchIntent, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	return db.newRawKVCommandWALIntentFromEntryScanWithHint(func(emit func(batchpkg.Entry) error) error {
		for i := range entries {
			if err := emit(entries[i]); err != nil {
				return err
			}
		}
		return nil
	}, len(entries))
}

func (db *DB) newRawKVCommandWALIntentFromEntryScan(scanEntries func(func(batchpkg.Entry) error) error) (*commandWALBatchIntent, error) {
	return db.newRawKVCommandWALIntentFromEntryScanWithHint(scanEntries, 0)
}

func (db *DB) newRawKVCommandWALIntentFromEntryScanWithHint(scanEntries func(func(batchpkg.Entry) error) error, opHint int) (*commandWALBatchIntent, error) {
	externalRefs := false
	var ridCache *rawKVCommandWALRIDCache
	var externalRefFileIDs []uint32
	maxEntryRevision := page.LegacyEntryRevision
	planScan := db.rawKVCommandWALOperationScannerFromEntryScan(func(emit func(batchpkg.Entry) error) error {
		if scanEntries == nil {
			return nil
		}
		return scanEntries(func(entry batchpkg.Entry) error {
			if entry.Type != batchpkg.OpDeleteRange && entry.Revision > maxEntryRevision {
				maxEntryRevision = entry.Revision
			}
			return emit(entry)
		})
	}, &ridCache, &externalRefs, &externalRefFileIDs, opHint)
	plan, err := commitlog.PlanRawKVBatchPayloadScan(planScan)
	if err != nil {
		return nil, err
	}
	if plan.Count == 0 {
		return nil, nil
	}
	writeScan := db.rawKVCommandWALOperationScannerFromEntryScan(scanEntries, &ridCache, nil, nil, opHint)
	return &commandWALBatchIntent{
		kind:               commitlog.CommandKindRawKVBatch,
		scope:              commitlog.CommandScopeRawKV,
		payloadFormat:      commitlog.PayloadFormatRawKVBatchV1,
		rawKVScan:          writeScan,
		rawKVPlan:          plan,
		rawKVRIDCache:      ridCache,
		rawKVDirect:        true,
		externalRefs:       externalRefs,
		externalRefFileIDs: externalRefFileIDs,
		maxEntryRevision:   maxEntryRevision,
	}, nil
}

func (db *DB) newRawKVCommandWALPayloadIntentFromEntries(entries []batchpkg.Entry) (*commandWALBatchIntent, error) {
	externalRefs := false
	var ridCache *rawKVCommandWALRIDCache
	scan := db.rawKVCommandWALOperationScannerWithHint(entries, &ridCache, &externalRefs, len(entries))
	plan, err := commitlog.PlanRawKVBatchPayloadScan(scan)
	if err != nil {
		return nil, err
	}
	if plan.Count == 0 {
		return nil, nil
	}
	var payload []byte
	if plan.EntryRevisions {
		payload, err = commitlog.EncodeRawKVBatchPayloadPlanned(plan, scan)
	} else {
		payload, err = commitlog.EncodeRawKVBatchPayloadScanWithHint(scan, plan.Count, 0)
	}
	if err != nil {
		return nil, err
	}
	var externalRefFileIDs []uint32
	if externalRefs {
		externalRefFileIDs = rawKVCommandWALExternalRefFileIDs(entries)
	}
	return &commandWALBatchIntent{
		kind:               commitlog.CommandKindRawKVBatch,
		scope:              commitlog.CommandScopeRawKV,
		payloadFormat:      commitlog.PayloadFormatRawKVBatchV1,
		payload:            payload,
		externalRefs:       externalRefs,
		externalRefFileIDs: externalRefFileIDs,
		maxEntryRevision:   maxEntryRevisionFromEntries(entries),
	}, nil
}

func maxEntryRevisionFromEntries(entries []batchpkg.Entry) page.EntryRevision {
	maxRevision := page.LegacyEntryRevision
	for i := range entries {
		entry := &entries[i]
		if entry.Type == batchpkg.OpDeleteRange {
			continue
		}
		if entry.Revision > maxRevision {
			maxRevision = entry.Revision
		}
	}
	return maxRevision
}

func maxEntryRevisionFromCommandWALPayload(kind commitlog.CommandKind, scope commitlog.CommandScope, payloadFormat commitlog.PayloadFormat, payload []byte) (page.EntryRevision, error) {
	if kind != commitlog.CommandKindRawKVBatch || scope != commitlog.CommandScopeRawKV || payloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		return page.LegacyEntryRevision, nil
	}
	maxRevision := page.LegacyEntryRevision
	err := commitlog.ScanRawKVBatchPayloadWithRevision(payload, func(op commitlog.RawKVOp, _ []byte, _ []byte, revision uint64) error {
		if op == commitlog.RawKVOpDeleteRange || revision == 0 {
			return nil
		}
		entryRevision := page.EntryRevision(revision)
		if entryRevision > maxRevision {
			maxRevision = entryRevision
		}
		return nil
	})
	if err != nil {
		return page.LegacyEntryRevision, err
	}
	return maxRevision, nil
}

func (db *DB) rawKVCommandWALOperationScanner(entries []batchpkg.Entry, ridCache **rawKVCommandWALRIDCache, externalRefs *bool) commitlog.RawKVBatchOperationScanner {
	return db.rawKVCommandWALOperationScannerWithHint(entries, ridCache, externalRefs, len(entries))
}

func (db *DB) rawKVCommandWALOperationScannerWithHint(entries []batchpkg.Entry, ridCache **rawKVCommandWALRIDCache, externalRefs *bool, opHint int) commitlog.RawKVBatchOperationScanner {
	return db.rawKVCommandWALOperationScannerFromEntryScan(func(emit func(batchpkg.Entry) error) error {
		for i := range entries {
			if err := emit(entries[i]); err != nil {
				return err
			}
		}
		return nil
	}, ridCache, externalRefs, nil, opHint)
}

func (db *DB) rawKVCommandWALOperationScannerFromEntryScan(scanEntries func(func(batchpkg.Entry) error) error, ridCache **rawKVCommandWALRIDCache, externalRefs *bool, externalRefFileIDs *[]uint32, opHint int) commitlog.RawKVBatchOperationScanner {
	return func(emit func(commitlog.RawKVOperation) error) error {
		if scanEntries == nil {
			return nil
		}
		return scanEntries(func(entry batchpkg.Entry) error {
			op, ok, err := db.rawKVCommandWALOperationFromEntry(entry, ridCache, externalRefs, externalRefFileIDs, opHint)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			return emit(op)
		})
	}
}

func (db *DB) rawKVCommandWALOperationFromEntry(entry batchpkg.Entry, ridCache **rawKVCommandWALRIDCache, externalRefs *bool, externalRefFileIDs *[]uint32, opHint int) (commitlog.RawKVOperation, bool, error) {
	switch entry.Type {
	case batchpkg.OpDeleteRange:
		if batchpkg.IsDeleteRangeNoop(entry.Key, entry.Value) {
			return commitlog.RawKVOperation{}, false, nil
		}
		return commitlog.RawKVOperation{Op: commitlog.RawKVOpDeleteRange, Key: entry.Key, Value: entry.Value}, true, nil
	case batchpkg.OpDelete:
		return commitlog.RawKVOperation{Op: commitlog.RawKVOpDelete, Key: entry.Key, Revision: uint64(entry.Revision)}, true, nil
	case batchpkg.OpPut:
		if entry.IsPtr {
			if externalRefs != nil {
				*externalRefs = true
			}
			if externalRefFileIDs != nil {
				appendRawKVCommandWALExternalRefFileID(externalRefFileIDs, entry.ValuePtr.FileID)
			}
			if ridCache == nil {
				return commitlog.RawKVOperation{}, false, fmt.Errorf("treedb: command wal raw kv rid cache unavailable")
			}
			if *ridCache == nil {
				*ridCache = makeRawKVCommandWALRIDCache(opHint)
			}
			rid, err := db.lookupCommandWALValueLogRID(entry.ValuePtr, *ridCache)
			if err != nil {
				return commitlog.RawKVOperation{}, false, err
			}
			return commitlog.RawKVOperation{Op: commitlog.RawKVOpSetRID, Key: entry.Key, RID: rid, Revision: uint64(entry.Revision)}, true, nil
		}
		return commitlog.RawKVOperation{Op: commitlog.RawKVOpSet, Key: entry.Key, Value: entry.Value, Revision: uint64(entry.Revision)}, true, nil
	default:
		return commitlog.RawKVOperation{}, false, fmt.Errorf("treedb: command wal unknown raw kv batch op %d", entry.Type)
	}
}

func makeRawKVCommandWALRIDCache(opHint int) *rawKVCommandWALRIDCache {
	if opHint <= 0 {
		return &rawKVCommandWALRIDCache{}
	}
	return &rawKVCommandWALRIDCache{mapHint: NormalizePublicBatchReserveHint(opHint)}
}

func rawKVCommandWALExternalRefFileIDs(entries []batchpkg.Entry) []uint32 {
	var ids []uint32
	for i := range entries {
		entry := entries[i]
		if entry.Type != batchpkg.OpPut || !entry.IsPtr || entry.ValuePtr.FileID == 0 {
			continue
		}
		appendRawKVCommandWALExternalRefFileID(&ids, entry.ValuePtr.FileID)
	}
	return ids
}

func appendRawKVCommandWALExternalRefFileID(ids *[]uint32, fileID uint32) {
	if ids == nil || fileID == 0 {
		return
	}
	for _, id := range *ids {
		if id == fileID {
			return
		}
	}
	*ids = append(*ids, fileID)
}

func (db *DB) lookupCommandWALValueLogRID(ptr page.ValuePtr, ridCache *rawKVCommandWALRIDCache) (uint64, error) {
	if db == nil || db.valueLogManager == nil {
		return 0, fmt.Errorf("treedb: command wal raw kv pointer rid reader unavailable (file=%d offset=%d len=%d)", ptr.FileID, ptr.Offset, ptr.Length)
	}
	if ptr.FileID == 0 || ptr.Length == 0 {
		return 0, fmt.Errorf("treedb: command wal raw kv invalid value-log pointer (file=%d offset=%d len=%d)", ptr.FileID, ptr.Offset, ptr.Length)
	}
	if ridCache == nil {
		return 0, fmt.Errorf("treedb: command wal raw kv rid cache unavailable")
	}
	if rid, ok := ridCache.lookup(ptr); ok {
		return rid, nil
	}
	rid, err := db.valueLogManager.ReadRIDUnverified(ptr)
	if isCommandWALRIDLookupVisibilityError(err) {
		if flushErr := db.flushCommandWALExternalRefs(false, []uint32{ptr.FileID}); flushErr != nil {
			return 0, flushErr
		}
		rid, err = db.valueLogManager.ReadRIDUnverified(ptr)
	}
	if err != nil {
		return 0, err
	}
	ridCache.store(ptr, rid)
	return rid, nil
}

func isCommandWALRIDLookupVisibilityError(err error) bool {
	// Missing segment files are real recovery blockers. Retry only short-read
	// visibility cases where the current appender may not have flushed bytes yet.
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (db *DB) flushCommandWALExternalRefs(sync bool, fileIDs []uint32) error {
	appender := db.currentValueLogAppender()
	if appender == nil && len(fileIDs) == 0 {
		return ErrValueLogAppenderUnavailable
	}
	var activeFileID uint32
	if appender != nil {
		if len(fileIDs) > 0 {
			if flusher, ok := appender.(ValueLogExternalRefFlusher); ok {
				if err := flusher.FlushValueLogExternalRefs(fileIDs, sync); err != nil {
					return err
				}
				return nil
			}
		}
		if _, fileID, ok := appender.CurrentValueLogSegment(); ok {
			activeFileID = fileID
		}
		if sync {
			if err := appender.Sync(); err != nil {
				return err
			}
		} else if err := appender.Flush(); err != nil {
			return err
		}
	}
	if !sync {
		return nil
	}
	for _, fileID := range fileIDs {
		if fileID == 0 || fileID == activeFileID {
			continue
		}
		if err := db.syncCommandWALExternalRefSegment(fileID); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) syncCommandWALExternalRefSegment(fileID uint32) error {
	if db == nil || db.valueLogManager == nil || fileID == 0 {
		return ErrValueLogAppenderUnavailable
	}
	path := db.valueLogManager.SegmentPath(fileID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

func (db *DB) appendRawKVCommandWALIntent(intent *commandWALBatchIntent, sync bool) (uint64, error) {
	return db.appendCommandWALIntent(intent, sync)
}

func (db *DB) appendPublicCommandWALIntent(intent *CommandWALIntent, sync bool) (uint64, error) {
	if intent == nil {
		return 0, nil
	}
	if intent.inner.fromReplay && intent.inner.lsn == 0 {
		return 0, fmt.Errorf("%w: replay intent missing assigned lsn", ErrCommandWALRejected)
	}
	if err := db.checkCommandWALReplayIntentActive(&intent.inner); err != nil {
		return 0, err
	}
	if intent.inner.lsn != 0 {
		if !intent.inner.fromReplay {
			if err := db.commandWALPoisonedError(); err != nil {
				return 0, err
			}
		}
		// Replay intents already refer to a durable frame; recovery must only
		// publish that covered LSN, never append a duplicate command.
		return intent.inner.lsn, nil
	}
	return db.appendCommandWALIntent(&intent.inner, sync)
}

func commandWALIntentFrameAlreadyAppended(intent *CommandWALIntent) bool {
	return intent != nil && intent.inner.lsn != 0
}

// AppendCommandWALIntent appends a deterministic command frame without
// publishing roots. It is used by cached public command-WAL writers that must
// make a typed frame replay-visible before inserting the mutation into memory.
// If the append succeeds but the post-append flush fails, the returned LSN is
// still the allocated LSN and the open handle is poisoned for recovery.
func (db *DB) AppendCommandWALIntent(intent *CommandWALIntent, sync bool) (uint64, error) {
	if db != nil && db.readOnly {
		return 0, ErrReadOnly
	}
	if intent == nil || commandWALIntentFrameAlreadyAppended(intent) {
		return db.appendPublicCommandWALIntent(intent, sync)
	}
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return 0, err
	}
	defer unlockCommandWALPublish()
	return db.appendPublicCommandWALIntent(intent, sync)
}

// AppendStagedCommandWALIntent appends an intent while the caller holds a
// higher-level staging guard observed by public command-WAL barriers.
func (db *DB) AppendStagedCommandWALIntent(intent *CommandWALIntent, sync bool) (uint64, error) {
	if db != nil && db.readOnly {
		return 0, ErrReadOnly
	}
	lsn, err := db.appendPublicCommandWALIntent(intent, sync)
	if err != nil {
		return 0, err
	}
	if intent != nil && lsn != 0 {
		intent.inner.staged = true
	}
	return lsn, nil
}

// AppendCommandWALPayload appends a command-WAL frame without allocating a
// reusable intent token. It is for public cached write paths that only need the
// assigned LSN after the append succeeds.
func (db *DB) AppendCommandWALPayload(kind commitlog.CommandKind, scope commitlog.CommandScope, payloadFormat commitlog.PayloadFormat, payload []byte, sync bool) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if db.readOnly {
		return 0, ErrReadOnly
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	intent := commandWALBatchIntent{
		kind:          kind,
		scope:         scope,
		payloadFormat: payloadFormat,
		payload:       payload,
	}
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return 0, err
	}
	defer unlockCommandWALPublish()
	return db.appendCommandWALIntent(&intent, sync)
}

// AppendRawKVSingleCommandWAL appends a one-operation RawKVBatch command frame.
// It flushes while holding the command-journal lock; relaxed durability flushes
// without fsync rather than forcing strict-sync semantics. If that post-append
// flush fails, the returned LSN is still the allocated LSN; callers must record
// the command as pending and treat subsequent command-WAL appends as
// recovery-required until the DB is reopened.
func (db *DB) AppendRawKVSingleCommandWAL(op commitlog.RawKVOperation, sync bool) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	unlockRawPublish := db.lockCommandWALRawPublish()
	defer unlockRawPublish()
	if err := db.runCommandWALRawPublishBarriers(); err != nil {
		return 0, err
	}
	if op.Op == commitlog.RawKVOpSetRID {
		return 0, fmt.Errorf("%w: public single-op command WAL cannot carry external refs", ErrCommandWALUnsupported)
	}
	if db.closing.Load() {
		return 0, ErrClosed
	}
	if db.commandJournal == nil {
		return 0, db.commandWALJournalUnavailableError()
	}
	if db.commandWALFlushPoisoned.Load() {
		return 0, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
	}
	baseAppliedLSN := uint64(0)
	if state := db.state.Load(); state != nil {
		baseAppliedLSN = state.AppliedCommandLSN
	}
	actualSync := sync && db.durability != DurabilityWALOnRelaxed
	lsn, timing, err := db.commandJournal.AppendRawKVSingleCommandAndFlushMeasured(baseAppliedLSN, op, actualSync)
	if err := db.finishCommandWALAppendFlush(commandWALAppendStatsPoint, actualSync, lsn, timing, err); err != nil {
		return lsn, err
	}
	return lsn, nil
}

// AppendRawKVPointCommandWALTrusted appends a caller-validated public raw KV
// point Set/Delete command. It is intended for public cached command-WAL writes
// after cached preflight has validated the user input and before visibility.
// It flushes while holding the command-journal lock; relaxed durability flushes
// without fsync rather than forcing strict-sync semantics. If that post-append
// flush fails, the returned LSN is still the allocated LSN; callers must record
// the command as pending and treat subsequent command-WAL appends as
// recovery-required until the DB is reopened.
func (db *DB) AppendRawKVPointCommandWALTrusted(op commitlog.RawKVOp, key, value []byte, sync bool) (uint64, error) {
	return db.appendRawKVPointCommandWALTrustedWithRevision(op, key, value, 0, sync)
}

// AppendRawKVPointCommandWALTrustedWithRevision appends a caller-validated
// public raw-KV point command with native entry revision metadata.
func (db *DB) AppendRawKVPointCommandWALTrustedWithRevision(op commitlog.RawKVOp, key, value []byte, revision page.EntryRevision, sync bool) (uint64, error) {
	return db.appendRawKVPointCommandWALTrustedWithRevision(op, key, value, uint64(revision), sync)
}

func (db *DB) appendRawKVPointCommandWALTrustedWithRevision(op commitlog.RawKVOp, key, value []byte, revision uint64, sync bool) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	unlockRawPublish := db.lockCommandWALRawPublish()
	defer unlockRawPublish()
	if err := db.runCommandWALRawPublishBarriers(); err != nil {
		return 0, err
	}
	if db.closing.Load() {
		return 0, ErrClosed
	}
	if db.commandJournal == nil {
		return 0, db.commandWALJournalUnavailableError()
	}
	if db.commandWALFlushPoisoned.Load() {
		return 0, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
	}
	baseAppliedLSN := uint64(0)
	if state := db.state.Load(); state != nil {
		baseAppliedLSN = state.AppliedCommandLSN
	}
	actualSync := sync && db.durability != DurabilityWALOnRelaxed
	lsn, timing, err := db.commandJournal.AppendRawKVPointCommandTrustedWithRevisionAndFlushMeasured(baseAppliedLSN, op, key, value, revision, actualSync)
	if err := db.finishCommandWALAppendFlush(commandWALAppendStatsPoint, actualSync, lsn, timing, err); err != nil {
		return lsn, err
	}
	return lsn, nil
}

// AppendRawKVBatchPayloadCommandWAL appends a prebuilt RawKVBatch payload as a
// command frame. It delegates post-append flushing to FlushCommandWAL(sync);
// relaxed durability flushes without fsync rather than forcing strict-sync
// semantics. If that post-append flush fails, the returned LSN is still the
// allocated LSN; callers must record the command as pending and treat subsequent
// command-WAL appends as recovery-required until the DB is reopened.
func (db *DB) AppendRawKVBatchPayloadCommandWAL(payload []byte, sync bool) (uint64, error) {
	lsn, _, err := db.appendRawKVBatchPayloadCommandWAL(payload, sync, false, false)
	return lsn, err
}

// AppendRawKVBatchPayloadCommandWALTrusted appends a prebuilt RawKVBatch
// payload that was constructed through a trusted canonical encoder/builder.
// It has the same post-append flush-failure contract as
// AppendRawKVBatchPayloadCommandWAL.
func (db *DB) AppendRawKVBatchPayloadCommandWALTrusted(payload []byte, sync bool) (uint64, error) {
	lsn, _, err := db.appendRawKVBatchPayloadCommandWAL(payload, sync, true, false)
	return lsn, err
}

// AppendRawKVBatchPayloadCommandWALTrustedMeasured is the opt-in diagnostic
// variant of AppendRawKVBatchPayloadCommandWALTrusted. It returns the measured
// append and post-append flush/sync subphases without changing write ordering.
func (db *DB) AppendRawKVBatchPayloadCommandWALTrustedMeasured(payload []byte, sync bool) (uint64, CommandWALRequestTiming, error) {
	return db.appendRawKVBatchPayloadCommandWAL(payload, sync, true, true)
}

func (db *DB) appendRawKVBatchPayloadCommandWAL(payload []byte, sync bool, trusted bool, measured bool) (uint64, CommandWALRequestTiming, error) {
	var requestTiming CommandWALRequestTiming
	if db == nil || !db.commandWAL {
		return 0, requestTiming, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, requestTiming, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	unlockRawPublish := db.lockCommandWALRawPublish()
	defer unlockRawPublish()
	if err := db.runCommandWALRawPublishBarriers(); err != nil {
		return 0, requestTiming, err
	}
	if db.closing.Load() {
		return 0, requestTiming, ErrClosed
	}
	if db.commandJournal == nil {
		return 0, requestTiming, db.commandWALJournalUnavailableError()
	}
	if db.commandWALFlushPoisoned.Load() {
		return 0, requestTiming, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
	}
	baseAppliedLSN := uint64(0)
	if state := db.state.Load(); state != nil {
		baseAppliedLSN = state.AppliedCommandLSN
	}
	var lsn uint64
	var err error
	var timing commitlog.CommandJournalAppendFlushTiming
	actualSync := sync && db.durability != DurabilityWALOnRelaxed
	if trusted {
		lsn, timing, err = db.commandJournal.AppendRawKVBatchPayloadCommandTrustedAndFlushMeasured(baseAppliedLSN, payload, actualSync)
	} else {
		lsn, timing, err = db.commandJournal.AppendRawKVBatchPayloadCommandAndFlushMeasured(baseAppliedLSN, payload, actualSync)
	}
	if measured {
		requestTiming.Append = timing.Append
		requestTiming.Flush = timing.Flush
		requestTiming.AppendObserved = lsn != 0 || err == nil
		requestTiming.FlushObserved = lsn != 0
		requestTiming.Sync = actualSync
	}
	if err := db.finishCommandWALAppendFlush(commandWALAppendStatsPayload, actualSync, lsn, timing, err); err != nil {
		return lsn, requestTiming, err
	}
	return lsn, requestTiming, nil
}

func (db *DB) PublishCommandWALNoop(intent *CommandWALIntent, sync bool) error {
	if intent == nil {
		return nil
	}
	unlockCommandWALPublish := func() {}
	if !intent.staged() {
		var err error
		unlockCommandWALPublish, err = db.LockCommandWALPublishWithBarriers()
		if err != nil {
			return err
		}
	}
	defer unlockCommandWALPublish()
	return db.publishCommandWALNoop(intent, sync)
}

// PublishStagedCommandWALNoop publishes an already-staged command-WAL no-op.
// Callers must hold a higher-level raw publish or staging guard from the frame
// append through this publish call.
func (db *DB) PublishStagedCommandWALNoop(intent *CommandWALIntent, sync bool) error {
	return db.publishCommandWALNoop(intent, sync)
}

func (db *DB) publishCommandWALNoop(intent *CommandWALIntent, sync bool) error {
	if intent == nil {
		return nil
	}
	if db == nil {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if !db.CommandWALEnabled() {
		return ErrCommandWALUnsupported
	}
	sync = commandWALIntentPublishSync(intent, sync)
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.commitMu.Lock()
	if _, err := db.appendPublicCommandWALIntent(intent, sync); err != nil {
		db.commitMu.Unlock()
		return err
	}
	db.mu.RLock()
	baseSeq := db.meta.CommitSeq
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	vlogRefDelta := db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
	post, err := db.finalizeCommitLockedWithOptions(userRoot, systemRoot, nil, sync, adaptive.Metrics{}, nil, false, vlogRefDelta, nil, nil, commandWALFinalizeOptionsForPublicIntent(intent))
	if err != nil {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		db.poisonCommandWALAfterPublicPostAppendFailure(intent)
		db.commitMu.Unlock()
		return err
	}
	db.commitMu.Unlock()
	db.finalizeCommitPostWork(post)
	intent.inner.staged = false
	return nil
}

func commandWALIntentPublishSync(intent *CommandWALIntent, sync bool) bool {
	return sync || (intent != nil && intent.inner.syncOnPublish)
}

func (db *DB) appendCommandWALIntent(intent *commandWALBatchIntent, sync bool) (uint64, error) {
	return db.appendCommandWALIntentWithTiming(intent, sync, nil)
}

func (db *DB) appendCommandWALIntentWithTiming(intent *commandWALBatchIntent, sync bool, requestTiming *CommandWALRequestTiming) (uint64, error) {
	if intent == nil {
		return 0, nil
	}
	if intent.fromReplay && intent.lsn == 0 {
		return 0, fmt.Errorf("%w: replay intent missing assigned lsn", ErrCommandWALRejected)
	}
	if err := db.checkCommandWALReplayIntentActive(intent); err != nil {
		return 0, err
	}
	if intent.lsn != 0 {
		// The frame was already durably appended. Fail closed if poison was set
		// after the append (e.g. by finalizeCommitLockedWithOptions failing on a
		// subsequent attempt in the same retry loop) so we don't re-enter finalize
		// with a stale LSN on a poisoned handle.
		if db != nil && db.commandWALFlushPoisoned.Load() {
			return 0, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
		}
		return intent.lsn, nil
	}
	if db == nil {
		return 0, ErrClosed
	}
	if db.closing.Load() || db.commandJournal == nil {
		return 0, db.commandWALJournalUnavailableError()
	}
	if db.commandWALFlushPoisoned.Load() {
		return 0, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
	}
	if intent.rawKVDirect && intent.rawKVRIDCache != nil {
		defer intent.rawKVRIDCache.release()
	}
	if intent.externalRefs {
		// SetRID frames reference value-log positions by offset. The
		// value-log data must be visible before the command frame is written.
		// WriteSync also fsyncs referenced value-log segments for power-loss
		// durability. Non-sync Write only flushes process buffers, matching the
		// command-journal Flush path's non-fsync durability contract.
		if err := db.flushCommandWALExternalRefs(sync, intent.externalRefFileIDs); err != nil {
			return 0, err
		}
	}
	db.mu.RLock()
	baseAppliedLSN := db.meta.AppliedCommandLSN
	db.mu.RUnlock()
	var lsn uint64
	var err error
	appendPath := commandWALAppendStatsIntent
	appendStart := time.Now()
	if intent.rawKVDirect {
		appendPath = commandWALAppendStatsEntryScan
		scan := intent.rawKVScan
		if scan == nil {
			scan = db.rawKVCommandWALOperationScanner(intent.rawKVEntries, &intent.rawKVRIDCache, nil)
		}
		lsn, err = db.commandJournal.AppendRawKVBatchPayloadScanCommandTrusted(baseAppliedLSN, intent.rawKVPlan, scan)
	} else if intent.trustedPayload && !intent.externalRefs {
		appendPath = commandWALAppendStatsPayload
		lsn, err = db.commandJournal.AppendCommandPayloadTrusted(intent.kind, intent.scope, intent.payloadFormat, baseAppliedLSN, intent.payload)
	} else {
		lsn, err = db.commandJournal.AppendCommand(commitlog.CommandEnvelope{
			Kind:           intent.kind,
			Scope:          intent.scope,
			BaseAppliedLSN: baseAppliedLSN,
			PayloadFormat:  intent.payloadFormat,
			Payload:        intent.payload,
		})
	}
	appendElapsed := time.Since(appendStart)
	if requestTiming != nil {
		requestTiming.Append = appendElapsed
		requestTiming.AppendObserved = lsn != 0 || err == nil
	}
	if lsn != 0 || err == nil {
		db.observeCommandWALAppend(appendPath, appendElapsed)
	}
	if err != nil {
		return 0, err
	}
	if lsn != 0 {
		intent.lsn = lsn
		intent.coveredRange[0] = CommandWALLSNRange{First: lsn, Last: lsn}
	}
	actualSync := sync && db.durability != DurabilityWALOnRelaxed
	flushStart := time.Time{}
	if requestTiming != nil {
		requestTiming.FlushObserved = true
		requestTiming.Sync = actualSync
		flushStart = time.Now()
	}
	flushErr := db.FlushCommandWAL(sync)
	if requestTiming != nil {
		requestTiming.Flush = time.Since(flushStart)
	}
	if flushErr != nil {
		// AppendCommand already assigned a logical LSN, and the frame may be
		// replayed if the append reached disk. A later flush/sync failure is
		// commit-ambiguous: reopen recovery may apply the frame, so this handle
		// must fail closed instead of allowing a retry to create an LSN gap.
		// FlushCommandWAL owns the relaxed-sync downgrade and poison state.
		return lsn, flushErr
	}
	db.observeCommandWALAccepted(lsn)
	return lsn, nil
}

func commandWALFinalizeOptions(intent *commandWALBatchIntent) finalizeCommitOptions {
	if intent == nil || intent.lsn == 0 {
		return finalizeCommitOptions{}
	}
	// Return a copy rather than a slice aliasing the array embedded in intent so
	// that downstream consumers cannot mutate coveredRange[0] through the slice.
	appliedRange := intent.coveredRange[0]
	if appliedRange.First == 0 {
		appliedRange = CommandWALLSNRange{First: intent.lsn, Last: intent.lsn}
	}
	return finalizeCommitOptions{
		commandWALPublish: true,
		appliedCommandLSN: intent.lsn,
		appliedRanges:     []CommandWALLSNRange{appliedRange},
		maxEntryRevision:  intent.maxEntryRevision,
	}
}

func commandWALFinalizeOptionsForPublicIntent(intent *CommandWALIntent) finalizeCommitOptions {
	if intent == nil {
		return finalizeCommitOptions{}
	}
	return commandWALFinalizeOptions(&intent.inner)
}

func (db *DB) poisonCommandWALAfterPostAppendFailure(intent *commandWALBatchIntent) {
	if db == nil || intent == nil || intent.lsn == 0 {
		// intent.lsn == 0 means the frame was never durably appended
		// (appendCommandWALIntent sets lsn once AppendCommand succeeds).
		// No need to poison; appendCommandWALIntent already poisons its own
		// flush/sync failures.
		return
	}
	// appendRawKVCommandWALIntent poisons its own flush/sync failures. This
	// path covers the later case where a command frame was appended but root
	// publication failed before AppliedCommandLSN could be published.
	db.commandWALFlushPoisoned.Store(true)
}

func (db *DB) poisonCommandWALAfterPublicPostAppendFailure(intent *CommandWALIntent) {
	if intent == nil {
		return
	}
	db.poisonCommandWALAfterPostAppendFailure(&intent.inner)
}

// MarkCommandWALIntentRecoveryRequired marks this open handle as requiring
// recovery after a command frame was appended but the caller could not make
// the corresponding mutation visible in memory or durable roots. Reopen
// recovery may apply the frame, so retrying on the same handle can create
// command-WAL gaps.
func (db *DB) MarkCommandWALIntentRecoveryRequired(intent *CommandWALIntent) {
	if db == nil || intent == nil {
		return
	}
	db.poisonCommandWALAfterPublicPostAppendFailure(intent)
}

// MarkCommandWALRecoveryRequired marks this open handle as requiring recovery
// after a public command-WAL frame was appended but the caller could not prove
// that the matching physical mutation became visible/publishable on this
// handle. Reopen recovery may apply the frame, so further command appends and
// AppliedCommandLSN publishes must fail closed.
func (db *DB) MarkCommandWALRecoveryRequired() {
	if db == nil || !db.commandWAL {
		return
	}
	db.commandWALFlushPoisoned.Store(true)
}

func applyRawKVCommandWALFrame(db *DB, env commitlog.CommandEnvelope, ridMap map[uint64]page.ValuePtr, inlineAppender *replayInlineAppender, ensureReplayRIDMap commandWALReplayRIDMapFunc, ensureReplayLogSupport commandWALReplayLogSupportFunc) error {
	if db == nil {
		return fmt.Errorf("treedb: command wal recovery missing db")
	}
	if env.Kind != commitlog.CommandKindRawKVBatch || env.Scope != commitlog.CommandScopeRawKV || env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		return commitlog.ErrCommandWALUnsupportedKind
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		return err
	}
	if len(ops) == 0 {
		// Production empty writes currently return before appending a frame.
		// Empty RawKVBatch frames are still part of the replay contract so
		// fixtures/future command kinds can explicitly advance AppliedCommandLSN
		// without changing roots. This path is only reached during recovery
		// replay; there are no concurrent writers at that point.
		db.mu.RLock()
		rootID := db.meta.UserRootPageID
		sysRootID := db.meta.SystemRootPageID
		db.mu.RUnlock()
		return db.publishCommandWALRoots(rootID, sysRootID, env.LSN, []CommandWALLSNRange{{First: env.LSN, Last: env.LSN}}, true)
	}
	b := db.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	for i := range ops {
		entry := ops[i]
		revision := page.EntryRevision(entry.Revision)
		switch entry.Op {
		case commitlog.RawKVOpSet:
			if err := b.SetWithRevision(entry.Key, entry.Value, revision); err != nil {
				if !errors.Is(err, batchpkg.ErrValueTooLarge) {
					// Non-ErrValueTooLarge errors abort frame replay entirely.
					// The caller will propagate the error; recovery will retry
					// the full frame on next reopen (at-least-once semantics).
					// Any replay value-log bytes appended by earlier ops in this
					// failed frame are unreachable until normal value-log GC.
					return err
				}
				if inlineAppender == nil {
					if ensureReplayLogSupport == nil {
						return fmt.Errorf("treedb: command wal replay log support unavailable")
					}
					var err error
					// ensureReplayLogSupport owns the replay appender lifecycle;
					// these assignments only cache the frame-local handles.
					ridMap, inlineAppender, err = ensureReplayLogSupport()
					if err != nil {
						return err
					}
				}
				if inlineAppender == nil {
					return fmt.Errorf("treedb: command wal missing replay value-log appender")
				}
				ptr, err := inlineAppender.append(entry.Value)
				if err != nil {
					return err
				}
				if err := b.SetPointerWithRevision(entry.Key, ptr, revision); err != nil {
					return err
				}
			}
		case commitlog.RawKVOpDelete:
			if err := b.DeleteWithRevision(entry.Key, revision); err != nil {
				return err
			}
		case commitlog.RawKVOpDeleteRange:
			if err := b.DeleteRange(entry.Key, entry.Value); err != nil {
				return err
			}
		case commitlog.RawKVOpSetRID:
			if ridMap == nil {
				if ensureReplayRIDMap == nil {
					return fmt.Errorf("treedb: command wal replay rid map unavailable")
				}
				var err error
				ridMap, err = ensureReplayRIDMap()
				if err != nil {
					return err
				}
			}
			ptr, ok := ridMap[entry.RID]
			if !ok {
				return fmt.Errorf("%w: %d", ErrCommandWALMissingValueLogRID, entry.RID)
			}
			if err := b.SetPointerWithRevision(entry.Key, ptr, revision); err != nil {
				return err
			}
		default:
			return commitlog.ErrCorrupt
		}
	}
	if inlineAppender != nil {
		if err := inlineAppender.syncIfDirty(); err != nil {
			return err
		}
	}
	maxEntryRevision := db.assignBatchEntryRevisions(b.batch)
	return b.writeWithCommandWALIntent(true, &commandWALBatchIntent{
		lsn:          env.LSN,
		coveredRange: [1]CommandWALLSNRange{{First: env.LSN, Last: env.LSN}},
	}, maxEntryRevision)
}
