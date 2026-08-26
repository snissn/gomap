package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"time"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type commandWALBatchIntent struct {
	kind                commitlog.CommandKind
	scope               commitlog.CommandScope
	payloadFormat       commitlog.PayloadFormat
	payload             []byte
	rawKVEntries        []batchpkg.Entry
	rawKVScan           commitlog.RawKVBatchOperationScanner
	rawKVPlan           commitlog.RawKVBatchPayloadPlan
	rawKVRIDCache       *rawKVCommandWALRIDCache
	rawKVDirect         bool
	trustedPayload      bool
	externalRefs        bool
	externalRefFileIDs  []uint32
	fromReplay          bool
	lsn                 uint64
	maxEntryRevision    page.EntryRevision
	replayToken         uint64
	coveredRange        [1]CommandWALLSNRange
	syncOnPublish       bool
	staged              bool
	dependencyResources *rootpublication.StableResourceSet
	statsPath           commandWALAppendStatsPath
	statsPathSet        bool
}

const rawKVCommandWALRIDInlineCacheEntries = 4
const rawKVCommandWALRIDMaxPooledOverflowEntries = 4 * 1024

// Bounded RawKVBatchV2 materialization limits. The cached public preflight and
// backend command encoder both enforce these values so callers cannot select a
// dependency-free materialized-RID frame merely by retaining entry.Value.
const (
	RawKVCommandWALMaterializedRIDMaxValueBytes = 64 << 10
	RawKVCommandWALMaterializedRIDMaxFrameBytes = 1 << 20
	RawKVCommandWALMaterializedRIDMaxOperations = 256
	RawKVCommandWALMaterializedRIDFrameReserve  = 256
)

// RawKVCommandWALAppendMode declares the durability boundary that will cover a
// raw-KV command frame. It is intentionally separate from whether this append
// performs the sync: grouped durable writes append participant frames first and
// acknowledge them only after a later durable-prefix barrier.
type RawKVCommandWALAppendMode uint8

const (
	// RawKVCommandWALAppendRelaxed flushes without a power-loss durability
	// boundary and always retains the external SetRID dependency contract.
	RawKVCommandWALAppendRelaxed RawKVCommandWALAppendMode = iota
	// RawKVCommandWALAppendDurable syncs this frame directly and may select the
	// bounded, self-contained SetMaterializedRID representation.
	RawKVCommandWALAppendDurable
	// RawKVCommandWALAppendDurablePrefixParticipant appends without syncing this
	// frame. The caller must not acknowledge it until a later durable-prefix
	// barrier has synced the frame. It may select bounded SetMaterializedRID.
	RawKVCommandWALAppendDurablePrefixParticipant
)

func (mode RawKVCommandWALAppendMode) valid() bool {
	return mode <= RawKVCommandWALAppendDurablePrefixParticipant
}

func (mode RawKVCommandWALAppendMode) sync() bool {
	return mode == RawKVCommandWALAppendDurable
}

func (mode RawKVCommandWALAppendMode) allowsMaterializedRID() bool {
	return mode == RawKVCommandWALAppendDurable || mode == RawKVCommandWALAppendDurablePrefixParticipant
}

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

func (c *rawKVCommandWALRIDCache) snapshot() []rawKVCommandWALRIDCacheEntry {
	if c == nil {
		return nil
	}
	result := make([]rawKVCommandWALRIDCacheEntry, 0, c.count+c.overflowCount)
	result = append(result, c.entries[:c.count]...)
	for ptr, rid := range c.overflow {
		result = append(result, rawKVCommandWALRIDCacheEntry{ptr: ptr, rid: rid})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ptr.FileID != result[j].ptr.FileID {
			return result[i].ptr.FileID < result[j].ptr.FileID
		}
		if result[i].ptr.Offset != result[j].ptr.Offset {
			return result[i].ptr.Offset < result[j].ptr.Offset
		}
		if result[i].ptr.Length != result[j].ptr.Length {
			return result[i].ptr.Length < result[j].ptr.Length
		}
		return result[i].rid < result[j].rid
	})
	return result
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
	inner         commandWALBatchIntent
	publishTiming *CommandWALPublishTiming
}

// CommandWALPublishTiming captures exclusive phases of one command-WAL
// ordered-root publication. It is opt-in because callers normally need only
// the aggregate DB telemetry. An intent is single-use while it is published.
type CommandWALPublishTiming struct {
	WriteLockWait             time.Duration
	Preflight                 time.Duration
	Append                    time.Duration
	ContextBuild              time.Duration
	RootApply                 time.Duration
	SystemBuild               time.Duration
	SystemApply               time.Duration
	Finalize                  time.Duration
	FinalizePrepareDurability time.Duration
	FinalizeCandidateBuild    time.Duration
	// FinalizeCandidate* below are additive children of
	// FinalizeCandidateBuild. ResourceWork counts exact physical-entry and
	// logical-obligation work performed by those phases.
	FinalizeCandidateVisibleBaseClone time.Duration
	FinalizeCandidateInheritedFilter  time.Duration
	FinalizeCandidateFreshCapture     time.Duration
	FinalizeCandidateClosureAssemble  time.Duration
	FinalizeCandidateVisibleClone     time.Duration
	FinalizeCandidateCOWPrepare       time.Duration
	FinalizeCandidateOther            time.Duration
	FinalizeCandidateResourceWork     rootpublication.StableResourceClosureWork
	FinalizeEnqueueActivation         time.Duration
	FinalizeAdmissionWait             time.Duration
	FinalizeDurabilityWait            time.Duration
	PostFinalize                      time.Duration
}

// SetPublishTiming requests request-scoped timings from the preflight
// context-root-builder publication path used by collection column publishes.
// Other command-WAL publication helpers do not populate this diagnostic target;
// callers outside that path must not interpret an all-zero value as measured
// work. Passing nil disables collection, and the previous target is returned
// so nested diagnostic callers can restore it.
func (intent *CommandWALIntent) SetPublishTiming(timing *CommandWALPublishTiming) (previous *CommandWALPublishTiming) {
	if intent == nil {
		return nil
	}
	previous = intent.publishTiming
	intent.publishTiming = timing
	return previous
}

// Add accumulates another publication's exclusive timings. It is useful when
// a higher-level executor temporarily installs its own timing target while
// preserving an opt-in caller target on the same intent.
func (timing *CommandWALPublishTiming) Add(other CommandWALPublishTiming) {
	if timing == nil {
		return
	}
	timing.WriteLockWait += other.WriteLockWait
	timing.Preflight += other.Preflight
	timing.Append += other.Append
	timing.ContextBuild += other.ContextBuild
	timing.RootApply += other.RootApply
	timing.SystemBuild += other.SystemBuild
	timing.SystemApply += other.SystemApply
	timing.Finalize += other.Finalize
	timing.FinalizePrepareDurability += other.FinalizePrepareDurability
	timing.FinalizeCandidateBuild += other.FinalizeCandidateBuild
	timing.FinalizeCandidateVisibleBaseClone += other.FinalizeCandidateVisibleBaseClone
	timing.FinalizeCandidateInheritedFilter += other.FinalizeCandidateInheritedFilter
	timing.FinalizeCandidateFreshCapture += other.FinalizeCandidateFreshCapture
	timing.FinalizeCandidateClosureAssemble += other.FinalizeCandidateClosureAssemble
	timing.FinalizeCandidateVisibleClone += other.FinalizeCandidateVisibleClone
	timing.FinalizeCandidateCOWPrepare += other.FinalizeCandidateCOWPrepare
	timing.FinalizeCandidateOther += other.FinalizeCandidateOther
	timing.FinalizeCandidateResourceWork.Add(other.FinalizeCandidateResourceWork)
	timing.FinalizeEnqueueActivation += other.FinalizeEnqueueActivation
	timing.FinalizeAdmissionWait += other.FinalizeAdmissionWait
	timing.FinalizeDurabilityWait += other.FinalizeDurabilityWait
	timing.PostFinalize += other.PostFinalize
}

var ErrCommandWALMissingValueLogRID = errors.New("treedb: command wal missing value-log rid")
var ErrCommandWALConflictingValueLogRID = errors.New("treedb: command wal conflicting value-log rid")

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
// opt-in diagnostic caller. All durations are exclusive. Append, Flush, and
// GroupCommitWait are non-overlapping. Sync reports whether the request joined
// a durable public group. FlushSync reports whether this request's Flush
// duration includes a physical durable sync; a shared group sync is not
// attributed to an individual request.
type CommandWALRequestTiming struct {
	PublicPayloadEntryScanPreparation          time.Duration
	PublishLockBarrierWait                     time.Duration
	BackendIntentPlanningSerialization         time.Duration
	ExternalRefOrdering                        time.Duration
	Append                                     time.Duration
	Flush                                      time.Duration
	GroupCommitWait                            time.Duration
	PostAppendPendingLSNBookkeeping            time.Duration
	PublicPreparationObserved                  bool
	PublishLockBarrierWaitObserved             bool
	BackendIntentPlanningSerializationObserved bool
	ExternalRefOrderingObserved                bool
	AppendObserved                             bool
	FlushObserved                              bool
	GroupCommitWaitObserved                    bool
	PostAppendPendingLSNBookkeepingObserved    bool
	Sync                                       bool
	FlushSync                                  bool
}

// FlushCommandWAL flushes the command WAL writer. When sync is true, it fsyncs
// the command WAL even when ordinary writes use relaxed durability. Callers
// that need a recoverable explicit-sync frontier use FlushCommandWALBarrier,
// which first closes dependency debt and appends a durable V2 barrier.
func (db *DB) FlushCommandWAL(sync bool) error {
	return db.flushCommandWAL(sync, true)
}

// FlushCommandWALBarrier makes value-log bytes referenced by earlier command
// frames durable before flushing the command WAL itself. It serializes with
// command-WAL publishers so the two durability boundaries cannot be reordered.
func (db *DB) FlushCommandWALBarrier(sync bool) error {
	_, err := db.FlushCommandWALBarrierWithLSN(sync)
	return err
}

// CommandWALBarrierResult reports the dependency-ledger work observed at the
// same raw-publish serialization cut as a command-WAL barrier.
type CommandWALBarrierResult struct {
	LSN                        uint64
	AttemptedDependencyEntries uint64
	CoveredDependencyEntries   uint64
}

// FlushCommandWALBarrierWithLSN has the same durability semantics as
// FlushCommandWALBarrier and also reports the LSN of a durable-prefix barrier
// appended by this call. It returns zero when only a physical flush is needed.
// Callers that publish command-WAL applied ranges must retain a non-zero LSN
// even when the append is followed by an error because recovery may observe the
// assigned command identity.
func (db *DB) FlushCommandWALBarrierWithLSN(sync bool) (uint64, error) {
	result, err := db.FlushCommandWALBarrierWithResult(sync)
	return result.LSN, err
}

// FlushCommandWALBarrierWithResult has the same durability semantics as
// FlushCommandWALBarrierWithLSN and additionally distinguishes dependency
// entries attempted at the exact serialized prefix from entries successfully
// covered by the completed barrier.
func (db *DB) FlushCommandWALBarrierWithResult(sync bool) (CommandWALBarrierResult, error) {
	var result CommandWALBarrierResult
	unlock, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return result, err
	}
	defer unlock()
	if sync && db != nil && db.commandWAL {
		// A barrier is a logical frame only when it promotes an undurable
		// command prefix. Appending one when the current prefix is already
		// durable leaves an unapplied no-op LSN between public checkpoint
		// ranges. The next dirty checkpoint would then reject that artificial
		// gap. Keep an empty sync at the existing frontier as a physical sync
		// below, without manufacturing a new command identity.
		nextLSN := db.CommandWALNextLSN()
		if nextLSN > 1 && db.commandWALDurableLSN.Load() < nextLSN-1 {
			result.AttemptedDependencyEntries = db.commandWALDebt.entryCountThrough(nextLSN - 1)
			result.LSN, err = db.appendCommandWALDurablePrefixBarrier()
			if err == nil {
				result.CoveredDependencyEntries = result.AttemptedDependencyEntries
			}
			return result, err
		}
		return result, db.FlushCommandWAL(true)
	}

	if appender := db.currentValueLogAppender(); appender != nil {
		if flusher, ok := appender.(ValueLogExternalRefFlusher); ok {
			if err := flusher.FlushValueLogExternalRefs(nil, sync); err != nil {
				return result, err
			}
		} else if sync {
			if err := appender.Sync(); err != nil {
				return result, err
			}
		} else if err := appender.Flush(); err != nil {
			return result, err
		}
	}
	return result, db.FlushCommandWAL(sync)
}

// SyncCommandWALAppliedPrefix closes a dependency-complete durable command-WAL
// prefix and publishes any durable-prefix barrier as a root-neutral applied
// command. Registered raw-publish barriers run while the append domain is
// serialized, so every earlier collection frame is reflected in the current
// roots before the prefix is promoted. A successful return therefore leaves no
// artificial unapplied barrier LSN for the next writer to trip over.
//
// This is the explicit-sync operation for callers that no longer retain the
// original command intent. Callers that still own an intent should use its
// normal sync publish path so the mutation and barrier share one coverage
// range. physicalSync reports whether this call completed the physical sync
// for a new barrier, even if a later root publication step reports failure; it
// is false when there is no command prefix or the applied prefix was already
// durable.
func (db *DB) SyncCommandWALAppliedPrefix() (physicalSync bool, err error) {
	if db == nil {
		return false, ErrClosed
	}
	if !db.CommandWALEnabled() {
		return false, ErrCommandWALUnsupported
	}
	if db.readOnly {
		return false, ErrReadOnly
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return false, err
	}
	unlock, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return false, err
	}
	defer unlock()

	state := db.State()
	if state == nil {
		return false, ErrClosed
	}
	nextLSN := db.CommandWALNextLSN()
	if nextLSN <= 1 {
		return false, nil
	}
	frontier := nextLSN - 1
	if frontier != state.AppliedCommandLSN {
		return false, fmt.Errorf("%w: explicit sync frontier=%d applied=%d", ErrCommandWALAppliedLSNNonContig, frontier, state.AppliedCommandLSN)
	}
	if db.commandWALDurableLSN.Load() >= frontier {
		return false, nil
	}

	barrier := &commandWALBatchIntent{
		kind: commitlog.CommandKindDurablePrefixBarrier, scope: commitlog.CommandScopeSystem,
		payloadFormat: commitlog.PayloadFormatDurablePrefixBarrierV1,
		statsPath:     commandWALAppendStatsBarrier, statsPathSet: true,
	}
	barrierLSN, err := db.appendCommandWALIntent(barrier, true)
	if err != nil {
		return false, err
	}
	if barrierLSN != frontier+1 {
		db.poisonCommandWALAfterPostAppendFailure(barrier)
		return true, fmt.Errorf("%w: durable barrier lsn=%d want=%d", ErrCommandWALAppliedLSNNonContig, barrierLSN, frontier+1)
	}
	covered := []CommandWALLSNRange{{First: barrierLSN, Last: barrierLSN}}
	if err := db.publishCurrentCommandWALRootsTeardownPinned(barrierLSN, covered, true); err != nil {
		db.poisonCommandWALAfterPostAppendFailure(barrier)
		return true, err
	}
	return true, nil
}

func (db *DB) appendCommandWALDurablePrefixBarrier() (uint64, error) {
	return db.appendCommandWALIntent(&commandWALBatchIntent{
		kind: commitlog.CommandKindDurablePrefixBarrier, scope: commitlog.CommandScopeSystem,
		payloadFormat: commitlog.PayloadFormatDurablePrefixBarrierV1,
		statsPath:     commandWALAppendStatsBarrier, statsPathSet: true,
	}, true)
}

func (db *DB) flushCommandWAL(sync bool, observe bool) error {
	return db.flushCommandWALForPath(sync, observe, commandWALAppendStatsBarrier)
}

func (db *DB) flushCommandWALForPath(sync bool, observe bool, path commandWALAppendStatsPath) error {
	if db == nil || !db.commandWAL || db.commandJournal == nil {
		return nil
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	var err error
	actualSync := sync
	start := time.Time{}
	if observe {
		start = time.Now()
	}
	err = db.commandJournal.FlushObserved(actualSync)
	if err == nil && db.testFailCommandWALFlush.Load() {
		err = errTestCommandWALFlushFailpoint
	}
	if observe {
		db.observeCommandWALFlush(path, actualSync, time.Since(start))
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
		db.observeCommandWALFlush(path, actualSync, timing.Flush)
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
		return fmt.Errorf("%w: root publication cannot safely progress; reopen required", ErrRecoveryRequired)
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
	return db.commandJournal.RotateActiveSegment(sync)
}

// CleanupCommandWALCoveredSegments removes complete command-WAL segments covered
// by a freshly revalidated monotonic durable-root proof and outside every
// journal/generation/pin protection. Any successful unlink is followed by a
// WAL-directory sync before success is reported. The legacy sync argument is
// retained for callers, but cleanup never mints or advances a durable WAL
// frontier.
func (db *DB) CleanupCommandWALCoveredSegments(_ bool) (retErr error) {
	if db == nil || !db.commandWAL {
		return nil
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	if db.closing.Load() {
		return ErrClosed
	}
	return db.cleanupCommandWALCoveredSegmentsV1()
}

// CleanupCommandWALCoveredSegmentsAtCheckpoint performs opportunistic cleanup
// after a checkpoint durability boundary. A checkpoint can be fully successful
// while cleanup authority is intentionally unavailable, for example when this
// journal session recovered a relaxed frame above the physically classified WAL
// frontier. Retain the WAL and report checkpoint success in that case. Every
// stale-proof, unlink, namespace-sync, poison, and admission failure still
// propagates; explicit cleanup callers continue to receive the unavailable
// authority error from CleanupCommandWALCoveredSegments.
func (db *DB) CleanupCommandWALCoveredSegmentsAtCheckpoint(_ bool) error {
	return db.cleanupCommandWALCoveredSegmentsAtCheckpointV1(false)
}

// cleanupCommandWALCoveredSegmentsAtCheckpointV1 accepts maintenanceAlreadyHeld
// only from an enclosing backend maintenance operation. Ordinary checkpoint
// callers still enter through the public maintenance admission gate.
func (db *DB) cleanupCommandWALCoveredSegmentsAtCheckpointV1(maintenanceAlreadyHeld bool) error {
	var err error
	if maintenanceAlreadyHeld {
		err = db.cleanupCommandWALCoveredSegmentsV1()
	} else {
		err = db.CleanupCommandWALCoveredSegments(false)
	}
	return normalizeCommandWALCheckpointCleanupError(err)
}

func normalizeCommandWALCheckpointCleanupError(err error) error {
	if errors.Is(err, errDurableWALCleanupProofStale) || errors.Is(err, commitlog.ErrCommandWALCleanupSnapshotStale) {
		return errors.Join(ErrDurableWALCleanupProofStale, err)
	}
	if errors.Is(err, errDurableWALCleanupProofUnavailable) {
		return nil
	}
	return err
}

func retainCommandWALCleanupAuthoritySegments(decisions []commandWALSegmentCleanupDecision, captured, current commitlog.CommandJournalCleanupSnapshot) error {
	for i := range decisions {
		decision := &decisions[i]
		if !decision.Covered {
			continue
		}
		if !decision.generationKnown || decision.lane < 0 {
			return errors.Join(ErrRecoveryRequired, fmt.Errorf("command WAL cleanup candidate has invalid generation: %s", filepath.Base(decision.Path)))
		}
		if rootpublication.SamePhysicalIdentity(decision.identity, captured.ActiveIdentity) ||
			rootpublication.SamePhysicalIdentity(decision.identity, current.ActiveIdentity) {
			decision.Active = true
			continue
		}
		if decision.lane == captured.Lane && decision.seq >= captured.SegmentSeq {
			decision.Active = true
		}
	}
	return nil
}

// cleanupCommandWALCoveredSegmentsV1 is the allow-closing cleanup path used by
// DB.Close after writers and root publication have drained but before the
// journal owner is detached. External callers must use the admission-checked
// CleanupCommandWALCoveredSegments method above.
func (db *DB) cleanupCommandWALCoveredSegmentsV1() (retErr error) {
	db.commandWALCleanupMu.Lock()
	defer db.commandWALCleanupMu.Unlock()
	cleanupStart := time.Now()
	defer func() {
		if elapsed := commandWALDurationNs(time.Since(cleanupStart)); elapsed > 0 {
			db.commandWALCleanupNs.Add(elapsed)
		}
		if retErr != nil {
			db.commandWALCleanupRetries.Add(1)
		}
	}()
	if db.readOnly {
		return ErrReadOnly
	}
	if db.commandJournal == nil {
		return fmt.Errorf("%w: command journal owner is not live", errDurableWALCleanupProofUnavailable)
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	proofStart := time.Now()
	proof, err := db.captureDurableWALCleanupProofV1()
	db.commandWALCleanupProofs.Add(1)
	if proofNs := commandWALDurationNs(time.Since(proofStart)); proofNs > 0 {
		db.commandWALCleanupProofNs.Add(proofNs)
	}
	if err != nil {
		return err
	}
	db.commandWALCleanupProofFrontier.Store(proof.cleanupThrough)
	db.commandWALCleanupProofDurableLSN.Store(proof.durableWALLSN)
	db.commandWALCleanupSelectedRoot.Store(proof.selectedCommitSeq())
	db.commandWALCleanupOlderRoot.Store(proof.olderCommitSeq())
	db.commandWALCleanupCaptureEpoch.Store(proof.journal.CleanupEpoch)
	db.commandWALCleanupNamespaceGen.Store(proof.journal.NamespaceGeneration)
	if proof.cleanupThrough == 0 {
		return db.syncCommandWALCleanupNamespaceIfDirty()
	}
	scanStart := time.Now()
	decisions, err := scanCommandWALSegmentsForCleanupProof(db.dir, proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes)
	proof.segments = decisions
	db.commandWALCleanupScans.Add(1)
	if scanNs := commandWALDurationNs(time.Since(scanStart)); scanNs > 0 {
		db.commandWALCleanupScanNs.Add(scanNs)
	}
	defer closeCommandWALCleanupDecisions(decisions)
	if errors.Is(err, ErrCommandWALAppliedLSNNonContig) || errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		// A missing or duplicate frame in the retained replay interval means the
		// captured roots/WAL lineage cannot authorize deletion. Nothing has been
		// unlinked yet, so classify this as unavailable proof: explicit cleanup
		// still receives the exact lineage error, while checkpoint/close retain
		// the namespace and allow a later recovery baseline to converge.
		err = errors.Join(errDurableWALCleanupProofUnavailable, err)
	}
	if err == nil && db.testCommandWALCleanupAfterScanHook != nil {
		db.testCommandWALCleanupAfterScanHook()
	}
	if err == nil {
		db.durablePublishMu.Lock()
		if err = db.revalidateDurableWALCleanupProofV1(proof); err == nil {
			err = proof.journalOwner.WithCleanupSnapshot(proof.journal, func(registry *rootpublication.IdentityPinRegistry, current commitlog.CommandJournalCleanupSnapshot) (bool, error) {
				if poisonErr := db.commandWALPoisonedError(); poisonErr != nil {
					return false, poisonErr
				}
				if retainErr := retainCommandWALCleanupAuthoritySegments(proof.segments, proof.journal, current); retainErr != nil {
					return false, retainErr
				}
				var removeErr error
				proof.segments, removeErr = removeCoveredCommandWALSegmentsWithRegistry(proof.segments, registry)
				for i := range proof.segments {
					if proof.segments[i].Removed {
						return true, removeErr
					}
				}
				return false, removeErr
			})
		}
		db.durablePublishMu.Unlock()
	}
	decisions = proof.segments
	removed := uint64(0)
	removedBytes := uint64(0)
	scannedBytes := uint64(0)
	scannedFrames := uint64(0)
	covered := uint64(0)
	coveredBytes := uint64(0)
	retained := uint64(0)
	retainedBytes := uint64(0)
	retainedActive := uint64(0)
	retainedUncovered := uint64(0)
	retainedPinned := uint64(0)
	retainedError := uint64(0)
	oldestPinnedLSN := uint64(0)
	for _, decision := range decisions {
		if decision.ScannedBytes > 0 {
			scannedBytes += uint64(decision.ScannedBytes)
		}
		scannedFrames += decision.Frames
		if decision.Covered {
			covered++
			if decision.Size > 0 {
				coveredBytes += uint64(decision.Size)
			}
		}
		if !decision.Removed {
			retained++
			if decision.Size > 0 {
				retainedBytes += uint64(decision.Size)
			}
			if decision.Active {
				retainedActive++
			}
			if !decision.Covered {
				retainedUncovered++
			}
			if decision.Pinned {
				retainedPinned++
			}
			if decision.Error != "" || (err != nil && decision.Covered && !decision.Active && !decision.Pinned) {
				retainedError++
			}
			if (decision.Active || decision.Pinned) && decision.MinLSN != 0 && (oldestPinnedLSN == 0 || decision.MinLSN < oldestPinnedLSN) {
				oldestPinnedLSN = decision.MinLSN
			}
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
	if covered > 0 {
		db.commandWALCleanupCovered.Add(covered)
		db.commandWALCleanupCoveredBytes.Add(coveredBytes)
	}
	if retained > 0 {
		db.commandWALCleanupRetained.Add(retained)
		db.commandWALCleanupRetainedBytes.Add(retainedBytes)
		db.commandWALCleanupRetainedActive.Add(retainedActive)
		db.commandWALCleanupRetainedUncover.Add(retainedUncovered)
		db.commandWALCleanupRetainedPinned.Add(retainedPinned)
		db.commandWALCleanupRetainedError.Add(retainedError)
	}
	db.commandWALCleanupOldestPinnedLSN.Store(oldestPinnedLSN)
	if removed > 0 {
		db.commandWALCleanupUnlinkedPending.Add(removed)
		db.commandWALCleanupBytesPending.Add(removedBytes)
		db.commandWALCleanupNamespaceDirty.Store(true)
		if removedBytes > 0 {
			db.commandWALClosedBytes.Add(-int64(removedBytes))
		}
		if err != nil {
			// At least one unlink completed, but cleanup stopped before the
			// deletion directory could be proven durable.
			return errors.Join(err, ErrRecoveryRequired)
		}
	}
	if syncErr := db.syncCommandWALCleanupNamespaceIfDirty(); syncErr != nil {
		return syncErr
	}
	return err
}

func (db *DB) syncCommandWALCleanupNamespaceIfDirty() error {
	if db == nil || !db.commandWALCleanupNamespaceDirty.Load() {
		return nil
	}
	db.commandWALCleanupNamespaceSyncs.Add(1)
	if err := durabilitycut.EmitPath(durabilitycut.BeforeDeletionDirectorySync, durabilitycut.ResourceCommandWAL, db.dir, WALDirPath(db.dir)); err != nil {
		db.commandWALCleanupNamespaceErrors.Add(1)
		return errors.Join(err, ErrRecoveryRequired)
	}
	if err := syncDirFn(WALDirPath(db.dir)); err != nil {
		db.commandWALCleanupNamespaceErrors.Add(1)
		return errors.Join(err, ErrRecoveryRequired)
	}
	if err := durabilitycut.EmitPath(durabilitycut.AfterDeletionDirectorySync, durabilitycut.ResourceCommandWAL, db.dir, WALDirPath(db.dir)); err != nil {
		db.commandWALCleanupNamespaceErrors.Add(1)
		return errors.Join(err, ErrRecoveryRequired)
	}
	db.commandWALCleanupNamespaceDirty.Store(false)
	if removed := db.commandWALCleanupUnlinkedPending.Swap(0); removed > 0 {
		db.commandWALCleanupRemoved.Add(removed)
	}
	if removedBytes := db.commandWALCleanupBytesPending.Swap(0); removedBytes > 0 {
		db.commandWALCleanupBytes.Add(removedBytes)
	}
	return nil
}

func (db *DB) closeCommandWALDurablePrefixThrough(lsn uint64) {
	if db == nil || lsn == 0 {
		return
	}
	commandWALStoreMax(&db.commandWALDurableLSN, lsn)
	db.commandWALDebt.releaseThrough(lsn)
}

func (db *DB) syncCommandWALDependenciesThrough(lsn uint64, extra *rootpublication.StableResourceSet) error {
	if db == nil || lsn == 0 {
		return nil
	}
	if extra == nil && !db.commandWALDebt.hasPhysicalDependenciesThrough(lsn) {
		return nil
	}
	var (
		view *rootpublication.StableResourceSet
		err  error
	)
	if extra == nil {
		view, err = db.commandWALDebt.resourceViewThrough(lsn)
	} else {
		view, err = db.commandWALDebt.resourceViewThrough(lsn, extra)
	}
	if err != nil {
		return err
	}
	if _, err := syncStableResourceDependenciesV1(view, db.dir, view.SyncThrough, func() {
		db.commandWALDebt.noteRetryThrough(lsn)
	}); err != nil {
		return err
	}
	if err := db.commandWALDebt.syncRotationFilesThrough(db.dir, db.commandWALDir, lsn); err != nil {
		db.commandWALDebt.noteRetryThrough(lsn)
		return err
	}
	if err := stabilizeCommandWALResourceNamespaces(view); err != nil {
		db.commandWALDebt.noteRetryThrough(lsn)
		return err
	}
	if err := db.commandWALDebt.stabilizeRotationNamespacesThrough(db.dir, db.commandWALDir, lsn); err != nil {
		db.commandWALDebt.noteRetryThrough(lsn)
		return err
	}
	return nil
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
		syncOnPublish:    db.resolvedProfile == ProfileCommandWALDurable,
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

func (db *DB) prepareRawKVCommandWALIntent(b *Batch, sync bool) (*commandWALBatchIntent, error) {
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
	return db.newRawKVCommandWALIntentFromEntries(entries, sync)
}

// NewRawKVCommandWALIntentFromOrderedEntries builds a public raw-KV command
// intent from entries that are already in the caller's required application
// order. Unlike prepareRawKVCommandWALIntent, this does not sort or compact
// point ops; public cached batches rely on replay order to preserve mixed
// set/delete/range-delete semantics. Because a reusable intent does not declare
// its eventual durability boundary, this constructor conservatively encodes
// pointer entries as SetRID. One-shot append APIs with an explicit append mode
// own bounded SetMaterializedRID selection.
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
	intent, err := db.newRawKVCommandWALPayloadIntentFromEntries(entries, false)
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
	mode := RawKVCommandWALAppendRelaxed
	if sync {
		mode = RawKVCommandWALAppendDurable
	}
	return db.AppendRawKVCommandWALOrderedEntriesWithMode(entries, mode)
}

// AppendRawKVCommandWALOrderedEntriesWithMode is the durability-explicit form
// of AppendRawKVCommandWALOrderedEntries. A durable-prefix participant is
// appended relaxed and is safe to acknowledge only after its caller establishes
// a covering durable-prefix barrier.
func (db *DB) AppendRawKVCommandWALOrderedEntriesWithMode(entries []batchpkg.Entry, mode RawKVCommandWALAppendMode) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if !mode.valid() {
		return 0, fmt.Errorf("%w: invalid raw kv command wal append mode %d", ErrCommandWALUnsupported, mode)
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if len(entries) == 0 {
		return 0, nil
	}
	intent, err := db.newRawKVCommandWALIntentFromEntries(entries, mode.allowsMaterializedRID())
	if intent == nil || err != nil {
		return 0, err
	}
	publicIntent := &CommandWALIntent{inner: *intent}
	defer releaseUnassignedCommandWALIntent(&publicIntent.inner)
	return db.AppendCommandWALIntent(publicIntent, mode.sync())
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
	mode := RawKVCommandWALAppendRelaxed
	if sync {
		mode = RawKVCommandWALAppendDurable
	}
	return db.AppendRawKVCommandWALOrderedEntryScanWithHintAndMode(scanEntries, opHint, mode)
}

// AppendRawKVCommandWALOrderedEntryScanWithHintAndMode is the
// durability-explicit ordered-scan append variant.
func (db *DB) AppendRawKVCommandWALOrderedEntryScanWithHintAndMode(scanEntries func(func(batchpkg.Entry) error) error, opHint int, mode RawKVCommandWALAppendMode) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if !mode.valid() {
		return 0, fmt.Errorf("%w: invalid raw kv command wal append mode %d", ErrCommandWALUnsupported, mode)
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if scanEntries == nil {
		return 0, nil
	}
	intent, err := db.newRawKVCommandWALIntentFromEntryScanWithHint(scanEntries, opHint, mode.allowsMaterializedRID())
	if intent == nil || err != nil {
		return 0, err
	}
	publicIntent := &CommandWALIntent{inner: *intent}
	defer releaseUnassignedCommandWALIntent(&publicIntent.inner)
	return db.AppendCommandWALIntent(publicIntent, mode.sync())
}

// AppendRawKVCommandWALOrderedEntryScanWithHintMeasured is the opt-in
// diagnostic variant of AppendRawKVCommandWALOrderedEntryScanWithHint. It
// partitions intent planning, publish serialization/barriers, append,
// flush/sync, and post-append bookkeeping without changing their ordering.
func (db *DB) AppendRawKVCommandWALOrderedEntryScanWithHintMeasured(scanEntries func(func(batchpkg.Entry) error) error, opHint int, sync bool) (uint64, CommandWALRequestTiming, error) {
	mode := RawKVCommandWALAppendRelaxed
	if sync {
		mode = RawKVCommandWALAppendDurable
	}
	return db.AppendRawKVCommandWALOrderedEntryScanWithHintAndModeMeasured(scanEntries, opHint, mode)
}

// AppendRawKVCommandWALOrderedEntryScanWithHintAndModeMeasured is the
// durability-explicit diagnostic ordered-scan append variant.
func (db *DB) AppendRawKVCommandWALOrderedEntryScanWithHintAndModeMeasured(scanEntries func(func(batchpkg.Entry) error) error, opHint int, mode RawKVCommandWALAppendMode) (uint64, CommandWALRequestTiming, error) {
	var timing CommandWALRequestTiming
	if db == nil || !db.commandWAL {
		return 0, timing, nil
	}
	if !mode.valid() {
		return 0, timing, fmt.Errorf("%w: invalid raw kv command wal append mode %d", ErrCommandWALUnsupported, mode)
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
	planningStart := time.Now()
	timing.BackendIntentPlanningSerializationObserved = true
	intent, err := db.newRawKVCommandWALIntentFromEntryScanWithHint(scanEntries, opHint, mode.allowsMaterializedRID())
	timing.BackendIntentPlanningSerialization += time.Since(planningStart)
	if intent == nil || err != nil {
		return 0, timing, err
	}
	defer releaseUnassignedCommandWALIntent(intent)
	publishStart := time.Now()
	timing.PublishLockBarrierWaitObserved = true
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	timing.PublishLockBarrierWait += time.Since(publishStart)
	if err != nil {
		return 0, timing, err
	}
	defer unlockCommandWALPublish()
	lsn, err := db.appendCommandWALIntentWithTiming(intent, mode.sync(), &timing)
	return lsn, timing, err
}

// AppendRawKVCommandWALOrderedEntryScanWithHintPrepared assigns any caller
// metadata while holding the barrier-aware publish lock, then plans and appends
// the ordered entry scan under that same serialization boundary.
func (db *DB) AppendRawKVCommandWALOrderedEntryScanWithHintPrepared(prepare func() error, scanEntries func(func(batchpkg.Entry) error) error, opHint int, sync bool) (uint64, error) {
	mode := RawKVCommandWALAppendRelaxed
	if sync {
		mode = RawKVCommandWALAppendDurable
	}
	lsn, _, err := db.appendRawKVCommandWALOrderedEntryScanWithHintPrepared(prepare, scanEntries, opHint, mode, false)
	return lsn, err
}

// AppendRawKVCommandWALOrderedEntryScanWithHintPreparedAndMode is the
// durability-explicit prepared ordered-scan append variant.
func (db *DB) AppendRawKVCommandWALOrderedEntryScanWithHintPreparedAndMode(prepare func() error, scanEntries func(func(batchpkg.Entry) error) error, opHint int, mode RawKVCommandWALAppendMode) (uint64, error) {
	lsn, _, err := db.appendRawKVCommandWALOrderedEntryScanWithHintPrepared(prepare, scanEntries, opHint, mode, false)
	return lsn, err
}

// AppendRawKVCommandWALOrderedEntryScanWithHintPreparedMeasured is the
// diagnostic counterpart to the prepared ordered-entry scan append.
func (db *DB) AppendRawKVCommandWALOrderedEntryScanWithHintPreparedMeasured(prepare func() error, scanEntries func(func(batchpkg.Entry) error) error, opHint int, sync bool) (uint64, CommandWALRequestTiming, error) {
	mode := RawKVCommandWALAppendRelaxed
	if sync {
		mode = RawKVCommandWALAppendDurable
	}
	return db.appendRawKVCommandWALOrderedEntryScanWithHintPrepared(prepare, scanEntries, opHint, mode, true)
}

// AppendRawKVCommandWALOrderedEntryScanWithHintPreparedAndModeMeasured is the
// durability-explicit diagnostic prepared ordered-scan append variant.
func (db *DB) AppendRawKVCommandWALOrderedEntryScanWithHintPreparedAndModeMeasured(prepare func() error, scanEntries func(func(batchpkg.Entry) error) error, opHint int, mode RawKVCommandWALAppendMode) (uint64, CommandWALRequestTiming, error) {
	return db.appendRawKVCommandWALOrderedEntryScanWithHintPrepared(prepare, scanEntries, opHint, mode, true)
}

func (db *DB) appendRawKVCommandWALOrderedEntryScanWithHintPrepared(prepare func() error, scanEntries func(func(batchpkg.Entry) error) error, opHint int, mode RawKVCommandWALAppendMode, measured bool) (uint64, CommandWALRequestTiming, error) {
	var timing CommandWALRequestTiming
	if db == nil || !db.commandWAL {
		return 0, timing, nil
	}
	if !mode.valid() {
		return 0, timing, fmt.Errorf("%w: invalid raw kv command wal append mode %d", ErrCommandWALUnsupported, mode)
	}
	if db.readOnly {
		return 0, timing, ErrReadOnly
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, timing, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if prepare == nil || scanEntries == nil {
		return 0, timing, nil
	}
	publishStart := time.Time{}
	if measured {
		publishStart = time.Now()
		timing.PublishLockBarrierWaitObserved = true
	}
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		if measured {
			timing.PublishLockBarrierWait += time.Since(publishStart)
		}
		return 0, timing, err
	}
	defer unlockCommandWALPublish()
	if measured {
		timing.PublishLockBarrierWait += time.Since(publishStart)
	}
	if db.closing.Load() {
		return 0, timing, ErrClosed
	}
	if db.commandJournal == nil {
		return 0, timing, db.commandWALJournalUnavailableError()
	}
	if db.commandWALFlushPoisoned.Load() {
		return 0, timing, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
	}
	planningStart := time.Time{}
	if measured {
		planningStart = time.Now()
		timing.BackendIntentPlanningSerializationObserved = true
	}
	if err := prepare(); err != nil {
		if measured {
			timing.BackendIntentPlanningSerialization += time.Since(planningStart)
		}
		return 0, timing, err
	}
	intent, err := db.newRawKVCommandWALIntentFromEntryScanWithHint(scanEntries, opHint, mode.allowsMaterializedRID())
	if measured {
		timing.BackendIntentPlanningSerialization += time.Since(planningStart)
	}
	if intent == nil || err != nil {
		return 0, timing, err
	}
	defer releaseUnassignedCommandWALIntent(intent)
	lsn, err := db.appendCommandWALIntentWithTiming(intent, mode.sync(), func() *CommandWALRequestTiming {
		if measured {
			return &timing
		}
		return nil
	}())
	return lsn, timing, err
}

func (db *DB) newRawKVCommandWALIntentFromEntries(entries []batchpkg.Entry, allowMaterializedRID bool) (*commandWALBatchIntent, error) {
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
	}, len(entries), allowMaterializedRID)
}

func (db *DB) newRawKVCommandWALIntentFromEntryScan(scanEntries func(func(batchpkg.Entry) error) error, allowMaterializedRID bool) (*commandWALBatchIntent, error) {
	return db.newRawKVCommandWALIntentFromEntryScanWithHint(scanEntries, 0, allowMaterializedRID)
}

func (db *DB) newRawKVCommandWALIntentFromEntryScanWithHint(scanEntries func(func(batchpkg.Entry) error) error, opHint int, allowMaterializedRID bool) (_ *commandWALBatchIntent, err error) {
	materialize := allowMaterializedRID
	externalRefs := false
	var ridCache *rawKVCommandWALRIDCache
	var externalRefFileIDs []uint32
	maxEntryRevision := page.LegacyEntryRevision
	defer func() {
		if err != nil && ridCache != nil {
			ridCache.release()
		}
	}()
	materializedRefs := false
	materializedEligible := true
	operations := 0
	planScan := commitlog.RawKVBatchOperationScanner(func(emit func(commitlog.RawKVOperation) error) error {
		if scanEntries == nil {
			return nil
		}
		return scanEntries(func(entry batchpkg.Entry) error {
			materializeEntry := materialize
			if materialize {
				operations++
				if operations > RawKVCommandWALMaterializedRIDMaxOperations {
					materializedEligible = false
				}
				if entry.Type == batchpkg.OpPut && entry.IsPtr && entry.Value != nil {
					materializedRefs = true
					if len(entry.Value) == 0 || len(entry.Value) > RawKVCommandWALMaterializedRIDMaxValueBytes {
						materializedEligible = false
					}
				}
				materializeEntry = materializedEligible
			}
			if entry.Type != batchpkg.OpDeleteRange && entry.Revision > maxEntryRevision {
				maxEntryRevision = entry.Revision
			}
			validateRetainedValue := !materialize || materializeEntry
			op, ok, err := db.rawKVCommandWALOperationFromEntry(entry, &ridCache, &externalRefs, &externalRefFileIDs, opHint, materializeEntry, validateRetainedValue)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			return emit(op)
		})
	})
	plan, err := commitlog.PlanRawKVBatchPayloadScan(planScan)
	if err != nil {
		return nil, err
	}
	if materialize && !materializedRefs {
		// The optimistic scan already produced the exact V1 plan: without a
		// retained pointer value, materialization changes no operation.
		materialize = false
	} else if materialize && (!materializedEligible || !db.rawKVCommandWALMaterializedRIDPlanFits(plan)) {
		materialize = false
		externalRefs = false
		externalRefFileIDs = externalRefFileIDs[:0]
		fallbackScan := db.rawKVCommandWALOperationScannerFromEntryScan(scanEntries, &ridCache, &externalRefs, &externalRefFileIDs, opHint, false, false)
		plan, err = commitlog.PlanRawKVBatchPayloadScan(fallbackScan)
		if err != nil {
			return nil, err
		}
	}
	if plan.Count == 0 {
		return nil, nil
	}
	// The successful planning scan already validated every retained pointer
	// value while the caller-owned entry buffers were immutable.
	writeScan := db.rawKVCommandWALOperationScannerFromEntryScan(scanEntries, &ridCache, nil, nil, opHint, materialize, false)
	payloadFormat := commitlog.PayloadFormatRawKVBatchV1
	if materialize {
		payloadFormat = commitlog.PayloadFormatRawKVBatchV2
	}
	return &commandWALBatchIntent{
		kind:               commitlog.CommandKindRawKVBatch,
		scope:              commitlog.CommandScopeRawKV,
		payloadFormat:      payloadFormat,
		rawKVScan:          writeScan,
		rawKVPlan:          plan,
		rawKVRIDCache:      ridCache,
		rawKVDirect:        true,
		externalRefs:       externalRefs,
		externalRefFileIDs: externalRefFileIDs,
		maxEntryRevision:   maxEntryRevision,
	}, nil
}

func (db *DB) newRawKVCommandWALPayloadIntentFromEntries(entries []batchpkg.Entry, allowMaterializedRID bool) (*commandWALBatchIntent, error) {
	intent, err := db.newRawKVCommandWALIntentFromEntries(entries, allowMaterializedRID)
	if err != nil || intent == nil {
		return intent, err
	}
	payload, err := commitlog.EncodeRawKVBatchPayloadPlanned(intent.rawKVPlan, intent.rawKVScan)
	if err != nil {
		if intent.rawKVRIDCache != nil {
			intent.rawKVRIDCache.release()
		}
		return nil, err
	}
	intent.payload = payload
	intent.rawKVScan = nil
	intent.rawKVDirect = false
	return intent, nil
}

func (db *DB) rawKVCommandWALMaterializedRIDPlanFits(plan commitlog.RawKVBatchPayloadPlan) bool {
	if plan.Count <= 0 || plan.Count > RawKVCommandWALMaterializedRIDMaxOperations {
		return false
	}
	maxFrame := int64(RawKVCommandWALMaterializedRIDMaxFrameBytes)
	if db != nil && db.walMaxSegmentBytes > 0 && db.walMaxSegmentBytes < maxFrame {
		maxFrame = db.walMaxSegmentBytes
	}
	maxPayload := maxFrame - RawKVCommandWALMaterializedRIDFrameReserve
	return maxPayload >= 0 && plan.PayloadLen >= 0 && int64(plan.PayloadLen) <= maxPayload
}

func stableExternalRIDSegmentDigest(fileID uint32) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("command-wal-v2/external-rid/value-log-segment"))
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], fileID)
	_, _ = h.Write(encoded[:])
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func (db *DB) captureCommandWALExternalDependencies(intent *commandWALBatchIntent) (*rootpublication.StableResourceSet, error) {
	if intent == nil {
		return nil, nil
	}
	if intent.dependencyResources != nil {
		return intent.dependencyResources, nil
	}
	if intent.kind != commitlog.CommandKindRawKVBatch {
		return nil, nil
	}
	fence, err := commitlog.ExternalRefFenceV1FromRawKVPayload(intent.payload)
	if err != nil {
		return nil, err
	}
	if fence.Count == 0 {
		return nil, nil
	}
	if db == nil || db.valueLogManager == nil || intent.rawKVRIDCache == nil {
		return nil, fmt.Errorf("%w: command WAL external-RID payload has no exact producer closure", rootpublication.ErrUnresolvedResource)
	}
	entries := intent.rawKVRIDCache.snapshot()
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: command WAL external-RID payload has an empty producer closure", rootpublication.ErrUnresolvedResource)
	}
	if intent.payloadFormat == commitlog.PayloadFormatRawKVBatchV2 {
		// A V2 intent may also cache self-contained materialized RIDs. Keep
		// those out of the external dependency closure. V1 is all-SetRID here,
		// so retaining its lower-allocation snapshot path avoids regressing the
		// conservative fallback.
		operations, err := commitlog.DecodeRawKVBatchPayload(intent.payload)
		if err != nil {
			return nil, err
		}
		requiredRIDs := make(map[uint64]struct{}, fence.Count)
		for i := range operations {
			if operations[i].Op == commitlog.RawKVOpSetRID {
				requiredRIDs[operations[i].RID] = struct{}{}
			}
		}
		filtered := entries[:0]
		for i := range entries {
			if _, ok := requiredRIDs[entries[i].rid]; ok {
				filtered = append(filtered, entries[i])
			}
		}
		entries = filtered
		if len(entries) == 0 {
			return nil, fmt.Errorf("%w: command WAL external-RID payload has no matching producer closure", rootpublication.ErrUnresolvedResource)
		}
	}
	fileIDs := make([]uint32, 0)
	for _, entry := range entries {
		appendRawKVCommandWALExternalRefFileID(&fileIDs, entry.ptr.FileID)
	}
	if err := db.flushCommandWALExternalRefs(fileIDs); err != nil {
		return nil, err
	}
	segments := make([]valuelog.StableExternalRIDSegment, 0, len(fileIDs))
	for _, entry := range entries {
		if len(segments) == 0 || segments[len(segments)-1].FileID != entry.ptr.FileID {
			segments = append(segments, valuelog.StableExternalRIDSegment{
				FileID: entry.ptr.FileID,
				Digest: stableExternalRIDSegmentDigest(entry.ptr.FileID),
			})
		}
		segment := &segments[len(segments)-1]
		segment.RIDs = append(segment.RIDs, entry.rid)
		segment.Pointers = append(segment.Pointers, entry.ptr)
	}
	resources, err := db.valueLogManager.CaptureStableExternalRIDFence(valuelog.StableExternalRIDFence{
		Count: fence.Count, Digest: fence.Digest,
	}, segments)
	if err != nil {
		return nil, err
	}
	if err := resources.FlushThrough(); err != nil {
		resources.Release()
		return nil, err
	}
	intent.dependencyResources = resources
	intent.rawKVRIDCache.release()
	return resources, nil
}

// releaseUnassignedCommandWALIntent abandons resources captured by an internal
// one-shot intent after an error returned before any command LSN was assigned.
// Reusable public intents deliberately do not call this helper: they retain the
// same exact handles so a caller can retry the pre-WAL durability boundary.
func releaseUnassignedCommandWALIntent(intent *commandWALBatchIntent) {
	if intent == nil || intent.lsn != 0 {
		return
	}
	if intent.dependencyResources != nil {
		intent.dependencyResources.Release()
		intent.dependencyResources = nil
	}
	if intent.rawKVRIDCache != nil {
		intent.rawKVRIDCache.release()
		intent.rawKVRIDCache = nil
	}
}

func commandWALStableRotationTokens(rotations []*commitlog.CommandJournalStableRotation) []*rootpublication.StableResourceToken {
	if len(rotations) == 0 {
		return nil
	}
	tokens := make([]*rootpublication.StableResourceToken, 0, 2*len(rotations))
	for _, rotation := range rotations {
		for _, token := range []*rootpublication.StableResourceToken{rotation.TakeClosed(), rotation.TakeActive()} {
			if token != nil {
				tokens = append(tokens, token)
			}
		}
		rotation.Release()
	}
	return tokens
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
	if kind != commitlog.CommandKindRawKVBatch || scope != commitlog.CommandScopeRawKV ||
		(payloadFormat != commitlog.PayloadFormatRawKVBatchV1 && payloadFormat != commitlog.PayloadFormatRawKVBatchV2) {
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
	return db.rawKVCommandWALOperationScannerWithHint(entries, ridCache, externalRefs, len(entries), false)
}

func (db *DB) rawKVCommandWALOperationScannerWithHint(entries []batchpkg.Entry, ridCache **rawKVCommandWALRIDCache, externalRefs *bool, opHint int, materialize bool) commitlog.RawKVBatchOperationScanner {
	return db.rawKVCommandWALOperationScannerFromEntryScan(func(emit func(batchpkg.Entry) error) error {
		for i := range entries {
			if err := emit(entries[i]); err != nil {
				return err
			}
		}
		return nil
	}, ridCache, externalRefs, nil, opHint, materialize, true)
}

func (db *DB) rawKVCommandWALOperationScannerFromEntryScan(scanEntries func(func(batchpkg.Entry) error) error, ridCache **rawKVCommandWALRIDCache, externalRefs *bool, externalRefFileIDs *[]uint32, opHint int, materialize, validateRetainedValues bool) commitlog.RawKVBatchOperationScanner {
	return func(emit func(commitlog.RawKVOperation) error) error {
		if scanEntries == nil {
			return nil
		}
		return scanEntries(func(entry batchpkg.Entry) error {
			op, ok, err := db.rawKVCommandWALOperationFromEntry(entry, ridCache, externalRefs, externalRefFileIDs, opHint, materialize, validateRetainedValues)
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

func (db *DB) rawKVCommandWALOperationFromEntry(entry batchpkg.Entry, ridCache **rawKVCommandWALRIDCache, externalRefs *bool, externalRefFileIDs *[]uint32, opHint int, materializePointers, validateRetainedValues bool) (commitlog.RawKVOperation, bool, error) {
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
			hasRetainedValue := entry.Value != nil
			materialized := materializePointers && hasRetainedValue
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
			if hasRetainedValue && validateRetainedValues {
				persisted, err := db.valueLogManager.Read(entry.ValuePtr)
				if isCommandWALRIDLookupVisibilityError(err) {
					if flushErr := db.flushCommandWALExternalRefs([]uint32{entry.ValuePtr.FileID}); flushErr != nil {
						return commitlog.RawKVOperation{}, false, flushErr
					}
					persisted, err = db.valueLogManager.Read(entry.ValuePtr)
				}
				if err != nil {
					return commitlog.RawKVOperation{}, false, err
				}
				if !bytes.Equal(persisted, entry.Value) {
					return commitlog.RawKVOperation{}, false, fmt.Errorf("%w: rid=%d", ErrCommandWALConflictingValueLogRID, rid)
				}
			}
			if materialized {
				return commitlog.RawKVOperation{
					Op:       commitlog.RawKVOpSetMaterializedRID,
					Key:      entry.Key,
					Value:    entry.Value,
					RID:      rid,
					Revision: uint64(entry.Revision),
				}, true, nil
			}
			if externalRefs != nil {
				*externalRefs = true
			}
			if externalRefFileIDs != nil {
				appendRawKVCommandWALExternalRefFileID(externalRefFileIDs, entry.ValuePtr.FileID)
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
		if entry.Type != batchpkg.OpPut || !entry.IsPtr || entry.Value != nil || entry.ValuePtr.FileID == 0 {
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
		if flushErr := db.flushCommandWALExternalRefs([]uint32{ptr.FileID}); flushErr != nil {
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

// flushCommandWALExternalRefs makes producer bytes visible before exact stable
// handles are captured. Durability is performed later through those pinned
// handles; this helper must never reopen a dependency by pathname.
func (db *DB) flushCommandWALExternalRefs(fileIDs []uint32) error {
	appender := db.currentValueLogAppender()
	if appender == nil {
		return ErrValueLogAppenderUnavailable
	}
	if len(fileIDs) > 0 {
		if flusher, ok := appender.(ValueLogExternalRefFlusher); ok {
			if err := flusher.FlushValueLogExternalRefs(fileIDs, false); err != nil {
				return err
			}
			return nil
		}
	}
	return appender.Flush()
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
			// An already-appended relaxed foreground frame may later be published
			// through an explicit sync API. Its durability class is immutable, so close
			// the prefix through a durable barrier and publish that barrier as part of
			// the same contiguous applied-command range. The intent keeps its original
			// LSN as the mutation identity returned to its caller.
			if sync && db.commandWALDurableLSN.Load() < intent.inner.lsn {
				barrierLSN, err := db.appendCommandWALDurablePrefixBarrier()
				if err != nil {
					return intent.inner.lsn, err
				}
				if intent.inner.coveredRange[0].First == 0 {
					intent.inner.coveredRange[0].First = intent.inner.lsn
				}
				intent.inner.coveredRange[0].Last = barrierLSN
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

// commandWALIntentNeedsPublicAppendLock reports whether appendPublicCommandWALIntent
// can append either the command frame itself or a durable-prefix barrier and
// therefore needs the barrier-aware raw publish lock. A staged intent inherits
// that lock from its caller. An already-appended relaxed coverage intent does
// not need the lock until a sync request would extend it with a barrier; its
// higher-level coordinator remains responsible for publication ordering.
func (db *DB) commandWALIntentNeedsPublicAppendLock(intent *CommandWALIntent, sync bool) bool {
	if intent == nil || intent.staged() {
		return false
	}
	if !commandWALIntentFrameAlreadyAppended(intent) {
		return true
	}
	return !intent.inner.fromReplay && sync && db.commandWALDurableLSN.Load() < intent.inner.lsn
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
	sync = commandWALIntentPublishSync(intent, sync)
	if !db.commandWALIntentNeedsPublicAppendLock(intent, sync) {
		return db.appendPublicCommandWALIntent(intent, sync)
	}
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return intent.AssignedLSN(), err
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
	sync = commandWALIntentPublishSync(intent, sync)
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
// It flushes while holding the command-journal lock; sync=true always opts up
// to the durable-prefix protocol. If that post-append flush fails, the returned
// LSN is still the allocated LSN; callers must record the command as pending
// and treat subsequent command-WAL appends as recovery-required until reopen.
func (db *DB) AppendRawKVSingleCommandWAL(op commitlog.RawKVOperation, sync bool) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return 0, err
	}
	defer unlockCommandWALPublish()
	if op.Op == commitlog.RawKVOpSetRID || op.Op == commitlog.RawKVOpSetMaterializedRID {
		return 0, fmt.Errorf("%w: public single-op command WAL cannot carry RID-bearing operations", ErrCommandWALUnsupported)
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
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{op})
	if err != nil {
		return 0, err
	}
	return db.appendCommandWALIntent(&commandWALBatchIntent{
		kind: commitlog.CommandKindRawKVBatch, scope: commitlog.CommandScopeRawKV,
		payloadFormat: commitlog.PayloadFormatRawKVBatchV1, payload: payload,
		statsPath: commandWALAppendStatsPoint, statsPathSet: true,
	}, sync)
}

// AppendRawKVPointCommandWALTrusted appends a caller-validated public raw KV
// point Set/Delete command. It is intended for public cached command-WAL writes
// after cached preflight has validated the user input and before visibility.
// It flushes while holding the command-journal lock; sync=true always opts up
// to the durable-prefix protocol. If that post-append flush fails, the returned
// LSN is still the allocated LSN; callers must record the command as pending
// and treat subsequent command-WAL appends as recovery-required until reopen.
func (db *DB) AppendRawKVPointCommandWALTrusted(op commitlog.RawKVOp, key, value []byte, sync bool) (uint64, error) {
	return db.appendRawKVPointCommandWALTrustedWithRevision(op, key, value, 0, sync)
}

// AppendRawKVPointCommandWALTrustedWithRevision appends a caller-validated
// public raw-KV point command with native entry revision metadata.
func (db *DB) AppendRawKVPointCommandWALTrustedWithRevision(op commitlog.RawKVOp, key, value []byte, revision page.EntryRevision, sync bool) (uint64, error) {
	return db.appendRawKVPointCommandWALTrustedWithRevision(op, key, value, uint64(revision), sync)
}

// AppendRawKVPointCommandWALTrustedWithPreparedRevision assigns the cached
// entry revision while holding the same barrier-aware publish lock that orders
// command-WAL frames. The assigned revision is therefore ordered with the
// frame's LSN and may be reused by the caller for subsequent memtable publish.
func (db *DB) AppendRawKVPointCommandWALTrustedWithPreparedRevision(op commitlog.RawKVOp, key, value []byte, assignRevision func() page.EntryRevision, sync bool) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if op == commitlog.RawKVOpSetMaterializedRID {
		return 0, fmt.Errorf("%w: point command WAL cannot carry a materialized RID", ErrCommandWALUnsupported)
	}
	if assignRevision == nil {
		return 0, fmt.Errorf("%w: missing command wal revision allocator", ErrCommandWALRejected)
	}
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return 0, err
	}
	defer unlockCommandWALPublish()
	if db.closing.Load() {
		return 0, ErrClosed
	}
	if db.commandJournal == nil {
		return 0, db.commandWALJournalUnavailableError()
	}
	if db.commandWALFlushPoisoned.Load() {
		return 0, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
	}
	revision := assignRevision()
	if revision == page.LegacyEntryRevision {
		return 0, fmt.Errorf("%w: command wal revision allocator returned legacy revision", ErrCommandWALRejected)
	}
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op: op, Key: key, Value: value, Revision: uint64(revision),
	}})
	if err != nil {
		return 0, err
	}
	return db.appendCommandWALIntent(&commandWALBatchIntent{
		kind: commitlog.CommandKindRawKVBatch, scope: commitlog.CommandScopeRawKV,
		payloadFormat: commitlog.PayloadFormatRawKVBatchV1, payload: payload, trustedPayload: true,
		statsPath: commandWALAppendStatsPoint, statsPathSet: true,
	}, sync)
}

func (db *DB) appendRawKVPointCommandWALTrustedWithRevision(op commitlog.RawKVOp, key, value []byte, revision uint64, sync bool) (uint64, error) {
	if db == nil || !db.commandWAL {
		return 0, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if op == commitlog.RawKVOpSetMaterializedRID {
		return 0, fmt.Errorf("%w: point command WAL cannot carry a materialized RID", ErrCommandWALUnsupported)
	}
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return 0, err
	}
	defer unlockCommandWALPublish()
	if db.closing.Load() {
		return 0, ErrClosed
	}
	if db.commandJournal == nil {
		return 0, db.commandWALJournalUnavailableError()
	}
	if db.commandWALFlushPoisoned.Load() {
		return 0, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
	}
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: op, Key: key, Value: value, Revision: revision}})
	if err != nil {
		return 0, err
	}
	return db.appendCommandWALIntent(&commandWALBatchIntent{
		kind: commitlog.CommandKindRawKVBatch, scope: commitlog.CommandScopeRawKV,
		payloadFormat: commitlog.PayloadFormatRawKVBatchV1, payload: payload, trustedPayload: true,
		statsPath: commandWALAppendStatsPoint, statsPathSet: true,
	}, sync)
}

// AppendRawKVBatchPayloadCommandWAL appends a prebuilt RawKVBatch payload as a
// command frame. A sync request opts up to the durable-prefix protocol. If the
// post-append flush fails, the returned LSN is still the allocated LSN; callers
// must record the command as pending and treat subsequent command-WAL appends as
// recovery-required until the DB is reopened.
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
// variant of AppendRawKVBatchPayloadCommandWALTrusted. It partitions publish
// serialization/barriers, backend preparation, append, flush/sync, and
// post-append bookkeeping without changing write ordering.
func (db *DB) AppendRawKVBatchPayloadCommandWALTrustedMeasured(payload []byte, sync bool) (uint64, CommandWALRequestTiming, error) {
	return db.appendRawKVBatchPayloadCommandWAL(payload, sync, true, true)
}

func (db *DB) appendRawKVBatchPayloadCommandWAL(payload []byte, sync bool, trusted bool, measured bool) (uint64, CommandWALRequestTiming, error) {
	return db.appendRawKVBatchPayloadCommandWALPrepared(func() ([]byte, error) {
		return payload, nil
	}, sync, trusted, measured)
}

// AppendRawKVBatchPayloadCommandWALTrustedPrepared builds a trusted payload
// while holding the barrier-aware command-WAL publish lock, then appends that
// exact payload before releasing the lock.
func (db *DB) AppendRawKVBatchPayloadCommandWALTrustedPrepared(prepare func() ([]byte, error), sync bool) (uint64, error) {
	lsn, _, err := db.appendRawKVBatchPayloadCommandWALPrepared(prepare, sync, true, false)
	return lsn, err
}

// AppendRawKVBatchPayloadCommandWALTrustedPreparedMeasured is the diagnostic
// counterpart to AppendRawKVBatchPayloadCommandWALTrustedPrepared.
func (db *DB) AppendRawKVBatchPayloadCommandWALTrustedPreparedMeasured(prepare func() ([]byte, error), sync bool) (uint64, CommandWALRequestTiming, error) {
	return db.appendRawKVBatchPayloadCommandWALPrepared(prepare, sync, true, true)
}

func (db *DB) appendRawKVBatchPayloadCommandWALPrepared(prepare func() ([]byte, error), sync bool, trusted bool, measured bool) (uint64, CommandWALRequestTiming, error) {
	var requestTiming CommandWALRequestTiming
	if db == nil || !db.commandWAL {
		return 0, requestTiming, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return 0, requestTiming, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if prepare == nil {
		return 0, requestTiming, fmt.Errorf("%w: missing command wal payload preparation", ErrCommandWALRejected)
	}
	publishStart := time.Time{}
	if measured {
		publishStart = time.Now()
		requestTiming.PublishLockBarrierWaitObserved = true
	}
	unlockCommandWALPublish, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		if measured {
			requestTiming.PublishLockBarrierWait += time.Since(publishStart)
		}
		return 0, requestTiming, err
	}
	defer unlockCommandWALPublish()
	if measured {
		requestTiming.PublishLockBarrierWait += time.Since(publishStart)
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
	backendStart := time.Time{}
	if measured {
		backendStart = time.Now()
		requestTiming.BackendIntentPlanningSerializationObserved = true
	}
	payload, err := prepare()
	if err != nil {
		if measured {
			requestTiming.BackendIntentPlanningSerialization += time.Since(backendStart)
		}
		return 0, requestTiming, err
	}
	if measured {
		requestTiming.BackendIntentPlanningSerialization += time.Since(backendStart)
	}
	intent := &commandWALBatchIntent{
		kind: commitlog.CommandKindRawKVBatch, scope: commitlog.CommandScopeRawKV,
		payloadFormat: commitlog.PayloadFormatRawKVBatchV1, payload: payload, trustedPayload: trusted,
		statsPath: commandWALAppendStatsPayload, statsPathSet: true,
	}
	lsn, err := db.appendCommandWALIntentWithTiming(intent, sync, func() *CommandWALRequestTiming {
		if measured {
			return &requestTiming
		}
		return nil
	}())
	return lsn, requestTiming, err
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
// Callers must hold a higher-level raw publish or staging guard, including its
// teardown lease, from the frame append through this publish call.
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
	builder, err := db.acquireCommandWALPublicationBuilderV1()
	if err != nil {
		return err
	}
	if builder != nil {
		defer builder.Release()
	}
	durablePublishLocked := false
	releaseDurablePublish := func() {
		if durablePublishLocked {
			db.durablePublishMu.Unlock()
			durablePublishLocked = false
		}
	}
	defer releaseDurablePublish()
	db.commitMu.Lock()
	if _, err := db.appendPublicCommandWALIntent(intent, sync); err != nil {
		db.commitMu.Unlock()
		db.writeMu.Unlock()
		return err
	}
	if hook := db.testCommandWALBeforeDurablePublishLockHook; hook != nil {
		hook()
	}
	db.durablePublishMu.Lock()
	durablePublishLocked = true
	// The intent is root-neutral. Capture the roots only after entering the
	// durable publish gate so a publication that completed while this caller
	// waited cannot be rolled back when the applied LSN advances.
	db.mu.RLock()
	baseSeq := db.meta.CommitSeq
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	vlogRefDelta := db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
	db.commitMu.Unlock()
	db.writeMu.Unlock()
	publishPrepareGuard, err := db.prepareFlushApplyPublish(sync)
	if err != nil {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		db.poisonCommandWALAfterPublicPostAppendFailure(intent)
		return err
	}
	defer publishPrepareGuard.Release()
	finalizeOpts := commandWALFinalizeOptionsForPublicIntent(intent)
	finalizeOpts.skipPrePublishFlush = true
	finalizeOpts.durablePublishLocked = true
	finalizeOpts.durablePublishRelease = releaseDurablePublish
	finalizeOpts.rootPublicationBuilder = builder
	finalizeOpts.closeTeardownPinned = true
	finalizeOpts.expectedBaseCommitSeq = baseSeq
	finalizeOpts.hasExpectedBaseCommitSeq = true
	finalizeOpts.releaseRootSerialization = func() {}
	post, err := db.finalizeCommitLockedWithOptions(userRoot, systemRoot, nil, sync, adaptive.Metrics{}, nil, false, vlogRefDelta, nil, nil, finalizeOpts)
	if err != nil {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		if !post.accepted {
			db.poisonCommandWALAfterPublicPostAppendFailure(intent)
		} else {
			intent.inner.staged = false
		}
		return err
	}
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
	planningStart := time.Time{}
	if requestTiming != nil {
		planningStart = time.Now()
		requestTiming.BackendIntentPlanningSerializationObserved = true
	}
	appendPath := commandWALAppendStatsIntent
	if intent.statsPathSet {
		appendPath = intent.statsPath
	}
	if intent.rawKVDirect {
		appendPath = commandWALAppendStatsEntryScan
		scan := intent.rawKVScan
		if scan == nil {
			scan = db.rawKVCommandWALOperationScanner(intent.rawKVEntries, &intent.rawKVRIDCache, nil)
		}
		payload, err := commitlog.EncodeRawKVBatchPayloadPlanned(intent.rawKVPlan, scan)
		if err != nil {
			return 0, err
		}
		intent.payload = payload
	} else if intent.trustedPayload && !intent.statsPathSet {
		appendPath = commandWALAppendStatsPayload
	}
	if requestTiming != nil {
		requestTiming.BackendIntentPlanningSerialization += time.Since(planningStart)
		requestTiming.ExternalRefOrderingObserved = true
	}
	externalRefStart := time.Now()
	dependencies, err := db.captureCommandWALExternalDependencies(intent)
	if requestTiming != nil {
		requestTiming.ExternalRefOrdering += time.Since(externalRefStart)
		planningStart = time.Now()
	}
	if err != nil {
		return 0, err
	}
	if intent.rawKVRIDCache != nil && dependencies == nil {
		intent.rawKVRIDCache.release()
	}
	beforeAppend := func() error {
		if !sync {
			return nil
		}
		return db.syncCommandWALDependenciesThrough(^uint64(0), dependencies)
	}
	afterAppend := func(lsn uint64, rotations []*commitlog.CommandJournalStableRotation) error {
		rotationFiles := commandWALStableRotationTokens(rotations)
		if err := db.commandWALDebt.add(lsn, rotationFiles, dependencies); err != nil {
			for _, token := range rotationFiles {
				token.Release()
			}
			if intent.dependencyResources != nil {
				intent.dependencyResources.Release()
				intent.dependencyResources = nil
			}
			return err
		}
		intent.dependencyResources = nil
		return nil
	}
	db.mu.RLock()
	baseAppliedLSN := db.meta.AppliedCommandLSN
	db.mu.RUnlock()
	if requestTiming != nil {
		requestTiming.BackendIntentPlanningSerialization += time.Since(planningStart)
	}
	class := commitlog.CommandDurabilityRelaxed
	if sync {
		class = commitlog.CommandDurabilityDurable
	}
	appendStart := time.Now()
	lsn, err := db.commandJournal.AppendCommandObservedWithHooks(commitlog.CommandEnvelope{
		Version:         commitlog.CommandFrameVersionV2,
		DurabilityClass: class,
		Kind:            intent.kind,
		Scope:           intent.scope,
		BaseAppliedLSN:  baseAppliedLSN,
		PayloadFormat:   intent.payloadFormat,
		Payload:         intent.payload,
	}, beforeAppend, afterAppend)
	appendElapsed := time.Since(appendStart)
	postAppendStart := time.Time{}
	if requestTiming != nil {
		requestTiming.Append = appendElapsed
		requestTiming.AppendObserved = lsn != 0 || err == nil
		requestTiming.PostAppendPendingLSNBookkeepingObserved = true
		postAppendStart = time.Now()
	}
	if lsn != 0 || err == nil {
		db.observeCommandWALAppend(appendPath, appendElapsed)
	}
	if lsn != 0 {
		intent.lsn = lsn
		intent.coveredRange[0] = CommandWALLSNRange{First: lsn, Last: lsn}
	}
	if err != nil {
		if requestTiming != nil {
			requestTiming.PostAppendPendingLSNBookkeeping += time.Since(postAppendStart)
		}
		if lsn != 0 {
			// The frame already owns this LSN and may become replay-visible later.
			// Keep the intent identity and fail the open handle closed so a retry
			// cannot append a second frame for the same public mutation.
			db.poisonCommandWALAfterPostAppendFailure(intent)
		}
		return lsn, err
	}
	actualSync := sync
	if requestTiming != nil {
		requestTiming.PostAppendPendingLSNBookkeeping += time.Since(postAppendStart)
	}
	flushStart := time.Time{}
	if requestTiming != nil {
		requestTiming.FlushObserved = true
		requestTiming.Sync = actualSync
		requestTiming.FlushSync = actualSync
		flushStart = time.Now()
	}
	flushErr := db.flushCommandWALForPath(sync, true, appendPath)
	if requestTiming != nil {
		requestTiming.Flush = time.Since(flushStart)
	}
	if flushErr != nil {
		// AppendCommand already assigned a logical LSN, and the frame may be
		// replayed if the append reached disk. A later flush/sync failure is
		// commit-ambiguous: reopen recovery may apply the frame, so this handle
		// must fail closed instead of allowing a retry to create an LSN gap.
		// Poison here rather than relying on FlushCommandWAL: pre-flush
		// durability cuts return before that helper reaches its poison block.
		db.poisonCommandWALAfterPostAppendFailure(intent)
		return lsn, flushErr
	}
	if sync {
		db.closeCommandWALDurablePrefixThrough(lsn)
	}
	if requestTiming != nil {
		postAppendStart = time.Now()
	}
	db.observeCommandWALAccepted(lsn)
	if requestTiming != nil {
		requestTiming.PostAppendPendingLSNBookkeeping += time.Since(postAppendStart)
	}
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
		appliedCommandLSN: appliedRange.Last,
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
		// No need to poison because no command identity was assigned.
		return
	}
	// This covers both flush/sync failures after append and the later case where
	// root publication failed before AppliedCommandLSN could be published.
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
	if env.Kind != commitlog.CommandKindRawKVBatch || env.Scope != commitlog.CommandScopeRawKV ||
		(env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 && env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV2) {
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
	var materializedThisFrame map[uint64][]byte
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
			if err := db.registerReplayValueLogPointer(ptr); err != nil {
				return err
			}
			if err := b.SetPointerWithRevision(entry.Key, ptr, revision); err != nil {
				return err
			}
		case commitlog.RawKVOpSetMaterializedRID:
			if env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV2 {
				return commitlog.ErrCorrupt
			}
			if inlineAppender == nil || ridMap == nil {
				if ensureReplayLogSupport == nil {
					return fmt.Errorf("treedb: command wal replay log support unavailable")
				}
				var err error
				ridMap, inlineAppender, err = ensureReplayLogSupport()
				if err != nil {
					return err
				}
			}
			if inlineAppender == nil || ridMap == nil {
				return fmt.Errorf("treedb: command wal missing replay value-log appender")
			}
			ptr, ok := ridMap[entry.RID]
			if ok {
				got, producedThisFrame := materializedThisFrame[entry.RID]
				if !producedThisFrame {
					// Registration binds this exact existing segment to the synchronous
					// root-publication closure below. That closure syncs it after the
					// provisional in-memory LSN assignment but before the publication
					// seal, index, and durable meta make the LSN/root tuple stable.
					if err := db.registerReplayValueLogPointer(ptr); err != nil {
						return err
					}
					var err error
					got, err = db.valueLogManager.Read(ptr)
					if err != nil {
						return err
					}
				}
				if !bytes.Equal(got, entry.Value) {
					return errors.Join(
						commitlog.ErrCorrupt,
						fmt.Errorf("%w: rid=%d", ErrCommandWALConflictingValueLogRID, entry.RID),
					)
				}
			} else {
				var err error
				ptr, err = inlineAppender.appendExactRID(entry.RID, entry.Value)
				if err != nil {
					return err
				}
				ridMap[entry.RID] = ptr
				if materializedThisFrame == nil {
					materializedThisFrame = make(map[uint64][]byte)
				}
				materializedThisFrame[entry.RID] = entry.Value
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
