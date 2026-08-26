package commitlog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type JournalOwner struct {
	lock      *lockfile.Lock
	mu        sync.Mutex
	nextLSN   uint64
	exhausted bool
}

func AcquireJournalOwner(dir string) (*JournalOwner, error) {
	return AcquireJournalOwnerWithOptions(dir, JournalOwnerOptions{})
}

type JournalOwnerOptions struct {
	// InitialLSN is the highest durable/applied LSN known by the caller. The
	// first reserved LSN is InitialLSN+1. InitialLSN==MaxUint64 is rejected
	// because there is no next LSN; InitialLSN==MaxUint64-1 is valid and permits
	// reserving the final MaxUint64 LSN exactly once.
	InitialLSN uint64
}

const MaxCommandJournalLane = 1023

func AcquireJournalOwnerWithOptions(dir string, opts JournalOwnerOptions) (*JournalOwner, error) {
	if dir == "" {
		return nil, errors.New("commitlog: journal owner dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockfile.Acquire(filepath.Join(dir, "command-wal-journal-owner.lock"))
	if err != nil {
		if errors.Is(err, lockfile.ErrLocked) {
			return nil, fmt.Errorf("%w: %v", ErrJournalOwnerExists, err)
		}
		return nil, err
	}
	if opts.InitialLSN == ^uint64(0) {
		_ = lock.Close()
		return nil, errors.New("commitlog: journal owner lsn space exhausted")
	}
	return &JournalOwner{lock: lock, nextLSN: opts.InitialLSN + 1}, nil
}

// reserveLSN reserves one LSN from this owner. Only CommandJournal should call
// this directly because failed appends rely on tail-only rollback. Reserving
// MaxUint64 is legal exactly once; rollbackReservedLSN reopens that final LSN
// for one retry if the append fails after the reservation.
func (o *JournalOwner) reserveLSN() (uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	first, _, err := o.reserveLSNRangeLocked(1)
	return first, err
}

func (o *JournalOwner) rollbackReservedLSN(lsn uint64) error {
	if o == nil {
		return errors.New("commitlog: journal owner is closed")
	}
	if lsn == 0 {
		return errors.New("commitlog: cannot rollback zero lsn")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.rollbackReservedLSNSerialized(lsn)
}

// rollbackReservedLSNSerialized rolls back the tail LSN. Callers must serialize
// owner access by holding either o.mu (direct JournalOwner callers) or the
// owning CommandJournal's mutex when the owner is private to that journal.
func (o *JournalOwner) rollbackReservedLSNSerialized(lsn uint64) error {
	if o.lock == nil {
		return errors.New("commitlog: journal owner is closed")
	}
	if lsn == ^uint64(0) {
		if !o.exhausted {
			return fmt.Errorf("commitlog: cannot rollback non-tail lsn %d", lsn)
		}
		// Re-open the final LSN for exactly one retry. A subsequent single-LSN
		// reservation may reuse MaxUint64; larger ranges still fail exhausted.
		o.exhausted = false
		o.nextLSN = lsn
		return nil
	}
	if o.exhausted || o.nextLSN != lsn+1 {
		return fmt.Errorf("commitlog: cannot rollback non-tail lsn %d", lsn)
	}
	o.nextLSN = lsn
	return nil
}

// reserveLSNRange reserves a contiguous LSN range from this owner. As with
// reserveLSN, callers must not interleave direct reservations with a
// CommandJournal that depends on tail-only rollback for failed appends.
func (o *JournalOwner) reserveLSNRange(count uint64) (first uint64, last uint64, err error) {
	if o == nil {
		return 0, 0, errors.New("commitlog: journal owner is closed")
	}
	if count == 0 {
		return 0, 0, errors.New("commitlog: journal owner lsn range is empty")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.reserveLSNRangeLocked(count)
}

func (o *JournalOwner) reserveLSNRangeLocked(count uint64) (first uint64, last uint64, err error) {
	if o == nil {
		return 0, 0, errors.New("commitlog: journal owner is closed")
	}
	if count == 0 {
		return 0, 0, errors.New("commitlog: journal owner lsn range is empty")
	}
	if o.lock == nil {
		return 0, 0, errors.New("commitlog: journal owner is closed")
	}
	if o.exhausted {
		return 0, 0, errors.New("commitlog: journal owner lsn space exhausted")
	}
	first = o.nextLSN
	if first == 0 || count-1 > ^uint64(0)-first {
		return 0, 0, errors.New("commitlog: journal owner lsn space exhausted")
	}
	last = first + count - 1
	if last == ^uint64(0) {
		o.exhausted = true
	} else {
		o.nextLSN = last + 1
	}
	return first, last, nil
}

func (o *JournalOwner) Close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.lock == nil {
		return nil
	}
	lock := o.lock
	o.lock = nil
	return lock.Close()
}

type CommandJournalOptions struct {
	// Lane selects the command WAL lane and must be in [0, MaxCommandJournalLane].
	// Lanes are encoded in decimal segment names, but command WAL is designed
	// around a small configured lane set rather than unbounded dynamic lanes.
	Lane int
	// SegmentSeq selects the segment sequence to append to. Zero means append to
	// the latest existing segment for Lane, or segment 1 when the lane is empty.
	// Explicitly naming the current latest segment appends to its tail. Callers
	// that want rotation must choose the next sequence explicitly.
	// Explicit sequences behind the current lane tail are rejected because they
	// would place newer LSNs before older segments in segment-ordered replay.
	SegmentSeq uint64
	// MaxSegmentSize caps individual command frame payloads; zero uses the
	// commitlog default.
	MaxSegmentSize int64
	// SegmentTargetBytes is the active command-WAL segment file-size target.
	// When >0, appends that would make a non-empty active segment exceed this
	// target rotate to the next lane segment before reserving an LSN. The target
	// is independent from MaxSegmentSize, which remains a per-frame safety cap.
	SegmentTargetBytes int64
	// BufferSize controls this command journal's buffered writer size. Zero
	// uses the commitlog default.
	BufferSize int
	// DeferredCommandBufferSize caps the writer-owned buffer used to finalize
	// trusted public command frames at flush/sync boundaries.
	DeferredCommandBufferSize int
	// DeferredCommandBufferRetainSize bounds retained command buffer capacity
	// after flush. Zero keeps the writer default.
	DeferredCommandBufferRetainSize int
	// Compress enables commitlog frame compression.
	Compress bool
	// OnSegmentRotated is called after a successful active segment rotation with
	// the closed segment bytes, including writer-buffered command frames flushed
	// by the rotation. The callback runs while the journal mutex is held and must
	// not re-enter the journal.
	OnSegmentRotated func(closedBytes int64)
	// InitialLSN is the highest already-applied/durable command LSN. CommandJournal
	// scans existing frames while holding the owner lock and advances reservation
	// to max(InitialLSN, observed frame LSN)+1.
	InitialLSN uint64
	// CaptureStableResources makes automatic size rotations retain the exact
	// closed and successor segment handles for later root publication. Callers
	// drain ownership with TakePendingStableRotations.
	CaptureStableResources bool
}

type CommandJournal struct {
	mu                     sync.Mutex
	owner                  *JournalOwner
	writer                 *Writer
	walDir                 string
	path                   string
	lane                   int
	segmentSeq             uint64
	activeSegmentMaxLSN    uint64
	cleanupEpoch           uint64
	namespaceGeneration    uint64
	segmentTargetBytes     int64
	onSegmentRotated       func(closedBytes int64)
	stableParent           *os.File
	stableParentErr        error
	captureStableResources bool
	pendingStableRotations []*CommandJournalStableRotation
	pendingStableSuccessor *pendingCommandWALSuccessor
	stableResourcePins     *rootpublication.IdentityPinRegistry
}

// CommandJournalCleanupSnapshot captures the monotonic append/rotation
// namespace state that ordinary WAL cleanup must revalidate immediately before
// deletion. StableIdentity is captured from the exact active writer handle.
type CommandJournalCleanupSnapshot struct {
	CleanupEpoch          uint64
	NamespaceGeneration   uint64
	Lane                  int
	SegmentSeq            uint64
	ActiveSegmentMaxLSN   uint64
	ActiveBytes           int64
	ActivePath            string
	ActiveIdentity        rootpublication.StableIdentity
	PendingStableRotation int
	PendingSuccessor      bool
}

var ErrCommandWALCleanupSnapshotStale = errors.New("commitlog: command WAL cleanup snapshot stale")

type pendingCommandWALSuccessor struct {
	parent          *os.File
	file            *os.File
	path            string
	seq             uint64
	observerEmitted bool
	failStop        bool
}

var errCommandJournalStableRotationFailStop = fmt.Errorf(
	"%w: command-WAL journal stopped after old-writer close failure",
	rootpublication.ErrResourceOwnership,
)

type CommandJournalAppendFlushTiming struct {
	Append time.Duration
	Flush  time.Duration
}

func CommandSegmentName(lane int, seq uint64) string {
	return fmt.Sprintf("commit-l%d-%06d.log", lane, seq)
}

// IsCommandSegmentName reports whether name matches the command WAL segment
// naming grammar used by CommandSegmentName.
func IsCommandSegmentName(name string) bool {
	_, _, ok := parseCommandSegmentName(name)
	return ok
}

func OpenCommandJournal(walDir string, opts CommandJournalOptions) (*CommandJournal, error) {
	if walDir == "" {
		return nil, errors.New("commitlog: command journal dir required")
	}
	if opts.Lane < 0 || opts.Lane > MaxCommandJournalLane {
		return nil, fmt.Errorf("commitlog: invalid command journal lane %d", opts.Lane)
	}
	owner, err := AcquireJournalOwnerWithOptions(walDir, JournalOwnerOptions{InitialLSN: opts.InitialLSN})
	if err != nil {
		return nil, err
	}
	if opts.SegmentSeq == 0 {
		seq, err := commandJournalLatestSegmentSeq(walDir, opts.Lane)
		if err != nil {
			_ = owner.Close()
			return nil, err
		}
		opts.SegmentSeq = seq
	} else {
		latestSeq, err := commandJournalLatestSegmentSeq(walDir, opts.Lane)
		if err != nil {
			_ = owner.Close()
			return nil, err
		}
		if opts.SegmentSeq < latestSeq {
			_ = owner.Close()
			return nil, fmt.Errorf("%w: lane %d segment %d is behind latest segment %d", ErrCommandWALStaleSegment, opts.Lane, opts.SegmentSeq, latestSeq)
		}
	}
	path := filepath.Join(walDir, CommandSegmentName(opts.Lane, opts.SegmentSeq))
	// The owner lock covers scan/truncate/seed so another writer cannot append
	// between max-LSN discovery and this journal's first reservation.
	initialLSN, activeSegmentMaxLSN, err := commandJournalInitialLSN(walDir, path, opts)
	if err != nil {
		_ = owner.Close()
		return nil, err
	}
	if err := owner.seedInitialLSN(initialLSN); err != nil {
		_ = owner.Close()
		return nil, err
	}
	writer, err := NewWriterWithOptions(path, Options{
		MaxSegmentSize:                  opts.MaxSegmentSize,
		BufferSize:                      opts.BufferSize,
		DeferredCommandBufferSize:       opts.DeferredCommandBufferSize,
		DeferredCommandBufferRetainSize: opts.DeferredCommandBufferRetainSize,
		Compress:                        opts.Compress,
	})
	if err != nil {
		_ = owner.Close()
		return nil, err
	}
	stableParent, stableParentErr := captureStableCommandWALParent(walDir, writer.f)
	if opts.CaptureStableResources && stableParentErr != nil {
		_ = writer.Close()
		_ = owner.Close()
		return nil, fmt.Errorf("commitlog: capture stable command-WAL parent: %w", stableParentErr)
	}
	return &CommandJournal{
		owner:                  owner,
		writer:                 writer,
		walDir:                 walDir,
		path:                   path,
		lane:                   opts.Lane,
		segmentSeq:             opts.SegmentSeq,
		activeSegmentMaxLSN:    activeSegmentMaxLSN,
		cleanupEpoch:           1,
		namespaceGeneration:    opts.SegmentSeq,
		segmentTargetBytes:     opts.SegmentTargetBytes,
		onSegmentRotated:       opts.OnSegmentRotated,
		stableParent:           stableParent,
		stableParentErr:        stableParentErr,
		captureStableResources: opts.CaptureStableResources,
		stableResourcePins:     rootpublication.NewIdentityPinRegistry(),
	}, nil
}

func commandJournalInitialLSN(walDir, activePath string, opts CommandJournalOptions) (uint64, uint64, error) {
	initialLSN := opts.InitialLSN
	var activeMaxLSN uint64
	segments, err := commandJournalSegments(walDir, activePath)
	if err != nil {
		return 0, 0, err
	}
	seenLSNs := make(map[uint64]struct{})
	for _, seg := range segments {
		if seg.size == 0 {
			// Empty command segments contain no recoverable LSN and no torn typed
			// frame tail. NewWriterWithOptions opens them in append mode at offset 0.
			continue
		}
		maxLSN, typed, completeEnd, err := scanCommandSegmentSummary(seg.path, Options{MaxSegmentSize: opts.MaxSegmentSize}, func(lsn uint64) error {
			if _, exists := seenLSNs[lsn]; exists {
				return ErrCommandWALDuplicateLSN
			}
			seenLSNs[lsn] = struct{}{}
			return nil
		})
		if err != nil {
			if errors.Is(err, ErrCommandWALLegacyPayload) && !typed {
				if seg.active {
					return 0, 0, fmt.Errorf("commitlog: scan command journal segment %s: %w", filepath.Base(seg.path), ErrCommandWALLegacyPayload)
				}
				continue
			}
			return 0, 0, fmt.Errorf("commitlog: scan command journal segment %s: %w", filepath.Base(seg.path), err)
		}
		if completeEnd < seg.size {
			if !seg.active {
				return 0, 0, fmt.Errorf("commitlog: non-active command journal segment %s has incomplete tail", filepath.Base(seg.path))
			}
			if err := truncateCommandJournalTail(seg.path, completeEnd); err != nil {
				return 0, 0, err
			}
		}
		if typed && maxLSN > initialLSN {
			initialLSN = maxLSN
		}
		if seg.active && typed {
			activeMaxLSN = maxLSN
		}
	}
	return initialLSN, activeMaxLSN, nil
}

func commandJournalLatestSegmentSeq(walDir string, lane int) (uint64, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}
		return 0, err
	}
	var maxSeq uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		entryLane, seq, ok := parseCommandSegmentName(entry.Name())
		if ok && entryLane == lane && seq > maxSeq {
			maxSeq = seq
		}
	}
	if maxSeq == 0 {
		return 1, nil
	}
	return maxSeq, nil
}

func (o *JournalOwner) seedInitialLSN(initialLSN uint64) error {
	if o == nil {
		return errors.New("commitlog: journal owner is closed")
	}
	if initialLSN == ^uint64(0) {
		return errors.New("commitlog: journal owner lsn space exhausted")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.lock == nil {
		return errors.New("commitlog: journal owner is closed")
	}
	o.nextLSN = initialLSN + 1
	o.exhausted = false
	return nil
}

type commandJournalSegment struct {
	path   string
	lane   int
	seq    uint64
	size   int64
	active bool
}

func commandJournalSegments(walDir string, activePath string) ([]commandJournalSegment, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	activeLane, activeSeq, hasActivePath := parseCommandSegmentName(filepath.Base(activePath))
	segments := make([]commandJournalSegment, 0, len(entries))
	activeSeqByLane := make(map[int]uint64)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		entryLane, seq, ok := parseCommandSegmentName(entry.Name())
		if !ok {
			continue
		}
		path := filepath.Join(walDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		segments = append(segments, commandJournalSegment{
			path: path,
			lane: entryLane,
			seq:  seq,
			size: info.Size(),
		})
		if seq > activeSeqByLane[entryLane] {
			activeSeqByLane[entryLane] = seq
		}
	}
	for i := range segments {
		seg := &segments[i]
		if hasActivePath && seg.lane == activeLane {
			seg.active = seg.seq == activeSeq
			continue
		}
		seg.active = seg.seq == activeSeqByLane[seg.lane]
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].lane != segments[j].lane {
			return segments[i].lane < segments[j].lane
		}
		if segments[i].seq != segments[j].seq {
			return segments[i].seq < segments[j].seq
		}
		return segments[i].path < segments[j].path
	})
	return segments, nil
}

func parseCommandSegmentName(name string) (lane int, seq uint64, ok bool) {
	if !strings.HasPrefix(name, "commit-l") || !strings.HasSuffix(name, ".log") {
		return 0, 0, false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(name, "commit-l"), ".log")
	parts := strings.SplitN(rest, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	lane, err := strconv.Atoi(parts[0])
	if err != nil || lane < 0 {
		return 0, 0, false
	}
	seq, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil || seq == 0 {
		return 0, 0, false
	}
	return lane, seq, true
}

func truncateCommandJournalTail(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDirFn(path)
}

// reserveLSNLocked reserves one LSN for CommandJournal append paths. Callers
// must hold j.mu. The owner mutex still protects owner state even though
// OpenCommandJournal keeps the owner private to this journal.
func (j *CommandJournal) reserveLSNLocked() (uint64, error) {
	if j == nil || j.owner == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return 0, err
	}
	j.owner.mu.Lock()
	defer j.owner.mu.Unlock()
	first, _, err := j.owner.reserveLSNRangeLocked(1)
	return first, err
}

func (j *CommandJournal) maybeRotateForFrameLocked(frameSize int, syncCurrent bool) error {
	return j.maybeRotateForFrameLockedObserved(frameSize, syncCurrent, false)
}

func (j *CommandJournal) maybeRotateForFrameLockedObserved(frameSize int, syncCurrent, observe bool) error {
	return j.maybeRotateForFrameLockedObservedWithNamespaceDebt(frameSize, syncCurrent, false, observe)
}

func (j *CommandJournal) maybeRotateForFrameLockedObservedWithNamespaceDebt(frameSize int, syncCurrent, deferNamespace, observe bool) error {
	if j == nil || j.writer == nil || j.segmentTargetBytes <= 0 || frameSize <= 0 {
		return nil
	}
	total := int64(segmentHeaderSize + frameSize)
	activeBytes := j.writer.ActiveBytes()
	if activeBytes <= 0 || activeBytes+total <= j.segmentTargetBytes {
		return nil
	}
	if err := j.requireNoPendingStableRotationLocked(); err != nil {
		return err
	}
	if j.captureStableResources {
		rotation, err := j.rotateActiveSegmentWithStableResourcesLocked(syncCurrent, deferNamespace, observe)
		if err != nil {
			return err
		}
		j.pendingStableRotations = append(j.pendingStableRotations, rotation)
		return nil
	}
	return j.rotateActiveSegmentLockedObserved(syncCurrent, observe)
}

func (j *CommandJournal) requireNoPendingStableRotationLocked() error {
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return err
	}
	if len(j.pendingStableRotations) != 0 {
		return fmt.Errorf("%w: drain pending stable command-WAL rotation before rotating again", rootpublication.ErrResourceOwnership)
	}
	if j.pendingStableSuccessor != nil {
		return fmt.Errorf("%w: resolve pending stable command-WAL successor before rotating again", rootpublication.ErrResourceOwnership)
	}
	return nil
}

func (j *CommandJournal) stableRotationFailStopErrorLocked() error {
	if j != nil && j.pendingStableSuccessor != nil && j.pendingStableSuccessor.failStop {
		return errCommandJournalStableRotationFailStop
	}
	return nil
}

func (j *CommandJournal) rotateActiveSegmentLocked(syncCurrent bool) error {
	return j.rotateActiveSegmentLockedObserved(syncCurrent, false)
}

func (j *CommandJournal) rotateActiveSegmentLockedObserved(syncCurrent, observe bool) error {
	if j == nil || j.writer == nil {
		return errors.New("commitlog: command journal is closed")
	}
	if err := j.requireNoPendingStableRotationLocked(); err != nil {
		return err
	}
	if j.segmentSeq == ^uint64(0) {
		return ErrCommandWALSegmentSeqExhausted
	}
	nextSeq := j.segmentSeq + 1
	nextPath := filepath.Join(j.walDir, CommandSegmentName(j.lane, nextSeq))
	closedBytes := j.writer.ActiveBytes()
	outcome, rotateErr := j.writer.rotateToWithSyncObserved(nextPath, syncCurrent, filepath.Dir(j.walDir), observe)
	if !outcome.Installed {
		return rotateErr
	}
	stableParent, stableParentErr := captureStableCommandWALParent(j.walDir, j.writer.f)
	oldStableParent := j.stableParent
	j.stableParent = stableParent
	j.stableParentErr = stableParentErr
	if closedBytes > 0 && j.onSegmentRotated != nil {
		j.onSegmentRotated(closedBytes)
	}
	j.segmentSeq = nextSeq
	j.path = nextPath
	j.activeSegmentMaxLSN = 0
	j.namespaceGeneration++
	j.cleanupEpoch++
	var closeParentErr error
	if oldStableParent != nil {
		closeParentErr = oldStableParent.Close()
	}
	if j.captureStableResources {
		return errors.Join(rotateErr, stableParentErr, closeParentErr)
	}
	return errors.Join(rotateErr, closeParentErr)
}

// RotateActiveSegment rotates the command WAL to the next segment. It is safe to
// call at checkpoint boundaries; appends are serialized by the journal mutex.
func (j *CommandJournal) RotateActiveSegment(syncCurrent bool) error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.rotateActiveSegmentLocked(syncCurrent)
}

// CommandJournalStableRotation retains exact handles for the segment closed by
// a rotation and its newly-created successor.
type CommandJournalStableRotation struct {
	Closed *rootpublication.StableResourceToken
	Active *rootpublication.StableResourceToken
}

var newCommandWALStableNamespaceToken = rootpublication.NewStableNamespaceToken
var openStableCommandWALParent = rootpublication.OpenStableParent

func captureStableCommandWALParent(walDir string, resource *os.File) (*os.File, error) {
	parent, err := openStableCommandWALParent(walDir)
	if err != nil {
		if parent != nil {
			_ = parent.Close()
		}
		return nil, err
	}
	if err := rootpublication.ValidateStableChildLink(parent, resource, filepath.Base(resource.Name())); err != nil {
		_ = parent.Close()
		return nil, err
	}
	return parent, nil
}

func (rotation *CommandJournalStableRotation) TakeClosed() *rootpublication.StableResourceToken {
	if rotation == nil {
		return nil
	}
	token := rotation.Closed
	rotation.Closed = nil
	return token
}

func (rotation *CommandJournalStableRotation) TakeActive() *rootpublication.StableResourceToken {
	if rotation == nil {
		return nil
	}
	token := rotation.Active
	rotation.Active = nil
	return token
}

func (rotation *CommandJournalStableRotation) Release() {
	if rotation == nil {
		return
	}
	rotation.Closed.Release()
	rotation.Active.Release()
	rotation.Closed = nil
	rotation.Active = nil
}

func commandWALDiagnosticPath(path string) string {
	return filepath.Join("maindb", "wal", filepath.Base(path))
}

// NewStableCommandWALResourceToken registers an exact active or rotated
// command-WAL segment handle captured by the journal owner.
func NewStableCommandWALResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	switch spec.Reachability {
	case rootpublication.ReachabilityCommandWALActive, rootpublication.ReachabilityCommandWALRotated:
		return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerCommandWAL, spec, "authoritative")
	default:
		return nil, fmt.Errorf("%w: command-WAL producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
}

func (j *CommandJournal) stableSegmentToken(file *os.File, path string, seq uint64, field rootpublication.ReachabilityField, frontier rootpublication.DurableFrontier, namespace *rootpublication.StableNamespaceToken, contentSynced bool) (*rootpublication.StableResourceToken, error) {
	if j == nil || j.stableResourcePins == nil {
		return nil, fmt.Errorf("%w: command-WAL identity registry unavailable", rootpublication.ErrUnresolvedResource)
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		return nil, err
	}
	if err := j.stableResourcePins.Observe(identity); err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("command-wal/lane=%d/segment=%d", j.lane, seq)))
	spec := rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceCommandWAL, LogicalLane: fmt.Sprintf("lane-%d", j.lane),
		ResourceID: fmt.Sprintf("%d:%d", j.lane, seq), Generation: seq,
		DiagnosticPath: commandWALDiagnosticPath(path), File: file, Frontier: frontier,
		Digest: digest, Reachability: field, Namespace: namespace,
		ContentSynced: contentSynced, PinRegistry: j.stableResourcePins,
	}
	token, tokenErr := NewStableCommandWALResourceToken(spec)
	unobserveErr := j.stableResourcePins.Unobserve(identity)
	if unobserveErr != nil {
		if token != nil {
			token.Release()
		}
		return nil, errors.Join(tokenErr, unobserveErr)
	}
	return token, tokenErr
}

func (j *CommandJournal) recordAppendedLSNLocked(lsn uint64) {
	if lsn > j.activeSegmentMaxLSN {
		j.activeSegmentMaxLSN = lsn
	}
	j.cleanupEpoch++
}

func openStableCommandWALFile(parent *os.File, path string) (*os.File, error) {
	// The journal owner serializes writes and installs only a fresh empty
	// successor, so an append-only access mask is unnecessary here. Retain full
	// write access because Windows FlushFileBuffers requires it when the same
	// exact handle later proves content and creation-metadata durability.
	return rootpublication.OpenStableChildFile(parent, filepath.Base(path), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
}

func pendingCommandWALFailure(parent, file *os.File, path string, err error) error {
	return errors.Join(err, rootpublication.ValidateStableChildLink(parent, file, filepath.Base(path)))
}

// RotateActiveSegmentWithStableResources flushes and optionally syncs the old
// segment, pins it before the writer closes it, rotates, persists the successor
// namespace entry, and pins the new active file before returning.
func (j *CommandJournal) RotateActiveSegmentWithStableResources(syncCurrent bool) (*CommandJournalStableRotation, error) {
	if j == nil {
		return nil, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.rotateActiveSegmentWithStableResourcesLocked(syncCurrent, false, durabilitycut.Enabled())
}

func (j *CommandJournal) rotateActiveSegmentWithStableResourcesLocked(syncCurrent, deferNamespace, observe bool) (*CommandJournalStableRotation, error) {
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return nil, err
	}
	if j.writer == nil || j.writer.f == nil {
		return nil, errors.New("commitlog: command journal is closed")
	}
	if len(j.pendingStableRotations) != 0 {
		return nil, fmt.Errorf("%w: drain pending stable command-WAL rotation before rotating again", rootpublication.ErrResourceOwnership)
	}
	if j.segmentSeq == ^uint64(0) {
		return nil, ErrCommandWALSegmentSeqExhausted
	}
	closedPath, closedSeq := j.path, j.segmentSeq
	nextSeq := closedSeq + 1
	nextPath := filepath.Join(j.walDir, CommandSegmentName(j.lane, nextSeq))
	parent := j.stableParent
	if pending := j.pendingStableSuccessor; pending != nil {
		if pending.parent != parent || pending.path != nextPath || pending.seq != nextSeq {
			return nil, errors.Join(rootpublication.ErrResourceOwnership,
				fmt.Errorf("%w: stable command-WAL retry does not match the pending exact successor", rootpublication.ErrResourceConflict))
		}
	}
	if syncCurrent {
		if observe {
			if err := durabilitycut.EmitPath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceCommandWAL, filepath.Dir(j.walDir), closedPath); err != nil {
				return nil, err
			}
		}
		if err := j.writer.Sync(); err != nil {
			return nil, err
		}
		if observe {
			if err := durabilitycut.EmitPath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceCommandWAL, filepath.Dir(j.walDir), closedPath); err != nil {
				return nil, err
			}
		}
	} else if err := j.writer.Flush(); err != nil {
		return nil, err
	}
	closedInfo, err := j.writer.f.Stat()
	if err != nil {
		return nil, err
	}
	closedToken, err := j.stableSegmentToken(j.writer.f, closedPath, closedSeq, rootpublication.ReachabilityCommandWALRotated,
		rootpublication.DurableFrontier{Bytes: uint64(closedInfo.Size()), MaxLSN: j.activeSegmentMaxLSN}, nil, syncCurrent)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		closedToken.Release()
		if j.stableParentErr != nil {
			return nil, fmt.Errorf("commitlog: stable command-WAL parent unavailable: %w", j.stableParentErr)
		}
		return nil, errors.New("commitlog: stable command-WAL parent unavailable")
	}
	pending := j.pendingStableSuccessor
	if pending == nil {
		prepared, openErr := openStableCommandWALFile(parent, nextPath)
		if openErr != nil {
			closedToken.Release()
			return nil, openErr
		}
		pending = &pendingCommandWALSuccessor{parent: parent, file: prepared, path: nextPath, seq: nextSeq}
		j.pendingStableSuccessor = pending
	}
	prepared := pending.file
	if !pending.observerEmitted {
		pending.observerEmitted = true
		if observeErr := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceCommandWAL, filepath.Dir(nextPath), "", nextPath); observeErr != nil {
			closedToken.Release()
			return nil, pendingCommandWALFailure(parent, prepared, nextPath, observeErr)
		}
	}
	activeInfo, err := prepared.Stat()
	if err != nil {
		closedToken.Release()
		return nil, err
	}
	parentGeneration, err := rootpublication.StableNamespaceParentGeneration(parent)
	if err != nil {
		closedToken.Release()
		return nil, pendingCommandWALFailure(parent, prepared, nextPath, err)
	}
	namespace, err := newCommandWALStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: parent, LinkedResource: prepared, ParentGeneration: parentGeneration, Operation: rootpublication.NamespaceCreate,
		NewName: filepath.Base(nextPath), DiagnosticPath: filepath.Join("maindb", "wal"),
	})
	if err != nil {
		closedToken.Release()
		return nil, pendingCommandWALFailure(parent, prepared, nextPath, err)
	}
	// Relaxed rotations retain this exact parent/name obligation for the
	// dependency-debt ledger. A durable rotation must persist the successor name
	// before its durable-class frame is appended; ordinary relaxed appends must
	// not introduce a stable namespace sync.
	if !deferNamespace {
		if observe {
			if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceCommandWAL, filepath.Dir(j.walDir), filepath.Dir(nextPath)); err != nil {
				namespace.Release()
				closedToken.Release()
				return nil, pendingCommandWALFailure(parent, prepared, nextPath, err)
			}
		}
		if err := namespace.Stabilize(); err != nil {
			namespace.Release()
			closedToken.Release()
			return nil, pendingCommandWALFailure(parent, prepared, nextPath, err)
		}
		if observe {
			if err := durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceCommandWAL, filepath.Dir(j.walDir), filepath.Dir(nextPath)); err != nil {
				namespace.Release()
				closedToken.Release()
				return nil, pendingCommandWALFailure(parent, prepared, nextPath, err)
			}
		}
	}
	activeToken, err := j.stableSegmentToken(prepared, nextPath, nextSeq, rootpublication.ReachabilityCommandWALActive,
		rootpublication.DurableFrontier{Bytes: uint64(activeInfo.Size())}, namespace, false)
	namespace.Release()
	if err != nil {
		closedToken.Release()
		return nil, pendingCommandWALFailure(parent, prepared, nextPath, err)
	}
	closedBytes := j.writer.ActiveBytes()
	old := j.writer.f
	if err := j.writer.closeRotatedResource(old); err != nil {
		activeToken.Release()
		closedToken.Release()
		pending.failStop = true
		j.writer.f = nil
		j.writer.fileSink.file = nil
		return nil, fmt.Errorf("commitlog: close old writer during stable rotation: %w", err)
	}
	pending.file = nil
	j.writer.f = prepared
	j.writer.fileSink.file = prepared
	j.writer.bw.Reset(&j.writer.fileSink)
	j.writer.size = activeInfo.Size()
	j.writer.scratch = j.writer.scratch[:0]
	j.segmentSeq = nextSeq
	j.path = nextPath
	j.activeSegmentMaxLSN = 0
	j.namespaceGeneration++
	j.cleanupEpoch++
	if closedBytes > 0 && j.onSegmentRotated != nil {
		j.onSegmentRotated(closedBytes)
	}
	j.pendingStableSuccessor = nil
	return &CommandJournalStableRotation{Closed: closedToken, Active: activeToken}, nil
}

// TakePendingStableRotations flushes and refreshes the current active segment
// frontier, then transfers the exact resources captured by automatic size
// rotations. The caller must consume or Release every result.
func (j *CommandJournal) TakePendingStableRotations() ([]*CommandJournalStableRotation, error) {
	if j == nil {
		return nil, nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.takePendingStableRotationsLocked()
}

func (j *CommandJournal) takePendingStableRotationsLocked() ([]*CommandJournalStableRotation, error) {
	if len(j.pendingStableRotations) == 0 {
		return nil, nil
	}
	if j.writer == nil || j.writer.f == nil {
		return nil, errors.New("commitlog: command journal is closed")
	}
	if err := j.writer.Flush(); err != nil {
		return nil, err
	}
	info, err := j.writer.f.Stat()
	if err != nil {
		return nil, err
	}
	last := j.pendingStableRotations[len(j.pendingStableRotations)-1]
	if last == nil || last.Active == nil {
		return nil, fmt.Errorf("%w: pending command-WAL rotation has no active resource", rootpublication.ErrResourceOwnership)
	}
	active, err := j.stableSegmentToken(j.writer.f, j.path, j.segmentSeq, rootpublication.ReachabilityCommandWALActive,
		rootpublication.DurableFrontier{Bytes: uint64(info.Size()), MaxLSN: j.activeSegmentMaxLSN}, last.Active.Namespace(), false)
	if err != nil {
		return nil, err
	}
	last.Active.Release()
	last.Active = active
	rotations := j.pendingStableRotations
	j.pendingStableRotations = nil
	return rotations, nil
}

func (j *CommandJournal) ActiveBytes() int64 {
	_, bytes := j.ActiveSegmentSnapshot()
	return bytes
}

func (j *CommandJournal) WriterBufferStats() WriterBufferStats {
	if j == nil {
		return WriterBufferStats{}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil {
		return WriterBufferStats{}
	}
	return j.writer.BufferStats()
}

// WriterDurabilityStats reports cumulative file and directory sync hook calls
// for the active writer, including calls made while rotating prior segments.
// The writer object is reused across rotations, so the snapshot is journal
// lifetime cumulative.
func (j *CommandJournal) WriterDurabilityStats() DurabilityStats {
	if j == nil {
		return DurabilityStats{}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil {
		return DurabilityStats{}
	}
	return j.writer.DurabilityStats()
}

// ActiveSegmentSnapshot reports the active segment path and accepted bytes under
// one journal lock acquisition.
func (j *CommandJournal) ActiveSegmentSnapshot() (path string, bytes int64) {
	if j == nil {
		return "", 0
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil {
		return j.path, 0
	}
	return j.path, j.writer.ActiveBytes()
}

func (j *CommandJournal) cleanupSnapshotLocked() (CommandJournalCleanupSnapshot, error) {
	if j == nil || j.writer == nil || j.writer.f == nil {
		return CommandJournalCleanupSnapshot{}, errors.New("commitlog: command journal is closed")
	}
	identity, err := rootpublication.StableIdentityFromFile(j.writer.f)
	if err != nil {
		return CommandJournalCleanupSnapshot{}, err
	}
	return CommandJournalCleanupSnapshot{
		CleanupEpoch:          j.cleanupEpoch,
		NamespaceGeneration:   j.namespaceGeneration,
		Lane:                  j.lane,
		SegmentSeq:            j.segmentSeq,
		ActiveSegmentMaxLSN:   j.activeSegmentMaxLSN,
		ActiveBytes:           j.writer.ActiveBytes(),
		ActivePath:            j.path,
		ActiveIdentity:        identity,
		PendingStableRotation: len(j.pendingStableRotations),
		PendingSuccessor:      j.pendingStableSuccessor != nil,
	}, nil
}

// CaptureCleanupSnapshot captures the journal namespace and append epoch under
// the journal owner lock. The snapshot contains no deletion authority by
// itself; callers must consume it with WithCleanupSnapshot.
func (j *CommandJournal) CaptureCleanupSnapshot() (CommandJournalCleanupSnapshot, error) {
	if j == nil {
		return CommandJournalCleanupSnapshot{}, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.cleanupSnapshotLocked()
}

func validateMonotonicCleanupSnapshot(captured, current CommandJournalCleanupSnapshot) error {
	if current.PendingStableRotation != 0 || current.PendingSuccessor {
		return errors.Join(ErrCommandWALCleanupSnapshotStale, errors.New("commitlog: command WAL rotation/retry ownership is pending"))
	}
	if current.CleanupEpoch < captured.CleanupEpoch ||
		current.NamespaceGeneration < captured.NamespaceGeneration ||
		current.Lane != captured.Lane ||
		current.SegmentSeq < captured.SegmentSeq {
		return ErrCommandWALCleanupSnapshotStale
	}
	if current.SegmentSeq == captured.SegmentSeq {
		if current.ActivePath != captured.ActivePath ||
			!rootpublication.SamePhysicalIdentity(current.ActiveIdentity, captured.ActiveIdentity) ||
			current.ActiveBytes < captured.ActiveBytes ||
			current.ActiveSegmentMaxLSN < captured.ActiveSegmentMaxLSN {
			return ErrCommandWALCleanupSnapshotStale
		}
		return nil
	}
	if current.CleanupEpoch == captured.CleanupEpoch ||
		current.NamespaceGeneration == captured.NamespaceGeneration {
		return ErrCommandWALCleanupSnapshotStale
	}
	return nil
}

// WithCleanupSnapshot revalidates that snapshot authority advanced only
// monotonically, then retains the journal owner lock while fn marks every
// captured/current active generation, acquires exact identity deletion leases,
// and unlinks covered closed segments. The callback reports whether it changed
// the namespace, including a partial batch that also returned an error, so the
// next cleanup proof cannot reuse the pre-unlink generation.
func (j *CommandJournal) WithCleanupSnapshot(snapshot CommandJournalCleanupSnapshot, fn func(*rootpublication.IdentityPinRegistry, CommandJournalCleanupSnapshot) (bool, error)) error {
	if j == nil {
		return errors.Join(ErrCommandWALCleanupSnapshotStale, errors.New("commitlog: command journal is closed"))
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	current, err := j.cleanupSnapshotLocked()
	if err != nil {
		return errors.Join(ErrCommandWALCleanupSnapshotStale, err)
	}
	if err := validateMonotonicCleanupSnapshot(snapshot, current); err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	mutated, err := fn(j.stableResourcePins, current)
	if mutated {
		j.namespaceGeneration++
		j.cleanupEpoch++
	}
	return err
}

func (j *CommandJournal) NextLSN() uint64 {
	if j == nil || j.owner == nil {
		return 0
	}
	j.owner.mu.Lock()
	defer j.owner.mu.Unlock()
	return j.owner.nextLSN
}

// AppendCommand validates a complete command frame, assigns the next journal
// LSN, and appends it through this lane's single writer while holding the
// journal mutex. This intentionally optimizes for deterministic frame order and
// tail-only rollback, not parallel appends within one lane.
func (j *CommandJournal) AppendCommand(env CommandEnvelope) (uint64, error) {
	return j.appendCommand(env, false, nil, nil)
}

// AppendCommandObserved appends a complete command frame and emits its
// durability boundaries while the journal lock still identifies the exact
// active segment that owns the frame.
func (j *CommandJournal) AppendCommandObserved(env CommandEnvelope) (uint64, error) {
	return j.appendCommand(env, durabilitycut.Enabled(), nil, nil)
}

// AppendCommandObservedWithHooks runs beforeAppend after validation but before
// rotation/LSN assignment, and afterAppend after the frame and any captured
// rotations exist but before the observable post-append boundary. Both hooks
// run under the journal mutex and must not re-enter the journal. The post hook
// receives ownership of every returned rotation.
// This is the serialization seam used by durable-prefix dependency debt.
func (j *CommandJournal) AppendCommandObservedWithHooks(
	env CommandEnvelope,
	beforeAppend func() error,
	afterAppend func(uint64, []*CommandJournalStableRotation) error,
) (uint64, error) {
	return j.appendCommand(env, durabilitycut.Enabled(), beforeAppend, afterAppend)
}

func (j *CommandJournal) appendCommand(
	env CommandEnvelope,
	observe bool,
	beforeAppend func() error,
	afterAppend func(uint64, []*CommandJournalStableRotation) error,
) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil || j.owner == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	if err := j.emitDirectCommandWALAppendBeforeLocked(observe); err != nil {
		return 0, err
	}
	if env.LSN != 0 {
		return 0, errors.New("commitlog: command journal owns lsn assignment")
	}
	// Validate shape and payload before reserving, using a synthetic non-zero
	// LSN. Writer.AppendCommand repeats encode validation with the actual
	// reserved LSN before it writes bytes, so late LSN-sensitive validation
	// still rolls the reservation back through the error path below.
	probe := env
	probe.LSN = 1
	if probe.Kind == CommandKindRawKVBatch && probe.Payload == nil {
		// Nil RawKVBatch payloads are accepted as an explicit empty batch so
		// callers can append a no-op frame without constructing payload bytes.
		payload, err := EncodeRawKVBatchPayload(nil)
		if err != nil {
			return 0, err
		}
		probe.Payload = payload
		env.Payload = payload
	}
	v2 := probe.Version == CommandFrameVersionV2 || probe.DurabilityClass != 0
	var size int
	if v2 {
		var err error
		size, err = commandFrameV2EncodedSize(probe)
		if err != nil {
			return 0, err
		}
	} else {
		if err := validateCommandEnvelopeForEncode(probe); err != nil {
			return 0, err
		}
		var err error
		size, err = commandFrameEncodedSize(probe)
		if err != nil {
			return 0, err
		}
	}
	// maxSegmentSize is the per-frame safety cap used by Writer.AppendCommand;
	// segmentTargetBytes is the separate active file rotation target checked
	// before LSN reservation.
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return 0, ErrRecordTooLarge
	}
	if beforeAppend != nil {
		if err := beforeAppend(); err != nil {
			return 0, err
		}
	}
	syncRotation := v2 && probe.DurabilityClass == CommandDurabilityDurable
	deferRotationNamespace := v2 && probe.DurabilityClass == CommandDurabilityRelaxed
	if err := j.maybeRotateForFrameLockedObservedWithNamespaceDebt(size, syncRotation, deferRotationNamespace, observe); err != nil {
		return 0, err
	}
	lsn, err := j.reserveLSNLocked()
	if err != nil {
		return 0, err
	}
	env.LSN = lsn
	if err := j.writer.AppendCommand(env); err != nil {
		if rollbackErr := j.owner.rollbackReservedLSN(lsn); rollbackErr != nil {
			return 0, errors.Join(err, rollbackErr)
		}
		return 0, err
	}
	j.recordAppendedLSNLocked(lsn)
	if afterAppend != nil {
		rotations, err := j.takePendingStableRotationsLocked()
		if err != nil {
			return lsn, err
		}
		if err := afterAppend(lsn, rotations); err != nil {
			return lsn, err
		}
	}
	if err := j.emitDirectCommandWALAppendAfterLocked(observe, lsn); err != nil {
		return lsn, err
	}
	return lsn, nil
}

func (j *CommandJournal) AppendRawKVSingleCommand(baseAppliedLSN uint64, op RawKVOperation) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.appendRawKVSingleCommandLocked(baseAppliedLSN, op, false, false)
}

func (j *CommandJournal) AppendRawKVSingleCommandWithRotateSync(baseAppliedLSN uint64, op RawKVOperation, syncCurrent bool) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.appendRawKVSingleCommandLocked(baseAppliedLSN, op, syncCurrent, false)
}

// AppendRawKVSingleCommandAndFlush appends a one-operation RawKVBatch command
// and flushes/syncs the writer while holding the journal lock. When sync is
// true, any segment rotated before the append is also synced before the new
// frame is written, preserving durable command-WAL prefix ordering for sync
// point writes.
func (j *CommandJournal) AppendRawKVSingleCommandAndFlush(baseAppliedLSN uint64, op RawKVOperation, sync bool) (uint64, error) {
	lsn, _, err := j.AppendRawKVSingleCommandAndFlushMeasured(baseAppliedLSN, op, sync)
	return lsn, err
}

func (j *CommandJournal) AppendRawKVSingleCommandAndFlushMeasured(baseAppliedLSN uint64, op RawKVOperation, sync bool) (uint64, CommandJournalAppendFlushTiming, error) {
	var timing CommandJournalAppendFlushTiming
	if j == nil {
		return 0, timing, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	observe := durabilitycut.Enabled()
	appendStart := time.Now()
	lsn, err := j.appendRawKVSingleCommandLocked(baseAppliedLSN, op, sync, observe)
	timing.Append = time.Since(appendStart)
	if err != nil {
		return 0, timing, err
	}
	if err := j.emitDirectCommandWALAppendAfterLocked(observe, lsn); err != nil {
		return lsn, timing, err
	}
	flushStart := time.Now()
	if err := j.flushDirectCommandWALLocked(sync, observe); err != nil {
		timing.Flush = time.Since(flushStart)
		return lsn, timing, err
	}
	timing.Flush = time.Since(flushStart)
	return lsn, timing, nil
}

func (j *CommandJournal) appendRawKVSingleCommandLocked(baseAppliedLSN uint64, op RawKVOperation, syncCurrent, observe bool) (uint64, error) {
	if j.writer == nil || j.owner == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	if err := validateRawKVOperation(&op); err != nil {
		return 0, err
	}
	if op.Op == RawKVOpSetMaterializedRID {
		return 0, ErrCommandWALUnsupportedVersion
	}
	valueLen := len(op.Value)
	if op.Op == RawKVOpSetRID {
		valueLen = 8
	}
	if commandFrameIntExceedsUint32(len(op.Key)) || commandFrameIntExceedsUint32(valueLen) {
		return 0, ErrRecordTooLarge
	}
	payloadLen := rawKVBatchHeaderSize + rawKVOpHeaderSize + len(op.Key) + valueLen
	size, err := commandFrameEncodedSizeFromLengths(payloadLen, 0, 0, 0)
	if err != nil {
		return 0, err
	}
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return 0, ErrRecordTooLarge
	}
	if err := j.emitDirectCommandWALAppendBeforeLocked(observe); err != nil {
		return 0, err
	}
	if err := j.maybeRotateForFrameLockedObserved(size, syncCurrent, observe); err != nil {
		return 0, err
	}
	lsn, err := j.reserveLSNLocked()
	if err != nil {
		return 0, err
	}
	if err := j.writer.AppendRawKVSingleCommandDirect(lsn, baseAppliedLSN, op); err != nil {
		if rollbackErr := j.owner.rollbackReservedLSN(lsn); rollbackErr != nil {
			return 0, errors.Join(err, rollbackErr)
		}
		return 0, err
	}
	j.recordAppendedLSNLocked(lsn)
	return lsn, nil
}

// AppendRawKVPointCommandTrusted appends a caller-validated public raw KV point
// Set/Delete command. It preserves the same LSN reservation and rollback
// semantics as AppendRawKVSingleCommand while avoiding redundant operation
// validation in the public cached hot path.
func (j *CommandJournal) AppendRawKVPointCommandTrusted(baseAppliedLSN uint64, op RawKVOp, key, value []byte) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.appendRawKVPointCommandTrustedLocked(baseAppliedLSN, op, key, value, false, false)
}

func (j *CommandJournal) AppendRawKVPointCommandTrustedWithRotateSync(baseAppliedLSN uint64, op RawKVOp, key, value []byte, syncCurrent bool) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.appendRawKVPointCommandTrustedLocked(baseAppliedLSN, op, key, value, syncCurrent, false)
}

// AppendRawKVPointCommandTrustedAndFlush appends a caller-validated public raw
// KV point command and flushes/syncs the writer while holding the journal lock.
// When sync is true, any segment rotated before the append is also synced before
// the new frame is written, preserving durable command-WAL prefix ordering for
// sync point writes.
func (j *CommandJournal) AppendRawKVPointCommandTrustedAndFlush(baseAppliedLSN uint64, op RawKVOp, key, value []byte, sync bool) (uint64, error) {
	lsn, _, err := j.AppendRawKVPointCommandTrustedAndFlushMeasured(baseAppliedLSN, op, key, value, sync)
	return lsn, err
}

func (j *CommandJournal) AppendRawKVPointCommandTrustedAndFlushMeasured(baseAppliedLSN uint64, op RawKVOp, key, value []byte, sync bool) (uint64, CommandJournalAppendFlushTiming, error) {
	var timing CommandJournalAppendFlushTiming
	if j == nil {
		return 0, timing, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	observe := durabilitycut.Enabled()
	appendStart := time.Now()
	lsn, err := j.appendRawKVPointCommandTrustedLocked(baseAppliedLSN, op, key, value, sync, observe)
	timing.Append = time.Since(appendStart)
	if err != nil {
		return 0, timing, err
	}
	if err := j.emitDirectCommandWALAppendAfterLocked(observe, lsn); err != nil {
		return lsn, timing, err
	}
	flushStart := time.Now()
	if err := j.flushDirectCommandWALLocked(sync, observe); err != nil {
		timing.Flush = time.Since(flushStart)
		return lsn, timing, err
	}
	timing.Flush = time.Since(flushStart)
	return lsn, timing, nil
}

// AppendRawKVPointCommandTrustedWithRevisionAndFlush appends a caller-validated
// public raw KV point command with native entry revision metadata and
// flushes/syncs the writer while holding the journal lock.
func (j *CommandJournal) AppendRawKVPointCommandTrustedWithRevisionAndFlush(baseAppliedLSN uint64, op RawKVOp, key, value []byte, revision uint64, sync bool) (uint64, error) {
	lsn, _, err := j.AppendRawKVPointCommandTrustedWithRevisionAndFlushMeasured(baseAppliedLSN, op, key, value, revision, sync)
	return lsn, err
}

func (j *CommandJournal) AppendRawKVPointCommandTrustedWithRevisionAndFlushMeasured(baseAppliedLSN uint64, op RawKVOp, key, value []byte, revision uint64, sync bool) (uint64, CommandJournalAppendFlushTiming, error) {
	var timing CommandJournalAppendFlushTiming
	if j == nil {
		return 0, timing, errors.New("commitlog: command journal is closed")
	}
	if revision == 0 {
		return j.AppendRawKVPointCommandTrustedAndFlushMeasured(baseAppliedLSN, op, key, value, sync)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	observe := durabilitycut.Enabled()
	appendStart := time.Now()
	lsn, err := j.appendRawKVPointCommandTrustedWithRevisionLocked(baseAppliedLSN, op, key, value, revision, sync, observe)
	timing.Append = time.Since(appendStart)
	if err != nil {
		return 0, timing, err
	}
	if err := j.emitDirectCommandWALAppendAfterLocked(observe, lsn); err != nil {
		return lsn, timing, err
	}
	flushStart := time.Now()
	if err := j.flushDirectCommandWALLocked(sync, observe); err != nil {
		timing.Flush = time.Since(flushStart)
		return lsn, timing, err
	}
	timing.Flush = time.Since(flushStart)
	return lsn, timing, nil
}

func (j *CommandJournal) AppendRawKVPointCommandTrustedWithRevisionAndRotateSync(baseAppliedLSN uint64, op RawKVOp, key, value []byte, revision uint64, syncCurrent bool) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	if revision == 0 {
		return j.AppendRawKVPointCommandTrustedWithRotateSync(baseAppliedLSN, op, key, value, syncCurrent)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.appendRawKVPointCommandTrustedWithRevisionLocked(baseAppliedLSN, op, key, value, revision, syncCurrent, false)
}

func (j *CommandJournal) appendRawKVPointCommandTrustedLocked(baseAppliedLSN uint64, op RawKVOp, key, value []byte, syncCurrent, observe bool) (uint64, error) {
	return j.appendRawKVPointCommandTrustedWithRevisionLocked(baseAppliedLSN, op, key, value, 0, syncCurrent, observe)
}

func (j *CommandJournal) appendRawKVPointCommandTrustedWithRevisionLocked(baseAppliedLSN uint64, op RawKVOp, key, value []byte, revision uint64, syncCurrent, observe bool) (uint64, error) {
	if j.writer == nil || j.owner == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	valueLen, payloadLen, size, err := trustedRawKVPointCommandFrameLens(op, key, value, revision)
	if err != nil {
		return 0, err
	}
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return 0, ErrRecordTooLarge
	}
	if err := j.emitDirectCommandWALAppendBeforeLocked(observe); err != nil {
		return 0, err
	}
	if err := j.maybeRotateForFrameLockedObserved(size, syncCurrent, observe); err != nil {
		return 0, err
	}
	lsn, err := j.reserveLSNLocked()
	if err != nil {
		return 0, err
	}
	if err := j.writer.appendRawKVPointCommandDirectTrustedSizedWithRevision(lsn, baseAppliedLSN, op, key, value, revision, valueLen, payloadLen, size); err != nil {
		if rollbackErr := j.owner.rollbackReservedLSN(lsn); rollbackErr != nil {
			return 0, errors.Join(err, rollbackErr)
		}
		return 0, err
	}
	j.recordAppendedLSNLocked(lsn)
	return lsn, nil
}

func (j *CommandJournal) AppendRawKVBatchPayloadCommand(baseAppliedLSN uint64, payload []byte) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	if err := validateRawKVBatchPayload(payload); err != nil {
		return 0, err
	}
	return j.AppendRawKVBatchPayloadCommandTrusted(baseAppliedLSN, payload)
}

func (j *CommandJournal) AppendRawKVBatchPayloadCommandAndFlushMeasured(baseAppliedLSN uint64, payload []byte, sync bool) (uint64, CommandJournalAppendFlushTiming, error) {
	if j == nil {
		return 0, CommandJournalAppendFlushTiming{}, errors.New("commitlog: command journal is closed")
	}
	if err := validateRawKVBatchPayload(payload); err != nil {
		return 0, CommandJournalAppendFlushTiming{}, err
	}
	return j.AppendRawKVBatchPayloadCommandTrustedAndFlushMeasured(baseAppliedLSN, payload, sync)
}

// AppendRawKVBatchPayloadCommandTrusted appends a caller-validated canonical
// RawKVBatch payload.
func (j *CommandJournal) AppendRawKVBatchPayloadCommandTrusted(baseAppliedLSN uint64, payload []byte) (uint64, error) {
	return j.AppendRawKVBatchPayloadCommandTrustedWithRotateSync(baseAppliedLSN, payload, false)
}

func (j *CommandJournal) AppendRawKVBatchPayloadCommandTrustedWithRotateSync(baseAppliedLSN uint64, payload []byte, syncCurrent bool) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil || j.owner == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	size, err := commandFrameEncodedSizeFromLengths(len(payload), 0, 0, 0)
	if err != nil {
		return 0, err
	}
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return 0, ErrRecordTooLarge
	}
	if err := j.maybeRotateForFrameLocked(size, syncCurrent); err != nil {
		return 0, err
	}
	lsn, err := j.reserveLSNLocked()
	if err != nil {
		return 0, err
	}
	if err := j.writer.AppendRawKVBatchPayloadCommandDirectTrusted(lsn, baseAppliedLSN, payload); err != nil {
		if rollbackErr := j.owner.rollbackReservedLSN(lsn); rollbackErr != nil {
			return 0, errors.Join(err, rollbackErr)
		}
		return 0, err
	}
	j.recordAppendedLSNLocked(lsn)
	return lsn, nil
}

// AppendRawKVBatchPayloadScanCommandTrusted appends a caller-validated
// replayable RawKVBatch operation source without materializing the canonical
// payload slice.
func (j *CommandJournal) AppendRawKVBatchPayloadScanCommandTrusted(baseAppliedLSN uint64, plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner) (uint64, error) {
	return j.appendRawKVBatchPayloadScanCommandTrusted(baseAppliedLSN, plan, scan, false)
}

// AppendRawKVBatchPayloadScanCommandTrustedObserved appends a scanned payload
// and emits append boundaries under the journal lock.
func (j *CommandJournal) AppendRawKVBatchPayloadScanCommandTrustedObserved(baseAppliedLSN uint64, plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner) (uint64, error) {
	return j.appendRawKVBatchPayloadScanCommandTrusted(baseAppliedLSN, plan, scan, durabilitycut.Enabled())
}

func (j *CommandJournal) appendRawKVBatchPayloadScanCommandTrusted(baseAppliedLSN uint64, plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner, observe bool) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil || j.owner == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	if err := j.emitDirectCommandWALAppendBeforeLocked(observe); err != nil {
		return 0, err
	}
	if err := plan.validate(); err != nil {
		return 0, err
	}
	size, err := commandFrameEncodedSizeFromLengths(plan.PayloadLen, 0, 0, 0)
	if err != nil {
		return 0, err
	}
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return 0, ErrRecordTooLarge
	}
	if err := j.maybeRotateForFrameLocked(size, false); err != nil {
		return 0, err
	}
	lsn, err := j.reserveLSNLocked()
	if err != nil {
		return 0, err
	}
	if err := j.writer.AppendRawKVBatchPayloadScanCommandDirectTrusted(lsn, baseAppliedLSN, plan, scan); err != nil {
		if rollbackErr := j.owner.rollbackReservedLSN(lsn); rollbackErr != nil {
			return 0, errors.Join(err, rollbackErr)
		}
		return 0, err
	}
	j.recordAppendedLSNLocked(lsn)
	if err := j.emitDirectCommandWALAppendAfterLocked(observe, lsn); err != nil {
		return lsn, err
	}
	return lsn, nil
}

// AppendCommandPayloadTrusted appends a caller-validated canonical command
// payload. It validates only command identity and frame size; callers must use
// this only for payloads built by the matching commitlog encoder.
func (j *CommandJournal) AppendCommandPayloadTrusted(kind CommandKind, scope CommandScope, format PayloadFormat, baseAppliedLSN uint64, payload []byte) (uint64, error) {
	return j.appendCommandPayloadTrusted(kind, scope, format, baseAppliedLSN, payload, false)
}

// AppendCommandPayloadTrustedObserved appends a trusted payload and emits
// append boundaries under the journal lock.
func (j *CommandJournal) AppendCommandPayloadTrustedObserved(kind CommandKind, scope CommandScope, format PayloadFormat, baseAppliedLSN uint64, payload []byte) (uint64, error) {
	return j.appendCommandPayloadTrusted(kind, scope, format, baseAppliedLSN, payload, durabilitycut.Enabled())
}

func (j *CommandJournal) appendCommandPayloadTrusted(kind CommandKind, scope CommandScope, format PayloadFormat, baseAppliedLSN uint64, payload []byte, observe bool) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil || j.owner == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	if err := j.emitDirectCommandWALAppendBeforeLocked(observe); err != nil {
		return 0, err
	}
	if err := validateCommandEnvelopeIdentity(CommandEnvelope{
		LSN:           1,
		Kind:          kind,
		Scope:         scope,
		PayloadFormat: format,
	}); err != nil {
		return 0, err
	}
	size, err := commandFrameEncodedSizeFromLengths(len(payload), 0, 0, 0)
	if err != nil {
		return 0, err
	}
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return 0, ErrRecordTooLarge
	}
	if err := j.maybeRotateForFrameLocked(size, false); err != nil {
		return 0, err
	}
	lsn, err := j.reserveLSNLocked()
	if err != nil {
		return 0, err
	}
	if err := j.writer.AppendCommandPayloadDirectTrusted(lsn, baseAppliedLSN, kind, scope, format, payload); err != nil {
		if rollbackErr := j.owner.rollbackReservedLSN(lsn); rollbackErr != nil {
			return 0, errors.Join(err, rollbackErr)
		}
		return 0, err
	}
	j.recordAppendedLSNLocked(lsn)
	if err := j.emitDirectCommandWALAppendAfterLocked(observe, lsn); err != nil {
		return lsn, err
	}
	return lsn, nil
}

func (j *CommandJournal) flushLocked(sync bool) error {
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return err
	}
	if j == nil || j.writer == nil {
		return errors.New("commitlog: command journal is closed")
	}
	if sync {
		return j.writer.Sync()
	}
	return j.writer.Flush()
}

func (j *CommandJournal) emitDirectCommandWALAppendBeforeLocked(observe bool) error {
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return err
	}
	if !observe {
		return nil
	}
	return durabilitycut.EmitBasic(durabilitycut.BeforeDependencyAppend, durabilitycut.ResourceCommandWAL, filepath.Dir(j.walDir))
}

func (j *CommandJournal) emitDirectCommandWALAppendAfterLocked(observe bool, lsn uint64) error {
	if !observe {
		return nil
	}
	return durabilitycut.EmitPathLSN(durabilitycut.AfterDependencyAppend, durabilitycut.ResourceCommandWAL, filepath.Dir(j.walDir), j.path, lsn)
}

func (j *CommandJournal) flushDirectCommandWALLocked(sync, observe bool) error {
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return err
	}
	if !observe {
		return j.flushLocked(sync)
	}
	before, after := durabilitycut.BeforeUserspaceFlush, durabilitycut.AfterUserspaceFlush
	if sync {
		before, after = durabilitycut.BeforeDependencyFileSync, durabilitycut.AfterDependencyFileSync
	}
	if err := durabilitycut.EmitPath(before, durabilitycut.ResourceCommandWAL, filepath.Dir(j.walDir), j.path); err != nil {
		return err
	}
	if err := j.flushLocked(sync); err != nil {
		return err
	}
	return durabilitycut.EmitPath(after, durabilitycut.ResourceCommandWAL, filepath.Dir(j.walDir), j.path)
}

// AppendRawKVBatchPayloadCommandTrustedAndFlush appends a caller-validated
// RawKVBatch payload and flushes/syncs the writer while holding the journal
// lock. If the append succeeds but the flush fails, the allocated LSN is
// returned with the flush error so callers can preserve the commit-ambiguous
// command-WAL failure contract.
func (j *CommandJournal) AppendRawKVBatchPayloadCommandTrustedAndFlush(baseAppliedLSN uint64, payload []byte, sync bool) (uint64, error) {
	lsn, _, err := j.AppendRawKVBatchPayloadCommandTrustedAndFlushMeasured(baseAppliedLSN, payload, sync)
	return lsn, err
}

func (j *CommandJournal) AppendRawKVBatchPayloadCommandTrustedAndFlushMeasured(baseAppliedLSN uint64, payload []byte, sync bool) (uint64, CommandJournalAppendFlushTiming, error) {
	var timing CommandJournalAppendFlushTiming
	if j == nil {
		return 0, timing, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil || j.owner == nil {
		return 0, timing, errors.New("commitlog: command journal is closed")
	}
	appendStart := time.Now()
	size, err := commandFrameEncodedSizeFromLengths(len(payload), 0, 0, 0)
	if err != nil {
		timing.Append = time.Since(appendStart)
		return 0, timing, err
	}
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		timing.Append = time.Since(appendStart)
		return 0, timing, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		timing.Append = time.Since(appendStart)
		return 0, timing, ErrRecordTooLarge
	}
	observe := durabilitycut.Enabled()
	if err := j.emitDirectCommandWALAppendBeforeLocked(observe); err != nil {
		timing.Append = time.Since(appendStart)
		return 0, timing, err
	}
	if err := j.maybeRotateForFrameLockedObserved(size, sync, observe); err != nil {
		timing.Append = time.Since(appendStart)
		return 0, timing, err
	}
	lsn, err := j.reserveLSNLocked()
	if err != nil {
		timing.Append = time.Since(appendStart)
		return 0, timing, err
	}
	if err := j.writer.AppendRawKVBatchPayloadCommandDirectTrusted(lsn, baseAppliedLSN, payload); err != nil {
		if rollbackErr := j.owner.rollbackReservedLSN(lsn); rollbackErr != nil {
			timing.Append = time.Since(appendStart)
			return 0, timing, errors.Join(err, rollbackErr)
		}
		timing.Append = time.Since(appendStart)
		return 0, timing, err
	}
	j.recordAppendedLSNLocked(lsn)
	timing.Append = time.Since(appendStart)
	if err := j.emitDirectCommandWALAppendAfterLocked(observe, lsn); err != nil {
		return lsn, timing, err
	}
	flushStart := time.Now()
	err = j.flushDirectCommandWALLocked(sync, observe)
	timing.Flush = time.Since(flushStart)
	if err != nil {
		return lsn, timing, err
	}
	return lsn, timing, nil
}

// AppendRawKVBatchPayloadScanCommandTrustedAndFlush appends a caller-validated
// replayable RawKVBatch operation source and flushes/syncs the writer while
// holding the journal lock.
func (j *CommandJournal) AppendRawKVBatchPayloadScanCommandTrustedAndFlush(baseAppliedLSN uint64, plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner, sync bool) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil || j.owner == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	if err := plan.validate(); err != nil {
		return 0, err
	}
	size, err := commandFrameEncodedSizeFromLengths(plan.PayloadLen, 0, 0, 0)
	if err != nil {
		return 0, err
	}
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return 0, ErrRecordTooLarge
	}
	if err := j.maybeRotateForFrameLocked(size, sync); err != nil {
		return 0, err
	}
	lsn, err := j.reserveLSNLocked()
	if err != nil {
		return 0, err
	}
	if err := j.writer.AppendRawKVBatchPayloadScanCommandDirectTrusted(lsn, baseAppliedLSN, plan, scan); err != nil {
		if rollbackErr := j.owner.rollbackReservedLSN(lsn); rollbackErr != nil {
			return 0, errors.Join(err, rollbackErr)
		}
		return 0, err
	}
	j.recordAppendedLSNLocked(lsn)
	err = j.flushLocked(sync)
	if err != nil {
		return lsn, err
	}
	return lsn, nil
}

func (j *CommandJournal) Path() string {
	if j == nil {
		return ""
	}
	return j.path
}

func (j *CommandJournal) Flush() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return err
	}
	if j.writer == nil {
		return nil
	}
	return j.writer.Flush()
}

func (j *CommandJournal) Sync() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return err
	}
	if j.writer == nil {
		return nil
	}
	return j.writer.Sync()
}

// FlushObserved flushes or syncs the exact active segment while holding the
// same lock used to emit its durability boundaries.
func (j *CommandJournal) FlushObserved(sync bool) error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return err
	}
	if j.writer == nil {
		return nil
	}
	return j.flushDirectCommandWALLocked(sync, durabilitycut.Enabled())
}

func (j *CommandJournal) Close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	var closeErrs []error
	for _, rotation := range j.pendingStableRotations {
		rotation.Release()
	}
	j.pendingStableRotations = nil
	if pending := j.pendingStableSuccessor; pending != nil {
		if pending.file != nil {
			closeErrs = append(closeErrs, pending.file.Close())
			pending.file = nil
		}
		j.pendingStableSuccessor = nil
	}
	if j.writer != nil {
		if err := j.writer.Close(); err != nil {
			closeErrs = append(closeErrs, err)
		}
		j.writer = nil
	}
	if j.owner != nil {
		closeErrs = append(closeErrs, j.owner.Close())
		j.owner = nil
	}
	if j.stableParent != nil {
		closeErrs = append(closeErrs, j.stableParent.Close())
		j.stableParent = nil
	}
	return errors.Join(closeErrs...)
}
