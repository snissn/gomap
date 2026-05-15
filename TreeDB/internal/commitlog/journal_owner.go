package commitlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
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
	// first reserved LSN is InitialLSN+1.
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
// this directly because failed appends rely on tail-only rollback.
func (o *JournalOwner) reserveLSN() (uint64, error) {
	first, _, err := o.reserveLSNRange(1)
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
	if o.lock == nil {
		return errors.New("commitlog: journal owner is closed")
	}
	if lsn == ^uint64(0) {
		if !o.exhausted {
			return fmt.Errorf("commitlog: cannot rollback non-tail lsn %d", lsn)
		}
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
	// Callers that want rotation must choose the next sequence explicitly.
	// Explicit sequences behind the current lane tail are rejected because they
	// would place newer LSNs before older segments in segment-ordered replay.
	SegmentSeq uint64
	// MaxSegmentSize caps individual command frame payloads; zero uses the
	// commitlog default.
	MaxSegmentSize int64
	// Compress enables commitlog frame compression.
	Compress bool
	// InitialLSN is the highest already-applied/durable command LSN. CommandJournal
	// scans existing frames while holding the owner lock and advances reservation
	// to max(InitialLSN, observed frame LSN)+1.
	InitialLSN uint64
}

type CommandJournal struct {
	mu     sync.Mutex
	owner  *JournalOwner
	writer *Writer
	path   string
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
	initialLSN, err := commandJournalInitialLSN(walDir, path, opts)
	if err != nil {
		_ = owner.Close()
		return nil, err
	}
	if err := owner.seedInitialLSN(initialLSN); err != nil {
		_ = owner.Close()
		return nil, err
	}
	writer, err := NewWriterWithOptions(path, Options{MaxSegmentSize: opts.MaxSegmentSize, Compress: opts.Compress})
	if err != nil {
		_ = owner.Close()
		return nil, err
	}
	return &CommandJournal{
		owner:  owner,
		writer: writer,
		path:   path,
	}, nil
}

func commandJournalInitialLSN(walDir, activePath string, opts CommandJournalOptions) (uint64, error) {
	initialLSN := opts.InitialLSN
	segments, err := commandJournalSegments(walDir, activePath)
	if err != nil {
		return 0, err
	}
	seenLSNs := make(map[uint64]struct{})
	for _, seg := range segments {
		if seg.size == 0 {
			continue
		}
		maxLSN, typed, completeEnd, err := scanCommandFrameMaxLSNAndEndWithLSN(seg.path, Options{MaxSegmentSize: opts.MaxSegmentSize}, func(lsn uint64) error {
			if _, exists := seenLSNs[lsn]; exists {
				return ErrCommandWALDuplicateLSN
			}
			seenLSNs[lsn] = struct{}{}
			return nil
		})
		if err != nil {
			if errors.Is(err, ErrCommandWALLegacyPayload) && !typed {
				if seg.active {
					return 0, fmt.Errorf("commitlog: scan command journal segment %s: %w", filepath.Base(seg.path), ErrCommandWALLegacyPayload)
				}
				continue
			}
			return 0, fmt.Errorf("commitlog: scan command journal segment %s: %w", filepath.Base(seg.path), err)
		}
		if completeEnd < seg.size {
			if !seg.active {
				return 0, fmt.Errorf("commitlog: non-active command journal segment %s has incomplete tail", filepath.Base(seg.path))
			}
			if err := truncateCommandJournalTail(seg.path, completeEnd); err != nil {
				return 0, err
			}
		}
		if typed && maxLSN > initialLSN {
			initialLSN = maxLSN
		}
	}
	return initialLSN, nil
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

// AppendCommand validates a complete command frame, assigns the next journal
// LSN, and appends it through this lane's single writer while holding the
// journal mutex. This intentionally optimizes for deterministic frame order and
// tail-only rollback, not parallel appends within one lane. The owner mutex
// still protects direct JournalOwner users, but CommandJournal requires no other
// owner reservation between this reserve and a failed append rollback.
func (j *CommandJournal) AppendCommand(env CommandEnvelope) (uint64, error) {
	if j == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.writer == nil || j.owner == nil {
		return 0, errors.New("commitlog: command journal is closed")
	}
	if env.LSN != 0 {
		return 0, errors.New("commitlog: command journal owns lsn assignment")
	}
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
	if err := validateCommandEnvelopeForEncode(probe); err != nil {
		return 0, err
	}
	size, err := commandFrameEncodedSize(probe)
	if err != nil {
		return 0, err
	}
	// maxSegmentSize is the per-frame safety cap used by Writer.AppendCommand;
	// segment-file rotation is caller-owned and not based on remaining bytes in
	// the current file.
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return 0, ErrRecordTooLarge
	}
	lsn, err := j.owner.reserveLSN()
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
	if j.writer == nil {
		return nil
	}
	return j.writer.Sync()
}

func (j *CommandJournal) Close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	var first error
	if j.writer != nil {
		if err := j.writer.Close(); err != nil {
			first = err
		}
		j.writer = nil
	}
	if j.owner != nil {
		if err := j.owner.Close(); err != nil && first == nil {
			first = err
		}
		j.owner = nil
	}
	return first
}
