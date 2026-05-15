package commitlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
)

type JournalOwner struct {
	lock    *lockfile.Lock
	mu      sync.Mutex
	nextLSN uint64
}

func AcquireJournalOwner(dir string) (*JournalOwner, error) {
	return AcquireJournalOwnerWithOptions(dir, JournalOwnerOptions{})
}

type JournalOwnerOptions struct {
	// InitialLSN is the highest durable/applied LSN known by the caller. The
	// first reserved LSN is InitialLSN+1.
	InitialLSN uint64
}

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

func (o *JournalOwner) ReserveLSN() (uint64, error) {
	first, _, err := o.ReserveLSNRange(1)
	return first, err
}

func (o *JournalOwner) rollbackReservedLSN(lsn uint64) error {
	if o == nil || o.lock == nil {
		return errors.New("commitlog: journal owner is closed")
	}
	if lsn == 0 {
		return errors.New("commitlog: cannot rollback zero lsn")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.nextLSN != lsn+1 {
		return fmt.Errorf("commitlog: cannot rollback non-tail lsn %d", lsn)
	}
	o.nextLSN = lsn
	return nil
}

func (o *JournalOwner) ReserveLSNRange(count uint64) (first uint64, last uint64, err error) {
	if o == nil || o.lock == nil {
		return 0, 0, errors.New("commitlog: journal owner is closed")
	}
	if count == 0 {
		return 0, 0, errors.New("commitlog: journal owner lsn range is empty")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	first = o.nextLSN
	if first == 0 || count-1 > ^uint64(0)-first {
		return 0, 0, errors.New("commitlog: journal owner lsn space exhausted")
	}
	last = first + count - 1
	if last == ^uint64(0) {
		o.nextLSN = 0
	} else {
		o.nextLSN = last + 1
	}
	return first, last, nil
}

func (o *JournalOwner) Close() error {
	if o == nil || o.lock == nil {
		return nil
	}
	lock := o.lock
	o.lock = nil
	return lock.Close()
}

type CommandJournalOptions struct {
	Lane           int
	SegmentSeq     uint64
	MaxSegmentSize int64
	Compress       bool
	InitialLSN     uint64
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

func OpenCommandJournal(walDir string, opts CommandJournalOptions) (*CommandJournal, error) {
	if walDir == "" {
		return nil, errors.New("commitlog: command journal dir required")
	}
	if opts.Lane < 0 {
		return nil, fmt.Errorf("commitlog: invalid command journal lane %d", opts.Lane)
	}
	if opts.SegmentSeq == 0 {
		opts.SegmentSeq = 1
	}
	owner, err := AcquireJournalOwnerWithOptions(walDir, JournalOwnerOptions{InitialLSN: opts.InitialLSN})
	if err != nil {
		return nil, err
	}
	path := filepath.Join(walDir, CommandSegmentName(opts.Lane, opts.SegmentSeq))
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
	if j.writer.maxSegmentSize > 0 && int64(size) > j.writer.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return 0, ErrRecordTooLarge
	}
	lsn, err := j.owner.ReserveLSN()
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
