package collectionwal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type AppenderOptions struct {
	Lane         uint32
	SegmentSeq   uint64
	FirstWALLSN  uint64
	SegmentBytes int64
	SyncOnAppend bool
}

type AppendResult struct {
	Path          string
	Offset        int64
	Length        int64
	WALLSN        uint64
	CollectionSeq uint64
}

type Appender struct {
	mu           sync.Mutex
	file         *os.File
	dbDir        string
	path         string
	lane         uint32
	segmentSeq   uint64
	segmentBytes int64
	nextWALLSN   uint64
	syncOnAppend bool
	offset       int64
	closed       bool
}

func CreateSegmentAppender(dbDir string, opts AppenderOptions) (*Appender, error) {
	if dbDir == "" {
		return nil, fmt.Errorf("%w: empty db dir", ErrCollectionWALUnsafePath)
	}
	opts = normalizeAppenderOptions(opts)
	if err := validateAppenderOptions(opts); err != nil {
		return nil, err
	}

	walDir := filepath.Join(dbDir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		return nil, err
	}
	path := SegmentPath(dbDir, opts.Lane, opts.SegmentSeq)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	header := EncodeSegmentHeader(SegmentHeader{
		Lane:        opts.Lane,
		SegmentSeq:  opts.SegmentSeq,
		FirstWALLSN: opts.FirstWALLSN,
	})
	if err := writeFull(file, header); err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}
	if err := syncDir(walDir); err != nil {
		return nil, err
	}
	ok = true
	return &Appender{
		file:         file,
		dbDir:        dbDir,
		path:         path,
		lane:         opts.Lane,
		segmentSeq:   opts.SegmentSeq,
		segmentBytes: opts.SegmentBytes,
		nextWALLSN:   opts.FirstWALLSN,
		syncOnAppend: opts.SyncOnAppend,
		offset:       SegmentHeaderLen,
	}, nil
}

func OpenOrCreateSegmentAppender(dbDir string, opts AppenderOptions) (*Appender, error) {
	app, err := CreateSegmentAppender(dbDir, opts)
	if err == nil {
		return app, nil
	}
	if errors.Is(err, os.ErrExist) {
		return OpenSegmentAppender(dbDir, opts)
	}
	return nil, err
}

func OpenOrCreateTailSegmentAppender(dbDir string, opts AppenderOptions) (*Appender, error) {
	if dbDir == "" {
		return nil, fmt.Errorf("%w: empty db dir", ErrCollectionWALUnsafePath)
	}
	opts = normalizeAppenderOptions(opts)
	if err := validateAppenderOptions(opts); err != nil {
		return nil, err
	}
	infos, err := dirtySegmentInfos(dbDir)
	if err != nil {
		return nil, err
	}
	var tail *segmentFileInfo
	for i := range infos {
		if infos[i].lane == opts.Lane {
			tail = &infos[i]
		}
	}
	if tail == nil {
		return OpenOrCreateSegmentAppender(dbDir, opts)
	}
	header, err := readSegmentHeader(tail.path)
	if err != nil {
		return nil, err
	}
	opts.SegmentSeq = tail.seq
	opts.FirstWALLSN = header.FirstWALLSN
	return OpenSegmentAppender(dbDir, opts)
}

func OpenSegmentAppender(dbDir string, opts AppenderOptions) (*Appender, error) {
	if dbDir == "" {
		return nil, fmt.Errorf("%w: empty db dir", ErrCollectionWALUnsafePath)
	}
	opts = normalizeAppenderOptions(opts)
	if err := validateAppenderOptions(opts); err != nil {
		return nil, err
	}
	path := SegmentPath(dbDir, opts.Lane, opts.SegmentSeq)
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
		}
	}()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	header, frames, err := ScanSegment(data, true)
	if err != nil {
		return nil, err
	}
	if header.Lane != opts.Lane || header.SegmentSeq != opts.SegmentSeq {
		return nil, fmt.Errorf("%w: segment identity lane=%d seq=%d want lane=%d seq=%d", ErrCollectionWALIdentityMismatch, header.Lane, header.SegmentSeq, opts.Lane, opts.SegmentSeq)
	}
	if header.FirstWALLSN != 0 && header.FirstWALLSN != opts.FirstWALLSN {
		return nil, fmt.Errorf("%w: first WALLSN=%d want %d", ErrCollectionWALIdentityMismatch, header.FirstWALLSN, opts.FirstWALLSN)
	}
	offset := int64(SegmentHeaderLen)
	nextWALLSN := opts.FirstWALLSN
	truncatedTail := false
	for i, frame := range frames {
		switch frame.Outcome {
		case OutcomeCompleteValid:
			offset = frame.Offset + frame.Length
			nextWALLSN = frame.Header.WALLSN + 1
		case OutcomeTerminalIncompleteTail:
			if i != len(frames)-1 {
				return nil, fmt.Errorf("%w: terminal tail before end of scan: %w", ErrCollectionWALCorruptMiddle, frame.Err)
			}
			offset = frame.Offset
			truncatedTail = true
		default:
			return nil, fmt.Errorf("%w: cannot append after frame outcome %s: %w", ErrCollectionWALRecoveryRequired, frame.Outcome, frame.Err)
		}
	}
	if truncatedTail {
		if err := file.Truncate(offset); err != nil {
			return nil, err
		}
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
		if err := file.Sync(); err != nil {
			return nil, err
		}
	} else if offset != int64(len(data)) {
		return nil, fmt.Errorf("%w: segment scan ended at %d of %d", ErrCollectionWALCorruptMiddle, offset, len(data))
	}
	if offset > opts.SegmentBytes {
		return nil, fmt.Errorf("%w: existing segment bytes %d exceed configured cap %d", ErrCollectionWALResourceLimit, offset, opts.SegmentBytes)
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	ok = true
	return &Appender{
		file:         file,
		dbDir:        dbDir,
		path:         path,
		lane:         opts.Lane,
		segmentSeq:   opts.SegmentSeq,
		segmentBytes: opts.SegmentBytes,
		nextWALLSN:   nextWALLSN,
		syncOnAppend: opts.SyncOnAppend,
		offset:       offset,
	}, nil
}

func readSegmentHeader(path string) (SegmentHeader, error) {
	var buf [SegmentHeaderLen]byte
	file, err := os.Open(path)
	if err != nil {
		return SegmentHeader{}, err
	}
	defer func() { _ = file.Close() }()
	if _, err := io.ReadFull(file, buf[:]); err != nil {
		return SegmentHeader{}, err
	}
	return DecodeSegmentHeader(buf[:])
}

func normalizeAppenderOptions(opts AppenderOptions) AppenderOptions {
	if opts.SegmentSeq == 0 {
		opts.SegmentSeq = 1
	}
	if opts.FirstWALLSN == 0 {
		opts.FirstWALLSN = 1
	}
	if opts.SegmentBytes == 0 {
		opts.SegmentBytes = DefaultSegmentBytes
	}
	return opts
}

func validateAppenderOptions(opts AppenderOptions) error {
	if opts.SegmentBytes < SegmentHeaderLen || opts.SegmentBytes > MaxSegmentBytes {
		return fmt.Errorf("%w: segment bytes %d outside [%d,%d]", ErrCollectionWALResourceLimit, opts.SegmentBytes, SegmentHeaderLen, MaxSegmentBytes)
	}
	return nil
}

func (a *Appender) Path() string {
	if a == nil {
		return ""
	}
	return a.path
}

func (a *Appender) AppendTransaction(txn Transaction, syncAppend bool) (AppendResult, error) {
	var result AppendResult
	if a == nil {
		return result, errors.New("collectionwal: nil appender")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.file == nil {
		return result, errors.New("collectionwal: appender closed")
	}
	if txn.WALLSN == 0 {
		txn.WALLSN = a.nextWALLSN
	} else if txn.WALLSN != a.nextWALLSN {
		return result, fmt.Errorf("%w: append WALLSN=%d next=%d", ErrCollectionWALSequenceGap, txn.WALLSN, a.nextWALLSN)
	}
	frame, err := EncodeTransactionFrame(txn)
	if err != nil {
		return result, err
	}
	if int64(len(frame)) > a.segmentBytes-SegmentHeaderLen {
		return result, fmt.Errorf("%w: frame bytes %d exceed usable segment bytes %d", ErrCollectionWALResourceLimit, len(frame), a.segmentBytes-SegmentHeaderLen)
	}
	for int64(len(frame)) > a.segmentBytes-a.offset {
		if err := a.rolloverLocked(); err != nil {
			return result, err
		}
	}
	offset := a.offset
	if err := writeFull(a.file, frame); err != nil {
		return result, err
	}
	if syncAppend || a.syncOnAppend {
		if err := a.file.Sync(); err != nil {
			return result, err
		}
	}
	a.offset += int64(len(frame))
	a.nextWALLSN++
	return AppendResult{
		Path:          a.path,
		Offset:        offset,
		Length:        int64(len(frame)),
		WALLSN:        txn.WALLSN,
		CollectionSeq: txn.CollectionSeq,
	}, nil
}

func (a *Appender) rolloverLocked() error {
	if a == nil || a.closed || a.file == nil {
		return errors.New("collectionwal: appender closed")
	}
	if a.dbDir == "" {
		return fmt.Errorf("%w: empty db dir", ErrCollectionWALUnsafePath)
	}
	if a.segmentSeq == ^uint64(0) {
		return fmt.Errorf("%w: segment sequence overflow", ErrCollectionWALResourceLimit)
	}
	next, err := OpenOrCreateSegmentAppender(a.dbDir, AppenderOptions{
		Lane:         a.lane,
		SegmentSeq:   a.segmentSeq + 1,
		FirstWALLSN:  a.nextWALLSN,
		SegmentBytes: a.segmentBytes,
		SyncOnAppend: a.syncOnAppend,
	})
	if err != nil {
		return err
	}
	oldFile := a.file
	a.file = next.file
	a.path = next.path
	a.lane = next.lane
	a.segmentSeq = next.segmentSeq
	a.segmentBytes = next.segmentBytes
	a.nextWALLSN = next.nextWALLSN
	a.syncOnAppend = next.syncOnAppend
	a.offset = next.offset
	next.file = nil
	next.closed = true
	if oldFile != nil {
		return oldFile.Close()
	}
	return nil
}

func (a *Appender) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	if a.file == nil {
		return nil
	}
	file := a.file
	a.file = nil
	return file.Close()
}

func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}

func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}
