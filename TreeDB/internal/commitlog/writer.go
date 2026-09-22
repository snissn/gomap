package commitlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/limits"
)

const (
	defaultBufferSize                = 4 << 20
	defaultMaxSegmentSize            = 64 * 1024 * 1024
	defaultCompressMinLen            = 64 << 10
	directSegmentPayloadMinLen       = 128 << 10
	directCommandPayloadMinLen       = 64 << 10
	batchChecksumChunkBytes          = 64 << 10
	pendingAppendBatchInitialPayload = 64 << 10
	pendingAppendBatchMaxPayload     = defaultBufferSize - segmentHeaderSize
)

var syncDirFn = syncDir

func openLogFile(path string, flags int) (*os.File, error) {
	created, err := os.OpenFile(path, flags|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		if observeErr := durabilitycut.EmitNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceCommandWAL, filepath.Dir(path), "", path); observeErr != nil {
			_ = created.Close()
			return nil, observeErr
		}
		return created, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	return os.OpenFile(path, flags|os.O_CREATE, 0o600)
}

func syncNewLogFileDirectory(w *Writer, file *os.File, path string) error {
	dir := filepath.Dir(path)
	if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceCommandWAL, "", dir); err != nil {
		return err
	}
	if err := w.syncCreatedFileNamespace(file, path); err != nil {
		return err
	}
	return durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceCommandWAL, "", dir)
}

func normalizeMaxSegmentSize(size int64) int64 {
	if size == 0 {
		return defaultMaxSegmentSize
	}
	if size < 0 {
		return 0
	}
	return size
}

func normalizeBufferSize(size int) int {
	if size <= 0 {
		return defaultBufferSize
	}
	return size
}

func recordSizeExceedsMaxWithHeader(headerSize int, keyLen uint16, valueLen uint32) bool {
	if limits.MaxRecordSize <= 0 {
		return false
	}
	recordLen := int64(headerSize) + int64(keyLen) + int64(valueLen)
	return recordLen > limits.MaxRecordSize
}

func recordSizeExceedsMax(keyLen uint16, valueLen uint32) bool {
	return recordSizeExceedsMaxWithHeader(recordHeaderSize, keyLen, valueLen)
}

func recordNeedsRevisionEncoding(r *Record, batchSeq uint64) bool {
	return r != nil && r.Revision != 0 && r.Revision != batchSeq
}

func batchNeedsRevisionEncoding(records []Record, batchSeq uint64) (bool, error) {
	needsRevision := false
	for i := range records {
		if records[i].Seq != batchSeq {
			return false, ErrMixedBatchSeq
		}
		if recordNeedsRevisionEncoding(&records[i], batchSeq) {
			needsRevision = true
		}
	}
	return needsRevision, nil
}

func allZeroBytes(p []byte) bool {
	for _, b := range p {
		if b != 0 {
			return false
		}
	}
	return true
}

func sameNonEmptyBytesData(a, b []byte) bool {
	return len(a) > 0 && len(a) == len(b) && &a[0] == &b[0]
}

type Writer struct {
	f                      *os.File
	bw                     *bufio.Writer
	fileSink               writerFileSink
	scratch                []byte
	encScratch             []byte
	commandBuf             []byte
	commandBufLimit        int
	commandBufRetain       int
	commandBufTrims        uint64
	commandBufDroppedBytes uint64
	commandErr             error
	pendingBatch           []byte
	pendingBatchSeq        uint64
	pendingBatchRecs       uint32
	headerBuf              [segmentHeaderSize]byte
	rawLenPrefix           [4]byte
	size                   int64
	maxSegmentSize         int64
	compress               bool
	enc                    *zstd.Encoder
	syncFn                 func(*os.File) error
	closeRotateFn          func(*os.File) error
	fileSyncCalls          atomic.Uint64
	fileSyncNs             atomic.Uint64
	fileSyncErrors         atomic.Uint64
	directorySyncCalls     atomic.Uint64
	directorySyncNs        atomic.Uint64
	directorySyncErrors    atomic.Uint64
	writeSyscalls          atomic.Uint64
	writeBytes             atomic.Uint64
	writeNs                atomic.Uint64
	writeErrors            atomic.Uint64
}

type writerFileSink struct {
	owner *Writer
	file  *os.File
}

func (s *writerFileSink) Write(p []byte) (int, error) {
	if s == nil || s.file == nil {
		return 0, os.ErrInvalid
	}
	start := time.Now()
	n, err := s.file.Write(p)
	if s.owner != nil {
		s.owner.observeWriteSyscalls(1, uint64(max(n, 0)), time.Since(start), err)
	}
	return n, err
}

type WriterBufferStats struct {
	BufferedWriterSize          int
	BufferedWriterBufferedBytes int
	ScratchCapacity             int
	CommandBufferLength         int
	CommandBufferCapacity       int
	CommandBufferLimit          int
	CommandBufferRetainLimit    int
	CommandBufferTrimCount      uint64
	CommandBufferDroppedBytes   uint64
	PendingBatchLength          int
	PendingBatchCapacity        int
}

// DurabilityStats reports underlying writer calls plus injected durability
// boundaries. The production hooks are os.File.Sync for FileSync and a parent
// directory os.File.Sync for DirectorySync. Keeping sync counters at the hook
// boundary makes tests deterministic while Linux strace can validate the
// production write/writev and sync mappings.
type DurabilityStats struct {
	WriteSyscalls       uint64
	WriteBytes          uint64
	WriteNs             uint64
	WriteErrors         uint64
	FileSyncCalls       uint64
	FileSyncNs          uint64
	FileSyncErrors      uint64
	DirectorySyncCalls  uint64
	DirectorySyncNs     uint64
	DirectorySyncErrors uint64
}

func (w *Writer) ActiveBytes() int64 {
	if w == nil {
		return 0
	}
	buffered := int64(0)
	buffered += int64(len(w.commandBuf))
	if w.pendingBatchRecs != 0 {
		buffered += int64(segmentHeaderSize + len(w.pendingBatch))
	}
	return w.size + buffered
}

func NewWriter(path string) (*Writer, error) {
	return NewWriterWithOptions(path, Options{})
}

func NewWriterWithOptions(path string, opts Options) (*Writer, error) {
	f, err := openLogFile(path, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		f:      f,
		syncFn: func(file *os.File) error { return file.Sync() },
	}
	w.fileSink = writerFileSink{owner: w, file: f}
	if err := syncNewLogFileDirectory(w, f, path); err != nil {
		_ = f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(info.Size(), io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	var commandBuf []byte
	commandBufLimit := 0
	if opts.DeferredCommandBufferSize > 0 {
		commandBufLimit = opts.DeferredCommandBufferSize
	}
	commandBufRetain := normalizeCommandBufferRetainSize(opts.DeferredCommandBufferRetainSize, commandBufLimit)
	w.bw = bufio.NewWriterSize(&w.fileSink, normalizeBufferSize(opts.BufferSize))
	w.commandBuf = commandBuf
	w.commandBufLimit = commandBufLimit
	w.commandBufRetain = commandBufRetain
	w.size = info.Size()
	w.maxSegmentSize = normalizeMaxSegmentSize(opts.MaxSegmentSize)
	w.compress = opts.Compress
	return w, nil
}

// DurabilityStats returns a lock-free cumulative snapshot. Writer callers
// already serialize mutations; atomics keep diagnostic snapshots race-safe.
func (w *Writer) DurabilityStats() DurabilityStats {
	if w == nil {
		return DurabilityStats{}
	}
	return DurabilityStats{
		WriteSyscalls:       w.writeSyscalls.Load(),
		WriteBytes:          w.writeBytes.Load(),
		WriteNs:             w.writeNs.Load(),
		WriteErrors:         w.writeErrors.Load(),
		FileSyncCalls:       w.fileSyncCalls.Load(),
		FileSyncNs:          w.fileSyncNs.Load(),
		FileSyncErrors:      w.fileSyncErrors.Load(),
		DirectorySyncCalls:  w.directorySyncCalls.Load(),
		DirectorySyncNs:     w.directorySyncNs.Load(),
		DirectorySyncErrors: w.directorySyncErrors.Load(),
	}
}

func (w *Writer) observeWriteSyscalls(calls, bytes uint64, elapsed time.Duration, err error) {
	if w == nil {
		return
	}
	if calls > 0 {
		w.writeSyscalls.Add(calls)
	}
	if bytes > 0 {
		w.writeBytes.Add(bytes)
	}
	if ns := elapsed.Nanoseconds(); calls > 0 && ns > 0 {
		w.writeNs.Add(uint64(ns))
	}
	if err != nil {
		w.writeErrors.Add(1)
	}
}

func (w *Writer) writev(parts [][]byte) error {
	start := time.Now()
	stats, err := writevFull(w.f, parts)
	w.observeWriteSyscalls(stats.syscalls, stats.bytes, time.Since(start), err)
	return err
}

func (w *Writer) syncFile() error {
	if w == nil || w.f == nil {
		return nil
	}
	start := time.Now()
	var err error
	if w.syncFn != nil {
		err = w.syncFn(w.f)
	} else {
		err = w.f.Sync()
	}
	w.fileSyncCalls.Add(1)
	if ns := time.Since(start).Nanoseconds(); ns > 0 {
		w.fileSyncNs.Add(uint64(ns))
	}
	if err != nil {
		w.fileSyncErrors.Add(1)
	}
	return err
}

func (w *Writer) syncDirectory(path string) error {
	if w == nil {
		return syncDirFn(path)
	}
	start := time.Now()
	err := syncDirFn(path)
	w.directorySyncCalls.Add(1)
	if ns := time.Since(start).Nanoseconds(); ns > 0 {
		w.directorySyncNs.Add(uint64(ns))
	}
	if err != nil {
		w.directorySyncErrors.Add(1)
	}
	return err
}

// syncCreatedFileNamespace persists a just-created log name. Unix-like
// systems do that through the parent directory. Windows has no equivalent
// retained-directory flush contract, but FlushFileBuffers on the exact child
// persists file metadata; os.File.Sync maps to that primitive. Count both as
// namespace syncs because that is the durability obligation being discharged.
func (w *Writer) syncCreatedFileNamespace(file *os.File, path string) error {
	if runtime.GOOS != "windows" {
		return w.syncDirectory(path)
	}
	if file == nil {
		return os.ErrInvalid
	}
	start := time.Now()
	var err error
	if w != nil && w.syncFn != nil {
		err = w.syncFn(file)
	} else {
		err = file.Sync()
	}
	if w != nil {
		w.directorySyncCalls.Add(1)
		if ns := time.Since(start).Nanoseconds(); ns > 0 {
			w.directorySyncNs.Add(uint64(ns))
		}
		if err != nil {
			w.directorySyncErrors.Add(1)
		}
	}
	return err
}

func normalizeCommandBufferRetainSize(retain, limit int) int {
	if retain <= 0 || limit <= 0 {
		return 0
	}
	if retain > limit {
		return limit
	}
	return retain
}

func (w *Writer) BufferStats() WriterBufferStats {
	if w == nil {
		return WriterBufferStats{}
	}
	stats := WriterBufferStats{
		ScratchCapacity:           cap(w.scratch),
		CommandBufferLength:       len(w.commandBuf),
		CommandBufferCapacity:     cap(w.commandBuf),
		CommandBufferLimit:        w.commandBufferLimit(),
		CommandBufferRetainLimit:  w.commandBufRetain,
		CommandBufferTrimCount:    w.commandBufTrims,
		CommandBufferDroppedBytes: w.commandBufDroppedBytes,
		PendingBatchLength:        len(w.pendingBatch),
		PendingBatchCapacity:      cap(w.pendingBatch),
	}
	if w.bw != nil {
		stats.BufferedWriterSize = w.bw.Size()
		stats.BufferedWriterBufferedBytes = w.bw.Buffered()
	}
	return stats
}

func newEncoder() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(false),
		zstd.WithNoEntropyCompression(true),
	)
}

func (w *Writer) ensureEncoder() error {
	if w == nil || !w.compress || w.enc != nil {
		return nil
	}
	enc, err := newEncoder()
	if err != nil {
		return err
	}
	w.enc = enc
	return nil
}

func (w *Writer) pendingBatchBytes() int64 {
	if w == nil || w.pendingBatchRecs == 0 {
		return 0
	}
	return int64(segmentHeaderSize + len(w.pendingBatch))
}

func (w *Writer) pendingAppendBatchLimit(recLen int) int {
	limit := pendingAppendBatchMaxPayload
	if w != nil && w.maxSegmentSize > 0 && int64(limit) > w.maxSegmentSize {
		limit = int(w.maxSegmentSize)
	}
	if limit > int(segmentLenMask) {
		limit = int(segmentLenMask)
	}
	minNeed := batchHeaderSize + recLen
	if limit < minNeed {
		limit = minNeed
	}
	return limit
}

func (w *Writer) appendPendingRecord(record Record, keyLen uint16, valLen uint32) error {
	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	recLen := recordHeaderSize + len(record.Key) + len(record.Value)
	limit := w.pendingAppendBatchLimit(recLen)
	if w.pendingBatchRecs > 0 && w.pendingBatchSeq != record.Seq {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	if w.pendingBatchRecs > 0 && (w.pendingBatchRecs == ^uint32(0) || len(w.pendingBatch)+recLen > limit) {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	if w.pendingBatchRecs == 0 {
		need := batchHeaderSize + recLen
		capHint := pendingAppendBatchInitialPayload
		if capHint < need {
			capHint = need
		}
		if capHint > limit {
			capHint = limit
		}
		if cap(w.pendingBatch) < need {
			w.pendingBatch = make([]byte, batchHeaderSize, capHint)
		} else {
			w.pendingBatch = w.pendingBatch[:batchHeaderSize]
		}
		w.pendingBatch[0] = Version
		binary.LittleEndian.PutUint32(w.pendingBatch[1:5], 0)
		w.pendingBatchSeq = record.Seq
	}

	off := len(w.pendingBatch)
	next := off + recLen
	if next > cap(w.pendingBatch) {
		newCap := cap(w.pendingBatch) * 2
		if newCap < next {
			newCap = next
		}
		if newCap > limit {
			newCap = limit
		}
		nextBuf := make([]byte, next, newCap)
		copy(nextBuf, w.pendingBatch)
		w.pendingBatch = nextBuf
	} else {
		w.pendingBatch = w.pendingBatch[:next]
	}
	buf := w.pendingBatch
	buf[off] = record.Op
	binary.LittleEndian.PutUint16(buf[off+1:off+3], keyLen)
	binary.LittleEndian.PutUint32(buf[off+3:off+7], valLen)
	binary.LittleEndian.PutUint64(buf[off+7:off+15], record.RID)
	binary.LittleEndian.PutUint64(buf[off+15:off+23], record.Seq)
	copy(buf[off+recordHeaderSize:], record.Key)
	copy(buf[off+recordHeaderSize+len(record.Key):], record.Value)
	w.pendingBatchRecs++
	binary.LittleEndian.PutUint32(buf[1:5], w.pendingBatchRecs)
	return nil
}

func (w *Writer) flushPendingBatch() error {
	if w == nil || w.pendingBatchRecs == 0 {
		return nil
	}
	payload := w.pendingBatch
	if err := w.writeRawSegmentWithChecksumNoPending(payload, crc.Checksum(payload)); err != nil {
		return err
	}
	w.pendingBatch = w.pendingBatch[:0]
	w.pendingBatchSeq = 0
	w.pendingBatchRecs = 0
	return nil
}

// RotateTo flushes and closes the current file, then opens (or creates) the
// provided path and reuses the writer's buffers for future appends.
func (w *Writer) RotateTo(path string) error {
	return w.RotateToWithSync(path, true)
}

// RotateToWithSync flushes and closes the current file, then opens (or creates)
// the provided path and reuses the writer's buffers for future appends. When
// syncCurrent is false, the current file is flushed to the OS but not fsynced.
func (w *Writer) RotateToWithSync(path string, syncCurrent bool) error {
	outcome, err := w.rotateToWithSyncObserved(path, syncCurrent, "", false)
	if outcome.Installed && err != nil {
		return rotationInstalledError{err: err}
	}
	return err
}

type rotationInstalledMarker interface {
	RotationInstalled() bool
}

type rotationInstalledError struct {
	err error
}

func (err rotationInstalledError) Error() string { return err.err.Error() }
func (err rotationInstalledError) Unwrap() error { return err.err }
func (rotationInstalledError) RotationInstalled() bool {
	return true
}

// RotationInstalled reports whether a rotation error happened only after the
// successor became the writer's authoritative file. Callers must finish their
// corresponding metadata transition before propagating such an error.
func RotationInstalled(err error) bool {
	var marker rotationInstalledMarker
	return errors.As(err, &marker) && marker.RotationInstalled()
}

type writerRotationOutcome struct {
	Installed bool
}

func (w *Writer) rotateToWithSyncObserved(path string, syncCurrent bool, root string, observe bool) (writerRotationOutcome, error) {
	if w == nil {
		return writerRotationOutcome{}, errors.New("commitlog: nil writer")
	}

	if w.f == nil {
		f, err := openLogFile(path, os.O_RDWR|os.O_APPEND)
		if err != nil {
			return writerRotationOutcome{}, err
		}
		if syncCurrent {
			if err := syncNewLogFileDirectory(w, f, path); err != nil {
				_ = f.Close()
				return writerRotationOutcome{}, err
			}
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return writerRotationOutcome{}, err
		}
		w.f = f
		w.fileSink.file = f
		w.bw.Reset(&w.fileSink)
		w.size = info.Size()
		w.scratch = w.scratch[:0]
		return writerRotationOutcome{Installed: true}, nil
	}

	before, after := durabilitycut.BeforeUserspaceFlush, durabilitycut.AfterUserspaceFlush
	if syncCurrent {
		before, after = durabilitycut.BeforeDependencyFileSync, durabilitycut.AfterDependencyFileSync
	}
	if observe {
		if err := durabilitycut.EmitPath(before, durabilitycut.ResourceCommandWAL, root, w.f.Name()); err != nil {
			return writerRotationOutcome{}, err
		}
	}
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return writerRotationOutcome{}, err
		}
	}
	if err := w.flushBufferedCommandFrames(); err != nil {
		return writerRotationOutcome{}, err
	}
	if err := w.bw.Flush(); err != nil {
		return writerRotationOutcome{}, err
	}
	if syncCurrent {
		if err := w.syncFile(); err != nil {
			return writerRotationOutcome{}, err
		}
	}
	if observe {
		if err := durabilitycut.EmitPath(after, durabilitycut.ResourceCommandWAL, root, w.f.Name()); err != nil {
			return writerRotationOutcome{}, err
		}
	}

	f, err := openLogFile(path, os.O_RDWR|os.O_APPEND)
	if err != nil {
		return writerRotationOutcome{}, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return writerRotationOutcome{}, err
	}
	if syncCurrent {
		if err := syncNewLogFileDirectory(w, f, path); err != nil {
			_ = f.Close()
			return writerRotationOutcome{}, err
		}
	}

	old := w.f
	w.f = f
	w.fileSink.file = f
	w.bw.Reset(&w.fileSink)
	w.size = info.Size()
	w.scratch = w.scratch[:0]
	return writerRotationOutcome{Installed: true}, w.closeRotatedResource(old)
}

func (w *Writer) closeRotatedResource(file *os.File) error {
	if file == nil {
		return nil
	}
	if w.closeRotateFn != nil {
		return w.closeRotateFn(file)
	}
	return file.Close()
}

func syncDir(path string) (err error) {
	if runtime.GOOS == "windows" || path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *Writer) Append(record Record) error {
	if err := validateRecord(&record); err != nil {
		return err
	}
	if len(record.Key) > int(^uint16(0)) {
		return ErrRecordTooLarge
	}
	if len(record.Value) > int(^uint32(0)) {
		return ErrRecordTooLarge
	}

	keyLen := uint16(len(record.Key))
	valLen := uint32(len(record.Value))

	headerSize := recordHeaderSize
	version := byte(Version)
	if recordNeedsRevisionEncoding(&record, record.Seq) {
		headerSize = recordRevisionHeaderSize
		version = recordRevisionBatchVersion
	}
	if recordSizeExceedsMaxWithHeader(headerSize, keyLen, valLen) {
		return ErrRecordTooLarge
	}

	total := int64(batchHeaderSize) + int64(headerSize) + int64(len(record.Key)) + int64(len(record.Value))
	if w.maxSegmentSize > 0 && total > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if total > int64(int(^uint(0)>>1)) {
		return ErrRecordTooLarge
	}

	payloadLen := int(total)
	if payloadLen > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	if version == Version && !w.compress && segmentHeaderSize+payloadLen < directSegmentPayloadMinLen {
		return w.appendPendingRecord(record, keyLen, valLen)
	}

	if cap(w.scratch) < payloadLen {
		w.scratch = make([]byte, payloadLen)
	}
	buf := w.scratch[:payloadLen]

	buf[0] = version
	binary.LittleEndian.PutUint32(buf[1:5], 1)

	off := batchHeaderSize
	buf[off] = record.Op
	binary.LittleEndian.PutUint16(buf[off+1:off+3], keyLen)
	binary.LittleEndian.PutUint32(buf[off+3:off+7], valLen)
	binary.LittleEndian.PutUint64(buf[off+7:off+15], record.RID)
	binary.LittleEndian.PutUint64(buf[off+15:off+23], record.Seq)
	if version == recordRevisionBatchVersion {
		binary.LittleEndian.PutUint64(buf[off+23:off+31], record.Revision)
	}
	copy(buf[off+headerSize:], record.Key)
	copy(buf[off+headerSize+len(record.Key):], record.Value)

	if !w.compress {
		return w.writeRawSegmentWithChecksum(buf, crc.ChecksumParts(buf[:batchHeaderSize], buf[batchHeaderSize:]))
	}
	return w.writeSegment(buf)
}

func (w *Writer) AppendBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	if len(records) > int(^uint32(0)) {
		return ErrRecordTooLarge
	}

	batchSeq := records[0].Seq
	needsRevision, err := batchNeedsRevisionEncoding(records, batchSeq)
	if err != nil {
		return err
	}
	if !needsRevision && !w.compress {
		if ok, err := w.appendZeroInlineBatchIfCompact(records, batchSeq); ok || err != nil {
			return err
		}
	}
	headerSize := recordHeaderSize
	version := byte(Version)
	if needsRevision {
		headerSize = recordRevisionHeaderSize
		version = recordRevisionBatchVersion
	}
	if cap(w.scratch) < batchHeaderSize {
		w.scratch = make([]byte, batchHeaderSize)
	}
	buf := w.scratch
	if len(buf) < batchHeaderSize {
		buf = buf[:batchHeaderSize]
	}
	buf[0] = version
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(records)))

	off := batchHeaderSize
	checksum := uint32(0)
	checksumStart := 0
	var zeroValueRef []byte
	for i := range records {
		r := &records[i]
		if err := validateRecord(r); err != nil {
			return err
		}
		if len(r.Key) > int(^uint16(0)) {
			return ErrRecordTooLarge
		}
		if len(r.Value) > int(^uint32(0)) {
			return ErrRecordTooLarge
		}
		keyLen := uint16(len(r.Key))
		valLen := uint32(len(r.Value))
		if recordSizeExceedsMaxWithHeader(headerSize, keyLen, valLen) {
			return ErrRecordTooLarge
		}

		encodedOp := r.Op
		writeValue := len(r.Value) > 0
		if r.Op == OpSetInline && len(r.Value) > 0 {
			if sameNonEmptyBytesData(r.Value, zeroValueRef) || allZeroBytes(r.Value) {
				zeroValueRef = r.Value
				encodedOp = OpSetInlineZero
				writeValue = false
			}
		}

		recLen := headerSize + len(r.Key)
		if writeValue {
			recLen += len(r.Value)
		}
		if off > int(^uint(0)>>1)-recLen {
			return ErrRecordTooLarge
		}
		next := off + recLen
		if w.maxSegmentSize > 0 && int64(next) > w.maxSegmentSize {
			return ErrRecordTooLarge
		}
		if next > int(segmentLenMask) {
			return ErrRecordTooLarge
		}
		if next > cap(buf) {
			newCap := cap(buf) * 2
			if newCap < next {
				newCap = next
			}
			nextBuf := make([]byte, next, newCap)
			copy(nextBuf, buf[:off])
			buf = nextBuf
		} else if next > len(buf) {
			buf = buf[:next]
		}
		buf[off] = encodedOp
		binary.LittleEndian.PutUint16(buf[off+1:off+3], keyLen)
		binary.LittleEndian.PutUint32(buf[off+3:off+7], valLen)
		binary.LittleEndian.PutUint64(buf[off+7:off+15], r.RID)
		binary.LittleEndian.PutUint64(buf[off+15:off+23], r.Seq)
		if needsRevision {
			binary.LittleEndian.PutUint64(buf[off+23:off+31], r.Revision)
		}
		copy(buf[off+headerSize:], r.Key)
		if writeValue {
			valueStart := off + headerSize + len(r.Key)
			valueEnd := valueStart + len(r.Value)
			copy(buf[valueStart:valueEnd], r.Value)
		}
		off = next
		if !w.compress && off-checksumStart >= batchChecksumChunkBytes {
			checksum = crc.Update(checksum, buf[checksumStart:off])
			checksumStart = off
		}
	}

	w.scratch = buf[:off]
	if !w.compress {
		checksum = crc.Update(checksum, w.scratch[checksumStart:])
		return w.writeRawSegmentWithChecksum(w.scratch, checksum)
	}
	return w.writeSegment(w.scratch)
}

func (w *Writer) appendZeroInlineBatchIfCompact(records []Record, batchSeq uint64) (bool, error) {
	valueLen := -1
	var zeroValueRef []byte
	total := int64(zeroInlineBatchHeaderSize)
	for i := range records {
		r := &records[i]
		if recordNeedsRevisionEncoding(r, batchSeq) {
			return false, nil
		}
		if r.Op != OpSetInline {
			return false, nil
		}
		if err := validateRecord(r); err != nil {
			return true, err
		}
		if r.Seq != batchSeq {
			return true, ErrMixedBatchSeq
		}
		if len(r.Key) > int(^uint16(0)) {
			return true, ErrRecordTooLarge
		}
		if len(r.Value) > int(^uint32(0)) {
			return true, ErrRecordTooLarge
		}
		keyLen := uint16(len(r.Key))
		valLen := uint32(len(r.Value))
		if recordSizeExceedsMax(keyLen, valLen) {
			return true, ErrRecordTooLarge
		}
		if len(r.Value) == 0 {
			return false, nil
		}
		if valueLen < 0 {
			valueLen = len(r.Value)
		} else if valueLen != len(r.Value) {
			return false, nil
		}
		if sameNonEmptyBytesData(r.Value, zeroValueRef) {
			// Same immutable value buffer as the first zero value in this batch.
		} else if allZeroBytes(r.Value) {
			zeroValueRef = r.Value
		} else {
			return false, nil
		}
		total += int64(zeroInlineRecordHeaderSize) + int64(len(r.Key))
		if w.maxSegmentSize > 0 && total > w.maxSegmentSize {
			return true, ErrRecordTooLarge
		}
		if total > int64(segmentLenMask) || total > int64(int(^uint(0)>>1)) {
			return true, ErrRecordTooLarge
		}
	}
	if valueLen < 0 {
		return false, nil
	}
	payloadLen := int(total)
	if cap(w.scratch) < payloadLen {
		w.scratch = make([]byte, payloadLen)
	}
	buf := w.scratch[:payloadLen]
	buf[0] = zeroInlineBatchVersion
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(records)))
	binary.LittleEndian.PutUint64(buf[5:13], batchSeq)
	binary.LittleEndian.PutUint32(buf[13:17], uint32(valueLen))

	off := zeroInlineBatchHeaderSize
	checksum := uint32(0)
	checksumStart := 0
	for i := range records {
		r := &records[i]
		keyLen := uint16(len(r.Key))
		binary.LittleEndian.PutUint16(buf[off:off+2], keyLen)
		off += zeroInlineRecordHeaderSize
		copy(buf[off:off+len(r.Key)], r.Key)
		off += len(r.Key)
		if off-checksumStart >= batchChecksumChunkBytes {
			checksum = crc.Update(checksum, buf[checksumStart:off])
			checksumStart = off
		}
	}
	checksum = crc.Update(checksum, buf[checksumStart:])
	w.scratch = buf
	return true, w.writeRawSegmentWithChecksum(buf, checksum)
}

func (w *Writer) AppendZeroInlineBatchFunc(count int, seq uint64, valueLen int, keyAt func(int) []byte) error {
	if count == 0 {
		return nil
	}
	if count < 0 || count > int(^uint32(0)) || valueLen <= 0 || valueLen > int(^uint32(0)) {
		return ErrRecordTooLarge
	}
	if cap(w.scratch) < zeroInlineBatchHeaderSize {
		w.scratch = make([]byte, zeroInlineBatchHeaderSize)
	}
	buf := w.scratch
	if len(buf) < zeroInlineBatchHeaderSize {
		buf = buf[:zeroInlineBatchHeaderSize]
	}
	buf[0] = zeroInlineBatchVersion
	binary.LittleEndian.PutUint32(buf[1:5], uint32(count))
	binary.LittleEndian.PutUint64(buf[5:13], seq)
	binary.LittleEndian.PutUint32(buf[13:17], uint32(valueLen))

	off := zeroInlineBatchHeaderSize
	checksum := uint32(0)
	checksumStart := 0
	for i := 0; i < count; i++ {
		key := keyAt(i)
		if len(key) > int(^uint16(0)) {
			return ErrRecordTooLarge
		}
		keyLen := uint16(len(key))
		if recordSizeExceedsMax(keyLen, uint32(valueLen)) {
			return ErrRecordTooLarge
		}
		recLen := zeroInlineRecordHeaderSize + len(key)
		if off > int(^uint(0)>>1)-recLen {
			return ErrRecordTooLarge
		}
		next := off + recLen
		if w.maxSegmentSize > 0 && int64(next) > w.maxSegmentSize {
			return ErrRecordTooLarge
		}
		if next > int(segmentLenMask) {
			return ErrRecordTooLarge
		}
		if next > cap(buf) {
			newCap := cap(buf) * 2
			if newCap < next {
				newCap = next
			}
			nextBuf := make([]byte, next, newCap)
			copy(nextBuf, buf[:off])
			buf = nextBuf
		} else if next > len(buf) {
			buf = buf[:next]
		}
		binary.LittleEndian.PutUint16(buf[off:off+2], keyLen)
		off += zeroInlineRecordHeaderSize
		copy(buf[off:off+len(key)], key)
		off += len(key)
		if off-checksumStart >= batchChecksumChunkBytes {
			checksum = crc.Update(checksum, buf[checksumStart:off])
			checksumStart = off
		}
	}
	checksum = crc.Update(checksum, buf[checksumStart:])
	w.scratch = buf[:off]
	return w.writeRawSegmentWithChecksum(w.scratch, checksum)
}

func (w *Writer) AppendCommand(env CommandEnvelope) error {
	if env.Version == CommandFrameVersionV2 || env.DurabilityClass != 0 {
		return w.AppendCommandV2(env)
	}
	size, err := commandFrameEncodedSize(env)
	if err != nil {
		return err
	}
	if w.maxSegmentSize > 0 && int64(size) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	payload, err := encodeCommandFrameTo(w.scratch[:0], env)
	if err != nil {
		return err
	}
	w.scratch = payload
	return w.writeSegment(payload)
}

// AppendCommandV2 appends one strict V2 command envelope. The generic path is
// intentionally shared by every command kind so activation cannot leave a V1
// fast-path escape hatch; specialized allocation-free V2 encoders may replace
// it without changing the persisted contract.
func (w *Writer) AppendCommandV2(env CommandEnvelope) error {
	size, err := commandFrameV2EncodedSize(env)
	if err != nil {
		return err
	}
	if w.maxSegmentSize > 0 && int64(size) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	payload, err := EncodeCommandFrameV2To(w.scratch[:0], env)
	if err != nil {
		return err
	}
	if len(payload) != size {
		return ErrCorrupt
	}
	w.scratch = payload
	// Strict V2 readers reject compressed segment storage so recovery can
	// inspect frame identity directly at a torn terminal tail. Keep V2 command
	// frames raw even when compression remains enabled for other journal data.
	err = w.writeRawSegmentWithChecksum(payload, crc.Checksum(payload))
	if w.commandBufRetain > 0 && cap(w.scratch) > w.commandBufRetain {
		w.scratch = make([]byte, 0, w.commandBufRetain)
	}
	return err
}

func (w *Writer) AppendRawKVSingleCommandDirect(lsn, baseAppliedLSN uint64, op RawKVOperation) error {
	if op.Op == RawKVOpSetMaterializedRID {
		return ErrCommandWALUnsupportedVersion
	}
	valueLen := len(op.Value)
	if op.Op == RawKVOpSetRID {
		valueLen = 8
	}
	payloadLen := rawKVBatchHeaderSize + rawKVOpHeaderSize + len(op.Key) + valueLen
	size, err := commandFrameEncodedSizeFromLengths(payloadLen, 0, 0, 0)
	if err != nil {
		return err
	}
	if w.maxSegmentSize > 0 && int64(size) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	total := segmentHeaderSize + size
	if cap(w.scratch) < total {
		w.scratch = make([]byte, total)
	}
	buf := w.scratch[:total]
	frame, err := encodeRawKVSingleCommandFrameTo(buf[segmentHeaderSize:segmentHeaderSize:total], lsn, baseAppliedLSN, op)
	if err != nil {
		return err
	}
	frame = frame[:size]
	binary.LittleEndian.PutUint32(buf[0:4], uint32(size))
	binary.LittleEndian.PutUint32(buf[4:8], crc.Checksum(frame))
	if _, err := w.bw.Write(buf); err != nil {
		return err
	}
	w.size += int64(total)
	return nil
}

// AppendRawKVPointCommandDirectTrusted appends a public point Set/Delete command
// whose key/value have already passed the public cached preflight.
func (w *Writer) AppendRawKVPointCommandDirectTrusted(lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte) error {
	if err := w.commandBufferError(); err != nil {
		return err
	}
	valueLen, payloadLen, size, err := trustedRawKVPointCommandFrameLens(op, key, value, 0)
	if err != nil {
		return err
	}
	if w.maxSegmentSize > 0 && int64(size) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	return w.appendRawKVPointCommandDirectTrustedSized(lsn, baseAppliedLSN, op, key, value, valueLen, payloadLen, size)
}

func (w *Writer) appendRawKVPointCommandDirectTrustedSized(lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte, valueLen, payloadLen, size int) error {
	return w.appendRawKVPointCommandDirectTrustedSizedWithRevision(lsn, baseAppliedLSN, op, key, value, 0, valueLen, payloadLen, size)
}

func (w *Writer) appendRawKVPointCommandDirectTrustedSizedWithRevision(lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte, revision uint64, valueLen, payloadLen, size int) error {
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	total := segmentHeaderSize + size
	if !w.canBufferCommandFrame(total) {
		if cap(w.scratch) < total {
			w.scratch = make([]byte, total)
		}
		buf := w.scratch[:total]
		frame, err := encodeTrustedRawKVPointCommandFrameWithRevisionSizedTo(buf[segmentHeaderSize:segmentHeaderSize+size], lsn, baseAppliedLSN, op, key, value, revision, valueLen, payloadLen, size)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(buf[0:4], uint32(size))
		binary.LittleEndian.PutUint32(buf[4:8], crc.Checksum(frame))
		if _, err := w.bw.Write(buf); err != nil {
			return err
		}
		w.size += int64(total)
		return nil
	}
	off, err := w.reserveCommandBufferSpace(total)
	if err != nil {
		return err
	}
	newLen := off + total
	buf := w.commandBuf[off:newLen]
	encodeTrustedRawKVPointCommandFramePayloadSizedWithRevisionTo(buf[segmentHeaderSize:segmentHeaderSize+size], lsn, baseAppliedLSN, op, key, value, revision, valueLen, payloadLen, size)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(size))
	return nil
}

func (w *Writer) AppendRawKVBatchPayloadCommandDirect(lsn, baseAppliedLSN uint64, payload []byte) error {
	if err := validateRawKVBatchPayload(payload); err != nil {
		return err
	}
	return w.AppendRawKVBatchPayloadCommandDirectTrusted(lsn, baseAppliedLSN, payload)
}

// AppendRawKVBatchPayloadCommandDirectTrusted appends a canonical RawKVBatch
// payload that the caller has already validated or constructed through the
// RawKVBatchPayloadBuilder.
func (w *Writer) AppendRawKVBatchPayloadCommandDirectTrusted(lsn, baseAppliedLSN uint64, payload []byte) error {
	if err := w.commandBufferError(); err != nil {
		return err
	}
	if lsn == 0 {
		return fmt.Errorf("%w: zero lsn", ErrCorrupt)
	}
	size, err := commandFrameEncodedSizeFromLengths(len(payload), 0, 0, 0)
	if err != nil {
		return err
	}
	if w.maxSegmentSize > 0 && int64(size) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	total := segmentHeaderSize + size
	if len(payload) >= directCommandPayloadMinLen {
		return w.appendRawKVBatchPayloadCommandDirect(lsn, baseAppliedLSN, payload, size)
	}
	if w.canBufferCommandFrame(total) {
		off, err := w.reserveCommandBufferSpace(total)
		if err != nil {
			return err
		}
		newLen := off + total
		buf := w.commandBuf[off:newLen]
		frameHeader := buf[segmentHeaderSize : segmentHeaderSize+commandFrameHeaderSize]
		copy(frameHeader, rawKVCommandFrameHeaderTemplate[:])
		binary.LittleEndian.PutUint64(frameHeader[20:28], lsn)
		binary.LittleEndian.PutUint64(frameHeader[44:52], baseAppliedLSN)
		binary.LittleEndian.PutUint32(frameHeader[56:60], uint32(len(payload)))
		copy(buf[segmentHeaderSize+commandFrameHeaderSize:], payload)
		binary.LittleEndian.PutUint32(buf[0:4], uint32(size))
		return nil
	}
	return w.appendRawKVBatchPayloadCommandDirect(lsn, baseAppliedLSN, payload, size)
}

// AppendRawKVBatchPayloadScanCommandDirectTrusted appends a RawKVBatch command
// from a replayable operation scanner without materializing the canonical
// payload slice first.
func (w *Writer) AppendRawKVBatchPayloadScanCommandDirectTrusted(lsn, baseAppliedLSN uint64, plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner) error {
	if err := w.commandBufferError(); err != nil {
		return err
	}
	if lsn == 0 {
		return fmt.Errorf("%w: zero lsn", ErrCorrupt)
	}
	if err := plan.validate(); err != nil {
		return err
	}
	size, err := commandFrameEncodedSizeFromLengths(plan.PayloadLen, 0, 0, 0)
	if err != nil {
		return err
	}
	if w.maxSegmentSize > 0 && int64(size) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	total := segmentHeaderSize + size
	if plan.PayloadLen >= directCommandPayloadMinLen {
		return w.appendRawKVBatchPayloadScanCommandDirect(lsn, baseAppliedLSN, plan, scan, size)
	}
	if w.canBufferCommandFrame(total) {
		off, err := w.reserveCommandBufferSpace(total)
		if err != nil {
			return err
		}
		newLen := off + total
		buf := w.commandBuf[off:newLen]
		frame := buf[segmentHeaderSize : segmentHeaderSize+size]
		fillRawKVCommandFrameHeader(frame[:commandFrameHeaderSize], lsn, baseAppliedLSN, plan.PayloadLen)
		n, err := writeRawKVBatchPayloadTo(frame[commandFrameHeaderSize:], plan, scan)
		if err != nil {
			w.commandBuf = w.commandBuf[:off]
			return err
		}
		if n != plan.PayloadLen {
			w.commandBuf = w.commandBuf[:off]
			return ErrCorrupt
		}
		binary.LittleEndian.PutUint32(buf[0:4], uint32(size))
		return nil
	}
	return w.appendRawKVBatchPayloadScanCommandDirect(lsn, baseAppliedLSN, plan, scan, size)
}

// AppendCommandPayloadDirectTrusted appends a canonical command payload whose
// bytes were constructed by the matching payload encoder.
func (w *Writer) AppendCommandPayloadDirectTrusted(lsn, baseAppliedLSN uint64, kind CommandKind, scope CommandScope, format PayloadFormat, payload []byte) error {
	if err := w.commandBufferError(); err != nil {
		return err
	}
	if lsn == 0 {
		return fmt.Errorf("%w: zero lsn", ErrCorrupt)
	}
	if err := validateCommandEnvelopeIdentity(CommandEnvelope{
		LSN:           lsn,
		Kind:          kind,
		Scope:         scope,
		PayloadFormat: format,
	}); err != nil {
		return err
	}
	size, err := commandFrameEncodedSizeFromLengths(len(payload), 0, 0, 0)
	if err != nil {
		return err
	}
	if w.maxSegmentSize > 0 && int64(size) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if size > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	total := segmentHeaderSize + size
	if len(payload) >= directCommandPayloadMinLen {
		return w.appendCommandPayloadDirectTrusted(lsn, baseAppliedLSN, kind, scope, format, payload, size)
	}
	if w.canBufferCommandFrame(total) {
		off, err := w.reserveCommandBufferSpace(total)
		if err != nil {
			return err
		}
		newLen := off + total
		buf := w.commandBuf[off:newLen]
		frameHeader := buf[segmentHeaderSize : segmentHeaderSize+commandFrameHeaderSize]
		fillTrustedCommandFrameHeader(frameHeader, lsn, baseAppliedLSN, kind, scope, format, len(payload))
		copy(buf[segmentHeaderSize+commandFrameHeaderSize:], payload)
		binary.LittleEndian.PutUint32(buf[0:4], uint32(size))
		return nil
	}
	return w.appendCommandPayloadDirectTrusted(lsn, baseAppliedLSN, kind, scope, format, payload, size)
}

func (w *Writer) appendRawKVBatchPayloadCommandDirect(lsn, baseAppliedLSN uint64, payload []byte, size int) error {
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	var prefix [segmentHeaderSize + commandFrameHeaderSize]byte
	frameHeader := prefix[segmentHeaderSize:]
	fillRawKVCommandFrameHeader(frameHeader, lsn, baseAppliedLSN, len(payload))
	binary.LittleEndian.PutUint32(prefix[0:4], uint32(size))
	binary.LittleEndian.PutUint32(prefix[4:8], crc.ChecksumParts(frameHeader, payload))
	if len(payload) >= directCommandPayloadMinLen {
		if err := w.bw.Flush(); err != nil {
			return err
		}
		parts := [2][]byte{prefix[:], payload}
		if err := w.writev(parts[:]); err != nil {
			return w.poisonCommandBuffer(err)
		}
		w.size += int64(segmentHeaderSize + size)
		return nil
	}
	if _, err := w.bw.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(payload); err != nil {
		return err
	}
	w.size += int64(segmentHeaderSize + size)
	return nil
}

func (w *Writer) appendRawKVBatchPayloadScanCommandDirect(lsn, baseAppliedLSN uint64, plan RawKVBatchPayloadPlan, scan RawKVBatchOperationScanner, size int) error {
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	var prefix [segmentHeaderSize + commandFrameHeaderSize]byte
	frameHeader := prefix[segmentHeaderSize:]
	fillRawKVCommandFrameHeader(frameHeader, lsn, baseAppliedLSN, plan.PayloadLen)
	sum := crc.Checksum(frameHeader)
	if err := writeRawKVBatchPayloadPieces(plan, scan, func(part []byte) error {
		sum = crc.Update(sum, part)
		return nil
	}); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(prefix[0:4], uint32(size))
	binary.LittleEndian.PutUint32(prefix[4:8], sum)
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.writeFull(prefix[:]); err != nil {
		return w.poisonCommandBuffer(err)
	}
	scratch := w.scratch[:0]
	if cap(scratch) < batchChecksumChunkBytes {
		scratch = make([]byte, 0, batchChecksumChunkBytes)
	}
	flushScratch := func() error {
		if len(scratch) == 0 {
			return nil
		}
		if err := w.writeFull(scratch); err != nil {
			return err
		}
		scratch = scratch[:0]
		return nil
	}
	if err := writeRawKVBatchPayloadPieces(plan, scan, func(part []byte) error {
		for len(part) > 0 {
			free := cap(scratch) - len(scratch)
			if free == 0 {
				if err := flushScratch(); err != nil {
					return err
				}
				free = cap(scratch)
			}
			if free > len(part) {
				free = len(part)
			}
			scratch = append(scratch, part[:free]...)
			part = part[free:]
		}
		return nil
	}); err != nil {
		w.scratch = scratch[:0]
		return w.poisonCommandBuffer(err)
	}
	if err := flushScratch(); err != nil {
		w.scratch = scratch[:0]
		return w.poisonCommandBuffer(err)
	}
	w.scratch = scratch[:0]
	w.size += int64(segmentHeaderSize + size)
	return nil
}

func (w *Writer) appendCommandPayloadDirectTrusted(lsn, baseAppliedLSN uint64, kind CommandKind, scope CommandScope, format PayloadFormat, payload []byte, size int) error {
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	var prefix [segmentHeaderSize + commandFrameHeaderSize]byte
	frameHeader := prefix[segmentHeaderSize:]
	fillTrustedCommandFrameHeader(frameHeader, lsn, baseAppliedLSN, kind, scope, format, len(payload))
	binary.LittleEndian.PutUint32(prefix[0:4], uint32(size))
	binary.LittleEndian.PutUint32(prefix[4:8], crc.ChecksumParts(frameHeader, payload))
	if len(payload) >= directCommandPayloadMinLen {
		if err := w.bw.Flush(); err != nil {
			return err
		}
		parts := [2][]byte{prefix[:], payload}
		if err := w.writev(parts[:]); err != nil {
			return w.poisonCommandBuffer(err)
		}
		w.size += int64(segmentHeaderSize + size)
		return nil
	}
	if _, err := w.bw.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(payload); err != nil {
		return err
	}
	w.size += int64(segmentHeaderSize + size)
	return nil
}

func fillRawKVCommandFrameHeader(frame []byte, lsn, baseAppliedLSN uint64, payloadLen int) {
	copy(frame, rawKVCommandFrameHeaderTemplate[:])
	binary.LittleEndian.PutUint64(frame[20:28], lsn)
	binary.LittleEndian.PutUint64(frame[44:52], baseAppliedLSN)
	binary.LittleEndian.PutUint32(frame[56:60], uint32(payloadLen))
}

func fillTrustedCommandFrameHeader(frame []byte, lsn, baseAppliedLSN uint64, kind CommandKind, scope CommandScope, format PayloadFormat, payloadLen int) {
	clear(frame[:commandFrameHeaderSize])
	copy(frame[0:4], commandFrameMagic[:])
	binary.LittleEndian.PutUint16(frame[4:6], CommandFrameVersion)
	binary.LittleEndian.PutUint16(frame[6:8], CommandFrameVersion)
	binary.LittleEndian.PutUint16(frame[8:10], uint16(kind))
	binary.LittleEndian.PutUint16(frame[10:12], uint16(scope))
	binary.LittleEndian.PutUint64(frame[20:28], lsn)
	binary.LittleEndian.PutUint64(frame[44:52], baseAppliedLSN)
	binary.LittleEndian.PutUint16(frame[52:54], uint16(format))
	binary.LittleEndian.PutUint32(frame[56:60], uint32(payloadLen))
}

func (w *Writer) reserveCommandBufferSpace(total int) (int, error) {
	if total <= 0 {
		return 0, ErrCorrupt
	}
	limit := w.commandBufferLimit()
	if limit <= 0 || total > limit {
		return 0, ErrRecordTooLarge
	}
	if len(w.commandBuf) > 0 && len(w.commandBuf)+total > limit {
		if err := w.flushBufferedCommandFrames(); err != nil {
			return 0, err
		}
	}
	off := len(w.commandBuf)
	newLen := off + total
	if cap(w.commandBuf) < newLen {
		newCap := cap(w.commandBuf) * 2
		if newCap < newLen {
			newCap = newLen
		}
		if newCap > limit && newLen <= limit {
			newCap = limit
		}
		next := make([]byte, newLen, newCap)
		copy(next, w.commandBuf)
		w.commandBuf = next
	} else {
		w.commandBuf = w.commandBuf[:newLen]
	}
	return off, nil
}

func (w *Writer) canBufferCommandFrame(total int) bool {
	return w != nil && total > 0 && w.commandBufferLimit() >= total
}

func (w *Writer) commandBufferLimit() int {
	if w == nil || w.commandBufLimit <= 0 {
		return 0
	}
	return w.commandBufLimit
}

func (w *Writer) writeFull(p []byte) error {
	for len(p) > 0 {
		n, err := w.fileSink.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}

func (w *Writer) flushBufferedCommandFrames() error {
	if w == nil || len(w.commandBuf) == 0 {
		if w != nil {
			return w.commandBufferError()
		}
		return nil
	}
	if err := w.commandBufferError(); err != nil {
		return err
	}
	if w.f == nil {
		return w.poisonCommandBuffer(errors.New("commitlog: nil writer"))
	}
	for off := 0; off < len(w.commandBuf); {
		if len(w.commandBuf)-off < segmentHeaderSize {
			return w.poisonCommandBuffer(ErrCorrupt)
		}
		size := int(binary.LittleEndian.Uint32(w.commandBuf[off:off+4]) & segmentLenMask)
		frameStart := off + segmentHeaderSize
		frameEnd := frameStart + size
		if size < commandFrameHeaderSize || frameEnd > len(w.commandBuf) {
			return w.poisonCommandBuffer(ErrCorrupt)
		}
		frame := w.commandBuf[frameStart:frameEnd]
		payloadLen := int(binary.LittleEndian.Uint32(frame[56:60]))
		if commandFrameHeaderSize+payloadLen > size {
			return w.poisonCommandBuffer(ErrCorrupt)
		}
		binary.LittleEndian.PutUint32(w.commandBuf[off+4:off+8], crc.Checksum(frame))
		off = frameEnd
	}
	if err := w.bw.Flush(); err != nil {
		return w.poisonCommandBuffer(err)
	}
	flushed := len(w.commandBuf)
	if err := w.writeFull(w.commandBuf); err != nil {
		return w.poisonCommandBuffer(err)
	}
	w.size += int64(flushed)
	w.trimCommandBufferAfterFlush()
	return nil
}

func (w *Writer) trimCommandBufferAfterFlush() {
	if w == nil || w.commandBuf == nil {
		return
	}
	retain := w.commandBufRetain
	if retain <= 0 || cap(w.commandBuf) <= retain {
		w.commandBuf = w.commandBuf[:0]
		return
	}
	dropped := cap(w.commandBuf) - retain
	w.commandBuf = make([]byte, 0, retain)
	w.commandBufTrims++
	w.commandBufDroppedBytes += uint64(dropped)
}

func (w *Writer) commandBufferError() error {
	if w == nil {
		return nil
	}
	return w.commandErr
}

func (w *Writer) poisonCommandBuffer(err error) error {
	if w == nil || err == nil {
		return err
	}
	if w.commandErr == nil {
		if w.f != nil {
			if truncErr := w.f.Truncate(w.size); truncErr != nil {
				err = errors.Join(err, truncErr)
			}
		}
		w.commandErr = err
	}
	if w.commandBuf != nil {
		w.commandBuf = nil
	}
	return w.commandErr
}

func (w *Writer) writeSegment(payload []byte) error {
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	stored := payload
	length := uint32(len(payload))
	wantCRC := crc.Checksum(payload)
	rawLenPrefix := w.rawLenPrefix[:]

	if w.compress && len(payload) >= defaultCompressMinLen {
		if err := w.ensureEncoder(); err != nil {
			return err
		}
		encDst := w.encScratch[:0]
		encoded := w.enc.EncodeAll(payload, encDst)
		// Only keep compressed bytes when it is a strict size win even after
		// including the raw-length prefix.
		if len(encoded)+len(rawLenPrefix) < len(payload) && len(encoded) <= int(segmentLenMask)-len(rawLenPrefix) {
			binary.LittleEndian.PutUint32(rawLenPrefix, uint32(len(payload)))
			length = uint32(len(encoded) + len(rawLenPrefix))
			length |= segmentFlagCompressed
			wantCRC = crc.ChecksumParts(rawLenPrefix, encoded)
			stored = encoded
			w.encScratch = encoded[:0]
		}
	}

	header := w.headerBuf[:]
	binary.LittleEndian.PutUint32(header[0:4], length)
	binary.LittleEndian.PutUint32(header[4:8], wantCRC)

	storedLen := int(length & segmentLenMask)
	if segmentHeaderSize+storedLen >= directSegmentPayloadMinLen {
		if err := w.bw.Flush(); err != nil {
			return err
		}
		if length&segmentFlagCompressed != 0 {
			if err := w.writev([][]byte{header, rawLenPrefix, stored}); err != nil {
				return err
			}
		} else if err := w.writev([][]byte{header, stored}); err != nil {
			return err
		}
		w.size += int64(segmentHeaderSize) + int64(storedLen)
		return nil
	}

	if _, err := w.bw.Write(header); err != nil {
		return err
	}
	if length&segmentFlagCompressed != 0 {
		if _, err := w.bw.Write(rawLenPrefix); err != nil {
			return err
		}
	}
	if _, err := w.bw.Write(stored); err != nil {
		return err
	}
	w.size += int64(segmentHeaderSize) + int64(storedLen)
	return nil
}

func (w *Writer) writeRawSegmentWithChecksum(payload []byte, wantCRC uint32) error {
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	return w.writeRawSegmentWithChecksumNoPending(payload, wantCRC)
}

func (w *Writer) writeRawSegmentWithChecksumNoPending(payload []byte, wantCRC uint32) error {
	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	if len(payload) > int(segmentLenMask) {
		return ErrRecordTooLarge
	}
	length := uint32(len(payload))
	header := w.headerBuf[:]
	binary.LittleEndian.PutUint32(header[0:4], length)
	binary.LittleEndian.PutUint32(header[4:8], wantCRC)

	storedLen := int(length)
	if segmentHeaderSize+storedLen >= directSegmentPayloadMinLen {
		if err := w.bw.Flush(); err != nil {
			return err
		}
		if err := w.writev([][]byte{header, payload}); err != nil {
			return err
		}
		w.size += int64(segmentHeaderSize) + int64(storedLen)
		return nil
	}

	if _, err := w.bw.Write(header); err != nil {
		return err
	}
	if _, err := w.bw.Write(payload); err != nil {
		return err
	}
	w.size += int64(segmentHeaderSize) + int64(storedLen)
	return nil
}

func validateRecord(r *Record) error {
	switch r.Op {
	case OpDelete:
		if r.RID != 0 || len(r.Value) != 0 {
			return fmt.Errorf("commitlog: delete record carries payload")
		}
	case OpSetRID:
		if r.RID == 0 {
			return fmt.Errorf("commitlog: missing RID")
		}
		if len(r.Value) != 0 {
			return fmt.Errorf("commitlog: RID record carries inline value")
		}
	case OpSetInline:
		if r.RID != 0 {
			return fmt.Errorf("commitlog: inline record carries RID")
		}
	default:
		return fmt.Errorf("commitlog: unknown op %d", r.Op)
	}
	return nil
}

func (w *Writer) Size() int64 {
	if w == nil {
		return 0
	}
	return w.size + w.pendingBatchBytes()
}

func (w *Writer) Flush() error {
	if w == nil || w.f == nil {
		return nil
	}
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) Sync() error {
	if w == nil || w.f == nil {
		return nil
	}
	if w.pendingBatchRecs != 0 {
		if err := w.flushPendingBatch(); err != nil {
			return err
		}
	}
	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.syncFile()
}

func (w *Writer) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	if w.enc != nil {
		w.enc.Close()
		w.enc = nil
	}
	if err := w.flushPendingBatch(); err != nil {
		_ = w.f.Close()
		return err
	}
	if err := w.flushBufferedCommandFrames(); err != nil {
		_ = w.f.Close()
		return err
	}
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
