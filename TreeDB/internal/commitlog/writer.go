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

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
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

func recordSizeExceedsMax(keyLen uint16, valueLen uint32) bool {
	if limits.MaxRecordSize <= 0 {
		return false
	}
	recordLen := int64(recordHeaderSize) + int64(keyLen) + int64(valueLen)
	return recordLen > limits.MaxRecordSize
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
	f                *os.File
	bw               *bufio.Writer
	scratch          []byte
	encScratch       []byte
	commandBuf       []byte
	commandBufLimit  int
	commandErr       error
	pendingBatch     []byte
	pendingBatchSeq  uint64
	pendingBatchRecs uint32
	headerBuf        [segmentHeaderSize]byte
	rawLenPrefix     [4]byte
	size             int64
	maxSegmentSize   int64
	compress         bool
	enc              *zstd.Encoder
	syncFn           func(*os.File) error
}

func NewWriter(path string) (*Writer, error) {
	return NewWriterWithOptions(path, Options{})
}

func NewWriterWithOptions(path string, opts Options) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syncDirFn(path); err != nil {
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
		commandBuf = make([]byte, 0, opts.DeferredCommandBufferSize)
		commandBufLimit = opts.DeferredCommandBufferSize
	}
	return &Writer{
		f:               f,
		bw:              bufio.NewWriterSize(f, normalizeBufferSize(opts.BufferSize)),
		scratch:         make([]byte, 0, defaultBufferSize),
		commandBuf:      commandBuf,
		commandBufLimit: commandBufLimit,
		size:            info.Size(),
		maxSegmentSize:  normalizeMaxSegmentSize(opts.MaxSegmentSize),
		compress:        opts.Compress,
		syncFn:          func(file *os.File) error { return file.Sync() },
	}, nil
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
	if w == nil {
		return errors.New("commitlog: nil writer")
	}

	if w.f == nil {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		if syncCurrent {
			if err := syncDirFn(path); err != nil {
				_ = f.Close()
				return err
			}
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		w.f = f
		w.bw.Reset(f)
		w.size = info.Size()
		w.scratch = w.scratch[:0]
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
	if syncCurrent && w.syncFn != nil {
		if err := w.syncFn(w.f); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if syncCurrent {
		if err := syncDirFn(path); err != nil {
			_ = f.Close()
			return err
		}
	}

	old := w.f
	w.f = f
	w.bw.Reset(f)
	w.size = info.Size()
	w.scratch = w.scratch[:0]
	if err := old.Close(); err != nil {
		return err
	}
	return nil
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
	if recordSizeExceedsMax(keyLen, valLen) {
		return ErrRecordTooLarge
	}

	total := int64(batchHeaderSize) + int64(recordHeaderSize) + int64(len(record.Key)) + int64(len(record.Value))
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
	if !w.compress && segmentHeaderSize+payloadLen < directSegmentPayloadMinLen {
		return w.appendPendingRecord(record, keyLen, valLen)
	}

	if cap(w.scratch) < payloadLen {
		w.scratch = make([]byte, payloadLen)
	}
	buf := w.scratch[:payloadLen]

	buf[0] = Version
	binary.LittleEndian.PutUint32(buf[1:5], 1)

	off := batchHeaderSize
	buf[off] = record.Op
	binary.LittleEndian.PutUint16(buf[off+1:off+3], keyLen)
	binary.LittleEndian.PutUint32(buf[off+3:off+7], valLen)
	binary.LittleEndian.PutUint64(buf[off+7:off+15], record.RID)
	binary.LittleEndian.PutUint64(buf[off+15:off+23], record.Seq)
	copy(buf[off+recordHeaderSize:], record.Key)
	copy(buf[off+recordHeaderSize+len(record.Key):], record.Value)

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
	if !w.compress {
		if ok, err := w.appendZeroInlineBatchIfCompact(records, batchSeq); ok || err != nil {
			return err
		}
	}
	if cap(w.scratch) < batchHeaderSize {
		w.scratch = make([]byte, batchHeaderSize)
	}
	buf := w.scratch
	if len(buf) < batchHeaderSize {
		buf = buf[:batchHeaderSize]
	}
	buf[0] = Version
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
		if r.Seq != batchSeq {
			return ErrMixedBatchSeq
		}
		if len(r.Key) > int(^uint16(0)) {
			return ErrRecordTooLarge
		}
		if len(r.Value) > int(^uint32(0)) {
			return ErrRecordTooLarge
		}
		keyLen := uint16(len(r.Key))
		valLen := uint32(len(r.Value))
		if recordSizeExceedsMax(keyLen, valLen) {
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

		recLen := recordHeaderSize + len(r.Key)
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
		copy(buf[off+recordHeaderSize:], r.Key)
		if writeValue {
			valueStart := off + recordHeaderSize + len(r.Key)
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

func (w *Writer) AppendRawKVSingleCommandDirect(lsn, baseAppliedLSN uint64, op RawKVOperation) error {
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
	valueLen := len(value)
	if op == RawKVOpDelete {
		valueLen = 0
	}
	payloadLen := rawKVBatchHeaderSize + rawKVOpHeaderSize + len(key) + valueLen
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
	return w.appendRawKVPointCommandDirectTrustedSized(lsn, baseAppliedLSN, op, key, value, valueLen, payloadLen, size)
}

func (w *Writer) appendRawKVPointCommandDirectTrustedSized(lsn, baseAppliedLSN uint64, op RawKVOp, key, value []byte, valueLen, payloadLen, size int) error {
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
		frame, err := encodeTrustedRawKVPointCommandFrameSizedTo(buf[segmentHeaderSize:segmentHeaderSize+size], lsn, baseAppliedLSN, op, key, value, valueLen, payloadLen, size)
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
	encodeTrustedRawKVPointCommandFramePayloadSizedTo(buf[segmentHeaderSize:segmentHeaderSize+size], lsn, baseAppliedLSN, op, key, value, valueLen, payloadLen, size)
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
	copy(frameHeader, rawKVCommandFrameHeaderTemplate[:])
	binary.LittleEndian.PutUint64(frameHeader[20:28], lsn)
	binary.LittleEndian.PutUint64(frameHeader[44:52], baseAppliedLSN)
	binary.LittleEndian.PutUint32(frameHeader[56:60], uint32(len(payload)))
	binary.LittleEndian.PutUint32(prefix[0:4], uint32(size))
	binary.LittleEndian.PutUint32(prefix[4:8], crc.ChecksumParts(frameHeader, payload))
	if len(payload) >= directCommandPayloadMinLen {
		if err := w.bw.Flush(); err != nil {
			return err
		}
		parts := [2][]byte{prefix[:], payload}
		if err := writevFull(w.f, parts[:]); err != nil {
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
		if err := writevFull(w.f, parts[:]); err != nil {
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
	if w == nil || w.commandBufLimit <= 0 || cap(w.commandBuf) == 0 {
		return 0
	}
	return w.commandBufLimit
}

func writeFull(f *os.File, p []byte) error {
	for len(p) > 0 {
		n, err := f.Write(p)
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
	if err := writeFull(w.f, w.commandBuf); err != nil {
		return w.poisonCommandBuffer(err)
	}
	w.size += int64(flushed)
	w.commandBuf = w.commandBuf[:0]
	return nil
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
		w.commandBuf = w.commandBuf[:0]
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
			if err := writevFull(w.f, [][]byte{header, rawLenPrefix, stored}); err != nil {
				return err
			}
		} else if err := writevFull(w.f, [][]byte{header, stored}); err != nil {
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
		if err := writevFull(w.f, [][]byte{header, payload}); err != nil {
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
	return w.f.Sync()
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
