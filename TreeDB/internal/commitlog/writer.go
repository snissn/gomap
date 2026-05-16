package commitlog

import (
	"bufio"
	"crypto/sha256"
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
	defaultBufferSize          = 4 << 20
	defaultMaxSegmentSize      = 64 * 1024 * 1024
	defaultCompressMinLen      = 64 << 10
	directCommandPayloadMinLen = 32 << 10
	deferredCommandBufferSize  = 64 << 20
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

type Writer struct {
	f              *os.File
	bw             *bufio.Writer
	scratch        []byte
	encScratch     []byte
	commandBuf     []byte
	commandErr     error
	headerBuf      [segmentHeaderSize]byte
	rawLenPrefix   [4]byte
	size           int64
	maxSegmentSize int64
	compress       bool
	enc            *zstd.Encoder
	syncFn         func(*os.File) error
}

func NewWriter(path string) (*Writer, error) {
	return NewWriterWithOptions(path, Options{})
}

func NewWriterWithOptions(path string, opts Options) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
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
	var commandBuf []byte
	if opts.DeferredCommandBufferSize > 0 {
		commandBuf = make([]byte, 0, opts.DeferredCommandBufferSize)
	}
	return &Writer{
		f:              f,
		bw:             bufio.NewWriterSize(f, normalizeBufferSize(opts.BufferSize)),
		scratch:        make([]byte, 0, defaultBufferSize),
		commandBuf:     commandBuf,
		size:           info.Size(),
		maxSegmentSize: normalizeMaxSegmentSize(opts.MaxSegmentSize),
		compress:       opts.Compress,
		syncFn:         func(file *os.File) error { return file.Sync() },
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

// RotateTo flushes and closes the current file, then opens (or creates) the
// provided path and reuses the writer's buffers for future appends.
func (w *Writer) RotateTo(path string) error {
	if w == nil {
		return errors.New("commitlog: nil writer")
	}

	if w.f == nil {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		if err := syncDirFn(path); err != nil {
			_ = f.Close()
			return err
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

	if err := w.flushBufferedCommandFrames(); err != nil {
		return err
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if w.syncFn != nil {
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
	if err := syncDirFn(path); err != nil {
		_ = f.Close()
		return err
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

	if cap(w.scratch) < int(total) {
		w.scratch = make([]byte, int(total))
	}
	buf := w.scratch[:int(total)]

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

	return w.writeSegment(buf)
}

func (w *Writer) AppendBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	if len(records) > int(^uint32(0)) {
		return ErrRecordTooLarge
	}

	total := int64(batchHeaderSize)
	batchSeq := records[0].Seq
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
		total += int64(recordHeaderSize) + int64(len(r.Key)) + int64(len(r.Value))
	}
	if w.maxSegmentSize > 0 && total > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if total > int64(int(^uint(0)>>1)) {
		return ErrRecordTooLarge
	}

	if cap(w.scratch) < int(total) {
		w.scratch = make([]byte, int(total))
	}
	buf := w.scratch[:int(total)]

	buf[0] = Version
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(records)))

	off := batchHeaderSize
	for i := range records {
		r := &records[i]
		keyLen := uint16(len(r.Key))
		valLen := uint32(len(r.Value))
		buf[off] = r.Op
		binary.LittleEndian.PutUint16(buf[off+1:off+3], keyLen)
		binary.LittleEndian.PutUint32(buf[off+3:off+7], valLen)
		binary.LittleEndian.PutUint64(buf[off+7:off+15], r.RID)
		binary.LittleEndian.PutUint64(buf[off+15:off+23], r.Seq)
		copy(buf[off+recordHeaderSize:], r.Key)
		copy(buf[off+recordHeaderSize+len(r.Key):], r.Value)
		off += recordHeaderSize + len(r.Key) + len(r.Value)
	}

	return w.writeSegment(buf)
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
	total := segmentHeaderSize + size
	if cap(w.commandBuf) == 0 {
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
	if cap(w.commandBuf) > 0 {
		total := segmentHeaderSize + size
		if total <= deferredCommandBufferSize {
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
	digest := sha256.Sum256(payload)
	copy(frameHeader[72:72+sha256.Size], digest[:])
	binary.LittleEndian.PutUint32(prefix[0:4], uint32(size))
	binary.LittleEndian.PutUint32(prefix[4:8], crc.ChecksumParts(frameHeader, payload))
	if len(payload) >= directCommandPayloadMinLen {
		if err := w.bw.Flush(); err != nil {
			return err
		}
		parts := [2][]byte{prefix[:], payload}
		if err := writevFull(w.f, parts[:]); err != nil {
			return err
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

func (w *Writer) reserveCommandBufferSpace(total int) (int, error) {
	if total <= 0 {
		return 0, ErrCorrupt
	}
	if len(w.commandBuf) > 0 && len(w.commandBuf)+total > deferredCommandBufferSize {
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
		if newCap < defaultBufferSize {
			newCap = defaultBufferSize
		}
		if newCap > deferredCommandBufferSize && newLen <= deferredCommandBufferSize {
			newCap = deferredCommandBufferSize
		}
		next := make([]byte, newLen, newCap)
		copy(next, w.commandBuf)
		w.commandBuf = next
	} else {
		w.commandBuf = w.commandBuf[:newLen]
	}
	return off, nil
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
		payload := frame[commandFrameHeaderSize : commandFrameHeaderSize+payloadLen]
		digest := sha256.Sum256(payload)
		copy(frame[72:72+sha256.Size], digest[:])
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
	w.size += int64(segmentHeaderSize) + int64(length&segmentLenMask)
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
	return w.size
}

func (w *Writer) Flush() error {
	if w == nil || w.f == nil {
		return nil
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
