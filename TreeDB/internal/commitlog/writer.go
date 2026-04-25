package commitlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
)

const (
	defaultBufferSize     = 4 << 20
	defaultMaxSegmentSize = 64 * 1024 * 1024
	defaultCompressMinLen = 64 << 10
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
	return &Writer{
		f:              f,
		bw:             bufio.NewWriterSize(f, defaultBufferSize),
		scratch:        make([]byte, 0, defaultBufferSize),
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

// AppendBatchFunc appends count records supplied by recordAt. recordAt must be
// deterministic for each index for the duration of the call.
func (w *Writer) AppendBatchFunc(count int, recordAt func(int) Record) error {
	_, err := w.AppendBatchFuncWithSize(count, recordAt)
	return err
}

// AppendBatchFuncWithSize appends count records supplied by recordAt and
// returns the bytes written. recordAt may be called more than once for the same
// index and must return the same Record each time during this call.
func (w *Writer) AppendBatchFuncWithSize(count int, recordAt func(int) Record) (int64, error) {
	if count == 0 {
		return 0, nil
	}
	if count < 0 || count > int(^uint32(0)) {
		return 0, ErrRecordTooLarge
	}
	if recordAt == nil {
		return 0, ErrCorrupt
	}

	total := int64(batchHeaderSize)
	first := recordAt(0)
	if err := validateRecord(&first); err != nil {
		return 0, err
	}
	if len(first.Key) > int(^uint16(0)) || len(first.Value) > int(^uint32(0)) {
		return 0, ErrRecordTooLarge
	}
	firstKeyLen := uint16(len(first.Key))
	firstValLen := uint32(len(first.Value))
	if recordSizeExceedsMax(firstKeyLen, firstValLen) {
		return 0, ErrRecordTooLarge
	}
	batchSeq := first.Seq
	total += int64(recordHeaderSize) + int64(len(first.Key)) + int64(len(first.Value))

	for i := 1; i < count; i++ {
		r := recordAt(i)
		if err := validateRecord(&r); err != nil {
			return 0, err
		}
		if r.Seq != batchSeq {
			return 0, ErrMixedBatchSeq
		}
		if len(r.Key) > int(^uint16(0)) {
			return 0, ErrRecordTooLarge
		}
		if len(r.Value) > int(^uint32(0)) {
			return 0, ErrRecordTooLarge
		}
		keyLen := uint16(len(r.Key))
		valLen := uint32(len(r.Value))
		if recordSizeExceedsMax(keyLen, valLen) {
			return 0, ErrRecordTooLarge
		}
		total += int64(recordHeaderSize) + int64(len(r.Key)) + int64(len(r.Value))
	}
	if w.maxSegmentSize > 0 && total > w.maxSegmentSize {
		return 0, ErrRecordTooLarge
	}
	if total > int64(int(^uint(0)>>1)) {
		return 0, ErrRecordTooLarge
	}

	if cap(w.scratch) < int(total) {
		w.scratch = make([]byte, int(total))
	}
	buf := w.scratch[:int(total)]

	buf[0] = Version
	binary.LittleEndian.PutUint32(buf[1:5], uint32(count))

	off := batchHeaderSize
	for i := 0; i < count; i++ {
		r := recordAt(i)
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

	if err := w.writeSegment(buf); err != nil {
		return 0, err
	}
	return int64(segmentHeaderSize) + total, nil
}

func (w *Writer) writeSegment(payload []byte) error {
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
	return w.bw.Flush()
}

func (w *Writer) Sync() error {
	if w == nil || w.f == nil {
		return nil
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
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
