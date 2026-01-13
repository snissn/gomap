package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/klauspost/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
)

const (
	OpSet        = byte(0)
	OpDelete     = byte(1)
	OpSetPointer = byte(2)

	// FlagCompressed is set in the MSB of the segment length.
	FlagCompressed uint32 = 0x80000000
)

var ErrCorrupt = errors.New("wal: corrupt record")
var ErrRecordTooLarge = errors.New("wal: record too large")

var syncDirFn = syncDir

type Writer struct {
	f              *os.File
	bw             *bufio.Writer
	scratch        []byte
	pending        []byte
	size           int64
	segmentMax     int
	maxSegmentSize int64
	syncFn         func(*os.File) error

	compress bool
	zstdEnc  *zstd.Encoder
	compBuf  []byte
}

const (
	defaultWALBufferSize  = 4 << 20
	defaultMaxSegmentSize = 64 * 1024 * 1024
)

type Options struct {
	// MaxSegmentSize bounds the total WAL segment payload size (bytes).
	// 0 uses the default limit; values < 0 disable the cap.
	MaxSegmentSize int64

	// Compress enables per-segment ZSTD compression.
	Compress bool
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

type Record struct {
	Seq   uint64
	Op    byte
	Key   []byte
	Value []byte
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
	w := &Writer{
		f:              f,
		bw:             bufio.NewWriterSize(f, defaultWALBufferSize),
		scratch:        make([]byte, 0, defaultWALBufferSize),
		pending:        make([]byte, 0, defaultWALBufferSize),
		segmentMax:     defaultWALBufferSize,
		maxSegmentSize: normalizeMaxSegmentSize(opts.MaxSegmentSize),
		syncFn:         func(file *os.File) error { return file.Sync() },
		compress:       opts.Compress,
	}
	if opts.Compress {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false))
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		w.zstdEnc = enc
	}
	return w, nil
}

// RotateTo flushes and closes the current WAL file, then opens (or creates) the
// provided path and reuses the writer's buffers for future appends.
func (w *Writer) RotateTo(path string) error {
	if w == nil {
		return errors.New("wal: nil writer")
	}

	if w.f == nil {
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

		w.f = f
		w.bw.Reset(f)
		w.size = info.Size()
		w.pending = w.pending[:0]
		return nil
	}
	if err := w.flushPending(); err != nil {
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
	w.pending = w.pending[:0]
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

// writeSegment writes a chunk of data as a checksummed segment.
// Format: [Length 4][CRC 4][Payload...]
func (w *Writer) writeSegment(payload []byte) error {
	var header [8]byte
	data := payload
	length := uint32(len(payload))

	if w.compress && len(payload) > 128 {
		w.compBuf = w.zstdEnc.EncodeAll(payload, w.compBuf[:0])
		if len(w.compBuf) < len(payload) {
			data = w.compBuf
			length = uint32(len(w.compBuf)) | FlagCompressed
		}
	}

	binary.LittleEndian.PutUint32(header[0:4], length)
	c := crc.Checksum(data)
	binary.LittleEndian.PutUint32(header[4:8], c)

	if _, err := w.bw.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(data); err != nil {
		return err
	}
	w.size += 8 + int64(len(data))
	return nil
}

func (w *Writer) flushPending() error {
	if len(w.pending) == 0 {
		return nil
	}
	if err := w.writeSegment(w.pending); err != nil {
		return err
	}
	w.pending = w.pending[:0]
	return nil
}

// Append writes a single record as an atomic segment.
func (w *Writer) Append(seq uint64, op byte, key, value []byte) error {
	// Encode record into scratch
	// Record: [Seq 8][Op 1][KeyLen 2][ValLen 4][Key][Value]
	size := 8 + 1 + 2 + 4 + len(key) + len(value)
	if w.maxSegmentSize > 0 && int64(size) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}
	if size > w.segmentMax {
		if err := w.flushPending(); err != nil {
			return err
		}
		if cap(w.scratch) < size {
			w.scratch = make([]byte, size)
		}
		buf := w.scratch[:size]
		binary.LittleEndian.PutUint64(buf[0:8], seq)
		buf[8] = op
		binary.LittleEndian.PutUint16(buf[9:11], uint16(len(key)))
		binary.LittleEndian.PutUint32(buf[11:15], uint32(len(value)))
		copy(buf[15:], key)
		copy(buf[15+len(key):], value)
		return w.writeSegment(buf)
	}

	if len(w.pending)+size > w.segmentMax {
		if err := w.flushPending(); err != nil {
			return err
		}
	}
	need := len(w.pending) + size
	if cap(w.pending) < need {
		newCap := w.segmentMax
		if newCap < need {
			newCap = need
		}
		next := make([]byte, len(w.pending), newCap)
		copy(next, w.pending)
		w.pending = next
	}
	off := len(w.pending)
	w.pending = w.pending[:need]
	buf := w.pending[off:need]
	binary.LittleEndian.PutUint64(buf[0:8], seq)
	buf[8] = op
	binary.LittleEndian.PutUint16(buf[9:11], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[11:15], uint32(len(value)))
	copy(buf[15:], key)
	copy(buf[15+len(key):], value)
	return nil
}

// AppendBatch writes multiple records as a single atomic segment.
func (w *Writer) AppendBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	if err := w.flushPending(); err != nil {
		return err
	}

	// Calculate total size
	total := 0
	for _, r := range records {
		total += 8 + 1 + 2 + 4 + len(r.Key) + len(r.Value)
	}
	if w.maxSegmentSize > 0 && int64(total) > w.maxSegmentSize {
		return ErrRecordTooLarge
	}

	if cap(w.scratch) < total {
		w.scratch = make([]byte, total)
	}
	buf := w.scratch[:total]

	off := 0
	for _, r := range records {
		binary.LittleEndian.PutUint64(buf[off:off+8], r.Seq)
		buf[off+8] = r.Op
		binary.LittleEndian.PutUint16(buf[off+9:off+11], uint16(len(r.Key)))
		binary.LittleEndian.PutUint32(buf[off+11:off+15], uint32(len(r.Value)))
		copy(buf[off+15:], r.Key)
		copy(buf[off+15+len(r.Key):], r.Value)
		off += 8 + 1 + 2 + 4 + len(r.Key) + len(r.Value)
	}

	return w.writeSegment(buf)
}

func (w *Writer) Size() int64 {
	if len(w.pending) == 0 {
		return w.size
	}
	return w.size + int64(len(w.pending)) + 8
}

func (w *Writer) Flush() error {
	if err := w.flushPending(); err != nil {
		return err
	}
	return w.bw.Flush()
}

func (w *Writer) Sync() error {
	if err := w.flushPending(); err != nil {
		return err
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() error {
	if err := w.flushPending(); err != nil {
		_ = w.f.Close()
		return err
	}
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

type Reader struct {
	f              *os.File
	buf            []byte
	pos            int
	maxSegmentSize int64
	zstdDec        *zstd.Decoder
}

func NewReader(path string) (*Reader, error) {
	return NewReaderWithOptions(path, Options{})
}

func NewReaderWithOptions(path string, opts Options) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Reader{
		f:              f,
		maxSegmentSize: normalizeMaxSegmentSize(opts.MaxSegmentSize),
		zstdDec:        dec,
	}, nil
}

// ReadNext yields the next record. It transparently handles segments.
func (r *Reader) ReadNext() (seq uint64, op byte, key, val []byte, err error) {
	// If buffer is empty or exhausted, read next segment
	if r.buf == nil || r.pos >= len(r.buf) {
		if err := r.readSegment(); err != nil {
			return 0, 0, nil, nil, err
		}
	}

	// Parse record from buffer
	// Record: [Seq 8][Op 1][KeyLen 2][ValLen 4][Key][Value]
	if r.pos+15 > len(r.buf) {
		return 0, 0, nil, nil, ErrCorrupt // Segment truncated in memory? Should not happen if readSegment checks size.
	}

	seq = binary.LittleEndian.Uint64(r.buf[r.pos : r.pos+8])
	op = r.buf[r.pos+8]
	kLen := int(binary.LittleEndian.Uint16(r.buf[r.pos+9 : r.pos+11]))
	vLen := int(binary.LittleEndian.Uint32(r.buf[r.pos+11 : r.pos+15]))
	recSize := 8 + 1 + 2 + 4 + kLen + vLen

	if r.pos+recSize > len(r.buf) {
		return 0, 0, nil, nil, ErrCorrupt
	}

	key = r.buf[r.pos+15 : r.pos+15+kLen]
	val = r.buf[r.pos+15+kLen : r.pos+recSize]
	r.pos += recSize

	return seq, op, key, val, nil
}

func (r *Reader) readSegment() error {
	// Header: [Length 4][CRC 4]
	var header [8]byte
	if _, err := io.ReadFull(r.f, header[:]); err != nil {
		return err // EOF or UnexpectedEOF propagates here
	}

	rawLength := binary.LittleEndian.Uint32(header[0:4])
	compressed := rawLength&FlagCompressed != 0
	length := rawLength & ^FlagCompressed
	wantCRC := binary.LittleEndian.Uint32(header[4:8])

	// Sanity check length to avoid OOM on corrupt file.
	if r.maxSegmentSize > 0 && int64(length) > r.maxSegmentSize {
		return ErrCorrupt
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r.f, data); err != nil {
		return err // UnexpectedEOF if file truncated in payload
	}

	if crc.Checksum(data) != wantCRC {
		return ErrCorrupt
	}

	if compressed {
		decompressed, err := r.zstdDec.DecodeAll(data, nil)
		if err != nil {
			return err
		}
		r.buf = decompressed
	} else {
		r.buf = data
	}
	r.pos = 0
	return nil
}

func (r *Reader) Close() error {
	if r.zstdDec != nil {
		r.zstdDec.Close()
	}
	return r.f.Close()
}
