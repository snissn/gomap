package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

const (
	OpSet    = byte(0)
	OpDelete = byte(1)
)

var ErrCorrupt = errors.New("wal: corrupt record")

type Writer struct {
	f       *os.File
	bw      *bufio.Writer
	scratch []byte
	size    int64
}

type Record struct {
	Op    byte
	Key   []byte
	Value []byte
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	return &Writer{
		f:       f,
		bw:      bufio.NewWriterSize(f, 1<<20), // 1MB buffer
		scratch: make([]byte, 0, 1<<20),
	}, nil
}

// writeSegment writes a chunk of data as a checksummed segment.
// Format: [Length 4][CRC 4][Payload...]
func (w *Writer) writeSegment(payload []byte) error {
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	c := crc.Checksum(payload)
	binary.LittleEndian.PutUint32(header[4:8], c)

	if _, err := w.bw.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(payload); err != nil {
		return err
	}
	w.size += 8 + int64(len(payload))
	return nil
}

// Append writes a single record as an atomic segment.
func (w *Writer) Append(op byte, key, value []byte) error {
	// Encode record into scratch
	// Record: [Op 1][KeyLen 2][ValLen 4][Key][Value]
	size := 1 + 2 + 4 + len(key) + len(value)
	if cap(w.scratch) < size {
		w.scratch = make([]byte, size)
	}
	buf := w.scratch[:size]

	buf[0] = op
	binary.LittleEndian.PutUint16(buf[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[3:7], uint32(len(value)))
	copy(buf[7:], key)
	copy(buf[7+len(key):], value)

	return w.writeSegment(buf)
}

// AppendBatch writes multiple records as a single atomic segment.
func (w *Writer) AppendBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}

	// Calculate total size
	total := 0
	for _, r := range records {
		total += 1 + 2 + 4 + len(r.Key) + len(r.Value)
	}

	if cap(w.scratch) < total {
		w.scratch = make([]byte, total)
	}
	buf := w.scratch[:total]

	off := 0
	for _, r := range records {
		buf[off] = r.Op
		binary.LittleEndian.PutUint16(buf[off+1:off+3], uint16(len(r.Key)))
		binary.LittleEndian.PutUint32(buf[off+3:off+7], uint32(len(r.Value)))
		copy(buf[off+7:], r.Key)
		copy(buf[off+7+len(r.Key):], r.Value)
		off += 1 + 2 + 4 + len(r.Key) + len(r.Value)
	}

	return w.writeSegment(buf)
}

func (w *Writer) Size() int64 {
	return w.size
}

func (w *Writer) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() error {
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

type Reader struct {
	f   *os.File
	buf []byte
	pos int
}

func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{f: f}, nil
}

// ReadNext yields the next record. It transparently handles segments.
func (r *Reader) ReadNext() (op byte, key, val []byte, err error) {
	// If buffer is empty or exhausted, read next segment
	if r.buf == nil || r.pos >= len(r.buf) {
		if err := r.readSegment(); err != nil {
			return 0, nil, nil, err
		}
	}

	// Parse record from buffer
	// Record: [Op 1][KeyLen 2][ValLen 4][Key][Value]
	if r.pos+7 > len(r.buf) {
		return 0, nil, nil, ErrCorrupt // Segment truncated in memory? Should not happen if readSegment checks size.
	}

	op = r.buf[r.pos]
	kLen := int(binary.LittleEndian.Uint16(r.buf[r.pos+1 : r.pos+3]))
	vLen := int(binary.LittleEndian.Uint32(r.buf[r.pos+3 : r.pos+7]))
	recSize := 1 + 2 + 4 + kLen + vLen

	if r.pos+recSize > len(r.buf) {
		return 0, nil, nil, ErrCorrupt
	}

	key = r.buf[r.pos+7 : r.pos+7+kLen]
	val = r.buf[r.pos+7+kLen : r.pos+recSize]
	r.pos += recSize

	return op, key, val, nil
}

func (r *Reader) readSegment() error {
	// Header: [Length 4][CRC 4]
	var header [8]byte
	if _, err := io.ReadFull(r.f, header[:]); err != nil {
		return err // EOF or UnexpectedEOF propagates here
	}

	length := binary.LittleEndian.Uint32(header[0:4])
	wantCRC := binary.LittleEndian.Uint32(header[4:8])

	// Sanity check length (e.g. max 64MB) to avoid OOM on corrupt file
	if length > 64*1024*1024 {
		return ErrCorrupt
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r.f, data); err != nil {
		return err // UnexpectedEOF if file truncated in payload
	}

	if crc.Checksum(data) != wantCRC {
		return ErrCorrupt
	}

	r.buf = data
	r.pos = 0
	return nil
}

func (r *Reader) Close() error {
	return r.f.Close()
}
