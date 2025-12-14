package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/snissn/gomap-gemini/TreeDB/internal/crc"
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
		bw:      bufio.NewWriterSize(f, 1<<20), // 1MB buffer to amortize syscalls
		scratch: make([]byte, 0, 1<<20),
	}, nil
}

func (w *Writer) Append(op byte, key, value []byte) error {
	// [CRC 4][Op 1][KeyLen 2][ValLen 4][Key][Value]
	// Total overhead: 4+1+2+4 = 11 bytes
	var header [7]byte
	header[0] = op
	binary.LittleEndian.PutUint16(header[1:3], uint16(len(key)))
	binary.LittleEndian.PutUint32(header[3:7], uint32(len(value)))

	c := crc.ChecksumParts(header[:], key, value)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], c)

	if _, err := w.bw.Write(crcBuf[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(header[:]); err != nil {
		return err
	}
	if len(key) > 0 {
		if _, err := w.bw.Write(key); err != nil {
			return err
		}
	}
	if len(value) > 0 {
		if _, err := w.bw.Write(value); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) AppendBatch(records []Record) error {
	total := 0
	for _, r := range records {
		total += 11 + len(r.Key) + len(r.Value)
	}
	if total == 0 {
		return nil
	}

	if cap(w.scratch) < total {
		w.scratch = make([]byte, total)
	} else {
		w.scratch = w.scratch[:total]
	}
	buf := w.scratch
	off := 0
	for _, r := range records {
		kLen := len(r.Key)
		vLen := len(r.Value)
		recSize := 11 + kLen + vLen

		body := buf[off+4 : off+recSize] // [Op|KeyLen|ValLen|Key|Value]
		body[0] = r.Op
		binary.LittleEndian.PutUint16(body[1:3], uint16(kLen))
		binary.LittleEndian.PutUint32(body[3:7], uint32(vLen))
		copy(body[7:], r.Key)
		copy(body[7+kLen:], r.Value)

		c := crc.Checksum(body)
		binary.LittleEndian.PutUint32(buf[off:off+4], c)

		off += recSize
	}

	_, err := w.bw.Write(buf)
	return err
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
	f *os.File
}

func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{f: f}, nil
}

func (r *Reader) ReadNext() (op byte, key, val []byte, err error) {
	// Read header: 11 bytes
	header := make([]byte, 11)
	if _, err := io.ReadFull(r.f, header); err != nil {
		return 0, nil, nil, err
	}

	wantCRC := binary.LittleEndian.Uint32(header[0:4])
	op = header[4]
	kLen := int(binary.LittleEndian.Uint16(header[5:7]))
	vLen := int(binary.LittleEndian.Uint32(header[7:11]))

	payload := make([]byte, kLen+vLen)
	if _, err := io.ReadFull(r.f, payload); err != nil {
		return 0, nil, nil, err
	}

	// Verify CRC
	// Reconstruct buffer for verification (Op + Len + Payload)
	checkBuf := append(header[4:], payload...)
	gotCRC := crc.Checksum(checkBuf)
	if gotCRC != wantCRC {
		return 0, nil, nil, ErrCorrupt
	}

	key = payload[:kLen]
	val = payload[kLen:]
	return op, key, val, nil
}

func (r *Reader) Close() error {
	return r.f.Close()
}
