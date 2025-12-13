package wal

import (
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
	f *os.File
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f}, nil
}

func (w *Writer) Append(op byte, key, value []byte) error {
	// [CRC 4][Op 1][KeyLen 2][ValLen 4][Key][Value]
	// Total overhead: 4+1+2+4 = 11 bytes
	size := 11 + len(key) + len(value)
	buf := make([]byte, size)

	buf[4] = op
	binary.LittleEndian.PutUint16(buf[5:7], uint16(len(key)))
	binary.LittleEndian.PutUint32(buf[7:11], uint32(len(value)))
	copy(buf[11:], key)
	copy(buf[11+len(key):], value)

	c := crc.Checksum(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c)

	_, err := w.f.Write(buf)
	return err
}

func (w *Writer) Sync() error {
	return w.f.Sync()
}

func (w *Writer) Close() error {
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
