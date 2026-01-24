package valuelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/page"
)

type Reader struct {
	f        *os.File
	r        *bufio.Reader
	pos      int64
	fileID   uint32
	verifies bool
}

func NewReader(path string, fileID uint32) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		f:        f,
		r:        bufio.NewReaderSize(f, defaultBufferSize),
		fileID:   fileID,
		verifies: true,
	}, nil
}

func (r *Reader) DisableChecksum() {
	r.verifies = false
}

func (r *Reader) ReadNext() (uint64, []byte, page.ValuePtr, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r.r, header[:]); err != nil {
		return 0, nil, page.ValuePtr{}, err
	}

	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return 0, nil, page.ValuePtr{}, ErrCorrupt
	}
	rid := binary.LittleEndian.Uint64(header[8:16])
	if rid == 0 {
		return 0, nil, page.ValuePtr{}, ErrCorrupt
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return 0, nil, page.ValuePtr{}, ErrRecordTooLarge
	}

	payload := make([]byte, int(valueLen))
	if _, err := io.ReadFull(r.r, payload); err != nil {
		return 0, nil, page.ValuePtr{}, err
	}

	if r.verifies {
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return 0, nil, page.ValuePtr{}, ErrCorrupt
		}
	}

	start := r.pos
	r.pos += int64(HeaderSize + valueLen)

	ptr := page.ValuePtr{
		Offset: uint64(start + 4),
		Length: uint32(headerWithoutCRC) + valueLen,
		FileID: r.fileID,
	}

	return rid, payload, ptr, nil
}

func (r *Reader) Close() error {
	if r == nil || r.f == nil {
		return nil
	}
	return r.f.Close()
}

func ReadAt(f *os.File, ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	if f == nil {
		return nil, errors.New("valuelog: nil file")
	}
	if ptr.Offset < 4 {
		return nil, ErrCorrupt
	}
	start := int64(ptr.Offset - 4)
	if ptr.Length != 0 {
		totalLen := int(ptr.Length) + 4
		if totalLen < HeaderSize {
			return nil, ErrCorrupt
		}
		buf := make([]byte, totalLen)
		if _, err := f.ReadAt(buf, start); err != nil {
			return nil, err
		}
		header := buf[:HeaderSize]
		payload := buf[HeaderSize:]

		crcVal := binary.LittleEndian.Uint32(header[0:4])
		version := header[4]
		if version != Version {
			return nil, ErrCorrupt
		}
		valueLen := binary.LittleEndian.Uint32(header[16:20])
		if recordSizeExceedsMax(valueLen) {
			return nil, ErrRecordTooLarge
		}
		if int(valueLen) != len(payload) {
			return nil, ErrCorrupt
		}
		expectedLen := uint32(headerWithoutCRC) + valueLen
		if ptr.Length != expectedLen {
			return nil, ErrCorrupt
		}
		if verifyCRC {
			sum := crc.ChecksumParts(header[4:], payload)
			if sum != crcVal {
				return nil, ErrCorrupt
			}
		}
		return payload, nil
	}

	var header [HeaderSize]byte
	if _, err := f.ReadAt(header[:], start); err != nil {
		return nil, err
	}
	crcVal := binary.LittleEndian.Uint32(header[0:4])
	version := header[4]
	if version != Version {
		return nil, ErrCorrupt
	}
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if recordSizeExceedsMax(valueLen) {
		return nil, ErrRecordTooLarge
	}
	payload := make([]byte, int(valueLen))
	if _, err := f.ReadAt(payload, start+HeaderSize); err != nil {
		return nil, err
	}
	if verifyCRC {
		sum := crc.ChecksumParts(header[4:], payload)
		if sum != crcVal {
			return nil, ErrCorrupt
		}
	}
	return payload, nil
}
