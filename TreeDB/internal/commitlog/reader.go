package commitlog

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

type Reader struct {
	f              *os.File
	maxSegmentSize int64
}

func NewReader(path string) (*Reader, error) {
	return NewReaderWithOptions(path, Options{})
}

func NewReaderWithOptions(path string, opts Options) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		f:              f,
		maxSegmentSize: normalizeMaxSegmentSize(opts.MaxSegmentSize),
	}, nil
}

// ReadBatch reads the next batch segment.
func (r *Reader) ReadBatch() ([]Record, error) {
	var header [segmentHeaderSize]byte
	if _, err := io.ReadFull(r.f, header[:]); err != nil {
		return nil, err
	}

	length := binary.LittleEndian.Uint32(header[0:4])
	wantCRC := binary.LittleEndian.Uint32(header[4:8])
	if r.maxSegmentSize > 0 && int64(length) > r.maxSegmentSize {
		return nil, ErrCorrupt
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r.f, payload); err != nil {
		return nil, err
	}
	if crc.Checksum(payload) != wantCRC {
		return nil, ErrCorrupt
	}

	return decodeBatch(payload)
}

func decodeBatch(payload []byte) ([]Record, error) {
	if len(payload) < batchHeaderSize {
		return nil, ErrCorrupt
	}
	if payload[0] != Version {
		return nil, ErrCorrupt
	}
	count := binary.LittleEndian.Uint32(payload[1:5])
	minBytes := int64(count) * int64(recordHeaderSize)
	if minBytes < 0 || minBytes > int64(len(payload)) {
		return nil, ErrCorrupt
	}
	if count > uint32(len(payload)/recordHeaderSize) {
		return nil, ErrCorrupt
	}

	records := make([]Record, 0, count)
	off := batchHeaderSize
	for i := uint32(0); i < count; i++ {
		if off+recordHeaderSize > len(payload) {
			return nil, ErrCorrupt
		}
		op := payload[off]
		keyLen := binary.LittleEndian.Uint16(payload[off+1 : off+3])
		valLen := binary.LittleEndian.Uint32(payload[off+3 : off+7])
		rid := binary.LittleEndian.Uint64(payload[off+7 : off+15])
		seq := binary.LittleEndian.Uint64(payload[off+15 : off+23])
		if recordSizeExceedsMax(keyLen, valLen) {
			return nil, ErrRecordTooLarge
		}
		recSize := recordHeaderSize + int(keyLen) + int(valLen)
		if off+recSize > len(payload) {
			return nil, ErrCorrupt
		}
		keyStart := off + recordHeaderSize
		valStart := keyStart + int(keyLen)
		key := payload[keyStart:valStart]
		val := payload[valStart : valStart+int(valLen)]

		switch op {
		case OpDelete:
			if valLen != 0 || rid != 0 {
				return nil, ErrCorrupt
			}
		case OpSetRID:
			if valLen != 0 || rid == 0 {
				return nil, ErrCorrupt
			}
		case OpSetInline:
			if rid != 0 {
				return nil, ErrCorrupt
			}
		default:
			return nil, ErrCorrupt
		}

		records = append(records, Record{Op: op, Key: key, Value: val, RID: rid, Seq: seq})
		off += recSize
	}
	if off != len(payload) {
		return nil, ErrCorrupt
	}
	return records, nil
}

func (r *Reader) Close() error {
	if r == nil || r.f == nil {
		return nil
	}
	return r.f.Close()
}
