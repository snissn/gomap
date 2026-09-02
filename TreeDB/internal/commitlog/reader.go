package commitlog

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
)

type Reader struct {
	f              *os.File
	ownsFile       bool
	maxSegmentSize int64
	dec            *zstd.Decoder
	scanRaw        []byte
	scanDecoded    []byte
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
		ownsFile:       true,
		maxSegmentSize: normalizeMaxSegmentSize(opts.MaxSegmentSize),
	}, nil
}

// NewReaderFromFileWithOptions streams from an already-open exact file handle.
// The caller retains ownership of file; Close releases only reader-owned
// decoder state. This is used by destructive maintenance so classification and
// later identity validation refer to the same physical segment.
func NewReaderFromFileWithOptions(file *os.File, opts Options) (*Reader, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	return &Reader{
		f:              file,
		maxSegmentSize: normalizeMaxSegmentSize(opts.MaxSegmentSize),
	}, nil
}

// ReadBatch reads the next batch segment.
func (r *Reader) ReadBatch() ([]Record, error) {
	payload, err := r.readSegmentPayload(false)
	if err != nil {
		return nil, err
	}
	return decodeBatch(payload)
}

func (r *Reader) Offset() (int64, error) {
	if r == nil || r.f == nil {
		return 0, os.ErrInvalid
	}
	return r.f.Seek(0, io.SeekCurrent)
}

func (r *Reader) readSegmentPayload(commandMode bool) ([]byte, error) {
	return r.readSegmentPayloadWithCompression(commandMode, true)
}

func (r *Reader) readSegmentPayloadWithCompression(commandMode, allowCompressed bool) ([]byte, error) {
	return r.readSegmentPayloadWithCompressionBuffers(commandMode, allowCompressed, nil, nil)
}

func (r *Reader) readSegmentPayloadForCommandScan() ([]byte, error) {
	return r.readSegmentPayloadWithCompressionBuffers(true, true, &r.scanRaw, &r.scanDecoded)
}

func (r *Reader) readSegmentPayloadWithCompressionBuffers(commandMode, allowCompressed bool, rawBuf, decodedBuf *[]byte) ([]byte, error) {
	var header [segmentHeaderSize]byte
	if n, err := io.ReadFull(r.f, header[:]); err != nil {
		if commandMode && n > 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
			return nil, ErrCommandWALTerminalTail
		}
		return nil, err
	}

	lengthField := binary.LittleEndian.Uint32(header[0:4])
	wantCRC := binary.LittleEndian.Uint32(header[4:8])
	compressed := lengthField&segmentFlagCompressed != 0
	if compressed && !allowCompressed {
		return nil, ErrCommandWALV2CompressedRecordUnsupported
	}
	length := lengthField & segmentLenMask
	if r.maxSegmentSize > 0 && int64(length) > r.maxSegmentSize {
		return nil, ErrCorrupt
	}
	if uint64(length) > uint64(^uint(0)>>1) {
		return nil, ErrCorrupt
	}

	payload := resizeReaderBuffer(rawBuf, int(length))
	if _, err := io.ReadFull(r.f, payload); err != nil {
		if commandMode && (err == io.EOF || err == io.ErrUnexpectedEOF) {
			return nil, ErrCommandWALTerminalTail
		}
		return nil, err
	}
	if crc.Checksum(payload) != wantCRC {
		return nil, ErrCorrupt
	}

	if compressed {
		if len(payload) < 4 {
			return nil, ErrCorrupt
		}
		rawLen := binary.LittleEndian.Uint32(payload[:4])
		if r.maxSegmentSize > 0 && int64(rawLen) > r.maxSegmentSize {
			return nil, ErrCorrupt
		}
		maxInt := uint64(^uint(0) >> 1)
		if uint64(rawLen) > maxInt {
			return nil, ErrCorrupt
		}
		if r.dec == nil {
			dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			if err != nil {
				return nil, ErrCorrupt
			}
			r.dec = dec
		}
		dst := resizeReaderBuffer(decodedBuf, int(rawLen))[:0]
		decoded, err := r.dec.DecodeAll(payload[4:], dst)
		if err != nil || uint32(len(decoded)) != rawLen {
			return nil, ErrCorrupt
		}
		if decodedBuf != nil {
			*decodedBuf = decoded
		}
		payload = decoded
	}

	return payload, nil
}

func resizeReaderBuffer(buf *[]byte, size int) []byte {
	if buf == nil {
		return make([]byte, size)
	}
	if cap(*buf) < size {
		*buf = make([]byte, size)
	} else {
		*buf = (*buf)[:size]
	}
	return *buf
}

func decodeBatch(payload []byte) ([]Record, error) {
	if len(payload) < batchHeaderSize {
		return nil, ErrCorrupt
	}
	switch payload[0] {
	case Version:
		return decodeBatchRecords(payload, false)
	case zeroInlineBatchVersion:
		return decodeZeroInlineBatch(payload)
	case recordRevisionBatchVersion:
		return decodeBatchRecords(payload, true)
	default:
		return nil, ErrCorrupt
	}
}

func decodeBatchV1(payload []byte) ([]Record, error) {
	return decodeBatchRecords(payload, false)
}

func decodeBatchRecords(payload []byte, withRevision bool) ([]Record, error) {
	count := binary.LittleEndian.Uint32(payload[1:5])
	headerSize := recordHeaderSize
	if withRevision {
		headerSize = recordRevisionHeaderSize
	}
	minBytes := int64(count) * int64(headerSize)
	if minBytes < 0 || minBytes > int64(len(payload)) {
		return nil, ErrCorrupt
	}
	if count > uint32(len(payload)/headerSize) {
		return nil, ErrCorrupt
	}

	records := make([]Record, 0, count)
	off := batchHeaderSize
	for i := uint32(0); i < count; i++ {
		if off+headerSize > len(payload) {
			return nil, ErrCorrupt
		}
		op := payload[off]
		keyLen := binary.LittleEndian.Uint16(payload[off+1 : off+3])
		valLen := binary.LittleEndian.Uint32(payload[off+3 : off+7])
		rid := binary.LittleEndian.Uint64(payload[off+7 : off+15])
		seq := binary.LittleEndian.Uint64(payload[off+15 : off+23])
		revision := uint64(0)
		if withRevision {
			revision = binary.LittleEndian.Uint64(payload[off+23 : off+31])
		}
		if recordSizeExceedsMaxWithHeader(headerSize, keyLen, valLen) {
			return nil, ErrRecordTooLarge
		}
		recSize := headerSize + int(keyLen) + int(valLen)
		if op == OpSetInlineZero {
			recSize = headerSize + int(keyLen)
		}
		if off+recSize > len(payload) {
			return nil, ErrCorrupt
		}
		keyStart := off + headerSize
		valStart := keyStart + int(keyLen)
		key := payload[keyStart:valStart]
		var val []byte
		if op != OpSetInlineZero {
			val = payload[valStart : valStart+int(valLen)]
		}

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
		case OpSetInlineZero:
			if rid != 0 {
				return nil, ErrCorrupt
			}
			val = make([]byte, int(valLen))
			op = OpSetInline
		default:
			return nil, ErrCorrupt
		}

		records = append(records, Record{Op: op, Key: key, Value: val, RID: rid, Seq: seq, Revision: revision})
		off += recSize
	}
	if off != len(payload) {
		return nil, ErrCorrupt
	}
	return records, nil
}

func decodeZeroInlineBatch(payload []byte) ([]Record, error) {
	if len(payload) < zeroInlineBatchHeaderSize {
		return nil, ErrCorrupt
	}
	count := binary.LittleEndian.Uint32(payload[1:5])
	seq := binary.LittleEndian.Uint64(payload[5:13])
	valLen := binary.LittleEndian.Uint32(payload[13:17])
	minBytes := int64(count) * int64(zeroInlineRecordHeaderSize)
	if minBytes < 0 || minBytes > int64(len(payload)-zeroInlineBatchHeaderSize) {
		return nil, ErrCorrupt
	}
	if recordSizeExceedsMax(0, valLen) {
		return nil, ErrRecordTooLarge
	}
	records := make([]Record, 0, count)
	value := make([]byte, int(valLen))
	off := zeroInlineBatchHeaderSize
	for i := uint32(0); i < count; i++ {
		if off+zeroInlineRecordHeaderSize > len(payload) {
			return nil, ErrCorrupt
		}
		keyLen := binary.LittleEndian.Uint16(payload[off : off+2])
		if recordSizeExceedsMax(keyLen, valLen) {
			return nil, ErrRecordTooLarge
		}
		off += zeroInlineRecordHeaderSize
		if off+int(keyLen) > len(payload) {
			return nil, ErrCorrupt
		}
		key := payload[off : off+int(keyLen)]
		off += int(keyLen)
		records = append(records, Record{Op: OpSetInline, Key: key, Value: value, Seq: seq})
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
	if r.dec != nil {
		r.dec.Close()
		r.dec = nil
	}
	r.scanRaw = nil
	r.scanDecoded = nil
	if !r.ownsFile {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}
