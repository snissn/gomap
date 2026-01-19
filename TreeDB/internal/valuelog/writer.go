package valuelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/slab"
)

const (
	defaultBufferSize = 4 << 20
	headerWithoutCRC  = HeaderSize - 4
)

var syncDirFn = syncDir

func recordSizeExceedsMax(valueLen uint32) bool {
	if slab.MaxRecordSize <= 0 {
		return false
	}
	recordLen := int64(HeaderSize) + int64(valueLen)
	return recordLen > slab.MaxRecordSize
}

type Writer struct {
	f          *os.File
	bw         *bufio.Writer
	size       int64
	fileID     uint32
	scratch    []byte
	prefixBuf  []byte
	rawScratch []byte
	encScratch []byte
	skipDictID uint64
	noBenefit  uint8
	skipRemain uint16
	syncFn     func(*os.File) error
}

func dictSkipFrames(noBenefit uint8) uint16 {
	// Exponential backoff with periodic probes:
	// 1 no-benefit probe  -> skip 8 frames
	// 2 no-benefit probes -> skip 16 frames
	// ...
	// (capped)            -> skip 256 frames
	shift := uint(noBenefit) + 2
	if shift > 8 {
		shift = 8
	}
	return uint16(1 << shift)
}

func NewWriter(path string, fileID uint32) (*Writer, error) {
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
		f:         f,
		bw:        bufio.NewWriterSize(f, defaultBufferSize),
		size:      info.Size(),
		fileID:    fileID,
		scratch:   make([]byte, 0, defaultBufferSize),
		prefixBuf: make([]byte, 0, FrameHeaderSize+(MaxFrameK*8)+((MaxFrameK+1)*4)),
		syncFn:    func(file *os.File) error { return file.Sync() },
	}, nil
}

func newWriterWithSink(sink io.Writer, fileID uint32) *Writer {
	return &Writer{
		bw:        bufio.NewWriterSize(sink, defaultBufferSize),
		fileID:    fileID,
		scratch:   make([]byte, 0, defaultBufferSize),
		prefixBuf: make([]byte, 0, FrameHeaderSize+(MaxFrameK*8)+((MaxFrameK+1)*4)),
	}
}

func (w *Writer) FileID() uint32 {
	if w == nil {
		return 0
	}
	return w.fileID
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

func (w *Writer) RotateTo(path string, fileID uint32) error {
	if w == nil {
		return errors.New("valuelog: nil writer")
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
		w.fileID = fileID
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
	w.fileID = fileID
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

func (w *Writer) Append(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	if w == nil {
		return page.ValuePtr{}, errors.New("valuelog: nil writer")
	}
	if rid == 0 {
		return page.ValuePtr{}, errors.New("valuelog: missing rid")
	}
	// Fast path: single record, no dict/compression.
	// Avoids the per-call allocations from AppendFrame/AppendFrameWithStats.
	if dictID == 0 {
		if len(value) > int(^uint32(0)) {
			return page.ValuePtr{}, ErrRecordTooLarge
		}
		bodyLen := uint32(FrameHeaderSize + 8 + 8 + len(value))
		if recordSizeExceedsMax(bodyLen) {
			return page.ValuePtr{}, ErrRecordTooLarge
		}

		recordLen := HeaderSize + int(bodyLen)
		start := w.size
		if cap(w.scratch) < recordLen {
			w.scratch = make([]byte, recordLen)
		}
		buf := w.scratch[:recordLen]

		buf[4] = Version
		buf[5] = recordFlagGrouped
		buf[6] = 0
		buf[7] = 0
		binary.LittleEndian.PutUint64(buf[8:16], 0)
		binary.LittleEndian.PutUint32(buf[16:20], bodyLen)

		off := HeaderSize
		buf[off] = FrameVersion
		buf[off+1] = 0
		buf[off+2] = 1
		buf[off+3] = 0
		binary.LittleEndian.PutUint64(buf[off+4:off+12], 0)
		off += FrameHeaderSize

		binary.LittleEndian.PutUint64(buf[off:off+8], rid)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:off+4], 0)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], uint32(len(value)))
		off += 8
		copy(buf[off:], value)

		sum := crc.ChecksumParts(buf[4:HeaderSize], buf[HeaderSize:])
		binary.LittleEndian.PutUint32(buf[0:4], sum)

		if _, err := w.bw.Write(buf); err != nil {
			return page.ValuePtr{}, err
		}
		w.size += int64(recordLen)

		recordLenNoCRC := uint32(headerWithoutCRC) + bodyLen
		return page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenNoCRC, 0),
			FileID: w.fileID,
		}, nil
	}

	ptrs, err := w.AppendFrame(dictID, dict, []Record{{RID: rid, Value: value}})
	if err != nil {
		return page.ValuePtr{}, err
	}
	return ptrs[0], nil
}

func (w *Writer) AppendFrame(dictID uint64, dict []byte, records []Record) ([]page.ValuePtr, error) {
	ptrs, _, err := w.AppendFrameWithStats(dictID, dict, records)
	return ptrs, err
}

func (w *Writer) AppendFrameWithStats(dictID uint64, dict []byte, records []Record) ([]page.ValuePtr, FrameStats, error) {
	dst := make([]page.ValuePtr, len(records))
	ptrs, stats, err := w.AppendFrameWithStatsInto(dictID, dict, records, dst)
	if err != nil {
		return nil, FrameStats{}, err
	}
	return ptrs, stats, nil
}

// AppendFrameWithStatsInto appends a grouped frame and fills dst with the
// returned pointers (dst must be at least len(records) long).
//
// This is a performance-oriented helper to avoid allocating a new pointer slice
// on every frame append.
func (w *Writer) AppendFrameWithStatsInto(dictID uint64, dict []byte, records []Record, dst []page.ValuePtr) ([]page.ValuePtr, FrameStats, error) {
	if w == nil {
		return nil, FrameStats{}, errors.New("valuelog: nil writer")
	}
	if len(records) == 0 {
		return dst[:0], FrameStats{}, nil
	}
	if len(dst) < len(records) {
		return nil, FrameStats{}, errors.New("valuelog: dst too small")
	}
	dst = dst[:len(records)]
	if len(records) == 1 && dictID == 0 {
		rec := records[0]
		if rec.RID == 0 {
			return nil, FrameStats{}, errors.New("valuelog: missing rid")
		}
		if len(rec.Value) > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		bodyLen := uint32(FrameHeaderSize + 8 + 8 + len(rec.Value))
		if recordSizeExceedsMax(bodyLen) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		recordLen := HeaderSize + int(bodyLen)
		start := w.size
		if cap(w.scratch) < recordLen {
			w.scratch = make([]byte, recordLen)
		}
		buf := w.scratch[:recordLen]

		buf[4] = Version
		buf[5] = recordFlagGrouped
		buf[6] = 0
		buf[7] = 0
		binary.LittleEndian.PutUint64(buf[8:16], 0)
		binary.LittleEndian.PutUint32(buf[16:20], bodyLen)

		off := HeaderSize
		buf[off] = FrameVersion
		buf[off+1] = 0
		buf[off+2] = 1
		buf[off+3] = 0
		binary.LittleEndian.PutUint64(buf[off+4:off+12], 0)
		off += FrameHeaderSize

		binary.LittleEndian.PutUint64(buf[off:off+8], rec.RID)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:off+4], 0)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], uint32(len(rec.Value)))
		off += 8
		copy(buf[off:], rec.Value)

		sum := crc.ChecksumParts(buf[4:HeaderSize], buf[HeaderSize:])
		binary.LittleEndian.PutUint32(buf[0:4], sum)

		if _, err := w.bw.Write(buf); err != nil {
			return nil, FrameStats{}, err
		}
		w.size += int64(recordLen)

		recordLenNoCRC := uint32(headerWithoutCRC) + bodyLen
		dst[0] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenNoCRC, 0),
			FileID: w.fileID,
		}
		return dst[:1], FrameStats{Records: 1, RawPayloadBytes: len(rec.Value), StoredPayloadBytes: len(rec.Value), Compressed: false}, nil
	}

	rawPayloadBytes := 0
	for i := range records {
		rawPayloadBytes += len(records[i].Value)
	}

	if dictID != 0 {
		if len(dict) == 0 {
			return nil, FrameStats{}, ErrMissingDict
		}
		if w.skipDictID != dictID {
			w.skipDictID = dictID
			w.noBenefit = 0
			w.skipRemain = 0
		}
		k := len(records)
		if k <= 0 || k > MaxFrameK {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if rawPayloadBytes > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if slab.MaxRecordSize > 0 && int64(rawPayloadBytes) > slab.MaxRecordSize {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		var offsets [MaxFrameK + 1]uint32
		offsets[0] = 0
		payloadOff := 0
		for i := 0; i < k; i++ {
			payloadOff += len(records[i].Value)
			if payloadOff < 0 || payloadOff > int(^uint32(0)) {
				return nil, FrameStats{}, ErrRecordTooLarge
			}
			offsets[i+1] = uint32(payloadOff)
		}

		// If recent probes show compression yields no benefit, temporarily skip
		// zstd and write raw values without concatenating them into a temporary
		// buffer. We periodically probe again so we can adapt if data changes.
		if w.skipRemain > 0 {
			w.skipRemain--
			bodyLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4) + rawPayloadBytes
			if slab.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > slab.MaxRecordSize {
				return nil, FrameStats{}, ErrRecordTooLarge
			}
			if bodyLen > int(^uint32(0)) {
				return nil, FrameStats{}, ErrRecordTooLarge
			}
			if recordSizeExceedsMax(uint32(bodyLen)) {
				return nil, FrameStats{}, ErrRecordTooLarge
			}

			start := w.size
			var header [HeaderSize]byte
			header[4] = Version
			header[5] = recordFlagGrouped
			header[6] = 0
			header[7] = 0
			binary.LittleEndian.PutUint64(header[8:16], 0)
			binary.LittleEndian.PutUint32(header[16:20], uint32(bodyLen))

			prefixLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4)
			if cap(w.prefixBuf) < prefixLen {
				w.prefixBuf = make([]byte, 0, prefixLen)
			}
			prefix := w.prefixBuf[:prefixLen]
			prefixOff := 0
			prefix[prefixOff] = FrameVersion
			prefix[prefixOff+1] = 0
			prefix[prefixOff+2] = byte(k)
			prefix[prefixOff+3] = 0
			binary.LittleEndian.PutUint64(prefix[prefixOff+4:prefixOff+12], dictID)
			prefixOff += FrameHeaderSize

			for i := 0; i < k; i++ {
				rid := records[i].RID
				if rid == 0 {
					return nil, FrameStats{}, errors.New("valuelog: missing rid")
				}
				binary.LittleEndian.PutUint64(prefix[prefixOff:prefixOff+8], rid)
				prefixOff += 8
			}
			for i := 0; i < k+1; i++ {
				binary.LittleEndian.PutUint32(prefix[prefixOff:prefixOff+4], offsets[i])
				prefixOff += 4
			}

			sum := crc.ChecksumParts(header[4:], prefix)
			for i := 0; i < k; i++ {
				sum = crc.Update(sum, records[i].Value)
			}
			binary.LittleEndian.PutUint32(header[0:4], sum)

			if _, err := w.bw.Write(header[:]); err != nil {
				return nil, FrameStats{}, err
			}
			if _, err := w.bw.Write(prefix); err != nil {
				return nil, FrameStats{}, err
			}
			for i := 0; i < k; i++ {
				if _, err := w.bw.Write(records[i].Value); err != nil {
					return nil, FrameStats{}, err
				}
			}
			w.size += int64(HeaderSize + bodyLen)

			recordLenNoCRC := uint32(headerWithoutCRC) + uint32(bodyLen)
			for i := range records {
				dst[i] = page.ValuePtr{
					Offset: uint64(start + 4),
					Length: page.ValuePtrMarkGrouped(recordLenNoCRC, uint8(i)),
					FileID: w.fileID,
				}
			}

			return dst, FrameStats{
				Records:            k,
				RawPayloadBytes:    rawPayloadBytes,
				StoredPayloadBytes: rawPayloadBytes,
				Compressed:         false,
			}, nil
		}

		if cap(w.rawScratch) < rawPayloadBytes {
			w.rawScratch = make([]byte, rawPayloadBytes)
		}
		raw := w.rawScratch[:rawPayloadBytes]
		pos := 0
		for i := 0; i < k; i++ {
			copy(raw[pos:], records[i].Value)
			pos += len(records[i].Value)
		}

		codecs := getDictCodecs(dictID, dict)
		if codecs == nil || codecs.encPool == nil {
			return nil, FrameStats{}, ErrMissingDict
		}
		if cap(w.encScratch) < rawPayloadBytes {
			w.encScratch = make([]byte, 0, rawPayloadBytes)
		}
		encDst := w.encScratch[:0]

		enc := codecs.encPool.Get().(*zstd.Encoder)
		encoded := enc.EncodeAll(raw, encDst)
		codecs.encPool.Put(enc)

		flags := byte(0)
		storedPayloadBytes := len(encoded)
		if len(encoded) < len(raw) {
			flags |= FrameFlagCompressed
			w.noBenefit = 0
			w.skipRemain = 0
		} else {
			encoded = raw
			storedPayloadBytes = len(raw)
			if w.noBenefit < 0xff {
				w.noBenefit++
			}
			w.skipRemain = dictSkipFrames(w.noBenefit)
		}

		bodyLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4) + len(encoded)
		if slab.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > slab.MaxRecordSize {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if bodyLen > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if recordSizeExceedsMax(uint32(bodyLen)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		start := w.size
		var header [HeaderSize]byte
		header[4] = Version
		header[5] = recordFlagGrouped
		header[6] = 0
		header[7] = 0
		binary.LittleEndian.PutUint64(header[8:16], 0)
		binary.LittleEndian.PutUint32(header[16:20], uint32(bodyLen))

		prefixLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4)
		if cap(w.prefixBuf) < prefixLen {
			w.prefixBuf = make([]byte, 0, prefixLen)
		}
		prefix := w.prefixBuf[:prefixLen]
		prefixOff := 0
		prefix[prefixOff] = FrameVersion
		prefix[prefixOff+1] = flags
		prefix[prefixOff+2] = byte(k)
		prefix[prefixOff+3] = 0
		binary.LittleEndian.PutUint64(prefix[prefixOff+4:prefixOff+12], dictID)
		prefixOff += FrameHeaderSize

		for i := 0; i < k; i++ {
			rid := records[i].RID
			if rid == 0 {
				return nil, FrameStats{}, errors.New("valuelog: missing rid")
			}
			binary.LittleEndian.PutUint64(prefix[prefixOff:prefixOff+8], rid)
			prefixOff += 8
		}
		for i := 0; i < k+1; i++ {
			binary.LittleEndian.PutUint32(prefix[prefixOff:prefixOff+4], offsets[i])
			prefixOff += 4
		}

		sum := crc.ChecksumParts(header[4:], prefix, encoded)
		binary.LittleEndian.PutUint32(header[0:4], sum)

		if _, err := w.bw.Write(header[:]); err != nil {
			return nil, FrameStats{}, err
		}
		if _, err := w.bw.Write(prefix); err != nil {
			return nil, FrameStats{}, err
		}
		if _, err := w.bw.Write(encoded); err != nil {
			return nil, FrameStats{}, err
		}
		w.size += int64(HeaderSize + bodyLen)

		recordLenNoCRC := uint32(headerWithoutCRC) + uint32(bodyLen)
		for i := range records {
			dst[i] = page.ValuePtr{
				Offset: uint64(start + 4),
				Length: page.ValuePtrMarkGrouped(recordLenNoCRC, uint8(i)),
				FileID: w.fileID,
			}
		}

		return dst, FrameStats{
			Records:            k,
			RawPayloadBytes:    rawPayloadBytes,
			StoredPayloadBytes: storedPayloadBytes,
			Compressed:         flags&FrameFlagCompressed != 0,
		}, nil
	}

	body, header, err := EncodeFrame(dictID, dict, records)
	if err != nil {
		return nil, FrameStats{}, err
	}
	if len(body) > int(^uint32(0)) {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	bodyLen := uint32(len(body))
	if recordSizeExceedsMax(bodyLen) {
		return nil, FrameStats{}, ErrRecordTooLarge
	}

	recordLen := HeaderSize + len(body)
	start := w.size
	if cap(w.scratch) < recordLen {
		w.scratch = make([]byte, recordLen)
	}
	buf := w.scratch[:recordLen]

	buf[4] = Version
	buf[5] = recordFlagGrouped
	buf[6] = 0
	buf[7] = 0
	binary.LittleEndian.PutUint64(buf[8:16], 0)
	binary.LittleEndian.PutUint32(buf[16:20], bodyLen)
	copy(buf[HeaderSize:], body)

	sum := crc.ChecksumParts(buf[4:HeaderSize], body)
	binary.LittleEndian.PutUint32(buf[0:4], sum)

	if _, err := w.bw.Write(buf); err != nil {
		return nil, FrameStats{}, err
	}
	w.size += int64(recordLen)

	recordLenNoCRC := uint32(headerWithoutCRC) + bodyLen
	for i := range records {
		dst[i] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenNoCRC, uint8(i)),
			FileID: w.fileID,
		}
	}
	k := len(records)
	headerEnd := FrameHeaderSize + (k * 8) + ((k + 1) * 4)
	storedPayloadBytes := len(body) - headerEnd
	if storedPayloadBytes < 0 {
		storedPayloadBytes = 0
	}
	return dst, FrameStats{
		Records:            k,
		RawPayloadBytes:    rawPayloadBytes,
		StoredPayloadBytes: storedPayloadBytes,
		Compressed:         header.Flags&FrameFlagCompressed != 0,
	}, nil
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
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
