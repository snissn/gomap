package valuelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
)

const headerWithoutCRC = HeaderSize - 4

// defaultBufferSize controls the default per-writer buffering in file-backed
// value-log writers (bufio + append buffer). A smaller default keeps
// heap high-watermarks reasonable when workloads open many concurrent writers
// (e.g. sharded cached mode under state-sync restore).
const defaultBufferSize = 4 << 20

const (
	// Retain up to one normal writer buffer of scratch across explicit flush-style
	// boundaries, but trim larger transient spikes back down once the writer cools.
	writerScratchKeepCap = defaultBufferSize
	writerScratchTrimCap = writerScratchKeepCap * 2
)

var syncDirFn = syncDir

func recordSizeExceedsMax(valueLen uint32) bool {
	if limits.MaxRecordSize <= 0 {
		return false
	}
	recordLen := int64(HeaderSize) + int64(valueLen)
	return recordLen > limits.MaxRecordSize
}

type Writer struct {
	f                      *os.File
	bw                     *bufio.Writer
	size                   int64
	fileID                 uint32
	appendBuf              []byte
	appendMax              int
	rawWritevMinAvgBytes   int
	rawWritevMinBatchRecs  int
	scratch                []byte
	prefixBuf              []byte
	rawScratch             []byte
	rawWritevIovs          [][]byte
	rawWritevVecs          []writevIovec
	rawWritevMeta          []byte
	rawWritevSyscalls      atomic.Uint64
	rawWritevBytes         atomic.Uint64
	rawWritevIovecs        atomic.Uint64
	rawWritevFlushes       atomic.Uint64
	rawWriteSyscalls       atomic.Uint64
	rawWriteBytes          atomic.Uint64
	rawWriteCalls          atomic.Uint64
	encScratch             []byte
	encLimiter             limitedSliceWriter
	blockScratch           []byte
	skipDictID             uint64
	codecs                 *dictCodecEntry
	dictFrameEncodeLevel   zstd.EncoderLevel
	dictFrameEnableEntropy bool
	blockCodec             BlockCodec
	blockCompression       bool
	noBenefit              uint8
	skipRemain             uint16
	syncFn                 func(*os.File) error
	clock                  Clock
	encodeCostModel        EncodeCostModel
	encodeSampleStride     uint64
	encodeSampleCount      uint64
	keepIoNsPerStoredByte  float64
	keepEncodeNsPerRawByte float64
	keepSafetyMargin       float64
}

var errEncodedTooLarge = errors.New("valuelog: encoded payload too large")

type limitedSliceWriter struct {
	buf   []byte
	limit int
}

func (w *limitedSliceWriter) Write(p []byte) (int, error) {
	if w == nil {
		return 0, errors.New("valuelog: nil writer")
	}
	if w.limit > 0 && len(w.buf)+len(p) > w.limit {
		return 0, errEncodedTooLarge
	}
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func (w *Writer) writeAllToFile(buf []byte) error {
	if w == nil {
		return errors.New("valuelog: nil file writer")
	}
	if w.f == nil {
		return errors.New("valuelog: nil file writer")
	}
	w.rawWriteCalls.Add(1)
	written := 0
	for written < len(buf) {
		w.rawWriteSyscalls.Add(1)
		n, err := w.f.Write(buf[written:])
		if n > 0 {
			w.rawWriteBytes.Add(uint64(n))
			written += n
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("valuelog: short write")
		}
	}
	return nil
}

func (w *Writer) flushAppendBuf() error {
	if w == nil {
		return errors.New("valuelog: nil writer")
	}
	if len(w.appendBuf) == 0 {
		return nil
	}
	if w.f == nil {
		_, err := w.bw.Write(w.appendBuf)
		w.appendBuf = w.appendBuf[:0]
		return err
	}
	if err := w.writeAllToFile(w.appendBuf); err != nil {
		return err
	}
	w.appendBuf = w.appendBuf[:0]
	return nil
}

func (w *Writer) flushNoTrim() error {
	if w == nil {
		return nil
	}
	if err := w.flushAppendBuf(); err != nil {
		return err
	}
	if w.bw != nil {
		if err := w.bw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func trimTransientWriterScratch(buf []byte) []byte {
	if cap(buf) == 0 {
		return nil
	}
	if cap(buf) <= writerScratchTrimCap {
		return buf[:0]
	}
	return make([]byte, 0, writerScratchKeepCap)
}

func (w *Writer) trimTransientScratchBuffers() {
	if w == nil {
		return
	}
	w.scratch = trimTransientWriterScratch(w.scratch)
	w.rawScratch = trimTransientWriterScratch(w.rawScratch)
	w.encScratch = trimTransientWriterScratch(w.encScratch)
	w.blockScratch = trimTransientWriterScratch(w.blockScratch)
	w.encLimiter.buf = nil
	w.encLimiter.limit = 0
}

func (w *Writer) releaseTransientScratchBuffers() {
	if w == nil {
		return
	}
	w.scratch = nil
	w.rawScratch = nil
	w.encScratch = nil
	w.blockScratch = nil
	w.encLimiter.buf = nil
	w.encLimiter.limit = 0
}

func (w *Writer) writeBytes(buf []byte) error {
	if w == nil {
		return errors.New("valuelog: nil writer")
	}
	if len(buf) == 0 {
		return nil
	}
	if w.f == nil {
		_, err := w.bw.Write(buf)
		return err
	}

	max := w.appendMax
	if max <= 0 {
		max = defaultBufferSize
	}
	directThreshold := max >> 10
	if directThreshold < 8<<10 {
		directThreshold = 8 << 10
	}
	if directThreshold > 256<<10 {
		directThreshold = 256 << 10
	}
	if len(buf) >= max {
		if err := w.flushAppendBuf(); err != nil {
			return err
		}
		return w.writeAllToFile(buf)
	}
	if len(w.appendBuf) == 0 && len(buf) >= directThreshold {
		return w.writeAllToFile(buf)
	}
	if len(w.appendBuf)+len(buf) > max {
		if err := w.flushAppendBuf(); err != nil {
			return err
		}
		if len(buf) >= directThreshold {
			return w.writeAllToFile(buf)
		}
	}
	w.appendBuf = append(w.appendBuf, buf...)
	if len(w.appendBuf) >= max {
		return w.flushAppendBuf()
	}
	return nil
}

// writeBytesBuffered appends bytes through appendBuf even for medium/large
// payloads, avoiding direct-write threshold bypass so callers can coalesce
// multiple records into fewer large writes.
func (w *Writer) writeBytesBuffered(buf []byte) error {
	if w == nil {
		return errors.New("valuelog: nil writer")
	}
	if len(buf) == 0 {
		return nil
	}
	if w.f == nil {
		_, err := w.bw.Write(buf)
		return err
	}

	max := w.appendMax
	if max <= 0 {
		max = defaultBufferSize
	}
	for len(buf) > 0 {
		if len(w.appendBuf) >= max {
			if err := w.flushAppendBuf(); err != nil {
				return err
			}
		}
		avail := max - len(w.appendBuf)
		if avail <= 0 {
			continue
		}
		if len(buf) <= avail {
			w.appendBuf = append(w.appendBuf, buf...)
			if len(w.appendBuf) >= max {
				return w.flushAppendBuf()
			}
			return nil
		}
		w.appendBuf = append(w.appendBuf, buf[:avail]...)
		buf = buf[avail:]
		if err := w.flushAppendBuf(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeFrameBatch(buf []byte) error {
	return w.writeBytes(buf)
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

func dictSkipFramesAggressive(noBenefit uint8, rawPayloadBytes, encodedLen int, encodeNs int64, ioNsPerStoredByte, safetyMargin float64) uint16 {
	skip := dictSkipFrames(noBenefit)
	if rawPayloadBytes <= 0 || encodedLen <= 0 || encodedLen >= rawPayloadBytes {
		return skip
	}
	if encodeNs <= 0 || ioNsPerStoredByte <= 0 {
		return skip
	}
	savings := float64(rawPayloadBytes-encodedLen) * ioNsPerStoredByte
	cost := float64(encodeNs)
	if safetyMargin > 0 {
		cost *= 1 + safetyMargin
	}
	if savings <= 0 {
		return skip
	}
	if cost/savings >= 4 {
		if skip < 512 {
			return 512
		}
	}
	return skip
}

func isUltraLowEntropySample(value []byte) bool {
	// EncodeAllParts can be faster than concatenating into a contiguous payload
	// for ultra-compressible workloads (where the zstd/dict encode itself is very
	// fast and the memcpy dominates). However, EncodeAllParts can emit more
	// (smaller) blocks for many small parts which can degrade ratio and
	// throughput on higher-entropy payloads, so we gate the no-copy path on a
	// cheap entropy proxy.
	//
	// We use a prefix+suffix unique byte count to detect highly repetitive
	// values even when high-entropy bytes appear only in the tail.
	if len(value) == 0 {
		return false
	}
	const sampleBytes = 512
	prefixLen := len(value)
	suffixLen := 0
	if prefixLen > sampleBytes {
		prefixLen = sampleBytes / 2
		suffixLen = sampleBytes - prefixLen
	}

	var seen [4]uint64
	for i := 0; i < prefixLen; i++ {
		b := value[i]
		seen[b>>6] |= 1 << (b & 63)
	}
	if suffixLen > 0 {
		start := len(value) - suffixLen
		for i := start; i < len(value); i++ {
			b := value[i]
			seen[b>>6] |= 1 << (b & 63)
		}
	}
	unique := bits.OnesCount64(seen[0]) + bits.OnesCount64(seen[1]) + bits.OnesCount64(seen[2]) + bits.OnesCount64(seen[3])
	return unique <= 32
}

func shouldUseEncodeAllParts(records []Record, rawPayloadBytes int) bool {
	k := len(records)
	if k <= 1 || rawPayloadBytes <= 0 {
		return false
	}
	const (
		noCopyMinRawBytes = 128 << 10
		noCopyMinAvgBytes = 8 << 10
		// Allow a more aggressive no-copy path for ultra-compressible payloads
		// where the memcpy becomes a measurable fraction of wall time.
		noCopyUltraMinAvgBytes = 4 << 10
		// Small-value grouped frames (e.g. 128B values at large K) can still be
		// copy-bound on ultra-low-entropy streams.
		noCopyTinyUltraMinRawBytes = 8 << 10
		noCopyTinyUltraMinAvgBytes = 96
	)
	avg := rawPayloadBytes / k
	if rawPayloadBytes >= noCopyMinRawBytes && avg >= noCopyMinAvgBytes {
		return true
	}
	if rawPayloadBytes >= noCopyMinRawBytes && avg >= noCopyUltraMinAvgBytes {
		return isUltraLowEntropySample(records[0].Value)
	}
	if rawPayloadBytes >= noCopyTinyUltraMinRawBytes && avg >= noCopyTinyUltraMinAvgBytes {
		return isUltraLowEntropySample(records[0].Value)
	}
	return false
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
		f: f,
		// File-backed writers use direct writes and append buffers; bufio is only
		// needed for sink-backed writers (tests/benchmarks).
		bw:                    nil,
		size:                  info.Size(),
		fileID:                fileID,
		appendMax:             defaultBufferSize,
		rawWritevMinAvgBytes:  defaultRawWritevMinAvgBytes,
		rawWritevMinBatchRecs: defaultRawWritevMinBatchRecords,
		appendBuf:             make([]byte, 0, defaultBufferSize),
		prefixBuf:             make([]byte, 0, FrameHeaderSize+(MaxFrameK*8)+((MaxFrameK+1)*4)),
		dictFrameEncodeLevel:  zstd.SpeedFastest,
		blockCodec:            BlockCodecSnappy,
		syncFn:                func(file *os.File) error { return file.Sync() },
		clock:                 RealClock{},
		keepSafetyMargin:      DefaultKeepSafetyMargin,
	}, nil
}

func newWriterWithSink(sink io.Writer, fileID uint32) *Writer {
	return &Writer{
		bw:                    bufio.NewWriterSize(sink, defaultBufferSize),
		fileID:                fileID,
		appendMax:             0,
		rawWritevMinAvgBytes:  defaultRawWritevMinAvgBytes,
		rawWritevMinBatchRecs: defaultRawWritevMinBatchRecords,
		prefixBuf:             make([]byte, 0, FrameHeaderSize+(MaxFrameK*8)+((MaxFrameK+1)*4)),
		dictFrameEncodeLevel:  zstd.SpeedFastest,
		blockCodec:            BlockCodecSnappy,
		clock:                 RealClock{},
		keepSafetyMargin:      DefaultKeepSafetyMargin,
	}
}

// NewWriterWithSink creates a value-log writer that writes to the provided sink.
// Use this for sink-backed/custom outputs that do not provide file-backed durability.
func NewWriterWithSink(sink io.Writer, fileID uint32) *Writer {
	if sink == nil {
		return newWriterWithSink(io.Discard, fileID)
	}
	return newWriterWithSink(sink, fileID)
}

func (w *Writer) SetClock(clock Clock) {
	if w == nil {
		return
	}
	if clock == nil {
		w.clock = RealClock{}
		return
	}
	w.clock = clock
}

func normalizeDictFrameEncodeLevel(level zstd.EncoderLevel) zstd.EncoderLevel {
	switch level {
	case zstd.SpeedFastest, zstd.SpeedDefault, zstd.SpeedBetterCompression, zstd.SpeedBestCompression:
		return level
	default:
		return zstd.SpeedFastest
	}
}

func (w *Writer) SetDictFrameEncoderOptions(level zstd.EncoderLevel, enableEntropy bool) {
	if w == nil {
		return
	}
	w.dictFrameEncodeLevel = normalizeDictFrameEncodeLevel(level)
	w.dictFrameEnableEntropy = enableEntropy
	w.codecs = nil
	w.skipDictID = 0
	w.noBenefit = 0
	w.skipRemain = 0
}

// SetBlockCompression configures grouped frame block compression for dictID=0
// append paths.
func (w *Writer) SetBlockCompression(codec BlockCodec, enabled bool) {
	if w == nil {
		return
	}
	w.blockCodec = normalizeBlockCodec(codec)
	w.blockCompression = enabled
}

func (w *Writer) SetEncodeSampleStride(stride uint64) {
	if w == nil {
		return
	}
	w.encodeSampleStride = stride
}

func (w *Writer) SetEncodeCostModel(model EncodeCostModel) {
	if w == nil {
		return
	}
	w.encodeCostModel = model
}

func (w *Writer) SetKeepPolicy(ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin float64) {
	if w == nil {
		return
	}
	w.keepIoNsPerStoredByte = ioNsPerStoredByte
	w.keepEncodeNsPerRawByte = encodeNsPerRawByte
	if safetyMargin < 0 {
		safetyMargin = 0
	}
	w.keepSafetyMargin = safetyMargin
}

func normalizeRawWritevStrategy(minAvgBytes, minBatchRecs int) (int, int) {
	if minAvgBytes < 0 {
		minAvgBytes = 0
	}
	if minBatchRecs <= 0 {
		minBatchRecs = defaultRawWritevMinBatchRecords
	}
	return minAvgBytes, minBatchRecs
}

// SetRawWritevStrategy configures grouped raw writev usage heuristics.
//
// minAvgBytes:
//   - 0 enables adaptive mode (no average-bytes floor).
//   - >0 requires average payload bytes/record to meet this floor.
//
// minBatchRecs:
//   - <=0 uses the default.
func (w *Writer) SetRawWritevStrategy(minAvgBytes, minBatchRecs int) {
	if w == nil {
		return
	}
	w.rawWritevMinAvgBytes, w.rawWritevMinBatchRecs = normalizeRawWritevStrategy(minAvgBytes, minBatchRecs)
}

func (w *Writer) rawWritevStrategy() (int, int) {
	if w == nil {
		return 0, defaultRawWritevMinBatchRecords
	}
	return normalizeRawWritevStrategy(w.rawWritevMinAvgBytes, w.rawWritevMinBatchRecs)
}

// ResetCompressionHints clears skip/backoff state for deterministic benches.
func (w *Writer) ResetCompressionHints() {
	if w == nil {
		return
	}
	w.noBenefit = 0
	w.skipRemain = 0
	w.skipDictID = 0
}

func (w *Writer) sampleEncodeStart() time.Time {
	if w == nil {
		return time.Time{}
	}
	stride := w.encodeSampleStride
	if stride == 0 {
		return time.Time{}
	}
	w.encodeSampleCount++
	if stride > 1 && w.encodeSampleCount%stride != 0 {
		return time.Time{}
	}
	if w.clock == nil {
		w.clock = RealClock{}
	}
	return w.clock.Now()
}

func (w *Writer) sampleEncodeEnd(start time.Time, rawPayloadBytes, records int) int64 {
	if start.IsZero() {
		return 0
	}
	if w.encodeCostModel != nil {
		if ns := w.encodeCostModel.EncodeNs(rawPayloadBytes, records); ns > 0 {
			if adv, ok := w.clock.(interface{ Advance(int64) }); ok {
				adv.Advance(ns)
			}
			return ns
		}
	}
	if w.clock == nil {
		w.clock = RealClock{}
	}
	return w.clock.Now().Sub(start).Nanoseconds()
}

func (w *Writer) shouldKeepCompressed(rawPayloadBytes, encodedLen int, encodeNs int64) bool {
	encodeNsUsed := encodeNs
	if encodeNsUsed <= 0 && w.keepEncodeNsPerRawByte > 0 && rawPayloadBytes > 0 {
		encodeNsUsed = int64(w.keepEncodeNsPerRawByte * float64(rawPayloadBytes))
	}
	return ShouldKeepCompressed(rawPayloadBytes, encodedLen, encodeNsUsed, w.keepIoNsPerStoredByte, w.keepSafetyMargin)
}

func (w *Writer) shouldSkipCompression(rawPayloadBytes int) bool {
	if w == nil || rawPayloadBytes <= 0 {
		return false
	}
	if w.keepIoNsPerStoredByte <= 0 || w.keepEncodeNsPerRawByte <= 0 {
		return false
	}
	costPerRaw := w.keepEncodeNsPerRawByte
	if w.keepSafetyMargin > 0 {
		costPerRaw *= 1 + w.keepSafetyMargin
	}
	return w.keepIoNsPerStoredByte <= costPerRaw
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

// PendingBytes reports bytes accepted by the writer but not yet flushed to the
// underlying file descriptor. For file-backed writers this is the append-buffer
// tail that same-process readers cannot see through ReadAt until flushed.
func (w *Writer) PendingBytes() int {
	if w == nil {
		return 0
	}
	pending := len(w.appendBuf)
	if w.bw != nil {
		pending += w.bw.Buffered()
	}
	return pending
}

func (w *Writer) Flush() error {
	if err := w.flushNoTrim(); err != nil {
		return err
	}
	w.trimTransientScratchBuffers()
	return nil
}

func (w *Writer) RotateTo(path string, fileID uint32) error {
	if w == nil {
		return errors.New("valuelog: nil writer")
	}

	if w.f == nil {
		if w.bw != nil {
			// Preserve sink semantics for tests before switching to file-backed.
			if err := w.flushNoTrim(); err != nil {
				return err
			}
		}
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
		// File-backed writers do not use bufio; drop any sink buffer.
		w.bw = nil
		w.size = info.Size()
		w.fileID = fileID
		w.appendMax = defaultBufferSize
		if cap(w.appendBuf) < defaultBufferSize {
			w.appendBuf = make([]byte, 0, defaultBufferSize)
		} else {
			w.appendBuf = w.appendBuf[:0]
		}
		w.trimTransientScratchBuffers()
		return nil
	}

	if err := w.flushNoTrim(); err != nil {
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
	if w.bw != nil {
		if err := w.bw.Flush(); err != nil {
			_ = f.Close()
			return err
		}
	}
	w.f = f
	// File-backed writers do not use bufio. Drop any leftover sink buffer
	// rather than retargeting it across rotations.
	w.bw = nil
	w.size = info.Size()
	w.fileID = fileID
	w.appendBuf = w.appendBuf[:0]
	if err := old.Close(); err != nil {
		return err
	}
	w.trimTransientScratchBuffers()
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
	if dictID == 0 && w.blockCompression {
		var (
			rec [1]Record
			dst [1]page.ValuePtr
		)
		rec[0] = Record{RID: rid, Value: value}
		ptrs, _, err := w.AppendFrameWithStatsInto(0, nil, rec[:], dst[:])
		if err != nil {
			return page.ValuePtr{}, err
		}
		if len(ptrs) != 1 {
			return page.ValuePtr{}, ErrCorrupt
		}
		return ptrs[0], nil
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
		max := w.appendMax
		if max <= 0 {
			max = defaultBufferSize
		}

		// Hot path: build directly into the writer append buffer to avoid copying
		// through a scratch buffer.
		if w.f != nil && recordLen <= max {
			if len(w.appendBuf)+recordLen > max {
				if err := w.flushAppendBuf(); err != nil {
					return page.ValuePtr{}, err
				}
			}
			if cap(w.appendBuf) < max {
				w.appendBuf = make([]byte, len(w.appendBuf), max)
			}
			base := len(w.appendBuf)
			w.appendBuf = w.appendBuf[:base+recordLen]
			buf := w.appendBuf[base : base+recordLen]

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
			w.size += int64(recordLen)

			if len(w.appendBuf) >= max {
				if err := w.flushAppendBuf(); err != nil {
					return page.ValuePtr{}, err
				}
			}

			recordLenHint := uint32(headerWithoutCRC) + bodyLen
			if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
				recordLenHint = 0
			}
			return page.ValuePtr{
				Offset: uint64(start + 4),
				Length: page.ValuePtrMarkGrouped(recordLenHint, 0),
				FileID: w.fileID,
			}, nil
		}

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

		if err := w.writeBytes(buf); err != nil {
			return page.ValuePtr{}, err
		}
		w.size += int64(recordLen)

		recordLenHint := uint32(headerWithoutCRC) + bodyLen
		if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
			recordLenHint = 0
		}
		return page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, 0),
			FileID: w.fileID,
		}, nil
	}

	ptrs, err := w.AppendFrame(dictID, dict, []Record{{RID: rid, Value: value}})
	if err != nil {
		return page.ValuePtr{}, err
	}
	return ptrs[0], nil
}

// AppendCompactLeafPage appends a compact split-leaf payload directly from the
// source page when compaction is beneficial, avoiding a transient merged
// payload allocation. When the page does not benefit from compaction, it falls
// back to a normal raw append.
func (w *Writer) AppendCompactLeafPage(rid uint64, leafPage []byte) (page.ValuePtr, FrameStats, bool, error) {
	if w == nil {
		return page.ValuePtr{}, FrameStats{}, false, errors.New("valuelog: nil writer")
	}
	if rid == 0 {
		return page.ValuePtr{}, FrameStats{}, false, errors.New("valuelog: missing rid")
	}
	prefixLen, suffixStart, suffixLen, compacted, err := compactLeafPageLivePayloadBounds(leafPage)
	if err != nil {
		return page.ValuePtr{}, FrameStats{}, false, err
	}
	payloadLen := len(leafPage)
	if compacted {
		payloadLen = compactLeafPagePayloadLen(prefixLen, suffixLen)
	}
	if w.blockCompression {
		var (
			rec [1]Record
			dst [1]page.ValuePtr
		)
		if compacted {
			if cap(w.rawScratch) < payloadLen {
				w.rawScratch = make([]byte, payloadLen)
			}
			payload := w.rawScratch[:payloadLen]
			encodeCompactLeafLogPayload(payload, leafPage, prefixLen, suffixStart, suffixLen)
			rec[0] = Record{RID: rid, Value: payload}
		} else {
			rec[0] = Record{RID: rid, Value: leafPage}
		}
		ptrs, stats, appendErr := w.appendBlockFrameWithStats(rec[:], payloadLen, dst[:])
		if appendErr != nil {
			return page.ValuePtr{}, FrameStats{}, compacted, appendErr
		}
		if len(ptrs) != 1 {
			return page.ValuePtr{}, FrameStats{}, compacted, ErrCorrupt
		}
		return ptrs[0], stats, compacted, nil
	}
	if !compacted {
		ptr, appendErr := w.Append(0, nil, rid, leafPage)
		if appendErr != nil {
			return page.ValuePtr{}, FrameStats{}, false, appendErr
		}
		return ptr, FrameStats{Records: 1, RawPayloadBytes: len(leafPage), StoredPayloadBytes: len(leafPage), Kept: false}, false, nil
	}

	bodyLen := uint32(FrameHeaderSize + 8 + 8 + payloadLen)
	if recordSizeExceedsMax(bodyLen) {
		return page.ValuePtr{}, FrameStats{}, false, ErrRecordTooLarge
	}

	recordLen := HeaderSize + int(bodyLen)
	start := w.size
	max := w.appendMax
	if max <= 0 {
		max = defaultBufferSize
	}
	recordLenHint := uint32(headerWithoutCRC) + bodyLen
	if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
		recordLenHint = 0
	}

	writeRecord := func(buf []byte) {
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
		binary.LittleEndian.PutUint32(buf[off+4:off+8], uint32(payloadLen))
		off += 8
		encodeCompactLeafLogPayload(buf[off:off+payloadLen], leafPage, prefixLen, suffixStart, suffixLen)

		sum := crc.ChecksumParts(buf[4:HeaderSize], buf[HeaderSize:])
		binary.LittleEndian.PutUint32(buf[0:4], sum)
	}

	if w.f != nil && recordLen <= max {
		if len(w.appendBuf)+recordLen > max {
			if err := w.flushAppendBuf(); err != nil {
				return page.ValuePtr{}, FrameStats{}, false, err
			}
		}
		if cap(w.appendBuf) < max {
			w.appendBuf = make([]byte, len(w.appendBuf), max)
		}
		base := len(w.appendBuf)
		w.appendBuf = w.appendBuf[:base+recordLen]
		buf := w.appendBuf[base : base+recordLen]
		writeRecord(buf)
		w.size += int64(recordLen)

		if len(w.appendBuf) >= max {
			if err := w.flushAppendBuf(); err != nil {
				return page.ValuePtr{}, FrameStats{}, false, err
			}
		}
		return page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, 0),
			FileID: w.fileID,
		}, FrameStats{Records: 1, RawPayloadBytes: payloadLen, StoredPayloadBytes: payloadLen, Kept: false}, true, nil
	}

	if cap(w.scratch) < recordLen {
		w.scratch = make([]byte, recordLen)
	}
	buf := w.scratch[:recordLen]
	writeRecord(buf)
	if err := w.writeBytes(buf); err != nil {
		return page.ValuePtr{}, FrameStats{}, false, err
	}
	w.size += int64(recordLen)
	return page.ValuePtr{
		Offset: uint64(start + 4),
		Length: page.ValuePtrMarkGrouped(recordLenHint, 0),
		FileID: w.fileID,
	}, FrameStats{Records: 1, RawPayloadBytes: payloadLen, StoredPayloadBytes: payloadLen, Kept: false}, true, nil
}

func (w *Writer) AppendOneFrameWithStats(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, FrameStats, error) {
	if w == nil {
		return page.ValuePtr{}, FrameStats{}, errors.New("valuelog: nil writer")
	}
	if rid == 0 {
		return page.ValuePtr{}, FrameStats{}, errors.New("valuelog: missing rid")
	}
	var (
		rec [1]Record
		dst [1]page.ValuePtr
	)
	rec[0] = Record{RID: rid, Value: value}
	ptrs, stats, err := w.AppendFrameWithStatsInto(dictID, dict, rec[:], dst[:])
	if err != nil {
		return page.ValuePtr{}, FrameStats{}, err
	}
	if len(ptrs) != 1 {
		return page.ValuePtr{}, FrameStats{}, ErrCorrupt
	}
	return ptrs[0], stats, nil
}

// AppendRawRecord appends a raw value-log record (CRC + header + payload)
// without re-encoding. The length argument should include any pointer flags.
func (w *Writer) AppendRawRecord(raw []byte, length uint32) (page.ValuePtr, error) {
	if w == nil {
		return page.ValuePtr{}, errors.New("valuelog: nil writer")
	}
	if len(raw) < 4 {
		return page.ValuePtr{}, errors.New("valuelog: empty record")
	}
	expected := uint32(len(raw) - 4)
	if !page.ValuePtrRecordLengthHintMatches(page.ValuePtr{Length: length}, expected) {
		return page.ValuePtr{}, errors.New("valuelog: raw record size mismatch")
	}
	start := w.size
	if err := w.writeBytes(raw); err != nil {
		return page.ValuePtr{}, err
	}
	w.size += int64(len(raw))
	return page.ValuePtr{
		Offset: uint64(start + 4),
		Length: length,
		FileID: w.fileID,
	}, nil
}

// AppendRawRecordBuffered appends a pre-encoded raw value-log record through the
// writer append buffer. This keeps syscall coalescing for records prepared
// outside the writer's critical section.
func (w *Writer) AppendRawRecordBuffered(raw []byte, length uint32) (page.ValuePtr, error) {
	if w == nil {
		return page.ValuePtr{}, errors.New("valuelog: nil writer")
	}
	if len(raw) < 4 {
		return page.ValuePtr{}, errors.New("valuelog: empty record")
	}
	expected := uint32(len(raw) - 4)
	if !page.ValuePtrRecordLengthHintMatches(page.ValuePtr{Length: length}, expected) {
		return page.ValuePtr{}, errors.New("valuelog: raw record size mismatch")
	}
	start := w.size
	if err := w.writeBytesBuffered(raw); err != nil {
		return page.ValuePtr{}, err
	}
	w.size += int64(len(raw))
	return page.ValuePtr{
		Offset: uint64(start + 4),
		Length: length,
		FileID: w.fileID,
	}, nil
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

// AppendEncodedFrameInto appends a pre-encoded grouped frame body and fills dst
// with grouped value pointers (dst must be at least k long).
//
// The body must be produced by EncodeFrame/EncodeFrameWithOptions.
func (w *Writer) AppendEncodedFrameInto(body []byte, k int, dst []page.ValuePtr) ([]page.ValuePtr, error) {
	if w == nil {
		return nil, errors.New("valuelog: nil writer")
	}
	if k <= 0 {
		return dst[:0], nil
	}
	if k > MaxFrameK {
		return nil, ErrRecordTooLarge
	}
	if len(dst) < k {
		return nil, errors.New("valuelog: dst too small")
	}
	if len(body) < FrameHeaderSize {
		return nil, ErrCorrupt
	}
	if body[0] != FrameVersion {
		return nil, ErrCorrupt
	}
	if int(body[2]) != k {
		return nil, ErrCorrupt
	}
	if len(body) > int(^uint32(0)) {
		return nil, ErrRecordTooLarge
	}
	bodyLen := uint32(len(body))
	if recordSizeExceedsMax(bodyLen) {
		return nil, ErrRecordTooLarge
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

	if err := w.writeBytes(buf); err != nil {
		return nil, err
	}
	w.size += int64(recordLen)

	recordLenHint := uint32(headerWithoutCRC) + bodyLen
	if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
		recordLenHint = 0
	}
	for i := 0; i < k; i++ {
		dst[i] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
			FileID: w.fileID,
		}
	}
	return dst[:k], nil
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
	if len(records) == 1 && dictID == 0 && !w.blockCompression {
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

		if err := w.writeBytes(buf); err != nil {
			return nil, FrameStats{}, err
		}
		w.size += int64(recordLen)

		recordLenHint := uint32(headerWithoutCRC) + bodyLen
		if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
			recordLenHint = 0
		}
		dst[0] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, 0),
			FileID: w.fileID,
		}
		return dst[:1], FrameStats{Records: 1, RawPayloadBytes: len(rec.Value), StoredPayloadBytes: len(rec.Value), Kept: false}, nil
	}

	rawPayloadBytes := 0
	for i := range records {
		rawPayloadBytes += len(records[i].Value)
	}

	if dictID == 0 && w.blockCompression {
		return w.appendBlockFrameWithStats(records, rawPayloadBytes, dst)
	}

	// Fast path: grouped frame with no dict/compression.
	//
	// We can write the frame prefix + raw values directly without first
	// concatenating the payload into a temporary buffer.
	//
	// This matters for high-throughput append workloads (e.g. IAVL node storage)
	// where dict compression is disabled or not yet available.
	if dictID == 0 {
		k := len(records)
		if k <= 0 || k > MaxFrameK {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if rawPayloadBytes > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if limits.MaxRecordSize > 0 && int64(rawPayloadBytes) > limits.MaxRecordSize {
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

		bodyLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4) + rawPayloadBytes
		if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if bodyLen > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if recordSizeExceedsMax(uint32(bodyLen)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		start := w.size
		prefixLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4)
		totalLen := HeaderSize + prefixLen + rawPayloadBytes
		max := w.appendMax
		if max <= 0 {
			max = defaultBufferSize
		}

		// Hot path (wal_off): write directly into the writer append buffer so we
		// don't copy `frame` into `appendBuf` after building it.
		if w.f != nil && totalLen <= max {
			if len(w.appendBuf)+totalLen > max {
				if err := w.flushAppendBuf(); err != nil {
					return nil, FrameStats{}, err
				}
			}
			if cap(w.appendBuf) < max {
				w.appendBuf = make([]byte, len(w.appendBuf), max)
			}
			base := len(w.appendBuf)
			w.appendBuf = w.appendBuf[:base+totalLen]
			frame := w.appendBuf[base : base+totalLen]

			frame[4] = Version
			frame[5] = recordFlagGrouped
			frame[6] = 0
			frame[7] = 0
			binary.LittleEndian.PutUint64(frame[8:16], 0)
			binary.LittleEndian.PutUint32(frame[16:20], uint32(bodyLen))

			off := HeaderSize
			frame[off] = FrameVersion
			frame[off+1] = 0
			frame[off+2] = byte(k)
			frame[off+3] = 0
			binary.LittleEndian.PutUint64(frame[off+4:off+12], 0)
			off += FrameHeaderSize

			for i := 0; i < k; i++ {
				rid := records[i].RID
				if rid == 0 {
					return nil, FrameStats{}, errors.New("valuelog: missing rid")
				}
				binary.LittleEndian.PutUint64(frame[off:off+8], rid)
				off += 8
			}
			for i := 0; i < k+1; i++ {
				binary.LittleEndian.PutUint32(frame[off:off+4], offsets[i])
				off += 4
			}

			for i := 0; i < k; i++ {
				copy(frame[off:], records[i].Value)
				off += len(records[i].Value)
			}

			sum := crc.ChecksumParts(frame[4:HeaderSize], frame[HeaderSize:])
			binary.LittleEndian.PutUint32(frame[0:4], sum)
			w.size += int64(HeaderSize + bodyLen)

			if len(w.appendBuf) >= max {
				if err := w.flushAppendBuf(); err != nil {
					return nil, FrameStats{}, err
				}
			}
		} else {
			if cap(w.prefixBuf) < prefixLen {
				w.prefixBuf = make([]byte, 0, prefixLen)
			}
			prefix := w.prefixBuf[:prefixLen]
			prefixOff := 0
			prefix[prefixOff] = FrameVersion
			prefix[prefixOff+1] = 0
			prefix[prefixOff+2] = byte(k)
			prefix[prefixOff+3] = 0
			binary.LittleEndian.PutUint64(prefix[prefixOff+4:prefixOff+12], 0)
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

			const maxKeepScratch = 16 << 20 // 16 MiB
			var header [HeaderSize]byte
			header[4] = Version
			header[5] = recordFlagGrouped
			header[6] = 0
			header[7] = 0
			binary.LittleEndian.PutUint64(header[8:16], 0)
			binary.LittleEndian.PutUint32(header[16:20], uint32(bodyLen))

			var frame []byte
			if totalLen <= maxKeepScratch {
				if cap(w.rawScratch) < totalLen {
					w.rawScratch = make([]byte, totalLen)
				}
				frame = w.rawScratch[:totalLen]
			} else {
				frame = make([]byte, totalLen)
			}
			copy(frame[0:HeaderSize], header[:])
			copy(frame[HeaderSize:HeaderSize+prefixLen], prefix)
			off := HeaderSize + prefixLen
			for i := 0; i < k; i++ {
				copy(frame[off:], records[i].Value)
				off += len(records[i].Value)
			}
			sum := crc.ChecksumParts(frame[4:HeaderSize], frame[HeaderSize:])
			binary.LittleEndian.PutUint32(frame[0:4], sum)
			if err := w.writeFrameBatch(frame); err != nil {
				return nil, FrameStats{}, err
			}
			w.size += int64(HeaderSize + bodyLen)
		}

		recordLenHint := uint32(headerWithoutCRC) + uint32(bodyLen)
		if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
			recordLenHint = 0
		}
		for i := range records {
			dst[i] = page.ValuePtr{
				Offset: uint64(start + 4),
				Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
				FileID: w.fileID,
			}
		}

		return dst, FrameStats{
			Records:            k,
			RawPayloadBytes:    rawPayloadBytes,
			StoredPayloadBytes: rawPayloadBytes,
			Kept:               false,
		}, nil
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
		if limits.MaxRecordSize > 0 && int64(rawPayloadBytes) > limits.MaxRecordSize {
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

		writeRaw := func(attempted bool, encodeNs int64) ([]page.ValuePtr, FrameStats, error) {
			ptrs, stats, err := w.appendRawFrameWithDictID(dictID, records, &offsets, rawPayloadBytes, dst)
			if err != nil {
				return nil, FrameStats{}, err
			}
			if attempted {
				stats.Attempted = true
				stats.EncodeNs = encodeNs
			}
			return ptrs, stats, nil
		}

		// If recent probes show compression yields no benefit, temporarily skip
		// zstd. We periodically probe again so we can adapt if data changes.
		forceDictProbe := shouldForceDictProbe(rawPayloadBytes)
		if w.skipRemain > 0 {
			if !shouldProbeLargeDictDuringBackoff(w.skipRemain, rawPayloadBytes) {
				w.skipRemain--
				return writeRaw(false, 0)
			}
			w.skipRemain--
		}
		if w.shouldSkipCompression(rawPayloadBytes) && !forceDictProbe {
			return writeRaw(false, 0)
		}

		level := normalizeDictFrameEncodeLevel(w.dictFrameEncodeLevel)
		noEntropy := !w.dictFrameEnableEntropy
		key := dictCodecKey{dictID: dictID, level: level, noEntropy: noEntropy}

		codecs := w.codecs
		if codecs == nil || codecs.key != key {
			codecs = getDictCodecsWithOpts(dictID, dict, level, noEntropy)
			if codecs != nil {
				w.codecs = codecs
			}
		}
		if codecs == nil || codecs.encPool == nil {
			return nil, FrameStats{}, ErrMissingDict
		}
		if rawPayloadBytes == 0 {
			return writeRaw(false, 0)
		}

		prefixLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4)
		start := w.size

		max := w.appendMax
		if max <= 0 {
			max = defaultBufferSize
		}

		enc := codecs.encPool.Get().(*zstd.Encoder)

		canDirectEncodeAll := w.f != nil && (k == 1 || rawPayloadBytes <= (1<<20))
		if canDirectEncodeAll {
			recordMaxLen := HeaderSize + prefixLen + enc.MaxEncodedSize(rawPayloadBytes)
			// EncodeAllParts can emit more blocks than EncodeAll (but still within a
			// single frame). Block overhead is 3 bytes, so add a small margin that
			// covers the worst-case extra blocks (k-1).
			if k > 1 {
				recordMaxLen += 3 * (k - 1)
			}
			if recordMaxLen > max {
				canDirectEncodeAll = false
			}
		}

		if canDirectEncodeAll {
			recordMaxLen := HeaderSize + prefixLen + enc.MaxEncodedSize(rawPayloadBytes)
			if k > 1 {
				recordMaxLen += 3 * (k - 1)
			}
			if len(w.appendBuf)+recordMaxLen > max {
				if err := w.flushAppendBuf(); err != nil {
					codecs.encPool.Put(enc)
					return nil, FrameStats{}, err
				}
			}
			if cap(w.appendBuf) < max {
				newBuf := make([]byte, len(w.appendBuf), max)
				copy(newBuf, w.appendBuf)
				w.appendBuf = newBuf
			}

			recordStart := len(w.appendBuf)
			headerEnd := recordStart + HeaderSize
			encodedStart := headerEnd + prefixLen
			w.appendBuf = w.appendBuf[:encodedStart]
			header := w.appendBuf[recordStart:headerEnd]
			prefix := w.appendBuf[headerEnd:encodedStart]

			// Build prefix in-place (compressed).
			prefixOff := 0
			prefix[prefixOff] = FrameVersion
			prefix[prefixOff+1] = FrameFlagCompressed
			prefix[prefixOff+2] = byte(k)
			prefix[prefixOff+3] = 0
			binary.LittleEndian.PutUint64(prefix[prefixOff+4:prefixOff+12], dictID)
			prefixOff += FrameHeaderSize

			for i := 0; i < k; i++ {
				rid := records[i].RID
				if rid == 0 {
					w.appendBuf = w.appendBuf[:recordStart]
					codecs.encPool.Put(enc)
					return nil, FrameStats{}, errors.New("valuelog: missing rid")
				}
				binary.LittleEndian.PutUint64(prefix[prefixOff:prefixOff+8], rid)
				prefixOff += 8
			}
			for i := 0; i < k+1; i++ {
				binary.LittleEndian.PutUint32(prefix[prefixOff:prefixOff+4], offsets[i])
				prefixOff += 4
			}

			useNoCopyParts := shouldUseEncodeAllParts(records[:k], rawPayloadBytes)
			var payload []byte
			var parts [MaxFrameK][]byte
			if k == 1 {
				payload = records[0].Value
			} else if !useNoCopyParts {
				// For small/medium grouped frames, EncodeAll on a contiguous payload
				// is typically faster than streaming many small writes.
				if cap(w.rawScratch) < rawPayloadBytes {
					w.rawScratch = make([]byte, rawPayloadBytes)
				}
				payload = w.rawScratch[:rawPayloadBytes]
				off := 0
				for i := 0; i < k; i++ {
					off += copy(payload[off:], records[i].Value)
				}
			} else {
				for i := 0; i < k; i++ {
					parts[i] = records[i].Value
				}
			}
			encodeStart := w.sampleEncodeStart()
			if useNoCopyParts {
				w.appendBuf = enc.EncodeAllParts(parts[:k], w.appendBuf)
			} else {
				w.appendBuf = enc.EncodeAll(payload, w.appendBuf)
			}
			encodeNs := w.sampleEncodeEnd(encodeStart, rawPayloadBytes, k)
			codecs.encPool.Put(enc)

			encodedLen := len(w.appendBuf) - encodedStart
			keepCompressed := w.shouldKeepCompressed(rawPayloadBytes, encodedLen, encodeNs)
			if !keepCompressed && shouldForceKeepLargeDictCompressed(rawPayloadBytes, encodedLen) {
				keepCompressed = true
			}
			if !keepCompressed {
				w.appendBuf = w.appendBuf[:recordStart]
				if w.noBenefit < 0xff {
					w.noBenefit++
				}
				w.skipRemain = dictSkipFramesAggressive(w.noBenefit, rawPayloadBytes, encodedLen, encodeNs, w.keepIoNsPerStoredByte, w.keepSafetyMargin)
				return writeRaw(true, encodeNs)
			}

			bodyLen := prefixLen + encodedLen
			if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
				w.appendBuf = w.appendBuf[:recordStart]
				return nil, FrameStats{}, ErrRecordTooLarge
			}
			if bodyLen > int(^uint32(0)) {
				w.appendBuf = w.appendBuf[:recordStart]
				return nil, FrameStats{}, ErrRecordTooLarge
			}
			if recordSizeExceedsMax(uint32(bodyLen)) {
				w.appendBuf = w.appendBuf[:recordStart]
				return nil, FrameStats{}, ErrRecordTooLarge
			}

			// Build header in-place (now that we know bodyLen).
			header[4] = Version
			header[5] = recordFlagGrouped
			header[6] = 0
			header[7] = 0
			binary.LittleEndian.PutUint64(header[8:16], 0)
			binary.LittleEndian.PutUint32(header[16:20], uint32(bodyLen))

			encoded := w.appendBuf[encodedStart:]
			sum := crc.ChecksumParts(header[4:HeaderSize], prefix, encoded)
			binary.LittleEndian.PutUint32(header[0:4], sum)

			w.size += int64(HeaderSize + bodyLen)
			w.noBenefit = 0
			w.skipRemain = 0

			if len(w.appendBuf) >= max {
				if err := w.flushAppendBuf(); err != nil {
					return nil, FrameStats{}, err
				}
			}

			recordLenHint := uint32(headerWithoutCRC) + uint32(bodyLen)
			if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
				recordLenHint = 0
			}
			for i := range records {
				dst[i] = page.ValuePtr{
					Offset: uint64(start + 4),
					Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
					FileID: w.fileID,
				}
			}

			return dst, FrameStats{
				Records:            k,
				RawPayloadBytes:    rawPayloadBytes,
				StoredPayloadBytes: encodedLen,
				Attempted:          true,
				Kept:               true,
				EncodeNs:           encodeNs,
			}, nil
		}

		if cap(w.encScratch) < rawPayloadBytes {
			w.encScratch = make([]byte, 0, rawPayloadBytes)
		}
		encDst := w.encScratch[:0]

		var encoded []byte
		var encodeErr error
		encodeStart := w.sampleEncodeStart()
		if k == 1 {
			encoded = enc.EncodeAll(records[0].Value, encDst)
		} else if rawPayloadBytes <= (1 << 20) {
			// For small/medium grouped frames, it can be faster to encode a single
			// contiguous payload via EncodeAll than to stream many small writes.
			//
			// This also avoids per-call allocations in the streaming encoder path.
			useNoCopyParts := shouldUseEncodeAllParts(records[:k], rawPayloadBytes)
			if useNoCopyParts {
				var parts [MaxFrameK][]byte
				for i := 0; i < k; i++ {
					parts[i] = records[i].Value
				}
				encoded = enc.EncodeAllParts(parts[:k], encDst)
			} else {
				if cap(w.rawScratch) < rawPayloadBytes {
					w.rawScratch = make([]byte, rawPayloadBytes)
				}
				payload := w.rawScratch[:rawPayloadBytes]
				off := 0
				for i := 0; i < k; i++ {
					off += copy(payload[off:], records[i].Value)
				}
				encoded = enc.EncodeAll(payload, encDst)
			}
		} else {
			w.encLimiter.buf = encDst
			w.encLimiter.limit = rawPayloadBytes - 1
			enc.Reset(&w.encLimiter)
			for i := 0; i < k; i++ {
				if _, encodeErr = enc.Write(records[i].Value); encodeErr != nil {
					break
				}
			}
			if encodeErr == nil {
				encodeErr = enc.Close()
			}
			enc.Reset(nil)
			encoded = w.encLimiter.buf
		}
		encodeNs := w.sampleEncodeEnd(encodeStart, rawPayloadBytes, k)
		codecs.encPool.Put(enc)
		w.encScratch = w.encScratch[:0]

		keepCompressed := false
		if encodeErr == nil {
			keepCompressed = w.shouldKeepCompressed(rawPayloadBytes, len(encoded), encodeNs)
			if !keepCompressed && shouldForceKeepLargeDictCompressed(rawPayloadBytes, len(encoded)) {
				keepCompressed = true
			}
		}
		if encodeErr != nil && !errors.Is(encodeErr, errEncodedTooLarge) {
			return nil, FrameStats{}, encodeErr
		}
		if !keepCompressed {
			// No benefit (or we aborted early because the output grew too large).
			if w.noBenefit < 0xff {
				w.noBenefit++
			}
			w.skipRemain = dictSkipFramesAggressive(w.noBenefit, rawPayloadBytes, len(encoded), encodeNs, w.keepIoNsPerStoredByte, w.keepSafetyMargin)
			return writeRaw(true, encodeNs)
		}
		storedPayloadBytes := len(encoded)
		w.noBenefit = 0
		w.skipRemain = 0

		bodyLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4) + len(encoded)
		if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if bodyLen > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if recordSizeExceedsMax(uint32(bodyLen)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		var header [HeaderSize]byte
		header[4] = Version
		header[5] = recordFlagGrouped
		header[6] = 0
		header[7] = 0
		binary.LittleEndian.PutUint64(header[8:16], 0)
		binary.LittleEndian.PutUint32(header[16:20], uint32(bodyLen))

		if cap(w.prefixBuf) < prefixLen {
			w.prefixBuf = make([]byte, 0, prefixLen)
		}
		prefix := w.prefixBuf[:prefixLen]
		prefixOff := 0
		prefix[prefixOff] = FrameVersion
		prefix[prefixOff+1] = FrameFlagCompressed
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

		if err := w.writeBytes(header[:]); err != nil {
			return nil, FrameStats{}, err
		}
		if err := w.writeBytes(prefix); err != nil {
			return nil, FrameStats{}, err
		}
		if err := w.writeBytes(encoded); err != nil {
			return nil, FrameStats{}, err
		}
		w.size += int64(HeaderSize + bodyLen)

		recordLenHint := uint32(headerWithoutCRC) + uint32(bodyLen)
		if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
			recordLenHint = 0
		}
		for i := range records {
			dst[i] = page.ValuePtr{
				Offset: uint64(start + 4),
				Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
				FileID: w.fileID,
			}
		}

		return dst, FrameStats{
			Records:            k,
			RawPayloadBytes:    rawPayloadBytes,
			StoredPayloadBytes: storedPayloadBytes,
			Attempted:          true,
			Kept:               true,
			EncodeNs:           encodeNs,
		}, nil
	}

	body, header, err := EncodeFrameWithOptions(dictID, dict, records, w.dictFrameEncodeLevel, w.dictFrameEnableEntropy)
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

	if err := w.writeBytes(buf); err != nil {
		return nil, FrameStats{}, err
	}
	w.size += int64(recordLen)

	recordLenHint := uint32(headerWithoutCRC) + bodyLen
	if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
		recordLenHint = 0
	}
	for i := range records {
		dst[i] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
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
		Attempted:          dictID != 0 && len(dict) > 0 && rawPayloadBytes > 0,
		Kept:               header.Flags&FrameFlagCompressed != 0,
	}, nil
}

func (w *Writer) appendBlockFrameWithStats(records []Record, rawPayloadBytes int, dst []page.ValuePtr) ([]page.ValuePtr, FrameStats, error) {
	if w == nil {
		return nil, FrameStats{}, errors.New("valuelog: nil writer")
	}
	// Skip/backoff hints are shared with dict mode; reset when crossing into
	// block mode so stale dict hints do not suppress block probes.
	if w.skipDictID != 0 {
		w.skipDictID = 0
		w.noBenefit = 0
		w.skipRemain = 0
	}
	k := len(records)
	if k <= 0 {
		return dst[:0], FrameStats{}, nil
	}
	if k > MaxFrameK {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	if len(dst) < k {
		return nil, FrameStats{}, errors.New("valuelog: dst too small")
	}
	dst = dst[:k]
	if rawPayloadBytes < 0 || rawPayloadBytes > int(^uint32(0)) {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	if limits.MaxRecordSize > 0 && int64(rawPayloadBytes) > limits.MaxRecordSize {
		return nil, FrameStats{}, ErrRecordTooLarge
	}

	var offsets [MaxFrameK + 1]uint32
	offsets[0] = 0
	payloadOff := 0
	for i := 0; i < k; i++ {
		rid := records[i].RID
		if rid == 0 {
			return nil, FrameStats{}, errors.New("valuelog: missing rid")
		}
		payloadOff += len(records[i].Value)
		if payloadOff < 0 || payloadOff > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		offsets[i+1] = uint32(payloadOff)
	}

	writeRaw := func(attempted bool, encodeNs int64) ([]page.ValuePtr, FrameStats, error) {
		ptrs, stats, err := w.appendRawFrameWithDictID(0, records, &offsets, rawPayloadBytes, dst)
		if err != nil {
			return nil, FrameStats{}, err
		}
		if attempted {
			stats.Attempted = true
			stats.EncodeNs = encodeNs
		}
		return ptrs, stats, nil
	}

	if rawPayloadBytes == 0 {
		return writeRaw(false, 0)
	}
	if w.skipRemain > 0 {
		w.skipRemain--
		return writeRaw(false, 0)
	}
	if w.shouldSkipCompression(rawPayloadBytes) {
		return writeRaw(false, 0)
	}

	if cap(w.rawScratch) < rawPayloadBytes {
		w.rawScratch = make([]byte, rawPayloadBytes)
	}
	payload := w.rawScratch[:rawPayloadBytes]
	off := 0
	for i := 0; i < k; i++ {
		off += copy(payload[off:], records[i].Value)
	}

	encodeStart := w.sampleEncodeStart()
	encoded, encodeErr := encodeBlockPayload(w.blockCodec, payload, w.blockScratch[:0])
	encodeNs := w.sampleEncodeEnd(encodeStart, rawPayloadBytes, k)
	if encoded != nil {
		w.blockScratch = encoded[:0]
	}
	keepCompressed := false
	if encodeErr == nil {
		keepCompressed = w.shouldKeepCompressed(rawPayloadBytes, len(encoded), encodeNs)
	}
	if encodeErr != nil && !errors.Is(encodeErr, errEncodedTooLarge) {
		if w.noBenefit < 0xff {
			w.noBenefit++
		}
		w.skipRemain = dictSkipFramesAggressive(w.noBenefit, rawPayloadBytes, len(encoded), encodeNs, w.keepIoNsPerStoredByte, w.keepSafetyMargin)
		return writeRaw(true, encodeNs)
	}
	if !keepCompressed {
		if w.noBenefit < 0xff {
			w.noBenefit++
		}
		w.skipRemain = dictSkipFramesAggressive(w.noBenefit, rawPayloadBytes, len(encoded), encodeNs, w.keepIoNsPerStoredByte, w.keepSafetyMargin)
		return writeRaw(true, encodeNs)
	}
	w.noBenefit = 0
	w.skipRemain = 0

	bodyLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4) + len(encoded)
	if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	if bodyLen > int(^uint32(0)) {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	if recordSizeExceedsMax(uint32(bodyLen)) {
		return nil, FrameStats{}, ErrRecordTooLarge
	}

	recordLen := HeaderSize + bodyLen
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
	binary.LittleEndian.PutUint32(buf[16:20], uint32(bodyLen))

	frameOff := HeaderSize
	buf[frameOff] = FrameVersion
	buf[frameOff+1] = FrameFlagCompressed
	buf[frameOff+2] = byte(k)
	buf[frameOff+3] = byte(w.blockCodec)
	binary.LittleEndian.PutUint64(buf[frameOff+4:frameOff+12], 0)
	frameOff += FrameHeaderSize
	for i := 0; i < k; i++ {
		binary.LittleEndian.PutUint64(buf[frameOff:frameOff+8], records[i].RID)
		frameOff += 8
	}
	for i := 0; i < k+1; i++ {
		binary.LittleEndian.PutUint32(buf[frameOff:frameOff+4], offsets[i])
		frameOff += 4
	}
	copy(buf[frameOff:], encoded)

	sum := crc.ChecksumParts(buf[4:HeaderSize], buf[HeaderSize:])
	binary.LittleEndian.PutUint32(buf[0:4], sum)
	if err := w.writeBytes(buf); err != nil {
		return nil, FrameStats{}, err
	}
	w.size += int64(recordLen)

	recordLenHint := uint32(headerWithoutCRC) + uint32(bodyLen)
	if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
		recordLenHint = 0
	}
	for i := range records {
		dst[i] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
			FileID: w.fileID,
		}
	}

	return dst, FrameStats{
		Records:            k,
		RawPayloadBytes:    rawPayloadBytes,
		StoredPayloadBytes: len(encoded),
		Attempted:          true,
		Kept:               true,
		EncodeNs:           encodeNs,
	}, nil
}

func (w *Writer) appendRawFrameWithDictID(dictID uint64, records []Record, offsets *[MaxFrameK + 1]uint32, rawPayloadBytes int, dst []page.ValuePtr) ([]page.ValuePtr, FrameStats, error) {
	if w == nil {
		return nil, FrameStats{}, errors.New("valuelog: nil writer")
	}
	k := len(records)
	if k <= 0 {
		return dst[:0], FrameStats{}, nil
	}
	if k > MaxFrameK {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	if len(dst) < k {
		return nil, FrameStats{}, errors.New("valuelog: dst too small")
	}
	dst = dst[:k]

	bodyLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4) + rawPayloadBytes
	if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	if bodyLen > int(^uint32(0)) {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	if recordSizeExceedsMax(uint32(bodyLen)) {
		return nil, FrameStats{}, ErrRecordTooLarge
	}

	start := w.size
	prefixLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4)
	totalLen := HeaderSize + prefixLen + rawPayloadBytes
	max := w.appendMax
	if max <= 0 {
		max = defaultBufferSize
	}

	// Fast path: build directly into the writer append buffer (wal_off),
	// avoiding an additional copy into appendBuf.
	if w.f != nil && totalLen <= max {
		if len(w.appendBuf)+totalLen > max {
			if err := w.flushAppendBuf(); err != nil {
				return nil, FrameStats{}, err
			}
		}
		if cap(w.appendBuf) < max {
			w.appendBuf = make([]byte, len(w.appendBuf), max)
		}
		base := len(w.appendBuf)
		w.appendBuf = w.appendBuf[:base+totalLen]
		frame := w.appendBuf[base : base+totalLen]

		frame[4] = Version
		frame[5] = recordFlagGrouped
		frame[6] = 0
		frame[7] = 0
		binary.LittleEndian.PutUint64(frame[8:16], 0)
		binary.LittleEndian.PutUint32(frame[16:20], uint32(bodyLen))

		off := HeaderSize
		frame[off] = FrameVersion
		frame[off+1] = 0
		frame[off+2] = byte(k)
		frame[off+3] = 0
		binary.LittleEndian.PutUint64(frame[off+4:off+12], dictID)
		off += FrameHeaderSize

		for i := 0; i < k; i++ {
			rid := records[i].RID
			if rid == 0 {
				return nil, FrameStats{}, errors.New("valuelog: missing rid")
			}
			binary.LittleEndian.PutUint64(frame[off:off+8], rid)
			off += 8
		}
		for i := 0; i < k+1; i++ {
			binary.LittleEndian.PutUint32(frame[off:off+4], offsets[i])
			off += 4
		}
		for i := 0; i < k; i++ {
			copy(frame[off:], records[i].Value)
			off += len(records[i].Value)
		}

		sum := crc.ChecksumParts(frame[4:HeaderSize], frame[HeaderSize:])
		binary.LittleEndian.PutUint32(frame[0:4], sum)
		w.size += int64(HeaderSize + bodyLen)

		if len(w.appendBuf) >= max {
			if err := w.flushAppendBuf(); err != nil {
				return nil, FrameStats{}, err
			}
		}
	} else {
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

		const maxKeepScratch = 16 << 20 // 16 MiB
		var header [HeaderSize]byte
		header[4] = Version
		header[5] = recordFlagGrouped
		header[6] = 0
		header[7] = 0
		binary.LittleEndian.PutUint64(header[8:16], 0)
		binary.LittleEndian.PutUint32(header[16:20], uint32(bodyLen))

		var frame []byte
		if totalLen <= maxKeepScratch {
			if cap(w.rawScratch) < totalLen {
				w.rawScratch = make([]byte, totalLen)
			}
			frame = w.rawScratch[:totalLen]
		} else {
			frame = make([]byte, totalLen)
		}
		copy(frame[0:HeaderSize], header[:])
		copy(frame[HeaderSize:HeaderSize+prefixLen], prefix)
		off := HeaderSize + prefixLen
		for i := 0; i < k; i++ {
			copy(frame[off:], records[i].Value)
			off += len(records[i].Value)
		}
		sum := crc.ChecksumParts(frame[4:HeaderSize], frame[HeaderSize:])
		binary.LittleEndian.PutUint32(frame[0:4], sum)
		if err := w.writeFrameBatch(frame); err != nil {
			return nil, FrameStats{}, err
		}
		w.size += int64(HeaderSize + bodyLen)
	}

	recordLenHint := uint32(headerWithoutCRC) + uint32(bodyLen)
	if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
		recordLenHint = 0
	}
	for i := range records {
		dst[i] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
			FileID: w.fileID,
		}
	}
	return dst, FrameStats{
		Records:            k,
		RawPayloadBytes:    rawPayloadBytes,
		StoredPayloadBytes: rawPayloadBytes,
		Kept:               false,
	}, nil
}

func (w *Writer) Sync() error {
	if w == nil || w.f == nil {
		return nil
	}
	if err := w.flushNoTrim(); err != nil {
		return err
	}
	if w.syncFn != nil {
		if err := w.syncFn(w.f); err != nil {
			return err
		}
		w.trimTransientScratchBuffers()
		return nil
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.trimTransientScratchBuffers()
	return nil
}

func (w *Writer) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	if err := w.flushNoTrim(); err != nil {
		_ = w.f.Close()
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	w.releaseTransientScratchBuffers()
	return nil
}
