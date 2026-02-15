package valuelog

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	defaultRawWritevMinAvgBytes     = 0
	defaultRawWritevMinBatchRecords = 8
)

type writevCallStats struct {
	syscalls uint64
	bytes    uint64
	iovecs   uint64
}

func rawWritevLimits(w *Writer) (maxBytes, maxIovs int) {
	maxBytes = defaultBufferSize
	if w != nil && w.appendMax > 0 {
		maxBytes = w.appendMax
	}
	maxIovs = writevMaxIovs
	if maxIovs <= 0 {
		maxIovs = 1024
	}
	// Leave headroom: some platforms enforce lower limits than UIO_MAXIOV.
	if maxIovs > 128 {
		maxIovs -= 64
	}
	return maxBytes, maxIovs
}

func rawFrameLen(records []Record, start, end int) (int, int, bool) {
	kFrame := end - start
	if kFrame <= 0 || kFrame > MaxFrameK {
		return 0, 0, false
	}
	framePayloadBytes := 0
	for i := start; i < end; i++ {
		framePayloadBytes += len(records[i].Value)
		if framePayloadBytes < 0 || framePayloadBytes > int(^uint32(0)) {
			return 0, 0, false
		}
	}
	prefixLen := FrameHeaderSize + (kFrame * 8) + ((kFrame + 1) * 4)
	bodyLen := prefixLen + framePayloadBytes
	if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
		return 0, 0, false
	}
	if bodyLen > int(^uint32(0)) || recordSizeExceedsMax(uint32(bodyLen)) {
		return 0, 0, false
	}
	return HeaderSize + bodyLen, kFrame, true
}

func predictRawWritevFlushes(records []Record, k, maxBytes, maxIovs int) int {
	if len(records) == 0 || k <= 0 {
		return 0
	}
	flushes := 0
	queuedBytes := 0
	queuedIovs := 0
	for pos := 0; pos < len(records); pos += k {
		end := pos + k
		if end > len(records) {
			end = len(records)
		}
		totalLen, kFrame, ok := rawFrameLen(records, pos, end)
		if !ok {
			return 0
		}
		neededIovs := 2 + kFrame
		if queuedIovs > 0 && (queuedIovs+neededIovs > maxIovs || queuedBytes+totalLen > maxBytes) {
			flushes++
			queuedBytes = 0
			queuedIovs = 0
		}
		queuedBytes += totalLen
		queuedIovs += neededIovs
	}
	if queuedIovs > 0 {
		flushes++
	}
	return flushes
}

func predictRawFallbackFlushes(records []Record, k, maxBytes, existingBuffered int) int {
	if len(records) == 0 || k <= 0 {
		return 0
	}
	if existingBuffered < 0 {
		existingBuffered = 0
	}
	flushes := 0
	queued := existingBuffered
	for pos := 0; pos < len(records); pos += k {
		end := pos + k
		if end > len(records) {
			end = len(records)
		}
		totalLen, _, ok := rawFrameLen(records, pos, end)
		if !ok {
			return 0
		}
		if totalLen >= maxBytes {
			if queued > 0 {
				flushes++
				queued = 0
			}
			// Large frames bypass appendBuf and write directly once in the
			// fallback path; do not model these as maxBytes-sized splits.
			flushes++
			continue
		}
		if queued+totalLen > maxBytes {
			flushes++
			queued = 0
		}
		queued += totalLen
		if queued >= maxBytes {
			flushes++
			queued = 0
		}
	}
	if queued > 0 {
		flushes++
	}
	return flushes
}

func (w *Writer) shouldUseRawWritev(records []Record, k int, rawPayloadBytes int) bool {
	if w == nil || !writevSupported || w.f == nil {
		return false
	}
	minAvgBytes, minBatchRecs := w.rawWritevStrategy()
	if len(records) < minBatchRecs {
		return false
	}
	if minAvgBytes > 0 && (rawPayloadBytes/len(records)) < minAvgBytes {
		return false
	}
	existingBuffered := len(w.appendBuf)
	maxBytes, maxIovs := rawWritevLimits(w)
	writevFlushes := predictRawWritevFlushes(records, k, maxBytes, maxIovs)
	if writevFlushes == 0 {
		return false
	}
	// The writev path must flush any existing append buffer before queuing iovs.
	if existingBuffered > 0 {
		writevFlushes++
	}
	fallbackFlushes := predictRawFallbackFlushes(records, k, maxBytes, existingBuffered)
	if fallbackFlushes == 0 {
		return false
	}
	// Adaptive mode: choose writev when it is no worse in estimated flush count.
	return writevFlushes <= fallbackFlushes
}

// AppendRawFramesWritevInto appends raw (uncompressed) grouped frames using a
// writev batching strategy.
//
// This avoids concatenating payloads into a contiguous frame buffer, reducing
// user-space copying for large value workloads. It always flushes the writev
// queue before returning, so it does not retain references to the input slices.
//
// Prefer this when the caller needs pointers durably visible at function
// return and cannot rely on future appends to coalesce writes.
//
// dst must be at least len(records) long.
func (w *Writer) AppendRawFramesWritevInto(records []Record, k int, dst []page.ValuePtr) ([]page.ValuePtr, FrameStats, error) {
	if w == nil {
		return nil, FrameStats{}, errors.New("valuelog: nil writer")
	}
	if len(records) == 0 {
		return dst[:0], FrameStats{}, nil
	}
	if len(dst) < len(records) {
		return nil, FrameStats{}, errors.New("valuelog: dst too small")
	}
	if k <= 0 || k > MaxFrameK {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	dst = dst[:len(records)]

	rawPayloadBytes := 0
	for i := range records {
		if records[i].RID == 0 {
			return nil, FrameStats{}, errors.New("valuelog: missing rid")
		}
		if len(records[i].Value) > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		rawPayloadBytes += len(records[i].Value)
		if rawPayloadBytes < 0 || rawPayloadBytes > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
	}

	// Adaptive strategy: only use writev when the estimated iov/flush behavior
	// is no worse than the contiguous-buffer fallback.
	if !w.shouldUseRawWritev(records, k, rawPayloadBytes) {
		stats := FrameStats{Kept: false}
		for i := 0; i < len(records); i += k {
			end := i + k
			if end > len(records) {
				end = len(records)
			}
			_, frameStats, err := w.AppendFrameWithStatsInto(0, nil, records[i:end], dst[i:end])
			if err != nil {
				return nil, FrameStats{}, err
			}
			stats.Records += frameStats.Records
			stats.RawPayloadBytes += frameStats.RawPayloadBytes
			stats.StoredPayloadBytes += frameStats.StoredPayloadBytes
		}
		return dst, stats, nil
	}

	if err := w.flushAppendBuf(); err != nil {
		return nil, FrameStats{}, err
	}

	max, maxIovs := rawWritevLimits(w)

	fd := int(w.f.Fd())
	iovs := w.rawWritevIovs[:0]
	if cap(iovs) < maxIovs {
		iovs = make([][]byte, 0, maxIovs)
	}
	vecs := w.rawWritevVecs[:0]
	meta := w.rawWritevMeta[:0]
	if cap(meta) < 4096 {
		meta = make([]byte, 0, 4096)
	}
	queuedBytes := 0
	maxUsedIovs := 0
	defer func() {
		// Keep reusable writev scratch without retaining references to caller-owned values.
		if maxUsedIovs > 0 {
			full := iovs[:maxUsedIovs:maxUsedIovs]
			clear(full)
			if cap(vecs) >= maxUsedIovs {
				fullVecs := vecs[:maxUsedIovs:maxUsedIovs]
				clear(fullVecs)
			}
		}
		w.rawWritevIovs = iovs[:0]
		w.rawWritevVecs = vecs[:0]
		w.rawWritevMeta = meta[:0]
	}()

	flush := func() error {
		if len(iovs) == 0 {
			meta = meta[:0]
			queuedBytes = 0
			return nil
		}
		var (
			err      error
			callStat writevCallStats
		)
		vecs, callStat, err = writevAll(fd, iovs, vecs)
		if err != nil {
			return err
		}
		w.rawWritevSyscalls.Add(callStat.syscalls)
		w.rawWritevBytes.Add(callStat.bytes)
		w.rawWritevIovecs.Add(callStat.iovecs)
		w.rawWritevFlushes.Add(1)
		w.size += int64(queuedBytes)
		iovs = iovs[:0]
		meta = meta[:0]
		queuedBytes = 0
		return nil
	}

	pos := 0
	for pos < len(records) {
		end := pos + k
		if end > len(records) {
			end = len(records)
		}
		frameRecords := records[pos:end]
		kFrame := len(frameRecords)
		if kFrame <= 0 || kFrame > MaxFrameK {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		var offsets [MaxFrameK + 1]uint32
		offsets[0] = 0
		framePayloadBytes := 0
		for i := 0; i < kFrame; i++ {
			framePayloadBytes += len(frameRecords[i].Value)
			if framePayloadBytes < 0 || framePayloadBytes > int(^uint32(0)) {
				return nil, FrameStats{}, ErrRecordTooLarge
			}
			offsets[i+1] = uint32(framePayloadBytes)
		}
		if limits.MaxRecordSize > 0 && int64(framePayloadBytes) > limits.MaxRecordSize {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		prefixLen := FrameHeaderSize + (kFrame * 8) + ((kFrame + 1) * 4)
		bodyLen := prefixLen + framePayloadBytes
		if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if bodyLen > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		if recordSizeExceedsMax(uint32(bodyLen)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		totalLen := HeaderSize + bodyLen
		neededIovs := 2 + kFrame
		if len(iovs)+neededIovs > maxIovs || queuedBytes+totalLen > max {
			if err := flush(); err != nil {
				return nil, FrameStats{}, err
			}
		}

		start := w.size + int64(queuedBytes)
		recordLenHint := uint32(headerWithoutCRC) + uint32(bodyLen)
		if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
			recordLenHint = 0
		}
		for i := 0; i < kFrame; i++ {
			dst[pos+i] = page.ValuePtr{
				Offset: uint64(start + 4),
				Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
				FileID: w.fileID,
			}
		}

		metaOff := len(meta)
		metaNeed := HeaderSize + prefixLen
		if cap(meta)-metaOff < metaNeed {
			newCap := cap(meta) * 2
			if newCap < metaOff+metaNeed {
				newCap = metaOff + metaNeed
			}
			grown := make([]byte, metaOff+metaNeed, newCap)
			copy(grown, meta[:metaOff])
			meta = grown
		} else {
			meta = meta[:metaOff+metaNeed]
		}
		header := meta[metaOff : metaOff+HeaderSize]
		prefix := meta[metaOff+HeaderSize : metaOff+HeaderSize+prefixLen]

		header[4] = Version
		header[5] = recordFlagGrouped
		header[6] = 0
		header[7] = 0
		binary.LittleEndian.PutUint64(header[8:16], 0)
		binary.LittleEndian.PutUint32(header[16:20], uint32(bodyLen))

		prefixOff := 0
		prefix[prefixOff] = FrameVersion
		prefix[prefixOff+1] = 0
		prefix[prefixOff+2] = byte(kFrame)
		prefix[prefixOff+3] = 0
		binary.LittleEndian.PutUint64(prefix[prefixOff+4:prefixOff+12], 0)
		prefixOff += FrameHeaderSize
		for i := 0; i < kFrame; i++ {
			binary.LittleEndian.PutUint64(prefix[prefixOff:prefixOff+8], frameRecords[i].RID)
			prefixOff += 8
		}
		for i := 0; i < kFrame+1; i++ {
			binary.LittleEndian.PutUint32(prefix[prefixOff:prefixOff+4], offsets[i])
			prefixOff += 4
		}

		sum := uint32(0)
		sum = crc.Update(sum, header[4:HeaderSize])
		sum = crc.Update(sum, prefix)
		for i := 0; i < kFrame; i++ {
			sum = crc.Update(sum, frameRecords[i].Value)
		}
		binary.LittleEndian.PutUint32(header[0:4], sum)

		iovs = append(iovs, header, prefix)
		for i := 0; i < kFrame; i++ {
			iovs = append(iovs, frameRecords[i].Value)
		}
		if len(iovs) > maxUsedIovs {
			maxUsedIovs = len(iovs)
		}
		queuedBytes += totalLen

		pos = end
	}

	if err := flush(); err != nil {
		return nil, FrameStats{}, err
	}

	return dst, FrameStats{
		Records:            len(records),
		RawPayloadBytes:    rawPayloadBytes,
		StoredPayloadBytes: rawPayloadBytes,
		Kept:               false,
	}, nil
}

// AppendRawFramesBufferedInto appends raw grouped frames through the writer's
// append buffer, allowing multiple calls to coalesce into larger write syscalls.
//
// Compared to AppendRawFramesWritevInto, this path stages frame bytes in the
// writer append buffer and flushes later, so independent append calls can merge
// into fewer, larger writes.
//
// Prefer this for high-throughput append streams where delayed flush is
// acceptable and syscall coalescing across calls is desirable.
//
// dst must be at least len(records) long.
func (w *Writer) AppendRawFramesBufferedInto(records []Record, k int, dst []page.ValuePtr) ([]page.ValuePtr, FrameStats, error) {
	if w == nil {
		return nil, FrameStats{}, errors.New("valuelog: nil writer")
	}
	if len(records) == 0 {
		return dst[:0], FrameStats{}, nil
	}
	if len(dst) < len(records) {
		return nil, FrameStats{}, errors.New("valuelog: dst too small")
	}
	if k <= 0 || k > MaxFrameK {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	dst = dst[:len(records)]

	rawPayloadBytes := 0
	for i := range records {
		if records[i].RID == 0 {
			return nil, FrameStats{}, errors.New("valuelog: missing rid")
		}
		if len(records[i].Value) > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		rawPayloadBytes += len(records[i].Value)
		if rawPayloadBytes < 0 || rawPayloadBytes > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
	}

	meta := w.rawWritevMeta[:0]
	if cap(meta) < 4096 {
		meta = make([]byte, 0, 4096)
	}
	defer func() {
		w.rawWritevMeta = meta[:0]
	}()

	pos := 0
	for pos < len(records) {
		end := pos + k
		if end > len(records) {
			end = len(records)
		}
		frameRecords := records[pos:end]
		kFrame := len(frameRecords)
		if kFrame <= 0 || kFrame > MaxFrameK {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		var offsets [MaxFrameK + 1]uint32
		offsets[0] = 0
		framePayloadBytes := 0
		for i := 0; i < kFrame; i++ {
			framePayloadBytes += len(frameRecords[i].Value)
			if framePayloadBytes < 0 || framePayloadBytes > int(^uint32(0)) {
				return nil, FrameStats{}, ErrRecordTooLarge
			}
			offsets[i+1] = uint32(framePayloadBytes)
		}
		if limits.MaxRecordSize > 0 && int64(framePayloadBytes) > limits.MaxRecordSize {
			return nil, FrameStats{}, ErrRecordTooLarge
		}

		prefixLen := FrameHeaderSize + (kFrame * 8) + ((kFrame + 1) * 4)
		bodyLen := prefixLen + framePayloadBytes
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
		recordLenHint := uint32(headerWithoutCRC) + uint32(bodyLen)
		if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
			recordLenHint = 0
		}
		for i := 0; i < kFrame; i++ {
			dst[pos+i] = page.ValuePtr{
				Offset: uint64(start + 4),
				Length: page.ValuePtrMarkGrouped(recordLenHint, uint8(i)),
				FileID: w.fileID,
			}
		}

		metaNeed := HeaderSize + prefixLen
		if cap(meta) < metaNeed {
			meta = make([]byte, metaNeed)
		} else {
			meta = meta[:metaNeed]
		}
		header := meta[:HeaderSize]
		prefix := meta[HeaderSize : HeaderSize+prefixLen]

		header[4] = Version
		header[5] = recordFlagGrouped
		header[6] = 0
		header[7] = 0
		binary.LittleEndian.PutUint64(header[8:16], 0)
		binary.LittleEndian.PutUint32(header[16:20], uint32(bodyLen))

		prefixOff := 0
		prefix[prefixOff] = FrameVersion
		prefix[prefixOff+1] = 0
		prefix[prefixOff+2] = byte(kFrame)
		prefix[prefixOff+3] = 0
		binary.LittleEndian.PutUint64(prefix[prefixOff+4:prefixOff+12], 0)
		prefixOff += FrameHeaderSize
		for i := 0; i < kFrame; i++ {
			binary.LittleEndian.PutUint64(prefix[prefixOff:prefixOff+8], frameRecords[i].RID)
			prefixOff += 8
		}
		for i := 0; i < kFrame+1; i++ {
			binary.LittleEndian.PutUint32(prefix[prefixOff:prefixOff+4], offsets[i])
			prefixOff += 4
		}

		sum := uint32(0)
		sum = crc.Update(sum, header[4:HeaderSize])
		sum = crc.Update(sum, prefix)
		for i := 0; i < kFrame; i++ {
			sum = crc.Update(sum, frameRecords[i].Value)
		}
		binary.LittleEndian.PutUint32(header[0:4], sum)

		if err := w.writeBytesBuffered(header); err != nil {
			return nil, FrameStats{}, err
		}
		if err := w.writeBytesBuffered(prefix); err != nil {
			return nil, FrameStats{}, err
		}
		for i := 0; i < kFrame; i++ {
			if err := w.writeBytesBuffered(frameRecords[i].Value); err != nil {
				return nil, FrameStats{}, err
			}
		}
		w.size += int64(HeaderSize + bodyLen)

		pos = end
	}

	return dst, FrameStats{
		Records:            len(records),
		RawPayloadBytes:    rawPayloadBytes,
		StoredPayloadBytes: rawPayloadBytes,
		Kept:               false,
	}, nil
}
