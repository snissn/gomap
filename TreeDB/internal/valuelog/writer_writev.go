package valuelog

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/page"
)

const writevMinAvgValueSize = 16 << 10

// AppendRawFramesWritevInto appends raw (uncompressed) grouped frames using a
// writev batching strategy.
//
// This avoids concatenating payloads into a contiguous frame buffer, reducing
// user-space copying for large value workloads. It always flushes the writev
// queue before returning, so it does not retain references to the input slices.
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

	// Only use writev when it is likely to produce large batched writes. For
	// small average values, iov limits can force tiny writev calls and regress
	// syscall counts compared to the contiguous-buffer path.
	if !writevSupported || w.f == nil || (rawPayloadBytes/len(records)) < writevMinAvgValueSize {
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

	max := w.appendMax
	if max <= 0 {
		max = defaultBufferSize
	}
	// Leave headroom: some platforms enforce lower limits than UIO_MAXIOV.
	maxIovs := writevMaxIovs
	if maxIovs <= 0 {
		maxIovs = 1024
	}
	if maxIovs > 128 {
		maxIovs -= 64
	}

	fd := int(w.f.Fd())
	iovs := make([][]byte, 0, 128)
	meta := make([]byte, 0, 4096)
	queuedBytes := 0

	flush := func() error {
		if len(iovs) == 0 {
			meta = meta[:0]
			queuedBytes = 0
			return nil
		}
		if err := writevAll(fd, iovs); err != nil {
			return err
		}
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
		recordLenNoCRC := uint32(headerWithoutCRC) + uint32(bodyLen)
		for i := 0; i < kFrame; i++ {
			dst[pos+i] = page.ValuePtr{
				Offset: uint64(start + 4),
				Length: page.ValuePtrMarkGrouped(recordLenNoCRC, uint8(i)),
				FileID: w.fileID,
			}
		}

		metaOff := len(meta)
		meta = append(meta, make([]byte, HeaderSize+prefixLen)...)
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
