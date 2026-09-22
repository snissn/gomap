package valuelog

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/limits"
)

// FramePreparer reuses dict codecs and scratch buffers to prepare grouped frame
// bodies outside the value-log write lock.
//
// It is not safe for concurrent use.
type FramePreparer struct {
	rawScratch        []byte
	encScratch        []byte
	blockScratch      []byte
	blockCodecScratch blockCodecScratch
	encLimiter        limitedSliceWriter

	skipDictID uint64
	codecs     *dictCodecEntry
	noBenefit  uint8
	skipRemain uint16

	dictEncoder       *zstd.Encoder
	dictEncoderCodecs *dictCodecEntry
	dictEncoderKey    dictCodecKey

	dictFrameEncodeLevel   zstd.EncoderLevel
	dictFrameEnableEntropy bool
	blockCodec             BlockCodec
	blockCompression       bool

	clock              Clock
	encodeCostModel    EncodeCostModel
	encodeSampleStride uint64
	encodeSampleCount  uint64

	keepIoNsPerStoredByte  float64
	keepEncodeNsPerRawByte float64
	keepSafetyMargin       float64
}

func NewFramePreparer() *FramePreparer {
	p := &FramePreparer{}
	p.ResetForReuse()
	return p
}

const framePreparerScratchKeepCap = 8 << 20

// ResetForReuse restores the default policy/hint state while retaining bounded
// scratch buffers. It is intended for callers that pool FramePreparers between
// independent append batches without changing compression semantics from a
// freshly-created preparer.
func (p *FramePreparer) ResetForReuse() {
	if p == nil {
		return
	}
	p.releaseDictEncoder()
	p.TrimScratchForReuse()
	p.skipDictID = 0
	p.codecs = nil
	p.noBenefit = 0
	p.skipRemain = 0
	p.dictFrameEncodeLevel = zstd.SpeedFastest
	p.dictFrameEnableEntropy = false
	p.blockCodec = BlockCodecSnappy
	p.blockCompression = false
	p.clock = RealClock{}
	p.encodeCostModel = nil
	p.encodeSampleStride = 0
	p.encodeSampleCount = 0
	p.keepIoNsPerStoredByte = 0
	p.keepEncodeNsPerRawByte = 0
	p.keepSafetyMargin = DefaultKeepSafetyMargin
}

// TrimScratchForReuse drops oversized temporary buffers while preserving codec,
// active dictionary encoder, and compression-hint state. Long-lived prepare
// workers call this between tasks so an occasional large frame does not pin
// unbounded scratch memory.
func (p *FramePreparer) TrimScratchForReuse() {
	if p == nil {
		return
	}
	if cap(p.rawScratch) > framePreparerScratchKeepCap {
		p.rawScratch = nil
	} else {
		p.rawScratch = p.rawScratch[:0]
	}
	if cap(p.encScratch) > framePreparerScratchKeepCap {
		p.encScratch = nil
	} else {
		p.encScratch = p.encScratch[:0]
	}
	if cap(p.blockScratch) > framePreparerScratchKeepCap {
		p.blockScratch = nil
	} else {
		p.blockScratch = p.blockScratch[:0]
	}
	p.encLimiter = limitedSliceWriter{}
}

func (p *FramePreparer) releaseDictEncoder() {
	if p == nil || p.dictEncoder == nil {
		return
	}
	if p.dictEncoderCodecs != nil && p.dictEncoderCodecs.encPool != nil {
		p.dictEncoderCodecs.encPool.Put(p.dictEncoder)
	}
	p.dictEncoder = nil
	p.dictEncoderCodecs = nil
	p.dictEncoderKey = dictCodecKey{}
}

func (p *FramePreparer) dictEncoderFor(codecs *dictCodecEntry) (*zstd.Encoder, error) {
	if p == nil || codecs == nil || codecs.encPool == nil {
		return nil, ErrMissingDict
	}
	if p.dictEncoder != nil && p.dictEncoderCodecs == codecs && p.dictEncoderKey == codecs.key {
		return p.dictEncoder, nil
	}
	p.releaseDictEncoder()
	enc, ok := codecs.encPool.Get().(*zstd.Encoder)
	if !ok || enc == nil {
		return nil, ErrMissingDict
	}
	p.dictEncoder = enc
	p.dictEncoderCodecs = codecs
	p.dictEncoderKey = codecs.key
	return enc, nil
}

func (p *FramePreparer) SetDictFrameEncoderOptions(level zstd.EncoderLevel, enableEntropy bool) {
	if p == nil {
		return
	}
	level = normalizeDictFrameEncodeLevel(level)
	if p.dictFrameEncodeLevel != level || p.dictFrameEnableEntropy != enableEntropy {
		p.releaseDictEncoder()
	}
	p.dictFrameEncodeLevel = level
	p.dictFrameEnableEntropy = enableEntropy
	p.codecs = nil
	p.skipDictID = 0
	p.noBenefit = 0
	p.skipRemain = 0
}

func (p *FramePreparer) SetBlockCompression(codec BlockCodec, enabled bool) {
	if p == nil {
		return
	}
	p.blockCodec = normalizeBlockCodec(codec)
	p.blockCompression = enabled
}

func (p *FramePreparer) SetClock(clock Clock) {
	if p == nil {
		return
	}
	if clock == nil {
		p.clock = RealClock{}
		return
	}
	p.clock = clock
}

func (p *FramePreparer) SetEncodeSampleStride(stride uint64) {
	if p == nil {
		return
	}
	p.encodeSampleStride = stride
}

func (p *FramePreparer) SetEncodeCostModel(model EncodeCostModel) {
	if p == nil {
		return
	}
	p.encodeCostModel = model
}

func (p *FramePreparer) SetKeepPolicy(ioNsPerStoredByte, encodeNsPerRawByte, safetyMargin float64) {
	if p == nil {
		return
	}
	p.keepIoNsPerStoredByte = ioNsPerStoredByte
	p.keepEncodeNsPerRawByte = encodeNsPerRawByte
	if safetyMargin < 0 {
		safetyMargin = 0
	}
	p.keepSafetyMargin = safetyMargin
}

func (p *FramePreparer) ResetCompressionHints() {
	if p == nil {
		return
	}
	p.noBenefit = 0
	p.skipRemain = 0
	p.skipDictID = 0
}

func (p *FramePreparer) sampleEncodeStart() time.Time {
	if p == nil {
		return time.Time{}
	}
	stride := p.encodeSampleStride
	if stride == 0 {
		return time.Time{}
	}
	p.encodeSampleCount++
	if stride > 1 && p.encodeSampleCount%stride != 0 {
		return time.Time{}
	}
	if p.clock == nil {
		p.clock = RealClock{}
	}
	return p.clock.Now()
}

func (p *FramePreparer) sampleEncodeEnd(start time.Time, rawPayloadBytes, records int) int64 {
	if start.IsZero() {
		return 0
	}
	if p.encodeCostModel != nil {
		if ns := p.encodeCostModel.EncodeNs(rawPayloadBytes, records); ns > 0 {
			if adv, ok := p.clock.(interface{ Advance(int64) }); ok {
				adv.Advance(ns)
			}
			return ns
		}
	}
	if p.clock == nil {
		p.clock = RealClock{}
	}
	return p.clock.Now().Sub(start).Nanoseconds()
}

func (p *FramePreparer) shouldKeepCompressed(rawPayloadBytes, encodedLen int, encodeNs int64) bool {
	encodeNsUsed := encodeNs
	if encodeNsUsed <= 0 && p.keepEncodeNsPerRawByte > 0 && rawPayloadBytes > 0 {
		encodeNsUsed = int64(p.keepEncodeNsPerRawByte * float64(rawPayloadBytes))
	}
	return ShouldKeepCompressed(rawPayloadBytes, encodedLen, encodeNsUsed, p.keepIoNsPerStoredByte, p.keepSafetyMargin)
}

func (p *FramePreparer) shouldSkipCompression(rawPayloadBytes int) bool {
	if p == nil || rawPayloadBytes <= 0 {
		return false
	}
	if p.keepIoNsPerStoredByte <= 0 || p.keepEncodeNsPerRawByte <= 0 {
		return false
	}
	costPerRaw := p.keepEncodeNsPerRawByte
	if p.keepSafetyMargin > 0 {
		costPerRaw *= 1 + p.keepSafetyMargin
	}
	return p.keepIoNsPerStoredByte <= costPerRaw
}

func ensureFrameBody(dst []byte, bodyLen int) []byte {
	if cap(dst) >= bodyLen {
		return dst[:bodyLen]
	}
	return make([]byte, bodyLen)
}

func (p *FramePreparer) PrepareFrame(dictID uint64, dict []byte, records []Record) ([]byte, FrameStats, error) {
	return p.PrepareFrameInto(nil, dictID, dict, records)
}

// PrepareFrameInto behaves like PrepareFrame, but writes the frame body into dst
// when capacity allows. Callers can reuse large frame buffers across requests.
func (p *FramePreparer) PrepareFrameInto(dst []byte, dictID uint64, dict []byte, records []Record) ([]byte, FrameStats, error) {
	if p == nil {
		return nil, FrameStats{}, errors.New("valuelog: nil frame preparer")
	}
	if len(records) == 0 {
		return nil, FrameStats{}, ErrCorrupt
	}
	if len(records) > MaxFrameK {
		return nil, FrameStats{}, ErrRecordTooLarge
	}
	if dictID != 0 && len(dict) == 0 {
		return nil, FrameStats{}, ErrMissingDict
	}
	if dictID == 0 {
		dict = nil
	}

	k := len(records)
	rawPayloadBytes := 0
	var offsets [MaxFrameK + 1]uint32
	for i := 0; i < k; i++ {
		rid := records[i].RID
		if rid == 0 {
			return nil, FrameStats{}, errors.New("valuelog: missing rid")
		}
		valueLen := len(records[i].Value)
		if valueLen > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		rawPayloadBytes += valueLen
		if rawPayloadBytes < 0 || rawPayloadBytes > int(^uint32(0)) {
			return nil, FrameStats{}, ErrRecordTooLarge
		}
		offsets[i+1] = uint32(rawPayloadBytes)
	}
	if limits.MaxRecordSize > 0 && int64(rawPayloadBytes) > limits.MaxRecordSize {
		return nil, FrameStats{}, ErrRecordTooLarge
	}

	if dictID == 0 && p.blockCompression {
		return p.prepareBlockFrameBody(dst, records, &offsets, rawPayloadBytes)
	}

	if dictID != 0 {
		if p.skipDictID != dictID {
			p.skipDictID = dictID
			p.noBenefit = 0
			p.skipRemain = 0
		}
		if rawPayloadBytes == 0 {
			return p.buildRawFrameBody(dst, dictID, records, &offsets, rawPayloadBytes, false, 0)
		}
		forceDictProbe := shouldForceDictProbe(rawPayloadBytes)
		if p.skipRemain > 0 {
			if !shouldProbeLargeDictDuringBackoff(p.skipRemain, rawPayloadBytes) {
				p.skipRemain--
				return p.buildRawFrameBody(dst, dictID, records, &offsets, rawPayloadBytes, false, 0)
			}
			p.skipRemain--
		}
		if p.shouldSkipCompression(rawPayloadBytes) && !forceDictProbe {
			return p.buildRawFrameBody(dst, dictID, records, &offsets, rawPayloadBytes, false, 0)
		}

		level := normalizeDictFrameEncodeLevel(p.dictFrameEncodeLevel)
		noEntropy := !p.dictFrameEnableEntropy
		key := dictCodecKey{dictID: dictID, level: level, noEntropy: noEntropy}

		codecs := p.codecs
		if codecs == nil || codecs.key != key {
			codecs = getDictCodecsWithOpts(dictID, dict, level, noEntropy)
			if codecs != nil {
				p.codecs = codecs
			}
		}
		if codecs == nil || codecs.encPool == nil {
			return nil, FrameStats{}, ErrMissingDict
		}

		enc, err := p.dictEncoderFor(codecs)
		if err != nil {
			return nil, FrameStats{}, err
		}
		encodeStart := p.sampleEncodeStart()
		encoded, encodeErr := p.encodePayload(enc, records, rawPayloadBytes)
		encodeNs := p.sampleEncodeEnd(encodeStart, rawPayloadBytes, k)
		p.encScratch = p.encScratch[:0]

		keepCompressed := false
		if encodeErr == nil {
			keepCompressed = p.shouldKeepCompressed(rawPayloadBytes, len(encoded), encodeNs)
			if !keepCompressed && shouldForceKeepLargeDictCompressed(rawPayloadBytes, len(encoded)) {
				keepCompressed = true
			}
		}
		if encodeErr != nil && !errors.Is(encodeErr, errEncodedTooLarge) {
			return nil, FrameStats{}, encodeErr
		}
		if !keepCompressed {
			if p.noBenefit < 0xff {
				p.noBenefit++
			}
			p.skipRemain = dictSkipFramesAggressive(p.noBenefit, rawPayloadBytes, len(encoded), encodeNs, p.keepIoNsPerStoredByte, p.keepSafetyMargin)
			return p.buildRawFrameBody(dst, dictID, records, &offsets, rawPayloadBytes, true, encodeNs)
		}

		body, err := p.buildCompressedFrameBody(dst, dictID, records, &offsets, encoded)
		if err != nil {
			return nil, FrameStats{}, err
		}
		p.noBenefit = 0
		p.skipRemain = 0
		return body, FrameStats{
			Records:            k,
			RawPayloadBytes:    rawPayloadBytes,
			StoredPayloadBytes: len(encoded),
			Attempted:          true,
			Kept:               true,
			EncodeNs:           encodeNs,
		}, nil
	}

	return p.buildRawFrameBody(dst, 0, records, &offsets, rawPayloadBytes, false, 0)
}

func (p *FramePreparer) prepareBlockFrameBody(dst []byte, records []Record, offsets *[MaxFrameK + 1]uint32, rawPayloadBytes int) ([]byte, FrameStats, error) {
	if p == nil {
		return nil, FrameStats{}, errors.New("valuelog: nil frame preparer")
	}
	// Skip/backoff hints are shared with dict mode; reset when crossing into
	// block mode so stale dict hints do not suppress block probes.
	if p.skipDictID != 0 {
		p.skipDictID = 0
		p.noBenefit = 0
		p.skipRemain = 0
	}
	if rawPayloadBytes == 0 || p.shouldSkipCompression(rawPayloadBytes) {
		return p.buildRawFrameBody(dst, 0, records, offsets, rawPayloadBytes, false, 0)
	}
	if p.skipRemain > 0 {
		p.skipRemain--
		return p.buildRawFrameBody(dst, 0, records, offsets, rawPayloadBytes, false, 0)
	}
	payload := records[0].Value
	if len(records) > 1 {
		if cap(p.rawScratch) < rawPayloadBytes {
			p.rawScratch = make([]byte, rawPayloadBytes)
		}
		payload = p.rawScratch[:rawPayloadBytes]
		off := 0
		for i := range records {
			off += copy(payload[off:], records[i].Value)
		}
	}
	encodeStart := p.sampleEncodeStart()
	encoded, encodeErr := encodeBlockPayloadWithScratch(p.blockCodec, payload, p.blockScratch[:0], &p.blockCodecScratch)
	encodeNs := p.sampleEncodeEnd(encodeStart, rawPayloadBytes, len(records))
	if encoded != nil {
		p.blockScratch = encoded[:0]
	}
	keepCompressed := false
	if encodeErr == nil {
		keepCompressed = p.shouldKeepCompressed(rawPayloadBytes, len(encoded), encodeNs)
	}
	if encodeErr != nil && !errors.Is(encodeErr, errEncodedTooLarge) {
		if p.noBenefit < 0xff {
			p.noBenefit++
		}
		p.skipRemain = dictSkipFramesAggressive(p.noBenefit, rawPayloadBytes, len(encoded), encodeNs, p.keepIoNsPerStoredByte, p.keepSafetyMargin)
		return p.buildRawFrameBody(dst, 0, records, offsets, rawPayloadBytes, true, encodeNs)
	}
	if !keepCompressed {
		if p.noBenefit < 0xff {
			p.noBenefit++
		}
		p.skipRemain = dictSkipFramesAggressive(p.noBenefit, rawPayloadBytes, len(encoded), encodeNs, p.keepIoNsPerStoredByte, p.keepSafetyMargin)
		return p.buildRawFrameBody(dst, 0, records, offsets, rawPayloadBytes, true, encodeNs)
	}
	p.noBenefit = 0
	p.skipRemain = 0
	body, err := p.buildBlockCompressedFrameBody(dst, records, offsets, encoded)
	if err != nil {
		return nil, FrameStats{}, err
	}
	return body, FrameStats{
		Records:            len(records),
		RawPayloadBytes:    rawPayloadBytes,
		StoredPayloadBytes: len(encoded),
		Attempted:          true,
		Kept:               true,
		EncodeNs:           encodeNs,
	}, nil
}

func (p *FramePreparer) encodePayload(enc *zstd.Encoder, records []Record, rawPayloadBytes int) ([]byte, error) {
	if enc == nil {
		return nil, errors.New("valuelog: nil encoder")
	}
	k := len(records)
	if cap(p.encScratch) < rawPayloadBytes {
		p.encScratch = make([]byte, 0, rawPayloadBytes)
	}
	encDst := p.encScratch[:0]

	if k == 1 {
		return enc.EncodeAll(records[0].Value, encDst), nil
	}
	if rawPayloadBytes <= (1 << 20) {
		useNoCopyParts := shouldUseEncodeAllParts(records[:k], rawPayloadBytes)
		if !useNoCopyParts && k >= 8 {
			avg := rawPayloadBytes / k
			// For tiny high-repeat grouped frames (e.g. 128B values at k=8+),
			// forcing no-copy parts encoding can outperform the streaming path
			// while avoiding an extra concat allocation/copy.
			if rawPayloadBytes >= (1<<10) && avg >= 96 && isUltraLowEntropySample(records[0].Value) {
				useNoCopyParts = true
			}
		}
		if useNoCopyParts {
			var parts [MaxFrameK][]byte
			for i := 0; i < k; i++ {
				parts[i] = records[i].Value
			}
			return enc.EncodeAllParts(parts[:k], encDst), nil
		}
		return p.encodePayloadStreaming(enc, records[:k], rawPayloadBytes, encDst)
	}

	return p.encodePayloadStreaming(enc, records[:k], rawPayloadBytes, encDst)
}

func (p *FramePreparer) encodePayloadStreaming(enc *zstd.Encoder, records []Record, rawPayloadBytes int, encDst []byte) ([]byte, error) {
	if enc == nil {
		return nil, errors.New("valuelog: nil encoder")
	}
	p.encLimiter.buf = encDst
	p.encLimiter.limit = rawPayloadBytes - 1
	enc.Reset(&p.encLimiter)
	var encodeErr error
	for i := 0; i < len(records); i++ {
		if _, encodeErr = enc.Write(records[i].Value); encodeErr != nil {
			break
		}
	}
	if encodeErr == nil {
		encodeErr = enc.Close()
	}
	enc.Reset(nil)
	return p.encLimiter.buf, encodeErr
}

func (p *FramePreparer) buildRawFrameBody(dst []byte, dictID uint64, records []Record, offsets *[MaxFrameK + 1]uint32, rawPayloadBytes int, attempted bool, encodeNs int64) ([]byte, FrameStats, error) {
	k := len(records)
	if k <= 0 || k > MaxFrameK {
		return nil, FrameStats{}, ErrRecordTooLarge
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

	body := ensureFrameBody(dst, bodyLen)
	body[0] = FrameVersion
	body[1] = 0
	body[2] = byte(k)
	body[3] = 0
	binary.LittleEndian.PutUint64(body[4:12], dictID)
	off := FrameHeaderSize
	for i := 0; i < k; i++ {
		binary.LittleEndian.PutUint64(body[off:off+8], records[i].RID)
		off += 8
	}
	for i := 0; i < k+1; i++ {
		binary.LittleEndian.PutUint32(body[off:off+4], offsets[i])
		off += 4
	}
	for i := 0; i < k; i++ {
		copy(body[off:], records[i].Value)
		off += len(records[i].Value)
	}
	return body, FrameStats{
		Records:            k,
		RawPayloadBytes:    rawPayloadBytes,
		StoredPayloadBytes: rawPayloadBytes,
		Attempted:          attempted,
		Kept:               false,
		EncodeNs:           encodeNs,
	}, nil
}

func (p *FramePreparer) buildCompressedFrameBody(dst []byte, dictID uint64, records []Record, offsets *[MaxFrameK + 1]uint32, encoded []byte) ([]byte, error) {
	k := len(records)
	if k <= 0 || k > MaxFrameK {
		return nil, ErrRecordTooLarge
	}
	bodyLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4) + len(encoded)
	if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
		return nil, ErrRecordTooLarge
	}
	if bodyLen > int(^uint32(0)) {
		return nil, ErrRecordTooLarge
	}
	if recordSizeExceedsMax(uint32(bodyLen)) {
		return nil, ErrRecordTooLarge
	}

	body := ensureFrameBody(dst, bodyLen)
	body[0] = FrameVersion
	body[1] = FrameFlagCompressed
	body[2] = byte(k)
	body[3] = 0
	binary.LittleEndian.PutUint64(body[4:12], dictID)
	off := FrameHeaderSize
	for i := 0; i < k; i++ {
		binary.LittleEndian.PutUint64(body[off:off+8], records[i].RID)
		off += 8
	}
	for i := 0; i < k+1; i++ {
		binary.LittleEndian.PutUint32(body[off:off+4], offsets[i])
		off += 4
	}
	copy(body[off:], encoded)
	return body, nil
}

func (p *FramePreparer) buildBlockCompressedFrameBody(dst []byte, records []Record, offsets *[MaxFrameK + 1]uint32, encoded []byte) ([]byte, error) {
	k := len(records)
	if k <= 0 || k > MaxFrameK {
		return nil, ErrRecordTooLarge
	}
	bodyLen := FrameHeaderSize + (k * 8) + ((k + 1) * 4) + len(encoded)
	if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
		return nil, ErrRecordTooLarge
	}
	if bodyLen > int(^uint32(0)) {
		return nil, ErrRecordTooLarge
	}
	if recordSizeExceedsMax(uint32(bodyLen)) {
		return nil, ErrRecordTooLarge
	}

	body := ensureFrameBody(dst, bodyLen)
	body[0] = FrameVersion
	body[1] = FrameFlagCompressed
	body[2] = byte(k)
	body[3] = byte(p.blockCodec)
	binary.LittleEndian.PutUint64(body[4:12], 0)
	off := FrameHeaderSize
	for i := 0; i < k; i++ {
		binary.LittleEndian.PutUint64(body[off:off+8], records[i].RID)
		off += 8
	}
	for i := 0; i < k+1; i++ {
		binary.LittleEndian.PutUint32(body[off:off+4], offsets[i])
		off += 4
	}
	copy(body[off:], encoded)
	return body, nil
}
