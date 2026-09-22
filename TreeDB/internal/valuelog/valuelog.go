package valuelog

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/limits"
)

const (
	Version = 1

	HeaderSize = 4 + 1 + 1 + 2 + 8 + 4
)

const (
	recordFlagGrouped byte = 1 << 0
)

const (
	FrameVersion    = 1
	FrameHeaderSize = 12
	MaxFrameK       = 255
)

const (
	FrameFlagCompressed byte = 1 << 0
)

var (
	ErrCorrupt         = errors.New("valuelog: corrupt record")
	ErrRecordTooLarge  = errors.New("valuelog: record too large")
	ErrMissingDict     = errors.New("valuelog: missing dict bytes")
	ErrMissingTemplate = errors.New("valuelog: missing template bytes")
)

type DictLookup func(dictID uint64) ([]byte, error)
type TemplateLookup func(templateID uint64) ([]byte, error)

type Record struct {
	RID   uint64
	Value []byte
}

type FrameStats struct {
	Records            int
	RawPayloadBytes    int
	StoredPayloadBytes int
	// Attempted is true when zstd encoding was executed (even if we fell back to raw).
	Attempted bool
	// Kept is true when compressed bytes were stored.
	Kept bool
	// EncodeNs is the sampled encode time (monotonic ns); 0 when not measured.
	EncodeNs int64
}

type FrameHeader struct {
	Version byte
	Flags   byte
	K       uint8
	// Reserved stores per-frame metadata. For compressed block frames
	// (Flags&FrameFlagCompressed, DictID==0), this holds the BlockCodec ID.
	Reserved uint8
	DictID   uint64
}

func EncodeFrame(dictID uint64, dict []byte, records []Record) ([]byte, FrameHeader, error) {
	return EncodeFrameWithOptions(dictID, dict, records, zstd.SpeedFastest, false)
}

func EncodeFrameWithOptions(dictID uint64, dict []byte, records []Record, level zstd.EncoderLevel, enableEntropy bool) ([]byte, FrameHeader, error) {
	if len(records) == 0 {
		return nil, FrameHeader{}, ErrCorrupt
	}
	if len(records) > MaxFrameK {
		return nil, FrameHeader{}, ErrRecordTooLarge
	}
	if dictID != 0 && len(dict) == 0 {
		return nil, FrameHeader{}, ErrMissingDict
	}
	if dictID == 0 {
		dict = nil
	}
	if level <= 0 {
		level = zstd.SpeedFastest
	}
	noEntropy := !enableEntropy

	rawTotal := 0
	maxUint32 := int(^uint32(0))
	offsets := make([]uint32, len(records)+1)
	for i, rec := range records {
		if rec.RID == 0 {
			return nil, FrameHeader{}, ErrCorrupt
		}
		if len(rec.Value) > int(^uint32(0)) {
			return nil, FrameHeader{}, ErrRecordTooLarge
		}
		rawTotal += len(rec.Value)
		if rawTotal < 0 || rawTotal > maxUint32 {
			return nil, FrameHeader{}, ErrRecordTooLarge
		}
		offsets[i+1] = uint32(rawTotal)
	}
	if limits.MaxRecordSize > 0 && int64(rawTotal) > limits.MaxRecordSize {
		return nil, FrameHeader{}, ErrRecordTooLarge
	}

	payload := make([]byte, rawTotal)
	pos := 0
	for _, rec := range records {
		copy(payload[pos:], rec.Value)
		pos += len(rec.Value)
	}

	flags := byte(0)
	encoded := payload
	if rawTotal > 0 && len(dict) > 0 {
		codecs := getDictCodecsWithOpts(dictID, dict, level, noEntropy)
		if codecs == nil || codecs.encPool == nil {
			return nil, FrameHeader{}, ErrMissingDict
		}
		enc := codecs.encPool.Get().(*zstd.Encoder)
		encoded = enc.EncodeAll(payload, nil)
		codecs.encPool.Put(enc)
		if len(encoded) < len(payload) {
			flags |= FrameFlagCompressed
		} else {
			encoded = payload
		}
	}

	k := len(records)
	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	bodyLen := FrameHeaderSize + ridBytes + offsetBytes + len(encoded)
	if limits.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > limits.MaxRecordSize {
		return nil, FrameHeader{}, ErrRecordTooLarge
	}

	body := make([]byte, bodyLen)
	body[0] = FrameVersion
	body[1] = flags
	body[2] = byte(k)
	body[3] = 0
	binary.LittleEndian.PutUint64(body[4:12], dictID)
	off := FrameHeaderSize
	for _, rec := range records {
		binary.LittleEndian.PutUint64(body[off:off+8], rec.RID)
		off += 8
	}
	for _, o := range offsets {
		binary.LittleEndian.PutUint32(body[off:off+4], o)
		off += 4
	}
	copy(body[off:], encoded)

	return body, FrameHeader{
		Version: FrameVersion,
		Flags:   flags,
		K:       uint8(k),
		DictID:  dictID,
	}, nil
}

func DecodeFrame(body []byte) (FrameHeader, []uint64, []uint32, []byte, error) {
	if len(body) < FrameHeaderSize {
		return FrameHeader{}, nil, nil, nil, ErrCorrupt
	}
	if body[0] != FrameVersion {
		return FrameHeader{}, nil, nil, nil, ErrCorrupt
	}
	k := int(body[2])
	if k <= 0 || k > MaxFrameK {
		return FrameHeader{}, nil, nil, nil, ErrCorrupt
	}
	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	headerEnd := FrameHeaderSize + ridBytes + offsetBytes
	if len(body) < headerEnd {
		return FrameHeader{}, nil, nil, nil, ErrCorrupt
	}

	header := FrameHeader{
		Version:  body[0],
		Flags:    body[1],
		K:        uint8(k),
		Reserved: body[3],
		DictID:   binary.LittleEndian.Uint64(body[4:12]),
	}

	rids := make([]uint64, k)
	off := FrameHeaderSize
	for i := 0; i < k; i++ {
		rid := binary.LittleEndian.Uint64(body[off : off+8])
		if rid == 0 {
			return FrameHeader{}, nil, nil, nil, ErrCorrupt
		}
		rids[i] = rid
		off += 8
	}

	offsets := make([]uint32, k+1)
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(body[off : off+4])
		if cur < prev {
			return FrameHeader{}, nil, nil, nil, ErrCorrupt
		}
		offsets[i] = cur
		prev = cur
		off += 4
	}

	payload := body[headerEnd:]
	if len(payload) == 0 && offsets[len(offsets)-1] != 0 {
		return FrameHeader{}, nil, nil, nil, ErrCorrupt
	}
	return header, rids, offsets, payload, nil
}

func decodeFrameValueBounds(body []byte, subIndex int) (FrameHeader, uint32, uint32, uint32, []byte, error) {
	if len(body) < FrameHeaderSize {
		return FrameHeader{}, 0, 0, 0, nil, ErrCorrupt
	}
	if body[0] != FrameVersion {
		return FrameHeader{}, 0, 0, 0, nil, ErrCorrupt
	}
	k := int(body[2])
	if k <= 0 || k > MaxFrameK {
		return FrameHeader{}, 0, 0, 0, nil, ErrCorrupt
	}
	if subIndex < 0 || subIndex >= k {
		return FrameHeader{}, 0, 0, 0, nil, ErrCorrupt
	}

	ridBytes := k * 8
	offsetBytes := (k + 1) * 4
	headerEnd := FrameHeaderSize + ridBytes + offsetBytes
	if len(body) < headerEnd {
		return FrameHeader{}, 0, 0, 0, nil, ErrCorrupt
	}

	header := FrameHeader{
		Version:  body[0],
		Flags:    body[1],
		K:        uint8(k),
		Reserved: body[3],
		DictID:   binary.LittleEndian.Uint64(body[4:12]),
	}

	ridOff := FrameHeaderSize
	for i := 0; i < k; i++ {
		if binary.LittleEndian.Uint64(body[ridOff:ridOff+8]) == 0 {
			return FrameHeader{}, 0, 0, 0, nil, ErrCorrupt
		}
		ridOff += 8
	}

	var (
		start  uint32
		end    uint32
		rawLen uint32
		prev   uint32
	)
	off := FrameHeaderSize + ridBytes
	for i := 0; i < k+1; i++ {
		cur := binary.LittleEndian.Uint32(body[off : off+4])
		if cur < prev {
			return FrameHeader{}, 0, 0, 0, nil, ErrCorrupt
		}
		if i == subIndex {
			start = cur
		}
		if i == subIndex+1 {
			end = cur
		}
		prev = cur
		rawLen = cur
		off += 4
	}

	payload := body[headerEnd:]
	if len(payload) == 0 && rawLen != 0 {
		return FrameHeader{}, 0, 0, 0, nil, ErrCorrupt
	}
	if header.Flags&FrameFlagCompressed == 0 {
		if uint32(len(payload)) != rawLen {
			return FrameHeader{}, 0, 0, 0, nil, ErrCorrupt
		}
	}
	return header, start, end, rawLen, payload, nil
}
