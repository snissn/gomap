package valuelog

import (
	"encoding/binary"
	"errors"

	"github.com/klauspost/compress/zstd"
	"github.com/snissn/gomap/TreeDB/slab"
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
	MaxFrameK       = 8
)

const (
	FrameFlagCompressed byte = 1 << 0
)

var (
	ErrCorrupt        = errors.New("valuelog: corrupt record")
	ErrRecordTooLarge = errors.New("valuelog: record too large")
	ErrMissingDict    = errors.New("valuelog: missing dict bytes")
)

type DictLookup func(dictID uint64) ([]byte, error)

type Record struct {
	RID   uint64
	Value []byte
}

type FrameHeader struct {
	Version  byte
	Flags    byte
	K        uint8
	Reserved uint8
	DictID   uint64
}

func EncodeFrame(dictID uint64, dict []byte, records []Record) ([]byte, FrameHeader, error) {
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
	if slab.MaxRecordSize > 0 && int64(rawTotal) > slab.MaxRecordSize {
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
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderCRC(false))
		if err != nil {
			return nil, FrameHeader{}, err
		}
		encoded = enc.EncodeAll(payload, nil)
		enc.Close()
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
	if slab.MaxRecordSize > 0 && int64(HeaderSize+bodyLen) > slab.MaxRecordSize {
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
