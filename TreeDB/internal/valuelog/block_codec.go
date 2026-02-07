package valuelog

import (
	"fmt"
	"sync"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
)

// BlockCodec identifies non-dictionary value-log frame compression codecs.
type BlockCodec uint8

const (
	BlockCodecNone BlockCodec = iota
	BlockCodecSnappy
	BlockCodecLZ4
	BlockCodecZSTD
)

// UnsupportedBlockCodecError reports an unknown per-frame block codec ID.
type UnsupportedBlockCodecError struct {
	CodecID uint8
}

func (e UnsupportedBlockCodecError) Error() string {
	return fmt.Sprintf("valuelog: unsupported block codec id %d", e.CodecID)
}

func normalizeBlockCodec(codec BlockCodec) BlockCodec {
	switch codec {
	case BlockCodecSnappy, BlockCodecLZ4, BlockCodecZSTD:
		return codec
	default:
		return BlockCodecSnappy
	}
}

func newBlockZSTDEncoder() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(false),
	)
	if err != nil {
		panic(err)
	}
	return enc
}

func newBlockZSTDDecoder() *zstd.Decoder {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
	return dec
}

var blockZSTDEncPool = sync.Pool{
	New: func() any { return newBlockZSTDEncoder() },
}

var blockZSTDDecPool = sync.Pool{
	New: func() any { return newBlockZSTDDecoder() },
}

func encodeBlockPayload(codec BlockCodec, raw []byte, dst []byte) ([]byte, error) {
	if len(raw) == 0 {
		if dst == nil {
			return nil, nil
		}
		return dst[:0], nil
	}
	switch normalizeBlockCodec(codec) {
	case BlockCodecSnappy:
		return snappy.Encode(dst[:0], raw), nil
	case BlockCodecLZ4:
		bound := lz4.CompressBlockBound(len(raw))
		if cap(dst) < bound {
			dst = make([]byte, bound)
		}
		dst = dst[:bound]
		n, err := lz4.CompressBlock(raw, dst, nil)
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return nil, errEncodedTooLarge
		}
		return dst[:n], nil
	case BlockCodecZSTD:
		enc := blockZSTDEncPool.Get().(*zstd.Encoder)
		out := enc.EncodeAll(raw, dst[:0])
		blockZSTDEncPool.Put(enc)
		return out, nil
	default:
		return nil, UnsupportedBlockCodecError{CodecID: uint8(codec)}
	}
}

func decodeBlockPayload(codecID uint8, payload []byte, rawLen uint32, dst []byte) ([]byte, error) {
	if rawLen == 0 {
		if len(payload) != 0 {
			return nil, ErrCorrupt
		}
		return dst[:0], nil
	}
	switch BlockCodec(codecID) {
	case BlockCodecSnappy:
		decodedLen, err := snappy.DecodedLen(payload)
		if err != nil {
			return nil, err
		}
		if decodedLen != int(rawLen) {
			return nil, ErrCorrupt
		}
		if cap(dst) < decodedLen {
			dst = make([]byte, 0, decodedLen)
		} else {
			dst = dst[:0]
		}
		out, err := snappy.Decode(dst, payload)
		if err != nil {
			return nil, err
		}
		if uint32(len(out)) != rawLen {
			return nil, ErrCorrupt
		}
		return out, nil
	case BlockCodecLZ4:
		if cap(dst) < int(rawLen) {
			dst = make([]byte, int(rawLen))
		} else {
			dst = dst[:int(rawLen)]
		}
		n, err := lz4.UncompressBlock(payload, dst)
		if err != nil {
			return nil, err
		}
		if n != int(rawLen) {
			return nil, ErrCorrupt
		}
		return dst[:n], nil
	case BlockCodecZSTD:
		dec := blockZSTDDecPool.Get().(*zstd.Decoder)
		out, err := dec.DecodeAll(payload, dst[:0])
		blockZSTDDecPool.Put(dec)
		if err != nil {
			return nil, err
		}
		if uint32(len(out)) != rawLen {
			return nil, ErrCorrupt
		}
		return out, nil
	default:
		return nil, UnsupportedBlockCodecError{CodecID: codecID}
	}
}
