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

var blockZstdEncoderPool sync.Pool

func getBlockZstdEncoder() (*zstd.Encoder, error) {
	if v := blockZstdEncoderPool.Get(); v != nil {
		if enc, ok := v.(*zstd.Encoder); ok && enc != nil {
			return enc, nil
		}
	}
	return zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(false),
	)
}

func putBlockZstdEncoder(enc *zstd.Encoder) {
	if enc == nil {
		return
	}
	enc.Reset(nil)
	blockZstdEncoderPool.Put(enc)
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
		need := snappy.MaxEncodedLen(len(raw))
		if len(dst) < need {
			if cap(dst) < need {
				dst = make([]byte, need)
			} else {
				dst = dst[:need]
			}
		} else {
			dst = dst[:need]
		}
		return snappy.Encode(dst, raw), nil
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
		enc, err := getBlockZstdEncoder()
		if err != nil {
			return nil, err
		}
		defer putBlockZstdEncoder(enc)
		return enc.EncodeAll(raw, dst[:0]), nil
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
		need := int(rawLen)
		if cap(dst) < need {
			dst = make([]byte, need)
		} else {
			dst = dst[:need]
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
		dec, err := getNoDictDecoder()
		if err != nil {
			return nil, err
		}
		defer putNoDictDecoder(dec)
		out, err := dec.DecodeAll(payload, dst[:0])
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
