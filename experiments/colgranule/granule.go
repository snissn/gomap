package colgranule

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

const DefaultRowsPerGranule = 8192

type Encoding uint8

const (
	EncodingRawInt64 Encoding = iota + 1
	EncodingDeltaVarint
)

func (e Encoding) String() string {
	switch e {
	case EncodingRawInt64:
		return "raw_int64"
	case EncodingDeltaVarint:
		return "delta_varint"
	default:
		return fmt.Sprintf("encoding_%d", e)
	}
}

type Compression uint8

const (
	CompressionNone Compression = iota
	CompressionSnappy
	CompressionLZ4
)

func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	default:
		return fmt.Sprintf("compression_%d", c)
	}
}

type Config struct {
	Encoding    Encoding
	Compression Compression
}

type EncodedGranule struct {
	Rows        int
	Min         int64
	Max         int64
	Encoding    Encoding
	Compression Compression
	RawBytes    int
	Payload     []byte
}

func EncodeInt64(dst []byte, values []int64, cfg Config) (EncodedGranule, error) {
	if len(values) == 0 {
		return EncodedGranule{}, errors.New("colgranule: empty granule")
	}
	min, max := minMax(values)
	raw, err := encodeInt64Payload(dst[:0], values, cfg.Encoding)
	if err != nil {
		return EncodedGranule{}, err
	}
	payload, compression, err := compressPayload(raw, cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	return EncodedGranule{
		Rows:        len(values),
		Min:         min,
		Max:         max,
		Encoding:    cfg.Encoding,
		Compression: compression,
		RawBytes:    len(raw),
		Payload:     payload,
	}, nil
}

func DecodeInt64(dst []int64, g EncodedGranule) ([]int64, error) {
	raw, err := decompressPayload(g)
	if err != nil {
		return nil, err
	}
	out := dst[:0]
	switch g.Encoding {
	case EncodingRawInt64:
		if len(raw) != g.Rows*8 {
			return nil, fmt.Errorf("colgranule: raw int64 length=%d rows=%d", len(raw), g.Rows)
		}
		for len(raw) >= 8 {
			out = append(out, int64(binary.LittleEndian.Uint64(raw[:8])))
			raw = raw[8:]
		}
	case EncodingDeltaVarint:
		out, err = decodeDeltaVarint(out, raw, g.Rows)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("colgranule: unsupported encoding %d", g.Encoding)
	}
	return out, nil
}

func RangeScanCount(g EncodedGranule, low, high int64, scratch []int64) (int, []int64, error) {
	if low > high || high < g.Min || low > g.Max {
		return 0, scratch[:0], nil
	}
	values, err := DecodeInt64(scratch, g)
	if err != nil {
		return 0, scratch, err
	}
	count := 0
	for _, v := range values {
		if v >= low && v <= high {
			count++
		}
	}
	return count, values, nil
}

func encodeInt64Payload(dst []byte, values []int64, encoding Encoding) ([]byte, error) {
	switch encoding {
	case EncodingRawInt64:
		need := len(values) * 8
		if cap(dst) < need {
			dst = make([]byte, need)
		} else {
			dst = dst[:need]
		}
		for i, v := range values {
			binary.LittleEndian.PutUint64(dst[i*8:], uint64(v))
		}
		return dst, nil
	case EncodingDeltaVarint:
		if cap(dst) < len(values)*2 {
			dst = make([]byte, 0, len(values)*2)
		}
		var buf [binary.MaxVarintLen64]byte
		prev := int64(0)
		for i, v := range values {
			delta := v
			if i > 0 {
				delta = v - prev
			}
			n := binary.PutUvarint(buf[:], zigzag(delta))
			dst = append(dst, buf[:n]...)
			prev = v
		}
		return dst, nil
	default:
		return nil, fmt.Errorf("colgranule: unsupported encoding %d", encoding)
	}
}

func decodeDeltaVarint(dst []int64, raw []byte, rows int) ([]int64, error) {
	out := dst[:0]
	prev := int64(0)
	for len(raw) > 0 && len(out) < rows {
		u, n := binary.Uvarint(raw)
		if n <= 0 {
			return nil, errors.New("colgranule: malformed delta varint")
		}
		delta := unzigzag(u)
		v := delta
		if len(out) > 0 {
			v = prev + delta
		}
		out = append(out, v)
		prev = v
		raw = raw[n:]
	}
	if len(out) != rows {
		return nil, fmt.Errorf("colgranule: decoded rows=%d want=%d", len(out), rows)
	}
	if len(raw) != 0 {
		return nil, errors.New("colgranule: trailing delta bytes")
	}
	return out, nil
}

func compressPayload(raw []byte, compression Compression) ([]byte, Compression, error) {
	switch compression {
	case CompressionNone:
		return append([]byte(nil), raw...), CompressionNone, nil
	case CompressionSnappy:
		return snappy.Encode(nil, raw), CompressionSnappy, nil
	case CompressionLZ4:
		dst := make([]byte, lz4.CompressBlockBound(len(raw)))
		n, err := lz4.CompressBlock(raw, dst, nil)
		if err != nil {
			return nil, 0, err
		}
		if n == 0 || n >= len(raw) {
			return append([]byte(nil), raw...), CompressionNone, nil
		}
		return dst[:n], CompressionLZ4, nil
	default:
		return nil, 0, fmt.Errorf("colgranule: unsupported compression %d", compression)
	}
}

func decompressPayload(g EncodedGranule) ([]byte, error) {
	if g.RawBytes < 0 {
		return nil, fmt.Errorf("colgranule: negative raw payload length %d", g.RawBytes)
	}
	switch g.Compression {
	case CompressionNone:
		if len(g.Payload) != g.RawBytes {
			return nil, fmt.Errorf("colgranule: raw payload length=%d want=%d", len(g.Payload), g.RawBytes)
		}
		return g.Payload, nil
	case CompressionSnappy:
		out, err := snappy.Decode(nil, g.Payload)
		if err != nil {
			return nil, err
		}
		if len(out) != g.RawBytes {
			return nil, fmt.Errorf("colgranule: snappy decoded length=%d want=%d", len(out), g.RawBytes)
		}
		return out, nil
	case CompressionLZ4:
		out := make([]byte, g.RawBytes)
		n, err := lz4.UncompressBlock(g.Payload, out)
		if err != nil {
			return nil, err
		}
		if n != g.RawBytes {
			return nil, fmt.Errorf("colgranule: lz4 decoded length=%d want=%d", n, g.RawBytes)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("colgranule: unsupported compression %d", g.Compression)
	}
}

func minMax(values []int64) (int64, int64) {
	min, max := int64(math.MaxInt64), int64(math.MinInt64)
	for _, v := range values {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func zigzag(v int64) uint64 {
	return uint64(v<<1) ^ uint64(v>>63)
}

func unzigzag(v uint64) int64 {
	return int64(v>>1) ^ -int64(v&1)
}
