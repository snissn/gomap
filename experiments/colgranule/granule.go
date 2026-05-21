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

type PayloadRefKind uint8

const (
	PayloadRefInline PayloadRefKind = iota + 1
)

func (k PayloadRefKind) String() string {
	switch k {
	case PayloadRefInline:
		return "inline"
	default:
		return fmt.Sprintf("payload_ref_%d", k)
	}
}

type PayloadRef struct {
	Kind   PayloadRefKind
	Offset int64
	Length int
}

type EncodedGranule struct {
	Rows         int
	NullCount    int
	DefaultCount int
	Min          int64
	Max          int64
	Encoding     Encoding
	Compression  Compression
	RawBytes     int
	StoredBytes  int
	PayloadRef   PayloadRef
	Payload      []byte
}

type GranuleBuilder struct {
	cfg        Config
	raw        []byte
	compressed []byte
}

func NewGranuleBuilder(cfg Config) *GranuleBuilder {
	return &GranuleBuilder{cfg: cfg}
}

func (b *GranuleBuilder) Reset(cfg Config) {
	b.cfg = cfg
	b.raw = b.raw[:0]
	b.compressed = b.compressed[:0]
}

// BuildInt64 returns a granule whose payload aliases builder-owned scratch until
// the next BuildInt64 or Reset call.
func (b *GranuleBuilder) BuildInt64(values []int64) (EncodedGranule, error) {
	if len(values) == 0 {
		return EncodedGranule{}, errors.New("colgranule: empty granule")
	}
	min, max := minMax(values)
	raw, err := encodeInt64Payload(b.raw[:0], values, b.cfg.Encoding)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	payload, compression, compressed, err := compressPayloadInto(b.compressed[:0], raw, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = compressed
	return newEncodedGranule(len(values), min, max, b.cfg.Encoding, compression, len(raw), payload), nil
}

type GranuleReader struct {
	raw    []byte
	values []int64
}

func (r *GranuleReader) DecodeInt64(g EncodedGranule) ([]int64, error) {
	values, err := r.DecodeInt64Into(r.values[:0], g)
	if err != nil {
		return nil, err
	}
	r.values = values
	return values, nil
}

func (r *GranuleReader) DecodeInt64Into(dst []int64, g EncodedGranule) ([]int64, error) {
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return decodeInt64Payload(dst, raw, g)
}

func (r *GranuleReader) RangeScanCountInt64(g EncodedGranule, low, high int64) (int, error) {
	if low > high || high < g.Min || low > g.Max {
		r.values = r.values[:0]
		return 0, nil
	}
	values, err := r.DecodeInt64(g)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, v := range values {
		if v >= low && v <= high {
			count++
		}
	}
	return count, nil
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
	return newEncodedGranule(len(values), min, max, cfg.Encoding, compression, len(raw), payload), nil
}

func DecodeInt64(dst []int64, g EncodedGranule) ([]int64, error) {
	var reader GranuleReader
	return reader.DecodeInt64Into(dst, g)
}

func decodeInt64Payload(dst []int64, raw []byte, g EncodedGranule) ([]int64, error) {
	out := dst[:0]
	switch g.Encoding {
	case EncodingRawInt64:
		if len(raw)%8 != 0 || len(raw)/8 != g.Rows {
			return nil, fmt.Errorf("colgranule: raw int64 length=%d rows=%d", len(raw), g.Rows)
		}
		if cap(out) < g.Rows {
			out = make([]int64, g.Rows)
		} else {
			out = out[:g.Rows]
		}
		for i := range out {
			out[i] = int64(binary.LittleEndian.Uint64(raw[i*8:]))
		}
	case EncodingDeltaVarint:
		var err error
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
	var reader GranuleReader
	values, err := reader.DecodeInt64Into(scratch, g)
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

func newEncodedGranule(rows int, min int64, max int64, encoding Encoding, compression Compression, rawBytes int, payload []byte) EncodedGranule {
	return EncodedGranule{
		Rows:        rows,
		Min:         min,
		Max:         max,
		Encoding:    encoding,
		Compression: compression,
		RawBytes:    rawBytes,
		StoredBytes: len(payload),
		PayloadRef: PayloadRef{
			Kind:   PayloadRefInline,
			Length: len(payload),
		},
		Payload: payload,
	}
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
	if cap(out) < rows {
		out = make([]int64, 0, rows)
	}
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

func compressPayloadInto(dst []byte, raw []byte, compression Compression) ([]byte, Compression, []byte, error) {
	switch compression {
	case CompressionNone:
		return raw, CompressionNone, dst[:0], nil
	case CompressionSnappy:
		need := snappy.MaxEncodedLen(len(raw))
		if cap(dst) < need {
			dst = make([]byte, need)
		} else {
			dst = dst[:0]
		}
		out := snappy.Encode(dst, raw)
		return out, CompressionSnappy, out, nil
	case CompressionLZ4:
		need := lz4.CompressBlockBound(len(raw))
		if cap(dst) < need {
			dst = make([]byte, need)
		} else {
			dst = dst[:need]
		}
		n, err := lz4.CompressBlock(raw, dst, nil)
		if err != nil {
			return nil, 0, nil, err
		}
		if n == 0 || n >= len(raw) {
			return raw, CompressionNone, dst[:0], nil
		}
		out := dst[:n]
		return out, CompressionLZ4, out, nil
	default:
		return nil, 0, nil, fmt.Errorf("colgranule: unsupported compression %d", compression)
	}
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
	var reader GranuleReader
	return reader.decompressPayload(g)
}

func (r *GranuleReader) decompressPayload(g EncodedGranule) ([]byte, error) {
	if err := validateGranule(g); err != nil {
		return nil, err
	}
	switch g.Compression {
	case CompressionNone:
		if len(g.Payload) != g.RawBytes {
			return nil, fmt.Errorf("colgranule: raw payload length=%d want=%d", len(g.Payload), g.RawBytes)
		}
		return g.Payload, nil
	case CompressionSnappy:
		decodedLen, err := snappy.DecodedLen(g.Payload)
		if err != nil {
			return nil, err
		}
		if decodedLen != g.RawBytes {
			return nil, fmt.Errorf("colgranule: snappy decoded length=%d want=%d", decodedLen, g.RawBytes)
		}
		if cap(r.raw) < decodedLen {
			r.raw = make([]byte, decodedLen)
		} else {
			r.raw = r.raw[:decodedLen]
		}
		out, err := snappy.Decode(r.raw, g.Payload)
		if err != nil {
			return nil, err
		}
		if len(out) != g.RawBytes {
			return nil, fmt.Errorf("colgranule: snappy decoded length=%d want=%d", len(out), g.RawBytes)
		}
		r.raw = out
		return out, nil
	case CompressionLZ4:
		if cap(r.raw) < g.RawBytes {
			r.raw = make([]byte, g.RawBytes)
		} else {
			r.raw = r.raw[:g.RawBytes]
		}
		out := r.raw
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

func validateGranule(g EncodedGranule) error {
	if g.Rows <= 0 {
		return fmt.Errorf("colgranule: invalid row count %d", g.Rows)
	}
	if g.NullCount < 0 {
		return fmt.Errorf("colgranule: negative null count %d", g.NullCount)
	}
	if g.DefaultCount < 0 {
		return fmt.Errorf("colgranule: negative default count %d", g.DefaultCount)
	}
	if g.NullCount > g.Rows || g.DefaultCount > g.Rows-g.NullCount {
		return errors.New("colgranule: null/default count exceeds rows")
	}
	if g.RawBytes < 0 {
		return fmt.Errorf("colgranule: negative raw payload length %d", g.RawBytes)
	}
	if g.StoredBytes < 0 {
		return fmt.Errorf("colgranule: negative stored payload length %d", g.StoredBytes)
	}
	if g.StoredBytes != 0 && g.StoredBytes != len(g.Payload) {
		return fmt.Errorf("colgranule: stored payload length=%d want=%d", len(g.Payload), g.StoredBytes)
	}
	if g.PayloadRef.Length != 0 && g.PayloadRef.Length != len(g.Payload) {
		return fmt.Errorf("colgranule: payload ref length=%d want=%d", g.PayloadRef.Length, len(g.Payload))
	}
	return nil
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
