package colgranule

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

const (
	DefaultRowsPerGranule = 8192
	maxGranuleDecodeRows  = 1 << 20
)

type Encoding uint8

const (
	EncodingRawInt64 Encoding = iota + 1
	EncodingDeltaVarint
	EncodingDoubleDeltaVarint
	EncodingNullableInt64
	EncodingBoolBitpackRLE
	EncodingLowCardinalityUint32
)

func (e Encoding) String() string {
	switch e {
	case EncodingRawInt64:
		return "raw_int64"
	case EncodingDeltaVarint:
		return "delta_varint"
	case EncodingDoubleDeltaVarint:
		return "double_delta_varint"
	case EncodingNullableInt64:
		return "nullable_int64"
	case EncodingBoolBitpackRLE:
		return "bool_bitpack_rle"
	case EncodingLowCardinalityUint32:
		return "low_cardinality_uint32"
	default:
		return fmt.Sprintf("encoding_%d", e)
	}
}

type Compression uint8

const (
	CompressionNone Compression = iota
	CompressionSnappy
	CompressionLZ4
	CompressionZSTD
	CompressionZSTDDict
)

func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	case CompressionZSTD:
		return "zstd"
	case CompressionZSTDDict:
		return "zstd_dict"
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

type CodecReport struct {
	Encoding                  Encoding
	RequestedCompression      Compression
	ActualCompression         Compression
	CompressionAttempted      bool
	CompressionKept           bool
	CompressionFallbackReason string
	RawBytes                  int
	StoredBytes               int
	CompressionNanos          int64
}

type EncodedGranule struct {
	Rows         int
	NullCount    int
	DefaultCount int
	HasMinMax    bool
	Min          int64
	Max          int64
	Encoding     Encoding
	Compression  Compression
	RawBytes     int
	StoredBytes  int
	PayloadRef   PayloadRef
	CodecReport  CodecReport
	Payload      []byte
}

type GranuleBuilder struct {
	cfg        Config
	raw        []byte
	encoded    []byte
	compressed []byte
	values64   []int64
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
	if err := validateGranuleDecodeRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	min, max := minMax(values)
	raw, err := encodeInt64Payload(b.raw[:0], values, b.cfg.Encoding)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, b.cfg.Encoding, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(len(values), min, max, true, b.cfg.Encoding, selection), nil
}

type GranuleReader struct {
	raw      []byte
	values   []int64
	stored64 []int64
	bools    []bool
	nulls    []bool
	defaults []bool
	codes    []uint32
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

func (r *GranuleReader) int64Cursor(g EncodedGranule) (int64Cursor, error) {
	raw, err := r.decompressPayload(g)
	if err != nil {
		return int64Cursor{}, err
	}
	cursor := int64Cursor{
		encoding: g.Encoding,
		raw:      raw,
		rows:     g.Rows,
	}
	switch g.Encoding {
	case EncodingRawInt64:
		if len(raw)%8 != 0 || len(raw)/8 != g.Rows {
			return int64Cursor{}, fmt.Errorf("colgranule: raw int64 length=%d rows=%d", len(raw), g.Rows)
		}
	case EncodingDeltaVarint, EncodingDoubleDeltaVarint:
	default:
		return int64Cursor{}, fmt.Errorf("colgranule: unsupported encoding %d", g.Encoding)
	}
	return cursor, nil
}

func (r *GranuleReader) RangeScanCountInt64(g EncodedGranule, low, high int64) (int, error) {
	if low > high || (g.HasMinMax && (high < g.Min || low > g.Max)) {
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
	if err := validateGranuleDecodeRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	min, max := minMax(values)
	raw, err := encodeInt64Payload(dst[:0], values, cfg.Encoding)
	if err != nil {
		return EncodedGranule{}, err
	}
	selection, err := compressPayload(raw, cfg.Encoding, cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	return newEncodedGranule(len(values), min, max, true, cfg.Encoding, selection), nil
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
	case EncodingDoubleDeltaVarint:
		var err error
		out, err = decodeDoubleDeltaVarint(out, raw, g.Rows)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("colgranule: unsupported encoding %d", g.Encoding)
	}
	return out, nil
}

func RangeScanCount(g EncodedGranule, low, high int64, scratch []int64) (int, []int64, error) {
	if low > high || (g.HasMinMax && (high < g.Min || low > g.Max)) {
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

func newEncodedGranule(rows int, min int64, max int64, hasMinMax bool, encoding Encoding, selection CompressionSelection) EncodedGranule {
	report := selection.Report
	report.Encoding = encoding
	report.ActualCompression = selection.Actual
	report.StoredBytes = len(selection.Payload)
	return EncodedGranule{
		Rows:        rows,
		HasMinMax:   hasMinMax,
		Min:         min,
		Max:         max,
		Encoding:    encoding,
		Compression: selection.Actual,
		RawBytes:    report.RawBytes,
		StoredBytes: len(selection.Payload),
		PayloadRef: PayloadRef{
			Kind:   PayloadRefInline,
			Length: len(selection.Payload),
		},
		CodecReport: report,
		Payload:     selection.Payload,
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
	case EncodingDoubleDeltaVarint:
		if cap(dst) < len(values)*2 {
			dst = make([]byte, 0, len(values)*2)
		}
		var buf [binary.MaxVarintLen64]byte
		prev := int64(0)
		prevDelta := int64(0)
		for i, v := range values {
			var encoded int64
			switch i {
			case 0:
				encoded = v
			case 1:
				encoded = v - prev
				prevDelta = encoded
			default:
				delta := v - prev
				encoded = delta - prevDelta
				prevDelta = delta
			}
			n := binary.PutUvarint(buf[:], zigzag(encoded))
			dst = append(dst, buf[:n]...)
			prev = v
		}
		return dst, nil
	default:
		return nil, fmt.Errorf("colgranule: unsupported encoding %d", encoding)
	}
}

func decodeDeltaVarint(dst []int64, raw []byte, rows int) ([]int64, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return nil, err
	}
	if rows > len(raw) {
		return nil, fmt.Errorf("colgranule: delta rows=%d exceed payload bytes=%d", rows, len(raw))
	}
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

func decodeDoubleDeltaVarint(dst []int64, raw []byte, rows int) ([]int64, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return nil, err
	}
	if rows > len(raw) {
		return nil, fmt.Errorf("colgranule: double-delta rows=%d exceed payload bytes=%d", rows, len(raw))
	}
	out := dst[:0]
	if cap(out) < rows {
		out = make([]int64, 0, rows)
	}
	prev := int64(0)
	prevDelta := int64(0)
	for len(raw) > 0 && len(out) < rows {
		u, n := binary.Uvarint(raw)
		if n <= 0 {
			return nil, errors.New("colgranule: malformed double-delta varint")
		}
		encoded := unzigzag(u)
		var v int64
		switch len(out) {
		case 0:
			v = encoded
		case 1:
			prevDelta = encoded
			v = prev + encoded
		default:
			delta := prevDelta + encoded
			v = prev + delta
			prevDelta = delta
		}
		out = append(out, v)
		prev = v
		raw = raw[n:]
	}
	if len(out) != rows {
		return nil, fmt.Errorf("colgranule: decoded rows=%d want=%d", len(out), rows)
	}
	if len(raw) != 0 {
		return nil, errors.New("colgranule: trailing double-delta bytes")
	}
	return out, nil
}

type int64Cursor struct {
	encoding  Encoding
	raw       []byte
	rows      int
	row       int
	offset    int
	prev      int64
	prevDelta int64
}

func (c *int64Cursor) Next() (int64, error) {
	if c.row >= c.rows {
		return 0, fmt.Errorf("colgranule: int64 cursor row=%d exceeds rows=%d", c.row, c.rows)
	}
	switch c.encoding {
	case EncodingRawInt64:
		v := int64(binary.LittleEndian.Uint64(c.raw[c.row*8:]))
		c.row++
		c.offset = c.row * 8
		return v, nil
	case EncodingDeltaVarint:
		u, n := binary.Uvarint(c.raw[c.offset:])
		if n <= 0 {
			return 0, errors.New("colgranule: malformed delta varint")
		}
		delta := unzigzag(u)
		v := delta
		if c.row > 0 {
			v = c.prev + delta
		}
		c.row++
		c.offset += n
		c.prev = v
		return v, nil
	case EncodingDoubleDeltaVarint:
		u, n := binary.Uvarint(c.raw[c.offset:])
		if n <= 0 {
			return 0, errors.New("colgranule: malformed double-delta varint")
		}
		encoded := unzigzag(u)
		var v int64
		switch c.row {
		case 0:
			v = encoded
		case 1:
			c.prevDelta = encoded
			v = c.prev + encoded
		default:
			delta := c.prevDelta + encoded
			v = c.prev + delta
			c.prevDelta = delta
		}
		c.row++
		c.offset += n
		c.prev = v
		return v, nil
	default:
		return 0, fmt.Errorf("colgranule: unsupported encoding %d", c.encoding)
	}
}

func (c *int64Cursor) Finish() error {
	if c.row != c.rows {
		return fmt.Errorf("colgranule: decoded rows=%d want=%d", c.row, c.rows)
	}
	if c.encoding != EncodingRawInt64 && c.offset != len(c.raw) {
		return fmt.Errorf("colgranule: trailing %s bytes", c.encoding)
	}
	return nil
}

func validateGranuleDecodeRows(rows int) error {
	if rows < 0 {
		return fmt.Errorf("colgranule: negative rows=%d", rows)
	}
	if rows > maxGranuleDecodeRows {
		return fmt.Errorf("colgranule: rows=%d exceed cap %d", rows, maxGranuleDecodeRows)
	}
	return nil
}

func (c *int64Cursor) RawBytesRead() int {
	return c.offset
}

type CompressionSelection struct {
	Payload []byte
	Actual  Compression
	Scratch []byte
	Report  CodecReport
}

func admitCompressionInto(dst []byte, raw []byte, encoding Encoding, compression Compression) (CompressionSelection, error) {
	report := CodecReport{
		Encoding:             encoding,
		RequestedCompression: compression,
		ActualCompression:    compression,
		RawBytes:             len(raw),
	}
	switch compression {
	case CompressionNone:
		report.ActualCompression = CompressionNone
		report.CompressionKept = true
		report.StoredBytes = len(raw)
		return CompressionSelection{Payload: raw, Actual: CompressionNone, Scratch: dst[:0], Report: report}, nil
	case CompressionSnappy:
		need := snappy.MaxEncodedLen(len(raw))
		if cap(dst) < need {
			dst = make([]byte, 0, need)
		} else {
			dst = dst[:0]
		}
		start := time.Now()
		out := snappy.Encode(dst, raw)
		report.CompressionNanos = time.Since(start).Nanoseconds()
		report.CompressionAttempted = true
		if len(out) >= len(raw) {
			report.ActualCompression = CompressionNone
			report.CompressionFallbackReason = "not_smaller"
			report.StoredBytes = len(raw)
			return CompressionSelection{Payload: raw, Actual: CompressionNone, Scratch: dst[:0], Report: report}, nil
		}
		report.ActualCompression = CompressionSnappy
		report.CompressionKept = true
		report.StoredBytes = len(out)
		return CompressionSelection{Payload: out, Actual: CompressionSnappy, Scratch: out, Report: report}, nil
	case CompressionLZ4:
		need := lz4.CompressBlockBound(len(raw))
		if cap(dst) < need {
			dst = make([]byte, need)
		} else {
			dst = dst[:need]
		}
		start := time.Now()
		n, err := lz4.CompressBlock(raw, dst, nil)
		report.CompressionNanos = time.Since(start).Nanoseconds()
		if err != nil {
			return CompressionSelection{}, err
		}
		report.CompressionAttempted = true
		if n == 0 || n >= len(raw) {
			report.ActualCompression = CompressionNone
			report.CompressionFallbackReason = "not_smaller"
			report.StoredBytes = len(raw)
			return CompressionSelection{Payload: raw, Actual: CompressionNone, Scratch: dst[:0], Report: report}, nil
		}
		out := dst[:n]
		report.ActualCompression = CompressionLZ4
		report.CompressionKept = true
		report.StoredBytes = len(out)
		return CompressionSelection{Payload: out, Actual: CompressionLZ4, Scratch: out, Report: report}, nil
	default:
		return CompressionSelection{}, fmt.Errorf("colgranule: unsupported compression %d", compression)
	}
}

func compressPayload(raw []byte, encoding Encoding, compression Compression) (CompressionSelection, error) {
	selection, err := admitCompressionInto(nil, raw, encoding, compression)
	if err != nil {
		return CompressionSelection{}, err
	}
	payload := append([]byte(nil), selection.Payload...)
	selection.Payload = payload
	selection.Report.StoredBytes = len(payload)
	return selection, nil
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
	if err := validateGranuleDecodeRows(g.Rows); err != nil {
		return err
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
	maxRaw := maxGranuleRawPayloadBytes(g.Encoding, g.Rows)
	if maxRaw < 0 {
		return fmt.Errorf("colgranule: unsupported encoding %d", g.Encoding)
	}
	if g.RawBytes > maxRaw {
		return fmt.Errorf("colgranule: raw payload length=%d exceeds max=%d", g.RawBytes, maxRaw)
	}
	maxStored, err := maxGranuleStoredPayloadBytes(g.Compression, maxRaw)
	if err != nil {
		return err
	}
	if g.StoredBytes > maxStored {
		return fmt.Errorf("colgranule: stored payload length=%d exceeds max=%d", g.StoredBytes, maxStored)
	}
	if len(g.Payload) > maxStored {
		return fmt.Errorf("colgranule: payload length=%d exceeds max=%d", len(g.Payload), maxStored)
	}
	if g.PayloadRef.Kind != PayloadRefInline {
		return fmt.Errorf("colgranule: unsupported payload ref kind %d", g.PayloadRef.Kind)
	}
	if g.PayloadRef.Offset != 0 {
		return fmt.Errorf("colgranule: inline payload offset=%d want=0", g.PayloadRef.Offset)
	}
	if g.StoredBytes != len(g.Payload) {
		return fmt.Errorf("colgranule: stored payload length=%d want=%d", len(g.Payload), g.StoredBytes)
	}
	if g.PayloadRef.Length != len(g.Payload) {
		return fmt.Errorf("colgranule: payload ref length=%d want=%d", g.PayloadRef.Length, len(g.Payload))
	}
	return nil
}

func maxGranuleRawPayloadBytes(encoding Encoding, rows int) int {
	switch encoding {
	case EncodingRawInt64:
		return rows * 8
	case EncodingDeltaVarint, EncodingDoubleDeltaVarint:
		return rows * binary.MaxVarintLen64
	case EncodingNullableInt64:
		return nullableInt64HeaderBytes + 2*bitmapBytes(rows) + rows*binary.MaxVarintLen64
	case EncodingBoolBitpackRLE:
		return 2 + rows*binary.MaxVarintLen64
	case EncodingLowCardinalityUint32:
		return 1 + binary.MaxVarintLen64 + rows*4
	default:
		return -1
	}
}

func maxGranuleStoredPayloadBytes(compression Compression, maxRaw int) (int, error) {
	switch compression {
	case CompressionNone:
		return maxRaw, nil
	case CompressionSnappy:
		return snappy.MaxEncodedLen(maxRaw), nil
	case CompressionLZ4:
		return lz4.CompressBlockBound(maxRaw), nil
	default:
		return 0, fmt.Errorf("colgranule: unsupported compression %d", compression)
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
