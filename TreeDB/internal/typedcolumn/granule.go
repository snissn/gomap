package typedcolumn

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
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
	EncodingRawFloat32Vector
	EncodingRawUint32Dense
	EncodingRawFloat32
	EncodingRawFloat64
	// EncodingRawUint32OffsetsList is the v1 variable-length uint32 list
	// primitive: little-endian uint64 offsets plus little-endian uint32 values.
	EncodingRawUint32OffsetsList
	// EncodingRawBytesOffsets is the v1 non-null opaque bytes primitive:
	// little-endian uint64 offsets plus exact concatenated byte payloads.
	EncodingRawBytesOffsets
	// Raw fixed-width scalar primitives used by quantized side arrays. Multi-byte
	// values are little-endian; float16/bfloat16 are raw uint16 bit payloads.
	EncodingRawInt8
	EncodingRawUint8
	EncodingRawInt16
	EncodingRawUint16
	EncodingRawInt32
	EncodingRawUint32
	EncodingRawUint64
	EncodingRawFloat16
	EncodingRawBFloat16

	// Dense numeric vector encodings. The legacy raw_float32_vector keeps its
	// original value; this block follows #1929's scalar encodings.
	EncodingRawUint8Vector
	EncodingRawInt8Vector
	EncodingRawUint16Vector
	EncodingRawInt16Vector
	EncodingRawUint32Vector
	EncodingRawInt32Vector
	EncodingRawUint64Vector
	EncodingRawInt64Vector
	EncodingRawFloat16Vector
	EncodingRawBFloat16Vector
	EncodingRawFloat64Vector

	// Fixed-byte and packed sub-byte row-major vector encodings. Packed elements
	// are unsigned and LSB-first within each byte; unused high bits in the final
	// byte of each row must be zero.
	EncodingRawFixedBytes
	EncodingRawPackedBitVector
	EncodingRawPackedUint2Vector
	EncodingRawPackedUint4Vector

	// Section-level row locator encoding for contiguous primary IDs. This is
	// not a column granule codec.
	EncodingRowLocatorContiguous
	// Section-level dictionary encoding for dense 0..n-1 dictionary codes. This
	// is not a column granule codec.
	EncodingDictionaryDense
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
	case EncodingRawFloat32Vector:
		return "raw_float32_vector"
	case EncodingRawUint32Dense:
		return "raw_uint32_dense"
	case EncodingRawFloat32:
		return "raw_float32"
	case EncodingRawFloat64:
		return "raw_float64"
	case EncodingRawUint32OffsetsList:
		return "raw_uint32_offsets_list"
	case EncodingRawBytesOffsets:
		return "raw_bytes_offsets"
	case EncodingRawInt8:
		return "raw_int8"
	case EncodingRawUint8:
		return "raw_uint8"
	case EncodingRawInt16:
		return "raw_int16"
	case EncodingRawUint16:
		return "raw_uint16"
	case EncodingRawInt32:
		return "raw_int32"
	case EncodingRawUint32:
		return "raw_uint32"
	case EncodingRawUint64:
		return "raw_uint64"
	case EncodingRawFloat16:
		return "raw_float16"
	case EncodingRawBFloat16:
		return "raw_bfloat16"
	case EncodingRawUint8Vector:
		return "raw_uint8_vector"
	case EncodingRawInt8Vector:
		return "raw_int8_vector"
	case EncodingRawUint16Vector:
		return "raw_uint16_vector"
	case EncodingRawInt16Vector:
		return "raw_int16_vector"
	case EncodingRawUint32Vector:
		return "raw_uint32_vector"
	case EncodingRawInt32Vector:
		return "raw_int32_vector"
	case EncodingRawUint64Vector:
		return "raw_uint64_vector"
	case EncodingRawInt64Vector:
		return "raw_int64_vector"
	case EncodingRawFloat16Vector:
		return "raw_float16_vector"
	case EncodingRawBFloat16Vector:
		return "raw_bfloat16_vector"
	case EncodingRawFloat64Vector:
		return "raw_float64_vector"
	case EncodingRawFixedBytes:
		return "raw_fixed_bytes"
	case EncodingRawPackedBitVector:
		return "raw_packed_bit_vector"
	case EncodingRawPackedUint2Vector:
		return "raw_packed_uint2_vector"
	case EncodingRawPackedUint4Vector:
		return "raw_packed_uint4_vector"
	case EncodingRowLocatorContiguous:
		return "row_locator_contiguous"
	case EncodingDictionaryDense:
		return "dictionary_dense"
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
		return EncodedGranule{}, errors.New("typedcolumn: empty granule")
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
	raw       []byte
	values    []int64
	stored64  []int64
	bools     []bool
	nulls     []bool
	defaults  []bool
	codes     []uint32
	int8s     []int8
	uint8s    []uint8
	int16s    []int16
	uint16s   []uint16
	int32s    []int32
	uint32s   []uint32
	uint64s   []uint64
	float16s  []uint16
	bfloat16s []uint16
	float32s  []float32
	float64s  []float64
	offsets64 []uint64
	u32values []uint32
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
			return int64Cursor{}, fmt.Errorf("typedcolumn: raw int64 length=%d rows=%d", len(raw), g.Rows)
		}
	case EncodingDeltaVarint, EncodingDoubleDeltaVarint:
	default:
		return int64Cursor{}, fmt.Errorf("typedcolumn: unsupported encoding %s", g.Encoding)
	}
	return cursor, nil
}

// Int64Cursor streams int64 values from a granule without materializing a full
// []int64. It is intended for reducers and predicate cursors that have already
// selected a concrete typed-column path during prepare.
type Int64Cursor struct {
	cursor int64Cursor
}

// Int64Cursor returns a cursor over raw, delta-varint, or double-delta int64
// payloads after payload decompression/validation. The cursor aliases reader
// scratch for compressed payloads and is valid until the next GranuleReader
// operation.
func (r *GranuleReader) Int64Cursor(g EncodedGranule) (Int64Cursor, error) {
	cursor, err := r.int64Cursor(g)
	if err != nil {
		return Int64Cursor{}, err
	}
	return Int64Cursor{cursor: cursor}, nil
}

// Next returns the next value. Call Finish after consuming Rows values to catch
// truncated or trailing variable-width payload bytes.
func (c *Int64Cursor) Next() (int64, error) { return c.cursor.Next() }

// Finish validates that exactly the declared rows were consumed and that
// variable-width encodings have no trailing bytes.
func (c *Int64Cursor) Finish() error { return c.cursor.Finish() }

// Row returns the next row index to be decoded.
func (c *Int64Cursor) Row() int { return c.cursor.row }

// Rows returns the declared row count.
func (c *Int64Cursor) Rows() int { return c.cursor.rows }

// RawBytesRead returns the payload byte offset consumed by the cursor.
func (c *Int64Cursor) RawBytesRead() int { return c.cursor.RawBytesRead() }

// CountSumInt64 streams all int64 values in the granule and returns count+sum
// without materializing a []int64. It is intended for full-block reducers on
// certified prepared paths where predicate/visibility/selection do not require
// per-row branching in the caller.
func (r *GranuleReader) CountSumInt64(g EncodedGranule) (int, int64, error) {
	raw, err := r.decompressPayload(g)
	if err != nil {
		return 0, 0, err
	}
	if err := validateGranuleDecodeRows(g.Rows); err != nil {
		return 0, 0, err
	}
	var sum int64
	switch g.Encoding {
	case EncodingRawInt64:
		if len(raw)%8 != 0 || len(raw)/8 != g.Rows {
			return 0, 0, fmt.Errorf("typedcolumn: raw int64 length=%d rows=%d", len(raw), g.Rows)
		}
		for row := 0; row < g.Rows; row++ {
			v := int64(readLittleEndianUint64(raw[row*8:]))
			if err := addCountSumInt64(&sum, v); err != nil {
				return 0, 0, err
			}
		}
	case EncodingDeltaVarint:
		prev := int64(0)
		for row := 0; row < g.Rows; row++ {
			u, n := binary.Uvarint(raw)
			if n <= 0 {
				return 0, 0, errors.New("typedcolumn: malformed delta varint")
			}
			delta := unzigzag(u)
			v := delta
			if row > 0 {
				v = prev + delta
			}
			if err := addCountSumInt64(&sum, v); err != nil {
				return 0, 0, err
			}
			prev = v
			raw = raw[n:]
		}
		if len(raw) != 0 {
			return 0, 0, errors.New("typedcolumn: trailing delta bytes")
		}
	case EncodingDoubleDeltaVarint:
		prev := int64(0)
		prevDelta := int64(0)
		for row := 0; row < g.Rows; row++ {
			u, n := binary.Uvarint(raw)
			if n <= 0 {
				return 0, 0, errors.New("typedcolumn: malformed double-delta varint")
			}
			encoded := unzigzag(u)
			var v int64
			switch row {
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
			if err := addCountSumInt64(&sum, v); err != nil {
				return 0, 0, err
			}
			prev = v
			raw = raw[n:]
		}
		if len(raw) != 0 {
			return 0, 0, errors.New("typedcolumn: trailing double-delta bytes")
		}
	default:
		return 0, 0, fmt.Errorf("typedcolumn: unsupported encoding %s", g.Encoding)
	}
	return g.Rows, sum, nil
}

func addCountSumInt64(sum *int64, value int64) error {
	updated, err := checkedInt64Add(*sum, value)
	if err != nil {
		return fmt.Errorf("typedcolumn: int64 sum overflow: %w", err)
	}
	*sum = updated
	return nil
}

func checkedInt64Add(current, value int64) (int64, error) {
	if value > 0 && current > math.MaxInt64-value {
		return 0, fmt.Errorf("current=%d value=%d", current, value)
	}
	if value < 0 && current < math.MinInt64-value {
		return 0, fmt.Errorf("current=%d value=%d", current, value)
	}
	return current + value, nil
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
		return EncodedGranule{}, errors.New("typedcolumn: empty granule")
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
		var err error
		out, err = decodeLittleEndian8Payload(out, raw, g.Rows, "raw int64")
		if err != nil {
			return nil, err
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
		return nil, fmt.Errorf("typedcolumn: unsupported encoding %s", g.Encoding)
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
		return encodeLittleEndian8Payload(dst, values, "raw int64")
	case EncodingDeltaVarint:
		if cap(dst) < len(values)*2 {
			dst = make([]byte, 0, len(values)*2)
		} else {
			dst = dst[:0]
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
		} else {
			dst = dst[:0]
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
		return nil, fmt.Errorf("typedcolumn: unsupported encoding %s", encoding)
	}
}

func decodeDeltaVarint(dst []int64, raw []byte, rows int) ([]int64, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return nil, err
	}
	if rows > len(raw) {
		return nil, fmt.Errorf("typedcolumn: delta rows=%d exceed payload bytes=%d", rows, len(raw))
	}
	out := dst[:0]
	if cap(out) < rows {
		out = make([]int64, 0, rows)
	}
	prev := int64(0)
	for len(raw) > 0 && len(out) < rows {
		u, n := binary.Uvarint(raw)
		if n <= 0 {
			return nil, errors.New("typedcolumn: malformed delta varint")
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
		return nil, fmt.Errorf("typedcolumn: decoded rows=%d want=%d", len(out), rows)
	}
	if len(raw) != 0 {
		return nil, errors.New("typedcolumn: trailing delta bytes")
	}
	return out, nil
}

func decodeDoubleDeltaVarint(dst []int64, raw []byte, rows int) ([]int64, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return nil, err
	}
	if rows > len(raw) {
		return nil, fmt.Errorf("typedcolumn: double-delta rows=%d exceed payload bytes=%d", rows, len(raw))
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
			return nil, errors.New("typedcolumn: malformed double-delta varint")
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
		return nil, fmt.Errorf("typedcolumn: decoded rows=%d want=%d", len(out), rows)
	}
	if len(raw) != 0 {
		return nil, errors.New("typedcolumn: trailing double-delta bytes")
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
		return 0, fmt.Errorf("typedcolumn: int64 cursor row=%d exceeds rows=%d", c.row, c.rows)
	}
	switch c.encoding {
	case EncodingRawInt64:
		v := int64(readLittleEndianUint64(c.raw[c.row*8:]))
		c.row++
		c.offset = c.row * 8
		return v, nil
	case EncodingDeltaVarint:
		u, n := binary.Uvarint(c.raw[c.offset:])
		if n <= 0 {
			return 0, errors.New("typedcolumn: malformed delta varint")
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
			return 0, errors.New("typedcolumn: malformed double-delta varint")
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
		return 0, fmt.Errorf("typedcolumn: unsupported encoding %s", c.encoding)
	}
}

func (c *int64Cursor) Finish() error {
	if c.row != c.rows {
		return fmt.Errorf("typedcolumn: decoded rows=%d want=%d", c.row, c.rows)
	}
	if c.encoding != EncodingRawInt64 && c.offset != len(c.raw) {
		return fmt.Errorf("typedcolumn: trailing %s bytes", c.encoding)
	}
	return nil
}

func validateGranuleDecodeRows(rows int) error {
	if rows < 0 {
		return fmt.Errorf("typedcolumn: negative rows=%d", rows)
	}
	if rows > maxGranuleDecodeRows {
		return fmt.Errorf("typedcolumn: rows=%d exceed cap %d", rows, maxGranuleDecodeRows)
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

var (
	zstdEncoderPool sync.Pool
)

func getZstdEncoder() (*zstd.Encoder, error) {
	if v := zstdEncoderPool.Get(); v != nil {
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

func putZstdEncoder(enc *zstd.Encoder) {
	if enc == nil {
		return
	}
	enc.Reset(nil)
	zstdEncoderPool.Put(enc)
}

func getZstdDecoder(rawBytes int) (*zstd.Decoder, error) {
	maxDecodedBytes := rawBytes
	if maxDecodedBytes < 1<<20 {
		maxDecodedBytes = 1 << 20
	}
	return zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(uint64(maxDecodedBytes)),
		zstd.WithDecodeAllCapLimit(true),
	)
}

func putZstdDecoder(dec *zstd.Decoder) {
	if dec == nil {
		return
	}
	dec.Close()
}

func decodeZstdPayload(label string, payload []byte, rawBytes int, dst []byte) ([]byte, error) {
	if rawBytes < 0 {
		return nil, fmt.Errorf("typedcolumn: %s zstd raw bytes=%d is invalid", label, rawBytes)
	}
	dec, err := getZstdDecoder(rawBytes)
	if err != nil {
		return nil, fmt.Errorf("typedcolumn: %s zstd decoder: %w", label, err)
	}
	defer putZstdDecoder(dec)
	decodeDst := dst[:0]
	if rawBytes >= 0 {
		if cap(decodeDst) >= rawBytes {
			decodeDst = decodeDst[:0:rawBytes]
		} else {
			decodeDst = make([]byte, 0, rawBytes)
		}
	}
	out, err := dec.DecodeAll(payload, decodeDst)
	if err != nil {
		return nil, fmt.Errorf("typedcolumn: %s zstd decode: %w", label, err)
	}
	if len(out) != rawBytes {
		return nil, fmt.Errorf("typedcolumn: %s zstd decoded length=%d want=%d", label, len(out), rawBytes)
	}
	return out, nil
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
	case CompressionZSTD:
		enc, err := getZstdEncoder()
		if err != nil {
			return CompressionSelection{}, err
		}
		defer putZstdEncoder(enc)
		start := time.Now()
		out := enc.EncodeAll(raw, dst[:0])
		report.CompressionNanos = time.Since(start).Nanoseconds()
		report.CompressionAttempted = true
		if len(out) >= len(raw) {
			report.ActualCompression = CompressionNone
			report.CompressionFallbackReason = "not_smaller"
			report.StoredBytes = len(raw)
			return CompressionSelection{Payload: raw, Actual: CompressionNone, Scratch: out[:0], Report: report}, nil
		}
		report.ActualCompression = CompressionZSTD
		report.CompressionKept = true
		report.StoredBytes = len(out)
		return CompressionSelection{Payload: out, Actual: CompressionZSTD, Scratch: out, Report: report}, nil
	default:
		return CompressionSelection{}, fmt.Errorf("typedcolumn: unsupported compression %s", compression)
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
			return nil, fmt.Errorf("typedcolumn: raw payload length=%d want=%d", len(g.Payload), g.RawBytes)
		}
		return g.Payload, nil
	case CompressionSnappy:
		decodedLen, err := snappy.DecodedLen(g.Payload)
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: snappy decoded length: %w", err)
		}
		if decodedLen != g.RawBytes {
			return nil, fmt.Errorf("typedcolumn: snappy decoded length=%d want=%d", decodedLen, g.RawBytes)
		}
		if cap(r.raw) < decodedLen {
			r.raw = make([]byte, decodedLen)
		} else {
			r.raw = r.raw[:decodedLen]
		}
		out, err := snappy.Decode(r.raw, g.Payload)
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: snappy decode: %w", err)
		}
		if len(out) != g.RawBytes {
			return nil, fmt.Errorf("typedcolumn: snappy decoded length=%d want=%d", len(out), g.RawBytes)
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
			return nil, fmt.Errorf("typedcolumn: lz4 decode: %w", err)
		}
		if n != g.RawBytes {
			return nil, fmt.Errorf("typedcolumn: lz4 decoded length=%d want=%d", n, g.RawBytes)
		}
		return out, nil
	case CompressionZSTD:
		if cap(r.raw) < g.RawBytes {
			r.raw = make([]byte, 0, g.RawBytes)
		} else {
			r.raw = r.raw[:0]
		}
		out, err := decodeZstdPayload("granule", g.Payload, g.RawBytes, r.raw)
		if err != nil {
			return nil, err
		}
		r.raw = out
		return out, nil
	default:
		return nil, fmt.Errorf("typedcolumn: unsupported compression %s", g.Compression)
	}
}

func validateGranule(g EncodedGranule) error {
	if g.Rows <= 0 {
		return fmt.Errorf("typedcolumn: invalid row count %d", g.Rows)
	}
	if err := validateGranuleDecodeRows(g.Rows); err != nil {
		return err
	}
	if g.NullCount < 0 {
		return fmt.Errorf("typedcolumn: negative null count %d", g.NullCount)
	}
	if g.DefaultCount < 0 {
		return fmt.Errorf("typedcolumn: negative default count %d", g.DefaultCount)
	}
	if g.NullCount > g.Rows || g.DefaultCount > g.Rows-g.NullCount {
		return errors.New("typedcolumn: null/default count exceeds rows")
	}
	if g.RawBytes < 0 {
		return fmt.Errorf("typedcolumn: negative raw payload length %d", g.RawBytes)
	}
	if g.StoredBytes < 0 {
		return fmt.Errorf("typedcolumn: negative stored payload length %d", g.StoredBytes)
	}
	maxRaw := maxGranuleRawPayloadBytes(g.Encoding, g.Rows)
	if maxRaw < 0 {
		return fmt.Errorf("typedcolumn: unsupported encoding %s", g.Encoding)
	}
	if g.RawBytes > maxRaw {
		return fmt.Errorf("typedcolumn: raw payload length=%d exceeds max=%d", g.RawBytes, maxRaw)
	}
	maxStored, err := maxGranuleStoredPayloadBytes(g.Compression, maxRaw)
	if err != nil {
		return err
	}
	if g.StoredBytes > maxStored {
		return fmt.Errorf("typedcolumn: stored payload length=%d exceeds max=%d", g.StoredBytes, maxStored)
	}
	if len(g.Payload) > maxStored {
		return fmt.Errorf("typedcolumn: payload length=%d exceeds max=%d", len(g.Payload), maxStored)
	}
	if g.PayloadRef.Kind != PayloadRefInline {
		return fmt.Errorf("typedcolumn: unsupported payload ref kind %d", g.PayloadRef.Kind)
	}
	if g.PayloadRef.Offset != 0 {
		return fmt.Errorf("typedcolumn: inline payload offset=%d want=0", g.PayloadRef.Offset)
	}
	if g.StoredBytes != len(g.Payload) {
		return fmt.Errorf("typedcolumn: stored payload length=%d want=%d", len(g.Payload), g.StoredBytes)
	}
	if g.PayloadRef.Length != len(g.Payload) {
		return fmt.Errorf("typedcolumn: payload ref length=%d want=%d", g.PayloadRef.Length, len(g.Payload))
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
	case EncodingRawFloat32:
		return rows * 4
	case EncodingRawFloat64:
		return rows * 8
	case EncodingRawInt8, EncodingRawUint8:
		return rows
	case EncodingRawInt16, EncodingRawUint16, EncodingRawFloat16, EncodingRawBFloat16:
		return rows * 2
	case EncodingRawInt32, EncodingRawUint32:
		return rows * 4
	case EncodingRawUint64:
		return rows * 8
	case EncodingRawFloat32Vector,
		EncodingRawUint8Vector,
		EncodingRawInt8Vector,
		EncodingRawUint16Vector,
		EncodingRawInt16Vector,
		EncodingRawUint32Vector,
		EncodingRawInt32Vector,
		EncodingRawUint64Vector,
		EncodingRawInt64Vector,
		EncodingRawFloat16Vector,
		EncodingRawBFloat16Vector,
		EncodingRawFloat64Vector,
		EncodingRawUint32Dense,
		EncodingRawUint32OffsetsList,
		EncodingRawBytesOffsets,
		EncodingRawFixedBytes,
		EncodingRawPackedBitVector,
		EncodingRawPackedUint2Vector,
		EncodingRawPackedUint4Vector:
		return int(^uint(0) >> 1)
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
	case CompressionZSTD:
		return maxRaw, nil
	default:
		return 0, fmt.Errorf("typedcolumn: unsupported compression %s", compression)
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
