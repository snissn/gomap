package colgranule

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

const (
	boolPayloadBitpack byte = 1
	boolPayloadRLE     byte = 2

	nullableInt64HeaderBytes = 21
	maxCodeCardinality       = 1 << 20
)

// BuildBool returns a granule whose payload aliases builder-owned scratch until
// the next builder Build* or Reset call.
func (b *GranuleBuilder) BuildBool(values []bool) (EncodedGranule, error) {
	if len(values) == 0 {
		return EncodedGranule{}, errors.New("colgranule: empty granule")
	}
	if err := validateGranuleDecodeRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	raw := encodeBoolPayload(b.raw[:0], values)
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingBoolBitpackRLE, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	min, max := boolMinMax(values)
	return newEncodedGranule(len(values), min, max, true, EncodingBoolBitpackRLE, selection), nil
}

func (r *GranuleReader) DecodeBool(g EncodedGranule) ([]bool, error) {
	values, err := r.DecodeBoolInto(r.bools[:0], g)
	if err != nil {
		return nil, err
	}
	r.bools = values
	return values, nil
}

func (r *GranuleReader) DecodeBoolInto(dst []bool, g EncodedGranule) ([]bool, error) {
	if g.Encoding != EncodingBoolBitpackRLE {
		return nil, fmt.Errorf("colgranule: bool decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	return decodeBoolPayload(dst, raw, g.Rows)
}

func (r *GranuleReader) CountTrueBool(g EncodedGranule) (int, error) {
	if g.Encoding != EncodingBoolBitpackRLE {
		return 0, fmt.Errorf("colgranule: bool count got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return 0, err
	}
	return countTrueBoolPayload(raw, g.Rows)
}

// BuildNullableInt64 returns a granule whose payload aliases builder-owned
// scratch until the next builder Build* or Reset call.
func (b *GranuleBuilder) BuildNullableInt64(values []int64, nulls []bool, defaults []bool, defaultValue int64) (EncodedGranule, error) {
	if len(values) == 0 {
		return EncodedGranule{}, errors.New("colgranule: empty granule")
	}
	if err := validateGranuleDecodeRows(len(values)); err != nil {
		return EncodedGranule{}, err
	}
	valueEncoding, err := nullableValueEncoding(b.cfg.Encoding)
	if err != nil {
		return EncodedGranule{}, err
	}
	if err := validateOptionalBoolRows("nulls", len(values), nulls); err != nil {
		return EncodedGranule{}, err
	}
	if err := validateOptionalBoolRows("defaults", len(values), defaults); err != nil {
		return EncodedGranule{}, err
	}

	b.values64 = b.values64[:0]
	nullCount := 0
	defaultCount := 0
	hasMinMax := false
	min, max := int64(0), int64(0)
	for i, v := range values {
		isNull := boolAt(nulls, i)
		isDefault := boolAt(defaults, i)
		if isNull && isDefault {
			return EncodedGranule{}, fmt.Errorf("colgranule: row %d is both null and default", i)
		}
		switch {
		case isNull:
			nullCount++
		case isDefault:
			defaultCount++
			min, max, hasMinMax = updateOptionalMinMax(min, max, hasMinMax, defaultValue)
		default:
			b.values64 = append(b.values64, v)
			min, max, hasMinMax = updateOptionalMinMax(min, max, hasMinMax, v)
		}
	}

	encodedValues, err := encodeInt64Payload(b.encoded[:0], b.values64, valueEncoding)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.encoded = encodedValues

	nullMaskLen := bitmapBytes(len(values))
	defaultMaskLen := bitmapBytes(len(values))
	raw := b.raw[:0]
	raw = append(raw, make([]byte, nullableInt64HeaderBytes)...)
	raw[0] = byte(valueEncoding)
	binary.LittleEndian.PutUint64(raw[1:9], uint64(defaultValue))
	binary.LittleEndian.PutUint32(raw[9:13], uint32(len(b.values64)))
	binary.LittleEndian.PutUint32(raw[13:17], uint32(nullMaskLen))
	binary.LittleEndian.PutUint32(raw[17:21], uint32(defaultMaskLen))
	raw = appendBitmap(raw, len(values), nulls)
	raw = appendBitmap(raw, len(values), defaults)
	raw = append(raw, encodedValues...)
	b.raw = raw

	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingNullableInt64, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	g := newEncodedGranule(len(values), min, max, hasMinMax, EncodingNullableInt64, selection)
	g.NullCount = nullCount
	g.DefaultCount = defaultCount
	return g, nil
}

func (r *GranuleReader) DecodeNullableInt64(g EncodedGranule) ([]int64, []bool, []bool, error) {
	values, nulls, defaults, err := r.DecodeNullableInt64Into(r.values[:0], r.nulls[:0], r.defaults[:0], g)
	if err != nil {
		return nil, nil, nil, err
	}
	r.values = values
	r.nulls = nulls
	r.defaults = defaults
	return values, nulls, defaults, nil
}

func (r *GranuleReader) DecodeNullableInt64Into(dst []int64, nulls []bool, defaults []bool, g EncodedGranule) ([]int64, []bool, []bool, error) {
	if g.Encoding != EncodingNullableInt64 {
		return nil, nil, nil, fmt.Errorf("colgranule: nullable int64 decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, nil, nil, err
	}
	header, err := parseNullableInt64Header(raw, g.Rows)
	if err != nil {
		return nil, nil, nil, err
	}
	storedGranule := EncodedGranule{
		Rows:        header.storedRows,
		HasMinMax:   false,
		Encoding:    header.valueEncoding,
		Compression: CompressionNone,
		RawBytes:    len(header.encodedValues),
		StoredBytes: len(header.encodedValues),
		PayloadRef:  PayloadRef{Kind: PayloadRefInline, Length: len(header.encodedValues)},
		Payload:     header.encodedValues,
	}
	stored, err := decodeInt64Payload(r.stored64[:0], header.encodedValues, storedGranule)
	if err != nil {
		return nil, nil, nil, err
	}
	r.stored64 = stored

	out := ensureInt64Len(dst, g.Rows)
	nullOut := ensureBoolLen(nulls, g.Rows)
	defaultOut := ensureBoolLen(defaults, g.Rows)
	storedIndex := 0
	for i := 0; i < g.Rows; i++ {
		isNull := bitmapBit(header.nullMask, i)
		isDefault := bitmapBit(header.defaultMask, i)
		if isNull && isDefault {
			return nil, nil, nil, fmt.Errorf("colgranule: nullable int64 row %d is both null and default", i)
		}
		nullOut[i] = isNull
		defaultOut[i] = isDefault
		switch {
		case isNull:
			out[i] = 0
		case isDefault:
			out[i] = header.defaultValue
		default:
			if storedIndex >= len(stored) {
				return nil, nil, nil, errors.New("colgranule: nullable int64 stored values underflow")
			}
			out[i] = stored[storedIndex]
			storedIndex++
		}
	}
	if storedIndex != len(stored) {
		return nil, nil, nil, errors.New("colgranule: nullable int64 stored values overflow")
	}
	return out, nullOut, defaultOut, nil
}

// BuildUint32Codes returns a granule whose payload aliases builder-owned
// scratch until the next builder Build* or Reset call.
func (b *GranuleBuilder) BuildUint32Codes(codes []uint32, cardinality uint32) (EncodedGranule, error) {
	if len(codes) == 0 {
		return EncodedGranule{}, errors.New("colgranule: empty granule")
	}
	if err := validateGranuleDecodeRows(len(codes)); err != nil {
		return EncodedGranule{}, err
	}
	minCode, maxCode := minMaxUint32(codes)
	if cardinality == 0 {
		if maxCode == ^uint32(0) {
			return EncodedGranule{}, errors.New("colgranule: inferred cardinality overflows uint32")
		}
		cardinality = maxCode + 1
	}
	if cardinality > maxCodeCardinality {
		return EncodedGranule{}, fmt.Errorf("colgranule: cardinality %d exceeds cap %d", cardinality, maxCodeCardinality)
	}
	if maxCode >= cardinality {
		return EncodedGranule{}, fmt.Errorf("colgranule: code %d outside cardinality %d", maxCode, cardinality)
	}
	raw := encodeUint32CodesPayload(b.raw[:0], codes, cardinality, maxCode)
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingLowCardinalityUint32, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(len(codes), int64(minCode), int64(maxCode), true, EncodingLowCardinalityUint32, selection), nil
}

func (r *GranuleReader) DecodeUint32Codes(g EncodedGranule) ([]uint32, error) {
	codes, err := r.DecodeUint32CodesInto(r.codes[:0], g)
	if err != nil {
		return nil, err
	}
	r.codes = codes
	return codes, nil
}

func (r *GranuleReader) DecodeUint32CodesInto(dst []uint32, g EncodedGranule) ([]uint32, error) {
	if g.Encoding != EncodingLowCardinalityUint32 {
		return nil, fmt.Errorf("colgranule: code decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	header, err := parseUint32CodesHeader(raw, g.Rows)
	if err != nil {
		return nil, err
	}
	out := ensureUint32Len(dst, g.Rows)
	for i := 0; i < g.Rows; i++ {
		code := readUint32Code(header.data, header.width, i)
		if code >= header.cardinality {
			return nil, fmt.Errorf("colgranule: code %d outside cardinality %d", code, header.cardinality)
		}
		out[i] = code
	}
	return out, nil
}

func (r *GranuleReader) CountUint32Codes(g EncodedGranule, counts []int) ([]int, error) {
	if g.Encoding != EncodingLowCardinalityUint32 {
		return nil, fmt.Errorf("colgranule: code count got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	header, err := parseUint32CodesHeader(raw, g.Rows)
	if err != nil {
		return nil, err
	}
	out := ensureIntLen(counts, int(header.cardinality))
	clear(out)
	for i := 0; i < g.Rows; i++ {
		code := readUint32Code(header.data, header.width, i)
		if code >= header.cardinality {
			return nil, fmt.Errorf("colgranule: code %d outside cardinality %d", code, header.cardinality)
		}
		out[code]++
	}
	return out, nil
}

func encodeBoolPayload(dst []byte, values []bool) []byte {
	bitpackLen := 1 + bitmapBytes(len(values))
	rleLen := boolRLEPayloadLen(values)
	if rleLen < bitpackLen {
		dst = appendBoolRLE(dst[:0], values)
		return dst
	}
	dst = append(dst[:0], boolPayloadBitpack)
	return appendBitmap(dst, len(values), values)
}

func decodeBoolPayload(dst []bool, raw []byte, rows int) ([]bool, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, errors.New("colgranule: missing bool payload mode")
	}
	switch raw[0] {
	case boolPayloadBitpack:
		mask := raw[1:]
		need, err := bitmapBytesChecked(rows)
		if err != nil {
			return nil, err
		}
		if len(mask) != need {
			return nil, fmt.Errorf("colgranule: bool bitpack bytes=%d want=%d", len(mask), need)
		}
		out := ensureBoolLen(dst, rows)
		for i := 0; i < rows; i++ {
			out[i] = bitmapBit(mask, i)
		}
		return out, nil
	case boolPayloadRLE:
		value, data, err := parseBoolRLEHeader(raw)
		if err != nil {
			return nil, err
		}
		if err := validateBoolRLERuns(data, rows); err != nil {
			return nil, err
		}
		out := ensureBoolLen(dst, rows)
		row := 0
		for len(data) > 0 && row < rows {
			run, n := binary.Uvarint(data)
			for i := 0; i < int(run); i++ {
				out[row+i] = value
			}
			row += int(run)
			value = !value
			data = data[n:]
		}
		return out, nil
	default:
		return nil, fmt.Errorf("colgranule: unsupported bool payload mode %d", raw[0])
	}
}

func countTrueBoolPayload(raw []byte, rows int) (int, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		return 0, errors.New("colgranule: missing bool payload mode")
	}
	switch raw[0] {
	case boolPayloadBitpack:
		mask := raw[1:]
		need, err := bitmapBytesChecked(rows)
		if err != nil {
			return 0, err
		}
		if len(mask) != need {
			return 0, fmt.Errorf("colgranule: bool bitpack bytes=%d want=%d", len(mask), need)
		}
		count := 0
		fullBytes := rows / 8
		for _, b := range mask[:fullBytes] {
			count += bits.OnesCount8(b)
		}
		if rows%8 != 0 {
			last := mask[fullBytes] & byte((1<<uint(rows%8))-1)
			count += bits.OnesCount8(last)
		}
		return count, nil
	case boolPayloadRLE:
		value, data, err := parseBoolRLEHeader(raw)
		if err != nil {
			return 0, err
		}
		row := 0
		count := 0
		for len(data) > 0 && row < rows {
			run, n := binary.Uvarint(data)
			if n <= 0 {
				return 0, errors.New("colgranule: malformed bool rle run")
			}
			if run == 0 || run > uint64(rows-row) {
				return 0, errors.New("colgranule: invalid bool rle run")
			}
			if value {
				count += int(run)
			}
			row += int(run)
			value = !value
			data = data[n:]
		}
		if row != rows {
			return 0, fmt.Errorf("colgranule: bool rle rows=%d want=%d", row, rows)
		}
		if len(data) != 0 {
			return 0, errors.New("colgranule: trailing bool rle bytes")
		}
		return count, nil
	default:
		return 0, fmt.Errorf("colgranule: unsupported bool payload mode %d", raw[0])
	}
}

func boolRLEPayloadLen(values []bool) int {
	if len(values) == 0 {
		return 0
	}
	var buf [binary.MaxVarintLen64]byte
	n := 2
	run := uint64(1)
	prev := values[0]
	for _, v := range values[1:] {
		if v == prev {
			run++
			continue
		}
		n += binary.PutUvarint(buf[:], run)
		run = 1
		prev = v
	}
	n += binary.PutUvarint(buf[:], run)
	return n
}

func appendBoolRLE(dst []byte, values []bool) []byte {
	dst = append(dst, boolPayloadRLE)
	if values[0] {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	var buf [binary.MaxVarintLen64]byte
	run := uint64(1)
	prev := values[0]
	for _, v := range values[1:] {
		if v == prev {
			run++
			continue
		}
		n := binary.PutUvarint(buf[:], run)
		dst = append(dst, buf[:n]...)
		run = 1
		prev = v
	}
	n := binary.PutUvarint(buf[:], run)
	return append(dst, buf[:n]...)
}

func encodeUint32CodesPayload(dst []byte, codes []uint32, cardinality uint32, maxCode uint32) []byte {
	width := uint32CodeWidth(maxCode)
	dst = append(dst[:0], width)
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(cardinality))
	dst = append(dst, buf[:n]...)
	switch width {
	case 1:
		for _, code := range codes {
			dst = append(dst, byte(code))
		}
	case 2:
		oldLen := len(dst)
		dst = append(dst, make([]byte, len(codes)*2)...)
		for i, code := range codes {
			binary.LittleEndian.PutUint16(dst[oldLen+i*2:], uint16(code))
		}
	case 4:
		oldLen := len(dst)
		dst = append(dst, make([]byte, len(codes)*4)...)
		for i, code := range codes {
			binary.LittleEndian.PutUint32(dst[oldLen+i*4:], code)
		}
	}
	return dst
}

type uint32CodesHeader struct {
	width       byte
	cardinality uint32
	data        []byte
}

func parseUint32CodesHeader(raw []byte, rows int) (uint32CodesHeader, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return uint32CodesHeader{}, err
	}
	if len(raw) < 2 {
		return uint32CodesHeader{}, errors.New("colgranule: truncated code header")
	}
	width := raw[0]
	if width != 1 && width != 2 && width != 4 {
		return uint32CodesHeader{}, fmt.Errorf("colgranule: unsupported code width %d", width)
	}
	cardinality64, n := binary.Uvarint(raw[1:])
	if n <= 0 {
		return uint32CodesHeader{}, errors.New("colgranule: malformed code cardinality")
	}
	if cardinality64 == 0 || cardinality64 > maxCodeCardinality {
		return uint32CodesHeader{}, fmt.Errorf("colgranule: invalid code cardinality %d", cardinality64)
	}
	data := raw[1+n:]
	if rows > int(^uint(0)>>1)/int(width) {
		return uint32CodesHeader{}, fmt.Errorf("colgranule: code rows=%d width=%d overflow", rows, width)
	}
	need := rows * int(width)
	if len(data) != need {
		return uint32CodesHeader{}, fmt.Errorf("colgranule: code data bytes=%d want=%d", len(data), need)
	}
	return uint32CodesHeader{width: width, cardinality: uint32(cardinality64), data: data}, nil
}

func readUint32Code(data []byte, width byte, row int) uint32 {
	switch width {
	case 1:
		return uint32(data[row])
	case 2:
		return uint32(binary.LittleEndian.Uint16(data[row*2:]))
	default:
		return binary.LittleEndian.Uint32(data[row*4:])
	}
}

type nullableInt64Header struct {
	valueEncoding Encoding
	defaultValue  int64
	storedRows    int
	nullMask      []byte
	defaultMask   []byte
	encodedValues []byte
}

func parseNullableInt64Header(raw []byte, rows int) (nullableInt64Header, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return nullableInt64Header{}, err
	}
	if len(raw) < nullableInt64HeaderBytes {
		return nullableInt64Header{}, errors.New("colgranule: truncated nullable int64 header")
	}
	valueEncoding := Encoding(raw[0])
	if _, err := nullableValueEncoding(valueEncoding); err != nil {
		return nullableInt64Header{}, err
	}
	defaultValue := int64(binary.LittleEndian.Uint64(raw[1:9]))
	storedRows := int(binary.LittleEndian.Uint32(raw[9:13]))
	if storedRows > rows {
		return nullableInt64Header{}, fmt.Errorf("colgranule: nullable stored rows=%d exceeds rows=%d", storedRows, rows)
	}
	nullMaskLen := int(binary.LittleEndian.Uint32(raw[13:17]))
	defaultMaskLen := int(binary.LittleEndian.Uint32(raw[17:21]))
	wantMaskLen, err := bitmapBytesChecked(rows)
	if err != nil {
		return nullableInt64Header{}, err
	}
	if nullMaskLen != wantMaskLen {
		return nullableInt64Header{}, fmt.Errorf("colgranule: null mask bytes=%d want=%d", nullMaskLen, wantMaskLen)
	}
	if defaultMaskLen != wantMaskLen {
		return nullableInt64Header{}, fmt.Errorf("colgranule: default mask bytes=%d want=%d", defaultMaskLen, wantMaskLen)
	}
	need := nullableInt64HeaderBytes + nullMaskLen + defaultMaskLen
	if need > len(raw) {
		return nullableInt64Header{}, errors.New("colgranule: truncated nullable int64 masks")
	}
	return nullableInt64Header{
		valueEncoding: valueEncoding,
		defaultValue:  defaultValue,
		storedRows:    storedRows,
		nullMask:      raw[nullableInt64HeaderBytes : nullableInt64HeaderBytes+nullMaskLen],
		defaultMask:   raw[nullableInt64HeaderBytes+nullMaskLen : need],
		encodedValues: raw[need:],
	}, nil
}

func nullableValueEncoding(encoding Encoding) (Encoding, error) {
	switch encoding {
	case EncodingRawInt64, EncodingDeltaVarint, EncodingDoubleDeltaVarint:
		return encoding, nil
	default:
		return 0, fmt.Errorf("colgranule: unsupported nullable int64 value encoding %d", encoding)
	}
}

func appendBitmap(dst []byte, rows int, values []bool) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, bitmapBytes(rows))...)
	for i := 0; i < rows; i++ {
		if boolAt(values, i) {
			dst[start+i/8] |= 1 << uint(i%8)
		}
	}
	return dst
}

func bitmapBytes(rows int) int {
	return (rows + 7) / 8
}

func bitmapBytesChecked(rows int) (int, error) {
	if err := validateGranuleDecodeRows(rows); err != nil {
		return 0, err
	}
	return bitmapBytes(rows), nil
}

func parseBoolRLEHeader(raw []byte) (bool, []byte, error) {
	if len(raw) < 2 {
		return false, nil, errors.New("colgranule: truncated bool rle header")
	}
	switch raw[1] {
	case 0:
		return false, raw[2:], nil
	case 1:
		return true, raw[2:], nil
	default:
		return false, nil, fmt.Errorf("colgranule: invalid bool rle start value %d", raw[1])
	}
}

func validateBoolRLERuns(data []byte, rows int) error {
	row := 0
	for len(data) > 0 && row < rows {
		run, n := binary.Uvarint(data)
		if n <= 0 {
			return errors.New("colgranule: malformed bool rle run")
		}
		if run == 0 || run > uint64(rows-row) {
			return errors.New("colgranule: invalid bool rle run")
		}
		row += int(run)
		data = data[n:]
	}
	if row != rows {
		return fmt.Errorf("colgranule: bool rle rows=%d want=%d", row, rows)
	}
	if len(data) != 0 {
		return errors.New("colgranule: trailing bool rle bytes")
	}
	return nil
}

func bitmapBit(mask []byte, row int) bool {
	return mask[row/8]&(1<<uint(row%8)) != 0
}

func boolAt(values []bool, i int) bool {
	return i >= 0 && i < len(values) && values[i]
}

func validateOptionalBoolRows(name string, rows int, values []bool) error {
	if len(values) != 0 && len(values) != rows {
		return fmt.Errorf("colgranule: %s rows=%d want 0 or %d", name, len(values), rows)
	}
	return nil
}

func updateOptionalMinMax(min int64, max int64, has bool, v int64) (int64, int64, bool) {
	if !has {
		return v, v, true
	}
	if v < min {
		min = v
	}
	if v > max {
		max = v
	}
	return min, max, true
}

func boolMinMax(values []bool) (int64, int64) {
	seenFalse := false
	seenTrue := false
	for _, v := range values {
		if v {
			seenTrue = true
		} else {
			seenFalse = true
		}
	}
	switch {
	case seenFalse && seenTrue:
		return 0, 1
	case seenTrue:
		return 1, 1
	default:
		return 0, 0
	}
}

func minMaxUint32(values []uint32) (uint32, uint32) {
	min, max := values[0], values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

func uint32CodeWidth(maxCode uint32) byte {
	switch {
	case maxCode <= 0xff:
		return 1
	case maxCode <= 0xffff:
		return 2
	default:
		return 4
	}
}

func ensureInt64Len(dst []int64, n int) []int64 {
	if cap(dst) < n {
		return make([]int64, n)
	}
	return dst[:n]
}

func ensureBoolLen(dst []bool, n int) []bool {
	if cap(dst) < n {
		return make([]bool, n)
	}
	return dst[:n]
}

func ensureUint32Len(dst []uint32, n int) []uint32 {
	if cap(dst) < n {
		return make([]uint32, n)
	}
	return dst[:n]
}

func ensureIntLen(dst []int, n int) []int {
	if cap(dst) < n {
		return make([]int, n)
	}
	return dst[:n]
}
