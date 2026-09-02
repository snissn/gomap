package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// BSON index key codec v2 is staged beside the current homogeneous
// IndexValueType codec. Issue #4062 owns adoption by collection indexes; this
// file defines only the versioned scalar-component and entry-suffix contract.
const (
	bsonIndexKeyComponentV2AscendingMarker  byte = 0xb2
	bsonIndexKeyComponentV2DescendingMarker byte = ^bsonIndexKeyComponentV2AscendingMarker

	bsonIndexKeyTagMissingV2   byte = 0x08
	bsonIndexKeyTagNullV2      byte = 0x10
	bsonIndexKeyTagNumberV2    byte = 0x20
	bsonIndexKeyTagStringV2    byte = 0x30
	bsonIndexKeyTagObjectIDV2  byte = 0x70
	bsonIndexKeyTagBoolV2      byte = 0x80
	bsonIndexKeyTagDateTimeV2  byte = 0x90
	bsonIndexKeyTagTimestampV2 byte = 0xa0

	bsonIndexNumberNaNV2              byte = 0x08
	bsonIndexNumberNegativeInfinityV2 byte = 0x10
	bsonIndexNumberNegativeFiniteV2   byte = 0x20
	bsonIndexNumberZeroV2             byte = 0x30
	bsonIndexNumberPositiveFiniteV2   byte = 0x40
	bsonIndexNumberPositiveInfinityV2 byte = 0x50

	bsonIndexKeyComponentV2MaxBytes      = 1 << 20
	bsonIndexKeyNumericV2MaxDigits       = 2048
	bsonIndexKeyDocumentIDSuffixMarkerV2 = 0x01
	bsonIndexKeyDecimalExponentWorkBound = 10000
)

var (
	errBSONIndexKeyV2Malformed   = errors.New("collections: malformed BSON index key component v2")
	errBSONIndexKeyV2Unsupported = errors.New("collections: unsupported BSON scalar for index key component v2")
	errBSONIndexKeyV2TooLarge    = errors.New("collections: BSON index key component v2 is too large")
)

type bsonIndexKeyKindV2 uint8

const (
	bsonIndexKeyKindInvalidV2 bsonIndexKeyKindV2 = iota
	bsonIndexKeyKindMissingV2
	bsonIndexKeyKindNullV2
	bsonIndexKeyKindNumberV2
	bsonIndexKeyKindStringV2
	bsonIndexKeyKindObjectIDV2
	bsonIndexKeyKindBoolV2
	bsonIndexKeyKindDateTimeV2
	bsonIndexKeyKindTimestampV2
)

type bsonIndexKeyDecodedV2 struct {
	Descending bool
	Kind       bsonIndexKeyKindV2
	Canonical  string
}

type bsonIndexKeyCanonicalNumberV2 struct {
	class       byte
	coefficient *big.Int
	exponent    int
}

func encodeBSONIndexKeyComponentV2(value bson.RawValue) ([]byte, error) {
	out, component, err := appendBSONIndexKeyComponentV2(nil, value)
	if err != nil {
		return nil, err
	}
	return out[:len(component):len(component)], nil
}

// EncodeBSONIndexKeyComponentV2 exposes the frozen v2 scalar key encoding to
// the Mongo gateway planner without exposing its internal decoder surface.
func EncodeBSONIndexKeyComponentV2(value bson.RawValue) ([]byte, error) {
	return encodeBSONIndexKeyComponentV2(value)
}

// appendBSONIndexKeyComponentV2 appends one ascending, self-delimiting scalar
// component. Concatenating returned components is safe for compound indexes.
// The document ID is deliberately appended by bsonIndexEntryKeyV2 afterwards.
func appendBSONIndexKeyComponentV2(dst []byte, value bson.RawValue) ([]byte, []byte, error) {
	start := len(dst)
	if start > math.MaxInt-bsonIndexKeyComponentV2MaxBytes {
		return dst, nil, errBSONIndexKeyV2TooLarge
	}
	dst = append(dst, bsonIndexKeyComponentV2AscendingMarker)

	if value.Type == 0 {
		if len(value.Value) != 0 {
			return dst[:start], nil, fmt.Errorf("%w: missing payload length %d, want 0", errBSONIndexKeyV2Malformed, len(value.Value))
		}
		dst = append(dst, bsonIndexKeyTagMissingV2)
		return dst, dst[start:len(dst):len(dst)], nil
	}

	switch value.Type {
	case bson.TypeNull:
		if len(value.Value) != 0 {
			return dst[:start], nil, fmt.Errorf("%w: null payload length %d, want 0", errBSONIndexKeyV2Malformed, len(value.Value))
		}
		dst = append(dst, bsonIndexKeyTagNullV2)
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		dst = append(dst, bsonIndexKeyTagNumberV2)
		var err error
		dst, err = appendCanonicalBSONIndexNumberV2(dst, value)
		if err != nil {
			return dst[:start], nil, err
		}
	case bson.TypeString:
		text, ok := bsonIndexStringBytesV2(value)
		if !ok || !utf8.Valid(text) {
			return dst[:start], nil, fmt.Errorf("%w: invalid UTF-8 BSON string", errBSONIndexKeyV2Malformed)
		}
		if len(text) > bsonIndexKeyComponentV2MaxBytes-4 {
			return dst[:start], nil, fmt.Errorf("%w: string length %d", errBSONIndexKeyV2TooLarge, len(text))
		}
		escapedLength := len(text)
		for _, value := range text {
			if value == 0 {
				escapedLength++
			}
		}
		if escapedLength > bsonIndexKeyComponentV2MaxBytes-4 {
			return dst[:start], nil, fmt.Errorf("%w: escaped string length %d", errBSONIndexKeyV2TooLarge, escapedLength)
		}
		dst = append(dst, bsonIndexKeyTagStringV2)
		for _, value := range text {
			if value == 0 {
				dst = append(dst, 0, 0xff)
			} else {
				dst = append(dst, value)
			}
		}
		dst = append(dst, 0, 0)
	case bson.TypeObjectID:
		if len(value.Value) != 12 {
			return dst[:start], nil, fmt.Errorf("%w: ObjectID payload length %d, want 12", errBSONIndexKeyV2Malformed, len(value.Value))
		}
		objectID, ok := value.ObjectIDOK()
		if !ok {
			return dst[:start], nil, fmt.Errorf("%w: invalid ObjectID", errBSONIndexKeyV2Malformed)
		}
		dst = append(dst, bsonIndexKeyTagObjectIDV2)
		dst = append(dst, objectID[:]...)
	case bson.TypeBoolean:
		if len(value.Value) != 1 || value.Value[0] > 1 {
			return dst[:start], nil, fmt.Errorf("%w: invalid bool payload %x", errBSONIndexKeyV2Malformed, value.Value)
		}
		boolean, ok := value.BooleanOK()
		if !ok {
			return dst[:start], nil, fmt.Errorf("%w: invalid bool", errBSONIndexKeyV2Malformed)
		}
		dst = append(dst, bsonIndexKeyTagBoolV2)
		if boolean {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
	case bson.TypeDateTime:
		if len(value.Value) != 8 {
			return dst[:start], nil, fmt.Errorf("%w: datetime payload length %d, want 8", errBSONIndexKeyV2Malformed, len(value.Value))
		}
		milliseconds, ok := value.DateTimeOK()
		if !ok {
			return dst[:start], nil, fmt.Errorf("%w: invalid datetime", errBSONIndexKeyV2Malformed)
		}
		dst = append(dst, bsonIndexKeyTagDateTimeV2)
		dst = binary.BigEndian.AppendUint64(dst, uint64(milliseconds)^uint64(1<<63))
	case bson.TypeTimestamp:
		if len(value.Value) != 8 {
			return dst[:start], nil, fmt.Errorf("%w: timestamp payload length %d, want 8", errBSONIndexKeyV2Malformed, len(value.Value))
		}
		timestamp, ordinal, ok := value.TimestampOK()
		if !ok {
			return dst[:start], nil, fmt.Errorf("%w: invalid timestamp", errBSONIndexKeyV2Malformed)
		}
		dst = append(dst, bsonIndexKeyTagTimestampV2)
		dst = binary.BigEndian.AppendUint32(dst, timestamp)
		dst = binary.BigEndian.AppendUint32(dst, ordinal)
	default:
		return dst[:start], nil, fmt.Errorf("%w: %s", errBSONIndexKeyV2Unsupported, value.Type)
	}

	component := dst[start:len(dst):len(dst)]
	if len(component) > bsonIndexKeyComponentV2MaxBytes {
		return dst[:start], nil, fmt.Errorf("%w: encoded length %d", errBSONIndexKeyV2TooLarge, len(component))
	}
	return dst, component, nil
}

func bsonIndexStringBytesV2(value bson.RawValue) ([]byte, bool) {
	if value.Type != bson.TypeString || len(value.Value) < 5 {
		return nil, false
	}
	length := int64(int32(binary.LittleEndian.Uint32(value.Value[:4])))
	if length < 1 || length != int64(len(value.Value)-4) || value.Value[len(value.Value)-1] != 0 {
		return nil, false
	}
	return value.Value[4 : len(value.Value)-1], true
}

func appendCanonicalBSONIndexNumberV2(dst []byte, value bson.RawValue) ([]byte, error) {
	switch value.Type {
	case bson.TypeInt32:
		if len(value.Value) != 4 {
			return dst, fmt.Errorf("%w: int32 payload length %d, want 4", errBSONIndexKeyV2Malformed, len(value.Value))
		}
		integer, ok := value.Int32OK()
		if !ok {
			return dst, fmt.Errorf("%w: invalid int32", errBSONIndexKeyV2Malformed)
		}
		return appendBSONIndexIntegerV2(dst, int64(integer))
	case bson.TypeInt64:
		if len(value.Value) != 8 {
			return dst, fmt.Errorf("%w: int64 payload length %d, want 8", errBSONIndexKeyV2Malformed, len(value.Value))
		}
		integer, ok := value.Int64OK()
		if !ok {
			return dst, fmt.Errorf("%w: invalid int64", errBSONIndexKeyV2Malformed)
		}
		return appendBSONIndexIntegerV2(dst, integer)
	case bson.TypeDouble:
		if len(value.Value) != 8 {
			return dst, fmt.Errorf("%w: double payload length %d, want 8", errBSONIndexKeyV2Malformed, len(value.Value))
		}
		number, ok := value.DoubleOK()
		if !ok {
			return dst, fmt.Errorf("%w: invalid double", errBSONIndexKeyV2Malformed)
		}
		switch {
		case math.IsInf(number, -1):
			return append(dst, bsonIndexNumberNegativeInfinityV2), nil
		case math.IsInf(number, 1):
			return append(dst, bsonIndexNumberPositiveInfinityV2), nil
		case math.IsNaN(number):
			return append(dst, bsonIndexNumberNaNV2), nil
		}
		if out, ok, err := appendFastFiniteBSONIndexFloat64V2(dst, number); ok || err != nil {
			return out, err
		}
	case bson.TypeDecimal128:
		if len(value.Value) != 16 {
			return dst, fmt.Errorf("%w: Decimal128 payload length %d, want 16", errBSONIndexKeyV2Malformed, len(value.Value))
		}
	}

	number, err := canonicalBSONIndexNumberV2(value)
	if err != nil {
		return dst, err
	}
	if number.class != bsonIndexNumberNegativeFiniteV2 && number.class != bsonIndexNumberPositiveFiniteV2 {
		return append(dst, number.class), nil
	}
	if number.coefficient == nil {
		return dst, fmt.Errorf("%w: finite number without coefficient", errBSONIndexKeyV2Malformed)
	}
	return appendCanonicalBSONIndexFiniteV2(dst, number.class, number.coefficient.Text(10), number.exponent)
}

func appendBSONIndexIntegerV2(dst []byte, integer int64) ([]byte, error) {
	negative := integer < 0
	magnitude := uint64(integer)
	if negative {
		magnitude = uint64(-(integer + 1)) + 1
	}
	return appendBSONIndexUintV2(dst, magnitude, negative, 0)
}

func appendFastFiniteBSONIndexFloat64V2(dst []byte, number float64) ([]byte, bool, error) {
	bits := math.Float64bits(number)
	negative := bits>>63 != 0
	exponentBits := int((bits >> 52) & 0x7ff)
	mantissa := bits & ((uint64(1) << 52) - 1)
	if exponentBits == 0 && mantissa == 0 {
		return append(dst, bsonIndexNumberZeroV2), true, nil
	}

	var binaryExponent int
	if exponentBits == 0 {
		binaryExponent = -1022 - 52
	} else {
		mantissa |= uint64(1) << 52
		binaryExponent = exponentBits - 1023 - 52
	}
	for binaryExponent < 0 && mantissa&1 == 0 {
		mantissa >>= 1
		binaryExponent++
	}

	var coefficient uint64
	decimalExponent := 0
	if binaryExponent >= 0 {
		if binaryExponent >= 64 || mantissa > math.MaxUint64>>uint(binaryExponent) {
			return dst, false, nil
		}
		coefficient = mantissa << uint(binaryExponent)
	} else {
		power := -binaryExponent
		scale := uint64(1)
		for i := 0; i < power; i++ {
			if scale > math.MaxUint64/5 {
				return dst, false, nil
			}
			scale *= 5
		}
		if mantissa > math.MaxUint64/scale {
			return dst, false, nil
		}
		coefficient = mantissa * scale
		decimalExponent = -power
	}
	out, err := appendBSONIndexUintV2(dst, coefficient, negative, decimalExponent)
	return out, true, err
}

func appendBSONIndexUintV2(dst []byte, magnitude uint64, negative bool, exponent int) ([]byte, error) {
	if magnitude == 0 {
		return append(dst, bsonIndexNumberZeroV2), nil
	}
	var scratch [20]byte
	digits := strconv.AppendUint(scratch[:0], magnitude, 10)
	for len(digits) > 1 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
		exponent++
	}
	class := bsonIndexNumberPositiveFiniteV2
	if negative {
		class = bsonIndexNumberNegativeFiniteV2
	}
	return appendCanonicalBSONIndexFiniteV2(dst, class, digits, exponent)
}

type bsonIndexDigitsV2 interface {
	~string | ~[]byte
}

func appendCanonicalBSONIndexFiniteV2[T bsonIndexDigitsV2](dst []byte, class byte, digits T, exponent int) ([]byte, error) {
	dst = append(dst, class)
	if class != bsonIndexNumberNegativeFiniteV2 && class != bsonIndexNumberPositiveFiniteV2 {
		return dst, nil
	}
	if len(digits) == 0 || len(digits) > bsonIndexKeyNumericV2MaxDigits {
		return dst, errBSONIndexKeyV2TooLarge
	}
	adjustedExponent := int64(exponent) + int64(len(digits)) - 1
	if exponent < -bsonIndexKeyDecimalExponentWorkBound || exponent > bsonIndexKeyDecimalExponentWorkBound || adjustedExponent < math.MinInt16 || adjustedExponent > math.MaxInt16 {
		return dst, fmt.Errorf("%w: decimal exponent %d adjusted %d", errBSONIndexKeyV2TooLarge, exponent, adjustedExponent)
	}
	payloadStart := len(dst)
	dst = binary.BigEndian.AppendUint16(dst, uint16(int16(adjustedExponent))^0x8000)
	var err error
	dst, err = appendPackedBSONIndexDigitsV2(dst, digits)
	if err != nil {
		return dst, err
	}
	if class == bsonIndexNumberNegativeFiniteV2 {
		complementBSONIndexBytesV2(dst[payloadStart:])
	}
	return dst, nil
}

func canonicalBSONIndexNumberV2(value bson.RawValue) (bsonIndexKeyCanonicalNumberV2, error) {
	switch value.Type {
	case bson.TypeInt32:
		integer, ok := value.Int32OK()
		if !ok {
			return bsonIndexKeyCanonicalNumberV2{}, fmt.Errorf("%w: invalid int32", errBSONIndexKeyV2Malformed)
		}
		return normalizeBSONIndexDecimalV2(big.NewInt(int64(integer)), 0)
	case bson.TypeInt64:
		integer, ok := value.Int64OK()
		if !ok {
			return bsonIndexKeyCanonicalNumberV2{}, fmt.Errorf("%w: invalid int64", errBSONIndexKeyV2Malformed)
		}
		return normalizeBSONIndexDecimalV2(big.NewInt(integer), 0)
	case bson.TypeDouble:
		number, ok := value.DoubleOK()
		if !ok {
			return bsonIndexKeyCanonicalNumberV2{}, fmt.Errorf("%w: invalid double", errBSONIndexKeyV2Malformed)
		}
		switch {
		case math.IsInf(number, -1):
			return bsonIndexKeyCanonicalNumberV2{class: bsonIndexNumberNegativeInfinityV2}, nil
		case math.IsInf(number, 1):
			return bsonIndexKeyCanonicalNumberV2{class: bsonIndexNumberPositiveInfinityV2}, nil
		case math.IsNaN(number):
			return bsonIndexKeyCanonicalNumberV2{class: bsonIndexNumberNaNV2}, nil
		default:
			return canonicalFiniteFloat64BSONIndexV2(number)
		}
	case bson.TypeDecimal128:
		decimal, ok := value.Decimal128OK()
		if !ok {
			return bsonIndexKeyCanonicalNumberV2{}, fmt.Errorf("%w: invalid Decimal128", errBSONIndexKeyV2Malformed)
		}
		if decimal.IsNaN() {
			return bsonIndexKeyCanonicalNumberV2{class: bsonIndexNumberNaNV2}, nil
		}
		switch decimal.IsInf() {
		case -1:
			return bsonIndexKeyCanonicalNumberV2{class: bsonIndexNumberNegativeInfinityV2}, nil
		case 1:
			return bsonIndexKeyCanonicalNumberV2{class: bsonIndexNumberPositiveInfinityV2}, nil
		}
		coefficient, exponent, err := decimal.BigInt()
		if err != nil {
			return bsonIndexKeyCanonicalNumberV2{}, fmt.Errorf("%w: Decimal128: %v", errBSONIndexKeyV2Malformed, err)
		}
		return normalizeBSONIndexDecimalV2(coefficient, exponent)
	default:
		return bsonIndexKeyCanonicalNumberV2{}, fmt.Errorf("%w: numeric type %s", errBSONIndexKeyV2Unsupported, value.Type)
	}
}

func canonicalFiniteFloat64BSONIndexV2(value float64) (bsonIndexKeyCanonicalNumberV2, error) {
	bits := math.Float64bits(value)
	negative := bits>>63 != 0
	exponentBits := int((bits >> 52) & 0x7ff)
	fraction := bits & ((uint64(1) << 52) - 1)
	if exponentBits == 0 && fraction == 0 {
		return bsonIndexKeyCanonicalNumberV2{class: bsonIndexNumberZeroV2, coefficient: new(big.Int)}, nil
	}

	mantissa := fraction
	var binaryExponent int
	if exponentBits == 0 {
		binaryExponent = -1022 - 52
	} else {
		mantissa |= uint64(1) << 52
		binaryExponent = exponentBits - 1023 - 52
	}
	coefficient := new(big.Int).SetUint64(mantissa)
	decimalExponent := 0
	if binaryExponent >= 0 {
		coefficient.Lsh(coefficient, uint(binaryExponent))
	} else {
		power := -binaryExponent
		if power > bsonIndexKeyDecimalExponentWorkBound {
			return bsonIndexKeyCanonicalNumberV2{}, errBSONIndexKeyV2TooLarge
		}
		coefficient.Mul(coefficient, new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(power)), nil))
		decimalExponent = -power
	}
	if negative {
		coefficient.Neg(coefficient)
	}
	return normalizeBSONIndexDecimalV2(coefficient, decimalExponent)
}

func normalizeBSONIndexDecimalV2(coefficient *big.Int, exponent int) (bsonIndexKeyCanonicalNumberV2, error) {
	if coefficient == nil {
		return bsonIndexKeyCanonicalNumberV2{}, fmt.Errorf("%w: nil coefficient", errBSONIndexKeyV2Malformed)
	}
	if exponent < -bsonIndexKeyDecimalExponentWorkBound || exponent > bsonIndexKeyDecimalExponentWorkBound {
		return bsonIndexKeyCanonicalNumberV2{}, errBSONIndexKeyV2TooLarge
	}
	if coefficient.Sign() == 0 {
		return bsonIndexKeyCanonicalNumberV2{class: bsonIndexNumberZeroV2, coefficient: new(big.Int)}, nil
	}

	class := bsonIndexNumberPositiveFiniteV2
	magnitude := new(big.Int).Set(coefficient)
	if magnitude.Sign() < 0 {
		class = bsonIndexNumberNegativeFiniteV2
		magnitude.Abs(magnitude)
	}

	ten := big.NewInt(10)
	quotient := new(big.Int)
	remainder := new(big.Int)
	for {
		quotient.QuoRem(magnitude, ten, remainder)
		if remainder.Sign() != 0 {
			break
		}
		magnitude.Set(quotient)
		exponent++
		if exponent > bsonIndexKeyDecimalExponentWorkBound {
			return bsonIndexKeyCanonicalNumberV2{}, errBSONIndexKeyV2TooLarge
		}
	}
	if len(magnitude.Text(10)) > bsonIndexKeyNumericV2MaxDigits {
		return bsonIndexKeyCanonicalNumberV2{}, errBSONIndexKeyV2TooLarge
	}
	return bsonIndexKeyCanonicalNumberV2{class: class, coefficient: magnitude, exponent: exponent}, nil
}

func appendPackedBSONIndexDigitsV2[T bsonIndexDigitsV2](dst []byte, digits T) ([]byte, error) {
	if len(digits) == 0 || len(digits) > bsonIndexKeyNumericV2MaxDigits || digits[0] == '0' || digits[len(digits)-1] == '0' {
		return dst, fmt.Errorf("%w: non-canonical decimal coefficient", errBSONIndexKeyV2Malformed)
	}
	for index := 0; index < len(digits); index += 2 {
		if digits[index] < '0' || digits[index] > '9' {
			return dst, fmt.Errorf("%w: invalid decimal digit", errBSONIndexKeyV2Malformed)
		}
		high := digits[index] - '0' + 1
		low := byte(0)
		if index+1 < len(digits) {
			if digits[index+1] < '0' || digits[index+1] > '9' {
				return dst, fmt.Errorf("%w: invalid decimal digit", errBSONIndexKeyV2Malformed)
			}
			low = digits[index+1] - '0' + 1
		}
		dst = append(dst, high<<4|low)
	}
	if len(digits)%2 == 0 {
		dst = append(dst, 0)
	}
	return dst, nil
}

func decodeBSONIndexKeyComponentV2(encoded []byte) (bsonIndexKeyDecodedV2, int, error) {
	if len(encoded) < 2 {
		return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: truncated header", errBSONIndexKeyV2Malformed)
	}
	if len(encoded) > bsonIndexKeyComponentV2MaxBytes && encoded[0] != bsonIndexKeyComponentV2AscendingMarker && encoded[0] != bsonIndexKeyComponentV2DescendingMarker {
		return bsonIndexKeyDecodedV2{}, 0, errBSONIndexKeyV2TooLarge
	}
	descending := encoded[0] == bsonIndexKeyComponentV2DescendingMarker
	if !descending && encoded[0] != bsonIndexKeyComponentV2AscendingMarker {
		return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: marker 0x%02x", errBSONIndexKeyV2Malformed, encoded[0])
	}
	logical := func(index int) byte {
		if descending {
			return ^encoded[index]
		}
		return encoded[index]
	}
	if logical(0) != bsonIndexKeyComponentV2AscendingMarker {
		return bsonIndexKeyDecodedV2{}, 0, errBSONIndexKeyV2Malformed
	}

	decoded := bsonIndexKeyDecodedV2{Descending: descending}
	tag := logical(1)
	switch tag {
	case bsonIndexKeyTagMissingV2:
		decoded.Kind = bsonIndexKeyKindMissingV2
		decoded.Canonical = "missing"
		return decoded, 2, nil
	case bsonIndexKeyTagNullV2:
		decoded.Kind = bsonIndexKeyKindNullV2
		decoded.Canonical = "null"
		return decoded, 2, nil
	case bsonIndexKeyTagNumberV2:
		decoded.Kind = bsonIndexKeyKindNumberV2
		canonical, n, err := decodeBSONIndexNumberV2(encoded, logical, 2)
		if err != nil {
			return bsonIndexKeyDecodedV2{}, 0, err
		}
		decoded.Canonical = canonical
		return decoded, n, nil
	case bsonIndexKeyTagStringV2:
		decoded.Kind = bsonIndexKeyKindStringV2
		payload := make([]byte, 0, 32)
		for index := 2; index < len(encoded) && index < bsonIndexKeyComponentV2MaxBytes; {
			value := logical(index)
			index++
			if value != 0 {
				payload = append(payload, value)
				continue
			}
			if index >= len(encoded) {
				return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: truncated string escape", errBSONIndexKeyV2Malformed)
			}
			if index >= bsonIndexKeyComponentV2MaxBytes {
				return bsonIndexKeyDecodedV2{}, 0, errBSONIndexKeyV2TooLarge
			}
			next := logical(index)
			index++
			switch next {
			case 0:
				if !utf8.Valid(payload) {
					return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: invalid UTF-8 string", errBSONIndexKeyV2Malformed)
				}
				decoded.Canonical = string(payload)
				return decoded, index, nil
			case 0xff:
				payload = append(payload, 0)
			default:
				return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: invalid string escape", errBSONIndexKeyV2Malformed)
			}
		}
		return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: unterminated string", errBSONIndexKeyV2Malformed)
	case bsonIndexKeyTagObjectIDV2:
		if len(encoded) < 14 {
			return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: truncated ObjectID", errBSONIndexKeyV2Malformed)
		}
		var objectID bson.ObjectID
		for i := range objectID {
			objectID[i] = logical(2 + i)
		}
		decoded.Kind = bsonIndexKeyKindObjectIDV2
		decoded.Canonical = objectID.Hex()
		return decoded, 14, nil
	case bsonIndexKeyTagBoolV2:
		if len(encoded) < 3 {
			return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: truncated bool", errBSONIndexKeyV2Malformed)
		}
		value := logical(2)
		if value > 1 {
			return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: invalid bool", errBSONIndexKeyV2Malformed)
		}
		decoded.Kind = bsonIndexKeyKindBoolV2
		decoded.Canonical = strconv.FormatBool(value == 1)
		return decoded, 3, nil
	case bsonIndexKeyTagDateTimeV2:
		if len(encoded) < 10 {
			return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: truncated datetime", errBSONIndexKeyV2Malformed)
		}
		var raw [8]byte
		for i := range raw {
			raw[i] = logical(2 + i)
		}
		milliseconds := int64(binary.BigEndian.Uint64(raw[:]) ^ uint64(1<<63))
		decoded.Kind = bsonIndexKeyKindDateTimeV2
		decoded.Canonical = strconv.FormatInt(milliseconds, 10)
		return decoded, 10, nil
	case bsonIndexKeyTagTimestampV2:
		if len(encoded) < 10 {
			return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: truncated timestamp", errBSONIndexKeyV2Malformed)
		}
		var raw [8]byte
		for i := range raw {
			raw[i] = logical(2 + i)
		}
		timestamp := binary.BigEndian.Uint32(raw[:4])
		ordinal := binary.BigEndian.Uint32(raw[4:])
		decoded.Kind = bsonIndexKeyKindTimestampV2
		decoded.Canonical = fmt.Sprintf("%d:%d", timestamp, ordinal)
		return decoded, 10, nil
	default:
		return bsonIndexKeyDecodedV2{}, 0, fmt.Errorf("%w: unknown tag 0x%02x", errBSONIndexKeyV2Malformed, tag)
	}
}

func decodeBSONIndexNumberV2(encoded []byte, logical func(int) byte, classIndex int) (string, int, error) {
	if len(encoded) <= classIndex {
		return "", 0, fmt.Errorf("%w: truncated number", errBSONIndexKeyV2Malformed)
	}
	class := logical(classIndex)
	switch class {
	case bsonIndexNumberNegativeInfinityV2:
		return "-Infinity", classIndex + 1, nil
	case bsonIndexNumberZeroV2:
		return "0", classIndex + 1, nil
	case bsonIndexNumberPositiveInfinityV2:
		return "Infinity", classIndex + 1, nil
	case bsonIndexNumberNaNV2:
		return "NaN", classIndex + 1, nil
	case bsonIndexNumberNegativeFiniteV2, bsonIndexNumberPositiveFiniteV2:
		// Continue below.
	default:
		return "", 0, fmt.Errorf("%w: numeric class 0x%02x", errBSONIndexKeyV2Malformed, class)
	}
	if len(encoded) < classIndex+4 {
		return "", 0, fmt.Errorf("%w: truncated finite number", errBSONIndexKeyV2Malformed)
	}
	negative := class == bsonIndexNumberNegativeFiniteV2
	magnitudeByte := func(index int) byte {
		value := logical(index)
		if negative {
			return ^value
		}
		return value
	}
	adjustedExponent := int16(binary.BigEndian.Uint16([]byte{magnitudeByte(classIndex + 1), magnitudeByte(classIndex + 2)}) ^ 0x8000)

	digits := make([]byte, 0, 32)
	for index := classIndex + 3; index < len(encoded) && index < bsonIndexKeyComponentV2MaxBytes; index++ {
		packed := magnitudeByte(index)
		for nibbleIndex, nibble := range []byte{packed >> 4, packed & 0x0f} {
			if nibble == 0 {
				if nibbleIndex == 0 && packed&0x0f != 0 {
					return "", 0, fmt.Errorf("%w: nonzero padding after numeric terminator", errBSONIndexKeyV2Malformed)
				}
				if len(digits) == 0 || digits[0] == '0' || digits[len(digits)-1] == '0' {
					return "", 0, fmt.Errorf("%w: non-canonical numeric coefficient", errBSONIndexKeyV2Malformed)
				}
				exponent := int(adjustedExponent) - len(digits) + 1
				if exponent < -bsonIndexKeyDecimalExponentWorkBound || exponent > bsonIndexKeyDecimalExponentWorkBound {
					return "", 0, fmt.Errorf("%w: decimal exponent %d", errBSONIndexKeyV2TooLarge, exponent)
				}
				prefix := ""
				if negative {
					prefix = "-"
				}
				return prefix + string(digits) + "e" + strconv.Itoa(exponent), index + 1, nil
			}
			if nibble > 10 {
				return "", 0, fmt.Errorf("%w: invalid packed decimal digit", errBSONIndexKeyV2Malformed)
			}
			digits = append(digits, '0'+nibble-1)
			if len(digits) > bsonIndexKeyNumericV2MaxDigits {
				return "", 0, errBSONIndexKeyV2TooLarge
			}
		}
	}
	return "", 0, fmt.Errorf("%w: unterminated finite number", errBSONIndexKeyV2Malformed)
}

func bsonIndexKeyComponentV2Length(encoded []byte) (int, error) {
	if len(encoded) < 2 {
		return 0, fmt.Errorf("%w: truncated header", errBSONIndexKeyV2Malformed)
	}
	descending := encoded[0] == bsonIndexKeyComponentV2DescendingMarker
	logical := func(index int) byte {
		if descending {
			return ^encoded[index]
		}
		return encoded[index]
	}
	if logical(0) != bsonIndexKeyComponentV2AscendingMarker {
		return 0, errBSONIndexKeyV2Malformed
	}

	switch tag := logical(1); tag {
	case bsonIndexKeyTagMissingV2, bsonIndexKeyTagNullV2:
		return 2, nil
	case bsonIndexKeyTagNumberV2:
		return bsonIndexNumberV2Length(encoded, logical, 2)
	case bsonIndexKeyTagStringV2:
		return bsonIndexStringV2Length(encoded, logical, descending)
	case bsonIndexKeyTagObjectIDV2:
		if len(encoded) < 14 {
			return 0, fmt.Errorf("%w: truncated ObjectID", errBSONIndexKeyV2Malformed)
		}
		return 14, nil
	case bsonIndexKeyTagBoolV2:
		if len(encoded) < 3 {
			return 0, fmt.Errorf("%w: truncated bool", errBSONIndexKeyV2Malformed)
		}
		if logical(2) > 1 {
			return 0, fmt.Errorf("%w: invalid bool", errBSONIndexKeyV2Malformed)
		}
		return 3, nil
	case bsonIndexKeyTagDateTimeV2:
		if len(encoded) < 10 {
			return 0, fmt.Errorf("%w: truncated datetime", errBSONIndexKeyV2Malformed)
		}
		return 10, nil
	case bsonIndexKeyTagTimestampV2:
		if len(encoded) < 10 {
			return 0, fmt.Errorf("%w: truncated timestamp", errBSONIndexKeyV2Malformed)
		}
		return 10, nil
	default:
		return 0, fmt.Errorf("%w: unknown tag 0x%02x", errBSONIndexKeyV2Malformed, tag)
	}
}

func bsonIndexStringV2Length(encoded []byte, logical func(int) byte, descending bool) (int, error) {
	if !descending {
		segmentStart := 2
		for index := 2; index < len(encoded) && index < bsonIndexKeyComponentV2MaxBytes; {
			if encoded[index] != 0 {
				index++
				continue
			}
			if !utf8.Valid(encoded[segmentStart:index]) {
				return 0, fmt.Errorf("%w: invalid UTF-8 string", errBSONIndexKeyV2Malformed)
			}
			index++
			if index >= len(encoded) {
				return 0, fmt.Errorf("%w: truncated string escape", errBSONIndexKeyV2Malformed)
			}
			if index >= bsonIndexKeyComponentV2MaxBytes {
				return 0, errBSONIndexKeyV2TooLarge
			}
			switch encoded[index] {
			case 0:
				return index + 1, nil
			case 0xff:
				index++
				segmentStart = index
			default:
				return 0, fmt.Errorf("%w: invalid string escape", errBSONIndexKeyV2Malformed)
			}
		}
		return 0, fmt.Errorf("%w: unterminated string", errBSONIndexKeyV2Malformed)
	}

	continuations := 0
	continuationMin := byte(0x80)
	continuationMax := byte(0xbf)
	appendPayloadByte := func(value byte) error {
		if continuations > 0 {
			if value < continuationMin || value > continuationMax {
				return fmt.Errorf("%w: invalid UTF-8 string", errBSONIndexKeyV2Malformed)
			}
			continuations--
			continuationMin, continuationMax = 0x80, 0xbf
			return nil
		}
		switch {
		case value <= 0x7f:
		case value >= 0xc2 && value <= 0xdf:
			continuations = 1
		case value == 0xe0:
			continuations, continuationMin = 2, 0xa0
		case value >= 0xe1 && value <= 0xec || value >= 0xee && value <= 0xef:
			continuations = 2
		case value == 0xed:
			continuations, continuationMax = 2, 0x9f
		case value == 0xf0:
			continuations, continuationMin = 3, 0x90
		case value >= 0xf1 && value <= 0xf3:
			continuations = 3
		case value == 0xf4:
			continuations, continuationMax = 3, 0x8f
		default:
			return fmt.Errorf("%w: invalid UTF-8 string", errBSONIndexKeyV2Malformed)
		}
		return nil
	}

	for index := 2; index < len(encoded) && index < bsonIndexKeyComponentV2MaxBytes; {
		value := logical(index)
		index++
		if value != 0 {
			if err := appendPayloadByte(value); err != nil {
				return 0, err
			}
			continue
		}
		if index >= len(encoded) {
			return 0, fmt.Errorf("%w: truncated string escape", errBSONIndexKeyV2Malformed)
		}
		if index >= bsonIndexKeyComponentV2MaxBytes {
			return 0, errBSONIndexKeyV2TooLarge
		}
		next := logical(index)
		index++
		switch next {
		case 0:
			if continuations != 0 {
				return 0, fmt.Errorf("%w: invalid UTF-8 string", errBSONIndexKeyV2Malformed)
			}
			return index, nil
		case 0xff:
			if err := appendPayloadByte(0); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("%w: invalid string escape", errBSONIndexKeyV2Malformed)
		}
	}
	return 0, fmt.Errorf("%w: unterminated string", errBSONIndexKeyV2Malformed)
}

func bsonIndexNumberV2Length(encoded []byte, logical func(int) byte, classIndex int) (int, error) {
	if len(encoded) <= classIndex {
		return 0, fmt.Errorf("%w: truncated number", errBSONIndexKeyV2Malformed)
	}
	class := logical(classIndex)
	switch class {
	case bsonIndexNumberNaNV2, bsonIndexNumberNegativeInfinityV2, bsonIndexNumberZeroV2, bsonIndexNumberPositiveInfinityV2:
		return classIndex + 1, nil
	case bsonIndexNumberNegativeFiniteV2, bsonIndexNumberPositiveFiniteV2:
		// Continue below.
	default:
		return 0, fmt.Errorf("%w: numeric class 0x%02x", errBSONIndexKeyV2Malformed, class)
	}
	if len(encoded) < classIndex+4 {
		return 0, fmt.Errorf("%w: truncated finite number", errBSONIndexKeyV2Malformed)
	}
	negative := class == bsonIndexNumberNegativeFiniteV2
	magnitudeByte := func(index int) byte {
		value := logical(index)
		if negative {
			return ^value
		}
		return value
	}
	adjustedExponentBits := uint16(magnitudeByte(classIndex+1))<<8 | uint16(magnitudeByte(classIndex+2))
	adjustedExponent := int16(adjustedExponentBits ^ 0x8000)
	digitCount := 0
	firstDigitZero := false
	lastDigitZero := false
	for index := classIndex + 3; index < len(encoded) && index < bsonIndexKeyComponentV2MaxBytes; index++ {
		packed := magnitudeByte(index)
		for nibbleIndex, nibble := range []byte{packed >> 4, packed & 0x0f} {
			if nibble == 0 {
				if nibbleIndex == 0 && packed&0x0f != 0 {
					return 0, fmt.Errorf("%w: nonzero padding after numeric terminator", errBSONIndexKeyV2Malformed)
				}
				if digitCount == 0 || firstDigitZero || lastDigitZero {
					return 0, fmt.Errorf("%w: non-canonical numeric coefficient", errBSONIndexKeyV2Malformed)
				}
				exponent := int(adjustedExponent) - digitCount + 1
				if exponent < -bsonIndexKeyDecimalExponentWorkBound || exponent > bsonIndexKeyDecimalExponentWorkBound {
					return 0, fmt.Errorf("%w: decimal exponent %d", errBSONIndexKeyV2TooLarge, exponent)
				}
				return index + 1, nil
			}
			if nibble > 10 {
				return 0, fmt.Errorf("%w: invalid packed decimal digit", errBSONIndexKeyV2Malformed)
			}
			lastDigitZero = nibble == 1
			if digitCount == 0 {
				firstDigitZero = lastDigitZero
			}
			digitCount++
			if digitCount > bsonIndexKeyNumericV2MaxDigits {
				return 0, errBSONIndexKeyV2TooLarge
			}
		}
	}
	return 0, fmt.Errorf("%w: unterminated finite number", errBSONIndexKeyV2Malformed)
}

func descendingBSONIndexKeyComponentV2(ascending []byte) ([]byte, error) {
	decoded, n, err := decodeBSONIndexKeyComponentV2(ascending)
	if err != nil {
		return nil, err
	}
	if decoded.Descending || n != len(ascending) {
		return nil, fmt.Errorf("%w: descending transform requires one ascending component", errBSONIndexKeyV2Malformed)
	}
	out := append([]byte(nil), ascending...)
	complementBSONIndexBytesV2(out)
	return out, nil
}

func ascendingBSONIndexKeyComponentV2(descending []byte) ([]byte, error) {
	decoded, n, err := decodeBSONIndexKeyComponentV2(descending)
	if err != nil {
		return nil, err
	}
	if !decoded.Descending || n != len(descending) {
		return nil, fmt.Errorf("%w: ascending transform requires one descending component", errBSONIndexKeyV2Malformed)
	}
	out := append([]byte(nil), descending...)
	complementBSONIndexBytesV2(out)
	return out, nil
}

func complementBSONIndexBytesV2(values []byte) {
	for i := range values {
		values[i] = ^values[i]
	}
}

// bsonIndexEntryKeyV2 appends a self-delimiting document-ID suffix after an
// exact scalar component. The suffix does not participate in scalar ordering.
func bsonIndexEntryKeyV2(component, documentID []byte) ([]byte, error) {
	_, out, err := appendBSONIndexEntryKeyV2(nil, component, documentID)
	return out, err
}

// appendBSONIndexEntryKeyV2 appends the frozen v2 entry representation to dst.
// Keeping the caller-owned arena avoids an intermediate entry allocation in
// buffered secondary-index writes.
func appendBSONIndexEntryKeyV2(dst, component, documentID []byte) ([]byte, []byte, error) {
	if err := validateBSONIndexKeyComponentsV2(component); err != nil {
		return dst, nil, err
	}
	if len(documentID) > bsonIndexKeyComponentV2MaxBytes-3 {
		return dst, nil, errBSONIndexKeyV2TooLarge
	}
	escapedDocumentIDLength := len(documentID)
	for _, value := range documentID {
		if value == 0 {
			escapedDocumentIDLength++
		}
	}
	if escapedDocumentIDLength > bsonIndexKeyComponentV2MaxBytes-3 {
		return dst, nil, errBSONIndexKeyV2TooLarge
	}
	start := len(dst)
	dst = append(dst, component...)
	dst = append(dst, bsonIndexKeyDocumentIDSuffixMarkerV2)
	for _, value := range documentID {
		if value == 0 {
			dst = append(dst, 0, 0xff)
		} else {
			dst = append(dst, value)
		}
	}
	dst = append(dst, 0, 0)
	return dst, dst[start:len(dst):len(dst)], nil
}

func validateBSONIndexKeyComponentsV2(components []byte) error {
	if len(components) == 0 {
		return fmt.Errorf("%w: no scalar component", errBSONIndexKeyV2Malformed)
	}
	for offset := 0; offset < len(components); {
		n, err := bsonIndexKeyComponentV2Length(components[offset:])
		if err != nil {
			return err
		}
		offset += n
	}
	return nil
}

func bsonIndexKeyDocumentIDV2(entry []byte) ([]byte, error) {
	componentLength, err := bsonIndexKeyValuePrefixLengthV2(entry)
	if err != nil {
		return nil, err
	}
	if len(entry)-componentLength < 3 {
		return nil, fmt.Errorf("%w: truncated document ID suffix", errBSONIndexKeyV2Malformed)
	}
	if len(entry)-componentLength > bsonIndexKeyComponentV2MaxBytes {
		return nil, errBSONIndexKeyV2TooLarge
	}
	start := componentLength + 1
	// Document IDs overwhelmingly do not contain NUL. Validate and return the
	// entry-backed suffix directly in that case; callers consume it before the
	// iterator advances, so this avoids a per-result allocation on v2 lookups.
	for index := start; index < len(entry); index++ {
		if entry[index] != 0 {
			continue
		}
		if index+1 >= len(entry) {
			return nil, fmt.Errorf("%w: truncated document ID escape", errBSONIndexKeyV2Malformed)
		}
		switch entry[index+1] {
		case 0:
			if index+2 != len(entry) {
				return nil, fmt.Errorf("%w: trailing document ID bytes", errBSONIndexKeyV2Malformed)
			}
			return entry[start:index], nil
		case 0xff:
			// Fall through to the allocating escape decoder below.
		default:
			return nil, fmt.Errorf("%w: invalid document ID escape", errBSONIndexKeyV2Malformed)
		}
		break
	}
	out := make([]byte, 0, len(entry)-componentLength-3)
	for index := start; index < len(entry); {
		value := entry[index]
		index++
		if value != 0 {
			out = append(out, value)
			if len(out) > bsonIndexKeyComponentV2MaxBytes-3 {
				return nil, errBSONIndexKeyV2TooLarge
			}
			continue
		}
		if index >= len(entry) {
			return nil, fmt.Errorf("%w: truncated document ID escape", errBSONIndexKeyV2Malformed)
		}
		next := entry[index]
		index++
		switch next {
		case 0:
			if index != len(entry) {
				return nil, fmt.Errorf("%w: trailing document ID bytes", errBSONIndexKeyV2Malformed)
			}
			return out, nil
		case 0xff:
			out = append(out, 0)
			if len(out) > bsonIndexKeyComponentV2MaxBytes-3 {
				return nil, errBSONIndexKeyV2TooLarge
			}
		default:
			return nil, fmt.Errorf("%w: invalid document ID escape", errBSONIndexKeyV2Malformed)
		}
	}
	return nil, fmt.Errorf("%w: unterminated document ID suffix", errBSONIndexKeyV2Malformed)
}

func bsonIndexKeyValuePrefixV2(entry []byte) ([]byte, error) {
	n, err := bsonIndexKeyValuePrefixLengthV2(entry)
	if err != nil {
		return nil, err
	}
	return entry[:n], nil
}

// BSONIndexKeyStableSortPrefixV2 returns a BSON-v2 logical key suitable for a
// Mongo-compatible stable sort tie group. Mongo comparison treats a missing
// field and null as equal; the physical v2 index deliberately keeps them
// distinct so equality and range bounds remain precise. Normalizing only that
// distinction lets an ordered caller group the adjacent physical runs and use
// document ID as the stable tie breaker without changing the stored format.
func BSONIndexKeyStableSortPrefixV2(entry []byte) ([]byte, error) {
	prefix, err := bsonIndexKeyValuePrefixV2(entry)
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), prefix...)
	for at := 0; at < len(out); {
		n, err := bsonIndexKeyComponentV2Length(out[at:])
		if err != nil {
			return nil, err
		}
		if out[at] == bsonIndexKeyComponentV2DescendingMarker {
			if ^out[at+1] == bsonIndexKeyTagMissingV2 {
				out[at+1] = ^bsonIndexKeyTagNullV2
			}
		} else if out[at+1] == bsonIndexKeyTagMissingV2 {
			out[at+1] = bsonIndexKeyTagNullV2
		}
		at += n
	}
	return out, nil
}

func bsonIndexKeyValuePrefixLengthV2(entry []byte) (int, error) {
	componentLength := 0
	for componentLength < len(entry) && entry[componentLength] != bsonIndexKeyDocumentIDSuffixMarkerV2 {
		n, err := bsonIndexKeyComponentV2Length(entry[componentLength:])
		if err != nil {
			return 0, err
		}
		componentLength += n
	}
	if componentLength == 0 || componentLength >= len(entry) || entry[componentLength] != bsonIndexKeyDocumentIDSuffixMarkerV2 {
		return 0, fmt.Errorf("%w: missing document ID suffix", errBSONIndexKeyV2Malformed)
	}
	return componentLength, nil
}
