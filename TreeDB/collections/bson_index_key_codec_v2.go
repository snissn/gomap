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

	bsonIndexNumberNegativeInfinityV2 byte = 0x10
	bsonIndexNumberNegativeFiniteV2   byte = 0x20
	bsonIndexNumberZeroV2             byte = 0x30
	bsonIndexNumberPositiveFiniteV2   byte = 0x40
	bsonIndexNumberPositiveInfinityV2 byte = 0x50
	bsonIndexNumberNaNV2              byte = 0x60

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

// appendBSONIndexKeyComponentV2 appends one ascending, self-delimiting scalar
// component. Concatenating returned components is safe for compound indexes.
// The document ID is deliberately appended by bsonIndexEntryKeyV2 afterwards.
func appendBSONIndexKeyComponentV2(dst []byte, value bson.RawValue) ([]byte, []byte, error) {
	start := len(dst)
	if start > math.MaxInt-bsonIndexKeyComponentV2MaxBytes {
		return dst, nil, errBSONIndexKeyV2TooLarge
	}
	dst = append(dst, bsonIndexKeyComponentV2AscendingMarker)

	if value.IsZero() {
		dst = append(dst, bsonIndexKeyTagMissingV2)
		return dst, dst[start:len(dst):len(dst)], nil
	}

	switch value.Type {
	case bson.TypeNull:
		dst = append(dst, bsonIndexKeyTagNullV2)
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		dst = append(dst, bsonIndexKeyTagNumberV2)
		number, err := canonicalBSONIndexNumberV2(value)
		if err != nil {
			return dst[:start], nil, err
		}
		dst = append(dst, number.class)
		if number.class == bsonIndexNumberNegativeFiniteV2 || number.class == bsonIndexNumberPositiveFiniteV2 {
			digits := number.coefficient.Text(10)
			if len(digits) == 0 || len(digits) > bsonIndexKeyNumericV2MaxDigits {
				return dst[:start], nil, errBSONIndexKeyV2TooLarge
			}
			adjustedExponent := int64(number.exponent) + int64(len(digits)) - 1
			if adjustedExponent < math.MinInt16 || adjustedExponent > math.MaxInt16 {
				return dst[:start], nil, fmt.Errorf("%w: adjusted decimal exponent %d", errBSONIndexKeyV2TooLarge, adjustedExponent)
			}
			payloadStart := len(dst)
			dst = binary.BigEndian.AppendUint16(dst, uint16(int16(adjustedExponent))^0x8000)
			dst, err = appendPackedBSONIndexDigitsV2(dst, digits)
			if err != nil {
				return dst[:start], nil, err
			}
			if number.class == bsonIndexNumberNegativeFiniteV2 {
				complementBSONIndexBytesV2(dst[payloadStart:])
			}
		}
	case bson.TypeString:
		text, ok := value.StringValueOK()
		if !ok || !utf8.ValidString(text) {
			return dst[:start], nil, fmt.Errorf("%w: invalid UTF-8 BSON string", errBSONIndexKeyV2Malformed)
		}
		if len(text) >= bsonIndexKeyComponentV2MaxBytes {
			return dst[:start], nil, fmt.Errorf("%w: string length %d", errBSONIndexKeyV2TooLarge, len(text))
		}
		dst = append(dst, bsonIndexKeyTagStringV2)
		for i := 0; i < len(text); i++ {
			if text[i] == 0 {
				dst = append(dst, 0, 0xff)
			} else {
				dst = append(dst, text[i])
			}
		}
		dst = append(dst, 0, 0)
	case bson.TypeObjectID:
		objectID, ok := value.ObjectIDOK()
		if !ok {
			return dst[:start], nil, fmt.Errorf("%w: invalid ObjectID", errBSONIndexKeyV2Malformed)
		}
		dst = append(dst, bsonIndexKeyTagObjectIDV2)
		dst = append(dst, objectID[:]...)
	case bson.TypeBoolean:
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
		milliseconds, ok := value.DateTimeOK()
		if !ok {
			return dst[:start], nil, fmt.Errorf("%w: invalid datetime", errBSONIndexKeyV2Malformed)
		}
		dst = append(dst, bsonIndexKeyTagDateTimeV2)
		dst = binary.BigEndian.AppendUint64(dst, uint64(milliseconds)^uint64(1<<63))
	case bson.TypeTimestamp:
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

func appendPackedBSONIndexDigitsV2(dst []byte, digits string) ([]byte, error) {
	if len(digits) == 0 || len(digits) > bsonIndexKeyNumericV2MaxDigits || digits[0] == '0' || digits[len(digits)-1] == '0' {
		return dst, fmt.Errorf("%w: non-canonical decimal coefficient", errBSONIndexKeyV2Malformed)
	}
	nibbles := make([]byte, 0, len(digits)+2)
	for i := range digits {
		if digits[i] < '0' || digits[i] > '9' {
			return dst, fmt.Errorf("%w: invalid decimal digit", errBSONIndexKeyV2Malformed)
		}
		nibbles = append(nibbles, digits[i]-'0'+1)
	}
	nibbles = append(nibbles, 0)
	if len(nibbles)%2 != 0 {
		nibbles = append(nibbles, 0)
	}
	for i := 0; i < len(nibbles); i += 2 {
		dst = append(dst, nibbles[i]<<4|nibbles[i+1])
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
	_, n, err := decodeBSONIndexKeyComponentV2(encoded)
	return n, err
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
	n, err := bsonIndexKeyComponentV2Length(component)
	if err != nil || n != len(component) {
		if err == nil {
			err = fmt.Errorf("%w: scalar component has trailing bytes", errBSONIndexKeyV2Malformed)
		}
		return nil, err
	}
	if len(documentID) >= bsonIndexKeyComponentV2MaxBytes {
		return nil, errBSONIndexKeyV2TooLarge
	}
	out := make([]byte, 0, len(component)+len(documentID)+3)
	out = append(out, component...)
	out = append(out, bsonIndexKeyDocumentIDSuffixMarkerV2)
	for _, value := range documentID {
		if value == 0 {
			out = append(out, 0, 0xff)
		} else {
			out = append(out, value)
		}
	}
	out = append(out, 0, 0)
	return out, nil
}

func bsonIndexKeyDocumentIDV2(entry []byte) ([]byte, error) {
	componentLength, err := bsonIndexKeyComponentV2Length(entry)
	if err != nil {
		return nil, err
	}
	if componentLength >= len(entry) || entry[componentLength] != bsonIndexKeyDocumentIDSuffixMarkerV2 {
		return nil, fmt.Errorf("%w: missing document ID suffix", errBSONIndexKeyV2Malformed)
	}
	out := make([]byte, 0, len(entry)-componentLength-3)
	for index := componentLength + 1; index < len(entry); {
		value := entry[index]
		index++
		if value != 0 {
			out = append(out, value)
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
		default:
			return nil, fmt.Errorf("%w: invalid document ID escape", errBSONIndexKeyV2Malformed)
		}
	}
	return nil, fmt.Errorf("%w: unterminated document ID suffix", errBSONIndexKeyV2Malformed)
}
