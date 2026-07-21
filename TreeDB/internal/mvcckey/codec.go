// Package mvcckey defines TreeDB's opt-in physical-key encoding for
// caller-assigned MVCC timestamps.
//
// The package is internal because TreeDB is pre-alpha and this format is not a
// stable public API. Raw TreeDB key/value operations do not use this encoding.
package mvcckey

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

const (
	// TimestampSize is the encoded size of an external timestamp.
	TimestampSize = 8
	// MaxEncodedKeySize is the codec envelope. TreeDB pages may impose a
	// smaller physical-key limit for a particular value and page shape.
	MaxEncodedKeySize = math.MaxUint16
)

var (
	ErrZeroTimestamp  = errors.New("mvcckey: timestamp zero is reserved")
	ErrKeyTooLarge    = errors.New("mvcckey: encoded key exceeds uint16 envelope")
	ErrWrongNamespace = errors.New("mvcckey: key is outside the external-version namespace")
	ErrMalformedKey   = errors.New("mvcckey: malformed external-version key")
)

// namespaceV1 is a versioned physical-key subspace: NUL + "TDBMVCC" + v1.
// The subspace is reserved only when the opt-in MVCC layer owns the DB keyspace.
var namespaceV1 = [...]byte{0x00, 'T', 'D', 'B', 'M', 'V', 'C', 'C', 0x01}

// EncodedLen reports the exact number of bytes needed by Encode.
func EncodedLen(logical []byte) (int, error) {
	// Every byte contributes at least one encoded byte. Reject impossible input
	// before scanning it or performing arithmetic that could overflow.
	const fixed = len(namespaceV1) + 2 + TimestampSize
	if len(logical) > MaxEncodedKeySize-fixed {
		return 0, ErrKeyTooLarge
	}

	zeros := 0
	for _, b := range logical {
		if b == 0 {
			zeros++
		}
	}
	encodedLen := fixed + len(logical) + zeros
	if encodedLen > MaxEncodedKeySize {
		return 0, ErrKeyTooLarge
	}
	return encodedLen, nil
}

// Encode returns the physical key for logical at timestamp. Logical keys may
// contain arbitrary bytes and may be empty. Timestamp zero is reserved.
func Encode(logical []byte, timestamp uint64) ([]byte, error) {
	return Append(nil, logical, timestamp)
}

// Append appends the physical key to dst. On error it returns dst unchanged.
func Append(dst, logical []byte, timestamp uint64) ([]byte, error) {
	if timestamp == 0 {
		return dst, ErrZeroTimestamp
	}
	encodedLen, err := EncodedLen(logical)
	if err != nil {
		return dst, err
	}

	start := len(dst)
	dst = grow(dst, encodedLen)
	dst = append(dst, namespaceV1[:]...)
	dst = appendEscaped(dst, logical)
	dst = append(dst, 0x00, 0x00)
	var suffix [TimestampSize]byte
	binary.BigEndian.PutUint64(suffix[:], ^timestamp)
	dst = append(dst, suffix[:]...)
	if len(dst)-start != encodedLen {
		panic("mvcckey: internal encoded length mismatch")
	}
	return dst, nil
}

// Decode returns the logical key and caller-assigned timestamp. The returned
// logical key is newly allocated.
func Decode(physical []byte) ([]byte, uint64, error) {
	return DecodeAppend(nil, physical)
}

// DecodeAppend appends the decoded logical key to dst. On error it returns dst
// unchanged and timestamp zero.
func DecodeAppend(dst, physical []byte) ([]byte, uint64, error) {
	if len(physical) > MaxEncodedKeySize {
		return dst, 0, ErrKeyTooLarge
	}
	if !bytes.HasPrefix(physical, namespaceV1[:]) {
		return dst, 0, ErrWrongNamespace
	}
	if len(physical) < len(namespaceV1)+2+TimestampSize {
		return dst, 0, ErrMalformedKey
	}

	bodyEnd, decodedLen, timestamp, err := inspect(physical)
	if err != nil {
		return dst, 0, err
	}
	start := len(dst)
	dst = grow(dst, decodedLen)
	for i := len(namespaceV1); i < bodyEnd; {
		if physical[i] != 0 {
			dst = append(dst, physical[i])
			i++
			continue
		}
		dst = append(dst, 0)
		i += 2
	}
	if len(dst)-start != decodedLen {
		panic("mvcckey: internal decoded length mismatch")
	}
	return dst, timestamp, nil
}

// InNamespace reports whether physical begins with the version-1 MVCC marker.
// It does not validate the remainder of the key.
func InNamespace(physical []byte) bool {
	return bytes.HasPrefix(physical, namespaceV1[:])
}

// VersionAffinityPrefix returns the allocation-free physical prefix through
// the encoded logical-key terminator. Unlike VersionPrefix, it deliberately
// accepts a malformed or incomplete timestamp suffix. This keeps every key
// that can sort inside one logical key's version range on the same in-memory
// shard, so an exact-version read cannot hide malformed reserved-namespace
// records that the MVCC layer must reject fail-closed. The total physical
// length is intentionally not bounded: an oversized malformed suffix still
// sorts in that range and must retain the logical prefix's affinity.
func VersionAffinityPrefix(physical []byte) (prefix []byte, ok bool) {
	if !bytes.HasPrefix(physical, namespaceV1[:]) {
		return nil, false
	}
	for i := len(namespaceV1); i < len(physical); {
		if physical[i] != 0 {
			i++
			continue
		}
		if i+1 >= len(physical) {
			return nil, false
		}
		switch physical[i+1] {
		case 0xff:
			i += 2
		case 0x00:
			return physical[:i+2], true
		default:
			return nil, false
		}
	}
	return nil, false
}

// VersionPrefix returns the allocation-free physical prefix shared by every
// encoded version of one logical key. The returned slice aliases physical.
// Malformed and non-MVCC keys return ok=false so generic callers retain their
// existing full-key behavior.
func VersionPrefix(physical []byte) (prefix []byte, ok bool) {
	if len(physical) > MaxEncodedKeySize || !bytes.HasPrefix(physical, namespaceV1[:]) {
		return nil, false
	}
	bodyEnd, _, _, err := inspect(physical)
	if err != nil || bodyEnd+2+TimestampSize != len(physical) {
		return nil, false
	}
	return physical[:bodyEnd+2], true
}

// ExactVersionRange reports whether [start,end) is the canonical range from
// one encoded version lower bound through the exclusive upper bound of all
// versions of that same logical key. The returned prefix aliases start.
func ExactVersionRange(start, end []byte) (prefix []byte, ok bool) {
	prefix, ok = VersionPrefix(start)
	if !ok || len(end) != len(prefix) || len(prefix) == 0 || prefix[len(prefix)-1] != 0 {
		return nil, false
	}
	if !bytes.Equal(prefix[:len(prefix)-1], end[:len(end)-1]) || end[len(end)-1] != 1 {
		return nil, false
	}
	return prefix, true
}

// AppendNamespaceLower appends the inclusive lower bound of the v1 namespace.
func AppendNamespaceLower(dst []byte) []byte {
	return append(dst, namespaceV1[:]...)
}

// AppendNamespaceUpper appends the exclusive upper bound of the v1 namespace.
func AppendNamespaceUpper(dst []byte) []byte {
	dst = append(dst, namespaceV1[:]...)
	dst[len(dst)-1]++
	return dst
}

// AppendLogicalPrefixLower appends the inclusive physical lower bound for all
// MVCC keys whose decoded logical key begins with logicalPrefix.
func AppendLogicalPrefixLower(dst, logicalPrefix []byte) ([]byte, error) {
	encodedLen, err := escapedPrefixLen(logicalPrefix)
	if err != nil {
		return dst, err
	}
	dst = grow(dst, encodedLen)
	dst = append(dst, namespaceV1[:]...)
	return appendEscaped(dst, logicalPrefix), nil
}

// AppendLogicalPrefixUpper appends the exclusive physical upper bound for all
// MVCC keys whose decoded logical key begins with logicalPrefix.
func AppendLogicalPrefixUpper(dst, logicalPrefix []byte) ([]byte, error) {
	start := len(dst)
	var err error
	dst, err = AppendLogicalPrefixLower(dst, logicalPrefix)
	if err != nil {
		return dst, err
	}
	return prefixSuccessor(dst, start), nil
}

// AppendKeyVersionsLower appends the inclusive lower bound for every encoded
// version of exactly logical.
func AppendKeyVersionsLower(dst, logical []byte) ([]byte, error) {
	if _, err := EncodedLen(logical); err != nil {
		return dst, err
	}
	prefixLen, err := escapedPrefixLen(logical)
	if err != nil {
		return dst, err
	}
	dst = grow(dst, prefixLen+2)
	dst = append(dst, namespaceV1[:]...)
	dst = appendEscaped(dst, logical)
	return append(dst, 0x00, 0x00), nil
}

// AppendKeyVersionsUpper appends the exclusive upper bound for every encoded
// version of exactly logical.
func AppendKeyVersionsUpper(dst, logical []byte) ([]byte, error) {
	start := len(dst)
	var err error
	dst, err = AppendKeyVersionsLower(dst, logical)
	if err != nil {
		return dst, err
	}
	return prefixSuccessor(dst, start), nil
}

func escapedPrefixLen(logical []byte) (int, error) {
	if len(logical) > MaxEncodedKeySize-len(namespaceV1) {
		return 0, ErrKeyTooLarge
	}
	zeros := 0
	for _, b := range logical {
		if b == 0 {
			zeros++
		}
	}
	encodedLen := len(namespaceV1) + len(logical) + zeros
	if encodedLen > MaxEncodedKeySize {
		return 0, ErrKeyTooLarge
	}
	return encodedLen, nil
}

func inspect(physical []byte) (bodyEnd int, decodedLen int, timestamp uint64, err error) {
	for i := len(namespaceV1); i < len(physical); {
		if physical[i] != 0 {
			decodedLen++
			i++
			continue
		}
		if i+1 >= len(physical) {
			return 0, 0, 0, ErrMalformedKey
		}
		switch physical[i+1] {
		case 0xff:
			decodedLen++
			i += 2
		case 0x00:
			bodyEnd = i
			i += 2
			if len(physical)-i != TimestampSize {
				return 0, 0, 0, ErrMalformedKey
			}
			timestamp = ^binary.BigEndian.Uint64(physical[i:])
			if timestamp == 0 {
				return 0, 0, 0, ErrZeroTimestamp
			}
			return bodyEnd, decodedLen, timestamp, nil
		default:
			return 0, 0, 0, ErrMalformedKey
		}
	}
	return 0, 0, 0, ErrMalformedKey
}

func appendEscaped(dst, logical []byte) []byte {
	for _, b := range logical {
		if b == 0 {
			dst = append(dst, 0x00, 0xff)
			continue
		}
		dst = append(dst, b)
	}
	return dst
}

func grow(dst []byte, additional int) []byte {
	if cap(dst)-len(dst) >= additional {
		return dst
	}
	n := len(dst) + additional
	out := make([]byte, len(dst), n)
	copy(out, dst)
	return out
}

func prefixSuccessor(dst []byte, start int) []byte {
	for i := len(dst) - 1; i >= start; i-- {
		if dst[i] != 0xff {
			dst[i]++
			return dst[:i+1]
		}
	}
	panic("mvcckey: namespace has no finite successor")
}
