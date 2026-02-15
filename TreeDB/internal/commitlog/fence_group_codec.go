package commitlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
)

const (
	fenceRIDGroupPayloadVersion = byte(1)
	maxCommitLogKeyLen          = int(^uint16(0))
)

const (
	WALFenceGroupEncodingSimple = "simple"
	WALFenceGroupEncodingPrefix = "prefix"
)

type FenceRIDGroupEncoding byte

const (
	FenceRIDGroupEncodingSimple FenceRIDGroupEncoding = iota
	FenceRIDGroupEncodingPrefix
)

func (e FenceRIDGroupEncoding) String() string {
	switch e {
	case FenceRIDGroupEncodingSimple:
		return WALFenceGroupEncodingSimple
	case FenceRIDGroupEncodingPrefix:
		return WALFenceGroupEncodingPrefix
	default:
		return fmt.Sprintf("encoding_%d", byte(e))
	}
}

func ParseFenceRIDGroupEncoding(s string) (FenceRIDGroupEncoding, error) {
	switch strings.TrimSpace(strings.ToLower(s)) {
	case "", WALFenceGroupEncodingSimple:
		return FenceRIDGroupEncodingSimple, nil
	case WALFenceGroupEncodingPrefix:
		return FenceRIDGroupEncodingPrefix, nil
	default:
		return FenceRIDGroupEncodingSimple, fmt.Errorf("commitlog: unsupported WAL fence group encoding %q", s)
	}
}

type FenceRIDGroupEntry struct {
	Key []byte
	RID uint64
}

func appendUvarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func decodeUvarint(src []byte, off *int) (uint64, error) {
	if off == nil || *off < 0 || *off > len(src) {
		return 0, ErrCorrupt
	}
	v, n := binary.Uvarint(src[*off:])
	if n <= 0 {
		return 0, ErrCorrupt
	}
	*off += n
	return v, nil
}

func maxFenceRIDGroupDecodeCount(encoding FenceRIDGroupEncoding, remaining int) uint64 {
	if remaining <= 0 {
		return 0
	}
	rem := uint64(remaining)
	// The payload format enforces non-empty keys and non-zero RIDs, so each
	// simple entry must consume at least 3 bytes:
	//   keyLen(>=1 varint) + key(>=1 byte) + rid(>=1 varint).
	// Prefix encoding uses the simple form for the first key, and each
	// subsequent key must consume at least 4 bytes:
	//   shared(>=1 varint) + suffixLen(>=1 varint) + suffix(>=1 byte) + rid(>=1 varint).
	switch encoding {
	case FenceRIDGroupEncodingSimple:
		return rem / 3
	case FenceRIDGroupEncodingPrefix:
		if rem < 3 {
			return 0
		}
		return 1 + (rem-3)/4
	default:
		return 0
	}
}

func EncodeFenceRIDGroupPayload(entries []FenceRIDGroupEntry, encoding FenceRIDGroupEncoding) ([]byte, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("commitlog: empty fence RID group")
	}
	switch encoding {
	case FenceRIDGroupEncodingSimple, FenceRIDGroupEncodingPrefix:
	default:
		return nil, fmt.Errorf("commitlog: unknown fence RID group encoding %d", encoding)
	}

	size := 3
	for i := range entries {
		entry := entries[i]
		if len(entry.Key) == 0 {
			return nil, fmt.Errorf("commitlog: fence RID group key[%d] is empty", i)
		}
		if len(entry.Key) > maxCommitLogKeyLen {
			return nil, ErrRecordTooLarge
		}
		if entry.RID == 0 {
			return nil, fmt.Errorf("commitlog: fence RID group key[%d] missing RID", i)
		}
		// 20B overhead leaves room for 2 uvarints in the vast majority of cases.
		size += len(entry.Key) + 20
	}
	if size < 0 {
		return nil, ErrRecordTooLarge
	}
	dst := make([]byte, 0, size)
	dst = append(dst, fenceRIDGroupPayloadVersion)
	dst = append(dst, byte(encoding))
	dst = appendUvarint(dst, uint64(len(entries)))

	var prevKey []byte
	for i := range entries {
		entry := entries[i]
		if encoding == FenceRIDGroupEncodingSimple || i == 0 {
			dst = appendUvarint(dst, uint64(len(entry.Key)))
			dst = append(dst, entry.Key...)
			dst = appendUvarint(dst, entry.RID)
			prevKey = entry.Key
			continue
		}

		shared := commonPrefixLen(prevKey, entry.Key)
		if bytes.Compare(prevKey, entry.Key) >= 0 {
			return nil, fmt.Errorf("commitlog: prefix fence RID encoding requires strictly increasing keys")
		}
		suffix := entry.Key[shared:]
		dst = appendUvarint(dst, uint64(shared))
		dst = appendUvarint(dst, uint64(len(suffix)))
		dst = append(dst, suffix...)
		dst = appendUvarint(dst, entry.RID)
		prevKey = entry.Key
	}

	return dst, nil
}

func DecodeFenceRIDGroupPayload(payload []byte, dst []FenceRIDGroupEntry) ([]FenceRIDGroupEntry, FenceRIDGroupEncoding, error) {
	if len(payload) < 3 {
		return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
	}
	off := 0
	version := payload[off]
	off++
	if version != fenceRIDGroupPayloadVersion {
		return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
	}
	encoding := FenceRIDGroupEncoding(payload[off])
	off++
	switch encoding {
	case FenceRIDGroupEncodingSimple, FenceRIDGroupEncodingPrefix:
	default:
		return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
	}
	countU, err := decodeUvarint(payload, &off)
	if err != nil {
		return nil, FenceRIDGroupEncodingSimple, err
	}
	if countU == 0 {
		return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
	}
	maxCount := uint64(int(^uint(0) >> 1))
	if countU > maxCount {
		return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
	}
	countBound := maxFenceRIDGroupDecodeCount(encoding, len(payload)-off)
	if countBound == 0 || countU > countBound {
		return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
	}
	count := int(countU)
	// Avoid attacker-controlled large preallocation on corrupt payloads. We only
	// reserve a bounded initial capacity and let append grow as needed.
	initCap := count
	const maxDecodePrealloc = 4096
	if initCap > maxDecodePrealloc {
		initCap = maxDecodePrealloc
	}
	if cap(dst) < initCap {
		dst = make([]FenceRIDGroupEntry, 0, initCap)
	} else {
		dst = dst[:0]
	}

	var prevKey []byte
	for i := 0; i < count; i++ {
		if encoding == FenceRIDGroupEncodingSimple || i == 0 {
			keyLenU, keyErr := decodeUvarint(payload, &off)
			if keyErr != nil {
				return nil, FenceRIDGroupEncodingSimple, keyErr
			}
			if keyLenU == 0 || keyLenU > uint64(maxCommitLogKeyLen) {
				return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
			}
			keyLen := int(keyLenU)
			if keyLen > len(payload)-off {
				return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
			}
			key := payload[off : off+keyLen]
			off += keyLen
			rid, ridErr := decodeUvarint(payload, &off)
			if ridErr != nil || rid == 0 {
				return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
			}
			dst = append(dst, FenceRIDGroupEntry{Key: key, RID: rid})
			prevKey = key
			continue
		}

		sharedU, sharedErr := decodeUvarint(payload, &off)
		if sharedErr != nil {
			return nil, FenceRIDGroupEncodingSimple, sharedErr
		}
		if sharedU > uint64(len(prevKey)) {
			return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
		}
		suffixLenU, suffixErr := decodeUvarint(payload, &off)
		if suffixErr != nil {
			return nil, FenceRIDGroupEncodingSimple, suffixErr
		}
		if suffixLenU == 0 || suffixLenU > uint64(len(payload)-off) {
			return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
		}
		suffixLen := int(suffixLenU)
		suffix := payload[off : off+suffixLen]
		off += suffixLen
		rid, ridErr := decodeUvarint(payload, &off)
		if ridErr != nil || rid == 0 {
			return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
		}
		shared := int(sharedU)
		keyLen := shared + suffixLen
		if keyLen <= 0 || keyLen > maxCommitLogKeyLen {
			return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
		}
		key := make([]byte, keyLen)
		copy(key, prevKey[:shared])
		copy(key[shared:], suffix)
		if bytes.Compare(prevKey, key) >= 0 {
			return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
		}
		dst = append(dst, FenceRIDGroupEntry{Key: key, RID: rid})
		prevKey = key
	}
	if off != len(payload) {
		return nil, FenceRIDGroupEncodingSimple, ErrCorrupt
	}
	return dst, encoding, nil
}

func ValidateFenceRIDGroupPayload(payload []byte) error {
	_, _, err := DecodeFenceRIDGroupPayload(payload, nil)
	return err
}

func commonPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
