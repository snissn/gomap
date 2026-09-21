package pebblecompat

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	pebblerangekey "github.com/cockroachdb/pebble/rangekey"
)

type pointMeta struct {
	Seq  uint64
	Kind pebble.InternalKeyKind
}

type rangeLogRecord struct {
	Seq    uint64
	Order  uint32
	Kind   pebble.InternalKeyKind
	Start  []byte
	End    []byte
	Suffix []byte
	Value  []byte
}

func encodeSeq(seq uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, seq)
	return buf
}

func decodeSeq(buf []byte) (uint64, bool) {
	if len(buf) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(buf), true
}

func encodePointMeta(seq uint64, kind pebble.InternalKeyKind) []byte {
	buf := make([]byte, 9)
	binary.BigEndian.PutUint64(buf[:8], seq)
	buf[8] = byte(kind)
	return buf
}

func decodePointMeta(buf []byte) (pointMeta, bool) {
	if len(buf) != 9 {
		return pointMeta{}, false
	}
	return pointMeta{
		Seq:  binary.BigEndian.Uint64(buf[:8]),
		Kind: pebble.InternalKeyKind(buf[8]),
	}, true
}

func appendUvarint(dst []byte, x uint64) []byte {
	var scratch [10]byte
	n := binary.PutUvarint(scratch[:], x)
	return append(dst, scratch[:n]...)
}

func appendLengthPrefixed(dst, data []byte) []byte {
	dst = appendUvarint(dst, uint64(len(data)))
	return append(dst, data...)
}

func readLengthPrefixed(src []byte, off *int) ([]byte, error) {
	if *off >= len(src) {
		return nil, fmt.Errorf("unexpected eof")
	}
	lenVal, n := binary.Uvarint(src[*off:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid uvarint")
	}
	*off += n
	if lenVal > uint64(len(src)-*off) {
		return nil, fmt.Errorf("truncated field")
	}
	start := *off
	end := start + int(lenVal)
	*off = end
	return append([]byte(nil), src[start:end]...), nil
}

func encodeRangeLogValue(rec rangeLogRecord) []byte {
	out := make([]byte, 0, 1+1+len(rec.Start)+len(rec.End)+len(rec.Suffix)+len(rec.Value)+20)
	out = append(out, byte(1))
	out = append(out, byte(rec.Kind))
	out = appendLengthPrefixed(out, rec.Start)
	out = appendLengthPrefixed(out, rec.End)
	out = appendLengthPrefixed(out, rec.Suffix)
	out = appendLengthPrefixed(out, rec.Value)
	return out
}

func decodeRangeLogValue(value []byte) (rangeLogRecord, error) {
	if len(value) < 2 {
		return rangeLogRecord{}, fmt.Errorf("short record")
	}
	if value[0] != 1 {
		return rangeLogRecord{}, fmt.Errorf("unknown record version: %d", value[0])
	}
	out := rangeLogRecord{Kind: pebble.InternalKeyKind(value[1])}
	off := 2
	var err error
	if out.Start, err = readLengthPrefixed(value, &off); err != nil {
		return rangeLogRecord{}, fmt.Errorf("decode start: %w", err)
	}
	if out.End, err = readLengthPrefixed(value, &off); err != nil {
		return rangeLogRecord{}, fmt.Errorf("decode end: %w", err)
	}
	if out.Suffix, err = readLengthPrefixed(value, &off); err != nil {
		return rangeLogRecord{}, fmt.Errorf("decode suffix: %w", err)
	}
	if out.Value, err = readLengthPrefixed(value, &off); err != nil {
		return rangeLogRecord{}, fmt.Errorf("decode value: %w", err)
	}
	if off != len(value) {
		return rangeLogRecord{}, fmt.Errorf("trailing bytes in range record")
	}
	return out, nil
}

func decodeRangeKeyBatchRecord(
	kind pebble.InternalKeyKind, seq uint64, start, encoded []byte, orderBase uint32,
) ([]rangeLogRecord, error) {
	ik := pebble.InternalKey{
		UserKey: append([]byte(nil), start...),
		Trailer: (seq << 8) | uint64(kind),
	}
	span, err := pebblerangekey.Decode(ik, encoded, nil)
	if err != nil {
		return nil, err
	}
	records := make([]rangeLogRecord, 0, len(span.Keys))
	for i := range span.Keys {
		k := span.Keys[i]
		records = append(records, rangeLogRecord{
			Seq:    seq,
			Order:  orderBase + uint32(i),
			Kind:   pebble.InternalKeyKind(k.Trailer & 0xff),
			Start:  append([]byte(nil), span.Start...),
			End:    append([]byte(nil), span.End...),
			Suffix: append([]byte(nil), k.Suffix...),
			Value:  append([]byte(nil), k.Value...),
		})
	}
	return records, nil
}

func keyInRange(key, start, end []byte) bool {
	if start != nil && bytes.Compare(key, start) < 0 {
		return false
	}
	if end != nil && bytes.Compare(key, end) >= 0 {
		return false
	}
	return true
}

func clipRange(start, end, lower, upper []byte) ([]byte, []byte, bool) {
	cs := append([]byte(nil), start...)
	ce := append([]byte(nil), end...)
	if lower != nil && bytes.Compare(cs, lower) < 0 {
		cs = append(cs[:0], lower...)
	}
	if upper != nil && bytes.Compare(ce, upper) > 0 {
		ce = append(ce[:0], upper...)
	}
	if bytes.Compare(cs, ce) >= 0 {
		return nil, nil, false
	}
	return cs, ce, true
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
