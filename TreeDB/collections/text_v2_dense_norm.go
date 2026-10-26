package collections

import (
	"encoding/binary"
	"math/bits"
)

const textV2DenseNormBlockValueVersion byte = 2

func textV2DenseNormWidth(value uint64) byte {
	switch {
	case value <= 255:
		return 1
	case value <= 65535:
		return 2
	default:
		return 4
	}
}

func textV2DenseNormWidthMax(width byte) (uint64, bool) {
	switch width {
	case 0:
		return 0, true
	case 1:
		return 255, true
	case 2:
		return 65535, true
	case 4:
		return 1<<32 - 1, true
	default:
		return 0, false
	}
}

func textV2DenseNormVarintSize(value uint64) uint64 {
	return uint64((bits.Len64(value|1) + 6) / 7)
}

// entries is the existing encoder's sorted, private copy. Size both formats
// without encoding either; sparse/invalid/nonbeneficial inputs retain V1 bytes.
func encodeTextV2DenseNormBlock(value textV2NormBlockValue, entries []textV2NormBlockEntry) ([]byte, bool) {
	n := uint64(len(entries))
	if n == 0 || value.BlockStart == 0 || value.BlockSize == 0 || n > uint64(value.BlockSize) || value.FieldCount == 0 || value.BlockStart > ^uint64(0)-uint64(value.BlockSize) {
		return nil, false
	}
	headerSize := uint64(1) + textV2DenseNormVarintSize(value.BlockStart) + textV2DenseNormVarintSize(uint64(value.BlockSize)) + textV2DenseNormVarintSize(uint64(value.FieldCount)) + textV2DenseNormVarintSize(n)
	v1Size := headerSize
	base, maximum, maxLength := ^uint64(0), uint64(0), uint64(0)
	for i, entry := range entries {
		if entry.Ordinal != value.BlockStart+uint64(i) || entry.Generation == 0 || entry.Flags&^textV2DocFlagTombstone != 0 || uint64(len(entry.FieldLengths)) != uint64(value.FieldCount) {
			return nil, false
		}
		base = min(base, entry.Generation)
		maximum = max(maximum, entry.Generation)
		add := textV2DenseNormVarintSize(entry.Ordinal) + textV2DenseNormVarintSize(entry.Generation) + 1
		for _, length := range entry.FieldLengths {
			maxLength = max(maxLength, uint64(length))
			if add > ^uint64(0)-textV2DenseNormVarintSize(uint64(length)) {
				return nil, false
			}
			add += textV2DenseNormVarintSize(uint64(length))
		}
		if v1Size > ^uint64(0)-add {
			return nil, false
		}
		v1Size += add
	}
	delta := maximum - base
	if delta > 1<<32-1 {
		return nil, false
	}
	generationWidth := byte(0)
	if delta != 0 {
		generationWidth = textV2DenseNormWidth(delta)
	}
	generationMax, _ := textV2DenseNormWidthMax(generationWidth)
	if base > ^uint64(0)-generationMax {
		return nil, false
	}
	fieldWidth := textV2DenseNormWidth(maxLength)
	bitmapSize := (n + 7) / 8
	headerSize += textV2DenseNormVarintSize(base) + 2
	overhead := headerSize + bitmapSize
	entryBytes := uint64(generationWidth) + uint64(value.FieldCount)*uint64(fieldWidth)
	maxInt := uint64(int(^uint(0) >> 1))
	if overhead > maxInt || n > (maxInt-overhead)/entryBytes {
		return nil, false
	}
	size := overhead + n*entryBytes
	if size >= v1Size {
		return nil, false
	}
	out := make([]byte, int(size))
	out[0] = textV2DenseNormBlockValueVersion
	pos := 1
	for _, number := range [...]uint64{value.BlockStart, uint64(value.BlockSize), uint64(value.FieldCount), n, base} {
		pos += binary.PutUvarint(out[pos:], number)
	}
	out[pos], out[pos+1] = generationWidth, fieldWidth
	pos += 2
	bitmap := out[pos : pos+int(bitmapSize)]
	pos += int(bitmapSize)
	generations := out[pos : pos+int(n)*int(generationWidth)]
	pos += len(generations)
	fields := out[pos:]
	for i, entry := range entries {
		bitmap[i/8] |= entry.Flags << uint(i%8)
		putTextV2DenseNormFixed(generations, i*int(generationWidth), generationWidth, uint32(entry.Generation-base))
		for j, length := range entry.FieldLengths {
			putTextV2DenseNormFixed(fields, (i*int(value.FieldCount)+j)*int(fieldWidth), fieldWidth, length)
		}
	}
	return out, true
}

func putTextV2DenseNormFixed(raw []byte, offset int, width byte, value uint32) {
	switch width {
	case 1:
		raw[offset] = byte(value)
	case 2:
		binary.LittleEndian.PutUint16(raw[offset:], uint16(value))
	case 4:
		binary.LittleEndian.PutUint32(raw[offset:], value)
	}
}

func readTextV2DenseNormFixed(raw []byte, offset int, width byte) uint32 {
	switch width {
	case 0:
		return 0
	case 1:
		return uint32(raw[offset])
	case 2:
		return uint32(binary.LittleEndian.Uint16(raw[offset:]))
	default: // The complete layout proof restricts the remaining case to 4.
		return binary.LittleEndian.Uint32(raw[offset:])
	}
}

// This borrowed descriptor is confined to synchronous decoding. It proves
// legality for ALL slots, including excluded entries, before any output make.
// Fixed-width lanes have no invalid uint32 values. A positive base plus the
// maximum REPRESENTABLE delta proves every generation nonzero/nonoverflowing;
// checking only observed deltas would incorrectly require an excluded-slot scan.
type textV2DenseNormLayout struct {
	start, base                  uint64
	size, fields                 uint32
	count                        int
	generationWidth, fieldWidth  byte
	bitmap, generations, lengths []byte
}

func parseTextV2DenseNormLayout(raw []byte) (textV2DenseNormLayout, error) {
	var layout textV2DenseNormLayout
	cur, err := textV2ValueCursor(raw, textV2DenseNormBlockValueVersion, "dense norm block")
	if err != nil {
		return layout, err
	}
	var header [5]uint64
	for i := range header {
		header[i], err = cur.readUvarint()
		if err != nil {
			return layout, errMalformedTextStorage("text-v2 dense norm header[%d]: %v", i, err)
		}
	}
	start, size, fields, count, base := header[0], header[1], header[2], header[3], header[4]
	if err := validateTextV2BlockHeader(start, size, checkedTextUint32(size), "dense norm"); err != nil {
		return layout, err
	}
	if start > ^uint64(0)-size || fields == 0 || fields > 1<<32-1 || count == 0 || count > size || base == 0 {
		return layout, errMalformedTextStorage("text-v2 dense norm range/count/field/generation invalid")
	}
	if cur.remaining() < 2 {
		return layout, errMalformedTextStorage("text-v2 dense norm missing widths")
	}
	generationWidth, fieldWidth := cur.buf[cur.pos], cur.buf[cur.pos+1]
	cur.pos += 2
	generationMax, validGenerationWidth := textV2DenseNormWidthMax(generationWidth)
	_, validFieldWidth := textV2DenseNormWidthMax(fieldWidth)
	if !validGenerationWidth || !validFieldWidth || fieldWidth == 0 || base > ^uint64(0)-generationMax {
		return layout, errMalformedTextStorage("text-v2 dense norm widths/generation range invalid")
	}
	// Subtract/divide against the existing input before multiplying. This
	// proves every size fits int as well as rejecting huge malformed headers.
	remaining := uint64(cur.remaining())
	bitmapSize := (count + 7) / 8
	if bitmapSize > remaining {
		return layout, errMalformedTextStorage("text-v2 dense norm short bitmap")
	}
	remaining -= bitmapSize
	if generationWidth != 0 && count > remaining/uint64(generationWidth) {
		return layout, errMalformedTextStorage("text-v2 dense norm short generations")
	}
	generationBytes := count * uint64(generationWidth)
	remaining -= generationBytes
	if count > remaining/fields/uint64(fieldWidth) || count*fields*uint64(fieldWidth) != remaining {
		return layout, errMalformedTextStorage("text-v2 dense norm payload size mismatch")
	}
	bitmap := cur.buf[cur.pos : cur.pos+int(bitmapSize)]
	if used := count % 8; used != 0 && bitmap[len(bitmap)-1]&^byte((uint16(1)<<used)-1) != 0 {
		return layout, errMalformedTextStorage("text-v2 dense norm unused tombstone bits")
	}
	cur.pos += int(bitmapSize)
	generations := cur.buf[cur.pos : cur.pos+int(generationBytes)]
	cur.pos += int(generationBytes)
	return textV2DenseNormLayout{start: start, base: base, size: uint32(size), fields: uint32(fields), count: int(count), generationWidth: generationWidth, fieldWidth: fieldWidth, bitmap: bitmap, generations: generations, lengths: cur.buf[cur.pos:]}, nil
}

func (b textV2DenseNormLayout) generation(i int) uint64 {
	return b.base + uint64(readTextV2DenseNormFixed(b.generations, i*int(b.generationWidth), b.generationWidth))
}

func (b textV2DenseNormLayout) flags(i int) byte {
	return (b.bitmap[i/8] >> uint(i%8)) & textV2DocFlagTombstone
}

func (b textV2DenseNormLayout) appendFields(dst []uint32, i int) []uint32 {
	offset := i * int(b.fields) * int(b.fieldWidth)
	for field := uint32(0); field < b.fields; field++ {
		dst = append(dst, readTextV2DenseNormFixed(b.lengths, offset, b.fieldWidth))
		offset += int(b.fieldWidth)
	}
	return dst
}

func decodeTextV2DenseNormBlock(raw []byte) (textV2NormBlockValue, error) {
	b, err := parseTextV2DenseNormLayout(raw)
	if err != nil {
		return textV2NormBlockValue{}, err
	}
	out := textV2NormBlockValue{BlockStart: b.start, BlockSize: b.size, FieldCount: b.fields, Entries: make([]textV2NormBlockEntry, 0, b.count)}
	for i := 0; i < b.count; i++ {
		lengths := b.appendFields(make([]uint32, 0, int(b.fields)), i)
		out.Entries = append(out.Entries, textV2NormBlockEntry{Ordinal: b.start + uint64(i), Generation: b.generation(i), Flags: b.flags(i), FieldLengths: lengths})
	}
	return out, nil
}

func decodeTextV2SearchDenseNormBlockSelected(raw []byte, allowed *textV2SearchOrdinalAllowSet) (textV2SearchNormBlock, error) {
	b, err := parseTextV2DenseNormLayout(raw)
	if err != nil {
		return textV2SearchNormBlock{}, err
	}
	all := allowed == nil || allowed.all
	retained := b.count
	var ordinals []uint64
	if !all {
		first, end, _ := allowed.rangeBounds(b.start, b.start+uint64(b.count)-1)
		ordinals = allowed.sorted[first:end]
		retained = len(ordinals)
	}
	out := textV2SearchNormBlock{BlockStart: b.start, BlockSize: b.size, FieldCount: b.fields, Entries: make([]textV2SearchNormEntry, 0, retained), FieldLengths: make([]uint32, 0, retained*int(b.fields))}
	for at := 0; at < retained; at++ {
		i := at
		if !all {
			i = int(ordinals[at] - b.start)
		}
		offset := len(out.FieldLengths)
		out.FieldLengths = b.appendFields(out.FieldLengths, i)
		out.Entries = append(out.Entries, textV2SearchNormEntry{Ordinal: b.start + uint64(i), Generation: b.generation(i), Flags: b.flags(i), FieldLengths: out.FieldLengths[offset:len(out.FieldLengths)]})
	}
	return out, nil
}
