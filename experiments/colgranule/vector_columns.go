package colgranule

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

type Float32VectorColumn struct {
	Dims   int
	Values []float32
}

type AdjacencyListColumn struct {
	Offsets []uint32
	Values  []uint32
}

func (b *GranuleBuilder) BuildFloat32Vectors(values []float32, dims int) (EncodedGranule, error) {
	rows, err := validateFloat32VectorValues(values, dims)
	if err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeFloat32VectorPayload(b.raw[:0], values)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingRawFloat32Vector, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(rows, 0, 0, false, EncodingRawFloat32Vector, selection), nil
}

func (r *GranuleReader) DecodeFloat32VectorsInto(dst []float32, g EncodedGranule, dims int) ([]float32, error) {
	if g.Encoding != EncodingRawFloat32Vector {
		return nil, fmt.Errorf("colgranule: float32 vector decode got encoding %d", g.Encoding)
	}
	if dims <= 0 {
		return nil, fmt.Errorf("colgranule: invalid vector dims %d", dims)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, err
	}
	values, err := decodeFloat32VectorPayload(dst, raw, g.Rows, dims)
	if err != nil {
		return nil, err
	}
	r.vectors32 = values
	return values, nil
}

func (b *GranuleBuilder) BuildUint32AdjacencyLists(offsets []uint32, values []uint32) (EncodedGranule, error) {
	rows, err := validateAdjacencyListValues(offsets, values)
	if err != nil {
		return EncodedGranule{}, err
	}
	raw, err := encodeUint32AdjacencyListPayload(b.raw[:0], offsets, values)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.raw = raw
	selection, err := admitCompressionInto(b.compressed[:0], raw, EncodingRawUint32AdjacencyList, b.cfg.Compression)
	if err != nil {
		return EncodedGranule{}, err
	}
	b.compressed = selection.Scratch
	return newEncodedGranule(rows, 0, 0, false, EncodingRawUint32AdjacencyList, selection), nil
}

func (r *GranuleReader) DecodeUint32AdjacencyListsInto(offsetDst []uint32, valueDst []uint32, g EncodedGranule) ([]uint32, []uint32, error) {
	if g.Encoding != EncodingRawUint32AdjacencyList {
		return nil, nil, fmt.Errorf("colgranule: adjacency-list decode got encoding %d", g.Encoding)
	}
	raw, err := r.decompressPayload(g)
	if err != nil {
		return nil, nil, err
	}
	offsets, values, err := decodeUint32AdjacencyListPayload(offsetDst, valueDst, raw, g.Rows)
	if err != nil {
		return nil, nil, err
	}
	r.listOffsets = offsets
	r.listValues = values
	return offsets, values, nil
}

func validateFloat32VectorValues(values []float32, dims int) (int, error) {
	if dims <= 0 {
		return 0, fmt.Errorf("colgranule: invalid vector dims %d", dims)
	}
	if len(values) == 0 {
		return 0, errors.New("colgranule: empty vector granule")
	}
	if len(values)%dims != 0 {
		return 0, fmt.Errorf("colgranule: vector values=%d not divisible by dims=%d", len(values), dims)
	}
	rows := len(values) / dims
	if rows <= 0 {
		return 0, fmt.Errorf("colgranule: invalid vector rows %d", rows)
	}
	return rows, nil
}

func encodeFloat32VectorPayload(dst []byte, values []float32) ([]byte, error) {
	need, err := checkedMulInt(len(values), 4, "float32 vector bytes")
	if err != nil {
		return nil, err
	}
	if cap(dst) < need {
		dst = make([]byte, need)
	} else {
		dst = dst[:need]
	}
	for i, value := range values {
		binary.LittleEndian.PutUint32(dst[i*4:], math.Float32bits(value))
	}
	return dst, nil
}

func decodeFloat32VectorPayload(dst []float32, raw []byte, rows int, dims int) ([]float32, error) {
	values, err := checkedMulInt(rows, dims, "float32 vector values")
	if err != nil {
		return nil, err
	}
	need, err := checkedMulInt(values, 4, "float32 vector bytes")
	if err != nil {
		return nil, err
	}
	if len(raw) != need {
		return nil, fmt.Errorf("colgranule: float32 vector raw bytes=%d want=%d", len(raw), need)
	}
	out := ensureFloat32Len(dst, values)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
	}
	return out, nil
}

func decodeFloat32VectorRowInto(dst []float32, raw []byte, rows int, dims int, row int) ([]float32, error) {
	if row < 0 || row >= rows {
		return nil, fmt.Errorf("colgranule: vector row=%d outside rows=%d", row, rows)
	}
	values, err := checkedMulInt(rows, dims, "float32 vector values")
	if err != nil {
		return nil, err
	}
	need, err := checkedMulInt(values, 4, "float32 vector bytes")
	if err != nil {
		return nil, err
	}
	if len(raw) != need {
		return nil, fmt.Errorf("colgranule: float32 vector raw bytes=%d want=%d", len(raw), need)
	}
	out := ensureFloat32Len(dst, dims)
	offset := row * dims * 4
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[offset+i*4:]))
	}
	return out, nil
}

func validateAdjacencyListValues(offsets []uint32, values []uint32) (int, error) {
	if len(offsets) < 2 {
		return 0, errors.New("colgranule: adjacency list requires rows+1 offsets")
	}
	if len(values) > math.MaxUint32 {
		return 0, fmt.Errorf("colgranule: adjacency value count %d exceeds uint32", len(values))
	}
	if offsets[0] != 0 {
		return 0, fmt.Errorf("colgranule: adjacency first offset=%d want 0", offsets[0])
	}
	prev := uint32(0)
	for i, offset := range offsets[1:] {
		if offset < prev {
			return 0, fmt.Errorf("colgranule: adjacency offset %d=%d before previous=%d", i+1, offset, prev)
		}
		prev = offset
	}
	if int(prev) != len(values) {
		return 0, fmt.Errorf("colgranule: adjacency final offset=%d values=%d", prev, len(values))
	}
	return len(offsets) - 1, nil
}

func encodeUint32AdjacencyListPayload(dst []byte, offsets []uint32, values []uint32) ([]byte, error) {
	offsetBytes, err := checkedMulInt(len(offsets), 4, "adjacency offset bytes")
	if err != nil {
		return nil, err
	}
	valueBytes, err := checkedMulInt(len(values), 4, "adjacency value bytes")
	if err != nil {
		return nil, err
	}
	need, err := checkedAddInt(offsetBytes, valueBytes, "adjacency raw bytes")
	if err != nil {
		return nil, err
	}
	if cap(dst) < need {
		dst = make([]byte, need)
	} else {
		dst = dst[:need]
	}
	for i, offset := range offsets {
		binary.LittleEndian.PutUint32(dst[i*4:], offset)
	}
	valueStart := offsetBytes
	for i, value := range values {
		binary.LittleEndian.PutUint32(dst[valueStart+i*4:], value)
	}
	return dst, nil
}

func decodeUint32AdjacencyListPayload(offsetDst []uint32, valueDst []uint32, raw []byte, rows int) ([]uint32, []uint32, error) {
	if rows <= 0 {
		return nil, nil, fmt.Errorf("colgranule: invalid adjacency rows %d", rows)
	}
	offsetCount := rows + 1
	offsetBytes, err := checkedMulInt(offsetCount, 4, "adjacency offset bytes")
	if err != nil {
		return nil, nil, err
	}
	if len(raw) < offsetBytes {
		return nil, nil, fmt.Errorf("colgranule: adjacency raw bytes=%d below offsets=%d", len(raw), offsetBytes)
	}
	offsets := ensureUint32Len(offsetDst, offsetCount)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(raw[i*4:])
	}
	valueCount := int(offsets[len(offsets)-1])
	valueBytes, err := checkedMulInt(valueCount, 4, "adjacency value bytes")
	if err != nil {
		return nil, nil, err
	}
	need, err := checkedAddInt(offsetBytes, valueBytes, "adjacency raw bytes")
	if err != nil {
		return nil, nil, err
	}
	if len(raw) != need {
		return nil, nil, fmt.Errorf("colgranule: adjacency raw bytes=%d want=%d", len(raw), need)
	}
	values := ensureUint32Len(valueDst, valueCount)
	valueRaw := raw[offsetBytes:]
	for i := range values {
		values[i] = binary.LittleEndian.Uint32(valueRaw[i*4:])
	}
	if _, err := validateAdjacencyListValues(offsets, values); err != nil {
		return nil, nil, err
	}
	return offsets, values, nil
}

func decodeUint32AdjacencyListRowInto(dst []uint32, raw []byte, rows int, row int) ([]uint32, error) {
	if rows <= 0 {
		return nil, fmt.Errorf("colgranule: invalid adjacency rows %d", rows)
	}
	if row < 0 || row >= rows {
		return nil, fmt.Errorf("colgranule: adjacency row=%d outside rows=%d", row, rows)
	}
	offsetCount := rows + 1
	offsetBytes, err := checkedMulInt(offsetCount, 4, "adjacency offset bytes")
	if err != nil {
		return nil, err
	}
	if len(raw) < offsetBytes {
		return nil, fmt.Errorf("colgranule: adjacency raw bytes=%d below offsets=%d", len(raw), offsetBytes)
	}
	start := binary.LittleEndian.Uint32(raw[row*4:])
	end := binary.LittleEndian.Uint32(raw[(row+1)*4:])
	final := binary.LittleEndian.Uint32(raw[rows*4:])
	if start > end || end > final {
		return nil, fmt.Errorf("colgranule: invalid adjacency row offsets start=%d end=%d final=%d", start, end, final)
	}
	valueBytes, err := checkedMulInt(int(final), 4, "adjacency value bytes")
	if err != nil {
		return nil, err
	}
	need, err := checkedAddInt(offsetBytes, valueBytes, "adjacency raw bytes")
	if err != nil {
		return nil, err
	}
	if len(raw) != need {
		return nil, fmt.Errorf("colgranule: adjacency raw bytes=%d want=%d", len(raw), need)
	}
	out := ensureUint32Len(dst, int(end-start))
	valueRaw := raw[offsetBytes:]
	for i := range out {
		out[i] = binary.LittleEndian.Uint32(valueRaw[(int(start)+i)*4:])
	}
	return out, nil
}
