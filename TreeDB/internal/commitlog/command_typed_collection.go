package commitlog

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
)

// Typed collection payloads retain accepted values independently of JSON.
const (
	CollectionTypedString        uint8 = 1
	CollectionTypedFloat32Vector uint8 = 2
)

type CollectionTypedColumn struct {
	Name       string
	Type       uint8
	Dimensions uint32
}

type CollectionTypedValue struct {
	String string
	Vector []float32
}

type CollectionTypedDocument struct {
	ID, Retained []byte
	Values       []CollectionTypedValue
}

type CollectionTypedBatchPayload struct {
	LegacyProjection bool
	Collection       string
	SchemaHash       uint64
	Columns          []CollectionTypedColumn
	Documents        []CollectionTypedDocument
}

func validateCollectionTypedBatch(p CollectionTypedBatchPayload) error {
	if p.Collection == "" || p.SchemaHash == 0 || len(p.Columns) == 0 {
		return ErrCorrupt
	}
	if p.LegacyProjection && (len(p.Columns) != 1 || p.Columns[0].Type != CollectionTypedFloat32Vector) {
		return ErrCorrupt
	}
	for i, c := range p.Columns {
		if c.Name == "" {
			return ErrCorrupt
		}
		if i > 0 && p.Columns[i-1].Name >= c.Name {
			return ErrCorrupt
		}
		switch c.Type {
		case CollectionTypedString:
			if c.Dimensions != 0 {
				return ErrCorrupt
			}
		case CollectionTypedFloat32Vector:
			if c.Dimensions == 0 {
				return ErrCorrupt
			}
		default:
			return ErrCorrupt
		}
	}
	for _, d := range p.Documents {
		if len(d.ID) == 0 || len(d.Values) != len(p.Columns) {
			return ErrCorrupt
		}
		for i, v := range d.Values {
			if p.Columns[i].Type == CollectionTypedString {
				if len(v.Vector) != 0 {
					return ErrCorrupt
				}
				continue
			}
			if v.String != "" || uint64(len(v.Vector)) != uint64(p.Columns[i].Dimensions) {
				return ErrCorrupt
			}
			for _, f := range v.Vector {
				if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
					return ErrCorrupt
				}
			}
		}
	}
	return nil
}

// EncodeCollectionTypedBatchPayload canonicalizes rows by ID. Column order is
// lexical name order; values and IDs are copied into one owned command buffer.
func EncodeCollectionTypedBatchPayload(p CollectionTypedBatchPayload) ([]byte, error) {
	if err := validateCollectionTypedBatch(p); err != nil {
		return nil, err
	}
	rows := p.Documents
	if !sort.SliceIsSorted(rows, func(i, j int) bool { return bytes.Compare(rows[i].ID, rows[j].ID) < 0 }) {
		rows = append([]CollectionTypedDocument(nil), rows...)
		sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].ID, rows[j].ID) < 0 })
	}
	for i := 1; i < len(rows); i++ {
		if bytes.Equal(rows[i-1].ID, rows[i].ID) {
			return nil, ErrCorrupt
		}
	}
	total := uint64(23) + uint64(len(p.Collection))
	for _, c := range p.Columns {
		total += 9 + uint64(len(c.Name))
	}
	for _, d := range rows {
		total += 8 + uint64(len(d.ID)) + uint64(len(d.Retained))
		for i, v := range d.Values {
			if p.Columns[i].Type == CollectionTypedString {
				total += 4 + uint64(len(v.String))
			} else {
				total += uint64(len(v.Vector)) * 4
			}
		}
	}
	if total > math.MaxUint32 || total > uint64(int(^uint(0)>>1)) {
		return nil, ErrRecordTooLarge
	}
	if _, err := addCommandFrameEncodedSectionLen(0, int(total)); err != nil {
		return nil, err
	}
	b := make([]byte, 0, int(total))
	b = binary.LittleEndian.AppendUint16(b, 1)
	b = binary.LittleEndian.AppendUint64(b, p.SchemaHash)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(p.Columns)))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(rows)))
	flags := byte(0)
	if p.LegacyProjection {
		flags = 1
	}
	b = append(b, flags)
	b = appendTypedCommandBytes(b, []byte(p.Collection))
	for _, c := range p.Columns {
		b = appendTypedCommandBytes(b, []byte(c.Name))
		b = append(b, c.Type)
		b = binary.LittleEndian.AppendUint32(b, c.Dimensions)
	}
	for _, d := range rows {
		b = appendTypedCommandBytes(b, d.ID)
		b = appendTypedCommandBytes(b, d.Retained)
		for i, v := range d.Values {
			if p.Columns[i].Type == CollectionTypedString {
				b = appendTypedCommandBytes(b, []byte(v.String))
				continue
			}
			for _, f := range v.Vector {
				b = binary.LittleEndian.AppendUint32(b, math.Float32bits(f))
			}
		}
	}
	return b, nil
}

func appendTypedCommandBytes(dst, value []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...)
}

// DecodeCollectionTypedBatchPayload returns owned values. Bounds are validated
// before allocations, including the row/column product and fixed vector widths.
func DecodeCollectionTypedBatchPayload(raw []byte) (CollectionTypedBatchPayload, error) {
	if err := validateCollectionTypedBatchPayload(raw); err != nil {
		return CollectionTypedBatchPayload{}, err
	}
	fail := func() (CollectionTypedBatchPayload, error) { return CollectionTypedBatchPayload{}, ErrCorrupt }
	if len(raw) < 23 || binary.LittleEndian.Uint16(raw) != 1 || raw[18]&^byte(1) != 0 {
		return fail()
	}
	p := CollectionTypedBatchPayload{SchemaHash: binary.LittleEndian.Uint64(raw[2:]), LegacyProjection: raw[18]&1 != 0}
	cols, rows := uint64(binary.LittleEndian.Uint32(raw[10:])), uint64(binary.LittleEndian.Uint32(raw[14:]))
	if cols == 0 || cols > uint64(len(raw))/9 || rows > uint64(len(raw))/8 {
		return fail()
	}
	raw = raw[19:]
	readBytes := func() ([]byte, bool) {
		if len(raw) < 4 {
			return nil, false
		}
		n := uint64(binary.LittleEndian.Uint32(raw))
		raw = raw[4:]
		if n > uint64(len(raw)) {
			return nil, false
		}
		out := raw[:int(n)]
		raw = raw[int(n):]
		return out, true
	}
	name, ok := readBytes()
	if !ok {
		return fail()
	}
	p.Collection = string(name)
	p.Columns = make([]CollectionTypedColumn, int(cols))
	minRowBytes := uint64(8)
	for i := range p.Columns {
		name, ok := readBytes()
		if !ok || len(raw) < 5 {
			return fail()
		}
		c := CollectionTypedColumn{Name: string(name), Type: raw[0], Dimensions: binary.LittleEndian.Uint32(raw[1:])}
		raw = raw[5:]
		p.Columns[i] = c
		if c.Type == CollectionTypedString {
			minRowBytes += 4
		} else if c.Type == CollectionTypedFloat32Vector {
			minRowBytes += uint64(c.Dimensions) * 4
		} else {
			return fail()
		}
	}
	if err := validateCollectionTypedBatch(p); err != nil {
		return fail()
	}
	if rows > uint64(len(raw))/minRowBytes || rows*cols > uint64(len(raw))/4 {
		return fail()
	}
	p.Documents = make([]CollectionTypedDocument, int(rows))
	values := make([]CollectionTypedValue, int(rows*cols))
	vectorWidth := uint64(0)
	for _, c := range p.Columns {
		if c.Type == CollectionTypedFloat32Vector {
			vectorWidth += uint64(c.Dimensions)
		}
	}
	vectors := make([]float32, int(rows*vectorWidth))
	for i := range p.Documents {
		id, ok := readBytes()
		if !ok || len(id) == 0 {
			return fail()
		}
		if i > 0 && bytes.Compare(p.Documents[i-1].ID, id) >= 0 {
			return fail()
		}
		retained, ok := readBytes()
		if !ok {
			return fail()
		}
		d := CollectionTypedDocument{ID: bytes.Clone(id), Retained: bytes.Clone(retained), Values: values[i*int(cols) : (i+1)*int(cols)]}
		for j, c := range p.Columns {
			if c.Type == CollectionTypedString {
				v, ok := readBytes()
				if !ok {
					return fail()
				}
				d.Values[j].String = string(v)
			} else {
				n := uint64(c.Dimensions) * 4
				if n > uint64(len(raw)) {
					return fail()
				}
				v := vectors[:int(c.Dimensions):int(c.Dimensions)]
				vectors = vectors[int(c.Dimensions):]
				for k := range v {
					v[k] = math.Float32frombits(binary.LittleEndian.Uint32(raw[k*4:]))
				}
				raw = raw[int(n):]
				d.Values[j].Vector = v
			}
		}
		p.Documents[i] = d
	}
	if len(raw) != 0 {
		return fail()
	}
	if err := validateCollectionTypedBatch(p); err != nil {
		return fail()
	}
	return p, nil
}

// Validation on the append/recovery boundary never materializes typed rows.
func validateCollectionTypedBatchPayload(raw []byte) error {
	if len(raw) < 23 || binary.LittleEndian.Uint16(raw) != 1 || binary.LittleEndian.Uint64(raw[2:]) == 0 || raw[18]&^byte(1) != 0 {
		return ErrCorrupt
	}
	cols, rows := uint64(binary.LittleEndian.Uint32(raw[10:])), uint64(binary.LittleEndian.Uint32(raw[14:]))
	if cols == 0 || cols > uint64(len(raw))/9 || rows > uint64(len(raw))/8 {
		return ErrCorrupt
	}
	legacy := raw[18]&1 != 0
	if legacy && cols != 1 {
		return ErrCorrupt
	}
	raw = raw[19:]
	readBytes := func() ([]byte, bool) {
		if len(raw) < 4 {
			return nil, false
		}
		n := uint64(binary.LittleEndian.Uint32(raw))
		raw = raw[4:]
		if n > uint64(len(raw)) {
			return nil, false
		}
		v := raw[:int(n)]
		raw = raw[int(n):]
		return v, true
	}
	collection, ok := readBytes()
	if !ok || len(collection) == 0 {
		return ErrCorrupt
	}
	descriptors := raw
	var previousName []byte
	minRowBytes := uint64(8)
	for i := uint64(0); i < cols; i++ {
		name, ok := readBytes()
		if !ok || len(name) == 0 || len(raw) < 5 || (i > 0 && bytes.Compare(previousName, name) >= 0) {
			return ErrCorrupt
		}
		previousName = name
		typ, dims := raw[0], binary.LittleEndian.Uint32(raw[1:])
		if legacy && typ != CollectionTypedFloat32Vector {
			return ErrCorrupt
		}
		raw = raw[5:]
		switch typ {
		case CollectionTypedString:
			if dims != 0 {
				return ErrCorrupt
			}
			minRowBytes += 4
		case CollectionTypedFloat32Vector:
			if dims == 0 {
				return ErrCorrupt
			}
			minRowBytes += uint64(dims) * 4
		default:
			return ErrCorrupt
		}
	}
	if rows > uint64(len(raw))/minRowBytes {
		return ErrCorrupt
	}
	var previousID []byte
	for i := uint64(0); i < rows; i++ {
		id, ok := readBytes()
		if !ok || len(id) == 0 || (i > 0 && bytes.Compare(previousID, id) >= 0) {
			return ErrCorrupt
		}
		previousID = id
		if _, ok := readBytes(); !ok {
			return ErrCorrupt
		}
		schema := descriptors
		for j := uint64(0); j < cols; j++ {
			nameLen := int(binary.LittleEndian.Uint32(schema))
			schema = schema[4+nameLen:]
			typ, dims := schema[0], binary.LittleEndian.Uint32(schema[1:])
			schema = schema[5:]
			if typ == CollectionTypedString {
				if _, ok := readBytes(); !ok {
					return ErrCorrupt
				}
			} else {
				n := uint64(dims) * 4
				if n > uint64(len(raw)) {
					return ErrCorrupt
				}
				for k := 0; k < int(n); k += 4 {
					if binary.LittleEndian.Uint32(raw[k:])&0x7f800000 == 0x7f800000 {
						return ErrCorrupt
					}
				}
				raw = raw[int(n):]
			}
		}
	}
	if len(raw) != 0 {
		return ErrCorrupt
	}
	return nil
}
