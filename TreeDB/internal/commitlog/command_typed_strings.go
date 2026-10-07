package commitlog

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"unicode/utf8"
	"unsafe"
)

type CollectionTypedStringEdit struct {
	Column uint32
	Value  string
}
type CollectionTypedStringPatch struct {
	ID                                              []byte
	Generation, PartID, RowIndex, AppliedCommandLSN uint64
	Edits                                           []CollectionTypedStringEdit
	ReplaceResidual                                 bool
	Residual                                        []byte
}
type CollectionTypedStringsPayload struct {
	Collection  string
	SchemaHash  uint64
	SchemaCount uint32
	Documents   []CollectionTypedStringPatch
}

// EncodeCollectionTypedStringsPayload owns one canonical logical command. It
// records the exact preceding revision and edited schema ordinals, never an
// unchanged declared after-image or reconstructed full document.
func EncodeCollectionTypedStringsPayload(p CollectionTypedStringsPayload) ([]byte, error) {
	if p.Collection == "" || !utf8.ValidString(p.Collection) || p.SchemaHash == 0 || p.SchemaCount == 0 || len(p.Documents) == 0 {
		return nil, ErrCorrupt
	}
	// Validate all raw dimensions before allocating the canonical document
	// table or output. A uint32 section cannot contain more than total/45 rows.
	if uint64(len(p.Documents)) > uint64(math.MaxInt)/uint64(unsafe.Sizeof(CollectionTypedStringPatch{})) {
		return nil, ErrRecordTooLarge
	}
	total := uint64(22)
	add := func(n uint64) error {
		if n > uint64(math.MaxUint32)-total {
			return ErrRecordTooLarge
		}
		total += n
		return nil
	}
	if err := add(uint64(len(p.Collection))); err != nil {
		return nil, err
	}
	for _, d := range p.Documents {
		if len(d.ID) == 0 || !utf8.Valid(d.ID) || d.Generation == 0 || d.PartID == 0 || d.AppliedCommandLSN == 0 || len(d.Edits) > int(p.SchemaCount) || !d.ReplaceResidual && (len(d.Residual) != 0 || len(d.Edits) == 0) {
			return nil, ErrCorrupt
		}
		for _, n := range [...]uint64{45, uint64(len(d.ID)), uint64(len(d.Residual))} {
			if err := add(n); err != nil {
				return nil, err
			}
		}
		for j, e := range d.Edits {
			if e.Column >= p.SchemaCount || j > 0 && d.Edits[j-1].Column >= e.Column || !utf8.ValidString(e.Value) {
				return nil, ErrCorrupt
			}
			for _, n := range [...]uint64{8, uint64(len(e.Value))} {
				if err := add(n); err != nil {
					return nil, err
				}
			}
		}
	}
	if total > uint64(math.MaxInt) {
		return nil, ErrRecordTooLarge
	}
	if _, err := addCommandFrameEncodedSectionLen(0, int(total)); err != nil {
		return nil, err
	}
	rows := make([]CollectionTypedStringPatch, len(p.Documents))
	copy(rows, p.Documents)
	slices.SortFunc(rows, func(a, b CollectionTypedStringPatch) int { return bytes.Compare(a.ID, b.ID) })
	for i := 1; i < len(rows); i++ {
		if bytes.Equal(rows[i-1].ID, rows[i].ID) {
			return nil, ErrCorrupt
		}
	}
	out := make([]byte, 0, int(total))
	out = binary.LittleEndian.AppendUint16(out, 1)
	out = binary.LittleEndian.AppendUint64(out, p.SchemaHash)
	out = binary.LittleEndian.AppendUint32(out, p.SchemaCount)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(rows)))
	out = appendTypedCommandBytes(out, []byte(p.Collection))
	for _, d := range rows {
		out = appendTypedCommandBytes(out, d.ID)
		for _, n := range []uint64{d.Generation, d.PartID, d.RowIndex, d.AppliedCommandLSN} {
			out = binary.LittleEndian.AppendUint64(out, n)
		}
		out = binary.LittleEndian.AppendUint32(out, uint32(len(d.Edits)))
		for _, e := range d.Edits {
			out = binary.LittleEndian.AppendUint32(out, e.Column)
			out = appendTypedCommandBytes(out, []byte(e.Value))
		}
		if d.ReplaceResidual {
			out = append(out, 1)
		} else {
			out = append(out, 0)
		}
		out = appendTypedCommandBytes(out, d.Residual)
	}
	return out, nil
}

// scan validates every count against remaining encoded bytes before the owned
// decoder allocates. emit is nil at append/recovery validation boundaries.
func scanCollectionTypedStringsPayload(raw []byte, emit func(CollectionTypedStringPatch)) (CollectionTypedStringsPayload, error) {
	var p CollectionTypedStringsPayload
	if len(raw) < 22 {
		return p, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(raw) != 1 {
		return p, ErrCommandWALUnsupportedVersion
	}
	p.SchemaHash = binary.LittleEndian.Uint64(raw[2:])
	p.SchemaCount = binary.LittleEndian.Uint32(raw[10:])
	n := uint64(binary.LittleEndian.Uint32(raw[14:]))
	raw = raw[18:]
	if p.SchemaHash == 0 || p.SchemaCount == 0 || n == 0 || n > uint64(len(raw))/45 {
		return p, ErrCorrupt
	}
	read := func() ([]byte, bool) {
		if len(raw) < 4 {
			return nil, false
		}
		n := uint64(binary.LittleEndian.Uint32(raw))
		raw = raw[4:]
		if n > uint64(len(raw)) {
			return nil, false
		}
		b := raw[:int(n)]
		raw = raw[int(n):]
		return b, true
	}
	collection, ok := read()
	if !ok || len(collection) == 0 || !utf8.Valid(collection) {
		return p, ErrCorrupt
	}
	p.Collection = string(collection)
	var previous []byte
	for i := uint64(0); i < n; i++ {
		id, ok := read()
		if !ok || len(id) == 0 || !utf8.Valid(id) || i > 0 && bytes.Compare(previous, id) >= 0 || len(raw) < 36 {
			return p, ErrCorrupt
		}
		previous = id
		d := CollectionTypedStringPatch{Generation: binary.LittleEndian.Uint64(raw), PartID: binary.LittleEndian.Uint64(raw[8:]), RowIndex: binary.LittleEndian.Uint64(raw[16:]), AppliedCommandLSN: binary.LittleEndian.Uint64(raw[24:])}
		k := uint64(binary.LittleEndian.Uint32(raw[32:]))
		raw = raw[36:]
		if d.Generation == 0 || d.PartID == 0 || d.AppliedCommandLSN == 0 || k > uint64(p.SchemaCount) || k > uint64(len(raw))/8 {
			return p, ErrCorrupt
		}
		if emit != nil {
			d.ID = make([]byte, len(id))
			copy(d.ID, id)
			d.Edits = make([]CollectionTypedStringEdit, int(k))
		}
		var prior uint32
		for j := uint64(0); j < k; j++ {
			if len(raw) < 4 {
				return p, ErrCorrupt
			}
			ordinal := binary.LittleEndian.Uint32(raw)
			raw = raw[4:]
			value, ok := read()
			if !ok || !utf8.Valid(value) || ordinal >= p.SchemaCount || j > 0 && prior >= ordinal {
				return p, ErrCorrupt
			}
			prior = ordinal
			if emit != nil {
				d.Edits[j] = CollectionTypedStringEdit{Column: ordinal, Value: string(value)}
			}
		}
		if len(raw) < 1 || raw[0] > 1 {
			return p, ErrCorrupt
		}
		d.ReplaceResidual = raw[0] == 1
		raw = raw[1:]
		residual, ok := read()
		if !ok || !d.ReplaceResidual && (len(residual) != 0 || k == 0) {
			return p, ErrCorrupt
		}
		if emit != nil {
			d.Residual = make([]byte, len(residual))
			copy(d.Residual, residual)
			emit(d)
		}
	}
	if len(raw) != 0 {
		return p, ErrCorrupt
	}
	return p, nil
}
func validateCollectionTypedStringsPayload(raw []byte) error {
	_, err := scanCollectionTypedStringsPayload(raw, nil)
	return err
}
func DecodeCollectionTypedStringsPayload(raw []byte) (CollectionTypedStringsPayload, error) {
	if err := validateCollectionTypedStringsPayload(raw); err != nil {
		return CollectionTypedStringsPayload{}, err
	}
	docs := make([]CollectionTypedStringPatch, 0, int(binary.LittleEndian.Uint32(raw[14:])))
	p, err := scanCollectionTypedStringsPayload(raw, func(d CollectionTypedStringPatch) { docs = append(docs, d) })
	p.Documents = docs
	return p, err
}
