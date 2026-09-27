package commitlog

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
	"unicode/utf8"
)

const collectionTypedMetadataPrefixSize = 2 + 8 + 4 + 4

// CollectionTypedMetadataDocument is a logical metadata after-image. Retained
// contains the primary residual JSON after-image; Values correspond to the
// payload's declared metadata Columns.
type CollectionTypedMetadataDocument struct {
	ID, Retained []byte
	Values       []string
}

type CollectionTypedMetadataPayload struct {
	Collection string
	SchemaHash uint64
	Columns    []string
	Documents  []CollectionTypedMetadataDocument
}

func validateCollectionTypedMetadata(p CollectionTypedMetadataPayload) error {
	if p.Collection == "" || !utf8.ValidString(p.Collection) || p.SchemaHash == 0 || len(p.Documents) == 0 {
		return ErrCorrupt
	}
	if commandFrameIntExceedsUint32(len(p.Columns)) || commandFrameIntExceedsUint32(len(p.Documents)) {
		return ErrRecordTooLarge
	}
	for i, name := range p.Columns {
		if name == "" || !utf8.ValidString(name) || i > 0 && p.Columns[i-1] >= name {
			return ErrCorrupt
		}
		if commandFrameIntExceedsUint32(len(name)) {
			return ErrRecordTooLarge
		}
	}
	for _, d := range p.Documents {
		if len(d.ID) == 0 || len(d.Values) != len(p.Columns) {
			return ErrCorrupt
		}
		if commandFrameIntExceedsUint32(len(d.ID)) || commandFrameIntExceedsUint32(len(d.Retained)) {
			return ErrRecordTooLarge
		}
		for _, value := range d.Values {
			if !utf8.ValidString(value) {
				return ErrCorrupt
			}
			if commandFrameIntExceedsUint32(len(value)) {
				return ErrRecordTooLarge
			}
		}
	}
	return nil
}

// EncodeCollectionTypedMetadataPayload canonicalizes documents by ID. Columns
// must already be in lexical order because Values correspond positionally.
func EncodeCollectionTypedMetadataPayload(p CollectionTypedMetadataPayload) ([]byte, error) {
	if err := validateCollectionTypedMetadata(p); err != nil {
		return nil, err
	}
	rows := p.Documents
	if !sort.SliceIsSorted(rows, func(i, j int) bool { return bytes.Compare(rows[i].ID, rows[j].ID) < 0 }) {
		rows = append([]CollectionTypedMetadataDocument(nil), rows...)
		sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].ID, rows[j].ID) < 0 })
	}
	for i := 1; i < len(rows); i++ {
		if bytes.Equal(rows[i-1].ID, rows[i].ID) {
			return nil, ErrCorrupt
		}
	}

	total := uint64(collectionTypedMetadataPrefixSize+4) + uint64(len(p.Collection))
	for _, name := range p.Columns {
		total += 4 + uint64(len(name))
	}
	for _, d := range rows {
		total += 8 + uint64(len(d.ID)) + uint64(len(d.Retained))
		for _, value := range d.Values {
			total += 4 + uint64(len(value))
		}
	}
	if total > math.MaxUint32 || total > uint64(int(^uint(0)>>1)) {
		return nil, ErrRecordTooLarge
	}
	if _, err := addCommandFrameEncodedSectionLen(0, int(total)); err != nil {
		return nil, err
	}

	out := make([]byte, 0, int(total))
	out = binary.LittleEndian.AppendUint16(out, 1)
	out = binary.LittleEndian.AppendUint64(out, p.SchemaHash)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(p.Columns)))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(rows)))
	out = appendTypedCommandBytes(out, []byte(p.Collection))
	for _, name := range p.Columns {
		out = appendTypedCommandBytes(out, []byte(name))
	}
	for _, d := range rows {
		out = appendTypedCommandBytes(out, d.ID)
		out = appendTypedCommandBytes(out, d.Retained)
		for _, value := range d.Values {
			out = appendTypedCommandBytes(out, []byte(value))
		}
	}
	return out, nil
}

// DecodeCollectionTypedMetadataPayload returns data owned independently of raw.
func DecodeCollectionTypedMetadataPayload(raw []byte) (CollectionTypedMetadataPayload, error) {
	if err := validateCollectionTypedMetadataPayload(raw); err != nil {
		return CollectionTypedMetadataPayload{}, err
	}
	p := CollectionTypedMetadataPayload{SchemaHash: binary.LittleEndian.Uint64(raw[2:])}
	columnCount := int(binary.LittleEndian.Uint32(raw[10:]))
	rowCount := int(binary.LittleEndian.Uint32(raw[14:]))
	raw = raw[collectionTypedMetadataPrefixSize:]
	readBytes := func() []byte {
		n := int(binary.LittleEndian.Uint32(raw))
		raw = raw[4:]
		value := raw[:n]
		raw = raw[n:]
		return value
	}
	p.Collection = string(readBytes())
	p.Columns = make([]string, columnCount)
	for i := range p.Columns {
		p.Columns[i] = string(readBytes())
	}
	p.Documents = make([]CollectionTypedMetadataDocument, rowCount)
	values := make([]string, rowCount*columnCount)
	for i := range p.Documents {
		id := readBytes()
		retained := readBytes()
		d := CollectionTypedMetadataDocument{
			ID:       bytes.Clone(id),
			Retained: bytes.Clone(retained),
			Values:   values[i*columnCount : (i+1)*columnCount],
		}
		for j := range d.Values {
			d.Values[j] = string(readBytes())
		}
		p.Documents[i] = d
	}
	return p, nil
}

// Validation on the append/recovery boundary does not materialize rows.
func validateCollectionTypedMetadataPayload(raw []byte) error {
	if len(raw) < collectionTypedMetadataPrefixSize+4 {
		return ErrCorrupt
	}
	if binary.LittleEndian.Uint16(raw) != 1 {
		return ErrCommandWALUnsupportedVersion
	}
	if binary.LittleEndian.Uint64(raw[2:]) == 0 {
		return ErrCorrupt
	}
	columns := uint64(binary.LittleEndian.Uint32(raw[10:]))
	rows := uint64(binary.LittleEndian.Uint32(raw[14:]))
	if rows == 0 || columns > uint64(len(raw))/4 || rows > uint64(len(raw))/8 || rows*columns > uint64(len(raw))/4 {
		return ErrCorrupt
	}
	raw = raw[collectionTypedMetadataPrefixSize:]
	readBytes := func() ([]byte, bool) {
		if len(raw) < 4 {
			return nil, false
		}
		n := uint64(binary.LittleEndian.Uint32(raw))
		raw = raw[4:]
		if n > uint64(len(raw)) {
			return nil, false
		}
		value := raw[:int(n)]
		raw = raw[int(n):]
		return value, true
	}
	collection, ok := readBytes()
	if !ok || len(collection) == 0 || !utf8.Valid(collection) {
		return ErrCorrupt
	}
	var previousName []byte
	for i := uint64(0); i < columns; i++ {
		name, ok := readBytes()
		if !ok || len(name) == 0 || !utf8.Valid(name) || i > 0 && bytes.Compare(previousName, name) >= 0 {
			return ErrCorrupt
		}
		previousName = name
	}
	var previousID []byte
	for i := uint64(0); i < rows; i++ {
		id, ok := readBytes()
		if !ok || len(id) == 0 || i > 0 && bytes.Compare(previousID, id) >= 0 {
			return ErrCorrupt
		}
		previousID = id
		if _, ok := readBytes(); !ok {
			return ErrCorrupt
		}
		for j := uint64(0); j < columns; j++ {
			value, ok := readBytes()
			if !ok || !utf8.Valid(value) {
				return ErrCorrupt
			}
		}
	}
	if len(raw) != 0 {
		return ErrCorrupt
	}
	return nil
}
