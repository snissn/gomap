package collections

import (
	"bytes"
	"encoding/binary"
	"math"
)

// The same concrete sink counts and writes the installed binary grammar.
// A bounded write cannot grow its prepaid backing, even if input changes
// between passes. It exports no backing or authority during the count pass.
type columnPhysicalAssetEncodingSink struct {
	buffer   *bytes.Buffer
	written  uint64
	limit    uint64
	overflow bool
}

func (s *columnPhysicalAssetEncodingSink) take(n uint64) bool {
	if s.overflow || n > ^uint64(0)-s.written {
		s.overflow = true
		return false
	}
	next := s.written + n
	if s.buffer != nil && s.limit != 0 && next > s.limit {
		s.overflow = true
		return false
	}
	s.written = next
	return true
}
func (s *columnPhysicalAssetEncodingSink) writeRawBytes(v []byte) {
	if s.take(uint64(len(v))) && s.buffer != nil {
		_, _ = s.buffer.Write(v)
	}
}
func (s *columnPhysicalAssetEncodingSink) writeString(v string) {
	s.writeUint64(uint64(len(v)))
	if s.take(uint64(len(v))) && s.buffer != nil {
		_, _ = s.buffer.WriteString(v)
	}
}
func (s *columnPhysicalAssetEncodingSink) writeBytes(v []byte) {
	s.writeUint64(uint64(len(v)))
	s.writeRawBytes(v)
}
func (s *columnPhysicalAssetEncodingSink) writeUint8(v uint8) {
	if s.take(1) && s.buffer != nil {
		_ = s.buffer.WriteByte(v)
	}
}
func (s *columnPhysicalAssetEncodingSink) writeBool(v bool) {
	if v {
		s.writeUint8(1)
	} else {
		s.writeUint8(0)
	}
}
func (s *columnPhysicalAssetEncodingSink) writeUint16(v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	s.writeRawBytes(b[:])
}
func (s *columnPhysicalAssetEncodingSink) writeUint32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	s.writeRawBytes(b[:])
}
func (s *columnPhysicalAssetEncodingSink) writeUint64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	s.writeRawBytes(b[:])
}
func (s *columnPhysicalAssetEncodingSink) writeFloat32SliceWithEncoding(v []float32, encoding ColumnFixedWidthEncoding) error {
	little, err := columnFixedWidthEncodingIsLittleEndian(encoding)
	if err != nil {
		return err
	}
	s.writeUint64(uint64(len(v)))
	var b [4]byte
	for _, x := range v {
		if little {
			binary.LittleEndian.PutUint32(b[:], math.Float32bits(x))
		} else {
			binary.BigEndian.PutUint32(b[:], math.Float32bits(x))
		}
		s.writeRawBytes(b[:])
	}
	return nil
}
func (s *columnPhysicalAssetEncodingSink) writeUint32SliceWithEncoding(v []uint32, encoding ColumnFixedWidthEncoding) error {
	little, err := columnFixedWidthEncodingIsLittleEndian(encoding)
	if err != nil {
		return err
	}
	s.writeUint64(uint64(len(v)))
	var b [4]byte
	for _, x := range v {
		if little {
			binary.LittleEndian.PutUint32(b[:], x)
		} else {
			binary.BigEndian.PutUint32(b[:], x)
		}
		s.writeRawBytes(b[:])
	}
	return nil
}
func (s *columnPhysicalAssetEncodingSink) writeSparseStoredColumns(stored []bool) {
	// Emit the existing length-prefixed bitmap without a temporary heap mask.
	n := len(stored) / 8
	if len(stored)%8 != 0 {
		n++
	}
	s.writeUint64(uint64(n))
	for start := 0; start < len(stored); start += 8 {
		var mask byte
		for bit := 0; bit < 8 && start+bit < len(stored); bit++ {
			if stored[start+bit] {
				mask |= 1 << uint(bit)
			}
		}
		s.writeUint8(mask)
	}
}
func (s *columnPhysicalAssetEncodingSink) writePreservedRow(v columnRowCoordinates) {
	s.writeUint64(v.Generation)
	s.writeUint64(v.PartID)
	s.writeUint64(uint64(v.RowIndex))
	s.writeUint64(v.AppliedCommandLSN)
}

// The currently selected native scalar row family has no dynamic vector
// encoder or callback constructor. Refuse every unsupported family before
// ordinary validators can format an error or construct any output.
func requireCreditedColumnPhysicalScalarRows(input columnPhysicalAssetEncodeInput, rows columnDeclaredRowSource) error {
	owned, ok := rows.(columnDeclaredRowsSource)
	if !ok || input.Collection == "" || input.Namespace == "" || input.Generation == 0 || input.PartID == 0 || !isSupportedColumnPhysicalAssetOperation(input.Operation) {
		return ErrPreparedInsertResourceLimit
	}
	for _, col := range input.Columns {
		if col.FixedWidthEncoding != ColumnFixedWidthEncodingDefault || col.ElementsPerRow != 0 {
			return ErrPreparedInsertResourceLimit
		}
		switch col.ValueType {
		case ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueFloat32, ColumnStoreValueDouble, ColumnStoreValueString, ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32, ColumnStoreValueUint64, ColumnStoreValueFloat16, ColumnStoreValueBFloat16:
		default:
			return ErrPreparedInsertResourceLimit
		}
	}
	var metadata, sparse bool
	for i, row := range owned {
		if i == 0 {
			metadata, sparse = row.Preserved != nil, row.Stored != nil
		}
		if (row.Preserved != nil) != metadata || (row.Stored != nil) != sparse ||
			(sparse && (metadata || len(row.Stored) != len(input.Columns) || input.Operation != ColumnPublishOperationUpdate || row.Deleted)) {
			return ErrPreparedInsertResourceLimit
		}
		if metadata {
			ref := row.Preserved
			if input.Operation != ColumnPublishOperationUpdate || row.Deleted || len(row.ID) == 0 ||
				ref.Generation == 0 || ref.Generation >= input.Generation || ref.PartID == 0 || ref.RowIndex < 0 || ref.AppliedCommandLSN == 0 || ref.AppliedCommandLSN >= input.AppliedCommandLSN {
				return ErrPreparedInsertResourceLimit
			}
		}
		if input.Operation == ColumnPublishOperationDelete {
			if !row.Deleted || len(row.Values) != 0 {
				return ErrPreparedInsertResourceLimit
			}
			continue
		}
		if row.Deleted || len(row.Values) != len(input.Columns) {
			return ErrPreparedInsertResourceLimit
		}
		for j, value := range row.Values {
			col := input.Columns[j]
			if sparse && !row.Stored[j] || metadata && !columnMetadataStoredColumn(col) {
				continue
			}
			if sparse && (col.ValueType != ColumnStoreValueString || !value.Present || value.Null) ||
				!value.Present && !value.Null || value.Type != col.ValueType ||
				(!value.Present || value.Null) && !col.Nullable {
				return ErrPreparedInsertResourceLimit
			}
		}
	}
	return nil
}
