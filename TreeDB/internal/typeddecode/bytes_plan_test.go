package typeddecode

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestBytesDirectViewValidation(t *testing.T) {
	t.Parallel()
	offsets := []uint64{0, 0, 2, 5}
	values := []byte{0x00, 0xff, 'x', 0x00, 0x80}
	cert := bytesDirectViewCert(3, len(offsets)*8, len(values))
	plan := BytesPlan(cert)
	if !plan.DirectCandidate() {
		t.Fatalf("BytesPlan=%+v want direct candidate", plan)
	}
	req := BytesDirectViewRequest{Plan: plan, Certification: cert, Rows: 3, OffsetsBytes: len(offsets) * 8, ValuesBytes: len(values), AssetOffset: 0, HasAssetOffset: true}
	if status := ValidateBytesDirectView(req, offsets, values); !status.Direct() {
		t.Fatalf("ValidateBytesDirectView=%+v want direct", status)
	}

	badOffsets := append([]uint64(nil), offsets...)
	badOffsets[1] = 3
	badOffsets[2] = 1
	if status := ValidateBytesDirectView(req, badOffsets, values); status.Reason != ReasonOffsetsNonMonotonic {
		t.Fatalf("non-monotonic status=%+v want %s", status, ReasonOffsetsNonMonotonic)
	}
	shortValues := values[:len(values)-1]
	if status := ValidateBytesDirectView(req, offsets, shortValues); status.Reason != ReasonValuesLengthMismatch {
		t.Fatalf("short values status=%+v want %s", status, ReasonValuesLengthMismatch)
	}
	badReq := req
	badReq.AssetOffset = 1
	if status := ValidateBytesDirectViewSections(badReq); status.Reason != ReasonAbsoluteOffsetUnaligned {
		t.Fatalf("misaligned status=%+v want %s", status, ReasonAbsoluteOffsetUnaligned)
	}
}

func TestBytesPlanRejectsTextOrNullableSemantics(t *testing.T) {
	t.Parallel()
	cert := bytesDirectViewCert(1, 16, 1)
	text := cert
	text.LogicalType = string(columnsemantics.LogicalString)
	if plan := BytesPlan(text); plan.Path != PathUnsupported || plan.Reason != ReasonValidationFailed {
		t.Fatalf("string bytes plan=%+v want unsupported validation failure", plan)
	}
	nullable := cert
	nullable.NullMaskPresent = true
	nullable.NullCount = 1
	if status := ValidateBytesDirectViewSections(BytesDirectViewRequest{Plan: BytesPlan(nullable), Certification: nullable, Rows: 1, OffsetsBytes: 16, ValuesBytes: 1, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonNullableWrapper {
		t.Fatalf("nullable status=%+v want %s", status, ReasonNullableWrapper)
	}
}

func bytesDirectViewCert(rows int, offsetsBytes int, valuesBytes int) typedcolumn.ColumnPartLayoutContractColumn {
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name:                "opaque",
		LogicalType:         string(columnsemantics.LogicalBytes),
		Type:                typedcolumn.ColumnTypeBytes,
		Encoding:            typedcolumn.EncodingRawBytesOffsets,
		Compression:         typedcolumn.CompressionNone,
		Rows:                rows,
		OffsetsSection:      typedcolumn.ColumnPartLayoutContractSection{Offset: 8, Length: offsetsBytes},
		ValuesSection:       typedcolumn.ColumnPartLayoutContractSection{Offset: 8 + offsetsBytes, Length: valuesBytes},
		OffsetsBytes:        offsetsBytes,
		ValuesBytes:         valuesBytes,
		ElementSize:         1,
		Alignment:           1,
		Endian:              typedcolumn.ColumnPartLayoutEndianLittle,
		LengthMultiple:      1,
		DirectViewCertified: true,
	}
}
