package page

import "testing"

func TestLeafRef_EncodeDecode_RoundTrip(t *testing.T) {
	tests := []ValuePtr{
		{FileID: ValueLogFileID(1), Offset: 0, Length: ValuePtrMarkGrouped(0, 0)},
		{FileID: ValueLogFileID(1), Offset: 123, Length: ValuePtrMarkGrouped(0, 3)},
		{FileID: ValueLogFileID(1234), Offset: LeafRefMaxOffset, Length: ValuePtrMarkGrouped(0, 7)},
	}
	for _, in := range tests {
		id, err := EncodeLeafRef(in)
		if err != nil {
			t.Fatalf("EncodeLeafRef(%+v): %v", in, err)
		}
		out, ok := DecodeLeafRef(id)
		if !ok {
			t.Fatalf("DecodeLeafRef(%d) ok=false, want true", id)
		}
		if out.FileID != in.FileID {
			t.Fatalf("LeafRef FileID mismatch: got=%d want=%d", out.FileID, in.FileID)
		}
		if out.Offset != in.Offset {
			t.Fatalf("LeafRef Offset mismatch: got=%d want=%d", out.Offset, in.Offset)
		}
		if !ValuePtrIsGrouped(out) {
			t.Fatalf("LeafRef ptr should be grouped: %+v", out)
		}
		if ValuePtrSubIndex(out) != ValuePtrSubIndex(in) {
			t.Fatalf("LeafRef sub-index mismatch: got=%d want=%d (%+v)", ValuePtrSubIndex(out), ValuePtrSubIndex(in), out)
		}
		if ValuePtrRecordLength(out) != 0 {
			t.Fatalf("LeafRef record length hint should be 0: got=%d (%+v)", ValuePtrRecordLength(out), out)
		}
	}
}

func TestLeafRef_EncodeRejectsNonValueLogFileID(t *testing.T) {
	if _, err := EncodeLeafRef(ValuePtr{FileID: 123, Offset: 0}); err == nil {
		t.Fatalf("expected EncodeLeafRef to reject non-value-log file id")
	}
}

func TestLeafRef_EncodeRejectsOffsetOverflow(t *testing.T) {
	if _, err := EncodeLeafRef(ValuePtr{FileID: ValueLogFileID(1), Offset: LeafRefMaxOffset + 1}); err == nil {
		t.Fatalf("expected EncodeLeafRef to reject leafref offset overflow")
	}
}

func TestLeafRef_EncodeRejectsSubIndexOverflow(t *testing.T) {
	if _, err := EncodeLeafRef(ValuePtr{FileID: ValueLogFileID(1), Offset: 0, Length: ValuePtrMarkGrouped(0, 8)}); err == nil {
		t.Fatalf("expected EncodeLeafRef to reject sub-index overflow")
	}
}

func TestLeafRef_DecodeRejectsNonValueLogFileID(t *testing.T) {
	id := (uint64(123) << 32) | 1 // FileID without value-log marker.
	if _, ok := DecodeLeafRef(id); ok {
		t.Fatalf("expected DecodeLeafRef ok=false for non-value-log file id")
	}
}
