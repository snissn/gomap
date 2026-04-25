package page

import "testing"

func TestLeafRef_EncodeDecode_RoundTrip(t *testing.T) {
	tests := []LeafLogPtr{
		{FileID: 1, Offset: 0},
		{FileID: 1, Offset: 123},
		{FileID: 1234, Offset: uint64(^uint32(0))},
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
		vptr := out.ValuePtr()
		if !IsValueLogFileID(vptr.FileID) {
			t.Fatalf("LeafRef ValuePtr should target value-log namespace: %+v", vptr)
		}
		if !ValuePtrIsGrouped(vptr) {
			t.Fatalf("LeafRef ValuePtr should be grouped: %+v", vptr)
		}
		if ValuePtrSubIndex(vptr) != 0 {
			t.Fatalf("LeafRef sub-index mismatch: got=%d want=0 (%+v)", ValuePtrSubIndex(vptr), vptr)
		}
		if ValuePtrRecordLength(vptr) != 0 {
			t.Fatalf("LeafRef record length hint should be 0: got=%d (%+v)", ValuePtrRecordLength(vptr), vptr)
		}
	}
}

func TestLeafRef_EncodeRejectsFileIDOverflow(t *testing.T) {
	if _, err := EncodeLeafRef(LeafLogPtr{FileID: 1 << 31, Offset: 0}); err == nil {
		t.Fatalf("expected EncodeLeafRef to reject 31-bit overflow file id")
	}
}

func TestLeafRef_EncodeRejectsOffsetOverflow(t *testing.T) {
	if _, err := EncodeLeafRef(LeafLogPtr{FileID: 1, Offset: uint64(^uint32(0)) + 1}); err == nil {
		t.Fatalf("expected EncodeLeafRef to reject u32 overflow offset")
	}
}

func TestLeafRef_DecodeRejectsNonLeafRefID(t *testing.T) {
	id := (uint64(123) << 32) | 1
	if _, ok := DecodeLeafRef(id); ok {
		t.Fatalf("expected DecodeLeafRef ok=false for non-leafref id")
	}
}

func TestLeafLogPtrFromValuePtr_RejectsNonValueLogID(t *testing.T) {
	if _, err := LeafLogPtrFromValuePtr(ValuePtr{FileID: 123, Offset: 0}); err == nil {
		t.Fatalf("expected LeafLogPtrFromValuePtr to reject non-value-log file id")
	}
}
