package limits

import "testing"

func TestMaxRecordSizeDefaultAndMutable(t *testing.T) {
	const wantDefault = 64 * 1024 * 1024
	if MaxRecordSize != wantDefault {
		t.Fatalf("MaxRecordSize=%d, want %d", MaxRecordSize, wantDefault)
	}

	old := MaxRecordSize
	t.Cleanup(func() { MaxRecordSize = old })

	MaxRecordSize = 0
	if MaxRecordSize != 0 {
		t.Fatalf("MaxRecordSize=%d, want 0", MaxRecordSize)
	}
}
