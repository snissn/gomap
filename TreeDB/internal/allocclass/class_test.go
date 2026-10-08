package allocclass

import "testing"

func TestClassBytesSeparateScanHeaderAndTinyBacking(t *testing.T) {
	for _, tc := range []struct {
		raw  uint64
		scan bool
		want uint64
	}{
		{0, false, 0}, {1, false, 16}, {1, true, 8}, {512, true, 512}, {513, true, 576},
		{4096, false, 4096}, {4096, true, 4864}, {32760, true, 32768}, {32761, true, 32768},
	} {
		got, err := ClassBytes(tc.raw, tc.scan)
		if err != nil || got != tc.want {
			t.Fatalf("raw=%d scan=%v got=%d want=%d err=%v", tc.raw, tc.scan, got, tc.want, err)
		}
	}
	if _, err := ClassBytes(^uint64(0), true); err == nil {
		t.Fatal("overflowing backing accepted")
	}
}
