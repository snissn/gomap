package valuelog

import (
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

// The maintained record inspector is the characterization oracle for every
// check and EOF boundary. The optimized leaf window may read extra bytes, but
// must return the identical shape or error and keep validating warmed headers.
func TestCOWLeafInspectionWindowMatchesRecordInspector(t *testing.T) {
	leaf := buildSparseLeafPageForPayloadTest(t)
	compact, ok, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil || !ok {
		t.Fatalf("compact=%t error=%v", ok, err)
	}
	for _, fixture := range []struct {
		name  string
		value []byte
		codec BlockCodec
	}{
		{"page", leaf, BlockCodecNone},
		{"compact", compact, BlockCodecNone},
		{"snappy", compact, BlockCodecSnappy},
		{"lz4", compact, BlockCodecLZ4},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			f, ptr := cowTestLeafRecord(t, fixture.value, fixture.codec)
			original, err := os.ReadFile(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			compare := func(name string, limits COWReadLimits) {
				t.Helper()
				limits.MaxRecordBytes = min(limits.MaxRecordBytes, 2<<20)
				limits.MaxRawBytes = min(limits.MaxRawBytes, MaxFrameK*page.PageSize)
				limits.MaxValueBytes = min(limits.MaxValueBytes, page.PageSize)
				want, wantErr := inspectCOWRecord(f, ptr, limits, true)
				got, gotErr := InspectCOWLeafRecord(f, ptr, limits)
				if (wantErr == nil) != (gotErr == nil) || (wantErr != nil && wantErr.Error() != gotErr.Error()) || (wantErr == nil && want != got) {
					t.Fatalf("%s: shape equal=%t optimized error=%v original error=%v", name, want == got, gotErr, wantErr)
				}
			}
			compare("complete", cowTestLimits)
			for _, limits := range []COWReadLimits{
				{}, {1, 1, 1}, {1 << 20, 1, 1 << 20}, {1 << 20, 1 << 20, 1},
			} {
				compare(fmt.Sprintf("limits=%+v", limits), limits)
			}
			// The small compressed record ends inside the maximum window. A
			// complete frame prefix must still work while partial prefixes fail.
			for end := 0; end <= min(len(original), HeaderSize+FrameHeaderSize+24); end++ {
				if err := os.WriteFile(f.Name(), original[:end], 0600); err != nil {
					t.Fatal(err)
				}
				compare(fmt.Sprintf("truncate=%d", end), cowTestLimits)
			}
			if err := os.WriteFile(f.Name(), original, 0600); err != nil {
				t.Fatal(err)
			}
			for at := 0; at < min(len(original), HeaderSize+FrameHeaderSize+20); at++ {
				changed := append([]byte(nil), original...)
				changed[at] ^= 0x80
				if err := os.WriteFile(f.Name(), changed, 0600); err != nil {
					t.Fatal(err)
				}
				compare(fmt.Sprintf("corrupt=%d", at), cowTestLimits)
			}
		})
	}
}
