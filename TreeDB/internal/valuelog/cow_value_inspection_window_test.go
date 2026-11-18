package valuelog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/golang/snappy"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWValueInspectionWindowMatchesOriginal(t *testing.T) {
	value := bytes.Repeat([]byte("data"), 256)
	leaf := buildSparseLeafPageForPayloadTest(t)
	compact, ok, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil || !ok {
		t.Fatalf("compact=%t error=%v", ok, err)
	}
	for _, fixture := range []struct {
		name       string
		k, sub     int
		value      []byte
		compressed bool
	}{
		{"plain", 0, 0, value, false},
		{"empty", 0, 0, nil, false},
		{"single", 1, 0, value, false},
		{"compact", 0, 0, compact, false},
		{"grouped-compact", 2, 1, compact, false},
		{"compressed-small", 1, 0, value, true},
		{"compressed-k32", 32, 24, value, true},
		{"raw-k32-last", 32, 31, value, false},
		{"compressed-k255", MaxFrameK, MaxFrameK - 1, value, true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			body := fixture.value
			pref := 0
			if fixture.k > 0 {
				pref = FrameHeaderSize + fixture.k*8 + (fixture.k+1)*4
				values := make([][]byte, fixture.k)
				for i := range values {
					values[i] = fixture.value
				}
				var compressed []byte
				var codec BlockCodec
				if fixture.compressed {
					compressed = snappy.Encode(nil, bytes.Repeat(fixture.value, fixture.k))
					codec = BlockCodecSnappy
				}
				body = cowTestFrame(0, codec, values, compressed)
			}
			f, ptr, original := cowTestRecord(t, body, fixture.k > 0, uint8(fixture.sub))
			compare := func(p page.ValuePtr, limits COWReadLimits) {
				t.Helper()
				want, wantErr := inspectCOWRecord(f, p, limits, false)
				got, gotErr := InspectCOWRecord(f, p, limits)
				if !errors.Is(gotErr, wantErr) || (gotErr == nil && want != got) {
					t.Fatalf("shape equal=%t window error=%v original error=%v", want == got, gotErr, wantErr)
				}
			}
			compare(ptr, cowTestLimits)
			for _, limits := range []COWReadLimits{{}, {1, 1, 1}, {1 << 20, 1, 1 << 20}, {1 << 20, 1 << 20, 1}} {
				compare(ptr, limits)
			}
			omitted := ptr
			if fixture.k > 0 {
				omitted.Length = page.ValuePtrMarkGrouped(0, uint8(fixture.sub))
			} else {
				omitted.Length = 0
			}
			compare(omitted, cowTestLimits)
			badFlag := ptr
			if fixture.k > 0 {
				badFlag.Length = 0
			} else {
				badFlag.Length = page.ValuePtrMarkGrouped(0, 0)
			}
			compare(badFlag, cowTestLimits)

			write := func(contents []byte) {
				t.Helper()
				if err := f.Truncate(0); err != nil {
					t.Fatal(err)
				}
				if _, err := f.WriteAt(contents, 0); err != nil {
					t.Fatal(err)
				}
			}
			valueStart := HeaderSize + pref + len(fixture.value)*fixture.sub
			for _, end := range []int{
				0, HeaderSize - 1, HeaderSize, HeaderSize + FrameHeaderSize - 1,
				HeaderSize + FrameHeaderSize, 127, 128, 419, 420,
				HeaderSize + pref - 1, HeaderSize + pref,
				valueStart, valueStart + compactLeafPagePayloadHeaderSize - 1,
				valueStart + compactLeafPagePayloadHeaderSize, len(original) - 1,
			} {
				if end <= len(original) {
					write(original[:end])
					compare(ptr, cowTestLimits)
					compare(omitted, cowTestLimits)
				}
			}
			for _, at := range []int{0, 4, 5, 7, 8, 16, 19, HeaderSize, HeaderSize + 1, HeaderSize + 2, HeaderSize + 3, HeaderSize + pref - 1} {
				if at < len(original) {
					changed := bytes.Clone(original)
					changed[at] ^= 0x80
					write(changed)
					compare(ptr, cowTestLimits)
					compare(omitted, cowTestLimits)
				}
			}
			changed := bytes.Clone(original)
			binary.LittleEndian.PutUint32(changed[16:20], uint32(cowTestLimits.MaxRecordBytes))
			write(changed[:HeaderSize])
			compare(ptr, cowTestLimits)
		})
	}
}

func TestCOWValueInspectionWindowErrorAndVerifiedDecode(t *testing.T) {
	f, ptr, record := cowTestRecord(t, []byte("immutable plain payload"), false, 0)
	valid, err := InspectCOWRecord(f, ptr, cowTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	if got := cowTestRead(t, f, ptr, valid, nil); string(got) != "immutable plain payload" {
		t.Fatalf("decoded value %q", got)
	}
	record[len(record)-1] ^= 1
	if _, err := f.WriteAt(record, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := InspectCOWRecord(f, ptr, cowTestLimits); err != nil || got != valid {
		t.Fatalf("changed underlying payload revised inspection: equal=%t error=%v", got == valid, err)
	}
	if _, err := ReadCOWRecord(f, ptr, valid, true, make([]byte, valid.PayloadBytes()), nil, make([]byte, valid.ValueBytes), nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verified decode accepted corrupt payload: %v", err)
	}
	if err := f.Truncate(HeaderSize); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectCOWRecord(f, ptr, cowTestLimits); !errors.Is(err, io.EOF) {
		t.Fatalf("missing value encoding prefix: %v", err)
	}
	writeOnly, err := os.OpenFile(f.Name(), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer writeOnly.Close()
	_, want := inspectCOWRecord(writeOnly, ptr, cowTestLimits, false)
	_, got := InspectCOWRecord(writeOnly, ptr, cowTestLimits)
	var wantPath, gotPath *os.PathError
	if !errors.As(want, &wantPath) || !errors.As(got, &gotPath) || wantPath.Op != gotPath.Op || wantPath.Path != gotPath.Path || !errors.Is(gotPath.Err, wantPath.Err) {
		t.Fatalf("write-only source error changed: got=%v want=%v", got, want)
	}
}
