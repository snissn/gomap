package valuelog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

// A stale inspected shape must retain header-error precedence over a truncated
// payload, including when CRC verification is disabled. Exercise every public
// consumer of the shared full-record reader.
func TestCOWFullReadTruncationAndHeaderPrecedence(t *testing.T) {
	for _, consumer := range []string{"read", "append", "leaf"} {
		t.Run(consumer, func(t *testing.T) {
			var f *os.File
			var ptr page.ValuePtr
			if consumer == "leaf" {
				f, ptr = cowTestLeafRecord(t, buildSparseLeafPageForPayloadTest(t), BlockCodecNone)
				writable, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = writable.Close() })
				f = writable
			} else {
				f, ptr, _ = cowTestRecord(t, cowTestFrame(0, BlockCodecNone, [][]byte{[]byte("first"), []byte("selected")}, nil), true, 1)
			}
			var shape COWRecordShape
			var err error
			if consumer == "leaf" {
				shape, err = InspectCOWLeafRecord(f, ptr, cowTestLimits)
			} else {
				shape, err = InspectCOWRecord(f, ptr, cowTestLimits)
			}
			if err != nil {
				t.Fatal(err)
			}
			original := make([]byte, shape.RecordBytes)
			if _, err := f.ReadAt(original, 0); err != nil {
				t.Fatal(err)
			}
			payload, raw, out := make([]byte, shape.PayloadBytes()), make([]byte, shape.RawBytes), make([]byte, max(int(shape.ValueBytes), page.PageSize))
			for _, mutation := range []string{"none", "version", "capacity", "length", "frame"} {
				changed := bytes.Clone(original)
				switch mutation {
				case "version":
					changed[4]++
				case "capacity":
					binary.LittleEndian.PutUint32(changed[16:20], uint32(cowTestLimits.MaxRecordBytes))
				case "length":
					binary.LittleEndian.PutUint32(changed[16:20], uint32(shape.RecordBytes-HeaderSize-1))
				case "frame":
					changed[HeaderSize]++
				}
				cuts := []int{0, 1, HeaderSize - 1, HeaderSize, HeaderSize + 1, len(changed) - 1, len(changed)}
				for _, cut := range cuts {
					t.Run(fmt.Sprintf("%s/%d", mutation, cut), func(t *testing.T) {
						if err := f.Truncate(0); err != nil {
							t.Fatal(err)
						}
						if _, err := f.WriteAt(changed[:cut], 0); err != nil {
							t.Fatal(err)
						}
						var want error
						switch {
						case cut < HeaderSize:
							want = io.EOF
						case mutation == "version" || mutation == "length":
							want = ErrCorrupt
						case mutation == "capacity":
							want = ErrCOWReadCapacity
						case cut < len(changed):
							want = io.EOF
						case mutation == "frame":
							want = ErrCorrupt
						}
						prefix := []byte("keep")
						var got []byte
						var err error
						switch consumer {
						case "append":
							got, err = AppendCOWRecord(f, ptr, shape, false, payload, raw, prefix, nil)
						case "leaf":
							got, err = ReadCOWLeafPage(f, ptr, shape, false, payload, raw, out, nil)
						default:
							got, err = ReadCOWRecord(f, ptr, shape, false, payload, raw, out, nil)
						}
						if !errors.Is(err, want) {
							t.Fatalf("got %v want %v", err, want)
						}
						if want != nil && consumer == "append" && !bytes.Equal(got, prefix) {
							t.Fatal("failed append changed destination")
						}
					})
				}
			}
		})
	}
}

func TestCOWFullReadKeepsPinnedFileAcrossPathReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows open file does not permit rename without FILE_SHARE_DELETE")
	}
	f, ptr, _ := cowTestRecord(t, []byte("original"), false, 0)
	shape, err := InspectCOWRecord(f, ptr, cowTestLimits)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.Name(), f.Name()+".pinned"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.Name(), []byte("replacement is deliberately invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := cowTestRead(t, f, ptr, shape, nil); !bytes.Equal(got, []byte("original")) {
		t.Fatalf("read replacement instead of pinned source: %q", got)
	}
}

func TestCOWFullReadNonEOFSourceErrorsMatchHeaderRead(t *testing.T) {
	for _, mode := range []string{"closed", "write-only"} {
		t.Run(mode, func(t *testing.T) {
			f, ptr, _ := cowTestRecord(t, []byte("original"), false, 0)
			shape, err := InspectCOWRecord(f, ptr, cowTestLimits)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "closed" {
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				f, err = os.OpenFile(f.Name(), os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = f.Close() })
			}
			_, want := cowReadHeader(f, ptr, make([]byte, HeaderSize), shape.limits, false)
			_, got := ReadCOWRecord(f, ptr, shape, true, make([]byte, shape.PayloadBytes()), nil, make([]byte, shape.ValueBytes), nil)
			var wantPath, gotPath *os.PathError
			if !errors.As(want, &wantPath) || !errors.As(got, &gotPath) || wantPath.Op != gotPath.Op || wantPath.Path != gotPath.Path || !errors.Is(gotPath.Err, wantPath.Err) {
				t.Fatalf("got %v want %v", got, want)
			}
		})
	}
}
