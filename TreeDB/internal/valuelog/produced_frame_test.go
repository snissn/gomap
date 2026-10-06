package valuelog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

// Compare metadata delivered by the producer against the bytes actually
// visible on its file, including raw fallback carrying a dictionary ID.
func TestCOWProducedFrameObserverMatchesEncodedFrames(t *testing.T) {
	for _, route := range []string{"point", "group", "raw-dict", "encoded-one", "encoded-group", "buffered", "writev", "block"} {
		t.Run(route, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "value-000001.log")
			w, err := NewWriter(path, page.ValueLogFileID(1))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			var observed []struct {
				header FrameHeader
				ptr    page.ValuePtr
				count  int
			}
			old := w.SwapProducedFrameObserver(func(h FrameHeader, p page.ValuePtr, n int) {
				observed = append(observed, struct {
					header FrameHeader
					ptr    page.ValuePtr
					count  int
				}{h, p, n})
			})
			records := []Record{{RID: 1, Value: bytes.Repeat([]byte("abc"), 1024)}, {RID: 2, Value: []byte("second")}, {RID: 3, Value: []byte("third")}}
			dst := make([]page.ValuePtr, len(records))
			switch route {
			case "point":
				_, err = w.Append(0, nil, 1, records[0].Value)
			case "group":
				_, _, err = w.AppendFrameWithStatsInto(0, nil, records, dst)
			case "raw-dict":
				w.skipDictID, w.skipRemain = 73, 1
				for i := range records {
					records[i].Value = []byte{byte(i + 1)}
				}
				_, _, err = w.AppendFrameWithStatsInto(73, bytes.Repeat([]byte("dictionary"), 128), records, dst)
			case "encoded-one", "encoded-group":
				n := len(records)
				if route == "encoded-one" {
					n = 1
				}
				var body []byte
				body, _, err = EncodeFrame(0, nil, records[:n])
				if err == nil {
					if n == 1 {
						_, err = w.AppendEncodedFrameOne(body)
					} else {
						_, err = w.AppendEncodedFrameInto(body, n, dst)
					}
				}
			case "buffered":
				_, _, err = w.AppendRawFramesBufferedInto(records, 2, dst)
			case "writev":
				_, _, err = w.AppendRawFramesWritevInto(records, 2, dst)
			case "block":
				w.SetBlockCompression(BlockCodecSnappy, true)
				_, _, err = w.AppendFrameWithStatsInto(0, nil, records, dst)
			}
			w.SwapProducedFrameObserver(old)
			if err != nil {
				t.Fatal(err)
			}
			if len(observed) == 0 {
				t.Fatal("successful producer delivered no frame")
			}
			if err = w.Flush(); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			for _, got := range observed {
				shape, err := InspectCOWRecord(f, got.ptr, cowTestLimits)
				if err != nil {
					t.Fatal(err)
				}
				if shape.frame != got.header && page.ValuePtrIsGrouped(got.ptr) {
					t.Fatalf("actual header %+v != producer %+v", shape.frame, got.header)
				}
				if got.header.K != uint8(got.count) {
					t.Fatal("record count mismatch")
				}
				if route == "raw-dict" && (got.header.DictID != 73 || got.header.Flags&FrameFlagCompressed != 0) {
					t.Fatalf("raw dependency was guessed: %+v", got.header)
				}
			}
			before := len(observed)
			if _, err = w.Append(0, nil, 4, []byte("after scope")); err != nil {
				t.Fatal(err)
			}
			if len(observed) != before {
				t.Fatal("observer escaped append scope")
			}
		})
	}
}

func TestCOWProducedFrameObserverRejectsBeforeObservation(t *testing.T) {
	w, err := NewWriter(filepath.Join(t.TempDir(), "value-000001.log"), page.ValueLogFileID(1))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	calls := 0
	old := w.SwapProducedFrameObserver(func(FrameHeader, page.ValuePtr, int) { calls++ })
	_, err = w.AppendEncodedFrameInto([]byte{0}, 1, make([]page.ValuePtr, 1))
	w.SwapProducedFrameObserver(old)
	if err == nil || calls != 0 {
		t.Fatalf("invalid append: err=%v observations=%d", err, calls)
	}
}
