package valuelog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWNativeBlockOwnerAvoidsZSTDState(t *testing.T) {
	for _, codec := range []BlockCodec{BlockCodecSnappy, BlockCodecLZ4} {
		t.Run(fmt.Sprint(codec), func(t *testing.T) {
			value := bytes.Repeat([]byte("bounded-native-block-value"), 256)
			path := filepath.Join(t.TempDir(), "value.log")
			writer, err := NewWriter(path, page.ValueLogFileID(1))
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			writer.SetBlockCompression(codec, true)
			ptrs, _, err := writer.AppendFrameWithStatsInto(0, nil, []Record{{RID: 1, Value: value}}, make([]page.ValuePtr, 1))
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Flush(); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			shape, err := InspectCOWRecord(f, ptrs[0], cowTestLimits)
			if err != nil {
				t.Fatal(err)
			}
			if !shape.Compressed || shape.Codec != codec || shape.DictID != 0 {
				t.Fatalf("actual native header: %+v", shape)
			}
			admitted := false
			decoder, err := NewCOWBlockDecoder(codec, shape.RawBytes, shape.PayloadBytes(), func(s COWDecoderAllocationSizes) error {
				admitted = true
				if s.DefinitionCopy != 0 || s.SequenceBacking != 0 || s.FixedAllocationCount != 0 || s.RawBacking != shape.RawBytes || s.InputBacking != shape.PayloadBytes() {
					t.Fatalf("native owner has zstd backing: %+v", s)
				}
				return nil
			})
			if err != nil || !admitted {
				t.Fatalf("admission=%t err=%v", admitted, err)
			}
			defer decoder.Close()
			if decoder.decoder != nil {
				t.Fatal("native codec constructed zstd")
			}
			input, raw, output := make([]byte, shape.PayloadBytes()), make([]byte, shape.RawBytes), make([]byte, len(value))
			got, err := ReadCOWRecord(f, ptrs[0], shape, true, input, raw, output, decoder.Decode)
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("native read len=%d err=%v", len(got), err)
			}
			frame := shape.frame
			frame.Reserved = byte(BlockCodecZSTD)
			if _, err := decoder.Decode(frame, input[:0], raw); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("wrong codec accepted: %v", err)
			}
			decoder.Close()
			if _, err := ReadCOWRecord(f, ptrs[0], shape, true, input, raw, output, decoder.Decode); !errors.Is(err, ErrCOWDecoderClosed) {
				t.Fatalf("closed owner: %v", err)
			}
		})
	}
}
