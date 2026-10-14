package valuelog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestCOWNativeValueContinuationOversizedProducer(t *testing.T) {
	for _, codec := range []BlockCodec{BlockCodecNone, BlockCodecSnappy, BlockCodecLZ4, BlockCodecZSTD} {
		t.Run(fmt.Sprint(codec), func(t *testing.T) {
			want := bytes.Repeat([]byte("actual-persistent-large-mvcc-value"), (2<<20)/33+8192)
			path := filepath.Join(t.TempDir(), "value.log")
			writer, err := NewWriter(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			writer.SetBlockCompression(codec, codec != BlockCodecNone)
			ptrs, _, err := writer.AppendFrameWithStats(0, nil, []Record{{RID: 1, Value: want}, {RID: 2, Value: []byte("second")}})
			if err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			limits := COWReadLimits{MaxRecordBytes: uint64(len(want) + 8192), MaxRawBytes: uint64(len(want) + 6), MaxValueBytes: uint64(len(want))}
			input, raw := make([]byte, limits.MaxRecordBytes), make([]byte, limits.MaxRawBytes)
			owner := &cowCodecTestOwner{limit: 16 << 20}
			c := COWLeafContinuation{}
			if codec == BlockCodecZSTD {
				c.ZSTD = &COWZSTDContinuation{Owner: file, Retention: owner}
			}
			for index, expected := range [][]byte{want, []byte("second")} {
				ok, err := c.BeginValue(file, ptrs[index], limits, input, raw, true, codecQuantum())
				if err != nil || !ok {
					t.Fatal(ok, err)
				}
				for turn := 0; ; turn++ {
					if turn == 100000 {
						t.Fatal("stalled")
					}
					phase, readAt, crcAt := c.phase, c.readAt, c.crcAt
					if _, done, err := c.StepWithCodec(&iterator.OrdinalScanWork{}); done || err != nil || c.phase != phase || c.readAt != readAt || c.crcAt != crcAt {
						t.Fatal("refusal changed state", err)
					}
					work := codecQuantum()
					got, done, err := c.StepWithCodec(work)
					if err != nil {
						t.Fatal(err)
					}
					if work.Records > 32 || work.Bytes > 1<<20 {
						t.Fatal("quantum exceeded")
					}
					if done {
						if !bytes.Equal(got, expected) {
							t.Fatal("full value differential")
						}
						break
					}
				}
			}
			for turn := 0; ; turn++ {
				if turn == 10000 {
					t.Fatal("cancel stalled")
				}
				done, err := c.CancelWithCodec(codecQuantum())
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if owner.live != 0 || c.f != nil || c.value != nil {
				t.Fatal("custody survived cancellation")
			}
		})
	}
}
