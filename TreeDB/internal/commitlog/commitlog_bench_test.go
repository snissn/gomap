package commitlog

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func newDiscardWriter() *Writer {
	return &Writer{
		bw:             bufio.NewWriterSize(io.Discard, defaultBufferSize),
		scratch:        make([]byte, 0, defaultBufferSize),
		maxSegmentSize: normalizeMaxSegmentSize(Options{}.MaxSegmentSize),
	}
}

func BenchmarkCommitLogAppendBatch_UncompressedInline_Discard(b *testing.B) {
	cases := []struct {
		name     string
		records  int
		keySize  int
		valueLen int
	}{
		{name: "records=1/val=1024", records: 1, keySize: 16, valueLen: 1024},
		{name: "records=16/val=1024", records: 16, keySize: 16, valueLen: 1024},
		{name: "records=1/val=16384", records: 1, keySize: 16, valueLen: 16 << 10},
		{name: "records=3/val=16384", records: 3, keySize: 16, valueLen: 16 << 10},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			w := newDiscardWriter()

			key := bytes.Repeat([]byte{'k'}, tc.keySize)
			value := bytes.Repeat([]byte{'v'}, tc.valueLen)
			records := make([]Record, tc.records)
			for i := range records {
				records[i] = Record{
					Op:    OpSetInline,
					Key:   key,
					Value: value,
					Seq:   uint64(i + 1),
				}
			}

			segmentBytes := int64(segmentHeaderSize + batchHeaderSize)
			segmentBytes += int64(tc.records) * int64(recordHeaderSize+tc.keySize+tc.valueLen)
			b.SetBytes(segmentBytes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := w.AppendBatch(records); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
