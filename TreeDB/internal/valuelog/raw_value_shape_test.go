package valuelog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestInspectRawValueLengthAt(t *testing.T) {
	record := func(value []byte, grouped, compressed bool) ([]byte, page.ValuePtr) {
		header := make([]byte, HeaderSize)
		header[4] = Version
		if grouped {
			header[5] = recordFlagGrouped
		}
		binary.LittleEndian.PutUint32(header[16:20], uint32(len(value)))
		data := append(header, value...)
		ptr := page.ValuePtr{Offset: 4, Length: uint32(headerWithoutCRC + len(value))}
		if grouped {
			ptr.Length = page.ValuePtrMarkGrouped(ptr.Length, 0)
		} else if compressed {
			ptr.Length = page.ValuePtrMarkCompressed(ptr.Length)
		}
		return data, ptr
	}
	raw, ptr := record([]byte("12345678"), false, false)
	if n, err := inspectRawValueLengthAt(bytes.NewReader(raw), ptr, 8); err != nil || n != 8 {
		t.Fatalf("raw length=(%d,%v), want (8,nil)", n, err)
	}
	if _, err := inspectRawValueLengthAt(bytes.NewReader(raw), ptr, 7); !errors.Is(err, ErrUnboundedValueShape) {
		t.Fatalf("over-limit error=%v", err)
	}
	compressed, compressedPtr := record([]byte("zstd"), false, true)
	if _, err := inspectRawValueLengthAt(bytes.NewReader(compressed), compressedPtr, 8); !errors.Is(err, ErrUnboundedValueShape) {
		t.Fatalf("compressed error=%v", err)
	}
	templated, templatedPtr := record([]byte{'T', 'M', 1, 1, 0, 0, 0, 0}, false, false)
	if _, err := inspectRawValueLengthAt(bytes.NewReader(templated), templatedPtr, 8); !errors.Is(err, ErrUnboundedValueShape) {
		t.Fatalf("template error=%v", err)
	}
	frame := make([]byte, FrameHeaderSize+8+8+8)
	frame[0], frame[2] = FrameVersion, 1
	binary.LittleEndian.PutUint64(frame[FrameHeaderSize:FrameHeaderSize+8], 1)
	binary.LittleEndian.PutUint32(frame[FrameHeaderSize+8+4:FrameHeaderSize+8+8], 8)
	copy(frame[len(frame)-8:], "12345678")
	grouped, groupedPtr := record(frame, true, false)
	if n, err := inspectRawValueLengthAt(bytes.NewReader(grouped), groupedPtr, 8); err != nil || n != 8 {
		t.Fatalf("grouped raw length=(%d,%v), want (8,nil)", n, err)
	}
	grouped[HeaderSize+1] = FrameFlagCompressed
	if _, err := inspectRawValueLengthAt(bytes.NewReader(grouped), groupedPtr, 8); !errors.Is(err, ErrUnboundedValueShape) {
		t.Fatalf("grouped compressed error=%v", err)
	}
}
