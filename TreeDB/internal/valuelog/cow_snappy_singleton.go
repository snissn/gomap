package valuelog

import (
	"math"
	"os"

	"github.com/snissn/gomap/TreeDB/page"
)

const cowSnappySingletonMetadataBytes = HeaderSize + FrameHeaderSize + 8 + 2*4

// COWSnappySingletonProbeEligible recognizes the pointer fields needed to
// attempt a bounded read. Only the stored shape can select the actual codec.
func COWSnappySingletonProbeEligible(ptr page.ValuePtr, inputBytes int) bool {
	recordBytes := uint64(page.ValuePtrRecordLength(ptr)) + 4
	return page.ValuePtrIsGrouped(ptr) && page.ValuePtrSubIndex(ptr) == 0 &&
		recordBytes >= cowSnappySingletonMetadataBytes && recordBytes <= uint64(inputBytes) &&
		ptr.Offset >= 4 && ptr.Offset <= math.MaxInt64-recordBytes+4
}

// ReadCOWSnappySingleton borrows the exclusive, admitted input and raw scratch
// until copy-out. The file must be the original immutable pinned handle. A
// valid but unsuitable record falls back to the caller's ordinary reader.
// Source, I/O, integrity and decode errors on a selected read are authoritative.
func ReadCOWSnappySingleton(f *os.File, ptr page.ValuePtr, limits COWReadLimits, input, raw []byte) (value []byte, handled bool, err error) {
	if !COWSnappySingletonProbeEligible(ptr, len(input)) {
		return nil, false, nil
	}
	if err := cowCheckReadSource(f, ptr, limits, false); err != nil {
		return nil, true, err
	}
	metadata := input[:cowSnappySingletonMetadataBytes]
	read, readErr := f.ReadAt(metadata, int64(ptr.Offset-4))
	if read < HeaderSize {
		return nil, true, readErr
	}
	header := metadata[:HeaderSize]
	n, err := cowCheckReadHeader(ptr, header, limits)
	if err != nil {
		return nil, true, err
	}
	if header[5]&recordFlagGrouped == 0 {
		return nil, true, ErrCorrupt
	}
	if readErr != nil {
		return nil, true, readErr
	}
	prefix := metadata[HeaderSize:]
	if prefix[2] != 1 {
		return nil, false, nil
	}
	shape, err := cowShape(header, prefix, metadata, n, ptr, limits)
	if err != nil {
		return nil, true, err
	}
	if !shape.Compressed || shape.DictID != 0 || shape.Codec != BlockCodecSnappy ||
		shape.RawBytes == 0 || shape.RawBytes > uint64(len(raw)) {
		return nil, false, nil
	}

	// The ordinary reader revalidates the exact metadata, verifies all bytes
	// against the stored CRC and checks that the decoder used admitted backing.
	value, err = readCOWRecordValue(f, ptr, shape, true, input, raw, decodeCOWSnappySingleton, false)
	return value, true, err
}

func decodeCOWSnappySingleton(frame FrameHeader, payload, dst []byte) ([]byte, error) {
	// readCOWRecordValue passes exact length and capacity; the Snappy decoder
	// cannot grow this already admitted scratch or use a dictionary.
	return decodeBlockPayload(frame.Reserved, payload, uint32(len(dst)), dst)
}
