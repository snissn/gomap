package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/page"
)

func makeBenchOuterLeafPayload(b *testing.B) ([]byte, page.ValuePtr) {
	b.Helper()
	encoded, err := outerleaf.EncodeEntries(nil, []outerleaf.Entry{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}, uint8(ValueLogBlockSnappy), 16)
	if err != nil {
		b.Fatalf("encode outer-leaf payload: %v", err)
	}
	ptr := page.ValuePtr{
		FileID: page.ValueLogFileID(7),
		Offset: 128,
		Length: uint32(len(encoded)),
	}
	return encoded, ptr
}

func makeBenchOuterLeafBlobRefPayload(b *testing.B, key []byte, blobPtr page.ValuePtr) ([]byte, page.ValuePtr) {
	b.Helper()
	encoded, err := outerleaf.EncodeSingleBlobRef(nil, key, blobPtr, uint8(ValueLogBlockSnappy), 16)
	if err != nil {
		b.Fatalf("encode blob-ref outer-leaf payload: %v", err)
	}
	outerPtr := page.ValuePtr{
		FileID: page.ValueLogFileID(17),
		Offset: 4096,
		Length: uint32(len(encoded)),
	}
	return encoded, outerPtr
}

func BenchmarkValueReaderReadUnsafeAppendForKey_NoCacheInline(b *testing.B) {
	payload, ptr := makeBenchOuterLeafPayload(b)
	reader := &stubValueLogAppendReader{
		stubValueLogReader: stubValueLogReader{
			payloads: map[page.ValuePtr][]byte{
				ptr: payload,
			},
		},
	}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}
	key := []byte("k2")
	dst := make([]byte, 0, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		dst, err = r.ReadUnsafeAppendForKey(ptr, key, dst[:0])
		if err != nil {
			b.Fatalf("ReadUnsafeAppendForKey: %v", err)
		}
	}
}

func BenchmarkValueReaderReadUnsafeAppendForKey_NoCacheBlobRef(b *testing.B) {
	key := []byte("blob-k")
	blobPtr := page.ValuePtr{FileID: page.ValueLogFileID(31), Offset: 16384, Length: 9}
	outerPayload, outerPtr := makeBenchOuterLeafBlobRefPayload(b, key, blobPtr)
	blobValue := []byte("blob-data")

	reader := &stubValueLogAppendReader{
		stubValueLogReader: stubValueLogReader{
			payloads: map[page.ValuePtr][]byte{
				outerPtr: outerPayload,
				blobPtr:  blobValue,
			},
		},
	}
	r := valueReader{
		vlogs:         reader,
		outerLeafMode: outerleaf.ModeV2FencePtr,
	}
	dst := make([]byte, 0, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		dst, err = r.ReadUnsafeAppendForKey(outerPtr, key, dst[:0])
		if err != nil {
			b.Fatalf("ReadUnsafeAppendForKey: %v", err)
		}
	}
}
