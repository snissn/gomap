package valuelog

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var leafPagePayloadBenchSink []byte

func buildSparseLeafPageForPayloadBench(b *testing.B) []byte {
	b.Helper()
	buf := make([]byte, page.PageSize)
	builder := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	builder.SetPageID(17)
	for i := 0; i < 4; i++ {
		if err := builder.AddLeafEntry(
			[]byte(fmt.Sprintf("celestia/outer/leaf/key/%02d", i)),
			[]byte("value"),
			node.FlagInline,
			page.ValuePtr{},
		); err != nil {
			b.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	builder.FinishNoNode()
	return buf
}

func compactLeafPayloadBenchmarkFixture(b *testing.B) (uint32, string, []byte) {
	b.Helper()
	leaf := buildSparseLeafPageForPayloadBench(b)
	payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
	if err != nil {
		b.Fatalf("MaybeCompactLeafLogPayload: %v", err)
	}
	if !compacted {
		b.Fatal("expected sparse leaf fixture to compact")
	}
	fileID, err := EncodeFileID(ReservedLeafLogLaneID, 1)
	if err != nil {
		b.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join("bench", compactLeafPagePayloadDirName, "value-l255-000001.log")
	return fileID, path, payload
}

func BenchmarkMaybeCompactLeafLogPayload(b *testing.B) {
	leaf := buildSparseLeafPageForPayloadBench(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		payload, compacted, err := MaybeCompactLeafLogPayload(leaf)
		if err != nil {
			b.Fatalf("MaybeCompactLeafLogPayload: %v", err)
		}
		if !compacted {
			b.Fatal("expected sparse leaf fixture to compact")
		}
		leafPagePayloadBenchSink = payload
	}
}

func BenchmarkAppendMaybeDecodeLeafLogPayload(b *testing.B) {
	fileID, path, payload := compactLeafPayloadBenchmarkFixture(b)

	b.Run("fresh_dst", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, err := appendMaybeDecodeLeafLogPayload(fileID, path, nil, payload)
			if err != nil {
				b.Fatalf("appendMaybeDecodeLeafLogPayload: %v", err)
			}
			leafPagePayloadBenchSink = out
		}
	})

	b.Run("reused_dst", func(b *testing.B) {
		dst := make([]byte, 0, page.PageSize)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			dst = dst[:0]
			out, err := appendMaybeDecodeLeafLogPayload(fileID, path, dst, payload)
			if err != nil {
				b.Fatalf("appendMaybeDecodeLeafLogPayload: %v", err)
			}
			leafPagePayloadBenchSink = out
		}
	})
}

func BenchmarkDecodeCompactLeafLogPayloadTo(b *testing.B) {
	_, _, payload := compactLeafPayloadBenchmarkFixture(b)

	b.Run("fresh_dst", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, _, decoded, err := decodeCompactLeafLogPayloadTo(payload, nil)
			if err != nil {
				b.Fatalf("decodeCompactLeafLogPayloadTo: %v", err)
			}
			if !decoded {
				b.Fatal("expected compact payload to decode")
			}
			leafPagePayloadBenchSink = out
		}
	})

	b.Run("reused_dst", func(b *testing.B) {
		dst := make([]byte, 0, page.PageSize)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out, _, decoded, err := decodeCompactLeafLogPayloadTo(payload, dst[:0])
			if err != nil {
				b.Fatalf("decodeCompactLeafLogPayloadTo: %v", err)
			}
			if !decoded {
				b.Fatal("expected compact payload to decode")
			}
			leafPagePayloadBenchSink = out
		}
	})
}
