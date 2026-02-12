package node

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func searchLeafReferenceLinear(n *Node, key []byte) (uint16, bool, error) {
	count := n.Count()
	for idx := uint16(0); idx < count; idx++ {
		k, _, _, _, err := n.GetLeafEntryView(idx)
		if err != nil {
			return 0, false, err
		}
		cmp := bytes.Compare(k, key)
		if cmp >= 0 {
			return idx, cmp == 0, nil
		}
	}
	return count, false, nil
}

func TestLeafColumnarPrefixSearchMatchesReference(t *testing.T) {
	const keys = 128
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
	})
	b.SetPageID(1)

	sourceKeys := makeBenchKeys(keys, 24)
	for i := 0; i < keys; i++ {
		flags := byte(FlagInline)
		var val []byte
		var ptr page.ValuePtr
		switch i % 3 {
		case 0:
			flags = FlagInline
			val = []byte{byte(i), byte(i >> 8)}
		case 1:
			flags = FlagPointer
			ptr = page.ValuePtr{Offset: uint64(4096 + i), Length: 91, FileID: 2}
		default:
			flags = FlagTombstone
		}
		if err := b.AddLeafEntry(sourceKeys[i], val, flags, ptr); err != nil {
			t.Fatalf("AddLeafEntry: %v", err)
		}
	}

	n := b.Finish()
	if !n.leafColumnar() || !n.leafPrefixCompressed() || !n.leafPrefixV2() {
		t.Fatalf("expected combined columnar+prefix v2 flags")
	}

	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 10_000; i++ {
		var q []byte
		switch i % 4 {
		case 0:
			q = sourceKeys[rng.Intn(len(sourceKeys))]
		case 1:
			candidate := append([]byte(nil), sourceKeys[rng.Intn(len(sourceKeys))]...)
			if len(candidate) > 0 {
				candidate[len(candidate)-1] ^= 0x01
			}
			q = candidate
		case 2:
			q = []byte{0x00}
		default:
			q = []byte{0xFF, 0xFF, byte(i)}
		}

		gotIdx, gotFound, err := n.SearchLeaf(q)
		if err != nil {
			t.Fatalf("SearchLeaf: %v", err)
		}
		wantIdx, wantFound, err := searchLeafReferenceLinear(n, q)
		if err != nil {
			t.Fatalf("reference search: %v", err)
		}
		if gotIdx != wantIdx || gotFound != wantFound {
			t.Fatalf("mismatch q=%x got=(%d,%v) want=(%d,%v)", q, gotIdx, gotFound, wantIdx, wantFound)
		}
	}
}

func TestLeafColumnarPrefixSearchDoesNotUseLeafEntryKeyAt(t *testing.T) {
	const keys = smallSearchThreshold + 4
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
	})
	b.SetPageID(1)

	for i := 0; i < keys; i++ {
		k := make([]byte, 8)
		for j := 0; j < 6; j++ {
			k[j] = 'a'
		}
		k[6] = byte(i >> 8)
		k[7] = byte(i)
		if err := b.AddLeafEntry(k, []byte("v"), FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("add leaf entry: %v", err)
		}
	}

	n := b.Finish()
	n.leafValid = true

	if _, _, err := n.SearchLeaf([]byte{'a', 'a', 'a', 'a', 'a', 'a', 0, byte(keys / 2)}); err != nil {
		t.Fatalf("search leaf: %v", err)
	}
	if !n.leafValid {
		t.Fatalf("SearchLeaf cleared leafValid; expected combined columnar+prefix search to avoid leafEntryKeyAt")
	}
}

func benchmarkSearchLeafPrefixVariant(b *testing.B, opts BuilderOptions) {
	const keyCount = 128
	keys := makeBenchKeys(keyCount, 24)

	data := make([]byte, page.PageSize)
	builder := NewBuilderWithOptions(data, page.PageTypeLeaf, opts)
	builder.SetPageID(1)
	for i := 0; i < keyCount; i++ {
		flags := byte(FlagInline)
		var val []byte
		var ptr page.ValuePtr
		switch i % 3 {
		case 0:
			flags = FlagInline
			val = []byte{byte(i), byte(i + 1)}
		case 1:
			flags = FlagPointer
			ptr = page.ValuePtr{Offset: uint64(1000 + i), Length: 71, FileID: 2}
		default:
			flags = FlagTombstone
		}
		if err := builder.AddLeafEntry(keys[i], val, flags, ptr); err != nil {
			b.Fatalf("AddLeafEntry: %v", err)
		}
	}
	n := builder.Finish()

	queries := make([][]byte, 4096)
	for i := range queries {
		k := append([]byte(nil), keys[i%len(keys)]...)
		if i&1 == 1 && len(k) > 0 {
			k[len(k)-1] ^= 0x01
		}
		queries[i] = k
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := queries[i&(len(queries)-1)]
		if _, _, err := n.SearchLeaf(q); err != nil {
			b.Fatalf("SearchLeaf: %v", err)
		}
	}
}

func BenchmarkSearchLeaf_PrefixV2(b *testing.B) {
	benchmarkSearchLeafPrefixVariant(b, BuilderOptions{
		LeafPrefixCompression: true,
	})
}

func BenchmarkSearchLeaf_ColumnarPrefixV2(b *testing.B) {
	benchmarkSearchLeafPrefixVariant(b, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
	})
}

func benchmarkSearchLeafColumnarPrefixV2FixedBE8(b *testing.B, misses bool) {
	data := make([]byte, page.PageSize)
	builder := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
	})
	builder.SetPageID(1)

	inserted := 0
	for i := 0; ; i++ {
		var k [8]byte
		// Use even keys only so odd keys become deterministic misses.
		binary.BigEndian.PutUint64(k[:], uint64(i*2))
		if err := builder.AddLeafEntry(k[:], []byte{0x01}, FlagInline, page.ValuePtr{}); err != nil {
			if err == ErrNodeFull {
				break
			}
			b.Fatalf("AddLeafEntry: %v", err)
		}
		inserted++
	}
	if inserted == 0 {
		b.Fatalf("expected at least one key")
	}
	n := builder.Finish()

	queries := make([][]byte, inserted)
	for i := 0; i < inserted; i++ {
		var k [8]byte
		if misses {
			binary.BigEndian.PutUint64(k[:], uint64(i*2+1))
		} else {
			binary.BigEndian.PutUint64(k[:], uint64(i*2))
		}
		queries[i] = append([]byte(nil), k[:]...)
	}
	// Validate representative query semantics once outside the timed loop so the
	// benchmark actually measures the intended hit/miss path.
	_, found, err := n.SearchLeaf(queries[0])
	if err != nil {
		b.Fatalf("SearchLeaf probe: %v", err)
	}
	if misses && found {
		b.Fatalf("expected probe miss, got hit")
	}
	if !misses && !found {
		b.Fatalf("expected probe hit, got miss")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := queries[i%len(queries)]
		if _, _, err := n.SearchLeaf(q); err != nil {
			b.Fatalf("SearchLeaf: %v", err)
		}
	}
}

func BenchmarkSearchLeaf_ColumnarPrefixV2_FixedBE8_Hit(b *testing.B) {
	benchmarkSearchLeafColumnarPrefixV2FixedBE8(b, false)
}

func BenchmarkSearchLeaf_ColumnarPrefixV2_FixedBE8_Miss(b *testing.B) {
	benchmarkSearchLeafColumnarPrefixV2FixedBE8(b, true)
}
