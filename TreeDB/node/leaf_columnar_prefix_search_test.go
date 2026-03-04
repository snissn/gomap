package node

import (
	"bytes"
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

func TestLeafColumnarPrefixV2_AllInlineAllowsEmptyValues(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
	})
	b.SetPageID(1)

	keys := [][]byte{
		[]byte("a"),
		[]byte("b"),
		[]byte("c"),
		[]byte("d"),
	}
	values := [][]byte{
		[]byte("value-a"),
		nil, // empty inline value (len=0)
		[]byte("value-c"),
		[]byte(""),
	}

	for i := range keys {
		if err := b.AddLeafEntry(keys[i], values[i], FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%q): %v", keys[i], err)
		}
	}

	n := b.Finish()
	if !n.leafColumnar() || !n.leafPrefixCompressed() || !n.leafPrefixV2() {
		t.Fatalf("expected combined columnar+prefix v2 flags")
	}

	for i := range keys {
		k, v, _, flags, err := n.GetLeafEntryView(uint16(i))
		if err != nil {
			t.Fatalf("GetLeafEntryView(%d): %v", i, err)
		}
		if !bytes.Equal(k, keys[i]) {
			t.Fatalf("key mismatch idx=%d got=%q want=%q", i, k, keys[i])
		}
		if flags != FlagInline {
			t.Fatalf("flags mismatch idx=%d got=%d want=%d", i, flags, FlagInline)
		}
		if len(v) != len(values[i]) {
			t.Fatalf("value len mismatch idx=%d got=%d want=%d", i, len(v), len(values[i]))
		}
		if !bytes.Equal(v, values[i]) {
			t.Fatalf("value mismatch idx=%d got=%q want=%q", i, v, values[i])
		}
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
