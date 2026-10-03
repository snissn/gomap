package node

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func namespaceSuffixKeys(prefix []byte, count int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = append(append([]byte(nil), prefix...), make([]byte, 8)...)
		binary.BigEndian.PutUint64(keys[i][len(prefix):], uint64(i)*0x100000002)
	}
	return keys
}

func buildPointMetadataLeaf(tb testing.TB, keys [][]byte, opts BuilderOptions) []byte {
	tb.Helper()
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, opts)
	b.SetPageID(1)
	for i, key := range keys {
		flags, val, ptr := byte(FlagInline), []byte("value"), page.ValuePtr{}
		switch i % 4 {
		case 1:
			flags, val, ptr = FlagPointer, nil, page.ValuePtr{Offset: uint64(i + 1), Length: 4096, FileID: 7}
		case 2:
			val = nil
		case 3:
			flags, val = FlagTombstone, nil
		}
		if err := b.AddLeafEntryWithRevision(key, val, flags, ptr, page.EntryRevision(i+11)); err != nil {
			tb.Fatal(err)
		}
	}
	b.FinishNoNode()
	return data
}

func TestLeafValueViewWithRevisionParity(t *testing.T) {
	keys := namespaceSuffixKeys([]byte("binary\x00\xffnamespace:"), 32)
	for _, prefix := range []bool{false, true} {
		for _, columnar := range []bool{false, true} {
			for _, packed := range []bool{false, true} {
				for _, revisions := range []bool{false, true} {
					opts := BuilderOptions{LeafPrefixCompression: prefix, LeafColumnar: columnar, PackedValuePtr: packed, EntryRevisions: revisions}
					t.Run(fmt.Sprintf("prefix=%v/columnar=%v/packed=%v/revisions=%v", prefix, columnar, packed, revisions), func(t *testing.T) {
						data := buildPointMetadataLeaf(t, keys, opts)
						for i, key := range keys {
							n := NewNodeView(data)
							idx, found, err := n.SearchLeaf(key)
							if err != nil || !found || idx != uint16(i) {
								t.Fatalf("search %d: %d %v %v", i, idx, found, err)
							}
							v, ptr, flags, rev, err := n.GetLeafValueViewWithRevision(idx)
							full := NewNodeView(data)
							k, wv, wp, wf, wr, we := full.GetLeafEntryViewWithRevision(idx)
							if err != we || !bytes.Equal(k, key) || !bytes.Equal(v, wv) || ptr != wp || flags != wf || rev != wr {
								t.Fatalf("metadata parity %d: %x %+v %x %d %v; full %x %+v %x %d %v", i, v, ptr, flags, rev, err, wv, wp, wf, wr, we)
							}
						}
						n := NewNodeView(data)
						_, _, _, _, gotErr := n.GetLeafValueViewWithRevision(uint16(len(keys)))
						_, _, _, _, _, wantErr := n.GetLeafEntryViewWithRevision(uint16(len(keys)))
						if gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() {
							t.Fatalf("invalid index: %v; full %v", gotErr, wantErr)
						}
					})
				}
			}
		}
	}
	n := NewNode(make([]byte, page.PageSize))
	n.SetType(page.PageTypeInternal)
	if _, _, _, _, err := n.GetLeafValueViewWithRevision(0); err != ErrInvalidType {
		t.Fatalf("invalid type: %v", err)
	}
}

func TestLeafValueViewWithRevisionPreservesKeyView(t *testing.T) {
	keys := namespaceSuffixKeys([]byte("namespace:"), 8)
	data := buildPointMetadataLeaf(t, keys, BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true, EntryRevisions: true})
	n := NewNodeView(data)
	key, _, _, _, _, err := n.GetLeafEntryViewWithRevision(1)
	if err != nil {
		t.Fatal(err)
	}
	idx, found, err := n.SearchLeaf(keys[2])
	if err != nil || !found {
		t.Fatal(err)
	}
	if _, _, _, _, err := n.GetLeafValueViewWithRevision(idx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key, keys[1]) {
		t.Fatalf("value-only access changed previous key view: %x", key)
	}
}

func TestLeafPointMetadataCorruption(t *testing.T) {
	keys := namespaceSuffixKeys([]byte("namespace:"), 8)
	opts := BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true, EntryRevisions: true, PackedValuePtr: true}
	original := buildPointMetadataLeaf(t, keys, opts)
	n := NewNodeView(original)
	if err := n.leafColumnarPrefixV2EnsureMeta(); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"restart_prefix", func(d []byte) []byte { binary.LittleEndian.PutUint16(d[n.leafColPrefixPrefixStart:], 1); return d }},
		{"impossible_prefix", func(d []byte) []byte {
			binary.LittleEndian.PutUint16(d[n.leafColPrefixPrefixStart+2:], 65535)
			return d
		}},
		{"key_directory_before_blob", func(d []byte) []byte {
			binary.LittleEndian.PutUint16(d[NodeHeaderSize+2:], uint16(n.leafColPrefixHeaderEnd-1))
			return d
		}},
		{"key_directory_reversed", func(d []byte) []byte {
			binary.LittleEndian.PutUint16(d[NodeHeaderSize+4:], uint16(n.leafColPrefixKeysBlobBase))
			return d
		}},
		{"value_directory_before_header", func(d []byte) []byte {
			binary.LittleEndian.PutUint16(d[n.leafColPrefixValDirStart+2:], uint16(n.leafColPrefixHeaderEnd-1))
			return d
		}},
		{"pointer_wrong_length", func(d []byte) []byte {
			binary.LittleEndian.PutUint16(d[n.leafColPrefixValDirStart+4:], uint16(n.leafColPrefixHeaderEnd+6))
			return d
		}},
		{"truncated_revisions", func(d []byte) []byte { return d[:n.leafColPrefixRevisionStart+page.EntryRevisionSize] }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.mutate(append([]byte(nil), original...))
			point := NewNodeView(data)
			idx, found, err := point.SearchLeaf(keys[1])
			if err == nil && found {
				_, _, _, _, err = point.GetLeafValueViewWithRevision(idx)
			}
			if err != ErrCorruptedNode {
				t.Fatalf("point corruption: found=%v err=%v", found, err)
			}
			full := NewNodeView(data)
			if _, _, _, _, _, err := full.GetLeafEntryViewWithRevision(1); err != ErrCorruptedNode {
				t.Fatalf("full corruption: %v", err)
			}
		})
	}
}

func TestLeafCommonPrefixSuffixSearchParity(t *testing.T) {
	variable := namespaceSuffixKeys([]byte("namespace:"), 32)
	// These change eligibility after several numeric successors in a restart
	// block: a different length, then a different prefix of the same length.
	variable[5] = append(variable[5], 0)
	variable[9][0]++
	sort.Slice(variable, func(i, j int) bool { return bytes.Compare(variable[i], variable[j]) < 0 })
	boundaries := namespaceSuffixKeys([]byte{0, 0xff, 'b'}, 11)
	for i, suffix := range []uint64{0, 1, 0xff, 0x100, 0xffff, 0x10000, 0xffffffff, 0x100000000, 1 << 63, ^uint64(0) - 1, ^uint64(0)} {
		binary.BigEndian.PutUint64(boundaries[i][3:], suffix)
	}
	shapes := map[string][][]byte{
		"numeric8":          namespaceSuffixKeys(nil, 48),
		"binary_prefix":     namespaceSuffixKeys([]byte{0, 0xff, 1, 0x80}, 48),
		"long_prefix":       namespaceSuffixKeys(bytes.Repeat([]byte{0, 0xff, 'p'}, 50), 24),
		"suffix_boundaries": boundaries,
		"fallback_midblock": variable,
		"short_variable":    {nil, []byte{0}, []byte{0, 1}, []byte{0, 1, 0}, []byte("abcd"), []byte("abcd-long"), []byte{0xff}},
	}
	for name, keys := range shapes {
		t.Run(name, func(t *testing.T) {
			data := buildPointMetadataLeaf(t, keys, BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true})
			queries := [][]byte{nil, {0xff, 0xff, 0xff}, bytes.Repeat([]byte{0xff}, 200)}
			for _, key := range keys {
				queries = append(queries, key, append(append([]byte(nil), key...), 0))
				if len(key) > 0 {
					queries = append(queries, key[:len(key)-1])
					q := append([]byte(nil), key...)
					q[len(q)-1]++
					queries = append(queries, q)
				}
			}
			for _, q := range queries {
				want := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], q) >= 0 })
				wantFound := want < len(keys) && bytes.Equal(keys[want], q)
				n := NewNodeView(data)
				idx, found, err := n.SearchLeaf(q)
				if err != nil || int(idx) != want || found != wantFound {
					t.Fatalf("query %x: %d %v %v; want %d %v", q, idx, found, err, want, wantFound)
				}
			}
		})
	}
}

func BenchmarkLeafPointMetadata(b *testing.B) {
	keys := namespaceSuffixKeys([]byte("arbitrary\x00namespace:"), 64)
	data := buildPointMetadataLeaf(b, keys, BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true, EntryRevisions: true, PackedValuePtr: true})
	for _, full := range []bool{true, false} {
		b.Run(fmt.Sprintf("key_view=%v", full), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				n := NewNodeView(data)
				idx, found, err := n.SearchLeaf(keys[1+i%15])
				if err != nil || !found {
					b.Fatal(err)
				}
				if full {
					_, _, _, _, _, err = n.GetLeafEntryViewWithRevision(idx)
				} else {
					_, _, _, _, err = n.GetLeafValueViewWithRevision(idx)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLeafCommonPrefixSuffixSearch(b *testing.B) {
	for _, shape := range []string{"namespace", "numeric8", "variable", "long_prefix"} {
		prefix := []byte("arbitrary\x00namespace:")
		if shape == "numeric8" {
			prefix = nil
		}
		if shape == "long_prefix" {
			prefix = bytes.Repeat([]byte("x"), 150)
		}
		keys := namespaceSuffixKeys(prefix, 64)
		if shape == "variable" {
			for i := range keys {
				keys[i] = append(keys[i], bytes.Repeat([]byte("v"), i%5)...)
			}
		}
		data := buildPointMetadataLeaf(b, keys, BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true})
		for _, mode := range []string{"hit", "miss", "mixed"} {
			queries := make([][]byte, len(keys))
			for i := range queries {
				queries[i] = append([]byte(nil), keys[i]...)
				if mode == "miss" || mode == "mixed" && i%2 == 1 {
					queries[i][len(queries[i])-1]++
				}
			}
			b.Run(shape+"/"+mode, func(b *testing.B) {
				b.ReportAllocs()
				n := NewNodeView(data)
				for i := 0; i < b.N; i++ {
					if _, _, err := n.SearchLeaf(queries[i%len(queries)]); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
