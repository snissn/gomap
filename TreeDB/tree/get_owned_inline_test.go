package tree

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// Decode a prefix-compressed outer leaf into the caller's scratch, so the
// owned inline result must be copied before the nested decode lease ends.
type ownedGetCompressedReader struct {
	*mapValueReader
	encoded []byte
}

func (r *ownedGetCompressedReader) ReadChecksumEnabled() bool { return true }
func (r *ownedGetCompressedReader) ReadLeafLogPageUnsafeTo(_ page.LeafLogPtr, dst []byte) ([]byte, bool, error) {
	dst = dst[:page.PageSize]
	n, err := lz4.UncompressBlock(r.encoded, dst)
	return dst[:n], true, err
}

func ownedGetFixture(t testing.TB, compressed bool) (*Tree, map[string][]byte, *ownedGetCompressedReader) {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	rootData, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string][]byte{
		"00-empty": {}, "10-inline": []byte("inline-value-17xx!"),
		"20-pointer":     []byte("pointer-value-17!"),
		"30-page":        bytes.Repeat([]byte("p"), page.PageSize),
		"40-large":       bytes.Repeat([]byte("l"), 2*page.PageSize+13),
		"50-tight-large": bytes.Repeat([]byte("t"), 2*page.PageSize),
	}
	reader := &ownedGetCompressedReader{mapValueReader: newMapValueReader()}
	leafData := rootData
	if compressed {
		leafData = make([]byte, page.PageSize)
	}
	builder := node.NewBuilderWithOptions(leafData, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: compressed})
	builder.SetPageID(root)
	for _, key := range []string{"00-empty", "10-inline", "20-pointer", "30-page", "40-large", "50-tight-large"} {
		value := values[key]
		flags := byte(node.FlagInline)
		var ptr page.ValuePtr
		if key >= "20-pointer" {
			flags = node.FlagPointer
			ptr = reader.Add(value)
			value = nil
		}
		if err := builder.AddLeafEntry([]byte(key), value, flags, ptr); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.AddLeafEntry([]byte("60-tombstone"), nil, node.FlagTombstone, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddLeafEntry([]byte("70-error"), nil, node.FlagPointer, page.ValuePtr{FileID: page.ValueLogFileID(2), Offset: 123}); err != nil {
		t.Fatal(err)
	}
	builder.FinishNoNode()
	if compressed {
		reader.encoded = compressOwnedGetLeaf(t, leafData)
		rootBuilder := node.NewBuilder(rootData, page.PageTypeInternal)
		rootBuilder.SetPageID(root)
		ptr := page.LeafLogPtr{FileID: page.ValueLogFileID(3), Offset: 8, RecordLengthHint: page.PageSize}
		if err := rootBuilder.AddInternalChildRef(nil, page.LeafLogChildRef(ptr)); err != nil {
			t.Fatal(err)
		}
		rootBuilder.FinishNoNode()
		return New(p, reader, root), values, reader
	}
	return New(p, reader.mapValueReader, root), values, reader
}
func compressOwnedGetLeaf(t testing.TB, leaf []byte) []byte {
	t.Helper()
	encoded := make([]byte, lz4.CompressBlockBound(len(leaf)))
	n, err := lz4.CompressBlock(leaf, encoded, nil)
	if err != nil || n == 0 {
		t.Fatalf("compress leaf: %d %v", n, err)
	}
	return encoded[:n]
}

func TestTreeGetOwnedInlineLifetime(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed-outer=%v", compressed), func(t *testing.T) {
			tr, values, _ := ownedGetFixture(t, compressed)
			retained := make(map[string][]byte)
			for key, want := range values {
				got, err := tr.Get([]byte(key))
				if err != nil || got == nil || !bytes.Equal(got, want) || cap(got) != len(got) {
					t.Fatalf("%s: len/cap=%d/%d err=%v", key, len(got), cap(got), err)
				}
				retained[key] = got
			}
			for i := 0; i < 100; i++ {
				for key, want := range values {
					got, err := tr.Get([]byte(key))
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("%s: %v", key, err)
					}
					for j := range got {
						got[j] ^= 255
					}
				}
				for _, key := range []string{"missing", "60-tombstone", "70-error"} {
					got, err := tr.Get([]byte(key))
					if got != nil || err == nil {
						t.Fatalf("%s: result=%v err=%v", key, got, err)
					}
					if key != "70-error" && !errors.Is(err, ErrKeyNotFound) {
						t.Fatal(err)
					}
				}
			}
			for key, want := range values {
				if !bytes.Equal(retained[key], want) {
					t.Fatalf("retained %s corrupted after scratch reuse", key)
				}
			}
			scratch := getLeafRefPageScratch()
			defer putLeafRefPageScratch(scratch)
			if cap(scratch.buf) != page.PageSize {
				t.Fatalf("pooled oversized buffer: %d", cap(scratch.buf))
			}
		})
	}
}

func TestTreeGetOwnedInlineConcurrent(t *testing.T) {
	tr, values, _ := ownedGetFixture(t, true)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				for key, want := range values {
					got, err := tr.Get([]byte(key))
					if err != nil || !bytes.Equal(got, want) {
						t.Errorf("%s: err=%v", key, err)
						return
					}
					if len(got) > 0 {
						got[0] ^= 255
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestTreeGetOwnedInlineCompressedChecksum(t *testing.T) {
	tr, values, r := ownedGetFixture(t, true)
	leaf := make([]byte, page.PageSize)
	if _, err := lz4.UncompressBlock(r.encoded, leaf); err != nil {
		t.Fatal(err)
	}
	original := r.encoded
	leaf[8] ^= 1
	r.encoded = compressOwnedGetLeaf(t, leaf)
	got, err := tr.Get([]byte("10-inline"))
	if got != nil || err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("got=%v err=%v", got, err)
	}
	r.encoded = original
	got, err = tr.Get([]byte("10-inline"))
	if err != nil || !bytes.Equal(got, values["10-inline"]) {
		t.Fatalf("read after error: %v", err)
	}
}

var ownedGetInlineBenchmarkSink []byte

func TestTreeGetOwnedInlineAllocations(t *testing.T) {
	tr, _, _ := ownedGetFixture(t, false)
	for _, key := range []string{"10-inline", "20-pointer", "50-tight-large"} {
		keyBytes := []byte(key)
		allocs := testing.AllocsPerRun(1000, func() {
			var err error
			ownedGetInlineBenchmarkSink, err = tr.Get(keyBytes)
			if err != nil {
				panic(err)
			}
		})
		want := float64(1)
		if key == "20-pointer" {
			want = 2
		} // Existing pointer append/trim path.
		if allocs > want {
			t.Fatalf("%s: %.2f allocations, want at most %.0f", key, allocs, want)
		}
	}
}
func BenchmarkTreeGetOwnedInline(b *testing.B) {
	for _, compressed := range []bool{false, true} {
		b.Run(fmt.Sprintf("compressed-outer=%v", compressed), func(b *testing.B) {
			tr, _, _ := ownedGetFixture(b, compressed)
			for _, key := range []string{"00-empty", "10-inline", "20-pointer", "30-page", "40-large", "50-tight-large", "missing"} {
				b.Run(key, func(b *testing.B) {
					keyBytes := []byte(key)
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						ownedGetInlineBenchmarkSink, _ = tr.Get(keyBytes)
					}
				})
			}
		})
	}
}

func TestTreeGetOwnedInlineAppendDestinations(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		tr, values, _ := ownedGetFixture(t, compressed)
		for _, key := range []string{"00-empty", "10-inline", "20-pointer"} {
			want := values[key]
			got, err := tr.GetAppend([]byte(key), nil)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("nil destination %s: %v", key, err)
			}
			if key == "10-inline" && cap(got) != len(got) {
				t.Fatalf("inline len/cap=%d/%d", len(got), cap(got))
			}
			dst := make([]byte, 7, 64)
			copy(dst, "prefix:")
			got, err = tr.GetAppend([]byte(key), dst)
			if err != nil || !bytes.Equal(got, append([]byte("prefix:"), want...)) || &got[0] != &dst[0] {
				t.Fatalf("append prefix %s: %q %v", key, got, err)
			}
		}
	}
}
func TestTreeGetOwnedInlineViewLease(t *testing.T) {
	reader := &statefulLeafLogPageReader{mapValueReader: newMapValueReader()}
	leaf := make([]byte, page.PageSize)
	b := node.NewBuilder(leaf, page.PageTypeLeaf)
	b.SetPageID(1)
	want := []byte("inline-value-17xx!")
	if err := b.AddLeafEntry([]byte("k"), want, node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	ptr := page.LeafLogPtr{FileID: 1, Offset: 8, RecordLengthHint: page.PageSize}
	reader.values[ptr.ValuePtr()] = leaf
	tr, _ := newTreeWithLeafLogRoot(t, reader, nil, ptr)
	got, err := tr.Get([]byte("k"))
	if err != nil || !bytes.Equal(got, want) || cap(got) != len(got) {
		t.Fatalf("Get: %q %v", got, err)
	}
	if reader.views != 1 || reader.releases != 1 {
		t.Fatalf("views=%d releases=%d", reader.views, reader.releases)
	}
	clear(leaf)
	if !bytes.Equal(got, want) {
		t.Fatal("returned result aliases released leaf view")
	}
}
