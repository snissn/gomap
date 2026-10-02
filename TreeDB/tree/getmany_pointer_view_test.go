package tree

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type pointerViewReader struct {
	SlabReader
	leaf                                 []byte
	reads, appends, keyReads, keyAppends int
	destination                          *byte
}

func (r *pointerViewReader) ReadUnsafe(p page.ValuePtr) ([]byte, error) {
	r.reads++
	return r.SlabReader.ReadUnsafe(p)
}

// Decode into the active leaf scratch so value destination reuse must be separate.
func (r *pointerViewReader) ReadLeafLogPageUnsafeTo(p page.LeafLogPtr, dst []byte) ([]byte, bool, error) {
	return append(dst[:0], r.leaf...), true, nil
}

type pointerViewAppender struct{ *pointerViewReader }

func (r *pointerViewAppender) ReadUnsafeAppend(p page.ValuePtr, dst []byte) ([]byte, error) {
	r.appends++
	if len(dst) != 0 || cap(dst) != page.PageSize {
		return nil, fmt.Errorf("unbounded destination: %d/%d", len(dst), cap(dst))
	}
	base := &dst[:cap(dst)][0]
	if r.destination != nil && r.destination != base {
		return nil, errors.New("destination changed within batch")
	}
	r.destination = base
	v, err := r.SlabReader.ReadUnsafe(p)
	return append(dst, v...), err
}

type pointerViewKeyReader struct{ *pointerViewAppender }

func (r *pointerViewKeyReader) ReadUnsafeForKey(p page.ValuePtr, key []byte) ([]byte, error) {
	r.keyReads++
	return r.SlabReader.ReadUnsafe(p)
}

type pointerViewKeyAppender struct{ *pointerViewKeyReader }

func (r *pointerViewKeyAppender) ReadUnsafeAppendForKey(p page.ValuePtr, key, dst []byte) ([]byte, error) {
	r.keyAppends++
	before := r.appends
	v, err := r.pointerViewAppender.ReadUnsafeAppend(p, dst)
	r.appends = before
	return v, err
}

func pointerViewLeaf(t *testing.T, ptrs []page.ValuePtr) ([]byte, [][]byte) {
	t.Helper()
	data := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true})
	b.SetPageID(99)
	keys := make([][]byte, 0, 72)
	for i, ptr := range ptrs {
		key := []byte(fmt.Sprintf("k%03d", i))
		if err := b.AddLeafEntry(key, nil, node.FlagPointer, ptr); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	for i, flags := range []byte{node.FlagInline, node.FlagTombstone} {
		if err := b.AddLeafEntry([]byte(fmt.Sprintf("k%03d", len(ptrs)+i)), nil, flags, page.ValuePtr{}); err != nil {
			t.Fatal(err)
		}
	}
	b.FinishNoNode()
	for len(keys) < 72 {
		keys = append(keys, []byte(fmt.Sprintf("k%03d", len(keys)%(len(ptrs)+3))))
	}
	return data, keys
}

func TestTreeGetManyPointerViewReaderPriority(t *testing.T) {
	for _, kind := range []string{"reader", "append", "key-reader", "key-append"} {
		t.Run(kind, func(t *testing.T) {
			values := newMapValueReader()
			want := [][]byte{[]byte("small"), bytes.Repeat([]byte("large"), page.PageSize), {}, []byte("last")}
			ptrs := make([]page.ValuePtr, len(want))
			for i := range want {
				ptrs[i] = values.Add(want[i])
			}
			leaf, keys := pointerViewLeaf(t, ptrs)
			r := &pointerViewReader{SlabReader: values, leaf: leaf}
			a := &pointerViewAppender{r}
			kr := &pointerViewKeyReader{a}
			var sr SlabReader = r
			switch kind {
			case "append":
				sr = a
			case "key-reader":
				sr = kr
			case "key-append":
				sr = &pointerViewKeyAppender{kr}
			}
			tr, _ := newTreeWithLeafLogRoot(t, sr, nil, page.LeafLogPtr{FileID: 1, Offset: 100, RecordLengthHint: page.PageSize})
			seen := make([]bool, len(keys))
			retained := make([][]byte, len(keys))
			if err := tr.GetManyView(keys, func(i int, key, value []byte, found bool) error {
				var k int
				if _, err := fmt.Sscanf(string(key), "k%03d", &k); err != nil {
					return err
				}
				present := k <= len(want)
				var expected []byte
				if k < len(want) {
					expected = want[k]
				} else if present {
					expected = []byte{}
				}
				if seen[i] || !bytes.Equal(key, keys[i]) || found != present || !bytes.Equal(value, expected) || (value != nil) != present {
					t.Fatalf("callback %d: %q found=%v", i, value, found)
				}
				seen[i] = true
				retained[i] = bytes.Clone(value)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for i, ok := range seen {
				if !ok {
					t.Fatalf("missed %d", i)
				}
				if i < len(want) && !bytes.Equal(retained[i], want[i]) {
					t.Fatal("copied callback value changed")
				}
			}
			n := 0
			for _, key := range keys {
				if bytes.Compare(key, []byte("k004")) < 0 {
					n++
				}
			}
			expected := []int{0, 0, 0, 0}
			switch kind {
			case "reader":
				expected[0] = n
			case "append":
				expected[1] = n
			case "key-reader":
				expected[2] = n
			case "key-append":
				expected[3] = n
			}
			got := []int{r.reads, r.appends, r.keyReads, r.keyAppends}
			for i := range got {
				if got[i] != expected[i] {
					t.Fatalf("routes %v want %v", got, expected)
				}
			}
			r.destination = nil
			stop := errors.New("stop")
			calls := 0
			if err := tr.GetManyView(keys, func(int, []byte, []byte, bool) error { calls++; return stop }); !errors.Is(err, stop) || calls != 1 {
				t.Fatal(err, calls)
			}
		})
	}
}

func TestTreeGetManyPointerViewScratchLifetime(t *testing.T) {
	values := newMapValueReader()
	large := values.Add(bytes.Repeat([]byte{'x'}, 2*page.PageSize))
	small := values.Add([]byte("small"))
	leaf, _ := pointerViewLeaf(t, []page.ValuePtr{large, small})
	r := &pointerViewReader{SlabReader: values, leaf: leaf}
	tr, _ := newTreeWithLeafLogRoot(t, &pointerViewAppender{r}, nil, page.LeafLogPtr{FileID: 1, Offset: 100, RecordLengthHint: page.PageSize})
	var scratch *leafRefPageScratch
	defer func() { putLeafRefPageScratch(scratch) }()
	n := node.NewNode(leaf)
	// Inline, tombstone and missing results must not acquire a pointer destination.
	for _, key := range []string{"k002", "k003", "k004"} {
		if err := tr.visitLeafValueFromNode(n, []byte(key), 0, func(int, []byte, []byte, bool) error { return nil }, &scratch); err != nil {
			t.Fatal(err)
		}
		if scratch != nil {
			t.Fatal("non-pointer acquired destination")
		}
	}
	for _, key := range []string{"k000", "k001"} {
		if err := tr.visitLeafValueFromNode(n, []byte(key), 0, func(_ int, _ []byte, val []byte, found bool) error {
			if !found || (key == "k000" && len(val) != 2*page.PageSize) || (key == "k001" && string(val) != "small") {
				t.Fatal("value truncated or leaf overwritten")
			}
			return nil
		}, &scratch); err != nil {
			t.Fatal(err)
		}
		if len(scratch.buf) != 0 || cap(scratch.buf) != page.PageSize {
			t.Fatal("returned backing retained in scratch")
		}
	}
}
