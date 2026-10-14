package node

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func projectionWork() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 8192}
}
func TestProjectionDecoderDifferential(t *testing.T) {
	for mask := 0; mask < 8; mask++ {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			image := make([]byte, page.PageSize)
			opts := BuilderOptions{LeafPrefixCompression: mask&1 != 0, LeafColumnar: mask&2 != 0, PackedValuePtr: mask&4 != 0, EntryRevisions: true}
			b := NewBuilderWithOptions(image, page.PageTypeLeaf, opts)
			b.SetPageID(2)
			for i := 0; i < 24; i++ {
				key := []byte(fmt.Sprintf("%s%03d", bytes.Repeat([]byte("p"), 64), i))
				flags := byte(FlagInline)
				val := []byte("inline")
				ptr := page.ValuePtr{}
				if i%3 == 1 {
					flags = FlagPointer
					val = nil
					ptr = page.ValuePtr{FileID: 7, Offset: uint64(i*100 + 10), Length: 40}
				}
				if i%3 == 2 {
					flags = FlagTombstone
					val = nil
				}
				if err := b.AddLeafEntryWithRevision(key, val, flags, ptr, page.EntryRevision(i+1)); err != nil {
					t.Fatal(err)
				}
			}
			n := *b.Finish()
			for i := int(n.Count()) - 1; i >= 0; i-- {
				key, value, ptr, flags, revision, err := n.GetLeafEntryViewWithRevision(uint16(i))
				want := LeafEntry{Key: key, Value: value, ValuePtr: ptr, Flags: flags, Revision: revision}
				if err != nil {
					t.Fatal(err)
				}
				want.Key = bytes.Clone(want.Key)
				var d ProjectionDecoder
				var scratch [page.PageSize]byte
				ok, err := d.Begin(n, uint16(i), scratch[:], projectionWork())
				if err != nil || !ok {
					t.Fatal(ok, err)
				}
				if done, err := d.Step(&iterator.OrdinalScanWork{}); done || err != nil || d.at != uint16(i)-uint16(i)%leafPrefixRestartInterval && opts.LeafPrefixCompression {
					t.Fatal("zero allowance changed decoder")
				}
				done := false
				for turn := 0; turn < 200 && !done; turn++ {
					w := projectionWork()
					done, err = d.Step(w)
					if err != nil {
						t.Fatal(err)
					}
					if w.Records > 1 || w.Bytes > 8192 {
						t.Fatal("overrun")
					}
				}
				if !done {
					t.Fatal("stalled")
				}
				got, _ := d.Result()
				if !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.ValuePtr != want.ValuePtr || got.Flags != want.Flags || got.Revision != want.Revision {
					t.Fatalf("entry %d: got %+v want %+v", i, got, want)
				}
				if !d.Cancel(projectionWork()) || d.n.data != nil || d.scratch != nil {
					t.Fatal("cancel retained views")
				}
			}
		})
	}
}
func TestProjectionInternalDifferential(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		image := make([]byte, page.PageSize)
		b := NewBuilderWithOptions(image, page.PageTypeInternal, BuilderOptions{InternalBaseDelta: compressed})
		b.SetPageID(2)
		for i := 0; i < 20; i++ {
			if err := b.AddInternalChild([]byte(fmt.Sprintf("common-prefix-%04d", i)), uint64(i+3)); err != nil {
				t.Fatal(err)
			}
		}
		n := *b.Finish()
		for i := uint16(0); i < n.Count(); i++ {
			want, ref, err := n.GetInternalEntryRefView(i)
			if err != nil {
				t.Fatal(err)
			}
			want = bytes.Clone(want)
			var d ProjectionDecoder
			var scratch [page.PageSize]byte
			ok, err := d.Begin(n, i, scratch[:], projectionWork())
			if !ok || err != nil {
				t.Fatal(err)
			}
			done := false
			for turn := 0; turn < 30 && !done; turn++ {
				done, err = d.Step(projectionWork())
				if err != nil {
					t.Fatal(err)
				}
			}
			got, r := d.Result()
			if !done || !bytes.Equal(want, got.Key) || r != ref {
				t.Fatal("internal difference", compressed, i, string(got.Key), string(want), r, ref)
			}
		}
	}
}
func TestProjectionLongComparisonRetainsByteOffset(t *testing.T) {
	a := bytes.Repeat([]byte("x"), 3<<20)
	b := bytes.Clone(a)
	b[len(b)-1] = 'y'
	var c ProjectionComparison
	if !c.Begin(a, b, projectionWork()) {
		t.Fatal("begin")
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	_, done := c.Step(w)
	if done || c.offset != 1<<19 || w.Bytes != 1<<20 {
		t.Fatal("long comparison did not pause", c.offset, w)
	}
	at := c.offset
	if _, done = c.Step(&iterator.OrdinalScanWork{}); done || c.offset != at {
		t.Fatal("zero allowance moved")
	}
	for turn := 0; turn < 10; turn++ {
		w = &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
		cmp, done := c.Step(w)
		if done {
			if cmp != -1 {
				t.Fatal(cmp)
			}
			return
		}
	}
	t.Fatal("long comparison stalled")
}

func TestProjectionPrefixCopyPausesBeforeEffect(t *testing.T) {
	image := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(image, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true})
	for i := 0; i < 2; i++ {
		if err := b.AddLeafEntry([]byte(fmt.Sprintf("%s%d", bytes.Repeat([]byte("q"), 128), i)), nil, FlagTombstone, page.ValuePtr{}); err != nil {
			t.Fatal(err)
		}
	}
	n := *b.Finish()
	var d ProjectionDecoder
	var scratch [page.PageSize]byte
	ok, err := d.Begin(n, 1, scratch[:], projectionWork())
	if !ok || err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 20 && (d.at != 1 || d.phase != 1); turn++ {
		_, err = d.Step(projectionWork())
		if err != nil {
			t.Fatal(err)
		}
	}
	if d.at != 1 || d.phase != 1 {
		t.Fatal("no paused prefix")
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 16}
	done, err := d.Step(w)
	if done || err != nil || d.copied != 8 || w.Bytes != 16 {
		t.Fatal("copy quantum", d.copied, w, err)
	}
	at := d.copied
	if done, err = d.Step(&iterator.OrdinalScanWork{}); done || err != nil || d.copied != at {
		t.Fatal("unadmitted copy")
	}
	for turn := 0; turn < 20; turn++ {
		done, err = d.Step(projectionWork())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			return
		}
	}
	t.Fatal("copy stalled")
}

func TestProjectionLargePrefixGeometryIsUnfinishedNotCorruption(t *testing.T) {
	image := make([]byte, 7000)
	n := NewNodeView(image)
	n.SetType(page.PageTypeLeaf)
	binary.LittleEndian.PutUint16(image[12:14], uint16(page.PageTypeLeaf)|leafPrefixCompressedFlag)
	binary.LittleEndian.PutUint16(image[14:16], 2)
	binary.LittleEndian.PutUint16(image[16:18], 20)
	binary.LittleEndian.PutUint16(image[18:20], 6029)
	binary.LittleEndian.PutUint16(image[20:22], 0)
	binary.LittleEndian.PutUint16(image[22:24], 6000)
	image[28] = FlagTombstone
	copy(image[29:6029], bytes.Repeat([]byte("a"), 6000))
	binary.LittleEndian.PutUint16(image[6029:6031], 6000)
	binary.LittleEndian.PutUint16(image[6031:6033], 1)
	image[6037] = FlagTombstone
	image[6038] = 'z'
	n = NewNodeView(image)
	for _, malformed := range []bool{false, true} {
		if malformed {
			binary.LittleEndian.PutUint16(image[6029:6031], 6001)
		}
		var d ProjectionDecoder
		var scratch [page.PageSize]byte
		ok, err := d.Begin(n, 1, scratch[:], projectionWork())
		if !ok || err != nil {
			t.Fatal(err)
		}
		for turn := 0; turn < 10; turn++ {
			_, err = d.Step(projectionWork())
			if err != nil {
				break
			}
		}
		want := ErrProjectionGeometry
		if malformed {
			want = ErrCorruptedNode
		}
		if err != want {
			t.Fatal("geometry/corruption classification", malformed, err)
		}
		if e, _ := d.Result(); e.Key != nil {
			t.Fatal("unfinished decoder exported key")
		}
		if !d.Cancel(projectionWork()) {
			t.Fatal("cancel")
		}
	}
}

// The existing producer emits these prefix-v2/columnar-prefix-v2 bytes. Large
// construction images are codec differential fixtures, not a claim that the
// DB's ordinary 4KiB leaf producer emits a larger decoded leaf.
func TestProjectionProducerLargePrefixContinuation(t *testing.T) {
	for _, columnar := range []bool{false, true} {
		image := make([]byte, 60000)
		b := NewBuilderWithOptions(image, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true, LeafColumnar: columnar, EntryRevisions: true})
		keys := make([][]byte, 24)
		for i := range keys {
			keys[i] = append(bytes.Repeat([]byte{'p'}, 12000), byte(i))
			flags := byte(FlagPointer)
			if i%5 == 0 {
				flags = FlagTombstone
			}
			if err := b.AddLeafEntryWithRevision(keys[i], nil, flags, page.ValuePtr{FileID: 1, Offset: uint64(100 + i), Length: 7}, page.EntryRevision(i+1)); err != nil {
				t.Fatal(err)
			}
		}
		b.Finish()
		n := NewNodeView(image)
		for i := len(keys) - 1; i >= 0; i-- {
			var d ProjectionDecoder
			scratch := make([]byte, ProjectionKeyCapacity)
			if ok, err := d.Begin(n, uint16(i), scratch, projectionWork()); !ok || err != nil {
				t.Fatal(ok, err)
			}
			paused := false
			for turn := 0; turn < 1000; turn++ {
				work := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 8192}
				done, err := d.Step(work)
				if err != nil {
					t.Fatal(columnar, i, err)
				}
				if d.phase == 1 && d.copied > 0 && d.copied < d.prefix+len(d.suffix) {
					paused = true
					copied := d.copied
					if done, err := d.Step(&iterator.OrdinalScanWork{}); done || err != nil || d.copied != copied {
						t.Fatal("zero allowance advanced partial key")
					}
					if e, _ := d.Result(); e.Key != nil {
						t.Fatal("partial key exported")
					}
				}
				if done {
					got, _ := d.Result()
					key, _, ptr, flags, revision, err := n.GetLeafEntryViewWithRevision(uint16(i))
					if err != nil || !bytes.Equal(got.Key, key) || !bytes.Equal(got.Key, keys[i]) || got.ValuePtr != ptr || got.Flags != flags || got.Revision != revision {
						t.Fatal("producer differential", columnar, i, err)
					}
					if i%16 != 0 && !paused {
						t.Fatal("large reconstruction never paused")
					}
					break
				}
				if turn == 999 {
					t.Fatal("large producer prefix stalled")
				}
			}
			if !d.Cancel(projectionWork()) {
				t.Fatal("cancel")
			}
		}
		b.ReleaseScratch()
	}
}
