package tree_test

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
	"path/filepath"
	"testing"
)

type pointerViewReader struct {
	tree.SlabReader
	leaf []byte
}

func (r *pointerViewReader) ReadLeafLogPageUnsafeTo(p page.LeafLogPtr, dst []byte) ([]byte, bool, error) {
	return append(dst[:0], r.leaf...), true, nil
}
func pointerViewLeaf(t *testing.T, ptrs []page.ValuePtr) ([]byte, [][]byte) {
	t.Helper()
	data := make([]byte, page.PageSize)
	b := node.NewBuilder(data, page.PageTypeLeaf)
	b.SetPageID(99)
	keys := make([][]byte, 72)
	for i, p := range ptrs {
		if err := b.AddLeafEntry([]byte(fmt.Sprintf("k%03d", i)), nil, node.FlagPointer, p); err != nil {
			t.Fatal(err)
		}
	}
	b.FinishNoNode()
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%03d", i%len(ptrs)))
	}
	return data, keys
}
func newTreeWithLeafLogRoot(t *testing.T, sr tree.SlabReader, key []byte, ptr page.LeafLogPtr) (*tree.Tree, func()) {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	closeFn := func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(closeFn)
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	b := node.NewBuilder(data, page.PageTypeInternal)
	b.SetPageID(root)
	if err := b.AddInternalChildRef([]byte{}, page.LeafLogChildRef(ptr)); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	return tree.New(p, sr, root), closeFn
}
func TestTreeGetManyPointerViewVerifiedGroupedCache(t *testing.T) {
	dir := t.TempDir()
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	w.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	records := make([]valuelog.Record, 4)
	for i := range records {
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: bytes.Repeat([]byte{byte('a' + i)}, 512)}
	}
	ptrs, stats, err := w.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, len(records)))
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Kept {
		t.Fatal("frame was not compressed")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	m.SetDisableReadChecksum(false)
	m.SetCurrentWritableMmapEnabled(false)
	if err := m.PromoteCurrentWritable(fileID); err != nil {
		t.Fatal(err)
	}
	leaf, keys := pointerViewLeaf(t, ptrs)
	r := &pointerViewReader{SlabReader: m, leaf: leaf}
	// Expose the manager's actual append path rather than the map test appender.
	sr := &pointerViewManagerAppender{pointerViewReader: r, manager: m}
	tr, _ := newTreeWithLeafLogRoot(t, sr, nil, page.LeafLogPtr{FileID: 2, Offset: 100, RecordLengthHint: page.PageSize})
	if err := tr.GetManyView(keys, func(_ int, key, value []byte, found bool) error {
		for i := range records {
			if bytes.Equal(key, []byte(fmt.Sprintf("k%03d", i))) && !bytes.Equal(value, records[i].Value) {
				return errors.New("full grouped value mismatch")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cached := m.GroupedFrameCacheDetailedStats()
	t.Logf("same-batch CRC-verified grouped cache: %+v", cached)
	if cached.Hits != uint64(len(keys)-1) || cached.Stores != 1 || cached.Entries != 1 || cached.RetainedBytes != 2048 {
		t.Fatalf("same-batch verified cache not used: %+v", cached)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	off := int64(ptrs[0].Offset-4) + valuelog.HeaderSize + valuelog.FrameHeaderSize
	var b [1]byte
	if _, err = f.ReadAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 128
	if _, err = f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := tr.GetManyView(keys, func(int, []byte, []byte, bool) error { calls++; return nil }); !errors.Is(err, valuelog.ErrCorrupt) || calls != 0 {
		t.Fatalf("cached corruption err=%v callbacks=%d", err, calls)
	}
}

type pointerViewManagerAppender struct {
	*pointerViewReader
	manager *valuelog.Manager
}

func (r *pointerViewManagerAppender) ReadUnsafeAppend(p page.ValuePtr, dst []byte) ([]byte, error) {
	return r.manager.ReadUnsafeAppend(p, dst)
}
