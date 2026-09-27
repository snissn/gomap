package tree_test

import (
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

func TestIterator_GroupedRecordPrefetchChecksumMismatchFailsClosed(t *testing.T) {
	dir := t.TempDir()
	fileID, path, ptrs, _ := writeIteratorGroupedFrame(t, dir, 4)
	if fileID == 0 {
		t.Fatalf("unexpected zero fileID")
	}
	corruptGroupedFramePayloadByte(t, path, ptrs[0])

	mgr, err := valuelog.NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer func() { _ = mgr.Close() }()

	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1)
	data, _ := p.Get(0)
	n := node.NewNode(data)
	n.SetPageID(0)
	n.SetType(page.PageTypeLeaf)
	for i, ptr := range ptrs {
		key := fmt.Sprintf("k%02d", i)
		if err := n.AddLeafEntry([]byte(key), nil, node.FlagPointer, ptr); err != nil {
			t.Fatalf("AddLeafEntry(%s): %v", key, err)
		}
	}
	n.UpdateChecksum()

	tr := tree.New(p, mgr, 0)
	it := tr.Iterator(nil, nil)
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("iterator invalid before value read: %v", it.Error())
	}
	if got := it.ValueCopy(nil); got != nil {
		t.Fatalf("corrupt grouped record returned value %q", got)
	}
	if !errors.Is(it.Error(), valuelog.ErrCorrupt) {
		t.Fatalf("iterator error=%v want ErrCorrupt", it.Error())
	}
	if got := mgr.ReadStats().RecordCRCChecks; got != 1 {
		t.Fatalf("CRC checks after failed grouped prefetch=%d want 1", got)
	}
}

func writeIteratorGroupedFrame(t *testing.T, dir string, records int) (uint32, string, []page.ValuePtr, [][]byte) {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	recs := make([]valuelog.Record, records)
	want := make([][]byte, records)
	for i := range recs {
		value := []byte(fmt.Sprintf("grouped-value-%02d", i))
		recs[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
		want[i] = append([]byte(nil), value...)
	}
	ptrs, err := writer.AppendFrame(0, nil, recs)
	if err != nil {
		t.Fatalf("AppendFrame: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return fileID, path, ptrs, want
}

func corruptGroupedFramePayloadByte(t *testing.T, path string, ptr page.ValuePtr) {
	t.Helper()
	fh, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fh.Close() }()
	// ValuePtr.Offset points just after the record CRC prefix. Step back to the
	// record start, then skip the value-log record header and grouped-frame
	// header so the flipped byte lands inside the checksummed grouped payload.
	corruptOff := int64(ptr.Offset-4) + valuelog.HeaderSize + valuelog.FrameHeaderSize
	var b [1]byte
	if _, err := fh.ReadAt(b[:], corruptOff); err != nil {
		t.Fatalf("ReadAt corrupt byte: %v", err)
	}
	b[0] ^= 0xff
	if _, err := fh.WriteAt(b[:], corruptOff); err != nil {
		t.Fatalf("WriteAt corrupt byte: %v", err)
	}
}
