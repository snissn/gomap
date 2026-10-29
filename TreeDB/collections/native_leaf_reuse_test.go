package collections

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

type countedNativeLeafCapture struct {
	backenddb.NativeSemanticCapture
	page  [page.PageSize]byte
	reads int
	fail  error
}

func (c *countedNativeLeafCapture) NativeLeafPage(_ page.LeafLogPtr, dst []byte) ([]byte, error) {
	c.reads++
	dst = dst[:page.PageSize]
	copy(dst, c.page[:])
	return dst, c.fail
}

func TestNativeLeafReuseExactScratch(t *testing.T) {
	capture := &countedNativeLeafCapture{}
	r := NativeJSONRead{capture: capture, scratch: &nativeDomainReadScratch{}}
	ptr := page.LeafLogPtr{FileID: 1, Offset: 2, RecordLengthHint: 3, SubIndex: 4}
	dst := r.scratch.leaf[:0]
	read := func(p page.LeafLogPtr, target []byte, want int) {
		t.Helper()
		if _, err := r.readLeaf(p, target); err != nil || capture.reads != want {
			t.Fatalf("physical reads=%d want%d err=%v", capture.reads, want, err)
		}
	}
	read(ptr, dst, 1)
	read(ptr, dst, 1)
	for _, p := range []page.LeafLogPtr{
		{FileID: 2, Offset: 2, RecordLengthHint: 3, SubIndex: 4},
		{FileID: 1, Offset: 3, RecordLengthHint: 3, SubIndex: 4},
		{FileID: 1, Offset: 2, RecordLengthHint: 4, SubIndex: 4},
		{FileID: 1, Offset: 2, RecordLengthHint: 3, SubIndex: 5},
	} {
		want := capture.reads + 1
		read(p, dst, want)
		read(p, dst, want)
	}
	// The importer and owned cursor pass other page buffers to this same
	// callback. They must neither replay nor leave the point-page identity live.
	var other [page.PageSize]byte
	want := capture.reads + 1
	read(ptr, other[:0], want)
	read(ptr, dst, want+1)
	read(ptr, dst, want+1)
	// A failed read may have overwritten its destination. It must never leave
	// either the old or newly attempted pointer marked reusable.
	failure := errors.New("partial physical read")
	capture.fail = failure
	if _, err := r.readLeaf(ptr, other[:0]); !errors.Is(err, failure) || r.scratch.leafValid {
		t.Fatalf("failed read retained a page: %v", err)
	}
	capture.fail = nil
	read(ptr, dst, capture.reads+1)
	r.closing = true
	if _, err := r.readLeaf(ptr, dst); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("closed scope replayed a page: %v", err)
	}
	r.closing = false
	r.detachScratch()
	if r.scratch != nil {
		t.Fatal("page alias survives scratch detachment")
	}
}

func TestNativeLeafReuseStillValidatesTreePage(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetVerifyOnRead(true)
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	ptr := page.LeafLogPtr{FileID: 1, Offset: 8}
	b := node.NewBuilder(data, page.PageTypeInternal)
	if err := b.AddInternalChildRef([]byte("key"), page.LeafLogChildRef(ptr)); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	b.ReleaseScratch()
	capture := &countedNativeLeafCapture{}
	leaf := node.NewBuilder(capture.page[:], page.PageTypeLeaf)
	if err := leaf.AddLeafEntry([]byte("key"), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	leaf.FinishNoNode()
	leaf.ReleaseScratch()
	r := NativeJSONRead{capture: capture, scratch: &nativeDomainReadScratch{}}
	tr := tree.New(p, nil, root)
	get := func() (node.LeafEntry, error) {
		return tr.GetEntryWithFixedScratch([]byte("key"), r.scratch.key[:], r.scratch.leaf[:], r.readLeaf)
	}
	for range 2 {
		entry, err := get()
		if err != nil || !bytes.Equal(entry.Value, []byte("value")) {
			t.Fatalf("captured point read: %v", err)
		}
	}
	if capture.reads != 1 {
		t.Fatalf("did not reuse the decoded page: %d", capture.reads)
	}
	// A decoded-page replay is below the tree validator, never a cached lookup
	// answer or a permission to skip checksum/type/PRIMARY operand validation.
	r.scratch.leaf[len(r.scratch.leaf)-1] ^= 1
	if _, err := get(); err == nil || capture.reads != 1 {
		t.Fatalf("replayed corrupt page bypassed tree validation: %v reads=%d", err, capture.reads)
	}
}
