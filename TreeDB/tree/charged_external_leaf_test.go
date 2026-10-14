package tree_test

import (
	"bytes"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
)

// This test composes one frozen producer parent with the real file continuation.
// Parent setup is outside the scan; production pager capture remains separate.
type externalLeafFixture struct {
	parent []byte
	root   uint64
	leaf   valuelog.COWLeafProjectionReader
	dst    []byte
	at     int
	isLeaf bool
}

func (r *externalLeafFixture) BeginProjectionFrame(ref page.ChildRef, dst []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if ref.IsLeafLog() {
		r.isLeaf = true
		return r.leaf.BeginProjectionFrame(ref, dst, w)
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(ref))+48) {
		return false, nil
	}
	if ref.Page != r.root {
		return false, fmt.Errorf("foreign parent")
	}
	r.isLeaf = false
	r.dst = dst
	r.at = 0
	return true, nil
}
func (r *externalLeafFixture) StepProjectionFrame(w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if r.isLeaf {
		return r.leaf.StepProjectionFrame(w)
	}
	if w.Bytes > w.ByteLimit {
		return nil, false, nil
	}
	n := int(min(uint64(len(r.parent)-r.at), (w.ByteLimit-w.Bytes)/2))
	if n == 0 || !w.Reserve(1, uint64(2*n)) {
		return nil, false, nil
	}
	copy(r.dst[r.at:r.at+n], r.parent[r.at:r.at+n])
	r.at += n
	return r.dst, r.at == len(r.parent), nil
}
func (r *externalLeafFixture) CancelProjectionFrame(w *iterator.OrdinalScanWork) (bool, error) {
	if r.isLeaf {
		return r.leaf.CancelProjectionFrame(w)
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(*r))) {
		return false, nil
	}
	r.parent, r.dst = nil, nil
	return true, nil
}
func frameWork() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 16384}
}

type externalCodecOwner struct{ live uint64 }

func (o *externalCodecOwner) RetainCodecBytes(n uint64) bool {
	if n > 8<<20-o.live {
		return false
	}
	o.live += n
	return true
}
func (o *externalCodecOwner) ReleaseCodecBytes(n uint64) {
	if n > o.live {
		panic("unowned codec release")
	}
	o.live -= n
}

func TestChargedProjectionRealExternalProducerFrames(t *testing.T) {
	for _, codec := range []valuelog.BlockCodec{valuelog.BlockCodecNone, valuelog.BlockCodecSnappy, valuelog.BlockCodecLZ4, valuelog.BlockCodecZSTD} {
		t.Run(fmt.Sprint(codec), func(t *testing.T) {
			work := frameWork
			if codec == valuelog.BlockCodecZSTD {
				work = func() *iterator.OrdinalScanWork {
					return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				}
			}
			dir, err := os.MkdirTemp(os.TempDir(), "native4878-frame-")
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("RETAINED_FIXTURE %s; consumer: this test process only; files/pager closed on return", dir)
			leafDir := filepath.Join(dir, "leaf_vlog")
			if err = os.Mkdir(leafDir, 0700); err != nil {
				t.Fatal(err)
			}
			id, err := valuelog.EncodeFileID(valuelog.ReservedLeafLogLaneID, 1)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(leafDir, fmt.Sprintf("value-l%d-000001.log", valuelog.ReservedLeafLogLaneID))
			writer, err := valuelog.NewWriter(path, id)
			if err != nil {
				t.Fatal(err)
			}
			writer.SetBlockCompression(codec, codec != valuelog.BlockCodecNone)
			var records []valuelog.Record
			for part := 0; part < 3; part++ {
				image := make([]byte, page.PageSize)
				b := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true, EntryRevisions: true})
				for i := 0; i < 12; i++ {
					ordinal := part*12 + i
					flags := byte(node.FlagInline)
					value := []byte("real value")
					if ordinal%5 == 0 {
						flags = node.FlagTombstone
						value = nil
					}
					if err = b.AddLeafEntryWithRevision([]byte(fmt.Sprintf("external-%03d", ordinal)), value, flags, page.ValuePtr{}, page.EntryRevision(ordinal+1)); err != nil {
						t.Fatal(err)
					}
				}
				b.FinishNoNode()
				compact, ok, err := valuelog.MaybeCompactLeafLogPayload(image)
				if !ok || err != nil {
					t.Fatal(ok, err)
				}
				records = append(records, valuelog.Record{RID: uint64(part + 1), Value: compact})
			}
			ptrs, _, err := writer.AppendFrameWithStats(0, nil, records)
			if err != nil {
				t.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err = p.Alloc(2); err != nil {
				t.Fatal(err)
			}
			root, err := p.Alloc(1)
			if err != nil {
				t.Fatal(err)
			}
			parent, err := p.GetForWrite(root)
			if err != nil {
				t.Fatal(err)
			}
			b := node.NewBuilder(parent, page.PageTypeInternal)
			b.SetPageID(root)
			for part, ptr := range ptrs {
				log, err := page.LeafLogPtrFromValuePtr(ptr)
				if err != nil {
					t.Fatal(err)
				}
				if err = b.AddInternalChildRef([]byte(fmt.Sprintf("external-%03d", part*12)), page.LeafLogChildRef(log)); err != nil {
					t.Fatal(err)
				}
			}
			b.FinishNoNode()
			r := &externalLeafFixture{root: root, parent: bytes.Clone(parent), leaf: valuelog.COWLeafProjectionReader{File: f, FileID: page.ValueLogSegmentID(id), Limits: valuelog.COWReadLimits{MaxRecordBytes: 2 << 20, MaxRawBytes: valuelog.MaxFrameK * page.PageSize, MaxValueBytes: page.PageSize}, Input: make([]byte, 2<<20), Raw: make([]byte, valuelog.MaxFrameK*page.PageSize), VerifyCRC: true}}
			tr := tree.New(p, nil, root)
			codecOwner := &externalCodecOwner{}
			if codec == valuelog.BlockCodecZSTD {
				r.leaf.ZSTD = &valuelog.COWZSTDContinuation{Owner: f, Retention: codecOwner}
			}
			cursor, err := tr.NewChargedProjection(nil, nil, nil, r, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			run := func(op tree.ProjectionOperation) (node.LeafEntry, bool) {
				if err = cursor.Begin(op, nil, work()); err != nil {
					t.Fatal(err)
				}
				done := false
				for turns := 0; turns < 10000 && !done; turns++ {
					w := work()
					done, err = cursor.Step(w)
					if err != nil {
						t.Fatal(err)
					}
					if w.Records > w.RecordLimit || w.Bytes > w.ByteLimit {
						t.Fatal("quantum overrun")
					}
				}
				if !done {
					t.Fatal("stalled")
				}
				e, ok, err := cursor.Record(work())
				if err != nil {
					t.Fatal(err)
				}
				return e, ok
			}
			op := tree.ProjectionFirst
			for i := 0; i < 36; i++ {
				e, ok := run(op)
				if !ok || string(e.Key) != fmt.Sprintf("external-%03d", i) || e.Revision != page.EntryRevision(i+1) {
					t.Fatal("forward differential", i, e, ok)
				}
				if (e.Flags&node.FlagTombstone != 0) != (i%5 == 0) {
					t.Fatal("physical tombstone lost")
				}
				op = tree.ProjectionNext
			}
			for i := 34; i >= 0; i-- {
				e, ok := run(tree.ProjectionPrev)
				if !ok || string(e.Key) != fmt.Sprintf("external-%03d", i) {
					t.Fatal("reverse differential", i, e, ok)
				}
			}
			done := false
			for turns := 0; turns < 1000 && !done; turns++ {
				done, err = cursor.CloseStep(work())
				if err != nil {
					t.Fatal(err)
				}
			}
			if !done {
				t.Fatal("close stalled")
			}
			if codecOwner.live != 0 {
				t.Fatal("tree kept actual codec storage", codecOwner.live)
			}
			if r.leaf.File != nil || r.leaf.Input != nil || r.leaf.Raw != nil {
				t.Fatal("leaf reader custody retained")
			}
			if _, err = f.Stat(); err != nil {
				t.Fatal("consumer closed original file")
			}
		})
	}
}
