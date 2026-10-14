package tree

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"unsafe"
)

// This fixture reader is not a production pager/frame adapter. Setup freezes
// real page images outside cursor calls; every fixture lookup/copy is charged.
type projectionFixtureReader struct {
	images                  map[page.ChildRef][]byte
	image, dst              []byte
	offset, begins, cancels int
	active                  bool
}

func (r *projectionFixtureReader) BeginProjectionFrame(ref page.ChildRef, dst []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if r.active {
		return false, ErrProjectionPending
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*r))+uint64(unsafe.Sizeof(ref))) {
		return false, nil
	}
	image := r.images[ref]
	if image == nil {
		return false, ErrProjectionOwner
	}
	r.image, r.dst, r.offset, r.active = image, dst, 0, true
	r.begins++
	return true, nil
}
func (r *projectionFixtureReader) StepProjectionFrame(w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if !r.active {
		return nil, false, ErrProjectionReader
	}
	if len(r.image) > page.PageSize {
		if !w.Reserve(1, 48) {
			return nil, false, nil
		}
		r.active = false
		return r.image, true, nil // owner, not a newly copied large image
	}
	if w.Bytes > w.ByteLimit {
		return nil, false, nil
	}
	amount := int(min(uint64(len(r.image)-r.offset), (w.ByteLimit-w.Bytes)/2))
	if amount == 0 || !w.Reserve(1, uint64(amount*2)) {
		return nil, false, nil
	}
	copy(r.dst[r.offset:r.offset+amount], r.image[r.offset:r.offset+amount])
	r.offset += amount
	if r.offset != len(r.image) {
		return nil, false, nil
	}
	r.active = false
	return r.dst[:len(r.image)], true, nil
}
func (r *projectionFixtureReader) CancelProjectionFrame(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*r))) {
		return false, nil
	}
	r.image, r.dst = nil, nil
	r.active = false
	r.cancels++
	return true, nil
}
func treeProjectionWork() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 16384}
}
func projectionFixtureDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "native4878-projection-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RETAINED_FIXTURE %s; consumer: this test process only; pagers closed on return", dir)
	return dir
}
func projectionFixtureTree(t *testing.T) (*Tree, *projectionFixtureReader, func()) {
	t.Helper()
	dir := projectionFixtureDir(t)
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	images := make(map[page.ChildRef][]byte)
	if _, err = p.Alloc(2); err != nil {
		t.Fatal(err)
	} // DATA metadata extent
	root, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	rootData, err := p.GetForWrite(root)
	if err != nil {
		t.Fatal(err)
	}
	rb := node.NewBuilderWithOptions(rootData, page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true})
	rb.SetPageID(root)
	for part := 0; part < 4; part++ {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatal(err)
		}
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{LeafColumnar: true, LeafPrefixCompression: true, EntryRevisions: true})
		b.SetPageID(id)
		for j := 0; j < 12; j++ {
			i := part*12 + j
			key := []byte(fmt.Sprintf("shared-prefix-%03d", i))
			flags := byte(node.FlagInline)
			val := []byte(fmt.Sprintf("v%d", i))
			ptr := page.ValuePtr{}
			if i%5 == 0 {
				flags = node.FlagTombstone
				val = nil
			}
			if i%5 == 1 {
				flags = node.FlagPointer
				val = nil
				ptr = page.ValuePtr{FileID: 7, Offset: uint64(i*20 + 10), Length: 12}
			}
			if err = b.AddLeafEntryWithRevision(key, val, flags, ptr, page.EntryRevision(i+1)); err != nil {
				t.Fatal(err)
			}
		}
		b.FinishNoNode()
		images[page.PageChildRef(id)] = bytes.Clone(data)
		if err = rb.AddInternalChild([]byte(fmt.Sprintf("shared-prefix-%03d", part*12)), id); err != nil {
			t.Fatal(err)
		}
	}
	rb.FinishNoNode()
	images[page.PageChildRef(root)] = bytes.Clone(rootData)
	return New(p, nil, root), &projectionFixtureReader{images: images}, func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}
}
func projectionRun(t *testing.T, c *ChargedProjection, op ProjectionOperation, key []byte) (node.LeafEntry, bool) {
	t.Helper()
	if err := c.Begin(op, key, treeProjectionWork()); err != nil {
		t.Fatal(err)
	}
	phase := c.phase
	begins := c.reader.(*projectionFixtureReader).begins
	if done, err := c.Step(&iterator.OrdinalScanWork{}); done || err != nil || c.phase != phase || c.reader.(*projectionFixtureReader).begins != begins {
		t.Fatal("zero work moved")
	}
	done := false
	for turn := 0; turn < 3000 && !done; turn++ {
		w := treeProjectionWork()
		var err error
		done, err = c.Step(w)
		if err != nil {
			t.Fatalf("phase %d base %d: %v", c.phase, c.base.phase, err)
		}
		if w.Records > 1 || w.Bytes > 16384 {
			t.Fatal("work overrun")
		}
	}
	if !done {
		t.Fatalf("stalled phase %d base %d", c.phase, c.base.phase)
	}
	e, ok, err := c.Record(treeProjectionWork())
	if err != nil {
		t.Fatal(err)
	}
	return e, ok
}
func projectionClose(t *testing.T, c *ChargedProjection, r *projectionFixtureReader) {
	t.Helper()
	for turn := 0; turn < 100; turn++ {
		done, err := c.CloseStep(treeProjectionWork())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if r.cancels != 1 || r.image != nil || r.dst != nil || c.certificate != nil || c.base.image != nil || c.base.record.Key != nil || c.base.key != nil {
				t.Fatal("retained close alias")
			}
			for _, s := range c.base.stack {
				if s.Node.Data() != nil {
					t.Fatal("parent alias")
				}
			}
			return
		}
	}
	t.Fatal("close stalled")
}
func projectionNew(t *testing.T, tr *Tree, cert *primaryarena.ReadRootCertificateV6, start, end []byte, r *projectionFixtureReader) *ChargedProjection {
	t.Helper()
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	c, err := tr.NewChargedProjection(cert, start, end, r, w)
	if err != nil {
		t.Fatal(err)
	}
	if r.begins != 0 || c.valid || c.pending || c.base.depth != 0 {
		t.Fatal("eager constructor")
	}
	return c
}
func TestChargedProjectionBaseDifferential(t *testing.T) {
	tr, r, closeTree := projectionFixtureTree(t)
	defer closeTree()
	for _, reverse := range []bool{false, true} {
		c := projectionNew(t, tr, nil, []byte("shared-prefix-003"), []byte("shared-prefix-044"), r)
		opts := IteratorOptions{Mode: IteratorModePointerProjection, IncludeTombstones: true}
		var ordinary iterator.UnsafeIterator
		op, next := ProjectionFirst, ProjectionNext
		if reverse {
			ordinary = tr.ReverseIteratorWithOptions(c.start, c.end, opts)
			op, next = ProjectionLast, ProjectionPrev
		} else {
			ordinary = tr.IteratorWithOptions(c.start, c.end, opts)
		}
		for {
			got, ok := projectionRun(t, c, op, nil)
			if ok != ordinary.Valid() {
				t.Fatal("valid mismatch", reverse, string(got.Key), ordinary.Error())
			}
			if !ok {
				break
			}
			value, ptr, flags, revision := ordinary.(interface {
				UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision)
			}).UnsafeEntryWithRevision()
			if !bytes.Equal(got.Key, ordinary.UnsafeKey()) || !bytes.Equal(got.Value, value) || got.ValuePtr != ptr || got.Flags != flags || got.Revision != revision {
				t.Fatalf("different record %+v, key %s", got, ordinary.UnsafeKey())
			}
			ordinary.Next()
			op = next
		}
		if err := ordinary.Close(); err != nil {
			t.Fatal(err)
		}
		projectionClose(t, c, r)
		r.cancels, r.begins = 0, 0
	}
	// Independent seeks use the ordinary forward/reverse selector, including gaps.
	for _, key := range []string{"", "shared-prefix-000", "shared-prefix-011z", "shared-prefix-012", "shared-prefix-047", "zzz"} {
		for _, reverse := range []bool{false, true} {
			c := projectionNew(t, tr, nil, nil, nil, r)
			op := ProjectionSeekGE
			opts := IteratorOptions{Mode: IteratorModePointerProjection, IncludeTombstones: true}
			var ordinary iterator.UnsafeIterator
			if reverse {
				op = ProjectionSeekLE
				ordinary = tr.ReverseIteratorWithOptions(nil, nil, opts)
			} else {
				ordinary = tr.IteratorWithOptions(nil, nil, opts)
			}
			ordinary.Seek([]byte(key))
			got, ok := projectionRun(t, c, op, []byte(key))
			if ok != ordinary.Valid() || ok && !bytes.Equal(got.Key, ordinary.UnsafeKey()) {
				t.Fatalf("seek %q reverse %v got %q valid %v want %q valid %v err %v", key, reverse, got.Key, ok, ordinary.UnsafeKey(), ordinary.Valid(), ordinary.Error())
			}
			ordinary.Close()
			projectionClose(t, c, r)
			r.cancels, r.begins = 0, 0
		}
	}
}
func TestChargedProjectionRefusalAndPartialClose(t *testing.T) {
	tr, r, closeTree := projectionFixtureTree(t)
	defer closeTree()
	if c, err := tr.NewChargedProjection(nil, nil, nil, func() { panic("opaque callback invoked") }, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); c != nil || err != ErrProjectionReader {
		t.Fatal("opaque callback admitted", err)
	}
	if c, err := tr.NewChargedProjection(nil, nil, nil, r, &iterator.OrdinalScanWork{}); c != nil || err == nil || r.begins != 0 {
		t.Fatal("unadmitted constructor")
	}
	c := projectionNew(t, tr, nil, nil, nil, r)
	if err := c.Begin(ProjectionNext, nil, treeProjectionWork()); err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 20 && !r.active; turn++ {
		if _, err := c.Step(treeProjectionWork()); err != nil {
			t.Fatal(err)
		}
	}
	if !r.active {
		t.Fatal("no active phased read")
	}
	if _, err := c.Step(&iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 18}); err != nil {
		t.Fatal(err)
	}
	if r.offset != 9 {
		t.Fatal("frame did not retain partial copy", r.offset)
	}
	if err := c.Begin(ProjectionFirst, nil, treeProjectionWork()); err != ErrProjectionPending {
		t.Fatal("paused replacement admitted")
	}
	projectionClose(t, c, r)
	if _, err := c.Step(treeProjectionWork()); err != ErrProjectionClosed {
		t.Fatal("closed cursor revived")
	}
}
func TestChargedProjectionPrimaryDifferentialAndOwner(t *testing.T) {
	tr, r, closeTree := projectionFixtureTree(t)
	defer closeTree()
	a, err := primaryarena.OpenCapsule(filepath.Join(projectionFixtureDir(t), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	entries := []node.PrimaryDirectoryEntry{}
	var refs [49]primaryarena.Ref
	for i, key := range []string{"shared-prefix-002", "shared-prefix-011", "shared-prefix-019z", "shared-prefix-033"} {
		kind := node.PrimaryPut
		if i == 1 || i == 2 {
			kind = node.PrimaryAbsence
		}
		entry := node.PrimaryDirectoryEntry{Key: []byte(key), Kind: kind, Revision: page.EntryRevision(101 + i), ClassSlot: uint16(i)}
		if i != 2 {
			claim, ok, err := a.PrepareClaim(primaryarena.Component, nil)
			if !ok || err != nil {
				t.Fatal(err)
			}
			var ref primaryarena.Ref
			for turn := 0; turn < 100; turn++ {
				ref, ok, err = claim.Step(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					break
				}
			}
			if !ok {
				t.Fatal("component stalled")
			}
			refs[i] = ref
			data := make([]byte, page.PageSize)
			b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
			b.SetPageID(ref.PageID)
			flags := byte(node.FlagInline)
			value := []byte("new")
			if kind == node.PrimaryAbsence {
				flags = node.FlagTombstone
				value = nil
			}
			if err = b.AddLeafEntryWithRevision(entry.Key, value, flags, page.ValuePtr{}, entry.Revision); err != nil {
				t.Fatal(err)
			}
			b.FinishNoNode()
			if ok, err = a.SealComponent(ref, data, nil); !ok || err != nil {
				t.Fatal(err)
			}
			image, err := a.Get(ref.PageID)
			if err != nil {
				t.Fatal(err)
			}
			entry.Operand = node.PrimaryOperand{Ref: page.PageChildRef(ref.PageID), Digest: sha256.Sum256(image)}
			r.images[entry.Operand.Ref] = bytes.Clone(image)
		}
		entries = append(entries, entry)
	}
	root, ok, err := a.PrepareReadRootV6(nil)
	if !ok || err != nil {
		t.Fatal(err)
	}
	g, ok, err := a.NewGroup(refs, nil)
	if !ok || err != nil {
		t.Fatal(err)
	}
	var groups [1]*primaryarena.Group
	groups[0] = g
	image := make([]byte, page.PageSize)
	base := node.PrimaryOperand{Ref: page.PageChildRef(tr.rootPageID), Digest: sha256.Sum256(r.images[page.PageChildRef(tr.rootPageID)])}
	if err = node.EncodePrimaryDirectory(image, root.PageID, 1, base, entries); err != nil {
		t.Fatal(err)
	}
	if ok, err = a.SealDirectory(root, image, groups, nil); !ok || err != nil {
		t.Fatal(err)
	}
	// Drop constructor/intermediate references, keeping the real reader edge.
	if _, err = a.Acquire(root, nil); err != nil {
		t.Fatal(err)
	}
	a.Drop(root, nil)
	a.DropGroup(g, nil)
	for _, ref := range refs {
		if ref != (primaryarena.Ref{}) {
			a.Drop(ref, nil)
		}
	}
	for turn := 0; turn < 100; turn++ {
		_, progress, err := a.ReleaseStep(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if !progress {
			break
		}
	}
	cert, ok, err := a.BorrowReadRootCertificateV6(root, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ok || err != nil {
		t.Fatal(err)
	}
	if err = tr.pager.AttachPrimaryBankPager(a.Pager()); err != nil {
		t.Fatal(err)
	}
	primary := New(tr.pager, nil, root.PageID)
	before := a.Counters()
	for _, reverse := range []bool{false, true} {
		c := projectionNew(t, primary, cert, nil, nil, r)
		opts := IteratorOptions{Mode: IteratorModePointerProjection, IncludeTombstones: true}
		op, next := ProjectionFirst, ProjectionNext
		var ordinary iterator.UnsafeIterator
		if reverse {
			ordinary = primary.ReverseIteratorWithOptions(nil, nil, opts)
			op, next = ProjectionLast, ProjectionPrev
		} else {
			ordinary = primary.IteratorWithOptions(nil, nil, opts)
		}
		for {
			got, valid := projectionRun(t, c, op, nil)
			if valid != ordinary.Valid() {
				t.Fatal("primary valid mismatch", reverse, ordinary.Error(), string(got.Key))
			}
			if !valid {
				break
			}
			value, ptr, flags, revision := ordinary.(interface {
				UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision)
			}).UnsafeEntryWithRevision()
			if !bytes.Equal(got.Key, ordinary.UnsafeKey()) || !bytes.Equal(got.Value, value) || got.ValuePtr != ptr || got.Flags != flags || got.Revision != revision {
				t.Fatalf("primary difference %q got %+v want value %q ptr %+v flags %d revision %d", ordinary.UnsafeKey(), got, value, ptr, flags, revision)
			}
			ordinary.Next()
			op = next
		}
		ordinary.Close()
		projectionClose(t, c, r)
		r.cancels, r.begins = 0, 0
	}
	for _, key := range []string{"", "shared-prefix-002", "shared-prefix-011z", "shared-prefix-019z", "shared-prefix-047", "zzz"} {
		for _, reverse := range []bool{false, true} {
			c := projectionNew(t, primary, cert, []byte("shared-prefix-003"), []byte("shared-prefix-044"), r)
			op := ProjectionSeekGE
			opts := IteratorOptions{Mode: IteratorModePointerProjection, IncludeTombstones: true}
			var ordinary iterator.UnsafeIterator
			if reverse {
				op = ProjectionSeekLE
				ordinary = primary.ReverseIteratorWithOptions(c.start, c.end, opts)
			} else {
				ordinary = primary.IteratorWithOptions(c.start, c.end, opts)
			}
			ordinary.Seek([]byte(key))
			got, valid := projectionRun(t, c, op, []byte(key))
			if valid != ordinary.Valid() || valid && !bytes.Equal(got.Key, ordinary.UnsafeKey()) {
				t.Fatalf("primary seek %q reverse %v: got %q/%v want %q/%v error %v", key, reverse, got.Key, valid, ordinary.UnsafeKey(), ordinary.Valid(), ordinary.Error())
			}
			ordinary.Close()
			projectionClose(t, c, r)
			r.cancels, r.begins = 0, 0
		}
	}
	turnCursor := projectionNew(t, primary, cert, nil, nil, r)
	if e, ok := projectionRun(t, turnCursor, ProjectionSeekGE, []byte("shared-prefix-011")); !ok || string(e.Key) != "shared-prefix-011" {
		t.Fatal("turn seek")
	}
	for _, step := range []struct {
		op  ProjectionOperation
		key string
	}{
		{ProjectionPrev, "shared-prefix-010"}, {ProjectionNext, "shared-prefix-011"},
		{ProjectionNext, "shared-prefix-012"}, {ProjectionPrev, "shared-prefix-011"},
		{ProjectionPrev, "shared-prefix-010"}, {ProjectionNext, "shared-prefix-011"},
		{ProjectionSeekLE, "shared-prefix-047"},
	} {
		if e, ok := projectionRun(t, turnCursor, step.op, nil); !ok || string(e.Key) != step.key {
			t.Fatalf("direction change got %q/%v want %q", e.Key, ok, step.key)
		}
	}
	projectionClose(t, turnCursor, r)
	r.cancels, r.begins = 0, 0

	// CRC-valid substitution still fails its exact original operand digest.
	operand := entries[0].Operand.Ref
	old := r.images[operand]
	changed := bytes.Clone(old)
	changed[len(changed)-1] ^= 1
	page.UpdateChecksum(changed)
	r.images[operand] = changed
	c := projectionNew(t, primary, cert, nil, nil, r)
	if err = c.Begin(ProjectionSeekGE, entries[0].Key, treeProjectionWork()); err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 300; turn++ {
		_, err = c.Step(treeProjectionWork())
		if err != nil {
			break
		}
	}
	if err != node.ErrPrimaryDirectory {
		t.Fatal("substituted PRIMARY component accepted", err)
	}
	if _, valid, _ := c.Record(treeProjectionWork()); valid {
		t.Fatal("substitution fell back to old base")
	}
	projectionClose(t, c, r)
	r.cancels, r.begins = 0, 0
	r.images[operand] = old

	if a.Counters() != before {
		t.Fatal("consumer changed original arena custody")
	}
	if _, ok, err = a.BorrowReadRootCertificateV6(root, nil); !ok || err != nil {
		t.Fatal("close killed caller root")
	}
	if _, err = a.Drop(root, nil); err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 100; turn++ {
		_, progress, err := a.ReleaseStep(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if !progress {
			break
		}
	}
	if _, ok, err = a.BorrowReadRootCertificateV6(root, nil); ok || err == nil {
		t.Fatal("actual owner death did not invalidate certificate")
	}
}

func TestChargedProjectionLargeBorrowedLeafAndCRC(t *testing.T) {
	tr, r, closeTree := projectionFixtureTree(t)
	defer closeTree()
	// Plain v1 layout permits a large borrowed inline operand and uint16 key.
	// This is a codec fixture, not a qualification of the production frame reader.
	key := bytes.Repeat([]byte("k"), 12<<10)
	value := bytes.Repeat([]byte("v"), 2<<20)
	image := make([]byte, 18+7+len(key)+len(value))
	n := node.NewNodeView(image)
	n.SetType(page.PageTypeLeaf)
	binary.LittleEndian.PutUint16(image[14:16], 1)
	binary.LittleEndian.PutUint16(image[16:18], 18)
	binary.LittleEndian.PutUint16(image[18:20], uint16(len(key)))
	binary.LittleEndian.PutUint32(image[20:24], uint32(len(value)))
	image[24] = node.FlagInline
	copy(image[25:], key)
	copy(image[25+len(key):], value)
	page.UpdateChecksum(image)
	ref := page.LeafLogChildRef(page.LeafLogPtr{FileID: 1, Offset: 8, RecordLengthHint: uint32(len(image))})
	rootImage := make([]byte, page.PageSize)
	b := node.NewBuilder(rootImage, page.PageTypeInternal)
	b.SetPageID(tr.rootPageID)
	if err := b.AddInternalChildRef([]byte(""), ref); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	r.images[page.PageChildRef(tr.rootPageID)] = rootImage
	r.images[ref] = image
	c := projectionNew(t, tr, nil, nil, nil, r)
	if err := c.Begin(ProjectionSeekGE, key, treeProjectionWork()); err != nil {
		t.Fatal(err)
	}
	done := false
	sawCRC := false
	for turn := 0; turn < 3000 && !done; turn++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		var err error
		done, err = c.Step(w)
		if err != nil {
			t.Fatal(err)
		}
		if w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("large operand overrun")
		}
		if c.base.crcOffset > 0 && c.base.crcOffset < len(image) && c.base.ref == ref {
			sawCRC = true
			at := c.base.crcOffset
			phase := c.base.phase
			if _, err = c.Step(&iterator.OrdinalScanWork{}); err != nil || c.base.crcOffset != at || c.base.phase != phase {
				t.Fatal("unadmitted CRC advanced")
			}
		}
	}
	if !done || !sawCRC {
		t.Fatal("large CRC did not phase")
	}
	got, ok, err := c.Record(treeProjectionWork())
	if err != nil || !ok || !bytes.Equal(got.Key, key) || !bytes.Equal(got.Value, value) || &got.Value[0] != &image[25+len(key)] {
		t.Fatal("large borrowed projection", ok, err)
	}
	if _, valid := projectionRun(t, c, ProjectionPrev, nil); valid {
		t.Fatal("large turn passed its sole predecessor")
	}
	if e, valid := projectionRun(t, c, ProjectionNext, nil); !valid || !bytes.Equal(e.Key, key) {
		t.Fatal("large direction turn lost producer key")
	}

	projectionClose(t, c, r)
	// A wrong CRC refuses before entry/payload export, with no fallback.
	r.cancels, r.begins = 0, 0
	image[len(image)-1] ^= 1
	c = projectionNew(t, tr, nil, nil, nil, r)
	if err = c.Begin(ProjectionFirst, nil, treeProjectionWork()); err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 1000; turn++ {
		_, err = c.Step(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			break
		}
	}
	if err != node.ErrCorruptedNode {
		t.Fatal("invalid large CRC admitted", err)
	}
	if _, ok, _ = c.Record(treeProjectionWork()); ok {
		t.Fatal("invalid record exported")
	}
	projectionClose(t, c, r)
}
func TestChargedProjectionInvalidFixedPageFailsClosed(t *testing.T) {
	tr, r, closeTree := projectionFixtureTree(t)
	defer closeTree()
	r.images[page.PageChildRef(tr.rootPageID)][page.PageSize-1] ^= 1
	c := projectionNew(t, tr, nil, nil, nil, r)
	if err := c.Begin(ProjectionFirst, nil, treeProjectionWork()); err != nil {
		t.Fatal(err)
	}
	var err error
	for turn := 0; turn < 100; turn++ {
		_, err = c.Step(treeProjectionWork())
		if err != nil {
			break
		}
	}
	if err != node.ErrCorruptedNode {
		t.Fatal("bad CRC admitted", err)
	}
	if _, ok, _ := c.Record(treeProjectionWork()); ok {
		t.Fatal("bad CRC exported")
	}
	projectionClose(t, c, r)
}

func TestChargedProjectionAllocationAdmission(t *testing.T) {
	tr, r, closeTree := projectionFixtureTree(t)
	defer closeTree()
	runtime.GC()
	var before, after runtime.MemStats
	var cursors [8]*ChargedProjection
	runtime.ReadMemStats(&before)
	for i := range cursors {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		c, err := tr.NewChargedProjection(nil, nil, nil, r, w)
		if err != nil {
			t.Fatal(err)
		}
		cursors[i] = c
		if w.Bytes < ChargedProjectionRetainedCharge() {
			t.Fatal("constructor capacity omitted")
		}
	}
	runtime.ReadMemStats(&after)
	actual := after.TotalAlloc - before.TotalAlloc
	charge := uint64(len(cursors)) * ChargedProjectionRetainedCharge()
	// TotalAlloc is process-wide; concurrent runtime/test bookkeeping is not
	// attributable to this cursor. Assert the constructor itself has exactly one
	// allocation, then use the existing allocator capacity policy for that block.
	var samples [9]*ChargedProjection
	next := 0
	allocs := testing.AllocsPerRun(8, func() {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		c, e := tr.NewChargedProjection(nil, nil, nil, r, w)
		if e != nil {
			panic(e)
		}
		samples[next] = c
		next++
	})
	if allocs != 1 {
		t.Fatalf("constructor hidden allocations: %g", allocs)
	}
	for _, c := range samples {
		projectionClose(t, c, r)
		r.cancels = 0
	}
	if actual > charge {
		t.Logf("process-wide TotalAlloc includes %dB beyond cursor capacity; exact isolated witness is recorded separately", actual-charge)
	}
	t.Logf("raw=%dB retained=%dB; eight constructors actual=%dB admitted=%dB", ChargedProjectionAllocationSize(), ChargedProjectionRetainedCharge(), actual, charge)
	for _, c := range cursors {
		projectionClose(t, c, r)
		r.cancels = 0
	}
	runtime.KeepAlive(cursors)
}

func TestChargedProjectionConcurrentSameImmutableRoot(t *testing.T) {
	tr, source, closeTree := projectionFixtureTree(t)
	defer closeTree()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		r := &projectionFixtureReader{images: source.images}
		c := projectionNew(t, tr, nil, nil, nil, r)
		wg.Add(1)
		go func() {
			defer wg.Done()
			op := ProjectionFirst
			for i := 0; i < 48; i++ {
				e, ok := projectionRun(t, c, op, nil)
				if !ok || string(e.Key) != fmt.Sprintf("shared-prefix-%03d", i) {
					t.Errorf("shared root record %d = %q/%v", i, e.Key, ok)
					break
				}
				op = ProjectionNext
			}
			if done, err := c.CloseStep(&iterator.OrdinalScanWork{}); done || err != nil || c.closing {
				t.Error("zero close changed ownership")
			}
			projectionClose(t, c, r)
		}()
	}
	wg.Wait()
}

func TestChargedProjectionSkipsEmptyPhysicalLeaf(t *testing.T) {
	tr, r, closeTree := projectionFixtureTree(t)
	defer closeTree()
	ref := page.PageChildRef(tr.rootPageID + 1)
	image := r.images[ref]
	binary.LittleEndian.PutUint16(image[14:16], 0)
	page.UpdateChecksum(image)
	c := projectionNew(t, tr, nil, nil, nil, r)
	if e, ok := projectionRun(t, c, ProjectionFirst, nil); !ok || string(e.Key) != "shared-prefix-012" {
		t.Fatal("empty sibling ended traversal", string(e.Key), ok)
	}
	if _, ok := projectionRun(t, c, ProjectionPrev, nil); ok {
		t.Fatal("empty predecessor resurrected")
	}
	if e, ok := projectionRun(t, c, ProjectionNext, nil); !ok || string(e.Key) != "shared-prefix-012" {
		t.Fatal("empty-root boundary turn", string(e.Key), ok)
	}
	projectionClose(t, c, r)
}
