package freelist

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

// This tests the new retained internal capability against the SAME ordinary
// materializer and decoder contracts. The prior API had no finite emission
// entry point; compile absence is not claimed as a runtime RED.
func TestCanonicalStateEmissionRetainsPagesAndCanonicalBytesV1(t *testing.T) {
	makeRoot := func() *stateNode {
		g := MustNewFreelistGenerationV1(9, 4096, []uint64{2, 258, 514}, nil)
		c := lookupChunk(g.root, 1)
		c.retired[4], c.retired[250] = 7, 11
		for _, child := range g.root.child {
			if child != nil {
				recomputeStateNode(child, 0)
			}
		}
		recomputeStateNode(g.root, 0)
		return g.root
	}
	expectedRoot := makeRoot()
	expectedSink := &recordingSink{sink: NewCandidatePageSinkV1()}
	expectedNext := uint64(800)
	if e := emitStatePages(expectedRoot, 0, 10, &expectedNext, expectedSink); e != nil {
		t.Fatal(e)
	}
	root := makeRoot()
	var count statePageCountWorkV1
	for i := 0; ; i++ {
		if i == 128 {
			t.Fatal("retained count stalled")
		}
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, e := count.step(root, &w)
		if e != nil {
			t.Fatal(e)
		}
		if ready {
			break
		}
	}
	if count.count != uint64(len(expectedSink.pages)) {
		t.Fatalf("count=%d pages=%d", count.count, len(expectedSink.pages))
	}
	sink := &recordingSink{sink: NewCandidatePageSinkV1()}
	next := uint64(800)
	var work statePageEmissionWorkV1
	calls := 0
	for ; calls < 128; calls++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, e := work.step(root, 10, &next, sink, &w)
		if e != nil {
			t.Fatal(e)
		}
		if w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("actual emission exceeded allowance")
		}
		for _, p := range sink.pages {
			if !page.VerifyChecksumNonMutating(p.Data) {
				t.Fatal("sink retained unfinished page")
			}
		}
		if ready {
			break
		}
	}
	if calls == 128 || calls < 24 {
		t.Fatalf("actual 256-slot encoders did not retain progress: %d", calls)
	}
	if next != expectedNext || !pageImagesEqual(sink.pages, expectedSink.pages) {
		t.Fatal("finite canonical bytes/IDs differ from ordinary materialization")
	}
	// The reference run binds these existing ordinary encoder entry points to
	// the retained, pre-change format.go. Final-source tests also drain the same
	// entries normally; the receipt records which exact reference was compiled.
	images := make(map[uint64][]byte, len(sink.pages))
	for _, p := range sink.pages {
		images[p.PageID] = p.Data
	}
	var checkReference func(*stateNode)
	checkReference = func(n *stateNode) {
		if n == nil {
			return
		}
		var reference []byte
		if n.chunk != nil {
			reference = encodeChunkPage(n.pageID, 10, n.chunk)
		} else {
			var e error
			reference, e = encodeIndexPage(n.pageID, 10, n, int(n.depth))
			if e != nil {
				t.Fatal(e)
			}
			for _, child := range n.child {
				checkReference(child)
			}
		}
		if !bytes.Equal(reference, images[n.pageID]) {
			t.Fatalf("retained page %d differs from canonical format reference", n.pageID)
		}
		t.Logf("canonical reference page=%d SHA256=%x", n.pageID, sha256.Sum256(reference))
	}
	checkReference(expectedRoot)
	decoded, e := loadStateRefV2(&MemoryPageStoreV1{Pages: images}, root.pageID, stateNodeKindV2(root), 10, true, true, next, map[uint64]struct{}{}, -1)
	if e != nil {
		t.Fatal(e)
	}
	if decoded.freeCount != 3 || decoded.retiredCount != 2 || decoded.minRetiredSeq != 7 || lookupChunk(decoded, 1).retired[250] != 11 {
		t.Fatal("canonical decoder lost free/retired state")
	}
	first := append([]byte(nil), sink.pages[0].Data...)
	if _, e := decodeChunkPage(first, 10, 0); e != nil {
		t.Fatal(e)
	}
	// Control reuse must not reset bytes retained by a candidate/writer.
	for i := range sink.pages[1:] {
		clear(sink.pages[i+1].Data)
	}
	if !bytes.Equal(first, sink.pages[0].Data) {
		t.Fatal("candidate page output aliases a later encoded page")
	}
	t.Logf("actual canonical count=%d emission resumes=%d retained pages=%d", count.count, calls+1, len(sink.pages))
}

func TestCanonicalStateEmissionRejectsOpaqueBeforeEffectsAndKeepsFirstErrorV1(t *testing.T) {
	root := MustNewFreelistGenerationV1(9, 4096, []uint64{2}, nil).root
	original := lookupChunk(root, 0)
	next := uint64(800)
	opaque := &mutatingRetainingPageSinkV1{}
	sink := &recordingSink{sink: opaque}
	var refused statePageEmissionWorkV1
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if _, e := refused.step(root, 10, &next, sink, &w); !errors.Is(e, ErrGenerationFormat) || root.pageID != 0 || original.pageID != 0 || next != 800 || len(opaque.pages) != 0 {
		t.Fatal("opaque finite sink ran before refusal")
	}

	concrete := NewCandidatePageSinkV1()
	concrete.pageIDs[800] = struct{}{}
	sink = &recordingSink{sink: concrete}
	var work statePageEmissionWorkV1
	var first error
	for calls := 0; calls < 128; calls++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		_, e := work.step(root, 10, &next, sink, &w)
		if e != nil {
			first = e
			break
		}
	}
	if first == nil || len(sink.pages) != 0 || next != 801 {
		t.Fatal("duplicate original sink did not stop assigned page exactly once")
	}
	for i := 0; i < 2; i++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		if _, e := work.step(root, 10, &next, sink, &w); e != first || next != 801 || w.Records != 0 || len(sink.pages) != 0 {
			t.Fatal("failed page write replayed or lost sticky error")
		}
	}
}
