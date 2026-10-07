package freelist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// This oracle is the unchanged b57 index encoder. It intentionally stays
// independent of the caller-destination encoder to detect format drift.
func frozenB57IndexEncodingForTest(id, generationID uint64, n *stateNode, depth int) ([]byte, error) {
	b := make([]byte, page.PageSize)
	count := 0
	if depth == chunkTrieDepth {
		if n.chunk == nil || n.chunk.pageID == 0 {
			return nil, ErrGenerationFormat
		}
		count = 1
	} else {
		for _, child := range n.child {
			if child != nil && (child.freeCount != 0 || child.retiredCount != 0) {
				if child.pageID == 0 {
					return nil, ErrGenerationFormat
				}
				count++
			}
		}
	}
	encodePageHeader(b, id, page.PageTypeFreelistIndex, uint16(count))
	copy(b[16:24], indexMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 1)
	binary.LittleEndian.PutUint16(b[26:28], indexHeaderSize)
	b[28] = byte(depth)
	binary.LittleEndian.PutUint16(b[30:32], indexEntrySize)
	binary.LittleEndian.PutUint64(b[32:40], generationID)
	binary.LittleEndian.PutUint64(b[40:48], n.freeCount)
	binary.LittleEndian.PutUint64(b[48:56], n.retiredCount)
	binary.LittleEndian.PutUint64(b[56:64], n.minRetiredSeq)
	o := indexHeaderSize
	write := func(slot byte, kind byte, childChecksum uint32, childID, freeCount, retiredCount, minSeq uint64) error {
		if freeCount > uint64(^uint32(0)) || retiredCount > uint64(^uint32(0)) {
			return ErrGenerationFormat
		}
		b[o], b[o+1] = slot, kind
		binary.LittleEndian.PutUint32(b[o+2:o+6], childChecksum)
		binary.LittleEndian.PutUint64(b[o+8:o+16], childID)
		binary.LittleEndian.PutUint32(b[o+16:o+20], uint32(freeCount))
		binary.LittleEndian.PutUint32(b[o+20:o+24], uint32(retiredCount))
		binary.LittleEndian.PutUint64(b[o+24:o+32], minSeq)
		o += indexEntrySize
		return nil
	}
	if depth == chunkTrieDepth {
		retiredCount, minSeq := n.chunk.retiredSummary()
		if err := write(0, 1, n.chunk.checksum, n.chunk.pageID, n.chunk.freeCount(), retiredCount, minSeq); err != nil {
			return nil, err
		}
	} else {
		for slot, child := range n.child {
			if child != nil && (child.freeCount != 0 || child.retiredCount != 0) {
				if err := write(byte(slot), 0, child.checksum, child.pageID, child.freeCount, child.retiredCount, child.minRetiredSeq); err != nil {
					return nil, err
				}
			}
		}
	}
	finishPage(b)
	return b, nil
}

func TestCandidateIndexPlansCanonicalBytesAndCapacity(t *testing.T) {
	cases := []struct {
		name      string
		highWater uint64
		free      []uint64
		retired   map[uint64]uint64
	}{
		{"empty", 64, nil, nil},
		{"one-chunk", 256, []uint64{2, 9, 255}, map[uint64]uint64{11: 7}},
		{"sparse-multichunk", 1 << 42, []uint64{2, 257, 1 << 20, (1 << 40) + 9}, map[uint64]uint64{513: 3, (1 << 35) + 7: 11}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := MustNewFreelistGenerationV1(1, tc.highWater, tc.free, tc.retired)
			id := candidateIDFromString(tc.name)
			candidate, err := NewFreelistTxn(base, NewReservationLedger()).MaterializeCandidate(2, 12, id, NewCandidatePageSinkV1())
			if err != nil {
				t.Fatal(err)
			}
			// The generic sink remains the owning-image path for every metadata type.
			store := NewMemoryPageStoreV1()
			generic, err := NewFreelistTxn(base, NewReservationLedger()).MaterializeCandidate(2, 12, id, store)
			if err != nil {
				t.Fatal(err)
			}
			if candidate.GenerationRef() != generic.GenerationRef() || !slices.Equal(candidate.DirtyPageIDs(), generic.DirtyPageIDs()) || !pageImagesEqual(candidate.Pages(), generic.Pages()) {
				t.Fatal("plan path changed generation, page order or bytes")
			}
			mutating := &mutatingRetainingPageSinkV1{}
			isolated, err := NewFreelistTxn(base, NewReservationLedger()).MaterializeCandidate(2, 12, id, mutating)
			if err != nil {
				t.Fatal(err)
			}
			if !pageImagesEqual(candidate.Pages(), isolated.Pages()) {
				t.Fatal("generic input mutation reached candidate metadata")
			}
			for _, image := range mutating.pages {
				clear(image.Data)
			}
			if !pageImagesEqual(candidate.Pages(), isolated.Pages()) {
				t.Fatal("retained generic sink alias reached candidate metadata")
			}
			if cap(candidate.pages) != len(candidate.pages) {
				t.Fatal("candidate table has uncharged append capacity")
			}
			if unsafe.Sizeof(indexPagePlanV1{}) != 576 {
				t.Fatal("index plan exceeds detached fixed descriptor bound")
			}
			// The typed definition owns only byte arrays: neither the view nor its
			// plan may retain an allocator state, generation, chunk, slice or interface.
			planType := reflect.TypeOf(indexPagePlanV1{})
			for f := 0; f < planType.NumField(); f++ {
				field := planType.Field(f).Type
				if field.Kind() != reflect.Array || field.Elem().Kind() != reflect.Uint8 {
					t.Fatal("index plan retains indirect ownership")
				}
			}
			nodes := make(map[uint64]*stateNode)
			var visit func(*stateNode)
			visit = func(n *stateNode) {
				if n == nil {
					return
				}
				nodes[n.pageID] = n
				for _, child := range n.child {
					visit(child)
				}
			}
			visit(candidate.Generation().root)
			plans, imageBytes := 0, 0
			seen := make(map[page.PageType]bool)
			for _, entry := range candidate.pages {
				dst := bytes.Repeat([]byte{0xa5}, page.PageSize)
				if err := entry.view.CopyTo(dst); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(dst, store.Pages[entry.PageID]) || !page.VerifyChecksumNonMutating(dst) {
					t.Fatal("noncanonical full-page copy")
				}
				typ := page.PageType(page.DecodeHeader(dst).Flags & 0xff)
				seen[typ] = true
				if entry.view.index != nil {
					plans++
					if entry.view.data != nil || typ != page.PageTypeFreelistIndex {
						t.Fatal("plan retains an image")
					}
					plan := entry.view.index
					want, err := frozenB57IndexEncodingForTest(entry.PageID, binary.LittleEndian.Uint64(plan.prefix[32:40]), nodes[entry.PageID], int(plan.prefix[28]))
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(dst, want) {
						t.Fatal("index encoding differs from frozen b57")
					}
					if binary.LittleEndian.Uint32(want[8:12]) != page.CalculateChecksumWithZeroGap(plan.prefix[:], page.PageSize-len(plan.prefix), nil) {
						t.Fatal("logical zero-tail checksum differs from frozen full-page CRC")
					}
					for _, offset := range []int{0, 8, 16, 29, 40, 64, 70, 575} {
						corrupt := *plan
						corrupt.prefix[offset] ^= 1
						guard := bytes.Repeat([]byte{0xc7}, page.PageSize)
						unchanged := bytes.Clone(guard)
						if err := (CandidatePageViewV1{index: &corrupt}).CopyTo(guard); !errors.Is(err, ErrGenerationChecksum) {
							t.Fatalf("prefix corruption offset %d: %v", offset, err)
						}
						if !bytes.Equal(guard, unchanged) {
							t.Fatal("checksum failure changed destination")
						}
					}
					// An arbitrary nonzero tail is never certified by a prefix checksum:
					// it changes the full CRC. CopyTo must clear every such destination byte.
					for _, offset := range []int{576, page.PageSize - 1} {
						tailMutation := bytes.Clone(want)
						tailMutation[offset] ^= 1
						if page.VerifyChecksumNonMutating(tailMutation) {
							t.Fatal("full-page CRC accepted a changed logical zero tail")
						}
					}
					if !zeroTail(dst, len(plan.prefix)) {
						t.Fatal("copy retained destination tail bytes")
					}

					for _, size := range []int{0, page.PageSize - 1, page.PageSize + 1} {
						invalid := bytes.Repeat([]byte{0xab}, size)
						unchanged := bytes.Clone(invalid)
						if err := entry.view.CopyTo(invalid); !errors.Is(err, ErrGenerationFormat) {
							t.Fatalf("destination size %d: %v", size, err)
						}
						if !bytes.Equal(invalid, unchanged) {
							t.Fatal("invalid destination modified")
						}
					}
				} else {
					if typ == page.PageTypeFreelistIndex {
						t.Fatal("production index retained an image")
					}
					imageBytes += cap(entry.view.data)
				}
				clear(dst)
				if err := entry.view.CopyTo(dst); err != nil || !bytes.Equal(dst, store.Pages[entry.PageID]) {
					t.Fatal("destination mutation reached retained view")
				}
			}
			if plans == 0 || !seen[page.PageTypeFreelistGeneration] || !seen[page.PageTypeFreelistReservation] {
				t.Fatal("missing index/header/reservation coverage")
			}
			if tc.name != "empty" && !seen[page.PageTypeFreelistChunk] {
				t.Fatal("missing chunk coverage")
			}
			if imageBytes != (candidate.PageCount()-plans)*page.PageSize {
				t.Fatal("non-index images retain excess capacity")
			}
			// Architecture-independent accounting; the amd64 handoff gives numerical
			// sizes, while the invariant also guards future field/capacity expansion.
			tableBytes := cap(candidate.pages) * int(unsafe.Sizeof(candidatePageV1{}))
			if tableBytes >= candidate.PageCount()*page.PageSize {
				t.Fatal("plan table exceeds original image credit")
			}
			planBytes := plans * int(unsafe.Sizeof(indexPagePlanV1{}))
			t.Logf("pages=%d plans=%d entry=%d table_capacity_bytes=%d owned_plan_bytes=%d nonindex_capacity_bytes=%d prepare_scratch_bytes=%d", candidate.PageCount(), plans, unsafe.Sizeof(candidatePageV1{}), tableBytes, planBytes, imageBytes, page.PageSize)
		})
	}
}

func assertRetainedPlanViews(t *testing.T, writer *candidatePageRecordingWriterV1) {
	t.Helper()
	plans := 0
	for i, view := range writer.views {
		if view.index != nil {
			plans++
		}
		dst := bytes.Repeat([]byte{0xfe}, page.PageSize)
		if err := view.CopyTo(dst); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(dst, writer.pages[i].Data) {
			t.Fatalf("retained view %d changed", i)
		}
		clear(dst)
	}
	if plans == 0 {
		t.Fatal("lifetime fixture retained no index plans")
	}
}

func TestCandidateIndexPlansSurviveForkAndCandidateRelease(t *testing.T) {
	candidate, err := NewFreelistTxn(MustNewFreelistGenerationV1(1, 1024, []uint64{2, 9, 257}, nil), NewReservationLedger()).MaterializeCandidate(2, 2, candidateIDFromString("plan-parent"), NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	writer := &candidatePageRecordingWriterV1{}
	if err := candidate.WritePagesToV1(writer); err != nil {
		t.Fatal(err)
	}
	fork := NewFreelistTxn(candidate.Generation(), NewReservationLedger())
	if _, err := fork.Allocate(0); err != nil {
		t.Fatal(err)
	}
	fork.Retire(511, 2)
	if _, err := fork.MaterializeCandidate(3, 3, candidateIDFromString("plan-fork"), NewCandidatePageSinkV1()); err != nil {
		t.Fatal(err)
	}
	candidate = nil // Only views, not the candidate inventory, retain the old state.
	fork = nil
	assertRetainedPlanViews(t, writer)
}

func TestCandidateIndexPlansSurviveAllocatorAbortAndRetry(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(64); err != nil {
		t.Fatal(err)
	}
	allocator := New(p, 0)
	if err := allocator.EnableCOWV1(MustNewFreelistGenerationV1(1, 64, []uint64{2, 9, 33}, nil), NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	capability, err := NewReuseCapability(1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := allocator.PrepareCOWCandidateV1(2, 2, candidateIDFromString("plan-abort"), capability, 0, NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	writer := &candidatePageRecordingWriterV1{}
	if err := prepared.Candidate().WritePagesToV1(writer); err != nil {
		t.Fatal(err)
	}
	if err := allocator.AbortCOWCandidateV1(prepared); err != nil {
		t.Fatal(err)
	}
	prepared = nil
	if _, err := allocator.Alloc(0); err != nil {
		t.Fatal(err)
	}
	retry, err := allocator.PrepareCOWCandidateV1(2, 2, candidateIDFromString("plan-retry"), capability, 0, NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.AbortCOWCandidateV1(retry); err != nil {
		t.Fatal(err)
	}
	retry = nil
	assertRetainedPlanViews(t, writer)
}

func TestCandidateIndexPlanChecksumFailureDoesNotWritePager(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	n := &stateNode{pageID: id}
	expected, err := frozenB57IndexEncodingForTest(id, 2, n, 0)
	if err != nil {
		t.Fatal(err)
	}
	plan := &indexPagePlanV1{}
	copy(plan.prefix[:], expected[:len(plan.prefix)])
	view := CandidatePageViewV1{index: plan}
	if err := WriteCandidatePageToPagerV1(p, id, view); err != nil {
		t.Fatal(err)
	}
	before, err := p.ReadPage(id)
	if err != nil {
		t.Fatal(err)
	}
	before = bytes.Clone(before)
	// Deliberately break the private invariant to exercise fail-closed behavior.
	plan.prefix[40] ^= 1
	if err := WriteCandidatePageToPagerV1(p, id, view); !errors.Is(err, ErrGenerationChecksum) {
		t.Fatalf("checksum failure: %v", err)
	}
	after, err := p.ReadPage(id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("failed plan wrote pager bytes")
	}
	plan.prefix[40] ^= 1
	if err := WriteCandidatePageToPagerV1(p, id+1, view); !errors.Is(err, ErrGenerationFormat) {
		t.Fatalf("identity failure: %v", err)
	}
}
