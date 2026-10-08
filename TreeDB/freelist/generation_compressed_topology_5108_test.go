package freelist

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func independentPatriciaBranches5108(keys []uint64) int {
	if len(keys) < 2 {
		return 0
	}
	depth := 0
	for ; depth < 14; depth++ {
		shift := uint((13 - depth) * 4)
		if (keys[0]>>shift)&15 != (keys[len(keys)-1]>>shift)&15 {
			break
		}
	}
	count := 1
	for first := 0; first < len(keys); {
		last := first + 1
		shift := uint((13 - depth) * 4)
		for last < len(keys) && (keys[last]>>shift)&15 == (keys[first]>>shift)&15 {
			last++
		}
		count += independentPatriciaBranches5108(keys[first:last])
		first = last
	}
	return count
}

func TestCompressedAllocatorThousandActualMutations5108(t *testing.T) {
	rng := rand.New(rand.NewSource(5108))
	root := stateRefV1{}
	defer func() { releaseStateNodeV1(root) }()
	model := map[uint64]bool{}
	for step := 0; step < 1000; step++ {
		key := uint64(rng.Intn(32))
		if rng.Intn(2) != 0 {
			key |= 1 << 55
		}
		exists := model[key]
		plan := mutationStateBirthPlanV1(root, key, 0, true, false, false)
		var births FreelistTxnStats
		next := mutateChunkForPreparation(nil, root, key, 0, func(c *stateChunk) { c.setFree(2, !exists) }, true, &births)
		if births.StateNodeCopies != plan.nodes || births.StateChunkCopies != plan.chunks {
			t.Fatalf("step%d admitted=%+v actual=%+v", step, plan, births)
		}

		replaceStateNodeV1(&root, next)
		if exists {
			delete(model, key)
		} else {
			model[key] = true
		}
		if err := validateCanonicalStateV2(root, true); err != nil {
			t.Fatalf("step%d shape: %v", step, err)
		}
		var keys []uint64
		if err := walkState(root, 0, func(c *stateChunk) error {
			if !model[c.chunkNo] || c.freeCount() != 1 || !c.isFree(2) || c.retiredPages != 0 {
				t.Fatalf("step%d unexpected chunk%+v", step, c)
			}
			keys = append(keys, c.chunkNo)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if len(keys) != len(model) {
			t.Fatalf("step%d chunk count%d want%d", step, len(keys), len(model))
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		branches := 0
		var visit func(stateRefV1)
		visit = func(r stateRefV1) {
			if r.branch != nil {
				branches++
				for _, child := range r.branch.child {
					visit(child)
				}
			}
		}
		visit(root)
		if want := independentPatriciaBranches5108(keys); branches != want {
			t.Fatalf("step%d actual branches%d want%d", step, branches, want)
		}
		if len(model) == 1 && root.chunk == nil {
			t.Fatal("single chunk acquired wrapper")
		}
		if len(model) == 0 && !root.zero() {
			t.Fatal("empty mutation retained history")
		}
		if step%50 == 0 {
			held := retainPrivateGraph5105(t, root)
			extra := uint64(1<<54) | uint64(step)
			probe := mutateChunkForPreparation(nil, root, extra, 0, func(c *stateChunk) { c.setFree(3, true) }, true, nil)
			held.assertUnchanged(t)
			releaseStateNodeV1(probe)
		}
	}
}

func TestCompressedAllocatorCanonicalLoaderCorruption5108(t *testing.T) {
	cases := []struct {
		name   string
		change func([]byte, *MemoryPageStoreV1, GenerationRefV1)
	}{
		{"prefix", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { binary.LittleEndian.PutUint64(b[64:72], 1) }},
		{"depth", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { b[28] = 14 }},
		{"first-difference", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { b[28] = 12 }},
		{"unary", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) {
			binary.LittleEndian.PutUint16(b[14:16], 1)
			clear(b[112:])
		}},
		{"reserved-header", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { b[72] = 1 }},
		{"reserved-entry", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { b[86] = 1 }},
		{"reserved-tail", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { b[592] = 1 }},
		{"kind", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { b[81] = 2 }},
		{"flags", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { b[13] = 1 }},
		{"summary", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { binary.LittleEndian.PutUint32(b[96:100], 2) }},
		{"root-generation", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { binary.LittleEndian.PutUint64(b[32:40], 1) }},
		{"duplicate-child", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) {
			copy(b[120:128], b[88:96])
			copy(b[114:118], b[82:86])
		}},
		{"header-alias", func(b []byte, _ *MemoryPageStoreV1, ref GenerationRefV1) {
			binary.LittleEndian.PutUint64(b[88:96], ref.HeaderPageID)
		}},
		{"reservation-alias", func(b []byte, store *MemoryPageStoreV1, ref GenerationRefV1) {
			header := store.Pages[ref.HeaderPageID]
			copy(b[88:96], header[96:104])
		}},
		{"cycle", func(b []byte, _ *MemoryPageStoreV1, _ GenerationRefV1) { copy(b[88:96], b[:8]); b[81] = 0 }},
		{"child-depth-before-recursion", func(b []byte, store *MemoryPageStoreV1, _ GenerationRefV1) {
			id := binary.LittleEndian.Uint64(b[88:96])
			child := append([]byte(nil), b...)
			binary.LittleEndian.PutUint64(child[:8], id)
			copy(child[88:96], b[120:128])
			copy(child[82:86], b[114:118])
			page.UpdateChecksum(child)
			store.Pages[id] = child
			b[81] = 0
			copy(b[82:86], child[8:12])
		}},
		{"child-newer", func(b []byte, store *MemoryPageStoreV1, _ GenerationRefV1) {
			id := binary.LittleEndian.Uint64(b[88:96])
			child := store.Pages[id]
			binary.LittleEndian.PutUint64(child[32:40], 3)
			page.UpdateChecksum(child)
			copy(b[82:86], child[8:12])
		}},
		{"empty-child", func(b []byte, store *MemoryPageStoreV1, _ GenerationRefV1) {
			id := binary.LittleEndian.Uint64(b[88:96])
			child, err := encodeIndexPage(id, 2, &stateNode{}, 0)
			if err != nil {
				panic(err)
			}
			store.Pages[id] = child
			b[81] = 0
			copy(b[82:86], child[8:12])
			clear(b[96:112])
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryPageStoreV1()
			candidate, err := NewFreelistTxn(MustNewFreelistGenerationV1(1, 257, []uint64{2, 256}, nil), NewReservationLedger()).MaterializeCandidate(2, 2, candidateIDFromString(tc.name), store)
			if err != nil {
				t.Fatal(err)
			}
			ref := candidate.GenerationRef()
			header := store.Pages[ref.HeaderPageID]
			rootID := binary.LittleEndian.Uint64(header[64:72])
			root := store.Pages[rootID]
			tc.change(root, store, ref)
			page.UpdateChecksum(root)
			copy(header[112:116], root[8:12])
			digest := generationDigest(header)
			copy(header[152:184], digest[:])
			ref.Digest = digest
			page.UpdateChecksum(header)
			if _, err = LoadGenerationV1(store, ref); !errors.Is(err, ErrGenerationFormat) {
				t.Fatalf("canonical corruption accepted/wrong reason: %v", err)
			}
			if tc.name == "child-depth-before-recursion" && store.Reads != 4 {
				t.Fatalf("malformed branch recursed beyond depth fence: reads%d want4", store.Reads)
			}
		})
	}
}

func TestCompressedAllocatorDirectRootDirtyPageCounts5108(t *testing.T) {
	for _, free := range [][]uint64{{2}, {2, 256}, {2, (1 << 63) + 2}, {2, 256, (1 << 63) + 2}} {
		high := free[len(free)-1] + 1
		store := NewMemoryPageStoreV1()
		base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, high, free, nil), 2, store)
		// No mutation still writes exactly one new exact-generation root.
		txn := NewFreelistTxn(base, NewReservationLedger())
		candidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("unchanged-root"), store)
		if err != nil {
			t.Fatal(err)
		}
		state := 0
		for _, image := range candidate.Pages() {
			typ := page.PageType(page.DecodeHeader(image.Data).Flags)
			if typ == page.PageTypeFreelistChunk || typ == page.PageTypeFreelistIndex {
				state++
			}
		}
		if state != 1 {
			t.Fatalf("unchanged direct/branch root emitted %d state pages want1", state)
		}
		if _, err = LoadGenerationV1(store, candidate.GenerationRef()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompressedAllocatorEmptySentinelReplacementOwnership5108(t *testing.T) {
	store := NewMemoryPageStoreV1()
	base := materializeTestGeneration(t, MustNewFreelistGenerationV1(1, 32, nil, nil), 2, store)
	oldRoot := base.root.pageID()
	txn := NewFreelistTxn(base, NewReservationLedger())
	txn.Retire(2, 1)
	if err := txn.valid(); err != nil {
		t.Fatal(err)
	}
	candidate, err := txn.MaterializeCandidate(3, 3, candidateIDFromString("empty-root-replacement"), store)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range candidate.generation.record.pendingMetadata() {
		if id.id == oldRoot {
			found = true
		}
	}
	if !found {
		t.Fatal("discarded durable empty sentinel lost retirement ownership")
	}
	if _, err = LoadGenerationV1(store, candidate.GenerationRef()); err != nil {
		t.Fatal(err)
	}
}

func TestCompressedAllocatorExactMetadataOwnership5108(t *testing.T) {
	for _, name := range []string{"current-child-outside", "older-child-inside", "orphan-target", "reservation-outside"} {
		t.Run(name, func(t *testing.T) {
			store := NewMemoryPageStoreV1()
			candidate, err := NewFreelistTxn(MustNewFreelistGenerationV1(1, 257, []uint64{2, 256}, nil), NewReservationLedger()).MaterializeCandidate(2, 2, candidateIDFromString(name), store)
			if err != nil {
				t.Fatal(err)
			}
			ref := candidate.GenerationRef()
			header := store.Pages[ref.HeaderPageID]
			rootID := binary.LittleEndian.Uint64(header[64:72])
			root := store.Pages[rootID]
			record := candidate.ReservationRecord()
			switch name {
			case "current-child-outside":
				oldID := binary.LittleEndian.Uint64(root[88:96])
				child := append([]byte(nil), store.Pages[oldID]...)
				binary.LittleEndian.PutUint64(child[:8], 2)
				page.UpdateChecksum(child)
				store.Pages[2] = child
				binary.LittleEndian.PutUint64(root[88:96], 2)
				copy(root[82:86], child[8:12])
			case "older-child-inside":
				child := store.Pages[binary.LittleEndian.Uint64(root[88:96])]
				binary.LittleEndian.PutUint64(child[32:40], 1)
				page.UpdateChecksum(child)
				copy(root[82:86], child[8:12])
			case "orphan-target":
				for i := range record.Extents {
					if record.Extents[i].Kind == ReservationTargetMetadata {
						record.Extents[i].StartPageID--
						record.Extents[i].Count++
					}
				}
				binary.LittleEndian.PutUint32(header[104:108], binary.LittleEndian.Uint32(header[104:108])+1)
			case "reservation-outside":
				// A valid record digest and page CRC cannot lend a page outside its
				// own target reservation to the current generation.
				binary.LittleEndian.PutUint64(header[96:104], 2)
				record.pageID = 2
			}
			page.UpdateChecksum(root)
			copy(header[112:116], root[8:12])
			recordID := binary.LittleEndian.Uint64(header[96:104])
			images, encoded, err := encodeReservationPages(recordID, record)
			if err != nil {
				t.Fatal(err)
			}
			for i, image := range images {
				store.Pages[recordID+uint64(i)] = image
			}
			copy(header[120:152], encoded.digest[:])
			digest := generationDigest(header)
			copy(header[152:184], digest[:])
			ref.Digest = digest
			page.UpdateChecksum(header)
			if _, err = LoadGenerationV1(store, ref); !errors.Is(err, ErrGenerationFormat) {
				t.Fatalf("accepted ownership corruption: %v", err)
			}
		})
	}
}

func TestCompressedAllocatorFanoutActualEmission5108(t *testing.T) {
	for _, chunks := range []int{3, 16} {
		free := make([]uint64, chunks)
		for i := range free {
			free[i] = uint64(i)*256 + 2
		}
		store := NewMemoryPageStoreV1()
		candidate, err := NewFreelistTxn(MustNewFreelistGenerationV1(1, free[len(free)-1]+1, free, nil), NewReservationLedger()).MaterializeCandidate(2, 2, candidateIDFromString("fanout"), store)
		if err != nil {
			t.Fatal(err)
		}
		state, branches := 0, 0
		for _, image := range candidate.Pages() {
			switch page.PageType(page.DecodeHeader(image.Data).Flags) {
			case page.PageTypeFreelistChunk:
				state++
			case page.PageTypeFreelistIndex:
				state++
				branches++
			}
		}
		if state != chunks+1 || branches != 1 {
			t.Fatalf("fanout%d emitted state%d branches%d", chunks, state, branches)
		}
		if _, err = LoadGenerationV1(store, candidate.GenerationRef()); err != nil {
			t.Fatal(err)
		}
	}
}
