package freelist

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// Capability absence: the former public allocator constructor only exposed a
// synchronous MaterializeCandidate. This checks the new retained entry against
// that maintained driver and the independent on-disk decoder, not a fabricated
// compile-failure RED.
func TestCanonicalMaterializationWholeConstructorMatchesOrdinaryV1(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "tail", true: "reuse"}[reuse], func(t *testing.T) {
			free := []uint64{2, 258, 514}
			if reuse {
				free = make([]uint64, 32)
				for i := range free {
					free[i] = uint64(i) + 2
				}
			}
			base := MustNewFreelistGenerationV1(9, 1000, free, nil)
			normal := NewFreelistTxn(base, NewReservationLedger())
			retained := NewFreelistTxn(base, NewReservationLedger())
			id := candidateIDFromString("whole-materialization")
			expected, e := normal.MaterializeCandidate(10, 11, id, NewCandidatePageSinkV1())
			if e != nil {
				t.Fatal(e)
			}
			sink := NewCandidatePageSinkV1()
			var work CandidateMaterializationWorkV1
			var actual *FreelistCandidateV1
			calls := 0
			for ; calls < 512; calls++ {
				w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
				var ready bool
				actual, ready, e = work.Step(retained, 10, 11, id, sink, &w)
				if e != nil {
					t.Fatalf("phase=%d reusephase=%d: %v", work.phase, work.reuse.phase, e)
				}
				if w.Records > 32 || w.Bytes > 1<<20 {
					t.Fatal("constructor exceeded actual allowance")
				}
				if !ready {
					if _, e = retained.Allocate(0); !errors.Is(e, ErrCandidateConsumed) {
						t.Fatal("yielded private constructor accepted mutation", e)
					}
					// A separate ordinary transaction may allocate while preparation yields.
					// It cannot mutate the consumed input, sink or candidate identity.
					other := NewFreelistTxn(base, NewReservationLedger())
					if _, e = other.Allocate(0); e != nil {
						t.Fatal(e)
					}
				}
				if ready {
					break
				}
			}
			if calls == 512 || actual == nil {
				t.Fatalf("constructor stalled phase=%d", work.phase)
			}
			if len(actual.pages) != len(expected.pages) || actual.GenerationRef() != expected.GenerationRef() {
				t.Fatal("retained constructor changed canonical identity")
			}
			store := NewMemoryPageStoreV1()
			for i, p := range actual.pages {
				if p.PageID != expected.pages[i].PageID || !bytes.Equal(p.Data, expected.pages[i].Data) || !page.VerifyChecksumNonMutating(p.Data) {
					t.Fatalf("canonical output page %d differs", i)
				}
				store.Pages[p.PageID] = p.Data
			}
			decoded, e := LoadGenerationV1(store, actual.GenerationRef())
			if e != nil || decoded.FreeCount() != actual.generation.FreeCount() {
				t.Fatal("whole constructor failed decoder", e)
			}
			if retained.ledger.candidates[id] == nil || !retained.ledger.candidates[id].tailWriteAttempted {
				t.Fatal("output lost actual ledger ownership")
			}
			t.Logf("whole constructor reuse=%v calls=%d pages=%d", reuse, calls+1, len(actual.pages))
		})
	}
}

func TestCanonicalMaterializationRefusalPreservesOriginalClaimV1(t *testing.T) {
	base := MustNewFreelistGenerationV1(9, 1000, []uint64{2}, nil)
	txn := NewFreelistTxn(base, NewReservationLedger())
	sink := NewCandidatePageSinkV1()
	id := candidateIDFromString("whole-materialization-error")
	var work CandidateMaterializationWorkV1
	// Inject the real first-page duplicate error in the production validation
	// sink. The original candidate/ledger must survive and retries cannot write.
	sink.pageIDs[1000] = struct{}{}
	var first error
	for i := 0; i < 256; i++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		_, _, first = work.Step(txn, 10, 11, id, sink, &w)
		if first != nil {
			break
		}
	}
	if first == nil || !errors.Is(first, ErrGenerationFormat) {
		t.Fatal("missing actual sink failure", first)
	}
	original := txn.ledger.candidates[id]
	if original == nil || !original.tailWriteAttempted || len(work.recorded.pages) != 0 {
		t.Fatal("failed constructor lost claim/debt")
	}
	for i := 0; i < 2; i++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		_, ready, e := work.Step(txn, 10, 11, id, sink, &w)
		if ready || e != first || w.Records != 0 || txn.ledger.candidates[id] != original {
			t.Fatal("failure replayed or replaced original receipt")
		}
	}
}
