package freelist

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"testing"
)

func TestCanonicalStateMutationRetainsActualCallbackAndSummaryV1(t *testing.T) {
	base := MustNewFreelistGenerationV1(9, 4096, []uint64{2, 258, 514}, nil)
	initial := base.root
	var work stateMutationWorkV1
	offsets := []uint64{4, 5, 250}
	next, effects := 0, 0
	work.begin(initial, 0, false, nil, nil, func(c *stateChunk, w *iterator.OrdinalScanWork) (bool, error) {
		for next < len(offsets) {
			if w != nil && !w.Reserve(1, 16) {
				return false, nil
			}
			c.setFree(offsets[next], false)
			c.retired[offsets[next]] = 11
			next++
			effects++
		}
		return true, nil
	})
	calls := 0
	for {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := work.step(&w)
		if err != nil {
			t.Fatal(err)
		}
		calls++
		if w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("actual work exceeded allowance")
		}
		if base.root != initial || lookupChunk(base.root, 0).retired[4] != 0 {
			t.Fatal("immutable base changed during retained mutation")
		}
		if ready {
			break
		}
		if calls > 128 {
			t.Fatal("retained original stages made no progress")
		}
	}
	if calls < 9 || effects != len(offsets) {
		t.Fatalf("calls=%d effects=%d", calls, effects)
	}
	if err := validateCanonicalStateV2(work.result, true); err != nil {
		t.Fatal(err)
	}
	if work.result.freeCount != 3 || work.result.retiredCount != 3 {
		t.Fatalf("summary free=%d retired=%d", work.result.freeCount, work.result.retiredCount)
	}
	if lookupChunk(work.result, 1) != lookupChunk(base.root, 1) {
		t.Fatal("unrelated durable model child was copied")
	}
	t.Logf("actual mutation callbacks=%d resumes=%d retained-summary-slots=%d", effects, calls, freelistChunkSize)
}

func TestCanonicalPrivateMutationDetachesSyntheticDirtyChildrenV1(t *testing.T) {
	base := MustNewFreelistGenerationV1(9, 4096, []uint64{2, 258, 514}, nil)
	// A materialized synthetic parent can still contain PageID-zero children.
	// Acquiring its private grant must detach those original children first.
	base.root.pageID = 99
	originalChunk := lookupChunk(base.root, 0)
	var work stateMutationWorkV1
	applied := false
	work.begin(base.root, 0, true, nil, nil, func(c *stateChunk, w *iterator.OrdinalScanWork) (bool, error) {
		if applied {
			return true, nil
		}
		if w != nil && !w.Reserve(1, 16) {
			return false, nil
		}
		c.retired[4] = 11
		applied = true
		return true, nil
	})
	for calls := 0; ; calls++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := work.step(&w)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
		if calls > 128 {
			t.Fatal("dirty detachment failed to progress")
		}
	}
	if originalChunk.retired[4] != 0 || lookupChunk(work.result, 0) == originalChunk {
		t.Fatal("private grant mutated an original dirty alias")
	}
	if err := validateCanonicalStateV2(work.result, true); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateBeginStreamsPendingExtentsWithOriginalAuthorityV1(t *testing.T) {
	base := MustNewFreelistGenerationV1(9, 4096, []uint64{2, 258, 514}, nil)
	base.record.Extents = []ReservationExtentV1{
		{StartPageID: 4, Count: 2, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 7},
		{StartPageID: 9, Count: 1, Kind: ReservationTargetMetadata},
		{StartPageID: 10, Count: 2, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 8},
		{StartPageID: 260, Count: 2, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 7},
	}
	ledger := NewReservationLedger()
	var work candidateBeginWorkV1
	calls := 0
	var txn *FreelistTxn
	for {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		var ready bool
		var err error
		txn, ready, err = work.step(base, base.ref, ledger, &w)
		if err != nil {
			t.Fatal(err)
		}
		calls++
		if !ready && txn != nil {
			t.Fatal("unfinished original transaction escaped")
		}
		if ready {
			break
		}
		if calls > 256 {
			t.Fatal("normalized extent retirement made no progress")
		}
	}
	if txn.ledger != ledger || txn.base != base || !txn.privatePreparation {
		t.Fatal("successor replaced original authority")
	}
	if txn.root.retiredCount != 6 || txn.root.freeCount != 3 {
		t.Fatalf("free=%d retired=%d", txn.root.freeCount, txn.root.retiredCount)
	}
	if txn.stats.StateMutationPaths != 2 || txn.stats.StateMutationItems != 6 {
		t.Fatalf("chunk runs were not coalesced: %+v", txn.stats)
	}
	if lookupChunk(base.root, 0).retired[4] != 0 {
		t.Fatal("base changed")
	}
	if lookupChunk(txn.root, 0).retired[10] != 8 {
		t.Fatal("retirement sequence lost")
	}
	if err := validateCanonicalStateV2(txn.root, true); err != nil {
		t.Fatal(err)
	}
	t.Logf("original successor resumes=%d chunk-runs=%d retired-items=%d", calls, txn.stats.StateMutationPaths, txn.stats.StateMutationItems)
}

func TestCanonicalMutationRejectsOpaqueAndPreservesFirstErrorV1(t *testing.T) {
	var work stateMutationWorkV1
	called := 0
	work.begin(nil, 0, false, nil, func(*stateChunk) { called++ }, nil)
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if _, err := work.step(&w); !errors.Is(err, ErrGenerationFormat) || called != 0 {
		t.Fatal("opaque callback ran under work")
	}
	boom := errors.New("original retirement failed")
	work.begin(nil, 0, false, nil, nil, func(*stateChunk, *iterator.OrdinalScanWork) (bool, error) { called++; return false, boom })
	for i := 0; i < 2; i++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		if _, err := work.step(&w); !errors.Is(err, boom) {
			t.Fatal(err)
		}
	}
	if called != 1 {
		t.Fatal("original failed callback replayed")
	}
}

// This is an actual BeginCandidate boundary, including each caller's page/path
// observation. Each retry receives a fresh quantum; a fitting insertion must
// eventually run, while a genuinely oversized map-growth unit must refuse.
func TestCandidateBeginMapAdmissionQ32BoundaryV1(t *testing.T) {
	for _, tc := range []struct {
		name      string
		pages     int
		pending   bool
		byteLimit uint64
		oversized bool
	}{
		{"page-list-record-and-byte-boundary", 31, false, 4352, false},
		{"pending-path-record-boundary", 29, true, 1 << 20, false},
		{"oversized-page-list", 32, false, 1 << 20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := MustNewFreelistGenerationV1(9, 4096, []uint64{2, 258}, nil)
			base.ref.HeaderPageID = 900
			base.root.pageID = 700
			lookupChunk(base.root, 1).pageID = 701
			for i := 0; i < tc.pages; i++ {
				base.record.pageIDs = append(base.record.pageIDs, uint64(100+i))
			}
			if tc.pending {
				base.record.Extents = []ReservationExtentV1{{StartPageID: 260, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 7}}
			}
			ledger := NewReservationLedger()
			var work candidateBeginWorkV1
			var txn *FreelistTxn
			calls := 0
			for ; calls < 256; calls++ {
				w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: tc.byteLimit}
				var ready bool
				var err error
				txn, ready, err = work.step(base, base.ref, ledger, &w)
				if w.Records > 32 || w.Bytes > tc.byteLimit {
					t.Fatal("work exceeded fresh allowance")
				}
				if err != nil {
					var tooLarge *iterator.OrdinalUnitTooLarge
					if !tc.oversized || !errors.As(err, &tooLarge) {
						t.Fatal(err)
					}
					if txn != nil || len(work.txn.replacedMetadata) != 32 {
						t.Fatal("oversized insertion changed original map")
					}
					if _, exists := work.txn.replacedMetadata[131]; exists {
						t.Fatal("oversized key inserted")
					}
					t.Logf("explicit oversized-unit refusal after %d calls", calls+1)
					return
				}
				if !ready && txn != nil {
					t.Fatal("unfinished original successor escaped")
				}
				if ready {
					break
				}
			}
			if calls == 256 {
				t.Fatal("fitting map insertion starved across fresh Q32 allowances")
			}
			if tc.oversized {
				t.Fatal("oversized insertion accepted")
			}
			if txn.base != base || txn.ledger != ledger || len(txn.replacedMetadata) != tc.pages+1+2*boolIntV1(tc.pending) {
				t.Fatalf("original successor custody/metadata lost: metadata=%d", len(txn.replacedMetadata))
			}
			if tc.pending && (lookupChunk(txn.root, 1).retired[4] != 7 || lookupChunk(base.root, 1).retired[4] != 0) {
				t.Fatal("retained path did not preserve private retirement/base")
			}
			t.Logf("fitting original successor completed in %d calls; replaced=%d", calls+1, len(txn.replacedMetadata))
		})
	}
}
func boolIntV1(v bool) int {
	if v {
		return 1
	}
	return 0
}
