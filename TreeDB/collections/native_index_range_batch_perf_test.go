package collections

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// batchRangeCaptureSpy wraps a real NativeSemanticCapture to observe and inject
// physical batch/value behaviors on the settled batched range path without
// changing production semantics.
type batchRangeCaptureSpy struct {
	backenddb.NativeSemanticCapture
	batchCalls   atomic.Int64
	pointCalls   atomic.Int64
	valueCalls   atomic.Int64
	onBatch      func(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error
	onValueAtPtr func(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error)
}

func (s *batchRangeCaptureSpy) NativeScanLiveKeysAtRoot(root uint64, start, end []byte, visit func(key []byte) (bool, error)) error {
	return s.NativeSemanticCapture.(nativePhysicalRangeScanner).NativeScanLiveKeysAtRoot(root, start, end, visit)
}

func (s *batchRangeCaptureSpy) NativeEntriesAtRoot(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error {
	s.batchCalls.Add(1)
	if s.onBatch != nil {
		return s.onBatch(root, keys, dst, found, copyValues)
	}
	return s.NativeSemanticCapture.(nativePhysicalBatchEntryReader).NativeEntriesAtRoot(root, keys, dst, found, copyValues)
}

func (s *batchRangeCaptureSpy) NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error) {
	s.pointCalls.Add(1)
	return s.NativeSemanticCapture.(backenddb.NativePhysicalReader).NativeEntryAtRoot(root, key, copyValue)
}

func (s *batchRangeCaptureSpy) NativeLeafPage(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return s.NativeSemanticCapture.(backenddb.NativePhysicalLeafReader).NativeLeafPage(ptr, dst)
}

func (s *batchRangeCaptureSpy) NativeValueAtPointer(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
	s.valueCalls.Add(1)
	if s.onValueAtPtr != nil {
		return s.onValueAtPtr(ptr, admitOutput)
	}
	return s.NativeSemanticCapture.(backenddb.NativePhysicalValueReader).NativeValueAtPointer(ptr, admitOutput)
}

func TestScanIndexRangeBatchedPrimaryParity(t *testing.T) {
	modes := []struct {
		name            string
		outerLeavesDict bool
	}{
		{name: "inline_leaves", outerLeavesDict: false},
		{name: "outer_leaves_dict", outerLeavesDict: true},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			if mode.outerLeavesDict && runtime.GOOS == "windows" {
				t.Skip("outer-leaf dictionary value-log test skipped on windows")
			}
			// PointerThreshold = 512 keeps small docs inline while docs > 4 KiB
			// become value-log pointers in the same range scan.
			owner, _, col := openNativeIndexRangeFixture(t, nativeIndexRangeFixtureOpts{
				outerLeavesDict: mode.outerLeavesDict,
				pointerThresh:   512,
			})
			smallPad := string(bytes.Repeat([]byte("s"), 64))
			largePad := string(bytes.Repeat([]byte("L"), 5120))

			const total = 120
			ids := make([][]byte, total)
			docs := make([][]byte, total)
			docByID := make(map[string][]byte, total)
			for i := 0; i < total; i++ {
				id := fmt.Sprintf("b%04d", i)
				ids[i] = []byte(id)
				pad := smallPad
				if i%7 == 0 {
					// > 4 KiB document stored via value-log pointer.
					pad = largePad
				}
				tags := []string{
					fmt.Sprintf("g%03d", i%40),
					fmt.Sprintf("g%03d", (i+1)%40),
				}
				docs[i] = makeNativeRangeBSONDoc(t, int64(i), tags, pad)
				docByID[id] = docs[i]
			}
			if _, err := col.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			// Also write a 0-byte empty value and a normal inline value directly
			// on owner's primary tree to verify NativeEntriesAtRoot 0-byte empty
			// slice parity (non-nil empty []byte{} vs nil for missing).
			if err := owner.Set([]byte("z-empty-val"), []byte{}); err != nil {
				t.Fatal(err)
			}
			if err := owner.Set([]byte("z-inline-a"), []byte("alpha-value")); err != nil {
				t.Fatal(err)
			}
			if err := owner.Set([]byte("z-inline-b"), []byte("beta-longer-value")); err != nil {
				t.Fatal(err)
			}
			if err := owner.Set([]byte("z-large-ptr"), bytes.Repeat([]byte("P"), 5000)); err != nil {
				t.Fatal(err)
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			// Delete several docs and checkpoint so physical B-tree has gaps.
			deletedSet := map[int]bool{3: true, 16: true, 17: true, 63: true}
			for idx := range deletedSet {
				if err := col.Delete(ids[idx]); err != nil {
					t.Fatal(err)
				}
			}
			if err := owner.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			// 1. Direct NativeEntriesAtRoot contract checks:
			//    inline, 0-byte empty (non-nil), missing (nil), pointer (FlagPointer, Value==nil),
			//    copyValues=false, empty keys, mismatched lengths, closed capture.
			t.Run("direct_NativeEntriesAtRoot_contract", func(t *testing.T) {
				read, err := col.nativeJSON.Capture(col.name)
				if err != nil {
					t.Fatal(err)
				}
				batchReader, ok := read.capture.(nativePhysicalBatchEntryReader)
				if !ok {
					read.Close()
					t.Fatal("capture does not implement nativePhysicalBatchEntryReader")
				}
				token, ok := read.capture.Snapshot().StateToken()
				if !ok || token.RootPageID == 0 {
					read.Close()
					t.Fatal("missing root token")
				}

				// Empty keys succeeds immediately.
				if err := batchReader.NativeEntriesAtRoot(token.RootPageID, nil, nil, nil, true); err != nil {
					read.Close()
					t.Fatalf("empty keys: %v", err)
				}
				// Mismatched lengths return an error.
				if err := batchReader.NativeEntriesAtRoot(token.RootPageID, [][]byte{[]byte("z-inline-a")}, nil, make([]bool, 1), true); err == nil {
					read.Close()
					t.Fatal("expected error for mismatched dst length")
				}
				if err := batchReader.NativeEntriesAtRoot(token.RootPageID, [][]byte{[]byte("z-inline-a")}, make([]node.LeafEntry, 1), nil, true); err == nil {
					read.Close()
					t.Fatal("expected error for mismatched found length")
				}

				testKeys := [][]byte{
					[]byte("z-inline-a"),
					[]byte("z-empty-val"),
					[]byte("z-missing-key"),
					[]byte("z-inline-b"),
					[]byte("z-large-ptr"),
				}
				dst := make([]node.LeafEntry, len(testKeys))
				found := make([]bool, len(testKeys))
				if err := batchReader.NativeEntriesAtRoot(token.RootPageID, testKeys, dst, found, true); err != nil {
					read.Close()
					t.Fatalf("NativeEntriesAtRoot(copyValues=true): %v", err)
				}
				if !found[0] || !bytes.Equal(dst[0].Value, []byte("alpha-value")) || cap(dst[0].Value) != len(dst[0].Value) || dst[0].Key != nil {
					t.Fatalf("entry 0 (inline): found=%v val=%q cap=%d key=%v", found[0], dst[0].Value, cap(dst[0].Value), dst[0].Key)
				}
				if !found[1] || dst[1].Value == nil || len(dst[1].Value) != 0 || cap(dst[1].Value) != 0 || dst[1].Key != nil {
					t.Fatalf("entry 1 (0-byte empty): found=%v valNil=%v len=%d cap=%d", found[1], dst[1].Value == nil, len(dst[1].Value), cap(dst[1].Value))
				}
				if found[2] || dst[2].Value != nil || dst[2].Key != nil {
					t.Fatalf("entry 2 (missing): found=%v entry=%+v", found[2], dst[2])
				}
				if !found[3] || !bytes.Equal(dst[3].Value, []byte("beta-longer-value")) || cap(dst[3].Value) != len(dst[3].Value) || dst[3].Key != nil {
					t.Fatalf("entry 3 (inline): found=%v val=%q cap=%d", found[3], dst[3].Value, cap(dst[3].Value))
				}
				if !found[4] || dst[4].Flags&node.FlagPointer == 0 || dst[4].Value != nil || dst[4].Key != nil {
					t.Fatalf("entry 4 (pointer): found=%v flags=%d val=%v", found[4], dst[4].Flags, dst[4].Value)
				}
				// Appending to dst[0].Value must not corrupt dst[3].Value (3-index slab slice).
				_ = append(dst[0].Value, '!')
				if !bytes.Equal(dst[3].Value, []byte("beta-longer-value")) {
					t.Fatalf("slab isolation violated: dst[3].Value=%q", dst[3].Value)
				}

				// copyValues=false omits inline bytes while preserving found & flags.
				dstNoCopy := make([]node.LeafEntry, len(testKeys))
				foundNoCopy := make([]bool, len(testKeys))
				if err := batchReader.NativeEntriesAtRoot(token.RootPageID, testKeys, dstNoCopy, foundNoCopy, false); err != nil {
					read.Close()
					t.Fatalf("NativeEntriesAtRoot(copyValues=false): %v", err)
				}
				for i := range testKeys {
					if foundNoCopy[i] != found[i] || dstNoCopy[i].Value != nil || dstNoCopy[i].Key != nil {
						t.Fatalf("copyValues=false idx %d: found=%v want=%v val=%v key=%v", i, foundNoCopy[i], found[i], dstNoCopy[i].Value, dstNoCopy[i].Key)
					}
				}

				// Closed capture returns ErrClosed.
				rawCap := read.capture
				if err := read.Close(); err != nil {
					t.Fatal(err)
				}
				if err := rawCap.(nativePhysicalBatchEntryReader).NativeEntriesAtRoot(token.RootPageID, testKeys[:1], dst[:1], found[:1], true); !errors.Is(err, backenddb.ErrClosed) {
					t.Fatalf("closed capture err=%v want ErrClosed", err)
				}
			})

			liveInRange := func(lo, hi int) []string {
				var out []string
				for i := lo; i <= hi && i < total; i++ {
					if i < 0 || deletedSet[i] {
						continue
					}
					out = append(out, string(ids[i]))
				}
				return out
			}

			// 2. Limit parity across 1, 10, 16 (inline max), 17 (overflow boundary), 64 (overflow),
			//    exact count (no lookahead), empty range (0 batch calls), and documents=false (0 batch calls).
			t.Run("limits_inline_and_pointer_parity", func(t *testing.T) {
				read, err := col.nativeJSON.Capture(col.name)
				if err != nil {
					t.Fatal(err)
				}
				defer read.Close()
				spy := &batchRangeCaptureSpy{NativeSemanticCapture: read.capture}
				origCap := read.capture
				read.capture = spy
				defer func() { read.capture = origCap }()

				for _, tc := range []struct {
					name      string
					lo, hi    int
					limit     int
					documents bool
					wantCalls int64
				}{
					{name: "limit1", lo: 0, hi: 20, limit: 1, documents: true, wantCalls: 1},
					{name: "limit10_mixed_inline_and_ptr", lo: 0, hi: 40, limit: 10, documents: true, wantCalls: 1},
					{name: "limit16_inline_batch_max", lo: 0, hi: 40, limit: 16, documents: true, wantCalls: 1},
					{name: "limit17_overflow_batch", lo: 0, hi: 40, limit: 17, documents: true, wantCalls: 1},
					{name: "limit64_overflow_batch", lo: 0, hi: 119, limit: 64, documents: true, wantCalls: 1},
					{name: "exact_count_no_lookahead", lo: 20, hi: 29, limit: 10, documents: true, wantCalls: 1},
					{name: "exact_count_plus_one_ptr_lookahead", lo: 20, hi: 28, limit: 8, documents: true, wantCalls: 1}, // id 28 is a >4KiB pointer!
					{name: "empty_range_skips_batch", lo: 500, hi: 600, limit: 10, documents: true, wantCalls: 0},
					{name: "ids_only_skips_batch", lo: 0, hi: 40, limit: 10, documents: false, wantCalls: 0},
				} {
					t.Run(tc.name, func(t *testing.T) {
						spy.batchCalls.Store(0)
						spy.pointCalls.Store(0)
						wantAll := liveInRange(tc.lo, tc.hi)
						wantTrunc := len(wantAll) > tc.limit
						wantIDs := wantAll
						if len(wantIDs) > tc.limit {
							wantIDs = wantAll[:tc.limit]
						}
						var gotIDs []string
						trunc, found, err := read.scanIndexRange("age", IndexRangeOptions{
							Lower: IndexRangeBound{Value: bsonRawInt64(t, int64(tc.lo)), Inclusive: true},
							Upper: IndexRangeBound{Value: bsonRawInt64(t, int64(tc.hi)), Inclusive: true},
							Limit: tc.limit,
						}, tc.documents, func(id, doc []byte) (bool, error) {
							gotIDs = append(gotIDs, string(id))
							if tc.documents && !bytes.Equal(doc, docByID[string(id)]) {
								return false, fmt.Errorf("doc mismatch for %q", id)
							}
							return true, nil
						})
						if err != nil || !found {
							t.Fatalf("scanIndexRange: found=%v err=%v", found, err)
						}
						if trunc != wantTrunc || !reflect.DeepEqual(gotIDs, wantIDs) {
							t.Fatalf("got=%v trunc=%v want=%v trunc=%v", gotIDs, trunc, wantIDs, wantTrunc)
						}
						if gotCalls := spy.batchCalls.Load(); gotCalls != tc.wantCalls {
							t.Fatalf("NativeEntriesAtRoot calls=%d want=%d (pointCalls=%d)", gotCalls, tc.wantCalls, spy.pointCalls.Load())
						}
						if tc.wantCalls > 0 && spy.pointCalls.Load() != 0 {
							t.Fatalf("expected 0 point NativeEntryAtRoot calls on settled batch path, got %d", spy.pointCalls.Load())
						}
					})
				}
			})

			// 3. Orphan primaries (missing & physical tombstone), orphan lookahead
			//    (with and without subsequent live doc), and mandatory lookahead
			//    pointer error parity via r.readValue.
			t.Run("orphans_tombstones_and_lookahead_pointer_error", func(t *testing.T) {
				read, err := col.nativeJSON.Capture(col.name)
				if err != nil {
					t.Fatal(err)
				}
				defer read.Close()
				origCap := read.capture
				defer func() { read.capture = origCap }()

				// (a) Missing & physical tombstone inside first Limit=4 entries for [40, 48].
				//     b0041 -> missing (!found), b0042 -> physical FlagTombstone.
				//     Batch emits b0040, b0043 (emitted=2, missing=2), then cursor fallback
				//     must resume after b0043 and emit b0044, b0045 with truncated=true (b0046 live).
				spy := &batchRangeCaptureSpy{
					NativeSemanticCapture: origCap,
					onBatch: func(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error {
						if err := origCap.(nativePhysicalBatchEntryReader).NativeEntriesAtRoot(root, keys, dst, found, copyValues); err != nil {
							return err
						}
						for i, k := range keys {
							switch string(k) {
							case "b0041":
								found[i] = false
								dst[i] = node.LeafEntry{}
							case "b0042":
								found[i] = true
								dst[i] = node.LeafEntry{Flags: node.FlagTombstone}
							}
						}
						return nil
					},
				}
				read.capture = spy
				var got []string
				trunc, found, err := read.scanIndexRange("age", IndexRangeOptions{
					Lower: IndexRangeBound{Value: bsonRawInt64(t, 40), Inclusive: true},
					Upper: IndexRangeBound{Value: bsonRawInt64(t, 48), Inclusive: true},
					Limit: 4,
				}, true, func(id, doc []byte) (bool, error) {
					if !bytes.Equal(doc, docByID[string(id)]) {
						return false, fmt.Errorf("doc mismatch for %q", id)
					}
					got = append(got, string(id))
					return true, nil
				})
				if err != nil || !found || !trunc || !reflect.DeepEqual(got, []string{"b0040", "b0043", "b0044", "b0045"}) {
					t.Fatalf("missing+tombstone in batch: got=%v trunc=%v found=%v err=%v", got, trunc, found, err)
				}

				// (b) Orphan (Limit+1)-th lookahead with NO subsequent live doc:
				//     [40, 43] with Limit=3 -> b0040, b0041, b0042 live; b0043 lookahead tombstoned ->
				//     fallback starts after b0043 -> end of range -> truncated MUST be false!
				spy.onBatch = func(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error {
					if err := origCap.(nativePhysicalBatchEntryReader).NativeEntriesAtRoot(root, keys, dst, found, copyValues); err != nil {
						return err
					}
					for i, k := range keys {
						if string(k) == "b0043" {
							found[i] = true
							dst[i] = node.LeafEntry{Flags: node.FlagTombstone}
						}
					}
					return nil
				}
				got = nil
				trunc, found, err = read.scanIndexRange("age", IndexRangeOptions{
					Lower: IndexRangeBound{Value: bsonRawInt64(t, 40), Inclusive: true},
					Upper: IndexRangeBound{Value: bsonRawInt64(t, 43), Inclusive: true},
					Limit: 3,
				}, true, func(id, doc []byte) (bool, error) {
					got = append(got, string(id))
					return true, nil
				})
				if err != nil || !found || trunc || !reflect.DeepEqual(got, []string{"b0040", "b0041", "b0042"}) {
					t.Fatalf("orphan lookahead no subsequent live: got=%v trunc=%v err=%v", got, trunc, err)
				}

				// (c) Orphan (Limit+1)-th lookahead WITH a subsequent live doc:
				//     [40, 44] with Limit=3 -> b0043 missing -> fallback finds b0044 live -> truncated=true!
				spy.onBatch = func(root uint64, keys [][]byte, dst []node.LeafEntry, found []bool, copyValues bool) error {
					if err := origCap.(nativePhysicalBatchEntryReader).NativeEntriesAtRoot(root, keys, dst, found, copyValues); err != nil {
						return err
					}
					for i, k := range keys {
						if string(k) == "b0043" {
							found[i] = false
							dst[i] = node.LeafEntry{}
						}
					}
					return nil
				}
				got = nil
				trunc, found, err = read.scanIndexRange("age", IndexRangeOptions{
					Lower: IndexRangeBound{Value: bsonRawInt64(t, 40), Inclusive: true},
					Upper: IndexRangeBound{Value: bsonRawInt64(t, 44), Inclusive: true},
					Limit: 3,
				}, true, func(id, doc []byte) (bool, error) {
					got = append(got, string(id))
					return true, nil
				})
				if err != nil || !found || !trunc || !reflect.DeepEqual(got, []string{"b0040", "b0041", "b0042"}) {
					t.Fatalf("orphan lookahead with subsequent live: got=%v trunc=%v err=%v", got, trunc, err)
				}

				// (d) Mandatory lookahead pointer error parity:
				//     [50, 56] with Limit=6 -> b0050..b0055 (all inline), lookahead is b0056 (56%7==0 -> FlagPointer!).
				//     Inject an error from NativeValueAtPointer for the lookahead pointer -> MUST propagate error!
				wantPtrErr := errors.New("injected lookahead pointer read error")
				spy.onBatch = nil
				spy.onValueAtPtr = func(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error) {
					return nil, wantPtrErr
				}
				got = nil
				trunc, found, err = read.scanIndexRange("age", IndexRangeOptions{
					Lower: IndexRangeBound{Value: bsonRawInt64(t, 50), Inclusive: true},
					Upper: IndexRangeBound{Value: bsonRawInt64(t, 56), Inclusive: true},
					Limit: 6,
				}, true, func(id, doc []byte) (bool, error) {
					got = append(got, string(id))
					return true, nil
				})
				if !errors.Is(err, wantPtrErr) || trunc || !found || len(got) != 6 {
					t.Fatalf("lookahead pointer error parity: got=%v trunc=%v found=%v err=%v want %v", got, trunc, found, err, wantPtrErr)
				}
			})

			// 4. Reentrant r.Close() inside visit on the settled batched path.
			t.Run("reentrant_close_inside_visit", func(t *testing.T) {
				for _, tc := range []struct {
					name    string
					limit   int
					cont    bool
					wantErr error
				}{
					{name: "limit10_cont_true", limit: 10, cont: true, wantErr: backenddb.ErrClosed},
					{name: "limit1_cont_true", limit: 1, cont: true, wantErr: backenddb.ErrClosed},
					{name: "limit10_cont_false", limit: 10, cont: false, wantErr: nil},
				} {
					t.Run(tc.name, func(t *testing.T) {
						rr, err := col.nativeJSON.Capture(col.name)
						if err != nil {
							t.Fatal(err)
						}
						calls := 0
						_, found, err := rr.scanIndexRange("age", IndexRangeOptions{
							Lower: IndexRangeBound{Value: bsonRawInt64(t, 0), Inclusive: true},
							Upper: IndexRangeBound{Value: bsonRawInt64(t, 40), Inclusive: true},
							Limit: tc.limit,
						}, true, func(id, doc []byte) (bool, error) {
							calls++
							if !bytes.Equal(doc, docByID[string(id)]) {
								return false, fmt.Errorf("doc mismatch for %q", id)
							}
							if closeErr := rr.Close(); closeErr != nil {
								return false, closeErr
							}
							if _, getErr := rr.Get(id); !errors.Is(getErr, backenddb.ErrClosed) {
								return false, fmt.Errorf("expected ErrClosed after reentrant Close, got %v", getErr)
							}
							return tc.cont, nil
						})
						if !found || calls != 1 || rr.capture != nil {
							t.Fatalf("calls=%d found=%v captureOpen=%v err=%v", calls, found, rr.capture != nil, err)
						}
						if tc.wantErr != nil {
							if !errors.Is(err, tc.wantErr) {
								t.Fatalf("err=%v want %v", err, tc.wantErr)
							}
						} else if err != nil {
							t.Fatalf("unexpected err=%v", err)
						}
					})
				}
			})

			// 5. Tight COW capacity error handling (both initial batch admission and
			//    mid-scan overflow batch admission for Limit > 16) with zero lease leaks.
			t.Run("tight_cow_capacity_and_zero_leaks", func(t *testing.T) {
				read, err := col.nativeJSON.Capture(col.name)
				if err != nil {
					t.Fatal(err)
				}
				defer read.Close()

				// Warm lanes first so lane workspace admission is settled.
				warmOpts := IndexRangeOptions{
					Lower: IndexRangeBound{Value: int64(0), Inclusive: true},
					Upper: IndexRangeBound{Value: int64(40), Inclusive: true},
					Limit: 10,
				}
				if _, _, err := read.scanIndexRange("age_i64", warmOpts, true, func(_, _ []byte) (bool, error) {
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}

				budget := read.capture.NativeAllocator().(nativeHNSWAdmissionBudget)

				// (a) Overflow Limit=24 (totalKeys=25 > 17): give headroom for initial
				//     admission, but NOT enough for overflow slice admission inside
				//     collector.admit(...). Must fail with ErrCOWCapacity and zero leaks.
				overflowOpts := IndexRangeOptions{
					Lower: IndexRangeBound{Value: int64(0), Inclusive: true},
					Upper: IndexRangeBound{Value: int64(80), Inclusive: true},
					Limit: 24,
				}
				// Initial charge for Limit=24 (~4.5 KiB); overflow batch slices need ~2.8 KiB more.
				overflowExtra := memtable.COWAllocationCharge(25*uint64(unsafe.Sizeof(node.LeafEntry{}))) +
					memtable.COWAllocationCharge(25*uint64(unsafe.Sizeof(bool(false)))) +
					memtable.COWAllocationCharge(25*uint64(unsafe.Sizeof([]byte{})))
				// Leave strictly less than overflowExtra beyond the initial ~6 KiB headroom?
				// Easier: leave 64 bytes of headroom -> initial admission fails cleanly,
				// AND also test exact mid-scan failure by leaving initial + 128 bytes.
				blockerFull, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
				if err != nil {
					t.Fatal(err)
				}
				avail := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
				if err := blockerFull.Resize(avail - 64); err != nil {
					blockerFull.Close()
					t.Fatal(err)
				}
				beforeFull := budget.Stats()
				_, _, err = read.scanIndexRange("age_i64", warmOpts, true, func(_, _ []byte) (bool, error) {
					return true, nil
				})
				afterFull := budget.Stats()
				blockerFull.Close()
				if !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("tight initial batch err=%v want ErrCOWCapacity", err)
				}
				if afterFull.ReservedBytes != beforeFull.ReservedBytes || afterFull.ExternalLeases != beforeFull.ExternalLeases {
					t.Fatalf("tight initial batch leaked: before=%+v after=%+v", beforeFull, afterFull)
				}

				// (b) Mid-scan overflow refusal on Limit=24: find headroom where
				//     documents=false succeeds (no batch) or scan completes, while
				//     documents=true overflow admission fails with ErrCOWCapacity.
				//     Compute approximate initial charge for Limit=24 documents=true:
				//     warmRangeCollector + inline/24 slice + 25*64B + 2*256B + warmRangeDocBatch (~5.5 KiB),
				//     while overflow needs +overflowExtra (~2.8 KiB) AFTER collecting 24 IDs (consuming ~1.5 KiB of preAdmitted).
				//     With headroom = initial + overflowExtra/4, AdmitNativeBytes succeeds and
				//     collector.admit(overflow) fails with ErrCOWCapacity and zero leaks!
				blockerMid, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
				if err != nil {
					t.Fatal(err)
				}
				avail = budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
				midHeadroom := uint64(6400) + overflowExtra/4
				if midHeadroom > avail {
					midHeadroom = avail / 2
				}
				if err := blockerMid.Resize(avail - midHeadroom); err != nil {
					blockerMid.Close()
					t.Fatal(err)
				}
				beforeMid := budget.Stats()
				_, _, err = read.scanIndexRange("age_i64", overflowOpts, true, func(_, _ []byte) (bool, error) {
					return true, nil
				})
				afterMid := budget.Stats()
				blockerMid.Close()
				if !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("mid-scan overflow err=%v want ErrCOWCapacity (headroom=%d overflowExtra=%d)", err, midHeadroom, overflowExtra)
				}
				if afterMid.ReservedBytes != beforeMid.ReservedBytes || afterMid.ExternalLeases != beforeMid.ExternalLeases {
					t.Fatalf("mid-scan overflow leaked: before=%+v after=%+v", beforeMid, afterMid)
				}
			})

			// 6. Pending & captured primary overlay fallback: when primary domain has
			//    pending != nil or captured != nil, scanIndexRange must fall back to
			//    getAdmittedLocked (zero NativeEntriesAtRoot calls) and return exact results.
			t.Run("pending_and_captured_primary_overlay_fallback", func(t *testing.T) {
				updatedDoc := makeNativeRangeBSONDoc(t, 10, []string{"g010"}, "updated-in-pending-overlay")
				if _, _, err := col.Update(ids[10], func([]byte) ([]byte, bool, error) {
					return updatedDoc, true, nil
				}); err != nil {
					t.Fatal(err)
				}
				// Verify pending primary overlay (no checkpoint).
				overlayRead, err := col.nativeJSON.Capture(col.name)
				if err != nil {
					t.Fatal(err)
				}
				defer overlayRead.Close()
				if overlayRead.domain.domain == nil || overlayRead.domain.domain.pending == nil {
					t.Fatal("expected pending primary overlay before checkpoint")
				}
				spy := &batchRangeCaptureSpy{NativeSemanticCapture: overlayRead.capture}
				origCap := overlayRead.capture
				overlayRead.capture = spy
				defer func() { overlayRead.capture = origCap }()

				var sawUpdated bool
				trunc, found, err := overlayRead.scanIndexRange("age", IndexRangeOptions{
					Lower: IndexRangeBound{Value: bsonRawInt64(t, 8), Inclusive: true},
					Upper: IndexRangeBound{Value: bsonRawInt64(t, 20), Inclusive: true},
					Limit: 10,
				}, true, func(id, doc []byte) (bool, error) {
					if string(id) == "b0010" {
						sawUpdated = bytes.Equal(doc, updatedDoc)
					} else if !bytes.Equal(doc, docByID[string(id)]) {
						return false, fmt.Errorf("doc mismatch for %q", id)
					}
					return true, nil
				})
				if err != nil || !found || !trunc || !sawUpdated {
					t.Fatalf("pending overlay fallback: sawUpdated=%v trunc=%v found=%v err=%v", sawUpdated, trunc, found, err)
				}
				if spy.batchCalls.Load() != 0 {
					t.Fatalf("expected 0 NativeEntriesAtRoot calls with pending primary overlay, got %d", spy.batchCalls.Load())
				}

				// Verify captured != nil primary overlay also bypasses batched primary fetch.
				wantCapDoc := makeNativeRangeBSONDoc(t, 11, []string{"g011"}, "updated-for-captured")
				if _, _, err := col.Update(ids[11], func([]byte) ([]byte, bool, error) {
					return wantCapDoc, true, nil
				}); err != nil {
					t.Fatal(err)
				}
				// Construct a synthetic captured-only primary domain state (pending == nil, captured != nil)
				// using the real pending B-tree wrapped in a nativeCapturedSource node.
				capRead, err := col.nativeJSON.Capture(col.name)
				if err != nil {
					t.Fatal(err)
				}
				defer capRead.Close()
				if capRead.domain.domain == nil || capRead.domain.domain.pending == nil {
					t.Fatal("expected pending primary overlay to wrap as captured")
				}
				origPrimary := capRead.domain.domain
				fakeCapturedDomain := &nativeDomainState{
					baseRoot: origPrimary.baseRoot,
					captured: &nativePendingSources{
						source: &nativePendingSource{root: origPrimary.pending},
					},
				}
				fakeCapturedDomain.refs.Store(1)
				capRead.domain.domain = fakeCapturedDomain
				defer func() { capRead.domain.domain = origPrimary }()

				capSpy := &batchRangeCaptureSpy{NativeSemanticCapture: capRead.capture}
				origCap2 := capRead.capture
				capRead.capture = capSpy
				defer func() { capRead.capture = origCap2 }()

				var sawCapUpdated bool
				trunc, found, err = capRead.scanIndexRange("age", IndexRangeOptions{
					Lower: IndexRangeBound{Value: bsonRawInt64(t, 8), Inclusive: true},
					Upper: IndexRangeBound{Value: bsonRawInt64(t, 20), Inclusive: true},
					Limit: 10,
				}, true, func(id, doc []byte) (bool, error) {
					if string(id) == "b0011" {
						sawCapUpdated = bytes.Equal(doc, wantCapDoc)
					}
					return true, nil
				})
				if err != nil || !found || !trunc || !sawCapUpdated {
					t.Fatalf("captured overlay fallback: sawCapUpdated=%v trunc=%v found=%v err=%v", sawCapUpdated, trunc, found, err)
				}
				if capSpy.batchCalls.Load() != 0 {
					t.Fatalf("expected 0 NativeEntriesAtRoot calls with captured primary overlay, got %d", capSpy.batchCalls.Load())
				}
			})
		})
	}
}

func BenchmarkScanIndexRangeBatchedLimit10StandaloneCOW(b *testing.B) {
	owner, _, col := openNativeIndexRangeFixture(b, nativeIndexRangeFixtureOpts{
		outerLeavesDict: runtime.GOOS != "windows",
		indexes: []IndexDefinition{
			{Name: "age", Field: "age", ValueType: IndexValueBSONOrderedV2},
		},
	})
	const total = 4000
	pad := string(bytes.Repeat([]byte("a"), 192))
	ids := make([][]byte, total)
	docs := make([][]byte, total)
	for i := 0; i < total; i++ {
		ids[i] = []byte(fmt.Sprintf("doc%06d", i))
		docs[i] = makeNativeRangeBSONDoc(b, int64(i%100), nil, pad)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatal(err)
	}
	if err := owner.Checkpoint(); err != nil {
		b.Fatal(err)
	}

	const numRanges = 64
	rangeOpts := make([]IndexRangeOptions, numRanges)
	for i := 0; i < numRanges; i++ {
		lo := int64(i % 80)
		hi := lo + 15
		rangeOpts[i] = IndexRangeOptions{
			Lower: IndexRangeBound{Value: bsonRawInt64(b, lo), Inclusive: true},
			Upper: IndexRangeBound{Value: bsonRawInt64(b, hi), Inclusive: true},
			Limit: 10,
		}
	}

	// Warm caches and read lanes before timing.
	for i := 0; i < numRanges; i++ {
		recs, trunc, err := col.FindDocumentsByIndexRange("age", rangeOpts[i])
		if err != nil || !trunc || len(recs) != 10 {
			b.Fatalf("warmup FindDocumentsByIndexRange: len=%d trunc=%v err=%v", len(recs), trunc, err)
		}
	}

	b.Run("Serial", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			recs, trunc, err := col.FindDocumentsByIndexRange("age", rangeOpts[i%numRanges])
			if err != nil || !trunc || len(recs) != 10 {
				b.Fatalf("FindDocumentsByIndexRange: len=%d trunc=%v err=%v", len(recs), trunc, err)
			}
		}
	})

	b.Run("Parallel32", func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(32/max(runtime.GOMAXPROCS(0), 1) + 1)
		var counter atomic.Uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				idx := counter.Add(1) % numRanges
				recs, trunc, err := col.FindDocumentsByIndexRange("age", rangeOpts[idx])
				if err != nil || !trunc || len(recs) != 10 {
					b.Errorf("FindDocumentsByIndexRange: len=%d trunc=%v err=%v", len(recs), trunc, err)
					return
				}
			}
		})
	})
}
