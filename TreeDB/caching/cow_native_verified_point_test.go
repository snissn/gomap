package caching

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Real captured basis/cache pages first enter through the original verified
// decoder. The companion must reuse that slot/state and preserve old copy-out.
func TestCOWNativeAdmittedPointVerifiedView(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, entries := range []int{128, -1} {
			t.Run(fmt.Sprintf("compressed=%t/entries=%d", compressed, entries), func(t *testing.T) {
				db, backend, want := cowLeafCacheFixture(t, entries, compressed)
				budget := db.cow.budget
				t.Cleanup(func() {
					if !db.closing.Load() {
						if err := db.Close(); err != nil {
							t.Error(err)
						}
					}
					deadline := time.Now().Add(3 * time.Second)
					for budget.Stats().TotalBytes != 0 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
						t.Errorf("final original owner retained receipts: %+v", stats)
					}
				})
				if err := db.Set([]byte("pointer"), bytes.Repeat([]byte("p"), 1024)); err != nil {
					t.Fatal(err)
				}
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				held, err := db.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				defer held.Close()
				basis := held.cowCut.basis
				token, ok := basis.snapshot.StateToken()
				if !ok {
					t.Fatal("missing captured physical token")
				}
				// Public Get supplies the actual original full-record CRC verification.
				if got, err := db.Get([]byte("a000")); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("original decode: %q %v", got, err)
				}
				var key, leaf [page.PageSize]byte
				var ptr page.LeafLogPtr
				stop := errors.New("capture existing leaf")
				_, err = basis.snapshot.GetEntryExactWithFixedScratch([]byte("a000"), key[:], leaf[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) { ptr = p; return nil, stop })
				if !errors.Is(err, stop) || ptr.FileID == 0 {
					t.Fatalf("real pointer %+v %v", ptr, err)
				}
				beforeMarks := cowLeafCacheCounter(t, backend, "page_checksum_verified_marks")
				calls := 0
				admit := func(n uint64) error {
					calls++
					if n != memtable.COWAllocationCharge(uint64(len(want))) {
						t.Fatalf("rounded output charge=%d", n)
					}
					return nil
				}
				got, err := basis.nativeEntryAtRootAdmitted(db.cow, token.RootPageID, []byte("a000"), true, admit)
				if err != nil || !bytes.Equal(got.Value, want) || len(got.Key) != 0 || cap(got.Value) != len(got.Value) || calls != 1 {
					t.Fatalf("admitted point %+v calls=%d err=%v", got, calls, err)
				}
				got.Value[0] ^= 1
				if entries > 0 && cowLeafCacheCounter(t, backend, "page_checksum_verified_marks") <= beforeMarks {
					t.Fatal("actual page CRC did not mark existing slot")
				}
				point := basis.reader.point
				for i := range point.leaf {
					point.leaf[i] = 0xa5
				}
				beforeHits := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")
				got, err = basis.nativeEntryAtRootAdmitted(db.cow, token.RootPageID, []byte("a000"), true, admit)
				if err != nil || !bytes.Equal(got.Value, want) {
					t.Fatalf("independent repeated output: %q %v", got.Value, err)
				}
				if entries > 0 {
					if cowLeafCacheCounter(t, backend, "page_checksum_verified_hits") <= beforeHits {
						t.Fatal("existing verified page state not reused")
					}
					for _, b := range point.leaf {
						if b != 0xa5 {
							t.Fatal("cache view copied page scratch")
						}
					}
				}
				// The original metadata-only contract remains uncharged/without output.
				if e, err := basis.nativeEntryAtRootAdmitted(db.cow, token.RootPageID, []byte("a000"), false, func(uint64) error { t.Fatal("metadata probe admitted output"); return nil }); err != nil || e.Value != nil || e.Key != nil {
					t.Fatalf("metadata probe %+v %v", e, err)
				}
				if e, err := basis.nativeEntryAtRootAdmitted(db.cow, token.RootPageID, []byte("pointer"), true, func(uint64) error { t.Fatal("pointer admitted inline output"); return nil }); err != nil || e.Flags&node.FlagPointer == 0 || e.Value != nil || e.ValuePtr.FileID == 0 {
					t.Fatalf("pointer metadata %+v %v", e, err)
				}
				before := db.cow.budget.Stats()
				denied := errors.New("original output admission refused")
				if e, err := basis.nativeEntryAtRootAdmitted(db.cow, token.RootPageID, []byte("a000"), true, func(uint64) error { return denied }); !errors.Is(err, denied) || e.Value != nil {
					t.Fatalf("output refusal: %+v %v", e, err)
				}
				if !basis.readMu.TryLock() {
					t.Fatal("output refusal retained basis lock")
				}
				basis.readMu.Unlock()
				after := db.cow.budget.Stats()
				if after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases {
					t.Fatalf("refusal added ownership: before=%+v after=%+v", before, after)
				}
				if _, err := basis.nativeEntryAtRootAdmitted(db.cow, token.RootPageID, []byte("absent"), true, func(uint64) error { t.Fatal("missing output admission"); return nil }); !errors.Is(err, tree.ErrKeyNotFound) {
					t.Fatalf("missing: %v", err)
				}
				// Current inspection limits must apply even to an already verified slot.
				basis.readMu.Lock()
				originalLimits := basis.reader.limits
				basis.reader.limits.MaxValueBytes = 1
				_, lease, _, err := basis.readPhysicalLeafWithWorkspaceView(db.cow, basis.reader, ptr, point.leaf[:0], true, false, true)
				basis.reader.limits = originalLimits
				basis.readMu.Unlock()
				if lease != nil {
					lease.ReleaseLeafLogPageView()
				}
				if !errors.Is(err, valuelog.ErrCOWReadCapacity) {
					t.Fatalf("verified cache bypassed current limit: %v", err)
				}
				// The original public write/handoff cannot alter this captured basis.
				if err = db.Set([]byte("a000"), []byte("new")); err != nil {
					t.Fatal(err)
				}
				if err = db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				newer, err := db.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				defer newer.Close()
				newToken, ok := newer.cowCut.basis.snapshot.StateToken()
				if !ok {
					t.Fatal("missing new token")
				}
				for i := 0; i < 3; i++ {
					old, err := basis.nativeEntryAtRootAdmitted(db.cow, token.RootPageID, []byte("a000"), true, admit)
					if err != nil || !bytes.Equal(old.Value, want) {
						t.Fatalf("held old point %q %v", old.Value, err)
					}
					current, err := newer.cowCut.basis.nativeEntryAtRootAdmitted(db.cow, newToken.RootPageID, []byte("a000"), true, nil)
					if err != nil || string(current.Value) != "new" {
						t.Fatalf("new basis point %q %v", current.Value, err)
					}
				}
				// A verified slot never skips the original record inspector on a later call.
				file, _, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
				if err != nil {
					t.Fatal(err)
				}
				writer, err := os.OpenFile(file.Name(), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close()
				var version [1]byte
				if _, err = writer.ReadAt(version[:], int64(ptr.Offset)); err != nil {
					t.Fatal(err)
				}
				if _, err = writer.WriteAt([]byte{version[0] ^ 1}, int64(ptr.Offset)); err != nil {
					t.Fatal(err)
				}
				defer writer.WriteAt(version[:], int64(ptr.Offset))
				if _, err = basis.nativeEntryAtRootAdmitted(db.cow, token.RootPageID, []byte("a000"), true, admit); !errors.Is(err, valuelog.ErrCorrupt) {
					t.Fatalf("verified view bypassed inspector: %v", err)
				}
			})
		}
	}
}

func TestCOWNativeAdmittedPointOptionalClosedGuards(t *testing.T) {
	var capture *NativeSemanticCapture
	if _, err := capture.NativeEntryAtRootAdmitted(1, []byte("a"), true, nil); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatal(err)
	}
	var tx *nativeConditionalTxn
	if _, err := tx.NativeEntryAtRootAdmitted(1, []byte("a"), true, nil); !errors.Is(err, backenddb.ErrConditionalTxnClosed) {
		t.Fatal(err)
	}
	var _ backenddb.NativeAdmittedPhysicalReader = (*NativeSemanticCapture)(nil)
	var _ backenddb.NativeAdmittedPhysicalReader = (*nativeConditionalTxn)(nil)
}

// An existing cached dictionary page does not expand native codec eligibility.
func TestCOWNativeAdmittedPointRetainsCodecPolicy(t *testing.T) {
	db, _, ptr := cowSuccessorDictionaryLeafFixture(t)
	held, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	basis := held.cowCut.basis
	token, ok := basis.snapshot.StateToken()
	if !ok {
		t.Fatal("missing physical token")
	}
	f, _, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	limit := db.cow.budget.Limits().MaxInFlightBytes
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), valuelog.COWReadLimits{MaxRecordBytes: limit, MaxRawBytes: limit, MaxValueBytes: limit})
	if err != nil || !shape.Compressed || shape.DictID == 0 {
		t.Fatalf("dictionary fixture %+v %v", shape, err)
	}
	if _, err = basis.nativeEntryAtRootAdmitted(db.cow, token.RootPageID, []byte("a000"), true, func(uint64) error { t.Fatal("unsupported codec admitted output"); return nil }); !errors.Is(err, valuelog.ErrCOWReadUnsupported) {
		t.Fatalf("native codec policy widened: %v", err)
	}
}
