package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func cowNativeLeafProofFixture(tb testing.TB, entries int, compressed bool) (*DB, *backenddb.DB, *NativeSemanticCapture, uint64, []byte) {
	tb.Helper()
	dir := tb.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPageReadCacheEntries:   entries,
		VerifyOnRead:               false,
	})
	if err != nil {
		tb.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		_ = backend.Close()
		tb.Fatal(err)
	}
	compression := vlogCompressionOff
	if compressed {
		compression = vlogCompressionBlock
	}
	db, err := Open(dir, backend, Options{
		TransactionOwner:           owner,
		DisableWAL:                 true,
		AllowUnsafe:                true,
		MemtableMode:               "cow_btree",
		FlushThreshold:             1 << 30,
		IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold:   256,
		ValueLogCompression:        uint8(compression),
		ValueLogBlockCodec:         0, // Snappy when compressed
	})
	if err != nil {
		_ = backend.Close()
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				tb.Error(err)
			}
		}
	})
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		tb.Fatal(err)
	}
	want := bytes.Repeat([]byte("native-leaf-proof-val"), 6)
	for i := 0; i < 64; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k%03d", i)), want); err != nil {
			tb.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		tb.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		tb.Fatal(err)
	}
	capRaw, err := backend.CaptureNativeSemanticState()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := capRaw.Close(); err != nil {
			tb.Error(err)
		}
	})
	capture := capRaw.(*NativeSemanticCapture)
	token, ok := capture.Snapshot().StateToken()
	if !ok {
		tb.Fatal("missing basis state token")
	}
	return db, backend, capture, token.RootPageID, want
}

func cowOuterLeafStatCounter(tb testing.TB, backend *backenddb.DB, key string) uint64 {
	tb.Helper()
	raw := backend.Stats()[key]
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		tb.Fatalf("parse stat %q=%q: %v", key, raw, err)
	}
	return v
}

func TestNativeEntryAtRootPopulatesPageChecksumVerifiedHits(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			_, backend, capture, rootID, want := cowNativeLeafProofFixture(t, 128, compressed)
			key := []byte("k000")

			marksBefore := cowLeafCacheCounter(t, backend, "page_checksum_verified_marks")
			entry, err := capture.NativeEntryAtRoot(rootID, key, true)
			if err != nil || !bytes.Equal(entry.Value, want) {
				t.Fatalf("first NativeEntryAtRoot: got %q, err %v", entry.Value, err)
			}
			entry.Value[0] ^= 1 // caller mutation must not affect cached page

			marksAfter := cowLeafCacheCounter(t, backend, "page_checksum_verified_marks")
			if marksAfter <= marksBefore {
				t.Fatalf("first NativeEntryAtRoot did not mark page checksum verified: marks %d -> %d", marksBefore, marksAfter)
			}

			verifiedHitsBefore := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")
			skipsStatBefore := cowOuterLeafStatCounter(t, backend, "treedb.process.read_path.outer_leaf.checksum.skips_total")
			skipsBefore := tree.OuterLeafReadStatsSnapshot().ChecksumSkippedTotal

			for i := 0; i < 4; i++ {
				got, err := capture.NativeEntryAtRoot(rootID, key, true)
				if err != nil || !bytes.Equal(got.Value, want) {
					t.Fatalf("repeat NativeEntryAtRoot %d: got %q, err %v", i, got.Value, err)
				}
				got.Value[0] ^= 1
			}

			verifiedHitsAfter := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")
			skipsStatAfter := cowOuterLeafStatCounter(t, backend, "treedb.process.read_path.outer_leaf.checksum.skips_total")
			skipsAfter := tree.OuterLeafReadStatsSnapshot().ChecksumSkippedTotal
			if verifiedHitsAfter <= verifiedHitsBefore {
				t.Fatalf("repeat NativeEntryAtRoot did not record page_checksum_verified_hits: %d -> %d", verifiedHitsBefore, verifiedHitsAfter)
			}
			if skipsStatAfter <= skipsStatBefore || skipsAfter <= skipsBefore {
				t.Fatalf("repeat NativeEntryAtRoot did not skip page checksum verification: stat %d -> %d, snapshot %d -> %d",
					skipsStatBefore, skipsStatAfter, skipsBefore, skipsAfter)
			}

			// Verify NativeEntryAtRootInto reuses the verified proof with caller dst.
			intoHitsBefore := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")
			intoSkipsBefore := tree.OuterLeafReadStatsSnapshot().ChecksumSkippedTotal
			dst := make([]byte, 0, len(want))
			intoEntry, err := capture.NativeEntryAtRootInto(rootID, key, dst)
			if err != nil || !bytes.Equal(intoEntry.Value, want) {
				t.Fatalf("NativeEntryAtRootInto: got %q, err %v", intoEntry.Value, err)
			}
			if cowLeafCacheCounter(t, backend, "page_checksum_verified_hits") <= intoHitsBefore ||
				tree.OuterLeafReadStatsSnapshot().ChecksumSkippedTotal <= intoSkipsBefore {
				t.Fatal("NativeEntryAtRootInto did not reuse verified page checksum proof")
			}

			// Verify nativeConditionalTxn.NativeEntryAtRoot also reuses the verified proof.
			txRaw, err := backend.BeginNativeConditionalTxn()
			if err != nil {
				t.Fatal(err)
			}
			defer txRaw.Close()
			txReader := txRaw.(backenddb.NativePhysicalReader)
			txHitsBefore := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")
			txSkipsBefore := tree.OuterLeafReadStatsSnapshot().ChecksumSkippedTotal
			txEntry, err := txReader.NativeEntryAtRoot(rootID, key, true)
			if err != nil || !bytes.Equal(txEntry.Value, want) {
				t.Fatalf("nativeConditionalTxn.NativeEntryAtRoot: got %q, err %v", txEntry.Value, err)
			}
			if cowLeafCacheCounter(t, backend, "page_checksum_verified_hits") <= txHitsBefore ||
				tree.OuterLeafReadStatsSnapshot().ChecksumSkippedTotal <= txSkipsBefore {
				t.Fatal("nativeConditionalTxn.NativeEntryAtRoot did not reuse verified page checksum proof")
			}
		})
	}
}

func TestNativeEntryAtRootCorruptCachedLeafStillDetectedBeforeMark(t *testing.T) {
	disableCOWSealedLeafMmapForFaultInjection(t)
	_, backend, capture, rootID, _ := cowNativeLeafProofFixture(t, 128, false)
	key := []byte("k000")

	var ks, ls [page.PageSize]byte
	var ptr page.LeafLogPtr
	stop := errors.New("capture physical leaf pointer")
	_, err := capture.Snapshot().GetEntryExactWithFixedScratch(key, ks[:], ls[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) {
		ptr = p
		return nil, stop
	})
	if !errors.Is(err, stop) || ptr.FileID == 0 {
		t.Fatalf("fixture did not traverse real logged leaf: %+v %v", ptr, err)
	}
	f, _, err := capture.Snapshot().PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	// 1. Corrupt record CRC on disk: record decode fails before admission/mark.
	start := int64(ptr.Offset - 4)
	var header [valuelog.HeaderSize]byte
	if _, err := writer.ReadAt(header[:], start); err != nil {
		t.Fatal(err)
	}
	origCRCByte := header[0]
	marksBefore := cowLeafCacheCounter(t, backend, "page_checksum_verified_marks")
	verifiedHitsBefore := cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")

	writeCOWUnmappedLeafByte(t, backend, writer, start, origCRCByte^1)
	if _, err := capture.NativeEntryAtRoot(rootID, key, true); !errors.Is(err, valuelog.ErrCorrupt) {
		t.Fatalf("corrupt record CRC did not fail with ErrCorrupt: %v", err)
	}
	if cowLeafCacheCounter(t, backend, "page_checksum_verified_marks") != marksBefore ||
		cowLeafCacheCounter(t, backend, "page_checksum_verified_hits") != verifiedHitsBefore {
		t.Fatal("failed record CRC decode marked or hit verified page checksum")
	}
	writeCOWUnmappedLeafByte(t, backend, writer, start, origCRCByte)

	// 2. Corrupt the inner leaf page checksum for ptr.SubIndex while updating
	// the valuelog record CRC so record decode succeeds, then outer-leaf page
	// checksum validation must fail BEFORE markCopiedPageChecksumVerified is
	// ever called.
	valueLen := binary.LittleEndian.Uint32(header[16:20])
	if valueLen == 0 {
		t.Fatalf("unexpected empty leaf record payload")
	}
	payload := make([]byte, valueLen)
	if _, err := writer.ReadAt(payload, start+valuelog.HeaderSize); err != nil {
		t.Fatal(err)
	}
	subStart := 0
	if header[5]&1 != 0 { // grouped frame
		if len(payload) < valuelog.FrameHeaderSize {
			t.Fatalf("grouped leaf record payload too short: %d", len(payload))
		}
		k := int(payload[2])
		if k <= int(ptr.SubIndex) {
			t.Fatalf("grouped frame k=%d <= subIndex=%d", k, ptr.SubIndex)
		}
		offTableStart := valuelog.FrameHeaderSize + k*8
		dataStart := offTableStart + (k+1)*4
		relStart := int(binary.LittleEndian.Uint32(payload[offTableStart+int(ptr.SubIndex)*4:]))
		subStart = dataStart + relStart
	}
	subPayload := payload[subStart:]
	// Leaf page header bytes [8:12] hold the page CRC32C checksum. Compact leaf
	// payloads prepend a 12-byte header (8-byte magic + 2-byte prefixLen + 2-byte suffixLen).
	leafHeaderOff := 0
	if valuelog.HasCompactLeafLogPayload(subPayload) {
		leafHeaderOff = 12
	}
	corruptIdx := subStart + leafHeaderOff + 8
	if corruptIdx >= len(payload) {
		t.Fatalf("corrupt index %d out of range for payload len %d", corruptIdx, len(payload))
	}
	origChecksumByte := payload[corruptIdx]
	payload[corruptIdx] ^= 0xff
	newCRC := crc.ChecksumParts(header[4:], payload)
	var newCRCBytes [4]byte
	binary.LittleEndian.PutUint32(newCRCBytes[:], newCRC)

	requireCOWUnmappedFaultFixture(t, backend)
	if _, err := writer.WriteAt(payload[corruptIdx:corruptIdx+1], start+valuelog.HeaderSize+int64(corruptIdx)); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteAt(newCRCBytes[:], start); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = writer.WriteAt([]byte{origChecksumByte}, start+valuelog.HeaderSize+int64(corruptIdx))
		_, _ = writer.WriteAt(header[:4], start)
	}()

	marksBefore = cowLeafCacheCounter(t, backend, "page_checksum_verified_marks")
	verifiedHitsBefore = cowLeafCacheCounter(t, backend, "page_checksum_verified_hits")
	for attempt := 0; attempt < 2; attempt++ {
		_, err := capture.NativeEntryAtRoot(rootID, key, true)
		if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
			t.Fatalf("attempt %d: corrupt leaf page checksum not detected: %v", attempt, err)
		}
		if cowLeafCacheCounter(t, backend, "page_checksum_verified_marks") != marksBefore {
			t.Fatalf("attempt %d: corrupt leaf was marked page checksum verified", attempt)
		}
		if cowLeafCacheCounter(t, backend, "page_checksum_verified_hits") != verifiedHitsBefore {
			t.Fatalf("attempt %d: corrupt leaf recorded page_checksum_verified_hits", attempt)
		}
	}
}

func BenchmarkNativeEntryAtRootHotCachedLeaf(b *testing.B) {
	db, backend, _, _, want := cowNativeLeafProofFixture(b, 128, false)
	// Seal the outer-leaf segment so InspectCOWLeafRecordPinned uses the
	// steady-state mmap path instead of active-segment pread syscalls.
	for _, l := range db.leafLogAppendLanesSnapshot() {
		l.vlogMu.Lock()
		rotateErr := db.rotateValueLogMuHeld(l)
		l.vlogMu.Unlock()
		if rotateErr != nil {
			b.Fatal(rotateErr)
		}
	}
	if err := db.Set([]byte("z-seal-sentinel"), want); err != nil {
		b.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	capRaw, err := backend.CaptureNativeSemanticState()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = capRaw.Close() })
	capture := capRaw.(*NativeSemanticCapture)
	token, ok := capture.Snapshot().StateToken()
	if !ok {
		b.Fatal("missing basis state token")
	}
	rootID := token.RootPageID

	key := []byte("k000")
	// Warm the leaf into the verified cache before timing.
	for i := 0; i < 4; i++ {
		e, err := capture.NativeEntryAtRoot(rootID, key, true)
		if err != nil || !bytes.Equal(e.Value, want) {
			b.Fatalf("warmup: got %q, err %v", e.Value, err)
		}
	}

	b.Run("NativeEntryAtRoot", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e, err := capture.NativeEntryAtRoot(rootID, key, true)
			if err != nil || len(e.Value) != len(want) {
				b.Fatalf("NativeEntryAtRoot: len=%d err=%v", len(e.Value), err)
			}
		}
	})

	b.Run("NativeEntryAtRootInto", func(b *testing.B) {
		dst := make([]byte, 0, len(want))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e, err := capture.NativeEntryAtRootInto(rootID, key, dst[:0])
			if err != nil || len(e.Value) != len(want) {
				b.Fatalf("NativeEntryAtRootInto: len=%d err=%v", len(e.Value), err)
			}
			dst = e.Value
		}
	})
}
