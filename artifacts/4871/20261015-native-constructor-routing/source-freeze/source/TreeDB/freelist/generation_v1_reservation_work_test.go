package freelist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// The reference below is the unchanged production reservation encoder from
// 353a88fb160febce951dd4caa45ac079c261fca4. It fixes the pre-factor format
// independently of the new shared continuation's packing helpers.
func TestCanonicalReservationOutputRetainsCanonicalBytesV1(t *testing.T) {
	input := make([]ReservationExtentV1, 200)
	for i := range input {
		input[i] = ReservationExtentV1{StartPageID: 2 + uint64(i)*3, Count: 2, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: 7}
	}
	expectedRecord := ReservationRecordV1{CandidateID: candidateIDFromString("retained-reservation"), GenerationID: 10, BaseID: 9, Extents: input}
	expected, _, err := canonicalReservationReference353V1(800, expectedRecord)
	if err != nil {
		t.Fatal(err)
	}
	actualRecord := expectedRecord
	var work reservationPageEncodeWorkV1
	calls := 0
	for ; calls < 128; calls++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := work.step(800, &actualRecord, &w)
		if err != nil {
			t.Fatal(err)
		}
		if w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("actual reservation work exceeded allowance")
		}
		if ready {
			break
		}
	}
	if calls == 128 || calls < 6 || len(work.pages) != 2 {
		t.Fatalf("reservation progress/pages calls=%d pages=%d", calls, len(work.pages))
	}
	store := NewMemoryPageStoreV1()
	for i, b := range work.pages {
		if !bytes.Equal(b, expected[i]) || !page.VerifyChecksumNonMutating(b) {
			t.Fatalf("canonical reservation page %d differs", i)
		}
		store.Pages[800+uint64(i)] = b
	}
	decoded, err := loadReservationRecord(store, 800, 1000)
	if err != nil || len(decoded.Extents) != 200 || decoded.pendingMetadataCount() != 400 || decoded.Digest() != actualRecord.Digest() {
		t.Fatal("retained reservation lost decoded ranges/digest", err)
	}
	saved := append([]byte(nil), work.pages[0]...)
	var another reservationPageEncodeWorkV1
	later := expectedRecord
	if _, err = another.advance(900, &later, nil); err != nil {
		t.Fatal(err)
	}
	clear(another.pages[0])
	if !bytes.Equal(saved, work.pages[0]) {
		t.Fatal("completed control reuse invalidated retained output")
	}
	t.Logf("canonical reservation pages=%d Q32 calls=%d pending=%d", len(work.pages), calls+1, decoded.pendingMetadataCount())
}

func TestCanonicalCandidateOutputRetainsClaimAndFirstErrorV1(t *testing.T) {
	makeTxn := func() (*FreelistTxn, uint64, uint64, []ReservationExtentV1, *recordingSink) {
		base := MustNewFreelistGenerationV1(9, 1000, []uint64{2, 258, 514}, nil)
		txn, err := BeginCandidateV1(base, base.GenerationRef(), NewReservationLedger())
		if err != nil {
			t.Fatal(err)
		}
		txn.consumed = true
		count := countUnmaterializedStatePages(txn.root, 0)
		extents, err := txn.reservationExtents()
		if err != nil {
			t.Fatal(err)
		}
		id := candidateIDFromString("candidate-output")
		start, reserved, err := txn.ledger.reserveTail(id, txn.highWater, count, nil, extents)
		if err != nil {
			t.Fatal(err)
		}
		return txn, start, reserved, extents, &recordingSink{sink: NewCandidatePageSinkV1(), ledger: txn.ledger, candidateID: id}
	}
	txn, start, reserved, extents, sink := makeTxn()
	var work candidateOutputWorkV1
	count := countUnmaterializedStatePages(txn.root, 0)
	var candidate *FreelistCandidateV1
	calls := 0
	for ; calls < 256; calls++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		var ready bool
		var err error
		candidate, ready, err = work.step(txn, 10, 11, start, reserved, count, extents, sink, &w)
		if err != nil {
			t.Fatalf("phase=%d: %v", work.phase, err)
		}
		if w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("actual candidate work exceeded allowance")
		}
		for _, p := range sink.pages {
			if !page.VerifyChecksumNonMutating(p.Data) {
				t.Fatal("sink retained unfinished output")
			}
		}
		if ready {
			break
		}
	}
	if calls == 256 || candidate == nil || len(candidate.pages) != int(reserved) || txn.ledger.candidates[sink.candidateID] == nil {
		t.Fatalf("post-claim constructor stalled calls=%d phase=%d", calls, work.phase)
	}
	store := NewMemoryPageStoreV1()
	for _, p := range candidate.pages {
		store.Pages[p.PageID] = p.Data
	}
	decoded, err := LoadGenerationV1(store, candidate.Generation().GenerationRef())
	if err != nil || decoded.FreeCount() != 3 {
		t.Fatal("candidate decoder lost retained free pages", err)
	}
	record, err := loadReservationRecord(store, work.reservationID, candidate.generation.highWater)
	if err != nil || len(record.Extents) != 1 || record.Extents[0] != (ReservationExtentV1{StartPageID: start, Count: uint32(reserved), Kind: ReservationTargetMetadata}) {
		t.Fatal("candidate target reservation differs", err)
	}
	// The exact claimed candidate remains the owner after a sink error, and
	// retry neither assigns new IDs nor replays the original first write.
	failed, start, reserved, extents, failedSink := makeTxn()
	failedSink.sink.(*CandidatePageSinkV1).pageIDs[start] = struct{}{}
	var bad candidateOutputWorkV1
	var first error
	for i := 0; i < 128; i++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		_, _, first = bad.step(failed, 10, 11, start, reserved, count, extents, failedSink, &w)
		if first != nil {
			break
		}
	}
	if first == nil || !errors.Is(first, ErrGenerationFormat) || len(failedSink.pages) != 0 || !failed.ledger.candidates[failedSink.candidateID].tailWriteAttempted {
		t.Fatal("sink failure lost original claim/write custody", first)
	}
	for i := 0; i < 2; i++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		_, ready, err := bad.step(failed, 10, 11, start, reserved, count, extents, failedSink, &w)
		if ready || err != first || w.Records != 0 || len(failedSink.pages) != 0 {
			t.Fatal("sink error replayed or lost its exact receipt")
		}
	}
	t.Logf("post-claim Q32 constructor calls=%d pages=%d claim retained", calls+1, len(candidate.pages))
}

func canonicalReservationReference353V1(id uint64, record ReservationRecordV1) ([][]byte, ReservationRecordV1, error) {
	extents := record.Extents
	pageCount := (len(extents) + reservationEntriesPerPage - 1) / reservationEntriesPerPage
	if pageCount == 0 {
		pageCount = 1
	}
	if pageCount > int(^uint16(0)) || id > ^uint64(0)-uint64(pageCount) {
		return nil, ReservationRecordV1{}, fmt.Errorf("%w: reservation record page count %d", ErrGenerationFormat, pageCount)
	}
	record.pageID = id
	record.pageIDs = make([]uint64, pageCount)
	pages := make([][]byte, pageCount)
	for pageIndex := range pages {
		pageID := id + uint64(pageIndex)
		record.pageIDs[pageIndex] = pageID
		start := pageIndex * reservationEntriesPerPage
		end := min(start+reservationEntriesPerPage, len(extents))
		entries := extents[start:end]
		b := make([]byte, page.PageSize)
		encodePageHeader(b, pageID, page.PageTypeFreelistReservation, uint16(len(entries)))
		copy(b[16:24], reservationMagic[:])
		binary.LittleEndian.PutUint16(b[24:26], 1)
		binary.LittleEndian.PutUint16(b[26:28], reservationHeaderSize)
		binary.LittleEndian.PutUint16(b[28:30], reservationEntrySize)
		copy(b[32:48], record.CandidateID[:])
		binary.LittleEndian.PutUint64(b[48:56], record.GenerationID)
		binary.LittleEndian.PutUint64(b[56:64], record.BaseID)
		if pageIndex+1 < pageCount {
			binary.LittleEndian.PutUint64(b[64:72], pageID+1)
		}
		binary.LittleEndian.PutUint32(b[72:76], uint32(len(entries)))
		binary.LittleEndian.PutUint16(b[76:78], 1)
		binary.LittleEndian.PutUint16(b[78:80], uint16(pageCount))
		copy(b[80:112], record.BaseDigest[:])
		binary.LittleEndian.PutUint32(b[144:148], uint32(pageIndex))
		binary.LittleEndian.PutUint32(b[148:152], uint32(len(extents)))
		o := reservationHeaderSize
		for _, extent := range entries {
			binary.LittleEndian.PutUint64(b[o:o+8], extent.StartPageID)
			binary.LittleEndian.PutUint32(b[o+8:o+12], extent.Count)
			b[o+12] = byte(extent.Kind)
			binary.LittleEndian.PutUint64(b[o+16:o+24], extent.LastReachableCommitSeq)
			o += reservationEntrySize
		}
		pages[pageIndex] = b
	}
	record.digest = reservationDigest(pages)
	for _, b := range pages {
		copy(b[112:144], record.digest[:])
		finishPage(b)
	}
	return pages, record, nil
}

// The range selector must preserve the original per-page ownership oracle,
// including overlapping claims, without visiting every page of an interval.
func TestCanonicalTailConflictRangeMatchesOwnedPagesV1(t *testing.T) {
	ledger := NewReservationLedger()
	owner := candidateIDFromString("range-owner")
	ledger.owners[5] = owner
	ledger.burnedTails = []reservationInterval{{start: 9, count: 5}}
	ledger.candidates[owner] = &reservation{state: CandidatePreVisible, tailReserved: true, tailStart: 12, tailCount: 7}
	for _, query := range [][2]uint64{{2, 3}, {2, 4}, {6, 4}, {10, 10}, {18, 2}, {19, 4}} {
		start, count := query[0], query[1]
		var want uint64
		found := false
		for id := start; id < start+count; id++ {
			if ledger.reservedLocked(id) {
				want, found = id, true
				break
			}
		}
		got, after, actual, valid := ledger.firstConflictingRangeLocked(start, count)
		if !valid || actual != found || found && (got != want || after <= got) {
			t.Fatalf("query %v got=(%d,%d,%t,%t) want=(%d,%t)", query, got, after, actual, valid, want, found)
		}
	}
	// A single existing tail may cover far more pages than any output vector.
	// Placement skips that exact interval and records only the resulting range.
	huge := NewReservationLedger()
	huge.candidates[owner] = &reservation{state: CandidatePreVisible, tailReserved: true, tailStart: 100, tailCount: 1 << 32}
	start, _, err := huge.reserveTail(candidateIDFromString("range-new"), 100, 1, nil, nil)
	if err != nil || start != 100+1<<32 {
		t.Fatalf("large owned interval placement start=%d err=%v", start, err)
	}
	if _, _, _, valid := huge.firstConflictingRangeLocked(^uint64(0)-1, 2); valid {
		t.Fatal("overflow range accepted")
	}
}
