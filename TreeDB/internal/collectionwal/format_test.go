package collectionwal

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCollectionWALV1ExactByteFixtures(t *testing.T) {
	txn := testTransaction(11, 1)
	frame := mustFrame(t, txn)
	assertCollectionWALHexFixture(t, "transaction_frame.hex", frame)

	segment := append(EncodeSegmentHeader(SegmentHeader{Lane: 0, SegmentSeq: 1}), frame...)
	assertCollectionWALHexFixture(t, "segment_one_txn.hex", segment)
}

func TestCollectionWALV1FrameRoundTripAndScan(t *testing.T) {
	txn := testTransaction(11, 1)
	frame, err := EncodeTransactionFrame(txn)
	if err != nil {
		t.Fatalf("EncodeTransactionFrame: %v", err)
	}
	decoded, err := DecodeTransactionFrame(frame)
	if err != nil {
		t.Fatalf("DecodeTransactionFrame: %v", err)
	}
	if !reflect.DeepEqual(decoded, txn) {
		t.Fatalf("decoded transaction mismatch\ngot:  %+v\nwant: %+v", decoded, txn)
	}

	segment := append(EncodeSegmentHeader(SegmentHeader{Lane: 2, SegmentSeq: 9}), frame...)
	header, results, err := ScanSegment(segment, true)
	if err != nil {
		t.Fatalf("ScanSegment: %v", err)
	}
	if header.Lane != 2 || header.SegmentSeq != 9 {
		t.Fatalf("segment header = %+v", header)
	}
	if len(results) != 1 {
		t.Fatalf("scan results len=%d want 1", len(results))
	}
	if results[0].Outcome != OutcomeCompleteValid {
		t.Fatalf("scan outcome=%s err=%v", results[0].Outcome, results[0].Err)
	}
	if results[0].Header.WALLSN != txn.WALLSN || results[0].Header.CollectionSeq != txn.CollectionSeq {
		t.Fatalf("frame header = %+v", results[0].Header)
	}
	if results[0].Header.RequiredFeatureBitsLow != RequiredFeatureCollectionWALV1 {
		t.Fatalf("required feature bits=%08x", results[0].Header.RequiredFeatureBitsLow)
	}
}

func TestCollectionWALReplayDigestStableFields(t *testing.T) {
	base := testTransaction(11, 1)
	baseDigest, err := ReplayDigest(base)
	if err != nil {
		t.Fatalf("ReplayDigest(base): %v", err)
	}

	statsChanged := cloneTestTransaction(base)
	for i := range statsChanged.Sections {
		if statsChanged.Sections[i].Type == SectionTypeStats {
			statsChanged.Sections[i].Data = []byte("different stats are observability only")
		}
	}
	statsDigest, err := ReplayDigest(statsChanged)
	if err != nil {
		t.Fatalf("ReplayDigest(statsChanged): %v", err)
	}
	if baseDigest != statsDigest {
		t.Fatalf("stats section changed replay digest: %x vs %x", baseDigest, statsDigest)
	}

	deltaChanged := cloneTestTransaction(base)
	for i := range deltaChanged.Sections {
		if deltaChanged.Sections[i].Type == SectionTypeRootDeltaTable {
			deltaChanged.Sections[i].Data = []byte("mutated replay-critical root delta")
		}
	}
	deltaDigest, err := ReplayDigest(deltaChanged)
	if err != nil {
		t.Fatalf("ReplayDigest(deltaChanged): %v", err)
	}
	if baseDigest == deltaDigest {
		t.Fatalf("replay-critical root delta did not change replay digest: %x", baseDigest)
	}
}

func TestCollectionWALScanSegmentTailClassification(t *testing.T) {
	segment := testSegment(t, testTransaction(11, 1))
	truncated := segment[:len(segment)-3]

	_, results, err := ScanSegment(truncated, true)
	if err != nil {
		t.Fatalf("terminal ScanSegment returned top-level error: %v", err)
	}
	if len(results) != 1 || results[0].Outcome != OutcomeTerminalIncompleteTail {
		t.Fatalf("terminal results=%+v", results)
	}
	if !errors.Is(results[0].Err, ErrCollectionWALTerminalTail) {
		t.Fatalf("terminal err=%v want ErrCollectionWALTerminalTail", results[0].Err)
	}

	_, results, err = ScanSegment(truncated, false)
	if err != nil {
		t.Fatalf("nonterminal ScanSegment returned top-level error: %v", err)
	}
	if len(results) != 1 || results[0].Outcome != OutcomeNonTerminalShortRead {
		t.Fatalf("nonterminal results=%+v", results)
	}
	if !errors.Is(results[0].Err, ErrCollectionWALCorruptMiddle) {
		t.Fatalf("nonterminal err=%v want ErrCollectionWALCorruptMiddle", results[0].Err)
	}
}

func TestCollectionWALScanSegmentRejectsCorruptPayloadChecksum(t *testing.T) {
	segment := testSegment(t, testTransaction(11, 1))
	corrupt := append([]byte(nil), segment...)
	payloadDataOffset := firstSectionDataOffset(t, corrupt)
	corrupt[payloadDataOffset] ^= 0x80

	_, results, err := ScanSegment(corrupt, true)
	if err != nil {
		t.Fatalf("ScanSegment returned top-level error: %v", err)
	}
	if len(results) != 1 || results[0].Outcome != OutcomeCompleteCorrupt {
		t.Fatalf("results=%+v", results)
	}
	if !errors.Is(results[0].Err, ErrCollectionWALBadChecksum) {
		t.Fatalf("err=%v want ErrCollectionWALBadChecksum", results[0].Err)
	}
}

func TestCollectionWALMiddleCorruptionBlocksLaterSeq(t *testing.T) {
	segment := testSegment(t, testTransaction(11, 1), testTransaction(12, 2))
	corrupt := append([]byte(nil), segment...)
	payloadDataOffset := firstSectionDataOffset(t, corrupt)
	corrupt[payloadDataOffset] ^= 0x80

	_, results, err := ScanSegment(corrupt, true)
	if err != nil {
		t.Fatalf("ScanSegment returned top-level error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results len=%d want 1 so later seq is blocked: %+v", len(results), results)
	}
	if results[0].Outcome != OutcomeCompleteCorrupt {
		t.Fatalf("first outcome=%s err=%v want CompleteCorrupt", results[0].Outcome, results[0].Err)
	}
	if !errors.Is(results[0].Err, ErrCollectionWALBadChecksum) {
		t.Fatalf("first err=%v want ErrCollectionWALBadChecksum", results[0].Err)
	}
}

func TestCollectionWALScanSegmentRejectsOversizedFrameBeforeAllocation(t *testing.T) {
	segment := testSegment(t, testTransaction(11, 1))
	oversized := append([]byte(nil), segment...)
	header := oversized[SegmentHeaderLen : SegmentHeaderLen+FrameHeaderLen]
	binary.LittleEndian.PutUint64(header[56:64], MaxOuterFramePayloadBytes+1)
	binary.LittleEndian.PutUint64(header[64:72], MaxOuterFramePayloadBytes+1)
	binary.LittleEndian.PutUint32(header[72:76], crc32cWithZero(header, 72, 76))

	_, results, err := ScanSegment(oversized, true)
	if err != nil {
		t.Fatalf("ScanSegment returned top-level error: %v", err)
	}
	if len(results) != 1 || results[0].Outcome != OutcomeMaliciousLength {
		t.Fatalf("results=%+v", results)
	}
	if !errors.Is(results[0].Err, ErrCollectionWALResourceLimit) {
		t.Fatalf("err=%v want ErrCollectionWALResourceLimit", results[0].Err)
	}
}

func TestCollectionWALFrameLengthOverflowRejects(t *testing.T) {
	segment := testSegment(t, testTransaction(11, 1))
	oversized := append([]byte(nil), segment...)
	header := oversized[SegmentHeaderLen : SegmentHeaderLen+FrameHeaderLen]
	binary.LittleEndian.PutUint64(header[56:64], MaxOuterFramePayloadBytes+1)
	binary.LittleEndian.PutUint64(header[64:72], MaxOuterFramePayloadBytes+1)
	binary.LittleEndian.PutUint32(header[72:76], crc32cWithZero(header, 72, 76))

	_, results, err := ScanSegment(oversized, true)
	if err != nil {
		t.Fatalf("ScanSegment returned top-level error: %v", err)
	}
	if len(results) != 1 || results[0].Outcome != OutcomeMaliciousLength {
		t.Fatalf("results=%+v want one MaliciousLength", results)
	}
	if !errors.Is(results[0].Err, ErrCollectionWALResourceLimit) {
		t.Fatalf("err=%v want ErrCollectionWALResourceLimit", results[0].Err)
	}
}

func TestCollectionWALScanSegmentRejectsUnsupportedFrameVersion(t *testing.T) {
	segment := testSegment(t, testTransaction(11, 1))
	unsupported := append([]byte(nil), segment...)
	header := unsupported[SegmentHeaderLen : SegmentHeaderLen+FrameHeaderLen]
	binary.LittleEndian.PutUint16(header[10:12], 2)

	_, results, err := ScanSegment(unsupported, true)
	if err != nil {
		t.Fatalf("ScanSegment returned top-level error: %v", err)
	}
	if len(results) != 1 || results[0].Outcome != OutcomeUnsupportedVersion {
		t.Fatalf("results=%+v", results)
	}
	if !errors.Is(results[0].Err, ErrCollectionWALUnsupportedVersion) {
		t.Fatalf("err=%v want ErrCollectionWALUnsupportedVersion", results[0].Err)
	}
}

func TestCollectionWALSequenceGuards(t *testing.T) {
	txn := testTransaction(11, 3)
	txn.DependsOnCollectionSeq = 1
	_, err := EncodeTransactionFrame(txn)
	if !errors.Is(err, ErrCollectionWALSequenceGap) {
		t.Fatalf("EncodeTransactionFrame gap err=%v want ErrCollectionWALSequenceGap", err)
	}
}

func TestCollectionWALScanSegmentDetectsDuplicateSequence(t *testing.T) {
	first := testTransaction(11, 1)
	second := testTransaction(12, 1)
	segment := append(EncodeSegmentHeader(SegmentHeader{Lane: 0, SegmentSeq: 1}), mustFrame(t, first)...)
	segment = append(segment, mustFrame(t, second)...)

	_, results, err := ScanSegment(segment, true)
	if err != nil {
		t.Fatalf("ScanSegment returned top-level error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results len=%d want 2: %+v", len(results), results)
	}
	if results[0].Outcome != OutcomeCompleteValid {
		t.Fatalf("first outcome=%s err=%v", results[0].Outcome, results[0].Err)
	}
	if results[1].Outcome != OutcomeDuplicateCollectionSeq {
		t.Fatalf("second outcome=%s err=%v", results[1].Outcome, results[1].Err)
	}
	if !errors.Is(results[1].Err, ErrCollectionWALSequenceGap) {
		t.Fatalf("second err=%v want ErrCollectionWALSequenceGap", results[1].Err)
	}
}

func TestCollectionWALUnknownCriticalSectionFailsClosed(t *testing.T) {
	txn := testTransaction(11, 1)
	txn.Sections = append(txn.Sections, Section{
		Type:    SectionType(99),
		Version: SectionTableVersionV1,
		Flags:   SectionFlagCritical,
		Data:    []byte("future critical data"),
	})
	_, err := EncodeTransactionFrame(txn)
	if !errors.Is(err, ErrCollectionWALUnsupportedVersion) {
		t.Fatalf("EncodeTransactionFrame err=%v want ErrCollectionWALUnsupportedVersion", err)
	}
}

func TestCollectionWALTransactionRequiresFeatureBit(t *testing.T) {
	segment := testSegment(t, testTransaction(11, 1))
	missingFeature := append([]byte(nil), segment...)
	header := missingFeature[SegmentHeaderLen : SegmentHeaderLen+FrameHeaderLen]
	binary.LittleEndian.PutUint32(header[84:88], 0)
	binary.LittleEndian.PutUint32(header[72:76], crc32cWithZero(header, 72, 76))
	updateFrameTrailerCRC(missingFeature)

	_, results, err := ScanSegment(missingFeature, true)
	if err != nil {
		t.Fatalf("ScanSegment returned top-level error: %v", err)
	}
	if len(results) != 1 || results[0].Outcome != OutcomeUnsupportedVersion {
		t.Fatalf("results=%+v", results)
	}
	if !errors.Is(results[0].Err, ErrCollectionWALUnsupportedVersion) {
		t.Fatalf("err=%v want ErrCollectionWALUnsupportedVersion", results[0].Err)
	}
}

func testSegment(t *testing.T, txns ...Transaction) []byte {
	t.Helper()
	segment := EncodeSegmentHeader(SegmentHeader{Lane: 0, SegmentSeq: 1})
	for _, txn := range txns {
		segment = append(segment, mustFrame(t, txn)...)
	}
	return segment
}

func mustFrame(t *testing.T, txn Transaction) []byte {
	t.Helper()
	frame, err := EncodeTransactionFrame(txn)
	if err != nil {
		t.Fatalf("EncodeTransactionFrame: %v", err)
	}
	return frame
}

func testTransaction(wallsn, collectionSeq uint64) Transaction {
	var uid [CollectionUIDBytes]byte
	for i := range uid {
		uid[i] = byte(i + 1)
	}
	return Transaction{
		WALLSN:                   wallsn,
		CollectionUID:            uid,
		CollectionGeneration:     7,
		CollectionSeq:            collectionSeq,
		DependsOnCollectionSeq:   collectionSeq - 1,
		CatalogEpoch:             101,
		SchemaEpoch:              202,
		SchemaVersion:            303,
		BaseCommitSeq:            404,
		BaseSystemRootID:         505,
		BaseCatalogDigest:        testDigest(1),
		CatalogDigest:            testDigest(2),
		LogicalCatalogDigest:     testDigest(3),
		LocalReplayCatalogDigest: testDigest(4),
		MutationClass:            1,
		RootDeltaCount:           1,
		DescriptorOpCount:        1,
		Sections: []Section{
			{Type: SectionTypeRootDeltaTable, Version: SectionTableVersionV1, Flags: SectionFlagCritical | SectionFlagReplayCritical, Data: []byte("root-delta")},
			{Type: SectionTypeSideRefTable, Version: SectionTableVersionV1, Flags: SectionFlagCritical | SectionFlagReplayCritical, Data: []byte("side-refs-empty")},
			{Type: SectionTypeSystemDeltaTemplate, Version: SectionTableVersionV1, Flags: SectionFlagCritical | SectionFlagReplayCritical, Data: []byte("system-delta-template")},
			{Type: SectionTypeDescriptorOps, Version: SectionTableVersionV1, Flags: SectionFlagCritical | SectionFlagReplayCritical, Data: []byte("descriptor-ops")},
			{Type: SectionTypeStats, Version: SectionTableVersionV1, Data: []byte("stats")},
		},
	}
}

func testDigest(seed byte) [32]byte {
	var digest [32]byte
	for i := range digest {
		digest[i] = seed + byte(i)
	}
	return digest
}

func cloneTestTransaction(txn Transaction) Transaction {
	clone := txn
	clone.Sections = make([]Section, len(txn.Sections))
	for i, section := range txn.Sections {
		clone.Sections[i] = section
		clone.Sections[i].Data = append([]byte(nil), section.Data...)
	}
	return clone
}

func firstSectionDataOffset(t *testing.T, segment []byte) int {
	t.Helper()
	payloadStart := SegmentHeaderLen + FrameHeaderLen
	payload := segment[payloadStart:]
	sectionCount := binary.LittleEndian.Uint32(payload[240:244])
	if sectionCount == 0 {
		t.Fatal("test transaction has no sections")
	}
	return payloadStart + int(binary.LittleEndian.Uint64(payload[TransactionFixedHeaderLen+8:TransactionFixedHeaderLen+16]))
}

func updateFrameTrailerCRC(segment []byte) {
	header := segment[SegmentHeaderLen : SegmentHeaderLen+FrameHeaderLen]
	storedLen := binary.LittleEndian.Uint64(header[56:64])
	payloadStart := SegmentHeaderLen + FrameHeaderLen
	trailerStart := payloadStart + int(storedLen)
	trailer := segment[trailerStart : trailerStart+CommitTrailerLen]
	crc := crc32c(segment[SegmentHeaderLen:trailerStart])
	crc = crc32.Update(crc, collectionWALCRC32C, trailer[:12])
	binary.LittleEndian.PutUint32(trailer[12:16], crc)
}

func assertCollectionWALHexFixture(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", "v1", name)
	wantBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	want := strings.TrimSpace(string(wantBytes))
	if gotHex := hex.EncodeToString(got); gotHex != want {
		t.Fatalf("%s hex mismatch\ngot  %s\nwant %s", name, gotHex, want)
	}
}
