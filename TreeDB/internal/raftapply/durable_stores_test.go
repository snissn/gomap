package raftapply

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestDurableApplyStoresCloseReopenPreservesApplyProgressIdempotencyAndResult(t *testing.T) {
	root := t.TempDir()
	dbDir := filepath.Join(root, "db")
	storeDir := filepath.Join(root, "raftapply")
	db := openApplyHarnessDB(t, dbDir)
	progress, results := openDurableApplyStoresForTest(t, storeDir, DurableApplyStoreOptions{})

	raw := deterministicCreateCollectionEntry(t, "users", "durable:create:users", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	assertDurableLastApplied(t, progress, raftentry.ApplyEntryID{Term: 1, Index: 1})
	record, ok, err := results.LookupApplyResultByIdempotencyKey([]byte("durable:create:users"))
	if err != nil || !ok {
		t.Fatalf("LookupApplyResultByIdempotencyKey before reopen=(%+v,%t,%v), want record", record, ok, err)
	}
	if record.Result != result || record.AppliedCommandLSN == 0 {
		t.Fatalf("stored result before reopen=%+v lsn=%d, want %+v with coverage", record.Result, record.AppliedCommandLSN, result)
	}

	closeDurableApplyStoresForTest(t, progress, results)
	if err := db.Close(); err != nil {
		t.Fatalf("Close DB: %v", err)
	}

	reopenedDB := openApplyHarnessDB(t, dbDir)
	defer func() { _ = reopenedDB.Close() }()
	reopenedProgress, reopenedResults := openDurableApplyStoresForTest(t, storeDir, DurableApplyStoreOptions{})
	defer closeDurableApplyStoresForTest(t, reopenedProgress, reopenedResults)
	assertDurableLastApplied(t, reopenedProgress, raftentry.ApplyEntryID{Term: 1, Index: 1})
	reopenedRecord, ok, err := reopenedResults.LookupApplyResult(record.EntryID)
	if err != nil || !ok {
		t.Fatalf("LookupApplyResult after reopen=(%+v,%t,%v), want record", reopenedRecord, ok, err)
	}
	if reopenedRecord.Result != result || reopenedRecord.AppliedCommandLSN != record.AppliedCommandLSN {
		t.Fatalf("reopened record=%+v, want result %+v lsn %d", reopenedRecord, result, record.AppliedCommandLSN)
	}

	replayed, err := ApplyCommittedEntryV1(reopenedDB, raw, applyMeta(1, 1), Options{
		ProgressStore: reopenedProgress,
		ResultStore:   reopenedResults,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 replay after reopen: %v result=%+v", err, replayed)
	}
	if replayed != result {
		t.Fatalf("replayed result=%+v, want stored %+v", replayed, result)
	}
}

func TestDurableApplyStoresIdempotencyDuplicateSameDigestAndDifferentDigest(t *testing.T) {
	root := t.TempDir()
	db := openApplyHarnessDB(t, filepath.Join(root, "db"))
	defer func() { _ = db.Close() }()
	progress, results := openDurableApplyStoresForTest(t, filepath.Join(root, "raftapply"), DurableApplyStoreOptions{})
	defer closeDurableApplyStoresForTest(t, progress, results)

	raw := deterministicCreateCollectionEntry(t, "users", "durable:duplicate", testCreateCollectionMetaOptions{})
	first, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 first: %v result=%+v", err, first)
	}
	assertApplied(t, first, raftentry.ApplyStatusApplied, 1)

	duplicate, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 2), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1 duplicate: %v result=%+v", err, duplicate)
	}
	if duplicate.Status != raftentry.ApplyStatusAlreadyApplied || duplicate.CommandDigest != first.CommandDigest || duplicate.AffectedCount != 0 || duplicate.MatchedCount != 0 {
		t.Fatalf("duplicate result=%+v, want already-applied replay of digest %s", duplicate, first.CommandDigest.Hex())
	}
	record, ok, err := results.LookupApplyResult(raftentry.ApplyEntryID{Term: 1, Index: 2})
	if err != nil || !ok {
		t.Fatalf("Lookup duplicate result=(%+v,%t,%v), want durable record", record, ok, err)
	}
	if record.Result.Status != raftentry.ApplyStatusAlreadyApplied || record.Result.ResultDigest != first.ResultDigest {
		t.Fatalf("durable duplicate record result=%+v, want already-applied with original logical digest", record.Result)
	}
	assertDurableLastApplied(t, progress, raftentry.ApplyEntryID{Term: 1, Index: 2})

	conflictingRaw := deterministicCreateCollectionEntry(t, "orders", "durable:duplicate", testCreateCollectionMetaOptions{})
	rejected, err := ApplyCommittedEntryV1(db, conflictingRaw, applyMeta(1, 3), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	assertRejected(t, rejected, err, raftentry.ApplyStatusRejectedConflict, raftentry.ErrorRejectedConflictV1)
}

func TestDecodeDurableApplyResultRecordV1ToleratesLegacyMissingMatchedCount(t *testing.T) {
	record := testDurableApplyResultRecord(1, 1, "durable:legacy-result")
	payload, err := encodeDurableApplyResultRecordV1(record)
	if err != nil {
		t.Fatalf("encodeDurableApplyResultRecordV1: %v", err)
	}
	if len(payload) < 72 {
		t.Fatalf("encoded payload length=%d, want at least matched count plus result/progress digests", len(payload))
	}
	legacy := append([]byte(nil), payload[:len(payload)-72]...)
	legacy = append(legacy, payload[len(payload)-64:]...)

	decoded, err := decodeDurableApplyResultRecordV1(legacy)
	if err != nil {
		t.Fatalf("decodeDurableApplyResultRecordV1 legacy: %v", err)
	}
	want := record
	want.Result.MatchedCount = 0
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded legacy record=%+v want %+v", decoded, want)
	}
}

func TestDurableApplyResultStorePreflightRejectsEncodedRecordOverLimit(t *testing.T) {
	results, err := OpenDurableApplyResultStore(t.TempDir(), DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("OpenDurableApplyResultStore: %v", err)
	}
	defer func() { _ = results.Close() }()

	record := testDurableApplyResultRecord(1, 1, "durable:oversized")
	base := applyResultRecordSizeV1(record) - len(record.Result.Status)
	if base >= raftentry.MaxResultRecordBytesV1 {
		t.Fatalf("base result record size=%d unexpectedly exceeds max %d", base, raftentry.MaxResultRecordBytesV1)
	}
	record.Result.Status = raftentry.ApplyStatusV1(strings.Repeat("s", raftentry.MaxResultRecordBytesV1-base+1))
	if oldSize := applyResultRecordSizeV1(record) - int64SizeV1; oldSize > raftentry.MaxResultRecordBytesV1 {
		t.Fatalf("test record would not cover missed matched_count: old estimated size=%d max=%d", oldSize, raftentry.MaxResultRecordBytesV1)
	}
	if err := results.CheckCanRecordApplyResult(record); codeOf(err) != raftentry.ErrorResourceExhaustedV1 {
		t.Fatalf("CheckCanRecordApplyResult oversized error=%v code=%s, want resource exhausted", err, codeOf(err))
	}
	if _, ok, err := results.LookupApplyResult(record.EntryID); err != nil || ok {
		t.Fatalf("LookupApplyResult after oversized preflight=(ok=%t, err=%v), want absent without store error", ok, err)
	}
}

func TestDecodeDurableApplyProgressRecordV1ToleratesLegacyMissingLogicalDigest(t *testing.T) {
	record := ApplyProgressRecordV1{
		EntryID:           raftentry.ApplyEntryID{Term: 1, Index: 1},
		CommandDigest:     testDurableDigest(1),
		AppliedCommandLSN: 7,
		LogicalDigestV1:   LogicalDigestV1(testDurableDigest(101)),
	}
	payload, err := encodeDurableApplyProgressRecordV1(record)
	if err != nil {
		t.Fatalf("encodeDurableApplyProgressRecordV1: %v", err)
	}
	if len(payload) < 32 {
		t.Fatalf("encoded payload length=%d, want logical digest tail", len(payload))
	}
	legacy := append([]byte(nil), payload[:len(payload)-32]...)

	decoded, err := decodeDurableApplyProgressRecordV1(legacy)
	if err != nil {
		t.Fatalf("decodeDurableApplyProgressRecordV1 legacy: %v", err)
	}
	want := record
	want.LogicalDigestV1 = LogicalDigestV1{}
	if decoded != want {
		t.Fatalf("decoded legacy progress=%+v want %+v", decoded, want)
	}
}

func TestDurableApplyResultStorePreservesProgressLogicalDigest(t *testing.T) {
	dir := t.TempDir()
	results, err := OpenDurableApplyResultStore(dir, DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("OpenDurableApplyResultStore: %v", err)
	}
	record := testDurableApplyResultRecord(9, 1, "durable:progress-digest")
	record.ProgressLogicalDigestV1 = LogicalDigestV1(testDurableDigest(200))
	if err := results.RecordApplyResult(record); err != nil {
		t.Fatalf("RecordApplyResult: %v", err)
	}
	if err := results.Close(); err != nil {
		t.Fatalf("Close results: %v", err)
	}

	reopened, err := OpenDurableApplyResultStore(dir, DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("Reopen DurableApplyResultStore: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	got, ok, err := reopened.LookupApplyResult(record.EntryID)
	if err != nil || !ok {
		t.Fatalf("LookupApplyResult after reopen=(%+v,%t,%v), want record", got, ok, err)
	}
	if got.ProgressLogicalDigestV1 != record.ProgressLogicalDigestV1 {
		t.Fatalf("progress logical digest after reopen=%s, want %s", got.ProgressLogicalDigestV1.Hex(), record.ProgressLogicalDigestV1.Hex())
	}
	if got.Result.ResultDigest != record.Result.ResultDigest {
		t.Fatalf("result digest after reopen=%s, want %s", got.Result.ResultDigest.Hex(), record.Result.ResultDigest.Hex())
	}
}

func TestDurableApplyProgressStoreAllowsGapAndRejectsLowerIndex(t *testing.T) {
	dir := t.TempDir()
	progress, err := OpenDurableApplyProgressStore(dir, DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("OpenDurableApplyProgressStore: %v", err)
	}
	digest := testDurableDigest(1)
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: raftentry.ApplyEntryID{Term: 2, Index: 1}, CommandDigest: digest, AppliedCommandLSN: 1}); err != nil {
		t.Fatalf("RecordApplied index 1: %v", err)
	}
	if err := progress.CheckCanApply(raftentry.ApplyEntryID{Term: 2, Index: 3}); err != nil {
		t.Fatalf("gap CheckCanApply: %v", err)
	}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: raftentry.ApplyEntryID{Term: 2, Index: 3}, CommandDigest: testDurableDigest(3), AppliedCommandLSN: 2}); err != nil {
		t.Fatalf("RecordApplied index 3: %v", err)
	}
	if err := progress.CheckCanApply(raftentry.ApplyEntryID{Term: 2, Index: 1}); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("lower CheckCanApply error=%v code=%s, want rejected conflict", err, codeOf(err))
	}
	if err := progress.CheckCanApply(raftentry.ApplyEntryID{Term: 1, Index: 4}); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("lower-term CheckCanApply error=%v code=%s, want rejected conflict", err, codeOf(err))
	}
	if err := progress.Close(); err != nil {
		t.Fatalf("Close progress: %v", err)
	}

	reopened, err := OpenDurableApplyProgressStore(dir, DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("Reopen durable progress with raft index gap: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	assertDurableLastApplied(t, reopened, raftentry.ApplyEntryID{Term: 2, Index: 3})
}

func TestDurableApplyStoresFailClosedOnTruncatedAndCorruptMetadata(t *testing.T) {
	progressDir := filepath.Join(t.TempDir(), "progress")
	progress, err := OpenDurableApplyProgressStore(progressDir, DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("OpenDurableApplyProgressStore: %v", err)
	}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: raftentry.ApplyEntryID{Term: 1, Index: 1}, CommandDigest: testDurableDigest(1), AppliedCommandLSN: 1}); err != nil {
		t.Fatalf("RecordApplied: %v", err)
	}
	if err := progress.Close(); err != nil {
		t.Fatalf("Close progress: %v", err)
	}
	progressPath := DurableApplyProgressStorePath(progressDir)
	info, err := os.Stat(progressPath)
	if err != nil {
		t.Fatalf("Stat progress metadata: %v", err)
	}
	if err := os.Truncate(progressPath, info.Size()-1); err != nil {
		t.Fatalf("Truncate progress metadata: %v", err)
	}
	if _, err := OpenDurableApplyProgressStore(progressDir, DurableApplyStoreOptions{DisableSync: true}); codeOf(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("Open truncated progress error=%v code=%s, want unsafe durability", err, codeOf(err))
	}

	resultsDir := filepath.Join(t.TempDir(), "results")
	results, err := OpenDurableApplyResultStore(resultsDir, DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("OpenDurableApplyResultStore: %v", err)
	}
	if err := results.RecordApplyResult(testDurableApplyResultRecord(1, 1, "durable:corrupt")); err != nil {
		t.Fatalf("RecordApplyResult: %v", err)
	}
	if err := results.Close(); err != nil {
		t.Fatalf("Close results: %v", err)
	}
	resultsPath := DurableApplyResultStorePath(resultsDir)
	data, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatalf("Read result metadata: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(resultsPath, data, 0o600); err != nil {
		t.Fatalf("Write corrupt result metadata: %v", err)
	}
	if _, err := OpenDurableApplyResultStore(resultsDir, DurableApplyStoreOptions{DisableSync: true}); codeOf(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("Open corrupt result error=%v code=%s, want unsafe durability", err, codeOf(err))
	}
}

func TestDurableApplyStoresFailClosedOnExistingZeroLengthMetadata(t *testing.T) {
	tests := []struct {
		name string
		path func(string) string
		open func(string) error
	}{
		{
			name: "progress",
			path: DurableApplyProgressStorePath,
			open: func(dir string) error {
				store, err := OpenDurableApplyProgressStore(dir, DurableApplyStoreOptions{DisableSync: true})
				if store != nil {
					_ = store.Close()
				}
				return err
			},
		},
		{
			name: "results",
			path: DurableApplyResultStorePath,
			open: func(dir string) error {
				store, err := OpenDurableApplyResultStore(dir, DurableApplyStoreOptions{DisableSync: true})
				if store != nil {
					_ = store.Close()
				}
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := tc.path(dir)
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatalf("Write zero-length metadata: %v", err)
			}
			if err := tc.open(dir); codeOf(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
				t.Fatalf("Open zero-length metadata error=%v code=%s, want unsafe durability", err, codeOf(err))
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("Stat zero-length metadata: %v", err)
			}
			if info.Size() != 0 {
				t.Fatalf("zero-length metadata was rewritten to %d bytes", info.Size())
			}
		})
	}
}

func TestDurableApplyStoresPoisonAfterAppendFailure(t *testing.T) {
	results, err := OpenDurableApplyResultStore(t.TempDir(), DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("OpenDurableApplyResultStore: %v", err)
	}
	if err := results.file.Close(); err != nil {
		t.Fatalf("close underlying result file: %v", err)
	}
	if err := results.RecordApplyResult(testDurableApplyResultRecord(1, 1, "durable:append-fail")); codeOf(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("RecordApplyResult append failure error=%v code=%s, want unsafe durability", err, codeOf(err))
	}
	if err := results.RecordApplyResult(testDurableApplyResultRecord(2, 2, "durable:after-poison")); codeOf(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("RecordApplyResult after poison error=%v code=%s, want unsafe durability", err, codeOf(err))
	}
	if _, _, err := results.LookupApplyResult(raftentry.ApplyEntryID{Term: 1, Index: 1}); codeOf(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("LookupApplyResult after poison error=%v code=%s, want unsafe durability", err, codeOf(err))
	}

	progress, err := OpenDurableApplyProgressStore(t.TempDir(), DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("OpenDurableApplyProgressStore: %v", err)
	}
	if err := progress.file.Close(); err != nil {
		t.Fatalf("close underlying progress file: %v", err)
	}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: raftentry.ApplyEntryID{Term: 1, Index: 1}, CommandDigest: testDurableDigest(1), AppliedCommandLSN: 1}); codeOf(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("RecordApplied append failure error=%v code=%s, want unsafe durability", err, codeOf(err))
	}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: raftentry.ApplyEntryID{Term: 1, Index: 2}, CommandDigest: testDurableDigest(2), AppliedCommandLSN: 2}); codeOf(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("RecordApplied after poison error=%v code=%s, want unsafe durability", err, codeOf(err))
	}
	if err := progress.CheckCanApply(raftentry.ApplyEntryID{Term: 1, Index: 2}); codeOf(err) != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("CheckCanApply after poison error=%v code=%s, want unsafe durability", err, codeOf(err))
	}
}

func TestDurableApplyResultStoreRejectsUnsupportedVersion(t *testing.T) {
	dir := t.TempDir()
	path := DurableApplyResultStorePath(dir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	header := durableApplyFileHeader(durableApplyFrameKindResult)
	binary.LittleEndian.PutUint32(header[8:12], durableApplyFileVersionV1+1)
	binary.LittleEndian.PutUint32(header[16:20], durableApplyHeaderChecksumForTest(header[:]))
	if err := os.WriteFile(path, header[:], 0o600); err != nil {
		t.Fatalf("Write unsupported-version header: %v", err)
	}
	if _, err := OpenDurableApplyResultStore(dir, DurableApplyStoreOptions{DisableSync: true}); codeOf(err) != raftentry.ErrorUnsupportedVersionV1 {
		t.Fatalf("Open unsupported version error=%v code=%s, want unsupported version", err, codeOf(err))
	}
}

func TestDurableApplyStoresLogicalDigestIndependentOfMetadataFiles(t *testing.T) {
	root := t.TempDir()
	db := openApplyHarnessDB(t, filepath.Join(root, "db"))
	defer func() { _ = db.Close() }()
	progress, results := openDurableApplyStoresForTest(t, filepath.Join(root, "db", "raftapply"), DurableApplyStoreOptions{DisableSync: true})
	defer closeDurableApplyStoresForTest(t, progress, results)

	raw := deterministicCreateCollectionEntry(t, "users", "durable:logical", testCreateCollectionMetaOptions{})
	result, err := ApplyCommittedEntryV1(db, raw, applyMeta(1, 1), Options{
		ProgressStore: progress,
		ResultStore:   results,
	})
	if err != nil {
		t.Fatalf("ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	before, err := LogicalDigestV1ForDB(db, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB before metadata-only records: %v", err)
	}
	if result.ResultDigest != raftentry.CommandDigestV1(before) {
		t.Fatalf("apply result logical digest=%x, want current logical digest %x", result.ResultDigest, before)
	}

	if err := results.RecordApplyResult(testDurableApplyResultRecord(7, 2, "durable:metadata-only")); err != nil {
		t.Fatalf("RecordApplyResult metadata-only: %v", err)
	}
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: raftentry.ApplyEntryID{Term: 1, Index: 2}, CommandDigest: testDurableDigest(7), AppliedCommandLSN: 2}); err != nil {
		t.Fatalf("RecordApplied metadata-only: %v", err)
	}
	after, err := LogicalDigestV1ForDB(db, LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("LogicalDigestV1ForDB after metadata-only records: %v", err)
	}
	if after != before {
		t.Fatalf("logical digest changed after raftapply metadata-only writes: before=%x after=%x", before, after)
	}
}

func BenchmarkDurableApplyStoresRecordLookup(b *testing.B) {
	b.Run("record_progress_relaxed_sync", func(b *testing.B) {
		progress, err := OpenDurableApplyProgressStore(b.TempDir(), DurableApplyStoreOptions{DisableSync: true})
		if err != nil {
			b.Fatalf("OpenDurableApplyProgressStore: %v", err)
		}
		defer func() { _ = progress.Close() }()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			index := uint64(i + 1)
			if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: raftentry.ApplyEntryID{Term: 1, Index: index}, CommandDigest: testDurableDigest(byte(i + 1)), AppliedCommandLSN: index}); err != nil {
				b.Fatalf("RecordApplied: %v", err)
			}
		}
	})
	b.Run("record_result_relaxed_sync", func(b *testing.B) {
		results, err := OpenDurableApplyResultStore(b.TempDir(), DurableApplyStoreOptions{DisableSync: true})
		if err != nil {
			b.Fatalf("OpenDurableApplyResultStore: %v", err)
		}
		defer func() { _ = results.Close() }()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			index := uint64(i + 1)
			var key [16]byte
			binary.LittleEndian.PutUint64(key[:8], index)
			if err := results.RecordApplyResult(testDurableApplyResultRecordBytes(byte(i+1), index, key[:])); err != nil {
				b.Fatalf("RecordApplyResult: %v", err)
			}
		}
	})
	b.Run("lookup_result_by_entry_id", func(b *testing.B) {
		results, err := OpenDurableApplyResultStore(b.TempDir(), DurableApplyStoreOptions{DisableSync: true})
		if err != nil {
			b.Fatalf("OpenDurableApplyResultStore: %v", err)
		}
		defer func() { _ = results.Close() }()
		if err := results.RecordApplyResult(testDurableApplyResultRecord(1, 1, "bench-lookup")); err != nil {
			b.Fatalf("RecordApplyResult: %v", err)
		}
		id := raftentry.ApplyEntryID{Term: 1, Index: 1}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok, err := results.LookupApplyResult(id); err != nil || !ok {
				b.Fatalf("LookupApplyResult ok=%t err=%v", ok, err)
			}
		}
	})
	b.Run("lookup_result_by_idempotency_key", func(b *testing.B) {
		results, err := OpenDurableApplyResultStore(b.TempDir(), DurableApplyStoreOptions{DisableSync: true})
		if err != nil {
			b.Fatalf("OpenDurableApplyResultStore: %v", err)
		}
		defer func() { _ = results.Close() }()
		key := "bench-lookup-key"
		if err := results.RecordApplyResult(testDurableApplyResultRecord(1, 1, key)); err != nil {
			b.Fatalf("RecordApplyResult: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok, err := results.LookupApplyResultByIdempotencyKey([]byte(key)); err != nil || !ok {
				b.Fatalf("LookupApplyResultByIdempotencyKey ok=%t err=%v", ok, err)
			}
		}
	})
}

func openDurableApplyStoresForTest(t testing.TB, dir string, opts DurableApplyStoreOptions) (*DurableApplyProgressStore, *DurableApplyResultStore) {
	t.Helper()
	progress, err := OpenDurableApplyProgressStore(dir, opts)
	if err != nil {
		t.Fatalf("OpenDurableApplyProgressStore: %v", err)
	}
	results, err := OpenDurableApplyResultStore(dir, opts)
	if err != nil {
		_ = progress.Close()
		t.Fatalf("OpenDurableApplyResultStore: %v", err)
	}
	return progress, results
}

func closeDurableApplyStoresForTest(t testing.TB, progress *DurableApplyProgressStore, results *DurableApplyResultStore) {
	t.Helper()
	if err := progress.Close(); err != nil {
		t.Fatalf("Close progress store: %v", err)
	}
	if err := results.Close(); err != nil {
		t.Fatalf("Close result store: %v", err)
	}
}

func assertDurableLastApplied(t *testing.T, progress *DurableApplyProgressStore, want raftentry.ApplyEntryID) {
	t.Helper()
	got, ok := progress.LastApplied()
	if !ok || got != want {
		t.Fatalf("LastApplied=(%+v,%t), want %+v", got, ok, want)
	}
}

func testDurableApplyResultRecord(seed byte, index uint64, key string) ApplyResultRecordV1 {
	return testDurableApplyResultRecordBytes(seed, index, []byte(key))
}

func testDurableApplyResultRecordBytes(seed byte, index uint64, key []byte) ApplyResultRecordV1 {
	digest := testDurableDigest(seed)
	return ApplyResultRecordV1{
		EntryID:                 raftentry.ApplyEntryID{Term: 1, Index: index},
		CommandDigest:           digest,
		IdempotencyKey:          append([]byte(nil), key...),
		AppliedCommandLSN:       index,
		ProgressLogicalDigestV1: LogicalDigestV1(testDurableDigest(seed + 100)),
		Result: raftentry.ApplyResultV1{
			Status:        raftentry.ApplyStatusApplied,
			CommandDigest: digest,
			AffectedCount: 1,
			MatchedCount:  2,
			ResultDigest:  testDurableDigest(seed + 100),
		},
	}
}

func testDurableDigest(seed byte) raftentry.CommandDigestV1 {
	var digest raftentry.CommandDigestV1
	for i := range digest {
		digest[i] = seed + byte(i)
	}
	return digest
}

func durableApplyHeaderChecksumForTest(header []byte) uint32 {
	return crc32ChecksumForTest(header[:16])
}

func crc32ChecksumForTest(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
