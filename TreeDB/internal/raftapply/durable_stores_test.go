package raftapply

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
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
	if duplicate.Status != raftentry.ApplyStatusAlreadyApplied || duplicate.CommandDigest != first.CommandDigest || duplicate.AffectedCount != 0 {
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

func TestDurableApplyProgressStoreRejectsGapAndLowerIndex(t *testing.T) {
	dir := t.TempDir()
	progress, err := OpenDurableApplyProgressStore(dir, DurableApplyStoreOptions{DisableSync: true})
	if err != nil {
		t.Fatalf("OpenDurableApplyProgressStore: %v", err)
	}
	defer func() { _ = progress.Close() }()
	digest := testDurableDigest(1)
	if err := progress.RecordApplied(ApplyProgressRecordV1{EntryID: raftentry.ApplyEntryID{Term: 2, Index: 1}, CommandDigest: digest, AppliedCommandLSN: 1}); err != nil {
		t.Fatalf("RecordApplied index 1: %v", err)
	}
	if err := progress.CheckCanApply(raftentry.ApplyEntryID{Term: 2, Index: 3}); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("gap CheckCanApply error=%v code=%s, want rejected conflict", err, codeOf(err))
	}
	if err := progress.CheckCanApply(raftentry.ApplyEntryID{Term: 2, Index: 1}); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("lower CheckCanApply error=%v code=%s, want rejected conflict", err, codeOf(err))
	}
	if err := progress.CheckCanApply(raftentry.ApplyEntryID{Term: 1, Index: 2}); codeOf(err) != raftentry.ErrorRejectedConflictV1 {
		t.Fatalf("lower-term CheckCanApply error=%v code=%s, want rejected conflict", err, codeOf(err))
	}
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
		EntryID:           raftentry.ApplyEntryID{Term: 1, Index: index},
		CommandDigest:     digest,
		IdempotencyKey:    append([]byte(nil), key...),
		AppliedCommandLSN: index,
		Result: raftentry.ApplyResultV1{
			Status:        raftentry.ApplyStatusApplied,
			CommandDigest: digest,
			AffectedCount: 1,
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
