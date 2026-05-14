package db

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestReadOnlyOpenRejectsDirtyCollectionWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	prepareCollectionWALRequiredFeatureForTest(t, d)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeSyntheticCollectionWALSegment(t, dir)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-only error=%v, want ErrRecoveryRequired", err)
	}
	_, err = openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("openReadOnlyNoLock error=%v, want ErrRecoveryRequired", err)
	}
}

func TestReadOnlyOpenRejectsCommittedUnappliedCollectionWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	prepareCollectionWALRequiredFeatureForTest(t, d)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommittedCollectionWALSegment(t, dir)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-only error=%v, want ErrRecoveryRequired", err)
	}
	_, err = openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("openReadOnlyNoLock error=%v, want ErrRecoveryRequired", err)
	}
}

func TestOfflineMaintenanceRejectsDirtyCollectionWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	prepareCollectionWALRequiredFeatureForTest(t, d)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeSyntheticCollectionWALSegment(t, dir)

	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("ValueLogRewriteOffline error=%v, want ErrRecoveryRequired", err)
	}
	if err := VacuumIndexOffline(Options{Dir: dir}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("VacuumIndexOffline error=%v, want ErrRecoveryRequired", err)
	}
}

func TestOfflineMaintenanceRejectsCommittedUnappliedCollectionWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	prepareCollectionWALRequiredFeatureForTest(t, d)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommittedCollectionWALSegment(t, dir)

	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("ValueLogRewriteOffline error=%v, want ErrRecoveryRequired", err)
	}
	if err := VacuumIndexOffline(Options{Dir: dir}); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("VacuumIndexOffline error=%v, want ErrRecoveryRequired", err)
	}
}

func TestOfflineMaintenanceAllowsRetainedAppliedCollectionWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	prepareCollectionWALRequiredFeatureForTest(t, d)
	writeLiveValueLogPointerForTest(t, d, dir)
	uid := testCollectionWALUID()
	setCollectionWALAppliedSeqForTest(t, d, uid, 1)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommittedCollectionWALSegment(t, dir)

	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("ValueLogRewriteOffline with retained applied collection WAL: %v", err)
	}
	if err := VacuumIndexOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("VacuumIndexOffline with retained applied collection WAL: %v", err)
	}
}

func TestReadOnlyOpenRejectsWatermarkedMalformedCollectionWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	prepareCollectionWALRequiredFeatureForTest(t, d)
	uid := testCollectionWALUID()
	setCollectionWALAppliedSeqForTest(t, d, uid, 1)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeMalformedCommittedCollectionWALSegment(t, dir)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-only error=%v, want ErrRecoveryRequired", err)
	}
	_, err = openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("openReadOnlyNoLock error=%v, want ErrRecoveryRequired", err)
	}
	readWrite, err := Open(Options{Dir: dir})
	if !errors.Is(err, collectionwal.ErrCollectionWALIdentityMismatch) {
		if readWrite != nil {
			_ = readWrite.Close()
		}
		t.Fatalf("Open read-write error=%v, want ErrCollectionWALIdentityMismatch", err)
	}
}

func TestCollectionWALReplayPlanRejectsDescriptorOpsSectionMismatch(t *testing.T) {
	uid := testCollectionWALUID()
	rootName := "test/primary"
	txn := testCollectionWALReplayTransactionForTest(
		t,
		uid,
		rootName,
		testCollectionWALSystemDeltaTemplateSection(t, uid, "test", rootName, 1),
		testCollectionWALDescriptorOpsSection(t, uid, "test/other", 1),
		1,
	)

	_, err := collectionWALReplayPlanFromTransaction(txn)
	if !errors.Is(err, collectionwal.ErrCollectionWALIdentityMismatch) {
		t.Fatalf("collectionWALReplayPlanFromTransaction error=%v, want ErrCollectionWALIdentityMismatch", err)
	}
}

func TestCollectionWALReplayPlanRejectsSystemTemplateDescriptorMismatch(t *testing.T) {
	uid := testCollectionWALUID()
	rootName := "test/primary"
	txn := testCollectionWALReplayTransactionForTest(
		t,
		uid,
		rootName,
		testCollectionWALSystemDeltaTemplateSectionWithDescriptorRoot(t, uid, "test", rootName, "test/other", 1),
		testCollectionWALDescriptorOpsSection(t, uid, "test/other", 1),
		1,
	)

	_, err := collectionWALReplayPlanFromTransaction(txn)
	if !errors.Is(err, collectionwal.ErrCollectionWALIdentityMismatch) {
		t.Fatalf("collectionWALReplayPlanFromTransaction error=%v, want ErrCollectionWALIdentityMismatch", err)
	}
}

func TestCollectionWALReplayPlanRejectsRootUIDMismatch(t *testing.T) {
	uid := testCollectionWALUID()
	rootName := "test/primary"
	badRootUID := collectionwal.PR1MinPrimaryRootUID(uid)
	badRootUID[0] ^= 0xff
	txn := testCollectionWALReplayTransactionWithRootDeltaForTest(
		t,
		uid,
		testCollectionWALRootDeltaSectionWithRootUID(t, uid, rootName, badRootUID, 1),
		testCollectionWALSystemDeltaTemplateSection(t, uid, "test", rootName, 1),
		testCollectionWALDescriptorOpsSection(t, uid, rootName, 1),
		1,
	)

	_, err := collectionWALReplayPlanFromTransaction(txn)
	if !errors.Is(err, collectionwal.ErrCollectionWALIdentityMismatch) {
		t.Fatalf("collectionWALReplayPlanFromTransaction error=%v, want ErrCollectionWALIdentityMismatch", err)
	}
}

func TestCollectionWALReplayPlanRejectsRootDescriptorDigestMismatch(t *testing.T) {
	uid := testCollectionWALUID()
	rootName := "test/primary"
	badDescriptorDigest := collectionwal.PR1MinPrimaryRootDescriptorDigest(uid, 0, 0)
	badDescriptorDigest[0] ^= 0xff
	txn := testCollectionWALReplayTransactionWithRootDeltaForTest(
		t,
		uid,
		testCollectionWALRootDeltaSectionWithDescriptorDigest(t, uid, rootName, badDescriptorDigest, 1),
		testCollectionWALSystemDeltaTemplateSection(t, uid, "test", rootName, 1),
		testCollectionWALDescriptorOpsSection(t, uid, rootName, 1),
		1,
	)

	_, err := collectionWALReplayPlanFromTransaction(txn)
	if !errors.Is(err, collectionwal.ErrCollectionWALIdentityMismatch) {
		t.Fatalf("collectionWALReplayPlanFromTransaction error=%v, want ErrCollectionWALIdentityMismatch", err)
	}
}

func TestRecoveryUnsupportedVersionFailsWithCategory(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	prepareCollectionWALRequiredFeatureForTest(t, d)
	writeCommittedCollectionWALSegment(t, dir)
	segmentPath := collectionwal.SegmentPath(dir, 0, 1)
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("ReadFile(segment): %v", err)
	}
	binary.LittleEndian.PutUint16(data[collectionwal.SegmentHeaderLen+10:collectionwal.SegmentHeaderLen+12], 2)
	if err := os.WriteFile(segmentPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile(segment): %v", err)
	}

	err = d.recoverCollectionWAL()
	if !errors.Is(err, collectionwal.ErrCollectionWALUnsupportedVersion) {
		t.Fatalf("recoverCollectionWAL error=%v want ErrCollectionWALUnsupportedVersion", err)
	}
	if got := collectionwal.CategoryOf(err); got != collectionwal.ErrorCategoryUnsupportedWALVersion {
		t.Fatalf("recovery category=%q want %q", got, collectionwal.ErrorCategoryUnsupportedWALVersion)
	}
	stats := d.CollectionWALStatsSnapshot()
	if got := stats.RecoveryFailuresByCategory[collectionwal.ErrorCategoryUnsupportedWALVersion]; got != 1 {
		t.Fatalf("unsupported-version recovery failures=%d want 1", got)
	}
	if got := stats.RecoveryLastFailureCategory; got != collectionwal.ErrorCategoryUnsupportedWALVersion {
		t.Fatalf("last recovery failure category=%q want %q", got, collectionwal.ErrorCategoryUnsupportedWALVersion)
	}
	if got := stats.RecoveryArtifactsWritten; got != 1 {
		t.Fatalf("recovery artifacts written=%d want 1", got)
	}
	if got := stats.RecoveryArtifactWriteFailure; got != 0 {
		t.Fatalf("recovery artifact write failures=%d want 0", got)
	}
	assertCollectionWALRecoveryArtifactForTest(t, dir, collectionwal.ErrorCategoryUnsupportedWALVersion)
	nativeStats := d.Stats()
	if got := nativeStats["treedb.collection_wal.recovery.artifacts_written_total"]; got != "1" {
		t.Fatalf("native recovery artifacts_written_total=%q want 1", got)
	}
	if got := nativeStats["treedb.collection_wal.recovery.artifact_write_failures_total"]; got != "0" {
		t.Fatalf("native recovery artifact_write_failures_total=%q want 0", got)
	}
}

func TestRecoverySegmentGapWithoutCleanupFailsWithCategory(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	prepareCollectionWALRequiredFeatureForTest(t, d)
	segmentPath := collectionwal.SegmentPath(dir, 0, 2)
	if err := os.MkdirAll(filepath.Dir(segmentPath), 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	if err := os.WriteFile(segmentPath, collectionwal.EncodeSegmentHeader(collectionwal.SegmentHeader{
		Lane:        0,
		SegmentSeq:  2,
		FirstWALLSN: 2,
	}), 0o600); err != nil {
		t.Fatalf("WriteFile(segment): %v", err)
	}

	err = d.recoverCollectionWAL()
	if !errors.Is(err, collectionwal.ErrCollectionWALCorruptMiddle) {
		t.Fatalf("recoverCollectionWAL error=%v want ErrCollectionWALCorruptMiddle", err)
	}
	if got := collectionwal.CategoryOf(err); got != collectionwal.ErrorCategorySegmentGapWithoutCleanup {
		t.Fatalf("recovery category=%q want %q", got, collectionwal.ErrorCategorySegmentGapWithoutCleanup)
	}
	stats := d.CollectionWALStatsSnapshot()
	if got := stats.RecoveryFailuresByCategory[collectionwal.ErrorCategorySegmentGapWithoutCleanup]; got != 1 {
		t.Fatalf("segment-gap recovery failures=%d want 1", got)
	}
	if got := stats.RecoveryArtifactsWritten; got != 1 {
		t.Fatalf("recovery artifacts written=%d want 1", got)
	}
	nativeStats := d.Stats()
	if got := nativeStats["treedb.collection_wal.recovery.failures.segment_gap_without_cleanup_total"]; got != "1" {
		t.Fatalf("native segment_gap_without_cleanup failures=%q want 1", got)
	}
}

func TestCollectionWALBadFrameCRCCompleteRecordFailsOpen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	prepareCollectionWALRequiredFeatureForTest(t, d)
	writeCommittedCollectionWALSegment(t, dir)
	segmentPath := collectionwal.SegmentPath(dir, 0, 1)
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("ReadFile(segment): %v", err)
	}
	data[collectionwal.SegmentHeaderLen+16] ^= 0x01
	if err := os.WriteFile(segmentPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile(segment): %v", err)
	}

	err = d.recoverCollectionWAL()
	if !errors.Is(err, collectionwal.ErrCollectionWALBadChecksum) {
		t.Fatalf("recoverCollectionWAL error=%v want ErrCollectionWALBadChecksum", err)
	}
	if got := collectionwal.CategoryOf(err); got != collectionwal.ErrorCategoryRecordChecksumMismatch {
		t.Fatalf("recovery category=%q want %q", got, collectionwal.ErrorCategoryRecordChecksumMismatch)
	}
}

func TestCollectionWALBadTxnCRCCompleteRecordFailsOpen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	prepareCollectionWALRequiredFeatureForTest(t, d)
	writeCommittedCollectionWALSegment(t, dir)
	segmentPath := collectionwal.SegmentPath(dir, 0, 1)
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("ReadFile(segment): %v", err)
	}
	data[collectionwal.SegmentHeaderLen+collectionwal.FrameHeaderLen+32] ^= 0x01
	if err := os.WriteFile(segmentPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile(segment): %v", err)
	}

	err = d.recoverCollectionWAL()
	if !errors.Is(err, collectionwal.ErrCollectionWALBadChecksum) {
		t.Fatalf("recoverCollectionWAL error=%v want ErrCollectionWALBadChecksum", err)
	}
	if got := collectionwal.CategoryOf(err); got != collectionwal.ErrorCategoryRecordChecksumMismatch {
		t.Fatalf("recovery category=%q want %q", got, collectionwal.ErrorCategoryRecordChecksumMismatch)
	}
}

func TestCollectionWALTruncatedActiveTailIgnoredOnlyWithoutCommitMarker(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	prepareCollectionWALRequiredFeatureForTest(t, d)
	segmentPath := collectionwal.SegmentPath(dir, 0, 1)
	if err := os.MkdirAll(filepath.Dir(segmentPath), 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	partial := append(collectionwal.EncodeSegmentHeader(collectionwal.SegmentHeader{Lane: 0, SegmentSeq: 1, FirstWALLSN: 1}), []byte("partial-frame-no-commit")...)
	if err := os.WriteFile(segmentPath, partial, 0o600); err != nil {
		t.Fatalf("WriteFile(segment): %v", err)
	}

	if err := d.recoverCollectionWAL(); err != nil {
		t.Fatalf("recoverCollectionWAL terminal incomplete tail: %v", err)
	}
	stats := d.CollectionWALStatsSnapshot()
	if stats.RecoveryTailSkip != 1 || stats.RecoveryHardFailure != 0 {
		t.Fatalf("recovery stats tailSkip=%d hardFailure=%d want 1/0", stats.RecoveryTailSkip, stats.RecoveryHardFailure)
	}
}

func TestCollectionWALTruncatedSealedSegmentFailsOpen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	prepareCollectionWALRequiredFeatureForTest(t, d)
	segment1 := collectionwal.SegmentPath(dir, 0, 1)
	segment2 := collectionwal.SegmentPath(dir, 0, 2)
	if err := os.MkdirAll(filepath.Dir(segment1), 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	partial := append(collectionwal.EncodeSegmentHeader(collectionwal.SegmentHeader{Lane: 0, SegmentSeq: 1, FirstWALLSN: 1}), []byte("partial-frame-no-commit")...)
	if err := os.WriteFile(segment1, partial, 0o600); err != nil {
		t.Fatalf("WriteFile(segment1): %v", err)
	}
	if err := os.WriteFile(segment2, collectionwal.EncodeSegmentHeader(collectionwal.SegmentHeader{Lane: 0, SegmentSeq: 2, FirstWALLSN: 2}), 0o600); err != nil {
		t.Fatalf("WriteFile(segment2): %v", err)
	}

	err = d.recoverCollectionWAL()
	if !errors.Is(err, collectionwal.ErrCollectionWALCorruptMiddle) {
		t.Fatalf("recoverCollectionWAL error=%v want ErrCollectionWALCorruptMiddle", err)
	}
	if got := collectionwal.CategoryOf(err); got != collectionwal.ErrorCategorySegmentGapWithoutCleanup {
		t.Fatalf("recovery category=%q want %q", got, collectionwal.ErrorCategorySegmentGapWithoutCleanup)
	}
}

func TestCollectionWALMissingSeqNBlocksSeqNPlusOne(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	prepareCollectionWALRequiredFeatureForTest(t, d)
	writeCommittedCollectionWALSegmentWithSeq(t, dir, 2)

	err = d.recoverCollectionWAL()
	if !errors.Is(err, collectionwal.ErrCollectionWALSequenceGap) {
		t.Fatalf("recoverCollectionWAL error=%v want ErrCollectionWALSequenceGap", err)
	}
	if got := collectionwal.CategoryOf(err); got != collectionwal.ErrorCategoryDependencyGap {
		t.Fatalf("recovery category=%q want %q", got, collectionwal.ErrorCategoryDependencyGap)
	}
	stats := d.CollectionWALStatsSnapshot()
	if stats.RecoveryBlockedTotal != 1 || stats.RecoveryHardFailure != 1 {
		t.Fatalf("recovery stats blocked=%d hardFailure=%d want 1/1", stats.RecoveryBlockedTotal, stats.RecoveryHardFailure)
	}
}

func TestCollectionWALStatsExportsAppliedWatermarkByUIDHash(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	prepareCollectionWALRequiredFeatureForTest(t, d)
	uid := testCollectionWALUID()
	setCollectionWALAppliedSeqForTest(t, d, uid, 7)

	stats := d.Stats()
	var matches []string
	for key, value := range stats {
		if strings.HasPrefix(key, "treedb.collection_wal.by_collection.") && strings.HasSuffix(key, ".applied_seq_current") {
			if value == "7" {
				matches = append(matches, key)
			}
		}
	}
	if len(matches) != 1 {
		t.Fatalf("applied_seq_current matches=%v want one value 7", matches)
	}
	if rawUID := hex.EncodeToString(uid[:]); strings.Contains(matches[0], rawUID) {
		t.Fatalf("by_collection metric leaked raw UID hex: %s", matches[0])
	}
}

func assertCollectionWALRecoveryArtifactForTest(t *testing.T, dir string, category collectionwal.ErrorCategory) {
	t.Helper()
	artifactRoot := filepath.Join(dir, "collection_wal", "recovery-artifacts")
	entries, err := os.ReadDir(artifactRoot)
	if err != nil {
		t.Fatalf("ReadDir(recovery-artifacts): %v", err)
	}
	if len(entries) != 1 || !entries[0].IsDir() {
		t.Fatalf("artifact entries=%v want one directory", entries)
	}
	artifactDir := filepath.Join(artifactRoot, entries[0].Name())
	for _, name := range []string{
		"recovery-report.json",
		"segments.json",
		"transactions.json",
		"side-refs.json",
		"watermarks.json",
		"cleanup-decisions.json",
	} {
		data, err := os.ReadFile(filepath.Join(artifactDir, name))
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", name, err)
		}
		if len(data) == 0 {
			t.Fatalf("%s is empty", name)
		}
		if strings.Contains(string(data), dir) {
			t.Fatalf("%s leaked absolute db dir", name)
		}
	}
	report, err := os.ReadFile(filepath.Join(artifactDir, "recovery-report.json"))
	if err != nil {
		t.Fatalf("ReadFile(recovery-report): %v", err)
	}
	if !strings.Contains(string(report), `"error_category": "`+string(category)+`"`) {
		t.Fatalf("recovery report missing category %q: %s", category, report)
	}
	transactions, err := os.ReadFile(filepath.Join(artifactDir, "transactions.json"))
	if err != nil {
		t.Fatalf("ReadFile(transactions): %v", err)
	}
	if !strings.Contains(string(transactions), `"outcome": "UnsupportedVersion"`) {
		t.Fatalf("transactions artifact missing unsupported outcome: %s", transactions)
	}
}

func prepareCollectionWALRequiredFeatureForTest(t *testing.T, d *DB) {
	t.Helper()
	if err := d.EnsureCollectionWALRequiredFeature(); err != nil {
		t.Fatalf("EnsureCollectionWALRequiredFeature: %v", err)
	}
}

func writeSyntheticCollectionWALSegment(t *testing.T, dir string) {
	t.Helper()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "collection-l0-000001.log"), []byte("dirty collection wal"), 0o600); err != nil {
		t.Fatalf("write collection WAL segment: %v", err)
	}
}

func writeCommittedCollectionWALSegment(t *testing.T, dir string) {
	t.Helper()
	writeCommittedCollectionWALSegmentWithSeq(t, dir, 1)
}

func writeCommittedCollectionWALSegmentWithSeq(t *testing.T, dir string, collectionSeq uint64) {
	t.Helper()
	app, err := collectionwal.CreateSegmentAppender(dir, collectionwal.AppenderOptions{SegmentBytes: 4096})
	if err != nil {
		t.Fatalf("CreateSegmentAppender: %v", err)
	}
	defer func() { _ = app.Close() }()
	uid := testCollectionWALUID()
	rootName := "test/primary"
	rootDelta := testCollectionWALRootDeltaSection(t, uid, rootName, collectionSeq)
	sideRefs := testCollectionWALEmptySideRefSection()
	systemTemplate := testCollectionWALSystemDeltaTemplateSection(t, uid, "test", rootName, collectionSeq)
	descriptorOps := testCollectionWALDescriptorOpsSection(t, uid, rootName, collectionSeq)
	_, err = app.AppendTransaction(collectionwal.Transaction{
		CollectionUID:            uid,
		CollectionGeneration:     1,
		CollectionSeq:            collectionSeq,
		DependsOnCollectionSeq:   collectionSeq - 1,
		CatalogEpoch:             1,
		SchemaEpoch:              1,
		MutationClass:            1,
		RootDeltaCount:           1,
		DescriptorOpCount:        2,
		BaseCatalogDigest:        testCollectionWALDigest(1),
		CatalogDigest:            testCollectionWALDigest(2),
		LogicalCatalogDigest:     testCollectionWALDigest(2),
		LocalReplayCatalogDigest: testCollectionWALDigest(2),
		Sections: []collectionwal.Section{
			{Type: collectionwal.SectionTypeRootDeltaTable, Version: collectionwal.SectionTableVersionV1, Flags: collectionwal.SectionFlagCritical | collectionwal.SectionFlagReplayCritical, Data: rootDelta},
			{Type: collectionwal.SectionTypeSideRefTable, Version: collectionwal.SectionTableVersionV1, Flags: collectionwal.SectionFlagCritical | collectionwal.SectionFlagReplayCritical, Data: sideRefs},
			{Type: collectionwal.SectionTypeSystemDeltaTemplate, Version: collectionwal.SectionTableVersionV1, Flags: collectionwal.SectionFlagCritical | collectionwal.SectionFlagReplayCritical, Data: systemTemplate},
			{Type: collectionwal.SectionTypeDescriptorOps, Version: collectionwal.SectionTableVersionV1, Flags: collectionwal.SectionFlagCritical | collectionwal.SectionFlagReplayCritical, Data: descriptorOps},
		},
	}, true)
	if err != nil {
		t.Fatalf("AppendTransaction: %v", err)
	}
}

func writeMalformedCommittedCollectionWALSegment(t *testing.T, dir string) {
	t.Helper()
	app, err := collectionwal.CreateSegmentAppender(dir, collectionwal.AppenderOptions{SegmentBytes: 4096})
	if err != nil {
		t.Fatalf("CreateSegmentAppender: %v", err)
	}
	defer func() { _ = app.Close() }()
	uid := testCollectionWALUID()
	_, err = app.AppendTransaction(collectionwal.Transaction{
		CollectionUID:          uid,
		CollectionGeneration:   1,
		CollectionSeq:          1,
		DependsOnCollectionSeq: 0,
		CatalogEpoch:           1,
		SchemaEpoch:            1,
		MutationClass:          1,
		RootDeltaCount:         1,
		DescriptorOpCount:      2,
	}, true)
	if err != nil {
		t.Fatalf("AppendTransaction malformed: %v", err)
	}
}

func testCollectionWALReplayTransactionForTest(t *testing.T, uid [collectionwal.CollectionUIDBytes]byte, rootName string, systemTemplate, descriptorOps []byte, collectionSeq uint64) collectionwal.Transaction {
	t.Helper()
	return testCollectionWALReplayTransactionWithRootDeltaForTest(t, uid, testCollectionWALRootDeltaSection(t, uid, rootName, collectionSeq), systemTemplate, descriptorOps, collectionSeq)
}

func testCollectionWALReplayTransactionWithRootDeltaForTest(t *testing.T, uid [collectionwal.CollectionUIDBytes]byte, rootDelta, systemTemplate, descriptorOps []byte, collectionSeq uint64) collectionwal.Transaction {
	t.Helper()
	return collectionwal.Transaction{
		CollectionUID:            uid,
		CollectionGeneration:     1,
		CollectionSeq:            collectionSeq,
		DependsOnCollectionSeq:   collectionSeq - 1,
		CatalogEpoch:             1,
		SchemaEpoch:              1,
		MutationClass:            1,
		RootDeltaCount:           1,
		DescriptorOpCount:        2,
		BaseCatalogDigest:        testCollectionWALDigest(1),
		CatalogDigest:            testCollectionWALDigest(2),
		LogicalCatalogDigest:     testCollectionWALDigest(2),
		LocalReplayCatalogDigest: testCollectionWALDigest(2),
		Sections: []collectionwal.Section{
			{Type: collectionwal.SectionTypeRootDeltaTable, Version: collectionwal.SectionTableVersionV1, Flags: collectionwal.SectionFlagCritical | collectionwal.SectionFlagReplayCritical, Data: rootDelta},
			{Type: collectionwal.SectionTypeSideRefTable, Version: collectionwal.SectionTableVersionV1, Flags: collectionwal.SectionFlagCritical | collectionwal.SectionFlagReplayCritical, Data: testCollectionWALEmptySideRefSection()},
			{Type: collectionwal.SectionTypeSystemDeltaTemplate, Version: collectionwal.SectionTableVersionV1, Flags: collectionwal.SectionFlagCritical | collectionwal.SectionFlagReplayCritical, Data: systemTemplate},
			{Type: collectionwal.SectionTypeDescriptorOps, Version: collectionwal.SectionTableVersionV1, Flags: collectionwal.SectionFlagCritical | collectionwal.SectionFlagReplayCritical, Data: descriptorOps},
		},
	}
}

func testCollectionWALUID() [collectionwal.CollectionUIDBytes]byte {
	var uid [collectionwal.CollectionUIDBytes]byte
	uid[0] = 1
	return uid
}

func testCollectionWALRootDeltaSection(t *testing.T, uid [collectionwal.CollectionUIDBytes]byte, rootName string, collectionSeq uint64) []byte {
	t.Helper()
	return testCollectionWALRootDeltaSectionWithRootUID(t, uid, rootName, collectionwal.PR1MinPrimaryRootUID(uid), collectionSeq)
}

func testCollectionWALRootDeltaSectionWithRootUID(t *testing.T, uid [collectionwal.CollectionUIDBytes]byte, rootName string, rootUID [collectionwal.CollectionUIDBytes]byte, collectionSeq uint64) []byte {
	t.Helper()
	baseDescriptorDigest := collectionwal.PR1MinPrimaryRootDescriptorDigest(uid, 0, collectionSeq-1)
	return testCollectionWALRootDeltaSectionWithRootUIDAndDescriptorDigest(t, uid, rootName, rootUID, baseDescriptorDigest, collectionSeq)
}

func testCollectionWALRootDeltaSectionWithDescriptorDigest(t *testing.T, uid [collectionwal.CollectionUIDBytes]byte, rootName string, baseDescriptorDigest [32]byte, collectionSeq uint64) []byte {
	t.Helper()
	return testCollectionWALRootDeltaSectionWithRootUIDAndDescriptorDigest(t, uid, rootName, collectionwal.PR1MinPrimaryRootUID(uid), baseDescriptorDigest, collectionSeq)
}

func testCollectionWALRootDeltaSectionWithRootUIDAndDescriptorDigest(t *testing.T, uid [collectionwal.CollectionUIDBytes]byte, rootName string, rootUID [collectionwal.CollectionUIDBytes]byte, baseDescriptorDigest [32]byte, collectionSeq uint64) []byte {
	t.Helper()
	dependsOnCollectionSeq := collectionSeq - 1
	out := []byte{'T', 'D', 'B', 'C', 'W', 'R', 'D', 0x01}
	out = append(out, uid[:]...)
	out = appendTestCollectionWALUint32(out, 1)
	out = appendTestCollectionWALString(t, out, rootName, collectionwal.MaxRootNameBytes)
	out = append(out, rootUID[:]...)
	out = appendTestCollectionWALUint16(out, collectionwal.RootKindPrimary)
	out = appendTestCollectionWALUint64(out, 0)
	out = appendTestCollectionWALUint64(out, collectionwal.RootGenerationPrimary)
	out = appendTestCollectionWALUint64(out, dependsOnCollectionSeq)
	out = append(out, baseDescriptorDigest[:]...)
	out = appendTestCollectionWALUint64(out, 0)
	out = appendTestCollectionWALUint64(out, 0)
	return out
}

func testCollectionWALEmptySideRefSection() []byte {
	out := []byte{'T', 'D', 'B', 'C', 'W', 'S', 'R', 0x01}
	return appendTestCollectionWALUint32(out, 0)
}

func testCollectionWALSystemDeltaTemplateSection(t *testing.T, uid [collectionwal.CollectionUIDBytes]byte, collectionName, rootName string, collectionSeq uint64) []byte {
	t.Helper()
	return testCollectionWALSystemDeltaTemplateSectionWithDescriptorRoot(t, uid, collectionName, rootName, rootName, collectionSeq)
}

func testCollectionWALSystemDeltaTemplateSectionWithDescriptorRoot(t *testing.T, uid [collectionwal.CollectionUIDBytes]byte, collectionName, rootName, descriptorRootName string, collectionSeq uint64) []byte {
	t.Helper()
	out := []byte{'T', 'D', 'B', 'C', 'W', 'S', 'T', 0x01}
	out = append(out, uid[:]...)
	out = appendTestCollectionWALString(t, out, collectionName, collectionwal.MaxLogicalNameBytes)
	out = appendTestCollectionWALUint64(out, 1)
	out = appendTestCollectionWALUint64(out, 1)
	out = appendTestCollectionWALUint64(out, 1)
	out = appendTestCollectionWALUint64(out, 0)
	out = appendTestCollectionWALUint64(out, 0)
	out = appendTestCollectionWALUint64(out, collectionSeq-1)
	out = appendTestCollectionWALUint64(out, collectionSeq)
	out = appendTestCollectionWALUint32(out, 1)
	out = appendTestCollectionWALString(t, out, rootName, collectionwal.MaxRootNameBytes)
	rootUID := collectionwal.PR1MinPrimaryRootUID(uid)
	out = append(out, rootUID[:]...)
	out = appendTestCollectionWALUint16(out, collectionwal.RootKindPrimary)
	out = appendTestCollectionWALUint64(out, 0)
	out = appendTestCollectionWALUint64(out, collectionwal.RootGenerationPrimary)
	out = appendTestCollectionWALUint64(out, collectionSeq-1)
	baseDescriptorDigest := collectionwal.PR1MinPrimaryRootDescriptorDigest(uid, 0, collectionSeq-1)
	out = append(out, baseDescriptorDigest[:]...)
	out = appendTestCollectionWALUint64(out, 0)
	out = appendTestCollectionWALUint32(out, 2)
	return appendTestCollectionWALDescriptorOps(t, out, uid, descriptorRootName, collectionSeq)
}

func testCollectionWALDescriptorOpsSection(t *testing.T, uid [collectionwal.CollectionUIDBytes]byte, rootName string, collectionSeq uint64) []byte {
	t.Helper()
	out := []byte{'T', 'D', 'B', 'C', 'W', 'D', 'O', 0x01}
	out = appendTestCollectionWALUint32(out, 2)
	return appendTestCollectionWALDescriptorOps(t, out, uid, rootName, collectionSeq)
}

func appendTestCollectionWALDescriptorOps(t *testing.T, out []byte, uid [collectionwal.CollectionUIDBytes]byte, rootName string, collectionSeq uint64) []byte {
	t.Helper()
	maxDescriptorKey := collectionwal.MaxRootNameBytes + len(systemCollectionWALAppliedPrefix)
	out = appendTestCollectionWALUint16(out, collectionWALDescriptorOpRootUpdate)
	out = appendTestCollectionWALString(t, out, systemCollectionRootPrefix+rootName, maxDescriptorKey)
	out = appendTestCollectionWALBytes(t, out, encodeCollectionWALRootID(0), 8)
	out = appendTestCollectionWALUint16(out, collectionWALDescriptorOpAppliedWatermark)
	out = appendTestCollectionWALString(t, out, systemCollectionWALAppliedPrefix+hex.EncodeToString(uid[:]), maxDescriptorKey)
	out = appendTestCollectionWALBytes(t, out, encodeCollectionWALRootID(collectionSeq), 8)
	return out
}

func appendTestCollectionWALString(t *testing.T, out []byte, value string, max int) []byte {
	t.Helper()
	return appendTestCollectionWALBytes(t, out, []byte(value), max)
}

func appendTestCollectionWALBytes(t *testing.T, out, value []byte, max int) []byte {
	t.Helper()
	if len(value) > max {
		t.Fatalf("test collection WAL bytes len=%d exceeds %d", len(value), max)
	}
	out = appendTestCollectionWALUint32(out, uint32(len(value)))
	return append(out, value...)
}

func appendTestCollectionWALUint16(out []byte, value uint16) []byte {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], value)
	return append(out, buf[:]...)
}

func appendTestCollectionWALUint32(out []byte, value uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	return append(out, buf[:]...)
}

func appendTestCollectionWALUint64(out []byte, value uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	return append(out, buf[:]...)
}

func testCollectionWALDigest(seed byte) [32]byte {
	var digest [32]byte
	for i := range digest {
		digest[i] = seed + byte(i)
	}
	return digest
}

func setCollectionWALAppliedSeqForTest(t *testing.T, d *DB, uid [collectionwal.CollectionUIDBytes]byte, seq uint64) {
	t.Helper()
	key := []byte(systemCollectionWALAppliedPrefix + hex.EncodeToString(uid[:]))
	_, _, err := d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return &collectionWALSystemDeltaIterator{entries: []collectionWALSystemEntry{{
			key:   key,
			value: encodeCollectionWALRootID(seq),
		}}}, nil
	})
	if err != nil {
		t.Fatalf("publish collection WAL applied watermark: %v", err)
	}
}

func writeLiveValueLogPointerForTest(t *testing.T, d *DB, dir string) {
	t.Helper()
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 500_000, 1, func(int) []byte {
		return []byte("collection-wal-offline-maintenance-live-value")
	})
	b := d.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("live-value-log-key"), ptrs[0]); err != nil {
		_ = b.Close()
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write pointer batch: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close pointer batch: %v", err)
	}
}
