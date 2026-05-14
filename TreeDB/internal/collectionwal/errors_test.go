package collectionwal

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestCollectionWALErrorCategoriesStable(t *testing.T) {
	want := []ErrorCategory{
		ErrorCategoryIncompleteTail,
		ErrorCategoryAlreadyAppliedWatermark,
		ErrorCategoryRecordChecksumMismatch,
		ErrorCategoryUnsupportedWALVersion,
		ErrorCategorySegmentGapWithoutCleanup,
		ErrorCategoryMissingRequiredSideRef,
		ErrorCategoryCorruptRequiredSideRef,
		ErrorCategorySideRefClosureMismatch,
		ErrorCategoryCollectionIdentityMismatch,
		ErrorCategoryCollectionGenerationMismatch,
		ErrorCategorySchemaEpochMismatch,
		ErrorCategoryBaseRootMismatch,
		ErrorCategoryRootDescriptorEpochMismatch,
		ErrorCategoryBaseSystemRootMismatch,
		ErrorCategoryWatermarkInconsistency,
		ErrorCategoryDuplicateWALLSN,
		ErrorCategoryDuplicateCollectionSeq,
		ErrorCategoryDependencyGap,
		ErrorCategorySystemDeltaPreconditionFailed,
		ErrorCategoryReplayPublishFailure,
		ErrorCategoryCleanupManifestMissing,
		ErrorCategoryCleanupManifestCorrupt,
		ErrorCategoryCleanupFailure,
		ErrorCategoryOrphanPreparedSideRef,
		ErrorCategoryReadOnlyRecoveryRequired,
	}
	if got := AllErrorCategories(); !reflect.DeepEqual(got, want) {
		t.Fatalf("AllErrorCategories mismatch\ngot:  %#v\nwant: %#v", got, want)
	}
	if got := len(AllErrorCategories()); got != errorCategoryCount {
		t.Fatalf("category count=%d want %d", got, errorCategoryCount)
	}
	cases := []struct {
		err  error
		want ErrorCategory
	}{
		{fmt.Errorf("scan: %w", ErrCollectionWALTerminalTail), ErrorCategoryIncompleteTail},
		{fmt.Errorf("scan: %w", ErrCollectionWALBadChecksum), ErrorCategoryRecordChecksumMismatch},
		{fmt.Errorf("scan: %w", ErrCollectionWALUnsupportedVersion), ErrorCategoryUnsupportedWALVersion},
		{fmt.Errorf("scan: %w", ErrCollectionWALCorruptMiddle), ErrorCategorySegmentGapWithoutCleanup},
		{fmt.Errorf("scan: %w", ErrCollectionWALMissingSideRef), ErrorCategoryMissingRequiredSideRef},
		{fmt.Errorf("scan: %w", ErrCollectionWALIdentityMismatch), ErrorCategoryCollectionIdentityMismatch},
		{fmt.Errorf("scan: %w", ErrCollectionWALSequenceGap), ErrorCategoryDependencyGap},
		{fmt.Errorf("scan: %w", ErrCollectionWALRecoveryRequired), ErrorCategoryReadOnlyRecoveryRequired},
		{errors.New("random filesystem failure"), ""},
	}
	for _, tc := range cases {
		if got := CategoryOf(tc.err); got != tc.want {
			t.Fatalf("CategoryOf(%v)=%q want %q", tc.err, got, tc.want)
		}
	}
	if !IsIncompleteTail(fmt.Errorf("tail: %w", ErrCollectionWALTerminalTail)) {
		t.Fatalf("IsIncompleteTail rejected terminal tail sentinel")
	}
	if !IsRecoveryRequired(fmt.Errorf("readonly: %w", ErrCollectionWALRecoveryRequired)) {
		t.Fatalf("IsRecoveryRequired rejected recovery required sentinel")
	}
	if !IsCollectionWALCorruption(fmt.Errorf("checksum: %w", ErrCollectionWALBadChecksum)) {
		t.Fatalf("IsCollectionWALCorruption rejected checksum sentinel")
	}
}

func TestCollectionWALRedaction(t *testing.T) {
	nameHash := RedactedNameHash([]byte("redaction-key"), "users")
	pathHash := RedactedNameHash([]byte("redaction-key"), "/tmp/private/users.collection")
	if nameHash == "" || len(nameHash) != 16 {
		t.Fatalf("name hash=%q want 16 hex chars", nameHash)
	}
	if nameHash != RedactedNameHash([]byte("redaction-key"), "users") {
		t.Fatalf("name hash is not stable")
	}
	if nameHash == RedactedNameHash([]byte("other-key"), "users") {
		t.Fatalf("name hash did not change with redaction key")
	}
	if strings.Contains(nameHash, "users") || strings.Contains(pathHash, "users") {
		t.Fatalf("hash leaked raw collection name: name=%q path=%q", nameHash, pathHash)
	}

	err := (&CollectionWALError{
		Category:          ErrorCategoryMissingRequiredSideRef,
		TxnID:             "txn-1",
		WALLSN:            12,
		CollectionUIDHash: nameHash,
		CollectionSeq:     34,
		SegmentID:         "collection-l0-000001.log",
		SegmentOffset:     56,
		SideRef: &SideRefSummary{
			Class:    "vlog",
			FileID:   "000001",
			Offset:   78,
			Length:   90,
			Checksum: "crc32c:abcd1234",
			PathHash: pathHash,
		},
		Cause: ErrCollectionWALRedacted,
	}).Error()
	for _, want := range []string{
		"category=missing_required_side_ref",
		"txn_id=txn-1",
		"collection_uid_hash=" + nameHash,
		"side_ref_path_hash=" + pathHash,
		"side_ref_class=vlog",
	} {
		if !strings.Contains(err, want) {
			t.Fatalf("redacted error %q missing %q", err, want)
		}
	}
	for _, raw := range []string{"users", "/tmp/private"} {
		if strings.Contains(err, raw) {
			t.Fatalf("redacted error leaked %q: %s", raw, err)
		}
	}
}

func TestCollectionWALMetricMonotonicity(t *testing.T) {
	var stats Stats
	stats.RecordAppendFailure(fmt.Errorf("append: %w", ErrCollectionWALBadChecksum))
	first := stats.Snapshot()
	if got := first.AppendFailuresTotal; got != 1 {
		t.Fatalf("append failures total=%d want 1", got)
	}
	if got := first.AppendFailuresByCategory[ErrorCategoryRecordChecksumMismatch]; got != 1 {
		t.Fatalf("append checksum failures=%d want 1", got)
	}

	stats.RecordAppendFailure(fmt.Errorf("append: %w", ErrCollectionWALBadChecksum))
	second := stats.Snapshot()
	if got := second.AppendFailuresTotal; got != 2 {
		t.Fatalf("append failures total after second record=%d want 2", got)
	}
	if got := second.AppendFailuresByCategory[ErrorCategoryRecordChecksumMismatch]; got != 2 {
		t.Fatalf("append checksum failures after second record=%d want 2", got)
	}
	if got := second.AppendFailuresByCategory[ErrorCategoryUnsupportedWALVersion]; got != 0 {
		t.Fatalf("append unsupported-version failures=%d want 0", got)
	}

	stats.RecordRecoveryHardFailure(fmt.Errorf("recover: %w", ErrCollectionWALUnsupportedVersion))
	recovered := stats.Snapshot()
	if got := recovered.RecoveryHardFailure; got != 1 {
		t.Fatalf("recovery hard failures=%d want 1", got)
	}
	if got := recovered.RecoveryFailuresByCategory[ErrorCategoryUnsupportedWALVersion]; got != 1 {
		t.Fatalf("recovery unsupported-version failures=%d want 1", got)
	}
	if got := recovered.RecoveryLastFailureCategory; got != ErrorCategoryUnsupportedWALVersion {
		t.Fatalf("last recovery failure category=%q want %q", got, ErrorCategoryUnsupportedWALVersion)
	}

	stats.RecordRecoveryHardFailure(&CollectionWALError{
		Category:      ErrorCategoryDuplicateCollectionSeq,
		WALLSN:        99,
		CollectionSeq: 7,
		Cause:         ErrCollectionWALSequenceGap,
	})
	positioned := stats.Snapshot()
	if got := positioned.RecoveryHardFailure; got != 2 {
		t.Fatalf("recovery hard failures after positioned error=%d want 2", got)
	}
	if got := positioned.RecoveryFailuresByCategory[ErrorCategoryDuplicateCollectionSeq]; got != 1 {
		t.Fatalf("recovery duplicate collection seq failures=%d want 1", got)
	}
	if got := positioned.RecoveryLastFailureWALLSN; got != 99 {
		t.Fatalf("last recovery failure WALLSN=%d want 99", got)
	}
	if got := positioned.RecoveryLastFailureSeq; got != 7 {
		t.Fatalf("last recovery failure collection seq=%d want 7", got)
	}

	stats.RecordRecoveryArtifactWritten()
	stats.RecordRecoveryArtifactWriteFailure()
	artifacts := stats.Snapshot()
	if got := artifacts.RecoveryArtifactsWritten; got != 1 {
		t.Fatalf("recovery artifacts written=%d want 1", got)
	}
	if got := artifacts.RecoveryArtifactWriteFailure; got != 1 {
		t.Fatalf("recovery artifact write failures=%d want 1", got)
	}
}
