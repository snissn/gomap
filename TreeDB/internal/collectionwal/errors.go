package collectionwal

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

var (
	ErrCollectionWALTerminalTail       = errors.New("collectionwal: terminal incomplete tail")
	ErrCollectionWALCorruptMiddle      = errors.New("collectionwal: corrupt middle")
	ErrCollectionWALBadChecksum        = errors.New("collectionwal: bad checksum")
	ErrCollectionWALUnsupportedVersion = errors.New("collectionwal: unsupported version")
	ErrCollectionWALResourceLimit      = errors.New("collectionwal: resource limit")
	ErrCollectionWALUnsafePath         = errors.New("collectionwal: unsafe path")
	ErrCollectionWALMissingSideRef     = errors.New("collectionwal: missing side ref")
	ErrCollectionWALIdentityMismatch   = errors.New("collectionwal: identity mismatch")
	ErrCollectionWALSequenceGap        = errors.New("collectionwal: sequence gap")
	ErrCollectionWALRedacted           = errors.New("collectionwal: redacted")
	ErrCollectionWALRecoveryRequired   = errors.New("collectionwal: recovery required")
	ErrCollectionWALUnsupportedMode    = errors.New("collectionwal: unsupported mode")
)

type ErrorCategory string

const (
	ErrorCategoryIncompleteTail                ErrorCategory = "incomplete_tail"
	ErrorCategoryAlreadyAppliedWatermark       ErrorCategory = "already_applied_watermark"
	ErrorCategoryRecordChecksumMismatch        ErrorCategory = "record_checksum_mismatch"
	ErrorCategoryUnsupportedWALVersion         ErrorCategory = "unsupported_wal_version"
	ErrorCategorySegmentGapWithoutCleanup      ErrorCategory = "segment_gap_without_cleanup"
	ErrorCategoryMissingRequiredSideRef        ErrorCategory = "missing_required_side_ref"
	ErrorCategoryCorruptRequiredSideRef        ErrorCategory = "corrupt_required_side_ref"
	ErrorCategorySideRefClosureMismatch        ErrorCategory = "side_ref_closure_mismatch"
	ErrorCategoryCollectionIdentityMismatch    ErrorCategory = "collection_identity_mismatch"
	ErrorCategoryCollectionGenerationMismatch  ErrorCategory = "collection_generation_mismatch"
	ErrorCategorySchemaEpochMismatch           ErrorCategory = "schema_epoch_mismatch"
	ErrorCategoryBaseRootMismatch              ErrorCategory = "base_root_mismatch"
	ErrorCategoryRootDescriptorEpochMismatch   ErrorCategory = "root_descriptor_epoch_mismatch"
	ErrorCategoryBaseSystemRootMismatch        ErrorCategory = "base_system_root_mismatch"
	ErrorCategoryWatermarkInconsistency        ErrorCategory = "watermark_inconsistency"
	ErrorCategoryDuplicateWALLSN               ErrorCategory = "duplicate_wallsn"
	ErrorCategoryDuplicateCollectionSeq        ErrorCategory = "duplicate_collection_seq"
	ErrorCategoryDependencyGap                 ErrorCategory = "dependency_gap"
	ErrorCategorySystemDeltaPreconditionFailed ErrorCategory = "system_delta_precondition_failed"
	ErrorCategoryReplayPublishFailure          ErrorCategory = "replay_publish_failure"
	ErrorCategoryCleanupManifestMissing        ErrorCategory = "cleanup_manifest_missing"
	ErrorCategoryCleanupManifestCorrupt        ErrorCategory = "cleanup_manifest_corrupt"
	ErrorCategoryCleanupFailure                ErrorCategory = "cleanup_failure"
	ErrorCategoryOrphanPreparedSideRef         ErrorCategory = "orphan_prepared_side_ref"
	ErrorCategoryReadOnlyRecoveryRequired      ErrorCategory = "read_only_recovery_required"
)

const errorCategoryCount = 25

var allErrorCategories = [...]ErrorCategory{
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

type SideRefSummary struct {
	Class    string
	FileID   string
	Offset   uint64
	Length   uint64
	Checksum string
	PathHash string
}

type CollectionWALError struct {
	Category          ErrorCategory
	TxnID             string
	WALLSN            uint64
	CollectionUID     string
	CollectionUIDHash string
	CollectionSeq     uint64
	SegmentID         string
	SegmentOffset     uint64
	SideRef           *SideRefSummary
	Cause             error
}

func (e *CollectionWALError) Error() string {
	if e == nil {
		return "collectionwal: <nil>"
	}
	msg := fmt.Sprintf("collectionwal: category=%s", e.Category)
	if e.TxnID != "" {
		msg += " txn_id=" + e.TxnID
	}
	if e.WALLSN != 0 {
		msg += fmt.Sprintf(" wallsn=%d", e.WALLSN)
	}
	if e.CollectionUID != "" {
		msg += " collection_uid=" + e.CollectionUID
	}
	if e.CollectionUIDHash != "" {
		msg += " collection_uid_hash=" + e.CollectionUIDHash
	}
	if e.CollectionSeq != 0 {
		msg += fmt.Sprintf(" collection_seq=%d", e.CollectionSeq)
	}
	if e.SegmentID != "" {
		msg += " segment_id=" + e.SegmentID
	}
	if e.SegmentOffset != 0 {
		msg += fmt.Sprintf(" segment_offset=%d", e.SegmentOffset)
	}
	if e.SideRef != nil {
		if e.SideRef.Class != "" {
			msg += " side_ref_class=" + e.SideRef.Class
		}
		if e.SideRef.FileID != "" {
			msg += " side_ref_file_id=" + e.SideRef.FileID
		}
		if e.SideRef.Offset != 0 {
			msg += fmt.Sprintf(" side_ref_offset=%d", e.SideRef.Offset)
		}
		if e.SideRef.Length != 0 {
			msg += fmt.Sprintf(" side_ref_length=%d", e.SideRef.Length)
		}
		if e.SideRef.Checksum != "" {
			msg += " side_ref_checksum=" + e.SideRef.Checksum
		}
		if e.SideRef.PathHash != "" {
			msg += " side_ref_path_hash=" + e.SideRef.PathHash
		}
	}
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

func (e *CollectionWALError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func AllErrorCategories() []ErrorCategory {
	out := make([]ErrorCategory, len(allErrorCategories))
	copy(out, allErrorCategories[:])
	return out
}

func CategoryOf(err error) ErrorCategory {
	if err == nil {
		return ""
	}
	var typed *CollectionWALError
	if errors.As(err, &typed) && typed != nil {
		return typed.Category
	}
	switch {
	case errors.Is(err, ErrCollectionWALTerminalTail):
		return ErrorCategoryIncompleteTail
	case errors.Is(err, ErrCollectionWALBadChecksum):
		return ErrorCategoryRecordChecksumMismatch
	case errors.Is(err, ErrCollectionWALUnsupportedVersion), errors.Is(err, ErrCollectionWALUnsupportedMode):
		return ErrorCategoryUnsupportedWALVersion
	case errors.Is(err, ErrCollectionWALCorruptMiddle):
		return ErrorCategorySegmentGapWithoutCleanup
	case errors.Is(err, ErrCollectionWALUnsafePath):
		return ErrorCategoryCleanupManifestCorrupt
	case errors.Is(err, ErrCollectionWALMissingSideRef):
		return ErrorCategoryMissingRequiredSideRef
	case errors.Is(err, ErrCollectionWALIdentityMismatch):
		return ErrorCategoryCollectionIdentityMismatch
	case errors.Is(err, ErrCollectionWALSequenceGap):
		return ErrorCategoryDependencyGap
	case errors.Is(err, ErrCollectionWALRecoveryRequired):
		return ErrorCategoryReadOnlyRecoveryRequired
	default:
		return ""
	}
}

func IsIncompleteTail(err error) bool {
	return errors.Is(err, ErrCollectionWALTerminalTail) || CategoryOf(err) == ErrorCategoryIncompleteTail
}

func IsRecoveryRequired(err error) bool {
	return errors.Is(err, ErrCollectionWALRecoveryRequired) || CategoryOf(err) == ErrorCategoryReadOnlyRecoveryRequired
}

func IsCollectionWALCorruption(err error) bool {
	switch CategoryOf(err) {
	case ErrorCategoryRecordChecksumMismatch,
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
		ErrorCategorySystemDeltaPreconditionFailed,
		ErrorCategoryCleanupManifestCorrupt:
		return true
	default:
		return false
	}
}

func RedactedNameHash(redactionKey []byte, name string) string {
	mac := hmac.New(sha256.New, redactionKey)
	_, _ = mac.Write([]byte(name))
	sum := mac.Sum(nil)
	return hex.EncodeToString(sum[:8])
}
