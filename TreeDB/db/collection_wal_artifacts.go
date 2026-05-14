package db

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/tree"
)

type collectionWALRecoveryReportArtifact struct {
	GeneratedUnixNano      int64                       `json:"generated_unix_nano"`
	PID                    int                         `json:"pid"`
	ErrorCategory          collectionwal.ErrorCategory `json:"error_category,omitempty"`
	ErrorType              string                      `json:"error_type,omitempty"`
	SegmentCount           int                         `json:"segment_count"`
	SafeToRestart          bool                        `json:"safe_to_restart"`
	SafeToBackup           bool                        `json:"safe_to_backup"`
	SafeToCompact          bool                        `json:"safe_to_compact"`
	SafeToDeleteFiles      bool                        `json:"safe_to_delete_files"`
	RequiresOperatorAction bool                        `json:"requires_operator_action"`
}

type collectionWALSegmentArtifact struct {
	SegmentID         string                      `json:"segment_id"`
	SizeBytes         int64                       `json:"size_bytes,omitempty"`
	SHA256            string                      `json:"sha256,omitempty"`
	FrameCount        int                         `json:"frame_count,omitempty"`
	ScanErrorType     string                      `json:"scan_error_type,omitempty"`
	ScanErrorCategory collectionwal.ErrorCategory `json:"scan_error_category,omitempty"`
	ReadErrorType     string                      `json:"read_error_type,omitempty"`
}

type collectionWALTransactionArtifact struct {
	TxnID               string                      `json:"txn_id"`
	SegmentID           string                      `json:"segment_id"`
	SegmentOffset       int64                       `json:"segment_offset"`
	RecordLength        int64                       `json:"record_length"`
	Outcome             collectionwal.FrameOutcome  `json:"outcome"`
	ErrorCategory       collectionwal.ErrorCategory `json:"error_category,omitempty"`
	WALLSN              uint64                      `json:"wallsn,omitempty"`
	CollectionUIDHash   string                      `json:"collection_uid_hash,omitempty"`
	CollectionSeq       uint64                      `json:"collection_seq,omitempty"`
	CommitMarkerPresent bool                        `json:"commit_marker_present"`
	RecordChecksumValid bool                        `json:"record_checksum_valid"`
}

type collectionWALWatermarkArtifact struct {
	CollectionUIDHash string `json:"collection_uid_hash"`
	AppliedSeq        uint64 `json:"applied_seq,omitempty"`
	ValueLen          int    `json:"value_len,omitempty"`
	Malformed         bool   `json:"malformed,omitempty"`
}

func (db *DB) writeCollectionWALRecoveryFailureArtifact(recoveryErr error, segments []string) error {
	if db == nil || db.dir == "" {
		return nil
	}
	now := time.Now()
	artifactParent := filepath.Join(db.dir, "collection_wal")
	artifactRoot := filepath.Join(artifactParent, "recovery-artifacts")
	artifactDir := filepath.Join(artifactRoot, fmt.Sprintf("%d-%d-%s", now.UnixNano(), os.Getpid(), collectionWALArtifactNonce(now)))
	if err := os.MkdirAll(artifactDir, 0o700); err != nil {
		return err
	}
	category := collectionwal.CategoryOf(recoveryErr)
	report := collectionWALRecoveryReportArtifact{
		GeneratedUnixNano:      now.UnixNano(),
		PID:                    os.Getpid(),
		ErrorCategory:          category,
		ErrorType:              collectionWALArtifactErrorType(recoveryErr),
		SegmentCount:           len(segments),
		SafeToRestart:          true,
		SafeToBackup:           false,
		SafeToCompact:          false,
		SafeToDeleteFiles:      false,
		RequiresOperatorAction: true,
	}
	segmentArtifacts, txnArtifacts := collectionWALRecoverySegmentArtifacts(segments)
	watermarks, watermarkErrorType := db.collectionWALWatermarkArtifactData()
	files := []struct {
		name  string
		value any
	}{
		{"recovery-report.json", report},
		{"segments.json", map[string]any{"segments": segmentArtifacts}},
		{"transactions.json", map[string]any{"transactions": txnArtifacts}},
		{"side-refs.json", map[string]any{"side_refs": []any{}, "pr1_min_side_refs_supported": false}},
		{"watermarks.json", map[string]any{"watermarks": watermarks, "error_type": watermarkErrorType}},
		{"cleanup-decisions.json", map[string]any{"decisions": []any{}, "safe_to_delete_files": false}},
	}
	for _, file := range files {
		if err := writeCollectionWALArtifactJSON(artifactDir, file.name, file.value); err != nil {
			return err
		}
	}
	if err := syncDirFn(artifactDir); err != nil {
		return err
	}
	if err := syncDirFn(artifactRoot); err != nil {
		return err
	}
	return syncDirFn(artifactParent)
}

func collectionWALRecoverySegmentArtifacts(segments []string) ([]collectionWALSegmentArtifact, []collectionWALTransactionArtifact) {
	paths := append([]string(nil), segments...)
	sort.Strings(paths)
	segmentArtifacts := make([]collectionWALSegmentArtifact, 0, len(paths))
	var txnArtifacts []collectionWALTransactionArtifact
	for i, segmentPath := range paths {
		segmentID := filepath.Base(segmentPath)
		artifact := collectionWALSegmentArtifact{SegmentID: segmentID}
		data, err := os.ReadFile(segmentPath)
		if err != nil {
			artifact.ReadErrorType = collectionWALArtifactErrorType(err)
			segmentArtifacts = append(segmentArtifacts, artifact)
			continue
		}
		artifact.SizeBytes = int64(len(data))
		sum := sha256.Sum256(data)
		artifact.SHA256 = hex.EncodeToString(sum[:])
		_, frames, err := collectionwal.ScanSegment(data, i == len(paths)-1)
		artifact.FrameCount = len(frames)
		if err != nil {
			artifact.ScanErrorType = collectionWALArtifactErrorType(err)
			artifact.ScanErrorCategory = collectionwal.CategoryOf(err)
		}
		for _, frame := range frames {
			txnArtifacts = append(txnArtifacts, collectionWALTransactionArtifact{
				TxnID:               fmt.Sprintf("%s:%d", segmentID, frame.Offset),
				SegmentID:           segmentID,
				SegmentOffset:       frame.Offset,
				RecordLength:        frame.Length,
				Outcome:             frame.Outcome,
				ErrorCategory:       collectionWALArtifactFrameCategory(frame),
				WALLSN:              frame.Header.WALLSN,
				CollectionUIDHash:   collectionWALArtifactFrameUIDHash(frame),
				CollectionSeq:       frame.Header.CollectionSeq,
				CommitMarkerPresent: frame.Outcome == collectionwal.OutcomeCompleteValid || frame.Outcome == collectionwal.OutcomeCompleteCorrupt || frame.Outcome == collectionwal.OutcomeUnsupportedVersion || frame.Outcome == collectionwal.OutcomeDuplicateWALLSN || frame.Outcome == collectionwal.OutcomeDuplicateCollectionSeq,
				RecordChecksumValid: frame.Outcome == collectionwal.OutcomeCompleteValid,
			})
		}
		segmentArtifacts = append(segmentArtifacts, artifact)
	}
	return segmentArtifacts, txnArtifacts
}

func (db *DB) collectionWALWatermarkArtifacts() []collectionWALWatermarkArtifact {
	watermarks, _ := db.collectionWALWatermarkArtifactData()
	return watermarks
}

func (db *DB) collectionWALWatermarkArtifactData() ([]collectionWALWatermarkArtifact, string) {
	if db == nil {
		return nil, collectionWALArtifactErrorType(ErrClosed)
	}
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil || db.meta.SystemRootPageID == 0 {
		return nil, ""
	}
	prefix := []byte(systemCollectionWALAppliedPrefix)
	tr := tree.New(idx.pager, db.collectionWALSystemRootReader(), db.meta.SystemRootPageID)
	it := tr.IteratorWithOptions(prefix, collectionWALPrefixEnd(prefix), tree.IteratorOptions{})
	defer func() { _ = it.Close() }()
	var watermarks []collectionWALWatermarkArtifact
	for it.Valid() {
		key := it.UnsafeKey()
		if len(key) < len(prefix) || string(key[:len(prefix)]) != string(prefix) {
			break
		}
		if !it.IsDeleted() {
			raw := it.UnsafeValue()
			uidHash := collectionWALArtifactWatermarkUIDHash(key[len(prefix):])
			watermark := collectionWALWatermarkArtifact{
				CollectionUIDHash: uidHash,
				ValueLen:          len(raw),
			}
			if seq, err := decodeCollectionWALRootID(raw); err == nil {
				watermark.AppliedSeq = seq
			} else {
				watermark.Malformed = true
			}
			watermarks = append(watermarks, watermark)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return watermarks, collectionWALArtifactErrorType(err)
	}
	return watermarks, ""
}

func writeCollectionWALArtifactJSON(dir, name string, value any) error {
	path := filepath.Join(dir, name)
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	if err := writeCollectionWALArtifactFull(file, data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	closed = true
	return file.Close()
}

func writeCollectionWALArtifactFull(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("short write")
		}
		data = data[n:]
	}
	return nil
}

func collectionWALArtifactNonce(now time.Time) string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	return fmt.Sprintf("%016x", uint64(now.UnixNano()))
}

func collectionWALArtifactErrorType(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%T", err)
}

func collectionWALArtifactFrameCategory(frame collectionwal.FrameScanResult) collectionwal.ErrorCategory {
	switch frame.Outcome {
	case collectionwal.OutcomeUnsupportedVersion:
		return collectionwal.ErrorCategoryUnsupportedWALVersion
	case collectionwal.OutcomeDuplicateWALLSN:
		return collectionwal.ErrorCategoryDuplicateWALLSN
	case collectionwal.OutcomeDuplicateCollectionSeq:
		return collectionwal.ErrorCategoryDuplicateCollectionSeq
	case collectionwal.OutcomeNonTerminalShortRead:
		return collectionwal.ErrorCategorySegmentGapWithoutCleanup
	default:
		return collectionwal.CategoryOf(frame.Err)
	}
}

func collectionWALArtifactFrameUIDHash(frame collectionwal.FrameScanResult) string {
	uid := frame.Header.CollectionUID
	if uid == [collectionwal.CollectionUIDBytes]byte{} {
		uid = frame.Transaction.CollectionUID
	}
	return collectionWALArtifactUIDHash(uid)
}

func collectionWALArtifactWatermarkUIDHash(uidHex []byte) string {
	var uid [collectionwal.CollectionUIDBytes]byte
	decoded, err := hex.DecodeString(string(uidHex))
	if err == nil && len(decoded) == collectionwal.CollectionUIDBytes {
		copy(uid[:], decoded)
		return collectionWALArtifactUIDHash(uid)
	}
	sum := sha256.Sum256(uidHex)
	return hex.EncodeToString(sum[:8])
}

func collectionWALArtifactUIDHash(uid [collectionwal.CollectionUIDBytes]byte) string {
	if uid == [collectionwal.CollectionUIDBytes]byte{} {
		return ""
	}
	sum := sha256.Sum256(uid[:])
	return hex.EncodeToString(sum[:8])
}
