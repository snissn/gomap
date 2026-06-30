package raftapply

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const maxApplyMetadataRecordsV1 = raftentry.MaxProgressRecordsV1

// Error carries stable deterministic apply error names through fake stores and
// the harness boundary.
type Error struct {
	Code raftentry.DeterministicErrorCodeV1
	Err  error
}

func (e *Error) Error() string {
	if e == nil {
		return "raftapply: <nil>"
	}
	if e.Err == nil {
		return fmt.Sprintf("raftapply: %s", e.Code)
	}
	return fmt.Sprintf("raftapply: %s: %v", e.Code, e.Err)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ErrorCodeOf extracts stable deterministic error names returned by this
// package or by the predecessor raftentry decoder.
func ErrorCodeOf(err error) (raftentry.DeterministicErrorCodeV1, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Code, true
	}
	return raftentry.ErrorCodeOf(err)
}

func codedError(code raftentry.DeterministicErrorCodeV1, format string, args ...any) error {
	return &Error{Code: code, Err: fmt.Errorf(format, args...)}
}

// ApplyResultRecordV1 is the fake/durable result-store unit keyed by
// ApplyEntryID. The result payload is intentionally bounded separately from the
// deterministic entry bytes.
type ApplyResultRecordV1 struct {
	EntryID                 raftentry.ApplyEntryID
	CommandDigest           raftentry.CommandDigestV1
	IdempotencyKey          []byte
	AppliedCommandLSN       uint64
	ProgressLogicalDigestV1 LogicalDigestV1
	Result                  raftentry.ApplyResultV1
}

// ApplyResultStore records deterministic apply results for idempotency/replay.
type ApplyResultStore interface {
	LookupApplyResult(raftentry.ApplyEntryID) (ApplyResultRecordV1, bool, error)
	LookupApplyResultByIdempotencyKey([]byte) (ApplyResultRecordV1, bool, error)
	CheckCanRecordApplyResult(ApplyResultRecordV1) error
	RecordApplyResult(ApplyResultRecordV1) error
}

// ApplyProgressRecordV1 records the apply-progress transition made after an
// entry has reached the selected local durability and visibility boundary.
type ApplyProgressRecordV1 struct {
	EntryID           raftentry.ApplyEntryID
	CommandDigest     raftentry.CommandDigestV1
	AppliedCommandLSN uint64
	LogicalDigestV1   LogicalDigestV1
}

// ApplyProgressStore checks monotonic apply order and records durable progress.
type ApplyProgressStore interface {
	CheckCanApply(raftentry.ApplyEntryID) error
	CheckCanRecordApplied(ApplyProgressRecordV1) error
	RecordApplied(ApplyProgressRecordV1) error
	LookupApplyProgress(raftentry.ApplyEntryID) (ApplyProgressRecordV1, bool, error)
	LastApplied() (raftentry.ApplyEntryID, bool)
}

// MemoryApplyResultStore is a bounded fake result/idempotency store for tests
// and early harness wiring. It is not durable.
type MemoryApplyResultStore struct {
	mu      sync.Mutex
	max     int
	records map[raftentry.ApplyEntryID]ApplyResultRecordV1
	byKey   map[string]raftentry.ApplyEntryID
}

func NewMemoryApplyResultStore(maxRecords int) *MemoryApplyResultStore {
	if maxRecords < 0 {
		maxRecords = 0
	}
	if maxRecords > maxApplyMetadataRecordsV1 {
		maxRecords = maxApplyMetadataRecordsV1
	}
	return &MemoryApplyResultStore{
		max:     maxRecords,
		records: make(map[raftentry.ApplyEntryID]ApplyResultRecordV1),
		byKey:   make(map[string]raftentry.ApplyEntryID),
	}
}

func (s *MemoryApplyResultStore) LookupApplyResult(id raftentry.ApplyEntryID) (ApplyResultRecordV1, bool, error) {
	if s == nil {
		return ApplyResultRecordV1{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	return cloneApplyResultRecord(record), ok, nil
}

func (s *MemoryApplyResultStore) LookupApplyResultByIdempotencyKey(key []byte) (ApplyResultRecordV1, bool, error) {
	if s == nil || len(key) == 0 {
		return ApplyResultRecordV1{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byKey[string(key)]
	if !ok {
		return ApplyResultRecordV1{}, false, nil
	}
	record, ok := s.records[id]
	return cloneApplyResultRecord(record), ok, nil
}

func (s *MemoryApplyResultStore) RecordApplyResult(record ApplyResultRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyResultRecordV1(record, true); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkCanRecordApplyResultLocked(record); err != nil {
		return err
	}
	if _, ok := s.records[record.EntryID]; ok {
		return nil
	}
	s.records[record.EntryID] = cloneApplyResultRecord(record)
	if len(record.IdempotencyKey) > 0 {
		s.byKey[string(record.IdempotencyKey)] = record.EntryID
	}
	return nil
}

func (s *MemoryApplyResultStore) CheckCanRecordApplyResult(record ApplyResultRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyResultRecordV1(record, false); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkCanRecordApplyResultLocked(record)
}

func (s *MemoryApplyResultStore) checkCanRecordApplyResultLocked(record ApplyResultRecordV1) error {
	if existing, ok := s.records[record.EntryID]; ok {
		if existing.CommandDigest != record.CommandDigest {
			return codedError(raftentry.ErrorRejectedConflictV1, "apply result digest conflict for %d/%d", record.EntryID.Term, record.EntryID.Index)
		}
		if existing.ProgressLogicalDigestV1 != (LogicalDigestV1{}) &&
			record.ProgressLogicalDigestV1 != (LogicalDigestV1{}) &&
			existing.ProgressLogicalDigestV1 != record.ProgressLogicalDigestV1 {
			return codedError(raftentry.ErrorRejectedConflictV1, "apply result progress logical digest conflict for %d/%d", record.EntryID.Term, record.EntryID.Index)
		}
		return nil
	}
	if len(record.IdempotencyKey) > 0 {
		if existingID, ok := s.byKey[string(record.IdempotencyKey)]; ok {
			existing := s.records[existingID]
			if existing.CommandDigest != record.CommandDigest {
				return codedError(raftentry.ErrorRejectedConflictV1, "idempotency key digest conflict for %d/%d", record.EntryID.Term, record.EntryID.Index)
			}
		}
	}
	if len(s.records) >= s.max {
		return codedError(raftentry.ErrorResourceExhaustedV1, "apply result store capacity %d reached", s.max)
	}
	return nil
}

func (s *MemoryApplyResultStore) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

// MemoryApplyProgressStore is a bounded fake apply-progress store for tests and
// early harness wiring. The first in-memory entry must start at index 1; later
// entries enforce strictly increasing indexes, allowing gaps, and
// non-decreasing terms.
type MemoryApplyProgressStore struct {
	mu       sync.Mutex
	max      int
	maxIndex uint64
	last     raftentry.ApplyEntryID
	records  map[raftentry.ApplyEntryID]ApplyProgressRecordV1
}

func NewMemoryApplyProgressStore(maxRecords int, maxIndex uint64) *MemoryApplyProgressStore {
	if maxRecords < 0 {
		maxRecords = 0
	}
	if maxRecords > maxApplyMetadataRecordsV1 {
		maxRecords = maxApplyMetadataRecordsV1
	}
	return &MemoryApplyProgressStore{max: maxRecords, maxIndex: maxIndex, records: make(map[raftentry.ApplyEntryID]ApplyProgressRecordV1)}
}

func (s *MemoryApplyProgressStore) CheckCanApply(id raftentry.ApplyEntryID) error {
	if s == nil {
		return nil
	}
	if err := validateApplyEntryID(id); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkCanApplyLocked(id)
}

func (s *MemoryApplyProgressStore) RecordApplied(record ApplyProgressRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyProgressRecordV1(record, true); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkCanRecordAppliedLocked(record); err != nil {
		return err
	}
	s.records[record.EntryID] = record
	if record.EntryID.Index > s.last.Index {
		s.last = record.EntryID
	}
	return nil
}

func (s *MemoryApplyProgressStore) CheckCanRecordApplied(record ApplyProgressRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyProgressRecordV1(record, false); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkCanRecordAppliedLocked(record)
}

func (s *MemoryApplyProgressStore) checkCanRecordAppliedLocked(record ApplyProgressRecordV1) error {
	if existing, ok := s.records[record.EntryID]; ok {
		if existing.CommandDigest != record.CommandDigest {
			return codedError(raftentry.ErrorRejectedConflictV1, "apply progress digest conflict for %d/%d", record.EntryID.Term, record.EntryID.Index)
		}
		if existing.LogicalDigestV1 != (LogicalDigestV1{}) &&
			record.LogicalDigestV1 != (LogicalDigestV1{}) &&
			existing.LogicalDigestV1 != record.LogicalDigestV1 {
			return codedError(raftentry.ErrorRejectedConflictV1, "apply progress logical digest conflict for %d/%d", record.EntryID.Term, record.EntryID.Index)
		}
		return nil
	}
	if err := s.checkCanApplyLocked(record.EntryID); err != nil {
		return err
	}
	if len(s.records) >= s.max {
		return codedError(raftentry.ErrorResourceExhaustedV1, "apply progress store capacity %d reached", s.max)
	}
	return nil
}

func (s *MemoryApplyProgressStore) LastApplied() (raftentry.ApplyEntryID, bool) {
	if s == nil {
		return raftentry.ApplyEntryID{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last, s.last.Index != 0
}

func (s *MemoryApplyProgressStore) LookupApplyProgress(id raftentry.ApplyEntryID) (ApplyProgressRecordV1, bool, error) {
	if s == nil {
		return ApplyProgressRecordV1{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	return record, ok, nil
}

func (s *MemoryApplyProgressStore) LastAppliedRecord() (ApplyProgressRecordV1, bool) {
	if s == nil {
		return ApplyProgressRecordV1{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last.Index == 0 {
		return ApplyProgressRecordV1{}, false
	}
	record, ok := s.records[s.last]
	return record, ok
}

func (s *MemoryApplyProgressStore) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

func (s *MemoryApplyProgressStore) checkCanApplyLocked(id raftentry.ApplyEntryID) error {
	if s.maxIndex != 0 && id.Index > s.maxIndex {
		return codedError(raftentry.ErrorResourceExhaustedV1, "apply entry index %d exceeds bound %d", id.Index, s.maxIndex)
	}
	if s.last.Index == 0 {
		if id.Index != 1 {
			return codedError(raftentry.ErrorRejectedConflictV1, "apply entry starts at index %d; want 1", id.Index)
		}
		return nil
	}
	if id.Index <= s.last.Index {
		return codedError(raftentry.ErrorRejectedConflictV1, "apply entry index %d is not after last applied %d", id.Index, s.last.Index)
	}
	if id.Term < s.last.Term {
		return codedError(raftentry.ErrorRejectedConflictV1, "apply entry term %d is below last applied term %d", id.Term, s.last.Term)
	}
	return nil
}

func validateApplyResultRecordV1(record ApplyResultRecordV1, requireCoverage bool) error {
	if err := validateApplyEntryID(record.EntryID); err != nil {
		return err
	}
	if requireCoverage && record.AppliedCommandLSN == 0 {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "apply result record has no local AppliedCommandLSN coverage")
	}
	if len(record.IdempotencyKey) == 0 {
		return codedError(raftentry.ErrorNoIdempotencyV1, "apply result record missing idempotency key")
	}
	if len(record.IdempotencyKey) > raftentry.MaxIdempotencyKeyBytesV1 {
		return codedError(raftentry.ErrorResourceExhaustedV1, "apply result idempotency key length %d exceeds %d", len(record.IdempotencyKey), raftentry.MaxIdempotencyKeyBytesV1)
	}
	if record.Result.CommandDigest != (raftentry.CommandDigestV1{}) && record.Result.CommandDigest != record.CommandDigest {
		return codedError(raftentry.ErrorRejectedConflictV1, "apply result command digest does not match record digest")
	}
	if applyResultRecordSizeV1(record) > raftentry.MaxResultRecordBytesV1 {
		return codedError(raftentry.ErrorResourceExhaustedV1, "apply result record exceeds %d bytes", raftentry.MaxResultRecordBytesV1)
	}
	return nil
}

func validateApplyProgressRecordV1(record ApplyProgressRecordV1, requireCoverage bool) error {
	if err := validateApplyEntryID(record.EntryID); err != nil {
		return err
	}
	if requireCoverage && record.AppliedCommandLSN == 0 {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "apply progress record has no local AppliedCommandLSN coverage")
	}
	return nil
}

func applyResultRecordSizeV1(record ApplyResultRecordV1) int {
	return applyEntryIDSizeV1 +
		len(record.CommandDigest) +
		uint64SizeV1 +
		encodedBytesSizeV1(record.IdempotencyKey) +
		encodedBytesSizeV1([]byte(record.Result.Status)) +
		len(record.Result.CommandDigest) +
		encodedBytesSizeV1([]byte(record.Result.DeterministicErrorCode)) +
		int64SizeV1 +
		int64SizeV1 +
		len(record.Result.ResultDigest) +
		len(record.ProgressLogicalDigestV1)
}

const (
	applyEntryIDSizeV1 = uint64SizeV1 + uint64SizeV1
	uint64SizeV1       = 8
	int64SizeV1        = 8
)

func encodedBytesSizeV1(value []byte) int {
	return uint64SizeV1 + len(value)
}

func cloneApplyResultRecord(record ApplyResultRecordV1) ApplyResultRecordV1 {
	record.IdempotencyKey = bytes.Clone(record.IdempotencyKey)
	return record
}
