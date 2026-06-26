package raftapply

import (
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

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
	EntryID       raftentry.ApplyEntryID
	CommandDigest raftentry.CommandDigestV1
	Result        raftentry.ApplyResultV1
}

// ApplyResultStore records deterministic apply results for idempotency/replay.
type ApplyResultStore interface {
	LookupApplyResult(raftentry.ApplyEntryID) (ApplyResultRecordV1, bool, error)
	CheckCanRecordApplyResult(ApplyResultRecordV1) error
	RecordApplyResult(ApplyResultRecordV1) error
}

// ApplyProgressRecordV1 records the apply-progress transition made after an
// entry has reached the selected local durability and visibility boundary.
type ApplyProgressRecordV1 struct {
	EntryID       raftentry.ApplyEntryID
	CommandDigest raftentry.CommandDigestV1
}

// ApplyProgressStore checks monotonic apply order and records durable progress.
type ApplyProgressStore interface {
	CheckCanApply(raftentry.ApplyEntryID) error
	CheckCanRecordApplied(ApplyProgressRecordV1) error
	RecordApplied(ApplyProgressRecordV1) error
	LastApplied() (raftentry.ApplyEntryID, bool)
}

// MemoryApplyResultStore is a bounded fake result/idempotency store for tests
// and early harness wiring. It is not durable.
type MemoryApplyResultStore struct {
	mu      sync.Mutex
	max     int
	records map[raftentry.ApplyEntryID]ApplyResultRecordV1
}

func NewMemoryApplyResultStore(maxRecords int) *MemoryApplyResultStore {
	if maxRecords < 0 {
		maxRecords = 0
	}
	return &MemoryApplyResultStore{max: maxRecords, records: make(map[raftentry.ApplyEntryID]ApplyResultRecordV1)}
}

func (s *MemoryApplyResultStore) LookupApplyResult(id raftentry.ApplyEntryID) (ApplyResultRecordV1, bool, error) {
	if s == nil {
		return ApplyResultRecordV1{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	return record, ok, nil
}

func (s *MemoryApplyResultStore) RecordApplyResult(record ApplyResultRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyEntryID(record.EntryID); err != nil {
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
	s.records[record.EntryID] = record
	return nil
}

func (s *MemoryApplyResultStore) CheckCanRecordApplyResult(record ApplyResultRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyEntryID(record.EntryID); err != nil {
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
		return nil
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
// early harness wiring. It enforces contiguous indexes and non-decreasing terms.
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
	if err := validateApplyEntryID(record.EntryID); err != nil {
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
	if err := validateApplyEntryID(record.EntryID); err != nil {
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
	if id.Index != s.last.Index+1 {
		return codedError(raftentry.ErrorRejectedConflictV1, "apply entry index gap: got %d after %d", id.Index, s.last.Index)
	}
	if id.Term < s.last.Term {
		return codedError(raftentry.ErrorRejectedConflictV1, "apply entry term %d is below last applied term %d", id.Term, s.last.Term)
	}
	return nil
}
