package raftapply

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const (
	durableApplyFileVersionV1      = uint32(1)
	durableApplyFileHeaderSize     = 20
	durableApplyFrameHeaderSize    = 16
	durableApplyFrameVersionV1     = uint16(1)
	durableApplyFrameKindProgress  = uint16(1)
	durableApplyFrameKindResult    = uint16(2)
	durableApplyProgressFileNameV1 = "apply-progress-v1.log"
	durableApplyResultFileNameV1   = "apply-results-v1.log"
)

var (
	durableApplyFileMagicV1  = [8]byte{'T', 'D', 'B', 'R', '3', 'A', '1', '\n'}
	durableApplyFrameMagicV1 = [4]byte{'R', '3', 'A', 'F'}
)

// DurableApplyStoreOptions configures the append-only durable R3a metadata
// stores. By default, records are fsynced as they are appended; tests and
// microbenchmarks may set DisableSync to isolate encode/index overhead.
type DurableApplyStoreOptions struct {
	MaxRecords  int
	MaxIndex    uint64
	DisableSync bool

	// AllowInitialIndexGap accepts a first TreeDB command index above 1 for
	// consensus logs that reserve lower indexes for internal membership entries.
	// Later gaps are accepted when indexes strictly increase.
	AllowInitialIndexGap bool
}

// DurableApplyProgressStorePath returns the default progress metadata file
// under a caller-owned Raft/apply metadata directory.
func DurableApplyProgressStorePath(dir string) string {
	return filepath.Join(dir, durableApplyProgressFileNameV1)
}

// DurableApplyResultStorePath returns the default result/idempotency metadata
// file under a caller-owned Raft/apply metadata directory.
func DurableApplyResultStorePath(dir string) string {
	return filepath.Join(dir, durableApplyResultFileNameV1)
}

// DurableApplyResultStore is an append-only implementation of ApplyResultStore.
// It persists records before they are inserted into the in-memory lookup maps,
// and rebuilds those maps from checksummed records when reopened.
type DurableApplyResultStore struct {
	mu         sync.Mutex
	path       string
	file       *os.File
	max        int
	syncWrites bool
	records    map[raftentry.ApplyEntryID]ApplyResultRecordV1
	byKey      map[string]raftentry.ApplyEntryID
	closed     bool
}

// OpenDurableApplyResultStore opens the default result metadata file in dir.
func OpenDurableApplyResultStore(dir string, opts DurableApplyStoreOptions) (*DurableApplyResultStore, error) {
	return OpenDurableApplyResultStoreFile(DurableApplyResultStorePath(dir), opts)
}

// OpenDurableApplyResultStoreFile opens a result metadata file at path.
func OpenDurableApplyResultStoreFile(path string, opts DurableApplyStoreOptions) (*DurableApplyResultStore, error) {
	s := &DurableApplyResultStore{
		path:       path,
		max:        normalizeDurableApplyMaxRecords(opts.MaxRecords),
		syncWrites: !opts.DisableSync,
		records:    make(map[raftentry.ApplyEntryID]ApplyResultRecordV1),
		byKey:      make(map[string]raftentry.ApplyEntryID),
	}
	file, err := openDurableApplyFile(path, durableApplyFrameKindResult, opts, func(payload []byte) error {
		record, err := decodeDurableApplyResultRecordV1(payload)
		if err != nil {
			return err
		}
		if err := validateApplyResultRecordV1(record, true); err != nil {
			return err
		}
		if err := s.replayApplyResultRecord(record); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.file = file
	return s, nil
}

func (s *DurableApplyResultStore) LookupApplyResult(id raftentry.ApplyEntryID) (ApplyResultRecordV1, bool, error) {
	if s == nil {
		return ApplyResultRecordV1{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("lookup apply result"); err != nil {
		return ApplyResultRecordV1{}, false, err
	}
	record, ok := s.records[id]
	return cloneApplyResultRecord(record), ok, nil
}

func (s *DurableApplyResultStore) LookupApplyResultByIdempotencyKey(key []byte) (ApplyResultRecordV1, bool, error) {
	if s == nil || len(key) == 0 {
		return ApplyResultRecordV1{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("lookup apply result by idempotency key"); err != nil {
		return ApplyResultRecordV1{}, false, err
	}
	id, ok := s.byKey[string(key)]
	if !ok {
		return ApplyResultRecordV1{}, false, nil
	}
	record, ok := s.records[id]
	return cloneApplyResultRecord(record), ok, nil
}

func (s *DurableApplyResultStore) CheckCanRecordApplyResult(record ApplyResultRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyResultRecordV1(record, false); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("check apply result"); err != nil {
		return err
	}
	return s.checkCanRecordApplyResultLocked(record)
}

func (s *DurableApplyResultStore) RecordApplyResult(record ApplyResultRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyResultRecordV1(record, true); err != nil {
		return err
	}
	payload, err := encodeDurableApplyResultRecordV1(record)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("record apply result"); err != nil {
		return err
	}
	if err := s.checkCanRecordApplyResultLocked(record); err != nil {
		return err
	}
	if _, ok := s.records[record.EntryID]; ok {
		return nil
	}
	if err := appendDurableApplyFrame(s.file, durableApplyFrameKindResult, payload, s.syncWrites); err != nil {
		s.poisonAfterAppendFailureLocked()
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: append result metadata %s: %v", s.path, err)
	}
	s.records[record.EntryID] = cloneApplyResultRecord(record)
	if len(record.IdempotencyKey) > 0 {
		s.byKey[string(record.IdempotencyKey)] = record.EntryID
	}
	return nil
}

func (s *DurableApplyResultStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

// Sync flushes the durable result metadata file even when the store was opened
// with DisableSync for per-append writes.
func (s *DurableApplyResultStore) Sync() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("sync apply result"); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: sync result metadata %s: %v", s.path, err)
	}
	return nil
}

func (s *DurableApplyResultStore) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

func (s *DurableApplyResultStore) replayApplyResultRecord(record ApplyResultRecordV1) error {
	if existing, ok := s.records[record.EntryID]; ok {
		if existing.CommandDigest != record.CommandDigest {
			return codedError(raftentry.ErrorRejectedConflictV1, "raftapply: durable result metadata digest conflict for %d/%d", record.EntryID.Term, record.EntryID.Index)
		}
		if existing.ProgressLogicalDigestV1 != (LogicalDigestV1{}) &&
			record.ProgressLogicalDigestV1 != (LogicalDigestV1{}) &&
			existing.ProgressLogicalDigestV1 != record.ProgressLogicalDigestV1 {
			return codedError(raftentry.ErrorRejectedConflictV1, "raftapply: durable result metadata progress logical digest conflict for %d/%d", record.EntryID.Term, record.EntryID.Index)
		}
		return nil
	}
	if err := s.checkCanRecordApplyResultLocked(record); err != nil {
		return err
	}
	s.records[record.EntryID] = cloneApplyResultRecord(record)
	if len(record.IdempotencyKey) > 0 {
		s.byKey[string(record.IdempotencyKey)] = record.EntryID
	}
	return nil
}

func (s *DurableApplyResultStore) checkCanRecordApplyResultLocked(record ApplyResultRecordV1) error {
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

func (s *DurableApplyResultStore) checkOpenLocked(op string) error {
	if s.closed || s.file == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: durable result store is closed during %s", op)
	}
	return nil
}

func (s *DurableApplyResultStore) poisonAfterAppendFailureLocked() {
	s.closed = true
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
}

// DurableApplyProgressStore is an append-only implementation of
// ApplyProgressStore. It enforces the same strictly-increasing-index and
// non-decreasing-term rules as MemoryApplyProgressStore, then fsyncs each
// accepted record by default.
type DurableApplyProgressStore struct {
	mu                   sync.Mutex
	path                 string
	file                 *os.File
	max                  int
	maxIndex             uint64
	syncWrites           bool
	allowInitialIndexGap bool
	last                 raftentry.ApplyEntryID
	records              map[raftentry.ApplyEntryID]ApplyProgressRecordV1
	closed               bool
}

// OpenDurableApplyProgressStore opens the default progress metadata file in dir.
func OpenDurableApplyProgressStore(dir string, opts DurableApplyStoreOptions) (*DurableApplyProgressStore, error) {
	return OpenDurableApplyProgressStoreFile(DurableApplyProgressStorePath(dir), opts)
}

// OpenDurableApplyProgressStoreFile opens a progress metadata file at path.
func OpenDurableApplyProgressStoreFile(path string, opts DurableApplyStoreOptions) (*DurableApplyProgressStore, error) {
	s := &DurableApplyProgressStore{
		path:                 path,
		max:                  normalizeDurableApplyMaxRecords(opts.MaxRecords),
		maxIndex:             opts.MaxIndex,
		syncWrites:           !opts.DisableSync,
		allowInitialIndexGap: opts.AllowInitialIndexGap,
		records:              make(map[raftentry.ApplyEntryID]ApplyProgressRecordV1),
	}
	file, err := openDurableApplyFile(path, durableApplyFrameKindProgress, opts, func(payload []byte) error {
		record, err := decodeDurableApplyProgressRecordV1(payload)
		if err != nil {
			return err
		}
		if err := validateApplyProgressRecordV1(record, true); err != nil {
			return err
		}
		if err := s.replayApplyProgressRecord(record); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.file = file
	return s, nil
}

func (s *DurableApplyProgressStore) CheckCanApply(id raftentry.ApplyEntryID) error {
	if s == nil {
		return nil
	}
	if err := validateApplyEntryID(id); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("check apply"); err != nil {
		return err
	}
	return s.checkCanApplyLocked(id)
}

func (s *DurableApplyProgressStore) CheckCanRecordApplied(record ApplyProgressRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyProgressRecordV1(record, false); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("check applied progress"); err != nil {
		return err
	}
	return s.checkCanRecordAppliedLocked(record)
}

func (s *DurableApplyProgressStore) RecordApplied(record ApplyProgressRecordV1) error {
	if s == nil {
		return nil
	}
	if err := validateApplyProgressRecordV1(record, true); err != nil {
		return err
	}
	payload, err := encodeDurableApplyProgressRecordV1(record)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("record applied progress"); err != nil {
		return err
	}
	if err := s.checkCanRecordAppliedLocked(record); err != nil {
		return err
	}
	if _, ok := s.records[record.EntryID]; ok {
		return nil
	}
	if err := appendDurableApplyFrame(s.file, durableApplyFrameKindProgress, payload, s.syncWrites); err != nil {
		s.poisonAfterAppendFailureLocked()
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: append progress metadata %s: %v", s.path, err)
	}
	s.records[record.EntryID] = record
	if record.EntryID.Index > s.last.Index {
		s.last = record.EntryID
	}
	return nil
}

func (s *DurableApplyProgressStore) LastApplied() (raftentry.ApplyEntryID, bool) {
	if s == nil {
		return raftentry.ApplyEntryID{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last, s.last.Index != 0
}

func (s *DurableApplyProgressStore) LookupApplyProgress(id raftentry.ApplyEntryID) (ApplyProgressRecordV1, bool, error) {
	if s == nil {
		return ApplyProgressRecordV1{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("lookup apply progress"); err != nil {
		return ApplyProgressRecordV1{}, false, err
	}
	record, ok := s.records[id]
	return record, ok, nil
}

func (s *DurableApplyProgressStore) LastAppliedRecord() (ApplyProgressRecordV1, bool) {
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

func (s *DurableApplyProgressStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

// Sync flushes the durable progress metadata file even when the store was opened
// with DisableSync for per-append writes.
func (s *DurableApplyProgressStore) Sync() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked("sync applied progress"); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: sync progress metadata %s: %v", s.path, err)
	}
	return nil
}

func (s *DurableApplyProgressStore) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

func (s *DurableApplyProgressStore) replayApplyProgressRecord(record ApplyProgressRecordV1) error {
	if existing, ok := s.records[record.EntryID]; ok {
		if existing.CommandDigest != record.CommandDigest {
			return codedError(raftentry.ErrorRejectedConflictV1, "raftapply: durable progress metadata digest conflict for %d/%d", record.EntryID.Term, record.EntryID.Index)
		}
		if existing.LogicalDigestV1 != (LogicalDigestV1{}) &&
			record.LogicalDigestV1 != (LogicalDigestV1{}) &&
			existing.LogicalDigestV1 != record.LogicalDigestV1 {
			return codedError(raftentry.ErrorRejectedConflictV1, "raftapply: durable progress metadata logical digest conflict for %d/%d", record.EntryID.Term, record.EntryID.Index)
		}
		return nil
	}
	if err := s.checkCanRecordAppliedLocked(record); err != nil {
		return err
	}
	s.records[record.EntryID] = record
	if record.EntryID.Index > s.last.Index {
		s.last = record.EntryID
	}
	return nil
}

func (s *DurableApplyProgressStore) checkCanRecordAppliedLocked(record ApplyProgressRecordV1) error {
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

func (s *DurableApplyProgressStore) checkCanApplyLocked(id raftentry.ApplyEntryID) error {
	if s.maxIndex != 0 && id.Index > s.maxIndex {
		return codedError(raftentry.ErrorResourceExhaustedV1, "apply entry index %d exceeds bound %d", id.Index, s.maxIndex)
	}
	if s.last.Index == 0 {
		if id.Index != 1 && !s.allowInitialIndexGap {
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

func (s *DurableApplyProgressStore) checkOpenLocked(op string) error {
	if s.closed || s.file == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: durable progress store is closed during %s", op)
	}
	return nil
}

func (s *DurableApplyProgressStore) poisonAfterAppendFailureLocked() {
	s.closed = true
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
}

func normalizeDurableApplyMaxRecords(maxRecords int) int {
	if maxRecords <= 0 || maxRecords > maxApplyMetadataRecordsV1 {
		return maxApplyMetadataRecordsV1
	}
	return maxRecords
}

func openDurableApplyFile(path string, kind uint16, opts DurableApplyStoreOptions, replay func([]byte) error) (*os.File, error) {
	if path == "" {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: durable metadata path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: create durable metadata dir for %s: %v", path, err)
	}
	existed := false
	if _, err := os.Stat(path); err == nil {
		existed = true
	} else if !os.IsNotExist(err) {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: stat durable metadata %s before open: %v", path, err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: open durable metadata %s: %v", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: stat durable metadata %s: %v", path, err)
	}
	if info.Size() == 0 {
		if existed {
			_ = file.Close()
			return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: existing durable metadata %s is zero-length", path)
		}
		header := durableApplyFileHeader(kind)
		if err := writeFull(file, header[:]); err != nil {
			_ = file.Close()
			return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: initialize durable metadata %s: %v", path, err)
		}
		if !opts.DisableSync {
			if err := file.Sync(); err != nil {
				_ = file.Close()
				return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: sync durable metadata header %s: %v", path, err)
			}
			if err := syncDir(filepath.Dir(path)); err != nil {
				_ = file.Close()
				return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: sync durable metadata dir %s: %v", filepath.Dir(path), err)
			}
		}
		return file, nil
	}
	if err := replayDurableApplyFile(file, path, kind, replay); err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: seek durable metadata %s: %v", path, err)
	}
	return file, nil
}

func replayDurableApplyFile(file *os.File, path string, wantKind uint16, replay func([]byte) error) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: seek durable metadata %s: %v", path, err)
	}
	header := make([]byte, durableApplyFileHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: read durable metadata header %s: %v", path, err)
	}
	if err := verifyDurableApplyFileHeader(header, wantKind); err != nil {
		return err
	}
	offset := int64(durableApplyFileHeaderSize)
	frameHeader := make([]byte, durableApplyFrameHeaderSize)
	for {
		n, err := io.ReadFull(file, frameHeader)
		if err == io.EOF && n == 0 {
			return nil
		}
		if err == io.ErrUnexpectedEOF || (err == io.EOF && n != 0) {
			return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: truncated durable metadata frame header at %s:%d", path, offset)
		}
		if err != nil {
			return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: read durable metadata frame header at %s:%d: %v", path, offset, err)
		}
		payloadLen, err := verifyDurableApplyFrameHeader(frameHeader, wantKind, path, offset)
		if err != nil {
			return err
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(file, payload); err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: truncated durable metadata frame payload at %s:%d", path, offset)
			}
			return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: read durable metadata frame payload at %s:%d: %v", path, offset, err)
		}
		wantCRC := binary.LittleEndian.Uint32(frameHeader[12:16])
		if gotCRC := durableApplyFrameChecksum(frameHeader[:12], payload); gotCRC != wantCRC {
			return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: durable metadata frame checksum mismatch at %s:%d", path, offset)
		}
		if err := replay(payload); err != nil {
			return err
		}
		offset += int64(durableApplyFrameHeaderSize + payloadLen)
	}
}

func durableApplyFileHeader(kind uint16) [durableApplyFileHeaderSize]byte {
	var header [durableApplyFileHeaderSize]byte
	copy(header[0:8], durableApplyFileMagicV1[:])
	binary.LittleEndian.PutUint32(header[8:12], durableApplyFileVersionV1)
	binary.LittleEndian.PutUint16(header[12:14], kind)
	binary.LittleEndian.PutUint16(header[14:16], durableApplyFileHeaderSize)
	binary.LittleEndian.PutUint32(header[16:20], crc32.ChecksumIEEE(header[:16]))
	return header
}

func verifyDurableApplyFileHeader(header []byte, wantKind uint16) error {
	if len(header) != durableApplyFileHeaderSize {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: invalid durable metadata header size %d", len(header))
	}
	if !bytes.Equal(header[0:8], durableApplyFileMagicV1[:]) {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: corrupt durable metadata magic")
	}
	if gotCRC := crc32.ChecksumIEEE(header[:16]); gotCRC != binary.LittleEndian.Uint32(header[16:20]) {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: corrupt durable metadata header checksum")
	}
	if version := binary.LittleEndian.Uint32(header[8:12]); version != durableApplyFileVersionV1 {
		return codedError(raftentry.ErrorUnsupportedVersionV1, "raftapply: unsupported durable metadata version %d", version)
	}
	if kind := binary.LittleEndian.Uint16(header[12:14]); kind != wantKind {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: durable metadata kind %d does not match expected %d", kind, wantKind)
	}
	if headerLen := binary.LittleEndian.Uint16(header[14:16]); headerLen != durableApplyFileHeaderSize {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: durable metadata header length %d does not match expected %d", headerLen, durableApplyFileHeaderSize)
	}
	return nil
}

func appendDurableApplyFrame(file *os.File, kind uint16, payload []byte, syncWrite bool) error {
	if len(payload) > raftentry.MaxResultRecordBytesV1 {
		return codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: durable metadata payload %d exceeds %d", len(payload), raftentry.MaxResultRecordBytesV1)
	}
	var header [durableApplyFrameHeaderSize]byte
	copy(header[0:4], durableApplyFrameMagicV1[:])
	binary.LittleEndian.PutUint16(header[4:6], kind)
	binary.LittleEndian.PutUint16(header[6:8], durableApplyFrameVersionV1)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[12:16], durableApplyFrameChecksum(header[:12], payload))
	if err := writeFull(file, header[:]); err != nil {
		return err
	}
	if err := writeFull(file, payload); err != nil {
		return err
	}
	if syncWrite {
		return file.Sync()
	}
	return nil
}

func verifyDurableApplyFrameHeader(header []byte, wantKind uint16, path string, offset int64) (int, error) {
	if len(header) != durableApplyFrameHeaderSize {
		return 0, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: invalid durable metadata frame header size %d", len(header))
	}
	if !bytes.Equal(header[0:4], durableApplyFrameMagicV1[:]) {
		return 0, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: corrupt durable metadata frame magic at %s:%d", path, offset)
	}
	if kind := binary.LittleEndian.Uint16(header[4:6]); kind != wantKind {
		return 0, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: durable metadata frame kind %d does not match expected %d at %s:%d", kind, wantKind, path, offset)
	}
	if version := binary.LittleEndian.Uint16(header[6:8]); version != durableApplyFrameVersionV1 {
		return 0, codedError(raftentry.ErrorUnsupportedVersionV1, "raftapply: unsupported durable metadata frame version %d at %s:%d", version, path, offset)
	}
	payloadLen := binary.LittleEndian.Uint32(header[8:12])
	if payloadLen > raftentry.MaxResultRecordBytesV1 {
		return 0, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: durable metadata frame payload %d exceeds %d at %s:%d", payloadLen, raftentry.MaxResultRecordBytesV1, path, offset)
	}
	return int(payloadLen), nil
}

func durableApplyFrameChecksum(headerPrefix []byte, payload []byte) uint32 {
	h := crc32.NewIEEE()
	_, _ = h.Write(headerPrefix)
	_, _ = h.Write(payload)
	return h.Sum32()
}

func encodeDurableApplyProgressRecordV1(record ApplyProgressRecordV1) ([]byte, error) {
	var dst []byte
	dst = appendApplyEntryID(dst, record.EntryID)
	dst = append(dst, record.CommandDigest[:]...)
	dst = appendU64(dst, record.AppliedCommandLSN)
	dst = append(dst, record.LogicalDigestV1[:]...)
	return dst, nil
}

func decodeDurableApplyProgressRecordV1(payload []byte) (ApplyProgressRecordV1, error) {
	r := durableApplyPayloadReader{buf: payload}
	id, err := r.applyEntryID()
	if err != nil {
		return ApplyProgressRecordV1{}, err
	}
	digest, err := r.digest()
	if err != nil {
		return ApplyProgressRecordV1{}, err
	}
	lsn, err := r.u64()
	if err != nil {
		return ApplyProgressRecordV1{}, err
	}
	var logical LogicalDigestV1
	if r.remaining() > 0 {
		logical, err = r.logicalDigest()
		if err != nil {
			return ApplyProgressRecordV1{}, err
		}
	}
	if err := r.done(); err != nil {
		return ApplyProgressRecordV1{}, err
	}
	return ApplyProgressRecordV1{EntryID: id, CommandDigest: digest, AppliedCommandLSN: lsn, LogicalDigestV1: logical}, nil
}

func encodeDurableApplyResultRecordV1(record ApplyResultRecordV1) ([]byte, error) {
	var dst []byte
	dst = appendApplyEntryID(dst, record.EntryID)
	dst = append(dst, record.CommandDigest[:]...)
	dst = appendU64(dst, record.AppliedCommandLSN)
	dst = appendBytes(dst, record.IdempotencyKey)
	dst = appendBytes(dst, []byte(record.Result.Status))
	dst = append(dst, record.Result.CommandDigest[:]...)
	dst = appendBytes(dst, []byte(record.Result.DeterministicErrorCode))
	dst = appendI64(dst, record.Result.AffectedCount)
	dst = appendI64(dst, record.Result.MatchedCount)
	dst = append(dst, record.Result.ResultDigest[:]...)
	dst = append(dst, record.ProgressLogicalDigestV1[:]...)
	if len(dst) > raftentry.MaxResultRecordBytesV1 {
		return nil, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: durable result record payload %d exceeds %d", len(dst), raftentry.MaxResultRecordBytesV1)
	}
	return dst, nil
}

func decodeDurableApplyResultRecordV1(payload []byte) (ApplyResultRecordV1, error) {
	r := durableApplyPayloadReader{buf: payload}
	id, err := r.applyEntryID()
	if err != nil {
		return ApplyResultRecordV1{}, err
	}
	digest, err := r.digest()
	if err != nil {
		return ApplyResultRecordV1{}, err
	}
	lsn, err := r.u64()
	if err != nil {
		return ApplyResultRecordV1{}, err
	}
	key, err := r.bytes(raftentry.MaxIdempotencyKeyBytesV1)
	if err != nil {
		return ApplyResultRecordV1{}, err
	}
	status, err := r.bytes(raftentry.MaxResultRecordBytesV1)
	if err != nil {
		return ApplyResultRecordV1{}, err
	}
	resultDigest, err := r.digest()
	if err != nil {
		return ApplyResultRecordV1{}, err
	}
	code, err := r.bytes(raftentry.MaxResultRecordBytesV1)
	if err != nil {
		return ApplyResultRecordV1{}, err
	}
	affected, err := r.i64()
	if err != nil {
		return ApplyResultRecordV1{}, err
	}
	var matched int64
	switch r.remaining() {
	case 32, 64:
		// Legacy records omitted matched_count; preserve compatibility by
		// treating it as zero before reading result/progress digests.
	default:
		matched, err = r.i64()
		if err != nil {
			return ApplyResultRecordV1{}, err
		}
	}
	logicalDigest, err := r.digest()
	if err != nil {
		return ApplyResultRecordV1{}, err
	}
	var progressLogicalDigest LogicalDigestV1
	if r.remaining() > 0 {
		progressLogicalDigest, err = r.logicalDigest()
		if err != nil {
			return ApplyResultRecordV1{}, err
		}
	}
	if err := r.done(); err != nil {
		return ApplyResultRecordV1{}, err
	}
	return ApplyResultRecordV1{
		EntryID:                 id,
		CommandDigest:           digest,
		IdempotencyKey:          key,
		AppliedCommandLSN:       lsn,
		ProgressLogicalDigestV1: progressLogicalDigest,
		Result: raftentry.ApplyResultV1{
			Status:                 raftentry.ApplyStatusV1(status),
			CommandDigest:          resultDigest,
			DeterministicErrorCode: raftentry.DeterministicErrorCodeV1(code),
			AffectedCount:          affected,
			MatchedCount:           matched,
			ResultDigest:           logicalDigest,
		},
	}, nil
}

type durableApplyPayloadReader struct {
	buf []byte
	off int
}

func (r *durableApplyPayloadReader) applyEntryID() (raftentry.ApplyEntryID, error) {
	term, err := r.u64()
	if err != nil {
		return raftentry.ApplyEntryID{}, err
	}
	index, err := r.u64()
	if err != nil {
		return raftentry.ApplyEntryID{}, err
	}
	return raftentry.ApplyEntryID{Term: term, Index: index}, nil
}

func (r *durableApplyPayloadReader) digest() (raftentry.CommandDigestV1, error) {
	b, err := r.fixed(32)
	if err != nil {
		return raftentry.CommandDigestV1{}, err
	}
	var out raftentry.CommandDigestV1
	copy(out[:], b)
	return out, nil
}

func (r *durableApplyPayloadReader) logicalDigest() (LogicalDigestV1, error) {
	b, err := r.fixed(32)
	if err != nil {
		return LogicalDigestV1{}, err
	}
	var out LogicalDigestV1
	copy(out[:], b)
	return out, nil
}

func (r *durableApplyPayloadReader) u64() (uint64, error) {
	b, err := r.fixed(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *durableApplyPayloadReader) i64() (int64, error) {
	v, err := r.u64()
	return int64(v), err
}

func (r *durableApplyPayloadReader) bytes(max int) ([]byte, error) {
	n64, err := r.u64()
	if err != nil {
		return nil, err
	}
	if n64 > uint64(max) {
		return nil, codedError(raftentry.ErrorResourceExhaustedV1, "raftapply: durable metadata byte field %d exceeds %d", n64, max)
	}
	if n64 > uint64(len(r.buf)-r.off) {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: truncated durable metadata byte field")
	}
	out := bytes.Clone(r.buf[r.off : r.off+int(n64)])
	r.off += int(n64)
	return out, nil
}

func (r *durableApplyPayloadReader) fixed(n int) ([]byte, error) {
	if n < 0 || n > len(r.buf)-r.off {
		return nil, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: truncated durable metadata payload")
	}
	out := r.buf[r.off : r.off+n]
	r.off += n
	return out, nil
}

func (r *durableApplyPayloadReader) done() error {
	if r.off != len(r.buf) {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: durable metadata payload has %d trailing bytes", len(r.buf)-r.off)
	}
	return nil
}

func (r *durableApplyPayloadReader) remaining() int {
	return len(r.buf) - r.off
}

func appendApplyEntryID(dst []byte, id raftentry.ApplyEntryID) []byte {
	dst = appendU64(dst, id.Term)
	return appendU64(dst, id.Index)
}

func appendBytes(dst []byte, value []byte) []byte {
	dst = appendU64(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendU64(dst []byte, value uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	return append(dst, buf[:]...)
}

func appendI64(dst []byte, value int64) []byte {
	return appendU64(dst, uint64(value))
}

func writeFull(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func syncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync dir %s: %w", path, err)
	}
	return nil
}
