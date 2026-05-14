package collectionwal

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
)

// Stats owns process-local collection WAL observability counters and gauges.
type Stats struct {
	appendTxns                  atomic.Uint64
	appendDocs                  atomic.Uint64
	appendBytes                 atomic.Uint64
	appendSideRefs              atomic.Uint64
	appendLatencyNS             atomic.Uint64
	appendFlushNS               atomic.Uint64
	appendSyncNS                atomic.Uint64
	appendFailures              atomic.Uint64
	appendFailuresByCategory    [errorCategoryCount]atomic.Uint64
	recoveryOpens               atomic.Uint64
	recoveryDurationLastMS      atomic.Uint64
	recoveryDurationNS          atomic.Uint64
	recoveryReplay              atomic.Uint64
	recoveryTailSkip            atomic.Uint64
	recoveryWatermarkSkip       atomic.Uint64
	recoveryBlocked             atomic.Uint64
	recoveryHardFailure         atomic.Uint64
	recoveryFailuresByCategory  [errorCategoryCount]atomic.Uint64
	recoveryLastFailureCategory atomic.Uint64
	recoveryLastFailureWALLSN   atomic.Uint64
	recoveryLastFailureSeq      atomic.Uint64
	recoveryArtifactsWritten    atomic.Uint64
	recoveryArtifactWriteFail   atomic.Uint64
	retainedSegments            atomic.Uint64
	retainedBytes               atomic.Uint64
	debtScanFailure             atomic.Uint64
	cleanupFailure              atomic.Uint64
	valueLogGCBlockerBytes      atomic.Uint64
	valueLogGCBlockerSegs       atomic.Uint64
}

// StatsSnapshot is a reset-safe process-local view of collection WAL counters
// and gauges. Total counters are monotonic within one DB process.
type StatsSnapshot struct {
	AppendSuccess                uint64
	AppendFailure                uint64
	AppendTxnsTotal              uint64
	AppendDocsTotal              uint64
	AppendBytesTotal             uint64
	AppendSideRefsTotal          uint64
	AppendLatencyNSTotal         uint64
	AppendFlushNSTotal           uint64
	AppendSyncNSTotal            uint64
	AppendFailuresTotal          uint64
	AppendFailuresByCategory     map[ErrorCategory]uint64
	RecoveryOpensTotal           uint64
	RecoveryDurationLastMS       uint64
	RecoveryDurationNSTotal      uint64
	RecoveryReplay               uint64
	RecoveryTailSkip             uint64
	RecoverySkip                 uint64
	RecoveryWatermarkSkip        uint64
	RecoveryBlockedTotal         uint64
	RecoveryHardFailure          uint64
	RecoveryFailuresByCategory   map[ErrorCategory]uint64
	RecoveryLastFailureCategory  ErrorCategory
	RecoveryLastFailureWALLSN    uint64
	RecoveryLastFailureSeq       uint64
	RecoveryArtifactsWritten     uint64
	RecoveryArtifactWriteFailure uint64
	RetainedSegments             uint64
	RetainedBytes                uint64
	RetainedDebtScanFailure      uint64
	CleanupFailure               uint64
	ValueLogGCBlockerSegments    uint64
	ValueLogGCBlockerBytes       uint64
}

// Snapshot returns a point-in-time process-local stats snapshot.
func (s *Stats) Snapshot() StatsSnapshot {
	if s == nil {
		return StatsSnapshot{}
	}
	appendTxns := s.appendTxns.Load()
	appendFailures := s.appendFailures.Load()
	recoveryWatermarkSkip := s.recoveryWatermarkSkip.Load()
	appendFailuresByCategory := snapshotCategoryCounters(&s.appendFailuresByCategory)
	recoveryFailuresByCategory := snapshotCategoryCounters(&s.recoveryFailuresByCategory)
	return StatsSnapshot{
		AppendSuccess:                appendTxns,
		AppendFailure:                appendFailures,
		AppendTxnsTotal:              appendTxns,
		AppendDocsTotal:              s.appendDocs.Load(),
		AppendBytesTotal:             s.appendBytes.Load(),
		AppendSideRefsTotal:          s.appendSideRefs.Load(),
		AppendLatencyNSTotal:         s.appendLatencyNS.Load(),
		AppendFlushNSTotal:           s.appendFlushNS.Load(),
		AppendSyncNSTotal:            s.appendSyncNS.Load(),
		AppendFailuresTotal:          appendFailures,
		AppendFailuresByCategory:     appendFailuresByCategory,
		RecoveryOpensTotal:           s.recoveryOpens.Load(),
		RecoveryDurationLastMS:       s.recoveryDurationLastMS.Load(),
		RecoveryDurationNSTotal:      s.recoveryDurationNS.Load(),
		RecoveryReplay:               s.recoveryReplay.Load(),
		RecoveryTailSkip:             s.recoveryTailSkip.Load(),
		RecoverySkip:                 recoveryWatermarkSkip,
		RecoveryWatermarkSkip:        recoveryWatermarkSkip,
		RecoveryBlockedTotal:         s.recoveryBlocked.Load(),
		RecoveryHardFailure:          s.recoveryHardFailure.Load(),
		RecoveryFailuresByCategory:   recoveryFailuresByCategory,
		RecoveryLastFailureCategory:  categoryFromMetricIndex(s.recoveryLastFailureCategory.Load()),
		RecoveryLastFailureWALLSN:    s.recoveryLastFailureWALLSN.Load(),
		RecoveryLastFailureSeq:       s.recoveryLastFailureSeq.Load(),
		RecoveryArtifactsWritten:     s.recoveryArtifactsWritten.Load(),
		RecoveryArtifactWriteFailure: s.recoveryArtifactWriteFail.Load(),
		RetainedSegments:             s.retainedSegments.Load(),
		RetainedBytes:                s.retainedBytes.Load(),
		RetainedDebtScanFailure:      s.debtScanFailure.Load(),
		CleanupFailure:               s.cleanupFailure.Load(),
		ValueLogGCBlockerSegments:    s.valueLogGCBlockerSegs.Load(),
		ValueLogGCBlockerBytes:       s.valueLogGCBlockerBytes.Load(),
	}
}

func (s *Stats) RecordAppendSuccess(txn Transaction, result AppendResult) {
	if s != nil {
		s.appendTxns.Add(1)
		s.appendDocs.Add(statsDocumentCount(txn))
		s.appendBytes.Add(uint64NonNegative(result.Length))
		s.appendSideRefs.Add(uint64(txn.SideRefCount))
	}
}

func (s *Stats) RecordAppendFailure(err error) {
	if s != nil {
		s.appendFailures.Add(1)
		recordCategoryCounter(&s.appendFailuresByCategory, err)
	}
}

func (s *Stats) RecordRecoveryOpen(durationNS uint64) {
	if s == nil {
		return
	}
	s.recoveryOpens.Add(1)
	s.recoveryDurationNS.Add(durationNS)
	s.recoveryDurationLastMS.Store(durationNS / uint64(1_000_000))
}

func (s *Stats) RecordRecoveryReplay() {
	if s != nil {
		s.recoveryReplay.Add(1)
	}
}

func (s *Stats) RecordRecoveryTailSkip() {
	if s != nil {
		s.recoveryTailSkip.Add(1)
	}
}

func (s *Stats) RecordRecoveryWatermarkSkip() {
	if s != nil {
		s.recoveryWatermarkSkip.Add(1)
	}
}

func (s *Stats) RecordRecoveryBlocked(err error) {
	if s != nil {
		s.recoveryBlocked.Add(1)
	}
}

func (s *Stats) RecordRecoveryHardFailure(err error) {
	if s != nil {
		s.recoveryHardFailure.Add(1)
		if category, ok := recordCategoryCounter(&s.recoveryFailuresByCategory, err); ok {
			s.recoveryLastFailureCategory.Store(metricIndexForCategory(category))
		}
		if wallsn, seq, ok := recoveryFailurePosition(err); ok {
			s.recoveryLastFailureWALLSN.Store(wallsn)
			s.recoveryLastFailureSeq.Store(seq)
		}
	}
}

func (s *Stats) RecordRecoveryArtifactWritten() {
	if s != nil {
		s.recoveryArtifactsWritten.Add(1)
	}
}

func (s *Stats) RecordRecoveryArtifactWriteFailure() {
	if s != nil {
		s.recoveryArtifactWriteFail.Add(1)
	}
}

func (s *Stats) RecordRetainedDebtScanFailure() {
	if s != nil {
		s.debtScanFailure.Add(1)
	}
}

func (s *Stats) RecordCleanupFailure() {
	if s != nil {
		s.cleanupFailure.Add(1)
	}
}

func (s *Stats) SetRetainedDebt(segments, bytes uint64) {
	if s == nil {
		return
	}
	s.retainedSegments.Store(segments)
	s.retainedBytes.Store(bytes)
}

func (s *Stats) SetValueLogGCBlockers(segments, bytes uint64) {
	if s == nil {
		return
	}
	s.valueLogGCBlockerSegs.Store(segments)
	s.valueLogGCBlockerBytes.Store(bytes)
}

func statsDocumentCount(txn Transaction) uint64 {
	for _, section := range txn.Sections {
		if section.Type != SectionTypeStats || len(section.Data) < 16 {
			continue
		}
		if string(section.Data[:8]) != "TDBCWSS\x01" {
			continue
		}
		return binary.LittleEndian.Uint64(section.Data[8:16])
	}
	return 0
}

func uint64NonNegative(v int64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

func snapshotCategoryCounters(counters *[errorCategoryCount]atomic.Uint64) map[ErrorCategory]uint64 {
	out := make(map[ErrorCategory]uint64, len(allErrorCategories))
	if counters == nil {
		for _, category := range allErrorCategories {
			out[category] = 0
		}
		return out
	}
	for i, category := range allErrorCategories {
		out[category] = counters[i].Load()
	}
	return out
}

func recordCategoryCounter(counters *[errorCategoryCount]atomic.Uint64, err error) (ErrorCategory, bool) {
	category := CategoryOf(err)
	if category == "" || counters == nil {
		return "", false
	}
	idx, ok := errorCategoryIndex(category)
	if !ok {
		return "", false
	}
	counters[idx].Add(1)
	return category, true
}

func errorCategoryIndex(category ErrorCategory) (int, bool) {
	for i, known := range allErrorCategories {
		if known == category {
			return i, true
		}
	}
	return 0, false
}

func metricIndexForCategory(category ErrorCategory) uint64 {
	idx, ok := errorCategoryIndex(category)
	if !ok {
		return 0
	}
	return uint64(idx + 1)
}

func categoryFromMetricIndex(value uint64) ErrorCategory {
	if value == 0 || value > uint64(len(allErrorCategories)) {
		return ""
	}
	return allErrorCategories[value-1]
}

func recoveryFailurePosition(err error) (uint64, uint64, bool) {
	var typed *CollectionWALError
	if !errors.As(err, &typed) || typed == nil {
		return 0, 0, false
	}
	return typed.WALLSN, typed.CollectionSeq, typed.WALLSN != 0 || typed.CollectionSeq != 0
}
