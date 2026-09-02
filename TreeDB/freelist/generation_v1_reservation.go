package freelist

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/page"
)

type CandidateIDV1 [16]byte

type ReservationKindV1 uint8

const (
	ReservationReusedData ReservationKindV1 = iota + 1
	ReservationAppendedData
	ReservationTargetMetadata
	ReservationPendingMetadataRetirement
	ReservationAbandonedAppend
)

type ReservationExtentV1 struct {
	StartPageID            uint64
	Count                  uint32
	Kind                   ReservationKindV1
	LastReachableCommitSeq uint64
}

type ReservationRecordV1 struct {
	pageID       uint64
	pageIDs      []uint64
	CandidateID  CandidateIDV1
	GenerationID uint64
	BaseID       uint64
	BaseDigest   [32]byte
	Extents      []ReservationExtentV1
	digest       [32]byte
}

func (r ReservationRecordV1) PageID() uint64    { return r.pageID }
func (r ReservationRecordV1) PageIDs() []uint64 { return append([]uint64(nil), r.pageIDs...) }
func (r ReservationRecordV1) Digest() [32]byte  { return r.digest }
func (r ReservationRecordV1) Entries() []ReservationExtentV1 {
	return append([]ReservationExtentV1(nil), r.Extents...)
}

func normalizeExtents(extents []ReservationExtentV1) ([]ReservationExtentV1, error) {
	out := append([]ReservationExtentV1(nil), extents...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartPageID != out[j].StartPageID {
			return out[i].StartPageID < out[j].StartPageID
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].LastReachableCommitSeq < out[j].LastReachableCommitSeq
	})
	merged := out[:0]
	for _, extent := range out {
		if extent.StartPageID < 2 || extent.Count == 0 || extent.Kind < ReservationReusedData || extent.Kind > ReservationAbandonedAppend {
			return nil, ErrGenerationFormat
		}
		end := extent.StartPageID + uint64(extent.Count)
		if end < extent.StartPageID {
			return nil, ErrGenerationFormat
		}
		if len(merged) > 0 {
			last := &merged[len(merged)-1]
			lastEnd := last.StartPageID + uint64(last.Count)
			if lastEnd > extent.StartPageID {
				return nil, ErrGenerationFormat
			}
			if last.Kind == extent.Kind && last.LastReachableCommitSeq == extent.LastReachableCommitSeq && lastEnd == extent.StartPageID && uint64(last.Count)+uint64(extent.Count) <= uint64(^uint32(0)) {
				last.Count += extent.Count
				continue
			}
		}
		merged = append(merged, extent)
	}
	return merged, nil
}

const reservationEntriesPerPage = (page.PageSize - reservationHeaderSize) / reservationEntrySize

func encodeReservationPages(id uint64, record ReservationRecordV1) ([][]byte, ReservationRecordV1, error) {
	extents, err := normalizeExtents(record.Extents)
	if err != nil {
		return nil, ReservationRecordV1{}, err
	}
	record.Extents = extents
	return encodeNormalizedReservationPages(id, record)
}

func encodeNormalizedReservationPages(id uint64, record ReservationRecordV1) ([][]byte, ReservationRecordV1, error) {
	extents := record.Extents
	pageCount := (len(extents) + reservationEntriesPerPage - 1) / reservationEntriesPerPage
	if pageCount == 0 {
		pageCount = 1
	}
	if pageCount > int(^uint16(0)) || id > ^uint64(0)-uint64(pageCount) {
		return nil, ReservationRecordV1{}, fmt.Errorf("%w: reservation record page count %d", ErrGenerationFormat, pageCount)
	}
	record.pageID = id
	record.pageIDs = make([]uint64, pageCount)
	pages := make([][]byte, pageCount)
	for pageIndex := range pages {
		pageID := id + uint64(pageIndex)
		record.pageIDs[pageIndex] = pageID
		start := pageIndex * reservationEntriesPerPage
		end := min(start+reservationEntriesPerPage, len(extents))
		entries := extents[start:end]
		b := make([]byte, page.PageSize)
		encodePageHeader(b, pageID, page.PageTypeFreelistReservation, uint16(len(entries)))
		copy(b[16:24], reservationMagic[:])
		binary.LittleEndian.PutUint16(b[24:26], 1)
		binary.LittleEndian.PutUint16(b[26:28], reservationHeaderSize)
		binary.LittleEndian.PutUint16(b[28:30], reservationEntrySize)
		copy(b[32:48], record.CandidateID[:])
		binary.LittleEndian.PutUint64(b[48:56], record.GenerationID)
		binary.LittleEndian.PutUint64(b[56:64], record.BaseID)
		if pageIndex+1 < pageCount {
			binary.LittleEndian.PutUint64(b[64:72], pageID+1)
		}
		binary.LittleEndian.PutUint32(b[72:76], uint32(len(entries)))
		binary.LittleEndian.PutUint16(b[76:78], 1)
		binary.LittleEndian.PutUint16(b[78:80], uint16(pageCount))
		copy(b[80:112], record.BaseDigest[:])
		binary.LittleEndian.PutUint32(b[144:148], uint32(pageIndex))
		binary.LittleEndian.PutUint32(b[148:152], uint32(len(extents)))
		o := reservationHeaderSize
		for _, extent := range entries {
			binary.LittleEndian.PutUint64(b[o:o+8], extent.StartPageID)
			binary.LittleEndian.PutUint32(b[o+8:o+12], extent.Count)
			b[o+12] = byte(extent.Kind)
			binary.LittleEndian.PutUint64(b[o+16:o+24], extent.LastReachableCommitSeq)
			o += reservationEntrySize
		}
		pages[pageIndex] = b
	}
	record.digest = reservationDigest(pages)
	for _, b := range pages {
		copy(b[112:144], record.digest[:])
		finishPage(b)
	}
	return pages, record, nil
}

func reservationDigest(pages [][]byte) [32]byte {
	h := sha256.New()
	for _, b := range pages {
		canonical := append([]byte(nil), b...)
		for i := 8; i < 12; i++ {
			canonical[i] = 0
		}
		for i := 112; i < 144; i++ {
			canonical[i] = 0
		}
		_, _ = h.Write(canonical)
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func loadReservationRecord(src PageSource, id, highWater uint64) (ReservationRecordV1, error) {
	var pages [][]byte
	var record ReservationRecordV1
	nextID := id
	var pageCount uint16
	for pageIndex := 0; ; pageIndex++ {
		b, err := readTypedPage(src, nextID, page.PageTypeFreelistReservation, highWater)
		if err != nil {
			return ReservationRecordV1{}, err
		}
		h := page.DecodeHeader(b)
		if !bytes.Equal(b[16:24], reservationMagic[:]) || binary.LittleEndian.Uint16(b[24:26]) != 1 || binary.LittleEndian.Uint16(b[26:28]) != reservationHeaderSize || binary.LittleEndian.Uint16(b[28:30]) != reservationEntrySize || !zeroTail(b[30:32], 0) || binary.LittleEndian.Uint16(b[76:78]) != 1 || !zeroTail(b[152:176], 0) || int(h.Count) != int(binary.LittleEndian.Uint32(b[72:76])) || int(h.Count) > reservationEntriesPerPage || binary.LittleEndian.Uint32(b[144:148]) != uint32(pageIndex) || !zeroTail(b, reservationHeaderSize+int(h.Count)*reservationEntrySize) {
			return ReservationRecordV1{}, ErrGenerationFormat
		}
		if pageIndex == 0 {
			pageCount = binary.LittleEndian.Uint16(b[78:80])
			if pageCount == 0 {
				return ReservationRecordV1{}, ErrGenerationFormat
			}
			record = ReservationRecordV1{pageID: id, GenerationID: binary.LittleEndian.Uint64(b[48:56]), BaseID: binary.LittleEndian.Uint64(b[56:64])}
			copy(record.CandidateID[:], b[32:48])
			copy(record.BaseDigest[:], b[80:112])
		} else if binary.LittleEndian.Uint16(b[78:80]) != pageCount || binary.LittleEndian.Uint64(b[48:56]) != record.GenerationID || binary.LittleEndian.Uint64(b[56:64]) != record.BaseID || !bytes.Equal(b[32:48], record.CandidateID[:]) || !bytes.Equal(b[80:112], record.BaseDigest[:]) {
			return ReservationRecordV1{}, ErrGenerationFormat
		}
		if pageIndex > 0 && (binary.LittleEndian.Uint32(b[148:152]) != binary.LittleEndian.Uint32(pages[0][148:152]) || !bytes.Equal(b[112:144], pages[0][112:144])) {
			return ReservationRecordV1{}, ErrGenerationDigest
		}
		pages = append(pages, b)
		record.pageIDs = append(record.pageIDs, nextID)
		for i := 0; i < int(h.Count); i++ {
			o := reservationHeaderSize + i*reservationEntrySize
			if !zeroTail(b[o+13:o+16], 0) {
				return ReservationRecordV1{}, ErrGenerationFormat
			}
			record.Extents = append(record.Extents, ReservationExtentV1{
				StartPageID: binary.LittleEndian.Uint64(b[o : o+8]), Count: binary.LittleEndian.Uint32(b[o+8 : o+12]), Kind: ReservationKindV1(b[o+12]), LastReachableCommitSeq: binary.LittleEndian.Uint64(b[o+16 : o+24]),
			})
		}
		next := binary.LittleEndian.Uint64(b[64:72])
		if pageIndex+1 == int(pageCount) {
			if next != 0 {
				return ReservationRecordV1{}, ErrGenerationFormat
			}
			break
		}
		if next != nextID+1 {
			return ReservationRecordV1{}, ErrGenerationFormat
		}
		nextID = next
	}
	digest := reservationDigest(pages)
	if !bytes.Equal(digest[:], pages[0][112:144]) || len(record.Extents) != int(binary.LittleEndian.Uint32(pages[0][148:152])) {
		return ReservationRecordV1{}, ErrGenerationDigest
	}
	record.digest = digest
	normalized, err := normalizeExtents(record.Extents)
	if err != nil || len(normalized) != len(record.Extents) {
		return ReservationRecordV1{}, ErrGenerationFormat
	}
	for i := range normalized {
		if normalized[i] != record.Extents[i] {
			return ReservationRecordV1{}, ErrGenerationFormat
		}
		if normalized[i].StartPageID+uint64(normalized[i].Count) > highWater {
			return ReservationRecordV1{}, ErrGenerationFormat
		}
	}
	return record, nil
}

func (r ReservationRecordV1) pendingMetadata() []retiredPage {
	var out []retiredPage
	for _, extent := range r.Extents {
		if extent.Kind != ReservationPendingMetadataRetirement {
			continue
		}
		for i := uint32(0); i < extent.Count; i++ {
			out = append(out, retiredPage{extent.StartPageID + uint64(i), extent.LastReachableCommitSeq})
		}
	}
	return out
}

func (r ReservationRecordV1) metadataPages() []uint64 {
	var out []uint64
	for _, extent := range r.Extents {
		if extent.Kind != ReservationTargetMetadata {
			continue
		}
		for i := uint32(0); i < extent.Count; i++ {
			out = append(out, extent.StartPageID+uint64(i))
		}
	}
	return out
}

type CandidateState uint8

const (
	CandidatePreVisible CandidateState = iota
	CandidateVisible
	CandidateRetryable
	CandidatePoisoned
	CandidateShutdown
)

type reservation struct {
	state              CandidateState
	ids                []uint64
	tailReserved       bool
	tailWriteAttempted bool
	tailStart          uint64
	tailCount          uint64
	abandonedCoverage  []reservationInterval
}

type reservationInterval struct {
	start uint64
	count uint64
}

type ReservationLedger struct {
	mu          sync.Mutex
	owners      map[uint64]CandidateIDV1
	candidates  map[CandidateIDV1]*reservation
	burnedTails []reservationInterval
}

func NewReservationLedger() *ReservationLedger {
	return &ReservationLedger{owners: map[uint64]CandidateIDV1{}, candidates: map[CandidateIDV1]*reservation{}}
}

func (l *ReservationLedger) Reserved(id uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reservedLocked(id)
}

func (l *ReservationLedger) reservedLocked(id uint64) bool {
	if _, ok := l.owners[id]; ok {
		return true
	}
	for _, burned := range l.burnedTails {
		if id >= burned.start && id-burned.start < burned.count {
			return true
		}
	}
	for _, candidate := range l.candidates {
		if candidate.tailReserved && id >= candidate.tailStart && id-candidate.tailStart < candidate.tailCount {
			return true
		}
	}
	return false
}

func (l *ReservationLedger) reservedByOtherLocked(candidate CandidateIDV1, id uint64) bool {
	if owner, ok := l.owners[id]; ok && owner != candidate {
		return true
	}
	for _, burned := range l.burnedTails {
		if id >= burned.start && id-burned.start < burned.count {
			return true
		}
	}
	for idCandidate, reservation := range l.candidates {
		if idCandidate != candidate && reservation.tailReserved && id >= reservation.tailStart && id-reservation.tailStart < reservation.tailCount {
			return true
		}
	}
	return false
}

func (l *ReservationLedger) reserve(candidate CandidateIDV1, ids []uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if candidate == (CandidateIDV1{}) {
		return ErrGenerationFormat
	}
	if _, exists := l.candidates[candidate]; exists {
		return fmt.Errorf("candidate %x exists", candidate)
	}
	for _, id := range ids {
		if l.reservedByOtherLocked(candidate, id) {
			return ErrPageReserved
		}
	}
	r := &reservation{state: CandidatePreVisible, ids: append([]uint64(nil), ids...)}
	for _, id := range ids {
		l.owners[id] = candidate
	}
	l.candidates[candidate] = r
	return nil
}

func (l *ReservationLedger) firstConflictingOwnerLocked(start, count uint64) (uint64, bool, bool) {
	if count == 0 || start > ^uint64(0)-count {
		return 0, false, false
	}
	end := start + count
	for id := start; id < end; id++ {
		if l.reservedLocked(id) {
			return id, true, true
		}
	}
	return 0, false, true
}

func reservationPagesForEntries(entries uint64) uint64 {
	if entries == 0 {
		return 1
	}
	perPage := uint64(reservationEntriesPerPage)
	return (entries-1)/perPage + 1
}

// reserveTail atomically chooses and owns a contiguous metadata range. The
// range may move above another candidate's reservation; callers persist the
// skipped prefix as abandoned append space in their reservation record.
func (l *ReservationLedger) reserveTail(candidate CandidateIDV1, minimumStart, statePageCount uint64, dataIDs []uint64, baseExtents []ReservationExtentV1) (uint64, uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if candidate == (CandidateIDV1{}) || minimumStart < 2 {
		return 0, 0, ErrGenerationFormat
	}
	r := l.candidates[candidate]
	if r != nil && r.state != CandidatePreVisible {
		return 0, 0, fmt.Errorf("candidate %x is already visible", candidate)
	}
	if r != nil && r.tailReserved {
		return 0, 0, fmt.Errorf("candidate %x metadata tail already reserved", candidate)
	}
	for _, id := range dataIDs {
		if l.reservedByOtherLocked(candidate, id) {
			return 0, 0, ErrPageReserved
		}
	}
	start := minimumStart
	baseExtentCount := uint64(len(baseExtents))
	var count uint64
	for {
		skippedExtentCount := uint64(0)
		if skipped := start - minimumStart; skipped != 0 {
			skippedExtentCount = (skipped-1)/uint64(^uint32(0)) + 1
		}
		if baseExtentCount > ^uint64(0)-skippedExtentCount-1 {
			return 0, 0, ErrGenerationFormat
		}
		recordPageCount := reservationPagesForEntries(baseExtentCount + skippedExtentCount + 1)
		if recordPageCount > uint64(^uint16(0)) || statePageCount > ^uint64(0)-recordPageCount-1 {
			return 0, 0, ErrGenerationFormat
		}
		count = statePageCount + recordPageCount + 1
		if count > uint64(^uint32(0)) {
			return 0, 0, ErrGenerationFormat
		}
		conflictID, conflict, valid := l.firstConflictingOwnerLocked(start, count)
		if !valid {
			return 0, 0, ErrNoAllocatablePage
		}
		if !conflict {
			break
		}
		if conflictID == ^uint64(0) {
			return 0, 0, ErrNoAllocatablePage
		}
		start = conflictID + 1
	}
	if r == nil {
		r = &reservation{state: CandidatePreVisible, ids: make([]uint64, 0, len(dataIDs))}
		l.candidates[candidate] = r
	}
	for _, id := range dataIDs {
		if owner, exists := l.owners[id]; !exists {
			l.owners[id] = candidate
			r.ids = append(r.ids, id)
		} else if owner != candidate || l.reservedByOtherLocked(candidate, id) {
			return 0, 0, ErrPageReserved
		}
	}
	r.tailReserved = true
	r.tailStart = start
	r.tailCount = count
	for _, extent := range baseExtents {
		if extent.Kind == ReservationAbandonedAppend {
			r.abandonedCoverage = append(r.abandonedCoverage, reservationInterval{start: extent.StartPageID, count: uint64(extent.Count)})
		}
	}
	if start > minimumStart {
		r.abandonedCoverage = append(r.abandonedCoverage, reservationInterval{start: minimumStart, count: start - minimumStart})
	}
	return start, count, nil
}

func (l *ReservationLedger) markTailWriteAttempted(candidate CandidateIDV1) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.candidates[candidate]
	if r == nil || r.state != CandidatePreVisible || !r.tailReserved {
		return fmt.Errorf("candidate %x has no pre-visible metadata tail", candidate)
	}
	r.tailWriteAttempted = true
	return nil
}

func (l *ReservationLedger) firstUnreservedAtOrAfter(start uint64) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for id := start; ; id++ {
		if !l.reservedLocked(id) {
			return id, true
		}
		if id == ^uint64(0) {
			return 0, false
		}
	}
}

func (l *ReservationLedger) transition(candidate CandidateIDV1, to CandidateState) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.candidates[candidate]
	if r == nil {
		return fmt.Errorf("unknown candidate %x", candidate)
	}
	allowed := false
	switch r.state {
	case CandidatePreVisible:
		allowed = to == CandidateVisible
	case CandidateVisible:
		allowed = to == CandidateRetryable || to == CandidatePoisoned || to == CandidateShutdown
	case CandidateRetryable:
		allowed = to == CandidateVisible || to == CandidatePoisoned || to == CandidateShutdown
	}
	if !allowed {
		return fmt.Errorf("candidate %x transition %d -> %d", candidate, r.state, to)
	}
	r.state = to
	return nil
}

func (l *ReservationLedger) MarkVisible(c CandidateIDV1) error {
	return l.transition(c, CandidateVisible)
}
func (l *ReservationLedger) Retry(c CandidateIDV1) error  { return l.transition(c, CandidateRetryable) }
func (l *ReservationLedger) Poison(c CandidateIDV1) error { return l.transition(c, CandidatePoisoned) }
func (l *ReservationLedger) Shutdown(c CandidateIDV1) error {
	return l.transition(c, CandidateShutdown)
}

func (l *ReservationLedger) Supersede(old, next CandidateIDV1) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.candidates[old]
	if r == nil || next == (CandidateIDV1{}) {
		return fmt.Errorf("unknown candidate %x", old)
	}
	if _, exists := l.candidates[next]; exists {
		return fmt.Errorf("candidate %x exists", next)
	}
	l.candidates[next] = r
	delete(l.candidates, old)
	for _, id := range r.ids {
		l.owners[id] = next
	}
	return nil
}

func (l *ReservationLedger) release(candidate CandidateIDV1, preVisibleOnly bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.candidates[candidate]
	if r == nil {
		return fmt.Errorf("unknown candidate %x", candidate)
	}
	if preVisibleOnly && r.state != CandidatePreVisible {
		return fmt.Errorf("candidate %x is visible", candidate)
	}
	if preVisibleOnly && r.tailWriteAttempted {
		return fmt.Errorf("candidate %x attempted metadata writes; use Fail", candidate)
	}
	for _, id := range r.ids {
		delete(l.owners, id)
	}
	delete(l.candidates, candidate)
	return nil
}

func (l *ReservationLedger) Abandon(c CandidateIDV1) error { return l.release(c, true) }
func (l *ReservationLedger) Fail(c CandidateIDV1) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.candidates[c]
	if r == nil {
		return fmt.Errorf("unknown candidate %x", c)
	}
	if r.state != CandidatePreVisible {
		return fmt.Errorf("candidate %x is visible", c)
	}
	for _, id := range r.ids {
		delete(l.owners, id)
	}
	if r.tailReserved && r.tailWriteAttempted {
		l.burnedTails = append(l.burnedTails, reservationInterval{start: r.tailStart, count: r.tailCount})
	}
	delete(l.candidates, c)
	return nil
}

// RollbackPreVisible removes an unpublished candidate if it exists. A tail
// whose sink accepted a write remains burned, matching Fail; callers may use
// this after a staged allocator preparation error without first determining
// how far materialization progressed.
func (l *ReservationLedger) RollbackPreVisible(c CandidateIDV1) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.candidates[c]
	if r == nil {
		return nil
	}
	if r.state != CandidatePreVisible {
		return fmt.Errorf("candidate %x is visible", c)
	}
	for _, id := range r.ids {
		delete(l.owners, id)
	}
	if r.tailReserved && r.tailWriteAttempted {
		l.burnedTails = append(l.burnedTails, reservationInterval{start: r.tailStart, count: r.tailCount})
	}
	delete(l.candidates, c)
	return nil
}

func (l *ReservationLedger) Publish(c CandidateIDV1) error {
	return l.PublishBatch([]CandidateIDV1{c})
}

// PublishBatch atomically consumes an ordered set of candidates after one
// durable root makes all of them recoverable. Validation happens before any
// owner or burned-tail state is changed, so a malformed prefix cannot be
// partially published.
func (l *ReservationLedger) PublishBatch(candidates []CandidateIDV1) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := make(map[CandidateIDV1]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate == (CandidateIDV1{}) {
			return ErrGenerationFormat
		}
		if _, duplicate := seen[candidate]; duplicate {
			return fmt.Errorf("duplicate candidate %x", candidate)
		}
		seen[candidate] = struct{}{}
		if l.candidates[candidate] == nil {
			return fmt.Errorf("unknown candidate %x", candidate)
		}
	}
	for _, candidate := range candidates {
		r := l.candidates[candidate]
		for _, id := range r.ids {
			delete(l.owners, id)
		}
		if len(r.abandonedCoverage) != 0 {
			kept := l.burnedTails[:0]
			for _, burned := range l.burnedTails {
				covered := false
				for _, abandoned := range r.abandonedCoverage {
					if burned.start >= abandoned.start && burned.start-abandoned.start < abandoned.count && burned.count <= abandoned.count-(burned.start-abandoned.start) {
						covered = true
						break
					}
				}
				if covered {
					continue
				}
				kept = append(kept, burned)
			}
			l.burnedTails = kept
		}
		delete(l.candidates, candidate)
	}
	return nil
}

func (l *ReservationLedger) Reservations() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := uint64(len(l.owners))
	for _, burned := range l.burnedTails {
		if ^uint64(0)-count < burned.count {
			return ^uint64(0)
		}
		count += burned.count
	}
	for _, candidate := range l.candidates {
		if ^uint64(0)-count < candidate.tailCount {
			return ^uint64(0)
		}
		count += candidate.tailCount
	}
	return count
}
