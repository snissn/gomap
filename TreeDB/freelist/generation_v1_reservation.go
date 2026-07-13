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
	CandidateID  CandidateIDV1
	GenerationID uint64
	BaseID       uint64
	BaseDigest   [32]byte
	Extents      []ReservationExtentV1
	digest       [32]byte
}

func (r ReservationRecordV1) PageID() uint64   { return r.pageID }
func (r ReservationRecordV1) Digest() [32]byte { return r.digest }
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

func encodeReservationPage(id uint64, record ReservationRecordV1) ([]byte, ReservationRecordV1, error) {
	extents, err := normalizeExtents(record.Extents)
	if err != nil {
		return nil, ReservationRecordV1{}, err
	}
	if len(extents) > (page.PageSize-reservationHeaderSize)/reservationEntrySize {
		return nil, ReservationRecordV1{}, fmt.Errorf("%w: reservation record needs multiple pages", ErrGenerationFormat)
	}
	record.pageID, record.Extents = id, extents
	b := make([]byte, page.PageSize)
	encodePageHeader(b, id, page.PageTypeFreelistReservation, uint16(len(extents)))
	copy(b[16:24], reservationMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 1)
	binary.LittleEndian.PutUint16(b[26:28], reservationHeaderSize)
	binary.LittleEndian.PutUint16(b[28:30], reservationEntrySize)
	copy(b[32:48], record.CandidateID[:])
	binary.LittleEndian.PutUint64(b[48:56], record.GenerationID)
	binary.LittleEndian.PutUint64(b[56:64], record.BaseID)
	binary.LittleEndian.PutUint32(b[72:76], uint32(len(extents)))
	binary.LittleEndian.PutUint16(b[76:78], 1)
	copy(b[80:112], record.BaseDigest[:])
	o := reservationHeaderSize
	for _, extent := range extents {
		binary.LittleEndian.PutUint64(b[o:o+8], extent.StartPageID)
		binary.LittleEndian.PutUint32(b[o+8:o+12], extent.Count)
		b[o+12] = byte(extent.Kind)
		binary.LittleEndian.PutUint64(b[o+16:o+24], extent.LastReachableCommitSeq)
		o += reservationEntrySize
	}
	record.digest = reservationDigest(b)
	copy(b[112:144], record.digest[:])
	finishPage(b)
	return b, record, nil
}

func reservationDigest(b []byte) [32]byte {
	canonical := append([]byte(nil), b...)
	for i := 8; i < 12; i++ {
		canonical[i] = 0
	}
	for i := 112; i < 144; i++ {
		canonical[i] = 0
	}
	return sha256.Sum256(canonical)
}

func loadReservationRecord(src PageSource, id, highWater uint64) (ReservationRecordV1, error) {
	b, err := readTypedPage(src, id, page.PageTypeFreelistReservation, highWater)
	if err != nil {
		return ReservationRecordV1{}, err
	}
	h := page.DecodeHeader(b)
	if !bytes.Equal(b[16:24], reservationMagic[:]) || binary.LittleEndian.Uint16(b[24:26]) != 1 || binary.LittleEndian.Uint16(b[26:28]) != reservationHeaderSize || binary.LittleEndian.Uint16(b[28:30]) != reservationEntrySize || !zeroTail(b[30:32], 0) || !zeroTail(b[64:72], 0) || binary.LittleEndian.Uint16(b[76:78]) != 1 || !zeroTail(b[78:80], 0) || !zeroTail(b[144:176], 0) || int(h.Count) != int(binary.LittleEndian.Uint32(b[72:76])) || !zeroTail(b, reservationHeaderSize+int(h.Count)*reservationEntrySize) {
		return ReservationRecordV1{}, ErrGenerationFormat
	}
	digest := reservationDigest(b)
	if !bytes.Equal(digest[:], b[112:144]) {
		return ReservationRecordV1{}, ErrGenerationDigest
	}
	record := ReservationRecordV1{pageID: id, GenerationID: binary.LittleEndian.Uint64(b[48:56]), BaseID: binary.LittleEndian.Uint64(b[56:64]), digest: digest}
	copy(record.CandidateID[:], b[32:48])
	copy(record.BaseDigest[:], b[80:112])
	for i := 0; i < int(h.Count); i++ {
		o := reservationHeaderSize + i*reservationEntrySize
		if !zeroTail(b[o+13:o+16], 0) {
			return ReservationRecordV1{}, ErrGenerationFormat
		}
		record.Extents = append(record.Extents, ReservationExtentV1{
			StartPageID:            binary.LittleEndian.Uint64(b[o : o+8]),
			Count:                  binary.LittleEndian.Uint32(b[o+8 : o+12]),
			Kind:                   ReservationKindV1(b[o+12]),
			LastReachableCommitSeq: binary.LittleEndian.Uint64(b[o+16 : o+24]),
		})
	}
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
	state CandidateState
	ids   map[uint64]struct{}
}

type ReservationLedger struct {
	mu         sync.Mutex
	owners     map[uint64]CandidateIDV1
	candidates map[CandidateIDV1]*reservation
}

func NewReservationLedger() *ReservationLedger {
	return &ReservationLedger{owners: map[uint64]CandidateIDV1{}, candidates: map[CandidateIDV1]*reservation{}}
}

func (l *ReservationLedger) Reserved(id uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.owners[id]
	return ok
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
		if _, exists := l.owners[id]; exists {
			return ErrPageReserved
		}
	}
	r := &reservation{state: CandidatePreVisible, ids: make(map[uint64]struct{}, len(ids))}
	for _, id := range ids {
		l.owners[id], r.ids[id] = candidate, struct{}{}
	}
	l.candidates[candidate] = r
	return nil
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
	for id := range r.ids {
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
	for id := range r.ids {
		delete(l.owners, id)
	}
	delete(l.candidates, candidate)
	return nil
}

func (l *ReservationLedger) Abandon(c CandidateIDV1) error { return l.release(c, true) }
func (l *ReservationLedger) Fail(c CandidateIDV1) error    { return l.Abandon(c) }
func (l *ReservationLedger) Publish(c CandidateIDV1) error { return l.release(c, false) }

func (l *ReservationLedger) Reservations() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return uint64(len(l.owners))
}
