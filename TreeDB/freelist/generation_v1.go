package freelist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"sync"
)

// FreelistGenerationV1 is an immutable, standalone representation of reusable
// pages and pages awaiting an explicit recovery horizon. It is intentionally
// not wired to the current meta format; #3679 owns that activation.
type FreelistGenerationV1 struct {
	generationID uint64
	highWater    uint64
	free         []uint64
	retired      []retiredPage
}

type retiredPage struct{ id, lastReachableCommitSeq uint64 }

var (
	ErrGenerationChecksum = errors.New("freelist generation checksum mismatch")
	ErrGenerationFormat   = errors.New("invalid freelist generation format")
	ErrPageReserved       = errors.New("freelist page is reserved by a visible candidate")
	ErrNoAllocatablePage  = errors.New("no allocatable freelist page")
)

const (
	freelistGenerationMagic      = "FLGV1\x00\x00\x00"
	freelistGenerationHeaderSize = 8 + 8 + 8 + 8 + 8 + 4
)

func NewFreelistGenerationV1(generationID, highWater uint64, free []uint64, retired map[uint64]uint64) (*FreelistGenerationV1, error) {
	g := &FreelistGenerationV1{generationID: generationID, highWater: highWater, free: append([]uint64(nil), free...)}
	sort.Slice(g.free, func(i, j int) bool { return g.free[i] < g.free[j] })
	for id, seq := range retired {
		g.retired = append(g.retired, retiredPage{id, seq})
	}
	sort.Slice(g.retired, func(i, j int) bool { return g.retired[i].id < g.retired[j].id })
	if err := g.Validate(); err != nil {
		return nil, err
	}
	return g, nil
}

func MustNewFreelistGenerationV1(generationID, highWater uint64, free []uint64, retired map[uint64]uint64) *FreelistGenerationV1 {
	g, err := NewFreelistGenerationV1(generationID, highWater, free, retired)
	if err != nil {
		panic(err)
	}
	return g
}
func (g *FreelistGenerationV1) GenerationID() uint64       { return g.generationID }
func (g *FreelistGenerationV1) HighWater() uint64          { return g.highWater }
func (g *FreelistGenerationV1) Allocatable(id uint64) bool { return slicesContains(g.free, id) }
func (g *FreelistGenerationV1) Validate() error {
	if g == nil || g.generationID == 0 {
		return fmt.Errorf("%w: zero generation", ErrGenerationFormat)
	}
	seen := make(map[uint64]struct{}, len(g.free)+len(g.retired))
	for _, id := range g.free {
		if id == 0 || id >= g.highWater {
			return fmt.Errorf("%w: invalid free page %d", ErrGenerationFormat, id)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("%w: duplicate page %d", ErrGenerationFormat, id)
		}
		seen[id] = struct{}{}
	}
	for _, p := range g.retired {
		if p.id == 0 || p.id >= g.highWater || p.lastReachableCommitSeq == 0 {
			return fmt.Errorf("%w: invalid retired page", ErrGenerationFormat)
		}
		if _, ok := seen[p.id]; ok {
			return fmt.Errorf("%w: page %d is both retired and free", ErrGenerationFormat, p.id)
		}
		seen[p.id] = struct{}{}
	}
	return nil
}
func slicesContains(ids []uint64, id uint64) bool {
	i := sort.Search(len(ids), func(i int) bool { return ids[i] >= id })
	return i < len(ids) && ids[i] == id
}

// RecoveryHorizon is an explicit capability supplied by recovery/pin owners.
// Zero values do not make pages reusable.
type RecoveryHorizon struct{ OldestRecoverableCommitSeq, MinPinnedSnapshotCommitSeq, HistoryFloorCommitSeq uint64 }

func (h RecoveryHorizon) permits(last uint64) bool {
	return h.OldestRecoverableCommitSeq != 0 && last < h.OldestRecoverableCommitSeq &&
		(h.MinPinnedSnapshotCommitSeq == 0 || last < h.MinPinnedSnapshotCommitSeq) &&
		(h.HistoryFloorCommitSeq == 0 || last < h.HistoryFloorCommitSeq)
}

type FreelistTxn struct {
	base              *FreelistGenerationV1
	ledger            *ReservationLedger
	freeAdd, freeDrop map[uint64]struct{}
	retired           map[uint64]uint64
	allocated         []uint64
	highWater         uint64
	stats             FreelistTxnStats
}
type FreelistTxnStats struct{ COWChunks, COWPages, COWBytes, LogicalDelta, AppendAllocations, ReuseAllocations, Reservations uint64 }

func NewFreelistTxn(base *FreelistGenerationV1, ledger *ReservationLedger) *FreelistTxn {
	if ledger == nil {
		ledger = NewReservationLedger()
	}
	return &FreelistTxn{base: base, ledger: ledger, freeAdd: map[uint64]struct{}{}, freeDrop: map[uint64]struct{}{}, retired: map[uint64]uint64{}, highWater: base.highWater}
}
func (t *FreelistTxn) Allocate(regionHint uint64) (uint64, error) {
	for i := len(t.base.free) - 1; i >= 0; i-- {
		id := t.base.free[i]
		if _, drop := t.freeDrop[id]; drop || t.ledger.Reserved(id) {
			continue
		}
		t.freeDrop[id] = struct{}{}
		t.allocated = append(t.allocated, id)
		t.stats.ReuseAllocations++
		return id, nil
	}
	for id := range t.freeAdd {
		if !t.ledger.Reserved(id) {
			delete(t.freeAdd, id)
			t.allocated = append(t.allocated, id)
			t.stats.ReuseAllocations++
			return id, nil
		}
	}
	id := t.highWater
	if id == math.MaxUint64 {
		return 0, ErrNoAllocatablePage
	}
	t.highWater++
	t.allocated = append(t.allocated, id)
	t.stats.AppendAllocations++
	return id, nil
}
func (t *FreelistTxn) ReservePage(id uint64) error {
	if t.ledger.Reserved(id) {
		return ErrPageReserved
	}
	t.allocated = append(t.allocated, id)
	return nil
}
func (t *FreelistTxn) Retire(id, lastReachableCommitSeq uint64) {
	if id != 0 && lastReachableCommitSeq != 0 {
		t.retired[id] = lastReachableCommitSeq
	}
}
func (t *FreelistTxn) Prune(h RecoveryHorizon) {
	for _, p := range t.base.retired {
		if h.permits(p.lastReachableCommitSeq) {
			t.freeAdd[p.id] = struct{}{}
		}
	}
	for id, seq := range t.retired {
		if h.permits(seq) {
			t.freeAdd[id] = struct{}{}
			delete(t.retired, id)
		}
	}
}
func (t *FreelistTxn) Reserve(candidate string) error {
	if candidate == "" {
		return fmt.Errorf("empty candidate")
	}
	if err := t.ledger.reserve(candidate, t.allocated); err != nil {
		return err
	}
	t.stats.Reservations = uint64(len(t.allocated))
	return nil
}
func (t *FreelistTxn) Materialize(generationID uint64) (*FreelistGenerationV1, error) {
	free := make([]uint64, 0, len(t.base.free)+len(t.freeAdd))
	for _, id := range t.base.free {
		if _, drop := t.freeDrop[id]; !drop {
			free = append(free, id)
		}
	}
	for id := range t.freeAdd {
		free = append(free, id)
	}
	retired := make(map[uint64]uint64, len(t.base.retired)+len(t.retired))
	for _, p := range t.base.retired {
		if _, promoted := t.freeAdd[p.id]; !promoted {
			retired[p.id] = p.lastReachableCommitSeq
		}
	}
	for id, seq := range t.retired {
		retired[id] = seq
	}
	t.stats.LogicalDelta = uint64(len(t.freeDrop) + len(t.freeAdd) + len(t.retired))
	t.stats.COWChunks = t.stats.LogicalDelta
	if t.stats.COWChunks == 0 {
		t.stats.COWChunks = 1
	}
	t.stats.COWPages = t.stats.COWChunks
	t.stats.COWBytes = t.stats.COWPages * 4096
	return NewFreelistGenerationV1(generationID, t.highWater, free, retired)
}
func (t *FreelistTxn) Stats() FreelistTxnStats { return t.stats }

// ReservationLedger is candidate-scoped ownership for pages absent from the
// durable generation. It remains intentionally independent of publication.
type ReservationLedger struct {
	mu         sync.Mutex
	owners     map[uint64]string
	candidates map[string]map[uint64]struct{}
}

func NewReservationLedger() *ReservationLedger {
	return &ReservationLedger{owners: map[uint64]string{}, candidates: map[string]map[uint64]struct{}{}}
}
func (l *ReservationLedger) Reserved(id uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.owners[id]
	return ok
}
func (l *ReservationLedger) reserve(candidate string, ids []uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, id := range ids {
		if _, ok := l.owners[id]; ok {
			return ErrPageReserved
		}
	}
	set := l.candidates[candidate]
	if set == nil {
		set = map[uint64]struct{}{}
		l.candidates[candidate] = set
	}
	for _, id := range ids {
		l.owners[id] = candidate
		set[id] = struct{}{}
	}
	return nil
}
func (l *ReservationLedger) Supersede(old, next string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	set := l.candidates[old]
	if set == nil {
		return fmt.Errorf("unknown candidate %q", old)
	}
	if _, exists := l.candidates[next]; exists {
		return fmt.Errorf("candidate %q exists", next)
	}
	l.candidates[next] = set
	delete(l.candidates, old)
	for id := range set {
		l.owners[id] = next
	}
	return nil
}
func (l *ReservationLedger) Fail(candidate string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	set := l.candidates[candidate]
	if set == nil {
		return fmt.Errorf("unknown candidate %q", candidate)
	}
	for id := range set {
		delete(l.owners, id)
	}
	delete(l.candidates, candidate)
	return nil
}
func (l *ReservationLedger) Publish(candidate string) error { return l.Fail(candidate) }

func (g *FreelistGenerationV1) MarshalBinary() ([]byte, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}
	n := freelistGenerationHeaderSize + len(g.free)*8 + len(g.retired)*16
	b := make([]byte, n)
	copy(b, freelistGenerationMagic)
	binary.LittleEndian.PutUint64(b[8:], g.generationID)
	binary.LittleEndian.PutUint64(b[16:], g.highWater)
	binary.LittleEndian.PutUint64(b[24:], uint64(len(g.free)))
	binary.LittleEndian.PutUint64(b[32:], uint64(len(g.retired)))
	off := freelistGenerationHeaderSize
	for _, id := range g.free {
		binary.LittleEndian.PutUint64(b[off:], id)
		off += 8
	}
	for _, p := range g.retired {
		binary.LittleEndian.PutUint64(b[off:], p.id)
		binary.LittleEndian.PutUint64(b[off+8:], p.lastReachableCommitSeq)
		off += 16
	}
	binary.LittleEndian.PutUint32(b[40:], generationChecksum(b))
	return b, nil
}
func DecodeFreelistGenerationV1(b []byte) (*FreelistGenerationV1, error) {
	if len(b) < freelistGenerationHeaderSize || !bytes.Equal(b[:8], []byte(freelistGenerationMagic)) {
		return nil, ErrGenerationFormat
	}
	want := binary.LittleEndian.Uint32(b[40:])
	got := generationChecksum(b)
	if want != got {
		return nil, ErrGenerationChecksum
	}
	freeN, retiredN := binary.LittleEndian.Uint64(b[24:]), binary.LittleEndian.Uint64(b[32:])
	if freeN > uint64((len(b)-freelistGenerationHeaderSize)/8) || retiredN > uint64((len(b)-freelistGenerationHeaderSize-int(freeN)*8)/16) || freelistGenerationHeaderSize+int(freeN)*8+int(retiredN)*16 != len(b) {
		return nil, ErrGenerationFormat
	}
	off := freelistGenerationHeaderSize
	free := make([]uint64, freeN)
	for i := range free {
		free[i] = binary.LittleEndian.Uint64(b[off:])
		off += 8
	}
	retired := make(map[uint64]uint64, retiredN)
	for range retiredN {
		id, seq := binary.LittleEndian.Uint64(b[off:]), binary.LittleEndian.Uint64(b[off+8:])
		retired[id] = seq
		off += 16
	}
	return NewFreelistGenerationV1(binary.LittleEndian.Uint64(b[8:]), binary.LittleEndian.Uint64(b[16:]), free, retired)
}

func generationChecksum(b []byte) uint32 {
	checksum := crc32.Update(0, crc32.IEEETable, b[:40])
	return crc32.Update(checksum, crc32.IEEETable, b[44:])
}
