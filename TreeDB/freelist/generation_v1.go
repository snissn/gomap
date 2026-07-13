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

var (
	ErrGenerationChecksum = errors.New("freelist generation checksum mismatch")
	ErrGenerationFormat   = errors.New("invalid freelist generation format")
	ErrPageReserved       = errors.New("freelist page is reserved by a visible candidate")
	ErrNoAllocatablePage  = errors.New("no allocatable freelist page")
)

const (
	freelistGenerationMagic      = "FLGV1\x00\x00\x00"
	freelistGenerationHeaderSize = 44
	maxGenerationEntries         = 1 << 28
	trieDepth                    = 16
)

// FreelistGenerationV1 is immutable. free and retired are persistent nibble
// tries: a delta copies one leaf and at most trieDepth path nodes.
type FreelistGenerationV1 struct {
	generationID, highWater uint64
	free                    *freeNode
	retired                 *retiredNode
}
type freeNode struct {
	child [16]*freeNode
	ids   []uint64
}
type retiredNode struct {
	child   [16]*retiredNode
	id, seq uint64
	set     bool
}
type retiredPage struct{ id, lastReachableCommitSeq uint64 }

func NewFreelistGenerationV1(generationID, highWater uint64, free []uint64, retired map[uint64]uint64) (*FreelistGenerationV1, error) {
	g := &FreelistGenerationV1{generationID: generationID, highWater: highWater}
	for _, id := range free {
		g.free = freeInsert(g.free, id)
	}
	for id, seq := range retired {
		g.retired = retiredSet(g.retired, id, seq)
	}
	return g, g.Validate()
}
func MustNewFreelistGenerationV1(a, b uint64, c []uint64, d map[uint64]uint64) *FreelistGenerationV1 {
	g, e := NewFreelistGenerationV1(a, b, c, d)
	if e != nil {
		panic(e)
	}
	return g
}
func (g *FreelistGenerationV1) GenerationID() uint64 {
	if g == nil {
		return 0
	}
	return g.generationID
}
func (g *FreelistGenerationV1) HighWater() uint64 {
	if g == nil {
		return 0
	}
	return g.highWater
}
func (g *FreelistGenerationV1) Allocatable(id uint64) bool { return freeContains(g.free, id) }
func (g *FreelistGenerationV1) Validate() error {
	if g == nil || g.generationID == 0 || g.highWater == 0 {
		return fmt.Errorf("%w: missing generation", ErrGenerationFormat)
	}
	var err error
	freeWalk(g.free, func(id uint64) {
		if id == 0 || id >= g.highWater || retiredGet(g.retired, id) != 0 {
			err = fmt.Errorf("%w: invalid/overlap page %d", ErrGenerationFormat, id)
		}
	})
	retiredWalk(g.retired, func(id, seq uint64) {
		if id == 0 || id >= g.highWater || seq == 0 {
			err = fmt.Errorf("%w: invalid retired page", ErrGenerationFormat)
		}
	})
	return err
}

func nibble(id uint64, d int) int { return int((id >> uint((trieDepth-1-d)*4)) & 15) }
func freeContains(n *freeNode, id uint64) bool {
	for d := 0; n != nil && d < trieDepth; d++ {
		n = n.child[nibble(id, d)]
	}
	if n == nil {
		return false
	}
	i := sort.Search(len(n.ids), func(i int) bool { return n.ids[i] >= id })
	return i < len(n.ids) && n.ids[i] == id
}
func freeInsert(n *freeNode, id uint64) *freeNode { return freeMutate(n, id, true, 0) }
func freeRemove(n *freeNode, id uint64) *freeNode { return freeMutate(n, id, false, 0) }
func freeMutate(n *freeNode, id uint64, add bool, d int) *freeNode {
	out := &freeNode{}
	if n != nil {
		*out = *n
	}
	if d == trieDepth {
		i := sort.Search(len(out.ids), func(i int) bool { return out.ids[i] >= id })
		has := i < len(out.ids) && out.ids[i] == id
		if add && !has {
			out.ids = append(out.ids, 0)
			copy(out.ids[i+1:], out.ids[i:])
			out.ids[i] = id
		}
		if !add && has {
			out.ids = append(append([]uint64(nil), out.ids[:i]...), out.ids[i+1:]...)
		}
		return out
	}
	x := nibble(id, d)
	out.child[x] = freeMutate(out.child[x], id, add, d+1)
	return out
}
func freeWalk(n *freeNode, f func(uint64)) {
	if n == nil {
		return
	}
	if n.ids != nil {
		for _, id := range n.ids {
			f(id)
		}
		return
	}
	for _, c := range n.child {
		freeWalk(c, f)
	}
}
func retiredGet(n *retiredNode, id uint64) uint64 {
	for d := 0; n != nil && d < trieDepth; d++ {
		n = n.child[nibble(id, d)]
	}
	if n != nil && n.set {
		return n.seq
	}
	return 0
}
func retiredSet(n *retiredNode, id, seq uint64) *retiredNode { return retiredMutate(n, id, seq, 0) }
func retiredDelete(n *retiredNode, id uint64) *retiredNode   { return retiredMutate(n, id, 0, 0) }
func retiredMutate(n *retiredNode, id, seq uint64, d int) *retiredNode {
	out := &retiredNode{}
	if n != nil {
		*out = *n
	}
	if d == trieDepth {
		out.id, out.seq, out.set = id, seq, seq != 0
		return out
	}
	x := nibble(id, d)
	out.child[x] = retiredMutate(out.child[x], id, seq, d+1)
	return out
}
func retiredWalk(n *retiredNode, f func(uint64, uint64)) {
	if n == nil {
		return
	}
	if n.set {
		f(n.id, n.seq)
		return
	}
	for _, c := range n.child {
		retiredWalk(c, f)
	}
}

type RecoveryHorizon struct{ OldestRecoverableCommitSeq, MinPinnedSnapshotCommitSeq, HistoryFloorCommitSeq uint64 }

func (h RecoveryHorizon) permits(last uint64) bool {
	return h.OldestRecoverableCommitSeq != 0 && last < h.OldestRecoverableCommitSeq && (h.MinPinnedSnapshotCommitSeq == 0 || last < h.MinPinnedSnapshotCommitSeq) && (h.HistoryFloorCommitSeq == 0 || last < h.HistoryFloorCommitSeq)
}

type FreelistTxnStats struct{ COWChunks, COWPages, COWBytes, LogicalDelta, AppendAllocations, ReuseAllocations, Reservations, ReuseLag uint64 }
type FreelistTxn struct {
	base      *FreelistGenerationV1
	ledger    *ReservationLedger
	free      *freeNode
	retired   *retiredNode
	highWater uint64
	allocated []uint64
	changed   map[uint64]struct{}
	stats     FreelistTxnStats
}

func NewFreelistTxn(base *FreelistGenerationV1, ledger *ReservationLedger) *FreelistTxn {
	if ledger == nil {
		ledger = NewReservationLedger()
	}
	if base == nil {
		return &FreelistTxn{ledger: ledger}
	}
	return &FreelistTxn{base: base, ledger: ledger, free: base.free, retired: base.retired, highWater: base.highWater, changed: map[uint64]struct{}{}}
}
func (t *FreelistTxn) valid() error {
	if t == nil || t.base == nil || t.changed == nil {
		return fmt.Errorf("%w: nil transaction base", ErrGenerationFormat)
	}
	return nil
}
func (t *FreelistTxn) Allocate(regionHint uint64) (uint64, error) {
	if err := t.valid(); err != nil {
		return 0, err
	}
	var best uint64
	found := false
	bestDist := uint64(math.MaxUint64)
	freeWalk(t.free, func(id uint64) {
		if t.ledger.Reserved(id) {
			return
		}
		d := absDiff(id/8192, regionHint/8192)
		if !found || d < bestDist || (d == bestDist && id > best) {
			best, found, bestDist = id, true, d
		}
	})
	if found {
		t.free = freeRemove(t.free, best)
		t.changed[best] = struct{}{}
		t.allocated = append(t.allocated, best)
		t.stats.ReuseAllocations++
		return best, nil
	}
	if t.highWater == math.MaxUint64 {
		return 0, ErrNoAllocatablePage
	}
	id := t.highWater
	t.highWater++
	t.allocated = append(t.allocated, id)
	t.stats.AppendAllocations++
	return id, nil
}
func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
func (t *FreelistTxn) ReservePage(id uint64) error {
	if err := t.valid(); err != nil {
		return err
	}
	if t.ledger.Reserved(id) {
		return ErrPageReserved
	}
	t.allocated = append(t.allocated, id)
	return nil
}
func (t *FreelistTxn) Retire(id, seq uint64) {
	if t == nil || id == 0 || seq == 0 {
		return
	}
	t.free = freeRemove(t.free, id)
	t.retired = retiredSet(t.retired, id, seq)
	t.changed[id] = struct{}{}
}
func (t *FreelistTxn) Prune(h RecoveryHorizon) {
	if t == nil {
		return
	}
	var promote []uint64
	retiredWalk(t.retired, func(id, seq uint64) {
		if h.permits(seq) {
			promote = append(promote, id)
			t.stats.ReuseLag = max(t.stats.ReuseLag, h.OldestRecoverableCommitSeq-seq)
		}
	})
	for _, id := range promote {
		t.retired = retiredDelete(t.retired, id)
		t.free = freeInsert(t.free, id)
		t.changed[id] = struct{}{}
	}
}
func (t *FreelistTxn) Reserve(candidate string) error {
	if t == nil || candidate == "" {
		return fmt.Errorf("empty candidate")
	}
	if err := t.ledger.reserve(candidate, t.allocated); err != nil {
		return err
	}
	t.stats.Reservations = uint64(len(t.allocated))
	return nil
}
func (t *FreelistTxn) Materialize(id uint64) (*FreelistGenerationV1, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	g := &FreelistGenerationV1{generationID: id, highWater: t.highWater, free: t.free, retired: t.retired}
	if err := g.Validate(); err != nil {
		return nil, err
	}
	t.stats.LogicalDelta = uint64(len(t.changed))
	t.stats.COWChunks = t.stats.LogicalDelta
	t.stats.COWPages = t.stats.COWChunks * uint64(trieDepth+1)
	t.stats.COWBytes = t.stats.COWPages * 4096
	return g, nil
}
func (t *FreelistTxn) Stats() FreelistTxnStats { return t.stats }

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
	owners     map[uint64]string
	candidates map[string]*reservation
}

func NewReservationLedger() *ReservationLedger {
	return &ReservationLedger{owners: map[uint64]string{}, candidates: map[string]*reservation{}}
}
func (l *ReservationLedger) Reserved(id uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.owners[id]
	return ok
}
func (l *ReservationLedger) reserve(c string, ids []uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.candidates[c]; ok {
		return fmt.Errorf("candidate %q exists", c)
	}
	for _, id := range ids {
		if _, ok := l.owners[id]; ok {
			return ErrPageReserved
		}
	}
	r := &reservation{ids: map[uint64]struct{}{}}
	for _, id := range ids {
		l.owners[id] = c
		r.ids[id] = struct{}{}
	}
	l.candidates[c] = r
	return nil
}
func (l *ReservationLedger) MarkVisible(c string) error { return l.transition(c, CandidateVisible) }
func (l *ReservationLedger) Retry(c string) error       { return l.transition(c, CandidateRetryable) }
func (l *ReservationLedger) Poison(c string) error      { return l.transition(c, CandidatePoisoned) }
func (l *ReservationLedger) Shutdown(c string) error    { return l.transition(c, CandidateShutdown) }
func (l *ReservationLedger) transition(c string, s CandidateState) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.candidates[c]
	if r == nil {
		return fmt.Errorf("unknown candidate %q", c)
	}
	r.state = s
	return nil
}
func (l *ReservationLedger) Supersede(old, next string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.candidates[old]
	if r == nil {
		return fmt.Errorf("unknown candidate %q", old)
	}
	if _, ok := l.candidates[next]; ok {
		return fmt.Errorf("candidate %q exists", next)
	}
	l.candidates[next] = r
	delete(l.candidates, old)
	for id := range r.ids {
		l.owners[id] = next
	}
	return nil
}
func (l *ReservationLedger) release(c string, onlyPre bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.candidates[c]
	if r == nil {
		return fmt.Errorf("unknown candidate %q", c)
	}
	if onlyPre && r.state != CandidatePreVisible {
		return fmt.Errorf("candidate %q is visible", c)
	}
	for id := range r.ids {
		delete(l.owners, id)
	}
	delete(l.candidates, c)
	return nil
}
func (l *ReservationLedger) Abandon(c string) error { return l.release(c, true) }
func (l *ReservationLedger) Publish(c string) error { return l.release(c, false) }
func (l *ReservationLedger) Fail(c string) error    { return l.Abandon(c) }
func (l *ReservationLedger) Reservations() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return uint64(len(l.owners))
}

func (g *FreelistGenerationV1) MarshalBinary() ([]byte, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}
	free := []uint64{}
	ret := []retiredPage{}
	freeWalk(g.free, func(id uint64) { free = append(free, id) })
	retiredWalk(g.retired, func(id, s uint64) { ret = append(ret, retiredPage{id, s}) })
	n := freelistGenerationHeaderSize + len(free)*8 + len(ret)*16
	b := make([]byte, n)
	copy(b, freelistGenerationMagic)
	binary.LittleEndian.PutUint64(b[8:], g.generationID)
	binary.LittleEndian.PutUint64(b[16:], g.highWater)
	binary.LittleEndian.PutUint64(b[24:], uint64(len(free)))
	binary.LittleEndian.PutUint64(b[32:], uint64(len(ret)))
	o := 44
	for _, id := range free {
		binary.LittleEndian.PutUint64(b[o:], id)
		o += 8
	}
	for _, p := range ret {
		binary.LittleEndian.PutUint64(b[o:], p.id)
		binary.LittleEndian.PutUint64(b[o+8:], p.lastReachableCommitSeq)
		o += 16
	}
	binary.LittleEndian.PutUint32(b[40:], generationChecksum(b))
	return b, nil
}
func DecodeFreelistGenerationV1(b []byte) (*FreelistGenerationV1, error) {
	if len(b) < 44 || !bytes.Equal(b[:8], []byte(freelistGenerationMagic)) {
		return nil, ErrGenerationFormat
	}
	if binary.LittleEndian.Uint32(b[40:]) != generationChecksum(b) {
		return nil, ErrGenerationChecksum
	}
	f, r := binary.LittleEndian.Uint64(b[24:]), binary.LittleEndian.Uint64(b[32:])
	if f > maxGenerationEntries || r > maxGenerationEntries || f > (uint64(len(b)-44)/8) {
		return nil, ErrGenerationFormat
	}
	need := uint64(44) + f*8 + r*16
	if need != uint64(len(b)) {
		return nil, ErrGenerationFormat
	}
	free := make([]uint64, int(f))
	ret := make(map[uint64]uint64, int(r))
	o := 44
	for i := range free {
		free[i] = binary.LittleEndian.Uint64(b[o:])
		o += 8
	}
	for i := uint64(0); i < r; i++ {
		ret[binary.LittleEndian.Uint64(b[o:])] = binary.LittleEndian.Uint64(b[o+8:])
		o += 16
	}
	return NewFreelistGenerationV1(binary.LittleEndian.Uint64(b[8:]), binary.LittleEndian.Uint64(b[16:]), free, ret)
}
func generationChecksum(b []byte) uint32 {
	return crc32.Update(crc32.Update(0, crc32.IEEETable, b[:40]), crc32.IEEETable, b[44:])
}
