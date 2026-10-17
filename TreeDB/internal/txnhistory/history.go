// Package txnhistory owns the engine's bounded mutation FIFO and its derived
// latest-key index. Callers serialize prepare/validation/install with publication.
package txnhistory

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/batch"
	"hash/maphash"
	"slices"
	"strings"
	"unsafe"
)

var ErrTooOld = errors.New("treedb: transaction conflict history expired")

type Lease interface{ Close() }
type Admit func(uint64, uint64, uint64, uint64) (Lease, error)
type Range struct{ Start, End []byte }
type barrierReason uint8

const (
	barrierLogicalReplacement barrierReason = iota + 1
	barrierEffectCapacity
	barrierByteCapacity
)

// BarrierStats distinguishes accepted conservative fallbacks from real point
// overlap. Canceled preparation never advances these publication counters.
type BarrierStats struct {
	LogicalReplacement, EffectCapacity, ByteCapacity, Conflicts uint64
	Active                                                      bool
}
type mutation struct {
	seq                              uint64
	key, end                         string
	rangeEffect, barrier, endBounded bool
	barrierReason                    barrierReason
	owner                            *Prepared
}
type latest struct {
	key string
	seq uint64
}
type Prepared struct {
	effects   []mutation
	remaining int
	bytes     uint64
	lease     Lease
}

// Singleton commands share one fresh allocation for owner and sole effect.
// Bulk commands keep a compact owner and separately sized effect backing.
type singlePrepared struct {
	Prepared
	effect [1]mutation
}

func (p *Prepared) Cancel() {
	if p == nil {
		return
	}
	clear(p.effects)
	p.effects = nil
	if p.lease != nil {
		p.lease.Close()
		p.lease = nil
	}
}

type History struct {
	records                                      []mutation
	index                                        []latest
	head, count, points, ranges                  int
	floor, barrierSeq, rangeSeq, bytes, maxBytes uint64
	seed                                         maphash.Seed
	lease                                        Lease
	admit                                        Admit
	barriers                                     BarrierStats
}

func New(n int, maxBytes uint64, admit Admit) (*History, error) {
	if n < 8 || n&(n-1) != 0 || maxBytes == 0 {
		panic("invalid finite transaction history limits")
	}
	control := uint64(unsafe.Sizeof(History{}))
	recordBytes := uint64(n) * uint64(unsafe.Sizeof(mutation{}))
	indexBytes := uint64(2*n) * uint64(unsafe.Sizeof(latest{}))
	var lease Lease
	var err error
	if admit != nil {
		lease, err = admit(control, recordBytes, indexBytes, 2*uint64(unsafe.Sizeof(uintptr(0))))
		if err != nil {
			return nil, err
		}
	}
	return &History{records: make([]mutation, n), index: make([]latest, 2*n), seed: maphash.MakeSeed(), lease: lease, admit: admit, maxBytes: maxBytes}, nil
}
func (j *History) slot(key string) int {
	i := int(maphash.String(j.seed, key) & uint64(len(j.index)-1))
	for j.index[i].seq != 0 && j.index[i].key != key {
		i = (i + 1) & (len(j.index) - 1)
	}
	return i
}
func (j *History) remove(key string, seq uint64) {
	i := j.slot(key)
	if j.index[i].seq != seq {
		return
	}
	j.index[i] = latest{}
	j.points--
	for k := (i + 1) & (len(j.index) - 1); j.index[k].seq != 0; k = (k + 1) & (len(j.index) - 1) {
		e := j.index[k]
		j.index[k] = latest{}
		j.index[j.slot(e.key)] = e
	}
}
func (j *History) Evict() {
	e := j.records[j.head]
	if e.seq > j.floor {
		j.floor = e.seq
	}
	if !e.rangeEffect && !e.barrier {
		j.remove(e.key, e.seq)
	}
	if e.rangeEffect {
		j.ranges--
	}
	j.records[j.head] = mutation{}
	j.head = (j.head + 1) % len(j.records)
	j.count--
	e.owner.remaining--
	if e.owner.remaining == 0 {
		j.bytes -= e.owner.bytes
		e.owner.Cancel()
	}
}
func (j *History) Prepare(entries []batch.Entry, ranges []batch.DeleteRange, barrier bool) (*Prepared, error) {
	n := 1
	reason := barrierLogicalReplacement
	if len(entries) > len(j.records) || len(ranges) > len(j.records)-len(entries) {
		barrier = true
		reason = barrierEffectCapacity
	} else {
		n = len(entries) + len(ranges)
	}
	// Saturate before addition. Oversized key storage folds to a future
	// barrier without arithmetic wrap or allocating the oversized arena.
	keyBytes := uint64(0)
	addKey := func(key []byte) {
		if barrier {
			return
		}
		if uint64(len(key)) > j.maxBytes/2-keyBytes {
			barrier = true
			reason = barrierByteCapacity
			return
		}
		keyBytes += uint64(len(key))
	}
	for _, e := range entries {
		addKey(e.Key)
		if e.Type == batch.OpDeleteRange {
			addKey(e.Value)
		}
	}
	for _, r := range ranges {
		addKey(r.Start)
		addKey(r.End)
	}
	if barrier {
		n = 1
		keyBytes = 0
	}
	if n == 0 {
		return nil, nil
	}
	charge := uint64(unsafe.Sizeof(Prepared{})) + uint64(n)*uint64(unsafe.Sizeof(mutation{})) + keyBytes + 256
	if charge > j.maxBytes {
		barrier = true
		reason = barrierByteCapacity
		n = 1
		keyBytes = 0
		charge = uint64(unsafe.Sizeof(Prepared{})) + uint64(unsafe.Sizeof(mutation{})) + 256
	}
	if charge > j.maxBytes {
		return nil, errors.New("treedb: transaction history capacity exhausted")
	}
	for j.count > 0 && (j.count+n > len(j.records) || j.bytes+charge > j.maxBytes) {
		j.Evict()
	}
	ownerBytes, effectBytes := uint64(unsafe.Sizeof(Prepared{})), uint64(n)*uint64(unsafe.Sizeof(mutation{}))
	if n == 1 {
		ownerBytes = uint64(unsafe.Sizeof(singlePrepared{}))
		effectBytes = 0
	}
	var lease Lease
	var err error
	if j.admit != nil {
		lease, err = j.admit(ownerBytes, effectBytes, keyBytes, 256)
		if err != nil {
			return nil, err
		}
	}
	var p *Prepared
	if n == 1 {
		owner := &singlePrepared{}
		p = &owner.Prepared
		p.effects = owner.effect[:0]
	} else {
		p = &Prepared{effects: make([]mutation, 0, n)}
	}
	p.bytes, p.lease = charge, lease
	if barrier {
		p.effects = append(p.effects, mutation{barrier: true, barrierReason: reason})
		return p, nil
	}
	// One owned immutable key backing survives batch reuse and partial eviction.
	arena := make([]byte, keyBytes)
	off := 0
	own := func(key []byte) string {
		copy(arena[off:], key)
		s := arena[off : off+len(key)]
		off += len(key)
		if len(s) == 0 {
			return ""
		}
		return unsafe.String(unsafe.SliceData(s), len(s))
	}
	for _, e := range entries {
		if e.Type == batch.OpDeleteRange {
			if !batch.IsDeleteRangeNoop(e.Key, e.Value) {
				p.effects = append(p.effects, mutation{key: own(e.Key), end: own(e.Value), rangeEffect: true, endBounded: e.Value != nil})
			}
		} else {
			p.effects = append(p.effects, mutation{key: own(e.Key)})
		}
	}
	for _, r := range ranges {
		if !batch.IsDeleteRangeNoop(r.Start, r.End) {
			p.effects = append(p.effects, mutation{key: own(r.Start), end: own(r.End), rangeEffect: true, endBounded: r.End != nil})
		}
	}
	if len(p.effects) > 1 {
		slices.SortFunc(p.effects, func(a, b mutation) int {
			if a.rangeEffect != b.rangeEffect {
				if a.rangeEffect {
					return 1
				}
				return -1
			}
			return strings.Compare(a.key, b.key)
		})
	}

	out := p.effects[:0]
	for _, e := range p.effects {
		if e.rangeEffect || len(out) == 0 || out[len(out)-1].key != e.key {
			out = append(out, e)
		}
	}
	clear(p.effects[len(out):])
	p.effects = out
	if len(out) == 0 {
		p.Cancel()
		return nil, nil
	}
	return p, nil
}
func (j *History) Install(p *Prepared, seq uint64) {
	if p == nil {
		return
	}
	p.remaining = len(p.effects)
	j.bytes += p.bytes
	for _, e := range p.effects {
		e.seq = seq
		e.owner = p
		j.records[(j.head+j.count)%len(j.records)] = e
		j.count++
		if e.barrier {
			j.barrierSeq = seq
			switch e.barrierReason {
			case barrierEffectCapacity:
				j.barriers.EffectCapacity++
			case barrierByteCapacity:
				j.barriers.ByteCapacity++
			case barrierLogicalReplacement:
				j.barriers.LogicalReplacement++
			}
		} else if e.rangeEffect {
			j.ranges++
			j.rangeSeq = seq
		} else {
			i := j.slot(e.key)
			if j.index[i].seq == 0 {
				j.points++
			}
			j.index[i] = latest{key: e.key, seq: seq}
		}
	}
	// The ring now owns effect descriptors. Keep key backing and the original
	// charge through owner.remaining, but release duplicate descriptor aliases.
	clear(p.effects)
	p.effects = nil
}
func (j *History) Check(start uint64) error {
	if j != nil && start < j.floor {
		return ErrTooOld
	}
	return nil
}

// BarrierConflict is called once per validation after Check has distinguished
// history expiry. Callers serialize it with accepted publication.
func (j *History) BarrierConflict(start uint64) bool {
	if j != nil && start >= j.floor && j.barrierSeq > start {
		j.barriers.Conflicts++
		return true
	}
	return false
}
func (j *History) BarrierStats() BarrierStats {
	if j == nil {
		return BarrierStats{}
	}
	s := j.barriers
	s.Active = j.barrierSeq > j.floor
	return s
}

// firstNewer searches logical FIFO order even when physical storage wraps.
// Old retained effects are irrelevant to validation of a newer opening view.
func (j *History) firstNewer(start uint64) int {
	lo, hi := 0, j.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		if j.records[(j.head+mid)%len(j.records)].seq <= start {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// A command's descriptors occupy one contiguous logical ring span. The owner
// already retained for key lifetime identifies its boundary, including wrap.
// Prepare places sorted points before native ranges, so both boundaries can be
// found without inspecting a large unrelated point chunk entry by entry.
func (j *History) chunkBounds(begin int) (pointsEnd, end int) {
	owner := j.records[(j.head+begin)%len(j.records)].owner
	lo, hi := begin+1, j.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		if j.records[(j.head+mid)%len(j.records)].owner == owner {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	end = lo
	lo, hi = begin, end
	for lo < hi {
		mid := lo + (hi-lo)/2
		e := &j.records[(j.head+mid)%len(j.records)]
		if !e.rangeEffect && !e.barrier {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, end
}
func (j *History) PointChanged(start uint64, key []byte) bool {
	if j == nil {
		return false
	}
	if start < j.floor || j.barrierSeq > start {
		return true
	}
	if j.index[j.slot(string(key))].seq > start {
		return true
	}
	if j.rangeSeq <= start {
		return false
	}
	for k := j.firstNewer(start); k < j.count; {
		pointsEnd, end := j.chunkBounds(k)
		for r := pointsEnd; r < end; r++ {
			e := &j.records[(j.head+r)%len(j.records)]
			if e.rangeEffect && string(key) >= e.key && (!e.endBounded || string(key) < e.end) {
				return true
			}
		}
		k = end
	}
	return false
}
func (j *History) RangeChanged(start uint64, lo, hi []byte) bool {
	if j == nil {
		return false
	}
	if start < j.floor || j.barrierSeq > start {
		return true
	}
	if batch.IsDeleteRangeNoop(lo, hi) {
		return false
	}
	for k := j.firstNewer(start); k < j.count; {
		pointsEnd, end := j.chunkBounds(k)
		lower, upper := k, pointsEnd
		for lower < upper {
			mid := lower + (upper-lower)/2
			if j.records[(j.head+mid)%len(j.records)].key < string(lo) {
				lower = mid + 1
			} else {
				upper = mid
			}
		}
		if lower < pointsEnd && (hi == nil || j.records[(j.head+lower)%len(j.records)].key < string(hi)) {
			return true
		}
		for r := pointsEnd; r < end; r++ {
			e := &j.records[(j.head+r)%len(j.records)]
			if e.rangeEffect && (hi == nil || e.key < string(hi)) && (!e.endBounded || string(lo) < e.end) {
				return true
			}
		}
		k = end
	}
	return false
}
func (j *History) Counts() (points, ranges int, barrier bool) {
	if j == nil {
		return
	}
	return j.points, j.ranges, j.barrierSeq > j.floor
}
func (j *History) State() (count int, bytes, floor uint64) {
	if j == nil {
		return
	}
	return j.count, j.bytes, j.floor
}
func (j *History) Close() {
	if j == nil {
		return
	}
	for j.count > 0 {
		j.Evict()
	}
	j.records = nil
	j.index = nil
	if j.lease != nil {
		j.lease.Close()
	}
}
