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
type Admit func(uint64, uint64, uint64, uint64, uint64) (Lease, error)
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
type effect struct {
	key, end                         string
	rangeEffect, barrier, endBounded bool
	barrierReason                    barrierReason
}
type mutation struct {
	effect
	seq          uint64
	chunkEnd     int
	releaseBytes uint64
	lease        Lease
	// Short singleton keys belong to their original FIFO slot. The slot is
	// not reused until its record and any matching latest-index alias retire.
	inlineKey [32]byte
}
type latest struct {
	key string
	seq uint64
}

// Prepared is a by-value capability for one serialized preaccept reservation.
// Copies become inert after Install/Cancel, including after later preparations.
// Its nonce is ownership identity, never a logical transaction/commit clock.
type Prepared struct {
	history *History
	token   uint64
}

func (p Prepared) Valid() bool {
	return p.history != nil && p.token != 0 && p.history.pending.token == p.token
}
func (p Prepared) Cancel() {
	if p.Valid() {
		p.history.clearPending(true)
	}
}

type preparation struct {
	effects []effect
	bytes   uint64
	lease   Lease
	token   uint64
	inline  bool
	// One private singleton staging buffer preserves accepted slot/index bytes
	// until Install. It belongs to the already admitted History allocation.
	inlineKey [32]byte
}
type History struct {
	records                                      []mutation
	index                                        []latest
	staging                                      []effect
	pending                                      preparation
	nextToken                                    uint64
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
	var lease Lease
	var err error
	if admit != nil {
		lease, err = admit(uint64(unsafe.Sizeof(History{})), uint64(n)*uint64(unsafe.Sizeof(mutation{})), uint64(2*n)*uint64(unsafe.Sizeof(latest{})), uint64(n)*uint64(unsafe.Sizeof(effect{})), 2*uint64(unsafe.Sizeof(uintptr(0))))
		if err != nil {
			return nil, err
		}
	}
	return &History{records: make([]mutation, n), index: make([]latest, 2*n), staging: make([]effect, n), seed: maphash.MakeSeed(), lease: lease, admit: admit, maxBytes: maxBytes}, nil
}
func (j *History) clearPending(closeLease bool) {
	clear(j.pending.effects)
	lease := j.pending.lease
	j.pending = preparation{}
	if closeLease && lease != nil {
		lease.Close()
	}
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
	// The final slot survives every earlier fragment of this command. Its
	// original key arena/lease is released only after index and ring aliases clear.
	if e.releaseBytes != 0 {
		j.bytes -= e.releaseBytes
	}
	if e.lease != nil {
		e.lease.Close()
	}
}
func (j *History) Prepare(entries []batch.Entry, ranges []batch.DeleteRange, barrier bool) (Prepared, error) {
	if j == nil || len(j.records) == 0 {
		return Prepared{}, errors.New("treedb: transaction history closed")
	}
	if j.pending.token != 0 {
		return Prepared{}, errors.New("treedb: transaction history preparation already pending")
	}
	if j.nextToken == ^uint64(0) {
		return Prepared{}, errors.New("treedb: transaction history preparation identity exhausted")
	}
	n := 1
	reason := barrierLogicalReplacement
	if len(entries) > len(j.records) || len(ranges) > len(j.records)-len(entries) {
		barrier = true
		reason = barrierEffectCapacity
	} else {
		n = len(entries) + len(ranges)
	}
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
		return Prepared{}, nil
	}
	charge := uint64(n)*uint64(unsafe.Sizeof(effect{})) + keyBytes + 256
	if charge > j.maxBytes {
		barrier = true
		reason = barrierByteCapacity
		n = 1
		keyBytes = 0
		charge = uint64(unsafe.Sizeof(effect{})) + 256
	}
	if charge > j.maxBytes {
		return Prepared{}, errors.New("treedb: transaction history capacity exhausted")
	}
	var lease Lease
	var err error
	// Descriptor/ring capacity was admitted once. Only fresh immutable key
	// backing needs a per-command allocation/lease; empty barriers need neither.
	inline := !barrier && len(entries) == 1 && len(ranges) == 0 && entries[0].Type != batch.OpDeleteRange && keyBytes <= uint64(len(mutation{}.inlineKey))
	if j.admit != nil && keyBytes != 0 && !inline {
		lease, err = j.admit(0, 0, keyBytes, 0, 0)
		if err != nil {
			return Prepared{}, err
		}
	}
	j.pending = preparation{effects: j.staging[:0], bytes: charge, lease: lease, inline: inline}
	if barrier {
		j.pending.effects = append(j.pending.effects, effect{barrier: true, barrierReason: reason})
	} else {
		var arena []byte
		if inline {
			arena = j.pending.inlineKey[:keyBytes]
		} else {
			arena = make([]byte, keyBytes)
		}
		off := 0
		own := func(key []byte) string {
			copy(arena[off:], key)
			part := arena[off : off+len(key)]
			off += len(key)
			if len(part) == 0 {
				return ""
			}
			return unsafe.String(unsafe.SliceData(part), len(part))
		}
		for _, e := range entries {
			if e.Type == batch.OpDeleteRange {
				if !batch.IsDeleteRangeNoop(e.Key, e.Value) {
					j.pending.effects = append(j.pending.effects, effect{key: own(e.Key), end: own(e.Value), rangeEffect: true, endBounded: e.Value != nil})
				}
			} else {
				j.pending.effects = append(j.pending.effects, effect{key: own(e.Key)})
			}
		}
		for _, r := range ranges {
			if !batch.IsDeleteRangeNoop(r.Start, r.End) {
				j.pending.effects = append(j.pending.effects, effect{key: own(r.Start), end: own(r.End), rangeEffect: true, endBounded: r.End != nil})
			}
		}
		if len(j.pending.effects) > 1 {
			slices.SortFunc(j.pending.effects, func(a, b effect) int {
				if a.rangeEffect != b.rangeEffect {
					if a.rangeEffect {
						return 1
					}
					return -1
				}
				return strings.Compare(a.key, b.key)
			})
		}
		out := j.pending.effects[:0]
		for _, e := range j.pending.effects {
			if e.rangeEffect || len(out) == 0 || out[len(out)-1].key != e.key {
				out = append(out, e)
			}
		}
		clear(j.pending.effects[len(out):])
		j.pending.effects = out
	}
	if len(j.pending.effects) == 0 {
		j.clearPending(true)
		return Prepared{}, nil
	}
	j.nextToken++
	j.pending.token = j.nextToken
	return Prepared{history: j, token: j.nextToken}, nil
}

// Install consumes the capability once. A stale copied capability cannot
// publish or cancel a later reservation. Actual accepted callers retain their
// serialized live capability through this nonallocating installation.
func (j *History) Install(p Prepared, seq uint64) bool {
	if p.history != j || !p.Valid() {
		return false
	}
	// Preparation owns all new backing before final validation, but cannot
	// expire another transaction or discard accepted evidence. Only an accepted
	// installation retires old records. Callers keep writer exclusion here and
	// invoke Install outside the view-install latch; lease release may lock.
	for j.count > 0 && (j.count+len(j.pending.effects) > len(j.records) || j.bytes > j.maxBytes-j.pending.bytes) {
		j.Evict()
	}
	end := (j.head + j.count + len(j.pending.effects) - 1) % len(j.records)
	j.bytes += j.pending.bytes
	for _, e := range j.pending.effects {
		slot := (j.head + j.count) % len(j.records)
		record := &j.records[slot]
		*record = mutation{effect: e, seq: seq, chunkEnd: end}
		if j.pending.inline {
			copy(record.inlineKey[:], e.key)
			if len(e.key) != 0 {
				e.key = unsafe.String(&record.inlineKey[0], len(e.key))
			}
			record.key = e.key
		}
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
	j.records[end].releaseBytes = j.pending.bytes
	j.records[end].lease = j.pending.lease
	j.clearPending(false)
	return true
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

// A command occupies one contiguous logical ring span. Its physical final slot
// is unique among retained commands: reusing it first evicts the old command.
// Prepare places sorted points before native ranges, so both boundaries can be
// found without inspecting a large unrelated point chunk entry by entry.
func (j *History) chunkBounds(begin int) (pointsEnd, end int) {
	endSlot := j.records[(j.head+begin)%len(j.records)].chunkEnd
	lo, hi := begin+1, j.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		if j.records[(j.head+mid)%len(j.records)].chunkEnd == endSlot {
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
	if j.pending.token != 0 {
		j.clearPending(true)
	}
	j.records = nil
	j.index = nil
	j.staging = nil
	j.admit = nil
	if j.lease != nil {
		j.lease.Close()
		j.lease = nil
	}
}
