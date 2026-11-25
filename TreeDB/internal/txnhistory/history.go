// Package txnhistory owns the engine's bounded mutation FIFO and its derived
// latest-key index. Callers serialize prepare/validation/install with publication.
package txnhistory

import (
	"encoding/binary"
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
	namespace                        Namespace
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
	indexSlot    int32 // -1 when not indexed or superseded
	// Short singleton keys belong to their original FIFO slot. The slot is
	// not reused until its record and any matching latest-index alias retire.
	inlineKey [32]byte
}
type latest struct {
	namespace  Namespace
	key        string
	seq        uint64
	home       uint32
	recordSlot int32
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

// ControlAllocationSizes reports the same concrete original arrays that New
// admits before allocation. Finite callers use it to select a fitting ring
// after record layouts change; n must satisfy New's ring-size contract.
func ControlAllocationSizes(n int) [5]uint64 {
	return [5]uint64{uint64(unsafe.Sizeof(History{})), uint64(n) * uint64(unsafe.Sizeof(mutation{})), uint64(2*n) * uint64(unsafe.Sizeof(latest{})), uint64(n) * uint64(unsafe.Sizeof(effect{})), 2 * uint64(unsafe.Sizeof(uintptr(0)))}
}

func New(n int, maxBytes uint64, admit Admit) (*History, error) {
	if n < 8 || n&(n-1) != 0 || maxBytes == 0 {
		panic("invalid finite transaction history limits")
	}
	var lease Lease
	var err error
	if admit != nil {
		s := ControlAllocationSizes(n)
		lease, err = admit(s[0], s[1], s[2], s[3], s[4])
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
func (j *History) hashNamespace(namespace Namespace) uint64 {
	if namespace == (Namespace{}) {
		return 0
	}
	var identity [25]byte
	identity[0] = byte(namespace.Kind)
	binary.LittleEndian.PutUint64(identity[1:9], namespace.Collection)
	binary.LittleEndian.PutUint64(identity[9:17], namespace.Domain)
	binary.LittleEndian.PutUint64(identity[17:25], namespace.Interpretation)
	return maphash.Bytes(j.seed, identity[:])
}
func (j *History) hashKey(namespace Namespace, key string) uint64 {
	hash := maphash.String(j.seed, key)
	if namespace != (Namespace{}) {
		hash ^= j.hashNamespace(namespace)
	}
	return hash
}
func (j *History) slot(key string) int { return j.slotIn(Namespace{}, key) }
func (j *History) slotIn(namespace Namespace, key string) int {
	mask := uint32(len(j.index) - 1)
	i := int(uint32(j.hashKey(namespace, key)) & mask)
	for j.index[i].seq != 0 && (j.index[i].key != key || j.index[i].namespace != namespace) {
		i = (i + 1) & int(mask)
	}
	return i
}
func (j *History) removeSlot(slot int) {
	mask := uint32(len(j.index) - 1)
	hole := slot
	j.points--
	for k := (hole + 1) & int(mask); j.index[k].seq != 0; k = (k + 1) & int(mask) {
		h := j.index[k].home
		if (uint32(hole)-h)&mask < (uint32(k)-h)&mask {
			j.index[hole] = j.index[k]
			j.records[j.index[hole].recordSlot].indexSlot = int32(hole)
			hole = k
		}
	}
	j.index[hole] = latest{}
}
func (j *History) Evict() {
	e := j.records[j.head]
	if e.seq > j.floor {
		j.floor = e.seq
	}
	if !e.rangeEffect && !e.barrier && e.indexSlot >= 0 {
		j.removeSlot(int(e.indexSlot))
	}
	if e.rangeEffect {
		j.ranges--
	}
	j.records[j.head] = mutation{}
	j.head = (j.head + 1) & (len(j.records) - 1)
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

func preparationShape(capacity int, maxBytes uint64, entries []batch.Entry, ranges []batch.DeleteRange, barrier bool, native []Effect) (int, bool, barrierReason, uint64) {
	reason := barrierLogicalReplacement
	collapse := false
	// Arithmetic is checked against finite ring capacity before addition.
	n := len(entries)
	if len(ranges) > capacity || n > capacity-len(ranges) {
		collapse = true
	} else {
		n += len(ranges)
		if len(native) > capacity-n {
			collapse = true
		} else {
			n += len(native)
		}
	}
	if barrier {
		if n >= capacity {
			collapse = true
		} else {
			n++
		}
	}
	if collapse {
		reason = barrierEffectCapacity
	}
	keyBytes := uint64(0)
	addKey := func(key []byte) {
		if collapse {
			return
		}
		if uint64(len(key)) > maxBytes/2-keyBytes {
			collapse = true
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
	for _, e := range native {
		if e.Kind != BarrierEffect {
			addKey(e.Key)
			if e.Kind == RangeEffect {
				addKey(e.End)
			}
		}
	}
	if !collapse && uint64(n)*uint64(unsafe.Sizeof(effect{}))+keyBytes+256 > maxBytes {
		collapse = true
		reason = barrierByteCapacity
	}
	return n, collapse, reason, keyBytes
}
func preparationInline(collapse, barrier bool, entries []batch.Entry, ranges []batch.DeleteRange, native []Effect, keyBytes uint64) bool {
	return !collapse && !barrier && len(native) == 0 && len(entries) == 1 && len(ranges) == 0 && entries[0].Type != batch.OpDeleteRange && keyBytes <= uint64(len(mutation{}.inlineKey))
}

// PreparationAllocationSizes reports only not-yet-owned key arena backing.
// Collapsed barriers and inline singletons use the original admitted ring.
func PreparationAllocationSizes(capacity int, maxBytes uint64, entries []batch.Entry, ranges []batch.DeleteRange, barrier bool, native []Effect) [5]uint64 {
	_, collapse, _, keyBytes := preparationShape(capacity, maxBytes, entries, ranges, barrier, native)
	if collapse || keyBytes == 0 || preparationInline(collapse, barrier, entries, ranges, native, keyBytes) {
		return [5]uint64{}
	}
	return [5]uint64{0, 0, keyBytes, 0, 0}
}

// Prepare preserves the raw-key API. Semantic participants use PrepareEffects
// so raw byte keys never carry catalog or collection identities.
func (j *History) Prepare(entries []batch.Entry, ranges []batch.DeleteRange, barrier bool) (Prepared, error) {
	return j.PrepareEffects(entries, ranges, barrier, nil)
}

// PrepareEffects reserves one command in the SAME FIFO and latest-key index.
// All backing is owned before Install. Overflow collapses only the affected
// namespaces to conservative barriers; it never turns semantic keys into raw
// entries or changes the history floor for an unaccepted command.
func (j *History) PrepareEffects(entries []batch.Entry, ranges []batch.DeleteRange, barrier bool, native []Effect) (Prepared, error) {
	if j == nil || len(j.records) == 0 {
		return Prepared{}, errors.New("treedb: transaction history closed")
	}
	if j.pending.token != 0 {
		return Prepared{}, errors.New("treedb: transaction history preparation already pending")
	}
	if j.nextToken == ^uint64(0) {
		return Prepared{}, errors.New("treedb: transaction history preparation identity exhausted")
	}
	if len(entries) == 1 && len(ranges) == 0 && !barrier && len(native) == 0 && entries[0].Type != batch.OpDeleteRange {
		key := entries[0].Key
		keyBytes := uint64(len(key))
		if keyBytes <= uint64(len(mutation{}.inlineKey)) && keyBytes <= j.maxBytes/2 {
			if charge := uint64(unsafe.Sizeof(effect{})) + keyBytes + 256; charge <= j.maxBytes {
				j.pending = preparation{effects: j.staging[:1], bytes: charge, inline: true}
				copy(j.pending.inlineKey[:], key)
				var owned string
				if len(key) != 0 {
					owned = unsafe.String(&j.pending.inlineKey[0], len(key))
				}
				j.staging[0] = effect{key: owned}
				j.nextToken++
				j.pending.token = j.nextToken
				return Prepared{history: j, token: j.nextToken}, nil
			}
		}
	}
	for _, e := range native {
		if e.Kind < PointEffect || e.Kind > BarrierEffect {
			return Prepared{}, ErrInvalidEffect
		}
	}
	n, collapse, reason, keyBytes := preparationShape(len(j.records), j.maxBytes, entries, ranges, barrier, native)
	effects := j.staging[:0]
	if collapse {
		addNamespace := func(ns Namespace) bool {
			for _, e := range effects {
				if e.namespace == ns {
					return true
				}
			}
			if len(effects) == len(j.records) {
				return false
			}
			effects = append(effects, effect{namespace: ns, barrier: true, barrierReason: reason})
			return true
		}
		if len(entries) != 0 || len(ranges) != 0 || barrier {
			addNamespace(Namespace{})
		}
		for _, e := range native {
			if !addNamespace(e.Namespace) {
				clear(effects)
				return Prepared{}, ErrEffectCapacity
			}
		}
		n = len(effects)
		keyBytes = 0
	}
	if n == 0 {
		return Prepared{}, nil
	}
	charge := uint64(n)*uint64(unsafe.Sizeof(effect{})) + keyBytes + 256
	if charge > j.maxBytes {
		clear(effects)
		return Prepared{}, ErrEffectCapacity
	}
	var lease Lease
	var err error
	inline := preparationInline(collapse, barrier, entries, ranges, native, keyBytes)
	if j.admit != nil && keyBytes != 0 && !inline {
		lease, err = j.admit(0, 0, keyBytes, 0, 0)
		if err != nil {
			clear(effects)
			return Prepared{}, err
		}
	}
	j.pending = preparation{effects: effects, bytes: charge, lease: lease, inline: inline}
	if !collapse {
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
		if barrier {
			j.pending.effects = append(j.pending.effects, effect{barrier: true, barrierReason: barrierLogicalReplacement})
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
		for _, e := range native {
			switch e.Kind {
			case PointEffect:
				j.pending.effects = append(j.pending.effects, effect{namespace: e.Namespace, key: own(e.Key)})
			case RangeEffect:
				if !batch.IsDeleteRangeNoop(e.Key, e.End) {
					j.pending.effects = append(j.pending.effects, effect{namespace: e.Namespace, key: own(e.Key), end: own(e.End), rangeEffect: true, endBounded: e.End != nil})
				}
			case BarrierEffect:
				j.pending.effects = append(j.pending.effects, effect{namespace: e.Namespace, barrier: true, barrierReason: barrierLogicalReplacement})
			}
		}
	}
	// Points precede ranges/barriers for the existing chunk binary search. Within
	// points use the explicit namespace tuple and then canonical byte ordering.
	// Bulk native inserts already emit strictly sorted point effects; skip the
	// O(N log N) sort and deduplication pass when that holds.
	strictSortedPoints := true
	for i, e := range j.pending.effects {
		if e.rangeEffect || e.barrier {
			strictSortedPoints = false
			break
		}
		if i > 0 {
			prev := j.pending.effects[i-1]
			c := compareNamespace(prev.namespace, e.namespace)
			if c > 0 || (c == 0 && prev.key >= e.key) {
				strictSortedPoints = false
				break
			}
		}
	}
	if !strictSortedPoints {
		slices.SortFunc(j.pending.effects, func(a, b effect) int {
			ap, bp := !a.rangeEffect && !a.barrier, !b.rangeEffect && !b.barrier
			if ap != bp {
				if ap {
					return -1
				}
				return 1
			}
			if c := compareNamespace(a.namespace, b.namespace); c != 0 {
				return c
			}
			return strings.Compare(a.key, b.key)
		})
		out := j.pending.effects[:0]
		for _, e := range j.pending.effects {
			if e.rangeEffect || e.barrier || len(out) == 0 || out[len(out)-1].namespace != e.namespace || out[len(out)-1].key != e.key {
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
	if j.pending.inline {
		bytes := j.pending.bytes
		for j.count > 0 && (j.count+1 > len(j.records) || j.bytes > j.maxBytes-bytes) {
			j.Evict()
		}
		recordMask := len(j.records) - 1
		slot := (j.head + j.count) & recordMask
		record := &j.records[slot]
		keyLen := len(j.staging[0].key)
		*record = mutation{
			seq:          seq,
			chunkEnd:     slot,
			releaseBytes: bytes,
			indexSlot:    -1,
		}
		var key string
		if keyLen != 0 {
			copy(record.inlineKey[:keyLen], j.pending.inlineKey[:keyLen])
			key = unsafe.String(&record.inlineKey[0], keyLen)
			record.key = key
		}
		j.count++
		j.bytes += bytes
		indexMask := uint32(len(j.index) - 1)
		home := uint32(maphash.String(j.seed, key)) & indexMask
		i := int(home)
		for j.index[i].seq != 0 && (j.index[i].key != key || j.index[i].namespace != (Namespace{})) {
			i = (i + 1) & int(indexMask)
		}
		if j.index[i].seq != 0 {
			j.records[j.index[i].recordSlot].indexSlot = -1
		} else {
			j.points++
		}
		j.index[i] = latest{
			key:        key,
			seq:        seq,
			home:       home,
			recordSlot: int32(slot),
		}
		record.indexSlot = int32(i)
		j.staging[0] = effect{}
		j.pending = preparation{}
		return true
	}
	// Preparation owns all new backing before final validation, but cannot
	// expire another transaction or discard accepted evidence. Only an accepted
	// installation retires old records. Callers keep writer exclusion here and
	// invoke Install outside the view-install latch; lease release may lock.
	for j.count > 0 && (j.count+len(j.pending.effects) > len(j.records) || j.bytes > j.maxBytes-j.pending.bytes) {
		j.Evict()
	}
	recordMask := len(j.records) - 1
	indexMask := uint32(len(j.index) - 1)
	end := (j.head + j.count + len(j.pending.effects) - 1) & recordMask
	j.bytes += j.pending.bytes
	var cachedNS Namespace
	var cachedNSHash uint64
	hasCachedNS := false
	for _, e := range j.pending.effects {
		slot := (j.head + j.count) & recordMask
		record := &j.records[slot]
		*record = mutation{effect: e, seq: seq, chunkEnd: end, indexSlot: -1}
		j.count++
		if e.barrier {
			if e.namespace == (Namespace{}) {
				j.barrierSeq = seq
			}
			j.rangeSeq = seq
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
			if !hasCachedNS || e.namespace != cachedNS {
				cachedNS = e.namespace
				cachedNSHash = j.hashNamespace(e.namespace)
				hasCachedNS = true
			}
			home := uint32(maphash.String(j.seed, e.key)^cachedNSHash) & indexMask
			i := int(home)
			for j.index[i].seq != 0 && (j.index[i].key != e.key || j.index[i].namespace != e.namespace) {
				i = (i + 1) & int(indexMask)
			}
			if j.index[i].seq != 0 {
				j.records[j.index[i].recordSlot].indexSlot = -1
			} else {
				j.points++
			}
			j.index[i] = latest{
				namespace:  e.namespace,
				key:        e.key,
				seq:        seq,
				home:       home,
				recordSlot: int32(slot),
			}
			record.indexSlot = int32(i)
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
	mask := len(j.records) - 1
	lo, hi := 0, j.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		if j.records[(j.head+mid)&mask].seq <= start {
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
	mask := len(j.records) - 1
	endSlot := j.records[(j.head+begin)&mask].chunkEnd
	lo, hi := begin+1, j.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		if j.records[(j.head+mid)&mask].chunkEnd == endSlot {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	end = lo
	lo, hi = begin, end
	for lo < hi {
		mid := lo + (hi-lo)/2
		e := &j.records[(j.head+mid)&mask]
		if !e.rangeEffect && !e.barrier {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, end
}
func (j *History) PointChanged(start uint64, key []byte) bool {
	return j.PointChangedIn(start, Namespace{}, key)
}
func (j *History) PointChangedIn(start uint64, namespace Namespace, key []byte) bool {
	if j == nil {
		return false
	}
	if start < j.floor || (namespace == (Namespace{}) && j.barrierSeq > start) {
		return true
	}
	if j.index[j.slotIn(namespace, string(key))].seq > start {
		return true
	}
	return j.pointCoveredByRange(start, namespace, key)
}

func (j *History) pointCoveredByRange(start uint64, namespace Namespace, key []byte) bool {
	if j.rangeSeq <= start {
		return false
	}
	mask := len(j.records) - 1
	for k := j.firstNewer(start); k < j.count; {
		pointsEnd, end := j.chunkBounds(k)
		for r := pointsEnd; r < end; r++ {
			e := &j.records[(j.head+r)&mask]
			if e.namespace == namespace && (e.barrier || e.rangeEffect && string(key) >= e.key && (!e.endBounded || string(key) < e.end)) {
				return true
			}
		}
		k = end
	}
	return false
}
func (j *History) RangeChanged(start uint64, lo, hi []byte) bool {
	return j.RangeChangedIn(start, Namespace{}, lo, hi)
}
func (j *History) RangeChangedIn(start uint64, namespace Namespace, lo, hi []byte) bool {
	if j == nil {
		return false
	}
	if start < j.floor || (namespace == (Namespace{}) && j.barrierSeq > start) {
		return true
	}
	if batch.IsDeleteRangeNoop(lo, hi) {
		return false
	}
	mask := len(j.records) - 1
	for k := j.firstNewer(start); k < j.count; {
		pointsEnd, end := j.chunkBounds(k)
		lower, upper := k, pointsEnd
		for lower < upper {
			mid := lower + (upper-lower)/2
			e := &j.records[(j.head+mid)&mask]
			c := compareNamespace(e.namespace, namespace)
			if c < 0 || c == 0 && e.key < string(lo) {
				lower = mid + 1
			} else {
				upper = mid
			}
		}
		if lower < pointsEnd {
			e := &j.records[(j.head+lower)&mask]
			if e.namespace == namespace && (hi == nil || e.key < string(hi)) {
				return true
			}
		}
		for r := pointsEnd; r < end; r++ {
			e := &j.records[(j.head+r)&mask]
			if e.namespace == namespace && (e.barrier || e.rangeEffect && (hi == nil || e.key < string(hi)) && (!e.endBounded || string(lo) < e.end)) {
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
