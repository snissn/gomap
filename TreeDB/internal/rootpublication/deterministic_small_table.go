package rootpublication

import "unsafe"

// stableSmallTable is the shared deterministic-capacity representation for
// small field/kind metadata. It preserves map alias semantics through one
// private owner; sorted records permit allocation-free in-engine traversal.
// Every growth allocates a known full capacity and records old/new overlap.
type stableSmallTable[K any, V any] struct {
	entries    []stableSmallBinding[K, V]
	less       func(K, K) bool
	keyPlan    func(K) (uint64, uint64, error)
	ownKey     func(K) K
	census     BackingCensus
	arrayStamp backingStamp
}
type stableSmallBinding[K any, V any] struct {
	key   K
	value V
}
type preparedStableSmallInsert[K any, V any] struct {
	table                    *stableSmallTable[K, V]
	entries                  []stableSmallBinding[K, V]
	index                    int
	key                      K
	value                    V
	existing                 bool
	arrayStamp               backingStamp
	keyBytes, keyAllocations uint64
}

func newStableSmallTable[K any, V any](capacity int, less func(K, K) bool) *stableSmallTable[K, V] {
	if capacity < 0 {
		panic("negative stable table capacity")
	}
	stamp, err := prepareBackingStamp(uint64(unsafe.Sizeof(stableSmallTable[K, V]{})), true, nil)
	t := &stableSmallTable[K, V]{less: less}
	if err == nil {
		t.census.add(stamp)
	}
	if capacity != 0 {
		stamp, err = prepareBackingStamp(uint64(capacity)*uint64(unsafe.Sizeof(stableSmallBinding[K, V]{})), true, nil)
		t.entries = make([]stableSmallBinding[K, V], 0, capacity)
		t.arrayStamp = stamp
		if err == nil {
			t.census.add(stamp)
		}
	}
	return t
}
func (t *stableSmallTable[K, V]) len() int {
	if t == nil {
		return 0
	}
	return len(t.entries)
}
func (t *stableSmallTable[K, V]) records() []stableSmallBinding[K, V] {
	if t == nil {
		return nil
	}
	return t.entries
}
func (t *stableSmallTable[K, V]) position(k K) (int, bool) {
	lo, hi := 0, len(t.entries)
	for lo < hi {
		m := lo + (hi-lo)/2
		if t.less(t.entries[m].key, k) {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < len(t.entries) && !t.less(k, t.entries[lo].key)
}
func (t *stableSmallTable[K, V]) lookup(k K) (V, bool) {
	if t != nil {
		if i, ok := t.position(k); ok {
			return t.entries[i].value, true
		}
	}
	var zero V
	return zero, false
}
func (t *stableSmallTable[K, V]) get(k K) V    { v, _ := t.lookup(k); return v }
func (t *stableSmallTable[K, V]) has(k K) bool { _, ok := t.lookup(k); return ok }
func (t *stableSmallTable[K, V]) prepare(k K, v V, account StableMetadataAccount) (preparedStableSmallInsert[K, V], error) {
	var p preparedStableSmallInsert[K, V]
	if t == nil {
		return p, ErrResourceOwnership
	}
	i, found := t.position(k)
	p = preparedStableSmallInsert[K, V]{table: t, index: i, key: k, value: v, existing: found}
	if found {
		return p, nil
	}
	var keyBytes, keyAllocations uint64
	var err error
	if t.keyPlan != nil {
		keyBytes, keyAllocations, err = t.keyPlan(k)
		if err != nil && account != nil {
			return p, err
		}
	}
	capacity := cap(t.entries)
	var arrayBytes uint64
	if len(t.entries) == capacity {
		if capacity == 0 {
			capacity = 1
		} else {
			if capacity > int(^uint(0)>>1)/2 {
				return p, ErrStableMetadataShapeUnsupported
			}
			capacity *= 2
		}
		size := uint64(unsafe.Sizeof(stableSmallBinding[K, V]{}))
		if uint64(capacity) > ^uint64(0)/size {
			return p, ErrStableMetadataShapeUnsupported
		}
		arrayBytes, err = StableBackingClassBytes(uint64(capacity)*size, true)
		if err != nil && account != nil {
			return p, err
		}
	}
	total, err := finiteStableAdd(arrayBytes, keyBytes)
	if err != nil {
		return p, err
	}
	if account != nil {
		if err = account.ReserveStableMetadata(total); err != nil {
			return p, err
		}
	}
	if t.ownKey != nil {
		p.key = t.ownKey(k)
	}
	p.keyBytes, p.keyAllocations = keyBytes, keyAllocations
	if capacity != cap(t.entries) {
		stamp, err := prepareBackingStamp(uint64(capacity)*uint64(unsafe.Sizeof(stableSmallBinding[K, V]{})), true, nil)
		if err != nil && account != nil {
			return p, err
		}
		p.entries = make([]stableSmallBinding[K, V], len(t.entries), capacity)
		copy(p.entries, t.entries)
		p.arrayStamp = stamp
	} else {
		p.entries = t.entries
	}
	return p, nil
}
func (p preparedStableSmallInsert[K, V]) apply() {
	t := p.table
	if p.existing {
		t.entries[p.index].value = p.value
		return
	}
	if p.arrayStamp.identity != 0 {
		t.census.remove(t.arrayStamp)
		t.arrayStamp = p.arrayStamp
		t.census.add(p.arrayStamp)
	}
	entries := p.entries
	entries = entries[:len(entries)+1]
	copy(entries[p.index+1:], entries[p.index:])
	entries[p.index] = stableSmallBinding[K, V]{key: p.key, value: p.value}
	t.entries = entries
	t.census.addGroup(p.keyBytes, p.keyAllocations)
}
func (t *stableSmallTable[K, V]) set(k K, v V) {
	p, e := t.prepare(k, v, nil)
	if e != nil {
		panic(e)
	}
	p.apply()
}
func (t *stableSmallTable[K, V]) remove(k K) {
	if t == nil {
		return
	}
	i, ok := t.position(k)
	if !ok {
		return
	}
	if t.keyPlan != nil {
		b, n, e := t.keyPlan(t.entries[i].key)
		if e == nil {
			if t.census.LiveClassBytes < b || t.census.LiveAllocations < n {
				panic("stable small table key census imbalance")
			}
			t.census.LiveClassBytes -= b
			t.census.LiveAllocations -= n
		}
	}
	copy(t.entries[i:], t.entries[i+1:])
	var zero stableSmallBinding[K, V]
	t.entries[len(t.entries)-1] = zero
	t.entries = t.entries[:len(t.entries)-1]
}

type stableReachabilitySet = *stableSmallTable[ReachabilityField, struct{}]
type stableKindViews = *stableSmallTable[ResourceKind, stableResourceKindView]

func newStableReachabilitySet(capacity int, fields ...ReachabilityField) stableReachabilitySet {
	if capacity < len(fields) {
		capacity = len(fields)
	}
	t := newStableSmallTable[ReachabilityField, struct{}](capacity, func(a, b ReachabilityField) bool { return a < b })
	t.keyPlan = func(k ReachabilityField) (uint64, uint64, error) { return backingStringPlan(string(k)) }
	t.ownKey = func(k ReachabilityField) ReachabilityField { return ReachabilityField(finiteStableCopy(string(k))) }
	for _, field := range fields {
		t.set(field, struct{}{})
	}
	return t
}
func newStableKindViews(capacity int) stableKindViews {
	t := newStableSmallTable[ResourceKind, stableResourceKindView](capacity, func(a, b ResourceKind) bool { return a < b })
	t.keyPlan = func(k ResourceKind) (uint64, uint64, error) { return backingStringPlan(string(k)) }
	t.ownKey = func(k ResourceKind) ResourceKind { return ResourceKind(finiteStableCopy(string(k))) }
	return t
}
func newStableKindViewsWith(kind ResourceKind, view stableResourceKindView) stableKindViews {
	t := newStableKindViews(1)
	t.set(kind, view)
	return t
}
