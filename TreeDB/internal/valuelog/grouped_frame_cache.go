package valuelog

import (
	"sync"
	"sync/atomic"

	templ "github.com/snissn/gomap/TreeDB/template"
)

const (
	groupedFrameCacheMaxShards = 64
	groupedFrameEntryRetired   = uint64(1) << 63
)

// GroupedFrameCacheStats captures decoded grouped-frame cache behavior.
// RetainedBytes is the currently retained decoded-raw bytes; BudgetBytes is the
// configured manager/file budget (0 means no byte budget beyond entry limits).
type GroupedFrameCacheStats struct {
	Hits              uint64
	Misses            uint64
	Stores            uint64
	Evictions         uint64
	Releases          uint64
	RetainedBytes     uint64
	BudgetBytes       uint64
	SkippedDisabled   uint64
	SkippedOversize   uint64
	SkippedBudget     uint64
	SkippedContention uint64
	Entries           int
	Capacity          int
}

type groupedFrameCacheBudget struct {
	limit    atomic.Int64
	retained atomic.Int64
}

func newGroupedFrameCacheBudget(limit int64) *groupedFrameCacheBudget {
	b := &groupedFrameCacheBudget{}
	b.setLimit(limit)
	return b
}

func (b *groupedFrameCacheBudget) setLimit(limit int64) {
	if b == nil {
		return
	}
	if limit < 0 {
		limit = 0
	}
	b.limit.Store(limit)
}

func (b *groupedFrameCacheBudget) reserve(n int) bool {
	if b == nil || n <= 0 {
		return true
	}
	limit := b.limit.Load()
	if limit <= 0 {
		b.retained.Add(int64(n))
		return true
	}
	want := int64(n)
	for {
		cur := b.retained.Load()
		if cur > limit-want {
			return false
		}
		if b.retained.CompareAndSwap(cur, cur+want) {
			return true
		}
	}
}

func (b *groupedFrameCacheBudget) release(n int) {
	if b == nil || n <= 0 {
		return
	}
	b.retained.Add(-int64(n))
}

func (b *groupedFrameCacheBudget) retainedBytes() uint64 {
	if b == nil {
		return 0
	}
	v := b.retained.Load()
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func (b *groupedFrameCacheBudget) budgetBytes() uint64 {
	if b == nil {
		return 0
	}
	v := b.limit.Load()
	if v < 0 {
		return 0
	}
	return uint64(v)
}

type groupedFrameCache struct {
	owner    *File
	maxRaw   int
	maxBytes int64
	budget   *groupedFrameCacheBudget
	capacity int
	shards   []groupedFrameCacheShard

	hits              atomic.Uint64
	misses            atomic.Uint64
	stores            atomic.Uint64
	evictions         atomic.Uint64
	releases          atomic.Uint64
	retainedBytes     atomic.Int64
	skippedDisabled   atomic.Uint64
	skippedOversize   atomic.Uint64
	skippedBudget     atomic.Uint64
	skippedContention atomic.Uint64
}

type groupedFrameCacheShard struct {
	mu    sync.Mutex
	cap   int
	clock uint64
	slots []groupedFrameCacheSlot
}

type groupedFrameCacheSlot struct {
	ptr atomic.Pointer[groupedFrameCacheEntry]
}

type groupedFrameCacheEntry struct {
	start     int64
	verifyCRC bool
	k         int
	rawLen    uint32
	offsets   [MaxFrameK + 1]uint32
	raw       []byte
	rawPooled bool
	used      uint64
	state     atomic.Uint64 // low bits are refs; high bit means retired
}

func newGroupedFrameCache(owner *File, entries, maxRaw int, maxBytes int64, budget *groupedFrameCacheBudget) *groupedFrameCache {
	if entries < 0 {
		entries = 0
	}
	if maxRaw < 0 {
		maxRaw = 0
	}
	if maxBytes < 0 {
		maxBytes = 0
	}
	c := &groupedFrameCache{owner: owner, maxRaw: maxRaw, maxBytes: maxBytes, budget: budget, capacity: entries}
	if entries <= 0 {
		return c
	}
	shards := groupedFrameShardCount(entries)
	c.shards = make([]groupedFrameCacheShard, shards)
	base := entries / shards
	rem := entries % shards
	for i := range c.shards {
		capForShard := base
		if i < rem {
			capForShard++
		}
		c.shards[i].cap = capForShard
		if capForShard > 0 {
			c.shards[i].slots = make([]groupedFrameCacheSlot, capForShard)
		}
	}
	return c
}

func groupedFrameShardCount(entries int) int {
	if entries <= 1 {
		if entries <= 0 {
			return 0
		}
		return 1
	}
	max := entries
	if max > groupedFrameCacheMaxShards {
		max = groupedFrameCacheMaxShards
	}
	shards := 1
	for shards*2 <= max {
		shards *= 2
	}
	return shards
}

func (c *groupedFrameCache) budgetBytes() uint64 {
	if c == nil {
		return 0
	}
	if c.budget != nil {
		return c.budget.budgetBytes()
	}
	if c.maxBytes <= 0 {
		return 0
	}
	return uint64(c.maxBytes)
}

func (c *groupedFrameCache) retained() uint64 {
	if c == nil {
		return 0
	}
	v := c.retainedBytes.Load()
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func (c *groupedFrameCache) reserve(n int) bool {
	if c == nil || n <= 0 {
		return true
	}
	if c.maxBytes > 0 {
		want := int64(n)
		for {
			cur := c.retainedBytes.Load()
			if cur > c.maxBytes-want {
				return false
			}
			if c.retainedBytes.CompareAndSwap(cur, cur+want) {
				break
			}
		}
	} else {
		c.retainedBytes.Add(int64(n))
	}
	if c.budget != nil && !c.budget.reserve(n) {
		c.retainedBytes.Add(-int64(n))
		return false
	}
	return true
}

func (c *groupedFrameCache) releaseRetained(n int) {
	if c == nil || n <= 0 {
		return
	}
	c.retainedBytes.Add(-int64(n))
	if c.budget != nil {
		c.budget.release(n)
	}
}

func groupedFrameCacheHash(start int64, verifyCRC bool) uint64 {
	x := uint64(start)
	if verifyCRC {
		x ^= 0x9e3779b97f4a7c15
	}
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

func (c *groupedFrameCache) shardFor(start int64, verifyCRC bool) *groupedFrameCacheShard {
	if c == nil || len(c.shards) == 0 {
		return nil
	}
	h := groupedFrameCacheHash(start, verifyCRC)
	return &c.shards[int(h&uint64(len(c.shards)-1))]
}

func validateGroupedFrameCacheState(k int, offsets [MaxFrameK + 1]uint32, rawLen int) bool {
	if k <= 0 || k > MaxFrameK || rawLen <= 0 {
		return false
	}
	prev := uint32(0)
	for i := 0; i < k+1; i++ {
		cur := offsets[i]
		if i == 0 && cur != 0 {
			return false
		}
		if cur < prev {
			return false
		}
		prev = cur
	}
	return uint32(rawLen) == offsets[k]
}

func groupedFrameOffsetsEqual(a, b [MaxFrameK + 1]uint32, k int) bool {
	if k < 0 || k > MaxFrameK {
		return false
	}
	for i := 0; i < k+1; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (e *groupedFrameCacheEntry) tryAcquire() bool {
	if e == nil {
		return false
	}
	for {
		s := e.state.Load()
		if s&groupedFrameEntryRetired != 0 {
			return false
		}
		if s == groupedFrameEntryRetired-1 {
			return false
		}
		if e.state.CompareAndSwap(s, s+1) {
			return true
		}
	}
}

func (e *groupedFrameCacheEntry) release(c *groupedFrameCache) {
	if e == nil {
		return
	}
	s := e.state.Add(^uint64(0))
	if s == groupedFrameEntryRetired {
		c.releaseEntry(e)
	}
}

func (e *groupedFrameCacheEntry) retire(c *groupedFrameCache) {
	if e == nil {
		return
	}
	for {
		s := e.state.Load()
		if s&groupedFrameEntryRetired != 0 {
			return
		}
		if e.state.CompareAndSwap(s, s|groupedFrameEntryRetired) {
			if s == 0 {
				c.releaseEntry(e)
			}
			return
		}
	}
}

func (c *groupedFrameCache) releaseEntry(e *groupedFrameCacheEntry) {
	if c == nil || e == nil {
		return
	}
	c.releaseRetained(len(e.raw))
	if e.rawPooled {
		c.releases.Add(1)
		if c.owner != nil && !c.owner.closed.Load() {
			c.owner.releaseDecodeScratch(e.raw)
		} else {
			putDecodeScratch(e.raw)
		}
	}
}

func (c *groupedFrameCache) readTo(start int64, verifyCRC bool, expectedK int, expectedOffsets [MaxFrameK + 1]uint32, expectedRawLen uint32, subIndex int, dst []byte, f *File) (out []byte, usedDst bool, err error, hit bool) {
	if c == nil || c.capacity <= 0 || len(c.shards) == 0 {
		return nil, false, nil, false
	}
	if !validateGroupedFrameCacheState(expectedK, expectedOffsets, int(expectedRawLen)) || subIndex < 0 || subIndex >= expectedK {
		return nil, false, ErrCorrupt, true
	}
	s := c.shardFor(start, verifyCRC)
	if s == nil {
		return nil, false, nil, false
	}
	for i := range s.slots {
		e := s.slots[i].ptr.Load()
		if e == nil || e.start != start || e.verifyCRC != verifyCRC || e.k != expectedK || e.rawLen != expectedRawLen || !groupedFrameOffsetsEqual(e.offsets, expectedOffsets, expectedK) {
			continue
		}
		if !e.tryAcquire() {
			continue
		}
		valStart := e.offsets[subIndex]
		valEnd := e.offsets[subIndex+1]
		rawLen := e.rawLen
		if valEnd < valStart || valEnd > rawLen || uint32(len(e.raw)) != rawLen || e.offsets[e.k] != rawLen {
			e.release(c)
			c.hits.Add(1)
			return nil, false, ErrCorrupt, true
		}
		val := e.raw[valStart:valEnd]
		if f != nil && f.templateLookup != nil && templ.IsEncodedPayload(val) {
			encoded := append([]byte(nil), val...)
			e.release(c)
			c.hits.Add(1)
			decoded, decErr := templ.DecodePayloadAppend(nil, encoded, func(id uint64) (templ.TemplateDef, error) {
				return resolveTemplateDef(id, f.templateLookup, f.templateDefCache)
			}, f.templateDecodeOpts)
			if decErr != nil {
				return nil, false, decErr, true
			}
			return decoded, false, nil, true
		}
		if dst != nil && cap(dst) >= len(val) {
			out := dst[:len(val)]
			copy(out, val)
			e.release(c)
			c.hits.Add(1)
			return out, true, nil, true
		}
		out := make([]byte, len(val))
		copy(out, val)
		e.release(c)
		c.hits.Add(1)
		return out, false, nil, true
	}
	c.misses.Add(1)
	return nil, false, nil, false
}

func (c *groupedFrameCache) store(start int64, verifyCRC bool, k int, offsets [MaxFrameK + 1]uint32, raw []byte, pooled bool) bool {
	if c == nil || c.capacity <= 0 || len(c.shards) == 0 {
		if c != nil {
			c.skippedDisabled.Add(1)
		}
		return false
	}
	if !validateGroupedFrameCacheState(k, offsets, len(raw)) {
		c.skippedOversize.Add(1)
		return false
	}
	if c.maxRaw > 0 && len(raw) > c.maxRaw {
		c.skippedOversize.Add(1)
		return false
	}
	s := c.shardFor(start, verifyCRC)
	if s == nil || s.cap <= 0 {
		c.skippedDisabled.Add(1)
		return false
	}
	if !s.mu.TryLock() {
		c.skippedContention.Add(1)
		return false
	}
	defer s.mu.Unlock()

	// Replace any existing entry for the same identity in this shard. The new
	// entry includes K/raw length/offsets, so stale state cannot be reused across
	// frame shape changes at the same file-local start.
	for i := range s.slots {
		e := s.slots[i].ptr.Load()
		if e != nil && e.start == start && e.verifyCRC == verifyCRC {
			if old := s.slots[i].ptr.Swap(nil); old != nil {
				old.retire(c)
				c.evictions.Add(1)
			}
		}
	}

	for !c.reserve(len(raw)) {
		idx := s.oldestIndexLocked()
		if idx < 0 {
			c.skippedBudget.Add(1)
			return false
		}
		s.evictSlotLocked(c, idx)
	}

	idx := s.emptyIndexLocked()
	if idx < 0 {
		idx = s.oldestIndexLocked()
		if idx >= 0 {
			s.evictSlotLocked(c, idx)
		}
	}
	if idx < 0 {
		// Should only happen with a zero-capacity shard, guarded above. Release the
		// reservation and let the caller keep ownership of raw.
		c.releaseRetained(len(raw))
		c.skippedBudget.Add(1)
		return false
	}

	s.clock++
	e := &groupedFrameCacheEntry{
		start:     start,
		verifyCRC: verifyCRC,
		k:         k,
		rawLen:    uint32(len(raw)),
		offsets:   offsets,
		raw:       raw,
		rawPooled: pooled,
		used:      s.clock,
	}
	s.slots[idx].ptr.Store(e)
	c.stores.Add(1)
	return true
}

func (s *groupedFrameCacheShard) oldestIndexLocked() int {
	idx := -1
	var oldest uint64
	for i := range s.slots {
		e := s.slots[i].ptr.Load()
		if e == nil {
			continue
		}
		if idx < 0 || e.used < oldest {
			idx = i
			oldest = e.used
		}
	}
	return idx
}

func (s *groupedFrameCacheShard) emptyIndexLocked() int {
	for i := range s.slots {
		if s.slots[i].ptr.Load() == nil {
			return i
		}
	}
	return -1
}

func (s *groupedFrameCacheShard) evictSlotLocked(c *groupedFrameCache, idx int) bool {
	if idx < 0 || idx >= len(s.slots) {
		return false
	}
	old := s.slots[idx].ptr.Swap(nil)
	if old == nil {
		return false
	}
	old.retire(c)
	c.evictions.Add(1)
	return true
}

func (c *groupedFrameCache) clear() {
	if c == nil {
		return
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		entries := make([]*groupedFrameCacheEntry, 0, len(s.slots))
		for j := range s.slots {
			if e := s.slots[j].ptr.Swap(nil); e != nil {
				entries = append(entries, e)
			}
		}
		s.clock = 0
		s.mu.Unlock()
		for _, e := range entries {
			e.retire(c)
		}
	}
}

func (c *groupedFrameCache) stats() GroupedFrameCacheStats {
	if c == nil {
		return GroupedFrameCacheStats{}
	}
	st := GroupedFrameCacheStats{
		Hits:              c.hits.Load(),
		Misses:            c.misses.Load(),
		Stores:            c.stores.Load(),
		Evictions:         c.evictions.Load(),
		Releases:          c.releases.Load(),
		RetainedBytes:     c.retained(),
		BudgetBytes:       c.budgetBytes(),
		SkippedDisabled:   c.skippedDisabled.Load(),
		SkippedOversize:   c.skippedOversize.Load(),
		SkippedBudget:     c.skippedBudget.Load(),
		SkippedContention: c.skippedContention.Load(),
		Capacity:          c.capacity,
	}
	for i := range c.shards {
		s := &c.shards[i]
		for j := range s.slots {
			if s.slots[j].ptr.Load() != nil {
				st.Entries++
			}
		}
	}
	return st
}

func (st *GroupedFrameCacheStats) add(other GroupedFrameCacheStats) {
	st.Hits += other.Hits
	st.Misses += other.Misses
	st.Stores += other.Stores
	st.Evictions += other.Evictions
	st.Releases += other.Releases
	st.RetainedBytes += other.RetainedBytes
	if other.BudgetBytes > st.BudgetBytes {
		st.BudgetBytes = other.BudgetBytes
	}
	st.SkippedDisabled += other.SkippedDisabled
	st.SkippedOversize += other.SkippedOversize
	st.SkippedBudget += other.SkippedBudget
	st.SkippedContention += other.SkippedContention
	st.Entries += other.Entries
	st.Capacity += other.Capacity
}
