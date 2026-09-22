package lifecycle

import (
	"math"
	"sync"
	"sync/atomic"
)

const (
	fastReaderShardCount = 16
	fastReaderShardMask  = fastReaderShardCount - 1

	// FastReaderShardMask is the exported mask for the fast reader registry shard
	// space. This is exposed to avoid callers duplicating shard-derivation logic.
	FastReaderShardMask = fastReaderShardMask
	// FastReaderShardCount is the number of fast registry shards.
	FastReaderShardCount = fastReaderShardCount
	registerHintUnset    = -1
)

type readerRegistryShard struct {
	seq   atomic.Uint64
	count atomic.Int32
	_     [52]byte
}

type ReaderRegistry struct {
	mu sync.Mutex

	// seqs stores pinned commit seqs by handle index (handle = idx+1).
	// A value of math.MaxUint64 indicates a free slot.
	seqs []uint64
	free []int

	// min caches the lowest pinned seq when dirty=false. When there are no active
	// readers, min is math.MaxUint64.
	min   uint64
	dirty bool

	// fast shards keep common steady-state registrations lock-free and distributed
	// across multiple counters.
	fastShards    [fastReaderShardCount]readerRegistryShard
	nextFastShard atomic.Uint64
}

func NewReaderRegistry() *ReaderRegistry {
	return &ReaderRegistry{
		min: math.MaxUint64,
	}
}

// Register adds a reader pinned to the given sequence.
// Returns a handle to be used for Unregister.
func (r *ReaderRegistry) Register(seq uint64) int64 {
	id, _ := r.RegisterWithHint(seq, registerHintUnset)
	return id
}

// RegisterWithHint adds a reader pinned to the given sequence and returns both
// the handle and the shard index used for the fast path (if available).
// If hint is negative, a fresh shard hint is chosen.
func (r *ReaderRegistry) RegisterWithHint(seq uint64, hint int) (int64, int) {
	if hint < 0 {
		hint = int(r.nextFastShard.Add(1)-1) & fastReaderShardMask
	} else {
		hint &= fastReaderShardMask
	}

	// Joining an already-published shard is safe without the registry mutex: it
	// cannot lower the pinned sequence observed by MinPinnedSeq. Claiming an
	// empty shard can lower that minimum, so serialize first publication with
	// MinPinnedSeq's scan. Otherwise the scan can pair a recycled shard's old,
	// higher seq with the claimant's new positive count and reclaim pages below
	// an in-flight reader's actual generation.
	if id := r.tryJoinFast(seq, hint); id != 0 {
		return id, hint
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < fastReaderShardCount; i++ {
		idx := (hint + i) & fastReaderShardMask
		if id := r.tryJoinFast(seq, idx); id != 0 {
			return id, idx
		}
	}
	for i := 0; i < fastReaderShardCount; i++ {
		idx := (hint + i) & fastReaderShardMask
		if id := r.tryClaimFast(seq, idx); id != 0 {
			return id, idx
		}
	}

	var idx int
	if n := len(r.free); n > 0 {
		idx = r.free[n-1]
		r.free = r.free[:n-1]
	} else {
		idx = len(r.seqs)
		r.seqs = append(r.seqs, math.MaxUint64)
	}
	r.seqs[idx] = seq
	if seq < r.min {
		r.min = seq
	}
	return int64(idx + 1), registerHintUnset
}

// Unregister removes a reader.
func (r *ReaderRegistry) Unregister(id int64) {
	if id < 0 {
		shard := int(-id - 1)
		if shard < 0 || shard >= fastReaderShardCount {
			return
		}
		c := r.fastShards[shard].count.Add(-1)
		if c >= 0 {
			return
		}
		// Safety: avoid underflow if an invalid or duplicate id is observed.
		r.fastShards[shard].count.Store(0)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if id <= 0 {
		return
	}
	idx := int(id - 1)
	if idx < 0 || idx >= len(r.seqs) {
		return
	}
	old := r.seqs[idx]
	if old == math.MaxUint64 {
		return
	}
	r.seqs[idx] = math.MaxUint64
	r.free = append(r.free, idx)
	if old == r.min {
		r.dirty = true
	}
}

// MinPinnedSeq returns the lowest sequence number currently pinned by any reader.
// If no readers are active, returns MaxUint64 (meaning no restriction from readers).
func (r *ReaderRegistry) MinPinnedSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	fastMin := r.loadFastMin()

	if !r.dirty {
		if fastMin < r.min {
			return fastMin
		}
		return r.min
	}

	min := uint64(math.MaxUint64)
	for _, seq := range r.seqs {
		if seq < min {
			min = seq
		}
	}
	r.min = min
	r.dirty = false
	if fastMin < min {
		return fastMin
	}
	return min
}

func (r *ReaderRegistry) loadFastMin() uint64 {
	min := uint64(math.MaxUint64)
	for i := range r.fastShards {
		// Read seq before count to bias races toward a conservative (lower)
		// min rather than a potentially over-optimistic recycled seq value.
		seq := r.fastShards[i].seq.Load()
		if r.fastShards[i].count.Load() <= 0 {
			continue
		}
		if seq < min {
			min = seq
		}
	}
	return min
}

func makeFastHandle(shard int) int64 {
	return int64(-1 - shard)
}

func (r *ReaderRegistry) tryAcquireFast(seq uint64, hint int) int64 {
	if id := r.tryJoinFast(seq, hint); id != 0 {
		return id
	}
	return r.tryClaimFast(seq, hint)
}

func (r *ReaderRegistry) tryJoinFast(seq uint64, shard int) int64 {
	if r.fastShards[shard].seq.Load() != seq {
		return 0
	}

	for {
		c := r.fastShards[shard].count.Load()
		if c <= 0 || c >= math.MaxInt32 {
			return 0
		}
		if !r.fastShards[shard].count.CompareAndSwap(c, c+1) {
			continue
		}
		if r.fastShards[shard].seq.Load() == seq {
			return makeFastHandle(shard)
		}
		r.fastShards[shard].count.Add(-1)
		return 0
	}
}

func (r *ReaderRegistry) tryClaimFast(seq uint64, shard int) int64 {
	// Only the goroutine that wins this CAS is allowed to publish a new seq for
	// the shard. This prevents claim losers from overwriting an active shard seq.
	if !r.fastShards[shard].count.CompareAndSwap(0, -1) {
		return r.tryJoinFast(seq, shard)
	}

	// Publish the candidate sequence before making the counter positive so that
	// concurrent min-seq readers never observe a stale higher watermark.
	r.fastShards[shard].seq.Store(seq)
	r.fastShards[shard].count.Store(1)
	return makeFastHandle(shard)
}
