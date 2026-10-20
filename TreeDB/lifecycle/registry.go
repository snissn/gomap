package lifecycle

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
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

type pinPurpose uint32

const (
	readerPin pinPurpose = iota
	pageRetentionPin
)

type registryPin struct {
	seq     uint64
	purpose pinPurpose
}

type readerRegistryShard struct {
	seq     atomic.Uint64
	count   atomic.Int32
	purpose atomic.Uint32
	_       [48]byte
}

type ReaderRegistry struct {
	mu sync.Mutex

	metadata       *retainedalloc.Owner
	metadataCharge uint64

	// seqs stores pins by handle index (handle = idx+1).
	// A sequence of math.MaxUint64 indicates a free slot.
	seqs []registryPin
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
// Returns a handle to be used for Unregister. Growth refusal returns zero and
// the original admission error without publishing a pin.
func (r *ReaderRegistry) Register(seq uint64) (int64, error) {
	id, _, err := r.RegisterWithHint(seq, registerHintUnset)
	return id, err
}

// RegisterWithHint adds a reader pinned to the given sequence and returns both
// the handle and the shard index used for the fast path (if available).
// If hint is negative, a fresh shard hint is chosen. Owned fallback growth is
// admitted before allocation; on refusal the caller must roll back its capture.
func (r *ReaderRegistry) RegisterWithHint(seq uint64, hint int) (int64, int, error) {
	return r.registerWithHint(seq, hint, readerPin, false)
}

// RegisterPageRetention pins pages without claiming that a reader can access
// the logical assets described by them. It uses the same registry and release
// handle as reader pins; MinPinnedSeq includes both purposes.
func (r *ReaderRegistry) RegisterPageRetention(seq uint64) (int64, error) {
	id, _, err := r.registerWithHint(seq, registerHintUnset, pageRetentionPin, false)
	return id, err
}

// RegisterFastWithHint uses only the registry's existing fixed shard storage.
// It returns zero when all shards hold other sequence/purpose cohorts. Bounded
// callers can refuse admission without growing the fallback seqs/free arrays.
func (r *ReaderRegistry) RegisterFastWithHint(seq uint64, hint int) (int64, int) {
	id, shard, _ := r.registerWithHint(seq, hint, readerPin, true)
	return id, shard
}

func (r *ReaderRegistry) registerWithHint(seq uint64, hint int, purpose pinPurpose, fastOnly bool) (int64, int, error) {
	if hint < 0 {
		hint = int(r.nextFastShard.Add(1)-1) & fastReaderShardMask
	} else {
		hint &= fastReaderShardMask
	}

	// Joining an already-published sequence/purpose cohort cannot lower either
	// minimum. First publication can, so serialize it with both minimum scans.
	if id := r.tryJoinFastForPurpose(seq, hint, purpose); id != 0 {
		return id, hint, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < fastReaderShardCount; i++ {
		idx := (hint + i) & fastReaderShardMask
		if id := r.tryJoinFastForPurpose(seq, idx, purpose); id != 0 {
			return id, idx, nil
		}
	}
	for i := 0; i < fastReaderShardCount; i++ {
		idx := (hint + i) & fastReaderShardMask
		if id := r.tryClaimFastForPurpose(seq, idx, purpose); id != 0 {
			return id, idx, nil
		}
	}
	if fastOnly {
		return 0, registerHintUnset, nil
	}

	var idx int
	if n := len(r.free); n > 0 {
		idx = r.free[n-1]
		r.free = r.free[:n-1]
	} else {
		idx = len(r.seqs)
		if idx == cap(r.seqs) {
			if err := r.growFallbackLocked(); err != nil {
				return 0, registerHintUnset, err
			}
		}
		r.seqs = append(r.seqs, registryPin{seq: math.MaxUint64})
	}
	r.seqs[idx] = registryPin{seq: seq, purpose: purpose}
	if seq < r.min {
		r.min = seq
	}
	return int64(idx + 1), registerHintUnset, nil
}

// Unregister removes a reader without allocating. Slow free-slot capacity was
// admitted and allocated with the corresponding sequence-slot capacity.
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
	old := r.seqs[idx].seq
	if old == math.MaxUint64 {
		return
	}
	r.seqs[idx] = registryPin{seq: math.MaxUint64}
	r.free = append(r.free, idx)
	if old == r.min {
		r.dirty = true
	}
}

// MinPinnedSeq returns the lowest sequence number pinned for either purpose.
// If no pins are active, it returns MaxUint64. Reuse and pruning use this minimum.
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
	for _, pin := range r.seqs {
		if pin.seq < min {
			min = pin.seq
		}
	}
	r.min = min
	r.dirty = false
	if fastMin < min {
		return fastMin
	}
	return min
}

// MinReaderPinnedSeq excludes only explicitly page-retention-only pins. Existing
// reader-capable registrations, including recoverable-root captures, still count.
func (r *ReaderRegistry) MinReaderPinnedSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	min := r.loadFastMinForPurpose(true)
	for _, pin := range r.seqs {
		if pin.purpose == readerPin && pin.seq < min {
			min = pin.seq
		}
	}
	return min
}

func (r *ReaderRegistry) loadFastMin() uint64 {
	return r.loadFastMinForPurpose(false)
}

func (r *ReaderRegistry) loadFastMinForPurpose(readersOnly bool) uint64 {
	min := uint64(math.MaxUint64)
	for i := range r.fastShards {
		// Read seq before count to bias races toward a conservative (lower)
		// min rather than a potentially over-optimistic recycled seq value.
		seq := r.fastShards[i].seq.Load()
		if r.fastShards[i].count.Load() <= 0 {
			continue
		}
		if readersOnly && pinPurpose(r.fastShards[i].purpose.Load()) != readerPin {
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
	return r.tryJoinFastForPurpose(seq, shard, readerPin)
}

func (r *ReaderRegistry) tryJoinFastForPurpose(seq uint64, shard int, purpose pinPurpose) int64 {
	if r.fastShards[shard].seq.Load() != seq || pinPurpose(r.fastShards[shard].purpose.Load()) != purpose {
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
		if r.fastShards[shard].seq.Load() == seq && pinPurpose(r.fastShards[shard].purpose.Load()) == purpose {
			return makeFastHandle(shard)
		}
		r.fastShards[shard].count.Add(-1)
		return 0
	}
}

func (r *ReaderRegistry) tryClaimFast(seq uint64, shard int) int64 {
	return r.tryClaimFastForPurpose(seq, shard, readerPin)
}

func (r *ReaderRegistry) tryClaimFastForPurpose(seq uint64, shard int, purpose pinPurpose) int64 {
	// Only the goroutine that wins this CAS is allowed to publish a new seq for
	// the shard. This prevents claim losers from overwriting an active shard seq.
	if !r.fastShards[shard].count.CompareAndSwap(0, -1) {
		return r.tryJoinFastForPurpose(seq, shard, purpose)
	}

	// Publish both cohort fields before making the counter positive. First
	// claims hold mu, so neither minimum scan can mix fields from recycled pins.
	r.fastShards[shard].seq.Store(seq)
	r.fastShards[shard].purpose.Store(uint32(purpose))
	r.fastShards[shard].count.Store(1)
	return makeFastHandle(shard)
}
