package memtable

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"unsafe"
)

var (
	ErrCOWCapacity = errors.New("immutable memtable capacity exhausted")
	ErrCOWClosed   = errors.New("immutable memtable owner closed")
	ErrCOWPending  = errors.New("immutable memtable preparation already pending")
)

// COWLimits bounds engine-owned allocation charges, not RSS or GC timing.
// All fields must be positive; zero is deliberately not an unlimited setting.
type COWLimits struct {
	MaxViews, MaxGenerations, MaxSources, MaxResources                   int
	MaxGenerationBytes, MaxTotalBytes, MaxRetiredBytes, MaxInFlightBytes uint64
}

func DefaultCOWLimits() COWLimits {
	return COWLimits{MaxViews: 1024, MaxGenerations: 64, MaxSources: 32,
		MaxResources: 256, MaxGenerationBytes: 64 << 20, MaxTotalBytes: 512 << 20,
		MaxRetiredBytes: 512 << 20, MaxInFlightBytes: 64 << 20}
}

func (l COWLimits) Validate() error {
	if l.MaxViews <= 0 || l.MaxGenerations <= 0 || l.MaxSources <= 0 || l.MaxResources <= 0 ||
		l.MaxGenerationBytes == 0 || l.MaxTotalBytes == 0 || l.MaxRetiredBytes == 0 || l.MaxInFlightBytes == 0 {
		return fmt.Errorf("COW limits must all be finite and positive")
	}
	if l.MaxGenerations > 1<<20 || l.MaxSources > l.MaxGenerations || l.MaxViews > 1<<24 || l.MaxResources > 1<<20 ||
		l.MaxGenerationBytes > math.MaxInt64 || l.MaxTotalBytes > math.MaxInt64 || l.MaxRetiredBytes > math.MaxInt64 || l.MaxInFlightBytes > math.MaxInt64 {
		return fmt.Errorf("COW limits exceed supported bounds")
	}
	if l.MaxGenerationBytes > l.MaxTotalBytes || l.MaxGenerationBytes > l.MaxRetiredBytes {
		return fmt.Errorf("COW generation limit exceeds total or retirement capacity")
	}
	return nil
}

type COWStats struct {
	TotalBytes, HistoryBytes, ReservedBytes, RetiredBytes, PeakBytes, ControlBytes uint64
	Views, Generations, Sources                                                    int
}

// COWBudget is shared by the fixed shard vector and its frozen generations.
// C2 must include its own cut/vector/backend-lease allocations in ExtraBytes.
type COWBudget struct {
	mu     sync.Mutex
	limits COWLimits
	stats  COWStats
	closed bool
}

func NewCOWBudget(limits COWLimits) (*COWBudget, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	control := cowAllocation(uint64(unsafe.Sizeof(COWBudget{})))
	if control > limits.MaxTotalBytes {
		return nil, ErrCOWCapacity
	}
	return &COWBudget{limits: limits, stats: COWStats{TotalBytes: control, PeakBytes: control, ControlBytes: control}}, nil
}

func (b *COWBudget) Stats() COWStats   { b.mu.Lock(); defer b.mu.Unlock(); return b.stats }
func (b *COWBudget) Limits() COWLimits { return b.limits }

// Close refuses new retention-increasing admissions. Existing view/source
// leases remain valid; the budget wrapper is released after their last owner.
func (b *COWBudget) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.releaseControlLocked()
}
func (b *COWBudget) releaseControlLocked() {
	if b.closed && b.stats.Generations == 0 && b.stats.Views == 0 {
		b.stats.TotalBytes -= b.stats.ControlBytes
		b.stats.ControlBytes = 0
	}
}

// COWAllocationCharge lets C2 charge concrete wrapper/backing capacities using
// the same conservative Go allocator rounding as the source reservation.
func COWAllocationCharge(n uint64) uint64 { return cowAllocation(n) }

func cowAdd(a, c uint64) (uint64, bool) {
	if c > math.MaxUint64-a {
		return 0, false
	}
	return a + c, true
}

// Allocation charge bounds the Go allocator's rounding, including slice backing
// capacity. Powers of two cover small size classes; large objects round to pages.
// It is idempotent, so copying a rounded dependency capacity cannot escape it.
func cowAllocation(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	if n > math.MaxInt64-8192 {
		return math.MaxUint64
	}
	if n <= 32768 {
		c := uint64(16)
		for c < n {
			c *= 2
		}
		return c
	}
	return (n + 8191) &^ uint64(8191)
}

func (b *COWBudget) addLocked(n uint64) bool {
	total, ok := cowAdd(b.stats.TotalBytes, n)
	if !ok || total > b.limits.MaxTotalBytes {
		return false
	}
	b.stats.TotalBytes = total
	if total > b.stats.PeakBytes {
		b.stats.PeakBytes = total
	}
	return true
}

// COWResourceID identifies an existing file, dictionary or template authority.
// Kind separates the caller's concrete namespaces; C1 performs no physical GC.
type COWResourceID struct {
	Kind uint8
	ID   uint64
}

type cowResourceOwner struct{ release func() }

type cowGeneration struct {
	budget           *COWBudget
	refs             int
	history, pending uint64
	frozen, retired  bool
	ids              []COWResourceID
	owners           []cowResourceOwner
}

// All generation fields are guarded by budget.mu. Fixed-capacity identity and
// callback arrays avoid a per-record historical ledger and commit-time growth.
func newCOWGeneration(b *COWBudget, base uint64) (*cowGeneration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrCOWClosed
	}
	if b.stats.Generations >= b.limits.MaxGenerations || base > b.limits.MaxGenerationBytes ||
		base > b.limits.MaxRetiredBytes-b.stats.HistoryBytes-b.stats.ReservedBytes || !b.addLocked(base) {
		return nil, ErrCOWCapacity
	}
	b.stats.Generations++
	b.stats.HistoryBytes += base
	return &cowGeneration{budget: b, refs: 1, history: base,
		ids: make([]COWResourceID, 0, b.limits.MaxResources), owners: make([]cowResourceOwner, 0, b.limits.MaxResources)}, nil
}

func (g *cowGeneration) retain() { g.budget.mu.Lock(); g.refs++; g.budget.mu.Unlock() }

// Returns callbacks to the releasing owner. It must call them outside its
// publication latch and writer admission locks. No IO runs under budget.mu.
func (g *cowGeneration) release() []cowResourceOwner {
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	g.refs--
	if g.refs != 0 {
		return nil
	}
	if g.pending != 0 {
		panic("COW generation released with pending reservation")
	}
	b.stats.TotalBytes -= g.history
	b.stats.HistoryBytes -= g.history
	b.stats.Generations--
	if g.frozen {
		b.stats.Sources--
	}
	if g.retired {
		b.stats.RetiredBytes -= g.history
	}
	b.releaseControlLocked()
	owners := g.owners
	g.owners = nil
	g.ids = nil
	return owners
}

func cowReleaseOwners(owners []cowResourceOwner) {
	for _, o := range owners {
		if o.release != nil {
			o.release()
		}
	}
}
