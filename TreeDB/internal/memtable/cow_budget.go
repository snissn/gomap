package memtable

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
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
	TotalBytes, HistoryBytes, ReservedBytes, RetiredBytes, PeakBytes, ControlBytes, DeferredBytes uint64
	ExternalBytes                                                                                 uint64
	Views, Generations, Sources, ExternalLeases                                                   int
}

// COWBudget is shared by the fixed shard vector and its frozen generations.
// C2 charges caller-owned cut/vector/backend-lease allocations through an
// external lease before allocation, or ExtraBytes before private preparation.
type COWBudget struct {
	mu     sync.Mutex
	limits COWLimits
	stats  COWStats
	closed bool
	// Exactly one coarse physical-maintenance partition. Its original parent
	// lease is retained through Close and the last child finalizer.
	maintenance      *COWBudget
	reservation      *COWExternalLease
	maintenanceChild bool // immutable before publication; ordinary hot path skips partition cleanup
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
	b.closed = true
	maintenance := b.maintenance
	b.releaseControlLocked()
	b.mu.Unlock()
	if maintenance != nil {
		maintenance.Close()
	}
	b.releaseMaintenanceReservation()
}
func (b *COWBudget) releaseControlLocked() {
	if b.closed && b.stats.Generations == 0 && b.stats.Views == 0 && b.stats.DeferredBytes == 0 && b.stats.ExternalLeases == 0 {
		b.stats.TotalBytes -= b.stats.ControlBytes
		b.stats.ControlBytes = 0
	}
}

// ReserveMaintenance precharges one fixed physical-publication tranche. The
// existing budget machinery bounds its child utilization; ordinary producers
// retain only the parent and cannot consume this lane. No recursive partition
// or second resource registry is permitted.
func (b *COWBudget) ReserveMaintenance(bytes uint64) (*COWBudget, error) {
	b.mu.Lock()
	unavailable := b.closed || b.maintenance != nil || b.reservation != nil
	b.mu.Unlock()
	if unavailable || bytes == 0 {
		return nil, ErrCOWCapacity
	}
	lease, err := b.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	limits := b.limits
	limits.MaxTotalBytes, limits.MaxInFlightBytes, limits.MaxRetiredBytes = bytes, bytes, bytes
	limits.MaxGenerationBytes = min(limits.MaxGenerationBytes, bytes)
	child, err := NewCOWBudget(limits)
	if err != nil {
		lease.Close()
		return nil, err
	}
	child.reservation = lease
	child.maintenanceChild = true
	b.mu.Lock()
	if b.closed || b.maintenance != nil {
		b.mu.Unlock()
		child.Close()
		return nil, ErrCOWClosed
	}
	b.maintenance = child
	b.mu.Unlock()
	return child, nil
}

func (b *COWBudget) releaseMaintenanceReservation() {
	if !b.maintenanceChild {
		return
	}
	b.mu.Lock()
	var lease *COWExternalLease
	if b.closed && b.stats.ControlBytes == 0 {
		lease, b.reservation = b.reservation, nil
	}
	b.mu.Unlock()
	lease.Close()
}

// COWAllocationCharge lets C2 charge concrete wrapper/backing capacities using
// the same conservative Go allocator rounding as the source reservation.
// Apply it once to each raw size/capacity, then sum the returned charges.
func COWAllocationCharge(n uint64) uint64 { return cowAllocation(n) }

// COWExternalLease admits caller-owned storage before that storage is allocated.
// Its entire lifetime counts as in-flight, including persistent cut wrappers.
// It owns no callback or physical resource; Close follows the caller's cleanup.
// Keep the returned pointer; do not copy the lease struct after acquisition.
type COWExternalLease struct {
	budget *COWBudget
	charge uint64
	closed bool // guarded by budget.mu, including concurrent Close calls
}

func (b *COWBudget) AcquireExternal(bytes uint64) (*COWExternalLease, error) {
	charge, ok := cowAdd(bytes, cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{}))))
	if !ok {
		return nil, ErrCOWCapacity
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrCOWClosed
	}
	if b.stats.ExternalLeases == int(^uint(0)>>1) || !cowFits(b.limits.MaxInFlightBytes, b.stats.ReservedBytes, charge) ||
		!b.retirementFitsLocked(charge) || !b.addLocked(charge) {
		return nil, ErrCOWCapacity
	}
	b.stats.ReservedBytes += charge
	b.stats.ExternalBytes += charge
	b.stats.ExternalLeases++
	return &COWExternalLease{budget: b, charge: charge}, nil
}

// AcquireRetention lets the existing storage owner share one governing lease
// across basis/dictionary captures from this exact budget.
func (b *COWBudget) AcquireRetention(bytes uint64) (retainedalloc.Lease, error) {
	return b.AcquireExternal(bytes)
}

// Resize admits producer growth before allocation. Shrink is always permitted,
// including after budget Close, and follows actual storage cleanup.
func (l *COWExternalLease) Resize(bytes uint64) error {
	if l == nil {
		return ErrCOWClosed
	}
	target, ok := cowAdd(bytes, cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{}))))
	if !ok {
		return ErrCOWCapacity
	}
	b := l.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.closed {
		return ErrCOWClosed
	}
	if target > l.charge {
		delta := target - l.charge
		if b.closed {
			return ErrCOWClosed
		}
		if !cowFits(b.limits.MaxInFlightBytes, b.stats.ReservedBytes, delta) ||
			!b.retirementFitsLocked(delta) || !b.addLocked(delta) {
			return ErrCOWCapacity
		}
		b.stats.ReservedBytes += delta
		b.stats.ExternalBytes += delta
	} else {
		delta := l.charge - target
		b.stats.TotalBytes -= delta
		b.stats.ReservedBytes -= delta
		b.stats.ExternalBytes -= delta
	}
	l.charge = target
	return nil
}

// Close is idempotent and may race another Close. The owning caller releases
// all charged buffers/wrappers/resources before closing this allocation lease.
func (l *COWExternalLease) Close() {
	if l == nil {
		return
	}
	b := l.budget
	b.mu.Lock()
	if l.closed {
		b.mu.Unlock()
		return
	}
	l.closed = true
	b.stats.TotalBytes -= l.charge
	b.stats.ReservedBytes -= l.charge
	b.stats.ExternalBytes -= l.charge
	b.stats.ExternalLeases--
	b.releaseControlLocked()
	b.mu.Unlock()
	b.releaseMaintenanceReservation()
}

func cowFits(limit, used, n uint64) bool { return used <= limit && n <= limit-used }

func (b *COWBudget) retirementFitsLocked(n uint64) bool {
	used, ok := cowAdd(b.stats.HistoryBytes, b.stats.ReservedBytes)
	if !ok {
		return false
	}
	used, ok = cowAdd(used, b.stats.DeferredBytes)
	return ok && cowFits(b.limits.MaxRetiredBytes, used, n)
}

func cowAdd(a, c uint64) (uint64, bool) {
	if c > math.MaxUint64-a {
		return 0, false
	}
	return a + c, true
}

// Allocation charge bounds the Go allocator's rounding, including slice backing
// capacity. Include conservative space for pointer-bearing small-object GC
// headers before rounding; callers apply this once to raw allocation capacities.
// This deliberately also charges the allowance for pointer-free payload bytes.
func cowAllocation(n uint64) uint64 { return retainedalloc.AllocationCharge(n) }

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
	activeWriters    int
	history, pending uint64
	// Fixed empty-source construction can use the one reserved tranche. All
	// later mutation history remains charged to budget, never this lease.
	reservedCharge    uint64
	constructionLease *COWExternalLease
	nativeSource      *COWNativeSourceReservation // opt-in original native cohort
	frozen, retired   bool
	drainClaimed      bool
	ids               []COWResourceID
	owners            []cowResourceOwner
}

// All generation fields are guarded by budget.mu. Fixed-capacity identity and
// callback arrays avoid a per-record historical ledger and commit-time growth.
func newCOWGenerationWithConstruction(b *COWBudget, base uint64, maintenance *COWBudget) (*cowGeneration, error) {
	var lease *COWExternalLease
	if maintenance != nil {
		b.mu.Lock()
		valid := b.maintenance == maintenance && !b.closed
		b.mu.Unlock()
		if !valid {
			return nil, ErrCOWCapacity
		}
		var err error
		lease, err = maintenance.AcquireExternal(base)
		if err != nil {
			return nil, err
		}
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		lease.Close()
		return nil, ErrCOWClosed
	}
	if b.stats.Generations >= b.limits.MaxGenerations || base > b.limits.MaxGenerationBytes ||
		(lease == nil && (!b.retirementFitsLocked(base) || !b.addLocked(base))) {
		b.mu.Unlock()
		lease.Close()
		return nil, ErrCOWCapacity
	}
	b.stats.Generations++
	reserved := uint64(0)
	if lease != nil {
		reserved = base
	} else {
		b.stats.HistoryBytes += base
	}
	b.mu.Unlock()
	return &cowGeneration{budget: b, refs: 1, activeWriters: 1, history: base, reservedCharge: reserved, constructionLease: lease,
		ids: make([]COWResourceID, 0, b.limits.MaxResources), owners: make([]cowResourceOwner, 0, b.limits.MaxResources)}, nil
}

func (g *cowGeneration) retain() { g.budget.mu.Lock(); g.refs++; g.budget.mu.Unlock() }

// Returns callbacks to the releasing owner. It must call them outside its
// publication latch and writer admission locks. No IO runs under budget.mu.
func (g *cowGeneration) release() COWRetirement {
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	g.refs--
	if g.refs != 0 {
		return COWRetirement{}
	}
	if g.pending != 0 {
		panic("COW generation released with pending reservation")
	}
	return COWRetirement{generation: g}
}

// Claim under the budget lock, run callbacks outside it, then refund storage.
// A copied retirement descriptor shares this claim and cannot double-drain.
func (g *cowGeneration) drain() {
	b := g.budget
	b.mu.Lock()
	if g.drainClaimed {
		b.mu.Unlock()
		return
	}
	g.drainClaimed = true
	b.mu.Unlock()
	cowReleaseOwners(g.owners)
	b.mu.Lock()
	history, frozen, retired := g.history-g.reservedCharge, g.frozen, g.retired
	constructionLease := g.constructionLease
	g.constructionLease = nil
	nativeSource := g.nativeSource
	g.nativeSource = nil
	// All original callback and array aliases end before their containing
	// generation charge. The original budget serializes the terminal claim.
	g.owners, g.ids, g.history = nil, nil, 0
	g = nil
	b.stats.TotalBytes -= history
	b.stats.HistoryBytes -= history
	b.stats.Generations--
	if frozen {
		b.stats.Sources--
	}
	if retired {
		b.stats.RetiredBytes -= history
	}
	b.releaseControlLocked()
	b.mu.Unlock()
	if nativeSource != nil {
		lease := nativeSource.detachGeneration()
		nativeSource = nil
		lease.Close()
	}
	constructionLease.Close()
	b.releaseMaintenanceReservation()
}

func cowReleaseOwners(owners []cowResourceOwner) {
	for i := range owners {
		callback := owners[i].release
		if callback != nil {
			callback()
		}
		callback = nil
		owners[i] = cowResourceOwner{}
	}
}

// AdmitNativeBytes shares original admission and Close behavior with native
// immutable state. bytes describes one original containing allocation/cohort;
// independent returned lease outlives all of that owner's aliases.
func (b *COWBudget) AdmitNativeBytes(bytes uint64) (*COWExternalLease, error) {
	return b.AcquireExternal(COWAllocationCharge(bytes))
}
