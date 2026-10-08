package freelist

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/pager"
	"runtime"
	"sync"
	"unsafe"
)

// AllocationCreditV1 is the allocator facet of the caller's existing request
// account. Reservation publication never releases this allocation ownership.
// Admitted adapters allocate no backing, take only their independent account
// lock, and never call into or acquire allocator/ledger locks.
type AllocationCreditV1 interface {
	ReserveAllocation(uint64) error
	RetainAllocationCredit() error
	ReleaseAllocationCredit()
}

var ErrFiniteAllocationExportV1 = errors.New("freelist: finite backing requires an admitted copy or lease")
var ErrAllocationCertificateIncompleteV1 = errors.New("freelist: joint caller and resident allocation certificate incomplete")

// CandidateInfoV1 is an allocation-free copy of immutable scalar authority.
type CandidateInfoV1 struct {
	Ref                       GenerationRefV1
	FreePages, RetiredPages   uint64
	PageCount, AuxiliaryCount int
}

func (prepared *PreparedCOWCandidateV1) InfoV1() (CandidateInfoV1, error) {
	if prepared == nil {
		return CandidateInfoV1{}, ErrGenerationFormat
	}
	prepared.backingMu.RLock()
	defer prepared.backingMu.RUnlock()
	if prepared == nil || prepared.candidate == nil || prepared.candidate.generation == nil {
		return CandidateInfoV1{}, ErrGenerationFormat
	}
	g := prepared.candidate.generation
	return CandidateInfoV1{Ref: g.ref, FreePages: g.FreeCount(), RetiredPages: g.RetiredCount(),
		PageCount: len(prepared.candidate.pages), AuxiliaryCount: len(prepared.auxiliary)}, nil
}

// CopyAuxiliaryPageIDsV1 writes into caller-owned, separately admitted backing.
func (prepared *PreparedCOWCandidateV1) CopyAuxiliaryPageIDsV1(dst []uint64) (int, error) {
	if prepared == nil {
		return 0, ErrGenerationFormat
	}
	prepared.backingMu.RLock()
	defer prepared.backingMu.RUnlock()
	if prepared == nil {
		return 0, ErrGenerationFormat
	}
	if len(dst) < len(prepared.auxiliary) {
		return len(prepared.auxiliary), ErrFiniteAllocationExportV1
	}
	return copy(dst, prepared.auxiliary), nil
}

func (prepared *PreparedCOWCandidateV1) AuxiliaryPageIDV1(index int) (uint64, error) {
	if prepared == nil {
		return 0, ErrGenerationFormat
	}
	prepared.backingMu.RLock()
	defer prepared.backingMu.RUnlock()
	if prepared == nil || index < 0 || index >= len(prepared.auxiliary) {
		return 0, ErrGenerationFormat
	}
	return prepared.auxiliary[index], nil
}

func (prepared *PreparedCOWCandidateV1) WritePagesToV1(writer CandidatePageWriterV1) error {
	if prepared == nil {
		return ErrGenerationFormat
	}
	candidate, err := func() (*FreelistCandidateV1, error) {
		if prepared.allocator != nil {
			prepared.allocator.mu.Lock()
			defer prepared.allocator.mu.Unlock()
		}
		prepared.backingMu.RLock()
		defer prepared.backingMu.RUnlock()
		candidate := prepared.candidate
		if candidate == nil {
			return nil, ErrGenerationFormat
		}
		if candidate.hasFiniteBackingV1() {
			return nil, ErrFiniteAllocationExportV1
		}
		candidate.markOrdinaryEscapeV1()
		if prepared.allocator != nil && prepared.allocator.cow != nil {
			prepared.allocator.cow.rawGenerationEscaped = true
		}
		return candidate, nil
	}()
	if err != nil {
		return err
	}
	// Ordinary retaining callbacks run outside allocator/terminal locks.
	return candidate.WritePagesToV1(writer)
}

func (prepared *PreparedCOWCandidateV1) WritePagesToPagerV1(dst *pager.Pager) error {
	if prepared == nil {
		return ErrGenerationFormat
	}
	prepared.backingMu.RLock()
	defer prepared.backingMu.RUnlock()
	if prepared == nil || prepared.candidate == nil {
		return ErrGenerationFormat
	}
	return prepared.candidate.WritePagesToPagerV1(dst)
}

func (a *Allocator) COWGenerationInfoV1() (CandidateInfoV1, error) {
	if a == nil {
		return CandidateInfoV1{}, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return CandidateInfoV1{}, ErrCandidateConsumed
	}
	if a.cow == nil || a.cow.generation == nil {
		return CandidateInfoV1{}, ErrGenerationFormat
	}
	g := a.cow.generation
	return CandidateInfoV1{Ref: g.ref, FreePages: g.FreeCount(), RetiredPages: g.RetiredCount()}, nil
}

func (info CandidateInfoV1) GenerationID() uint64           { return info.Ref.GenerationID }
func (info CandidateInfoV1) CommitSeq() uint64              { return info.Ref.CommitSeq }
func (info CandidateInfoV1) HighWater() uint64              { return info.Ref.HighWater }
func (info CandidateInfoV1) GenerationRef() GenerationRefV1 { return info.Ref }
func (info CandidateInfoV1) FreeCount() uint64              { return info.FreePages }
func (info CandidateInfoV1) RetiredCount() uint64           { return info.RetiredPages }

// allocationCreditLeaseV1 aggregates intrinsic whole-radix-chunk references to one
// existing request facet. Node erasure releases ownership, never byte debit.
// All node edits occur under the containing transaction or ledger lock.
type allocationCreditLeaseV1 struct {
	mu    sync.Mutex
	facet AllocationCreditV1
	refs  uint64
}

func newAllocationCreditLeaseV1(facet AllocationCreditV1) (*allocationCreditLeaseV1, error) {
	if facet == nil {
		return nil, nil
	}
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" || runtime.Version() != "go1.26.3" {
		return nil, ErrAllocationCertificateIncompleteV1
	}
	if err := facet.ReserveAllocation(allocationClassV1(uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), true)); err != nil {
		return nil, err
	}
	if err := facet.RetainAllocationCredit(); err != nil {
		return nil, err
	}
	return &allocationCreditLeaseV1{facet: facet, refs: 1}, nil
}
func (lease *allocationCreditLeaseV1) reserve(bytes uint64, refs uint64) error {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.refs == 0 || lease.refs > ^uint64(0)-refs {
		return ErrCandidateConsumed
	}
	if err := lease.facet.ReserveAllocation(bytes); err != nil {
		return err
	}
	lease.refs += refs
	return nil
}

// retainReferencesV1 transfers actual alias authority without a new byte debit.
// The operation which creates a backing object admits its allocation first.
func (lease *allocationCreditLeaseV1) retainReferencesV1(refs uint64) error {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.refs == 0 || lease.refs > ^uint64(0)-refs {
		return ErrCandidateConsumed
	}
	lease.refs += refs
	return nil
}
func (lease *allocationCreditLeaseV1) release() {
	if lease == nil {
		return
	}
	lease.mu.Lock()
	if lease.refs == 0 {
		lease.mu.Unlock()
		panic("freelist: allocation credit lease released twice")
	}
	lease.refs--
	var facet AllocationCreditV1
	if lease.refs == 0 {
		facet, lease.facet = lease.facet, nil
	}
	lease.mu.Unlock()
	if facet != nil {
		facet.ReleaseAllocationCredit()
	}
}

// ClearTerminalBackingV1 is called only after exact consume/abort callbacks
// and ordered page work. Ordinary exported candidates retain their old API.
// Ambiguous, visible, retryable, and recovery-owned candidates refuse cleanup.
func (prepared *PreparedCOWCandidateV1) ClearTerminalBackingV1() error {
	if prepared == nil {
		return nil
	}
	if prepared.allocator != nil {
		prepared.allocator.mu.Lock()
		defer prepared.allocator.mu.Unlock()
	}
	prepared.backingMu.Lock()
	defer prepared.backingMu.Unlock()
	if prepared.candidate == nil {
		return nil
	}
	if !prepared.published && (prepared.activated || prepared.rollbackTxn != nil) {
		return ErrCandidateConsumed
	}
	candidate := prepared.candidate
	finite := candidate.hasFiniteBackingV1()
	if !finite && !prepared.ownedBacking {
		return nil
	}
	// Removing publication debt does not release this physical backing edge.
	if prepared.allocator != nil && prepared.allocator.cow != nil && prepared.ownedBacking {
		state := prepared.allocator.cow
		link := &state.ownedCandidates
		for *link != nil {
			if *link == prepared {
				*link = prepared.ownedNext
				break
			}
			link = &(*link).ownedNext
		}
		prepared.ownedNext, prepared.ownedBacking = nil, false
		// Ordinary escaped candidate/view contracts remain indefinite. Such a
		// lineage is permanently excluded from later finite adoption.
		if !finite && state.rawGenerationEscaped {
			return nil
		}
	}
	candidate.writeMu.Lock()
	defer candidate.writeMu.Unlock()
	generation, creator, preparedCreator := candidate.generation, candidate.creator, prepared.creator
	clear(candidate.pages[:cap(candidate.pages)])
	clear(candidate.dirtyIDs[:cap(candidate.dirtyIDs)])
	clear(prepared.auxiliary[:cap(prepared.auxiliary)])
	candidate.pages, candidate.dirtyIDs, candidate.generation = nil, nil, nil
	candidate.allocationCredit, candidate.creator = nil, nil
	prepared.candidate, prepared.auxiliary, prepared.creator = nil, nil, nil
	prepared.allocator = nil
	releaseGenerationV1(generation)
	creator.release()
	preparedCreator.release()
	return nil
}
