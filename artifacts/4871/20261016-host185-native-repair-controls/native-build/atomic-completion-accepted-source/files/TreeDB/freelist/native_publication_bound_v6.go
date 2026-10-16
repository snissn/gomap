package freelist

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/pager"
)

// NativePublicationBoundV6 is a by-value operation forecast, not a claim or
// activation permit. The original claim revalidates its mutable geometry under
// both locks. Once that claim succeeds, IDs and recorded pages are immutable.
type NativePublicationBoundV6 struct {
	PublicationRecords, PublicationBytes         uint64
	PageRecords, PageBytes                       uint64
	MaterializationRecords, MaterializationBytes uint64
	HighWater, Prefix, ClaimedIDs, MetadataPages uint64
}

// NativePublicationBoundWithWorkV6 reads the SAME recorded prefix before its
// final member is adopted. Empty abandoned coverage is deliberately a limited
// diagnostic shape: otherwise later private rollbacks can enlarge the actual
// burned-tail traversal after activation. Ordinary coverage is unchanged.
func (s *COWPreparationWorkV6) NativePublicationBoundWithWorkV6(prefix []*PreparedCOWCandidateV1, w *iterator.OrdinalScanWork) (g NativePublicationBoundV6, ready bool, err error) {
	if s == nil || w == nil || !w.Reserve(3, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(g))+uint64(unsafe.Sizeof(allocatorCOWStateV1{}))+uint64(unsafe.Sizeof(ReservationLedger{}))) {
		return g, false, nil
	}
	a := s.allocator
	if a == nil {
		return g, false, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || s.private == nil || s.result == nil || !s.sourceLocked(a) || a.cow.ledger != s.ledger || len(prefix) == 0 || len(prefix) != len(a.cow.activated)+1 || prefix[len(prefix)-1] != s.prepared {
		return g, false, ErrCOWPreparationStaleV6
	}
	l := s.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	g.Prefix = uint64(len(prefix))
	// This native bound uses the exact original base formula. Check all
	// products before evaluation; overflow is unsupported geometry, never a
	// wrapped grant for the later canonical operation.
	baseBytes := uint64(unsafe.Sizeof(allocatorCOWStateV1{})) + 256
	perPrefixBytes := uint64(unsafe.Sizeof(CandidateIDV1{})) + 144
	if g.Prefix > (^uint64(0)-3)/9 || g.Prefix > (^uint64(0)-baseBytes)/perPrefixBytes {
		return g, false, ErrGenerationFormat
	}
	g.PublicationRecords, g.PublicationBytes = a.cowPublicationBaseAdmissionV6(g.Prefix, g.Prefix)
	add := func(dst *uint64, value uint64) bool {
		if value > ^uint64(0)-*dst {
			return false
		}
		*dst += value
		return true
	}
	scopeBase := 2 * uint64(unsafe.Sizeof(COWPublicationScopeV6{}))
	scopePerPrefix := uint64(unsafe.Sizeof(PreparedCOWCandidateV1{})) + 32
	if g.Prefix > (^uint64(0)-scopeBase)/scopePerPrefix || !add(&g.PublicationRecords, 1+g.Prefix) || !add(&g.PublicationBytes, scopeBase+g.Prefix*scopePerPrefix) {
		return g, false, ErrGenerationFormat
	}
	for i := range prefix {
		if !w.Reserve(2, uint64(unsafe.Sizeof(PreparedCOWCandidateV1{}))+uint64(unsafe.Sizeof(reservation{}))+uint64(unsafe.Sizeof(FreelistCandidateV1{}))) {
			return g, false, nil
		}
		p := prefix[i]
		if p == nil || p.candidate == nil || p.published {
			return g, false, ErrCandidateConsumed
		}
		r := l.candidates[p.candidateID]
		if r == nil || !r.tailReserved || !r.tailWriteAttempted {
			return g, false, ErrGenerationFormat
		}
		if i+1 == len(prefix) {
			if p != s.prepared || p.activated || r.state != CandidatePreVisible {
				return g, false, ErrCandidateConsumed
			}
		} else if !p.activated || a.cow.activated[i] != p || r.state != CandidateVisible {
			return g, false, ErrCandidateConsumed
		}
		if len(r.abandonedCoverage) != 0 {
			return g, false, ErrGenerationFormat
		}
		n := uint64(len(r.ids))
		// Conservative upper bound: actual original claim only appends DATA IDs
		// not already owned. No private DATA is claimed by this observation.
		if i+1 == len(prefix) && !add(&n, uint64(len(s.private.allocated))) {
			return g, false, ErrGenerationFormat
		}
		if n > ^uint64(0)/32 || !add(&g.ClaimedIDs, n) || !add(&g.PublicationRecords, n) || !add(&g.PublicationBytes, 32*n) {
			return g, false, ErrGenerationFormat
		}
		if !add(&g.PageRecords, 1) || !add(&g.PageBytes, uint64(unsafe.Sizeof(FreelistCandidateV1{}))) {
			return g, false, ErrGenerationFormat
		}
		for j := range p.candidate.pages {
			if !w.Reserve(1, uint64(unsafe.Sizeof(PageImageV1{}))) {
				return g, false, nil
			}
			image := &p.candidate.pages[j]
			if len(image.Data) != 4096 {
				return g, false, ErrGenerationFormat
			}
			rw, bw := pager.NativePageWriteWorkV6()
			if !add(&g.MetadataPages, 1) || !add(&g.PageRecords, 2+rw) || !add(&g.PageBytes, uint64(unsafe.Sizeof(PageImageV1{}))+2*uint64(unsafe.Sizeof(CandidatePageViewV1{}))+uint64(unsafe.Sizeof((*pager.Pager)(nil)))+8+bw) {
				return g, false, ErrGenerationFormat
			}
		}
	}
	gen := s.result.Generation()
	if gen == nil {
		return g, false, ErrGenerationFormat
	}
	g.HighWater = gen.HighWater()
	materializationBase := 2*uint64(unsafe.Sizeof(Allocator{})) + 2*uint64(unsafe.Sizeof(allocatorCOWStateV1{})) + uint64(unsafe.Sizeof(ReservationLedger{})) + uint64(unsafe.Sizeof(FreelistTxn{})) + uint64(unsafe.Sizeof((*pager.Pager)(nil))) + 24
	materializationPerPrefix := uint64(unsafe.Sizeof(PreparedCOWCandidateV1{})) + uint64(unsafe.Sizeof(reservation{})) + 2*uint64(unsafe.Sizeof(CandidateIDV1{})) + 16
	if g.Prefix > (^uint64(0)-7)/3 || g.Prefix > (^uint64(0)-materializationBase)/materializationPerPrefix {
		return g, false, ErrGenerationFormat
	}
	g.MaterializationRecords = 7 + 3*g.Prefix
	g.MaterializationBytes = materializationBase + g.Prefix*materializationPerPrefix
	return g, true, nil
}
