package freelist

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/pager"
)

// WritePagesToPagerWithWorkV6 visits the original canonical candidate pages,
// rather than copying Pages or retaining a second page/image vector. It is a
// nonyielding publication preparation primitive: the caller checks remaining
// capacity for the whole turn before its first effect. Refusal or a write error
// does not discard original candidate/ledger custody; no nil-work path exists.
// Destination verified-word contention may require additional admitted retries.
func (c *FreelistCandidateV1) WritePagesToPagerWithWorkV6(dst *pager.Pager, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || dst == nil || w == nil {
		return false, ErrGenerationFormat
	}
	if ok, err := appendReserve(w, 1, uint64(unsafe.Sizeof(*c))); !ok || err != nil {
		return false, err
	}
	for i := range c.pages {
		if ok, err := appendReserve(w, 1, uint64(unsafe.Sizeof(PageImageV1{}))+uint64(unsafe.Sizeof(CandidatePageViewV1{}))); !ok || err != nil {
			return false, err
		}
		image := &c.pages[i]
		if ready, err := WriteCandidatePageToPagerWithWorkV6(dst, image.PageID, CandidatePageViewV1{data: image.Data}, w); !ready || err != nil {
			return false, err
		}
	}
	return true, nil
}

// ValidateCOWPhysicalTailWithWorkV6 observes the SAME allocator/live txn and
// original pager under the ordinary allocator exclusion. It never extends
// authority or consults a copied/native ledger.
func (a *Allocator) ValidateCOWPhysicalTailWithWorkV6(requiredHighWater uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if a == nil || w == nil {
		return false, ErrGenerationFormat
	}
	if ok, err := appendReserve(w, 4, uint64(unsafe.Sizeof(*a))+uint64(unsafe.Sizeof(allocatorCOWStateV1{}))+uint64(unsafe.Sizeof(FreelistTxn{}))+uint64(unsafe.Sizeof((*pager.Pager)(nil)))+24); !ok || err != nil {
		return false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pager == nil {
		return false, ErrGenerationFormat
	}
	err := a.validateCOWPhysicalTailLockedV1(requiredHighWater)
	return err == nil, err
}

// ValidateCOWMaterializationWithWorkV6 proves that the SAME activated prefix
// still owns its recorded metadata placement and that the original ledger
// marked tail-write-attempted BEFORE any physical count/write effect. This
// merely validates the original irreversible marker; it cannot mint one for a
// foreign/newer candidate or borrow a newer generation's reservation.
func (a *Allocator) ValidateCOWMaterializationWithWorkV6(prefix []*PreparedCOWCandidateV1, w *iterator.OrdinalScanWork) (bool, error) {
	if a == nil || w == nil {
		return false, ErrGenerationFormat
	}
	if ok, err := appendReserve(w, 3, uint64(unsafe.Sizeof(*a))+uint64(unsafe.Sizeof(allocatorCOWStateV1{}))+uint64(unsafe.Sizeof(ReservationLedger{}))); !ok || err != nil {
		return false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.ledger == nil || len(prefix) == 0 || len(prefix) > len(a.cow.activated) {
		return false, ErrCandidateConsumed
	}
	l := a.cow.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range prefix {
		if ok, err := appendReserve(w, 3, uint64(unsafe.Sizeof(PreparedCOWCandidateV1{}))+uint64(unsafe.Sizeof(reservation{}))+2*uint64(unsafe.Sizeof(CandidateIDV1{}))+16); !ok || err != nil {
			return false, err
		}
		prepared := prefix[i]
		if prepared == nil || a.cow.activated[i] != prepared || !prepared.activated || prepared.published || prepared.candidate == nil {
			return false, ErrCandidateConsumed
		}
		r := l.candidates[prepared.candidateID]
		if r == nil || r.state != CandidateVisible || !r.tailReserved || !r.tailWriteAttempted {
			return false, ErrGenerationFormat
		}
	}
	return true, nil
}
