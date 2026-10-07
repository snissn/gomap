package freelist

import "sync"

// OwnedGenerationHandleV1 exposes scalar identity and one synchronized transfer,
// never the underlying generation/tree/record. Close and transfer scrub its
// sole edge; ordinary raw public constructors keep their escape contract.
type OwnedGenerationHandleV1 struct {
	mu         sync.Mutex
	generation *FreelistGenerationV1
	info       CandidateInfoV1
}

func LoadOwnedGenerationHandleV1(source PageSource, ref GenerationRefV1) (*OwnedGenerationHandleV1, error) {
	g, err := loadGenerationOwnedV1(source, ref)
	if err != nil {
		return nil, err
	}
	return &OwnedGenerationHandleV1{generation: g, info: CandidateInfoV1{Ref: g.ref, FreePages: g.FreeCount(), RetiredPages: g.RetiredCount()}}, nil
}
func (handle *OwnedGenerationHandleV1) InfoV1() (CandidateInfoV1, error) {
	if handle == nil {
		return CandidateInfoV1{}, ErrGenerationFormat
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.generation == nil {
		return CandidateInfoV1{}, ErrCandidateConsumed
	}
	return handle.info, nil
}
func (handle *OwnedGenerationHandleV1) Close() {
	if handle == nil {
		return
	}
	handle.mu.Lock()
	g := handle.generation
	handle.generation = nil
	handle.info = CandidateInfoV1{}
	releaseGenerationV1(g)
	handle.mu.Unlock()
}
func (a *Allocator) EnableOwnedGenerationHandleV1(handle *OwnedGenerationHandleV1, ledger *ReservationLedger) error {
	if handle == nil {
		return ErrGenerationFormat
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	if handle.generation == nil {
		return ErrCandidateConsumed
	}
	if err := a.enableCOWOwnedV1(handle.generation, ledger); err != nil {
		return err
	}
	g := handle.generation
	handle.generation = nil
	handle.info = CandidateInfoV1{}
	releaseGenerationV1(g)
	return nil
}
func (a *Allocator) EnableNewCOWGenerationV1(generationID, highWater uint64, ledger *ReservationLedger) error {
	if a == nil {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return ErrCandidateConsumed
	}
	g, err := newFreelistGenerationOwnedV1(generationID, highWater, nil, nil)
	if err != nil {
		return err
	}
	defer releaseGenerationV1(g)
	return a.enableCOWOwnedV1(g, ledger)
}
