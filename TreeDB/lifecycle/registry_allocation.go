package lifecycle

import (
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// NewReaderRegistryWithMetadata admits the containing registry, including its
// fixed shards, under the original generation's metadata owner before allocation.
// NewReaderRegistry remains available for unowned diagnostic fixtures.
func NewReaderRegistryWithMetadata(metadata *retainedalloc.Owner) (*ReaderRegistry, error) {
	if metadata == nil {
		return nil, retainedalloc.ErrClosed
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ReaderRegistry{})))
	if err := metadata.AddPending(charge); err != nil {
		return nil, err
	}
	r := NewReaderRegistry()
	r.metadata, r.metadataCharge = metadata, charge
	return r, nil
}

func registryFallbackCharge(capacity int) (uint64, error) {
	if capacity < 0 || uint64(capacity) > math.MaxInt64/uint64(unsafe.Sizeof(registryPin{})) {
		return 0, retainedalloc.ErrCapacity
	}
	seqs := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(registryPin{})))
	free := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(int(0))))
	if seqs > math.MaxUint64-free {
		return 0, retainedalloc.ErrCapacity
	}
	return seqs + free, nil
}

func (r *ReaderRegistry) growFallbackLocked() error {
	oldCapacity := cap(r.seqs)
	if oldCapacity > math.MaxInt/2 {
		return retainedalloc.ErrCapacity
	}
	capacity := oldCapacity * 2
	if capacity == 0 {
		capacity = 1
	}
	charge, err := registryFallbackCharge(capacity)
	if err != nil {
		return err
	}
	oldCharge, _ := registryFallbackCharge(oldCapacity)
	// Both replacement backings coexist with the original backings until copy
	// and publication finish. Refusal changes neither storage nor pin state.
	if r.metadata != nil {
		if err := r.metadata.AddPending(charge); err != nil {
			return err
		}
	}
	seqs := make([]registryPin, len(r.seqs), capacity)
	free := make([]int, len(r.free), capacity)
	copy(seqs, r.seqs)
	copy(free, r.free)
	r.seqs, r.free = seqs, free
	seqs, free = nil, nil
	if r.metadata != nil {
		r.metadataCharge += charge - oldCharge
		if oldCharge != 0 {
			r.metadata.RemovePending(oldCharge)
		}
	}
	return nil
}

// DetachMetadataCharge transfers the ORIGINAL accounting receipt to the
// generation's final completion. The caller must have excluded all registry
// users through that generation's existing lifetime protocol. It consumes the
// returned charge only after ending the final registry/generation aliases.
// Failed physical cleanup must retain the registry and must not call this.
func (r *ReaderRegistry) DetachMetadataCharge() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	charge := r.metadataCharge
	r.seqs, r.free = nil, nil
	r.metadata, r.metadataCharge = nil, 0
	r.mu.Unlock()
	return charge
}
