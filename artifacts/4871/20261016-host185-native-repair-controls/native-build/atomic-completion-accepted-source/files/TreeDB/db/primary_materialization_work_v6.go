package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// materializePrimarySealWithWorkV6 uses the original private seal, recorded
// allocator metadata pages and original pager. No view/callback/vector survives
// this call. The already prepared original promotion avoids reaching the
// ordinary synchronous record decoder. This is nonyielding preparation; the
// caller must check the complete remaining turn allowance before activation.
func (runtime *rootPublicationRuntimeV1) materializePrimarySealWithWorkV6(seal *rootPublicationSealV1, w *iterator.OrdinalScanWork) (bool, error) {
	if seal == nil || w == nil {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	bytes := 2*uint64(unsafe.Sizeof(*seal)) + uint64(unsafe.Sizeof(primaryPublicationSealV5{})) + uint64(unsafe.Sizeof(rootpublication.PreparedPrimaryRootV5{})) + uint64(unsafe.Sizeof(freelist.FreelistGenerationV1{})) + uint64(unsafe.Sizeof(rootpublication.PrimaryPromotionV6{})) + uint64(unsafe.Sizeof(indexGen{}))
	if !w.Reserve(6, bytes) {
		return false, nil
	}
	if seal.primary == nil || seal.primary.prepared == nil || seal.primary.promotion == nil || seal.idx == nil {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	if seal.materialized {
		return true, nil
	}
	generation := seal.primary.prepared.Generation()
	if generation == nil || seal.primary.promotion.View.Slot() != seal.target || seal.primary.promotion.View.Current().Record != seal.record {
		return false, rootpublication.ErrDurableRootOwnership
	}
	// Original recording made the tail marker irreversible before recording
	// any immutable image. Validate it and the SAME live activated prefix now,
	// before extending physical visibility or writing destination bytes.
	if ready, err := seal.idx.allocator.ValidateCOWMaterializationWithWorkV6(seal.prefix, w); !ready || err != nil {
		return false, err
	}
	p := seal.idx.pager
	if ready, err := p.GrowWithinCapacityWithWorkV6(generation.HighWater(), w); !ready || err != nil {
		return false, err
	}
	if ready, err := seal.idx.allocator.ValidateCOWPhysicalTailWithWorkV6(generation.HighWater(), w); !ready || err != nil {
		return false, err
	}
	for i := range seal.prefix {
		if !w.Reserve(1, uint64(unsafe.Sizeof(freelist.PreparedCOWCandidateV1{}))+uint64(unsafe.Sizeof((*freelist.PreparedCOWCandidateV1)(nil)))) {
			return false, nil
		}
		cow := seal.prefix[i]
		if cow == nil || cow.Candidate() == nil {
			return false, rootpublication.ErrDurableRootOwnership
		}
		if ready, err := cow.Candidate().WriteNativePagesToPagerWithWorkV6(p, w); !ready || err != nil {
			return false, err
		}
	}
	// This revalidation and materialized store use the already admitted seal
	// and promotion operands; no mutable slot is read or installed here.
	if seal.primary.promotion.View.Slot() != seal.target || seal.primary.promotion.View.Current().Record != seal.record {
		return false, rootpublication.ErrDurableRootOwnership
	}
	seal.materialized = true
	return true, nil
}
