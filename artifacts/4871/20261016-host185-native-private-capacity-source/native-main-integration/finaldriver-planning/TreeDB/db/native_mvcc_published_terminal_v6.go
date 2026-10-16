package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// closePublishedNativeStepV6 is the adopted producer terminal. It never invokes
// COW Cancel, constructor Abort, or a private seal disposer. The SAME external
// receipt first drives genuine member/resource Finish. Embedded callback frames
// remain alive until it returns, then finite-only producer scratch is detached.
func (c *NativeMVCCCurrentDeleteV6) closePublishedNativeStepV6(w *iterator.OrdinalScanWork) (bool, error) {
	for {
		switch c.phase {
		case 7:
			if ready, err := c.candidatePreparation.backingRefund.complete(w); !ready || err != nil {
				return false, err
			}
			if ready, err := c.sealPreparation.backingRefund.complete(w); !ready || err != nil {
				return false, err
			}
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(c.candidatePreparation))+2*uint64(unsafe.Sizeof(c.rootPreparation))+2*uint64(unsafe.Sizeof(c.sealPreparation.seal))) {
				return false, nil
			}
			ready, err := c.release.BindFinishV6(c.transaction, w)
			if !ready || err != nil {
				return false, err
			}
			// These aliases are borrowed from the original member/retired-seal
			// closure. End them BEFORE that closure may refund its own storage.
			c.transaction = nil
			c.candidatePreparation = nativeMVCCCandidatePreparationV6{}
			// The acquired source witness is not member-owned. Keep its original
			// frame/edge until the external receipt has genuinely finished.
			c.sealPreparation.seal = nil
			c.phase = 8
		case 8:
			ready, err := c.release.ContinueFinishWorkV6(w, nil)
			if !ready || err != nil {
				return false, err
			}
			c.phase = 9
		case 9:
			if ready, err := c.rootPreparation.closeStep(w); !ready || err != nil {
				return false, err
			}
			// closeStep detached every source/queue/arena alias with admitted work.
			// Preserve its closed marker across later allocator cleanup yields.
			ready, err := c.allocatorPreparation.closePublishedStepV6(w, &c.cleanupOwner, &c.cleanupCharge)
			if !ready || err != nil {
				return false, err
			}
			c.phase = 10
		case 10:
			if ready, err := c.cleanupRefund.CompletePendingWork(&c.cleanupOwner, &c.cleanupCharge, w); !ready || err != nil {
				return false, err
			}
			if c.output != nil {
				owner, charge, ready, err := c.output.CompleteWithWorkV6(w)
				if !ready || err != nil {
					return false, err
				}
				c.output = nil
				c.cleanupOwner, c.cleanupCharge = owner, charge
			}
			c.phase = 11
		case 11:
			if ready, err := c.cleanupRefund.CompletePendingWork(&c.cleanupOwner, &c.cleanupCharge, w); !ready || err != nil {
				return false, err
			}
			if ready, err := c.sealPreparation.closeStep(w); !ready || err != nil {
				return false, err
			}
			if ready, err := c.closeMaterializedInputWorkV6(w); !ready || err != nil {
				return false, err
			}
			if c.outputReader != nil {
				ready, err := c.outputReader.CancelProjectionFrame(w)
				if !ready || err != nil {
					return false, err
				}
				c.outputReader = nil
			}
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))) {
				return false, nil
			}
			c.db, c.snapshot, c.source, c.records = nil, nil, nil, nil
			c.selection = tree.NativeMVCCSelectionV6{}
			c.record = node.LeafEntry{}
			c.request = mvccadmission.Request{}
			c.phase = 6
			return c.completeNativePublicationBindingV6(w)
		default:
			return false, ErrNativeMVCCSnapshotWork
		}
	}
}
