package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The original member owns its state edge separately from the constructor's
// edge. An unaccepted candidate cancels only its original private COW claim;
// the materialized producer still owns installed-page rollback until acceptance.
func (runtime *rootPublicationRuntimeV1) abortNativeVisibleMemberV6(m *rootPublicationVisibleMemberV1, cow *freelist.PreparedCOWCandidateV1, input rootpublication.DurableRootCallbackInput, w *iterator.OrdinalScanWork) (bool, error) {
	if runtime == nil || m == nil || m.install == nil || w == nil || input.PreparedCOW != cow || input.PreparedPrimary != m.primary || input.Sequence != m.sequence || m.install.activated {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*m))) {
		return false, nil
	}
	if m.abortPhase == 0 {
		if m.prepared != nil {
			if m.allocatorPreparation == nil {
				return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
			}
			ready, err := m.allocatorPreparation.Cancel(w)
			if !ready || err != nil {
				return false, err
			}
		}
		runtime.idx.primary.InitializeReleaseQueue(&m.retiredRecordQueue)
		m.retiredRecordQueueInitialized = true
		m.abortPhase = 1
	}
	if m.abortPhase == 1 {
		if m.primary != nil && m.primary.Bundle().Record != (primaryarena.Ref{}) {
			ready, err := runtime.idx.primary.DropToReleaseQueue(m.primary.Bundle().Record, &m.retiredRecordQueue, w)
			if !ready || err != nil {
				return false, err
			}
		}
		m.abortPhase = 2
	}
	if m.abortPhase == 2 {
		if m.install.primaryRoot != nil {
			ready, err := runtime.idx.primary.DropToReleaseQueue(m.install.primaryRoot.ref, &m.retiredRecordQueue, w)
			if !ready || err != nil {
				return false, err
			}
			m.install.primaryRoot = nil
		}
		m.abortPhase = 3
	}
	return true, nil
}

// The constructor invokes this only after dropping its own root into its SAME
// original queue. Scrub borrowed proof aliases before either queue can refund
// their backing, then release the original visible clone and Set pin finitely.
func (runtime *rootPublicationRuntimeV1) completeNativeVisibleMemberAbortV6(m *rootPublicationVisibleMemberV1, cow *freelist.PreparedCOWCandidateV1, input rootpublication.DurableRootCallbackInput, w *iterator.OrdinalScanWork) (bool, error) {
	if runtime == nil || m == nil || m.install == nil || w == nil || input.PreparedCOW != cow || input.Sequence != m.sequence || m.install.activated || m.abortPhase < 3 {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*m))+2*uint64(unsafe.Sizeof(*m.install))) {
		return false, nil
	}
	if ready, err := m.retirementCompletion.CompleteWithWorkV6(&m.retirementRefund, w); !ready || err != nil {
		return false, err
	}
	if m.abortPhase == 3 {
		if input.PreparedPrimary != m.primary {
			return false, rootpublication.ErrDurableRootOwnership
		}
		m.primary = nil
		m.install.activationState, m.install.activationSnapshot = nil, nil
		m.abortPhase = 4
	}
	for m.abortPhase == 4 {
		ready, progress, err := runtime.idx.primary.ReleaseQueueStep(&m.retiredRecordQueue, w)
		if !ready || err != nil {
			return false, err
		}
		if !progress {
			m.abortPhase = 5
		}
	}
	if m.abortPhase == 5 {
		if m.resources != nil {
			completion, ready, err := releasePrimaryResourceSetWithWorkV6(m.resources, w)
			if !ready || err != nil {
				return false, err
			}
			m.resources = nil
			m.retirementCompletion = completion
			if ready, err := m.retirementCompletion.CompleteWithWorkV6(&m.retirementRefund, w); !ready || err != nil {
				return false, err
			}
		}
		m.abortPhase = 6
	}
	if m.abortPhase == 6 {
		install := m.install
		// Native capture has no legacy scanned-count allocation. Its own
		// continuation is required before this route can accept such evidence.
		if install.vlogRefCounts != nil || install.metadataCharge != 0 {
			return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
		}
		if install.valueLogSet != nil {
			if install.db == nil || install.db.valueLogManager == nil {
				return false, rootpublication.ErrResourceOwnership
			}
			ready, err := install.db.valueLogManager.ReleaseNativeMVCCSetStep(install.valueLogSet, w)
			if !ready || err != nil {
				return false, err
			}
			install.valueLogSet = nil
		}
		m.abortPhase = 7
	}
	return true, nil
}
