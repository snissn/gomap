package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// continueRetirementV6 is called only by this SAME visible member's original
// RetireWithWork callback, after native publication-turn release. A physical
// Set driver is mandatory: a late nonlast-refcount assumption is not a driver.
func (m *rootPublicationVisibleMemberV1) continueRetirementV6(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (bool, error) {
	if m == nil || a == nil || w == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(*m))) {
		return false, nil
	}
	if ready, err := m.retirementCompletion.CompleteWithWorkV6(&m.retirementRefund, w); !ready || err != nil {
		return false, err
	}
	if ready, err := m.retirementChildCompletion.completeWithWork(&m.retirementRefund, w); !ready || err != nil {
		return false, err
	}
	if ready, err := m.retirementVectorCompletion.completeWithWork(&m.retirementRefund, w); !ready || err != nil {
		return false, err
	}
	if m.retiredPrimary == nil && len(m.retiredSeals) == 0 && m.retiredSealsOwner == nil && m.retiredVisibleResources == nil && m.retiredRecord == (primaryarena.Ref{}) && !m.retiredRecordQueueInitialized && m.retiredPostRoot == (primaryarena.Ref{}) && m.retiredPostSet == nil && (m.install == nil || m.install.post.oldState == nil) {
		return true, nil
	}
	if m.retirementReleaseSet == nil {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	// Consume/activation transfer these exact original edges without invoking a
	// physical callback under coordinator/publication locks. This SAME member
	// finishes them after the fenced native turn has been released.
	if m.retiredRecord != (primaryarena.Ref{}) || m.retiredRecordQueueInitialized {
		if !m.retiredRecordQueueInitialized {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(m.retiredRecordQueue))) {
				return false, nil
			}
			a.InitializeReleaseQueue(&m.retiredRecordQueue)
			m.retiredRecordQueueInitialized = true
		}
		if m.retiredRecord != (primaryarena.Ref{}) {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(m.retiredRecord))) {
				return false, nil
			}
			ready, err := a.DropToReleaseQueue(m.retiredRecord, &m.retiredRecordQueue, w)
			if err != nil || !ready {
				return false, err
			}
			m.retiredRecord = primaryarena.Ref{}
		}
		for {
			ready, progress, err := a.ReleaseQueueStep(&m.retiredRecordQueue, w)
			if err != nil || !ready {
				return false, err
			}
			if !progress {
				break
			}
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(m.retiredRecordQueueInitialized))) {
			return false, nil
		}
		m.retiredRecordQueueInitialized = false
	}
	// The immutable DBState may still be shared with independent snapshots.
	// Move only this publication's existing edges; never mutate that DBState
	// or acquire replacement edges. The containing member already admits these
	// fields and remains original Finish/error custody across every refusal.
	if !m.retiredPostTransferred && m.install != nil {
		if !w.Reserve(3, uint64(unsafe.Sizeof(m.install.post))+uint64(unsafe.Sizeof(DBState{}))+uint64(unsafe.Sizeof(primaryStateRootV5{}))+uint64(unsafe.Sizeof(m.retiredPostRoot))+uint64(unsafe.Sizeof(m.retiredPostSet))+uint64(unsafe.Sizeof(m.retiredPostManager))) {
			return false, nil
		}
		if !m.install.postActivationCompleted {
			return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
		}
		old := m.install.post.oldState
		if old != nil {
			if old.primaryRoot != nil && old.primaryRoot.arena != a {
				return false, rootpublication.ErrDurableRootOwnership
			}
			if old.ValueLogSet != nil && m.install.db.valueLogManager == nil {
				return false, rootpublication.ErrDurableRootOwnership
			}
			if old.primaryRoot != nil {
				m.retiredPostRoot = old.primaryRoot.ref
			}
			m.retiredPostSet = old.ValueLogSet
			if old.ValueLogSet != nil {
				m.retiredPostManager = m.install.db.valueLogManager
			}
			// The member now holds both original edges before detachment.
			m.install.post.oldState = nil
		}
		m.retiredPostTransferred = true
	}
	if m.retiredPostRoot != (primaryarena.Ref{}) || m.retiredRecordQueueInitialized {
		if !m.retiredRecordQueueInitialized {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(m.retiredRecordQueue))) {
				return false, nil
			}
			a.InitializeReleaseQueue(&m.retiredRecordQueue)
			m.retiredRecordQueueInitialized = true
		}
		if m.retiredPostRoot != (primaryarena.Ref{}) {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(m.retiredPostRoot))) {
				return false, nil
			}
			ready, err := a.DropToReleaseQueue(m.retiredPostRoot, &m.retiredRecordQueue, w)
			if err != nil || !ready {
				return false, err
			}
			m.retiredPostRoot = primaryarena.Ref{}
		}
		for {
			ready, progress, err := a.ReleaseQueueStep(&m.retiredRecordQueue, w)
			if err != nil || !ready {
				return false, err
			}
			if !progress {
				break
			}
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(m.retiredRecordQueueInitialized))) {
			return false, nil
		}
		m.retiredRecordQueueInitialized = false
	}
	if m.retiredPostSet != nil {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(m.retiredPostSet))+2*uint64(unsafe.Sizeof(m.retiredPostManager))) {
			return false, nil
		}
		ready, err := m.retiredPostManager.ReleaseNativeMVCCSetStep(m.retiredPostSet, w)
		if err != nil || !ready {
			return false, err
		}
		m.retiredPostSet, m.retiredPostManager = nil, nil
	}
	if m.retiredVisibleResources != nil {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(m.retiredVisibleResources))) {
			return false, nil
		}
		completion, ready, err := m.retirementReleaseSet(m.retiredVisibleResources, w)
		if err != nil || !ready {
			return false, err
		}
		m.retiredVisibleResources = nil
		m.retirementCompletion = completion
		if ready, err := m.retirementCompletion.CompleteWithWorkV6(&m.retirementRefund, w); !ready || err != nil {
			return false, err
		}
	}
	if m.retiredPrimary != nil {
		completion, ready, err := continuePrimaryRuntimeDisposalV6(a, m.retiredPrimary, w, m.retirementReleaseSet)
		if err != nil || !ready {
			return false, err
		}
		m.retiredPrimary = nil
		m.retirementChildCompletion = completion
		if ready, err := m.retirementChildCompletion.completeWithWork(&m.retirementRefund, w); !ready || err != nil {
			return false, err
		}
	}
	for m.retiredSealIndex < len(m.retiredSeals) {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof((*rootPublicationSealV1)(nil)))) {
			return false, nil
		}
		completion, ready, err := continueRootPublicationSealReleaseV6(m.retiredSeals[m.retiredSealIndex], w, m.retirementReleaseSet)
		if err != nil || !ready {
			return false, err
		}
		m.retiredSeals[m.retiredSealIndex] = nil
		m.retirementChildCompletion = completion
		if ready, err := m.retirementChildCompletion.completeWithWork(&m.retirementRefund, w); !ready || err != nil {
			return false, err
		}
		m.retiredSealIndex++
	}
	if !w.Reserve(1, 2*uint64(cap(m.retiredSeals))*uint64(unsafe.Sizeof((*rootPublicationSealV1)(nil)))) {
		return false, nil
	}
	clear(m.retiredSeals[:cap(m.retiredSeals)])
	m.retiredSeals = nil
	owner, charge := m.retiredSealsOwner, m.retiredSealsCharge
	m.retiredSealsOwner, m.retiredSealsCharge = nil, 0
	m.retirementVectorCompletion = primaryRuntimeDisposalCompletionV6{owner, charge}
	return m.retirementVectorCompletion.completeWithWork(&m.retirementRefund, w)
}
