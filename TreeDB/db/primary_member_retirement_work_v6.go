package db

import (
 "unsafe"
 "github.com/snissn/gomap/TreeDB/internal/iterator"
 "github.com/snissn/gomap/TreeDB/internal/primaryarena"
 "github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// continueRetirementV6 is called only by this SAME visible member's original
// RetireWithWork callback, after native publication-turn release. A physical
// Set driver is mandatory: a late nonlast-refcount assumption is not a driver.
func (m *rootPublicationVisibleMemberV1) continueRetirementV6(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (bool,error) {
 if m == nil || a == nil || w == nil { return false, rootpublication.ErrDurableRootOwnership }
 if m.retiredPrimary == nil && len(m.retiredSeals) == 0 && m.retiredSealsOwner == nil { return true,nil }
 if m.retirementReleaseSet == nil { return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6 }
 if m.retiredPrimary != nil {
  completion,ready,err:=continuePrimaryRuntimeDisposalV6(a,m.retiredPrimary,w,m.retirementReleaseSet)
  if err!=nil || !ready { return false,err }
  m.retiredPrimary=nil
  completion.complete()
 }
 for m.retiredSealIndex<len(m.retiredSeals) {
  // Until original seal root/bank/token/Set stages are connected, retain the
  // exact seal and its containing backing rather than invoking ordinary release.
  if m.retiredSeals[m.retiredSealIndex]!=nil { return false,rootpublication.ErrNativeMVCCRootWorkRequiredV6 }
  if !w.Reserve(1,2*uint64(unsafe.Sizeof((*rootPublicationSealV1)(nil)))) { return false,nil }
  m.retiredSealIndex++
 }
 if !w.Reserve(1,2*uint64(cap(m.retiredSeals))*uint64(unsafe.Sizeof((*rootPublicationSealV1)(nil)))) { return false,nil }
 clear(m.retiredSeals[:cap(m.retiredSeals)])
 m.retiredSeals=nil
 owner,charge:=m.retiredSealsOwner,m.retiredSealsCharge
 m.retiredSealsOwner,m.retiredSealsCharge=nil,0
 if owner!=nil { owner.RemovePending(charge) }
 return true,nil
}
