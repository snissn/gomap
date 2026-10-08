package freelist

import "unsafe"

// Ledger mutation keeps its existing lock and engine. This receipt admits
// prospective memberships and explicit old/new backing before the first edit.
type reservationAdmissionV1 struct {
	operation                     allocationOperationV1
	creator                       *allocationCreditLeaseV1
	newReservation                bool
	idsCapacity, coverageCapacity int
}

func grownCapacityV1(current, needed int) int {
	if needed <= current {
		return current
	}
	if current > int(^uint(0)>>1)/2 {
		return needed
	}
	return max(needed, max(1, current*2))
}
func (l *ReservationLedger) admitReservationV1(request AllocationRequestCreditV1, candidate CandidateIDV1, r *reservation, dataCount, newOwners, coverageCount int, creator *allocationCreditLeaseV1, extra ...allocationOperationV1) (reservationAdmissionV1, error) {
	maximum := int(^uint(0) >> 1)
	if dataCount < 0 || newOwners < 0 || coverageCount < 0 || len(extra) > 1 {
		return reservationAdmissionV1{}, ErrNoAllocatablePage
	}
	plan := reservationAdmissionV1{creator: creator, newReservation: r == nil}
	bytes, refs := l.owners.insertionCapacityV1(uint64(newOwners))
	if r == nil {
		b, n := l.candidates.insertionCapacityV1(1)
		bytes = cowSaturatingAddV1(bytes, b)
		refs = cowSaturatingAddV1(refs, n)
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(reservation{})), true))
		refs++
	}
	oldIDs, oldCoverage := 0, 0
	if r != nil {
		oldIDs, oldCoverage = cap(r.ids), cap(r.abandonedCoverage)
	}
	plan.idsCapacity = grownCapacityV1(oldIDs, dataCount)
	plan.coverageCapacity = grownCapacityV1(oldCoverage, coverageCount)
	if plan.idsCapacity > maximum/8 || plan.coverageCapacity > maximum/int(unsafe.Sizeof(reservationInterval{})) {
		return reservationAdmissionV1{}, ErrNoAllocatablePage
	}
	if plan.idsCapacity > oldIDs {
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(plan.idsCapacity)*8, false))
		refs++
	}
	if plan.coverageCapacity > oldCoverage {
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(plan.coverageCapacity)*uint64(unsafe.Sizeof(reservationInterval{})), false))
		refs++
	}
	if len(extra) != 0 {
		bytes = cowSaturatingAddV1(bytes, extra[0].bytes)
		refs = cowSaturatingAddV1(refs, extra[0].refs)
	}
	operation, err := admitAllocationOperationV1(request, creator, bytes, refs)
	if err != nil {
		return reservationAdmissionV1{}, err
	}
	plan.operation = operation
	return plan, nil
}
func (plan *reservationAdmissionV1) prepare(r *reservation) (*reservation, error) {
	if r == nil {
		if err := plan.operation.take(allocationClassV1(uint64(unsafe.Sizeof(reservation{})), true), 1); err != nil {
			return nil, err
		}
		r = &reservation{creator: plan.creator, state: CandidatePreVisible}
	}
	if plan.idsCapacity > cap(r.ids) {
		if err := plan.operation.take(allocationClassV1(uint64(plan.idsCapacity)*8, false), 1); err != nil {
			return nil, err
		}
		next := make([]uint64, len(r.ids), plan.idsCapacity)
		copy(next, r.ids)
		old := r.idsCreator
		clear(r.ids[:cap(r.ids)])
		r.ids, r.idsCreator = next, plan.creator
		old.release()
	}
	if plan.coverageCapacity > cap(r.abandonedCoverage) {
		if err := plan.operation.take(allocationClassV1(uint64(plan.coverageCapacity)*uint64(unsafe.Sizeof(reservationInterval{})), false), 1); err != nil {
			return nil, err
		}
		next := make([]reservationInterval, len(r.abandonedCoverage), plan.coverageCapacity)
		copy(next, r.abandonedCoverage)
		old := r.coverageCreator
		clear(r.abandonedCoverage[:cap(r.abandonedCoverage)])
		r.abandonedCoverage, r.coverageCreator = next, plan.creator
		old.release()
	}
	return r, nil
}
func releaseReservationBackingV1(r *reservation) {
	if r == nil {
		return
	}
	creator, idsCreator, coverageCreator := r.creator, r.idsCreator, r.coverageCreator
	clear(r.ids[:cap(r.ids)])
	clear(r.abandonedCoverage[:cap(r.abandonedCoverage)])
	r.ids, r.abandonedCoverage = nil, nil
	r.creator, r.idsCreator, r.coverageCreator = nil, nil, nil
	creator.release()
	idsCreator.release()
	coverageCreator.release()
}
