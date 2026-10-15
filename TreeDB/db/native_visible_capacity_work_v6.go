package db

import "unsafe"

// Caller holds runtime.mu. Count every initialized, copied and cleared slot
// in the SAME ordinary capacity operation, including possible head compaction.
// Nothing reserves a right to append later: activation rechecks actual capacity.
func (runtime *rootPublicationRuntimeV1) nativeVisibleCapacityWorkV6(withDebt bool) (records, bytes uint64) {
	records, bytes = 2, 2*uint64(unsafe.Sizeof(*runtime))
	visibleLen := len(runtime.visibleMembers)
	if runtime.visibleHead != 0 && visibleLen == cap(runtime.visibleMembers) {
		records += uint64(2 * visibleLen)
		bytes += uint64(3*visibleLen) * uint64(unsafe.Sizeof(uintptr(0)))
		visibleLen -= runtime.visibleHead
	}
	add := func(length, capacity int) {
		if length < capacity {
			return
		}
		next := capacity * 2
		if next < 4 {
			next = 4
		}
		if next < length+1 {
			next = length + 1
		}
		records += uint64(next + length + capacity)
		bytes += uint64(next+2*length+capacity) * uint64(unsafe.Sizeof(uintptr(0)))
	}
	add(visibleLen, cap(runtime.visibleMembers))
	if withDebt {
		add(len(runtime.debt), cap(runtime.debt))
	}
	return records, bytes
}
