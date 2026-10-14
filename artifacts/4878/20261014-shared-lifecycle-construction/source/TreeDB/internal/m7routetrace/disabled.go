//go:build !treedb_test || !m7_route_trace

package m7routetrace

// The ordinary/native qualification build has no observer storage or atomics.
type Observer struct{}

func Begin(store, publicDB, backendDB uintptr) *Observer { return nil }
func (o *Observer) End()                                 {}
func (o *Observer) Snapshot() Snapshot                   { return Snapshot{} }
func Observe(owner uintptr, e Event, a, b, c, d uint64)  {}

func Admission(owner uintptr, site, a, b, c, d, e, f uint64)             {}
func ReserveFailure(records, bytes, usedR, usedB, limitR, limitB uint64) {}

func DetailEnabled(owner uintptr) bool                                                            { return false }
func Detail(owner uintptr, site, reason uint64, work [4]uint64, state [48]uint64, keys [6][]byte) {}
