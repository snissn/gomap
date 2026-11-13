package caching

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// The startup maintenance tranche stays charged for the cache lifetime. It
// cannot be reclaimed by another producer drain and is not variable producer
// work. This exercises the bridge's original non-draining public observation.
func TestCOWNativeProducerFixedMaintenancePressure(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, true)
	c := cache.cow
	limit := c.budget.Limits().MaxInFlightBytes
	fixed := c.maintenance.Limits().MaxInFlightBytes
	watermark := fixed + (limit - fixed) - (limit-fixed)/4
	before := c.budget.Stats()
	leaseCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	if fixed == 0 || fixed >= limit || before.ReservedBytes+leaseCharge >= watermark-1 {
		t.Fatalf("invalid startup fixture: fixed=%d limit=%d stats=%+v", fixed, limit, before)
	}
	// An original retained external reservation positions the exact boundary;
	// no bytes or reclaimable generation are fabricated by this fixture.
	reserved := watermark - 1 - before.ReservedBytes - leaseCharge
	lease, err := c.budget.AcquireExternal(reserved)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	observation, err := backend.ObserveNativeProducer()
	if err != nil || observation.ReservedBytes != watermark-1 || observation.UnderPressure {
		t.Fatalf("fixed maintenance counted as variable pressure: observation=%+v fixed=%d watermark=%d err=%v", observation, fixed, watermark, err)
	}
	if err := lease.Resize(reserved + 1); err != nil {
		t.Fatal(err)
	}
	observation, err = backend.ObserveNativeProducer()
	if err != nil || observation.ReservedBytes != watermark || !observation.UnderPressure {
		t.Fatalf("variable producer watermark ignored: observation=%+v watermark=%d err=%v", observation, watermark, err)
	}
	lease.Close()
	observation, err = backend.ObserveNativeProducer()
	if err != nil || observation.UnderPressure {
		t.Fatalf("released variable reservation retained pressure: observation=%+v err=%v", observation, err)
	}
}
