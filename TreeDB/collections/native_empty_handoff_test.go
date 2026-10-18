package collections

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeEngineEmptyHandoffReservesBothFreshSuccessors(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	maintenance, err := budget.ReserveMaintenance(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := newNativeEmptyEngine(budget, maintenance)
	if err != nil {
		t.Fatal(err)
	}
	promise, count, domains, err := nativeHandoffCapacity(captured)
	charge, chargeErr := nativeEnginePreparationCharge(0, 0)
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	want := 2 * (memtable.COWAllocationCharge(charge) + header)
	if err != nil || chargeErr != nil || count != 0 || domains != 0 || promise != want {
		t.Fatal("empty successor promise", promise, want, count, domains, err, chargeErr)
	}
	build, err := prepareNativeEngineBuild(captured)
	if err != nil {
		t.Fatal(err)
	}
	// Physical acceptance may outlive both budget Close calls. Both successor
	// headers must spend the original promise, without fresh producer admission.
	budget.Close()
	maintenance.Close()
	latest, err := prepareNativeEngineHandoff(captured, nil, 0, &build.allocation)
	if err != nil {
		t.Fatal("first empty successor", err)
	}
	frozen, err := prepareNativeEngineHandoff(captured, nil, 0, &build.allocation)
	if err != nil {
		t.Fatal("second empty successor", err)
	}
	if latest == captured || frozen == captured || latest == frozen || latest.build != nil || frozen.build != nil || captured.build != build {
		t.Fatal("empty handoff reused an original build ledger")
	}
	build = nil
	nativeTestFinishEngine(t, &captured)
	nativeTestFinishEngine(t, &latest)
	nativeTestFinishEngine(t, &frozen)
	if budget.Stats().TotalBytes != 0 || maintenance.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats(), maintenance.Stats())
	}
}
