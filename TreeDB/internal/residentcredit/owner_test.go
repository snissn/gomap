package residentcredit

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"testing"
	"unsafe"
)

func TestNativeStringPatchResidentDestinationRetainsExactScope(t *testing.T) {
	control, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Scope{})), true)
	if err != nil {
		t.Fatal(err)
	}
	rootControl, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Owner{})), true)
	if err != nil {
		t.Fatal(err)
	}
	resident, err := New(rootControl + 2*control + 1024)
	if err != nil {
		t.Fatal(err)
	}
	first, err := resident.NewScope()
	if err != nil {
		t.Fatal(err)
	}
	if err = first.ReserveStableMetadata(1024); err != nil {
		t.Fatal(err)
	}
	if err = first.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	first.ReleaseStableMetadata() // Drop preparation owner; real token remains.
	resident.mu.Lock()
	live, births := resident.live, resident.births
	resident.mu.Unlock()
	if live != rootControl+control+1024 {
		t.Fatalf("destination disappeared at handoff: %d", live)
	}
	second, err := resident.NewScope()
	if err != nil {
		t.Fatal(err)
	}
	if err = second.ReserveStableMetadata(1); !errors.Is(err, ErrLimit) {
		t.Fatalf("resident limit: %v", err)
	}
	second.ReleaseStableMetadata()
	resident.Close()
	resident.Close() // DB cleanup may call its idempotent terminal twice.
	resident.mu.Lock()
	if resident.control != rootControl || resident.refs != 1 || resident.live != rootControl+control+1024 {
		t.Fatalf("root control refunded while retained cut lives: %d/%d/%d", resident.control, resident.refs, resident.live)
	}
	resident.mu.Unlock()
	if _, err = resident.NewScope(); !errors.Is(err, ErrLimit) {
		t.Fatalf("created resident after DB close: %v", err)
	}
	first.ReleaseStableMetadata() // Actual retained cut terminal after DB close.
	resident.mu.Lock()
	defer resident.mu.Unlock()
	if resident.live != 0 || resident.control != 0 || resident.refs != 0 || resident.births != births+control {
		t.Fatalf("terminal live/cumulative bytes %d/%d", resident.live, resident.births)
	}
}

func TestNativeStringPatchResidentOwnerRefusesBeforeBirth(t *testing.T) {
	control, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Owner{})), true)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := New(control - 1)
	if !errors.Is(err, ErrLimit) || owner != nil {
		t.Fatalf("unpaid resident owner born: %v %v", owner, err)
	}
	owner, err = New(control)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.NewScope(); !errors.Is(err, ErrLimit) {
		t.Fatalf("scope born without its control class: %v", err)
	}
	owner.Close()
	if owner.live != 0 || owner.refs != 0 || owner.control != 0 || owner.births != control {
		t.Fatalf("empty resident terminal lost birth accounting: %+v", owner)
	}
}

func TestNativeStringPatchResidentScopeDrainPlateau(t *testing.T) {
	rootControl, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Owner{})), true)
	if err != nil {
		t.Fatal(err)
	}
	scopeControl, err := allocclass.ClassBytes(uint64(unsafe.Sizeof(Scope{})), true)
	if err != nil {
		t.Fatal(err)
	}
	const payload = uint64(4096)
	owner, err := New(rootControl + 2*(scopeControl+payload))
	if err != nil {
		t.Fatal(err)
	}
	held, err := owner.NewScope()
	if err != nil {
		t.Fatal(err)
	}
	if err := held.ReserveStableMetadata(payload); err != nil {
		t.Fatal(err)
	}
	for range 128 {
		current, err := owner.NewScope()
		if err != nil {
			t.Fatal(err)
		}
		if err := current.ReserveStableMetadata(payload); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.NewScope(); !errors.Is(err, ErrLimit) {
			t.Fatalf("third full-live scope exceeded resident ceiling: %v", err)
		}
		current.ReleaseStableMetadata()
		if owner.live != rootControl+scopeControl+payload || owner.refs != 2 {
			t.Fatalf("drained scope retained historical backing: %d/%d", owner.live, owner.refs)
		}
	}
	owner.Close()
	held.ReleaseStableMetadata()
	if owner.live != 0 || owner.refs != 0 || owner.births != rootControl+129*(scopeControl+payload) {
		t.Fatalf("last held cut terminal: %d/%d/%d", owner.live, owner.refs, owner.births)
	}
}

func TestNativeStringPatchAllocatorAndMetadataUseSameResidentScopes(t *testing.T) {
	master, err := New(4096)
	if err != nil {
		t.Fatal(err)
	}
	creator, err := master.NewScope()
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := master.NewScope()
	if err != nil {
		t.Fatal(err)
	}
	if creator == scratch {
		t.Fatal("factory reused a retained scope")
	}
	if err = creator.ReserveAllocation(1024); err != nil {
		t.Fatal(err)
	}
	if err = creator.RetainAllocationCredit(); err != nil {
		t.Fatal(err)
	}
	if err = scratch.ReserveStableMetadata(1024); err != nil {
		t.Fatal(err)
	}
	master.mu.Lock()
	before, births := master.live, master.births
	master.mu.Unlock()
	scratchBytes := scratch.bytes
	scratch.ReleaseStableMetadata()
	creator.ReleaseStableMetadata() // End preparation, preserve actual intrinsic edge.
	master.mu.Lock()
	if master.live != before-scratchBytes || master.births != births || creator.closed {
		t.Fatal("scratch/ref transfer refunded births or retired intrinsic creator")
	}
	master.mu.Unlock()
	master.Close()
	if err = creator.ReserveAllocation(1); !errors.Is(err, ErrLimit) {
		t.Fatal("closed master admitted a new allocator birth", err)
	}
	creator.ReleaseAllocationCredit()
	master.mu.Lock()
	defer master.mu.Unlock()
	if master.live != 0 || master.refs != 0 || master.births != births {
		t.Fatal("resident master outlived exact final allocator edge")
	}
}

// Ordinary ownership is censused without a new DB cap. A live strict scope
// closes foreign growth admission on the SAME owner until its actual last edge.
func TestOrdinaryResidentGrowthAndStrictLoanEnvelope(t *testing.T) {
	c, err := NewOrdinary(1024)
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := c.NewOrdinaryScope()
	if err != nil {
		t.Fatal(err)
	}
	if err = ordinary.ReserveStableMetadata(2048); err != nil {
		t.Fatal("ordinary growth capped", err)
	}
	before := c.Stats()
	if strict, err := c.NewScope(); !errors.Is(err, ErrLimit) || strict != nil || c.Stats() != before {
		t.Fatal("over-budget ordinary closure became strict")
	}
	ordinary.ReleaseStableMetadata()
	ordinary, err = c.NewOrdinaryScope()
	if err != nil {
		t.Fatal(err)
	}
	strict, err := c.NewScope()
	if err != nil {
		t.Fatal(err)
	}
	if err = strict.ReserveStableMetadata(c.Limit() - c.Stats().Live); err != nil {
		t.Fatal(err)
	}
	before = c.Stats()
	if err = ordinary.ReserveStableMetadata(1); !errors.Is(err, ErrLimit) || c.Stats() != before {
		t.Fatal("foreign growth evaded live selected closure")
	}
	if err = strict.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	strict.ReleaseStableMetadata()
	if err = ordinary.ReserveStableMetadata(1); !errors.Is(err, ErrLimit) {
		t.Fatal("creator-first release erased a live borrower")
	}
	strict.ReleaseStableMetadata()
	if err = ordinary.ReserveStableMetadata(2048); err != nil {
		t.Fatal("drained strict scope changed ordinary behavior", err)
	}
	c.Close()
	if c.Stats().Refs != 1 || c.Stats().Control == 0 {
		t.Fatal("DB close released live ordinary creator")
	}
	ordinary.ReleaseStableMetadata()
	if got := c.Stats(); got.Live != 0 || got.Refs != 0 || got.Control != 0 || got.Births <= before.Births {
		t.Fatal(got)
	}
}
