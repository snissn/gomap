package db

import (
	"errors"
	"math"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/lifecycle"
)

// Observe the existing original-owner enrollment, without a second production
// admission hook. Refusal must follow the successful containing-lease charge.
type directoryRegistryAdmissionBudget struct {
	capacity, bytes, leaseTarget uint64
	leaseAdmitted, growthRefused bool
	refusedFrom, refusedTarget   uint64
}

type directoryRegistryAdmissionLease struct {
	budget *directoryRegistryAdmissionBudget
}

func (b *directoryRegistryAdmissionBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	if n > b.capacity {
		return nil, retainedalloc.ErrCapacity
	}
	b.bytes = n
	return &directoryRegistryAdmissionLease{budget: b}, nil
}
func (l *directoryRegistryAdmissionLease) Resize(n uint64) error {
	b := l.budget
	if n > b.capacity {
		b.growthRefused = true
		b.refusedFrom, b.refusedTarget = b.bytes, n
		return retainedalloc.ErrCapacity
	}
	b.bytes = n
	if b.leaseTarget != 0 && n == b.leaseTarget {
		b.leaseAdmitted = true
	}
	return nil
}
func (l *directoryRegistryAdmissionLease) Close() { l.budget.bytes = 0 }

func TestOriginalRegistryDataDirectoryGrowthRefusalRollsBackLease(t *testing.T) {
	opts := Options{Dir: t.TempDir(), DisableBackgroundPrune: true}
	cfg := formatConfigFromOptions(opts)
	cfg.RequiredFeatures = appendRequiredFormatFeature(cfg.RequiredFeatures, RequiredFeatureDependencyDirectoryV2)
	if err := SaveFormatConfig(opts.Dir, cfg); err != nil {
		t.Fatal(err)
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	idx := database.idx.Load()
	identityRegistry := database.StableResourceIdentityPinRegistry()
	if idx == nil || idx.metadata == nil || idx.metadata != identityRegistry.MetadataOwner() || !database.dependencyDirectoryRequiredFeature {
		t.Fatal("not the actual V2 DATA original-owner route")
	}
	originalRegistry, owner := idx.registry, idx.metadata
	record := database.durableRoot.record
	if record.Directory.RootPageID == 0 || record.TotalPages == 0 {
		t.Fatal("actual V2 directory was not bound before pressure")
	}
	budget := &directoryRegistryAdmissionBudget{capacity: math.MaxUint64}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		budget.capacity = math.MaxUint64
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
			database = nil
		}
		enrollment.Close()
	}()
	var pins [lifecycle.FastReaderShardCount]int64
	defer func() {
		for i, id := range pins {
			if id != 0 {
				originalRegistry.Unregister(id)
				pins[i] = 0
			}
		}
	}()
	// Opening already owns real directory cohorts. Fill only remaining fixed
	// slots; fast-only refusal cannot leave reusable slow-slot storage behind.
	for i := range pins {
		pins[i], _ = originalRegistry.RegisterFastWithHint(uint64(100+i), -1)
		if pins[i] == 0 {
			break
		}
	}
	const targetSequence = 1000
	if id, _ := originalRegistry.RegisterFastWithHint(targetSequence, -1); id != 0 {
		originalRegistry.Unregister(id)
		t.Fatal("fixture did not saturate original fixed cohorts")
	}
	before, state := owner.Bytes(), database.state.Load()
	borrows := idx.wrapperBorrowCountV6()
	allMin, readerMin := originalRegistry.MinPinnedSeq(), originalRegistry.MinReaderPinnedSeq()
	leaseCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(dataDependencyLeaseV6{})))
	budget.leaseTarget = before + leaseCharge
	budget.capacity = budget.leaseTarget
	database.rootReuseMu.Lock()
	directory, err := database.pinDependencyDirectoryV2(idx, targetSequence, record.Directory, record.TotalPages)
	database.rootReuseMu.Unlock()
	if directory != nil {
		if consumed, releaseErr := directory.ReleaseOutcome(); !consumed || releaseErr != nil {
			t.Errorf("unexpected directory cleanup consumed=%t: %v", consumed, releaseErr)
		}
		t.Fatal("refused registry growth returned an exposed directory")
	}
	if !errors.Is(err, retainedalloc.ErrCapacity) || !budget.leaseAdmitted || !budget.growthRefused || budget.refusedFrom != budget.leaseTarget || budget.refusedTarget <= budget.leaseTarget {
		t.Fatalf("refusal did not follow original lease admission: err=%v budget=%+v", err, budget)
	}
	if owner.Bytes() != before || budget.bytes != before || idx.wrapperBorrowCountV6() != borrows || database.state.Load() != state || database.idx.Load() != idx || idx.registry != originalRegistry || database.durableRoot.record.Directory != record.Directory || database.durableRoot.record.CommitSeq != record.CommitSeq || originalRegistry.MinPinnedSeq() != allMin || originalRegistry.MinReaderPinnedSeq() != readerMin {
		t.Fatal("refused directory registration changed original charge, borrow, pins, or published state")
	}
	// Leave fixed cohorts full: removing only capacity pressure must permit
	// the SAME route to grow admitted fallback storage and bind the actual root.
	budget.capacity = math.MaxUint64
	database.rootReuseMu.Lock()
	directory, err = database.pinDependencyDirectoryV2(idx, targetSequence, record.Directory, record.TotalPages)
	database.rootReuseMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if directory == nil {
		t.Fatal("available admission returned no directory")
	}
	defer func() {
		if directory != nil {
			if consumed, err := directory.ReleaseOutcome(); !consumed || err != nil {
				t.Errorf("directory cleanup consumed=%t: %v", consumed, err)
			}
		}
	}()
	if directory.Reference() != record.Directory || directory.MetadataOwner() != owner || idx.wrapperBorrowCountV6() != borrows+1 || owner.Bytes() <= before+leaseCharge {
		t.Fatal("successful retry did not bind actual original-owner directory with fallback storage")
	}
	if err := directory.Walk(func(_, _ []byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if consumed, err := directory.ReleaseOutcome(); !consumed || err != nil {
		t.Fatalf("directory release consumed=%t: %v", consumed, err)
	}
	directory = nil
	if idx.wrapperBorrowCountV6() != borrows || originalRegistry.MinPinnedSeq() != allMin || originalRegistry.MinReaderPinnedSeq() != readerMin || owner.Bytes() <= before {
		t.Fatal("directory release lost original pin state or retained fallback capacity")
	}
	for i, id := range pins {
		if id != 0 {
			originalRegistry.Unregister(id)
			pins[i] = 0
		}
	}
	originalRegistry, idx = nil, nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	enrollment.Close()
	if owner.Bytes() != 0 || budget.bytes != 0 || identityRegistry.ActivePins() != 0 {
		t.Fatalf("final original custody retained: owner=%d budget=%d pins=%d", owner.Bytes(), budget.bytes, identityRegistry.ActivePins())
	}
}
