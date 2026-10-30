package rootpublication

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestOwnedPreparedReadRangeSurvivesSourceRelease(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "range", "original-resource-bytes")
	registry := NewIdentityPinRegistry()
	t.Cleanup(registry.Close)
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	obligation := StableLogicalObligation{Class: "column", Kind: "part", Namespace: "n", Generation: 1, PartID: 1, FileID: 1, Offset: 1, Length: 4, Reachability: ReachabilityColumnManifest, Digest: sha256.Sum256([]byte("rigi"))}
	token, err := NewStableResourceToken(StableResourceSpec{Kind: ResourceColumnAsset, LogicalLane: "n", ResourceID: "1", Generation: 1,
		File: file, Frontier: DurableFrontier{Bytes: 23}, Reachability: ReachabilityColumnManifest,
		LogicalObligations: []StableLogicalObligation{obligation}, PinRegistry: registry, OriginalObservation: registry})
	if err != nil {
		_ = registry.Unobserve(identity)
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder()
	if err := builder.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if source != nil {
			source.Release()
		}
	})
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(budget.Close)
	var owner retainedalloc.Owner
	owner.Initialize(0)
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		enrollment.Close()
		if err := owner.Close(); err != nil {
			t.Errorf("owner close: %v", err)
		}
	})
	selector := StableResourceSelector{Kind: ResourceColumnAsset, LogicalLane: "n", ResourceID: "1", PhysicalGeneration: 1, Obligation: obligation}
	baseline := owner.Bytes()
	if reader, err := CaptureOwnedPreparedReadRange(&owner, nil, registry, source, selector); reader != nil || err == nil {
		t.Fatal("unenrolled capture admitted")
	}
	foreign := NewIdentityPinRegistry()
	defer foreign.Close()
	if reader, err := CaptureOwnedPreparedReadRange(&owner, enrollment, foreign, source, selector); reader != nil || err == nil {
		t.Fatal("foreign registry admitted")
	}
	if owner.Bytes() != baseline {
		t.Fatal("registry refusal retained side charge")
	}
	func() {
		source.mu.Lock()
		if source.kindViews == nil {
			source.mu.Unlock()
			t.Fatal("fixture lacks generic kind views")
		}
		previous := source.kindViews.cells
		source.kindViews.cells = make([]resourceKindCell, 65)
		source.mu.Unlock()
		defer func() { source.mu.Lock(); source.kindViews.cells = previous; source.mu.Unlock() }()
		if reader, err := CaptureOwnedPreparedReadRange(&owner, enrollment, registry, source, selector); reader != nil || err == nil {
			t.Fatal("over-cap kind census admitted")
		}
		if owner.Bytes() != baseline {
			t.Fatal("over-cap refusal retained side charge")
		}
	}()
	// An unsupported directory must be rejected before its lookup can allocate
	// or touch the zero-value directory used by this control.
	func() {
		source.mu.Lock()
		var entry *stableResourceEntry
		source.rangeEntriesLocked(func(candidate *stableResourceEntry) bool { entry = candidate; return false })
		if entry == nil {
			source.mu.Unlock()
			t.Fatal("missing generic entry")
		}
		previous := entry.logicalObligations.directory
		entry.logicalObligations.directory = &DependencyDirectoryV2{}
		source.mu.Unlock()
		defer func() { source.mu.Lock(); entry.logicalObligations.directory = previous; source.mu.Unlock() }()
		if reader, err := CaptureOwnedPreparedReadRange(&owner, enrollment, registry, source, selector); reader != nil || err == nil {
			t.Fatalf("directory-backed lookup admitted: %#v %v", reader, err)
		}
		if owner.Bytes() != baseline {
			t.Fatal("directory refusal retained side charge")
		}
	}()
	for _, bad := range []StableResourceSelector{
		{Kind: ResourceColumnAsset, LogicalLane: "n", ResourceID: "wrong", PhysicalGeneration: 1, Obligation: obligation},
		{Kind: ResourceColumnAsset, LogicalLane: "n", ResourceID: "1", PhysicalGeneration: 1, Obligation: StableLogicalObligation{Offset: math.MaxInt64, Length: 1, Reachability: ReachabilityColumnManifest}},
		{Kind: ResourceQueryReadyAsset, LogicalLane: "n", ResourceID: "1", PhysicalGeneration: 1, Obligation: StableLogicalObligation{Length: 1, Reachability: ReachabilityQueryReadyBase}},
	} {
		if reader, err := CaptureOwnedPreparedReadRange(&owner, enrollment, registry, source, bad); reader != nil || err == nil {
			t.Fatalf("bad selector admitted: %#v %v", reader, err)
		}
		if owner.Bytes() != baseline {
			t.Fatal("selector refusal retained side charge")
		}
	}
	reader, err := CaptureOwnedPreparedReadRange(&owner, enrollment, registry, source, selector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if reader != nil {
			if err := reader.CloseWithError(); err != nil {
				t.Errorf("reader close: %v", err)
			}
		}
	})
	if err := source.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	source = nil
	// The reader owns its separately admitted edge; the caller's original
	// enrollment can close without invalidating the captured handle.
	enrollment.Close()
	if budget.Stats().ExternalLeases == 0 {
		t.Fatal("reader did not retain its governing budget")
	}
	if got := registry.ObserverCount(identity); got != 0 {
		t.Fatalf("observations=%d", got)
	}
	if got := registry.ActivePins(); got != 1 {
		t.Fatalf("pins=%d", got)
	}
	if deletion, err := registry.BeginDelete(identity); !errors.Is(err, ErrResourcePinned) {
		if deletion != nil {
			deletion.Abort()
		}
		t.Fatalf("delete while range held: %v", err)
	}
	data := make([]byte, 4)
	if n, err := reader.ReadAt(data, 0); err != nil || n != 4 || string(data) != "rigi" {
		t.Fatalf("read=%q n=%d err=%v", data, n, err)
	}
	if n, err := reader.ReadAt(data, -1); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("negative read: %d %v", n, err)
	}
	if n, err := reader.ReadAt(data, 1); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("out-of-range read: %d %v", n, err)
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		for i := 0; i < 32; i++ {
			buf := make([]byte, 4)
			n, err := reader.ReadAt(buf, 0)
			if errors.Is(err, ErrResourceOwnership) {
				done <- nil
				return
			}
			if err != nil || n != 4 || string(buf) != "rigi" {
				done <- fmt.Errorf("concurrent read=%q n=%d err=%v", buf, n, err)
				return
			}
		}
		done <- nil
	}()
	<-started
	closeErr := reader.CloseWithError()
	readErr := <-done
	if closeErr != nil || readErr != nil {
		t.Fatalf("concurrent close=%v read=%v", closeErr, readErr)
	}
	reader = nil
	if owner.Bytes() != 0 || budget.Stats().ExternalLeases != 0 || registry.ActivePins() != 0 {
		t.Fatal("range cleanup retained admission or pin")
	}
}

// This injects a close error by consuming the duplicate before the capability
// closes it. It tests failure custody, not whether arbitrary OS close errors
// leave a descriptor open.
func TestOwnedPreparedReadRangeCloseFailureKeepsCustody(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "failed-close", "original-resource-bytes")
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	obligation := StableLogicalObligation{Class: "column", Kind: "part", Namespace: "n", Generation: 1, PartID: 1, FileID: 1, Offset: 1, Length: 4, Reachability: ReachabilityColumnManifest, Digest: sha256.Sum256([]byte("rigi"))}
	token, err := NewStableResourceToken(StableResourceSpec{Kind: ResourceColumnAsset, LogicalLane: "n", ResourceID: "1", Generation: 1, File: file, Frontier: DurableFrontier{Bytes: 23}, Reachability: ReachabilityColumnManifest, LogicalObligations: []StableLogicalObligation{obligation}, PinRegistry: registry, OriginalObservation: registry})
	if err != nil {
		_ = registry.Unobserve(identity)
		t.Fatal(err)
	}
	builder := NewStableResourceSetBuilder()
	if err = builder.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	defer func() {
		if source != nil {
			source.Release()
		}
	}()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	failed := false
	defer func() {
		if !failed {
			enrollment.Close()
			_ = owner.Close()
		}
	}()
	selector := StableResourceSelector{Kind: ResourceColumnAsset, LogicalLane: "n", ResourceID: "1", PhysicalGeneration: 1, Obligation: obligation}
	reader, err := CaptureOwnedPreparedReadRange(&owner, enrollment, registry, source, selector)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !failed {
			_ = reader.CloseWithError()
		}
	}()
	if err = source.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	source = nil
	if err = reader.file.Close(); err != nil {
		t.Fatal(err)
	}
	first := reader.CloseWithError()
	failed = first != nil
	if !errors.Is(first, os.ErrClosed) || !errors.Is(registry.CleanupError(), os.ErrClosed) {
		t.Fatalf("failure not retained: close=%v registry=%v", first, registry.CleanupError())
	}
	if registry.failedPinned == nil || registry.failedPinned.custody != reader || registry.ActivePins() != 1 || owner.Bytes() == 0 {
		t.Fatal("exact reader, pin, or owner charge lost")
	}
	if next := reader.CloseWithError(); next != first {
		t.Fatalf("second close changed failure: %v", next)
	}
	budget.Close()
	if budget.Stats().ExternalLeases == 0 {
		t.Fatal("closed budget refunded failed cleanup custody")
	}
	// The failed payload retains the reader's own enrollment. Other governing
	// enrollments still follow their own independent custody contracts.
	enrollment.CloseAfterCleanup(first)
}
