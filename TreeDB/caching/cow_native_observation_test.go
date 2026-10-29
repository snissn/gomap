package caching

import (
	"errors"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCOWNativeProducerObservationDoesNotJoinPressureMutex(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	c := cache.cow
	pressure := backenddb.NativeSemanticProducerPressure{ConstructionBytes: 75, MaxInFlightBytes: 100, MaxTotalBytes: 100, Sources: 1, MaxSources: 8}
	s := &constructionPressureTestState{testSemanticState: newTestSemanticState(nil)}
	s.observe = func(*backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error) { return pressure, nil }
	c.cutMu.Lock()
	c.cut.semanticRootSet = s
	cut, refs := c.cut, c.cut.refs
	c.cutMu.Unlock()
	before, runs, next := c.budget.Stats(), cache.checkpointRuns.Load(), backend.CommandWALNextLSN()
	type result struct {
		observation backenddb.NativeProducerObservation
		err         error
	}
	done := make(chan result, 1)
	c.nativePressureMu.Lock()
	locked := true
	defer func() {
		if locked {
			c.nativePressureMu.Unlock()
		}
	}()
	go func() { observation, err := backend.ObserveNativeProducer(); done <- result{observation, err} }()
	select {
	case got := <-done:
		if got.err != nil || !got.observation.UnderPressure || got.observation.Semantic != pressure {
			t.Fatalf("non-draining observation=%+v err=%v", got.observation, got.err)
		}
	case <-time.After(5 * time.Second):
		c.nativePressureMu.Unlock()
		locked = false
		<-done
		t.Fatal("observation waited for the maintenance mutex")
	}
	c.nativePressureMu.Unlock()
	locked = false
	after := c.budget.Stats()
	before.PeakBytes, after.PeakBytes = 0, 0
	if before != after || c.cut != cut || cut.refs != refs || cache.checkpointRuns.Load() != runs || backend.CommandWALNextLSN() != next || c.nativePressureRemembered {
		t.Fatal("observation retained ownership, assigned work or ran maintenance")
	}
}

func TestCOWNativeProducerPreparationReturnsCompletedObservation(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	c := cache.cow
	pressure := backenddb.NativeSemanticProducerPressure{ConstructionBytes: 75, MaxInFlightBytes: 100, MaxTotalBytes: 100, Sources: 1, MaxSources: 8}
	s := &constructionPressureTestState{testSemanticState: newTestSemanticState(nil)}
	s.observe = func(*backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error) { return pressure, nil }
	c.cutMu.Lock()
	c.cut.semanticRootSet = s
	c.cutMu.Unlock()
	runs := cache.checkpointRuns.Load()
	prepared, err := backend.PrepareNativeProducerObservation()
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := backend.ObserveNativeProducer()
	if err != nil || prepared != fresh || !fresh.UnderPressure || cache.checkpointRuns.Load() != runs+1 {
		t.Fatalf("completed=%+v current=%+v err=%v drains=%d", prepared, fresh, err, cache.checkpointRuns.Load()-runs)
	}
	repeated, err := backend.PrepareNativeProducerObservation()
	if err != nil || repeated != prepared || cache.checkpointRuns.Load() != runs+1 {
		t.Fatalf("unchanged pressured state did not terminate: %+v %v", repeated, err)
	}
	pressure.ConstructionBytes++
	changed, err := backend.PrepareNativeProducerObservation()
	if err != nil || changed == prepared || cache.checkpointRuns.Load() != runs+2 {
		t.Fatalf("changed state was hidden: %+v %v", changed, err)
	}
	failure := errors.New("original inventory failure")
	s.observe = func(*backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error) {
		return backenddb.NativeSemanticProducerPressure{}, failure
	}
	if _, err := backend.ObserveNativeProducer(); !errors.Is(err, failure) {
		t.Fatalf("observation error=%v", err)
	}
	if _, err := backend.PrepareNativeProducerObservation(); !errors.Is(err, failure) {
		t.Fatalf("preparation error=%v", err)
	}
	if cache.checkpointRuns.Load() != runs+2 {
		t.Fatal("failed observation ran maintenance")
	}
}

// A synchronous original cleanup notification may reenter observation. It
// cannot join its own cleanup frontier or the pressure mutex already held by a
// producer. The original Prepare self-join guard/error remains unchanged.
func TestCOWNativeProducerObservationFromNotifyError(t *testing.T) {
	db, c := cowCutFixture(t)
	db.ownedCloseGuard = db.backend.(*backenddb.DB).OwnedCallbackCloseGuard()
	failure := errors.New("original cleanup failure")
	var observation backenddb.NativeProducerObservation
	var observeErr, prepareErr error
	notifications := 0
	db.notifyError = func(err error) {
		notifications++
		if err != failure {
			t.Error(err)
		}
		observation, observeErr = db.ObserveNativeProducer()
		prepareErr = db.PrepareNativeProducer()
	}
	limit := c.budget.Limits().MaxInFlightBytes
	filler, err := c.budget.AcquireExternal(limit - limit/4 - c.budget.Stats().ReservedBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	state := newTestSemanticState(func() error { return failure })
	c.semanticMu.Lock()
	c.enqueueSemanticLocked(state.Release())
	c.semanticMu.Unlock()
	c.nativePressureMu.Lock()
	locked := true
	defer func() {
		if locked {
			c.nativePressureMu.Unlock()
		}
	}()
	done := make(chan struct{})
	go func() { db.drainSemanticCleanup(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		c.nativePressureMu.Unlock()
		locked = false
		<-done
		t.Fatal("cleanup notification joined producer maintenance")
	}
	c.nativePressureMu.Unlock()
	locked = false
	if notifications != 1 || observeErr != nil || !observation.UnderPressure || prepareErr == nil || db.backgroundError() != failure || c.semanticFailed != &state.cleanup || state.calls != 1 {
		t.Fatalf("notification=%d observation=%+v observe=%v prepare=%v original=%v", notifications, observation, observeErr, prepareErr, db.backgroundError())
	}
	// A failed original record remains parked, with its original resource and
	// charge. Observation neither retries Finish nor clears failure ownership.
	db.drainSemanticCleanup()
	if state.calls != 1 || state.resource == nil {
		t.Fatal("observation replayed or refunded failed cleanup")
	}
}
