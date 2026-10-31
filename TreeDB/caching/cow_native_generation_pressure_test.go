package caching

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCOWNativeGenerationPressureObservationTracksBasis(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	c := cache.cow
	pressure := backenddb.NativeSemanticProducerPressure{GenerationUnderPressure: true}
	s := &constructionPressureTestState{testSemanticState: newTestSemanticState(nil)}
	cut := c.cut
	refs := cut.refs
	basis, valid := cut.basis.snapshot.StateToken()
	if !valid || basis == (backenddb.StateToken{}) {
		t.Fatal("fixture needs a distinguishable actual physical basis")
	}
	calls := 0
	s.observe = func(snapshot *backenddb.Snapshot) (backenddb.NativeSemanticProducerPressure, error) {
		calls++
		if snapshot != cut.basis.snapshot || cut.refs != refs+1 {
			t.Fatal("observer lost its original paired cut")
		}
		return pressure, nil
	}
	c.cutMu.Lock()
	cut.semanticRootSet = s
	c.cutMu.Unlock()
	before, runs, next := c.budget.Stats(), cache.checkpointRuns.Load(), backend.CommandWALNextLSN()
	got, err := backend.ObserveNativeProducer()
	if err != nil || !got.UnderPressure || got.Semantic != pressure || got.Basis != basis || calls != 1 {
		t.Fatalf("ordinary observation=%+v err=%v calls=%d want basis=%+v", got, err, calls, basis)
	}
	after := c.budget.Stats()
	before.PeakBytes, after.PeakBytes = 0, 0
	if before != after || c.cut != cut || cut.refs != refs || cache.checkpointRuns.Load() != runs || backend.CommandWALNextLSN() != next || c.nativePressureRemembered {
		t.Fatal("generation observation retained ownership, assigned work or ran maintenance")
	}
}
