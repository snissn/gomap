package caching

import (
	"context"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// Component forwarding controls only. The transition is obtained from the real
// private producer; this fake participant owns no engine/recipe/dictionary and
// is not the graph-only or joined dictionary activation witness.
type combinedLeafPackRefreshControl struct {
	backenddb.NativeSemanticState
	reader                  backenddb.NativeMaintenanceLeafReader
	result                  backenddb.NativeSemanticState
	failure                 error
	leafCalls, genericCalls int
	before, after           *backenddb.Snapshot
	transition              backenddb.LeafGenerationPackTransition
}

func (s *combinedLeafPackRefreshControl) PrepareBasisRefresh(*backenddb.Snapshot, *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	s.genericCalls++
	return nil, backenddb.ErrNativeSemanticUnsupported
}
func (s *combinedLeafPackRefreshControl) PrepareBasisRefreshWithMaintenanceLeaves(before, after *backenddb.Snapshot, leaves backenddb.NativeMaintenanceLeafReader) (backenddb.NativeSemanticState, error) {
	s.genericCalls++
	s.before, s.after, s.reader = before, after, leaves
	return nil, backenddb.ErrNativeSemanticUnsupported
}
func (s *combinedLeafPackRefreshControl) PrepareLeafPackBasisRefresh(before, after *backenddb.Snapshot, transition backenddb.LeafGenerationPackTransition, leaves backenddb.NativeMaintenanceLeafReader) (backenddb.NativeSemanticState, error) {
	s.leafCalls++
	s.before, s.after, s.transition = before, after, transition
	if err := transition.Validate(before, after); err != nil {
		return nil, err
	}
	if leaves != s.reader || leaves != nil && !leaves.MatchesNativeMaintenanceSnapshots(before, after) {
		return nil, backenddb.ErrRootPublicationBasisMismatch
	}
	return s.result, s.failure // preserve the original partial edge and error
}

type combinedLeafPackReaderControl struct {
	before, after *backenddb.Snapshot
	matches       int
}

func (r *combinedLeafPackReaderControl) MatchesNativeMaintenanceSnapshots(before, after *backenddb.Snapshot) bool {
	r.matches++
	return before == r.before && after == r.after
}
func (*combinedLeafPackReaderControl) ReadNativeMaintenanceDictionaryLeaf(*backenddb.Snapshot, page.LeafLogPtr, valuelog.COWRecordShape, []byte) ([]byte, error) {
	return nil, errors.New("forwarding control must not manufacture a dictionary read")
}

func TestCOWLeafPackCombinedRefreshKeepsBothAuthorities(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	cache, backend := privateCOWPackFixture(t)
	preparePrivateCOWPack(t, cache, backend.DB)
	before := backend.DB.AcquireSnapshot()
	if before == nil {
		t.Fatal("missing original producer basis")
	}
	defer func() {
		if before != nil {
			if err := before.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	var after *backenddb.Snapshot
	var transition backenddb.LeafGenerationPackTransition
	calls := 0
	cache.testAfterPrivateCOWLeafPack = func(_ context.Context, stats backenddb.LeafGenerationPackRunOnceStats) error {
		calls++
		transition = stats.Transition
		after = backend.DB.AcquireSnapshot() // exact accepted endpoint, before GC
		return nil
	}
	attempted, err := cache.runPrivateCOWLeafPackSlice()
	cache.testAfterPrivateCOWLeafPack = nil
	if after != nil {
		defer func() {
			if after != nil {
				if err := after.Close(); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	if err != nil || !attempted || calls != 1 || after == nil || transition.IsZero() {
		t.Fatalf("actual producer transition: attempted%t calls%d after%p error%v", attempted, calls, after, err)
	}
	if err := transition.Validate(before, after); err != nil {
		t.Fatalf("actual paired endpoint: %v", err)
	}
	reader := &combinedLeafPackReaderControl{before: before, after: after}
	partial := &struct{ backenddb.NativeSemanticState }{}
	original := errors.New("original participant partial failure")
	state := &combinedLeafPackRefreshControl{reader: reader, result: partial, failure: original}
	next, err := refreshCOWSemanticPhysicalBasis(state, before, after, transition, reader)
	if next != partial || !errors.Is(err, original) || state.leafCalls != 1 || state.genericCalls != 0 || state.before != before || state.after != after || state.transition != transition || reader.matches != 1 {
		t.Fatalf("lost transition/reader/partial/error: next%p leaf%d generic%d matches%d error%v", next, state.leafCalls, state.genericCalls, reader.matches, err)
	}
	// The reader is optional only when the caller supplied nil, never when a
	// dispatcher silently drops the actual nonnil original reader.
	if next, err := refreshCOWSemanticPhysicalBasis(state, before, after, transition, nil); next != nil || !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) {
		t.Fatalf("lost nonnil reader accepted: %v", err)
	}
	wrong := &combinedLeafPackReaderControl{before: after, after: before}
	state.reader = wrong
	if next, err := refreshCOWSemanticPhysicalBasis(state, before, after, transition, wrong); next != nil || !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) || wrong.matches != 1 {
		t.Fatalf("mismatched reader pair accepted: %v", err)
	}
	state.reader = reader
	if next, err := refreshCOWSemanticPhysicalBasis(state, after, before, transition, reader); next != nil || !errors.Is(err, backenddb.ErrRootPublicationBasisMismatch) {
		t.Fatalf("reversed producer authority accepted: %v", err)
	}
	leafCalls := state.leafCalls
	if _, err := refreshCOWSemanticPhysicalBasis(state, before, after, backenddb.LeafGenerationPackTransition{}, reader); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || state.leafCalls != leafCalls || state.genericCalls != 1 {
		t.Fatalf("zero transition acquired leaf-pack privilege: %v", err)
	}
	unsupported := &struct{ backenddb.NativeSemanticState }{}
	if _, err := refreshCOWSemanticPhysicalBasis(unsupported, before, after, transition, reader); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("unsupported owner silently dropped authority: %v", err)
	}
	state.reader, state.result, state.failure = nil, partial, nil
	if next, err := refreshCOWSemanticPhysicalBasis(state, before, after, transition, nil); err != nil || next != partial {
		t.Fatalf("original nil reader interface changed semantics: %v", err)
	}
	state.before, state.after, state.reader, state.result = nil, nil, nil, nil
	reader.before, reader.after, wrong.before, wrong.after = nil, nil, nil, nil
	closing := after
	after = nil // Close consumes the original wrapper even if cleanup reports err.
	if err := closing.Close(); err != nil {
		t.Fatal(err)
	}
	closing = before
	before = nil
	if err := closing.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPrivateCOWLeafPackObserverFailureStillAdopts(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	cache, backend := privateCOWPackFixture(t)
	preparePrivateCOWPack(t, cache, backend.DB)
	want := errors.New("selected post-pack observer failure")
	original := errors.New("selected original late pack failure")
	backend.failure = original
	calls := 0
	cache.testAfterPrivateCOWLeafPack = func(_ context.Context, stats backenddb.LeafGenerationPackRunOnceStats) error {
		calls++
		if !stats.Ran || stats.Pack.BytesCopied == 0 {
			return errors.New("observer did not see actual accepted pack")
		}
		return want
	}
	attempted, err := cache.runPrivateCOWLeafPackSlice()
	if !attempted || !errors.Is(err, want) || !errors.Is(err, original) || calls != 1 {
		t.Fatalf("observer failure custody attempted=%t calls=%d err=%v", attempted, calls, err)
	}
	physical, live := backend.DB.StateToken()
	cut := cache.cow.retainCut()
	if cut == nil {
		t.Fatal("observer error discarded cache basis")
	}
	cached, cachedLive := cut.basis.snapshot.StateToken()
	releaseErr := cache.cow.releaseCut(cut)
	if releaseErr != nil || !live || !cachedLive || cached != physical || cache.cow.refreshRequired.Load() {
		t.Fatalf("observer error bypassed original adoption: cached=%+v physical=%+v release=%v", cached, physical, releaseErr)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("original Close after observer failure: %v", err)
	}
}
