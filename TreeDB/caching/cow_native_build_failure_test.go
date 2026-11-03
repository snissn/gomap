package caching

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type nativeBuildFailureState struct {
	*testSemanticState
	path      string
	file      *os.File
	builds    int
	retryable bool
	failure   error
}

func (s *nativeBuildFailureState) PrepareRollover() (backenddb.NativeSemanticState, error) {
	s.Retain()
	return s, nil
}

func (s *nativeBuildFailureState) PrepareBuild(*backenddb.RootPublicationBuildGroup, *backenddb.Snapshot, *backenddb.StableResourceCaptureLease) error {
	s.builds++
	if s.retryable && s.builds == 1 {
		// This refusal precedes every original asset/ledger mutation.
		return backenddb.NativeSemanticBuildRetryableRefusal{Cause: memtable.ErrCOWCapacity}
	}
	if s.file != nil {
		panic("original native build replayed")
	}
	var err error
	s.file, err = os.Create(s.path)
	if err != nil {
		return err
	}
	if _, err = s.file.Write([]byte("original partial build")); err != nil {
		return err
	}
	if !s.retryable {
		// The identical capacity cause is terminal after actual asset creation.
		s.failure = memtable.ErrCOWCapacity
		return s.failure
	}
	return nil
}

func (s *nativeBuildFailureState) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	if s.failure != nil {
		s.calls++
		return rootpublication.SemanticCleanupCompletion{}, s.failure
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return rootpublication.SemanticCleanupCompletion{}, err
		}
		s.file = nil
	}
	return s.testSemanticState.FinishSemanticCleanup()
}

func TestCOWNativeBuildCapacityBeforeAndAfterOriginalAssetMutation(t *testing.T) {
	for _, retryable := range []bool{false, true} {
		name := "terminal-after-asset"
		if retryable {
			name = "untouched-admission-refusal"
		}
		t.Run(name, func(t *testing.T) {
			cache, backend := nativeBridgeFixture(t, false)
			capture, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			state := &nativeBuildFailureState{testSemanticState: nativeBridgeState(t, capture), path: filepath.Join(t.TempDir(), "original.asset"), retryable: retryable}
			state.cleanup.Work = state
			result, err := backend.WriteNativePublication(&backenddb.NativePreparedPublication{Basis: capture.State(), State: state, StartSequence: capture.LogicalSequence()}, nil, false)
			if err != nil || !result.LogicalInstalled {
				t.Fatalf("logical acceptance=%+v: %v", result, err)
			}
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			if err := cache.Checkpoint(); !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatalf("original build failure=%v", err)
			}
			if retryable {
				if state.file != nil || state.failure != nil {
					t.Fatal("untouched refusal mutated original asset ledger")
				}
				if err := cache.Checkpoint(); err != nil || state.builds != 2 {
					t.Fatalf("untouched admission did not retry: builds=%d err=%v", state.builds, err)
				}
				if err := cache.Close(); err != nil || state.file != nil || state.refund != nil {
					t.Fatalf("successful retry cleanup: %v", err)
				}
				return
			}
			originalFile, originalCharge := state.file, state.refund
			if originalFile == nil || originalCharge == nil {
				t.Fatal("actual original partial asset/charge missing")
			}
			if err := cache.Set([]byte("must-not-advance"), []byte("suffix")); err == nil {
				t.Fatal("normal continuation passed poisoned logical prefix")
			}
			if err := cache.Checkpoint(); err == nil || state.builds != 1 {
				t.Fatal("terminal native materialization replayed")
			}
			if err := cache.Close(); !errors.Is(err, state.failure) || state.builds != 1 || state.calls != 1 || state.file != originalFile || state.refund != originalCharge || state.cleanup.Work != state || state.cleanup.Err != state.failure {
				t.Fatalf("terminal original build custody lost or retried: %v", err)
			}
			if _, err := originalFile.Stat(); err != nil {
				t.Fatalf("failed original asset destroyed: %v", err)
			}
			_ = originalFile.Close() // test-only process cleanup after custody proof
		})
	}
}

// This accepted physical build fails only when preparing the private successor.
// Its original preborn Work owns the actual partial file before reporting error.
type nativeHandoffFailureState struct {
	*testSemanticState
	path     string
	file     *os.File
	failure  error
	handoffs int
}

func (s *nativeHandoffFailureState) PrepareRollover() (backenddb.NativeSemanticState, error) {
	s.Retain()
	return s, nil
}
func (s *nativeHandoffFailureState) PrepareHandoff(backenddb.NativeSemanticState, *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	s.handoffs++
	if s.file != nil {
		panic("original native handoff replayed")
	}
	var err error
	s.file, err = os.Create(s.path)
	if err != nil {
		s.failure = err
		return nil, err
	}
	if _, err = s.file.Write([]byte("original accepted handoff asset")); err != nil {
		s.failure = err
		return nil, err
	}
	s.failure = memtable.ErrCOWCapacity
	return nil, s.failure
}
func (s *nativeHandoffFailureState) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	s.calls++
	return rootpublication.SemanticCleanupCompletion{}, s.failure
}
func TestCOWNativeAcceptedHandoffFailureParksOriginalAsset(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	state := &nativeHandoffFailureState{testSemanticState: nativeBridgeState(t, capture), path: filepath.Join(t.TempDir(), "original-handoff.asset")}
	state.cleanup.Work = state
	result, err := backend.WriteNativePublication(&backenddb.NativePreparedPublication{Basis: capture.State(), State: state, StartSequence: capture.LogicalSequence()}, nil, false)
	if err != nil || !result.LogicalInstalled {
		t.Fatalf("logical acceptance=%+v: %v", result, err)
	}
	if err = capture.Close(); err != nil {
		t.Fatal(err)
	}
	if err = cache.Checkpoint(); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("actual handoff failure=%v", err)
	}
	originalFile, originalCharge, originalHandoff := state.file, state.refund, cache.cow.handoff
	if originalFile == nil || originalCharge == nil || originalHandoff == nil || !originalHandoff.accepted || originalHandoff.failure != state.failure {
		t.Fatal("original accepted handoff custody missing")
	}
	if err = cache.Checkpoint(); err == nil || state.handoffs != 1 {
		t.Fatalf("accepted native handoff retried: %v", err)
	}
	if err = cache.Set([]byte("cannot-advance"), []byte("suffix")); err == nil {
		t.Fatal("publication passed terminal handoff")
	}
	if err = cache.Close(); !errors.Is(err, state.failure) || state.handoffs != 1 || state.calls != 1 || state.file != originalFile || state.refund != originalCharge || state.cleanup.Work != state || state.cleanup.Err != state.failure {
		t.Fatalf("original handoff asset/charge/failure lost or replayed: %v", err)
	}
	if _, err = originalFile.Stat(); err != nil {
		t.Fatalf("failed original physical asset destroyed: %v", err)
	}
	_ = originalFile.Close() // test-only process cleanup after custody proof
}

type nativeSecondHandoffRefusal struct {
	*testSemanticState
	handoffs int
	refusal  error
}

func (s *nativeSecondHandoffRefusal) PrepareRollover() (backenddb.NativeSemanticState, error) {
	s.Retain()
	return s, nil
}
func (s *nativeSecondHandoffRefusal) PrepareHandoff(latest backenddb.NativeSemanticState, _ *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	s.handoffs++
	if s.handoffs == 2 {
		return nil, s.refusal
	}
	latest.Retain()
	return latest, nil
}

// Even an untouched second refusal follows a successful first construction.
// The original accepted attempt must become terminal rather than spend its
// finite successor allowance again against a different suffix.
func TestCOWNativeSecondHandoffRefusalPoisonsBeforeWriterResumes(t *testing.T) {
	cache, backend := nativeBridgeFixture(t, false)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	first := &nativeSecondHandoffRefusal{testSemanticState: nativeBridgeState(t, capture), refusal: backenddb.NativeSemanticHandoffRetryableRefusal{Cause: memtable.ErrCOWCapacity}}
	result, err := backend.WriteNativePublication(&backenddb.NativePreparedPublication{Basis: capture.State(), State: first, StartSequence: capture.LogicalSequence()}, nil, false)
	if err != nil || !result.LogicalInstalled {
		t.Fatalf("first publication=%+v: %v", result, err)
	}
	if err = capture.Close(); err != nil {
		t.Fatal(err)
	}
	if err = cache.rolloverCOWForFlush(); err != nil {
		t.Fatal(err)
	}
	covered := cache.cow.retainCut()
	capture, err = backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	suffix := nativeBridgeState(t, capture)
	result, err = backend.WriteNativePublication(&backenddb.NativePreparedPublication{Basis: capture.State(), State: suffix, StartSequence: capture.LogicalSequence()}, nil, false)
	if err != nil || !result.LogicalInstalled {
		t.Fatalf("suffix publication=%+v: %v", result, err)
	}
	if err = capture.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := cache.cow.prepareCOWBasisInstall()
	if err != nil {
		t.Fatal(err)
	}
	basis, err := cache.cow.captureBackendBasis(backend)
	if err != nil {
		t.Fatal(err)
	}
	original := cache.cow.cut
	before := suffix.refs.Load()
	old, terminal, err := cache.installPreparedCOWBasis(basis, covered, next, backenddb.LeafGenerationPackTransition{})
	if err != first.refusal || !terminal || old != nil || cache.cow.cut != original || first.handoffs != 2 || suffix.refs.Load() != before || next.semanticRootSet != nil {
		t.Fatalf("second refusal changed/replayed ownership: terminal=%v calls=%d refs=%d/%d err=%v", terminal, first.handoffs, suffix.refs.Load(), before, err)
	}
	// This writer acquires the same owner after installPreparedCOWBasis returns;
	// poisoning must already have happened before that owner was unlocked.
	if err = cache.Set([]byte("must-not-advance"), []byte("suffix")); err == nil {
		t.Fatal("writer resumed before terminal poison")
	}
	refund := cache.cow.releasePrivateCOWBasisInstall(next)
	next = nil
	refund.Close()
	if err = basis.release(); err != nil {
		t.Fatal(err)
	}
	cache.cow.releaseCut(covered)
	_ = cache.Close()
}
