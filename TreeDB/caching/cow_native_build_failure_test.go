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
