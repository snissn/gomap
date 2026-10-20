package treedb

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The callback captures handles only as a test observer. Production cleanup
// receipts carry independent accounting, never this observer or an EngineVersion.
type publicSemanticCleanupState struct {
	refs      atomic.Int32
	cleanup   rootpublication.SemanticCleanup
	lease     backenddb.NativeAllocationReceipt
	file      *os.File
	directory string
	before    func()
	entered   chan struct{}
	resume    <-chan struct{}
	calls     atomic.Int32
	failure   error
}

func (s *publicSemanticCleanupState) PrepareRollover() (backenddb.NativeSemanticState, error) {
	s.Retain()
	return s, nil
}

func (s *publicSemanticCleanupState) Retain() { s.refs.Add(1) }
func (s *publicSemanticCleanupState) Release() *rootpublication.SemanticCleanup {
	if s.refs.Add(-1) == 0 {
		return &s.cleanup
	}
	return nil
}
func (*publicSemanticCleanupState) SparseProjections() []rootpublication.ParticipantProjection {
	return nil
}
func (*publicSemanticCleanupState) PrepareBuild(*backenddb.RootPublicationBuildGroup, *backenddb.Snapshot, *backenddb.StableResourceCaptureLease) error {
	return nil
}
func (*publicSemanticCleanupState) PrepareHandoff(latest backenddb.NativeSemanticState, _ *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	latest.Retain()
	return latest, nil
}
func (s *publicSemanticCleanupState) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	s.calls.Add(1)
	s.before()
	close(s.entered)
	<-s.resume
	if err := s.file.Close(); err != nil {
		s.failure = err
		return rootpublication.SemanticCleanupCompletion{}, err
	}
	s.file = nil
	if s.directory != "" {
		if err := os.Remove(s.directory); err != nil {
			s.failure = err
			return rootpublication.SemanticCleanupCompletion{}, err
		}
	}
	s.directory = ""
	s.before = nil
	s.entered = nil
	s.resume = nil
	receipt := s.lease
	s.lease = nil
	return rootpublication.SemanticCleanupCompletion{Refund: receipt}, nil
}

func TestCOWPublicSemanticDestructorAndNotificationCloseGuard(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "original-unlink-failure"
		}
		t.Run(name, func(t *testing.T) {
			var database *DB
			notified := make(chan error, 1)
			database, _, blocked := cowPublicContractOpen(t, ProfileNoWALFast, func(opts *Options) {
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				opts.DisableBackgroundPrune = true
				opts.NotifyError = func(error) { notified <- database.Close() }
			})
			cache, backend := database.cached, database.backend
			capture, err := database.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			defer capture.Close()
			receipt, err := capture.AdmitNativeBytes(uint64(unsafe.Sizeof(publicSemanticCleanupState{})) + 16<<10)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.Create(filepath.Join(t.TempDir(), "original.file"))
			if err != nil {
				t.Fatal(err)
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			state := &publicSemanticCleanupState{lease: receipt, file: file, entered: entered, resume: resume}
			if fail {
				state.directory = filepath.Join(t.TempDir(), "original-nonempty")
				if err := os.Mkdir(state.directory, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(state.directory, "original-child"), []byte("held"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var destructorPublic, destructorCache, destructorBackend error
			state.before = func() {
				destructorPublic = database.Close()
				destructorCache = cache.Close()
				destructorBackend = backend.Close()
			}
			state.refs.Store(1)
			state.cleanup.Work = state
			b := database.NewBatch()
			if err := database.SetNativeSemanticPublication(b, &backenddb.NativePreparedPublication{Basis: capture.State(), State: state, StartSequence: capture.LogicalSequence()}); err != nil {
				t.Fatal(err)
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			if err := capture.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				blocked.Store(true)
				t.Fatal("actual worker did not enter original destructor")
			}
			if destructorPublic == nil || destructorCache != destructorPublic || destructorBackend != destructorPublic || database.cached != cache || database.backend != backend {
				t.Fatal("same-caller Close bypassed original guard or had side effects")
			}
			done := make(chan error, 1)
			go func() { done <- database.Close() }()
			select {
			case err := <-done:
				t.Fatalf("unrelated Close did not await original worker: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			close(resume)
			var closeErr error
			select {
			case closeErr = <-done:
			case <-time.After(10 * time.Second):
				blocked.Store(true)
				t.Fatal("unrelated Close failed to progress after destructor release")
			}
			if state.calls.Load() != 1 {
				t.Fatal("original destructor repeated")
			}
			if fail {
				if state.failure == nil || !errors.Is(closeErr, state.failure) || state.lease == nil || state.cleanup.Work != state || state.cleanup.Err != state.failure || state.directory == "" {
					t.Fatalf("original failed unlink/charge lost: close=%v failure=%v", closeErr, state.failure)
				}
				select {
				case err := <-notified:
					if err != destructorPublic {
						t.Fatalf("NotifyError Close bypassed guard: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("original failure notification missing")
				}
			} else if closeErr != nil || state.lease != nil || state.file != nil || state.cleanup.Work != nil {
				t.Fatalf("successful original cleanup incomplete: %v", closeErr)
			}
		})
	}
}
