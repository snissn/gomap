package db

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOriginalDATAOnlySnapshotLateManagerFailureRetainedByDB(t *testing.T) {
	for _, activeRead := range []bool{false, true} {
		name := "held-snapshot"
		if activeRead {
			name = "held-active-read"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
			if err != nil {
				t.Fatal(err)
			}
			id := page.ValueLogFileID(1)
			w, err := valuelog.NewWriterWithStableResourcePinRegistry(valuelog.SegmentPath(ValueLogDirPath(dir), id), id, d.valueLogIdentityPins)
			if err != nil {
				t.Fatal(err)
			}
			ptr, err := w.Append(0, nil, 1, bytes.Repeat([]byte("v"), 4096))
			if err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			if err = d.RefreshValueLogSet(); err != nil {
				t.Fatal(err)
			}
			setValueLogPointer(t, d, []byte("original-key"), ptr)
			s := d.AcquireSnapshot()
			if s == nil || s.vlogManager == nil || s.state.ValueLogSet == nil {
				t.Fatal("fixture did not capture original DATA-only value-log snapshot")
			}
			vm := s.vlogManager
			generation := s.idx
			birthCharge := generation.metadataCharge
			originalRegistry := generation.registry
			var file *valuelog.File
			for _, f := range s.state.ValueLogSet.Files {
				file = f
				break
			}
			if file == nil {
				t.Fatal("fixture has no original physical value-log file")
			}
			registry := d.valueLogIdentityPins
			if activeRead {
				if err = s.beginRead(); err != nil {
					t.Fatal(err)
				}
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			if generation.metadataCharge != birthCharge || generation.registry == nil || generation.wrapperBorrowCountV6() != 1 {
				t.Fatal("terminal close refunded/cleared original held generation wrapper")
			}
			if d.valueLogManager != nil {
				t.Fatal("terminal DB did not withdraw Manager discovery")
			}
			prior := errors.New("preexisting unrelated background failure")
			d.bgErrMu.Lock()
			d.bgErr = prior
			d.bgErrMu.Unlock()
			if err = file.File.Close(); err != nil {
				t.Fatal(err)
			}
			err = s.Close()
			if activeRead {
				if err != nil {
					t.Fatal(err)
				}
				s.endRead()
				s.iteratorMu.Lock()
				err = s.cleanupErr
				s.iteratorMu.Unlock()
			}
			var receipt *valuelog.ManagerCleanupFailure
			if !errors.Is(err, os.ErrClosed) || !errors.As(err, &receipt) {
				t.Fatalf("lost actual original last-release error: %v", err)
			}
			retained := d.backgroundError()
			var original *valuelog.ManagerCleanupFailure
			if !errors.As(retained, &original) || original != receipt || !errors.Is(retained, prior) || !errors.Is(retained, os.ErrClosed) {
				t.Fatalf("DB lost original late Manager receipt or sibling: %v", retained)
			}
			if generation.wrapperBorrowCountV6() != 1 || generation.failedSnapshots != s || generation.metadataCharge != birthCharge || generation.registry != originalRegistry {
				t.Fatal("failed finalization lost original generation/Snapshot borrow")
			}
			charge := registry.MetadataOwner().Bytes()
			if charge == 0 {
				t.Fatal("actual failed payload charge disappeared")
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			var repeated *valuelog.ManagerCleanupFailure
			if !errors.As(vm.Close(), &repeated) || repeated != receipt || registry.MetadataOwner().Bytes() != charge {
				t.Fatal("repeated terminal observation changed receipt or refunded failed payload")
			}
		})
	}
}

func TestOriginalGenerationWrapperCloseRacesLastSnapshot(t *testing.T) {
	for i := 0; i < 16; i++ {
		d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		g := d.idx.Load()
		g.wrapperMu.Lock()
		originalBorrows := g.wrapperBorrowCountV6()
		g.wrapperMu.Unlock()
		s := d.AcquireSnapshot()
		if s == nil {
			t.Fatal("missing actual original Snapshot")
		}
		if s.idx != g || g.wrapperBorrowCountV6() != originalBorrows+1 || g.metadataCharge == 0 {
			t.Fatal("original wrapper was not retained/admitted")
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make(chan error, 2)
		wg.Add(2)
		go func() { defer wg.Done(); <-start; results <- d.Close() }()
		go func() { defer wg.Done(); <-start; results <- s.Close() }()
		close(start)
		wg.Wait()
		close(results)
		for err := range results {
			if err != nil {
				t.Fatal(err)
			}
		}
		if g.wrapperBorrowCountV6() != 0 || g.metadataCharge != 0 || g.registry != nil || g.pager != nil {
			t.Fatal("successful original final wrapper completion did not scrub/refund")
		}
	}
}

// A published physical failure remains the same original generation receipt
// while its held Snapshot reports a later physical completion failure.

func TestOriginalGenerationPublishedFailureConcurrentInspection(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	id := page.ValueLogFileID(1)
	w, err := valuelog.NewWriterWithStableResourcePinRegistry(valuelog.SegmentPath(ValueLogDirPath(dir), id), id, d.valueLogIdentityPins)
	if err != nil {
		t.Fatal(err)
	}
	ptr, err := w.Append(0, nil, 1, bytes.Repeat([]byte("v"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	setValueLogPointer(t, d, []byte("original-key"), ptr)
	s := d.AcquireSnapshot()
	if s == nil {
		t.Fatal("missing actual DATA-only Snapshot")
	}
	g := s.idx
	var file *valuelog.File
	for _, f := range s.state.ValueLogSet.Files {
		file = f
		break
	}
	if file == nil {
		t.Fatal("missing original value-log physical file")
	}
	if err = g.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err == nil {
		t.Fatal("actual DATA final Close refusal missing")
	}
	published := d.backgroundError()
	var receipt *originalDBCleanupFailure
	if !errors.As(published, &receipt) || receipt != &g.closeFailure {
		t.Fatalf("lost same original generation: %v", published)
	}
	if err = file.File.Close(); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var snapshotErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			_ = errors.Is(published, os.ErrClosed)
			var found *originalDBCleanupFailure
			_ = errors.As(published, &found)
			var late *valuelog.ManagerCleanupFailure
			_ = errors.As(published, &late)
		}
	}()
	go func() { defer wg.Done(); <-start; snapshotErr = s.Close() }()
	close(start)
	wg.Wait()
	var late *valuelog.ManagerCleanupFailure
	if !errors.As(snapshotErr, &late) || !errors.As(d.backgroundError(), &late) || g.failedSnapshots != s || g.wrapperBorrowCountV6() != 1 {
		t.Fatalf("late physical completion lost from published original receipt: %v", snapshotErr)
	}
	missing := errors.New("unrelated missing cause")
	if errors.Is(published, missing) || errors.Is(d.backgroundError(), missing) {
		t.Fatal("original error graph fabricated an unrelated cause")
	}
}
