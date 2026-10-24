package commitlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestFixedWALPrivateInitializationStableWitness(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container publication unsupported")
	}
	dir := t.TempDir()
	name := CommandSegmentName(0, 1)
	if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	var synced, published *powerlossoracle.Model
	afterFileSync := false
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterDependencyFileSync && strings.HasPrefix(filepath.Base(event.Path), ".wal-initialize-") {
			if event.StablePath == nil {
				t.Error("private sync lacks retained file")
			}
			afterFileSync = true
			synced = model.Clone()
		}
		if event.Namespace == durabilitycut.NamespaceRename && !afterFileSync {
			t.Error("published before successful private file sync")
		}
		if event.Point == durabilitycut.AfterNewFileDirectorySync && afterFileSync && published == nil {
			if event.StablePath == nil {
				t.Error("private publication namespace sync lacks retained parent")
			}
			published = model.Clone()
		}
		return nil
	})
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 4096})
	restore()
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if synced == nil || published == nil {
		t.Fatal("missing private sync/publication witnesses")
	}
	beforeDir, afterDir := t.TempDir(), t.TempDir()
	if err = synced.MaterializeStable(beforeDir); err != nil {
		t.Fatal(err)
	}
	if err = published.MaterializeStable(afterDir); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(filepath.Join(beforeDir, name))
	if err != nil || len(old) != 0 {
		t.Fatalf("pre-publication canonical bytes=%d error=%v", len(old), err)
	}
	got, err := os.ReadFile(filepath.Join(afterDir, name))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) || len(got) < fixedWALHeaderSize {
		t.Fatal("successful namespace sync did not preserve exact initialized container")
	}
}

func TestFixedWALFailedPrivateSyncHasNoStableWitness(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container publication unsupported")
	}
	injected := errors.New("private physical sync failed")
	original := fixedWALInitializeSync
	fixedWALInitializeSync = func(*os.File) error { return injected }
	defer func() { fixedWALInitializeSync = original }()
	after := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterDependencyFileSync {
			after++
		}
		return nil
	})
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 4096})
	restore()
	if journal != nil {
		_ = journal.Close()
	}
	if !errors.Is(err, injected) || after != 0 {
		t.Fatalf("error=%v successful sync witnesses=%d", err, after)
	}
}

func TestCapturedSyncObservesExactRetainedFile(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	lsn, err := journal.AppendCommand(fixedWALTestBarrier())
	if err != nil {
		t.Fatal(err)
	}
	capture, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	original, err := journal.writer.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err = journal.RotateActiveSegment(false); err != nil {
		t.Fatal(err)
	}
	cuts := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.BeforeDependencyFileSync && event.Point != durabilitycut.AfterDependencyFileSync {
			return nil
		}
		cuts++
		if event.StablePath == nil {
			t.Error("captured sync lacks retained handle")
			return nil
		}
		got, err := event.StablePath.Stat()
		if err != nil || !os.SameFile(original, got) || event.LSN != lsn {
			t.Errorf("sync identity/frontier changed: error=%v LSN=%d/%d", err, event.LSN, lsn)
		}
		return nil
	})
	err = capture.Sync()
	restore()
	if err != nil || cuts != 2 {
		t.Fatalf("sync error=%v cuts=%d", err, cuts)
	}
}

func TestCapturedSyncRotatedWitnessPerformsFreshPhysicalSync(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err = journal.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	rotation, err := journal.RotateActiveSegmentWithStableResources(true)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	// The original closed token is already producer-certified ContentSynced.
	capture, err := journal.CaptureSync([]*rootpublication.StableResourceToken{rotation.Closed})
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	token := capture.rotated[0]
	// Even an earlier successful sync of the private capture token cannot turn
	// the subsequent observed SyncThrough into a cached certification.
	if err = token.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if err = capture.Sync(); err != nil {
		t.Fatal(err)
	}
	// Transfer this test-owned closed pin into a set solely to inspect its public
	// physical-I/O counters. The capture still releases its active pin and lease.
	capture.rotated = nil
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityCommandWALRotated)
	if err = builder.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	set, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	stats := set.Stats(time.Now())
	if len(stats) != 1 || stats[0].PhysicalFileSyncs != 2 {
		t.Fatalf("fresh rotated physical syncs=%+v", stats)
	}
}
