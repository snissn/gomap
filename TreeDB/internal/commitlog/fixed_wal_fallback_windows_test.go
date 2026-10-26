//go:build windows

package commitlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestWindowsCommandWALFreshStreamDurableRotationAndReopen(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	initial := j.path
	for i := 0; i < 12; i++ {
		if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
			t.Fatal(err)
		}
		if err = j.Sync(); err != nil {
			t.Fatal(err)
		}
		if j.writer.fixed != nil || j.writer.fixedBudget != 0 {
			t.Fatal("new Windows segment selected fixed container")
		}
	}
	if j.path == initial {
		t.Fatal("stream fallback did not rotate")
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.NextLSN() != 13 {
		t.Fatalf("recovered next LSN=%d", j.NextLSN())
	}
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsFixedWALInitializerFailsBeforeMutation(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "canonical")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	original := f
	if _, err = initializeFixedWAL(&f, 256, nil); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("initializer=%v", err)
	}
	if f != original {
		t.Fatal("unsupported initializer changed custody")
	}
	info, err := f.Stat()
	if err != nil || info.Size() != 0 {
		t.Fatalf("unsupported initializer mutated/closed canonical: %v", err)
	}
}

func TestWindowsAcceptedFixedWALRemainsReadableAndRotatesToStream(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, CommandSegmentName(0, 1))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Exact fixture for an already accepted V3 file from a supported platform.
	if _, err = writeInitializedFixedWAL(f, 8192); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	j, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	if j.writer.fixed == nil {
		t.Fatal("existing V3 container was not loaded")
	}
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.NextLSN() != 2 {
		t.Fatalf("existing V3 next LSN=%d", j.NextLSN())
	}
	if _, err = j.AppendCommand(fixedWALLifecycleFrame(t, 32<<10)); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	if j.writer.fixed != nil {
		t.Fatal("successor did not use stream fallback")
	}
	image, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(image[:8], fixedWALMagic[:]) {
		t.Fatal("existing V3 header changed")
	}
	frames, err := ScanCommandFramesV2(path, Options{})
	if err != nil || len(frames) != 1 || frames[0].LSN != 1 {
		t.Fatalf("existing V3 frames=%v err=%v", frames, err)
	}
	image[fixedWALHeaderSize+fixedWALWitnessSize+segmentHeaderSize+20] ^= 1
	corrupt := filepath.Join(t.TempDir(), CommandSegmentName(0, 1))
	if err = os.WriteFile(corrupt, image, 0600); err != nil {
		t.Fatal(err)
	}
	if reader, readErr := NewReader(corrupt); !errors.Is(readErr, ErrCorrupt) {
		if reader != nil {
			_ = reader.Close()
		}
		t.Fatalf("accepted V3 corruption=%v", readErr)
	}
}
