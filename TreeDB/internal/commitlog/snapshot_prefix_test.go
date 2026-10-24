package commitlog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandWALSnapshotPrefixConcurrentTail(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(j.path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(j.path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	cut, err := CaptureCommandWALSnapshotPrefix(file)
	if err != nil {
		t.Fatal(err)
	}
	output := &snapshotBlockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(output.release) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() { done <- cut.WriteToContext(context.Background(), output) }()
	<-output.entered
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), before) {
		t.Fatal("concurrent tail entered snapshot")
	}
	// Export never clears source reserve: the live tail remains valid and
	// ordinary journal reopen retains both accepted commands.
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCommandJournal(j.walDir, CommandJournalOptions{SegmentTargetBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.NextLSN() != 3 {
		t.Fatalf("source frontier=%d", reopened.NextLSN())
	}
}

type snapshotBlockedWriter struct {
	bytes.Buffer
	entered, release chan struct{}
	once             sync.Once
}

func (w *snapshotBlockedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return w.Buffer.Write(p)
}

func TestCommandWALSnapshotPrefixSurvivesTailRotationAndClose(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "empty_resize"}[empty], func(t *testing.T) {
			j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 1024})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			if !empty {
				if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
					t.Fatal(err)
				}
				if err = j.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			parent, err := rootpublication.OpenStableParent(filepath.Dir(j.path))
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			file, err := rootpublication.OpenStableChildFile(parent, filepath.Base(j.path), os.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			original, err := os.ReadFile(j.path)
			if err != nil {
				t.Fatal(err)
			}
			cut, err := CaptureCommandWALSnapshotPrefix(file)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = j.AppendCommand(fixedWALLifecycleFrame(t, 32<<10)); err != nil {
				t.Fatal(err)
			}
			if err = j.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			// The original name can be replaced after capture; export still uses
			// the pinned object. Keep the replacement untouched as well.
			if err = os.Rename(file.Name(), file.Name()+".retained"); err != nil {
				t.Fatal(err)
			}
			foreign := []byte("foreign pathname replacement")
			if err = os.WriteFile(file.Name(), foreign, 0600); err != nil {
				t.Fatal(err)
			}
			var exported bytes.Buffer
			if err = cut.WriteToContext(context.Background(), &exported); err != nil {
				t.Fatal(err)
			}
			if cut.SizeBytes() != int64(len(original)) || !bytes.Equal(exported.Bytes(), original) {
				t.Fatalf("captured image changed: size=%d want=%d", exported.Len(), len(original))
			}
			got, err := os.ReadFile(file.Name())
			if err != nil || !bytes.Equal(got, foreign) {
				t.Fatalf("replacement changed: %v", err)
			}
			path := filepath.Join(t.TempDir(), CommandSegmentName(0, 1))
			if err = os.WriteFile(path, exported.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			frames, err := ScanCommandFramesV2(path, Options{})
			want := 1
			if empty {
				want = 0
			}
			if err != nil || len(frames) != want {
				t.Fatalf("frames=%d want=%d err=%v", len(frames), want, err)
			}
		})
	}
}

func TestCommandWALSnapshotPrefixFreezesLegacyEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), CommandSegmentName(0, 1))
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	a := fixedWALTestBarrier()
	a.Version = CommandFrameVersionV2
	a.LSN = 1
	if err = w.AppendCommand(a); err != nil {
		t.Fatal(err)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	cut, err := CaptureCommandWALSnapshotPrefix(file)
	if err != nil {
		t.Fatal(err)
	}
	a.LSN = 2
	if err = w.AppendCommand(a); err != nil {
		t.Fatal(err)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err = cut.WriteToContext(context.Background(), &output); err != nil || !bytes.Equal(output.Bytes(), before) {
		t.Fatalf("legacy export: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = cut.WriteToContext(ctx, &output); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	if err = cut.WriteToContext(context.Background(), snapshotShortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write=%v", err)
	}
}

type snapshotShortWriter struct{}

func (snapshotShortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestCommandWALSnapshotPrefixRejectsCorruptReserve(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("requires a fixed-container reserve")
	}
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err = j.writer.f.WriteAt([]byte{1}, j.writer.fixed.capacity-1); err != nil {
		t.Fatal(err)
	}
	if _, err = CaptureCommandWALSnapshotPrefix(j.writer.f); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("capture corrupt reserve=%v", err)
	}
}

func TestCommandWALSnapshotPrefixCannotDiscardIncompleteCommand(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("requires a fixed-container reserve")
	}
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	frame := fixedWALLifecycleFrame(t, 128)
	frame.DurabilityClass = CommandDurabilityRelaxed
	if _, err = j.AppendCommand(frame); err != nil {
		t.Fatal(err)
	}
	if err = j.Flush(); err != nil {
		t.Fatal(err)
	}
	state, _, err := loadFixedWAL(j.writer.f)
	if err != nil {
		t.Fatal(err)
	}
	extent := state.extents[len(state.extents)-1]
	image, err := os.ReadFile(j.path)
	if err != nil {
		t.Fatal(err)
	}
	clear(image[extent.physical+fixedWALWitnessSize+8+56 : extent.physical+fixedWALWitnessSize+extent.length])
	clear(image[extent.end-fixedWALWitnessSize : extent.end])
	if _, err = j.writer.f.WriteAt(image, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = CaptureCommandWALSnapshotPrefix(j.writer.f); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("incomplete capture=%v", err)
	}
	after, err := os.ReadFile(j.path)
	if err != nil || !bytes.Equal(after, image) {
		t.Fatalf("capture changed incomplete source: %v", err)
	}
}
