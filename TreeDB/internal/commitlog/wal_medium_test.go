package commitlog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"testing"
)

// Fixed container proofs must not disappear when production selects a stream
// on a memory filesystem. This is instance-local explicit low-level creation.
func openFixedTestCommandJournal(dir string, opts CommandJournalOptions) (*CommandJournal, error) {
	return openCommandJournal(dir, opts, false)
}

func TestWALMediumPolicyClassification(t *testing.T) {
	for _, tc := range []struct {
		name             string
		kind             uint64
		err              error
		supported, fixed bool
		reason           string
	}{
		{"tmpfs", 0x01021994, nil, true, false, "memory_filesystem"},
		{"ramfs", 0x858458f6, nil, true, false, "memory_filesystem"},
		{"ext4", 0xef53, nil, true, true, "persistent_or_unknown_filesystem"},
		{"overlay", 0x794c7630, nil, true, true, "persistent_or_unknown_filesystem"},
		{"unknown", 0, nil, true, true, "persistent_or_unknown_filesystem"},
		{"probe_error", 0, os.ErrPermission, true, true, "filesystem_probe_error"},
		{"unsupported", 0x01021994, nil, false, false, "platform_stream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := productionWALContainer(1024, tc.supported, tc.kind, tc.err)
			if got.fixed != tc.fixed || got.reason != tc.reason {
				t.Fatalf("selection=%+v", got)
			}
		})
	}
}

func TestWALMediumProductionSelectionAndBothRotations(t *testing.T) {
	for _, base := range []string{t.TempDir(), "/dev/shm"} {
		t.Run(filepath.Base(base), func(t *testing.T) {
			dir, err := os.MkdirTemp(base, "treedb-wal-medium-*")
			if err != nil {
				if base == "/dev/shm" {
					t.Skipf("memory filesystem unavailable: %v", err)
				}
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			for _, stable := range []bool{false, true} {
				t.Run(map[bool]string{false: "ordinary", true: "stable"}[stable], func(t *testing.T) {
					path := filepath.Join(dir, map[bool]string{false: "ordinary", true: "stable"}[stable])
					j, err := OpenCommandJournal(path, CommandJournalOptions{CaptureStableResources: stable, SegmentTargetBytes: 1024})
					if err != nil {
						t.Fatal(err)
					}
					defer j.Close()
					probe, err := walFilesystemType(j.writer.f)
					want := FixedWALCreationSupported() && (err != nil || !walMemoryFilesystem(probe))
					if (j.writer.fixed != nil) != want || j.writer.containerSelection.filesystemType != probe {
						t.Fatalf("initial=%+v fixed=%t filesystem=%x", j.writer.containerSelection, j.writer.fixed != nil, probe)
					}
					if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
						t.Fatal(err)
					}
					target, err := j.CaptureSync(nil)
					if err != nil {
						t.Fatal(err)
					}
					defer target.Release()
					if stable {
						rotation, e := j.RotateActiveSegmentWithStableResources(false)
						if e != nil {
							t.Fatal(e)
						}
						rotation.Release()
					} else if err = j.RotateActiveSegment(false); err != nil {
						t.Fatal(err)
					}
					if (j.writer.fixed != nil) != want || target.active.dataOnlySync != want {
						t.Fatal("rotation changed selected/captured format")
					}
					if err = target.Sync(); err != nil {
						t.Fatal(err)
					}
					target.Release()
					if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
						t.Fatal(err)
					}
					if err = j.Sync(); err != nil {
						t.Fatal(err)
					}
					before := j.WriterBufferStats().Container
					if err = j.Close(); err != nil {
						t.Fatal(err)
					}
					reopened, e := OpenCommandJournal(path, CommandJournalOptions{CaptureStableResources: stable})
					if e != nil {
						t.Fatal(e)
					}
					defer reopened.Close()
					if reopened.WriterBufferStats().Container != before || reopened.writer.containerSelection.reason != "existing_format" || reopened.NextLSN() != 3 {
						t.Fatalf("reopen format=%+v next=%d", reopened.WriterBufferStats(), reopened.NextLSN())
					}
				})
			}
		})
	}
}

func TestWALMediumExplicitFixedAndExistingStreamPrecedence(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed publication unsupported")
	}
	dir, err := os.MkdirTemp("/dev/shm", "treedb-wal-existing-*")
	if err != nil {
		t.Skipf("memory filesystem unavailable: %v", err)
	}
	defer os.RemoveAll(dir)
	for _, fixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "stream", true: "fixed"}[fixed], func(t *testing.T) {
			wal := filepath.Join(dir, map[bool]string{false: "stream", true: "fixed"}[fixed])
			if err = os.MkdirAll(wal, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(wal, CommandSegmentName(0, 1))
			var w *Writer
			if fixed {
				w, err = NewFixedWriter(path, 1024)
			} else {
				w, err = NewWriter(path)
			}
			if err != nil {
				t.Fatal(err)
			}
			probe, probeErr := walFilesystemType(w.f)
			if probeErr != nil || !walMemoryFilesystem(probe) {
				w.Close()
				t.Skipf("opened file is not a positively recognized memory filesystem: type=%x err=%v", probe, probeErr)
			}
			env := fixedWALTestBarrier()
			env.LSN = 1
			if err = w.AppendCommandV2(env); err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			// Even an explicit fixed request cannot convert an occupied stream.
			w, err = NewFixedWriter(path, 1024)
			if err != nil {
				t.Fatal(err)
			}
			if (w.fixed != nil) != fixed {
				t.Fatal("explicit request converted occupied format")
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			j, e := OpenCommandJournal(wal, CommandJournalOptions{CaptureStableResources: true})
			if e != nil {
				t.Fatal(e)
			}
			defer j.Close()
			if (j.writer.fixed != nil) != fixed || j.writer.containerSelection.reason != "existing_format" {
				t.Fatal("production reclassified existing file")
			}
			capture, e := j.CaptureSync(nil)
			if e != nil {
				t.Fatal(e)
			}
			defer capture.Release()
			rotation, e := j.RotateActiveSegmentWithStableResources(false)
			if e != nil {
				t.Fatal(e)
			}
			rotation.Release()
			if j.writer.fixed != nil || j.writer.containerSelection.reason != "memory_filesystem" || capture.active.dataOnlySync != fixed {
				t.Fatal("fresh memory successor or old capture chose wrong format")
			}
			if e = capture.Sync(); e != nil {
				t.Fatal(e)
			}
			capture.Release()
		})
	}
}

func TestWALMediumStreamSyncFailureDoesNotCertifyPrefix(t *testing.T) {
	dir, err := os.MkdirTemp("/dev/shm", "treedb-wal-sync-*")
	if err != nil {
		t.Skipf("memory filesystem unavailable: %v", err)
	}
	defer os.RemoveAll(dir)
	j, err := OpenCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	probe, probeErr := walFilesystemType(j.writer.f)
	if probeErr != nil || !walMemoryFilesystem(probe) {
		t.Skipf("opened file is not a positively recognized memory filesystem: type=%x err=%v", probe, probeErr)
	}
	if j.writer.fixed != nil {
		t.Fatal("fresh positive memory filesystem did not choose stream")
	}
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	target, err := j.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Release()
	// Break only the exact pinned sync descriptor, after capture/flush succeeds.
	if err = target.active.token.WithPinnedFile(func(f *os.File) error { return f.Close() }); err != nil {
		t.Fatal(err)
	}
	after := 0
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.AfterDependencyFileSync {
			after++
		}
		return nil
	})
	defer restore()
	before := j.WriterDurabilityStats()
	if err = target.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("sync error=%v", err)
	}
	if after != 0 || j.WriterDurabilityStats().FileSyncErrors != before.FileSyncErrors+1 {
		t.Fatal("failed physical stream sync emitted success")
	}
}

func TestNewFixedWriterRejectsInvalidCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	if _, err := NewFixedWriter(path, 0); err == nil {
		t.Fatal("accepted zero capacity")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid request touched destination: %v", err)
	}
	if !FixedWALCreationSupported() {
		if _, err := NewFixedWriter(path, 1024); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
			t.Fatalf("unsupported error=%v", err)
		}
	}
}

func TestNewFixedWriterRejectsOverflowBeforeTouchingDestination(t *testing.T) {
	dir := t.TempDir()
	for _, existing := range []bool{false, true} {
		path := filepath.Join(dir, map[bool]string{false: "absent", true: "existing"}[existing])
		original := []byte("existing stream remains untouched")
		if existing {
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := NewFixedWriter(path, int64(1<<63-1)); !errors.Is(err, ErrRecordTooLarge) {
			t.Fatalf("overflow error=%v", err)
		}
		data, err := os.ReadFile(path)
		if existing {
			if err != nil || string(data) != string(original) {
				t.Fatalf("changed existing destination: %q %v", data, err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("created invalid destination: %v", err)
		}
	}
	path := filepath.Join(dir, "initializer")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = writeInitializedFixedWAL(f, int64(1<<63-1)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("initializer error=%v", err)
	}
	if st, err := f.Stat(); err != nil || st.Size() != 0 {
		t.Fatalf("initializer mutated file: %v %v", st, err)
	}
	const largestTarget = (int64(1<<63-1) - fixedWALHeaderSize - 1024 - (fixedWALAlignment - 1)) / 4
	if capacity, err := fixedWALCapacity(largestTarget); err != nil || capacity < fixedWALHeaderSize {
		t.Fatalf("valid boundary capacity=%d err=%v", capacity, err)
	}
	if _, err := fixedWALCapacity(largestTarget + 1); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("overflow boundary error=%v", err)
	}
}
