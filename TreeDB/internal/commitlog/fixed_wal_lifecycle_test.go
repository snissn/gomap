package commitlog

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestFixedWALUnacceptedSuccessorNeutralization(t *testing.T) {
	for _, failSync := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "sync_failure"}[failSync], func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "unaccepted")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			// An actual partly initialized header/reserve, never an accepted file.
			if _, err = file.Write([]byte("TDBWAL\x03\x00partial reserve")); err != nil {
				t.Fatal(err)
			}
			oldSync := fixedWALUnacceptedSync
			defer func() { fixedWALUnacceptedSync = oldSync }()
			synced := false
			fixedWALUnacceptedSync = func(f *os.File) error {
				if f != file {
					t.Fatal("sync used another handle")
				}
				info, e := f.Stat()
				if e != nil || info.Size() != 0 {
					t.Fatalf("sync before canonical empty: %v", e)
				}
				if e = oldSync(f); e != nil {
					return e
				}
				synced = true
				if failSync {
					return syscall.EIO
				}
				return nil
			}
			err = neutralizeUnacceptedFixedSuccessor(file)
			if !synced || (failSync && !errors.Is(err, syscall.EIO)) || (!failSync && err != nil) {
				t.Fatalf("synced=%v err=%v", synced, err)
			}
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
			frames, err := ScanCommandFramesV2(file.Name(), Options{})
			if err != nil || len(frames) != 0 {
				t.Fatalf("neutralized file: frames=%d err=%v", len(frames), err)
			}
			if err = neutralizeUnacceptedFixedSuccessor(file); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("closed-handle truncate=%v", err)
			}
		})
	}
}

func TestFixedWALDiscardRejectsForeignSuccessor(t *testing.T) {
	parent, err := rootpublication.OpenStableParent(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	file, err := os.CreateTemp(t.TempDir(), "foreign")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	before := []byte("unowned corrupt candidate")
	if _, err = file.Write(before); err != nil {
		t.Fatal(err)
	}
	if err = discardUnacceptedFixedSuccessor(parent, file); err == nil {
		t.Fatal("discard accepted foreign identity")
	}
	after, err := os.ReadFile(file.Name())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("foreign file changed: %v", err)
	}
}

func TestFixedWALWindowsNeutralizationFailureStopsWriter(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows child-only creation persistence")
	}
	for _, stable := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "stable"}[stable], func(t *testing.T) {
			j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 1024, CaptureStableResources: stable})
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
			accepted, err := os.ReadFile(j.path)
			if err != nil {
				t.Fatal(err)
			}
			oldPath := j.path
			before := j.NextLSN()
			oldWrite, oldSync := fixedWALInitializeWrite, fixedWALUnacceptedSync
			defer func() { fixedWALInitializeWrite, fixedWALUnacceptedSync = oldWrite, oldSync }()
			var candidate *os.File
			writes, syncs := 0, 0
			fixedWALInitializeWrite = func(w io.WriterAt, p []byte, off int64) error {
				writes++
				candidate = w.(*os.File)
				if writes == 2 {
					if _, e := w.WriteAt(p[:97], off); e != nil {
						return e
					}
					return io.ErrShortWrite
				}
				return oldWrite(w, p, off)
			}
			fixedWALUnacceptedSync = func(f *os.File) error {
				syncs++
				if e := oldSync(f); e != nil {
					return e
				}
				return syscall.EIO
			}
			_, err = j.AppendCommand(fixedWALLifecycleFrame(t, 32<<10))
			fixedWALInitializeWrite, fixedWALUnacceptedSync = oldWrite, oldSync
			if !errors.Is(err, io.ErrShortWrite) || !errors.Is(err, syscall.EIO) || syncs != 1 {
				t.Fatalf("neutralization custody: syncs=%d err=%v", syncs, err)
			}
			if _, err = j.AppendCommand(fixedWALTestBarrier()); err == nil {
				t.Fatal("append ACK after neutralization error")
			}
			if err = j.Sync(); err == nil {
				t.Fatal("sync ACK after neutralization error")
			}
			if j.NextLSN() != before {
				t.Fatal("failed successor consumed LSN")
			}
			if candidate == nil {
				t.Fatal("no modified successor")
			}
			if _, err = candidate.ReadAt(make([]byte, 1), 0); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("candidate open: %v", err)
			}
			after, err := os.ReadFile(oldPath)
			if err != nil || !bytes.Equal(accepted, after) {
				t.Fatalf("accepted bytes changed: %v", err)
			}
			_ = j.Close() // The originating initialization error remains sticky.
			// The injected sync completed physically before returning an error.
			// Recovery is possible, but the failed writer must stay poisoned.
			reopened, err := OpenCommandJournal(j.walDir, CommandJournalOptions{SegmentTargetBytes: 1024})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.NextLSN() != before {
				t.Fatal("prior ACK frontier lost")
			}
		})
	}
}

func fixedWALLifecycleFrame(t *testing.T, size int) CommandEnvelope {
	t.Helper()
	payload, err := EncodeRawKVBatchPayload([]RawKVOperation{{Op: RawKVOpSet, Key: []byte("key"), Value: bytes.Repeat([]byte("v"), size)}})
	if err != nil {
		t.Fatal(err)
	}
	return CommandEnvelope{Version: CommandFrameVersionV2, DurabilityClass: CommandDurabilityDurable, Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1, Payload: payload}
}
func fixedWALDrainRotations(t *testing.T, j *CommandJournal) {
	t.Helper()
	rotations, err := j.TakePendingStableRotations()
	if err != nil {
		t.Fatal(err)
	}
	for _, rotation := range rotations {
		if rotation.Closed == nil || rotation.Active == nil {
			t.Fatal("rotation lost exact resource tokens")
		}
		active := rotation.Active
		if active.Generation() != j.segmentSeq || active.Frontier().Bytes != uint64(j.writer.StableBytes()) || active.Frontier().MaxLSN != j.activeSegmentMaxLSN {
			t.Fatalf("active resource frontier=%+v generation=%d", active.Frontier(), active.Generation())
		}
		if active.Namespace() == nil || active.Identity() == rotation.Closed.Identity() {
			t.Fatal("rotation lost successor namespace or exact identity")
		}
		if err := active.SyncThrough(); err != nil {
			t.Fatal(err)
		}
		rotation.Release()
	}
}
func TestFixedWALOversizedChosenSuccessor(t *testing.T) {
	for _, stable := range []bool{false, true} {
		for _, firstLarge := range []bool{false, true} {
			name := "ordinary"
			if stable {
				name = "stable"
			}
			if firstLarge {
				name += "/first_large"
			} else {
				name += "/accepted_small_first"
			}
			t.Run(name, func(t *testing.T) {
				j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 1024, CaptureStableResources: stable})
				if err != nil {
					t.Fatal(err)
				}
				defer j.Close()
				var acceptedPath string
				var accepted []byte
				sizes := []int{128, 32 << 10, 128, 8 << 20}
				if firstLarge {
					sizes = []int{32 << 10, 128, 8 << 20}
				}
				for i, size := range sizes {
					if _, err = j.AppendCommand(fixedWALLifecycleFrame(t, size)); err != nil {
						t.Fatalf("append %d size=%d: %v", i, size, err)
					}
					if err = j.Sync(); err != nil {
						t.Fatalf("sync %d size=%d: %v", i, size, err)
					}
					fixedWALDrainRotations(t, j)
					if i == 0 {
						acceptedPath = j.path
						accepted, err = os.ReadFile(acceptedPath)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						now, e := os.ReadFile(acceptedPath)
						if e != nil || !bytes.Equal(accepted, now) {
							t.Fatalf("accepted predecessor changed err=%v", e)
						}
					}
				}
				segments, err := commandJournalSegments(j.walDir, j.path)
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				for _, segment := range segments {
					frames, e := ScanCommandFramesV2(segment.path, Options{})
					if e != nil {
						t.Fatal(e)
					}
					for _, frame := range frames {
						count++
						if frame.LSN != uint64(count) {
							t.Fatalf("LSN=%d want=%d", frame.LSN, count)
						}
					}
				}
				if count != len(sizes) {
					t.Fatalf("frames=%d want=%d", count, len(sizes))
				}
			})
		}
	}
}

// Every injected failure actually modifies the file: write faults preserve the
// new header and part of its reserve; the sync fault follows all reserve writes
// and an actual full sync. Restoring I/O must not permit another ACK.
func TestFixedWALInitializationFailureStopsWriter(t *testing.T) {
	for _, where := range []string{"resize", "ordinary_rotation", "stable_rotation"} {
		for _, fault := range []string{"short_write", "enospc", "full_sync"} {
			t.Run(where+"/"+fault, func(t *testing.T) {
				j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 1024, CaptureStableResources: where == "stable_rotation"})
				if err != nil {
					t.Fatal(err)
				}
				oldFile := j.writer.f
				walDir := j.walDir
				var accepted []byte
				if where != "resize" {
					if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
						t.Fatal(err)
					}
					if err = j.Sync(); err != nil {
						t.Fatal(err)
					}
					accepted, err = os.ReadFile(j.path)
					if err != nil {
						t.Fatal(err)
					}
				}
				oldWrite, oldSync := fixedWALInitializeWrite, fixedWALInitializeSync
				defer func() { fixedWALInitializeWrite, fixedWALInitializeSync = oldWrite, oldSync; _ = j.Close() }()
				var modified *os.File
				writes := 0
				if fault == "full_sync" {
					fixedWALInitializeSync = func(file *os.File) error {
						modified = file
						if err := oldSync(file); err != nil {
							return err
						}
						return syscall.EIO
					}
				} else {
					fixedWALInitializeWrite = func(writer io.WriterAt, p []byte, offset int64) error {
						writes++
						modified = writer.(*os.File)
						if writes == 2 {
							if _, err := writer.WriteAt(p[:min(len(p), 97)], offset); err != nil {
								return err
							}
							if fault == "enospc" {
								return syscall.ENOSPC
							}
							return io.ErrShortWrite
						}
						return oldWrite(writer, p, offset)
					}
				}
				beforeLSN := j.NextLSN()
				_, err = j.AppendCommand(fixedWALLifecycleFrame(t, 32<<10))
				fixedWALInitializeWrite, fixedWALInitializeSync = oldWrite, oldSync
				if err == nil {
					t.Fatal("expected initialization failure")
				}
				if modified == nil {
					t.Fatal("fault did not modify a physical file")
				}
				if j.NextLSN() != beforeLSN {
					t.Fatalf("failure reserved LSN: next=%d before=%d", j.NextLSN(), beforeLSN)
				}
				if _, appendErr := j.AppendCommand(fixedWALTestBarrier()); appendErr == nil {
					t.Error("append succeeded after partially modified initialization failure")
				}
				if j.NextLSN() != beforeLSN {
					t.Error("retry reserved an LSN after initialization failure")
				}
				if syncErr := j.Sync(); syncErr == nil {
					t.Error("sync succeeded after initialization failure")
				}
				if flushErr := j.Flush(); flushErr == nil {
					t.Error("flush succeeded after initialization failure")
				}
				if len(accepted) > 0 {
					now, e := os.ReadFile(oldFile.Name())
					if e != nil || !bytes.Equal(accepted, now) {
						t.Errorf("accepted old file changed err=%v", e)
					}
				}
				if where != "resize" {
					if _, e := modified.ReadAt(make([]byte, 1), 0); !errors.Is(e, os.ErrClosed) {
						t.Errorf("failed prepared descriptor remains open: %v", e)
					}
					if runtime.GOOS == "windows" {
						info, e := os.Stat(modified.Name())
						if e != nil || info.Size() != 0 {
							t.Errorf("failed Windows successor is not canonical empty: info=%v err=%v", info, e)
						}
					} else if _, e := os.Stat(modified.Name()); !errors.Is(e, os.ErrNotExist) {
						t.Errorf("unaccepted partial successor remains linked: %v", e)
					}
				}
				if _, e := oldFile.Stat(); e != nil {
					t.Errorf("old descriptor closed before owner Close: %v", e)
				}
				_ = j.Close()
				if _, e := oldFile.ReadAt(make([]byte, 1), 0); !errors.Is(e, os.ErrClosed) {
					t.Errorf("old descriptor leaked after Close: %v", e)
				}
				if where != "resize" {
					reopened, e := OpenCommandJournal(walDir, CommandJournalOptions{SegmentTargetBytes: 1024})
					if e != nil {
						t.Fatalf("ordinary reopen lost prior ACK after failed successor: %v", e)
					}
					defer reopened.Close()
					if reopened.NextLSN() != beforeLSN {
						t.Errorf("reopen nextLSN=%d want=%d", reopened.NextLSN(), beforeLSN)
					}
					frames, e := ScanCommandFramesV2(oldFile.Name(), Options{})
					if e != nil || len(frames) != 1 || frames[0].LSN != 1 {
						t.Errorf("reopened accepted prefix: frames=%v err=%v", frames, e)
					}
				}
			})
		}
	}
}

func TestFixedWALFlushRetainsPendingBase(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i := 0; i < 3; i++ {
		if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
			t.Fatal(err)
		}
	}
	if err = j.writer.bw.Flush(); err != nil {
		t.Fatal(err)
	}
	original := j.writer.fixed.pending
	if len(original) == 0 {
		t.Fatal("missing pending frames")
	}
	base := &original[:cap(original)][0]
	capacity := cap(original)
	if err = j.Flush(); err != nil {
		t.Fatal(err)
	}
	pending := j.writer.fixed.pending
	if cap(pending) != capacity || &pending[:cap(pending)][0] != base {
		t.Fatalf("pending base/capacity lost: cap=%d want=%d", cap(pending), capacity)
	}
}

// Cleanup requires exclusive creation proof. An existing foreign/corrupt
// successor must remain untouched even though this writer then fails stop.
func TestFixedWALFailedPreparationRetainsExistingSuccessor(t *testing.T) {
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
	path := filepath.Join(j.walDir, CommandSegmentName(j.lane, j.segmentSeq+1))
	foreign := make([]byte, fixedWALHeaderSize)
	copy(foreign, fixedWALMagic[:]) // recognized container, invalid identity/capacity CRC
	copy(foreign[64:], "existing successor is not owned by this rotation")
	if err = os.WriteFile(path, foreign, 0600); err != nil {
		t.Fatal(err)
	}
	if err = j.RotateActiveSegment(false); err == nil {
		t.Fatal("expected existing successor validation error")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, foreign) {
		t.Fatalf("existing successor changed: %q err=%v", got, err)
	}
}

func TestFixedWALOversizedExistingEmptySuccessor(t *testing.T) {
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
	path := filepath.Join(j.walDir, CommandSegmentName(j.lane, j.segmentSeq+1))
	empty, err := NewWriterWithOptions(path, Options{fixedCapacity: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err = empty.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = j.AppendCommand(fixedWALLifecycleFrame(t, 32<<10)); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatalf("chosen existing empty successor: %v", err)
	}
	frames, err := ScanCommandFramesV2(path, Options{})
	if err != nil || len(frames) != 1 || frames[0].LSN != 2 {
		t.Fatalf("successor frames=%v err=%v", frames, err)
	}
}

func TestFixedWALRawJournalRejectsIncompleteRelaxedExtent(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	path, dir := j.path, j.walDir
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	relaxed := fixedWALLifecycleFrame(t, 128)
	relaxed.DurabilityClass = CommandDurabilityRelaxed
	if _, err = j.AppendCommand(relaxed); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := loadFixedWAL(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	last := state.extents[len(state.extents)-1]
	start := last.physical + fixedWALWitnessSize + segmentHeaderSize + 56
	if err = writeFixedAt(file, make([]byte, last.end-start), start); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if frame, e := reader.ReadCommandFrameV2(); e != nil || frame.LSN != 1 {
		t.Fatalf("accepted frame=%v err=%v", frame, e)
	}
	_, err = reader.ReadCommandFrameV2()
	var incomplete *CommandWALIncompleteFrame
	if !errors.As(err, &incomplete) || incomplete.Envelope.LSN != 2 || incomplete.Envelope.DurabilityClass != CommandDurabilityRelaxed {
		t.Fatalf("incomplete identity: %v", err)
	}
	reopened, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if reopened != nil {
		_ = reopened.Close()
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("raw journal must defer to global horizon recovery: %v", err)
	}
}

func TestFixedWALRejectsAnonymousPhysicalEOFTail(t *testing.T) {
	j, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	path, dir := j.path, j.walDir
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte{1, 2, 3}); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if reopened != nil {
		_ = reopened.Close()
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("anonymous bytes after initialized capacity are corruption: %v", err)
	}
}
