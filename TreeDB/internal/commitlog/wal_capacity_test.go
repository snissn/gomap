package commitlog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Ten individually legal frames cross the historical implicit 1 MiB clamp,
// but fit one small initialized reserve and the caller's 256 MiB logical limit.
// Zero disables that logical limit; a small positive target still rotates.
func TestCommandJournalPreservesLogicalRotationTarget(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, tc := range []struct {
		name     string
		target   int64
		segments uint64
	}{
		{"disabled", 0, 1}, {"large", 256 << 20, 1}, {"small", 1024, 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: tc.target})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			if j.writer.fixed == nil {
				t.Fatal("fixture must exercise actual fixed container")
			}
			for i := 0; i < 10; i++ {
				if _, err = j.AppendCommand(fixedWALLifecycleFrame(t, 128<<10)); err != nil {
					t.Fatal(err)
				}
				if err = j.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if j.segmentSeq != tc.segments {
				t.Fatalf("requested logical target=%d produced segment=%d want=%d", tc.target, j.segmentSeq, tc.segments)
			}
		})
	}
}

func TestFixedWALCheckedPhysicalGeometry(t *testing.T) {
	for _, tc := range []struct{ target, capacity int64 }{
		{0, 4199424}, {-1, 4199424}, {1, 4352}, {1024, 5376}, {256 << 20, 268439808},
	} {
		got, err := fixedWALPhysicalBudget(tc.target)
		if err != nil || got != tc.capacity {
			t.Fatalf("target=%d capacity=%d want=%d err=%v", tc.target, got, tc.capacity, err)
		}
	}
	for _, tc := range []struct{ frame, capacity int64 }{{1, 4352}, {128, 4480}, {4096, 8448}} {
		got, err := fixedWALFrameCapacity(tc.frame)
		if err != nil || got != tc.capacity {
			t.Fatalf("frame=%d capacity=%d want=%d err=%v", tc.frame, got, tc.capacity, err)
		}
	}
	for _, f := range []func() (int64, error){
		func() (int64, error) { return fixedWALPhysicalBudget(1<<63 - 1) },
		func() (int64, error) { return fixedWALFrameCapacity(1<<63 - 1) },
		func() (int64, error) {
			return fixedWALFrameCapacity((1<<63 - 1) - fixedWALHeaderSize - 2*fixedWALWitnessSize - (fixedWALAlignment - 1))
		},
		func() (int64, error) { return fixedWALExtentEnd(1<<63-1, 1) },
	} {
		if _, err := f(); !errors.Is(err, ErrRecordTooLarge) {
			t.Fatalf("overflow=%v", err)
		}
	}
	dir := filepath.Join(t.TempDir(), "untouched")
	if _, err := openFixedTestCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 1<<63 - 1}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid geometry touched journal directory: %v", err)
	}
}

func TestCommandJournalZeroTargetStreamDoesNotRotate(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, CommandSegmentName(0, 1)))
	if err != nil {
		t.Fatal(err)
	}
	first := fixedWALLifecycleFrame(t, 128<<10)
	first.LSN = 1
	if err = w.AppendCommandV2(first); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.writer.fixed != nil {
		t.Fatal("existing stream was converted")
	}
	for i := 0; i < 10; i++ {
		if _, err = j.AppendCommand(fixedWALLifecycleFrame(t, 128<<10)); err != nil {
			t.Fatal(err)
		}
		if err = j.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if j.segmentSeq != 1 {
		t.Fatalf("zero logical target rotated existing stream into %d", j.segmentSeq)
	}
}

func TestFixedWALPhysicalBoundaryZeroTargetAndReopen(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, stable := range []bool{false, true} {
		t.Run(fmt.Sprint("stable=", stable), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, CommandSegmentName(0, 1))
			frame := fixedWALLifecycleFrame(t, 4096)
			frame.LSN = 1
			encoded, err := EncodeCommandFrameV2(frame)
			if err != nil {
				t.Fatal(err)
			}
			required, err := fixedWALFrameCapacity(int64(len(encoded) + segmentHeaderSize))
			if err != nil {
				t.Fatal(err)
			}
			seed, err := NewFixedWriterWithCapacity(path, required)
			if err != nil {
				t.Fatal(err)
			}
			if err = seed.Close(); err != nil {
				t.Fatal(err)
			}
			j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: stable})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			if j.writer.fixed.capacity != required {
				t.Fatal("reopen changed existing capacity")
			}
			frame.LSN = 0
			if _, err = j.AppendCommand(frame); err != nil {
				t.Fatal(err)
			}
			if err = j.Sync(); err != nil {
				t.Fatal(err)
			}
			if j.segmentSeq != 1 {
				t.Fatal("exact fitting frame rotated")
			}
			accepted, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
				t.Fatal(err)
			}
			if err = j.Sync(); err != nil {
				t.Fatal(err)
			}
			fixedWALDrainRotations(t, j)
			if j.segmentSeq != 2 || j.writer.fixed.capacity != 2*required {
				t.Fatalf("physical pressure: seq=%d capacity=%d previous=%d", j.segmentSeq, j.writer.fixed.capacity, required)
			}
			now, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(now, accepted) {
				t.Fatal("accepted predecessor changed", err)
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openFixedTestCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.writer.fixed.capacity != 2*required {
				t.Fatal("larger logical target resized occupied active file")
			}
			frames, err := ScanCommandFramesV2(path, Options{})
			if err != nil || len(frames) != 1 || frames[0].LSN != 1 {
				t.Fatal("old accepted frame changed", err)
			}
		})
	}
}

func TestFixedWALDemandGrowthAndForcedReset(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, stable := range []bool{false, true} {
		t.Run(fmt.Sprint("stable=", stable), func(t *testing.T) {
			j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 8 << 20, CaptureStableResources: stable})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			if j.writer.fixed.capacity != 4199424 {
				t.Fatalf("eager reserve=%d", j.writer.fixed.capacity)
			}
			for i := 0; i < 5; i++ {
				if _, err = j.AppendCommand(fixedWALLifecycleFrame(t, 1<<20)); err != nil {
					t.Fatal(err)
				}
				if err = j.Sync(); err != nil {
					t.Fatal(err)
				}
				fixedWALDrainRotations(t, j)
			}
			if j.segmentSeq != 2 || j.writer.fixed.capacity != 8392960 {
				t.Fatalf("pressure growth: seq=%d capacity=%d", j.segmentSeq, j.writer.fixed.capacity)
			}
			if stable {
				r, e := j.RotateActiveSegmentWithStableResources(false)
				if e != nil {
					t.Fatal(e)
				}
				r.Release()
			} else if err = j.RotateActiveSegment(false); err != nil {
				t.Fatal(err)
			}
			if j.writer.fixed.capacity != 4199424 || j.writer.ActiveBytes() != 0 {
				t.Fatalf("forced empty successor retained growth: capacity=%d logical=%d", j.writer.fixed.capacity, j.writer.ActiveBytes())
			}
		})
	}
}

func TestFixedWALOversizedFrameCapacityDoesNotAmplify(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, stable := range []bool{false, true} {
		t.Run(fmt.Sprint("stable=", stable), func(t *testing.T) {
			j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 1024, CaptureStableResources: stable})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			frame := fixedWALLifecycleFrame(t, 32<<10)
			probe := frame
			probe.LSN = 1
			encoded, err := EncodeCommandFrameV2(probe)
			if err != nil {
				t.Fatal(err)
			}
			required, err := fixedWALFrameCapacity(int64(len(encoded) + segmentHeaderSize))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = j.AppendCommand(frame); err != nil {
				t.Fatal(err)
			}
			if err = j.Sync(); err != nil {
				t.Fatal(err)
			}
			fixedWALDrainRotations(t, j)
			if j.writer.fixed.capacity != required {
				t.Fatalf("oversized reserve=%d want exact %d", j.writer.fixed.capacity, required)
			}
			if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
				t.Fatal(err)
			}
			if err = j.Sync(); err != nil {
				t.Fatal(err)
			}
			fixedWALDrainRotations(t, j)
			if j.writer.fixed.capacity != 5376 {
				t.Fatalf("exception amplified successor: capacity=%d", j.writer.fixed.capacity)
			}
		})
	}
}

// OnSegmentRotated is a mutable callback boundary: an extension element's
// slice header is shared even though the envelope itself was passed by value.
func TestCommandJournalMutableRotationSizeBeforeLSN(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, version := range []uint16{CommandFrameVersion, CommandFrameVersionV2} {
		for _, stable := range []bool{false, true} {
			for _, large := range []bool{false, true} {
				t.Run(fmt.Sprintf("v%d/stable=%t/large=%t", version, stable, large), func(t *testing.T) {
					frame := encodingPlanEnvelope(t, 0)
					frame.Version = version
					if version == CommandFrameVersion {
						frame.DurabilityClass = 0
					}
					frame.Preconditions = []CommandExtension{{Type: 71, Payload: []byte("x")}}
					target := commandJournalSingleFrameTarget(t, frame)
					callbacks := 0
					j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: target, MaxSegmentSize: 1 << 20, CaptureStableResources: stable, OnSegmentRotated: func(int64) {
						callbacks++
						size := 2
						if large {
							size = 64 << 10
						}
						frame.Preconditions[0].Payload = bytes.Repeat([]byte("y"), size)
					}})
					if err != nil {
						t.Fatal(err)
					}
					defer j.Close()
					first := encodingPlanEnvelope(t, 0)
					first.Version = version
					if version == CommandFrameVersion {
						first.DurabilityClass = 0
					}
					if lsn, e := j.AppendCommand(first); e != nil || lsn != 1 {
						t.Fatal(lsn, e)
					}
					if err = j.Sync(); err != nil {
						t.Fatal(err)
					}
					oldPath, oldSeq := j.path, j.segmentSeq
					accepted, err := os.ReadFile(oldPath)
					if err != nil {
						t.Fatal(err)
					}
					beforeLSN := j.NextLSN()
					lsn, err := j.AppendCommand(frame)
					if large {
						if !errors.Is(err, ErrRecordTooLarge) || lsn != 0 || j.NextLSN() != beforeLSN {
							t.Fatalf("callback growth accepted/reserved: lsn=%d next=%d want=%d err=%v", lsn, j.NextLSN(), beforeLSN, err)
						}
						if j.writer.ActiveBytes() != 0 || len(j.writer.fixed.pending) != 0 || len(j.writer.commandBuf) != 0 {
							t.Fatal("rejected frame reached pending storage")
						}
					} else {
						if err != nil || lsn != beforeLSN {
							t.Fatalf("within-fit mutation rejected: lsn=%d err=%v", lsn, err)
						}
						if err = j.Sync(); err != nil {
							t.Fatal(err)
						}
						frames, e := ScanCommandFrames(j.path, Options{})
						if e != nil || len(frames) != 1 || frames[0].LSN != 2 || len(frames[0].Preconditions[0].Payload) != 2 {
							t.Fatal("stale within-fit mutation", frames, e)
						}
					}
					if callbacks != 1 || j.segmentSeq != oldSeq+1 {
						t.Fatalf("callback/rotation count=%d/%d", callbacks, j.segmentSeq-oldSeq)
					}
					now, e := os.ReadFile(oldPath)
					if e != nil || !bytes.Equal(now, accepted) {
						t.Fatal("accepted predecessor changed", e)
					}
					rotations, e := j.TakePendingStableRotations()
					if e != nil {
						t.Fatal(e)
					}
					if stable && len(rotations) != 1 {
						t.Fatalf("lost pending exact rotation: %d", len(rotations))
					}
					for _, rotation := range rotations {
						if rotation.Closed == nil || rotation.Active == nil || rotation.Closed.Frontier().MaxLSN != 1 {
							t.Fatal("lost predecessor custody")
						}
						wantMax := uint64(2)
						if large {
							wantMax = 0
						}
						if rotation.Active.Frontier().MaxLSN != wantMax {
							t.Fatal("rejected frame entered successor frontier")
						}
						rotation.Release()
					}
					if large {
						if got, e := j.AppendCommand(first); e != nil || got != beforeLSN {
							t.Fatal("preaccept refusal consumed LSN or poisoned reusable writer", got, e)
						}
						if err = j.Sync(); err != nil {
							t.Fatal(err)
						}
						frames, e := ScanCommandFrames(j.path, Options{})
						if e != nil || len(frames) != 1 || frames[0].LSN != 2 || len(frames[0].Preconditions) != 0 {
							t.Fatal("rejected frame leaked into recovered successor", frames, e)
						}
					}
				})
			}
		}
	}
}

func TestCommandJournalBeforeAppendMutableSizeReprepares(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, version := range []uint16{CommandFrameVersion, CommandFrameVersionV2} {
		for _, stable := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/stable=%t", version, stable), func(t *testing.T) {
				frame := encodingPlanEnvelope(t, 0)
				frame.Version = version
				if version == CommandFrameVersion {
					frame.DurabilityClass = 0
				}
				frame.ResultAssertions = []CommandExtension{{Type: 72, Payload: []byte("x")}}
				j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: commandJournalSingleFrameTarget(t, frame), MaxSegmentSize: 1 << 20, CaptureStableResources: stable})
				if err != nil {
					t.Fatal(err)
				}
				defer j.Close()
				lsn, err := j.AppendCommandObservedWithHooks(frame, func() error { frame.ResultAssertions[0].Payload = bytes.Repeat([]byte("z"), 64<<10); return nil }, nil)
				if err != nil || lsn != 1 {
					t.Fatal(lsn, err)
				}
				if err = j.Sync(); err != nil {
					t.Fatalf("fresh hook size did not reach preassignment physical sizing: %v", err)
				}
				fixedWALDrainRotations(t, j)
				frames, e := ScanCommandFrames(j.path, Options{})
				if e != nil || len(frames) != 1 || len(frames[0].ResultAssertions[0].Payload) != 64<<10 {
					t.Fatal("stale preappend extension", e)
				}
			})
		}
	}
}
