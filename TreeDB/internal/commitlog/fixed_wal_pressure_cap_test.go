package commitlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFixedWALPressureAdoptsAtAheadCap(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, tc := range []struct {
		name   string
		stable bool
	}{
		{name: "ordinary"},
		{name: "stable", stable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			// Seed the existing fourth geometric size to reach the pressure
			// boundary without replaying the first three growth cycles.
			initial := filepath.Join(dir, CommandSegmentName(0, 1))
			f, err := os.Create(initial)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := writeInitializedFixedWAL(f, 8*fixedWALDefaultCapacity); err != nil {
				_ = f.Close()
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			opts := CommandJournalOptions{SegmentTargetBytes: 256 << 20, CaptureStableResources: tc.stable}
			j, err := openFixedTestCommandJournal(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			frame := fixedWALLifecycleFrame(t, 1<<20)
			var accepted uint64
			appendFrame := func() {
				t.Helper()
				lsn, err := j.AppendCommand(frame)
				if err != nil {
					t.Fatal(err)
				}
				accepted++
				if lsn != accepted {
					t.Fatalf("LSN=%d want %d", lsn, accepted)
				}
				if err := j.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			for cycle := 0; cycle < 2; cycle++ {
				path := j.Path()
				before := j.writer.fixed.capacity
				for j.writer.fixed.physical-fixedWALHeaderSize < (before-fixedWALHeaderSize+1)/2 {
					appendFrame()
				}
				appendFrame() // Observe the flushed half threshold and start preparation.
				j.mu.Lock()
				hasAhead := j.fixedAhead != nil
				j.mu.Unlock()
				var ready os.FileInfo
				if hasAhead {
					aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })
					j.mu.Lock()
					ready, err = j.fixedAhead.reserve.file.Stat()
					j.mu.Unlock()
					if err != nil {
						t.Fatal(err)
					}
				}
				captured, err := j.CaptureSync(nil)
				if err != nil {
					t.Fatal(err)
				}
				defer captured.Release()
				for i := 0; j.Path() == path && i < 64; i++ {
					appendFrame()
				}
				if j.Path() == path {
					t.Fatal("did not rotate under physical pressure")
				}
				got, err := os.Stat(j.Path())
				if err != nil {
					t.Fatal(err)
				}
				if got.Size() != fixedWALDefaultPhysicalBudget {
					t.Fatalf("pressure successor capacity=%d want %d", got.Size(), fixedWALDefaultPhysicalBudget)
				}
				if ready == nil || !os.SameFile(ready, got) {
					t.Fatal("pressure did not adopt the exact prepared inode")
				}
				if err := captured.Sync(); err != nil {
					t.Fatal(err)
				}
				captured.Release()
				if tc.stable {
					fixedWALDrainRotations(t, j)
				}
				aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s == nil || s.done })
			}
			if err := j.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openFixedTestCommandJournal(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got := reopened.NextLSN(); got != accepted+1 {
				t.Fatalf("reopened next LSN=%d want %d", got, accepted+1)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFixedWALPressureCapPreservesAtomicFrame(t *testing.T) {
	const budget = 256 << 20
	const frameBytes = 80 << 20
	required, err := fixedWALFrameCapacity(frameBytes)
	if err != nil {
		t.Fatal(err)
	}
	current := &fixedWAL{capacity: fixedWALDefaultPhysicalBudget}
	plan, err := fixedSuccessorSizePlan(current, budget, fixedWALPressureRotation, frameBytes)
	if err != nil || plan.capacity != required {
		t.Fatalf("large frame plan=%+v want capacity=%d err=%v", plan, required, err)
	}
	current.capacity = required
	for _, reason := range []fixedWALRotationReason{fixedWALPressureRotation, fixedWALLogicalRotation} {
		plan, err := fixedSuccessorSizePlan(current, budget, reason, 256)
		if err != nil || plan.capacity != fixedWALDefaultPhysicalBudget {
			t.Fatalf("reason=%d inherited oversized reserve: plan=%+v err=%v", reason, plan, err)
		}
	}
}
