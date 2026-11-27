package commitlog

import (
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
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

func TestFixedWALBulkPressureSteppingAndAheadAdoption(t *testing.T) {
	current := &fixedWAL{capacity: fixedWALDefaultCapacity}
	// Small and sub-threshold frames preserve 2x doubling.
	for _, incoming := range []int64{256 << 10, 1800 << 10} {
		plan, err := fixedSuccessorSizePlan(current, fixedWALDefaultPhysicalBudget, fixedWALPressureRotation, incoming)
		if err != nil || plan.capacity != 2*fixedWALDefaultCapacity {
			t.Fatalf("sub-threshold incoming=%d plan=%+v want capacity=%d err=%v", incoming, plan, 2*fixedWALDefaultCapacity, err)
		}
	}
	// Multi-MiB bulk frames quantize to the same power-of-two multiple (~32 MiB)
	// across slightly varying sizes so ahead-reserve adoption matches exactly.
	wantBulk := int64(8 * fixedWALDefaultCapacity)
	for _, incoming := range []int64{3_200_000, 3_250_000} {
		plan, err := fixedSuccessorSizePlan(current, fixedWALDefaultPhysicalBudget, fixedWALPressureRotation, incoming)
		if err != nil || plan.capacity != wantBulk {
			t.Fatalf("bulk incoming=%d plan=%+v want capacity=%d err=%v", incoming, plan, wantBulk, err)
		}
	}

	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{
		SegmentTargetBytes:     64 << 20,
		CaptureStableResources: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	frame1 := fixedWALLifecycleFrame(t, 3_200_000)
	if _, err := j.AppendCommand(frame1); err != nil {
		t.Fatalf("append frame1: %v", err)
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("flush frame1: %v", err)
	}
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })
	j.mu.Lock()
	planned := j.fixedAhead.plannedCapacity
	j.mu.Unlock()
	if planned != wantBulk {
		t.Fatalf("ahead plannedCapacity=%d want %d", planned, wantBulk)
	}

	oldWrite := fixedWALInitializeWrite
	var syncWrites atomic.Int64
	fixedWALInitializeWrite = func(f io.WriterAt, p []byte, off int64) error {
		syncWrites.Add(1)
		return oldWrite(f, p, off)
	}
	defer func() { fixedWALInitializeWrite = oldWrite }()

	// Slightly different second bulk frame size must still adopt the exact
	// quantized 32 MiB ahead reserve with zero synchronous init writes.
	frame2 := fixedWALLifecycleFrame(t, 3_250_000)
	if _, err := j.AppendCommand(frame2); err != nil {
		t.Fatalf("append frame2: %v", err)
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("flush frame2: %v", err)
	}
	fixedWALInitializeWrite = oldWrite

	if got := syncWrites.Load(); got != 0 {
		t.Fatalf("bulk rotation performed %d synchronous init writes; want 0 (adopted ahead reserve)", got)
	}
	if got := j.writer.fixed.capacity; got != wantBulk {
		t.Fatalf("rotated fixed WAL capacity=%d want %d", got, wantBulk)
	}
}
