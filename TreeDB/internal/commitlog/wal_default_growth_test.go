package commitlog

import (
	"bytes"
	"path/filepath"
	"testing"
)

// This test uses only predecessor APIs so it can serve as an actual pressure
// counterfactual. No configured logical limit, oversized frame, or synthesized
// fixed-file header supplies the larger capacities.
func TestFixedWALDefaultPressureGrowthReplayAndReset(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if j != nil {
			_ = j.Close()
		}
	}()
	wantCapacities := []int64{4199424, 8398848, 16797696, 33595392, 67108864, 67108864}
	if j.writer.fixed == nil || j.writer.fixed.capacity != wantCapacities[0] {
		t.Fatal("default open must retain the small initialized reserve")
	}
	frame := fixedWALLifecycleFrame(t, 1<<20)
	framesWritten := 0
	for framesWritten < 160 && j.segmentSeq < uint64(len(wantCapacities)) {
		lsn, appendErr := j.AppendCommand(frame)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		framesWritten++
		if lsn != uint64(framesWritten) {
			t.Fatalf("assigned LSN=%d want=%d", lsn, framesWritten)
		}
		if err = j.Sync(); err != nil {
			t.Fatal(err)
		}
		fixedWALDrainRotations(t, j)
		if j.segmentSeq > uint64(len(wantCapacities)) || j.writer.fixed.capacity != wantCapacities[j.segmentSeq-1] {
			t.Fatalf("default pressure growth: sequence=%d capacity=%d want=%v", j.segmentSeq, j.writer.fixed.capacity, wantCapacities)
		}
	}
	if j.segmentSeq != uint64(len(wantCapacities)) {
		t.Fatalf("sustained appends did not reach and repeat ceiling: frames=%d sequence=%d", framesWritten, j.segmentSeq)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	// A negative logical target has the same default policy. Reopen must honor
	// the occupied header, not reset or resize its grown physical reserve.
	j, err = openFixedTestCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: -1, CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	if j.writer.fixed.capacity != 67108864 || j.NextLSN() != uint64(framesWritten+1) {
		t.Fatalf("grown reopen: capacity=%d next=%d", j.writer.fixed.capacity, j.NextLSN())
	}
	paths, err := filepath.Glob(filepath.Join(dir, "commit-l0-*.log"))
	if err != nil || len(paths) != len(wantCapacities) {
		t.Fatalf("segments=%v err=%v", paths, err)
	}
	next := uint64(1)
	for _, path := range paths {
		frames, scanErr := ScanCommandFramesV2(path, Options{})
		if scanErr != nil {
			t.Fatal(scanErr)
		}
		for _, got := range frames {
			if got.LSN != next || got.Kind != frame.Kind || !bytes.Equal(got.Payload, frame.Payload) {
				t.Fatalf("replay changed accepted frame at LSN=%d", next)
			}
			next++
		}
	}
	if next != uint64(framesWritten+1) {
		t.Fatalf("replayed=%d written=%d", next-1, framesWritten)
	}
	rotation, err := j.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	rotation.Release()
	if j.writer.fixed.capacity != 4199424 || j.writer.ActiveBytes() != 0 {
		t.Fatalf("forced reset: capacity=%d logical=%d", j.writer.fixed.capacity, j.writer.ActiveBytes())
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = openFixedTestCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if j.writer.fixed.capacity != 4199424 || j.NextLSN() != next {
		t.Fatalf("reset reopen: capacity=%d next=%d want=%d", j.writer.fixed.capacity, j.NextLSN(), next)
	}
	t.Logf("accepted_frames=%d pressure_capacities=%v forced_reset=%d", framesWritten, wantCapacities, j.writer.fixed.capacity)
}
