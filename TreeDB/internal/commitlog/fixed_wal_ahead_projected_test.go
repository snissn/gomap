package commitlog

import (
	"io"
	"sync/atomic"
	"testing"
)

func TestFixedWALAheadStartsOnProjectedHalfOccupancyBeforeOverflow(t *testing.T) {
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

	// Two ~1.8 MiB frames fit in the initial 4 MiB fixed WAL (3.6 MiB / 4.0 MiB).
	// Previously, ahead-reserve only checked pre-append occupancy (1.8 MiB < 2.0 MiB),
	// so the 2nd frame failed to start ahead-reserve and the 3rd frame stalled on a
	// synchronous 8 MiB zero-fill + fsync.
	frame := fixedWALLifecycleFrame(t, 1800<<10)
	for i := 0; i < 2; i++ {
		if _, err := j.AppendCommand(frame); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if err := j.Flush(); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}

	j.mu.Lock()
	started := j.fixedAhead != nil
	j.mu.Unlock()
	if !started {
		t.Fatalf("expected fixed WAL ahead-reserve to start once projected occupancy exceeded 50%%")
	}

	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })

	oldWrite := fixedWALInitializeWrite
	var syncWrites atomic.Int64
	fixedWALInitializeWrite = func(f io.WriterAt, p []byte, off int64) error {
		syncWrites.Add(1)
		return oldWrite(f, p, off)
	}
	defer func() { fixedWALInitializeWrite = oldWrite }()

	if _, err := j.AppendCommand(frame); err != nil {
		t.Fatalf("append 3 (overflow rotate): %v", err)
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("flush 3: %v", err)
	}
	fixedWALInitializeWrite = oldWrite

	if got := syncWrites.Load(); got != 0 {
		t.Fatalf("3rd large frame performed %d synchronous fixed-WAL init writes; want 0 (adopted ahead reserve)", got)
	}
}
