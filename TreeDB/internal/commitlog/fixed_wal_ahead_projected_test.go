package commitlog

import (
	"io"
	"math/rand"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
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

func TestCRC32IEEECombineMatchesChecksum(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	data := make([]byte, 2*1024*1024+137)
	_, _ = rng.Read(data)

	cases := []struct{ aLen, bLen int }{
		{0, 0},
		{8, 0},
		{0, 8},
		{8, 1},
		{8, 7},
		{8, 8},
		{8, 63},
		{8, 64},
		{8, 4095},
		{8, 4096},
		{8, 4097},
		{8, 65536},
		{8, 2*1024*1024 + 129},
		{19, 12345},
	}
	for _, tc := range cases {
		a := data[:tc.aLen]
		b := data[tc.aLen : tc.aLen+tc.bLen]
		full := data[:tc.aLen+tc.bLen]
		want := crc.Checksum(full)
		got := crc32IEEECombine(crc.Checksum(a), crc.Checksum(b), len(b))
		if got != want {
			t.Fatalf("combine(a=%d, b=%d) = %#x, want %#x", tc.aLen, tc.bLen, got, want)
		}
	}
}

func TestFixedWALAheadStartsWithinThreeFrameLookahead(t *testing.T) {
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

	// Fill segment 1 (4 MiB) with two ~1.8 MiB frames, wait for 8 MiB ahead
	// reserve, then rotate into segment 2 (8 MiB).
	frame := fixedWALLifecycleFrame(t, 1800<<10)
	for i := 0; i < 2; i++ {
		if _, err := j.AppendCommand(frame); err != nil {
			t.Fatalf("seg1 append %d: %v", i, err)
		}
		if err := j.Flush(); err != nil {
			t.Fatalf("seg1 flush %d: %v", i, err)
		}
	}
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })
	if _, err := j.AppendCommand(frame); err != nil {
		t.Fatalf("rotate to seg2: %v", err)
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("flush seg2 first frame: %v", err)
	}
	rots, err := j.TakePendingStableRotations()
	if err != nil {
		t.Fatalf("TakePendingStableRotations: %v", err)
	}
	for _, r := range rots {
		r.Release()
	}
	// Wait for the claimed ahead worker to mark done so segment 2 can schedule
	// its own 16 MiB successor.
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s == nil || s.done })

	// Segment 2 has 8 MiB capacity (~4 frames of 1.8 MiB). On the 2nd frame
	// (projected ~3.6 MiB, remaining ~4.4 MiB -> remaining/extent == 2 < 3),
	// ahead-reserve must start with 2 full batch intervals of lead time.
	if _, err := j.AppendCommand(frame); err != nil {
		t.Fatalf("seg2 append 2: %v", err)
	}
	if err := j.Flush(); err != nil {
		t.Fatalf("seg2 flush 2: %v", err)
	}
	j.mu.Lock()
	slot := j.fixedAhead
	j.mu.Unlock()
	if slot == nil || slot.done {
		t.Fatalf("expected 16 MiB ahead-reserve to start on 2nd frame of 8 MiB segment")
	}
}

func TestFixedWALWriterBufioDefersMultiMiBAllocationAndPreservesStats(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	const configuredBuf = 16 << 20
	path := filepath.Join(t.TempDir(), "commit-l0-000001.log")
	w, err := NewWriterWithOptions(path, Options{
		fixedCapacity: fixedWALDefaultCapacity,
		BufferSize:    configuredBuf,
	})
	if err != nil {
		t.Fatalf("NewWriterWithOptions: %v", err)
	}
	defer w.Close()

	if w.bw == nil {
		t.Fatal("expected non-nil bufio.Writer on fixed WAL")
	}
	if got := w.bw.Size(); got != fixedWALBufioSize {
		t.Fatalf("fixed WAL internal bufio size=%d, want %d", got, fixedWALBufioSize)
	}
	if stats := w.BufferStats(); stats.BufferedWriterSize != configuredBuf {
		t.Fatalf("BufferStats().BufferedWriterSize=%d, want %d", stats.BufferedWriterSize, configuredBuf)
	}
}
