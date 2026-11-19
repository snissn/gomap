//go:build linux

package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func ext4StreamReserveTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	f, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := IsExt4WALFilesystem(f)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires an ext4 filesystem")
	}
	return dir
}

func openStreamReserveTestJournal(t *testing.T, dir string, opts CommandJournalOptions) *CommandJournal {
	t.Helper()
	j, err := OpenCommandJournal(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func appendStreamReserveTestFrame(t *testing.T, j *CommandJournal, value []byte) uint64 {
	t.Helper()
	lsn, err := j.AppendRawKVPointCommandTrustedAndFlush(0, RawKVOpSet, []byte("key"), value, false)
	if err != nil {
		t.Fatal(err)
	}
	return lsn
}

// Use real CRC-checked command frames and finish at an exact logical frontier.
func growStreamReserveTestJournal(t *testing.T, j *CommandJournal, through int64) uint64 {
	t.Helper()
	value := make([]byte, 8<<20)
	_, _, frameBytes, err := trustedRawKVPointCommandFrameLens(RawKVOpSet, []byte("key"), value, 0)
	if err != nil {
		t.Fatal(err)
	}
	overhead := int64(segmentHeaderSize + frameBytes - len(value))
	var lsn uint64
	for remaining := through - j.writer.StableBytes(); remaining != 0; remaining = through - j.writer.StableBytes() {
		n := min(remaining-overhead, int64(len(value)))
		if n < 0 {
			t.Fatalf("cannot fill remaining %d bytes with a complete frame", remaining)
		}
		if rest := remaining - overhead - n; rest > 0 && rest < overhead {
			n -= overhead
		}
		lsn = appendStreamReserveTestFrame(t, j, value[:n])
	}
	return lsn
}

func captureStreamReserveTestTarget(t *testing.T, j *CommandJournal) *CapturedSync {
	t.Helper()
	target, err := j.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(target.Release)
	return target
}

func streamReserveTestStat(t *testing.T, path string) (os.FileInfo, int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		t.Fatal(err)
	}
	return info, int64(stat.Blocks) * 512
}

func TestGenerationStreamReserveExactTailAcrossRotation(t *testing.T) {
	dir := ext4StreamReserveTestDir(t)
	// Exercise a custom nonaligned target below the usual 8 MiB window.
	const ceiling = generationStreamReserveMinEOF + (6 << 20) + 123
	opts := CommandJournalOptions{PreferExt4Stream: true, SegmentTargetBytes: ceiling, CaptureStableResources: true}
	j := openStreamReserveTestJournal(t, dir, opts)
	t.Cleanup(func() { _ = j.Close() })
	initial := captureStreamReserveTestTarget(t, j)
	noReserve := func(*os.File, int64, int64) error {
		t.Error("ineligible or already reserved file reached allocation")
		return nil
	}
	if err := initial.syncGenerationWAL(noReserve); err != nil {
		t.Fatal(err)
	}
	if info, blocks := streamReserveTestStat(t, initial.path); info.Size() != 0 || blocks != 0 {
		t.Fatalf("empty initial file size=%d allocated=%d", info.Size(), blocks)
	}
	initial.Release()

	growStreamReserveTestJournal(t, j, generationStreamReserveMinEOF-1)
	below := captureStreamReserveTestTarget(t, j)
	if below.Bytes != generationStreamReserveMinEOF-1 {
		t.Fatalf("below-gate capture=%d", below.Bytes)
	}
	if err := below.syncGenerationWAL(noReserve); err != nil {
		t.Fatal(err)
	}
	below.Release()
	first := appendStreamReserveTestFrame(t, j, []byte("first eligible append"))
	target := captureStreamReserveTestTarget(t, j)
	if target.LSN != first || target.Bytes <= generationStreamReserveMinEOF {
		t.Fatalf("unexpected eligible frontier bytes=%d lsn=%d", target.Bytes, target.LSN)
	}
	if err := target.Sync(); err != nil { // The generic original sync must not reserve.
		t.Fatal(err)
	}
	original, _ := streamReserveTestStat(t, target.path)
	if original.Size() != int64(target.Bytes) || target.active.streamReserve.end != 0 {
		t.Fatalf("ordinary captured sync changed bytes or reserved: EOF=%d capture=%d reserveEnd=%d", original.Size(), target.Bytes, target.active.streamReserve.end)
	}

	type plan struct {
		file         *os.File
		from, length int64
	}
	entered := make(chan plan, 1)
	release := make(chan struct{})
	done := make(chan error, 1)
	finished := make(chan struct{})
	defer func() {
		close(release)
		<-finished
	}()
	beforeSyncs := j.WriterDurabilityStats().FileSyncCalls
	go func() {
		defer close(finished)
		done <- target.syncGenerationWAL(func(file *os.File, from, length int64) error {
			entered <- plan{file: file, from: from, length: length}
			<-release
			return reserveStreamTail(file, from, length)
		})
	}()
	var planned plan
	select {
	case planned = <-entered:
	case err := <-done:
		t.Fatalf("eligible capture did not reserve: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("generation reservation was not reached")
	}
	info, err := planned.file.Stat()
	if err != nil || !os.SameFile(info, original) {
		t.Fatalf("reservation lost original inode: %v", err)
	}
	block := target.active.streamReserve.block
	wantStart := (int64(target.Bytes) + block - 1) / block * block
	wantEnd := int64(ceiling) / block * block
	if j.WriterDurabilityStats().FileSyncCalls != beforeSyncs+1 || planned.from != wantStart || planned.from+planned.length != wantEnd {
		t.Fatalf("order or bounds: syncs=%d previous=%d from=%d through=%d want %d..%d",
			j.WriterDurabilityStats().FileSyncCalls, beforeSyncs, planned.from, planned.from+planned.length, wantStart, wantEnd)
	}

	// Advance the original live EOF into the planned range, then retire this
	// inode while the captured worker still owns the same writable descriptor.
	writeDone := make(chan error, 1)
	go func() {
		lsn, err := j.AppendRawKVPointCommandTrustedAndFlush(0, RawKVOpSet, []byte("newer"), make([]byte, 128<<10), false)
		if err == nil && lsn != first+1 {
			t.Errorf("newer LSN=%d want %d", lsn, first+1)
		}
		if err == nil {
			err = j.RotateActiveSegment(false)
		}
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("append or rotation waited for the optional I/O")
	}
	retired, oldAllocated := streamReserveTestStat(t, target.path)
	if !os.SameFile(retired, original) || retired.Size() <= planned.from {
		t.Fatal("the retained inode did not receive foreground writes within the planned range")
	}
	release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	after, allocated := streamReserveTestStat(t, target.path)
	if !os.SameFile(retired, after) || after.Size() != retired.Size() || allocated-oldAllocated < 4<<20 {
		t.Fatalf("wrong exact file or EOF: retired=%d after=%d extra=%d", retired.Size(), after.Size(), allocated-oldAllocated)
	}
	if target.Bytes != uint64(original.Size()) || target.LSN != first {
		t.Fatal("reservation changed capture authority")
	}
	if err := target.syncGenerationWAL(noReserve); err != nil {
		t.Fatal(err)
	}
	target.Release()

	empty := captureStreamReserveTestTarget(t, j)
	if err := empty.syncGenerationWAL(noReserve); err != nil {
		t.Fatal(err)
	}
	if info, blocks := streamReserveTestStat(t, empty.path); info.Size() != 0 || blocks != 0 || empty.path == target.path {
		t.Fatalf("new successor size=%d allocated=%d path=%s", info.Size(), blocks, empty.path)
	}
	empty.Release()
	if lsn := appendStreamReserveTestFrame(t, j, []byte("successor")); lsn != first+2 {
		t.Fatalf("successor LSN=%d want %d", lsn, first+2)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Existing stream framing and all LSNs survive the stock open/scan path.
	j = openStreamReserveTestJournal(t, dir, CommandJournalOptions{})
	t.Cleanup(func() { _ = j.Close() })
	if j.NextLSN() != first+3 || j.writer.fixed != nil {
		t.Fatalf("reopened LSN=%d fixed=%t", j.NextLSN(), j.writer.fixed != nil)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationStreamReserveRefusalsAndHardError(t *testing.T) {
	dir := ext4StreamReserveTestDir(t)
	opts := CommandJournalOptions{PreferExt4Stream: true, SegmentTargetBytes: 256 << 20}
	j := openStreamReserveTestJournal(t, dir, opts)
	growStreamReserveTestJournal(t, j, generationStreamReserveMinEOF)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		failure   error
		hard      bool
		preferred bool
		wantCalls int
	}{
		{preferred: false},
		{failure: unix.ENOSYS, preferred: true, wantCalls: 1},
		{failure: unix.EOPNOTSUPP, preferred: true, wantCalls: 1},
		{failure: unix.ENOSPC, preferred: true, wantCalls: 1},
		{failure: unix.EDQUOT, preferred: true, wantCalls: 1},
		{failure: unix.EIO, hard: true, preferred: true, wantCalls: 1},
	} {
		name := "unpreferred"
		if tc.failure != nil {
			name = tc.failure.Error()
		}
		t.Run(name, func(t *testing.T) {
			opts := opts
			opts.PreferExt4Stream = tc.preferred
			poisoned := 0
			opts.OnFileSyncError = func() { poisoned++ }
			j := openStreamReserveTestJournal(t, dir, opts)
			t.Cleanup(func() { _ = j.Close() })
			calls := 0
			attempts := 2
			if tc.hard {
				attempts = 1
			}
			for range attempts {
				target := captureStreamReserveTestTarget(t, j)
				if target.Bytes != generationStreamReserveMinEOF || j.writer.fixed != nil {
					t.Fatalf("fixture is not a reopened stream at the exact gate: %+v", target)
				}
				err := target.syncGenerationWAL(func(f *os.File, offset, _ int64) error {
					calls++
					if errors.Is(tc.failure, unix.ENOSPC) {
						// A capacity error can leave partial allocation. Make that
						// happen before returning the injected optional refusal.
						if err := reserveStreamTail(f, offset, 4096); err != nil {
							return err
						}
					}
					return tc.failure
				})
				target.Release()
				if tc.hard && !errors.Is(err, tc.failure) || !tc.hard && err != nil {
					t.Fatalf("generation sync error=%v", err)
				}
			}
			wantPoison := 0
			if tc.hard {
				wantPoison = 1
			}
			if calls != tc.wantCalls || poisoned != wantPoison {
				t.Fatalf("reservation calls=%d poisoned=%d", calls, poisoned)
			}
			if tc.hard {
				if _, err := j.CaptureSync(nil); !errors.Is(err, tc.failure) {
					t.Fatalf("capture after optional hard I/O error=%v", err)
				}
				if _, err := j.AppendCommand(fixedWALTestBarrier()); !errors.Is(err, tc.failure) {
					t.Fatalf("append after optional hard I/O error=%v", err)
				}
			}
			err := j.Close()
			if tc.hard && !errors.Is(err, tc.failure) || !tc.hard && err != nil {
				t.Fatalf("checked Close error=%v", err)
			}
		})
	}
}

func TestGenerationStreamReserveTailRepairDiscardsAllocation(t *testing.T) {
	dir := ext4StreamReserveTestDir(t)
	opts := CommandJournalOptions{PreferExt4Stream: true, SegmentTargetBytes: 256 << 20}
	j := openStreamReserveTestJournal(t, dir, opts)
	t.Cleanup(func() { _ = j.Close() })
	last := growStreamReserveTestJournal(t, j, generationStreamReserveMinEOF)
	target := captureStreamReserveTestTarget(t, j)
	if err := target.SyncGenerationWAL(); err != nil {
		t.Fatal(err)
	}
	target.Release()
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Append only a prefix of a valid next frame. Creating the tear without
	// truncate keeps the actual unused ext4 backing until normal recovery.
	src := filepath.Join(t.TempDir(), "frame")
	w, err := NewWriter(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRawKVPointCommandDirectTrusted(last+1, 0, RawKVOpSet, []byte("torn"), make([]byte, 1024)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	frame, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(target.path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.Write(frame[:len(frame)-8])
	if err := errors.Join(writeErr, f.Close()); err != nil {
		t.Fatal(err)
	}
	torn, before := streamReserveTestStat(t, target.path)
	if torn.Size() <= int64(target.Bytes) || before <= torn.Size()+(4<<20) {
		t.Fatalf("fixture lacks reserved, torn tail: size=%d allocated=%d", torn.Size(), before)
	}

	j = openStreamReserveTestJournal(t, dir, CommandJournalOptions{})
	t.Cleanup(func() { _ = j.Close() })
	repaired, after := streamReserveTestStat(t, target.path)
	if !os.SameFile(torn, repaired) || repaired.Size() != int64(target.Bytes) || j.NextLSN() != last+1 || before-after < 4<<20 {
		t.Fatalf("repair bytes=%d want=%d released=%d nextLSN=%d", repaired.Size(), target.Bytes, before-after, j.NextLSN())
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}
