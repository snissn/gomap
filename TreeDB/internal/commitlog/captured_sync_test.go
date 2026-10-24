package commitlog

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestCapturedSyncRetainsFormatAcrossMixedRotation(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("mixed fixed/stream rotation requires fixed-container creation")
	}
	for _, initialFixed := range []bool{false, true} {
		name := "stream_to_fixed"
		if initialFixed {
			name = "fixed_to_stream"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			journal, err := OpenCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 1024, CaptureStableResources: true})
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			rotate := func(fixed bool) {
				t.Helper()
				journal.writer.fixedTarget = 0
				if fixed {
					journal.writer.fixedTarget = 1024
				}
				rotation, err := journal.RotateActiveSegmentWithStableResources(false)
				if err != nil {
					t.Fatal(err)
				}
				rotation.Release()
			}
			if !initialFixed {
				rotate(false)
			}
			capture := func() *CapturedSync {
				t.Helper()
				if _, err := journal.AppendCommand(fixedWALTestBarrier()); err != nil {
					t.Fatal(err)
				}
				target, err := journal.CaptureSync(nil)
				if err != nil {
					t.Fatal(err)
				}
				return target
			}
			first := capture()
			defer first.Release()
			rotate(!initialFixed)
			second := capture()
			defer second.Release()
			if first.active.dataOnlySync != initialFixed || second.active.dataOnlySync == initialFixed {
				t.Fatal("capture format followed the mutable writer across rotation")
			}
			files := make(map[uint64]*os.File)
			for _, target := range []*CapturedSync{first, second} {
				if err := target.active.token.WithPinnedFile(func(file *os.File) error {
					files[target.LSN] = file
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			seen := make(map[uint64][]durabilitycut.Point)
			restore := sync.OnceFunc(durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource != durabilitycut.ResourceCommandWAL ||
					(event.Point != durabilitycut.BeforeDependencyFileSync && event.Point != durabilitycut.AfterDependencyFileSync) {
					return nil
				}
				if file, ok := files[event.LSN]; !ok || file != event.StablePath {
					t.Fatalf("physical sync witness changed exact captured handle/LSN: %+v", event)
				}
				seen[event.LSN] = append(seen[event.LSN], event.Point)
				return nil
			}))
			defer restore()
			before := journal.WriterDurabilityStats().FileSyncCalls
			// The old format remains authoritative after a successor is synced.
			for _, target := range []*CapturedSync{second, first} {
				if err := target.Sync(); err != nil {
					t.Fatal(err)
				}
				target.Release()
				points := seen[target.LSN]
				if len(points) != 2 || points[0] != durabilitycut.BeforeDependencyFileSync || points[1] != durabilitycut.AfterDependencyFileSync {
					t.Fatalf("captured LSN %d physical witnesses=%v", target.LSN, points)
				}
				frames, err := ScanCommandFramesV2(target.path, Options{})
				if err != nil || len(frames) != 1 || frames[0].LSN != target.LSN {
					t.Fatalf("captured history LSN=%d frames=%v err=%v", target.LSN, frames, err)
				}
			}
			restore()
			if got := journal.WriterDurabilityStats().FileSyncCalls - before; got != 2 {
				t.Fatalf("mixed-format physical sync calls=%d, want 2", got)
			}
			if journal.syncPins != 0 || first.active.refs != 0 {
				t.Fatal("mixed-format capture release retained a retired pin")
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenCommandJournal(dir, CommandJournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got := reopened.NextLSN(); got != second.LSN+1 {
				t.Fatalf("recovered next LSN=%d, want %d", got, second.LSN+1)
			}
		})
	}
}

func TestCapturedSyncPinsPrefixAcrossAppendRotationAndClose(t *testing.T) {
	dir := t.TempDir()
	journal, err := OpenCommandJournal(dir, CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	appendFrame := func() uint64 {
		t.Helper()
		lsn, err := journal.AppendCommand(CommandEnvelope{Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
		if err != nil {
			t.Fatal(err)
		}
		return lsn
	}
	first := appendFrame()
	target, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Release()
	capturedBytes := target.Bytes
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if got := appendFrame(); got != first+1 {
		t.Fatalf("successor LSN=%d", got)
	}
	if target.LSN != first || target.Bytes != capturedBytes {
		t.Fatal("capture frontier mutated after append/rotation")
	}
	closed := make(chan error, 1)
	go func() { closed <- journal.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close crossed live captured pin: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := target.Sync(); err != nil {
		t.Fatal(err)
	}
	target.Release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wake after release")
	}
	reopened, err := OpenCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.NextLSN(); got != first+2 {
		t.Fatalf("recovered nextLSN=%d", got)
	}
}

// Each capture must perform fresh physical I/O even while the active segment's
// exact handle is retained between captures. Captures retain distinct immutable
// frontiers and remain usable in either order after rotation.
func TestCapturedSyncSharedSegmentProgressAndOutOfOrderRelease(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	appendFrame := func() uint64 {
		t.Helper()
		lsn, err := journal.AppendCommand(CommandEnvelope{Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
		if err != nil {
			t.Fatal(err)
		}
		return lsn
	}
	firstLSN := appendFrame()
	first, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	secondLSN := appendFrame()
	second, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if first.LSN != firstLSN || second.LSN != secondLSN || second.Bytes <= first.Bytes {
		t.Fatal("capture did not retain distinct increasing complete prefixes")
	}
	if first.active != second.active {
		t.Fatal("same segment did not share its exact retained handle")
	}
	before := journal.WriterDurabilityStats().FileSyncCalls
	if err := second.Sync(); err != nil {
		t.Fatal(err)
	}
	second.Release()
	if err := first.Sync(); err != nil {
		t.Fatal(err)
	}
	first.Release()
	if got := journal.WriterDurabilityStats().FileSyncCalls - before; got != 2 {
		t.Fatalf("successive capture syncs=%d want 2; shared retention must not cache durability", got)
	}
	if journal.syncPins != 0 || journal.capturedFile.refs != 1 {
		t.Fatalf("released captures retain pins=%d refs=%d", journal.syncPins, journal.capturedFile.refs)
	}
	thirdLSN := appendFrame()
	third, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Release()
	oldHandle := third.active
	rotation, err := journal.RotateActiveSegmentWithStableResources(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if journal.capturedFile != nil || oldHandle.refs != 1 {
		t.Fatal("rotation failed to release only the journal's old-handle reference")
	}
	fourthLSN := appendFrame()
	fourth, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fourth.Release()
	if fourth.active == oldHandle || third.LSN != thirdLSN || fourth.LSN != fourthLSN {
		t.Fatal("rotation aliased capture identity/frontier")
	}
	if err := fourth.Sync(); err != nil {
		t.Fatal(err)
	}
	fourth.Release()
	if err := third.Sync(); err != nil {
		t.Fatal(err)
	}
	third.Release()
	if oldHandle.refs != 0 || journal.syncPins != 0 {
		t.Fatalf("retired handle leaked refs=%d pins=%d", oldHandle.refs, journal.syncPins)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if journal.capturedFile != nil {
		t.Fatal("Close retained active handle")
	}
}

func TestCapturedSyncFinalLegalLSN(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{InitialLSN: ^uint64(0) - 1, CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if got := journal.AppendedLSN(); got != ^uint64(0)-1 {
		t.Fatalf("initial prefix=%d", got)
	}
	lsn, err := journal.AppendCommand(CommandEnvelope{Kind: CommandKindRawKVBatch, Scope: CommandScopeRawKV, PayloadFormat: PayloadFormatRawKVBatchV1})
	if err != nil {
		t.Fatal(err)
	}
	if lsn != ^uint64(0) || journal.AppendedLSN() != lsn {
		t.Fatal("last legal LSN excluded from complete prefix")
	}
	target, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Release()
	if target.LSN != lsn {
		t.Fatal("capture omitted terminal LSN")
	}
	if err := target.Sync(); err != nil {
		t.Fatal(err)
	}
}
