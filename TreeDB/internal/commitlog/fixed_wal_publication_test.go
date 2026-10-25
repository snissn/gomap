package commitlog

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
)

// The helper exits without unwinding initialization or journal cleanup.
func TestFixedWALInitializationCrashHelper(t *testing.T) {
	dir := os.Getenv("GOMAP_FIXED_WAL_CRASH_DIR")
	if dir == "" {
		return
	}
	mode := os.Getenv("GOMAP_FIXED_WAL_CRASH_MODE")
	original := fixedWALInitializeWrite
	writes := 0
	fixedWALInitializeWrite = func(file io.WriterAt, p []byte, offset int64) error {
		writes++
		if writes == 2 {
			if _, err := file.WriteAt(p[:min(len(p), 97)], offset); err != nil {
				t.Fatal(err)
			}
			os.Exit(73)
		}
		return original(file, p, offset)
	}
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	if mode == "existing_empty_successor_growth" {
		j.writer.fixedIncomingBytes = 32 << 10
		plan, planErr := j.writer.fixedSuccessorSizePlan()
		if planErr != nil {
			t.Fatal(planErr)
		}
		_, _, _, err = j.writer.prepareFixedSuccessor(&j.writer.f, nil, plan)
		if err != nil {
			t.Fatal(err)
		}
	}
	if mode == "empty_growth" {
		_, err = j.AppendCommand(fixedWALLifecycleFrame(t, 32<<10))
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("did not reach initialization crash")
}

func TestFixedWALInitializationCrashRecovery(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container publication is unsupported on this platform")
	}
	for _, mode := range []string{"fresh", "empty_growth", "existing_empty_successor_growth"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			if mode != "fresh" {
				j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256})
				if err != nil {
					t.Fatal(err)
				}
				if err = j.Close(); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestFixedWALInitializationCrashHelper$")
			cmd.Env = append(os.Environ(), "GOMAP_FIXED_WAL_CRASH_DIR="+dir, "GOMAP_FIXED_WAL_CRASH_MODE="+mode)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("helper: %v\n%s", err, output)
			}
			reopened, err := openFixedTestCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256})
			if err != nil {
				t.Fatalf("reopen after unaccepted initialization crash: %v", err)
			}
			defer reopened.Close()
			if reopened.NextLSN() != 1 {
				t.Fatalf("next LSN=%d", reopened.NextLSN())
			}
			if _, err = reopened.AppendCommand(fixedWALTestBarrier()); err != nil {
				t.Fatal(err)
			}
			if err = reopened.Sync(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFixedWALReopenLargerTargetRotatesAtPhysicalCapacity(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container publication is unsupported on this platform")
	}
	for _, stable := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "stable"}[stable], func(t *testing.T) {
			dir := t.TempDir()
			j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256, CaptureStableResources: stable})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
				t.Fatal(err)
			}
			if err = j.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			j, err = openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: stable})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			initialSeq := j.segmentSeq
			for i := 0; i < 40; i++ {
				if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
				if err = j.Sync(); err != nil {
					t.Fatalf("sync %d: %v", i, err)
				}
				fixedWALDrainRotations(t, j)
			}
			if j.segmentSeq == initialSeq {
				t.Fatal("physical capacity did not rotate")
			}
			if j.NextLSN() != 42 {
				t.Fatalf("next LSN=%d", j.NextLSN())
			}
		})
	}
}

func TestCommandJournalCleanupScanKeepsListingAndReaderPrefix(t *testing.T) {
	j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{})
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
	snapshot, err := j.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	started := make(chan struct{})
	err = j.WithCleanupScan(snapshot, func() error {
		// Start a real flushed append between the listing and reader phases. It
		// must stay excluded through both complete reserve-validation passes.
		go func() {
			close(started)
			_, appendErr := j.AppendCommand(fixedWALTestBarrier())
			if appendErr == nil {
				appendErr = j.Sync()
			}
			done <- appendErr
		}()
		<-started
		logical, sizeErr := LogicalFileSize(j.writer.f)
		if sizeErr != nil {
			return sizeErr
		}
		select {
		case appendErr := <-done:
			t.Fatalf("append escaped cleanup scan authority: %v", appendErr)
		case <-time.After(20 * time.Millisecond):
		}
		reader, readErr := NewReader(j.path)
		if readErr != nil {
			return readErr
		}
		defer reader.Close()
		env, readErr := reader.ReadCommandFrameV2()
		if readErr != nil || env.LSN != 1 {
			t.Fatalf("first frame: %+v %v", env, readErr)
		}
		if _, readErr = reader.ReadCommandFrameV2(); readErr != io.EOF {
			t.Fatalf("reader crossed captured prefix: %v", readErr)
		}
		if logical != snapshot.ActiveBytes {
			t.Fatalf("logical listing=%d writer=%d", logical, snapshot.ActiveBytes)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("append did not resume after cleanup scan")
	}
	frames, err := ScanCommandFramesV2(j.path, Options{})
	if err != nil || len(frames) != 2 {
		t.Fatalf("resumed frames=%d err=%v", len(frames), err)
	}
}

func TestCommandJournalStableRotationToStreamPreservesFrontier(t *testing.T) {
	j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{CaptureStableResources: true})
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
	// Exercise the supported stream successor even on fixed-capable platforms.
	j.writer.fixedBudget = 0
	rotation, err := j.RotateActiveSegmentWithStableResources(true)
	if err != nil {
		t.Fatal(err)
	}
	defer rotation.Release()
	if j.writer.fixed != nil {
		t.Fatal("rotation did not select stream successor")
	}
	if got := rotation.Active.Frontier().Bytes; got != 0 {
		t.Fatalf("empty stream frontier=%d", got)
	}
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	frames, err := ScanCommandFramesV2(j.path, Options{})
	if err != nil || len(frames) != 1 || frames[0].LSN != 2 {
		t.Fatalf("stream frames=%v err=%v", frames, err)
	}
}

func TestFixedWALReopenPhysicalCapacityIncludesBufferedFrames(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("requires a fixed-container reserve")
	}
	dir := t.TempDir()
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{SegmentTargetBytes: 256})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err = openFixedTestCommandJournal(dir, CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i := 0; i < 40; i++ {
		if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
			t.Fatalf("buffered append %d: %v", i, err)
		}
	}
	if err = j.Sync(); err != nil {
		t.Fatal(err)
	}
	segments, err := commandJournalSegments(dir, j.path)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, segment := range segments {
		frames, scanErr := ScanCommandFramesV2(segment.path, Options{})
		if scanErr != nil {
			t.Fatal(scanErr)
		}
		for _, frame := range frames {
			count++
			if frame.LSN != uint64(count) {
				t.Fatalf("LSN=%d want %d", frame.LSN, count)
			}
		}
	}
	if count != 41 {
		t.Fatalf("retained frames=%d", count)
	}
}
