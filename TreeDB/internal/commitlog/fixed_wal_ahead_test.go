package commitlog

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func aheadWait(t *testing.T, j *CommandJournal, predicate func(*fixedWALAheadSlot) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j.mu.Lock()
		ok := predicate(j.fixedAhead)
		j.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("ahead worker did not reach the expected state")
}

func aheadFill(t *testing.T, j *CommandJournal) (CommandEnvelope, int) {
	t.Helper()
	frame := fixedWALLifecycleFrame(t, 256<<10)
	for i := 0; i < 10; i++ {
		if _, err := j.AppendCommand(frame); err != nil {
			t.Fatal(err)
		}
		if err := j.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	return frame, 10
}

func aheadRotatePressure(t *testing.T, j *CommandJournal, frame CommandEnvelope) int {
	t.Helper()
	seq := j.segmentSeq
	for i := 1; i <= 32; i++ {
		if _, err := j.AppendCommand(frame); err != nil {
			t.Fatal(err)
		}
		if err := j.Flush(); err != nil {
			t.Fatal(err)
		}
		if j.segmentSeq != seq {
			return i
		}
	}
	t.Fatal("fixture did not reach real physical pressure")
	return 0
}

func TestFixedWALAheadReadyAdoptionAndCapturedPrefix(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, stable := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "stable"}[stable], func(t *testing.T) {
			dir := t.TempDir()
			j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: stable})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			frame, count := aheadFill(t, j)
			aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })
			j.mu.Lock()
			reserve := j.fixedAhead.reserve
			preparedInfo, err := reserve.file.Stat()
			preparedIdentity := reserve.state.identity
			predictedCapacity := reserve.state.capacity
			j.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			oldPath := j.Path()
			oldBytes, err := os.ReadFile(oldPath)
			if err != nil {
				t.Fatal(err)
			}
			capture, err := j.CaptureSync(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer capture.Release()
			// Count initializations only after readiness. Adoption must use the
			// existing reserve, not initialize/scan another full private file.
			oldWrite := fixedWALInitializeWrite
			var writes atomic.Int64
			fixedWALInitializeWrite = func(f io.WriterAt, p []byte, off int64) error { writes.Add(1); return oldWrite(f, p, off) }
			defer func() { fixedWALInitializeWrite = oldWrite }()
			count += aheadRotatePressure(t, j, frame)
			fixedWALInitializeWrite = oldWrite
			if writes.Load() != 0 {
				t.Fatal("ready adoption initialized another reserve")
			}
			currentInfo, err := j.writer.f.Stat()
			if err != nil || !os.SameFile(preparedInfo, currentInfo) || j.writer.fixed.identity != preparedIdentity || j.writer.fixed.capacity != predictedCapacity {
				t.Fatalf("adoption lost exact initialized identity: %v", err)
			}
			if filepath.Base(j.writer.f.Name()) != CommandSegmentName(0, 2) {
				t.Fatal("private diagnostic handle became authoritative")
			}
			after, err := os.ReadFile(oldPath)
			if err != nil || len(after) < int(capture.Bytes) || !bytes.Equal(oldBytes[:capture.Bytes], after[:capture.Bytes]) {
				t.Fatalf("old accepted bytes changed: %v", err)
			}
			if err = capture.Sync(); err != nil {
				t.Fatal(err)
			}
			capture.Release()
			if stable {
				rotations, e := j.TakePendingStableRotations()
				if e != nil || len(rotations) != 1 {
					t.Fatalf("pending exact rotation: count=%d err=%v", len(rotations), e)
				}
				rotation := rotations[0]
				defer rotation.Release()
				before, after := 0, 0
				e = rotation.Active.SyncThroughWithFileSyncObserver(func(file *os.File) error {
					before++
					info, e := file.Stat()
					if e != nil {
						return e
					}
					if !os.SameFile(info, preparedInfo) {
						return errors.New("active token sync lost adopted physical file")
					}
					return nil
				}, func(*os.File) error { after++; return nil })
				if e != nil || before != 1 || after != 1 {
					t.Fatalf("empty initialization incorrectly credited active content: before=%d after=%d err=%v", before, after, e)
				}
				rotation.Release()
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := openFixedTestCommandJournal(dir, CommandJournalOptions{CaptureStableResources: stable})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.NextLSN() != uint64(count+1) {
				t.Fatalf("reopen next=%d want=%d", reopened.NextLSN(), count+1)
			}
		})
	}
}

func TestFixedWALAheadInFlightFallbackAndConcurrentClose(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oldSync := fixedWALInitializeSync
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var aheadSyncs atomic.Int64
	fixedWALInitializeSync = func(f *os.File) error {
		if strings.Contains(filepath.Base(f.Name()), fixedWALAheadPrefix) {
			aheadSyncs.Add(1)
			once.Do(func() { close(entered) })
			<-release
		}
		return oldSync(f)
	}
	defer func() { unblock(); _ = j.Close(); fixedWALInitializeSync = oldSync }()
	frame, _ := aheadFill(t, j)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never entered sync")
	}
	j.mu.Lock()
	slot := j.fixedAhead
	reserveInfo, err := slot.reserve.file.Stat()
	parentHandle := slot.reserve.parent
	j.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	rotated := make(chan error, 1)
	go func() {
		for i := 0; i < 32; i++ {
			if _, e := j.AppendCommand(frame); e != nil {
				rotated <- e
				return
			}
			if e := j.Flush(); e != nil {
				rotated <- e
				return
			}
			if j.Path() != filepath.Join(j.walDir, CommandSegmentName(0, 1)) {
				rotated <- nil
				return
			}
		}
		rotated <- errors.New("no physical rotation")
	}()
	select {
	case err = <-rotated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		unblock()
		<-rotated
		t.Fatal("rotation waited for private sync")
	}
	j.mu.Lock()
	info, err := j.writer.f.Stat()
	if err != nil || os.SameFile(info, reserveInfo) || j.fixedAhead != slot || !slot.cancelled || slot.done {
		j.mu.Unlock()
		t.Fatal("miss did not retain one cancelled slot")
	}
	j.mu.Unlock()
	if aheadSyncs.Load() != 1 {
		t.Fatal("second ahead worker started during fallback")
	}
	closing := make(chan error, 2)
	go func() { closing <- j.Close() }()
	go func() { closing <- j.Close() }()
	select {
	case e := <-closing:
		t.Fatalf("Close escaped held worker: %v", e)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	for i := 0; i < 2; i++ {
		select {
		case e := <-closing:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Close failed to join")
		}
	}
	if _, err = parentHandle.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("retained parent not closed: %v", err)
	}
}

func TestFixedWALAheadFailurePoisonsBeforeLSN(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "partial_write"}[partial], func(t *testing.T) {
			j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			oldWrite, oldSync := fixedWALInitializeWrite, fixedWALInitializeSync
			defer func() { _ = j.Close(); fixedWALInitializeWrite, fixedWALInitializeSync = oldWrite, oldSync }()
			if partial {
				fixedWALInitializeWrite = func(f io.WriterAt, p []byte, off int64) error {
					file, ok := f.(*os.File)
					if ok && strings.Contains(filepath.Base(file.Name()), fixedWALAheadPrefix) {
						if len(p) > 1 {
							_, _ = file.WriteAt(p[:len(p)/2], off)
						}
						return syscall.ENOSPC
					}
					return oldWrite(f, p, off)
				}
			} else {
				fixedWALInitializeSync = func(f *os.File) error {
					if strings.Contains(filepath.Base(f.Name()), fixedWALAheadPrefix) {
						return syscall.ENOSPC
					}
					return oldSync(f)
				}
			}
			// Do not race another foreground entry against asynchronous failure:
			// start exactly once using the real flushed half-capacity boundary.
			frame := fixedWALLifecycleFrame(t, 256<<10)
			for j.writer.fixed.physical-fixedWALHeaderSize < (j.writer.fixed.capacity-fixedWALHeaderSize+1)/2 {
				if _, err = j.AppendCommand(frame); err != nil {
					t.Fatal(err)
				}
				if err = j.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = j.AppendCommand(frame); err != nil {
				t.Fatal(err)
			}
			aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.done })
			next := j.NextLSN()
			if _, err = j.AppendCommand(fixedWALTestBarrier()); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("append cause=%v", err)
			}
			if j.NextLSN() != next {
				t.Fatal("background failure consumed an LSN")
			}
			if err = j.Sync(); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("sync lost sticky error: %v", err)
			}
			if err = j.Close(); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("Close lost original error: %v", err)
			}
		})
	}
}

func TestFixedWALAheadResetConflictAndOrphan(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "forced_reset", true: "private_conflict"}[conflict], func(t *testing.T) {
			dir := t.TempDir()
			j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			frame, _ := aheadFill(t, j)
			aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })
			j.mu.Lock()
			reserve := j.fixedAhead.reserve
			name := reserve.name
			preparedInfo, err := reserve.file.Stat()
			j.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if conflict {
				// Replace only the private link. The exact initialized FD remains
				// held and may never confer custody over this foreign entry.
				if err = os.Rename(filepath.Join(dir, name), filepath.Join(dir, "retained-test-reserve")); err != nil {
					t.Fatal(err)
				}
				foreign := []byte("foreign private replacement")
				if err = os.WriteFile(filepath.Join(dir, name), foreign, 0600); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 32; i++ {
					_, err = j.AppendCommand(frame)
					if err != nil {
						break
					}
					if err = j.Flush(); err != nil {
						break
					}
				}
				if !errors.Is(err, rootpublication.ErrResourceConflict) {
					t.Fatalf("private replacement accepted: %v", err)
				}
				got, e := os.ReadFile(filepath.Join(dir, name))
				if e != nil || !bytes.Equal(got, foreign) {
					t.Fatalf("foreign link changed: %v", e)
				}
			} else {
				if err = j.RotateActiveSegment(false); err != nil {
					t.Fatal(err)
				}
				if j.writer.fixed.capacity != fixedWALDefaultCapacity {
					t.Fatal("forced reset adopted grown prediction")
				}
				info, e := j.writer.f.Stat()
				if e != nil || os.SameFile(info, preparedInfo) {
					t.Fatal("forced reset reused ahead file")
				}
				if err = j.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err = os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unused reserve survived reset/Close: %v", err)
				}
				// Model an actual interrupted unpublished creation, not a fake
				// canonical frame. Next exclusive owner must remove it durably.
				orphan := filepath.Join(dir, fixedWALAheadPrefix+strings.Repeat("a", 32))
				if err = os.WriteFile(orphan, []byte("partial private reserve"), 0600); err != nil {
					t.Fatal(err)
				}
				reopened, e := openFixedTestCommandJournal(dir, CommandJournalOptions{})
				if e != nil {
					t.Fatal(e)
				}
				if _, e = os.Stat(orphan); !errors.Is(e, os.ErrNotExist) {
					t.Fatalf("orphan not removed: %v", e)
				}
				if e = reopened.Close(); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}

// This child exits without unwinding any private/canonical cleanup or Close.
// The real initializer and namespace operations supply every on-disk byte.
func TestFixedWALAheadCrashHelper(t *testing.T) {
	dir := os.Getenv("GOMAP_AHEAD_CRASH_DIR")
	if dir == "" {
		return
	}
	mode := os.Getenv("GOMAP_AHEAD_CRASH_MODE")
	j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{OnSegmentRotated: func(int64) {
		if mode == "installed" {
			os.Exit(73)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	frame := fixedWALLifecycleFrame(t, 256<<10)
	seed := 0
	for j.writer.fixed.physical-fixedWALHeaderSize < (j.writer.fixed.capacity-fixedWALHeaderSize+1)/2 {
		if _, err = j.AppendCommand(frame); err != nil {
			t.Fatal(err)
		}
		seed++
		if err = j.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.WriteFile(filepath.Join(dir, "accepted-prefix-count"), []byte(strconv.Itoa(seed)), 0600); err != nil {
		t.Fatal(err)
	}
	oldWrite, oldSync := fixedWALInitializeWrite, fixedWALInitializeSync
	fixedWALInitializeWrite = func(file io.WriterAt, p []byte, off int64) error {
		f, ok := file.(*os.File)
		if mode == "partial" && ok && strings.Contains(filepath.Base(f.Name()), fixedWALAheadPrefix) && off > 0 {
			if _, e := file.WriteAt(p[:min(len(p), 97)], off); e != nil {
				t.Fatal(e)
			}
			os.Exit(73)
		}
		return oldWrite(file, p, off)
	}
	fixedWALInitializeSync = func(f *os.File) error {
		if e := oldSync(f); e != nil {
			return e
		}
		if mode == "private_sync" && strings.Contains(filepath.Base(f.Name()), fixedWALAheadPrefix) {
			os.Exit(73)
		}
		return nil
	}
	var renamed atomic.Bool
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Namespace == durabilitycut.NamespaceRename && strings.HasPrefix(e.StableOldName, fixedWALAheadPrefix) {
			renamed.Store(true)
			if mode == "rename" {
				os.Exit(73)
			}
		}
		if renamed.Load() && e.Point == durabilitycut.AfterNewFileDirectorySync && mode == "directory_sync" {
			os.Exit(73)
		}
		return nil
	})
	defer restore()
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = j.Flush(); err != nil {
		t.Fatal(err)
	}
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })
	if mode == "ready" {
		os.Exit(73)
	}
	aheadRotatePressure(t, j, frame)
	t.Fatal("did not reach selected actual crash cut")
}

func TestFixedWALAheadCrashRecovery(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, mode := range []string{"partial", "private_sync", "ready", "rename", "directory_sync", "installed"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestFixedWALAheadCrashHelper$")
			cmd.Env = append(os.Environ(), "GOMAP_AHEAD_CRASH_DIR="+dir, "GOMAP_AHEAD_CRASH_MODE="+mode)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("helper: %v\n%s", err, output)
			}
			seedBytes, err := os.ReadFile(filepath.Join(dir, "accepted-prefix-count"))
			if err != nil {
				t.Fatal(err)
			}
			seed, err := strconv.Atoi(string(seedBytes))
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := openFixedTestCommandJournal(dir, CommandJournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			paths, err := filepath.Glob(filepath.Join(dir, "commit-l0-*.log"))
			if err != nil {
				t.Fatal(err)
			}
			next := uint64(1)
			for _, path := range paths {
				frames, e := ScanCommandFramesV2(path, Options{})
				if e != nil {
					t.Fatal(e)
				}
				for _, frame := range frames {
					if frame.LSN != next {
						t.Fatalf("lost/reordered accepted prefix: got=%d want=%d", frame.LSN, next)
					}
					next++
				}
			}
			if next <= uint64(seed) || reopened.NextLSN() != next {
				t.Fatalf("reopen frontier=%d replay_next=%d seed=%d", reopened.NextLSN(), next, seed)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if isFixedWALAheadName(entry.Name()) {
					t.Fatal("exclusive reopen retained private orphan")
				}
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

func TestFixedWALAheadOrphanMultiplicityAndForeignLink(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "multiplicity", true: "symlink"}[foreign], func(t *testing.T) {
			dir := t.TempDir()
			name := filepath.Join(dir, fixedWALAheadPrefix+strings.Repeat("a", 32))
			if foreign {
				target := filepath.Join(t.TempDir(), "foreign")
				if err := os.WriteFile(target, []byte("unrelated"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, name); err != nil {
					t.Fatal(err)
				}
			} else {
				for _, suffix := range []string{strings.Repeat("a", 32), strings.Repeat("b", 32)} {
					if err := os.WriteFile(filepath.Join(dir, fixedWALAheadPrefix+suffix), []byte("partial"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if j, err := openFixedTestCommandJournal(dir, CommandJournalOptions{}); err == nil {
				_ = j.Close()
				t.Fatal("unsafe orphan set accepted")
			}
			if _, err := os.Lstat(name); err != nil {
				t.Fatalf("unowned orphan entry removed: %v", err)
			}
		})
	}
}

func TestFixedWALAheadAfterSyncFailureRetainsCause(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.AfterDependencyFileSync && strings.HasPrefix(filepath.Base(e.Path), fixedWALAheadPrefix) {
			if e.StablePath == nil {
				return errors.New("ahead sync observation lost actual retained file")
			}
			return syscall.EIO
		}
		return nil
	})
	defer func() { _ = j.Close(); restore() }()
	frame := fixedWALLifecycleFrame(t, 256<<10)
	for j.writer.fixed.physical-fixedWALHeaderSize < (j.writer.fixed.capacity-fixedWALHeaderSize+1)/2 {
		if _, err = j.AppendCommand(frame); err != nil {
			t.Fatal(err)
		}
		if err = j.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.done })
	next := j.NextLSN()
	if _, err = j.AppendCommand(fixedWALTestBarrier()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("AFTER-sync failure lost: %v", err)
	}
	if j.NextLSN() != next {
		t.Fatal("AFTER-sync failure consumed LSN")
	}
	if err = j.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Close cause=%v", err)
	}
}

func TestFixedWALAheadRepeatedUnusedResourceRelease(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed creation unsupported")
	}
	j, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for cycle := 0; cycle < 3; cycle++ {
		aheadFill(t, j)
		aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s != nil && s.ready })
		j.mu.Lock()
		generation := j.fixedAhead.generation
		file, parent := j.fixedAhead.reserve.file, j.fixedAhead.reserve.parent
		name := j.fixedAhead.reserve.name
		j.mu.Unlock()
		if generation != uint64(cycle+1) {
			t.Fatalf("unexpected extra worker: generation=%d cycle=%d", generation, cycle)
		}
		if err = j.RotateActiveSegment(false); err != nil {
			t.Fatal(err)
		}
		aheadWait(t, j, func(s *fixedWALAheadSlot) bool { return s == nil || s.done })
		if _, err = file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("unused reserve descriptor survived join: %v", err)
		}
		if _, err = parent.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("unused parent duplicate survived join: %v", err)
		}
		if _, err = os.Stat(filepath.Join(j.walDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unused reserve survived: %v", err)
		}
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
}
