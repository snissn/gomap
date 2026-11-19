//go:build linux

package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

type retiredWALFixture struct {
	db     *DB
	opts   Options
	keys   [][]byte
	value  []byte
	paths  []string
	info   []os.FileInfo
	active string
	closed bool
}

func retiredWALOptions(dir string) Options {
	opts := Options{
		Dir: dir, CommandWAL: true, DisableBackgroundPrune: true,
		Durability: DurabilityWALOnRelaxed, ResolvedProfile: ProfileCommandWALRelaxed,
		MemtableMode: "cow_btree", FreshRelaxedStandaloneWALStream: true,
		CommandWALSegmentTargetBytes: 128 << 20,
	}
	opts.ValueLog.PointerThreshold = 4096
	return opts
}

func requireRetiredWALExt4(t *testing.T, dir string) {
	t.Helper()
	mount, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(mount)
	if err := mount.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires ext4")
	}
}

func seedRetiredWALFixture(t *testing.T, dir string, count, rounds, segments int) *retiredWALFixture {
	t.Helper()
	requireRetiredWALExt4(t, dir)
	opts := retiredWALOptions(dir)
	database, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	f := &retiredWALFixture{db: database, opts: opts, keys: make([][]byte, count), paths: make([]string, segments), info: make([]os.FileInfo, segments)}
	t.Cleanup(func() {
		if !f.closed {
			f.close(t)
		}
	})
	if c := database.commandJournal.WriterBufferStats().Container; c != "stream" {
		t.Fatalf("expected original ext4 stream, got %q", c)
	}
	for i := range f.keys {
		f.keys[i] = []byte(fmt.Sprintf("seed-%05d", i))
	}
	f.value = bytes.Repeat([]byte("retirement"), 116)
	for segment := range f.paths {
		f.paths[segment], _ = database.commandWALActiveSegmentSnapshot()
		for i := range rounds {
			f.value[0] = byte(segment*rounds + i)
			batch := database.NewBatch()
			for _, key := range f.keys {
				if err := batch.Set(key, f.value); err != nil {
					_ = batch.Close()
					t.Fatal(err)
				}
			}
			write := batch.Write
			if i == rounds-1 {
				write = batch.WriteSync
			}
			if err := errors.Join(write(), batch.Close()); err != nil {
				t.Fatal(err)
			}
		}
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
		if f.info[segment], err = os.Stat(f.paths[segment]); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.rootPublication.coordinator.WaitThrough(t.Context(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
		t.Fatalf("expected deferred original WAL debt, pending=%t err=%v", pending, err)
	}
	proof, err := database.captureDurableWALCleanupProofV1()
	if err != nil || proof.rootCount != 2 || proof.cleanupThrough != database.State().AppliedCommandLSN {
		t.Fatalf("both persistent roots must cover seeded commands, roots=%d covered=%d err=%v", proof.rootCount, proof.cleanupThrough, err)
	}
	f.active = proof.journal.ActivePath
	if slices.Contains(f.paths, f.active) || proof.journal.FixedContainer {
		t.Fatal("fixture lost its distinct active stream")
	}
	paths, err := filepath.Glob(filepath.Join(WALDirPath(dir), "commit-l*.log"))
	if err != nil || len(paths) != len(f.paths)+1 {
		t.Fatalf("unexpected original namespace %v err=%v", paths, err)
	}
	return f
}

func (f *retiredWALFixture) close(t *testing.T) {
	t.Helper()
	f.closed = true
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
}

func (f *retiredWALFixture) verify(t *testing.T, updated map[string][]byte) {
	t.Helper()
	reopened, err := OpenForNativeSemanticOwner(f.opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, key := range f.keys {
		if _, replaced := updated[string(key)]; !replaced {
			assertRetiredWALValue(t, reopened.Get, key, f.value)
		}
	}
	for key, want := range updated {
		assertRetiredWALValue(t, reopened.Get, []byte(key), want)
	}
}

func assertRetiredWALValue(t *testing.T, get func([]byte) ([]byte, error), key, want []byte) {
	t.Helper()
	got, err := get(key)
	if err != nil || !bytes.Equal(got, want) {
		t.Errorf("key %q matches=%t err=%v", key, bytes.Equal(got, want), err)
	}
}

func matchingRetiredFileHandles(t *testing.T, info os.FileInfo) int {
	t.Helper()
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, fd := range fds {
		if current, err := os.Stat(filepath.Join("/proc/self/fd", fd.Name())); err == nil && os.SameFile(info, current) {
			count++
		}
	}
	return count
}

func waitRetiredWALTest[T any](t *testing.T, done <-chan T, message string) T {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(10 * time.Second):
		t.Fatal(message)
		var zero T
		return zero
	}
}

func TestRetiredExt4WALCloseAllowsPublicWrites(t *testing.T) {
	f := seedRetiredWALFixture(t, t.TempDir(), 32, 3, 2)
	held := f.db.AcquireSnapshot()
	if held == nil {
		t.Fatal("no original root to hold")
	}
	defer func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	closeEntered, release := make(chan *os.File, 1), make(chan struct{})
	var releaseOnce, enterOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseClose()
	f.db.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{beforeCloseRetired: func(file *os.File) {
		enterOnce.Do(func() {
			closeEntered <- file
			<-release
		})
	}}
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- cleanupDeferredCommandWALPrefixForTest(f.db) }()
	file := waitRetiredWALTest(t, closeEntered, "no production retirement descriptor to close")
	identity, err := file.Stat()
	if err != nil || !os.SameFile(identity, f.info[0]) || identity.Sys().(*syscall.Stat_t).Nlink != 0 {
		t.Fatalf("checked close must own the original unlinked inode: %v", err)
	}
	for i, path := range f.paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) || matchingRetiredFileHandles(t, f.info[i]) != 1 {
			t.Errorf("predecessor %d: expected namespace removal and exactly one live handle: %v", i, err)
		}
	}
	if pending := f.db.commandWALCleanupUnlinkedPending.Load(); pending != 2 || !f.db.commandWALCleanupNamespaceDirty.Load() || f.db.commandWALCleanupRemoved.Load() != 0 {
		t.Errorf("hold before directory sync must record two pending mutations, pending=%d", pending)
	}
	if !f.db.commandJournal.AppendLockAvailableForTesting() {
		t.Error("live close is holding the command WAL append lock")
	}
	initialLSN := f.db.CommandWALNextLSN()
	updates := make(map[string][]byte, 4)
	writes := make(chan error, 4)
	for i := range 4 {
		key, value := f.keys[i], []byte(fmt.Sprintf("new-%d", i))
		updates[string(key)] = value
		go func() {
			if i == 0 {
				writes <- f.db.SetSync(key, value)
			} else {
				writes <- f.db.Set(key, value)
			}
		}()
	}
	for range 4 {
		if err := waitRetiredWALTest(t, writes, "public write waited for checked retirement close"); err != nil {
			t.Fatal(err)
		}
	}
	if f.db.CommandWALNextLSN() <= initialLSN {
		t.Fatal("no typed command WAL progress during held reclamation")
	}
	for key, value := range updates {
		assertRetiredWALValue(t, f.db.Get, []byte(key), value)
		assertRetiredWALValue(t, held.Get, []byte(key), f.value)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	closeDone := make(chan error, 1)
	go func() { closeDone <- f.db.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	stack := make([]byte, 1<<20)
	for {
		bodies := strings.Split(string(stack[:runtime.Stack(stack, true)]), "\n\n")
		if slices.ContainsFunc(bodies, func(body string) bool {
			return strings.Contains(body, "(*DB).closeAfterHooks(") && strings.Contains(body, "sync.(*Mutex).Lock")
		}) {
			break
		}
		select {
		case err := <-closeDone:
			t.Fatalf("DB Close completed during owned reclamation: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("DB Close did not enter the maintenance ownership gate")
		}
		runtime.Gosched()
	}
	releaseClose()
	if err := waitRetiredWALTest(t, cleanupDone, "cleanup did not complete after releasing checked close"); err != nil {
		t.Fatal(err)
	}
	f.closed = true
	if err := waitRetiredWALTest(t, closeDone, "DB Close did not wait through retirement"); err != nil {
		t.Fatal(err)
	}
	for _, info := range f.info {
		if matchingRetiredFileHandles(t, info) != 0 {
			t.Error("checked DB Close left an original inode open")
		}
	}
	f.verify(t, updates)
	t.Log("three relaxed and one synchronous foreground write completed during the held close; concurrent DB Close drained descriptor and directory ownership")
}

func TestRetiredExt4WALCloseFailureAndPartialUnlink(t *testing.T) {
	for _, name := range []string{"checked_close", "post_unlink"} {
		t.Run(name, func(t *testing.T) {
			f := seedRetiredWALFixture(t, t.TempDir(), 32, 3, 2)
			injected := errors.New("after first inspected exact unlink")
			wantErr, unlinked := injected, uint64(1)
			if name == "checked_close" {
				wantErr, unlinked = os.ErrClosed, 2
				f.db.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
					beforeCloseRetired: func(file *os.File) {
						if file.Name() == f.paths[0] {
							if err := file.Close(); err != nil {
								t.Errorf("injection could not close real file: %v", err)
							}
						}
					},
				}
			}
			var attemptedSync atomic.Bool
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Point == durabilitycut.BeforeDeletionDirectorySync {
					attemptedSync.Store(true)
				}
				if name == "post_unlink" && e.Point == durabilitycut.AfterWALOrAssetUnlink && e.Path == f.paths[0] {
					return injected
				}
				return nil
			})
			complete, err := f.db.CleanupCommandWALCoveredPrefix()
			restore()
			f.db.testCommandWALCleanupHooks = nil
			if complete || !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, wantErr) {
				t.Fatalf("complete=%t err=%v, want ErrRecoveryRequired wrapping %v", complete, err, wantErr)
			}
			if got := f.db.commandWALCleanupUnlinkedPending.Load(); got != unlinked || f.db.commandWALCleanupRemoved.Load() != 0 ||
				!f.db.commandWALCleanupNamespaceDirty.Load() || attemptedSync.Load() {
				t.Fatalf("pending=%d want=%d removed=%d dirty=%t attempted sync=%t", got, unlinked,
					f.db.commandWALCleanupRemoved.Load(), f.db.commandWALCleanupNamespaceDirty.Load(), attemptedSync.Load())
			}
			for i, path := range f.paths {
				info, err := os.Stat(path)
				if uint64(i) < unlinked {
					if !os.IsNotExist(err) {
						t.Errorf("unlinked predecessor still named: %s err=%v", path, err)
					}
				} else if err != nil || !os.SameFile(info, f.info[i]) {
					t.Errorf("unattempted predecessor lost identity: %s err=%v", path, err)
				}
				if matchingRetiredFileHandles(t, f.info[i]) != 0 {
					t.Errorf("failed cleanup leaked predecessor handle: %s", path)
				}
			}
			if complete, err := f.db.CleanupCommandWALCoveredPrefix(); err != nil || !complete {
				t.Fatalf("checked retry did not stabilize namespace: complete=%t err=%v", complete, err)
			}
			if pending, removed := f.db.commandWALCleanupUnlinkedPending.Load(), f.db.commandWALCleanupRemoved.Load(); pending != 0 ||
				removed != 2 || f.db.commandWALCleanupNamespaceDirty.Load() {
				t.Fatalf("checked retry did not drain pending=%d removed=%d dirty=%t", pending, removed, f.db.commandWALCleanupNamespaceDirty.Load())
			}
			f.close(t)
			f.verify(t, nil)
		})
	}
}

func TestRetiredExt4WALClosePreservesBatchCapAndExplicitCleanup(t *testing.T) {
	for _, test := range []struct {
		name     string
		segments int
	}{
		{"explicit", 2},
		{"over_cap", maxOpenClosedWALCleanupSegments + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := seedRetiredWALFixture(t, t.TempDir(), 32, 1, test.segments)
			var unlinked int
			var unexpectedRetained bool
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				i := slices.Index(f.paths, e.Path)
				if e.Point == durabilitycut.AfterWALOrAssetUnlink && i >= 0 {
					unlinked++
					unexpectedRetained = unexpectedRetained || matchingRetiredFileHandles(t, f.info[i]) != 0
				}
				return nil
			})
			var err error
			if test.name == "explicit" {
				err = f.db.CleanupCommandWALCoveredSegments(true)
			} else {
				err = cleanupDeferredCommandWALPrefixForTest(f.db)
			}
			restore()
			if err != nil || unlinked != test.segments || unexpectedRetained {
				t.Fatalf("baseline path did not retire all %d original files with no transfer: unlinked=%d retained=%t err=%v",
					test.segments, unlinked, unexpectedRetained, err)
			}
			f.close(t)
			f.verify(t, nil)
		})
	}
}

func TestRetiredExt4WALCloseUncleanReopen(t *testing.T) {
	const childDir = "TREEDB_TEST_RETIRED_CLOSE_CRASH_DIR"
	const crashPoint = "checked last close after two real unlinks and before directory sync"
	key, value := []byte("new-durable"), []byte("acked before crash")
	if dir := os.Getenv(childDir); dir != "" {
		f := seedRetiredWALFixture(t, dir, 32, 3, 2)
		if err := f.db.SetSync(key, value); err != nil {
			t.Fatal(err)
		}
		f.db.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
			beforeCloseRetired: func(file *os.File) {
				info, err := file.Stat()
				if err != nil || !os.SameFile(info, f.info[0]) || info.Sys().(*syscall.Stat_t).Nlink != 0 ||
					f.db.commandWALCleanupUnlinkedPending.Load() != 2 {
					t.Errorf("unexpected crash cut identity or namespace progress: %v", err)
					return
				}
				fmt.Fprintln(os.Stderr, crashPoint)
				if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
					t.Errorf("SIGKILL: %v", err)
				}
			},
		}
		err := cleanupDeferredCommandWALPrefixForTest(f.db)
		t.Fatalf("child passed the intended crash cut: %v", err)
	}
	dir := t.TempDir()
	requireRetiredWALExt4(t, dir)
	cmd := exec.Command(os.Args[0], "-test.run", "^TestRetiredExt4WALCloseUncleanReopen$", "-test.v")
	cmd.Env = append(os.Environ(), childDir+"="+dir)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL ||
		!bytes.Contains(out, []byte(crashPoint)) {
		t.Fatalf("child did not SIGKILL at exact original retirement cut: %v\n%s", err, out)
	}
	f := &retiredWALFixture{opts: retiredWALOptions(dir), keys: make([][]byte, 32), value: bytes.Repeat([]byte("retirement"), 116)}
	for i := range f.keys {
		f.keys[i] = []byte(fmt.Sprintf("seed-%05d", i))
	}
	f.value[0] = 5
	f.verify(t, map[string][]byte{string(key): value})
}
