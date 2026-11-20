//go:build linux

package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"golang.org/x/sys/unix"
)

func TestCommandWALRetirementPageAdvice(t *testing.T) {
	dir := commandWALRetirementTestMount(t)
	if dir == "" {
		return
	}
	opts := Options{
		Dir: dir, CommandWAL: true, DisableBackgroundPrune: true,
		ResolvedProfile: ProfileCommandWALRelaxed, Durability: DurabilityWALOnRelaxed,
		MemtableMode: "cow_btree", FreshRelaxedStandaloneWALStream: true,
	}
	opts.ValueLog.PointerThreshold = 4096
	database, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()

	var closed [2]string
	var sizes [2]int64
	key := []byte("covered")
	value := bytes.Repeat([]byte{'v'}, 3<<10)
	for i, writes := range [...]int{45000, 256} {
		closed[i], _ = database.commandWALActiveSegmentSnapshot()
		for range writes {
			if err := database.Set(key, value); err != nil {
				t.Fatal(err)
			}
		}
		if err := database.SetSync(key, []byte("before")); err != nil {
			t.Fatal(err)
		}
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(closed[i])
		if err != nil {
			t.Fatal(err)
		}
		sizes[i] = info.Size()
	}
	if sizes[0] < minCommandWALRetirementAdviceBytes || sizes[1] >= minCommandWALRetirementAdviceBytes {
		t.Fatalf("fixture did not straddle the per-file advice threshold: %v", sizes)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
		t.Fatalf("expected deferred cleanup debt: pending=%t err=%v", pending, err)
	}
	if err := database.SetSync([]byte("active"), []byte("preserved")); err != nil {
		t.Fatal(err)
	}
	active, _ := database.commandWALActiveSegmentSnapshot()

	// A known registry pin keeps the file and its cache on the original path.
	// Read the actual WAL through the descriptor used to bind the pin.
	walReader, err := os.Open(closed[0])
	if err != nil {
		t.Fatal(err)
	}
	var original [4096]byte
	if _, err := walReader.ReadAt(original[:], 0); err != nil {
		_ = walReader.Close()
		t.Fatal(err)
	}
	pin := pinCommandWALCleanupFile(t, database.commandJournal, walReader)
	calls := 0
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
		advisePages: func(*os.File, func() error) { calls++ },
	}
	complete, pinErr := database.CleanupCommandWALCoveredPrefix()
	database.testCommandWALCleanupHooks = nil
	pin.Release()
	if complete || pinErr != nil || calls != 0 {
		_ = walReader.Close()
		t.Fatalf("known pin: complete=%t err=%v advice=%d", complete, pinErr, calls)
	}
	// A pin can still appear during the hint. An open reader must read
	// identical contents after cache eviction, and the final deletion lease
	// must observe the newer pin or a stale root proof.
	for _, stale := range []bool{false, true} {
		frontier := database.commandWALDurableLSN.Load()
		var latePin *rootpublication.IdentityPin
		var scanReader *os.File
		database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
			advisePages: func(file *os.File, run func() error) {
				scanReader = file
				if !stale {
					latePin = pinCommandWALCleanupFile(t, database.commandJournal, walReader)
				}
				if err := run(); err != nil {
					t.Errorf("physical advice before refusal: %v", err)
				}
				var got [4096]byte
				if _, err := walReader.ReadAt(got[:], 0); err != nil || got != original {
					t.Errorf("held WAL reader changed after cache eviction: %v", err)
				}
				if stale {
					database.commandWALDurableLSN.Store(0)
				}
			},
		}
		removed := database.commandWALCleanupRemoved.Load()
		complete, err := database.CleanupCommandWALCoveredPrefix()
		database.commandWALDurableLSN.Store(frontier)
		database.testCommandWALCleanupHooks = nil
		latePin.Release()
		if scanReader == nil || complete || err != nil || database.commandWALCleanupRemoved.Load() != removed {
			t.Fatalf("late refusal (stale=%t): advice=%t complete=%t err=%v", stale, scanReader != nil, complete, err)
		}
		if _, err := scanReader.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("late refusal leaked original scan reader: %v", err)
		}
		for _, path := range closed {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("late refusal lost %s: %v", path, err)
			}
		}
	}
	if err := walReader.Close(); err != nil {
		t.Fatal(err)
	}

	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("no held snapshot")
	}
	defer held.Close()
	beforeBytes := database.commandWALCleanupBytes.Load()
	beforeFiles := database.commandWALCleanupRemoved.Load()
	beforeDebt := database.commandWALClosedBytes.Load()
	advising := make(chan struct{})
	allow := make(chan struct{})
	var advised *os.File
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
		advisePages: func(file *os.File, run func() error) {
			calls++
			if file.Name() != closed[0] || calls != 1 {
				t.Errorf("unexpected advice target: %s (call %d)", file.Name(), calls)
				return
			}
			advised = file
			if !database.commandJournal.AppendLockAvailableForTesting() {
				t.Error("journal mutex held at page disposal")
			}
			if !database.durablePublishMu.TryLock() {
				t.Error("durable-publication mutex held at page disposal")
			} else {
				database.durablePublishMu.Unlock()
			}
			close(advising)
			<-allow
			before, err := commandWALResidentPages(file, sizes[0])
			if err != nil || before == 0 {
				t.Errorf("no resident scanned pages: %d, %v", before, err)
			}
			if err := run(); err != nil {
				t.Errorf("physical advice failed: %v", err)
			}
			after, err := commandWALResidentPages(file, sizes[0])
			if err != nil || before == 0 || after > before/10 {
				t.Errorf("physical advice did not dispose the scanned cache: %d to %d, %v", before, after, err)
			}
			t.Logf("advised %d bytes on %s, resident pages %d to %d", sizes[0], filepath.Base(file.Name()), before, after)
		},
	}
	cleaned := make(chan error, 1)
	go func() { cleaned <- cleanupDeferredCommandWALPrefixForTest(database) }()
	select {
	case <-advising:
	case err := <-cleaned:
		t.Fatalf("deferred cleanup returned before page disposal: %v", err)
	case <-time.After(20 * time.Second):
		close(allow)
		if err := <-cleaned; err != nil {
			t.Logf("cleanup after missing advice: %v", err)
		}
		t.Fatal("page disposal was never reached")
	}
	foreground := make(chan error, 1)
	go func() {
		if got, err := held.Get(key); err != nil || string(got) != "before" {
			foreground <- fmt.Errorf("held snapshot: %q, %v", got, err)
			return
		}
		if err := database.SetSync(key, value); err != nil {
			foreground <- err
			return
		}
		got, err := database.Get(key)
		if err != nil || !bytes.Equal(got, value) {
			foreground <- fmt.Errorf("foreground read after SetSync: %q, %v", got, err)
			return
		}
		foreground <- nil
	}()
	var foregroundErr error
	select {
	case foregroundErr = <-foreground:
		close(allow)
	case <-time.After(10 * time.Second):
		close(allow)
		t.Error("foreground read and SetSync failed to finish while the cleanup advice was held")
		foregroundErr = <-foreground
	}
	cleanupErr := <-cleaned
	database.testCommandWALCleanupHooks = nil
	if err := errors.Join(foregroundErr, cleanupErr); err != nil {
		t.Fatal(err)
	}
	if _, err := advised.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("original scan reader did not close: %v", err)
	}
	if files, reclaimed := database.commandWALCleanupRemoved.Load()-beforeFiles, database.commandWALCleanupBytes.Load()-beforeBytes; files != 2 || reclaimed != uint64(sizes[0]+sizes[1]) || beforeDebt-database.commandWALClosedBytes.Load() != sizes[0]+sizes[1] {
		t.Fatalf("retirement files=%d bytes=%d debt=%d, want 2 files and %d retired/uncharged bytes",
			files, reclaimed, beforeDebt-database.commandWALClosedBytes.Load(), sizes[0]+sizes[1])
	}
	for _, path := range closed {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("covered segment still present: %s: %v", path, err)
		}
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("capture-active generation lost: %v", err)
	}
	if got, err := held.Get(key); err != nil || string(got) != "before" {
		t.Fatalf("held snapshot after retirement: %q, %v", got, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}

	// A store image taken before Close must retain the same typed state.
	crashDir := t.TempDir()
	if err := os.CopyFS(crashDir, os.DirFS(dir)); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	for _, reopenedDir := range []string{crashDir, dir} {
		reopenOpts := opts
		reopenOpts.Dir = reopenedDir
		reopened, err := OpenForNativeSemanticOwner(reopenOpts)
		if err != nil {
			t.Fatal(err)
		}
		covered, coveredErr := reopened.Get(key)
		active, activeErr := reopened.Get([]byte("active"))
		if !bytes.Equal(covered, value) || string(active) != "preserved" {
			t.Errorf("reopen %s: updated value length=%d, active value=%q", reopenedDir, len(covered), active)
		}
		if err := errors.Join(coveredErr, activeErr, reopened.Close()); err != nil {
			t.Fatal(err)
		}
	}
}

func commandWALRetirementTestMount(t *testing.T) string {
	t.Helper()
	const child = "GOMAP_TEST_EXT4_RETIREMENT_DATA"
	if dir := os.Getenv(child); dir != "" {
		return dir
	}
	wal := t.TempDir()
	file, err := os.Open(wal)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(file)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires an ext4 WAL location")
	}
	dir, err := os.MkdirTemp("/dev/shm", "gomap-retirement-data-")
	if err != nil {
		t.Skipf("separate data-volume fixture unavailable: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
	target := WALDirPath(dir)
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), "unshare", "-Urnm", "sh", "-c",
		`mount --make-rprivate / && mount --bind "$1" "$2" || exit 77
printf 'private WAL mount ready\n'
exec "$3" -test.run="^$4$" -test.v`, "gomap-mounted-wal", wal, target, binary, t.Name())
	command.Env = append(os.Environ(), child+"="+dir)
	output, err := command.CombinedOutput()
	t.Logf("isolated WAL mount:\n%s", output)
	if err != nil && !bytes.Contains(output, []byte("private WAL mount ready")) {
		t.Skipf("private bind mount unavailable: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return ""
}

func commandWALResidentPages(file *os.File, size int64) (int, error) {
	mapping, err := unix.Mmap(int(file.Fd()), 0, int(size), unix.PROT_NONE, unix.MAP_PRIVATE)
	if err != nil {
		return 0, err
	}
	pageSize := os.Getpagesize()
	pages := make([]byte, (len(mapping)+pageSize-1)/pageSize)
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&mapping[0])), uintptr(len(mapping)), uintptr(unsafe.Pointer(&pages[0])))
	closeErr := unix.Munmap(mapping)
	if errno != 0 {
		return 0, errors.Join(errno, closeErr)
	}
	count := 0
	for _, page := range pages {
		count += int(page & 1)
	}
	return count, closeErr
}
