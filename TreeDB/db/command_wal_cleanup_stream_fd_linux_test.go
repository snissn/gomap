//go:build linux

package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCommandWALDeferredExt4StreamFinalInventoryDescriptorExhaustion(t *testing.T) {
	const child = "GOMAP_WAL_STREAM_FINAL_INVENTORY_FD_CHILD"
	if os.Getenv(child) != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, executable, "-test.run=^"+t.Name()+"$", "-test.v")
		command.Env = append(os.Environ(), child+"=1")
		output, err := command.CombinedOutput()
		t.Logf("isolated descriptor limit:\n%s", output)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(output, []byte("capture and final FD pressure verified")) {
			t.Skip("requires ext4")
		}
		return
	}

	dir := t.TempDir()
	probe, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(probe)
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires ext4")
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
	closed, _ := database.commandWALActiveSegmentSnapshot()
	key := []byte("covered")
	value := bytes.Repeat([]byte("x"), 3<<10)
	for i := 0; i < 32768; i++ {
		if err := database.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.SetSync(key, []byte("sealed")); err != nil {
		t.Fatal(err)
	}
	if err := database.RotateCommandWALActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
		t.Fatalf("expected pending deferred cleanup, got %t, %v", pending, err)
	}
	activeKey := []byte("active")
	if err := database.SetSync(activeKey, []byte("requires replay")); err != nil {
		t.Fatal(err)
	}
	active, activeBytes := database.commandWALActiveSegmentSnapshot()
	info, err := os.Stat(closed)
	if err != nil || info.Size() < 96<<20 || active == closed || activeBytes <= 0 || activeBytes > 1<<20 {
		t.Fatalf("expected one large closed stream and one small active stream, got %v, %d, %v", info, activeBytes, err)
	}
	walDir := filepath.Dir(active)
	files, err := os.ReadDir(walDir)
	files = slices.DeleteFunc(files, func(entry os.DirEntry) bool { return !commitlog.IsCommandSegmentName(entry.Name()) })
	if err != nil || len(files) != 2 {
		t.Fatalf("expected exactly two named streams, got %v, %v", files, err)
	}

	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = min(limit.Cur, 128)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	var padding []*os.File
	releasePadding := func(count int) {
		for _, file := range padding[len(padding)-count:] {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
		padding = padding[:len(padding)-count]
	}
	defer func() { releasePadding(len(padding)) }()
	for {
		file, err := os.Open(os.DevNull)
		if errors.Is(err, syscall.EMFILE) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		padding = append(padding, file)
	}
	if len(padding) < 2 {
		t.Fatal("descriptor limit does not permit reserving two cleanup slots")
	}
	// Both paths begin with exactly two free slots. Detached capture uses
	// both. The locked scan releases the active reader before this hook.
	releasePadding(2)
	var scans []string
	var readDirErr error
	var finalHookRan bool
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
		beforeSegment: func(path string) { scans = append(scans, path) },
		afterScan: func() {
			finalHookRan = true
			_, readDirErr = os.ReadDir(walDir)
		},
	}
	complete, cleanupErr := database.CleanupCommandWALCoveredPrefix()
	database.testCommandWALCleanupHooks = nil
	releasePadding(len(padding))

	// The same fixture can be applied to a pre-detachment stock checkout.
	wantDetached := os.Getenv("GOMAP_WAL_STREAM_FINAL_INVENTORY_FD_STOCK_CONTROL") != "1"
	wantScans := []string{active, closed}
	wantErr := error(syscall.EMFILE)
	if !wantDetached {
		wantScans = []string{closed, active}
		wantErr = nil
	}
	if !finalHookRan || !slices.Equal(scans, wantScans) || !errors.Is(readDirErr, wantErr) {
		t.Fatalf("capture or kernel pressure not witnessed: scans=%q afterScan=%t ReadDir=%v", scans, finalHookRan, readDirErr)
	}
	t.Logf("capture and final FD pressure verified: RLIMIT_NOFILE=%d detached=%t ReadDir=%v", limit.Cur, wantDetached, readDirErr)
	if !complete || cleanupErr != nil {
		t.Fatalf("cleanup did not service covered prefix: complete=%t err=%v", complete, cleanupErr)
	}
	if _, err := os.Stat(closed); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("covered predecessor still linked: %v", err)
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("original active stream lost: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	reopened, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, pair := range []struct{ key, value []byte }{
		{key, []byte("sealed")},
		{activeKey, []byte("requires replay")},
	} {
		if got, err := reopened.Get(pair.key); err != nil || !bytes.Equal(got, pair.value) {
			t.Errorf("reopened key %q=%q, want %q, err=%v", pair.key, got, pair.value, err)
		}
	}
}
