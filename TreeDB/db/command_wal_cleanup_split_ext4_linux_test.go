package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandWALCleanupSplitDirectory(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "actual")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "wal")
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	notDirectory := filepath.Join(root, "regular")
	if err := os.WriteFile(notDirectory, nil, 0600); err != nil {
		t.Fatal(err)
	}
	want, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{path, alias} {
		dir, err := openSplitCommandWALCleanupDirectory(name)
		if err != nil {
			t.Fatal(err)
		}
		got, statErr := dir.Stat()
		entries, readErr := dir.ReadDir(-1)
		closeErr := dir.Close()
		if err := errors.Join(statErr, readErr, closeErr); err != nil {
			t.Fatal(err)
		}
		if !got.IsDir() || !os.SameFile(want, got) || len(entries) != 0 {
			t.Fatalf("%q did not open the expected directory", name)
		}
	}
	dir, err := openSplitCommandWALCleanupDirectory(notDirectory)
	if dir != nil {
		_ = dir.Close()
		t.Fatal("opened a regular file as the staged cleanup directory")
	}
	if !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("regular-file directory open error = %v, want ENOTDIR", err)
	}
}

func TestCommandWALCleanupSplitExt4SealedPrefix(t *testing.T) {
	dir := t.TempDir()
	// Put only the empty value-log namespace on the alternate filesystem.
	// The WAL and all large fixture data stay under the test temp root.
	docDir, err := os.MkdirTemp("/tmp", "treedb-split-value-log-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(docDir) })
	for _, path := range []string{dir, docDir} {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		ext4 := commitlog.IsExt4WALFilesystem(file)
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if !ext4 {
			t.Skip("requires two ext4 filesystems")
		}
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	docInfo, err := os.Stat(docDir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Sys().(*syscall.Stat_t).Dev == docInfo.Sys().(*syscall.Stat_t).Dev {
		t.Skip("requires TMPDIR on an ext4 filesystem separate from /tmp")
	}
	if err := os.Symlink(docDir, ValueLogDirPath(dir)); err != nil {
		t.Fatal(err)
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
	for i := range closed {
		closed[i], _ = database.commandWALActiveSegmentSnapshot()
		for n := 0; n < 32768; n++ {
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
		if err != nil || info.Size() < minSplitStreamCleanupSegmentBytes {
			t.Fatalf("fixture did not produce a large closed stream: %v, %v", info, err)
		}
		sizes[i] = info.Size()
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
		t.Fatalf("expected deferred cleanup debt: pending=%t err=%v", pending, err)
	}
	activeFirstLSN := database.CommandWALNextLSN()
	if err := database.SetSync([]byte("active-prefix"), []byte("still live")); err != nil {
		t.Fatal(err)
	}
	// Reach the inclusive capture limit with real stream frames. Use a
	// separate key so the checked reopen still proves the original value.
	paddingKey := []byte("active-padding")
	beforePad := database.CommandWALActiveBytes()
	if err := database.Set(paddingKey, value); err != nil {
		t.Fatal(err)
	}
	frameOverhead := database.CommandWALActiveBytes() - beforePad - int64(len(value))
	for remaining := int64(maxSplitStreamCleanupActiveBytes) - database.CommandWALActiveBytes(); remaining > 0; remaining = int64(maxSplitStreamCleanupActiveBytes) - database.CommandWALActiveBytes() {
		payload := remaining - frameOverhead
		if payload > int64(len(value)) {
			payload = min(int64(len(value)), remaining-2*frameOverhead-1)
		}
		if payload <= 0 {
			t.Fatalf("cannot pad %d active WAL bytes with frame overhead %d", remaining, frameOverhead)
		}
		write := database.Set
		if payload+frameOverhead == remaining {
			write = database.SetSync
		}
		if err := write(paddingKey, value[:payload]); err != nil {
			t.Fatal(err)
		}
	}
	proof, err := database.captureDurableWALCleanupProofV1()
	if err != nil || !proof.journal.Ext4Stream || proof.journal.ActiveBytes != maxSplitStreamCleanupActiveBytes ||
		!database.commandWALCleanupSplitValueLogVolume(proof.journal) {
		t.Fatalf("fixture did not select split ext4 stream cleanup at %d active bytes: proof=%+v err=%v",
			maxSplitStreamCleanupActiveBytes, proof.journal, err)
	}
	activeFrames := database.CommandWALNextLSN() - activeFirstLSN
	active := proof.journal.ActivePath
	assertPresent := func() {
		t.Helper()
		for _, path := range closed {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("closed predecessor lost: %s: %v", path, err)
			}
		}
	}
	// A pressure fallback must discard staged handles before the original
	// scanner runs and must not validate either frame stream twice.
	file, err := os.Open(closed[0])
	if err != nil {
		t.Fatal(err)
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err := errors.Join(err, file.Close()); err != nil {
		t.Fatal(err)
	}
	var pin *rootpublication.IdentityPin
	var pins *rootpublication.IdentityPinRegistry
	if err := database.commandJournal.WithCleanupSnapshot(proof.journal, func(registry *rootpublication.IdentityPinRegistry, _ commitlog.CommandJournalCleanupSnapshot) (bool, error) {
		pins = registry
		if err := registry.Observe(identity); err != nil {
			return false, err
		}
		var err error
		pin, err = registry.Pin(identity)
		return false, err
	}); err != nil {
		t.Fatal(err)
	}
	var staged *os.File
	var scanned []string
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
		openPrefix: func(path string) (*os.File, error) {
			if path == closed[1] {
				return nil, syscall.EMFILE
			}
			staged, err = os.Open(path)
			return staged, err
		},
		beforeSegment: func(path string) {
			if database.commandJournal.AppendLockAvailableForTesting() {
				t.Error("descriptor pressure did not restore locked scan")
			}
			scanned = append(scanned, path)
		},
	}
	complete, pressureErr := database.CleanupCommandWALCoveredPrefix()
	database.testCommandWALCleanupHooks = nil
	pin.Release()
	if err := pins.Unobserve(identity); err != nil {
		t.Fatal(err)
	}
	if count := pins.PinCount(identity); count != 0 {
		t.Fatalf("closed stream retained %d identity pins", count)
	}
	if complete || pressureErr != nil || staged == nil || !slices.Equal(scanned, []string{closed[0], closed[1], active}) {
		t.Fatalf("descriptor pressure: complete=%t err=%v staged=%v scans=%q", complete, pressureErr, staged, scanned)
	}
	if _, err := staged.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("staged descriptor leaked during fallback: %v", err)
	}
	assertPresent()

	// A same-inode corruption late in the second physical stream must still
	// be detected, even though its LSNs are already covered by both roots.
	sealed, err := os.OpenFile(closed[1], os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	last := sizes[1] - 1
	var original [1]byte
	if _, err := sealed.ReadAt(original[:], last); err != nil {
		t.Fatal(err)
	}
	if _, err := sealed.WriteAt([]byte{original[0] ^ 1}, last); err != nil {
		t.Fatal(err)
	}
	complete, damageErr := database.CleanupCommandWALCoveredPrefix()
	_, restoreErr := sealed.WriteAt(original[:], last)
	if err := errors.Join(restoreErr, sealed.Close()); err != nil {
		t.Fatal(err)
	}
	if complete || !errors.Is(damageErr, commitlog.ErrCorrupt) {
		t.Fatalf("second stream damage: complete=%t err=%v", complete, damageErr)
	}
	assertPresent()

	// Replacing a validated name after scanning must preserve both names.
	displaced := closed[1] + ".displaced"
	var replacementErr error
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{afterScan: func() {
		if replacementErr = os.Rename(closed[1], displaced); replacementErr == nil {
			replacementErr = os.Symlink(displaced, closed[1])
		}
	}}
	complete, replacementCleanupErr := database.CleanupCommandWALCoveredPrefix()
	database.testCommandWALCleanupHooks = nil
	if replacementErr != nil || complete || !errors.Is(replacementCleanupErr, ErrRecoveryRequired) {
		t.Fatalf("late replacement: replace=%v complete=%t err=%v", replacementErr, complete, replacementCleanupErr)
	}
	assertPresent()
	if err := os.Rename(displaced, closed[1]); err != nil {
		t.Fatal(err)
	}

	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("no held snapshot")
	}
	defer held.Close()
	beforeFrames := database.commandWALCleanupScanFrames.Load()
	beforeRemoved := database.commandWALCleanupBytes.Load()
	scanned = testCommandWALCleanupScanOverlap(t, database, cleanupDeferredCommandWALPrefixForTest, closed[0], true, func() error {
		if err := database.SetSync(key, []byte("during")); err != nil {
			return err
		}
		return database.RotateCommandWALActiveSegment(true)
	})
	current, _ := database.commandWALActiveSegmentSnapshot()
	if current == active || !slices.Equal(scanned, []string{closed[0], closed[0], closed[1], active, current}) {
		t.Fatalf("physical scan order=%q active at capture=%q current=%q", scanned, active, current)
	}
	if frames := database.commandWALCleanupScanFrames.Load() - beforeFrames; frames != 2*32769+activeFrames+1 {
		t.Fatalf("scanned physical frames=%d, want both complete predecessors, %d initially active frames, and the concurrent write", frames, activeFrames)
	}
	if removed := database.commandWALCleanupBytes.Load() - beforeRemoved; removed != uint64(sizes[0]+sizes[1]) {
		t.Fatalf("retired bytes=%d, want %d", removed, sizes[0]+sizes[1])
	}
	t.Logf("validated and retired %d closed-stream bytes, preserved the capture-active generation during a durable write and rotation", sizes[0]+sizes[1])
	for _, path := range closed {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("covered predecessor was not reclaimed: %s: %v", path, err)
		}
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("capture-active generation must remain after rotation: %v", err)
	}
	if got, err := held.Get(key); err != nil || string(got) != "before" {
		t.Fatalf("held value=%q, err=%v", got, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
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
	for key, want := range map[string]string{"covered": "during", "active-prefix": "still live"} {
		if got, err := reopened.Get([]byte(key)); err != nil || string(got) != want {
			t.Errorf("reopened %s=%q, want %q, err=%v", key, got, want, err)
		}
	}
}
