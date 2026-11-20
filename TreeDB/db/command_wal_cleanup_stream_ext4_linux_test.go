package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandWALCleanupStreamDirectory(t *testing.T) {
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
		dir, err := openStreamCleanupDirectory(name)
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
	dir, err := openStreamCleanupDirectory(notDirectory)
	if dir != nil {
		_ = dir.Close()
		t.Fatal("opened a regular file as the staged cleanup directory")
	}
	if !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("regular-file directory open error = %v, want ENOTDIR", err)
	}
}

func TestCommandWALCleanupDedicatedVolumeSelection(t *testing.T) {
	dir := t.TempDir()
	database, err := OpenForNativeSemanticOwner(Options{
		Dir: dir, CommandWAL: true, DisableBackgroundPrune: true,
		ResolvedProfile: ProfileCommandWALRelaxed, Durability: DurabilityWALOnRelaxed,
		MemtableMode: "cow_btree", FreshRelaxedStandaloneWALStream: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	snapshot, err := database.commandJournal.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Ext4Stream {
		t.Skip("requires an ext4 WAL stream")
	}
	check := func(snapshot commitlog.CommandJournalCleanupSnapshot) bool {
		t.Helper()
		selected, err := database.commandWALCleanupHasDedicatedVolume(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		return selected
	}
	if check(snapshot) {
		t.Fatal("ordinary layout selected detached validation")
	}
	snapshot.ActiveIdentity.VolumeID ^= 1
	if !check(snapshot) {
		t.Fatal("dedicated WAL volume was excluded")
	}
	snapshot.Ext4Stream = false
	if check(snapshot) {
		t.Fatal("non-ext4 WAL selected detached validation")
	}
	snapshot.Ext4Stream = true
	path := ValueLogDirPath(dir)
	displaced := path + ".ordinary"
	if err := os.Rename(path, displaced); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Rename(displaced, path); err != nil {
			t.Error(err)
		}
	}()
	if check(snapshot) {
		t.Fatal("missing value-log directory selected detached validation")
	}
	alternate, err := os.MkdirTemp("/dev/shm", "gomap-value-volume-")
	if err != nil {
		t.Skipf("separate directory fixture unavailable: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(alternate) })
	if err := os.Symlink(alternate, path); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(path) }()
	alternateDir, err := os.Open(alternate)
	if err != nil {
		t.Fatal(err)
	}
	alternateIdentity, identityErr := rootpublication.StableIdentityFromFile(alternateDir)
	if err := errors.Join(identityErr, alternateDir.Close()); err != nil {
		t.Fatal(err)
	}
	if alternateIdentity.VolumeID == snapshot.ActiveIdentity.VolumeID^1 {
		t.Skip("separate value directory is on the index volume")
	}
	if check(snapshot) {
		t.Fatal("separately mounted value log selected detached validation")
	}
}

func TestCommandWALCleanupMountedExt4AsymmetricSealedPrefix(t *testing.T) {
	const childDir = "GOMAP_TEST_MOUNTED_WAL_DATA"
	dir := os.Getenv(childDir)
	if dir == "" {
		wal := t.TempDir()
		rootFile, err := os.Open(wal)
		if err != nil {
			t.Fatal(err)
		}
		ext4 := commitlog.IsExt4WALFilesystem(rootFile)
		if err := rootFile.Close(); err != nil {
			t.Fatal(err)
		}
		if !ext4 {
			t.Skip("requires an ext4 WAL location")
		}
		dir, err := os.MkdirTemp("/dev/shm", "gomap-wal-only-data-")
		if err != nil {
			t.Skipf("separate data-volume fixture unavailable: %v", err)
		}
		defer os.RemoveAll(dir)
		target := WALDirPath(dir)
		if err := os.Mkdir(target, 0700); err != nil {
			t.Fatal(err)
		}
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		// Mount in a private user/mount namespace. No host mount changes.
		command := exec.CommandContext(t.Context(), "unshare", "-Urnm", "sh", "-c",
			`mount --make-rprivate / && mount --bind "$1" "$2" || exit 77
printf 'private WAL mount ready\n'
exec "$3" -test.run="^$4$" -test.v`, "gomap-mounted-wal", wal, target, binary, t.Name())
		command.Env = append(os.Environ(), childDir+"="+dir)
		output, err := command.CombinedOutput()
		t.Logf("isolated mount control:\n%s", output)
		if err != nil && !bytes.Contains(output, []byte("private WAL mount ready")) {
			t.Skipf("private bind mount unavailable: %v", err)
		}
		if err != nil {
			t.Fatal(err)
		}
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
	writes := [2]int{65536, 8192}
	var closed [2]string
	var sizes [2]int64
	key := []byte("covered")
	value := bytes.Repeat([]byte{'v'}, 3<<10)
	for i := range closed {
		closed[i], _ = database.commandWALActiveSegmentSnapshot()
		for n := 0; n < writes[i]; n++ {
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
			t.Fatalf("fixture did not produce a large closed stream: %v, %v", info, err)
		}
		sizes[i] = info.Size()
	}
	if sizes[0] < minDetachedStreamCleanupClosedBytes || sizes[1] >= minDetachedStreamCleanupClosedBytes/2 ||
		sizes[0]+sizes[1] < minDetachedStreamCleanupClosedBytes {
		t.Fatalf("unexpected asymmetric geometry: %v", sizes)
	}
	t.Logf("real sealed geometry: first=%d second=%d aggregate=%d", sizes[0], sizes[1], sizes[0]+sizes[1])
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
	for remaining := int64(maxDetachedStreamCleanupActiveBytes) - database.CommandWALActiveBytes(); remaining > 0; remaining = int64(maxDetachedStreamCleanupActiveBytes) - database.CommandWALActiveBytes() {
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
	if err != nil || !proof.journal.Ext4Stream || proof.journal.ActiveBytes != maxDetachedStreamCleanupActiveBytes {
		t.Fatalf("fixture did not prepare the mounted ext4 stream at %d active bytes: proof=%+v err=%v",
			maxDetachedStreamCleanupActiveBytes, proof.journal, err)
	}
	if selected, err := database.commandWALCleanupHasDedicatedVolume(proof.journal); err != nil || !selected {
		t.Fatalf("actual separated WAL volume was not selected: %v", err)
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

	// Exercise the real pre-unlink sticky check after a successful staged
	// proof. Clear only this injected test bit to continue the fixture.
	poisonObserved := false
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{afterScan: func() {
		poisonObserved = true
		database.commandWALFlushPoisoned.Store(true)
	}}
	complete, poisonErr := database.CleanupCommandWALCoveredPrefix()
	database.testCommandWALCleanupHooks = nil
	database.commandWALFlushPoisoned.Store(false)
	if !poisonObserved || complete || !errors.Is(poisonErr, ErrRecoveryRequired) {
		t.Fatalf("post-cut poison: observed=%t complete=%t err=%v", poisonObserved, complete, poisonErr)
	}
	assertPresent()

	cutInfo, err := os.Stat(active)
	if err != nil {
		t.Fatal(err)
	}
	if cutInfo.Size() != proof.journal.ActiveBytes {
		t.Fatalf("visible active cut differs from journal bytes: physical=%d logical=%d", cutInfo.Size(), proof.journal.ActiveBytes)
	}
	t.Logf("active cut: physical=%d logical=%d frames=%d", cutInfo.Size(), proof.journal.ActiveBytes, activeFrames)

	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("no held snapshot")
	}
	defer held.Close()
	beforeFrames := database.commandWALCleanupScanFrames.Load()
	beforeRemoved := database.commandWALCleanupBytes.Load()
	scanned = testCommandWALCleanupScanOverlap(t, database, cleanupDeferredCommandWALPrefixForTest, closed[0], true, func() error {
		start := time.Now()
		if err := database.SetSync([]byte("postcut"), value); err != nil {
			return err
		}
		elapsed := time.Since(start)
		t.Logf("durable postcut append ACK during paused detached scan in %s", elapsed)
		info, err := os.Stat(active)
		if err != nil {
			return err
		}
		if physical, logical := info.Size(), database.CommandWALActiveBytes(); physical <= maxDetachedStreamCleanupActiveBytes || logical != physical {
			return fmt.Errorf("postcut WAL did not grow past capture limit: physical=%d logical=%d", physical, logical)
		}
		t.Logf("postcut bytes: physical=%d logical=%d", info.Size(), database.CommandWALActiveBytes())
		return nil
	})
	current, _ := database.commandWALActiveSegmentSnapshot()
	if current != active || !slices.Equal(scanned, []string{closed[0], closed[1], active}) {
		t.Errorf("physical scan order=%q active at capture=%q current=%q", scanned, active, current)
	}
	if frames := database.commandWALCleanupScanFrames.Load() - beforeFrames; frames != uint64(writes[0]+writes[1]+2)+activeFrames {
		t.Errorf("scanned physical frames=%d, want exactly both predecessors and %d initially active frames", frames, activeFrames)
	}
	if removed := database.commandWALCleanupBytes.Load() - beforeRemoved; removed != uint64(sizes[0]+sizes[1]) {
		t.Fatalf("retired bytes=%d, want %d", removed, sizes[0]+sizes[1])
	}
	t.Logf("validated and retired %d closed-stream bytes, preserved the capture-active generation during a durable same-generation write", sizes[0]+sizes[1])
	for _, path := range closed {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("covered predecessor was not reclaimed: %s: %v", path, err)
		}
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("capture-active generation must remain after cleanup: %v", err)
	}
	if got, err := held.Get(key); err != nil || string(got) != "before" {
		t.Fatalf("held value=%q, err=%v", got, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	// Copy the stopped-in-test store while its owner remains open. The
	// copy has durable roots and WAL, with no graceful-close publication.
	crashCopy := t.TempDir()
	if err := os.CopyFS(crashCopy, os.DirFS(dir)); err != nil {
		t.Fatal(err)
	}
	crashOpts := opts
	crashOpts.Dir = crashCopy
	crashReopened, err := OpenForNativeSemanticOwner(crashOpts)
	if err != nil {
		t.Fatalf("recover captured and postcut frames from pre-close copy: %v", err)
	}
	for key, want := range map[string]string{"active-prefix": "still live", "postcut": string(value)} {
		if got, err := crashReopened.Get([]byte(key)); err != nil || !bytes.Equal(got, []byte(want)) {
			t.Errorf("recovered %s: bytes=%d want=%d err=%v", key, len(got), len(want), err)
		}
	}
	if err := crashReopened.Close(); err != nil {
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
	for key, want := range map[string]string{"covered": "before", "active-prefix": "still live", "postcut": string(value)} {
		if got, err := reopened.Get([]byte(key)); err != nil || string(got) != want {
			t.Errorf("reopened %s=%q, want %q, err=%v", key, got, want, err)
		}
	}
}

// Inject the duplicate after the writer has opened a valid journal. A shared
// active-first seen set would stop at the first sealed frame and miss its CRC.
func TestCommandWALCleanupActiveCutRetainsHardCorruption(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)
	journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
		SegmentSeq: 3, InitialLSN: 2, PreferExt4Stream: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	}()
	cut, err := journal.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !cut.Ext4Stream {
		t.Skip("requires an ext4 WAL stream")
	}
	// Assign real active LSN 3 through the owner before replacing the sealed file.
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSet, Key: []byte("active"), Value: []byte("after"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	lsn, err := journal.AppendCommand(commitlog.CommandEnvelope{
		Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1, Payload: payload,
	})
	if err != nil || lsn != 3 {
		t.Fatalf("append lsn=%d err=%v", lsn, err)
	}
	if err := journal.Sync(); err != nil {
		t.Fatal(err)
	}
	sealed := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	if err := os.Remove(sealed); err != nil {
		t.Fatal(err)
	}
	writeCommandWALSegmentFrames(t, dir, 1, 3, 4)
	image, err := os.ReadFile(sealed)
	if err != nil {
		t.Fatal(err)
	}
	image[len(image)-1] ^= 1
	if err := os.WriteFile(sealed, image, 0600); err != nil {
		t.Fatal(err)
	}
	db := &DB{dir: dir}
	proof := durableWALCleanupProofV1{journalOwner: journal, journal: cut, cleanupThrough: 2, durableWALLSN: 3}
	stock := newCommandWALCleanupScan(2, 3, 0, nil, &cut)
	_, err = stock.finish(journal.WithCleanupScan(cut, func() error { return stock.scanLockedDirectory(dir) }))
	if !errors.Is(err, commitlog.ErrCorrupt) || errors.Is(err, errDurableWALCleanupProofUnavailable) {
		t.Fatalf("stock lost the independent hard CRC error: %v", err)
	}
	scan := newCommandWALCleanupScan(2, 3, 0, nil, &cut)
	attempted, err := db.scanDetachedCommandWALCleanupPrefix(proof, scan, nil, 1)
	_, err = scan.finish(err)
	if !attempted || !errors.Is(err, commitlog.ErrCorrupt) || errors.Is(err, errDurableWALCleanupProofUnavailable) {
		t.Fatalf("duplicate and independent sealed CRC: attempted=%t err=%v", attempted, err)
	}
}

// A valid old capture cannot authorize a later larger or unflushed active
// stream. The optional path neither reads predecessors nor flushes the writer.
func TestCommandWALCleanupActiveCutStageBounds(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)
	journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
		SegmentSeq: 3, InitialLSN: 2, PreferExt4Stream: true,
		BufferSize: 4 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	}()
	cut, err := journal.CaptureCleanupSnapshot()
	if err != nil || cut.ActiveBytes != 0 {
		t.Fatalf("expected empty active capture: %+v err=%v", cut, err)
	}
	if !cut.Ext4Stream {
		t.Skip("requires an ext4 WAL stream")
	}
	appendValue := func(size int) {
		t.Helper()
		payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
			Op: commitlog.RawKVOpSet, Key: []byte("growth"), Value: bytes.Repeat([]byte("x"), size),
		}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := journal.AppendCommand(commitlog.CommandEnvelope{
			Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV,
			PayloadFormat: commitlog.PayloadFormatRawKVBatchV1, Payload: payload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	db := &DB{dir: dir}
	proof := durableWALCleanupProofV1{journalOwner: journal, journal: cut, cleanupThrough: 2, durableWALLSN: 2}
	checkDecline := func(label string, logicalOverCap, physicalOverCap bool) {
		t.Helper()
		now, err := journal.CaptureCleanupSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(now.ActivePath)
		if err != nil {
			t.Fatal(err)
		}
		if (now.ActiveBytes > maxDetachedStreamCleanupActiveBytes) != logicalOverCap ||
			(info.Size() > maxDetachedStreamCleanupActiveBytes) != physicalOverCap {
			t.Fatalf("%s: unexpected geometry physical=%d logical=%d", label, info.Size(), now.ActiveBytes)
		}
		visits := 0
		scan := newCommandWALCleanupScan(2, 2, 0, func(string) { visits++ }, &cut)
		attempted, err := db.scanDetachedCommandWALCleanupPrefix(proof, scan, nil, 1)
		_, err = scan.finish(err)
		after, statErr := os.Stat(now.ActivePath)
		if attempted || err != nil || visits != 0 ||
			statErr != nil || after.Size() != info.Size() {
			t.Fatalf("%s: attempted=%t err=%v visits=%d after=%v statErr=%v", label, attempted, err, visits, after, statErr)
		}
		t.Logf("%s declined without scanning or flushing: physical=%d logical=%d", label, info.Size(), now.ActiveBytes)
	}
	appendValue(64)
	checkDecline("small active still buffered", false, false)
	for i := 0; i < 900; i++ {
		appendValue(3 << 10)
	}
	checkDecline("accepted-byte overshoot", true, false)
	if err := journal.Flush(); err != nil {
		t.Fatal(err)
	}
	checkDecline("visible physical overshoot", true, true)
}

func TestCommandWALCleanupCapturedActiveReplayAndCorruption(t *testing.T) {
	db, proof, sealed := prepareDetachedStreamCleanupTest(t, 1, 2)
	lsn, err := db.commandJournal.AppendRawKVPointCommandTrustedWithRotateSync(2, commitlog.RawKVOpSet, []byte("replay"), []byte("required"), true)
	if err != nil || lsn != 3 {
		t.Fatalf("active append: LSN=%d err=%v", lsn, err)
	}
	if err := db.commandJournal.Sync(); err != nil {
		t.Fatal(err)
	}
	proof.durableWALLSN = 3
	scan := func(detached bool) ([]commandWALSegmentCleanupDecision, error) {
		scanner := newCommandWALCleanupScan(1, 3, 0, nil, &proof.journal)
		if detached {
			attempted, err := db.scanDetachedCommandWALCleanupPrefix(proof, scanner, nil, 1)
			if !attempted {
				t.Fatal("captured active validation did not run")
			}
			return scanner.finish(err)
		}
		return scanner.finish(proof.journalOwner.WithCleanupScan(proof.journal, func() error {
			return scanner.scanLockedDirectory(db.dir)
		}))
	}
	decisions, err := scan(true)
	if closeErr := closeCommandWALCleanupDecisions(decisions); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || len(decisions) != 3 || !decisions[2].Active || decisions[2].MaxLSN != 3 {
		t.Fatalf("replay membership only in copied active file: decisions=%+v err=%v", decisions, err)
	}
	segment, err := os.ReadFile(proof.journal.ActivePath)
	if err != nil || len(segment) == 0 {
		t.Fatalf("read active bytes: %v", err)
	}
	segment[len(segment)-1] ^= 1
	if err := os.WriteFile(proof.journal.ActivePath, segment, 0600); err != nil {
		t.Fatal(err)
	}
	for _, detached := range []bool{false, true} {
		if decisions, err := scan(detached); !errors.Is(err, commitlog.ErrCorrupt) || errors.Is(err, errDurableWALCleanupProofUnavailable) {
			t.Fatalf("detached=%t lost hard active CRC: decisions=%+v err=%v", detached, decisions, err)
		}
		for _, path := range sealed {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("hard corruption lost predecessor after detached=%t: %v", detached, err)
			}
		}
	}
}

func TestCommandWALCleanupDetachedStreamFinishPreservesCloseError(t *testing.T) {
	db, proof, sealed := prepareDetachedStreamCleanupTest(t, 2, 2)
	active := proof.journal.ActivePath
	displaced := active + ".displaced"
	var first *os.File
	scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, 0, func(path string) {
		if path != active {
			return
		}
		// Inject a failed cleanup-reader close after both sealed CRCs passed,
		// plus a concurrent active namespace rebind for Finish to refuse.
		if err := first.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(active, displaced); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(displaced, active); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := errors.Join(os.Remove(active), os.Rename(displaced, active)); err != nil {
				t.Error(err)
			}
		})
	}, &proof.journal)
	attempted, err := db.scanDetachedCommandWALCleanupPrefix(proof, scan, func(path string) (*os.File, error) {
		file, err := os.Open(path)
		if path == sealed[0] {
			first = file
		}
		return file, err
	}, 1)
	_, err = scan.finish(err)
	if !attempted || !errors.Is(err, ErrRecoveryRequired) || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cleanup lost either finish error: attempted=%t err=%v", attempted, err)
	}
	for _, path := range sealed {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("finish failure changed retired namespace: %v", err)
		}
	}
}

func TestCommandWALCleanupDetachedStreamStaleDoesNotHideHardClose(t *testing.T) {
	scan := newCommandWALCleanupScan(0, 0, 0, nil, nil)
	scan.hardErr = os.ErrClosed
	_, err := scan.finish(commitlog.ErrCommandWALCleanupSnapshotStale)
	if !errors.Is(err, os.ErrClosed) || errors.Is(err, commitlog.ErrCommandWALCleanupSnapshotStale) {
		t.Fatalf("hard close reported as retryable stale cut: %v", err)
	}
}

func TestCommandWALCleanupDetachedStreamMixedStageError(t *testing.T) {
	db, proof, sealed := prepareDetachedStreamCleanupTest(t, 2, 2)
	want := errors.New("independent open failure")
	var first *os.File
	scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, 0, nil, &proof.journal)
	attempted, err := db.scanDetachedCommandWALCleanupPrefix(proof, scan, func(path string) (*os.File, error) {
		if path == sealed[1] {
			return nil, errors.Join(syscall.EMFILE, want)
		}
		var err error
		first, err = os.Open(path)
		return first, err
	}, 1)
	if !attempted || !errors.Is(err, want) || !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("mixed stage refusal was normalized to fallback: attempted=%t err=%v", attempted, err)
	}
	if first == nil {
		t.Fatal("first descriptor was never opened")
	}
	if _, err := first.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("first staged descriptor leaked: %v", err)
	}
}
