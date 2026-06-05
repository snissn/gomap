package treedb

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestPublicCommandWALRawKVWritesUseTypedFrames(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if got := db.Stats()["treedb.write_path.mode"]; got != "command_wal_cached" {
		_ = db.Close()
		t.Fatalf("write_path.mode=%q, want command_wal_cached", got)
	}
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		_ = db.Close()
		t.Fatalf("Set: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Delete([]byte("k1")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Delete: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	assertPublicCommandWALFrames(t, db, 2)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{
		Dir:                 dir,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("reopen command WAL from format: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	if got := reopen.Stats()["treedb.write_path.mode"]; got != "command_wal_cached" {
		t.Fatalf("reopen write_path.mode=%q, want command_wal_cached", got)
	}
	got, err := reopen.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("Get(k2): %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("Get(k2)=%q, want v2", got)
	}
	hasK1, err := reopen.Has([]byte("k1"))
	if err != nil {
		t.Fatalf("Has(k1): %v", err)
	}
	if hasK1 {
		t.Fatal("k1 exists after command-WAL batch delete")
	}
	if got := reopen.backend.State().AppliedCommandLSN; got < 2 {
		t.Fatalf("AppliedCommandLSN=%d, want at least 2", got)
	}
}

func TestPublicCommandWALRawKVMethodMatrix(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if err := db.Set([]byte("set"), []byte("before-delete")); err != nil {
		_ = db.Close()
		t.Fatalf("Set: %v", err)
	}
	if err := db.SetSync([]byte("set-sync"), []byte("before-delete")); err != nil {
		_ = db.Close()
		t.Fatalf("SetSync: %v", err)
	}
	if err := db.Delete([]byte("set")); err != nil {
		_ = db.Close()
		t.Fatalf("Delete: %v", err)
	}
	if err := db.DeleteSync([]byte("set-sync")); err != nil {
		_ = db.Close()
		t.Fatalf("DeleteSync: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("batch-write"), []byte("visible-after-reopen")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	syncBatch := db.NewBatchWithSize(128)
	if err := syncBatch.Set([]byte("batch-sync"), []byte("visible-after-sync-reopen")); err != nil {
		_ = syncBatch.Close()
		_ = db.Close()
		t.Fatalf("sync batch Set: %v", err)
	}
	if err := syncBatch.WriteSync(); err != nil {
		_ = syncBatch.Close()
		_ = db.Close()
		t.Fatalf("sync batch WriteSync: %v", err)
	}
	if err := syncBatch.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("sync batch Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{
		Dir:                 dir,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("reopen command WAL from format: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	if got := reopen.Stats()["treedb.write_path.mode"]; got != "command_wal_cached" {
		t.Fatalf("reopen write_path.mode=%q, want command_wal_cached", got)
	}
	for _, key := range []string{"set", "set-sync"} {
		has, err := reopen.Has([]byte(key))
		if err != nil {
			t.Fatalf("Has(%s): %v", key, err)
		}
		if has {
			t.Fatalf("%s exists after command-WAL delete", key)
		}
	}
	for key, want := range map[string]string{
		"batch-write": "visible-after-reopen",
		"batch-sync":  "visible-after-sync-reopen",
	} {
		got, err := reopen.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%s)=%q, want %q", key, got, want)
		}
	}
	if got := reopen.backend.State().AppliedCommandLSN; got < 6 {
		t.Fatalf("AppliedCommandLSN=%d, want at least 6", got)
	}
}

func TestPublicCommandWALRawKVEmptyPointKeyAndZeroLengthValues(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                 dir,
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}

	if err := db.Set([]byte{}, []byte("value")); err != nil {
		_ = db.Close()
		t.Fatalf("Set empty key: %v", err)
	}
	if err := db.SetSync([]byte("zero-sync"), nil); err != nil {
		_ = db.Close()
		t.Fatalf("SetSync nil value: %v", err)
	}
	if err := db.Delete(nil); err != nil {
		_ = db.Close()
		t.Fatalf("Delete nil/empty key: %v", err)
	}
	if err := db.Set(nil, nil); err != nil {
		_ = db.Close()
		t.Fatalf("Set nil key/value: %v", err)
	}

	b := db.NewBatch()
	if err := b.Set([]byte{}, []byte("batch-empty")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set empty key: %v", err)
	}
	if err := b.Set([]byte("batch-zero"), nil); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Set nil value: %v", err)
	}
	if err := b.Delete([]byte("zero-sync")); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch Delete zero-sync: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		_ = db.Close()
		t.Fatalf("batch WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		_ = db.Close()
		t.Fatalf("batch Close: %v", err)
	}
	assertPublicCommandWALFrames(t, db, 5)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true})
	if err != nil {
		t.Fatalf("reopen command WAL dir: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	requireRawKVValue(t, reopen, []byte{}, []byte("batch-empty"))
	requireRawKVValue(t, reopen, nil, []byte("batch-empty"))
	requireRawKVValue(t, reopen, []byte("batch-zero"), []byte{})
	has, err := reopen.Has([]byte("zero-sync"))
	if err != nil {
		t.Fatalf("Has(zero-sync): %v", err)
	}
	if has {
		t.Fatal("zero-sync exists after command-WAL batch delete")
	}
	if got := reopen.backend.State().AppliedCommandLSN; got < 5 {
		t.Fatalf("AppliedCommandLSN=%d, want at least 5", got)
	}
}

func TestPublicCommandWALRejectsUnsupportedCachedRawMutations(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Set([]byte("keep"), []byte("original")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	updateCalled := false
	if err := db.Update([]byte("keep"), func(old []byte) (UpdateResult, error) {
		updateCalled = true
		return SetUpdate([]byte("updated")), nil
	}); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("Update err=%v, want ErrCommandWALRejected", err)
	}
	if updateCalled {
		t.Fatal("Update callback ran after command-WAL rejection")
	}
	updateSyncCalled := false
	if err := db.UpdateSync([]byte("keep"), func(old []byte) (UpdateResult, error) {
		updateSyncCalled = true
		return DeleteUpdate(), nil
	}); !errors.Is(err, ErrCommandWALRejected) {
		t.Fatalf("UpdateSync err=%v, want ErrCommandWALRejected", err)
	}
	if updateSyncCalled {
		t.Fatal("UpdateSync callback ran after command-WAL rejection")
	}
	got, err := db.Get([]byte("keep"))
	if err != nil {
		t.Fatalf("Get(keep): %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("Get(keep)=%q, want original", got)
	}
}

func assertPublicCommandWALFrames(t *testing.T, db *DB, minFrames uint64) {
	t.Helper()
	stats := db.Stats()
	if stats["treedb.command_wal.required_feature"] != "true" {
		t.Fatalf("required_feature=%q, want true", stats["treedb.command_wal.required_feature"])
	}
	if stats["treedb.command_wal.stats_scan"] != "true" {
		t.Fatalf("stats_scan=%q, want true", stats["treedb.command_wal.stats_scan"])
	}
	frames := publicCommandWALFrameCount(t, db)
	if frames < minFrames {
		t.Fatalf("command_wal.frames=%d, want at least %d", frames, minFrames)
	}
	maxLSN, err := strconv.ParseUint(stats["treedb.command_wal.max_lsn"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.max_lsn=%q: %v", stats["treedb.command_wal.max_lsn"], err)
	}
	if maxLSN < minFrames {
		t.Fatalf("command_wal.max_lsn=%d, want at least %d", maxLSN, minFrames)
	}
}

func publicCommandWALFrameCount(t *testing.T, db *DB) uint64 {
	t.Helper()
	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	return frames
}

func TestPublicCommandWALLiveCountersDoNotRequireStatsScan(t *testing.T) {
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	b := db.NewBatch()
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("batch Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.command_wal.stats_scan"]; got != "false" {
		t.Fatalf("stats_scan=%q, want false (stats=%#v)", got, stats)
	}
	for key, want := range map[string]string{
		"treedb.command_wal.live_accepted_frames":  "2",
		"treedb.command_wal.live_accepted_max_lsn": "2",
		"treedb.command_wal.live_covered_frames":   "2",
		"treedb.command_wal.live_covered_max_lsn":  "2",
		"treedb.applied_command_lsn":               "2",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("stats[%q]=%q, want %q (stats=%#v)", key, got, want, stats)
		}
	}
}

func TestPublicCommandWALPointWritesSerializeLSNWithCachedMutation(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	aAppended := make(chan struct{})
	bAppended := make(chan struct{})
	releaseA := make(chan struct{})
	var aOnce, bOnce, releaseOnce sync.Once
	testAfterPublicCommandWALPointAppend = func(op commitlog.RawKVOperation) {
		switch string(op.Value) {
		case "A":
			aOnce.Do(func() { close(aAppended) })
			<-releaseA
		case "B":
			bOnce.Do(func() { close(bAppended) })
		}
	}
	defer func() { testAfterPublicCommandWALPointAppend = nil }()

	errA := make(chan error, 1)
	go func() {
		errA <- db.Set([]byte("same-key"), []byte("A"))
	}()
	select {
	case <-aAppended:
	case <-time.After(5 * time.Second):
		t.Fatal("first command append did not reach test hook")
	}

	errB := make(chan error, 1)
	bStarted := make(chan struct{})
	go func() {
		close(bStarted)
		errB <- db.Set([]byte("same-key"), []byte("B"))
	}()
	<-bStarted
	select {
	case <-bAppended:
		releaseOnce.Do(func() { close(releaseA) })
		t.Fatal("second same-key command appended before first cached mutation was released")
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(releaseA) })
	if err := recvTestErr(t, errA); err != nil {
		t.Fatalf("first Set: %v", err)
	}
	if err := recvTestErr(t, errB); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	select {
	case <-bAppended:
	default:
		t.Fatal("second command append hook did not run")
	}
	got, err := db.Get([]byte("same-key"))
	if err != nil {
		t.Fatalf("Get(same-key): %v", err)
	}
	if string(got) != "B" {
		t.Fatalf("Get(same-key)=%q, want B", got)
	}
	assertPublicCommandWALFrames(t, db, 2)
}

func TestPublicCommandWALBatchCloseDiscardsDirtyPayload(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	if err := b.Set([]byte("discarded"), []byte("value")); err != nil {
		_ = b.Close()
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch Close: %v", err)
	}
	has, err := db.Has([]byte("discarded"))
	if err != nil {
		t.Fatalf("Has(discarded): %v", err)
	}
	if has {
		t.Fatal("closed dirty batch became visible without Write")
	}
	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames != 0 {
		t.Fatalf("command_wal.frames=%d, want 0 after Close without Write", frames)
	}
}

func TestPublicCommandWALBatchWriteFailureDoesNotAppendFrame(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	b, ok := db.NewBatch().(*commandWALPublicBatch)
	if !ok {
		t.Fatalf("NewBatch type=%T, want *commandWALPublicBatch", db.NewBatch())
	}
	defer func() { _ = b.Close() }()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.inner.Close(); err != nil {
		t.Fatalf("inner Close: %v", err)
	}
	if err := b.Write(); err == nil {
		t.Fatal("batch Write succeeded after inner batch was closed")
	}

	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		t.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames != 0 {
		t.Fatalf("command_wal.frames=%d, want 0 after failed batch Write", frames)
	}
}

func TestPublicCommandWALCheckpointPublishesOnlyCoveredLSNs(t *testing.T) {
	db, err := Open(Options{
		Dir:                 t.TempDir(),
		Durability:          DurabilityWALOnRelaxed,
		CommandWAL:          true,
		CommandWALStatsScan: true,
		DisableSideStores:   true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("covered"), []byte("v1")); err != nil {
		t.Fatalf("covered Set: %v", err)
	}

	var hookOnce sync.Once
	testAfterCachedCheckpoint = func() {
		hookOnce.Do(func() {
			if err := db.Set([]byte("post-cut"), []byte("v2")); err != nil {
				t.Errorf("post-cut Set: %v", err)
			}
		})
	}
	defer func() { testAfterCachedCheckpoint = nil }()
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if got := db.backend.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after checkpoint=%d, want only covered LSN 1", got)
	}
	first, last := db.publicCommandWALPendingRange()
	if first != 2 || last != 2 {
		t.Fatalf("pending command WAL range=(%d,%d), want post-cut LSN range (2,2)", first, last)
	}
	got, err := db.Get([]byte("post-cut"))
	if err != nil {
		t.Fatalf("Get(post-cut): %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("Get(post-cut)=%q, want v2", got)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("second Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("AppliedCommandLSN after second checkpoint=%d, want 2", got)
	}
}

func TestPublicCommandWALCheckpointPublishCapsAtCutoverLSN(t *testing.T) {
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("covered"), []byte("v1")); err != nil {
		t.Fatalf("covered Set: %v", err)
	}
	db.snapshotPublicCommandWALCheckpointCutover()
	db.recordPublicCommandWALPendingLSN(2)

	applied, ranges, err := db.preparePublicCommandWALPendingPublish(false)
	if err != nil {
		t.Fatalf("preparePublicCommandWALPendingPublish: %v", err)
	}
	if applied != 1 {
		t.Fatalf("prepared AppliedCommandLSN=%d, want cutover LSN 1", applied)
	}
	if len(ranges) != 1 || ranges[0].First != 1 || ranges[0].Last != 1 {
		t.Fatalf("prepared ranges=%+v, want [{First:1 Last:1}]", ranges)
	}

	// The synthetic post-cutover LSN is not backed by a cached mutation in this
	// test. Clear it so the close-time checkpoint does not try to publish it.
	db.clearPublicCommandWALPendingThrough(2)
}

func TestPublicCommandWALCheckpointRetainsCommandJournalSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:               dir,
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	before := publicCommandWALSegmentNames(t, dir)
	if len(before) == 0 {
		t.Fatal("expected command WAL segment before checkpoint")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	after := publicCommandWALSegmentNames(t, dir)
	afterSet := make(map[string]struct{}, len(after))
	for _, name := range after {
		afterSet[name] = struct{}{}
	}
	for _, name := range before {
		if _, ok := afterSet[name]; !ok {
			t.Fatalf("checkpoint removed active command WAL segment %s; before=%v after=%v", name, before, after)
		}
	}
}

func TestPublicCommandWALCheckpointPiggybacksAppliedLSN(t *testing.T) {
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	stats := db.cached.Stats()
	if got := stats["treedb.cache.command_wal.checkpoint_publish.piggybacked"]; got != "1" {
		t.Fatalf("piggybacked checkpoint publishes=%q, want 1", got)
	}
	if got := stats["treedb.cache.command_wal.checkpoint_publish.separate"]; got != "0" {
		t.Fatalf("separate checkpoint publishes=%q, want 0", got)
	}
}

func TestPublicCommandWALNoopCheckpointRunsPublishHook(t *testing.T) {
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	called := false
	db.cached.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		called = true
		return 1, []backenddb.CommandWALLSNRange{{First: 1, Last: 1}}, nil
	})
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if !called {
		t.Fatal("checkpoint publish hook was not called on no-op checkpoint")
	}
	if got := db.backend.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1 from no-op checkpoint publish", got)
	}
}

func publicCommandWALSegmentNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(backenddb.WALDirPath(dir))
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !commitlog.IsCommandSegmentName(entry.Name()) {
			continue
		}
		names = append(names, entry.Name())
	}
	return names
}

func TestPublicCommandWALCheckpointHookUsesSyncIntent(t *testing.T) {
	tests := []struct {
		name       string
		durability DurabilityMode
		wantSync   bool
	}{
		{name: "durable", durability: DurabilityDurable, wantSync: true},
		{name: "relaxed", durability: DurabilityWALOnRelaxed, wantSync: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := Open(Options{
				Dir:               t.TempDir(),
				Durability:        tt.durability,
				CommandWAL:        true,
				DisableSideStores: true,
			})
			if err != nil {
				t.Fatalf("Open command WAL: %v", err)
			}
			defer func() { _ = db.Close() }()
			if err := db.Set([]byte("k"), []byte("v")); err != nil {
				t.Fatalf("Set: %v", err)
			}

			called := false
			db.cached.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
				called = true
				if sync != tt.wantSync {
					t.Fatalf("checkpoint hook sync=%t, want %t", sync, tt.wantSync)
				}
				return 0, nil, nil
			})
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if !called {
				t.Fatal("checkpoint publish hook was not called")
			}
		})
	}
}

func TestPublicCommandWALCloseReportsFinalCheckpointError(t *testing.T) {
	checkpointErr := errors.New("forced checkpoint failure")
	var notified []error
	db, err := Open(Options{
		Dir:               t.TempDir(),
		Durability:        DurabilityWALOnRelaxed,
		CommandWAL:        true,
		DisableSideStores: true,
		NotifyError: func(err error) {
			notified = append(notified, err)
		},
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	db.cached.SetCommandWALCheckpointPublishHook(func(sync bool) (uint64, []backenddb.CommandWALLSNRange, error) {
		return 0, nil, checkpointErr
	})
	err = db.Close()
	if !errors.Is(err, checkpointErr) {
		t.Fatalf("Close error=%v, want checkpoint error", err)
	}
	found := false
	for _, err := range notified {
		if errors.Is(err, checkpointErr) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("NotifyError=%v, want checkpoint error", notified)
	}
}

func TestPublicCommandWALPublishSyncMatrix(t *testing.T) {
	tests := []struct {
		mode string
		sync bool
		want bool
	}{
		{mode: "wal_on_sync", sync: false, want: false},
		{mode: "wal_on_sync", sync: true, want: true},
		{mode: "wal_on_sync+no_read_checksum", sync: true, want: true},
		{mode: "wal_on_relaxed_sync", sync: true, want: false},
		{mode: "wal_on_relaxed_sync+verify_on_read", sync: true, want: false},
		{mode: "wal_off_relaxed_sync", sync: true, want: false},
	}
	for _, tt := range tests {
		if got := publicCommandWALPublishSync(tt.mode, tt.sync); got != tt.want {
			t.Fatalf("publicCommandWALPublishSync(%q, %t)=%t, want %t", tt.mode, tt.sync, got, tt.want)
		}
	}
}

func recvTestErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for goroutine")
		return nil
	}
}

func TestPublicCommandWALPendingClearPreservesNewerRange(t *testing.T) {
	var db DB
	db.recordPublicCommandWALPendingLSN(1)
	db.recordPublicCommandWALPendingLSN(3)
	db.clearPublicCommandWALPendingThrough(1)
	first, last := db.publicCommandWALPendingRange()
	if first != 2 || last != 3 {
		t.Fatalf("pending range after partial clear=(%d,%d), want (2,3)", first, last)
	}
	db.clearPublicCommandWALPendingThrough(3)
	first, last = db.publicCommandWALPendingRange()
	if first != 0 || last != 0 {
		t.Fatalf("pending range after full clear=(%d,%d), want (0,0)", first, last)
	}

	db.recordPublicCommandWALPendingLSN(4)
	db.clearPublicCommandWALPendingThrough(3)
	first, last = db.publicCommandWALPendingRange()
	if first != 4 || last != 4 {
		t.Fatalf("pending range after stale clear=(%d,%d), want newer LSN (4,4)", first, last)
	}

	db.clearPublicCommandWALPendingThrough(4)
	db.recordPublicCommandWALPendingLSN(5)
	first, last = db.publicCommandWALPendingRange()
	if first != 5 || last != 5 {
		t.Fatalf("pending range after record following full clear=(%d,%d), want new LSN (5,5)", first, last)
	}
}

func TestPublicCommandWALBatchResetFallbackKeepsWrapperUsable(t *testing.T) {
	inner := &commandWALNoResetBatch{}
	wrapped := &commandWALPublicBatch{inner: inner}
	_ = wrapped.payload.ResetWithHint(0, 0)
	wrapped.dirty = true
	wrapped.closed = true

	wrapped.Reset()
	if wrapped.closed {
		t.Fatal("Reset fallback marked command WAL batch closed")
	}
	if wrapped.dirty {
		t.Fatal("Reset fallback left command WAL batch dirty")
	}
	if err := wrapped.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set after Reset fallback: %v", err)
	}
	if len(inner.entries) != 1 {
		t.Fatalf("inner entries=%d, want 1 after Set", len(inner.entries))
	}
}

func TestPublicCommandWALBatchResetPreservesCompactZeroScanFallback(t *testing.T) {
	wrapped := newCommandWALPublicBatch(nil, &commandWALResetBatch{}, 8000)
	zeroValue := make([]byte, 128)
	key := []byte("k")

	if err := wrapped.SetView(key, zeroValue); err != nil {
		t.Fatalf("SetView before reset: %v", err)
	}
	payload, err := wrapped.commandWALPayload()
	if err != nil {
		t.Fatalf("commandWALPayload before reset: %v", err)
	}
	if len(payload) >= 6+9+len(key)+len(zeroValue) {
		t.Fatalf("commandWALPayload before reset len=%d, want compact zero payload below expanded size", len(payload))
	}
	visits := 0
	if err := commitlog.ScanRawKVBatchPayload(payload, func(op commitlog.RawKVOp, gotKey, gotValue []byte) error {
		visits++
		if op != commitlog.RawKVOpSet || string(gotKey) != string(key) || len(gotValue) != len(zeroValue) {
			t.Fatalf("scan before reset op=%v key=%q value_len=%d", op, gotKey, len(gotValue))
		}
		return nil
	}); err != nil {
		t.Fatalf("scan before reset: %v", err)
	}
	if visits != 1 {
		t.Fatalf("scan visits before reset=%d, want 1", visits)
	}

	wrapped.Reset()
	if err := wrapped.SetView(key, zeroValue); err != nil {
		t.Fatalf("SetView after reset: %v", err)
	}
	payload, err = wrapped.commandWALPayload()
	if err != nil {
		t.Fatalf("commandWALPayload after reset: %v", err)
	}
	if len(payload) >= 6+9+len(key)+len(zeroValue) {
		t.Fatalf("commandWALPayload after reset len=%d, want compact zero payload below expanded size", len(payload))
	}
	visits = 0
	if err := commitlog.ScanRawKVBatchPayload(payload, func(op commitlog.RawKVOp, gotKey, gotValue []byte) error {
		visits++
		if op != commitlog.RawKVOpSet || string(gotKey) != string(key) || len(gotValue) != len(zeroValue) {
			t.Fatalf("scan after reset op=%v key=%q value_len=%d", op, gotKey, len(gotValue))
		}
		return nil
	}); err != nil {
		t.Fatalf("scan after reset: %v", err)
	}
	if visits != 1 {
		t.Fatalf("scan visits after reset=%d, want 1", visits)
	}
}

type commandWALNoResetBatch struct {
	entries []batch.Entry
}

type commandWALResetBatch struct {
	commandWALNoResetBatch
}

func (b *commandWALResetBatch) Reset() {
	b.entries = b.entries[:0]
}

func (b *commandWALNoResetBatch) Set(key, value []byte) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	return nil
}

func (b *commandWALNoResetBatch) Delete(key []byte) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpDelete, Key: append([]byte(nil), key...)})
	return nil
}

func (b *commandWALNoResetBatch) DeleteRange(start, end []byte) error {
	var startCopy, endCopy []byte
	if start != nil {
		startCopy = append([]byte(nil), start...)
	}
	if end != nil {
		endCopy = append([]byte(nil), end...)
	}
	b.entries = append(b.entries, batch.Entry{Type: batch.OpDeleteRange, Key: startCopy, Value: endCopy})
	return nil
}

func (b *commandWALNoResetBatch) Write() error { return nil }

func (b *commandWALNoResetBatch) WriteSync() error { return nil }

func (b *commandWALNoResetBatch) Close() error { return nil }

func (b *commandWALNoResetBatch) Replay(fn func(batch.Entry) error) error {
	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *commandWALNoResetBatch) GetByteSize() (int, error) { return len(b.entries), nil }

func BenchmarkPublicCommandWALRawKVSet(b *testing.B) {
	for _, commandWAL := range []bool{false, true} {
		b.Run(fmt.Sprintf("command_wal=%t", commandWAL), func(b *testing.B) {
			db, err := Open(Options{
				Dir:                 b.TempDir(),
				Durability:          DurabilityWALOnRelaxed,
				CommandWAL:          commandWAL,
				CommandWALStatsScan: commandWAL,
				DisableSideStores:   true,
			})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			value := []byte("public-command-wal-value")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("k%09d", i))
				if err := db.Set(key, value); err != nil {
					b.Fatalf("Set: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "sets/s")
			if commandWAL {
				assertPublicCommandWALFramesB(b, db, uint64(b.N))
			}
		})
	}
}

func BenchmarkPublicCommandWALRawKVBatchWrite(b *testing.B) {
	for _, batchSize := range []int{64, 1024} {
		b.Run(fmt.Sprintf("batch_size=%d", batchSize), func(b *testing.B) {
			for _, commandWAL := range []bool{false, true} {
				b.Run(fmt.Sprintf("command_wal=%t", commandWAL), func(b *testing.B) {
					db, err := Open(Options{
						Dir:                 b.TempDir(),
						Durability:          DurabilityWALOnRelaxed,
						CommandWAL:          commandWAL,
						CommandWALStatsScan: commandWAL,
						DisableSideStores:   true,
					})
					if err != nil {
						b.Fatalf("Open: %v", err)
					}
					defer func() { _ = db.Close() }()

					value := []byte("public-command-wal-value")
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						batch := db.NewBatchWithSize(batchSize)
						base := i * batchSize
						for j := 0; j < batchSize; j++ {
							var keyBuf [32]byte
							key := strconv.AppendInt(keyBuf[:0], int64(base+j), 10)
							if err := batch.Set(key, value); err != nil {
								_ = batch.Close()
								b.Fatalf("batch Set: %v", err)
							}
						}
						if err := batch.Write(); err != nil {
							_ = batch.Close()
							b.Fatalf("batch Write: %v", err)
						}
						if err := batch.Close(); err != nil {
							b.Fatalf("batch Close: %v", err)
						}
					}
					b.StopTimer()
					totalSets := float64(b.N * batchSize)
					b.ReportMetric(totalSets/b.Elapsed().Seconds(), "sets/s")
					b.ReportMetric(float64(batchSize), "sets/batch")
					if commandWAL {
						assertPublicCommandWALFramesB(b, db, uint64(b.N))
					}
				})
			}
		})
	}
}

func assertPublicCommandWALFramesB(b *testing.B, db *DB, minFrames uint64) {
	b.Helper()
	stats := db.Stats()
	frames, err := strconv.ParseUint(stats["treedb.command_wal.frames"], 10, 64)
	if err != nil {
		b.Fatalf("parse command_wal.frames=%q: %v", stats["treedb.command_wal.frames"], err)
	}
	if frames < minFrames {
		b.Fatalf("command_wal.frames=%d, want at least %d", frames, minFrames)
	}
}

func TestPublicCommandWALDeleteRangeCachedReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	before := publicCommandWALFrameCount(t, db)
	if err := db.DeleteRange([]byte("b"), []byte("d")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if got := publicCommandWALFrameCount(t, db); got != before+1 {
		t.Fatalf("command_wal.frames=%d, want %d after one DB-level DeleteRange", got, before+1)
	}
	for _, key := range []string{"b", "c"} {
		has, err := db.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil before reopen", key, has, err)
		}
	}
	requireRawKVValue(t, db, []byte("a"), []byte("va"))
	requireRawKVValue(t, db, []byte("d"), []byte("vd"))

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	for _, key := range []string{"b", "c"} {
		has, err := reopen.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil after replay", key, has, err)
		}
	}
	requireRawKVValue(t, reopen, []byte("a"), []byte("va"))
	requireRawKVValue(t, reopen, []byte("d"), []byte("vd"))
	if got := reopen.backend.State().AppliedCommandLSN; got < before+1 {
		t.Fatalf("AppliedCommandLSN=%d, want at least %d after reopen", got, before+1)
	}
}

func TestPublicCommandWALDeleteRangeReplaysUnappliedFrame(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			_ = db.Close()
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	baseApplied := db.backend.State().AppliedCommandLSN
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("backend Open for manual command append: %v", err)
	}
	lsn, err := backend.AppendRawKVSingleCommandWAL(commitlog.RawKVOperation{Op: commitlog.RawKVOpDeleteRange, Key: []byte("b"), Value: []byte("d")}, true)
	if err != nil {
		_ = backend.Close()
		t.Fatalf("AppendRawKVSingleCommandWAL DeleteRange: %v", err)
	}
	if lsn <= baseApplied {
		_ = backend.Close()
		t.Fatalf("DeleteRange frame lsn=%d, want > base applied %d", lsn, baseApplied)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("backend Close after manual command append: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	for _, key := range []string{"b", "c"} {
		has, err := reopen.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil after replay", key, has, err)
		}
	}
	requireRawKVValue(t, reopen, []byte("a"), []byte("va"))
	requireRawKVValue(t, reopen, []byte("d"), []byte("vd"))
	if got := reopen.backend.State().AppliedCommandLSN; got < lsn {
		t.Fatalf("AppliedCommandLSN=%d, want at least replayed lsn %d", got, lsn)
	}
}

func TestPublicCommandWALDeleteRangeBoundsNoopFramesAndCheckpointLSN(t *testing.T) {
	db, err := Open(Options{
		Dir:                          t.TempDir(),
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"z", "vz"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	baseFrames := publicCommandWALFrameCount(t, db)
	for _, bounds := range []struct{ start, end []byte }{
		{start: []byte("c"), end: []byte("c")},
		{start: []byte("z"), end: []byte("a")},
		{start: nil, end: []byte{}},
	} {
		if err := db.DeleteRange(bounds.start, bounds.end); err != nil {
			t.Fatalf("noop DeleteRange(%q,%q): %v", bounds.start, bounds.end, err)
		}
		if got := publicCommandWALFrameCount(t, db); got != baseFrames {
			t.Fatalf("noop DeleteRange(%q,%q) frames=%d, want unchanged %d", bounds.start, bounds.end, got, baseFrames)
		}
	}

	if err := db.DeleteRange(nil, []byte("b")); err != nil {
		t.Fatalf("DeleteRange nil,b: %v", err)
	}
	if got := publicCommandWALFrameCount(t, db); got != baseFrames+1 {
		t.Fatalf("frames after lower-unbounded DeleteRange=%d, want %d", got, baseFrames+1)
	}
	for _, key := range []string{"a"} {
		has, err := db.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil", key, has, err)
		}
	}
	requireRawKVValue(t, db, []byte("b"), []byte("vb"))
	requireRawKVValue(t, db, []byte("c"), []byte("vc"))
	requireRawKVValue(t, db, []byte("z"), []byte("vz"))

	if err := db.DeleteRange([]byte("z"), nil); err != nil {
		t.Fatalf("DeleteRange z,nil: %v", err)
	}
	if got := publicCommandWALFrameCount(t, db); got != baseFrames+2 {
		t.Fatalf("frames after upper-unbounded DeleteRange=%d, want %d", got, baseFrames+2)
	}
	has, err := db.Has([]byte("z"))
	if err != nil || has {
		t.Fatalf("Has(z)=(%t,%v), want false,nil", has, err)
	}

	first, last := db.publicCommandWALPendingRange()
	if first == 0 || last != baseFrames+2 {
		t.Fatalf("pending command WAL range=(%d,%d), want non-empty ending at %d", first, last, baseFrames+2)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != baseFrames+2 {
		t.Fatalf("AppliedCommandLSN=%d, want %d", got, baseFrames+2)
	}
}

func TestPublicCommandWALDeleteRangeFullRangeInMemoryCheckpoint(t *testing.T) {
	db, err := Open(Options{
		Dir:                          t.TempDir(),
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	before := publicCommandWALFrameCount(t, db)
	if err := db.DeleteRange(nil, nil); err != nil {
		t.Fatalf("DeleteRange nil,nil: %v", err)
	}
	if got := publicCommandWALFrameCount(t, db); got != before+1 {
		t.Fatalf("frames after full-range DeleteRange=%d, want %d", got, before+1)
	}
	for _, key := range []string{"a", "b"} {
		has, err := db.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil after full range", key, has, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != before+1 {
		t.Fatalf("AppliedCommandLSN=%d, want %d", got, before+1)
	}
}

func TestPublicCommandWALDeleteRangeSnapshotIsolation(t *testing.T) {
	db, err := Open(Options{
		Dir:                          t.TempDir(),
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	if err := db.DeleteRange([]byte("b"), []byte("d")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	for _, key := range []string{"b", "c"} {
		has, err := snap.Has([]byte(key))
		if err != nil || !has {
			t.Fatalf("snapshot Has(%s)=(%t,%v), want true,nil", key, has, err)
		}
		has, err = db.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("db Has(%s)=(%t,%v), want false,nil after DeleteRange", key, has, err)
		}
	}
	requireRawKVValue(t, db, []byte("a"), []byte("va"))
	requireRawKVValue(t, db, []byte("d"), []byte("vd"))
}

func TestPublicCommandWALDeleteRangeValueLogPointersReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                          dir,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		DisableSideStores:            true,
		BackgroundCheckpointInterval: -1,
	}
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "left-pointer-value"}, {"b", "deleted-pointer-value"}, {"c", "right-pointer-value"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			_ = db.Close()
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	if err := db.DeleteRange([]byte("b"), []byte("c")); err != nil {
		_ = db.Close()
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, CommandWALStatsScan: true, DisableSideStores: true, BackgroundCheckpointInterval: -1})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	requireRawKVValue(t, reopen, []byte("a"), []byte("left-pointer-value"))
	requireRawKVValue(t, reopen, []byte("c"), []byte("right-pointer-value"))
	has, err := reopen.Has([]byte("b"))
	if err != nil || has {
		t.Fatalf("Has(b)=(%t,%v), want false,nil", has, err)
	}
}

func TestPublicCommandWALBatchDeleteRangeCachedReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, Durability: DurabilityWALOnRelaxed, CommandWAL: true})
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}
	b := db.NewBatch()
	if err := b.DeleteRange([]byte("a"), []byte("d")); err != nil {
		t.Fatalf("batch DeleteRange: %v", err)
	}
	if err := b.Set([]byte("b"), []byte("after")); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("batch WriteSync: %v", err)
	}
	_ = b.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopen.Close()
	for _, key := range []string{"a", "c"} {
		has, err := reopen.Has([]byte(key))
		if err != nil || has {
			t.Fatalf("Has(%s)=(%t,%v), want false,nil", key, has, err)
		}
	}
	got, err := reopen.Get([]byte("b"))
	if err != nil || string(got) != "after" {
		t.Fatalf("Get(b)=(%q,%v), want after,nil", got, err)
	}
	got, err = reopen.Get([]byte("d"))
	if err != nil || string(got) != "vd" {
		t.Fatalf("Get(d)=(%q,%v), want vd,nil", got, err)
	}
}
