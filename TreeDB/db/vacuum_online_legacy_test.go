package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type legacyOnlineVacuumTestCapabilityV1 struct{}

func (legacyOnlineVacuumTestCapabilityV1) allowLegacyOnlineVacuumV1() {}

func (db *DB) vacuumIndexOnlineLegacyForTest(ctx context.Context) error {
	return db.vacuumIndexOnlineLegacyV1(ctx, true, legacyOnlineVacuumTestCapabilityV1{})
}

func TestVacuumIndexOnlineUsesProductionRecoverableRootSetFence(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	oldIndex := db.idx.Load()
	if err := db.SetSync([]byte("before"), []byte("vacuum")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("VacuumIndexOnline: %v", err)
	}
	newIndex := db.idx.Load()
	if newIndex == nil || newIndex == oldIndex {
		t.Fatalf("published index=%p want replacement distinct from %p", newIndex, oldIndex)
	}
	if db.rootPublication == nil || db.rootPublication.idx != newIndex {
		t.Fatalf("root-publication index=%p want replacement %p", db.rootPublication.idx, newIndex)
	}
	stats := db.VacuumOnlineStats()
	if stats.AttemptID == 0 || !stats.WorkCompleted || stats.RecoverableSetCaptureAttempts != 1 || stats.RecoverableSetCaptures != 1 || stats.RecoverableRoots < 2 {
		t.Fatalf("vacuum stats=%+v want completed production recoverable-root snapshot", stats)
	}
	if stats.TotalDuration < stats.RecoverableSetCaptureDuration || stats.RecoverableSetCaptureDuration <= 0 || stats.OlderRootRebuilds != 1 || stats.OlderRootDurableResourceCaptures != 1 || stats.OlderRootDurableResourceCaptureDuration <= 0 || stats.OlderRootExactCandidateScans != 1 || stats.DurableResourceCaptures != 1 || !stats.ExactCandidateScan {
		t.Fatalf("vacuum attribution=%+v want capture, older rebuild, and durable-resource capture", stats)
	}
	if stats.ReplacementPagerPages == 0 || stats.ReplacementPagerPages != newIndex.pager.PageCount() {
		t.Fatalf("replacement pager pages=%d want final selected pager page count=%d", stats.ReplacementPagerPages, newIndex.pager.PageCount())
	}
}

func TestVacuumIndexOnlineSwapPublishEndsBeforeResourceSummary(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	database, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	seedVacuumOnlinePointer(t, database, "summary")
	var atSwap VacuumOnlineStats
	database.vacuumAfterSwapPublishHook = func(stats VacuumOnlineStats) { atSwap = stats }
	defer func() { database.vacuumAfterSwapPublishHook = nil }()

	if err := database.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("VacuumIndexOnline: %v", err)
	}
	if atSwap.SwapPublishDuration <= 0 || atSwap.DurableResourceDescriptors != 0 || atSwap.DurableResourceBytes != 0 {
		t.Fatalf("swap-boundary stats=%+v want timed swap before resource summary", atSwap)
	}
	if stats := database.VacuumOnlineStats(); stats.DurableResourceDescriptors == 0 || stats.DurableResourceBytes == 0 {
		t.Fatalf("completed vacuum stats=%+v want resource summary after swap", stats)
	}
}

func TestVacuumIndexOnlineFinalSyncFailureRetainsResourceTotals(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	database, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	seedVacuumOnlinePointer(t, database, "failure")
	wantErr := errors.New("injected final meta write failure")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.BeforeMetaWrite && event.Resource == durabilitycut.ResourceMeta && event.Path == filepath.Join(database.dir, indexNewFileName) {
			return wantErr
		}
		return nil
	})
	defer restore()
	finalSyncReached := make(chan struct{})
	releaseFinalSync := make(chan struct{})
	database.vacuumPagerSyncHook = func(phase vacuumPagerSyncPhase) {
		if phase == vacuumPagerSyncFinal {
			close(finalSyncReached)
			<-releaseFinalSync
		}
	}
	defer func() { database.vacuumPagerSyncHook = nil }()
	vacuumErr := make(chan error, 1)
	go func() { vacuumErr <- database.VacuumIndexOnline(context.Background()) }()
	waitVacuumTestSignal(t, finalSyncReached, "failing vacuum final sync")
	blockedAt := time.Now()
	time.Sleep(50 * time.Millisecond)
	blockedFor := time.Since(blockedAt)
	close(releaseFinalSync)

	if err := <-vacuumErr; !errors.Is(err, wantErr) {
		t.Fatalf("VacuumIndexOnline error=%v want %v", err, wantErr)
	}
	stats := database.VacuumOnlineStats()
	if stats.WorkCompleted || stats.DurableResourceCaptures != 1 || stats.DurableResourceDescriptors == 0 || stats.DurableResourceBytes == 0 {
		t.Fatalf("failed final-sync stats=%+v want captured resource totals", stats)
	}
	if stats.MaxWriterPause < blockedFor || database.vacuumCutoverInProgress.Load() {
		t.Fatalf("failed final-sync pause=%s gate=%t want released gate and pause >= %s", stats.MaxWriterPause, database.vacuumCutoverInProgress.Load(), blockedFor)
	}
}

func TestVacuumIndexOnlineRuntimeFailureRetainsResourceTotals(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	database, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	seedVacuumOnlinePointer(t, database, "runtime-failure")
	wantErr := errors.New("injected replacement runtime failure")
	database.vacuumReplacementRuntimeHook = func(*rootPublicationRuntimeV1) error { return wantErr }
	defer func() { database.vacuumReplacementRuntimeHook = nil }()

	if err := database.VacuumIndexOnline(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("VacuumIndexOnline error=%v want %v", err, wantErr)
	}
	stats := database.VacuumOnlineStats()
	if stats.WorkCompleted || stats.DurableResourceCaptures != 1 || stats.DurableResourceDescriptors == 0 || stats.DurableResourceBytes == 0 {
		t.Fatalf("failed runtime stats=%+v want captured resource totals", stats)
	}
}

func seedVacuumOnlinePointer(t *testing.T, database *DB, key string) {
	t.Helper()
	ptrs, err := database.AppendValueLogValues([][]byte{bytes.Repeat([]byte("value"), 256)})
	if err != nil {
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	if len(ptrs) != 1 {
		t.Fatalf("AppendValueLogValues pointers=%d want 1", len(ptrs))
	}
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte(key), ptrs[0]); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
}

func TestVacuumDurableResourceSummaryNil(t *testing.T) {
	if descriptors, bytes := vacuumDurableResourceSummary(nil); descriptors != 0 || bytes != 0 {
		t.Fatalf("nil resource summary=(%d,%d), want (0,0)", descriptors, bytes)
	}
}

func TestPublishVacuumOnlineStatsKeepsNewestAttempt(t *testing.T) {
	database := &DB{}
	database.publishVacuumOnlineStats(VacuumOnlineStats{AttemptID: 2, WorkCompleted: true})
	database.publishVacuumOnlineStats(VacuumOnlineStats{AttemptID: 1, Canceled: true})
	if got := database.VacuumOnlineStats(); got.AttemptID != 2 || !got.WorkCompleted {
		t.Fatalf("published stats=%+v want attempt 2", got)
	}
}

func TestVacuumIndexOnlineInitialCaptureFailureRecordsAttempt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	database.readOnly = true
	if err := database.VacuumIndexOnline(context.Background()); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("VacuumIndexOnline error=%v, want ErrReadOnly", err)
	}
	stats := database.VacuumOnlineStats()
	if stats.AttemptID == 0 || stats.Canceled || stats.RecoverableSetCaptureAttempts != 1 || stats.RecoverableSetCaptures != 0 || stats.RecoverableSetRecaptureAttempts != 0 || stats.RecoverableSetCaptureDuration <= 0 {
		t.Fatalf("capture-failure stats=%+v want one attempted and zero completed capture", stats)
	}
}

func TestVacuumIndexOnlineCancellationBeforeCutoverMutatesNoNamespace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := database.SetSync([]byte("before"), []byte("cancel")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	database.vacuumBeforeCutoverHook = func(int) { cancel() }
	defer func() { database.vacuumBeforeCutoverHook = nil }()
	var namespaceEvents []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace != "" {
			namespaceEvents = append(namespaceEvents, event)
		}
		return nil
	})
	defer restore()

	if err := database.VacuumIndexOnline(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("VacuumIndexOnline error=%v, want context.Canceled", err)
	}
	if stats := database.VacuumOnlineStats(); !stats.Canceled || stats.WorkCompleted || stats.UserTreeDuration <= 0 || stats.OlderRootRebuilds == 0 {
		t.Fatalf("canceled vacuum stats=%+v want canceled incomplete attempt", stats)
	}
	for _, event := range namespaceEvents {
		if event.Namespace == durabilitycut.NamespaceRename ||
			filepath.Clean(event.NewPath) == filepath.Join(dir, indexReadyFileName) {
			t.Fatalf("authoritative namespace event after pre-cutover cancellation: %#v", event)
		}
	}
	for _, name := range []string{indexNewFileName, indexReadyFileName, indexBakFileName} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("artifact %s remains: %v", name, err)
		}
	}
}

func TestVacuumIndexOnlineReplacementRuntimeFailurePrecedesRename(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if err := database.SetSync([]byte("before"), []byte("runtime")); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(dir, indexFileName)
	before, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("injected replacement runtime failure")
	database.vacuumReplacementRuntimeHook = func(*rootPublicationRuntimeV1) error { return wantErr }
	defer func() { database.vacuumReplacementRuntimeHook = nil }()

	if err := database.VacuumIndexOnline(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("VacuumIndexOnline error=%v, want %v", err, wantErr)
	}
	after, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("runtime construction failure replaced the authoritative index")
	}
	for _, name := range []string{indexNewFileName, indexReadyFileName, indexBakFileName} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("artifact %s remains: %v", name, err)
		}
	}
}

func TestVacuumIndexOnlineCancellationAfterOldRenameConverges(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("before"), []byte("irreversible")); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceRename &&
			filepath.Clean(event.OldPath) == filepath.Join(dir, indexFileName) &&
			filepath.Clean(event.NewPath) == filepath.Join(dir, indexBakFileName) {
			cancel()
		}
		return nil
	})
	if err := database.VacuumIndexOnline(ctx); err != nil {
		restore()
		_ = database.Close()
		t.Fatalf("VacuumIndexOnline after irreversible cancellation: %v", err)
	}
	restore()
	if !errors.Is(ctx.Err(), context.Canceled) {
		_ = database.Close()
		t.Fatalf("context error=%v, want canceled", ctx.Err())
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	got, err := reopened.Get([]byte("before"))
	if err != nil || string(got) != "irreversible" {
		t.Fatalf("reopen Get=%q err=%v", got, err)
	}
}

func TestVacuumIndexOnlinePostSwapWriteCheckpointAndReopenUseReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("before"), bytes.Repeat([]byte("a"), 64)); err != nil {
		_ = db.Close()
		t.Fatalf("seed: %v", err)
	}
	if err := db.VacuumIndexOnline(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("VacuumIndexOnline: %v", err)
	}
	replacement := db.idx.Load()
	if err := db.Set([]byte("after"), bytes.Repeat([]byte("b"), 64)); err != nil {
		_ = db.Close()
		t.Fatalf("post-swap Set: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("post-swap Checkpoint: %v", err)
	}
	if db.idx.Load() != replacement || db.rootPublication == nil || db.rootPublication.idx != replacement {
		_ = db.Close()
		t.Fatal("post-swap publication escaped the replacement generation")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	for key, want := range map[string]byte{"before": 'a', "after": 'b'} {
		got, err := reopened.Get([]byte(key))
		if err != nil || len(got) != 64 || got[0] != want {
			t.Fatalf("reopened Get(%q)=%q err=%v", key, got, err)
		}
	}
}

func TestVacuumIndexOnlinePreservesTwoRecoverySelectablePointerClosures(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir := t.TempDir()
	opts := Options{Dir: dir, DisableBackgroundPrune: true, ValueLog: ValueLogOptions{PointerThreshold: 1}}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	values := [][]byte{bytes.Repeat([]byte("older-"), 32), bytes.Repeat([]byte("newer-"), 32)}
	pointers := appendPointersInNewSegment(t, dir, 0, 71, 710_000, len(values), func(index int) []byte { return values[index] })
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	for index, key := range []string{"older", "newer"} {
		batch := database.NewBatch().(*Batch)
		if err := batch.SetPointer([]byte(key), pointers[index]); err != nil {
			t.Fatal(err)
		}
		if err := batch.WriteSync(); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
	}
	before := database.durableRoot.slotCommit
	if before[0] == 0 || before[1] == 0 || before[0] == before[1] {
		t.Fatalf("fixture slot commits=%v, want two distinct recovery generations", before)
	}
	if err := database.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("VacuumIndexOnline: %v", err)
	}
	after := database.durableRoot.slotCommit
	if after[0] == 0 || after[1] == 0 || after[0] == after[1] {
		t.Fatalf("replacement slot commits=%v, want two distinct recovery generations", after)
	}
	if got := database.stableIndexCaptures.Load(); got != 0 {
		t.Fatalf("stable index captures after replacement=%d, want 0", got)
	}
	if got := database.durableCandidateIndexCaptures.Load(); got != 0 {
		t.Fatalf("durable candidate captures after replacement=%d, want 0", got)
	}
	newestSlot := database.metaPageID
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen newest replacement slot: %v", err)
	}
	for index, key := range []string{"older", "newer"} {
		got, getErr := reopened.Get([]byte(key))
		if getErr != nil || !bytes.Equal(got, values[index]) {
			_ = reopened.Close()
			t.Fatalf("newest Get(%q)=(%q,%v), want pointer-backed value", key, got, getErr)
		}
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	indexPath := filepath.Join(dir, indexFileName)
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	image := make([]byte, page.PageSize)
	if _, err := indexFile.ReadAt(image, int64(newestSlot*page.PageSize)); err != nil {
		_ = indexFile.Close()
		t.Fatal(err)
	}
	image[page.PageHeaderSize+page.DurableMetaV1BodySize-1] ^= 0xff
	if _, err := indexFile.WriteAt(image, int64(newestSlot*page.PageSize)); err != nil {
		_ = indexFile.Close()
		t.Fatal(err)
	}
	if err := indexFile.Sync(); err != nil {
		_ = indexFile.Close()
		t.Fatal(err)
	}
	if err := indexFile.Close(); err != nil {
		t.Fatal(err)
	}

	fallback, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen fallback replacement slot: %v", err)
	}
	defer func() { _ = fallback.Close() }()
	got, err := fallback.Get([]byte("older"))
	if err != nil || !bytes.Equal(got, values[0]) {
		t.Fatalf("fallback older value=(%q,%v), want pointer-backed value", got, err)
	}
	if got, err := fallback.Get([]byte("newer")); err != nil && !errors.Is(err, tree.ErrKeyNotFound) || len(got) != 0 {
		t.Fatalf("fallback newer value=(%q,%v), want absent from distinct older slot", got, err)
	}
}
