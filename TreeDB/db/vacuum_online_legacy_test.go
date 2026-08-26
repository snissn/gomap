package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

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
	if !stats.WorkCompleted || stats.RecoverableSetCaptures != 1 || stats.RecoverableRoots < 2 {
		t.Fatalf("vacuum stats=%+v want completed production recoverable-root snapshot", stats)
	}
	if stats.TotalDuration < stats.RecoverableSetCaptureDuration || stats.RecoverableSetCaptureDuration <= 0 || stats.OlderRootRebuilds != 1 || stats.OlderRootDurableResourceCaptures != 1 || stats.OlderRootDurableResourceCaptureDuration <= 0 || stats.DurableResourceCaptures != 1 || !stats.ExactCandidateScan {
		t.Fatalf("vacuum attribution=%+v want capture, older rebuild, and durable-resource capture", stats)
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
	if stats := database.VacuumOnlineStats(); !stats.Canceled || stats.WorkCompleted {
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
