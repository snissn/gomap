package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestVacuumIndexOnlineRefreshFailureLeavesOldIndexAuthoritative(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	opts := Options{
		Dir:                        dir,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := &unregisteredLeafPageLog{dir: dir}
	if err := leafLog.ensureWriter(); err != nil {
		_ = d.Close()
		t.Fatalf("ensure leaf writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, leafLog.path, leafLog.fileID)
	d.SetLeafPageLog(leafLog)

	b := d.NewBatch()
	for i := 0; i < 256; i++ {
		if err := b.Set([]byte(fmt.Sprintf("refresh-failure/%06d", i)), bytes.Repeat([]byte{byte(i)}, 64)); err != nil {
			_ = b.Close()
			_ = d.Close()
			t.Fatalf("batch set %d: %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		_ = d.Close()
		t.Fatalf("seed WriteSync: %v", err)
	}
	_ = b.Close()

	indexPath := filepath.Join(dir, indexFileName)
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		_ = d.Close()
		t.Fatalf("stat index before vacuum: %v", err)
	}
	vlogDir := ValueLogDirPath(dir)
	parkedVlogDir := vlogDir + ".parked"
	if err := os.Rename(vlogDir, parkedVlogDir); err != nil {
		_ = d.Close()
		t.Fatalf("park value-log directory: %v", err)
	}
	if err := os.WriteFile(vlogDir, []byte("force refresh scan failure\n"), 0o644); err != nil {
		_ = os.Rename(parkedVlogDir, vlogDir)
		_ = d.Close()
		t.Fatalf("install refresh failure fixture: %v", err)
	}

	vacuumErr := d.VacuumIndexOnline(context.Background())
	removeErr := os.Remove(vlogDir)
	restoreErr := os.Rename(parkedVlogDir, vlogDir)
	if removeErr != nil || restoreErr != nil {
		_ = d.Close()
		t.Fatalf("restore value-log directory: remove=%v rename=%v", removeErr, restoreErr)
	}
	if vacuumErr == nil {
		_ = d.Close()
		t.Fatal("vacuum succeeded despite forced value-log refresh failure")
	}
	if !errors.Is(vacuumErr, syscall.ENOTDIR) {
		t.Logf("vacuum returned platform refresh error: %v", vacuumErr)
	}
	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		_ = d.Close()
		t.Fatalf("stat index after vacuum: %v", err)
	}
	if !os.SameFile(beforeInfo, afterInfo) {
		_ = d.Close()
		t.Fatal("value-log refresh failure replaced the authoritative index")
	}
	for _, name := range []string{indexNewFileName, indexReadyFileName, indexBakFileName} {
		if _, statErr := os.Stat(filepath.Join(dir, name)); !errors.Is(statErr, os.ErrNotExist) {
			_ = d.Close()
			t.Fatalf("artifact %s remains after refresh failure: %v", name, statErr)
		}
	}
	if err := d.Set([]byte("after-refresh-failure"), []byte("still-open")); err != nil {
		_ = d.Close()
		t.Fatalf("write after failed vacuum: %v", err)
	}
	if got, err := d.Get([]byte("refresh-failure/000042")); err != nil || len(got) != 64 || got[0] != 42 {
		_ = d.Close()
		t.Fatalf("read after failed vacuum len=%d err=%v", len(got), err)
	}

	d.SetLeafPageLog(nil)
	if err := leafLog.Close(); err != nil {
		_ = d.Close()
		t.Fatalf("close leaf log: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if got, err := reopened.Get([]byte("after-refresh-failure")); err != nil || string(got) != "still-open" {
		t.Fatalf("reopen Get=%q err=%v", got, err)
	}
}

func TestVacuumIndexOnline_PostOldIndexRenameCutStopsNamespaceCleanup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.SetSync([]byte("vacuum-cut/key"), bytes.Repeat([]byte("v"), 256)); err != nil {
		t.Fatalf("seed SetSync: %v", err)
	}

	indexPath := filepath.Join(dir, indexFileName)
	newPath := filepath.Join(dir, indexNewFileName)
	bakPath := filepath.Join(dir, indexBakFileName)
	readyPath := filepath.Join(dir, indexReadyFileName)
	cutErr := errors.New("injected post-old-index rename cut")
	var namespaceEvents []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == "" {
			return nil
		}
		namespaceEvents = append(namespaceEvents, event)
		if event.Namespace == durabilitycut.NamespaceRename &&
			event.Resource == durabilitycut.ResourceIndex &&
			filepath.Clean(event.OldPath) == filepath.Clean(indexPath) &&
			filepath.Clean(event.NewPath) == filepath.Clean(bakPath) {
			return cutErr
		}
		return nil
	})
	defer restore()
	err = d.VacuumIndexOnline(context.Background())

	if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("VacuumIndexOnline error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	if !d.publicationPoisoned.Load() {
		t.Fatal("post-index-rename cut did not poison DB")
	}
	if len(namespaceEvents) == 0 {
		t.Fatal("no namespace events observed")
	}
	if got := namespaceEvents[len(namespaceEvents)-1]; got.Namespace != durabilitycut.NamespaceRename || filepath.Clean(got.OldPath) != filepath.Clean(indexPath) || filepath.Clean(got.NewPath) != filepath.Clean(bakPath) {
		t.Fatalf("last namespace event=%#v, want old-index rename as final mutation", got)
	}
	eventsAtCut := len(namespaceEvents)
	if retryErr := d.VacuumIndexOnline(context.Background()); !errors.Is(retryErr, ErrRecoveryRequired) {
		t.Fatalf("VacuumIndexOnline retry error=%v, want ErrRecoveryRequired", retryErr)
	}
	if len(namespaceEvents) != eventsAtCut {
		t.Fatalf("VacuumIndexOnline retry emitted namespace events=%#v, want none after cut", namespaceEvents[eventsAtCut:])
	}
	for _, path := range []string{newPath, bakPath, readyPath} {
		if _, statErr := os.Stat(path); statErr != nil {
			t.Fatalf("artifact %s stat=%v, want retained crash-cut namespace", filepath.Base(path), statErr)
		}
	}
	if _, statErr := os.Stat(indexPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("index path stat=%v, want renamed away at crash cut", statErr)
	}
	if err := d.SetSync([]byte("after-vacuum-cut"), []byte("blocked")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("SetSync after vacuum cut error=%v, want ErrRecoveryRequired", err)
	}
}

func collectLeafRefIDsFromRoot(t *testing.T, d *DB, rootID uint64) map[page.LeafLogPtr]struct{} {
	t.Helper()
	out := make(map[page.LeafLogPtr]struct{})
	if d == nil || rootID == 0 {
		return out
	}
	err := leafrefscan.Walk(context.Background(), rootID, d.Pager().Get, func(pageID uint64, n node.Node) error {
		if !n.VerifyChecksum() {
			return fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		return nil
	}, func(ptr page.LeafLogPtr) error {
		out[ptr] = struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("collect leaf refs: %v", err)
	}
	return out
}

func closeVacuumTestLeafPageLog(t *testing.T, d *DB, leafLog *registeredLeafPageLog) {
	t.Helper()
	if d != nil {
		d.SetLeafPageLog(nil)
	}
	if leafLog == nil {
		return
	}
	if err := leafLog.Close(); err != nil {
		t.Errorf("close leaf page log: %v", err)
	}
}

func TestVacuumBuildInternalTreeFromLeafRefs_StreamsChildren(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                        dir,
		ChunkSize:                  64 * 1024,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		PreferAppendAlloc:          true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()
	leafLog := &registeredLeafPageLog{db: d, dir: dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	d.SetLeafPageLog(leafLog)
	defer closeVacuumTestLeafPageLog(t, d, leafLog)

	val := bytes.Repeat([]byte("v"), 64)
	for version := 1; version <= 16; version++ {
		b := d.NewBatch()
		for i := 0; i < 512; i++ {
			key := []byte(fmt.Sprintf("stream/k/%08d/%08d", version, i))
			val[0] = byte(version)
			if err := b.Set(key, val); err != nil {
				t.Fatalf("set version=%d key=%d: %v", version, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("writesync version=%d: %v", version, err)
		}
		_ = b.Close()
	}

	state := d.State()
	if state == nil || state.RootPageID == 0 {
		t.Fatalf("missing root")
	}
	allLeafRefs, err := vacuumTreeAllLeafRefsIfComplete(d.Pager(), state.RootPageID)
	if err != nil {
		t.Fatalf("classify leaf-ref root: %v", err)
	}
	if !allLeafRefs {
		t.Fatalf("expected fixture root to contain only leaf refs")
	}
	before, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(d.Pager(), state.RootPageID)
	if err != nil {
		t.Fatalf("collect source leaf refs: %v", err)
	}
	if !allLeafRefs || len(before) == 0 {
		t.Fatalf("source leaf refs all=%v len=%d", allLeafRefs, len(before))
	}

	streamPager, err := pager.Open(filepath.Join(t.TempDir(), "streamed-index.db"), 64*1024)
	if err != nil {
		t.Fatalf("open stream pager: %v", err)
	}
	defer func() { _ = streamPager.Close() }()
	if _, err := streamPager.Alloc(2); err != nil {
		t.Fatalf("alloc stream pager metadata pages: %v", err)
	}
	streamRoot, err := vacuumBuildInternalTreeFromLeafRefs(d.Pager(), state.RootPageID, streamPager, &pagerAllocator{p: streamPager}, false)
	if err != nil {
		t.Fatalf("stream leaf refs: %v", err)
	}
	after, allLeafRefs, err := vacuumCollectLeafRefChildrenIfComplete(streamPager, streamRoot)
	if err != nil {
		t.Fatalf("collect streamed leaf refs: %v", err)
	}
	if !allLeafRefs {
		t.Fatalf("streamed root is not leaf-ref complete")
	}
	if len(after) != len(before) {
		t.Fatalf("leaf-ref count mismatch: before=%d after=%d", len(before), len(after))
	}
	for i := range before {
		if !bytes.Equal(after[i].key, before[i].key) || after[i].childRef != before[i].childRef {
			t.Fatalf("leaf child %d mismatch: before=%+v after=%+v", i, before[i], after[i])
		}
	}
}

type countingLeafPageLog struct {
	inner   LeafPageLog
	appends atomic.Uint64
}

func (l *countingLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	l.appends.Add(1)
	return l.inner.AppendLeafPage(leafPage)
}

func (l *countingLeafPageLog) Flush() error { return l.inner.Flush() }
func (l *countingLeafPageLog) Sync() error  { return l.inner.Sync() }

func TestVacuumIndexOnline_ShrinksAndPreservesData(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:               dir,
		ChunkSize:         chunkSize,
		KeepRecent:        1,
		PreferAppendAlloc: true, // intentionally bloat index.db under churn
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	value := bytes.Repeat([]byte("v"), 200) // inline-ish to force page pressure
	for round := 0; round < 6; round++ {
		b := d.NewBatch()
		for i := 0; i < 4000; i++ {
			k := []byte(fmt.Sprintf("k%06d", i))
			if err := b.Set(k, value); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}

	indexPath := filepath.Join(dir, indexFileName)
	beforeInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var freeManyChecksumUpdates atomic.Int64
	freelist.TestHookFreeManyBeforeChecksum = func() { freeManyChecksumUpdates.Add(1) }
	defer func() { freelist.TestHookFreeManyBeforeChecksum = nil }()
	snapshotReady := make(chan struct{})
	continueVacuum := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(continueVacuum)
		}
	}()
	testHookVacuumAfterBaseSnapshot = func() {
		close(snapshotReady)
		<-continueVacuum
	}
	defer func() { testHookVacuumAfterBaseSnapshot = nil }()
	vacuumDone := make(chan error, 1)
	go func() { vacuumDone <- d.VacuumIndexOnline(ctx) }()
	select {
	case <-snapshotReady:
	case err := <-vacuumDone:
		t.Fatalf("vacuum finished before base snapshot hook: %v", err)
	case <-ctx.Done():
		t.Fatalf("waiting for vacuum base snapshot: %v", ctx.Err())
	}

	deltaValue := bytes.Repeat([]byte("w"), 200)
	delta := d.NewBatch()
	if err := delta.Set([]byte("k000010"), deltaValue); err != nil {
		t.Fatalf("delta set: %v", err)
	}
	deltaErr := delta.Write()
	_ = delta.Close()
	close(continueVacuum)
	released = true
	if deltaErr != nil {
		t.Fatalf("delta write: %v", deltaErr)
	}
	if err := <-vacuumDone; err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if got := d.stableIndexCaptures.Load(); got != 0 {
		t.Fatalf("recoverable-root recapture leaked stable index pins: %d", got)
	}
	if got := freeManyChecksumUpdates.Load(); got == 0 {
		t.Fatal("online vacuum did not batch retired pages through FreeMany")
	}

	afterInfo, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf("expected vacuum to shrink index.db: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}

	got, err := d.Get([]byte("k000010"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, deltaValue) {
		t.Fatalf("value mismatch")
	}
}

func TestVacuumIndexOnline_OuterLeavesInValueLog_DoesNotRewriteLeafPages(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                        dir,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		PreferAppendAlloc:          true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	baseLeafLog := &registeredLeafPageLog{db: d, dir: dir}
	if err := baseLeafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	leafLog := &countingLeafPageLog{inner: baseLeafLog}
	d.SetLeafPageLog(leafLog)
	defer closeVacuumTestLeafPageLog(t, d, baseLeafLog)

	val := bytes.Repeat([]byte("v"), 64)
	for version := 1; version <= 24; version++ {
		b := d.NewBatch()
		for i := 0; i < 256; i++ {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, i))
			val[0] = byte(version)
			if err := b.Set(key, val); err != nil {
				t.Fatalf("set version=%d key=%d: %v", version, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("writesync version=%d: %v", version, err)
		}
		_ = b.Close()
	}

	stBefore := d.State()
	if stBefore == nil {
		t.Fatalf("missing state before vacuum")
	}
	beforeRefs := collectLeafRefIDsFromRoot(t, d, stBefore.RootPageID)
	if len(beforeRefs) == 0 {
		t.Fatalf("expected outer-leaf refs before vacuum")
	}

	// Reset after initial population; we only want to observe what vacuum does.
	leafLog.appends.Store(0)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if got := leafLog.appends.Load(); got != 0 {
		t.Fatalf("vacuum rewrote outer leaf pages: leaf_page_appends=%d want 0", got)
	}

	stAfter := d.State()
	if stAfter == nil {
		t.Fatalf("missing state after vacuum")
	}
	afterRefs := collectLeafRefIDsFromRoot(t, d, stAfter.RootPageID)
	if len(afterRefs) == 0 {
		t.Fatalf("expected outer-leaf refs after vacuum")
	}
	if len(afterRefs) != len(beforeRefs) {
		t.Fatalf("leafref count changed across vacuum: before=%d after=%d", len(beforeRefs), len(afterRefs))
	}

	for _, version := range []int{1, 12, 24} {
		for _, idx := range []int{0, 127, 255} {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, idx))
			got, err := d.Get(key)
			if err != nil {
				t.Fatalf("get version=%d key=%d after vacuum: %v", version, idx, err)
			}
			if len(got) != len(val) {
				t.Fatalf("value length mismatch version=%d key=%d: got=%d want=%d", version, idx, len(got), len(val))
			}
			if got[0] != byte(version) {
				t.Fatalf("value content mismatch version=%d key=%d: got[0]=%d want=%d", version, idx, got[0], byte(version))
			}
		}
	}
}

func TestVacuumIndexOnline_AllowsSnapshotAcrossSwap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:        dir,
		ChunkSize:  chunkSize,
		KeepRecent: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.SetSync([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("set v1: %v", err)
	}

	snap := d.AcquireSnapshot()

	if err := d.SetSync([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("set v2: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	// DB reads see the latest value.
	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("expected v2, got %q", got)
	}

	// Old snapshot remains valid and sees the older value.
	old, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snap get: %v", err)
	}
	if string(old) != "v1" {
		t.Fatalf("expected v1 from snapshot, got %q", old)
	}

	d.idxMu.Lock()
	genCount := len(d.idxAll)
	d.idxMu.Unlock()
	if genCount < 2 {
		t.Fatalf("expected at least 2 index generations after vacuum, got %d", genCount)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snap close: %v", err)
	}

	d.idxMu.Lock()
	genCountAfter := len(d.idxAll)
	d.idxMu.Unlock()
	if genCountAfter != 1 {
		t.Fatalf("expected old index generation to be released after snapshot close; gens=%d", genCountAfter)
	}
}

func TestVacuumIndexOnline_RepeatWhileSnapshotPinned(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()
	chunkSize := int64(64 * 1024)

	d, err := Open(Options{
		Dir:        dir,
		ChunkSize:  chunkSize,
		KeepRecent: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.SetSync([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("set v1: %v", err)
	}

	snap := d.AcquireSnapshot()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := d.SetSync([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum 1: %v", err)
	}

	d.idxMu.Lock()
	genCount1 := len(d.idxAll)
	d.idxMu.Unlock()
	if genCount1 != 2 {
		t.Fatalf("expected 2 index generations after first vacuum, got %d", genCount1)
	}

	if err := d.SetSync([]byte("k"), []byte("v3")); err != nil {
		t.Fatalf("set v3: %v", err)
	}
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum 2: %v", err)
	}

	// DB reads see the latest value.
	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "v3" {
		t.Fatalf("expected v3, got %q", got)
	}

	// Old snapshot remains valid and sees the older value.
	old, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snap get: %v", err)
	}
	if string(old) != "v1" {
		t.Fatalf("expected v1 from snapshot, got %q", old)
	}

	d.idxMu.Lock()
	genCount2 := len(d.idxAll)
	d.idxMu.Unlock()
	if genCount2 < 2 {
		t.Fatalf("expected at least 2 index generations after second vacuum, got %d", genCount2)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snap close: %v", err)
	}

	d.idxMu.Lock()
	genCountAfter := len(d.idxAll)
	d.idxMu.Unlock()
	if genCountAfter != 1 {
		t.Fatalf("expected old index generations to be released after snapshot close; gens=%d", genCountAfter)
	}
}

func TestVacuumIndexOnline_RebuildsPackedInternalTreeForLeafRefs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                        dir,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		PreferAppendAlloc:          true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()
	leafLog := &registeredLeafPageLog{db: d, dir: dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	d.SetLeafPageLog(leafLog)
	defer closeVacuumTestLeafPageLog(t, d, leafLog)

	val := bytes.Repeat([]byte("v"), 64)
	const (
		stores   = 12
		versions = 120
		keys     = 48
	)

	for version := 1; version <= versions; version++ {
		for store := 0; store < stores; store++ {
			b := d.NewBatch()
			for i := 0; i < keys; i++ {
				key := []byte(fmt.Sprintf("s/k:store%02d/n/%08d/%08d", store, version, i))
				val[0] = byte(version)
				val[1] = byte(store)
				if err := b.Set(key, val); err != nil {
					t.Fatalf("set version=%d store=%d key=%d: %v", version, store, i, err)
				}
			}
			if err := b.WriteSync(); err != nil {
				t.Fatalf("writesync version=%d store=%d: %v", version, store, err)
			}
			_ = b.Close()
		}
	}

	parse := func(report map[string]string, key string) uint64 {
		t.Helper()
		v, err := strconv.ParseUint(report[key], 10, 64)
		if err != nil {
			t.Fatalf("parse %s=%q: %v", key, report[key], err)
		}
		return v
	}

	before, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(before): %v", err)
	}
	beforeP50 := parse(before, "treedb.user.internal_fill_ppm_p50")
	beforeAvg := parse(before, "treedb.user.internal_fill_ppm_avg")
	beforeSpanRatio := parse(before, "treedb.user.pages.span_ratio_ppm")
	beforePagesTotal := parse(before, "treedb.pages.total")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	after, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport(after): %v", err)
	}
	afterP50 := parse(after, "treedb.user.internal_fill_ppm_p50")
	afterAvg := parse(after, "treedb.user.internal_fill_ppm_avg")

	afterSpanRatio := parse(after, "treedb.user.pages.span_ratio_ppm")
	afterPagesTotal := parse(after, "treedb.pages.total")
	if afterSpanRatio >= beforeSpanRatio {
		t.Fatalf("expected vacuum to compact internal page span: before=%d after=%d before=%v after=%v", beforeSpanRatio, afterSpanRatio, before, after)
	}
	if afterPagesTotal >= beforePagesTotal {
		t.Fatalf("expected vacuum to reduce total page count: before=%d after=%d before=%v after=%v", beforePagesTotal, afterPagesTotal, before, after)
	}
	if afterP50 < beforeP50 || afterAvg < beforeAvg {
		t.Fatalf("expected vacuum to preserve internal fill while compacting span: beforeP50=%d afterP50=%d beforeAvg=%d afterAvg=%d before=%v after=%v", beforeP50, afterP50, beforeAvg, afterAvg, before, after)
	}
}

func TestVacuumIndexOnline_PreservesOuterLeafRefsAndDataWhenOuterLeavesInValueLog(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                        dir,
		KeepRecent:                 1,
		Durability:                 DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		PreferAppendAlloc:          true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()
	leafLog := &registeredLeafPageLog{db: d, dir: dir}
	if err := leafLog.ensureWriter(); err != nil {
		t.Fatalf("ensure leaf writer: %v", err)
	}
	d.SetLeafPageLog(leafLog)
	defer closeVacuumTestLeafPageLog(t, d, leafLog)

	val := bytes.Repeat([]byte("v"), 64)
	for version := 1; version <= 48; version++ {
		b := d.NewBatch()
		for i := 0; i < 512; i++ {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, i))
			val[0] = byte(version)
			if err := b.Set(key, val); err != nil {
				t.Fatalf("set version=%d key=%d: %v", version, i, err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("writesync version=%d: %v", version, err)
		}
		_ = b.Close()
	}

	stateBefore := d.State()
	if stateBefore == nil {
		t.Fatalf("missing state before vacuum")
	}
	beforeRefs := collectLeafRefIDsFromRoot(t, d, stateBefore.RootPageID)
	if len(beforeRefs) == 0 {
		t.Fatalf("expected outer-leaf refs before vacuum")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	stateAfter := d.State()
	if stateAfter == nil {
		t.Fatalf("missing state after vacuum")
	}
	afterRefs := collectLeafRefIDsFromRoot(t, d, stateAfter.RootPageID)
	if len(afterRefs) == 0 {
		t.Fatalf("expected outer-leaf refs after vacuum")
	}
	if len(afterRefs) != len(beforeRefs) {
		t.Fatalf("leafref count changed across vacuum: before=%d after=%d", len(beforeRefs), len(afterRefs))
	}

	for _, version := range []int{1, 24, 48} {
		for _, idx := range []int{0, 127, 511} {
			key := []byte(fmt.Sprintf("s/k:store/n/%08d/%08d", version, idx))
			got, err := d.Get(key)
			if err != nil {
				t.Fatalf("get version=%d key=%d after vacuum: %v", version, idx, err)
			}
			if len(got) != len(val) {
				t.Fatalf("value length mismatch version=%d key=%d: got=%d want=%d", version, idx, len(got), len(val))
			}
			if got[0] != byte(version) {
				t.Fatalf("value content mismatch version=%d key=%d: got[0]=%d want=%d", version, idx, got[0], byte(version))
			}
		}
	}
}

func TestVacuumIndexOnline_StampsPendingLeafGenerationSegments(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	db, leafLog := openLeafGenerationGCTestDB(t)

	writeLeafGenerationKeys(t, db, "vacuum-pending", 64, 'a')
	if _, fileID1 := currentLeafSegmentOrFatal(t, leafLog); fileID1 == 0 {
		t.Fatal("missing initial leaf segment")
	}
	if err := leafLog.rotateLeaf(); err != nil {
		t.Fatalf("rotateLeaf: %v", err)
	}
	_, fileID2 := currentLeafSegmentOrFatal(t, leafLog)
	rawFileID2 := page.ValueLogSegmentID(fileID2)

	stateBefore := db.State()
	if stateBefore == nil || stateBefore.LeafGenerations == nil {
		t.Fatalf("missing leaf generations before vacuum")
	}
	if _, ok := stateBefore.LeafGenerations.FileToGeneration[rawFileID2]; ok {
		t.Fatalf("pending raw file %d visible before vacuum", rawFileID2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	stateAfter := db.State()
	if stateAfter == nil || stateAfter.LeafGenerations == nil {
		t.Fatalf("missing leaf generations after vacuum")
	}
	if _, ok := stateAfter.LeafGenerations.FileToGeneration[rawFileID2]; !ok {
		t.Fatalf("pending raw file %d missing from published leaf-generation view", rawFileID2)
	}
	manifestAfter := loadLeafGenerationManifestOrFatal(t, db.dir)
	gen2 := findLeafGenerationByFileID(t, manifestAfter, rawFileID2)
	if got, want := gen2.PublishedCommitSeq, stateAfter.CommitSeq; got != want {
		t.Fatalf("published commit seq=%d, want vacuum commit seq %d", got, want)
	}
	db.leafGenerationPendingMu.Lock()
	_, pending := db.leafGenerationPendingSet[rawFileID2]
	db.leafGenerationPendingMu.Unlock()
	if pending {
		t.Fatalf("raw file %d still pending after vacuum publish", rawFileID2)
	}
}
