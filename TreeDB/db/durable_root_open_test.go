package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOpenFreshUsesOneDurableRootSlotAndReopens(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	idx := database.idx.Load()
	if idx == nil || idx.pager == nil {
		t.Fatal("missing index pager")
	}
	meta, err := readDurableMetaSlotV1(idx.pager, MetaPage0ID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CommitSeq != 1 || database.metaPageID != MetaPage0ID {
		t.Fatalf("initial commit/slot=(%d,%d), want (1,0)", meta.CommitSeq, database.metaPageID)
	}
	if _, err := readDurableMetaSlotV1(idx.pager, MetaPage1ID); err == nil || errors.Is(err, page.ErrDurableMetaLegacyFormat) {
		t.Fatalf("unused slot error=%v, want unsealed non-legacy slot", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state := reopened.State()
	if state == nil || state.CommitSeq != 1 || state.RootPageID < 2 || state.SystemRootPageID < 2 {
		t.Fatalf("reopened state=%+v", state)
	}
	if reopened.idx.Load().allocator.COWGenerationV1() == nil {
		t.Fatal("reopened allocator did not install selected COW generation")
	}
}

func TestScanCandidateValueLogReferencesUsesCallerPublicationLease(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	idx := database.idx.Load()
	if idx == nil {
		t.Fatal("missing index generation")
	}
	database.valueLogPublicationMu.Lock()
	done := make(chan error, 1)
	go func() {
		_, scanErr := database.scanCandidateValueLogReferencesV1(idx, database.meta, true)
		done <- scanErr
	}()
	select {
	case scanErr := <-done:
		database.valueLogPublicationMu.Unlock()
		if scanErr != nil {
			t.Fatalf("scan candidate references: %v", scanErr)
		}
	case <-time.After(2 * time.Second):
		// Release the gate so the pre-fix implementation can unwind instead of
		// leaking a permanently blocked goroutine into the remainder of the suite.
		database.valueLogPublicationMu.Unlock()
		<-done
		t.Fatal("candidate dependency scan recursively acquired valueLogPublicationMu")
	}
}

func TestStatsRenderSelectedDurableRootV1(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	selected := database.durableRoot
	record := selected.record
	want := map[string]string{
		"treedb.durable_root.format_version":     "1",
		"treedb.durable_root.selected_slot":      fmt.Sprint(selected.slot),
		"treedb.durable_root.commit_seq":         fmt.Sprint(record.CommitSeq),
		"treedb.durable_root.durable_seq":        fmt.Sprint(record.DurableSeq),
		"treedb.durable_root.record_page":        fmt.Sprint(selected.meta.RootRecordPageID),
		"treedb.durable_root.user_root_page":     fmt.Sprint(record.UserRootPageID),
		"treedb.durable_root.system_root_page":   fmt.Sprint(record.SystemRootPageID),
		"treedb.durable_root.total_pages":        fmt.Sprint(record.TotalPages),
		"treedb.durable_root.manifest.entries":   fmt.Sprint(record.Manifest.EntryCount),
		"treedb.durable_root.manifest.pages":     fmt.Sprint(record.Manifest.PageCount),
		"treedb.durable_root.parent.record_page": fmt.Sprint(record.ParentRecordPageID),
		"treedb.durable_root.parent.commit_seq":  fmt.Sprint(record.ParentCommitSeq),
		"treedb.durable_root.slot0.commit_seq":   fmt.Sprint(selected.slotCommit[0]),
		"treedb.durable_root.slot1.commit_seq":   fmt.Sprint(selected.slotCommit[1]),
	}
	stats := database.Stats()
	for key, expected := range want {
		if got := stats[key]; got != expected {
			t.Errorf("%s=%q want %q", key, got, expected)
		}
	}
	for _, key := range []string{
		"treedb.durable_root.freelist.header_page",
		"treedb.durable_root.freelist.generation",
		"treedb.durable_root.freelist.high_water",
		"treedb.durable_root.freelist.free_count",
		"treedb.durable_root.freelist.retired_count",
		"treedb.durable_root.manifest.first_page",
		"treedb.durable_root.manifest.bytes",
		"treedb.durable_root.manifest_build.count",
		"treedb.durable_root.manifest_build.nanos",
		"treedb.durable_root.manifest_build.entries_visited",
		"treedb.durable_root.manifest_build.entries_encoded",
		"treedb.durable_root.manifest_build.bytes_encoded",
	} {
		if stats[key] == "" {
			t.Errorf("missing %s", key)
		}
	}
}

func TestDurableRootPublicationAlternatesIndependentSlotsAndReopens(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if database.metaPageID != MetaPage1ID || database.durableRoot.slotCommit != [2]uint64{1, 2} {
		t.Fatalf("after first publish slot/commits=(%d,%v), want (1,[1 2])", database.metaPageID, database.durableRoot.slotCommit)
	}
	if err := database.SetSync([]byte("b"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if database.metaPageID != MetaPage0ID || database.durableRoot.slotCommit != [2]uint64{3, 2} {
		t.Fatalf("after second publish slot/commits=(%d,%v), want (0,[3 2])", database.metaPageID, database.durableRoot.slotCommit)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.metaPageID != MetaPage0ID || reopened.durableRoot.slotCommit != [2]uint64{3, 2} {
		t.Fatalf("reopened slot/commits=(%d,%v), want (0,[3 2])", reopened.metaPageID, reopened.durableRoot.slotCommit)
	}
	for slot := uint64(MetaPage0ID); slot <= MetaPage1ID; slot++ {
		pages, err := durableRootSlotAuxiliaryPagesV1(reopened.durableRoot.slotMeta[slot], reopened.durableRoot.slotRecord[slot])
		if err != nil || len(pages) == 0 {
			t.Fatalf("reopened slot %d auxiliary inventory=(%v,%v), want complete", slot, pages, err)
		}
	}
	for key, want := range map[string]string{"a": "one", "b": "two"} {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%q)=%q, want %q", key, got, want)
		}
	}
}

func TestDurableRootReuseCutoverBlocksOldGenerationCapture(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SetSync([]byte("seed"), []byte("one")); err != nil {
		t.Fatal(err)
	}

	prepared := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	database.testDurableRootCandidatePreparedHook = func() {
		once.Do(func() {
			close(prepared)
			<-release
		})
	}
	defer func() { database.testDurableRootCandidatePreparedHook = nil }()

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- database.SetSync([]byte("next"), []byte("two"))
	}()
	select {
	case <-prepared:
	case <-time.After(5 * time.Second):
		t.Fatal("durable publication did not reach prepared cutover")
	}

	snapshotStarted := make(chan struct{})
	snapshotDone := make(chan *Snapshot, 1)
	go func() {
		close(snapshotStarted)
		snapshotDone <- database.AcquireSnapshot()
	}()
	<-snapshotStarted
	select {
	case snapshot := <-snapshotDone:
		if snapshot != nil {
			_ = snapshot.Close()
		}
		t.Fatal("snapshot captured the old visible generation during COW reuse cutover")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	snapshot := <-snapshotDone
	if snapshot == nil {
		t.Fatal("missing snapshot after durable cutover")
	}
	defer snapshot.Close()
	state := snapshot.State()
	if state == nil || state.CommitSeq != database.currentCommitSeq() {
		t.Fatalf("snapshot/current commit=(%+v,%d), want matching post-cutover generation", state, database.currentCommitSeq())
	}
	got, err := snapshot.Get([]byte("next"))
	if err != nil || string(got) != "two" {
		t.Fatalf("snapshot next=(%q,%v), want two", got, err)
	}
}

func TestDurableRootInvalidReferenceTrackerScansBeforeReuseCutover(t *testing.T) {
	database, err := Open(Options{
		Dir:                    t.TempDir(),
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SetSync([]byte("seed"), []byte("value-log-seed")); err != nil {
		t.Fatal(err)
	}
	database.valueLogRefTracker.invalidate()

	done := make(chan error, 1)
	go func() {
		done <- database.SetSync([]byte("next"), []byte("value-log-next"))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("durable publication deadlocked while scanning an invalid reference tracker")
	}
	got, err := database.Get([]byte("next"))
	if err != nil || string(got) != "value-log-next" {
		t.Fatalf("Get(next)=(%q,%v), want value-log-next", got, err)
	}
}

func TestDurableRootManifestPinsValueLogIdentityAndFallsBackWhenNewestDependencyIsMissing(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 10_000, 1, func(int) []byte {
		value := make([]byte, 4<<10)
		for i := range value {
			value[i] = byte(i)
		}
		return value
	})
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("external"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if database.durableRoot.slot != MetaPage1ID || database.durableRoot.manifest == nil {
		t.Fatalf("durable root slot/manifest=(%d,%v), want slot 1 and manifest", database.durableRoot.slot, database.durableRoot.manifest)
	}
	entries := database.durableRoot.manifest.Entries()
	if len(entries) != 1 || entries[0].Kind != rootpublication.ResourceValueLog {
		t.Fatalf("manifest entries=%+v, want one value-log dependency", entries)
	}
	if entries[0].Frontier.Bytes == 0 || entries[0].Identity.Generation != entries[0].Generation {
		t.Fatalf("manifest value-log identity/frontier=%+v/%+v", entries[0].Identity, entries[0].Frontier)
	}
	resources := database.durableRoot.slotResources[MetaPage1ID]
	if resources == nil || resources.Len() != 1 {
		t.Fatalf("slot 1 resource closure=%v len=%d, want one retained dependency", resources, resources.Len())
	}
	registry := database.valueLogIdentityPins
	if registry.ActivePins() == 0 {
		t.Fatal("published durable slot did not retain its value-log identity pin")
	}
	dependencyPath := filepath.Join(dir, filepath.FromSlash(entries[0].DiagnosticPath))
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("durable slot pins after close=%d, want 0", got)
	}
	if err := os.Rename(dependencyPath, dependencyPath+".missing"); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.metaPageID != MetaPage0ID || reopened.currentCommitSeq() != 1 {
		t.Fatalf("fallback slot/commit=(%d,%d), want (0,1)", reopened.metaPageID, reopened.currentCommitSeq())
	}
	found, err := reopened.Has([]byte("external"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("fallback root unexpectedly contains value-log-backed key")
	}
}

func TestDurableRootRecoveryRetainsAndReplacesBothSlotDependencyClosures(t *testing.T) {
	dir := t.TempDir()
	options := Options{Dir: dir, ValueLog: ValueLogOptions{PointerThreshold: 1}}
	database, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 20_000, 1, func(int) []byte {
		return []byte("durable external value")
	})
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("external"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("inline-1"), []byte{}); err != nil {
		t.Fatal(err)
	}
	for slot, resources := range database.durableRoot.slotResources {
		if resources == nil || resources.Len() != 1 {
			t.Fatalf("published slot %d closure=%v len=%d, want one dependency", slot, resources, resources.Len())
		}
	}
	registry := database.valueLogIdentityPins
	if got := registry.ActivePins(); got != 3 {
		t.Fatalf("published slot plus visible-root pins=%d, want exactly 3", got)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("published slot pins after close=%d, want 0", got)
	}

	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	reopenedRegistry := reopened.valueLogIdentityPins
	for slot, resources := range reopened.durableRoot.slotResources {
		if resources == nil || resources.Len() != 1 {
			t.Fatalf("recovered slot %d closure=%v len=%d, want one dependency", slot, resources, resources.Len())
		}
	}
	if got := reopenedRegistry.ActivePins(); got != 3 {
		t.Fatalf("recovered slot plus visible-root pins=%d, want exactly 3", got)
	}
	if err := reopened.SetSync([]byte("inline-2"), []byte{}); err != nil {
		t.Fatal(err)
	}
	if got := reopenedRegistry.ActivePins(); got != 3 {
		t.Fatalf("slot plus visible-root pins after target replacement=%d, want exactly 3", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if got := reopenedRegistry.ActivePins(); got != 0 {
		t.Fatalf("recovered slot pins after close=%d, want 0", got)
	}
}

func TestDurableRootPublicationPreMetaFailureRetainsDebtWithoutBlocking(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	registry := database.valueLogIdentityPins

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 30_000, 1, func(int) []byte {
		return []byte("retry-owned durable value")
	})
	if err := database.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("retry"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	blockedBatch := database.NewBatch().(*Batch)
	if err := blockedBatch.SetPointer([]byte("already-admitted"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	baseCaptured := make(chan struct{})
	releaseBase := make(chan struct{})
	var baseCaptureCount atomic.Int32
	database.testAfterOptimisticBaseCaptureHook = func() {
		if baseCaptureCount.Add(1) == 1 {
			close(baseCaptured)
			<-releaseBase
		}
	}
	defer func() { database.testAfterOptimisticBaseCaptureHook = nil }()
	blockedWriteDone := make(chan error, 1)
	go func() { blockedWriteDone <- blockedBatch.WriteSync() }()
	select {
	case <-baseCaptured:
	case <-time.After(2 * time.Second):
		t.Fatal("second writer did not capture its base before publication")
	}

	prepared := make(chan struct{})
	releasePublish := make(chan struct{})
	var preparedOnce sync.Once
	database.testDurableRootCandidatePreparedHook = func() {
		preparedOnce.Do(func() {
			close(prepared)
			<-releasePublish
		})
	}
	defer func() { database.testDurableRootCandidatePreparedHook = nil }()
	database.testFailWriteMeta.Store(true)
	defer database.testFailWriteMeta.Store(false)
	firstWriteDone := make(chan error, 1)
	go func() { firstWriteDone <- batch.WriteSync() }()
	select {
	case <-prepared:
	case <-time.After(2 * time.Second):
		t.Fatal("first publication did not prepare a durable-root candidate")
	}

	allocatorWaiting := make(chan struct{})
	var allocatorWaitingOnce sync.Once
	freelist.TestHookCOWWaitBeforeSleep = func() {
		allocatorWaitingOnce.Do(func() { close(allocatorWaiting) })
	}
	defer func() { freelist.TestHookCOWWaitBeforeSleep = nil }()
	close(releaseBase)
	select {
	case <-allocatorWaiting:
	case <-time.After(2 * time.Second):
		t.Fatal("second writer did not block behind the prepared allocator candidate")
	}
	close(releasePublish)
	err = <-firstWriteDone
	if closeErr := batch.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, errTestWriteMetaFailpoint) {
		t.Fatalf("pre-meta publish error=%v, want failpoint", err)
	}
	if errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("pre-meta publish error=%v unexpectedly requires recovery", err)
	}
	if database.publicationPoisoned.Load() {
		t.Fatal("retryable pre-meta failure poisoned the writable handle")
	}
	database.durablePublishMu.Lock()
	failedMetaPageID := database.metaPageID
	failedCommitSeq := database.durableRoot.record.CommitSeq
	database.durablePublishMu.Unlock()
	stats := database.rootPublication.coordinator.Stats()
	retainedPins := registry.ActivePins()
	if failedMetaPageID != MetaPage0ID || failedCommitSeq != 1 {
		t.Fatalf("authoritative slot/commit after failure=(%d,%d), want (0,1)", failedMetaPageID, failedCommitSeq)
	}
	if stats.PendingCommits == 0 || stats.PreMetaFailures == 0 || stats.Poisoned {
		t.Fatalf("coordinator stats after retryable failure=%+v, want pending debt, at least one pre-meta failure, and no poison", stats)
	}
	if retainedPins == 0 {
		t.Fatal("retryable candidate debt did not retain its stable resources")
	}
	database.testFailWriteMeta.Store(false)

	var blockedWriteErr error
	select {
	case blockedWriteErr = <-blockedWriteDone:
	case <-time.After(5 * time.Second):
		t.Fatal("already-admitted SetSync remained blocked behind retryable publication debt")
	}
	if blockedWriteErr != nil && !errors.Is(blockedWriteErr, errTestWriteMetaFailpoint) {
		t.Fatalf("already-admitted SetSync error=%v, want success or the same retryable failpoint", blockedWriteErr)
	}
	if errors.Is(blockedWriteErr, ErrRecoveryRequired) {
		t.Fatalf("already-admitted SetSync error=%v unexpectedly requires recovery", blockedWriteErr)
	}
	if closeErr := blockedBatch.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	visibleCommitSeq := database.currentCommitSeq()
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint retrying retained publication debt: %v", err)
	}
	stats = database.rootPublication.coordinator.Stats()
	if stats.DurableCommitSeq < visibleCommitSeq || stats.PendingCommits != 0 || stats.Poisoned {
		t.Fatalf("coordinator stats after retry=%+v, want durable through %d with no pending debt or poison", stats, visibleCommitSeq)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- database.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked after retrying retained publication debt")
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("identity pins after close=%d, want 0", got)
	}

	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, key := range []string{"retry", "already-admitted"} {
		if got, err := reopened.Get([]byte(key)); err != nil || string(got) != "retry-owned durable value" {
			t.Fatalf("value %q after retry and reopen=(%q,%v), want retry-owned durable value", key, got, err)
		}
	}
	if err := reopened.SetSync([]byte("after-reopen"), []byte("progress")); err != nil {
		t.Fatalf("SetSync after reopen: %v", err)
	}
	if got, err := reopened.Get([]byte("after-reopen")); err != nil || string(got) != "progress" {
		t.Fatalf("value after reopened progress=(%q,%v), want progress", got, err)
	}
}

func TestDurableRootPublicationAcceptedWaitFailureRunsPostWork(t *testing.T) {
	database, err := Open(Options{
		Dir:                    t.TempDir(),
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		database.testFailWriteMeta.Store(false)
		_ = database.Close()
	}()

	if err := database.SetSync([]byte("seed"), []byte("seed-value")); err != nil {
		t.Fatalf("seed SetSync: %v", err)
	}
	oldState := database.state.Load()
	if oldState == nil || oldState.ValueLogSet == nil {
		t.Fatal("seed publication did not install a value-log set")
	}
	oldSet := oldState.ValueLogSet
	if got := oldSet.RefCount.Load(); got != 1 {
		t.Fatalf("seed value-log set refs=%d, want state ownership only", got)
	}
	baseVisible := database.currentCommitSeq()

	database.testFailWriteMeta.Store(true)
	err = database.SetSync([]byte("accepted"), []byte("visible-before-durable"))
	database.testFailWriteMeta.Store(false)
	if !errors.Is(err, errTestWriteMetaFailpoint) {
		t.Fatalf("accepted wait error=%v, want retryable meta failpoint", err)
	}
	if errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("accepted wait error=%v unexpectedly requires recovery", err)
	}
	if got := database.currentCommitSeq(); got != baseVisible+1 {
		t.Fatalf("visible commit after accepted wait error=%d, want %d", got, baseVisible+1)
	}
	if got, getErr := database.Get([]byte("accepted")); getErr != nil || string(got) != "visible-before-durable" {
		t.Fatalf("accepted value=(%q,%v), want visible value", got, getErr)
	}
	if got := oldSet.RefCount.Load(); got != 0 {
		t.Fatalf("superseded value-log set refs after accepted wait error=%d, want 0", got)
	}
	if tracker := database.valueLogRefTracker; tracker != nil {
		tracker.mu.RLock()
		trackerSeq := tracker.commitSeq
		trackerValid := tracker.valid
		tracker.mu.RUnlock()
		if !trackerValid || trackerSeq != baseVisible+1 {
			t.Fatalf("value-log ref tracker after accepted wait error=(valid=%t seq=%d), want (true,%d)", trackerValid, trackerSeq, baseVisible+1)
		}
	}
}

func TestDurableRootVisibleInstallFailureAbortsBeforeDurableMeta(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}

	database.testFailDurableRootVisibleInstall.Store(true)
	err = database.SetSync([]byte("post-meta"), []byte("recover-on-open"))
	database.testFailDurableRootVisibleInstall.Store(false)
	if !errors.Is(err, errTestDurableRootVisibleInstallFailpoint) {
		t.Fatalf("visible-install failure=%v, want failpoint", err)
	}
	if errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("pre-meta visible-install failure=%v unexpectedly requires recovery", err)
	}
	if database.publicationPoisoned.Load() {
		t.Fatal("pre-meta visible-install failure poisoned the writable handle")
	}
	if database.durableRoot.record.CommitSeq != 1 || database.currentCommitSeq() != 1 {
		t.Fatalf("durable/visible commits=(%d,%d), want unchanged (1,1)", database.durableRoot.record.CommitSeq, database.currentCommitSeq())
	}
	if got, getErr := database.Get([]byte("post-meta")); getErr != nil || got != nil {
		t.Fatalf("aborted value=(%q,%v), want absent", got, getErr)
	}
	if err := database.SetSync([]byte("after-failure"), []byte("progress")); err != nil {
		t.Fatalf("write after retryable visible-install failure: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if value, err := reopened.Get([]byte("post-meta")); err != nil || value != nil {
		t.Fatalf("aborted value after reopen=(%q,%v), want absent", value, err)
	}
	if value, err := reopened.Get([]byte("after-failure")); err != nil || string(value) != "progress" {
		t.Fatalf("progress value after reopen=(%q,%v), want progress", value, err)
	}
}

func TestDurableRootVisibleCandidateAbortFailurePoisonsAndWakesAllocator(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	abortFailure := errors.New("injected visible candidate abort failure")
	abortEntered := make(chan struct{})
	releaseAbort := make(chan struct{})
	var abortOnce sync.Once
	freelist.TestHookAbortCOWCandidateFailure = func() error {
		abortOnce.Do(func() { close(abortEntered) })
		<-releaseAbort
		return abortFailure
	}
	defer func() { freelist.TestHookAbortCOWCandidateFailure = nil }()

	allocatorWaiting := make(chan struct{})
	var allocatorWaitingOnce sync.Once
	freelist.TestHookCOWWaitBeforeSleep = func() {
		allocatorWaitingOnce.Do(func() { close(allocatorWaiting) })
	}
	defer func() { freelist.TestHookCOWWaitBeforeSleep = nil }()

	database.testFailDurableRootAfterCOWPrepare.Store(true)
	defer database.testFailDurableRootAfterCOWPrepare.Store(false)
	writeDone := make(chan error, 1)
	go func() { writeDone <- database.SetSync([]byte("abort-failure"), []byte("value")) }()
	select {
	case <-abortEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("visible candidate abort hook was not reached")
	}

	allocationDone := make(chan error, 1)
	go func() {
		_, err := database.idx.Load().allocator.AllocAppend()
		allocationDone <- err
	}()
	select {
	case <-allocatorWaiting:
	case <-time.After(5 * time.Second):
		close(releaseAbort)
		t.Fatal("allocator did not wait behind the prepared visible candidate")
	}
	close(releaseAbort)

	select {
	case err := <-writeDone:
		if !errors.Is(err, errTestDurableRootAfterCOWPrepareFailpoint) || !errors.Is(err, abortFailure) || !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("write error=%v, want prepare failpoint, abort failure, and ErrRecoveryRequired", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write remained blocked after abort failure")
	}
	select {
	case err := <-allocationDone:
		if !errors.Is(err, abortFailure) || !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("blocked allocator error=%v, want abort failure plus ErrRecoveryRequired", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("allocator waiter was not woken by fail-closed abort handling")
	}
	if !database.publicationPoisoned.Load() {
		t.Fatal("visible candidate abort failure did not poison the writable handle")
	}
}

func TestCaptureRebuiltIndexDurableResourcesRetainsSelectedImmutableDependencies(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("stable manifest namespace replacement is intentionally unsupported on Windows")
	}
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	fixtureDir := filepath.Join(dir, "rebuilt-resource-fixture")
	if err := os.MkdirAll(fixtureDir, 0o755); err != nil {
		t.Fatal(err)
	}
	store := newLeafGenerationManifestStore(
		fixtureDir,
		rootpublication.NewIdentityPinRegistry(),
		leafGenerationManifestStable,
		nil,
	)
	defer store.Close()
	token, err := store.Replace(newLeafGenerationManifest(database.currentCommitSeq()))
	if err != nil {
		t.Fatal(err)
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafGeneration)
	if err := builder.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	selectedResources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatal(err)
	}
	database.durableRoot.slotResources[database.durableRoot.slot] = selectedResources

	idx := database.idx.Load()
	captured, err := database.captureRebuiltIndexDurableResourcesV1(idx.pager, database.meta)
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Release()
	descriptors := captured.Descriptors()
	if len(descriptors) != 1 || descriptors[0].Kind() != rootpublication.ResourceOuterLeafManifest {
		t.Fatalf("rebuilt durable descriptors=%+v, want selected immutable manifest", descriptors)
	}
}

func TestDurableRootPublicationIOReleasesRootSerializationLocks(t *testing.T) {
	tests := []struct {
		name  string
		write func(*DB) error
	}{
		{name: "optimistic", write: func(database *DB) error {
			return database.SetSync([]byte("lock-free"), []byte("durable-root"))
		}},
		{name: "serialized", write: func(database *DB) error {
			physical := database.NewPhysicalBatch().(*Batch)
			defer physical.Close()
			if err := physical.Set([]byte("lock-free"), []byte("durable-root")); err != nil {
				return err
			}
			maxRevision := database.assignBatchEntryRevisions(physical.batch)
			return physical.writeSerialized(true, nil, maxRevision, nil)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()

			tryExclusive := func(tryLock func() bool, unlock func()) bool {
				deadline := time.Now().Add(100 * time.Millisecond)
				for {
					if tryLock() {
						unlock()
						return true
					}
					if time.Now().After(deadline) {
						return false
					}
					runtime.Gosched()
				}
			}
			checkLocks := func(stage string) error {
				if !tryExclusive(database.mu.TryLock, database.mu.Unlock) {
					return fmt.Errorf("db.mu held during %s", stage)
				}
				if !tryExclusive(database.writeMu.TryLock, database.writeMu.Unlock) {
					return fmt.Errorf("writeMu held during %s", stage)
				}
				if !tryExclusive(database.commitMu.TryLock, database.commitMu.Unlock) {
					return fmt.Errorf("commitMu held during %s", stage)
				}
				if !tryExclusive(database.rootReuseMu.TryLock, database.rootReuseMu.Unlock) {
					return fmt.Errorf("rootReuseMu held during %s", stage)
				}
				return nil
			}

			var observedMu sync.Mutex
			observed := make(map[durabilitycut.Point]int)
			var publicationOrder []durabilitycut.Point
			var lockErrors []error
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				switch event.Point {
				case durabilitycut.BeforePublicationSealWrite, durabilitycut.AfterPublicationSealWrite,
					durabilitycut.BeforeIndexDataSync, durabilitycut.AfterIndexDataSync,
					durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite,
					durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync:
					observedMu.Lock()
					observed[event.Point]++
					publicationOrder = append(publicationOrder, event.Point)
					if event.Point == durabilitycut.BeforePublicationSealWrite || event.Point == durabilitycut.AfterPublicationSealWrite {
						if event.Resource != durabilitycut.ResourceSeal {
							lockErrors = append(lockErrors, fmt.Errorf("publication seal resource=%q, want %q", event.Resource, durabilitycut.ResourceSeal))
						}
						if event.Path != filepath.Join(database.dir, indexFileName) {
							lockErrors = append(lockErrors, fmt.Errorf("publication seal path=%q, want index path", event.Path))
						}
						if event.Offset < int64(2*page.PageSize) || event.Offset%int64(page.PageSize) != 0 || event.Length != int64(page.PageSize) {
							lockErrors = append(lockErrors, fmt.Errorf("publication seal range=(%d,%d), want one aligned non-meta page", event.Offset, event.Length))
						}
					}
					if event.Point == durabilitycut.BeforePublicationSealWrite || event.Point == durabilitycut.BeforeIndexDataSync || event.Point == durabilitycut.BeforeMetaWrite || event.Point == durabilitycut.BeforeMetaSync {
						if lockErr := checkLocks(string(event.Point)); lockErr != nil {
							lockErrors = append(lockErrors, lockErr)
						}
					}
					observedMu.Unlock()
				}
				return nil
			})
			if err := test.write(database); err != nil {
				restore()
				t.Fatal(err)
			}
			restore()
			if err := errors.Join(lockErrors...); err != nil {
				t.Fatal(err)
			}
			wantOrder := []durabilitycut.Point{
				durabilitycut.BeforePublicationSealWrite, durabilitycut.AfterPublicationSealWrite,
				durabilitycut.BeforeIndexDataSync, durabilitycut.AfterIndexDataSync,
				durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite,
				durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync,
			}
			if len(publicationOrder) != len(wantOrder) {
				t.Fatalf("publication order=%v, want %v", publicationOrder, wantOrder)
			}
			for i, point := range wantOrder {
				if publicationOrder[i] != point {
					t.Fatalf("publication order=%v, want %v", publicationOrder, wantOrder)
				}
				if observed[point] != 1 {
					t.Fatalf("observed %s %d times, want exactly once", point, observed[point])
				}
			}
		})
	}
}

func TestDurableRootAlternatingSlotsRetireAuxiliaryHistory(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	state := database.State()
	if state == nil {
		t.Fatal("missing initial state")
	}
	initialAuxiliary, err := durableRootSlotAuxiliaryPagesV1(database.durableRoot.slotMeta[MetaPage0ID], database.durableRoot.slotRecord[MetaPage0ID])
	if err != nil {
		t.Fatal(err)
	}
	if len(initialAuxiliary) == 0 {
		t.Fatal("missing initial durable-root auxiliary inventory")
	}

	var steadyRetired []uint64
	for lsn := uint64(1); lsn <= 64; lsn++ {
		if err := database.publishCommandWALRoots(
			state.RootPageID,
			state.SystemRootPageID,
			lsn,
			[]CommandWALLSNRange{{First: lsn, Last: lsn}},
			true,
		); err != nil {
			t.Fatalf("publication %d: %v", lsn, err)
		}
		if lsn > 32 {
			steadyRetired = append(steadyRetired, database.idx.Load().allocator.COWGenerationV1().RetiredCount())
		}
	}
	generation := database.idx.Load().allocator.COWGenerationV1()
	for _, pageID := range initialAuxiliary {
		if !generation.Allocatable(pageID) {
			t.Fatalf("overwritten slot auxiliary page %d remains retained after the recovery horizon advanced", pageID)
		}
	}
	minimum, maximum := steadyRetired[0], steadyRetired[0]
	for _, count := range steadyRetired[1:] {
		if count < minimum {
			minimum = count
		}
		if count > maximum {
			maximum = count
		}
	}
	if maximum > 128 || maximum-minimum > 16 {
		t.Fatalf("steady retired inventory min=%d max=%d, want bounded two-slot history", minimum, maximum)
	}
}
