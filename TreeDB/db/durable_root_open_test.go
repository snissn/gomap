package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

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
	if got := registry.ActivePins(); got != 2 {
		t.Fatalf("published slot pins=%d, want exactly 2", got)
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
	if got := reopenedRegistry.ActivePins(); got != 2 {
		t.Fatalf("recovered slot pins=%d, want exactly 2", got)
	}
	if err := reopened.SetSync([]byte("inline-2"), []byte{}); err != nil {
		t.Fatal(err)
	}
	if got := reopenedRegistry.ActivePins(); got != 2 {
		t.Fatalf("slot pins after target replacement=%d, want exactly 2", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if got := reopenedRegistry.ActivePins(); got != 0 {
		t.Fatalf("recovered slot pins after close=%d, want 0", got)
	}
}

func TestDurableRootPublicationRetainsAndRetriesExactPreMetaCandidate(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, ValueLog: ValueLogOptions{PointerThreshold: 1}})
	if err != nil {
		t.Fatal(err)
	}
	registry := database.valueLogIdentityPins
	defer func() {
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		if got := registry.ActivePins(); got != 0 {
			t.Fatalf("identity pins after close=%d, want 0", got)
		}
	}()

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
	database.testFailWriteMeta.Store(true)
	err = batch.WriteSync()
	database.testFailWriteMeta.Store(false)
	if closeErr := batch.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, errTestWriteMetaFailpoint) || errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("pre-meta publish error=%v, want failpoint without recovery poison", err)
	}
	if database.publicationPoisoned.Load() {
		t.Fatal("pre-meta publication failure poisoned writable handle")
	}
	if database.metaPageID != MetaPage0ID || database.currentCommitSeq() != 1 {
		t.Fatalf("authoritative slot/commit after failure=(%d,%d), want (0,1)", database.metaPageID, database.currentCommitSeq())
	}
	pending := database.durableRoot.pending
	if pending == nil || pending.resources == nil || pending.resources.Len() != 1 || pending.token == nil || pending.prepared == nil {
		t.Fatalf("retained candidate=%+v, want exact resources/index/COW ownership", pending)
	}
	if got := database.durableCandidateIndexCaptures.Load(); got != 1 {
		t.Fatalf("durable candidate index captures while retry pending=%d, want 1", got)
	}
	if got := registry.ActivePins(); got != 2 {
		t.Fatalf("identity pins while retry pending=%d, want value-log plus index", got)
	}

	next, err := database.retryPendingDurableRootV1()
	if err != nil {
		t.Fatal(err)
	}
	if next.CommitSeq != 2 || database.durableRoot.pending != nil || database.durableRoot.slot != MetaPage1ID {
		t.Fatalf("retried commit/pending/slot=(%d,%v,%d), want (2,nil,1)", next.CommitSeq, database.durableRoot.pending, database.durableRoot.slot)
	}
	if got := database.durableCandidateIndexCaptures.Load(); got != 0 {
		t.Fatalf("durable candidate index captures after retry=%d, want 0", got)
	}
	if got := registry.ActivePins(); got != 1 {
		t.Fatalf("identity pins after retry=%d, want retained slot value-log pin", got)
	}
	meta, err := readDurableMetaSlotV1(database.idx.Load().pager, MetaPage1ID)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CommitSeq != 2 {
		t.Fatalf("retried durable meta commit=%d, want 2", meta.CommitSeq)
	}
}

func TestDurableRootPublicationPoisonsWhenVisibleInstallFailsAfterDurableMeta(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}

	database.testFailDurableRootVisibleInstall.Store(true)
	err = database.SetSync([]byte("post-meta"), []byte("recover-on-open"))
	database.testFailDurableRootVisibleInstall.Store(false)
	if !errors.Is(err, errTestDurableRootVisibleInstallFailpoint) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("visible-install failure=%v, want failpoint plus ErrRecoveryRequired", err)
	}
	if !database.publicationPoisoned.Load() {
		t.Fatal("post-meta visible-install failure did not poison writable handle")
	}
	if database.durableRoot.record.CommitSeq != 2 || database.currentCommitSeq() != 1 {
		t.Fatalf("durable/visible commits=(%d,%d), want (2,1) before recovery", database.durableRoot.record.CommitSeq, database.currentCommitSeq())
	}
	if err := database.SetSync([]byte("blocked"), []byte("until-reopen")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("write after visible-install poison=%v, want ErrRecoveryRequired", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, err := reopened.Get([]byte("post-meta"))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "recover-on-open" {
		t.Fatalf("recovered value=%q, want recover-on-open", value)
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
				return nil
			}

			var observedMu sync.Mutex
			observed := make(map[durabilitycut.Point]int)
			var publicationOrder []durabilitycut.Point
			var lockErrors []error
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				switch event.Point {
				case durabilitycut.BeforeIndexDataSync, durabilitycut.AfterIndexDataSync,
					durabilitycut.BeforeMetaWrite, durabilitycut.AfterMetaWrite,
					durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync:
					observedMu.Lock()
					observed[event.Point]++
					publicationOrder = append(publicationOrder, event.Point)
					if event.Point == durabilitycut.BeforeIndexDataSync || event.Point == durabilitycut.BeforeMetaWrite || event.Point == durabilitycut.BeforeMetaSync {
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
	if maximum > 32 || maximum-minimum > 16 {
		t.Fatalf("steady retired inventory min=%d max=%d, want bounded two-slot history", minimum, maximum)
	}
}
