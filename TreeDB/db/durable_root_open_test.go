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
	if got := database.stableIndexCaptures.Load(); got != 1 {
		t.Fatalf("stable index captures while retry pending=%d, want 1", got)
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
	if got := database.stableIndexCaptures.Load(); got != 0 {
		t.Fatalf("stable index captures after retry=%d, want 0", got)
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

func TestDurableRootPublicationIOReleasesRootSerializationLocks(t *testing.T) {
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
	observed := make(map[durabilitycut.Point]bool)
	var lockErrors []error
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch event.Point {
		case durabilitycut.BeforeIndexDataSync, durabilitycut.BeforeMetaWrite, durabilitycut.BeforeMetaSync:
			observedMu.Lock()
			observed[event.Point] = true
			if lockErr := checkLocks(string(event.Point)); lockErr != nil {
				lockErrors = append(lockErrors, lockErr)
			}
			observedMu.Unlock()
		}
		return nil
	})
	if err := database.SetSync([]byte("lock-free"), []byte("durable-root")); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	if err := errors.Join(lockErrors...); err != nil {
		t.Fatal(err)
	}
	for _, point := range []durabilitycut.Point{durabilitycut.BeforeIndexDataSync, durabilitycut.BeforeMetaWrite, durabilitycut.BeforeMetaSync} {
		if !observed[point] {
			t.Fatalf("did not observe %s", point)
		}
	}
}
