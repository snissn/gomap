package pager

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func ownedLifetimeChunkSize() int64 {
	chunk := mmapOffsetGranularity()
	if chunk < int64(os.Getpagesize()) {
		chunk = int64(os.Getpagesize())
	}
	if chunk < int64(page.PageSize) {
		chunk = int64(page.PageSize)
	}
	return chunk
}

func ownedLifetimePager(t *testing.T) (*Pager, *residentcredit.Owner) {
	t.Helper()
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	chunk := ownedLifetimeChunkSize()
	p, err := OpenWithOptions(filepath.Join(t.TempDir(), "index.db"), chunk, OpenOptions{ResidentOwner: owner})
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		testOwnedOpenAfterFile = nil
		testOwnedMunmap = nil
		testOwnedFileClose = nil
		_ = p.Close()
		owner.Close()
	})
	return p, owner
}

func TestPagerConstructorClosedOwnerRefusesBeforeFileBirth(t *testing.T) {
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	owner.Close()
	path := filepath.Join(t.TempDir(), "must-not-exist")
	p, err := OpenWithOptions(path, ownedLifetimeChunkSize(), OpenOptions{ResidentOwner: owner})
	if p != nil || !errors.Is(err, residentcredit.ErrLimit) {
		t.Fatalf("unowned birth: %p %v", p, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file born before refusal: %v", err)
	}
}

func TestPagerConstructorFailureRetainsActualFileForCloseRetry(t *testing.T) {
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("constructor and close fault")
	testOwnedOpenAfterFile = func(*Pager) error { return fault }
	testOwnedFileClose = func(*os.File) error { return fault }
	t.Cleanup(func() { testOwnedOpenAfterFile = nil; testOwnedFileClose = nil; owner.Close() })
	p, err := OpenWithOptions(filepath.Join(t.TempDir(), "index.db"), ownedLifetimeChunkSize(), OpenOptions{ResidentOwner: owner})
	if p == nil || !errors.Is(err, fault) {
		t.Fatalf("actual failed holder lost: %p %v", p, err)
	}
	t.Cleanup(func() { testOwnedFileClose = nil; _ = p.Close() })
	held := p.file
	if held == nil {
		t.Fatal("lost actual file")
	}
	if _, err := held.Stat(); err != nil {
		t.Fatal(err)
	}
	owner.Close()
	if owner.Stats().Live == 0 {
		t.Fatal("creator dropped while descriptor held")
	}
	testOwnedFileClose = nil
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := held.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("actual descriptor not closed: %v", err)
	}
	if owner.Stats().Live != 0 {
		t.Fatalf("retained constructor credit: %+v", owner.Stats())
	}
}

func TestPagerPartialUnmapRetrySkipsSuccessfulChunks(t *testing.T) {
	p, owner := ownedLifetimePager(t)
	pages := int(p.chunkSize / int64(page.PageSize))
	if _, err := p.Alloc(pages + 1); err != nil {
		t.Fatal(err)
	}
	chunks := len(p.chunks)
	if chunks < 2 {
		t.Fatal("fixture did not map two chunks")
	}
	calls := 0
	fault := errors.New("unmap second chunk")
	testOwnedMunmap = func(b []byte) error {
		calls++
		if calls == 2 {
			return fault
		}
		return munmapFile(b)
	}
	if err := p.Close(); !errors.Is(err, fault) {
		t.Fatalf("partial close: %v", err)
	}
	if len(p.chunks[0]) != 0 || len(p.chunks[1]) == 0 || p.file == nil {
		t.Fatal("remaining physical holder not preserved")
	}
	if _, err := p.Get(0); !errors.Is(err, ErrClosing) {
		t.Fatalf("closed read admitted: %v", err)
	}
	if owner.Stats().Live == 0 {
		t.Fatal("creator dropped before retry")
	}
	retries := 0
	testOwnedMunmap = func(b []byte) error { retries++; return munmapFile(b) }
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if retries != 1 {
		t.Fatalf("successful mappings unmapped twice: retry calls=%d", retries)
	}
	owner.Close()
	if owner.Stats().Live != 0 {
		t.Fatal("creator retained after actual terminal")
	}
}

func TestPagerCloseJoinsAdmittedOperationOutsideRequiredLocks(t *testing.T) {
	p, _ := ownedLifetimePager(t)
	if err := p.beginOperation(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.Close() }()
	deadline := time.Now().Add(time.Second)
	for {
		p.lifetime.mu.Lock()
		closing := p.lifetime.closing
		p.lifetime.mu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			p.endOperation()
			t.Fatal("Close not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		p.endOperation()
		t.Fatalf("Close discharged live invocation: %v", err)
	default:
	}
	if !p.mu.TryLock() {
		p.endOperation()
		t.Fatal("join holds mmap gate")
	}
	p.mu.Unlock()
	if !p.growMu.TryLock() {
		p.endOperation()
		t.Fatal("join holds grow gate")
	}
	p.growMu.Unlock()
	if !p.allocMu.TryLock() {
		p.endOperation()
		t.Fatal("join holds allocation gate")
	}
	p.allocMu.Unlock()
	if _, err := p.Alloc(1); !errors.Is(err, ErrClosing) {
		p.endOperation()
		t.Fatalf("new operation admitted: %v", err)
	}
	p.endOperation()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join")
	}
}

func TestPagerIndependentStableSyncSurvivesMappedCloseAndReopen(t *testing.T) {
	p, _ := ownedLifetimePager(t)
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, page.PageSize)
	data[0] = 197
	if err := p.Write(0, data); err != nil {
		t.Fatal(err)
	}
	stable, err := os.OpenFile(p.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stable.Close()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Sync(); !errors.Is(err, ErrClosing) {
		t.Fatalf("default closed sync: %v", err)
	}
	if err := p.SyncIndexDataWithStableFile(stable); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(p.path, p.chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	actual, err := reopened.Get(0)
	if err != nil || actual[0] != 197 {
		t.Fatalf("persistent mapped value lost: %v", err)
	}
}

func TestPagerDurabilityObserverMayCloseWithoutSelfJoin(t *testing.T) {
	p, _ := ownedLifetimePager(t)
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	}
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeIndexDataSync {
			return p.Close()
		}
		return nil
	})
	defer restore()
	done := make(chan error, 1)
	go func() { done <- p.SyncIndexData() }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosing) {
			t.Fatalf("closed sync outcome: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("durability callback waited on its own operation")
	}
}

func TestPagerRawExportRefusesBeforeExposureDuringSelectedLoan(t *testing.T) {
	p, owner := ownedLifetimePager(t)
	if _, err := p.Alloc(1); err != nil {
		t.Fatal(err)
	}
	p.lifetime.mu.Lock()
	p.stamp.selectedLoans = 1
	p.lifetime.mu.Unlock()
	if _, err := p.Get(0); !errors.Is(err, ErrRawExport) {
		t.Fatalf("raw export: %v", err)
	}
	called := false
	if err := p.WithStableResourceFile(func(*os.File) error { called = true; return nil }); !errors.Is(err, ErrRawExport) || called {
		t.Fatalf("file exposed: %v %t", err, called)
	}
	p.lifetime.mu.Lock()
	p.stamp.selectedLoans = 0
	p.lifetime.mu.Unlock()
	if _, err := p.Get(0); err != nil {
		t.Fatal(err)
	}
	if err := p.RequireOwnedCapacity(owner); !errors.Is(err, ErrOwnedCoverage) {
		t.Fatalf("incomplete stamp admitted: %v", err)
	}
}
