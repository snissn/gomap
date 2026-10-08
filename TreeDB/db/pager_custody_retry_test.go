package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/pager"
	"path/filepath"
	"testing"
)

func TestDBPendingCloseKeepsNamespaceUntilActualIndexRetry(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		name := "writer"
		if readOnly {
			name = "shared-read-only"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Dir: dir, DisableBackgroundPrune: true, DisableSideStores: true}
			if readOnly {
				initial, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				if err := initial.Close(); err != nil {
					t.Fatal(err)
				}
				opts.ReadOnly = true
			}
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			previous := testIndexClosePager
			t.Cleanup(func() { testIndexClosePager = previous; _ = database.Close() })
			fault := errors.New("pending actual index Close")
			testIndexClosePager = func(*pager.Pager) error { return fault }
			database.mu.Lock()
			lock := database.lock
			database.mu.Unlock()
			if lock == nil {
				t.Fatal("installed namespace holder unavailable")
			}
			if err := database.Close(); !errors.Is(err, fault) {
				t.Fatalf("first Close error=%v want physical fault", err)
			}
			database.mu.Lock()
			retained := database.lock == lock
			database.mu.Unlock()
			if !retained {
				t.Fatal("failed cleanup detached exact namespace holder")
			}
			competing, openErr := Open(Options{Dir: dir, DisableBackgroundPrune: true, DisableSideStores: true})
			if competing != nil {
				_ = competing.Close()
				t.Fatal("competing writer acquired pending namespace")
			}
			if !errors.Is(openErr, ErrLocked) {
				t.Fatalf("pending Open error=%v want ErrLocked", openErr)
			}
			testIndexClosePager = previous
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			database.mu.Lock()
			held := database.lock != nil
			database.mu.Unlock()
			if held {
				t.Fatal("successful actual retry retained namespace holder")
			}
			reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, DisableSideStores: true})
			if err != nil {
				t.Fatalf("Open after actual retry: %v", err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDBGhostOrdinaryScavengeKeepsPreparedCOWCustodyUntilRetry(t *testing.T) {
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	p, err := pager.OpenWithOptions(filepath.Join(t.TempDir(), "index.db"), 64*1024, pager.OpenOptions{ResidentOwner: owner})
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	m := &indexGhostManager{}
	previous := testIndexClosePager
	t.Cleanup(func() { testIndexClosePager = previous; _ = m.stop(); _ = p.Close(); owner.Close() })
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	allocator := freelist.New(p, 0)
	gen, err := newConstructorIndexGen(7, p, allocator, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.EnableNewCOWGenerationV1(1, 4, nil); err != nil {
		t.Fatal(err)
	}
	var id freelist.CandidateIDV1
	id[0] = 73
	prepared, err := allocator.PrepareOwnedCOWCandidateRetiringWithLimitsV1(2, 1, id, freelist.ReuseCapability{}, nil, 0, freelist.NewCandidatePageSinkV1(), nil)
	if err != nil {
		t.Fatal(err)
	}
	gen.closeMu.Lock()
	creator := gen.creator
	gen.closeMu.Unlock()
	gen.writerMu.RLock()
	authority := gen.writerAuthority
	gen.writerMu.RUnlock()
	if creator == nil || authority == nil {
		t.Fatal("real constructor/writer owners unavailable")
	}
	if gen.release() != 0 {
		t.Fatal("retired generation ref did not drain")
	}
	m.add(gen)
	closes := 0
	testIndexClosePager = func(*pager.Pager) error {
		closes++
		if closes > 1 {
			return errors.New("successful physical Close replayed")
		}
		return nil
	}
	m.scavenge(0)
	if closes != 1 || !gen.handlesClosed.Load() {
		t.Fatal("ordinary eligible scavenge did not perform actual physical Close")
	}
	m.mu.Lock()
	held := len(m.ghosts) == 1 && m.ghosts[0].gen == gen
	m.mu.Unlock()
	gen.closeMu.Lock()
	sameCreator := gen.creator == creator
	gen.closeMu.Unlock()
	gen.writerMu.RLock()
	sameWriter := gen.allocator == allocator && gen.writerAuthority == authority
	gen.writerMu.RUnlock()
	if !held || !sameCreator || !sameWriter || creator.Stats().Closed {
		t.Fatal("ordinary scavenge lost exact prepared-debt ghost/creator/writer")
	}
	// Another ordinary pass still cannot erase the real outstanding owner or
	// replay the already successful mmap/FD close.
	m.scavenge(0)
	m.mu.Lock()
	held = len(m.ghosts) == 1 && m.ghosts[0].gen == gen
	m.mu.Unlock()
	if !held || closes != 1 {
		t.Fatal("unresolved ordinary retry lost holder or replayed physical Close")
	}
	if err := allocator.AbortCOWCandidateV1(prepared); err != nil {
		t.Fatal(err)
	}
	if err := prepared.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
	m.scavenge(0)
	m.mu.Lock()
	remaining := len(m.ghosts)
	m.mu.Unlock()
	if remaining != 0 || closes != 1 || !gen.constructorTerminalCompleteV1() {
		t.Fatal("actual COW cleanup failed exact retry/removal")
	}
	owner.Close()
	if owner.Stats().Live != 0 {
		t.Fatalf("completed ghost creator still live: %+v", owner.Stats())
	}
}

func TestDBGhostOrdinaryScavengeRemovesHealthyExactOwner(t *testing.T) {
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	p, err := pager.OpenWithOptions(filepath.Join(t.TempDir(), "index.db"), 64*1024, pager.OpenOptions{ResidentOwner: owner})
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	m := &indexGhostManager{}
	t.Cleanup(func() { _ = m.stop(); _ = p.Close(); owner.Close() })
	if _, err := p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	allocator := freelist.New(p, 0)
	gen, err := newConstructorIndexGen(9, p, allocator, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.EnableNewCOWGenerationV1(1, 4, nil); err != nil {
		t.Fatal(err)
	}
	if gen.release() != 0 {
		t.Fatal("retired generation ref did not drain")
	}
	m.add(gen)
	m.scavenge(0)
	m.mu.Lock()
	remaining := len(m.ghosts)
	m.mu.Unlock()
	if remaining != 0 || !gen.constructorTerminalCompleteV1() {
		t.Fatal("healthy ordinary ghost remained pending")
	}
	owner.Close()
	if owner.Stats().Live != 0 {
		t.Fatalf("healthy ordinary creator retained: %+v", owner.Stats())
	}
}
