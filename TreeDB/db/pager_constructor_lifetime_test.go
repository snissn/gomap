package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"github.com/snissn/gomap/TreeDB/pager"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDBFailedOpenReturnsActualIndexCleanupOwner(t *testing.T) {
	fault := errors.New("installed constructor cleanup fault")
	previousOpen, previousClose := testDBOpenHook, testIndexClosePager
	var creator *residentcredit.Owner
	testDBOpenHook = func(database *DB) error {
		creator = (*residentcredit.Owner)(database.nativePublicationResident)
		return fault
	}
	testIndexClosePager = func(*pager.Pager) error { return fault }
	t.Cleanup(func() { testDBOpenHook = previousOpen; testIndexClosePager = previousClose })
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir})
	if database == nil || !errors.Is(err, fault) {
		t.Fatalf("actual cleanup owner discarded: %p %v", database, err)
	}
	t.Cleanup(func() { testIndexClosePager = previousClose; _ = database.Close() })
	database.idxMu.Lock()
	n := len(database.idxAll)
	database.idxMu.Unlock()
	if n == 0 {
		t.Fatal("failed index missing from real table")
	}
	if creator == nil || creator.Stats().Live == 0 {
		t.Fatal("failed index creator released")
	}
	// The failed actual constructor, not a new writer, still owns namespace.
	testDBOpenHook = previousOpen
	competing, openErr := Open(Options{Dir: dir})
	if competing != nil {
		_ = competing.Close()
		t.Fatal("pending constructor allowed competing writer custody")
	}
	if !errors.Is(openErr, ErrLocked) {
		t.Fatalf("pending constructor Open error=%v want ErrLocked", openErr)
	}
	testIndexClosePager = previousClose
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database.idxMu.Lock()
	n = len(database.idxAll)
	database.idxMu.Unlock()
	if n != 0 {
		t.Fatalf("completed index still pending: %d", n)
	}
	if creator.Stats().Live != 0 {
		t.Fatalf("remaining creator: %+v", creator.Stats())
	}
	reopened, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("namespace lock not discharged: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDBHeldSnapshotCreatorSurvivesCloseAndScrubsAtActualTerminal(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("snapshot unavailable")
	}
	defer snapshot.Close()
	if value, err := snapshot.Get([]byte("key")); err != nil || string(value) != "value" {
		t.Fatalf("read: %q %v", value, err)
	}
	creator := snapshot.pagerCreator
	if creator == nil {
		t.Fatal("constructor edge missing")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if creator.OwnerStats().Live == 0 || creator.Stats().Closed {
		t.Fatal("mapped Close dropped held reader creator")
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if snapshot.treePager != nil || snapshot.idx != nil || snapshot.db != nil || snapshot.pagerCreator != nil || snapshot.state != nil {
		t.Fatal("closed public Snapshot retains broad graph")
	}
	if creator.OwnerStats().Live != 0 {
		t.Fatalf("last real Snapshot edge not discharged: %+v", creator.OwnerStats())
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDBGhostKeepsActualIndexAcrossPhysicalCloseFailure(t *testing.T) {
	owner, err := residentcredit.NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	p, err := pager.OpenWithOptions(filepath.Join(t.TempDir(), "index.db"), defaultChunkSize, pager.OpenOptions{ResidentOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	gen, err := newConstructorIndexGen(7, p, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	m := &indexGhostManager{}
	m.add(gen)
	fault := errors.New("ghost physical close fault")
	previous := testIndexClosePager
	testIndexClosePager = func(*pager.Pager) error { return fault }
	t.Cleanup(func() { testIndexClosePager = previous; _ = m.stop() })
	if err := m.stop(); !errors.Is(err, fault) {
		t.Fatalf("shutdown discarded close error: %v", err)
	}
	m.mu.Lock()
	pending := len(m.ghosts)
	m.mu.Unlock()
	if pending != 1 || gen.creator == nil {
		t.Fatal("failed actual ghost/creator lost")
	}
	testIndexClosePager = previous
	if err := m.stop(); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	pending = len(m.ghosts)
	m.mu.Unlock()
	if pending != 0 {
		t.Fatal("completed ghost remained")
	}
	owner.Close()
	if owner.Stats().Live != 0 {
		t.Fatalf("ghost creator leaked: %+v", owner.Stats())
	}
}

func TestDBPagerJoinDropsMaintenanceTeardownAndWriteGates(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	p := database.idx.Load().pager
	entered, release := make(chan struct{}), make(chan struct{})
	operation := make(chan error, 1)
	go func() {
		operation <- p.WithStableResourceFile(func(*os.File) error {
			close(entered)
			<-release
			if !database.maintenanceMu.TryLock() {
				return errors.New("join retained maintenance")
			}
			database.maintenanceMu.Unlock()
			if !database.teardownMu.TryLock() {
				return errors.New("join retained teardown")
			}
			database.teardownMu.Unlock()
			if !database.writeMu.TryLock() {
				return errors.New("join retained write")
			}
			database.writeMu.Unlock()
			return nil
		})
	}()
	<-entered
	atIndexClose := make(chan struct{})
	previous := testIndexClosePager
	testIndexClosePager = func(*pager.Pager) error { close(atIndexClose); return nil }
	t.Cleanup(func() { testIndexClosePager = previous })
	done := make(chan error, 1)
	go func() { done <- database.Close() }()
	select {
	case <-atIndexClose:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Close did not reach exact index terminal")
	}
	select {
	case err := <-done:
		close(release)
		t.Fatalf("Close skipped admitted operation: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-operation:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("operation could not leave gates")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join")
	}
}

func TestDBPhysicallyClosedIndexRemainsActualOwnerUntilRuntimeTerminal(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	gen := database.idx.Load()
	creator := gen.creator
	if creator == nil {
		t.Fatal("missing actual constructor creator")
	}
	gens, err := database.closeAllIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if len(gens) != 1 || gens[0] != gen || !gen.handlesClosed.Load() {
		t.Fatal("physical shutdown did not close exact generation")
	}
	database.idxMu.Lock()
	held := database.idxAll[gen.id] == gen
	database.idxMu.Unlock()
	if !held || gen.creator != creator || creator.Stats().Closed {
		t.Fatal("physical Close dropped pre-terminal actual holder/credit")
	}
	// Real DB shutdown performs the runtime terminal, then exact allocator detach.
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database.idxMu.Lock()
	held = database.idxAll[gen.id] == gen
	database.idxMu.Unlock()
	if held || gen.creator != nil {
		t.Fatal("completed runtime terminal left constructor owner pending")
	}
}
