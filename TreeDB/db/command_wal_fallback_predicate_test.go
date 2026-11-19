package db

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandWALFallbackPredicateReleasesForegroundDuringSeal(t *testing.T) {
	for _, mode := range []string{"wal-append-and-close", "changed-basis", "failed-seal"} {
		t.Run(mode, func(t *testing.T) {
			testCommandWALFallbackPredicateSeal(t, mode)
		})
	}
}

func TestCommandWALFallbackPredicateRejectsRetiredVacuumRuntime(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	key, value := []byte("before"), []byte("vacuum")
	if err := db.SetSync(key, value); err != nil {
		t.Fatal(err)
	}
	oldBasis, live := db.StateToken()
	if !live {
		t.Fatal("missing original live basis")
	}
	oldRuntime := db.rootPublication
	oldCoordinator := oldRuntime.coordinator
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := oldCoordinator.WaitThrough(ctx, oldBasis.CommitSeq); err != nil {
		t.Fatal(err)
	}
	if err := db.VacuumIndexOnline(ctx); err != nil {
		t.Fatal(err)
	}
	newBasis, live := db.StateToken()
	if !live || db.rootPublication == oldRuntime || newBasis == oldBasis {
		t.Fatalf("vacuum did not replace the live basis: old=%+v new=%+v", oldBasis, newBasis)
	}
	if err := oldCoordinator.WaitThrough(ctx, oldBasis.CommitSeq); err != rootpublication.ErrClosed {
		t.Fatalf("retired coordinator did not expose pre-wait retirement: %v", err)
	}
	// Resume the exact check already admitted on the original runtime, at the
	// capture/wait boundary, after the real vacuum has retired that runtime.
	db.teardownMu.RLock()
	needs, err := db.commandWALCheckpointFallbackNeedsRefreshAfterCapture(oldBasis, oldBasis, oldRuntime, oldCoordinator)
	db.teardownMu.RUnlock()
	if needs || !errors.Is(err, ErrRootPublicationBasisMismatch) {
		t.Fatalf("retired runtime was not a retriable mismatch: %v, %v", needs, err)
	}
	if _, err := db.CommandWALCheckpointFallbackNeedsRefresh(newBasis); err != nil {
		t.Fatalf("replacement runtime is unusable: %v", err)
	}
	if got, err := db.Get(key); err != nil || string(got) != string(value) {
		t.Fatalf("vacuum changed the original value: %q, %v", got, err)
	}
}

func testCommandWALFallbackPredicateSeal(t *testing.T, mode string) {
	t.Helper()
	dir := t.TempDir()
	options := Options{
		Dir: dir, CommandWAL: true, DisableBackgroundPrune: true,
		Durability: DurabilityWALOnRelaxed, rootPublicationFixedDelay: 100 * time.Millisecond,
	}
	db, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	sealHeld, releaseSeal := make(chan struct{}), make(chan struct{})
	var releaseOnce, holdOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSeal) }) }
	injected := errors.New("fallback predicate index-data fence failure")
	var failSeal atomic.Bool
	failSeal.Store(mode == "failed-seal")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Root != dir || e.Point != durabilitycut.BeforeIndexDataSync || e.Resource != durabilitycut.ResourceIndex {
			return nil
		}
		holdOnce.Do(func() {
			close(sealHeld)
			<-releaseSeal
		})
		if failSeal.Load() {
			return injected
		}
		return nil
	})
	defer restore()
	defer release()

	if err := db.Set([]byte("preceding"), []byte("value")); err != nil {
		t.Fatalf("create pending physical root: %v", err)
	}
	basis, live := db.StateToken()
	if !live || basis.AppliedCommandLSN == 0 {
		t.Fatalf("no original visible root: %+v", basis)
	}
	coordinator := db.rootPublication.coordinator
	if coordinator.Stats().DurableCommitSeq >= basis.CommitSeq {
		t.Fatal("original physical root already sealed")
	}
	type result struct {
		needs bool
		err   error
	}
	predicate := make(chan result, 1)
	go func() {
		needs, err := db.CommandWALCheckpointFallbackNeedsRefresh(basis)
		predicate <- result{needs, err}
	}()
	select {
	case <-sealHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("original seal did not reach its index-data fence")
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		stats := coordinator.Stats()
		if stats.DurableCommitSeq >= basis.CommitSeq || time.Now().After(deadline) {
			t.Fatalf("predicate did not join the held original seal: %+v", stats)
		}
		if stats.WaiterCount == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	key, value := []byte("following"), []byte("acknowledged")
	foreground := make(chan error, 1)
	next := db.CommandWALNextLSN()
	if mode == "changed-basis" {
		go func() { foreground <- db.Set(key, value) }()
	} else {
		payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: key, Value: value}})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			lsn, err := db.AppendRawKVBatchPayloadCommandWALTrustedWithMode(payload, RawKVCommandWALAppendRelaxed)
			if err == nil && lsn != next {
				t.Errorf("relaxed canonical append assigned LSN %d, want %d", lsn, next)
			}
			foreground <- err
		}()
	}
	select {
	case err := <-foreground:
		if err != nil {
			t.Fatalf("foreground write while seal is held: %v", err)
		}
	case <-time.After(time.Second):
		release()
		<-foreground
		<-predicate
		t.Fatal("foreground command waited for the preceding root's index-data fence")
	}
	if got := db.CommandWALNextLSN(); got != next+1 {
		t.Fatalf("following command assigned %d times", got-next)
	}
	select {
	case got := <-predicate:
		t.Fatalf("predicate returned before its original seal: %+v", got)
	default:
	}

	var closed chan error
	if mode == "wal-append-and-close" {
		closed = make(chan error, 1)
		go func() { closed <- db.Close() }()
		for deadline := time.Now().Add(5 * time.Second); !db.IsClosing(); {
			if time.Now().After(deadline) {
				t.Fatal("Close did not reach teardown admission")
			}
			time.Sleep(time.Millisecond)
		}
		select {
		case err := <-closed:
			t.Fatalf("Close returned while the admitted seal was held: %v", err)
		default:
		}
	}
	release()
	select {
	case got := <-predicate:
		failSeal.Store(false)
		switch mode {
		case "changed-basis":
			if got.needs || !errors.Is(got.err, ErrRootPublicationBasisMismatch) {
				t.Fatalf("advanced basis was treated as current: %+v", got)
			}
		case "failed-seal":
			if got.needs || !errors.Is(got.err, injected) {
				t.Fatalf("physical seal failure lost: %+v", got)
			}
		default:
			if !got.needs || got.err != nil {
				t.Fatalf("original basis lost its lagging fallback: %+v", got)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("predicate failed to finish after the original seal returned")
	}
	if closed != nil {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close could not finish after predicate released ownership")
		}
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("reopened Close: %v", err)
		}
	}()
	if got, err := reopened.Get(key); err != nil || string(got) != string(value) {
		t.Fatalf("following command after reopen: %q, %v", got, err)
	}
	if got, err := reopened.Get([]byte("preceding")); err != nil || string(got) != "value" {
		t.Fatalf("preceding durable root after reopen: %q, %v", got, err)
	}
}
