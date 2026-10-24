package treedb

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPublicCommandWALIdleFusionOwnsCaptureGateAtAppend(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var appends, sharedGate atomic.Int32
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyAppend && event.LSN != 0 {
			appends.Add(1)
			if db.commandWALPublicOperationGate.TryRLock() {
				db.commandWALPublicOperationGate.RUnlock()
				sharedGate.Add(1)
			}
		}
		return nil
	})
	defer restore()
	b := db.NewBatch()
	defer b.Close()
	if err = b.Set([]byte("fused-inline"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err = b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if appends.Load() != 1 {
		t.Fatalf("appends=%d want1", appends.Load())
	}
	if sharedGate.Load() != 0 {
		t.Fatal("idle append released exclusive capture admission to a shared registration")
	}
	requireRawKVValue(t, db, []byte("fused-inline"), []byte("value"))
}

// A fused epoch must admit later appends while its physical sync is held, but
// neither an ACK nor Close may overtake the captured prefix.
func TestPublicCommandWALIdleFusionHeldSyncAdmitsSuccessorAndBlocksClose(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entered, release, successorAppended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var appends atomic.Int32
	var held sync.Once
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 && appends.Add(1) == 2 {
			close(successorAppended)
		}
		if e.Point == durabilitycut.BeforeDependencyFileSync && e.LSN != 0 {
			held.Do(func() { close(entered); <-release })
		}
		return nil
	})
	defer restore()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- db.SetSync([]byte("first"), []byte("one")) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("fused epoch did not reach physical sync")
	}
	if !db.commandWALPublicOperationGate.TryLock() {
		t.Fatal("physical sync retained public operation gate")
	}
	db.commandWALPublicOperationGate.Unlock()
	go func() { second <- db.SetSync([]byte("second"), []byte("two")) }()
	select {
	case <-successorAppended:
	case <-time.After(2 * time.Second):
		t.Fatal("successor could not append during physical sync")
	}
	for _, done := range []chan error{first, second} {
		select {
		case err := <-done:
			t.Fatalf("ACK overtook held sync: %v", err)
		default:
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close overtook held sync: %v", err)
	default:
	}
	unblock()
	for _, done := range []chan error{first, second, closed} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("completion or Close stalled")
		}
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireRawKVValue(t, reopened, []byte("first"), []byte("one"))
	requireRawKVValue(t, reopened, []byte("second"), []byte("two"))
}

func TestPublicCommandWALIdleFusionDemandDuringAppendUsesSameCommand(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var appends atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 && appends.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	defer restore()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- db.SetSync([]byte("intent-first"), []byte("one")) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first append not reached")
	}
	go func() { second <- db.SetSync([]byte("intent-second"), []byte("two")) }()
	deadline := time.Now().Add(2 * time.Second)
	for db.commandWALGroupCommit.durableIntents.Load() != 2 {
		if time.Now().After(deadline) {
			t.Fatal("second durable intent not announced during append")
		}
		runtime.Gosched()
	}
	unblock()
	for _, done := range []chan error{first, second} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("changed demand stalled")
		}
	}
	assertPublicFusionMutationCount(t, opts.Dir, 2)
	if db.commandWALGroupCommit.timer == nil {
		t.Fatal("changed demand did not return to common collection")
	}
	if got := db.commandWALGroupCommit.durableIntents.Load(); got != 0 {
		t.Fatalf("intents=%d want0", got)
	}
	requireRawKVValue(t, db, []byte("intent-first"), []byte("one"))
	requireRawKVValue(t, db, []byte("intent-second"), []byte("two"))
}

func TestPublicCommandWALIdleFusionCaptureFailurePoisonsAndReleases(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	injected := errors.New("injected fused capture flush failure")
	var appended atomic.Bool
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
			appended.Store(true)
		}
		if e.Point == durabilitycut.BeforeUserspaceFlush && appended.Load() {
			return injected
		}
		return nil
	})
	err = db.SetSync([]byte("failed-capture"), []byte("value"))
	restore()
	if !errors.Is(err, injected) {
		t.Fatalf("capture error=%v want %v", err, injected)
	}
	db.commandWALGroupCommit.mu.Lock()
	active, terminal := db.commandWALGroupCommit.leaderActive, db.commandWALGroupCommit.terminalErr
	db.commandWALGroupCommit.mu.Unlock()
	if active || terminal == nil {
		t.Fatalf("capture completion active=%t terminal=%v", active, terminal)
	}
	if err = db.SetSync([]byte("after-failure"), []byte("value")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("later append=%v want recovery required", err)
	}
	if !db.commandWALPublicOperationGate.TryLock() {
		t.Fatal("capture failure leaked operation gate")
	}
	db.commandWALPublicOperationGate.Unlock()
}

func assertPublicFusionMutationCount(t *testing.T, dir string, want int) {
	t.Helper()
	count := 0
	for _, frame := range scanPublicCommandWALV2(t, dir) {
		if frame.Kind != commitlog.CommandKindDurablePrefixBarrier {
			count++
		}
	}
	if count != want {
		t.Fatalf("mutation frames=%d want%d; assigned command must not retry", count, want)
	}
}

func TestPublicCommandWALGroupCommitCapturePanicRetiresLeader(t *testing.T) {
	for _, point := range []durabilitycut.Point{durabilitycut.BeforeUserspaceFlush, durabilitycut.BeforeDependencyFileSync} {
		t.Run(string(point), func(t *testing.T) {
			for _, generic := range []bool{true, false} {
				t.Run(map[bool]string{true: "generic", false: "idle"}[generic], func(t *testing.T) {
					opts := commandWALDurabilityProofOptions(t.TempDir())
					db, err := Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					if generic {
						db.commandWALGroupCommit.delay = time.Nanosecond
					}
					var appended atomic.Bool
					restore := durabilitycut.Install(func(e durabilitycut.Event) error {
						if e.Resource != durabilitycut.ResourceCommandWAL {
							return nil
						}
						if e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
							appended.Store(true)
						}
						db.commandWALGroupCommit.mu.Lock()
						active := db.commandWALGroupCommit.leaderActive
						db.commandWALGroupCommit.mu.Unlock()
						if e.Point == point && appended.Load() && active && (point != durabilitycut.BeforeDependencyFileSync || e.LSN != 0) {
							panic("injected capture flush panic")
						}
						return nil
					})
					var recovered any
					func() {
						defer func() { recovered = recover() }()
						_ = db.appendPublicRawKVPointCommand(commitlog.RawKVOpSet, []byte("capture-panic"), []byte("value"), func() page.EntryRevision { return 1 }, true, nil)
					}()
					restore()
					if recovered == nil {
						t.Fatal("capture observer did not panic")
					}
					group := &db.commandWALGroupCommit
					group.mu.Lock()
					active, terminal := group.leaderActive, group.terminalErr
					// Diagnostic cleanup lets the original-source RED exit without wedging Close.
					if active {
						group.leaderActive = false
						group.terminalErr = ErrRecoveryRequired
						group.completedTicket = group.nextTicket
						group.cond.Broadcast()
					}
					group.mu.Unlock()
					operationFree := db.commandWALPublicOperationGate.TryLock()
					if operationFree {
						db.commandWALPublicOperationGate.Unlock()
					} else {
						db.commandWALPublicOperationGate.Unlock()
					}
					publishFree := db.commandWALPublicPublishMu.TryLock()
					if publishFree {
						db.commandWALPublicPublishMu.Unlock()
					} else {
						db.commandWALPublicPublishMu.Unlock()
					}
					_ = db.Close()
					if active || terminal == nil || !operationFree || !publishFree {
						t.Fatalf("panic cleanup: active=%t terminal=%v operation_free=%t publish_free=%t", active, terminal, operationFree, publishFree)
					}
				})
			}
		})
	}
}

func TestPublicCommandWALIdleFusionFallsBackBehindRelaxedLease(t *testing.T) {
	opts := relaxedCommandWALDurablePrefixOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	held, release, appended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	testAfterPublicCommandWALPointAppend = func(op commitlog.RawKVOperation) {
		if string(op.Key) == "lease-relaxed" {
			close(held)
			<-release
		}
	}
	defer func() { testAfterPublicCommandWALPointAppend = nil }()
	var appends atomic.Int32
	var captured atomic.Bool
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource != durabilitycut.ResourceCommandWAL {
			return nil
		}
		if e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 && appends.Add(1) == 2 {
			close(appended)
		}
		if e.Point == durabilitycut.BeforeUserspaceFlush {
			db.commandWALGroupCommit.mu.Lock()
			active := db.commandWALGroupCommit.leaderActive
			db.commandWALGroupCommit.mu.Unlock()
			if active {
				captured.Store(true)
			}
		}
		return nil
	})
	defer restore()
	relaxed, durable := make(chan error, 1), make(chan error, 1)
	go func() { relaxed <- db.Set([]byte("lease-relaxed"), []byte("one")) }()
	select {
	case <-held:
	case <-time.After(2 * time.Second):
		t.Fatal("relaxed publication lease not held")
	}
	go func() { durable <- db.SetSync([]byte("lease-durable"), []byte("two")) }()
	select {
	case <-appended:
	case <-time.After(2 * time.Second):
		t.Fatal("durable append did not use shared fallback")
	}
	if captured.Load() {
		t.Fatal("capture overtook prior relaxed publication lease")
	}
	if db.commandWALPublicOperationGate.TryLock() {
		db.commandWALPublicOperationGate.Unlock()
		t.Fatal("prior relaxed lease was lost")
	}
	unblock()
	for _, done := range []chan error{relaxed, durable} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("lease release did not unblock completion")
		}
	}
	if !captured.Load() {
		t.Fatal("fallback did not perform common capture")
	}
	requireRawKVValue(t, db, []byte("lease-relaxed"), []byte("one"))
	requireRawKVValue(t, db, []byte("lease-durable"), []byte("two"))
	assertPublicFusionMutationCount(t, opts.Dir, 2)
}

func TestPublicCommandWALIdleFusionExcludesCOW(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	opts.MemtableMode = "cow_ordered"
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var appends, shared atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
			appends.Add(1)
			if db.commandWALPublicOperationGate.TryRLock() {
				db.commandWALPublicOperationGate.RUnlock()
				shared.Add(1)
			}
		}
		return nil
	})
	defer restore()
	if err = db.SetSync([]byte("cow-control"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if appends.Load() == 0 || shared.Load() != appends.Load() {
		t.Fatalf("COW append gates appends=%d shared=%d", appends.Load(), shared.Load())
	}
	requireRawKVValue(t, db, []byte("cow-control"), []byte("value"))
}
