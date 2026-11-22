package treedb

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestPublicRawCOWTxnGroupPressurePreAdmitsBothSlots(t *testing.T) {
	// Prevent an independent generational checkpoint from consuming the
	// deliberately unsealed prefix; foreground checkpoint remains enabled.
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, reclaimable := range []bool{true, false} {
		name := "installed-prefix"
		if !reclaimable {
			name = "only-uninstalled-reservations"
		}
		t.Run(name, func(t *testing.T) {
			db, opts, blocked := rawTxnGroupPressureOpen(t)
			prefix, opening, err := db.backend.PrivateCOWAcceptedPrefix(db.txnOwner)
			if err != nil {
				t.Fatal(err)
			}
			if prefix.AcceptedPendingCount() != 0 || opening.S.CanonicalLSN != opening.C {
				t.Fatalf("fresh accepted prefix is not quiescent: %+v", opening)
			}
			bound := opts.COWMemtableLimits.MaxSources * opts.COWMemtableLimits.MaxGenerations
			capacity, err := rawTxnGroupPressureFreeSlots(prefix, bound)
			if err != nil || capacity < 4 {
				t.Fatalf("fresh ordinary capacity=%d bound=%d err=%v", capacity, bound, err)
			}
			physical, ok := db.backend.StateToken()
			if !ok {
				t.Fatal("opening physical state unavailable")
			}
			const pair = "target/"
			if reclaimable {
				rawTxnGroupPressureSeed(t, db, blocked)
			}
			rawTxnGroupPressureRequireIdle(t, db)
			full := prefix.LogicalFrontiers()
			installed := prefix.AcceptedPendingCount()
			free, err := rawTxnGroupPressureFreeSlots(prefix, bound)
			if err != nil || free != capacity-installed || free < 1 {
				t.Fatalf("ordinary census: capacity=%d installed=%d free=%d: %v", capacity, installed, free, err)
			}
			if reclaimable {
				state, ok := db.backend.StateToken()
				if installed < 2 || full.V != opening.V+2 || full.C != opening.C+3 ||
					full.W != full.C || full.S != opening.S || !ok || state != physical {
					t.Fatalf("real two-txn group did not leave a stable, unsealed installed prefix: before=%+v after=%+v installed=%d", opening, full, installed)
				}
			} else if installed != 0 || full != opening {
				t.Fatalf("uninstalled-reservation control contains reclaimable work: before=%+v after=%+v installed=%d", opening, full, installed)
			}

			// Fixture-held tickets stay unassigned/uncancellable by the DB.
			// Only genuine raw writes above can supply reclaimable installed slots.
			var heldTickets []rootpublication.AcceptedTicket
			releaseHolds := func() {
				t.Helper()
				for i := range heldTickets {
					if err := heldTickets[i].CancelAccepted(); err != nil {
						t.Errorf("release fixture's own ordinary reservation: %v", err)
					}
				}
				heldTickets = nil
			}
			t.Cleanup(func() {
				if !blocked.Load() {
					releaseHolds()
				}
			})
			for i := 0; i < free-1; i++ {
				ticket, err := prefix.ReserveAccepted(false)
				if err != nil {
					t.Fatalf("hold ordinary slot %d/%d: %v", i, free-1, err)
				}
				heldTickets = append(heldTickets, ticket)
			}
			if left, err := rawTxnGroupPressureFreeSlots(prefix, bound); err != nil || left != 1 {
				t.Fatalf("require exactly one real ordinary slot before target commit: free=%d err=%v", left, err)
			}
			held := db.AcquireSnapshot()
			if held == nil {
				t.Fatal("no independent snapshot")
			}
			t.Cleanup(func() {
				if held != nil && !blocked.Load() {
					if err := held.Close(); err != nil {
						t.Errorf("close held snapshot: %v", err)
					}
				}
			})
			assertOld := func() {
				t.Helper()
				for _, suffix := range []string{"a", "b"} {
					cowPublicContractSnapshotValue(t, held, pair+suffix, nil, false)
					if reclaimable {
						cowPublicContractSnapshotValue(t, held, "seed-one/"+suffix, []byte("seed-one"), true)
						cowPublicContractSnapshotValue(t, held, "seed-two/"+suffix, []byte("seed-two"), true)
					}
				}
			}
			if reclaimable {
				// Explicit CommitSync must not use the new ordinary Commit route
				// even though this is the same profile and a reclaim is possible.
				excluded := rawTxnGroupPressureStage(t, db, "explicit-sync/", "excluded")
				next := db.backend.CommandWALNextLSN()
				err := cowPublicContractComplete(t, blocked, "explicit CommitSync at one free slot", excluded.CommitSync)
				if !errors.Is(err, memtable.ErrCOWCapacity) ||
					prefix.LogicalFrontiers() != full || db.backend.CommandWALNextLSN() != next ||
					prefix.AcceptedPendingCount() != installed {
					t.Fatalf("explicit CommitSync enrolled in ordinary pair admission or changed the accepted prefix: err=%v before=%+v after=%+v",
						err, full, prefix.LogicalFrontiers())
				}
				if left, probeErr := rawTxnGroupPressureFreeSlots(prefix, bound); probeErr != nil || left != 1 {
					t.Fatalf("explicit CommitSync refusal leaked its original reservation: free=%d: %v", left, probeErr)
				}
				for _, key := range []string{"explicit-sync/a", "explicit-sync/b"} {
					if got, err := db.Get([]byte(key)); err != nil || got != nil {
						t.Fatalf("explicit CommitSync refusal leaked %q: %q, %v", key, got, err)
					}
				}
				rawTxnGroupPressureRequireIdle(t, db)
			}
			next := db.backend.CommandWALNextLSN()
			before := db.Stats()
			seal := rootpublication.SealedPrefix{LogicalSequence: full.V, CanonicalLSN: full.C}
			target := rawTxnGroupPressureStage(t, db, pair, "target")
			var dataSyncs, targetPreparations atomic.Int32
			restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
				if !reclaimable {
					return nil
				}
				switch {
				case e.Point == durabilitycut.BeforeIndexDataSync && e.Resource == durabilitycut.ResourceIndex &&
					filepath.Clean(e.Root) == filepath.Clean(db.backend.Dir()) && targetPreparations.Load() == 0:
					dataSyncs.Add(1)
					left, err := rawTxnGroupPressureFreeSlots(prefix, bound)
					if err != nil || left != 1 {
						return fmt.Errorf("foreground checkpoint must not hold a partial producer reservation: free=%d: %v", left, err)
					}
				case e.Point == durabilitycut.BeforeCOWPreparation && e.Resource == durabilitycut.ResourceAuxiliary:
					targetPreparations.Add(1)
					state, ok := db.backend.StateToken()
					if dataSyncs.Load() == 0 || prefix.LogicalFrontiers().S != seal ||
						!ok || state.AppliedCommandLSN != full.C || state.CommitSeq <= physical.CommitSeq {
						return fmt.Errorf("target preparation before physical reclaim: data_syncs=%d frontiers=%+v state=%+v", dataSyncs.Load(), prefix.LogicalFrontiers(), state)
					}
				}
				return nil
			}))
			t.Cleanup(restore)
			err = cowPublicContractComplete(t, blocked, "ordinary durable target transaction at one free slot", target.Commit)
			restore()
			after := prefix.LogicalFrontiers()
			if !reclaimable {
				if !errors.Is(err, memtable.ErrCOWCapacity) || after != full || db.backend.CommandWALNextLSN() != next ||
					prefix.AcceptedPendingCount() != 0 {
					t.Fatalf("uninstalled reservations must refuse without assignment: err=%v before=%+v after=%+v next=%d", err, full, after, db.backend.CommandWALNextLSN())
				}
				if left, probeErr := rawTxnGroupPressureFreeSlots(prefix, bound); probeErr != nil || left != 1 {
					t.Fatalf("refusal did not return private ordinary reservation(s): free=%d: %v", left, probeErr)
				}
				rawTxnGroupPressureRequireIdle(t, db)
				assertOld()
				for _, key := range []string{pair + "a", pair + "b"} {
					if got, err := db.Get([]byte(key)); err != nil || got != nil {
						t.Fatalf("refusal leaked %q: %q, %v", key, got, err)
					}
				}
				releaseHolds()
				retry := rawTxnGroupPressureStage(t, db, pair, "target")
				if err := cowPublicContractComplete(t, blocked, "fresh ordinary Commit after releasing uninstalled fixture holds", retry.Commit); err != nil {
					t.Fatalf("released fixture holds did not restore legitimate writes: %v", err)
				}
				after = prefix.LogicalFrontiers()
			} else if err != nil {
				if errors.Is(err, memtable.ErrCOWCapacity) && after == full && db.backend.CommandWALNextLSN() == next &&
					dataSyncs.Load() == 0 && targetPreparations.Load() == 0 && prefix.AcceptedPendingCount() == installed {
					t.Fatalf("stock refused at the second ordinary (group-barrier) reservation before assignment: capacity=%d installed=%d free=1 next=%d: %v", capacity, installed, next, err)
				}
				t.Fatalf("target Commit failed without the exact expected stock refusal: frontiers=%+v data_syncs=%d prepares=%d: %v", after, dataSyncs.Load(), targetPreparations.Load(), err)
			} else if dataSyncs.Load() == 0 || targetPreparations.Load() != 1 || after.S != seal {
				t.Fatalf("successful reclaim must seal the original prefix before one target preparation: sealed=%+v data_syncs=%d prepares=%d", after.S, dataSyncs.Load(), targetPreparations.Load())
			}
			rawTxnGroupPressureRequireIdle(t, db)
			frames := scanPublicCommandWALV2(t, db.backend.Dir())
			raw, markers := 0, 0
			for _, f := range frames {
				if f.LSN < next {
					continue
				}
				switch f.Kind {
				case commitlog.CommandKindRawKVBatch:
					raw++
				case commitlog.CommandKindDurablePrefixBarrier:
					markers++
				default:
					t.Fatalf("unexpected new command %d at LSN %d", f.Kind, f.LSN)
				}
			}
			if raw != 1 || markers > 1 || after.V != full.V+1 || after.C != next+uint64(markers) ||
				after.W != after.C || db.backend.CommandWALNextLSN() != after.C+1 {
				t.Fatalf("require one ordinary mutation/group/durable ACK: raw=%d marker=%d start_lsn=%d after=%+v", raw, markers, next, after)
			}
			requirePublicStatDelta(t, before, db.Stats(), "treedb.command_wal.group_commit.groups_total", 1)
			requirePublicStatDelta(t, before, db.Stats(), "treedb.command_wal.group_commit.commits_total", 1)
			assertOld()
			if err := held.Close(); err != nil {
				t.Fatalf("release old snapshot: %v", err)
			}
			held = nil
			releaseHolds()
			cache := db.cached
			if err := db.Close(); err != nil {
				t.Fatalf("checked Close: %v", err)
			}
			requirePublicCOWFinalZero(t, cache)

			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("fresh reopen: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			values := map[string]string{pair + "a": "target", pair + "b": "target"}
			if reclaimable {
				for _, seed := range []string{"seed-one", "seed-two"} {
					values[seed+"/a"], values[seed+"/b"] = seed, seed
				}
			}
			for key, want := range values {
				if got, err := reopened.Get([]byte(key)); err != nil || string(got) != want {
					t.Fatalf("reopened %q=%q want %q: %v", key, got, want, err)
				}
			}
			if got := publicStatUint64(t, reopened, "treedb.applied_command_lsn"); got != after.C {
				t.Fatalf("reopened applied LSN=%d want %d", got, after.C)
			}
			reopenedCache := reopened.cached
			if err := reopened.Close(); err != nil {
				t.Fatalf("checked reopened Close: %v", err)
			}
			requirePublicCOWFinalZero(t, reopenedCache)
		})
	}
}

func rawTxnGroupPressureOpen(t *testing.T) (*DB, Options, *atomic.Bool) {
	t.Helper()
	return cowPublicContractOpen(t, ProfileCommandWALDurable, func(o *Options) {
		o.COWMemtableLimits = DefaultCOWMemtableLimits()
		o.COWMemtableLimits.MaxSources = 4
		o.COWMemtableLimits.MaxGenerations = 8
		o.ValueLog.ForcePointers = false
		o.ValueLog.PointerThreshold = 1 << 30
		o.DisableBackgroundPrune = true
		o.BackgroundCheckpointInterval = -1
		o.BackgroundCheckpointIdleDuration = -1
		o.MaxWALBytes = -1
		o.BackgroundIndexVacuumInterval = -1
	})
}

func TestPublicRawCOWTxnGroupPressureReturnsPreappendReservations(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	db, opts, blocked := rawTxnGroupPressureOpen(t)
	prefix, opening, err := db.backend.PrivateCOWAcceptedPrefix(db.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	bound := opts.COWMemtableLimits.MaxSources * opts.COWMemtableLimits.MaxGenerations
	capacity, err := rawTxnGroupPressureFreeSlots(prefix, bound)
	if err != nil || capacity < 2 || prefix.AcceptedPendingCount() != 0 {
		t.Fatalf("fresh ordinary capacity=%d bound=%d pending=%d: %v", capacity, bound, prefix.AcceptedPendingCount(), err)
	}
	rawTxnGroupPressureRequireIdle(t, db)
	next := db.backend.CommandWALNextLSN()
	before := db.Stats()
	const stem = "prepare-failure/"
	target := rawTxnGroupPressureStage(t, db, stem, "abort")
	injected := errors.New("test: refuse raw COW canonical preparation before append")
	var seen atomic.Int32
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point != durabilitycut.BeforeCOWPreparation || e.Resource != durabilitycut.ResourceAuxiliary {
			return nil
		}
		seen.Add(1)
		// On stock the second reservation is acquired under the group gate;
		// the new route pre-admits it. Both must exist at this genuine prepare
		// boundary, and both remain cancellable because no LSN is assigned.
		left, err := rawTxnGroupPressureFreeSlots(prefix, bound)
		if err != nil || left != capacity-2 || prefix.LogicalFrontiers() != opening ||
			prefix.AcceptedPendingCount() != 0 {
			return fmt.Errorf("before append expected exactly two ordinary reservations and no accepted history: free=%d capacity=%d frontiers=%+v: %v",
				left, capacity, prefix.LogicalFrontiers(), err)
		}
		return injected
	}))
	t.Cleanup(restore)
	err = cowPublicContractComplete(t, blocked, "ordinary durable Commit with preappend failure", target.Commit)
	restore()
	if err != injected || seen.Load() != 1 {
		t.Fatalf("preappend error lost its identity or repeated preparation: err=%v calls=%d", err, seen.Load())
	}
	if after := prefix.LogicalFrontiers(); after != opening || db.backend.CommandWALNextLSN() != next ||
		prefix.AcceptedPendingCount() != 0 {
		t.Fatalf("preappend error assigned or installed a command: before=%+v after=%+v next=%d",
			opening, after, db.backend.CommandWALNextLSN())
	}
	if left, probeErr := rawTxnGroupPressureFreeSlots(prefix, bound); probeErr != nil || left != capacity {
		t.Fatalf("preappend error leaked private reservation(s): free=%d capacity=%d: %v", left, capacity, probeErr)
	}
	rawTxnGroupPressureRequireIdle(t, db)
	after := db.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.append.count_total", 0)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.groups_total", 0)
	if active := publicStatUint64(t, db, "treedb.cache.conditional_txn.active"); active != 0 {
		t.Fatalf("failed Commit leaked a transaction handle: active=%d", active)
	}

	// Staging re-reads both still-absent keys before trying a healthy new
	// transaction; do not replay the failed transaction or its arena.
	retry := rawTxnGroupPressureStage(t, db, stem, "recovered")
	if err := cowPublicContractComplete(t, blocked, "new ordinary Commit after preappend failure", retry.Commit); err != nil {
		t.Fatal(err)
	}
	stable := prefix.LogicalFrontiers()
	if stable.V != opening.V+1 || stable.C != next || stable.W != next ||
		db.backend.CommandWALNextLSN() != next+1 {
		t.Fatalf("new transaction did not publish and durably assign exactly once: before=%+v after=%+v next=%d",
			opening, stable, db.backend.CommandWALNextLSN())
	}
	cache := db.cached
	if err := db.Close(); err != nil {
		t.Fatalf("checked Close after preappend failure: %v", err)
	}
	requirePublicCOWFinalZero(t, cache)

	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	for _, key := range []string{stem + "a", stem + "b"} {
		if got, err := reopened.Get([]byte(key)); err != nil || string(got) != "recovered" {
			t.Fatalf("reopened %q=%q want recovered: %v", key, got, err)
		}
	}
	if got := publicStatUint64(t, reopened, "treedb.applied_command_lsn"); got != next {
		t.Fatalf("reopened applied LSN=%d want exactly %d", got, next)
	}
	cache = reopened.cached
	if err := reopened.Close(); err != nil {
		t.Fatalf("checked reopened Close: %v", err)
	}
	requirePublicCOWFinalZero(t, cache)
}

func rawTxnGroupPressureFreeSlots(prefix *rootpublication.Coordinator, bound int) (count int, err error) {
	var probes []rootpublication.AcceptedTicket
	defer func() {
		for _, ticket := range probes {
			err = errors.Join(err, ticket.CancelAccepted())
		}
	}()
	for len(probes) <= bound {
		ticket, reserveErr := prefix.ReserveAccepted(false)
		if errors.Is(reserveErr, rootpublication.ErrAcceptedCapacity) {
			return len(probes), nil
		}
		if reserveErr != nil {
			return 0, reserveErr
		}
		probes = append(probes, ticket)
	}
	return 0, fmt.Errorf("ordinary free-slot probe exceeded its %d-slot bound", bound)
}

func rawTxnGroupPressureRequireIdle(t *testing.T, db *DB) {
	t.Helper()
	g := &db.commandWALGroupCommit
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pendingCount != 0 || g.pendingBarrier.Valid() || g.leaderActive || g.durableIntents.Load() != 0 || g.terminalErr != nil ||
		(g.cond.L != nil && (g.nextTicket != g.completedTicket || g.publishTicket != g.nextTicket+1)) {
		t.Fatalf("public WAL group not quiescent: pending=%d barrier=%v leader=%v intents=%d tickets=%d/%d/%d err=%v",
			g.pendingCount, g.pendingBarrier.Valid(), g.leaderActive, g.durableIntents.Load(), g.nextTicket, g.completedTicket, g.publishTicket, g.terminalErr)
	}
}

func rawTxnGroupPressureStage(t *testing.T, db *DB, prefix, value string) *ConditionalTxn {
	t.Helper()
	return rawTxnGroupPressureStageGuarded(t, db, prefix, value, nil)
}

func rawTxnGroupPressureStageGuarded(t *testing.T, db *DB, prefix, value string, blocked *atomic.Bool) *ConditionalTxn {
	t.Helper()
	tx, err := db.NewConditionalTxn()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if blocked == nil || !blocked.Load() {
			_ = tx.Close()
		}
	})
	for _, suffix := range []string{"a", "b"} {
		key := []byte(prefix + suffix)
		if got, err := tx.Get(key); err != nil || got != nil {
			t.Fatalf("opening read %q=%q, %v; want missing", key, got, err)
		}
		if err := tx.Set(key, []byte(value)); err != nil {
			t.Fatalf("stage %q: %v", key, err)
		}
	}
	return tx
}

func rawTxnGroupPressureStart(writers *sync.WaitGroup, tx *ConditionalTxn) <-chan error {
	done := make(chan error, 1)
	writers.Go(func() { done <- tx.Commit() })
	return done
}

func rawTxnGroupPressureJoin(t *testing.T, writers *sync.WaitGroup, blocked *atomic.Bool) {
	t.Helper()
	joined := make(chan struct{})
	go func() { writers.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(10 * time.Second):
		blocked.Store(true)
		t.Error("public raw writers did not unwind before fixture cleanup")
	}
}

func rawTxnGroupPressureSeed(t *testing.T, db *DB, blocked *atomic.Bool) {
	t.Helper()
	var writers sync.WaitGroup
	defer rawTxnGroupPressureJoin(t, &writers, blocked)
	g := &db.commandWALGroupCommit
	delay, maxCommits, maxBytes := g.delay, g.maxCommits, g.maxBytes
	g.delay, g.maxCommits, g.maxBytes = 10*time.Second, 2, 1<<30
	registered := make(chan struct{})
	var once sync.Once
	g.testAfterRegister = func(_ uint64, durable bool) {
		if durable {
			once.Do(func() { close(registered) })
		}
	}
	groups := make(chan int, 2)
	g.testBeforeSync = func(n int) { groups <- n }
	next := db.backend.CommandWALNextLSN()
	a := rawTxnGroupPressureStageGuarded(t, db, "seed-one/", "seed-one", blocked)
	b := rawTxnGroupPressureStageGuarded(t, db, "seed-two/", "seed-two", blocked)
	aDone := rawTxnGroupPressureStart(&writers, a)
	select {
	case <-registered:
	case err := <-aDone:
		t.Fatalf("first public transaction did not register a durable intent: %v", err)
	case <-time.After(10 * time.Second):
		blocked.Store(true)
		t.Fatal("first public WAL registration timed out")
	}
	bDone := rawTxnGroupPressureStart(&writers, b)
	for _, done := range []<-chan error{aDone, bDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("real grouped raw Commit: %v", err)
			}
		case <-time.After(10 * time.Second):
			blocked.Store(true)
			t.Fatal("grouped raw commits timed out")
		}
	}
	g.delay, g.maxCommits, g.maxBytes = delay, maxCommits, maxBytes
	g.testAfterRegister, g.testBeforeSync = nil, nil
	if len(groups) != 1 || <-groups != 2 {
		t.Fatal("require exactly one real durable group containing both raw transactions")
	}
	frames := scanPublicCommandWALV2(t, db.backend.Dir())
	var suffix []commitlog.CommandEnvelope
	for _, f := range frames {
		if f.LSN >= next {
			suffix = append(suffix, f)
		}
	}
	if len(suffix) != 3 || suffix[0].Kind != commitlog.CommandKindRawKVBatch ||
		suffix[1].Kind != commitlog.CommandKindRawKVBatch || suffix[2].Kind != commitlog.CommandKindDurablePrefixBarrier ||
		suffix[2].DurabilityClass != commitlog.CommandDurabilityDurable {
		t.Fatalf("required V2 WAL suffix is two actual raw transaction frames and their durable marker: %+v", suffix)
	}
}
