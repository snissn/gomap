package treedb

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func configureRawTxnQueuedPair(o *Options, maxWAL int64) {
	o.COWMemtableLimits = DefaultCOWMemtableLimits()
	o.COWMemtableLimits.MaxSources = 4
	o.COWMemtableLimits.MaxGenerations = 8
	o.ValueLog.ForcePointers = false
	o.ValueLog.PointerThreshold = 1 << 30
	o.DisableBackgroundPrune = true
	o.BackgroundCheckpointInterval = -1
	o.BackgroundCheckpointIdleDuration = -1
	// Arm the original worker in the positive case, but keep its
	// trigger safely above this bounded deterministic workload.
	o.MaxWALBytes = maxWAL
	o.BackgroundIndexVacuumInterval = -1
}

func TestPublicRawCOWTxnRechecksQueuedPairAfterCheckpoint(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	for _, tc := range []struct {
		name            string
		maxWAL          int64
		wantCheckpoints uint64
		withSuffix      bool
	}{
		{name: "live-size-worker", maxWAL: 2 << 30, wantCheckpoints: 1},
		{name: "disabled-size-worker", maxWAL: -1, wantCheckpoints: 2},
		{name: "live-size-worker-late-suffix", maxWAL: 2 << 30, wantCheckpoints: 1, withSuffix: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			crashRoot, crash := os.LookupEnv(rawTxnQueuedCrashRootEnv)
			if crash && (crashRoot == "" || tc.name != "live-size-worker") {
				t.Fatal("queued crash child requires its own directory and the live size worker")
			}
			var dir string
			if crash {
				dir = filepath.Join(crashRoot, "db")
			} else {
				dir = t.TempDir()
			}
			db, opts, blocked := cowPublicContractOpenAt(t, ProfileCommandWALDurable, dir, func(o *Options) {
				configureRawTxnQueuedPair(o, tc.maxWAL)
			})
			prefix, opening, err := db.backend.PrivateCOWAcceptedPrefix(db.txnOwner)
			if err != nil {
				t.Fatal(err)
			}
			bound := opts.COWMemtableLimits.MaxSources * opts.COWMemtableLimits.MaxGenerations
			capacity, err := rawTxnGroupPressureFreeSlots(prefix, bound)
			if err != nil || capacity < 5 || prefix.AcceptedPendingCount() != 0 {
				t.Fatalf("opening accepted-prefix census: capacity=%d pending=%d err=%v", capacity, prefix.AcceptedPendingCount(), err)
			}

			var writers sync.WaitGroup
			var releaseCheckpoint func()
			defer func() {
				// Failure can occur after any subset of the writers started.
				// Unpark DATA, then join all started callers before fixture cleanup.
				if releaseCheckpoint != nil {
					releaseCheckpoint()
				}
				rawTxnGroupPressureJoin(t, &writers, blocked)
			}()
			commit := func(label string, tx *ConditionalTxn) {
				t.Helper()
				select {
				case err := <-rawTxnGroupPressureStart(&writers, tx):
					if err != nil {
						t.Fatalf("%s: %v", label, err)
					}
				case <-time.After(10 * time.Second):
					blocked.Store(true)
					t.Fatalf("%s did not complete: possible lock reentry deadlock", label)
				}
			}

			// The genuine root-neutral marker separates installed entries and
			// prevents the following raw singleton from coalescing with the
			// grouped raw prefix. After reclaim we need at least four free
			// slots, enough for both contenders before either can publish.
			rawTxnGroupPressureSeed(t, db, blocked)
			extra := rawTxnGroupPressureStageGuarded(t, db, "seed-extra/", "seed-extra", blocked)
			commit("seed additional installed raw transaction", extra)
			rawTxnGroupPressureRequireIdle(t, db)
			full := prefix.LogicalFrontiers()
			installed := prefix.AcceptedPendingCount()
			free, err := rawTxnGroupPressureFreeSlots(prefix, bound)
			if err != nil || free < 1 || installed < 3 || free+installed != capacity ||
				full.V != opening.V+3 || full.C != opening.C+4 || full.C != full.W || full.S != opening.S {
				t.Fatalf("need at least three real installed and unsealed entries: installed=%d free=%d capacity=%d before=%+v full=%+v err=%v",
					installed, free, capacity, opening, full, err)
			}
			rootBefore, ok := db.backend.StateToken()
			if !ok || rootBefore.AppliedCommandLSN != opening.S.CanonicalLSN {
				t.Fatalf("seed prefix unexpectedly drained: physical=%+v opening=%+v", rootBefore, opening)
			}

			var heldTickets []rootpublication.AcceptedTicket
			releaseTickets := func() {
				t.Helper()
				for _, ticket := range heldTickets {
					if err := ticket.CancelAccepted(); err != nil {
						t.Errorf("release fixture reservation: %v", err)
					}
				}
				heldTickets = nil
			}
			t.Cleanup(func() {
				if !blocked.Load() {
					releaseTickets()
				}
			})
			for i := 0; i < free-1; i++ {
				ticket, err := prefix.ReserveAccepted(false)
				if err != nil {
					t.Fatalf("hold ordinary slot %d: %v", i, err)
				}
				heldTickets = append(heldTickets, ticket)
			}
			held := db.AcquireSnapshot()
			if held == nil {
				t.Fatal("independent old cut unavailable")
			}
			t.Cleanup(func() {
				if held != nil && !blocked.Load() {
					if err := held.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			first := rawTxnGroupPressureStageGuarded(t, db, "first/", "first", blocked)
			follower := rawTxnGroupPressureStageGuarded(t, db, "follower/", "follower", blocked)
			next := db.backend.CommandWALNextLSN()
			before := db.Stats()

			type pressureProof struct {
				free      int
				frontiers rootpublication.LogicalFrontiers
				err       error
			}
			pressure := make(chan pressureProof, 2)
			var pressureCount atomic.Int32
			dataReached := make(chan struct{})
			var dataOnce atomic.Bool
			unblock := make(chan struct{})
			releaseCheckpoint = sync.OnceFunc(func() { close(unblock) })
			restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
				switch {
				case e.Point == durabilitycut.BeforeCOWAcceptedPairPressureWait && e.Resource == durabilitycut.ResourceAuxiliary:
					if pressureCount.Add(1) <= 2 {
						left, err := rawTxnGroupPressureFreeSlots(prefix, bound)
						pressure <- pressureProof{left, prefix.LogicalFrontiers(), err}
					}
				case e.Point == durabilitycut.BeforeIndexDataSync && e.Resource == durabilitycut.ResourceIndex &&
					filepath.Clean(e.Root) == filepath.Clean(db.backend.Dir()) && dataOnce.CompareAndSwap(false, true):
					close(dataReached)
					<-unblock
				}
				return nil
			}))
			t.Cleanup(restore)
			awaitPressure := func(label string, done <-chan error) {
				t.Helper()
				select {
				case proof := <-pressure:
					if proof.err != nil || proof.free != 1 || proof.frontiers != full {
						t.Fatalf("%s failed to prove real pre-reclaim one-slot pressure: %+v want %+v", label, proof, full)
					}
				case err := <-done:
					t.Fatalf("%s completed before its initial paired pressure observation: %v", label, err)
				case <-time.After(10 * time.Second):
					blocked.Store(true)
					t.Fatalf("%s did not reach initial pair denial", label)
				}
			}

			firstDone := rawTxnGroupPressureStart(&writers, first)
			awaitPressure("first public commit", firstDone)
			select {
			case <-dataReached:
			case err := <-firstDone:
				t.Fatalf("first caller completed without reaching real original prefix DATA sync: %v", err)
			case <-time.After(10 * time.Second):
				blocked.Store(true)
				t.Fatal("first caller did not reach real original prefix DATA sync")
			}
			followerDone := rawTxnGroupPressureStart(&writers, follower)
			// Not merely a goroutine start: this observer runs only after
			// the follower itself actually got an atomic no-pair result.
			awaitPressure("queued follower", followerDone)
			commits := uint64(2)
			stems := []string{"first", "follower"}
			if tc.withSuffix {
				// Refund two fixture-only holds while the captured prefix is
				// still paused. The late writer uses its real pair; after the
				// old installed prefix retires both queued writers must
				// still have space before either can publish.
				n := len(heldTickets) - 2
				if n < 0 {
					t.Fatal("suffix fixture has too few private holds to refund")
				}
				for _, ticket := range heldTickets[n:] {
					if err := ticket.CancelAccepted(); err != nil {
						t.Fatal(err)
					}
				}
				heldTickets = heldTickets[:n]
				suffix := rawTxnGroupPressureStageGuarded(t, db, "late/", "late", blocked)
				commit("durable late suffix", suffix)
				remaining, err := rawTxnGroupPressureFreeSlots(prefix, bound)
				if err != nil || remaining+installed < 4 {
					t.Fatalf("late suffix consumed a queued contender's needed slot: remaining=%d old installed=%d: %v", remaining, installed, err)
				}
				f := prefix.LogicalFrontiers()
				if f.V != full.V+1 || f.C <= full.C || f.W != f.C || f.S != full.S {
					t.Fatalf("late suffix did not install outside prior capture before retirement: before=%+v after=%+v", full, f)
				}
				live := db.AcquireSnapshot()
				if live == nil {
					t.Fatal("cannot observe the live late suffix")
				}
				for _, key := range []string{"late/a", "late/b"} {
					cowPublicContractSnapshotValue(t, live, key, []byte("late"), true)
					cowPublicContractSnapshotValue(t, held, key, nil, false)
				}
				if err := live.Close(); err != nil {
					t.Fatal(err)
				}
				commits++
				stems = append(stems, "late")
			}
			releaseCheckpoint()
			for i, done := range []<-chan error{firstDone, followerDone} {
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("durable Commit %d: %v", i, err)
					}
				case <-time.After(10 * time.Second):
					blocked.Store(true)
					t.Fatalf("durable Commit %d never returned", i)
				}
			}
			restore()
			if calls := pressureCount.Load(); calls != 2 {
				t.Fatalf("want two actual initial pair denials, got %d", calls)
			}
			rawTxnGroupPressureRequireIdle(t, db)

			after := prefix.LogicalFrontiers()
			afterNext := db.backend.CommandWALNextLSN()
			statAfter := db.Stats()
			wantSeal := rootpublication.SealedPrefix{LogicalSequence: full.V, CanonicalLSN: full.C}
			// Both raw commits are real. Grouping can legitimately supply a
			// root-neutral certificate for a queued suffix, never extra raw
			// mutation frames or acceptance/ACK of only one key.
			if after.V != full.V+commits || after.W != after.C || after.C+1 != afterNext ||
				afterNext < next+commits || afterNext > next+2*commits ||
				after.S.LogicalSequence < wantSeal.LogicalSequence || after.S.CanonicalLSN < wantSeal.CanonicalLSN {
				t.Fatalf("wrong sealed prefix or durable mutation suffix: before=%+v after=%+v next=%d→%d", full, after, next, afterNext)
			}
			requirePublicStatDelta(t, before, statAfter, "treedb.command_wal.append.entry_scan.count_total", commits)
			requirePublicStatDelta(t, before, statAfter, "treedb.command_wal.append.count_total", afterNext-next)
			requirePublicStatDelta(t, before, statAfter, "treedb.command_wal.group_commit.commits_total", commits)
			requirePublicStatDelta(t, before, statAfter, "treedb.cache.auto_checkpoint.count", 0)
			physical, ok := db.backend.StateToken()
			if !ok || physical.CommitSeq <= rootBefore.CommitSeq || physical.AppliedCommandLSN < wantSeal.CanonicalLSN {
				t.Fatalf("no real physical reclamation: before=%+v after=%+v", rootBefore, physical)
			}
			for _, suffix := range []string{"a", "b"} {
				for _, stem := range stems {
					cowPublicContractSnapshotValue(t, held, stem+"/"+suffix, nil, false)
				}
				for _, seed := range []string{"seed-one", "seed-two", "seed-extra"} {
					cowPublicContractSnapshotValue(t, held, seed+"/"+suffix, []byte(seed), true)
				}
			}
			if crash {
				exitRawTxnQueuedCrashChild(t, db, crashRoot, wantSeal, after,
					physical.AppliedCommandLSN, afterNext, statCheckpointRuns(t, statAfter)-statCheckpointRuns(t, before))
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
			releaseTickets()
			cache := db.cached
			if err := db.Close(); err != nil {
				t.Fatalf("checked Close: %v", err)
			}
			requirePublicCOWFinalZero(t, cache)

			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			for _, stem := range append(stems, "seed-one", "seed-two", "seed-extra") {
				for _, suffix := range []string{"a", "b"} {
					key := stem + "/" + suffix
					if got, err := reopened.Get([]byte(key)); err != nil || string(got) != stem {
						t.Fatalf("reopened %q=%q want %q: %v", key, got, stem, err)
					}
				}
			}
			if got := publicStatUint64(t, reopened, "treedb.applied_command_lsn"); got != after.C {
				t.Fatalf("reopened applied LSN=%d want %d", got, after.C)
			}
			cache = reopened.cached
			if err := reopened.Close(); err != nil {
				t.Fatalf("checked reopened Close: %v", err)
			}
			requirePublicCOWFinalZero(t, cache)

			// On stock the queued caller always executes this second full
			// checkpoint, even though the first just reclaimed both pairs.
			// Compare the whole-boundary counter only after original and
			// reopened correctness were checked on either path.
			beforeCheckpoints := statCheckpointRuns(t, before)
			afterCheckpoints := statCheckpointRuns(t, statAfter)
			if afterCheckpoints != beforeCheckpoints+tc.wantCheckpoints {
				t.Fatalf("real Commit/reopen/prefix controls passed, but full checkpoint attempts %d→%d: want exactly %d",
					beforeCheckpoints, afterCheckpoints, tc.wantCheckpoints)
			}
			if tc.wantCheckpoints == 1 && after.S != wantSeal {
				t.Fatalf("the original checkpoint sealed a later logical prefix: %+v want %+v", after.S, wantSeal)
			}
			t.Logf("two true denials at one free; reclaimed=%d V/C/W/S=%+v full-checkpoint attempts=%d",
				installed, after, afterCheckpoints-beforeCheckpoints)
		})
	}
}

func statCheckpointRuns(t *testing.T, stats map[string]string) uint64 {
	t.Helper()
	const key = "treedb.cache.checkpoint.runs"
	v, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		t.Fatalf("required checkpoint counter %s=%q: %v", key, stats[key], err)
	}
	return v
}
