package treedb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Observe one combined schedule, not the frequency of overlap in a workload.
// The real size worker is only parked at its existing byte sampler. The
// foreground checkpoint and both commits still use their ordinary entrypoints.
func TestPublicRawCOWTxnQueuedRecheckOverlapsRealSizeWorker(t *testing.T) {
	const sizeLimit = 128 << 10
	const autoCount = "treedb.cache.auto_checkpoint.count"
	const autoReason = "treedb.cache.auto_checkpoint.last_reason"
	const autoBefore = "treedb.cache.auto_checkpoint.last_wal_reclaimable_before"
	const autoAfter = "treedb.cache.auto_checkpoint.last_wal_reclaimable_after"
	const autoTrimmed = "treedb.cache.auto_checkpoint.last_wal_bytes_trimmed"
	const cleanupDone = "treedb.cache.command_wal.cleanup.completions_total"
	const removedBytes = "treedb.command_wal.cleanup.removed_bytes_total"
	const removedSegments = "treedb.command_wal.cleanup.removed_segments_total"
	const namespaceSync = "treedb.command_wal.cleanup.namespace_sync.calls_total"
	const namespacePending = "treedb.command_wal.cleanup.namespace_sync.pending"
	const rawAppends = "treedb.command_wal.append.entry_scan.count_total"
	const totalAppends = "treedb.command_wal.append.count_total"
	const durableCommits = "treedb.command_wal.group_commit.commits_total"

	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	database, opts, blocked := cowPublicContractOpen(t, ProfileCommandWALDurable, func(o *Options) {
		o.COWMemtableLimits = DefaultCOWMemtableLimits()
		o.COWMemtableLimits.MaxSources = 4
		o.COWMemtableLimits.MaxGenerations = 8
		o.ValueLog.ForcePointers = false
		o.ValueLog.PointerThreshold = 256
		o.ValueLog.Compression = ValueLogCompressionOff
		o.CommandWALSegmentTargetBytes = 64 << 10
		o.WALMaxSegmentBytes = 1 << 20
		o.MaxWALBytes = sizeLimit
		o.DisableBackgroundPrune = true
		o.BackgroundCheckpointInterval = -1
		o.BackgroundCheckpointIdleDuration = -1
		o.BackgroundIndexVacuumInterval = -1
	})

	prefix, opening, err := database.backend.PrivateCOWAcceptedPrefix(database.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	bound := opts.COWMemtableLimits.MaxSources * opts.COWMemtableLimits.MaxGenerations
	capacity, err := rawTxnGroupPressureFreeSlots(prefix, bound)
	if err != nil || capacity < 5 || prefix.AcceptedPendingCount() != 0 {
		t.Fatalf("opening census: capacity=%d pending=%d err=%v", capacity, prefix.AcceptedPendingCount(), err)
	}
	rawTxnGroupPressureSeed(t, database, blocked)
	if debt := database.publicCommandWALAutoCheckpointBytes(); debt >= sizeLimit {
		t.Fatalf("small seed unexpectedly crossed the size limit before latch: bytes=%d", debt)
	}

	// Each ordinary transaction supplies three genuinely materialized values
	// below the 64KiB per-value limit, over 128KiB in its canonical raw frame.
	// Different immutable bytes let both held and restarted reads verify the
	// correct key and owner, not just the right length. Actual journal size
	// is still independently checked; payload sizes are not a debt surrogate.
	expected := map[string][]byte{}
	stage := func(stem string, distinct byte) *ConditionalTxn {
		t.Helper()
		tx, err := database.NewConditionalTxn()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if !blocked.Load() {
				_ = tx.Close()
			}
		})
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("%s/%d", stem, i)
			if got, err := tx.Get([]byte(key)); err != nil || got != nil {
				t.Fatalf("opening read %q found len=%d err=%v", key, len(got), err)
			}
			value := make([]byte, 48<<10)
			_, _ = rand.New(rand.NewSource(int64(distinct)*3 + int64(i))).Read(value)
			if err := tx.Set([]byte(key), value); err != nil {
				t.Fatalf("stage %q: %v", key, err)
			}
			expected[key] = value
		}
		return tx
	}

	// Do not replace any byte values or manufacture a worker event. All
	// write notifications continue to use the actual backend try sampler.
	// Only the first *eligible* blocking read pauses. After resuming it
	// must read current bytes, not re-use this pre-foreground observation.
	trySample := database.backend.NewCommandWALPressureSampler()
	if trySample == nil {
		t.Fatal("no real writer WAL pressure sampler")
	}
	sampled := make(chan int64, 1)
	resumeSample := make(chan struct{})
	releaseSample := sync.OnceFunc(func() { close(resumeSample) })
	defer releaseSample()
	var sampleHeld atomic.Bool
	database.cached.SetAutoCheckpointWALBytesHooks(func() int64 {
		debt := database.publicCommandWALAutoCheckpointBytes()
		if debt >= sizeLimit && sampleHeld.CompareAndSwap(false, true) {
			sampled <- debt
			<-resumeSample
			return database.publicCommandWALAutoCheckpointBytes()
		}
		return debt
	}, trySample)
	if err := cowPublicContractComplete(t, blocked, "real initial size debt", stage("seed-size", 1).Commit); err != nil {
		t.Fatal(err)
	}
	var initialBytes int64
	select {
	case initialBytes = <-sampled:
	case <-time.After(10 * time.Second):
		t.Fatalf("original worker did not reach actual size debt: bytes=%d", database.publicCommandWALAutoCheckpointBytes())
	}
	rawTxnGroupPressureRequireIdle(t, database)

	full := prefix.LogicalFrontiers()
	installed := prefix.AcceptedPendingCount()
	free, err := rawTxnGroupPressureFreeSlots(prefix, bound)
	if err != nil || free < 1 || installed < 3 || free+installed != capacity ||
		full.V != opening.V+3 || full.C != opening.C+4 || full.C != full.W || full.S != opening.S {
		t.Fatalf("real unsealed seed prefix: installed=%d free=%d capacity=%d opening=%+v full=%+v err=%v",
			installed, free, capacity, opening, full, err)
	}
	var heldTickets []rootpublication.AcceptedTicket
	releaseHolds := func() {
		t.Helper()
		for _, ticket := range heldTickets {
			if err := ticket.CancelAccepted(); err != nil {
				t.Errorf("release fixture-held reservation: %v", err)
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
			t.Fatalf("reserve fixture slot %d: %v", i, err)
		}
		heldTickets = append(heldTickets, ticket)
	}
	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("no independent old cut")
	}
	t.Cleanup(func() {
		if held != nil && !blocked.Load() {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	first, follower := stage("first", 2), stage("follower", 3)
	before := database.Stats()
	next := database.backend.CommandWALNextLSN()
	wantSeal := rootpublication.SealedPrefix{LogicalSequence: full.V, CanonicalLSN: full.C}
	rootBefore, live := database.backend.StateToken()
	if !live || rootBefore.AppliedCommandLSN != opening.S.CanonicalLSN || statMapUint64(t, before, autoCount) != 0 {
		t.Fatalf("checkpoint advanced while original worker was parked: root=%+v opening=%+v auto=%s", rootBefore, opening, before[autoCount])
	}

	type pressureProof struct {
		free      int
		frontiers rootpublication.LogicalFrontiers
		err       error
	}
	pressure := make(chan pressureProof, 2)
	var denied atomic.Int32
	dataReached := make(chan struct{})
	var dataOnce atomic.Bool
	resumeData := make(chan struct{})
	releaseData := sync.OnceFunc(func() { close(resumeData) })
	indexRoot := filepath.Clean(database.backend.Dir())
	// Cache-owned admission events use the main database's WAL directory,
	// while the original DATA sync uses the index's database root.
	cacheRoot := filepath.Join(indexRoot, "wal")
	restore := sync.OnceFunc(durabilitycut.Install(func(e durabilitycut.Event) error {
		switch {
		case e.Point == durabilitycut.BeforeCOWAcceptedPairPressureWait && e.Resource == durabilitycut.ResourceAuxiliary &&
			filepath.Clean(e.Root) == cacheRoot && denied.Add(1) <= 2:
			n, err := rawTxnGroupPressureFreeSlots(prefix, bound)
			pressure <- pressureProof{n, prefix.LogicalFrontiers(), err}
		case e.Point == durabilitycut.BeforeIndexDataSync && e.Resource == durabilitycut.ResourceIndex &&
			filepath.Clean(e.Root) == indexRoot && dataOnce.CompareAndSwap(false, true):
			close(dataReached)
			<-resumeData
		}
		return nil
	}))
	t.Cleanup(restore)
	awaitPressure := func(label string, done <-chan error) {
		t.Helper()
		select {
		case proof := <-pressure:
			if proof.err != nil || proof.free != 1 || proof.frontiers != full {
				t.Fatalf("%s denied outside exact one-slot pre-reclaim prefix: %+v want %+v", label, proof, full)
			}
		case err := <-done:
			t.Fatalf("%s finished before real pair denial: %v", label, err)
		case <-time.After(10 * time.Second):
			blocked.Store(true)
			t.Fatalf("%s did not reach initial pair denial", label)
		}
	}
	var writers sync.WaitGroup
	defer func() {
		// A failed witness must not race fixture-handle cleanup against
		// either public writer. Always unpark production before joining it.
		releaseData()
		releaseSample()
		joined := make(chan struct{})
		go func() { writers.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(10 * time.Second):
			blocked.Store(true)
			t.Error("public writers did not unwind after releasing fixture latches")
		}
	}()
	start := func(tx *ConditionalTxn) <-chan error {
		done := make(chan error, 1)
		writers.Go(func() { done <- tx.Commit() })
		return done
	}
	firstDone := start(first)
	awaitPressure("first", firstDone)
	select {
	case <-dataReached:
	case err := <-firstDone:
		t.Fatalf("first finished before original foreground data sync: %v", err)
	case <-time.After(10 * time.Second):
		blocked.Store(true)
		t.Fatal("original foreground data sync not reached")
	}
	followerDone := start(follower)
	awaitPressure("follower", followerDone)
	releaseData()
	for i, done := range []<-chan error{firstDone, followerDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("durable public Commit %d failed: %v", i, err)
			}
		case <-time.After(10 * time.Second):
			blocked.Store(true)
			t.Fatalf("durable public Commit %d did not return", i)
		}
	}
	restore()
	if denied.Load() != 2 {
		t.Fatalf("got %d initial real pair denials, want 2", denied.Load())
	}
	rawTxnGroupPressureRequireIdle(t, database)

	foreground := database.Stats()
	accepted, nextAfter := prefix.LogicalFrontiers(), database.backend.CommandWALNextLSN()
	physical, live := database.backend.StateToken()
	remainingBytes := database.publicCommandWALAutoCheckpointBytes()
	if remainingBytes < sizeLimit || statMapUint64(t, foreground, autoCount) != 0 ||
		!live || physical.CommitSeq <= rootBefore.CommitSeq || physical.AppliedCommandLSN < wantSeal.CanonicalLSN {
		t.Fatalf("need real eligible debt after foreground and no prior auto pass: bytes %d→%d root %+v→%+v auto=%s",
			initialBytes, remainingBytes, rootBefore, physical, foreground[autoCount])
	}
	if accepted.V != full.V+2 || accepted.W != accepted.C || accepted.C+1 != nextAfter ||
		nextAfter < next+2 || nextAfter > next+4 || accepted.S.LogicalSequence < wantSeal.LogicalSequence ||
		accepted.S.CanonicalLSN < wantSeal.CanonicalLSN {
		t.Fatalf("wrong before/after durable ACK or foreground seal: %+v→%+v next=%d→%d", full, accepted, next, nextAfter)
	}
	requirePublicStatDelta(t, before, foreground, rawAppends, 2)
	requirePublicStatDelta(t, before, foreground, totalAppends, nextAfter-next)
	requirePublicStatDelta(t, before, foreground, durableCommits, 2)

	// Record actual nonempty canonical file names from the acknowledged
	// suffix before letting the original worker run. Empty reserve files do
	// not count as real WAL debt. The scan closes each file before release.
	preCleanupBackend := database.backend.Stats()
	paths, err := filepath.Glob(filepath.Join(cacheRoot, "commit-l*.log"))
	if err != nil {
		t.Fatal(err)
	}
	var acknowledgedPaths []string
	for _, path := range paths {
		frames, err := commitlog.ScanCommandFramesV2(path, commitlog.Options{})
		if err != nil {
			t.Fatalf("pre-cleanup physical WAL %s: %v", path, err)
		}
		for _, frame := range frames {
			if frame.LSN > wantSeal.CanonicalLSN && frame.LSN <= accepted.C {
				acknowledgedPaths = append(acknowledgedPaths, path)
				break
			}
		}
	}
	if len(acknowledgedPaths) == 0 {
		t.Fatal("size worker was not given a real nonempty acknowledged WAL segment")
	}

	// Resume the original loop, not a forced, explicit, or teardown checkpoint.
	// Stats are stored separately, so wait for the detailed completed size
	// receipt instead of inferring completion from its count alone.
	releaseSample()
	var background map[string]string
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		background = database.Stats()
		if statMapUint64(t, background, autoCount) > 0 && background[autoReason] == "size" &&
			statMapUint64(t, background, autoBefore) >= sizeLimit && statMapUint64(t, background, autoTrimmed) > 0 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("no completed real size/trim observation before close: bytes=%d now=%d count=%s reason=%q before=%s after=%s trim=%s cleanup=%s removed=%s",
				remainingBytes, database.publicCommandWALAutoCheckpointBytes(), background[autoCount], background[autoReason],
				background[autoBefore], background[autoAfter], background[autoTrimmed], background[cleanupDone], background[removedBytes])
		case <-tick.C:
		}
	}
	// Re-read the combined cache map after its final trim receipt. Some
	// earlier fields in the first map may predate that receipt.
	background = database.Stats()
	requirePublicStatDelta(t, foreground, background, autoCount, 1)
	if statMapUint64(t, background, cleanupDone) <= statMapUint64(t, foreground, cleanupDone) {
		t.Fatalf("original deferred cleanup did not complete: %s→%s", foreground[cleanupDone], background[cleanupDone])
	}
	// Combined Stats reads the backend before the cache. Acquire fresh
	// backend stats only AFTER observing the worker's completed receipt, so
	// a mixed pre-unlink/post-completion sample cannot satisfy this proof.
	postCleanupBackend := database.backend.Stats()
	for _, key := range []string{removedBytes, removedSegments, namespaceSync} {
		if statMapUint64(t, postCleanupBackend, key) <= statMapUint64(t, preCleanupBackend, key) {
			t.Fatalf("original size worker did not durably remove WAL %s: %s→%s before=%s after=%s",
				key, preCleanupBackend[key], postCleanupBackend[key], background[autoBefore], background[autoAfter])
		}
	}
	if postCleanupBackend[namespacePending] != "false" {
		t.Fatalf("original size worker left unproven WAL namespace deletion: %s", postCleanupBackend[namespacePending])
	}
	removedAcknowledged := 0
	for _, path := range acknowledgedPaths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			removedAcknowledged++
		} else if err != nil {
			t.Fatalf("post-cleanup WAL stat %s: %v", path, err)
		}
	}
	if removedAcknowledged == 0 {
		t.Fatalf("no preexisting named acknowledged WAL segment was deleted: %v", acknowledgedPaths)
	}
	if got := statMapUint64(t, background, autoBefore); got != uint64(remainingBytes) {
		t.Fatalf("worker did not sample exact real remaining debt: got=%d want=%d", got, remainingBytes)
	}
	relieved := database.publicCommandWALAutoCheckpointBytes()
	if relieved < 0 || relieved >= sizeLimit || statMapUint64(t, background, autoAfter) != uint64(relieved) {
		t.Fatalf("still eligible WAL after size cleanup: sampled=%s real=%d", background[autoAfter], relieved)
	}
	if final := prefix.LogicalFrontiers(); final.V != accepted.V || final.C != accepted.C || final.W != accepted.W ||
		final.S.LogicalSequence != accepted.V || final.S.CanonicalLSN != accepted.C || database.backend.CommandWALNextLSN() != nextAfter {
		t.Fatalf("size worker changed mutation/ACK counts or failed to seal the original suffix: %+v→%+v next=%d", accepted, final, database.backend.CommandWALNextLSN())
	}
	for _, key := range []string{rawAppends, totalAppends, durableCommits} {
		requirePublicStatDelta(t, foreground, background, key, 0)
	}

	for key, value := range expected {
		old := strings.HasPrefix(key, "seed-size/")
		if old {
			cowPublicContractSnapshotValue(t, held, key, value, true)
		} else {
			cowPublicContractSnapshotValue(t, held, key, nil, false)
		}
	}
	for _, stem := range []string{"seed-one", "seed-two"} {
		for _, suffix := range []string{"a", "b"} {
			key := stem + "/" + suffix
			cowPublicContractSnapshotValue(t, held, key, []byte(stem), true)
			expected[key] = []byte(stem)
		}
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	releaseHolds()
	cache := database.cached
	if err := cowPublicContractComplete(t, blocked, "checked database close", database.Close); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, cache)
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	for key, value := range expected {
		if got, err := reopened.Get([]byte(key)); err != nil || !bytes.Equal(got, value) {
			t.Fatalf("reopen %q len=%d want=%d: %v", key, len(got), len(value), err)
		}
	}
	if lsn := publicStatUint64(t, reopened, "treedb.applied_command_lsn"); lsn != accepted.C {
		t.Fatalf("fresh reopen applied LSN=%d want %d", lsn, accepted.C)
	}
	cache = reopened.cached
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	requirePublicCOWFinalZero(t, cache)

	if n := statCheckpointRuns(t, background) - statCheckpointRuns(t, foreground); n != 1 {
		t.Fatalf("real size worker must keep its own full boundary, got %d", n)
	}
	t.Logf("real parked size debt=%d, post-foreground=%d, after automatic cleanup=%d; deleted %d named acknowledged WAL segments, two denials, one size checkpoint",
		initialBytes, remainingBytes, relieved, removedAcknowledged)
	// Same-size stock must get this far with its original second foreground
	// checkpoint. Only that extra full attempt is the intended RED difference.
	if n := statCheckpointRuns(t, foreground) - statCheckpointRuns(t, before); n != 1 || accepted.S != wantSeal {
		t.Fatalf("real size worker and checked reopen passed, but foreground checkpoints=%d seal=%+v want one, seal %+v", n, accepted.S, wantSeal)
	}
}
