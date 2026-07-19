package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	dbpkg "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestPublicCommandWALGroupCommitSharesOneSyncAcrossConcurrentWaiters(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	before := db.Stats()
	const waiters = 4
	db.commandWALGroupCommit.delay = time.Second
	db.commandWALGroupCommit.maxCommits = waiters
	db.commandWALGroupCommit.maxBytes = 1 << 30
	var barrierCalls atomic.Int32
	var barrierGroupSize atomic.Int32
	db.commandWALGroupCommit.testBeforeSync = func(groupSize int) {
		barrierCalls.Add(1)
		barrierGroupSize.Store(int32(groupSize))
	}
	start := make(chan struct{})
	ready := make(chan struct{}, waiters)
	errs := make(chan error, waiters)
	var workers sync.WaitGroup
	for i := 0; i < waiters; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			b := db.NewBatch()
			defer b.Close()
			key := []byte(fmt.Sprintf("group/%d", i))
			if err := b.Set(key, []byte("value")); err != nil {
				errs <- err
				return
			}
			ready <- struct{}{}
			<-start
			errs <- b.WriteSync()
		}(i)
	}
	for i := 0; i < waiters; i++ {
		<-ready
	}
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent WriteSync: %v", err)
		}
	}

	after := db.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.groups_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.commits_total", waiters)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.leaders_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.followers_total", waiters-1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 1)
	if got := barrierCalls.Load(); got != 1 {
		t.Fatalf("barrier hook calls=%d, want 1", got)
	}
	if got := barrierGroupSize.Load(); got != waiters {
		t.Fatalf("barrier hook group size=%d, want %d", got, waiters)
	}
	if got := publicStatUint64(t, db, "treedb.command_wal.group_commit.group_size_max"); got != waiters {
		t.Fatalf("group size max=%d, want %d", got, waiters)
	}
	for i := 0; i < waiters; i++ {
		key := []byte(fmt.Sprintf("group/%d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(got) != "value" {
			t.Fatalf("Get(%q)=%q, want value", key, got)
		}
	}
}

func TestPublicCommandWALGroupCommitDoesNotReuseStaleThresholdWake(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.commandWALGroupCommit.delay = 20 * time.Millisecond
	db.commandWALGroupCommit.maxCommits = 1
	db.commandWALGroupCommit.maxBytes = 1 << 30
	before := db.Stats()
	if err := db.SetSync([]byte("threshold"), []byte("one")); err != nil {
		t.Fatalf("threshold SetSync: %v", err)
	}
	afterFirst := db.Stats()
	requirePublicStatDelta(t, before, afterFirst, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, afterFirst, "treedb.command_wal.group_commit.groups_total", 1)
	requirePublicStatDelta(t, before, afterFirst, "treedb.command_wal.group_commit.trigger.commit_limit_total", 1)

	db.commandWALGroupCommit.maxCommits = 64
	secondStart := time.Now()
	if err := db.SetSync([]byte("timeout"), []byte("two")); err != nil {
		t.Fatalf("timeout SetSync: %v", err)
	}
	if elapsed := time.Since(secondStart); elapsed < db.commandWALGroupCommit.delay/2 {
		t.Fatalf("second SetSync returned after %s, want timeout wait near %s; stale threshold wake likely reused", elapsed, db.commandWALGroupCommit.delay)
	}
	afterSecond := db.Stats()
	requirePublicStatDelta(t, afterFirst, afterSecond, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, afterFirst, afterSecond, "treedb.command_wal.group_commit.groups_total", 1)
	requirePublicStatDelta(t, afterFirst, afterSecond, "treedb.command_wal.group_commit.trigger.timeout_total", 1)

	time.Sleep(2 * db.commandWALGroupCommit.delay)
	afterIdle := db.Stats()
	requirePublicStatDelta(t, afterSecond, afterIdle, "treedb.command_wal.file_sync.calls_total", 0)
	requirePublicStatDelta(t, afterSecond, afterIdle, "treedb.command_wal.group_commit.groups_total", 0)
}

func TestPublicCommandWALGroupCommitSameKeyRevisionWinsAfterReversePublication(t *testing.T) {
	dir := t.TempDir()
	opts := commandWALDurabilityProofOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	const waiters = 2
	db.commandWALGroupCommit.delay = time.Second
	db.commandWALGroupCommit.maxCommits = waiters
	db.commandWALGroupCommit.maxBytes = 1 << 30
	firstTurn := make(chan struct{})
	secondTurn := make(chan struct{})
	releaseFirst := make(chan struct{})
	db.commandWALGroupCommit.testAfterWait = func(ticket uint64) {
		if ticket == 1 {
			close(firstTurn)
			<-releaseFirst
			return
		}
		if ticket == 2 {
			close(secondTurn)
		}
	}

	start := make(chan struct{})
	ready := make(chan struct{}, waiters)
	errs := make(chan error, waiters)
	for i, value := range []string{"one", "two"} {
		go func(i int, value string) {
			b := db.NewBatch()
			defer b.Close()
			if err := b.Set([]byte("same-key"), []byte(value)); err != nil {
				errs <- err
				return
			}
			ready <- struct{}{}
			<-start
			errs <- b.WriteSync()
		}(i, value)
	}
	for i := 0; i < waiters; i++ {
		<-ready
	}
	close(start)
	select {
	case <-firstTurn:
	case <-time.After(time.Second):
		t.Fatal("first publication ticket did not become ready")
	}
	select {
	case <-secondTurn:
		t.Fatal("second publication ticket became ready before first published")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)
	for i := 0; i < waiters; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("same-key WriteSync: %v", err)
		}
	}
	select {
	case <-secondTurn:
	case <-time.After(time.Second):
		t.Fatal("second publication ticket did not advance after first published")
	}

	frames := scanPublicCommandWALV2(t, dir)
	if len(frames) != 3 {
		t.Fatalf("frames=%d, want two mutations plus one barrier: %+v", len(frames), frames)
	}
	type versionedValue struct {
		value    string
		revision uint64
	}
	mutations := make([]versionedValue, 0, 2)
	for _, env := range frames {
		if env.Kind == commitlog.CommandKindDurablePrefixBarrier {
			continue
		}
		err := commitlog.ScanRawKVBatchPayloadWithRevision(env.Payload, func(op commitlog.RawKVOp, key, value []byte, revision uint64) error {
			if op != commitlog.RawKVOpSet || string(key) != "same-key" {
				return fmt.Errorf("unexpected grouped mutation op=%v key=%q", op, key)
			}
			mutations = append(mutations, versionedValue{value: string(value), revision: revision})
			return nil
		})
		if err != nil {
			t.Fatalf("decode grouped mutation LSN %d: %v", env.LSN, err)
		}
	}
	if len(mutations) != 2 {
		t.Fatalf("mutations=%+v, want two", mutations)
	}
	if mutations[0].revision >= mutations[1].revision {
		t.Fatalf("LSN-ordered revisions=%d,%d, want strictly increasing", mutations[0].revision, mutations[1].revision)
	}
	got, revision, err := db.GetVersioned([]byte("same-key"))
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if string(got) != mutations[1].value || uint64(revision) != mutations[1].revision {
		t.Fatalf("runtime winner=(%q,%d), want later LSN winner=(%q,%d)", got, revision, mutations[1].value, mutations[1].revision)
	}
	wantValue := mutations[1].value
	wantRevision := mutations[1].revision
	db.commandWALGroupCommit.testAfterWait = nil
	if err := db.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}
	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen command WAL: %v", err)
	}
	got, revision, err = db.GetVersioned([]byte("same-key"))
	if err != nil {
		t.Fatalf("reopen GetVersioned: %v", err)
	}
	if string(got) != wantValue || uint64(revision) != wantRevision {
		t.Fatalf("reopen winner=(%q,%d), want later LSN winner=(%q,%d)", got, revision, wantValue, wantRevision)
	}
}

func TestPublicCommandWALGroupCommitAcknowledgedPrefixRecoversFromCrashImage(t *testing.T) {
	sourceDir := t.TempDir()
	opts := commandWALDurabilityProofOptions(sourceDir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	const waiters = 4
	db.commandWALGroupCommit.delay = time.Second
	db.commandWALGroupCommit.maxCommits = waiters
	db.commandWALGroupCommit.maxBytes = 1 << 30
	if errs := runPublicCommandWALGroupWaiters(db, waiters); len(errs) != waiters {
		t.Fatalf("grouped result count=%d, want %d", len(errs), waiters)
	} else {
		for i, err := range errs {
			if err != nil {
				t.Fatalf("grouped waiter %d: %v", i, err)
			}
		}
	}

	type acknowledgedValue struct {
		value    string
		revision EntryRevision
	}
	acknowledged := make(map[string]acknowledgedValue, waiters)
	for i := 0; i < waiters; i++ {
		key := fmt.Sprintf("failure-group/%d", i)
		value, revision, err := db.GetVersioned([]byte(key))
		if err != nil {
			t.Fatalf("GetVersioned(%q): %v", key, err)
		}
		if string(value) != "value" || revision == LegacyEntryRevision {
			t.Fatalf("acknowledged value %q=(%q,%d), want value with native revision", key, value, revision)
		}
		acknowledged[key] = acknowledgedValue{value: string(value), revision: revision}
	}
	frames := scanPublicCommandWALV2(t, sourceDir)
	if len(frames) != waiters+1 || frames[len(frames)-1].Kind != commitlog.CommandKindDurablePrefixBarrier {
		t.Fatalf("acknowledged group frames=%+v, want %d mutations plus terminal barrier", frames, waiters)
	}
	barrierLSN := frames[len(frames)-1].LSN

	// Capture the durable bytes before Close can checkpoint the public
	// memtable. Reopening this independent image therefore exercises command-WAL
	// recovery of the acknowledged group instead of the clean-close snapshot.
	crashDir := filepath.Join(t.TempDir(), "crash-image")
	if err := copyPublicCommandWALCrashImage(sourceDir, crashDir); err != nil {
		t.Fatalf("capture crash image: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close source after crash capture: %v", err)
	}
	closed = true

	reopenOpts := commandWALDurabilityProofOptions(crashDir)
	recovered, err := Open(reopenOpts)
	if err != nil {
		t.Fatalf("reopen crash image: %v", err)
	}
	defer func() { _ = recovered.Close() }()
	for key, want := range acknowledged {
		value, revision, err := recovered.GetVersioned([]byte(key))
		if err != nil {
			t.Fatalf("recovered GetVersioned(%q): %v", key, err)
		}
		if string(value) != want.value || revision != want.revision {
			t.Fatalf("recovered %q=(%q,%d), want acknowledged (%q,%d)", key, value, revision, want.value, want.revision)
		}
	}
	if got := recovered.backend.State().AppliedCommandLSN; got < barrierLSN {
		t.Fatalf("recovered AppliedCommandLSN=%d, want at least group barrier LSN %d", got, barrierLSN)
	}
}

func TestPublicCommandWALGroupCommitMixedRelaxedDurablePreservesLSNPublicationOrder(t *testing.T) {
	dir := t.TempDir()
	opts := relaxedCommandWALDurablePrefixOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.commandWALGroupCommit.delay = 50 * time.Millisecond
	db.commandWALGroupCommit.maxCommits = 64
	db.commandWALGroupCommit.maxBytes = 1 << 30
	firstRegistered := make(chan struct{})
	firstTurn := make(chan struct{})
	secondTurn := make(chan struct{})
	releaseFirst := make(chan struct{})
	db.commandWALGroupCommit.testAfterRegister = func(ticket uint64, durable bool) {
		if ticket == 1 && durable {
			close(firstRegistered)
		}
	}
	db.commandWALGroupCommit.testAfterWait = func(ticket uint64) {
		switch ticket {
		case 1:
			close(firstTurn)
			<-releaseFirst
		case 2:
			close(secondTurn)
		}
	}

	writeBatch := func(value string, durable bool) error {
		b := db.NewBatch()
		defer b.Close()
		if err := b.Set([]byte("mixed-key"), []byte(value)); err != nil {
			return err
		}
		if durable {
			return b.WriteSync()
		}
		return b.Write()
	}
	durableDone := make(chan error, 1)
	go func() { durableDone <- writeBatch("durable-first", true) }()
	select {
	case <-firstRegistered:
	case <-time.After(time.Second):
		t.Fatal("durable ticket was not registered")
	}
	relaxedDone := make(chan error, 1)
	go func() { relaxedDone <- writeBatch("relaxed-second", false) }()

	select {
	case <-firstTurn:
	case <-time.After(time.Second):
		t.Fatal("first mixed publication ticket did not become ready")
	}
	select {
	case <-secondTurn:
		t.Fatal("relaxed ticket overtook earlier durable publication")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-durableDone; err != nil {
		t.Fatalf("durable WriteSync: %v", err)
	}
	if err := <-relaxedDone; err != nil {
		t.Fatalf("relaxed Write: %v", err)
	}

	frames := scanPublicCommandWALV2(t, dir)
	if len(frames) != 3 {
		t.Fatalf("frames=%d, want durable mutation, relaxed mutation, barrier: %+v", len(frames), frames)
	}
	if frames[0].DurabilityClass != commitlog.CommandDurabilityRelaxed ||
		frames[1].DurabilityClass != commitlog.CommandDurabilityRelaxed ||
		frames[2].Kind != commitlog.CommandKindDurablePrefixBarrier {
		t.Fatalf("unexpected mixed durable-prefix frames: %+v", frames)
	}
	got, revision, err := db.GetVersioned([]byte("mixed-key"))
	if err != nil {
		t.Fatalf("GetVersioned: %v", err)
	}
	if string(got) != "relaxed-second" || revision == LegacyEntryRevision {
		t.Fatalf("runtime winner=(%q,%d), want relaxed-second with native revision", got, revision)
	}
}

func TestPublicCommandWALGroupCommitMeasuresRelaxedWaitBehindDurablePrefix(t *testing.T) {
	opts := relaxedCommandWALDurablePrefixOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.commandWALGroupCommit.delay = time.Hour
	db.commandWALGroupCommit.maxCommits = 64
	db.commandWALGroupCommit.maxBytes = 1 << 30
	durableRegistered := make(chan struct{})
	relaxedRegistered := make(chan struct{})
	db.commandWALGroupCommit.testAfterRegister = func(ticket uint64, durable bool) {
		switch ticket {
		case 1:
			if !durable {
				t.Errorf("ticket 1 durable=%t, want true", durable)
			}
			close(durableRegistered)
		case 2:
			if durable {
				t.Errorf("ticket 2 durable=%t, want false", durable)
			}
			close(relaxedRegistered)
		}
	}
	durablePayload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSet, Key: []byte("measured-durable"), Value: []byte("one"), Revision: 1,
	}})
	if err != nil {
		t.Fatalf("encode durable payload: %v", err)
	}
	relaxedPayload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op: commitlog.RawKVOpSet, Key: []byte("measured-relaxed"), Value: []byte("two"), Revision: 2,
	}})
	if err != nil {
		t.Fatalf("encode relaxed payload: %v", err)
	}
	type measuredResult struct {
		timing dbpkg.CommandWALRequestTiming
		ticket uint64
		err    error
	}
	appendMeasured := func(payload []byte, sync bool, done chan<- measuredResult) {
		var ticket uint64
		timing, err := db.appendPublicRawKVCommandPayloadMeasured(payload, sync, &ticket)
		if err == nil {
			db.finishPublicCommandWALGroupPublication(ticket, nil)
		}
		done <- measuredResult{timing: timing, ticket: ticket, err: err}
	}
	durableDone := make(chan measuredResult, 1)
	go appendMeasured(durablePayload, true, durableDone)
	select {
	case <-durableRegistered:
	case <-time.After(time.Second):
		t.Fatal("durable measured request did not register")
	}
	beforeWaitNs := publicStatUint64(t, db, "treedb.command_wal.group_commit.wait_ns_total")
	relaxedDone := make(chan measuredResult, 1)
	go appendMeasured(relaxedPayload, false, relaxedDone)
	select {
	case <-relaxedRegistered:
	case <-time.After(time.Second):
		t.Fatal("relaxed measured request did not register behind durable prefix")
	}
	select {
	case result := <-relaxedDone:
		t.Fatalf("relaxed request returned before durable prefix completed: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}

	db.commandWALGroupCommit.mu.Lock()
	db.commandWALGroupCommit.triggerLocked(&db.commandWALGroupCommit.triggerExplicit)
	db.commandWALGroupCommit.mu.Unlock()
	durableResult := <-durableDone
	relaxedResult := <-relaxedDone
	if durableResult.err != nil || relaxedResult.err != nil {
		t.Fatalf("measured results durable=%+v relaxed=%+v", durableResult, relaxedResult)
	}
	if !durableResult.timing.GroupCommitWaitObserved || !durableResult.timing.Sync {
		t.Fatalf("durable timing=%+v, want observed durable group wait", durableResult.timing)
	}
	if !relaxedResult.timing.GroupCommitWaitObserved || relaxedResult.timing.Sync {
		t.Fatalf("relaxed timing=%+v, want observed non-sync wait", relaxedResult.timing)
	}
	if relaxedResult.timing.GroupCommitWait < 20*time.Millisecond {
		t.Fatalf("relaxed group wait=%s, want held behind durable prefix", relaxedResult.timing.GroupCommitWait)
	}
	afterWaitNs := publicStatUint64(t, db, "treedb.command_wal.group_commit.wait_ns_total")
	if afterWaitNs <= beforeWaitNs {
		t.Fatalf("group wait ns total did not advance: before=%d after=%d", beforeWaitNs, afterWaitNs)
	}
}

func TestPublicCommandWALGroupCommitPendingCollectionBarrierMakesProgress(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	manager := collections.NewCollectionManager(db.backend)
	meta := collections.CollectionMeta{
		Name:    "group-commit-docs",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
	}
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	appliedBeforeInsert := db.backend.State().AppliedCommandLSN
	if _, err := collection.Insert([]byte("doc-1"), []byte(`{"name":"pending"}`)); err != nil {
		t.Fatalf("collection Insert: %v", err)
	}
	if got := db.backend.State().AppliedCommandLSN; got != appliedBeforeInsert {
		t.Fatalf("collection insert published eagerly: AppliedCommandLSN=%d, want pending frontier %d", got, appliedBeforeInsert)
	}

	db.commandWALGroupCommit.delay = time.Second
	db.commandWALGroupCommit.maxCommits = 2
	db.commandWALGroupCommit.maxBytes = 1 << 30
	done := make(chan []error, 1)
	go func() {
		done <- runPublicCommandWALGroupWaiters(db, 2)
	}()
	select {
	case errs := <-done:
		for i, err := range errs {
			if err != nil {
				t.Fatalf("grouped waiter %d: %v", i, err)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("grouped public writes deadlocked behind pending collection raw-publish barrier")
	}
	if got := db.backend.State().AppliedCommandLSN; got <= appliedBeforeInsert {
		t.Fatalf("pending collection barrier did not publish: AppliedCommandLSN=%d, want > %d", got, appliedBeforeInsert)
	}
}

func TestPublicCommandWALGroupCommitCheckpointAndCloseWaitForFormingGroup(t *testing.T) {
	for _, maintenance := range []string{"checkpoint", "close"} {
		t.Run(maintenance, func(t *testing.T) {
			opts := commandWALDurabilityProofOptions(t.TempDir())
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open command WAL: %v", err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = db.Close()
				}
			}()

			db.commandWALGroupCommit.delay = time.Second
			db.commandWALGroupCommit.maxCommits = 2
			db.commandWALGroupCommit.maxBytes = 1 << 30
			barrierReached := make(chan struct{})
			releaseBarrier := make(chan struct{})
			var barrierOnce sync.Once
			db.commandWALGroupCommit.testBeforeSync = func(groupSize int) {
				if groupSize != 2 {
					t.Errorf("forming group size=%d, want 2", groupSize)
				}
				barrierOnce.Do(func() { close(barrierReached) })
				<-releaseBarrier
			}
			writesDone := make(chan []error, 1)
			go func() { writesDone <- runPublicCommandWALGroupWaiters(db, 2) }()
			select {
			case <-barrierReached:
			case <-time.After(2 * time.Second):
				t.Fatal("forming group did not reach barrier hook")
			}

			maintenanceStarted := make(chan struct{})
			maintenanceDone := make(chan error, 1)
			go func() {
				close(maintenanceStarted)
				if maintenance == "checkpoint" {
					maintenanceDone <- db.Checkpoint()
					return
				}
				maintenanceDone <- db.Close()
			}()
			<-maintenanceStarted
			select {
			case err := <-maintenanceDone:
				t.Fatalf("%s completed before forming group was released: %v", maintenance, err)
			case <-time.After(20 * time.Millisecond):
			}
			close(releaseBarrier)

			select {
			case errs := <-writesDone:
				for i, err := range errs {
					if err != nil {
						t.Fatalf("grouped waiter %d: %v", i, err)
					}
				}
			case <-time.After(3 * time.Second):
				t.Fatal("grouped writes did not finish after barrier release")
			}
			select {
			case err := <-maintenanceDone:
				if err != nil {
					t.Fatalf("%s: %v", maintenance, err)
				}
				closed = maintenance == "close"
			case <-time.After(3 * time.Second):
				t.Fatalf("%s did not finish after group completion", maintenance)
			}
		})
	}
}

func TestPublicCommandWALGroupCommitCloseDoesNotTearDownArmedLeader(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()

	db.commandWALGroupCommit.delay = time.Hour
	db.commandWALGroupCommit.maxCommits = 64
	db.commandWALGroupCommit.maxBytes = 1 << 30
	leaderArmed := make(chan struct{})
	releaseLeader := make(chan struct{})
	db.commandWALGroupCommit.testBeforeLeaderWait = func() {
		close(leaderArmed)
		<-releaseLeader
	}

	// Use the same forced group participant that checkpoint/Close uses without
	// taking a public-operation read lock. This lets Close reach coordinator
	// lifecycle handling while the leader retains the armed timer and wake
	// channel immediately before select.
	forceDone := make(chan error, 1)
	go func() { forceDone <- db.forcePublicCommandWALGroupCommit() }()
	select {
	case <-leaderArmed:
	case <-time.After(time.Second):
		t.Fatal("group leader did not arm timer before select")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while armed leader was held: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseLeader)

	select {
	case err := <-forceDone:
		if err != nil {
			t.Fatalf("forced group participant: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("armed leader remained stranded after Close entered")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		closed = true
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not finish after armed leader progressed")
	}

	db.commandWALGroupCommit.mu.Lock()
	timer := db.commandWALGroupCommit.timer
	wake := db.commandWALGroupCommit.wake
	active := db.commandWALGroupCommit.leaderActive
	db.commandWALGroupCommit.mu.Unlock()
	if timer != nil || wake != nil || active {
		t.Fatalf("group resources after Close timer=%v wake=%v leader_active=%t, want released", timer, wake, active)
	}
}

func TestPublicCommandWALGroupCommitCoversRotationAndExternalValueDependencies(t *testing.T) {
	dir := t.TempDir()
	opts := relaxedCommandWALDurablePrefixOptions(dir)
	opts.CommandWALSegmentTargetBytes = 1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.commandWALGroupCommit.delay = time.Second
	db.commandWALGroupCommit.maxCommits = 2
	db.commandWALGroupCommit.maxBytes = 1 << 30
	var eventsMu sync.Mutex
	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
		return nil
	})
	defer restore()

	start := make(chan struct{})
	ready := make(chan struct{}, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			b := db.NewBatch()
			defer b.Close()
			if err := b.Set([]byte(fmt.Sprintf("external/%d", i)), bytes.Repeat([]byte{byte('a' + i)}, 4096)); err != nil {
				errs <- err
				return
			}
			ready <- struct{}{}
			<-start
			errs <- b.WriteSync()
		}(i)
	}
	<-ready
	<-ready
	before := db.Stats()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("external grouped WriteSync: %v", err)
		}
	}
	after := db.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.groups_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.commits_total", 2)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 1)
	if got := after["treedb.command_wal.dependency_debt.entries"]; got != "0" {
		t.Fatalf("dependency debt entries after shared group=%q, want 0", got)
	}

	eventsMu.Lock()
	defer eventsMu.Unlock()
	var sawExternalSync, sawRotationNamespaceSync, sawBarrier bool
	for _, event := range events {
		switch {
		case event.Resource == durabilitycut.ResourceAuxiliary && event.Point == durabilitycut.AfterDependencyFileSync:
			sawExternalSync = true
		case event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterNewFileDirectorySync:
			sawRotationNamespaceSync = true
		case event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyAppend && event.LSN >= 3:
			sawBarrier = true
		}
	}
	if !sawExternalSync || !sawRotationNamespaceSync || !sawBarrier {
		t.Fatalf("shared group dependency proof external_sync=%t rotation_namespace_sync=%t barrier=%t events=%#v",
			sawExternalSync, sawRotationNamespaceSync, sawBarrier, events)
	}
}

func TestPublicCommandWALGroupCommitRelaxedOnlyDoesNotStartTimerOrSync(t *testing.T) {
	opts := relaxedCommandWALDurablePrefixOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open relaxed command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("warm"), []byte("value")); err != nil {
		t.Fatalf("warm Set: %v", err)
	}
	before := db.Stats()
	allocs := testing.AllocsPerRun(50, func() {
		if err := db.Set([]byte("relaxed-alloc"), []byte("value")); err != nil {
			panic(err)
		}
	})
	after := db.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 0)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 0)
	db.commandWALGroupCommit.mu.Lock()
	timer := db.commandWALGroupCommit.timer
	db.commandWALGroupCommit.mu.Unlock()
	if timer != nil {
		t.Fatal("relaxed-only public writes initialized the group timer")
	}
	if allocs > 18 {
		t.Fatalf("relaxed Set allocations/op=%.1f, want <=18 historical 16-17 baseline plus at most one", allocs)
	}
}

func TestPublicCommandWALGroupCommitRepeatedGroupsReleaseTimerOnClose(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	db.commandWALGroupCommit.delay = time.Millisecond
	db.commandWALGroupCommit.maxCommits = 1
	for i := 0; i < 20; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("repeated/%d", i)), []byte("value")); err != nil {
			_ = db.Close()
			t.Fatalf("SetSync %d: %v", i, err)
		}
	}
	if db.commandWALGroupCommit.timer == nil {
		_ = db.Close()
		t.Fatal("durable groups did not initialize shared timer")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db.commandWALGroupCommit.mu.Lock()
	timer := db.commandWALGroupCommit.timer
	wake := db.commandWALGroupCommit.wake
	active := db.commandWALGroupCommit.leaderActive
	db.commandWALGroupCommit.mu.Unlock()
	if timer != nil || wake != nil || active {
		t.Fatalf("group resources after Close timer=%v wake=%v leader_active=%t, want released", timer, wake, active)
	}
}

func TestPublicCommandWALGroupCommitLowTrafficTimeoutMakesProgress(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.commandWALGroupCommit.delay = 2 * time.Millisecond
	db.commandWALGroupCommit.maxCommits = 64
	db.commandWALGroupCommit.maxBytes = 1 << 30
	before := db.Stats()

	done := make(chan error, 1)
	go func() {
		done <- db.SetSync([]byte("low-traffic"), []byte("value"))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SetSync: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("low-traffic group commit did not make bounded progress")
	}

	after := db.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.groups_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.commits_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.trigger.timeout_total", 1)
}

func TestPublicCommandWALGroupCommitDependencyFailurePoisonsAllWaiters(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	opts.CommandWALSegmentTargetBytes = 1
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.commandWALGroupCommit.delay = time.Second
	db.commandWALGroupCommit.maxCommits = 2
	db.commandWALGroupCommit.maxBytes = 1 << 30
	cutErr := errors.New("injected grouped dependency sync failure")
	var cutOnce sync.Once
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDependencyFileSync {
			var err error
			cutOnce.Do(func() { err = cutErr })
			return err
		}
		return nil
	})
	errs := runPublicCommandWALGroupWaiters(db, 2)
	restore()

	for i, err := range errs {
		if !errors.Is(err, cutErr) {
			t.Fatalf("waiter %d error=%v, want %v", i, err, cutErr)
		}
	}
	if err := db.SetSync([]byte("after-poison"), []byte("value")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("SetSync after dependency failure error=%v, want ErrRecoveryRequired", err)
	}
	if got := publicStatUint64(t, db, "treedb.command_wal.group_commit.errors_total"); got != 1 {
		t.Fatalf("group errors=%d, want 1", got)
	}
}

func TestPublicCommandWALGroupCommitAmbiguousBarrierFailurePoisonsAllWaiters(t *testing.T) {
	opts := commandWALDurabilityProofOptions(t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open command WAL: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.commandWALGroupCommit.delay = time.Second
	db.commandWALGroupCommit.maxCommits = 2
	db.commandWALGroupCommit.maxBytes = 1 << 30
	cutErr := errors.New("injected grouped post-barrier-append failure")
	var cutMu sync.Mutex
	appendCount := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceCommandWAL || event.Point != durabilitycut.AfterDependencyAppend {
			return nil
		}
		cutMu.Lock()
		defer cutMu.Unlock()
		appendCount++
		if appendCount == 3 {
			return cutErr
		}
		return nil
	})
	errs := runPublicCommandWALGroupWaiters(db, 2)
	restore()

	for i, err := range errs {
		if !errors.Is(err, cutErr) {
			t.Fatalf("waiter %d error=%v, want %v", i, err, cutErr)
		}
	}
	if err := db.SetSync([]byte("after-ambiguous"), []byte("value")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("SetSync after ambiguous barrier error=%v, want ErrRecoveryRequired", err)
	}
	if got := publicStatUint64(t, db, "treedb.command_wal.group_commit.errors_total"); got != 1 {
		t.Fatalf("group errors=%d, want 1", got)
	}
}

func runPublicCommandWALGroupWaiters(db *DB, waiters int) []error {
	start := make(chan struct{})
	ready := make(chan struct{}, waiters)
	errs := make(chan error, waiters)
	var workers sync.WaitGroup
	for i := 0; i < waiters; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			b := db.NewBatch()
			defer b.Close()
			key := []byte(fmt.Sprintf("failure-group/%d", i))
			if err := b.Set(key, []byte("value")); err != nil {
				errs <- err
				return
			}
			ready <- struct{}{}
			<-start
			errs <- b.WriteSync()
		}(i)
	}
	for i := 0; i < waiters; i++ {
		<-ready
	}
	close(start)
	workers.Wait()
	close(errs)
	out := make([]error, 0, waiters)
	for err := range errs {
		out = append(out, err)
	}
	return out
}

func copyPublicCommandWALCrashImage(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		output, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			_ = input.Close()
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeOutputErr := output.Close()
		closeInputErr := input.Close()
		return errors.Join(copyErr, closeOutputErr, closeInputErr)
	})
}
