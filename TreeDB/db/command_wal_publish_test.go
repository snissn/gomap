package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// cleanupCommandWALSegmentsCoveredByAppliedLSN is intentionally test-only.
// Production destructive cleanup must enter through
// DB.CleanupCommandWALCoveredSegments so raw visible coverage cannot become
// deletion authority without a validated durable cleanup proof.
func cleanupCommandWALSegmentsCoveredByAppliedLSN(dir string, appliedLSN uint64, maxSegmentBytes int64) ([]commandWALSegmentCleanupDecision, error) {
	return cleanupCommandWALSegmentsWithProof(dir, appliedLSN, appliedLSN, maxSegmentBytes)
}

func cleanupCommandWALSegmentsWithProof(dir string, cleanupThrough uint64, durableWALLSN uint64, maxSegmentBytes int64) ([]commandWALSegmentCleanupDecision, error) {
	decisions, err := scanCommandWALSegmentsForCleanupProof(dir, cleanupThrough, durableWALLSN, maxSegmentBytes)
	if err != nil {
		return decisions, err
	}
	return removeCoveredCommandWALSegments(decisions)
}

func TestCommandWALAppliedCommandLSNMetaFieldRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if state == nil {
		t.Fatalf("missing state")
	}
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if got := db.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN after publish=%d, want 1", got)
	}
	if got := db.Stats()["treedb.applied_command_lsn"]; got != "1" {
		t.Fatalf("stats applied_command_lsn=%q, want 1", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("reopen AppliedCommandLSN=%d, want 1", got)
	}
}

func TestRefreshCommandWALCheckpointFallbackConvergesSlotsWithoutNewLSN(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL DB: %v", err)
	}
	defer db.Close()

	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publish first applied LSN: %v", err)
	}
	if err := db.rootPublication.coordinator.WaitThrough(context.Background(), db.State().CommitSeq); err != nil {
		t.Fatalf("wait first durable root: %v", err)
	}
	state = db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 2, Last: 2}}, true); err != nil {
		t.Fatalf("publish second applied LSN: %v", err)
	}
	if err := db.rootPublication.coordinator.WaitThrough(context.Background(), db.State().CommitSeq); err != nil {
		t.Fatalf("wait second durable root: %v", err)
	}
	before := db.State()
	if before == nil || before.AppliedCommandLSN == 0 {
		t.Fatalf("state before refresh=%+v, want applied command WAL state", before)
	}
	nextLSN := db.CommandWALNextLSN()
	db.durablePublishMu.Lock()
	selected := db.durableRoot.slotRecord[db.durableRoot.slot]
	fallback := db.durableRoot.slotRecord[db.durableRoot.slot^1]
	db.durablePublishMu.Unlock()
	if fallback.AppliedCommandLSN >= selected.AppliedCommandLSN {
		t.Fatalf("test did not create lagging fallback: selected=%d fallback=%d", selected.AppliedCommandLSN, fallback.AppliedCommandLSN)
	}

	if err := db.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatalf("RefreshCommandWALCheckpointFallback: %v", err)
	}
	if got := db.State().AppliedCommandLSN; got != before.AppliedCommandLSN {
		t.Fatalf("AppliedCommandLSN after refresh=%d, want unchanged %d", got, before.AppliedCommandLSN)
	}
	if got := db.CommandWALNextLSN(); got != nextLSN {
		t.Fatalf("next command WAL LSN after refresh=%d, want unchanged %d", got, nextLSN)
	}
	db.durablePublishMu.Lock()
	selected = db.durableRoot.slotRecord[db.durableRoot.slot]
	fallback = db.durableRoot.slotRecord[db.durableRoot.slot^1]
	db.durablePublishMu.Unlock()
	if selected.AppliedCommandLSN != before.AppliedCommandLSN || fallback.AppliedCommandLSN != before.AppliedCommandLSN {
		t.Fatalf("refreshed root slots selected=%d fallback=%d, want both %d", selected.AppliedCommandLSN, fallback.AppliedCommandLSN, before.AppliedCommandLSN)
	}
}

func TestRefreshCommandWALCheckpointFallbackPublicationFailureRetainsFallback(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open command WAL DB: %v", err)
	}
	if err := db.SetSync([]byte("first"), []byte("one")); err != nil {
		t.Fatalf("SetSync first: %v", err)
	}
	if err := db.SetSync([]byte("second"), []byte("two")); err != nil {
		t.Fatalf("SetSync second: %v", err)
	}
	if err := db.rootPublication.coordinator.WaitThrough(context.Background(), db.State().CommitSeq); err != nil {
		t.Fatalf("wait durable command roots: %v", err)
	}
	before := db.State()
	db.durablePublishMu.Lock()
	fallbackBefore := db.durableRoot.slotRecord[db.durableRoot.slot^1].AppliedCommandLSN
	db.durablePublishMu.Unlock()
	if fallbackBefore >= before.AppliedCommandLSN {
		t.Fatalf("test did not create lagging fallback: applied=%d fallback=%d", before.AppliedCommandLSN, fallbackBefore)
	}

	db.testFailWriteMeta.Store(true)
	err = db.RefreshCommandWALCheckpointFallback()
	db.testFailWriteMeta.Store(false)
	if !errors.Is(err, errTestWriteMetaFailpoint) {
		t.Fatalf("RefreshCommandWALCheckpointFallback error=%v, want write-meta failpoint", err)
	}
	db.durablePublishMu.Lock()
	fallbackAfter := db.durableRoot.slotRecord[db.durableRoot.slot^1].AppliedCommandLSN
	db.durablePublishMu.Unlock()
	if fallbackAfter != fallbackBefore {
		t.Fatalf("failed refresh changed fallback applied LSN=%d, want %d", fallbackAfter, fallbackBefore)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close after failed refresh: %v", err)
	}
	reopened, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen command WAL DB: %v", err)
	}
	defer reopened.Close()
	assertDBValue(t, reopened, "first", "one")
	assertDBValue(t, reopened, "second", "two")
}

func TestCommandWALAppliedLSNOnlyPublishPreservesValueLogRefTracker(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 10_000, 4, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i)}, 4096)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	b := db.NewBatch().(*Batch)
	for i := range ptrs {
		if err := b.SetPointer([]byte(fmt.Sprintf("k%d", i)), ptrs[i]); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	beforeSeq := db.currentCommitSeq()
	beforeRefs, ok := db.valueLogRefTracker.referencedSet(beforeSeq)
	if !ok {
		t.Fatalf("expected ref tracker valid before applied-LSN publish at seq=%d", beforeSeq)
	}
	if len(beforeRefs) == 0 {
		t.Fatalf("expected value-log refs before applied-LSN publish")
	}

	state := db.State()
	if state == nil {
		t.Fatal("missing state")
	}
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, state.AppliedCommandLSN+1, []CommandWALLSNRange{{First: state.AppliedCommandLSN + 1, Last: state.AppliedCommandLSN + 1}}, false); err != nil {
		t.Fatalf("publish applied LSN only: %v", err)
	}

	afterSeq := db.currentCommitSeq()
	if afterSeq != beforeSeq+1 {
		t.Fatalf("commit seq after applied-LSN publish=%d, want %d", afterSeq, beforeSeq+1)
	}
	afterRefs, ok := db.valueLogRefTracker.referencedSet(afterSeq)
	if !ok {
		t.Fatalf("expected ref tracker to remain valid after applied-LSN publish at seq=%d", afterSeq)
	}
	if !reflect.DeepEqual(afterRefs, beforeRefs) {
		t.Fatalf("ref tracker changed after applied-LSN publish: before=%v after=%v", beforeRefs, afterRefs)
	}

	metaPath := db.valueLogRefCountsPath()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read ref-count metadata: %v", err)
	}
	disk, err := decodeValueLogRefCounts(data)
	if err != nil {
		t.Fatalf("decode ref-count metadata: %v", err)
	}
	if disk.commitSeq != afterSeq {
		t.Fatalf("metadata seq after close=%d, want %d", disk.commitSeq, afterSeq)
	}
}

func TestCommandWALAppliedLSNOnlyPublishRebindsRootsAfterDurableGateWait(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := db.SetSync([]byte("seed"), []byte("one")); err != nil {
		t.Fatalf("seed SetSync: %v", err)
	}
	before := db.State()
	if before == nil {
		t.Fatal("missing state before concurrent publications")
	}

	candidatePrepared := make(chan struct{})
	releaseCandidate := make(chan struct{})
	var candidateOnce sync.Once
	db.testDurableRootCandidatePreparedHook = func() {
		candidateOnce.Do(func() {
			close(candidatePrepared)
			<-releaseCandidate
		})
	}

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- db.SetSync([]byte("new-root"), []byte("two"))
	}()
	select {
	case <-candidatePrepared:
	case <-time.After(5 * time.Second):
		t.Fatal("root-changing publication did not reach the durable candidate gate")
	}

	commandBeforeGate := make(chan struct{})
	var commandOnce sync.Once
	db.testCommandWALBeforeDurablePublishLockHook = func() {
		commandOnce.Do(func() { close(commandBeforeGate) })
	}
	publishDone := make(chan error, 1)
	go func() {
		publishDone <- db.PublishCommandWALAppliedLSN(1, []CommandWALLSNRange{{First: 1, Last: 1}}, true)
	}()
	select {
	case <-commandBeforeGate:
	case <-time.After(5 * time.Second):
		close(releaseCandidate)
		t.Fatal("applied-LSN publication did not reach the durable publish gate")
	}

	close(releaseCandidate)
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("root-changing SetSync: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("root-changing publication did not finish")
	}
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("PublishCommandWALAppliedLSN: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("applied-LSN publication did not finish")
	}

	db.testDurableRootCandidatePreparedHook = nil
	db.testCommandWALBeforeDurablePublishLockHook = nil
	after := db.State()
	if after == nil {
		t.Fatal("missing state after concurrent publications")
	}
	if after.RootPageID == before.RootPageID {
		t.Fatalf("root rolled back to pre-publication root %d", before.RootPageID)
	}
	if after.AppliedCommandLSN != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", after.AppliedCommandLSN)
	}
	got, err := db.Get([]byte("new-root"))
	if err != nil || string(got) != "two" {
		t.Fatalf("Get(new-root)=(%q, %v), want (two, nil)", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if state := reopened.State(); state == nil || state.RootPageID != after.RootPageID || state.AppliedCommandLSN != 1 {
		t.Fatalf("reopened state=%+v, want root=%d applied_lsn=1", state, after.RootPageID)
	}
	got, err = reopened.Get([]byte("new-root"))
	if err != nil || string(got) != "two" {
		t.Fatalf("reopened Get(new-root)=(%q, %v), want (two, nil)", got, err)
	}
}

func TestCommandWALPublicationRebindsBuilderAfterRuntimeSwap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	for _, tc := range []struct {
		name    string
		publish func(*testing.T, *DB) error
	}{
		{
			name: "applied_lsn",
			publish: func(_ *testing.T, db *DB) error {
				return db.PublishCommandWALAppliedLSN(1, []CommandWALLSNRange{{First: 1, Last: 1}}, true)
			},
		},
		{
			name: "noop",
			publish: func(t *testing.T, db *DB) error {
				payload, err := commitlog.EncodeRawKVBatchPayload(nil)
				if err != nil {
					t.Fatalf("EncodeRawKVBatchPayload: %v", err)
				}
				intent, err := db.NewTrustedCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
				if err != nil {
					t.Fatalf("NewTrustedCommandWALIntent: %v", err)
				}
				return db.PublishCommandWALNoop(intent, true)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { _ = db.Close() }()

			oldRuntime := db.rootPublication
			replacement, err := newRootPublicationRuntimeV1(db, db.idx.Load(), db.durableRoot, db.meta)
			if err != nil {
				t.Fatalf("new replacement runtime: %v", err)
			}
			var once sync.Once
			db.testCommandWALAfterBuilderAcquireHook = func() {
				once.Do(func() {
					db.writeMu.Lock()
					db.rootPublication = replacement
					db.writeMu.Unlock()
				})
			}

			if err := tc.publish(t, db); err != nil {
				t.Fatalf("publish after runtime swap: %v", err)
			}
			db.testCommandWALAfterBuilderAcquireHook = nil
			handoff, err := stopRootPublicationRuntimeV1(oldRuntime)
			if err != nil {
				t.Fatalf("stop old runtime: %v", err)
			}
			handoff.Release()
			oldRuntime.release()
		})
	}
}

func TestCommandWALAppliedLSNOnlyPublishRechecksPoisonAfterDurableGateWait(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	before := db.State()
	if before == nil {
		t.Fatal("missing state before publication")
	}
	beforeGate := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	db.testCommandWALBeforeDurablePublishLockHook = func() {
		once.Do(func() {
			close(beforeGate)
			<-release
		})
	}
	db.durablePublishMu.Lock()
	gateLocked := true
	defer func() {
		if gateLocked {
			db.durablePublishMu.Unlock()
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- db.PublishCommandWALAppliedLSN(
			before.AppliedCommandLSN+1,
			[]CommandWALLSNRange{{First: before.AppliedCommandLSN + 1, Last: before.AppliedCommandLSN + 1}},
			true,
		)
	}()
	select {
	case <-beforeGate:
	case <-time.After(5 * time.Second):
		t.Fatal("applied-LSN publication did not reach the durable publish gate")
	}
	db.publicationPoisoned.Store(true)
	close(release)
	db.durablePublishMu.Unlock()
	gateLocked = false

	select {
	case err := <-done:
		if !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("PublishCommandWALAppliedLSN error=%v, want ErrRecoveryRequired", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("poisoned publication did not return")
	}
	if after := db.State(); after == nil || after.CommitSeq != before.CommitSeq || after.AppliedCommandLSN != before.AppliedCommandLSN {
		t.Fatalf("state changed after poisoned gate wait: before=%+v after=%+v", before, after)
	}
}

func TestPublishCommandWALNoopPreservesValueLogRefTracker(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                    dir,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 20_000, 4, func(i int) []byte {
		return bytes.Repeat([]byte{byte('v' + i)}, 4096)
	})
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	b := db.NewBatch().(*Batch)
	for i := range ptrs {
		if err := b.SetPointer([]byte(fmt.Sprintf("k%d", i)), ptrs[i]); err != nil {
			t.Fatalf("set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	beforeSeq := db.currentCommitSeq()
	beforeApplied := db.State().AppliedCommandLSN
	beforeRefs, ok := db.valueLogRefTracker.referencedSet(beforeSeq)
	if !ok {
		t.Fatalf("expected ref tracker valid before command-WAL no-op at seq=%d", beforeSeq)
	}
	if len(beforeRefs) == 0 {
		t.Fatalf("expected value-log refs before command-WAL no-op")
	}

	payload, err := commitlog.EncodeRawKVBatchPayload(nil)
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	intent, err := db.NewTrustedCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatalf("NewTrustedCommandWALIntent: %v", err)
	}
	if err := db.PublishCommandWALNoop(intent, false); err != nil {
		t.Fatalf("PublishCommandWALNoop: %v", err)
	}

	afterSeq := db.currentCommitSeq()
	if afterSeq != beforeSeq+1 {
		t.Fatalf("commit seq after command-WAL no-op=%d, want %d", afterSeq, beforeSeq+1)
	}
	if got := db.State().AppliedCommandLSN; got != beforeApplied+1 {
		t.Fatalf("AppliedCommandLSN after command-WAL no-op=%d, want %d", got, beforeApplied+1)
	}
	afterRefs, ok := db.valueLogRefTracker.referencedSet(afterSeq)
	if !ok {
		t.Fatalf("expected ref tracker to remain valid after command-WAL no-op at seq=%d", afterSeq)
	}
	if !reflect.DeepEqual(afterRefs, beforeRefs) {
		t.Fatalf("ref tracker changed after command-WAL no-op: before=%v after=%v", beforeRefs, afterRefs)
	}

	metaPath := db.valueLogRefCountsPath()
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read ref-count metadata: %v", err)
	}
	disk, err := decodeValueLogRefCounts(data)
	if err != nil {
		t.Fatalf("decode ref-count metadata: %v", err)
	}
	if disk.commitSeq != afterSeq {
		t.Fatalf("metadata seq after close=%d, want %d", disk.commitSeq, afterSeq)
	}
}

func TestCommandWALAppliedCommandLSNAlternatingMetaPages(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publish lsn 1: %v", err)
	}
	firstMetaPage := db.metaPageID
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 2, Last: 2}}, true); err != nil {
		t.Fatalf("publish lsn 2: %v", err)
	}
	secondMetaPage := db.metaPageID
	if firstMetaPage == secondMetaPage {
		t.Fatalf("meta page did not alternate: first=%d second=%d", firstMetaPage, secondMetaPage)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()
	if got := reopen.State().AppliedCommandLSN; got != 2 {
		t.Fatalf("reopen AppliedCommandLSN=%d, want 2", got)
	}
}

func TestCommandWALDurableMetaRejectsProjectionTampering(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	activeMetaPage := db.metaPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	tamperDurableMetaProjectionBytes(t, dir, activeMetaPage, 12345)
	if _, err := Open(Options{Dir: dir}); !errors.Is(err, ErrNoRecoverableMeta) || !strings.Contains(err.Error(), page.ErrDurableMetaProjection.Error()) {
		t.Fatalf("reopen error=%v, want no recoverable meta with projection mismatch", err)
	}
}

func TestCommandWALRootsAndAppliedCommandLSNPublishAtomically(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	before := *db.State()
	if err := db.publishCommandWALRoots(before.RootPageID, before.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	activeMetaPage := db.metaPageID
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	corruptIndexPageByte(t, dir, activeMetaPage)
	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after corrupt active meta: %v", err)
	}
	defer reopen.Close()
	after := reopen.State()
	if after.CommitSeq != before.CommitSeq {
		t.Fatalf("CommitSeq=%d, want previous durable tuple %d", after.CommitSeq, before.CommitSeq)
	}
	if after.AppliedCommandLSN != before.AppliedCommandLSN {
		t.Fatalf("AppliedCommandLSN=%d, want previous durable tuple %d", after.AppliedCommandLSN, before.AppliedCommandLSN)
	}
	if after.RootPageID != before.RootPageID || after.SystemRootPageID != before.SystemRootPageID {
		t.Fatalf("roots=(%d,%d), want previous durable tuple (%d,%d)", after.RootPageID, after.SystemRootPageID, before.RootPageID, before.SystemRootPageID)
	}
}

func TestCommandWALPublishHelperRejectsRootsWithoutAppliedLSN(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	before := db.State()
	err = db.publishCommandWALRoots(before.RootPageID+1, before.SystemRootPageID, before.AppliedCommandLSN, nil, false)
	if !errors.Is(err, ErrCommandWALSplitPublish) {
		t.Fatalf("publishCommandWALRoots error=%v, want ErrCommandWALSplitPublish", err)
	}
	after := db.State()
	if after.CommitSeq != before.CommitSeq || after.AppliedCommandLSN != before.AppliedCommandLSN {
		t.Fatalf("state changed after rejected publish: before=%+v after=%+v", before, after)
	}
}

func TestCommandWALAppliedLSNContiguousPrefixOnly(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current uint64
		next    uint64
		covered []CommandWALLSNRange
		wantErr error
	}{
		{name: "same", current: 5, next: 5},
		{name: "single", current: 5, next: 6, covered: []CommandWALLSNRange{{First: 6, Last: 6}}},
		{name: "adjacent", current: 5, next: 8, covered: []CommandWALLSNRange{{First: 6, Last: 7}, {First: 8, Last: 8}}},
		{name: "overlap", current: 5, next: 8, covered: []CommandWALLSNRange{{First: 6, Last: 7}, {First: 7, Last: 9}}},
		{name: "gap", current: 5, next: 8, covered: []CommandWALLSNRange{{First: 7, Last: 8}}, wantErr: ErrCommandWALAppliedLSNNonContig},
		{name: "regression", current: 5, next: 4, wantErr: ErrCommandWALAppliedLSNRegression},
		{name: "same with stale coverage", current: 5, next: 5, covered: []CommandWALLSNRange{{First: 6, Last: 6}}, wantErr: ErrCommandWALAppliedLSNNonContig},
		{name: "empty coverage", current: 5, next: 6, wantErr: ErrCommandWALAppliedLSNNonContig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateContiguousAppliedCommandLSN(tc.current, tc.next, tc.covered)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("validateContiguousAppliedCommandLSN: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("validateContiguousAppliedCommandLSN error=%v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCommandWALAppliedLSNValidationDoesNotMutateCoverage(t *testing.T) {
	covered := []CommandWALLSNRange{{First: 3, Last: 3}, {First: 1, Last: 2}}
	original := append([]CommandWALLSNRange(nil), covered...)
	if err := validateContiguousAppliedCommandLSN(0, 3, covered); err != nil {
		t.Fatalf("validateContiguousAppliedCommandLSN: %v", err)
	}
	if !reflect.DeepEqual(covered, original) {
		t.Fatalf("coverage mutated: got %+v want %+v", covered, original)
	}
}

func TestCommandWALAppliedLSNContiguousPrefixMatchesModelStress(t *testing.T) {
	rng := rand.New(rand.NewSource(0x5eed))
	for iter := 0; iter < 1000; iter++ {
		current := uint64(rng.Intn(12))
		next := uint64(rng.Intn(16))
		if iter%11 == 0 {
			next = current
		}
		if iter%37 == 0 {
			current = ^uint64(0)
			next = ^uint64(0)
		}
		ranges := make([]CommandWALLSNRange, rng.Intn(8))
		for i := range ranges {
			first := uint64(rng.Intn(18))
			last := first + uint64(rng.Intn(5))
			if rng.Intn(13) == 0 {
				first = 0
			}
			if rng.Intn(17) == 0 {
				last = first - 1
			}
			ranges[i] = CommandWALLSNRange{First: first, Last: last}
		}
		if rng.Intn(2) == 0 {
			sort.Slice(ranges, func(i, j int) bool {
				if ranges[i].First != ranges[j].First {
					return ranges[i].First < ranges[j].First
				}
				return ranges[i].Last < ranges[j].Last
			})
		}

		err := validateContiguousAppliedCommandLSN(current, next, ranges)
		wantErr := modelValidateContiguousAppliedCommandLSN(current, next, ranges)
		if (err == nil) != (wantErr == nil) {
			t.Fatalf("iter %d current=%d next=%d ranges=%+v err=%v wantErr=%v", iter, current, next, ranges, err, wantErr)
		}
		if wantErr != nil && !errors.Is(err, wantErr) {
			t.Fatalf("iter %d current=%d next=%d ranges=%+v err=%v wantErr=%v", iter, current, next, ranges, err, wantErr)
		}
	}
}

func TestCommandWALReadOnlyOpenWithUnappliedFrameFailsRecoveryRequired(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-only error=%v, want ErrRecoveryRequired", err)
	}
	_, err = openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("openReadOnlyNoLock error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALReadOnlyOpenAllowsFramesCoveredByAppliedLSN(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("covered command WAL segment should remain until cleanup proof: %v", err)
	}
}

func TestCommandWALWriteOpenSkipsCoveredFramesBeforeLegacyReplay(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open read-write with covered command WAL: %v", err)
	}
	if got := reopen.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("AppliedCommandLSN=%d, want 1", got)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("covered command WAL segment should be skipped, not raw-replayed or removed: %v", err)
	}
}

func TestCommandWALLegacyReplayFilterPreservesNonCommandOrderAroundCoveredFrame(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	coveredPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	info, err := os.Stat(coveredPath)
	if err != nil {
		t.Fatalf("Stat covered command segment: %v", err)
	}

	segments := []logSegment{
		{lane: 0, seq: 0, path: "/tmp/value-before.log", size: 123, valueLog: true},
		{lane: 0, seq: 1, path: coveredPath, size: info.Size()},
		{lane: 0, seq: 2, path: "/tmp/value-after.log", size: 456, valueLog: true},
		{lane: 0, seq: 3, path: "/tmp/commit-000003.log", size: 789},
	}
	filtered, err := filterCommandWALSegmentsForLegacyReplay(segments, 1, 0)
	if err != nil {
		t.Fatalf("filterCommandWALSegmentsForLegacyReplay: %v", err)
	}
	got := make([]string, 0, len(filtered))
	for _, seg := range filtered {
		got = append(got, filepath.Base(seg.path))
	}
	want := []string{"value-before.log", "value-after.log", "commit-000003.log"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered order=%v, want %v", got, want)
	}
}

func TestCommandWALLegacyReplayFilterToleratesCoveredDuplicateLSNAcrossSegments(t *testing.T) {
	// Covered duplicate LSNs at or below AppliedCommandLSN are tolerated while
	// cleanup converges (see PR description Correctness Gate).
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 1)
	segments, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("listWALSegments: %v", err)
	}

	// Both segments have LSN=1 <= appliedLSN=1; both are covered. The filter
	// must tolerate the duplicate and return the segments with both omitted
	// (they are fully covered).
	filtered, err := filterCommandWALSegmentsForLegacyReplay(segments, 1, 0)
	if err != nil {
		t.Fatalf("filterCommandWALSegmentsForLegacyReplay error=%v, want nil (covered duplicate tolerated)", err)
	}
	_ = filtered
}

func TestCommandWALLegacyReplayFilterRejectsPartiallyAppliedSegment(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALSegmentFrames(t, dir, 1, 1, 2)
	segments, err := listWALSegments(dir)
	if err != nil {
		t.Fatalf("listWALSegments: %v", err)
	}

	_, err = filterCommandWALSegmentsForLegacyReplay(segments, 1, 0)
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("filterCommandWALSegmentsForLegacyReplay error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALWriteOpenRejectsUnappliedFramesUntilDispatcher(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 2)

	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-write with unapplied command WAL error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALWriteOpenRejectsFirstUnappliedFrameUntilDispatcher(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open read-write with first unapplied command WAL error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALWALOffOpenRejectsUnappliedFramesUntilDispatcher(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)

	_, err = Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Open WAL-off with unapplied command WAL error=%v, want ErrRecoveryRequired", err)
	}
}

func TestCommandWALOpenAllowsCoveredDuplicateLSNAndCleanupConverges(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALSegmentFrames(t, dir, 1, 1)
	writeCommandWALSegmentFrames(t, dir, 2, 1)

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only covered duplicate LSN: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}
	rw, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open read-write covered duplicate LSN: %v", err)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("Close read-write: %v", err)
	}
	// Both physical copies are already covered by every recoverable root. The
	// exact-identity cleanup may remove the inactive copy while retaining the
	// active append target.
	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 2, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	removed := 0
	for _, d := range decisions {
		if d.Removed {
			removed++
		}
	}
	if removed != 1 {
		t.Fatalf("cleanup removed=%d segments, want inactive duplicate only", removed)
	}
}

func TestCommandWALOpenFailsClosedOnUnappliedDuplicateLSN(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALSegmentFrames(t, dir, 1, 3, 3)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("Open read-only error=%v, want ErrCommandWALDuplicateLSN", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("Open read-write error=%v, want ErrCommandWALDuplicateLSN", err)
	}
}

// TestCommandWALOpenFailsClosedOnCorruptCRCEvenWhenCovered verifies that a
// segment containing a CRC-corrupt record still fails closed on open even when
// the covered frames in that segment have LSN <= AppliedCommandLSN. Covered
// duplicate LSNs are tolerated (see
// TestCommandWALOpenAllowsCoveredDuplicateLSNBeforeCleanupConverges), but true
// data corruption (bad CRC) is not.
func TestCommandWALOpenFailsClosedOnCorruptCRCEvenWhenCovered(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Write a valid covered frame (LSN=1), then append a record header whose
	// CRC field is wrong (length=0, CRC=0x00000001). The reader will read the
	// 8-byte header, decode an empty payload, compute CRC(empty)!=1, and
	// return ErrCorrupt — even though LSN=1 is covered by AppliedCommandLSN=2.
	writeCommandWALFrame(t, dir, 1, 1)
	appendCommandWALTail(t, dir, 1, []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00})

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCorrupt) {
		t.Fatalf("Open read-only error=%v, want ErrCorrupt", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCorrupt) {
		t.Fatalf("Open read-write error=%v, want ErrCorrupt", err)
	}
}

func TestCommandWALOpenToleratesCoveredDuplicateLSNAcrossSegments(t *testing.T) {
	// Covered duplicate LSNs at or below AppliedCommandLSN are tolerated while
	// cleanup converges.  Both open paths (read-only and read-write) must succeed.
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 9, []CommandWALLSNRange{{First: 1, Last: 9}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 9)
	writeCommandWALFrame(t, dir, 2, 9)

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only covered duplicate LSN: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}
	rw, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open read-write covered duplicate LSN: %v", err)
	}
	if err := rw.Close(); err != nil {
		t.Fatalf("Close read-write: %v", err)
	}
}

func TestCommandWALOpenFailsClosedOnNonActiveTerminalTailEvenWhenCovered(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)
	appendCommandWALTail(t, dir, 1, []byte{0xde, 0xad, 0xbe})
	writeCommandWALFrame(t, dir, 2, 2)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-only error=%v, want ErrCommandWALTerminalTail", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-write error=%v, want ErrCommandWALTerminalTail", err)
	}
}

func TestCommandWALOpenFailsClosedOnTypedTailWithHigherLegacyRawSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)
	appendCommandWALTail(t, dir, 1, []byte{0xde, 0xad, 0xbe})
	writeLegacyRawWALFrame(t, dir, 999, 999)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-only error=%v, want ErrCommandWALTerminalTail", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-write error=%v, want ErrCommandWALTerminalTail", err)
	}
}

func TestCommandWALOpenAllowsActiveTypedTailWithHigherPartialLegacyAliasSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)
	appendCommandWALTail(t, dir, 1, []byte{0xde, 0xad, 0xbe})
	writePartialLegacyAliasWALSegmentTail(t, dir, 999, []byte{0xca, 0xfe})

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only with active typed tail and higher partial legacy alias segment: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}
	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open read-write with active typed tail and higher partial legacy alias segment: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
}

func TestCommandWALOpenAllowsActivePartialFirstFrameTail(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 1, []CommandWALLSNRange{{First: 1, Last: 1}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeCommandWALFrame(t, dir, 1, 1)
	writePartialCommandWALSegmentTail(t, dir, 2, []byte{0xde, 0xad})

	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only with active partial first frame: %v", err)
	}
	if err := ro.Close(); err != nil {
		t.Fatalf("Close read-only: %v", err)
	}
	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open read-write with active partial first frame: %v", err)
	}
	if err := reopen.Close(); err != nil {
		t.Fatalf("Close reopen: %v", err)
	}
}

func TestCommandWALOpenFailsClosedOnNonActivePartialFirstFrameTail(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	state := db.State()
	if err := db.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publishCommandWALRoots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writePartialCommandWALSegmentTail(t, dir, 1, []byte{0xde, 0xad})
	writeCommandWALFrame(t, dir, 2, 2)

	_, err = Open(Options{Dir: dir, ReadOnly: true})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-only error=%v, want ErrCommandWALTerminalTail", err)
	}
	_, err = Open(Options{Dir: dir})
	if !errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
		t.Fatalf("Open read-write error=%v, want ErrCommandWALTerminalTail", err)
	}
}

func TestCommandWALCleanupCoveredSegmentsRejectsReadOnlyHandle(t *testing.T) {
	db := &DB{commandWAL: true, readOnly: true}
	if err := db.CleanupCommandWALCoveredSegments(false); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("CleanupCommandWALCoveredSegments read-only error=%v want ErrReadOnly", err)
	}
}

func TestCommandWALCleanupRequiresLiveJournalOwner(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("close-cleanup-1"), Value: []byte("value")}})
	writeCommandWALRawKVFrame(t, dir, 2, 2, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("close-cleanup-2"), Value: []byte("value")}})
	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	installCommandWALCleanupRootRuntimeForTest(db, 1, 1)

	if _, err := db.captureDurableWALCleanupProofV1(); !errors.Is(err, errDurableWALCleanupProofUnavailable) {
		t.Fatalf("captureDurableWALCleanupProofV1 error=%v, want unavailable without live owner", err)
	}
	if err := db.CleanupCommandWALCoveredSegments(false); !errors.Is(err, errDurableWALCleanupProofUnavailable) {
		t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want unavailable without live owner", err)
	}
	if err := db.CleanupCommandWALCoveredSegmentsAtCheckpoint(false); err != nil {
		t.Fatalf("CleanupCommandWALCoveredSegmentsAtCheckpoint error=%v, want conservative retention", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))); err != nil {
		t.Fatalf("ownerless cleanup removed covered segment: %v", err)
	}
}

func TestCommandWALCleanupRejectsClosedHandleWhileNewOwnerIsLive(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open first owner: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close first owner: %v", err)
	}
	reopen, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open second owner: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	before := commandWALSegmentNamesForTest(t, dir)
	if err := db.CleanupCommandWALCoveredSegments(false); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed-handle cleanup error=%v, want ErrClosed", err)
	}
	if after := commandWALSegmentNamesForTest(t, dir); !reflect.DeepEqual(after, before) {
		t.Fatalf("closed-handle cleanup changed new owner's namespace: before=%v after=%v", before, after)
	}
}

func TestCommandWALCloseRetainsUnavailableCleanupProof(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	batch := db.NewBatch()
	if err := batch.Set([]byte("relaxed-close"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if runtime := db.rootPublication; runtime == nil || runtime.coordinator == nil {
		t.Fatal("missing root publication runtime")
	} else if err := runtime.coordinator.WaitThrough(context.Background(), db.State().CommitSeq); err != nil {
		t.Fatalf("wait first durable root: %v", err)
	}
	if db.commandWALDurableLSN.Load() >= db.State().AppliedCommandLSN {
		t.Fatalf("test did not create unavailable cleanup authority: durable=%d applied=%d", db.commandWALDurableLSN.Load(), db.State().AppliedCommandLSN)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close with conservative cleanup retention: %v", err)
	}
}

func TestCommandWALClosePropagatesCleanupFailure(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	bootstrap := openCommandWALDB(t, dir)
	state := bootstrap.State()
	if err := bootstrap.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, 2, []CommandWALLSNRange{{First: 1, Last: 2}}, true); err != nil {
		t.Fatalf("publish bootstrap command roots: %v", err)
	}
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("Close bootstrap: %v", err)
	}
	writeCommandWALRawKVFrame(t, dir, 1, 1, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("close-cleanup-1"), Value: []byte("value")}})
	writeCommandWALRawKVFrame(t, dir, 2, 2, []commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: []byte("close-cleanup-2"), Value: []byte("value")}})

	db := openCommandWALDB(t, dir)
	installCommandWALCleanupTwoRootRuntimeForTest(db, 1, 2)
	proof, err := db.captureDurableWALCleanupProofV1()
	if err != nil {
		t.Fatalf("capture close cleanup proof: %v", err)
	}
	if proof.cleanupThrough == 0 {
		t.Fatal("close cleanup proof has zero frontier")
	}
	wantErr := errors.New("injected close cleanup directory sync failure")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.BeforeDeletionDirectorySync && event.Resource == durabilitycut.ResourceCommandWAL {
			return wantErr
		}
		return nil
	})
	err = db.Close()
	restore()
	if !errors.Is(err, wantErr) || !errors.Is(err, ErrRecoveryRequired) || !strings.Contains(err.Error(), "cleanup command WAL during close") {
		t.Fatalf("Close error=%v, want propagated cleanup sync failure", err)
	}
	if err := db.Close(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("second Close error=%v, want retained teardown error", err)
	}
}

func installCommandWALCleanupRootForTest(tb testing.TB, db *DB, commitSeq, appliedLSN uint64) {
	tb.Helper()
	installCommandWALCleanupRootRuntimeForTest(db, commitSeq, appliedLSN)
	installCommandWALCleanupJournalOwnerForTest(tb, db, appliedLSN)
}

func installCommandWALCleanupJournalOwnerForTest(tb testing.TB, db *DB, appliedLSN uint64) {
	tb.Helper()
	if db.commandJournal != nil {
		return
	}
	segments, err := listRecoverySegments(db.dir)
	if err != nil {
		tb.Fatalf("list cleanup journal segments: %v", err)
	}
	segmentSeq := commandWALActiveSeqByLane(segments)[0]
	if segmentSeq == ^uint64(0) {
		tb.Fatalf("cleanup journal segment sequence exhausted")
	}
	hasTerminalTail, err := commandWALLaneActiveHasTerminalTail(segments, 0, db.walMaxSegmentBytes)
	if err != nil {
		tb.Fatalf("inspect cleanup journal active tail: %v", err)
	}
	if segmentSeq != 0 && !hasTerminalTail {
		segmentSeq++
	}
	journal, err := commitlog.OpenCommandJournal(WALDirPath(db.dir), commitlog.CommandJournalOptions{
		MaxSegmentSize:         db.walMaxSegmentBytes,
		InitialLSN:             appliedLSN,
		SegmentSeq:             segmentSeq,
		CaptureStableResources: true,
	})
	if err != nil {
		tb.Fatalf("OpenCommandJournal cleanup owner: %v", err)
	}
	db.commandJournal = journal
	tb.Cleanup(func() {
		if db.commandJournal == journal {
			db.commandJournal = nil
		}
		_ = journal.Close()
	})
}

func installCommandWALCleanupRootRuntimeForTest(db *DB, commitSeq, appliedLSN uint64) {
	meta := page.DurableMetaV1{CommitSeq: commitSeq}
	record := rootpublication.DurableRootRecordV1{CommitSeq: commitSeq, AppliedCommandLSN: appliedLSN}
	db.durableRoot.slot = 0
	db.durableRoot.meta = meta
	db.durableRoot.record = record
	db.durableRoot.slotCommit = [2]uint64{commitSeq, 0}
	db.durableRoot.slotMeta = [2]page.DurableMetaV1{meta, {}}
	db.durableRoot.slotRecord = [2]rootpublication.DurableRootRecordV1{record, {}}
	db.commandWALDurableLSN.Store(appliedLSN)
}

func installCommandWALCleanupTwoRootRuntimeForTest(db *DB, commitSeq, appliedLSN uint64) {
	meta0 := page.DurableMetaV1{CommitSeq: commitSeq}
	meta1 := page.DurableMetaV1{CommitSeq: commitSeq + 1}
	record0 := rootpublication.DurableRootRecordV1{CommitSeq: commitSeq, AppliedCommandLSN: appliedLSN}
	record1 := rootpublication.DurableRootRecordV1{CommitSeq: commitSeq + 1, AppliedCommandLSN: appliedLSN}
	db.durableRoot.slot = 1
	db.durableRoot.meta = meta1
	db.durableRoot.record = record1
	db.durableRoot.slotCommit = [2]uint64{commitSeq, commitSeq + 1}
	db.durableRoot.slotMeta = [2]page.DurableMetaV1{meta0, meta1}
	db.durableRoot.slotRecord = [2]rootpublication.DurableRootRecordV1{record0, record1}
	db.commandWALDurableLSN.Store(appliedLSN)
}

func TestCommandWALCheckpointEstablishesDurableCleanupProof(t *testing.T) {
	db, err := Open(Options{
		Dir:                    t.TempDir(),
		CommandWAL:             true,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	batch := db.NewBatch()
	if err := batch.Set([]byte("checkpoint-proof"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	applied := db.State().AppliedCommandLSN
	if applied == 0 {
		t.Fatal("relaxed write did not advance AppliedCommandLSN")
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if durable := db.commandWALDurableLSN.Load(); durable < applied {
		t.Fatalf("durable WAL LSN=%d, want at least applied LSN %d", durable, applied)
	}
	proof, err := db.captureDurableWALCleanupProofV1()
	if err != nil {
		t.Fatalf("captureDurableWALCleanupProofV1 after checkpoint: %v", err)
	}
	if proof.durableWALLSN < applied {
		t.Fatalf("proof durable WAL LSN=%d, want at least applied LSN %d", proof.durableWALLSN, applied)
	}
	selectedApplied := uint64(0)
	for i := 0; i < proof.rootCount; i++ {
		if proof.roots[i].slot == proof.selectedSlot {
			selectedApplied = proof.roots[i].record.AppliedCommandLSN
			break
		}
	}
	if selectedApplied != applied {
		t.Fatalf("selected-root applied LSN=%d, want %d", selectedApplied, applied)
	}
	if proof.cleanupThrough > applied {
		t.Fatalf("cleanup frontier=%d, want at most applied LSN %d", proof.cleanupThrough, applied)
	}
}

func TestCommandWALCleanupProofRejectsPendingJournalOwnership(t *testing.T) {
	t.Run("stable-rotation", func(t *testing.T) {
		dir := t.TempDir()
		journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
			Lane:                   0,
			SegmentTargetBytes:     1,
			CaptureStableResources: true,
		})
		if err != nil {
			t.Fatalf("OpenCommandJournal: %v", err)
		}
		defer func() { _ = journal.Close() }()
		for i := 0; i < 2; i++ {
			if _, err := journal.AppendCommand(commitlog.CommandEnvelope{
				Kind:          commitlog.CommandKindRawKVBatch,
				Scope:         commitlog.CommandScopeRawKV,
				PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
			}); err != nil {
				t.Fatalf("AppendCommand %d: %v", i, err)
			}
		}
		snapshot, err := journal.CaptureCleanupSnapshot()
		if err != nil {
			t.Fatalf("CaptureCleanupSnapshot: %v", err)
		}
		if snapshot.PendingStableRotation == 0 {
			t.Fatalf("PendingStableRotation=%d, want pending ownership", snapshot.PendingStableRotation)
		}

		db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable, commandJournal: journal}
		installCommandWALCleanupRootForTest(t, db, 1, 1)
		if _, err := db.captureDurableWALCleanupProofV1(); !errors.Is(err, errDurableWALCleanupProofUnavailable) {
			t.Fatalf("captureDurableWALCleanupProofV1 error=%v, want unavailable proof", err)
		}

		rotations, err := journal.TakePendingStableRotations()
		if err != nil {
			t.Fatalf("TakePendingStableRotations: %v", err)
		}
		for _, rotation := range rotations {
			rotation.Release()
		}
	})

	t.Run("successor-retry", func(t *testing.T) {
		err := validateCommandJournalCleanupSnapshotV1(commitlog.CommandJournalCleanupSnapshot{PendingSuccessor: true})
		if !errors.Is(err, errDurableWALCleanupProofUnavailable) {
			t.Fatalf("validateCommandJournalCleanupSnapshotV1 error=%v, want unavailable proof", err)
		}
	})
}

func TestCommandWALCleanupProofRejectsDurableRootPublicationDebt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*DB)
	}{
		{
			name: "pending-retry",
			setup: func(db *DB) {
				db.durableRoot.pending = &durableRootPublishCandidateV1{}
			},
		},
		{
			name: "ambiguous-outcome",
			setup: func(db *DB) {
				db.durableRoot.ambiguous = []*durableRootPublishCandidateV1{{}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeCommandWALFrame(t, dir, 1, 1)
			if err := os.WriteFile(filepath.Join(WALDirPath(dir), "commit-l0-000002.log"), nil, 0o600); err != nil {
				t.Fatalf("write active empty segment: %v", err)
			}

			db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
			installCommandWALCleanupRootForTest(t, db, 1, 1)
			tc.setup(db)

			if _, err := db.captureDurableWALCleanupProofV1(); !errors.Is(err, errDurableWALCleanupProofUnavailable) {
				t.Fatalf("captureDurableWALCleanupProofV1 error=%v, want unavailable proof", err)
			}
			if err := db.CleanupCommandWALCoveredSegments(false); !errors.Is(err, errDurableWALCleanupProofUnavailable) {
				t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want unavailable proof", err)
			}
			if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
				t.Fatalf("covered segment removed while durable-root publication debt exists: %v", err)
			}
		})
	}
}

func TestCommandWALCleanupFrontierIgnoresNonRootProgress(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*DB)
	}{
		{
			name: "visible-applied-lsn-ahead",
			setup: func(db *DB) {
				db.state.Store(&DBState{AppliedCommandLSN: 2})
			},
		},
		{
			name: "durable-wal-prefix-ahead",
			setup: func(db *DB) {
				db.commandWALDurableLSN.Store(2)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeCommandWALFrame(t, dir, 1, 1)
			writeCommandWALFrame(t, dir, 2, 2)
			if err := os.WriteFile(filepath.Join(WALDirPath(dir), "commit-l0-000003.log"), nil, 0o600); err != nil {
				t.Fatalf("write active empty segment: %v", err)
			}

			db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
			installCommandWALCleanupRootForTest(t, db, 1, 1)
			tc.setup(db)
			proof, err := db.captureDurableWALCleanupProofV1()
			if err != nil {
				t.Fatalf("captureDurableWALCleanupProofV1: %v", err)
			}
			if proof.cleanupThrough != 1 {
				t.Fatalf("cleanupThrough=%d, want durable-root frontier 1", proof.cleanupThrough)
			}
			if err := db.CleanupCommandWALCoveredSegments(false); err != nil {
				t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
			}
			if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(err) {
				t.Fatalf("root-covered segment stat=%v, want removed", err)
			}
			if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log")); err != nil {
				t.Fatalf("segment beyond durable-root frontier stat=%v, want retained", err)
			}
		})
	}
}

func TestCommandWALCleanupPinnedRetryRescansAndConverges(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)

	decisions, err := scanCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("scanCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	var coveredIdentity rootpublication.StableIdentity
	for _, decision := range decisions {
		if decision.Covered && !decision.Active {
			coveredIdentity = decision.identity
			break
		}
	}
	registry := rootpublication.NewIdentityPinRegistry()
	if err := registry.Observe(coveredIdentity); err != nil {
		closeCommandWALCleanupDecisions(decisions)
		t.Fatalf("Observe covered identity: %v", err)
	}
	pin, err := registry.Pin(coveredIdentity)
	if err != nil {
		closeCommandWALCleanupDecisions(decisions)
		t.Fatalf("Pin covered identity: %v", err)
	}

	decisions, err = removeCoveredCommandWALSegmentsWithRegistry(decisions, registry)
	if !errors.Is(err, rootpublication.ErrResourcePinned) {
		pin.Release()
		t.Fatalf("removeCoveredCommandWALSegmentsWithRegistry error=%v, want ErrResourcePinned", err)
	}
	pinned := false
	for _, decision := range decisions {
		if decision.Covered && !decision.Active {
			pinned = decision.Pinned
		}
	}
	if !pinned {
		pin.Release()
		t.Fatalf("cleanup decisions=%+v, want pinned retention", decisions)
	}
	coveredPath := filepath.Join(WALDirPath(dir), "commit-l0-000001.log")
	if _, err := os.Stat(coveredPath); err != nil {
		pin.Release()
		t.Fatalf("pinned covered segment was removed: %v", err)
	}

	pin.Release()
	decisions, err = scanCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("rescan after pin release: %v", err)
	}
	decisions, err = removeCoveredCommandWALSegmentsWithRegistry(decisions, registry)
	if err != nil {
		t.Fatalf("retry cleanup after pin release: %v", err)
	}
	if _, err := os.Stat(coveredPath); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat after retry=%v, want removed", err)
	}
	if err := registry.Unobserve(coveredIdentity); err != nil {
		t.Fatalf("Unobserve deleted covered identity: %v", err)
	}
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
		t.Fatalf("pin registry leaked after converged retry: %+v", stats)
	}
}

func TestCommandWALCleanupRetainsReplayForOlderRecoverableRoot(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)
	if err := os.WriteFile(filepath.Join(WALDirPath(dir), "commit-l0-000003.log"), nil, 0o600); err != nil {
		t.Fatalf("write active empty segment: %v", err)
	}

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	db.durableRoot.slot = 1
	db.durableRoot.meta = page.DurableMetaV1{CommitSeq: 2}
	db.durableRoot.record = rootpublication.DurableRootRecordV1{CommitSeq: 2, AppliedCommandLSN: 2}
	db.durableRoot.slotCommit = [2]uint64{1, 2}
	db.durableRoot.slotMeta = [2]page.DurableMetaV1{{CommitSeq: 1}, {CommitSeq: 2}}
	db.durableRoot.slotRecord = [2]rootpublication.DurableRootRecordV1{
		{CommitSeq: 1, AppliedCommandLSN: 1},
		{CommitSeq: 2, AppliedCommandLSN: 2},
	}
	db.commandWALDurableLSN.Store(2)
	installCommandWALCleanupJournalOwnerForTest(t, db, 2)

	if err := db.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(err) {
		t.Fatalf("segment covered by both roots stat=%v, want removed", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log")); err != nil {
		t.Fatalf("segment needed by older recoverable root stat=%v, want retained", err)
	}
}

func TestCommandWALCleanupTreatsMissingRetainedReplayLineageAsUnavailableProof(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	db.durableRoot.slot = 1
	db.durableRoot.meta = page.DurableMetaV1{CommitSeq: 2}
	db.durableRoot.record = rootpublication.DurableRootRecordV1{CommitSeq: 2, AppliedCommandLSN: 2}
	db.durableRoot.slotCommit = [2]uint64{1, 2}
	db.durableRoot.slotMeta = [2]page.DurableMetaV1{{CommitSeq: 1}, {CommitSeq: 2}}
	db.durableRoot.slotRecord = [2]rootpublication.DurableRootRecordV1{
		{CommitSeq: 1, AppliedCommandLSN: 1},
		{CommitSeq: 2, AppliedCommandLSN: 2},
	}
	db.commandWALDurableLSN.Store(2)
	installCommandWALCleanupJournalOwnerForTest(t, db, 2)

	err := db.CleanupCommandWALCoveredSegments(false)
	if !errors.Is(err, errDurableWALCleanupProofUnavailable) || !errors.Is(err, ErrCommandWALAppliedLSNNonContig) {
		t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want unavailable retained-lineage proof", err)
	}
	if err := db.CleanupCommandWALCoveredSegmentsAtCheckpoint(false); err != nil {
		t.Fatalf("checkpoint cleanup with unavailable retained lineage: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))); err != nil {
		t.Fatalf("unavailable proof removed covered segment: %v", err)
	}
}

func TestCommandWALCleanupConvergesAfterActiveAppend(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
		Lane:       0,
		SegmentSeq: 2,
		InitialLSN: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = journal.Close() }()

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable, commandJournal: journal}
	installCommandWALCleanupRootForTest(t, db, 1, 1)
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op:    commitlog.RawKVOpSet,
		Key:   []byte("appended-after-scan"),
		Value: []byte("retained-value"),
	}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	var appendErr error
	db.testCommandWALCleanupAfterScanHook = func() {
		_, appendErr = journal.AppendCommand(commitlog.CommandEnvelope{
			Kind:          commitlog.CommandKindRawKVBatch,
			Scope:         commitlog.CommandScopeRawKV,
			PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
			Payload:       payload,
		})
	}

	if err := db.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
	}
	if appendErr != nil {
		t.Fatalf("append after cleanup scan: %v", appendErr)
	}
	if err := journal.Flush(); err != nil {
		t.Fatalf("Flush appended active segment: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(err) {
		t.Fatalf("covered old segment stat=%v, want removed", err)
	}
	activePath := filepath.Join(WALDirPath(dir), "commit-l0-000002.log")
	info, err := os.Stat(activePath)
	if err != nil {
		t.Fatalf("appended active segment: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("appended active segment was truncated")
	}
	scan, err := scanCommandWALSegment(activePath, 0, true)
	if err != nil {
		t.Fatalf("scan appended active segment: %v", err)
	}
	if scan.frames != 1 || scan.maxLSN != 2 {
		t.Fatalf("appended active scan=%+v, want one retained frame at LSN 2", scan)
	}
}

func TestCommandWALCleanupAdvancesJournalNamespaceGeneration(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
		Lane:       0,
		SegmentSeq: 2,
		InitialLSN: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = journal.Close() }()

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable, commandJournal: journal}
	installCommandWALCleanupRootForTest(t, db, 1, 1)
	before, err := journal.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatalf("CaptureCleanupSnapshot before cleanup: %v", err)
	}
	if err := db.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
	}
	after, err := journal.CaptureCleanupSnapshot()
	if err != nil {
		t.Fatalf("CaptureCleanupSnapshot after cleanup: %v", err)
	}
	if after.NamespaceGeneration != before.NamespaceGeneration+1 {
		t.Fatalf("namespace generation after cleanup=%d, want %d", after.NamespaceGeneration, before.NamespaceGeneration+1)
	}
	if after.CleanupEpoch != before.CleanupEpoch+1 {
		t.Fatalf("cleanup epoch after cleanup=%d, want %d", after.CleanupEpoch, before.CleanupEpoch+1)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat=%v, want removed", err)
	}
}

func TestNormalizeCommandWALCheckpointCleanupErrorPrefersStaleOverUnavailable(t *testing.T) {
	err := normalizeCommandWALCheckpointCleanupError(errors.Join(
		errDurableWALCleanupProofUnavailable,
		errDurableWALCleanupProofStale,
	))
	if !errors.Is(err, ErrDurableWALCleanupProofStale) {
		t.Fatalf("checkpoint cleanup error=%v, want durable cleanup retry sentinel", err)
	}
}

func TestCommandWALCleanupAcceptsMonotonicDurableRootAdvance(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)
	journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
		Lane:       0,
		SegmentSeq: 3,
		InitialLSN: 2,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = journal.Close() }()

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable, commandJournal: journal}
	installCommandWALCleanupRootRuntimeForTest(db, 1, 1)
	db.commandWALDurableLSN.Store(2)
	db.testCommandWALCleanupAfterScanHook = func() {
		meta := page.DurableMetaV1{CommitSeq: 2}
		record := rootpublication.DurableRootRecordV1{CommitSeq: 2, AppliedCommandLSN: 2}
		db.durableRoot.slot = 0
		db.durableRoot.meta = meta
		db.durableRoot.record = record
		db.durableRoot.slotCommit = [2]uint64{2, 0}
		db.durableRoot.slotMeta = [2]page.DurableMetaV1{meta}
		db.durableRoot.slotRecord = [2]rootpublication.DurableRootRecordV1{record}
	}

	if err := db.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(err) {
		t.Fatalf("captured covered segment stat=%v, want removed", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log")); err != nil {
		t.Fatalf("segment beyond captured cleanup frontier removed: %v", err)
	}
}

func TestCommandWALCleanupRetainsSegmentsRotatedAfterScan(t *testing.T) {
	for _, rotations := range []int{1, 3} {
		t.Run(fmt.Sprintf("%d-rotations", rotations), func(t *testing.T) {
			dir := t.TempDir()
			writeCommandWALFrame(t, dir, 1, 1)
			journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
				Lane:       0,
				SegmentSeq: 2,
				InitialLSN: 1,
			})
			if err != nil {
				t.Fatalf("OpenCommandJournal: %v", err)
			}
			defer func() { _ = journal.Close() }()

			db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable, commandJournal: journal}
			installCommandWALCleanupRootRuntimeForTest(db, 1, 1)
			var rotateErr error
			db.testCommandWALCleanupAfterScanHook = func() {
				for i := 0; i < rotations && rotateErr == nil; i++ {
					rotateErr = journal.RotateActiveSegment(false)
				}
			}

			if err := db.CleanupCommandWALCoveredSegments(false); err != nil {
				t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
			}
			if rotateErr != nil {
				t.Fatalf("rotate after cleanup scan: %v", rotateErr)
			}
			if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))); !os.IsNotExist(err) {
				t.Fatalf("covered old segment stat=%v, want removed", err)
			}
			for seq := uint64(2); seq <= uint64(2+rotations); seq++ {
				if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, seq))); err != nil {
					t.Fatalf("post-capture segment %d was not retained: %v", seq, err)
				}
			}
		})
	}
}

func TestCommandWALCleanupRejectsPendingJournalOwnershipAfterScan(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
		Lane:                   0,
		SegmentSeq:             2,
		InitialLSN:             1,
		SegmentTargetBytes:     1,
		CaptureStableResources: true,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = journal.Close() }()

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable, commandJournal: journal}
	installCommandWALCleanupRootRuntimeForTest(db, 1, 1)
	var appendErr error
	db.testCommandWALCleanupAfterScanHook = func() {
		for i := 0; i < 2 && appendErr == nil; i++ {
			_, appendErr = journal.AppendCommand(commitlog.CommandEnvelope{
				Kind:          commitlog.CommandKindRawKVBatch,
				Scope:         commitlog.CommandScopeRawKV,
				PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
			})
		}
	}

	err = db.CleanupCommandWALCoveredSegments(false)
	if appendErr != nil {
		t.Fatalf("append after cleanup scan: %v", appendErr)
	}
	if !errors.Is(err, commitlog.ErrCommandWALCleanupSnapshotStale) {
		t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want stale pending ownership", err)
	}
	if _, statErr := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))); statErr != nil {
		t.Fatalf("covered segment removed with pending ownership: %v", statErr)
	}
	rotations, err := journal.TakePendingStableRotations()
	if err != nil {
		t.Fatalf("TakePendingStableRotations: %v", err)
	}
	for _, rotation := range rotations {
		rotation.Release()
	}
}

func TestCommandWALCleanupRejectsMonotonicAuthorityRegression(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*DB)
	}{
		{
			name: "pending-root-publication",
			setup: func(db *DB) {
				db.durableRoot.pending = &durableRootPublishCandidateV1{}
			},
		},
		{
			name: "ambiguous-root-publication",
			setup: func(db *DB) {
				db.durableRoot.ambiguous = []*durableRootPublishCandidateV1{{}}
			},
		},
		{
			name: "root-frontier",
			setup: func(db *DB) {
				meta := page.DurableMetaV1{CommitSeq: 3}
				record := rootpublication.DurableRootRecordV1{CommitSeq: 3, AppliedCommandLSN: 1}
				db.durableRoot.slot = 0
				db.durableRoot.meta = meta
				db.durableRoot.record = record
				db.durableRoot.slotCommit = [2]uint64{3}
				db.durableRoot.slotMeta = [2]page.DurableMetaV1{meta}
				db.durableRoot.slotRecord = [2]rootpublication.DurableRootRecordV1{record}
			},
		},
		{
			name: "durable-wal-lsn",
			setup: func(db *DB) {
				db.commandWALDurableLSN.Store(1)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeCommandWALFrame(t, dir, 1, 1)
			writeCommandWALFrame(t, dir, 2, 2)
			journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
				Lane:       0,
				SegmentSeq: 3,
				InitialLSN: 2,
			})
			if err != nil {
				t.Fatalf("OpenCommandJournal: %v", err)
			}
			defer func() { _ = journal.Close() }()

			db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable, commandJournal: journal}
			installCommandWALCleanupRootRuntimeForTest(db, 2, 2)
			db.testCommandWALCleanupAfterScanHook = func() { tc.setup(db) }

			err = db.CleanupCommandWALCoveredSegments(false)
			if !errors.Is(err, errDurableWALCleanupProofStale) {
				t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want stale proof", err)
			}
			for seq := uint64(1); seq <= 2; seq++ {
				if _, statErr := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, seq))); statErr != nil {
					t.Fatalf("covered segment %d removed under regressed authority: %v", seq, statErr)
				}
			}
		})
	}
}

func TestCommandWALCleanupRejectsPostAppendPoison(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	if err := os.WriteFile(filepath.Join(WALDirPath(dir), "commit-l0-000002.log"), nil, 0o600); err != nil {
		t.Fatalf("write active empty segment: %v", err)
	}

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	installCommandWALCleanupRootForTest(t, db, 1, 1)
	db.testCommandWALCleanupAfterScanHook = func() { db.commandWALFlushPoisoned.Store(true) }

	err := db.CleanupCommandWALCoveredSegments(false)
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want recovery required after scan", err)
	}
	if _, statErr := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); statErr != nil {
		t.Fatalf("covered segment removed from poisoned handle: %v", statErr)
	}
}

func TestCommandWALCheckpointCleanupDeletesOnlyCoveredSegments(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 3)
	if err := os.WriteFile(filepath.Join(WALDirPath(dir), "commit-l0-000003.log"), nil, 0o600); err != nil {
		t.Fatalf("write active empty segment: %v", err)
	}

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	if len(decisions) != 2 {
		t.Fatalf("len(decisions)=%d, want 2", len(decisions))
	}
	removed := map[string]bool{}
	for _, decision := range decisions {
		removed[filepath.Base(decision.Path)] = decision.Removed
		if decision.Removed && decision.MaxLSN > 1 {
			t.Fatalf("removed uncovered segment: %+v", decision)
		}
	}
	if !removed["commit-l0-000001.log"] {
		t.Fatalf("covered non-active segment was not removed: %+v", decisions)
	}
	if removed["commit-l0-000002.log"] {
		t.Fatalf("uncovered segment was removed: %+v", decisions)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat=%v, want removed", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log")); err != nil {
		t.Fatalf("uncovered segment stat=%v, want present", err)
	}
}

func TestCommandWALCleanupDoesNotEmitAfterDeletionSyncOnSyncFailure(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	db.state.Store(&DBState{AppliedCommandLSN: 1})
	installCommandWALCleanupRootForTest(t, db, 1, 1)

	originalSyncDir := syncDirFn
	defer func() { syncDirFn = originalSyncDir }()
	wantErr := errors.New("injected directory sync failure")
	syncDirFn = func(string) error { return wantErr }

	var points []durabilitycut.Point
	restoreObserver := durabilitycut.Install(func(event durabilitycut.Event) error {
		points = append(points, event.Point)
		return nil
	})
	defer restoreObserver()

	if err := db.CleanupCommandWALCoveredSegments(true); !errors.Is(err, wantErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want %v and ErrRecoveryRequired", err, wantErr)
	}
	wantPoints := []durabilitycut.Point{
		durabilitycut.BeforeWALOrAssetUnlink,
		durabilitycut.AfterWALOrAssetUnlink,
		durabilitycut.BeforeDeletionDirectorySync,
	}
	if !reflect.DeepEqual(points, wantPoints) {
		t.Fatalf("cut points=%v, want %v", points, wantPoints)
	}
	if !db.commandWALCleanupNamespaceDirty.Load() {
		t.Fatal("directory-sync failure lost pending command-WAL namespace debt")
	}
	if got := db.commandWALCleanupRemoved.Load(); got != 0 {
		t.Fatalf("removed metric=%d before durable directory sync, want 0", got)
	}
	if got := db.commandWALCleanupUnlinkedPending.Load(); got != 1 {
		t.Fatalf("unlinked pending metric=%d, want 1", got)
	}
	syncDirFn = originalSyncDir
	if err := db.CleanupCommandWALCoveredSegments(true); err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if got := db.commandWALCleanupRemoved.Load(); got != 1 {
		t.Fatalf("removed metric after durable directory sync=%d, want 1", got)
	}
	if got := db.commandWALCleanupUnlinkedPending.Load(); got != 0 {
		t.Fatalf("unlinked pending metric after retry=%d, want 0", got)
	}
}

func TestCommandWALCleanupAfterUnlinkFailureRetainsNamespaceSyncDebt(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	installCommandWALCleanupRootForTest(t, db, 1, 1)
	wantErr := errors.New("injected after-unlink cut")
	fired := false
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if !fired && event.Point == durabilitycut.AfterWALOrAssetUnlink && event.Resource == durabilitycut.ResourceCommandWAL {
			fired = true
			return wantErr
		}
		return nil
	})
	err := db.CleanupCommandWALCoveredSegments(true)
	restore()
	if !errors.Is(err, wantErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	if !fired || !db.commandWALCleanupNamespaceDirty.Load() {
		t.Fatalf("after-unlink fired=%t namespaceDirty=%t, want retained sync debt", fired, db.commandWALCleanupNamespaceDirty.Load())
	}
	if got := db.commandWALCleanupRemoved.Load(); got != 0 {
		t.Fatalf("removed metric=%d before durable directory sync, want 0", got)
	}
	if _, statErr := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(statErr) {
		t.Fatalf("covered segment stat after after-unlink cut=%v, want absent", statErr)
	}

	var retryPoints []durabilitycut.Point
	restore = durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL {
			retryPoints = append(retryPoints, event.Point)
		}
		return nil
	})
	err = db.CleanupCommandWALCoveredSegments(true)
	restore()
	if err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if db.commandWALCleanupNamespaceDirty.Load() {
		t.Fatal("cleanup retry left command-WAL namespace sync debt pending")
	}
	if got := db.commandWALCleanupRemoved.Load(); got != 1 {
		t.Fatalf("removed metric after retry=%d, want 1", got)
	}
	wantRetryPoints := []durabilitycut.Point{
		durabilitycut.BeforeDeletionDirectorySync,
		durabilitycut.AfterDeletionDirectorySync,
	}
	if !reflect.DeepEqual(retryPoints, wantRetryPoints) {
		t.Fatalf("retry cut points=%v, want pending namespace sync only %v", retryPoints, wantRetryPoints)
	}
}

func TestCommandWALCleanupAfterDirectorySyncFailureRetriesConservatively(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	installCommandWALCleanupRootForTest(t, db, 1, 1)
	wantErr := errors.New("injected after-directory-sync cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterDeletionDirectorySync && event.Resource == durabilitycut.ResourceCommandWAL {
			return wantErr
		}
		return nil
	})
	err := db.CleanupCommandWALCoveredSegments(true)
	restore()
	if !errors.Is(err, wantErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	if !db.commandWALCleanupNamespaceDirty.Load() {
		t.Fatal("after-directory-sync reporting failure lost conservative retry debt")
	}
	if got := db.commandWALCleanupRemoved.Load(); got != 0 {
		t.Fatalf("removed metric=%d before successful deletion reporting, want 0", got)
	}
	if err := db.CleanupCommandWALCoveredSegments(true); err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if db.commandWALCleanupNamespaceDirty.Load() {
		t.Fatal("cleanup retry left namespace sync debt pending")
	}
	if got := db.commandWALCleanupRemoved.Load(); got != 1 {
		t.Fatalf("removed metric after successful deletion reporting=%d, want 1", got)
	}
}

func TestCommandWALCleanupPartialUnlinkFailureRequiresRecoveryWithoutSync(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)
	writeCommandWALFrame(t, dir, 3, 3)

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
	db.state.Store(&DBState{AppliedCommandLSN: 2})
	installCommandWALCleanupRootForTest(t, db, 1, 2)

	wantErr := errors.New("injected second-unlink cut")
	var unlinkAttempts int
	var points []durabilitycut.Point
	restoreObserver := durabilitycut.Install(func(event durabilitycut.Event) error {
		points = append(points, event.Point)
		if event.Point == durabilitycut.BeforeWALOrAssetUnlink {
			unlinkAttempts++
			if unlinkAttempts == 2 {
				return wantErr
			}
		}
		return nil
	})
	defer restoreObserver()

	err := db.CleanupCommandWALCoveredSegments(true)
	if !errors.Is(err, wantErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CleanupCommandWALCoveredSegments error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	wantPoints := []durabilitycut.Point{
		durabilitycut.BeforeWALOrAssetUnlink,
		durabilitycut.AfterWALOrAssetUnlink,
		durabilitycut.BeforeWALOrAssetUnlink,
	}
	if !reflect.DeepEqual(points, wantPoints) {
		t.Fatalf("cut points=%v, want %v", points, wantPoints)
	}
	if _, statErr := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(statErr) {
		t.Fatalf("first covered segment stat=%v, want removed", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log")); statErr != nil {
		t.Fatalf("second covered segment stat=%v, want present", statErr)
	}
}

func TestCommandWALCheckpointScanDoesNotRemoveCoveredSegment(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)

	decisions, err := scanCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("scanCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	decisionByName := map[string]commandWALSegmentCleanupDecision{}
	for _, decision := range decisions {
		decisionByName[filepath.Base(decision.Path)] = decision
	}
	if got := decisionByName["commit-l0-000001.log"]; !got.Covered || got.Active || got.Removed {
		t.Fatalf("covered segment scan decision=%+v, want covered non-active not removed", got)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("covered segment stat after scan=%v, want present", err)
	}

	decisions, err = removeCoveredCommandWALSegments(decisions)
	if err != nil {
		t.Fatalf("removeCoveredCommandWALSegments: %v", err)
	}
	removed := false
	for _, decision := range decisions {
		if filepath.Base(decision.Path) == "commit-l0-000001.log" {
			removed = decision.Removed
		}
	}
	if !removed {
		t.Fatalf("covered segment was not marked removed: %+v", decisions)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat after removal=%v, want removed", err)
	}
}

func TestCommandWALCleanupClosesScanHandleBeforeUnlink(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)

	decisions, err := scanCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("scanCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	defer closeCommandWALCleanupDecisions(decisions)
	target := filepath.Join(WALDirPath(dir), "commit-l0-000001.log")
	foundOpenScanHandle := false
	for i := range decisions {
		if filepath.Clean(decisions[i].Path) == filepath.Clean(target) {
			foundOpenScanHandle = decisions[i].file != nil
		}
	}
	if !foundOpenScanHandle {
		t.Fatal("covered segment scan did not retain its discovery handle")
	}

	sawBeforeUnlink := false
	restoreObserver := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.BeforeWALOrAssetUnlink || filepath.Clean(event.Path) != filepath.Clean(target) {
			return nil
		}
		sawBeforeUnlink = true
		for i := range decisions {
			if filepath.Clean(decisions[i].Path) == filepath.Clean(target) && decisions[i].file != nil {
				return errors.New("command WAL scan handle remained open before unlink")
			}
		}
		return nil
	})
	defer restoreObserver()

	decisions, err = removeCoveredCommandWALSegments(decisions)
	if err != nil {
		t.Fatalf("removeCoveredCommandWALSegments: %v", err)
	}
	if !sawBeforeUnlink {
		t.Fatal("cleanup did not reach the pre-unlink boundary")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat after removal=%v, want removed", err)
	}
}

func TestCommandWALCleanupRetainedLineageDoesNotHoldScanHandles(t *testing.T) {
	dir := t.TempDir()
	const segmentCount = 96
	for lsn := uint64(1); lsn <= segmentCount; lsn++ {
		writeCommandWALFrame(t, dir, lsn, lsn)
	}

	decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 1, segmentCount, 0)
	if err != nil {
		t.Fatalf("scanCommandWALSegmentsForCleanupProof: %v", err)
	}
	defer closeCommandWALCleanupDecisions(decisions)

	openHandles := 0
	deletionCandidates := 0
	for _, decision := range decisions {
		if decision.file != nil {
			openHandles++
		}
		if decision.Covered && !decision.Active {
			deletionCandidates++
			if decision.file == nil {
				t.Fatalf("deletion candidate %s lost discovery handle", filepath.Base(decision.Path))
			}
			continue
		}
		if decision.file != nil {
			t.Fatalf("retained segment %s kept a scan handle", filepath.Base(decision.Path))
		}
	}
	if deletionCandidates != 1 {
		t.Fatalf("deletion candidates=%d, want 1", deletionCandidates)
	}
	if openHandles != deletionCandidates {
		t.Fatalf("open scan handles=%d, want deletion candidates=%d", openHandles, deletionCandidates)
	}
}

func TestCommandWALCheckpointCleanupRejectsFilenameReuse(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)

	path := filepath.Join(WALDirPath(dir), "commit-l0-000001.log")
	oldPath := path + ".old"
	rebound := false
	restoreObserver := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.BeforeWALOrAssetUnlink || rebound {
			return nil
		}
		rebound = true
		if err := os.Rename(path, oldPath); err != nil {
			return err
		}
		writeCommandWALFrame(t, dir, 1, 1)
		return nil
	})
	defer restoreObserver()

	_, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("cleanup error=%v, want ErrRecoveryRequired for rebound filename", err)
	}
	if !rebound {
		t.Fatal("cleanup did not reach filename reuse hook")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("replacement command WAL segment was removed: %v", err)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("original retained segment missing after rebind: %v", err)
	}
}

func TestCommandWALCleanupRetainsExactActiveIdentityAfterFilenameRebind(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not permit rebinding an open command-WAL segment")
	}
	dir := t.TempDir()
	journal, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
		Lane:       0,
		SegmentSeq: 2,
		InitialLSN: 0,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournal: %v", err)
	}
	defer func() { _ = journal.Close() }()
	if _, err := journal.AppendCommand(commitlog.CommandEnvelope{
		Kind:          commitlog.CommandKindRawKVBatch,
		Scope:         commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	}); err != nil {
		t.Fatalf("AppendCommand: %v", err)
	}

	activePath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2))
	reboundPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 0))
	if err := os.Rename(activePath, reboundPath); err != nil {
		t.Fatalf("rename active segment: %v", err)
	}
	if err := os.WriteFile(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1)), nil, 0o600); err != nil {
		t.Fatalf("write misleading higher-sequence segment: %v", err)
	}

	db := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable, commandJournal: journal}
	installCommandWALCleanupRootForTest(t, db, 1, 1)
	if err := db.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatalf("CleanupCommandWALCoveredSegments: %v", err)
	}
	if _, err := os.Stat(reboundPath); err != nil {
		t.Fatalf("exact active command-WAL identity was removed: %v", err)
	}
}

func TestCommandWALCheckpointCleanupRetainsActiveCoveredSegment(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)
	writeLegacyRawWALFrame(t, dir, 999, 999)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 2, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	decisionByName := map[string]commandWALSegmentCleanupDecision{}
	for _, decision := range decisions {
		decisionByName[filepath.Base(decision.Path)] = decision
	}
	if got := decisionByName["commit-l0-000001.log"]; !got.Covered || got.Active || !got.Removed {
		t.Fatalf("old covered segment decision=%+v, want covered non-active removed", got)
	}
	if got := decisionByName["commit-l0-000002.log"]; !got.Covered || got.Active || !got.Removed {
		t.Fatalf("covered segment before legacy barrier decision=%+v, want covered non-active removed", got)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000002.log")); !os.IsNotExist(err) {
		t.Fatalf("covered segment stat=%v, want removed", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000999.log")); err != nil {
		t.Fatalf("legacy raw WAL segment stat=%v, want retained", err)
	}
}

func TestCommandWALCheckpointCleanupRetainsActiveCoveredTypedSegment(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	if len(decisions) != 1 {
		t.Fatalf("len(decisions)=%d, want 1", len(decisions))
	}
	if got := decisions[0]; !got.Covered || !got.Active || got.Removed {
		t.Fatalf("active covered typed segment decision=%+v, want covered active retained", got)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), "commit-l0-000001.log")); err != nil {
		t.Fatalf("active covered typed segment stat=%v, want retained", err)
	}
}

func TestCommandWALCheckpointCleanupValidatesFullyUnappliedSegmentLineage(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALSegmentFrames(t, dir, 2, 2, 3, 4)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	decisionByName := map[string]commandWALSegmentCleanupDecision{}
	for _, decision := range decisions {
		decisionByName[filepath.Base(decision.Path)] = decision
	}
	if got := decisionByName["commit-l0-000001.log"]; !got.Covered || got.Active || !got.Removed {
		t.Fatalf("covered segment decision=%+v, want removed", got)
	}
	if got := decisionByName["commit-l0-000002.log"]; got.Covered || !got.Active || got.Removed || got.MinLSN != 2 || got.MaxLSN != 4 {
		t.Fatalf("unapplied segment decision=%+v, want fully validated active range [2,4]", got)
	}
	got := decisionByName["commit-l0-000002.log"]
	if got.Frames != 3 {
		t.Fatalf("unapplied segment scanned frames=%d, want all 3 frames", got.Frames)
	}
	if got.ScannedBytes != got.Size {
		t.Fatalf("unapplied segment scanned bytes=%d size=%d, want full segment validation", got.ScannedBytes, got.Size)
	}
}

func TestCommandWALCheckpointCleanupReturnsScanErrors(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALSegmentFrames(t, dir, 1, 2, 1)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 2, 0)
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN error=%v, want duplicate LSN", err)
	}
	if len(decisions) != 1 || decisions[0].Error == "" {
		t.Fatalf("cleanup decisions=%+v, want recorded scan error", decisions)
	}
}

func TestCommandWALCheckpointCleanupAllowsCoveredDuplicateLSNAcrossSegments(t *testing.T) {
	// Covered duplicates are no longer replay inputs. Exact-identity cleanup
	// may remove the inactive copy while preserving the active append target.
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 1)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 1, 0)
	if err != nil {
		t.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
	}
	var removed int
	for _, d := range decisions {
		if d.Removed {
			removed++
		}
	}
	if removed != 1 {
		t.Fatalf("cleanup removed=%d segments, want inactive duplicate only", removed)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))); !os.IsNotExist(err) {
		t.Fatalf("inactive duplicate stat=%v, want removed", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2))); err != nil {
		t.Fatalf("active duplicate stat=%v, want retained", err)
	}
}

func TestCommandWALCheckpointCleanupAllowsCoveredLineageGap(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 3)

	decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 3, 0)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	removed := 0
	for _, decision := range decisions {
		if decision.Removed {
			removed++
		}
	}
	if removed != 1 {
		t.Fatalf("cleanup removed=%d segments, want inactive covered prefix", removed)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))); !os.IsNotExist(err) {
		t.Fatalf("covered prefix stat=%v, want removed", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2))); err != nil {
		t.Fatalf("active covered segment stat=%v, want retained", err)
	}
}

func TestCommandWALCheckpointCleanupRejectsRetainedReplayDuplicate(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 2)
	writeCommandWALSegmentFramesForLane(t, dir, 1, 1, 2)

	decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 1, 2, 0)
	defer closeCommandWALCleanupDecisions(decisions)
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("cleanup proof scan error=%v, want duplicate retained replay LSN", err)
	}
	for _, decision := range decisions {
		if decision.Removed {
			t.Fatalf("decision=%+v, want no removal before exact replay proof", decision)
		}
	}
}

func TestCommandWALCheckpointCleanupRejectsRetainedReplayGap(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALFrame(t, dir, 1, 1)
	writeCommandWALFrame(t, dir, 2, 3)

	decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 1, 3, 0)
	defer closeCommandWALCleanupDecisions(decisions)
	if !errors.Is(err, ErrCommandWALAppliedLSNNonContig) {
		t.Fatalf("cleanup proof scan error=%v, want retained replay gap", err)
	}
	for _, decision := range decisions {
		if decision.Removed {
			t.Fatalf("decision=%+v, want no removal before exact replay proof", decision)
		}
	}
}

func TestCommandWALCheckpointCleanupConvergesAcrossInterleavedLanePrefixes(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALSegmentFramesForLane(t, dir, 0, 1, 1, 3, 5)
	writeCommandWALSegmentFramesForLane(t, dir, 0, 2, 7)
	writeCommandWALSegmentFramesForLane(t, dir, 1, 1, 2, 4, 6)
	writeCommandWALSegmentFramesForLane(t, dir, 1, 2, 8)

	if _, err := cleanupCommandWALSegmentsWithProof(dir, 5, 8, 0); err != nil {
		t.Fatalf("first cleanup through LSN 5: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))); !os.IsNotExist(err) {
		t.Fatalf("lane 0 covered prefix stat=%v, want removed", err)
	}

	if _, err := cleanupCommandWALSegmentsWithProof(dir, 6, 8, 0); err != nil {
		t.Fatalf("second cleanup through LSN 6: %v", err)
	}
	if _, err := os.Stat(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(1, 1))); !os.IsNotExist(err) {
		t.Fatalf("lane 1 covered prefix stat=%v, want removed", err)
	}
}

func TestCommandWALSegmentMaxLSNStreamsFrames(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALSegmentFrames(t, dir, 1, 1, 2, 3)

	path := filepath.Join(WALDirPath(dir), "commit-l0-000001.log")
	maxLSN, typed, err := commandWALSegmentMaxLSN(path, 0, true)
	if err != nil {
		t.Fatalf("commandWALSegmentMaxLSN: %v", err)
	}
	if !typed || maxLSN != 3 {
		t.Fatalf("typed=%t maxLSN=%d, want typed maxLSN=3", typed, maxLSN)
	}
}

func TestCommandWALSegmentClassifierRequiresParsedName(t *testing.T) {
	for _, tc := range []struct {
		name string
		want bool
	}{
		{name: "commit-l0-000001.log", want: true},
		{name: "commit-lane-readme.log", want: false},
		{name: "commit-l-foo.log", want: false},
		{name: "commit-l0-000001.tmp", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := isCommandWALLaneSegment(logSegment{path: filepath.Join(t.TempDir(), tc.name)})
			if got != tc.want {
				t.Fatalf("isCommandWALLaneSegment(%q)=%t, want %t", tc.name, got, tc.want)
			}
		})
	}
}

func modelValidateContiguousAppliedCommandLSN(current, next uint64, covered []CommandWALLSNRange) error {
	if next < current {
		return ErrCommandWALAppliedLSNRegression
	}
	if next == current {
		if len(covered) != 0 {
			return ErrCommandWALAppliedLSNNonContig
		}
		return nil
	}
	if current == ^uint64(0) || len(covered) == 0 {
		return ErrCommandWALAppliedLSNNonContig
	}
	ranges := append([]CommandWALLSNRange(nil), covered...)
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].First != ranges[j].First {
			return ranges[i].First < ranges[j].First
		}
		return ranges[i].Last < ranges[j].Last
	})
	cursor := current + 1
	for _, r := range ranges {
		if r.First == 0 || r.Last < r.First {
			return ErrCommandWALAppliedLSNNonContig
		}
		if r.Last < cursor {
			continue
		}
		if r.First > cursor {
			return ErrCommandWALAppliedLSNNonContig
		}
		if r.Last >= next {
			return nil
		}
		if r.Last == ^uint64(0) {
			return ErrCommandWALAppliedLSNNonContig
		}
		nextCursor := r.Last + 1
		if nextCursor <= r.Last {
			return ErrCommandWALAppliedLSNNonContig
		}
		cursor = nextCursor
	}
	return ErrCommandWALAppliedLSNNonContig
}

func TestCommandWALSegmentMaxLSNFailsClosedOnNonIncreasingLSN(t *testing.T) {
	dir := t.TempDir()
	writeCommandWALSegmentFrames(t, dir, 1, 1, 1)

	path := filepath.Join(WALDirPath(dir), "commit-l0-000001.log")
	_, typed, err := commandWALSegmentMaxLSN(path, 0, true)
	if !errors.Is(err, commitlog.ErrCommandWALDuplicateLSN) {
		t.Fatalf("commandWALSegmentMaxLSN error=%v, want ErrCommandWALDuplicateLSN", err)
	}
	if !typed {
		t.Fatalf("typed=false, want true for duplicate typed command segment")
	}
}

func writeCommandWALFrame(t *testing.T, dir string, segmentSeq uint64, lsn uint64) {
	t.Helper()
	writeCommandWALSegmentFrames(t, dir, segmentSeq, lsn)
}

func writeCommandWALSegmentFrames(t *testing.T, dir string, segmentSeq uint64, lsns ...uint64) {
	t.Helper()
	writeCommandWALSegmentFramesForLane(t, dir, 0, segmentSeq, lsns...)
}

func writeCommandWALSegmentFramesForLane(t *testing.T, dir string, lane int, segmentSeq uint64, lsns ...uint64) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(lane, segmentSeq))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, lsn := range lsns {
		if err := w.AppendCommand(commitlog.CommandEnvelope{
			LSN:           lsn,
			Kind:          commitlog.CommandKindRawKVBatch,
			Scope:         commitlog.CommandScopeRawKV,
			PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
		}); err != nil {
			_ = w.Close()
			t.Fatalf("AppendCommand: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func writeLegacyRawWALFrame(t *testing.T, dir string, segmentSeq uint64, seq uint64) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, segmentSeq))
	w, err := commitlog.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter legacy raw: %v", err)
	}
	if err := w.AppendBatch([]commitlog.Record{{
		Op:    commitlog.OpSetInline,
		Key:   []byte("legacy-key"),
		Value: []byte("legacy-value"),
		Seq:   seq,
	}}); err != nil {
		_ = w.Close()
		t.Fatalf("AppendBatch legacy raw: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close legacy raw writer: %v", err)
	}
}

func writePartialCommandWALSegmentTail(t *testing.T, dir string, segmentSeq uint64, tail []byte) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, commitlog.CommandSegmentName(0, segmentSeq))
	if err := os.WriteFile(path, tail, 0o600); err != nil {
		t.Fatalf("WriteFile partial command WAL tail: %v", err)
	}
}

func writePartialLegacyAliasWALSegmentTail(t *testing.T, dir string, segmentSeq uint64, tail []byte) {
	t.Helper()
	walDir := WALDirPath(dir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll wal: %v", err)
	}
	path := filepath.Join(walDir, fmt.Sprintf("commit-%06d.log", segmentSeq))
	if err := os.WriteFile(path, tail, 0o600); err != nil {
		t.Fatalf("WriteFile partial legacy WAL alias tail: %v", err)
	}
}

func appendCommandWALTail(t *testing.T, dir string, segmentSeq uint64, tail []byte) {
	t.Helper()
	path := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, segmentSeq))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile append command WAL tail: %v", err)
	}
	if _, err := f.Write(tail); err != nil {
		_ = f.Close()
		t.Fatalf("Write command WAL tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close command WAL tail: %v", err)
	}
}

func corruptIndexPageByte(t *testing.T, dir string, pageID uint64) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, indexFileName), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile index: %v", err)
	}
	defer f.Close()
	off := int64(pageID*page.PageSize + page.PageHeaderSize + 7)
	if _, err := f.WriteAt([]byte{0xff}, off); err != nil {
		t.Fatalf("WriteAt corrupt meta page: %v", err)
	}
}

func tamperDurableMetaProjectionBytes(t *testing.T, dir string, pageID uint64, value uint64) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, indexFileName), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile index: %v", err)
	}
	defer f.Close()
	buf := make([]byte, page.PageSize)
	off := int64(pageID * page.PageSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		t.Fatalf("ReadAt meta page: %v", err)
	}
	binary.LittleEndian.PutUint64(buf[page.PageHeaderSize+60:page.PageHeaderSize+68], value)
	node.NewNode(buf).UpdateChecksum()
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatalf("WriteAt meta page: %v", err)
	}
}
