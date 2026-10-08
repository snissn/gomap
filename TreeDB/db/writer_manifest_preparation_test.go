package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func openWriterManifestTestDB(t *testing.T) (*DB, *multiReportedLeafPageLog) {
	t.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact stable manifest namespace unavailable")
	}
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexOuterLeavesInValueLog: true, IndexPackedValuePtr: true, DisableBackgroundPrune: true}
	if err := SaveFormatConfig(dir, formatConfigFromOptions(opts)); err != nil {
		t.Fatal(err)
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	segment, writer := openLeafPageLogTestWriter(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 1)
	log := &multiReportedLeafPageLog{appendSegment: segment, writer: writer, currentSegments: []LeafPageLogSegment{segment}, createdSegments: []LeafPageLogSegment{segment}}
	database.SetLeafPageLog(log)
	t.Cleanup(func() { _ = database.Close(); _ = log.Close() })
	return database, log
}

func writerManifestTestBatch(t *testing.T, database *DB, key string) *Batch {
	t.Helper()
	b := database.NewPhysicalBatch().(*Batch)
	for i := 0; i < 128; i++ {
		k := append([]byte(key), bytes.Repeat([]byte{byte(i + 1)}, 120)...)
		if err := b.Set(k, bytes.Repeat([]byte("value"), 10)); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func assertWriterPreparationLocksAvailable(t *testing.T, database *DB) {
	t.Helper()
	checks := []struct {
		name    string
		acquire func() bool
		release func()
	}{
		{"write", database.writeMu.TryLock, database.writeMu.Unlock},
		{"commit", database.commitMu.TryLock, database.commitMu.Unlock},
		{"root reuse", database.rootReuseMu.TryLock, database.rootReuseMu.Unlock},
		{"durable publication", database.durablePublishMu.TryLock, database.durablePublishMu.Unlock},
		{"publish prepare", database.publishPrepareMu.TryLock, database.publishPrepareMu.Unlock},
		{"DB", database.mu.TryLock, database.mu.Unlock},
	}
	for _, check := range checks {
		if !check.acquire() {
			t.Errorf("%s retained during detached manifest preparation", check.name)
		} else {
			check.release()
		}
	}
}

func TestWriterManifestPreparationDriftAbandonsExactRevision(t *testing.T) {
	for _, drift := range []string{"manifest", "pending", "root"} {
		t.Run(drift, func(t *testing.T) {
			database, log := openWriterManifestTestDB(t)
			b := writerManifestTestBatch(t, database, "unaccepted")
			before := database.State()
			beforeNames, _ := filepath.Glob(filepath.Join(LeafLogDirPath(database.dir), "manifest.durable.*.json"))
			existed := make(map[string]bool, len(beforeNames))
			for _, name := range beforeNames {
				existed[name] = true
			}
			preparedNames := []string(nil)
			database.testWriterManifestPreparedHook = func() {
				assertWriterPreparationLocksAvailable(t, database)
				preparedNames, _ = filepath.Glob(filepath.Join(LeafLogDirPath(database.dir), "manifest.durable.*.json"))
				database.writeMu.Lock()
				database.mu.Lock()
				switch drift {
				case "manifest":
					database.leafGenerationManifest = database.leafGenerationManifest.clone()
				case "pending":
					database.queueLeafGenerationWritableFileIDAtCommit(log.appendSegment.FileID, 999)
				case "root":
					database.meta.CommitSeq++
				}
				database.mu.Unlock()
				database.writeMu.Unlock()
			}
			err := b.writeSerializedAttempt(true, nil, database.assignBatchEntryRevisions(b.batch), nil)
			database.testWriterManifestPreparedHook = nil
			if !errors.Is(err, errDurableRootCandidateStale) {
				t.Fatalf("drift returned %v", err)
			}
			if drift == "root" {
				database.mu.Lock()
				database.meta.CommitSeq = before.CommitSeq
				database.mu.Unlock()
			}
			if database.State().RootPageID != before.RootPageID {
				t.Fatal("abandoned output became visible")
			}
			if len(database.snapshotLeafGenerationPendingFileIDs(0)) == 0 {
				t.Fatal("abandon consumed pending producer ownership")
			}
			if len(preparedNames) == 0 {
				t.Fatal("test never prepared an immutable manifest")
			}
			// Only the attempt's new revision is abandoned; older authority survives.
			foundNew := false
			for _, name := range preparedNames {
				if existed[name] {
					continue
				}
				foundNew = true
				if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unpublished revision remains %s: %v", name, err)
				}
			}
			if !foundNew {
				t.Fatal("no new immutable revision was observed")
			}
		})
	}
}

func TestWriterManifestUnchangedMembershipDoesNotSyncReplacement(t *testing.T) {
	database, _ := openWriterManifestTestDB(t)
	first := writerManifestTestBatch(t, database, "same-producer")
	if err := first.WriteSync(); err != nil {
		t.Fatal(err)
	}
	store := database.leafGenerationManifestStore
	revision := database.leafGenerationManifest.ManifestRevision
	content, namespace := store.durabilityCounters.ContentSyncs.Load(), store.durabilityCounters.NamespaceSyncs.Load()
	second := writerManifestTestBatch(t, database, "same-producer")
	if err := second.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if database.leafGenerationManifest.ManifestRevision != revision {
		t.Fatal("unchanged membership minted another immutable revision")
	}
	if store.durabilityCounters.ContentSyncs.Load() != content || store.durabilityCounters.NamespaceSyncs.Load() != namespace {
		t.Fatal("unchanged membership performed manifest durability I/O")
	}
}

func TestWriterManifestGroupCloseWaitsForFinalizingOwner(t *testing.T) {
	database, _ := openWriterManifestTestDB(t)
	group, err := database.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	b := writerManifestTestBatch(t, database, "group")
	if err := b.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	database.testWriterManifestPreparedHook = func() {
		assertWriterPreparationLocksAvailable(t, database)
		if !group.mu.TryLock() {
			t.Error("group mutex retained during detached preparation")
		} else {
			group.mu.Unlock()
		}
		close(entered)
		<-resume
	}
	writeDone := make(chan error, 1)
	go func() { writeDone <- b.WriteSync() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not reach detached preparation")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- group.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close abandoned finalizing owner: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("finalizing writer did not finish")
	}
	database.testWriterManifestPreparedHook = nil
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join finalizing owner")
	}
	if !group.Accepted() {
		t.Fatal("Close lost the accepted receipt")
	}
}

func TestWriterManifestFailedPublishRetainsPendingAndAbandonsToken(t *testing.T) {
	database, _ := openWriterManifestTestDB(t)
	b := writerManifestTestBatch(t, database, "failed")
	beforeNames, _ := filepath.Glob(filepath.Join(LeafLogDirPath(database.dir), "manifest.durable.*.json"))
	database.testFailFinalizeCommit.Store(true)
	err := b.writeSerializedAttempt(true, nil, database.assignBatchEntryRevisions(b.batch), nil)
	database.testFailFinalizeCommit.Store(false)
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("failure returned %v", err)
	}
	if len(database.snapshotLeafGenerationPendingFileIDs(0)) == 0 {
		t.Fatal("failure consumed pending ownership")
	}
	names, err := filepath.Glob(filepath.Join(LeafLogDirPath(database.dir), "manifest.durable.*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, beforeNames) {
		t.Fatalf("immutable revisions changed after abandon: before=%v after=%v", beforeNames, names)
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(database.snapshotLeafGenerationPendingFileIDs(0)) != 0 {
		t.Fatal("accepted retry did not consume exact pending ownership")
	}
}

// The ordinary intent keeps the outer raw-publication guard, but encoding,
// dependency synchronization and journal append must release root serialization.
func TestWriterManifestAssignedWALFailureRequiresRecoveryWithoutSecondAppend(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	b := database.NewBatch().(*Batch)
	defer b.Close()
	if err := b.Set([]byte("assigned"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	unlock, err := database.LockCommandWALPublishWithBarriers()
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	revision := database.assignBatchEntryRevisions(b.batch)
	intent, err := database.prepareRawKVCommandWALIntent(b, true)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseUnassignedCommandWALIntent(intent)
	reachedAppend := false
	intent.rawKVFinalize = func(_ []byte, _ func(page.ValuePtr) (uint64, bool)) error {
		reachedAppend = true
		assertWriterPreparationLocksAvailable(t, database)
		database.testFailFinalizeCommit.Store(true)
		return nil
	}
	before := *database.State()
	err = b.writeSerializedAttempt(true, intent, revision, nil)
	database.testFailFinalizeCommit.Store(false)
	if !reachedAppend || intent.lsn == 0 {
		t.Fatalf("append reached=%v assigned=%d error=%v", reachedAppend, intent.lsn, err)
	}
	if !errors.Is(err, errTestFinalizeCommitFailpoint) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("assigned failure returned %v", err)
	}
	if !database.commandWALFlushPoisoned.Load() {
		t.Fatal("assigned unpublished command did not poison handle")
	}
	if database.State().RootPageID != before.RootPageID {
		t.Fatal("failed command output became visible")
	}
	next := database.CommandWALNextLSN()
	err = b.writeSerializedAttempt(true, intent, revision, nil)
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("retry returned %v", err)
	}
	if database.CommandWALNextLSN() != next {
		t.Fatal("retry appended a second command")
	}
}
