package raftfsm

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestRaftSnapshotV1InstallEmptyTargetPreservesDigestAndValueLogPointers(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, true)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	largeDoc := []byte(`{"_id":"u-large","name":"Ada","payload":"` + stringsRepeatForSnapshotTest("x", 8192) + `"}`)
	applySnapshotSourceEntries(t, sourceFSM, largeDoc)
	assertSnapshotValueLogHasFile(t, raftcluster.ValueLogDir(sourceDir))

	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}
	sourceDigest, err := sourceFSM.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("source LogicalDigestV1: %v", err)
	}

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()

	if err := targetFSM.InstallRaftSnapshotV1(bytes.NewReader(snapshot.Payload)); err != nil {
		t.Fatalf("InstallRaftSnapshotV1: %v", err)
	}
	if err := targetFSM.VerifyInstalledSnapshotManifestV1(snapshot.Manifest); err != nil {
		t.Fatalf("VerifyInstalledSnapshotManifestV1: %v", err)
	}
	targetDigest, err := targetFSM.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("target LogicalDigestV1: %v", err)
	}
	if targetDigest != sourceDigest {
		t.Fatalf("digest mismatch after snapshot install: target=%s source=%s", targetDigest.Hex(), sourceDigest.Hex())
	}
	assertSnapshotValueLogHasFile(t, raftcluster.ValueLogDir(targetDir))
	assertSnapshotDocument(t, targetFSM, "u-large", largeDoc)
}

func TestRaftSnapshotV1InstallReplacesStaleReplica(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	sourceDoc := []byte(`{"_id":"u-large","name":"source"}`)
	applySnapshotSourceEntries(t, sourceFSM, sourceDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	staleCreate := deterministicCreateCollectionEntry(t, "stale", "fsm:snapshot:stale:create")
	result, err := targetFSM.ApplyCommittedEntryV1(committedCommand(1, 1, staleCreate))
	if err != nil {
		t.Fatalf("target stale ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)

	if err := targetFSM.InstallRaftSnapshotV1(bytes.NewReader(snapshot.Payload)); err != nil {
		t.Fatalf("InstallRaftSnapshotV1 over stale target: %v", err)
	}
	if err := targetFSM.VerifyInstalledSnapshotManifestV1(snapshot.Manifest); err != nil {
		t.Fatalf("VerifyInstalledSnapshotManifestV1: %v", err)
	}
	assertSnapshotDocument(t, targetFSM, "u-large", sourceDoc)
	if _, err := collections.NewCollectionManager(targetFSM.db).OpenCollection("stale"); err == nil {
		t.Fatalf("stale collection survived snapshot restore")
	}
}

func TestRaftSnapshotV1TailReplayMatchesSourceDigest(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	initialDoc := []byte(`{"_id":"u1","name":"before"}`)
	applySnapshotSourceEntries(t, sourceFSM, initialDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}
	tailDoc := []byte(`{"_id":"u1","name":"after"}`)
	tail := committedCommand(3, snapshot.Manifest.LastIncludedIndex+1, deterministicReplaceBatchEntry(t, "users", "fsm:snapshot:tail:replace", nativewire.DocumentFormatJSON, [][]byte{[]byte("u1")}, [][]byte{tailDoc}))
	result, err := sourceFSM.ApplyCommittedEntryV1(tail)
	if err != nil {
		t.Fatalf("source tail ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	sourceDigest, err := sourceFSM.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("source LogicalDigestV1 after tail: %v", err)
	}

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	if err := targetFSM.InstallRaftSnapshotV1(bytes.NewReader(snapshot.Payload)); err != nil {
		t.Fatalf("InstallRaftSnapshotV1: %v", err)
	}
	result, err = targetFSM.ApplyCommittedEntryV1(tail)
	if err != nil {
		t.Fatalf("target tail ApplyCommittedEntryV1: %v result=%+v", err, result)
	}
	assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
	targetDigest, err := targetFSM.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatalf("target LogicalDigestV1 after tail: %v", err)
	}
	if targetDigest != sourceDigest {
		t.Fatalf("digest mismatch after tail replay: target=%s source=%s", targetDigest.Hex(), sourceDigest.Hex())
	}
	assertSnapshotDocument(t, targetFSM, "u1", tailDoc)
}

func BenchmarkRaftSnapshotV1ExportInstall(b *testing.B) {
	root := b.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(b, sourceDir, true)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(b, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()
	largeDoc := []byte(`{"_id":"u-large","name":"Ada","payload":"` + stringsRepeatForSnapshotTest("x", 8192) + `"}`)
	applySnapshotSourceEntries(b, sourceFSM, largeDoc)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		targetDir := filepath.Join(root, "target", strconv.Itoa(i))
		targetDB := openRaftSnapshotFSMTestDB(b, targetDir, true)
		targetFSM := openRaftSnapshotFSMForTest(b, targetDB, targetDir, true)
		b.StartTimer()
		snapshot, err := sourceFSM.ExportRaftSnapshotV1()
		if err != nil {
			b.Fatalf("ExportRaftSnapshotV1: %v", err)
		}
		if err := targetFSM.InstallRaftSnapshotV1(bytes.NewReader(snapshot.Payload)); err != nil {
			b.Fatalf("InstallRaftSnapshotV1: %v", err)
		}
		b.StopTimer()
		_ = targetFSM.Close()
	}
}

func openRaftSnapshotFSMTestDB(t testing.TB, dir string, forceValuePointers bool) *backenddb.DB {
	t.Helper()
	opts := backenddb.Options{
		Dir:                          dir,
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	}
	if forceValuePointers {
		opts.ValueLog = backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		}
	}
	db, err := backenddb.Open(opts)
	if err != nil {
		t.Fatalf("Open DB: %v", err)
	}
	return db
}

func openRaftSnapshotFSMForTest(t testing.TB, db *backenddb.DB, dbDir string, forceValuePointers bool) *FSM {
	t.Helper()
	restoreOpts := backenddb.Options{
		CommandWAL:                   true,
		CommandWALStatsScan:          true,
		CommandWALSegmentTargetBytes: 1 << 20,
		DisableBackgroundPrune:       true,
	}
	if forceValuePointers {
		restoreOpts.ValueLog = backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		}
	}
	fsm, err := Open(Options{
		DB:                       db,
		Cluster:                  validFSMClusterConfig(dbDir),
		SnapshotRestoreDBOptions: restoreOpts,
		StoreOptions: raftapply.DurableApplyStoreOptions{
			DisableSync:          true,
			AllowInitialIndexGap: true,
		},
	})
	if err != nil {
		t.Fatalf("Open FSM: %v", err)
	}
	return fsm
}

func applySnapshotSourceEntries(tb testing.TB, fsm *FSM, doc []byte) {
	tb.Helper()
	create := deterministicCreateCollectionEntry(tb, "users", "fsm:snapshot:create:users")
	insert := deterministicInsertBatchEntry(tb, "users", "fsm:snapshot:insert:users", nativewire.DocumentFormatJSON, [][]byte{[]byte("u1"), []byte("u-large")}, [][]byte{[]byte(`{"_id":"u1","name":"small"}`), doc})
	results, err := fsm.ApplyCommittedEntriesV1([]CommittedEntryV1{
		committedCommand(2, 1, create),
		committedCommand(2, 2, insert),
	})
	if err != nil {
		tb.Fatalf("ApplyCommittedEntriesV1: %v results=%+v", err, results)
	}
	if len(results) != 2 {
		tb.Fatalf("results=%d, want 2", len(results))
	}
	assertSnapshotApplyResult(tb, results[0], raftentry.ApplyStatusApplied, 1)
	assertSnapshotApplyResult(tb, results[1], raftentry.ApplyStatusApplied, 2)
}

func assertSnapshotApplyResult(tb testing.TB, result raftentry.ApplyResultV1, wantStatus raftentry.ApplyStatusV1, wantAffected int64) {
	tb.Helper()
	if result.Status != wantStatus || result.AffectedCount != wantAffected {
		tb.Fatalf("apply result=%+v want status=%s affected=%d", result, wantStatus, wantAffected)
	}
}

func assertSnapshotDocument(t *testing.T, fsm *FSM, id string, want []byte) {
	t.Helper()
	coll, err := collections.NewCollectionManager(fsm.db).OpenCollection("users")
	if err != nil {
		t.Fatalf("OpenCollection users: %v", err)
	}
	got, err := coll.Get([]byte(id))
	if err != nil {
		t.Fatalf("Get %s: %v", id, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("document %s=%s want %s", id, got, want)
	}
}

func assertSnapshotValueLogHasFile(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir value log %s: %v", dir, err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err == nil && info.Mode().IsRegular() && info.Size() > 0 {
			return
		}
	}
	t.Fatalf("value log %s has no non-empty segment files", dir)
}

func stringsRepeatForSnapshotTest(value string, count int) string {
	var b strings.Builder
	for i := 0; i < count; i++ {
		b.WriteString(value)
	}
	return b.String()
}
