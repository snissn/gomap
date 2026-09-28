package raftfsm

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestCapturedRaftSnapshotV1RebindsLargeDictionaryBeforeMainLookup(t *testing.T) {
	requireRaftSnapshotInstallSupportedV1(t)
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	sourceDir := filepath.Join(sourceRoot, "maindb")
	ctx := context.Background()

	dictDir := filepath.Join(sourceRoot, "dictdb")
	dictStore, err := dictdb.Open(dictDir, backenddb.Options{ChunkSize: 64 * 1024, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	dictBytes := bytes.Repeat([]byte("captured-pointer-dictionary|"), 512)
	dictID, err := dictStore.PutDictBytes(ctx, dictBytes)
	if err != nil {
		_ = dictStore.Close()
		t.Fatal(err)
	}
	resources, err := dictStore.CaptureDictionaryResources(ctx, dictID)
	if err != nil {
		_ = dictStore.Close()
		t.Fatal(err)
	}
	if got := resources.Len(); got != 2 {
		resources.Release()
		_ = dictStore.Close()
		t.Fatalf("dictionary physical closure=%d want index and value-log segment", got)
	}
	resources.Release()
	if err := dictStore.Close(); err != nil {
		t.Fatal(err)
	}

	opts := backenddb.Options{Dir: sourceDir, CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true}
	closeSides, err := wireRaftSnapshotSideStoreLookupsV1(sourceRoot, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSides()
	sourceDB, err := backenddb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceDB.Close()
	source := openRaftSnapshotFSMForTestWithClusterDir(t, sourceDB, sourceRoot, false)
	defer source.Close()
	doc := []byte(`{"_id":"u-large","payload":"` + string(bytes.Repeat([]byte("d"), 8192)) + `"}`)
	applySnapshotSourceEntries(t, source, doc)

	captured, err := source.CaptureRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Release()
	ready, err := captured.Materialize()
	if err != nil {
		t.Fatalf("materialize pointer-backed side store: %v", err)
	}
	defer ready.Release()

	targetRoot := filepath.Join(root, "target")
	targetDir := filepath.Join(targetRoot, "maindb")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	defer targetDB.Close()
	target := openRaftSnapshotFSMForTestWithClusterDir(t, targetDB, targetRoot, false)
	defer target.Close()
	installRaftSnapshotForTest(t, target, ready)
	assertSnapshotDocument(t, target, "u-large", doc)
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	reopen := backenddb.Options{Dir: targetDir, ReadOnly: true, CommandWAL: true, CommandWALStatsScan: true, DisableBackgroundPrune: true}
	closeReopenedSides, err := wireRaftSnapshotSideStoreLookupsV1(targetRoot, &reopen)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReopenedSides()
	reopened, err := backenddb.Open(reopen)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	gotDict, err := reopen.ValueLog.DictLookup(dictID)
	if err != nil || !bytes.Equal(gotDict, dictBytes) {
		t.Fatalf("reopened dictionary length=%d want=%d err=%v", len(gotDict), len(dictBytes), err)
	}
	collection, err := collections.NewCollectionManager(reopened).OpenCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	gotDoc, err := collection.Get([]byte("u-large"))
	if err != nil || !bytes.Equal(gotDoc, doc) {
		t.Fatalf("reopened document length=%d want=%d err=%v", len(gotDoc), len(doc), err)
	}
}

func TestCapturedRaftSnapshotV1RestoresCutAndReplaysTail(t *testing.T) {
	requireRaftSnapshotInstallSupportedV1(t)
	dir := t.TempDir()
	database := openRaftSnapshotFSMTestDB(t, dir, true)
	defer database.Close()
	source := openRaftSnapshotFSMForTest(t, database, dir, true)
	defer source.Close()
	doc := []byte(`{"_id":"u-large","name":"before"}`)
	applySnapshotSourceEntries(t, source, doc)
	digest, err := source.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.CaptureRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	if _, err := source.CaptureRaftSnapshotV1(); err == nil {
		t.Fatal("second outstanding capture admitted")
	}
	tail := deterministicInsertBatchEntry(t, "users", "snapshot:deferred-tail", nativewire.DocumentFormatJSON, [][]byte{[]byte("tail")}, [][]byte{[]byte(`{"_id":"tail","value":1}`)})
	if _, err := source.ApplyCommittedEntriesV1([]CommittedEntryV1{committedCommand(2, 3, tail)}); err != nil {
		t.Fatal(err)
	}
	ready, err := snapshot.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	if ready.Manifest.LastIncludedIndex != 2 || ready.Manifest.LogicalDigestV1 != digest.Hex() {
		t.Fatalf("wrong cut: %+v", ready.Manifest)
	}
	targetDir := t.TempDir()
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, true)
	defer targetDB.Close()
	target := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer target.Close()
	installRaftSnapshotForTest(t, target, ready)
	if err := target.VerifyInstalledSnapshotManifestV1(ready.Manifest); err != nil {
		t.Fatal(err)
	}
	assertSnapshotDocument(t, target, "u-large", doc)
	first, err := target.ApplyCommittedEntryV1(committedCommand(2, 3, tail))
	if err != nil || first.Status != raftentry.ApplyStatusApplied {
		t.Fatalf("tail apply: %+v %v", first, err)
	}
	replayed, err := target.ApplyCommittedEntryV1(committedCommand(2, 3, tail))
	if err != nil || replayed != first {
		t.Fatalf("exact tail replay: %+v %v, want stored %+v", replayed, err, first)
	}
	duplicate, err := target.ApplyCommittedEntryV1(committedCommand(2, 4, tail))
	if err != nil || duplicate.Status != raftentry.ApplyStatusAlreadyApplied || duplicate.ResultDigest != first.ResultDigest || duplicate.AffectedCount != 0 {
		t.Fatalf("tail idempotency replay: %+v %v, want already-applied with original digest", duplicate, err)
	}
	assertSnapshotDocument(t, target, "tail", []byte(`{"_id":"tail","value":1}`))
	if _, err := source.CaptureRaftSnapshotV1(); err == nil {
		t.Fatal("finalized archive lost export admission before Release")
	}
	if err := ready.Release(); err != nil {
		t.Fatal(err)
	}
	next, err := source.CaptureRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestCapturedRaftSnapshotV1AdmissionAndNeverPersistExpiry(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	dir := t.TempDir()
	database := openRaftSnapshotFSMTestDB(t, dir, true)
	defer database.Close()
	source := openRaftSnapshotFSMForTest(t, database, dir, true)
	defer source.Close()
	applySnapshotSourceEntries(t, source, []byte(`{"_id":"u-large","name":"before"}`))
	defaults, err := (SnapshotCaptureLimitsV1{}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	for _, limits := range []SnapshotCaptureLimitsV1{{MaxFiles: 1}, {MaxBytes: 1}, {MaxStagingBytes: 1}} {
		source.snapshotCaptureLimits = limits
		if snapshot, err := source.CaptureRaftSnapshotV1(); err == nil {
			snapshot.Release()
			t.Fatalf("limits admitted: %+v", limits)
		}
		if source.snapshotOperationActive.Load() {
			t.Fatal("failed capture retained admission")
		}
	}
	source.snapshotCaptureLimits = defaults
	source.snapshotCaptureLimits.Lifetime = 100 * time.Millisecond
	snapshot, err := source.CaptureRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	deadline := time.Now().Add(3 * time.Second)
	for source.snapshotOperationActive.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if source.snapshotOperationActive.Load() {
		t.Fatal("never-Persist capture did not expire")
	}
	if _, err := snapshot.Materialize(); err == nil {
		t.Fatal("expired capture materialized")
	}
	entries, err := os.ReadDir(raftSnapshotStagingDirV1(source.cluster.Layout.SnapshotDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expired capture left staged artifacts: %v", entries)
	}
}

func TestCapturedRaftSnapshotV1CanceledMaterializationCleansStage(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	dir := t.TempDir()
	database := openRaftSnapshotFSMTestDB(t, dir, true)
	defer database.Close()
	source := openRaftSnapshotFSMForTest(t, database, dir, true)
	defer source.Close()
	applySnapshotSourceEntries(t, source, []byte(`{"_id":"u-large","name":"before"}`))
	source.snapshotCaptureLimits.Lifetime = 30 * time.Millisecond
	snapshot, err := source.CaptureRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Release()
	// A bounded local-file hook spans expiry after materialization starts.
	// It always returns, including on failure, so it cannot deadlock teardown.
	first := true
	raftSnapshotBeforeCopyForTest = func() {
		if first {
			first = false
			time.Sleep(60 * time.Millisecond)
		}
	}
	defer func() { raftSnapshotBeforeCopyForTest = nil }()
	if _, err := snapshot.Materialize(); !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("materialize error=%v", err)
	}
	if err := snapshot.Release(); err != nil {
		t.Fatal(err)
	}
	if source.snapshotOperationActive.Load() {
		t.Fatal("canceled capture retained admission")
	}
	entries, err := os.ReadDir(raftSnapshotStagingDirV1(source.cluster.Layout.SnapshotDir))
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging after cancel: %v %v", entries, err)
	}
}

func TestCapturedRaftSnapshotV1UnregisteredSideStoreRefuses(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	dir := t.TempDir()
	database := openRaftSnapshotFSMTestDB(t, dir, true)
	defer database.Close()
	source := openRaftSnapshotFSMForTest(t, database, dir, true)
	defer source.Close()
	applySnapshotSourceEntries(t, source, []byte(`{"_id":"u-large","name":"before"}`))
	side := filepath.Join(snapshotSideStoreRootV1(source.db.Dir()), "dictdb")
	if err := os.MkdirAll(side, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(side, "index.db"), []byte("not an owned index"), 0600); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := source.CaptureRaftSnapshotV1(); err == nil {
		snapshot.Release()
		t.Fatal("unowned side index admitted")
	}
	if source.snapshotOperationActive.Load() {
		t.Fatal("side refusal retained capture")
	}
}

func TestCapturedRaftSnapshotV1FinalizedExpiryAndInstallAdmission(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	dir := t.TempDir()
	database := openRaftSnapshotFSMTestDB(t, dir, true)
	defer database.Close()
	source := openRaftSnapshotFSMForTest(t, database, dir, true)
	defer source.Close()
	applySnapshotSourceEntries(t, source, []byte(`{"_id":"u-large","name":"before"}`))
	source.snapshotCaptureLimits.Lifetime = time.Second
	ready, err := source.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer ready.Release()
	if ready.Manifest.LastIncludedIndex != 2 || ready.ArchivePath == "" {
		t.Fatal("synchronous export lost finalized fields")
	}
	reader, err := ready.OpenArchive()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := source.InstallRaftSnapshotV1(reader); err == nil {
		t.Fatal("install admitted while archive owned")
	}
	assertSnapshotDocument(t, source, "u-large", []byte(`{"_id":"u-large","name":"before"}`))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := ready.Materialize(); err != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := ready.Materialize(); err == nil {
		t.Fatal("finalized snapshot did not expire")
	}
	if !source.snapshotOperationActive.Load() {
		t.Fatal("expiry released admission while reader alive")
	}
	if _, err := os.Stat(ready.ArchivePath); err != nil {
		t.Fatal("expiry deleted active archive", err)
	}
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal("expired archive reader not canceled", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// Expiry cancels the carrier before its AfterFunc finishes owner cleanup.
	// The last reader may close between those two steps.
	cleanupDeadline := time.Now().Add(3 * time.Second)
	for source.snapshotOperationActive.Load() && time.Now().Before(cleanupDeadline) {
		time.Sleep(time.Millisecond)
	}
	if source.snapshotOperationActive.Load() {
		t.Fatal("closed final reader retained admission")
	}
	if _, err := os.Stat(ready.ArchivePath); !os.IsNotExist(err) {
		t.Fatal("expired archive not cleaned", err)
	}
	source.snapshotCaptureLimits.Lifetime = time.Minute
	next, err := source.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release()
}

func TestCapturedRaftSnapshotV1SideDirectoryWithoutIndexRefusesContents(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	for _, nonempty := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "unowned-content"}[nonempty], func(t *testing.T) {
			dir := t.TempDir()
			database := openRaftSnapshotFSMTestDB(t, dir, true)
			defer database.Close()
			source := openRaftSnapshotFSMForTest(t, database, dir, true)
			defer source.Close()
			applySnapshotSourceEntries(t, source, []byte(`{"_id":"u-large","name":"before"}`))
			side := filepath.Join(snapshotSideStoreRootV1(dir), "dictdb")
			if err := os.MkdirAll(side, 0700); err != nil {
				t.Fatal(err)
			}
			if nonempty {
				if err := os.WriteFile(filepath.Join(side, "sentinel"), []byte("unowned"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			snapshot, err := source.ExportRaftSnapshotV1()
			if nonempty {
				if err == nil {
					snapshot.Release()
					t.Fatal("unowned contents silently omitted")
				}
				if source.snapshotOperationActive.Load() {
					t.Fatal("refusal retained admission")
				}
				got, err := os.ReadFile(filepath.Join(side, "sentinel"))
				if err != nil || string(got) != "unowned" {
					t.Fatal("refusal mutated side directory")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer snapshot.Release()
			}
		})
	}
}

func TestCapturedRaftSnapshotV1CloseRetainsNamespaceUntilReaderCloses(t *testing.T) {
	requireRaftSnapshotExportSupportedV1(t)
	dir := t.TempDir()
	database := openRaftSnapshotFSMTestDB(t, dir, true)
	defer database.Close()
	source := openRaftSnapshotFSMForTest(t, database, dir, true)
	defer source.Close()
	applySnapshotSourceEntries(t, source, []byte(`{"_id":"u-large","name":"before"}`))
	ready, err := source.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	defer ready.Release()
	reader, err := ready.OpenArchive()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	opts := Options{DB: database, Cluster: validFSMClusterConfig(dir), StoreOptions: raftapply.DurableApplyStoreOptions{DisableSync: true, AllowInitialIndexGap: true}}
	if reopened, err := Open(opts); err == nil {
		reopened.Close()
		t.Fatal("reopen deleted an owned archive")
	}
	if _, err := os.Stat(ready.ArchivePath); err != nil {
		t.Fatal("live archive disappeared", err)
	}
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatal("Close did not revoke reader", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := os.Stat(ready.ArchivePath); !os.IsNotExist(err) {
		t.Fatal("released archive remains", err)
	}
}

func TestCapturedRaftSnapshotV1PreResultCleanupRetainsBothTargets(t *testing.T) {
	dir := t.TempDir()
	stage := filepath.Join(dir, "treedb-cut-test")
	archive := filepath.Join(dir, "treedb-snapshot-test.tar")
	if err := os.Mkdir(stage, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archive, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	files := &snapshotCapturedFilesV1{stage: stage, partialArchive: archive}
	denied := errors.New("injected cleanup failure")
	raftSnapshotBeforeCleanupForTest = func(string) error { return denied }
	defer func() { raftSnapshotBeforeCleanupForTest = nil }()
	if err := files.close(); !errors.Is(err, denied) {
		t.Fatal(err)
	}
	if files.stage != stage || files.partialArchive != archive {
		t.Fatal("lost cleanup targets")
	}
	raftSnapshotBeforeCleanupForTest = nil
	if err := files.close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{stage, archive} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal(path, err)
		}
	}
}
