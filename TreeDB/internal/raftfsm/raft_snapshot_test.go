package raftfsm

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/template"
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

	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}
	assertSnapshotValueLogHasFile(t, raftcluster.ValueLogDir(sourceDir))
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

func TestRaftSnapshotV1InstallReplacesStaleFormatConfig(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()

	sourceDoc := []byte(`{"_id":"u-large","name":"source-format"}`)
	applySnapshotSourceEntries(t, sourceFSM, sourceDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}
	sourceFormatPath := filepath.Join(raftcluster.MainDBDir(sourceDir), raftSnapshotFormatConfigFileV1)
	sourceFormat, err := os.ReadFile(sourceFormatPath)
	if err != nil {
		t.Fatalf("ReadFile source format config: %v", err)
	}

	targetDir := filepath.Join(root, "target")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	targetFormatPath := filepath.Join(raftcluster.MainDBDir(targetDir), raftSnapshotFormatConfigFileV1)
	if err := os.WriteFile(targetFormatPath, []byte(`{"version":999}`), 0o600); err != nil {
		t.Fatalf("WriteFile stale target format config: %v", err)
	}
	staleTransientPath := filepath.Join(targetDir, "index.db.bak")
	if err := os.WriteFile(staleTransientPath, []byte("stale-index-backup"), 0o600); err != nil {
		t.Fatalf("WriteFile stale transient file: %v", err)
	}

	if err := targetFSM.InstallRaftSnapshotV1(bytes.NewReader(snapshot.Payload)); err != nil {
		t.Fatalf("InstallRaftSnapshotV1 with stale target format config: %v", err)
	}
	targetFormat, err := os.ReadFile(targetFormatPath)
	if err != nil {
		t.Fatalf("ReadFile target format config: %v", err)
	}
	if !bytes.Equal(targetFormat, sourceFormat) {
		t.Fatalf("target format config=%s want source %s", targetFormat, sourceFormat)
	}
	if _, err := os.Stat(staleTransientPath); !os.IsNotExist(err) {
		t.Fatalf("stale transient file survived snapshot restore: err=%v", err)
	}
	assertSnapshotDocument(t, targetFSM, "u-large", sourceDoc)
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

func TestRaftSnapshotV1InstallReplacesSideStores(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	sourceDir := filepath.Join(sourceRoot, "maindb")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, false)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTest(t, sourceDB, sourceDir, true)
	defer func() { _ = sourceFSM.Close() }()
	applySnapshotSourceEntries(t, sourceFSM, []byte(`{"_id":"u-large","name":"source"}`))
	sourceDict := filepath.Join(sourceRoot, "dictdb")
	if err := os.MkdirAll(sourceDict, 0o700); err != nil {
		t.Fatalf("MkdirAll source dictdb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDict, "sentinel"), []byte("source-side-store"), 0o600); err != nil {
		t.Fatalf("WriteFile source sentinel: %v", err)
	}
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}

	targetRoot := filepath.Join(root, "target")
	targetDir := filepath.Join(targetRoot, "maindb")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTest(t, targetDB, targetDir, true)
	defer func() { _ = targetFSM.Close() }()
	staleTemplate := filepath.Join(targetRoot, "templatedb")
	if err := os.MkdirAll(staleTemplate, 0o700); err != nil {
		t.Fatalf("MkdirAll target templatedb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staleTemplate, "stale"), []byte("stale-side-store"), 0o600); err != nil {
		t.Fatalf("WriteFile target stale side store: %v", err)
	}

	if err := targetFSM.InstallRaftSnapshotV1(bytes.NewReader(snapshot.Payload)); err != nil {
		t.Fatalf("InstallRaftSnapshotV1: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(targetRoot, "dictdb", "sentinel"))
	if err != nil {
		t.Fatalf("ReadFile restored side-store sentinel: %v", err)
	}
	if string(got) != "source-side-store" {
		t.Fatalf("restored side-store sentinel=%q", got)
	}
	if _, err := os.Stat(staleTemplate); !os.IsNotExist(err) {
		t.Fatalf("stale target side store exists after restore: err=%v", err)
	}
}

func TestRaftSnapshotV1InstallReopensRestoredMainDBForRootLayout(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	sourceDir := filepath.Join(sourceRoot, "maindb")
	sourceDB := openRaftSnapshotFSMTestDB(t, sourceDir, true)
	defer func() { _ = sourceDB.Close() }()
	sourceFSM := openRaftSnapshotFSMForTestWithClusterDir(t, sourceDB, sourceRoot, true)
	defer func() { _ = sourceFSM.Close() }()

	sourceDoc := []byte(`{"_id":"u-large","name":"root-layout","payload":"` + stringsRepeatForSnapshotTest("x", 8192) + `"}`)
	applySnapshotSourceEntries(t, sourceFSM, sourceDoc)
	snapshot, err := sourceFSM.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatalf("ExportRaftSnapshotV1: %v", err)
	}

	targetRoot := filepath.Join(root, "target")
	targetDir := filepath.Join(targetRoot, "maindb")
	targetDB := openRaftSnapshotFSMTestDB(t, targetDir, false)
	targetFSM := openRaftSnapshotFSMForTestWithClusterDir(t, targetDB, targetRoot, true)
	defer func() { _ = targetFSM.Close() }()

	if err := targetFSM.InstallRaftSnapshotV1(bytes.NewReader(snapshot.Payload)); err != nil {
		t.Fatalf("InstallRaftSnapshotV1: %v", err)
	}
	if err := targetFSM.VerifyInstalledSnapshotManifestV1(snapshot.Manifest); err != nil {
		t.Fatalf("VerifyInstalledSnapshotManifestV1: %v", err)
	}
	assertSnapshotDocument(t, targetFSM, "u-large", sourceDoc)
	if _, err := os.Stat(filepath.Join(targetRoot, "index.db")); !os.IsNotExist(err) {
		t.Fatalf("restore reopened TreeDB root as backend DB: index.db stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "index.db")); err != nil {
		t.Fatalf("restored main DB index missing: %v", err)
	}
}

func TestRaftSnapshotV1RestoreWiresRestoredSideStoreLookups(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	sideRoot := snapshotSideStoreRootV1(mainDir)
	ctx := context.Background()

	dictBackend, err := backenddb.Open(backenddb.Options{
		Dir:                    filepath.Join(sideRoot, "dictdb"),
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open dictdb: %v", err)
	}
	dictStore := dictdb.New(dictBackend)
	dictBytes := []byte("restored-dict")
	dictID, err := dictStore.PutDictBytes(ctx, dictBytes)
	if err != nil {
		t.Fatalf("PutDictBytes: %v", err)
	}
	if err := dictBackend.Close(); err != nil {
		t.Fatalf("Close dictdb: %v", err)
	}

	templateBackend, err := backenddb.Open(backenddb.Options{
		Dir:                    filepath.Join(sideRoot, "templatedb"),
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open templatedb: %v", err)
	}
	templateStore := templatedb.New(raftSnapshotTemplateBackendKVV1{db: templateBackend}, templatedb.Config{})
	templateBytes := []byte("restored-template")
	templateID, err := templateStore.PutTemplateDef(ctx, templateBytes, nil)
	if err != nil {
		t.Fatalf("PutTemplateDef: %v", err)
	}
	if err := templateBackend.Close(); err != nil {
		t.Fatalf("Close templatedb: %v", err)
	}

	opts := backenddb.Options{
		ReadOnly: true,
		ValueLog: backenddb.ValueLogOptions{
			TemplateMode: template.TemplateOff,
		},
	}
	cleanup, err := wireRaftSnapshotSideStoreLookupsV1(sideRoot, &opts)
	if err != nil {
		t.Fatalf("wireRaftSnapshotSideStoreLookupsV1: %v", err)
	}
	defer func() { _ = cleanup() }()

	if opts.ValueLog.DictLookup == nil {
		t.Fatal("DictLookup was not wired from restored side store")
	}
	gotDict, err := opts.ValueLog.DictLookup(dictID)
	if err != nil {
		t.Fatalf("DictLookup(%d): %v", dictID, err)
	}
	if !bytes.Equal(gotDict, dictBytes) {
		t.Fatalf("DictLookup(%d)=%q want %q", dictID, gotDict, dictBytes)
	}
	if opts.ValueLog.TemplateLookup == nil {
		t.Fatal("TemplateLookup was not wired from restored side store")
	}
	gotTemplate, err := opts.ValueLog.TemplateLookup(templateID)
	if err != nil {
		t.Fatalf("TemplateLookup(%d): %v", templateID, err)
	}
	if !bytes.Equal(gotTemplate, templateBytes) {
		t.Fatalf("TemplateLookup(%d)=%q want %q", templateID, gotTemplate, templateBytes)
	}
}

func TestRaftSnapshotV1ExportRejectsTopLevelSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "outside")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatalf("WriteFile symlink target: %v", err)
	}
	link := filepath.Join(root, "value_vlog")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unsupported on this platform: %v", err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := appendRaftSnapshotStoragePathV1(tw, "value_vlog", link)
	_ = tw.Close()
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("appendRaftSnapshotStoragePathV1 symlink error=%v, want symlink rejection", err)
	}
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
	return openRaftSnapshotFSMForTestWithClusterDir(t, db, dbDir, forceValuePointers)
}

func openRaftSnapshotFSMForTestWithClusterDir(t testing.TB, db *backenddb.DB, clusterDir string, forceValuePointers bool) *FSM {
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
		Cluster:                  validFSMClusterConfig(clusterDir),
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
